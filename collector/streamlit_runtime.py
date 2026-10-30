"""Process-wide collector. Worker threads never call Streamlit APIs."""
import asyncio
import fcntl
import hashlib
import json
import logging
from logging.handlers import RotatingFileHandler
import os
import re
import threading
import time
from collections import deque
from datetime import datetime
from pathlib import Path

from collector.github_archive import git, merge_csv, publish_day
from collector.nse_poller import IST, NSEPoller


def market_open(now=None):
    now = (now or datetime.now(IST)).astimezone(IST)
    return now.weekday() < 5 and (9, 0) <= (now.hour, now.minute) < (15, 45)


class GitHubPublisher:
    """Dedicated bare repository; token travels via askpass, never remote URLs."""
    def __init__(self, root, repository, token, checkpoint_branch="nifty-live-data", archive_branch="main"):
        if not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repository):
            raise ValueError("GitHub repository must be owner/name")
        if checkpoint_branch == archive_branch:
            raise ValueError("Checkpoint branch must differ from the Streamlit deployment branch")
        for branch in (checkpoint_branch, archive_branch):
            git("check-ref-format", "--branch", branch)
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.repository = self.root / "publisher.git"
        askpass = self.root / "askpass.py"
        askpass.write_text("#!/usr/bin/env python3\nimport os, sys\nprint('x-access-token' if 'username' in sys.argv[1].lower() else os.environ['NIFTY_PUSH_TOKEN'])\n")
        askpass.chmod(0o700)
        self.environment = {"GIT_ASKPASS": str(askpass), "GIT_TERMINAL_PROMPT": "0", "NIFTY_PUSH_TOKEN": token}
        self.checkpoint_branch, self.archive_branch = checkpoint_branch, archive_branch
        self.ready = False
        if not self.repository.exists():
            git("init", "--bare", str(self.repository))
            git("remote", "add", "origin", f"https://github.com/{repository}.git", cwd=self.repository)
        # Fetch only requested blobs, rather than the historical data archive.
        git("config", "remote.origin.promisor", "true", cwd=self.repository)
        git("config", "remote.origin.partialclonefilter", "blob:none", cwd=self.repository)
        git("config", "credential.helper", "", cwd=self.repository)

    def run(self, *args):
        return git(*args, cwd=self.repository, env=dict(os.environ, **self.environment))

    def initialize(self):
        if self.ready:
            return
        if not self.run("ls-remote", "--heads", "origin", self.checkpoint_branch).strip():
            self.run("fetch", "--depth=1", "--filter=blob:none", "origin", self.archive_branch)
            parent = self.run("rev-parse", "FETCH_HEAD").decode().strip()
            self.run("push", "origin", f"{parent}:refs/heads/{self.checkpoint_branch}")
        self.ready = True

    def recover(self, output_root):
        """Recover checkpoints after an app reboot, including unarchived past days."""
        self.initialize()
        self.run("fetch", "--depth=1", "origin", self.checkpoint_branch)
        parent = self.run("rev-parse", "FETCH_HEAD").decode().strip()
        self.run("fetch", "--depth=1", "origin", self.archive_branch)
        archive = self.run("rev-parse", "FETCH_HEAD").decode().strip()
        def tree(ref):
            entries = self.run("ls-tree", "-r", ref).decode().splitlines()
            return {entry.split("\t", 1)[1]: entry.split("\t", 1)[0].split()[-1] for entry in entries}
        checkpoints, archived = tree(parent), tree(archive)
        pattern = r"reconstructed_(\d{4}-\d{2}-\d{2})/expiry_[A-Za-z0-9-]+_\d{4}-\d{2}-\d{2}_option_chain_rows\.csv"
        for path, sha in checkpoints.items():
            if not re.fullmatch(pattern, path):
                continue
            # Only materialize data that is absent or different on main.
            if sha == archived.get(path):
                continue
            data = self.run("show", f"{parent}:{path}")
            dest = Path(output_root) / path
            dest.parent.mkdir(parents=True, exist_ok=True)
            if dest.exists():
                data = merge_csv(dest.read_bytes(), [data])
            dest.write_bytes(data)

    def publish(self, day, files, final=False):
        self.initialize()
        return publish_day(day, files, self.archive_branch if final else self.checkpoint_branch,
                           repository=self.repository, environment=self.environment)


class MeasuredPoller(NSEPoller):
    def __init__(self, owner):
        self.owner = owner
        super().__init__(str(owner.data_root))

    async def fetch_json(self, url, source, expiry=None):
        started = time.monotonic()
        with self.owner.lock:
            stats = self.owner.expiries.setdefault(expiry or "expiry-list", {
                "requests": 0, "successes": 0, "errors": 0, "snapshots": 0,
                "last_source_timestamp": None, "last_success_at": None,
                "intervals": deque(maxlen=300), "latencies": deque(maxlen=300),
                "previous_start": None,
            })
            if stats["previous_start"] is not None:
                stats["intervals"].append(started - stats["previous_start"])
            stats["previous_start"] = started
            stats["requests"] += 1
        try:
            result = await super().fetch_json(url, source, expiry)
            with self.owner.lock:
                stats["successes"] += 1
                stats["latencies"].append(time.monotonic() - started)
                stats["last_success_at"] = datetime.now(IST).isoformat(timespec="seconds")
            return result
        except Exception:
            with self.owner.lock:
                stats["errors"] += 1
            raise

    def save_option_chain_rows_if_new_timestamp(self, expiry, payload):
        # Never archive stale previous-day payloads under today's collection.
        timestamp = str((payload.get("records") or {}).get("timestamp", ""))
        if self._parse_ts_date(timestamp) != datetime.now(IST).date().isoformat():
            return False
        with self.owner.file_lock:
            saved = super().save_option_chain_rows_if_new_timestamp(expiry, payload)
        if saved:
            with self.owner.lock:
                stats = self.owner.expiries.get(expiry)
                if stats is not None:
                    stats["snapshots"] += 1
                    stats["last_source_timestamp"] = timestamp
                self.owner.revision += 1
        return saved

    def log_error(self, source, message, expiry=None, error_type=None, extra=None):
        self.owner.logger.warning(json.dumps({"observed_at": self._now_ist(), "source": source,
            "expiry": expiry, "error_type": error_type, "message": str(message)[:500]}))
        with self.owner.lock:
            self.owner.last_error = f"{source}: {error_type or 'Error'}: {str(message)[:200]}"

    @staticmethod
    def _safe_print(message):
        # Snapshot volume belongs in counters, not the hosted application's logs.
        pass


class CollectorService:
    def __init__(self, data_root, publisher, checkpoint_seconds=900):
        self.data_root = Path(data_root)
        self.data_root.mkdir(parents=True, exist_ok=True)
        self.publisher = publisher
        self.checkpoint_seconds = max(60, checkpoint_seconds)
        self.lock, self.file_lock = threading.RLock(), threading.RLock()
        self.stop_event, self.publish_event = threading.Event(), threading.Event()
        self.expiries = {}
        self.revision = 0
        self.last_error = self.last_push_at = self.last_commit = None
        self.last_push_error = None
        self._finalized = {}
        self.recovery_pending = True
        self.state = "Starting"
        self.heartbeat = None
        self.started_at = datetime.now(IST).isoformat(timespec="seconds")
        self.logger = logging.getLogger(f"nifty.{self.data_root}")
        self.logger.setLevel(logging.WARNING)
        self.logger.propagate = False
        if not self.logger.handlers:
            handler = RotatingFileHandler(self.data_root / "poll_errors.jsonl", maxBytes=2_000_000, backupCount=2)
            self.logger.addHandler(handler)
        self.process_lock = None
        self.thread = self.publish_thread = None

    def start(self):
        with self.lock:
            if self.thread and self.thread.is_alive():
                return self
            if self.publish_thread and self.publish_thread.is_alive():
                return self
            self.stop_event.clear()
            self.publish_event.clear()
            handle = (self.data_root / ".collector.lock").open("a")
            try:
                fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                handle.close()
                self.state = "Another collector process owns this directory"
                return self
            self.process_lock = handle
            self.thread = threading.Thread(target=self._run, name="nifty-collector", daemon=True)
            self.thread.start()
        return self

    def _run(self):
        try:
            # Restore before polling so timestamp deduplication survives restarts.
            try:
                self.publisher.recover(self.data_root)
                self.recovery_pending = False
            except Exception as error:
                with self.lock:
                    self.last_push_error = self._safe_error(error)
            self.publish_thread = threading.Thread(target=self._publish_loop, name="nifty-publisher", daemon=True)
            self.publish_thread.start()
            asyncio.run(self._schedule())
        except Exception as error:
            with self.lock:
                self.state, self.last_error = "Stopped after error", self._safe_error(error)
        finally:
            self.stop_event.set()
            self.publish_event.set()
            if self.publish_thread:
                self.publish_thread.join(timeout=310)
            if self.process_lock:
                self.process_lock.close()
                self.process_lock = None

    def _safe_error(self, error):
        message = str(error)
        token = getattr(self.publisher, "environment", {}).get("NIFTY_PUSH_TOKEN")
        if isinstance(token, str) and token:
            message = message.replace(token, "[redacted]")
        return message[-500:]

    async def _schedule(self):
        poller = task = None
        try:
            while not self.stop_event.is_set():
                now = datetime.now(IST)
                with self.lock:
                    self.heartbeat = now.isoformat(timespec="seconds")
                if market_open(now):
                    if task is None or task.done():
                        if task is not None:
                            try:
                                task.result()
                            except Exception as error:
                                with self.lock:
                                    self.last_error = self._safe_error(error)
                        poller = MeasuredPoller(self)
                        task = asyncio.create_task(poller.run())
                    with self.lock:
                        self.state = "Collecting"
                else:
                    if task is not None:
                        poller.request_stop()
                        try:
                            await task
                        except Exception as error:
                            with self.lock:
                                self.last_error = self._safe_error(error)
                        poller = task = None
                        self.publish_event.set()
                    with self.lock:
                        self.state = "Waiting for weekday 09:00 IST"
                await asyncio.sleep(1)
        finally:
            if task:
                poller.request_stop()
                await task

    def _publish_loop(self):
        while not self.stop_event.is_set():
            self._checkpoint()
            self.publish_event.wait(self.checkpoint_seconds)
            self.publish_event.clear()
        self._checkpoint()

    def _checkpoint(self):
        now = datetime.now(IST)
        try:
            if self.recovery_pending:
                with self.file_lock:
                    self.publisher.recover(self.data_root)
                self.recovery_pending = False
            # Capture closed file contents under the same lock used for appends.
            with self.file_lock:
                days = {}
                for folder in sorted(self.data_root.glob("reconstructed_????-??-??")):
                    files = {f"{folder.name}/{p.name}": [p.read_bytes()] for p in sorted(folder.glob("*.csv")) if p.stat().st_size}
                    if files:
                        days[folder.name.removeprefix("reconstructed_")] = files
            for day, files in days.items():
                final = day < now.date().isoformat() or (day == now.date().isoformat() and (now.hour, now.minute) >= (16, 0))
                fingerprints = {path: hashlib.sha256(data[0]).digest() for path, data in files.items()}
                if final and self._finalized.get(day) == fingerprints:
                    continue
                commit = self.publisher.publish(day, files, final=final)
                with self.lock:
                    self.last_commit = commit
                    self.last_push_at = datetime.now(IST).isoformat(timespec="seconds")
                    self.last_push_error = None
                if final:
                    # No active writes to closed days. Keep the files for the
                    # viewer; skip their re-upload unless their contents change.
                    self._finalized[day] = fingerprints
        except Exception as error:
            with self.lock:
                self.last_push_error = self._safe_error(error)

    def status(self):
        with self.lock:
            rows = []
            for expiry, stats in sorted(self.expiries.items()):
                if expiry == "expiry-list":
                    continue
                def p95(values):
                    values = sorted(values)
                    return round(values[min(len(values) - 1, int(len(values) * .95))], 3) if values else None
                rows.append({"Expiry": expiry, "Requests": stats["requests"], "Successful": stats["successes"],
                    "Errors": stats["errors"], "Saved snapshots": stats["snapshots"],
                    "Poll interval p95 (s)": p95(stats["intervals"]), "Response p95 (s)": p95(stats["latencies"]),
                    "Last NSE timestamp": stats["last_source_timestamp"], "Last success": stats["last_success_at"]})
            return {"state": self.state, "heartbeat": self.heartbeat, "started_at": self.started_at,
                "last_push_at": self.last_push_at, "last_commit": self.last_commit,
                "last_error": self.last_error, "last_push_error": self.last_push_error,
                "alive": bool(self.thread and self.thread.is_alive()), "expiries": rows}


# Module state survives Streamlit script reruns and cache clears in one process.
_SERVICES = {}
_SERVICES_LOCK = threading.Lock()


def get_service(data_root, publisher, checkpoint_seconds=900):
    key = str(Path(data_root).resolve())
    with _SERVICES_LOCK:
        if key not in _SERVICES:
            _SERVICES[key] = CollectorService(key, publisher, checkpoint_seconds)
        return _SERVICES[key].start()
