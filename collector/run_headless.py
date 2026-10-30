"""Run the shared collector on a bounded GitHub-hosted job."""
import argparse
import json
import os
import signal
import tempfile
from datetime import datetime, timedelta
from pathlib import Path

from collector.nse_poller import IST
from collector.streamlit_runtime import CollectorService, GitHubPublisher


def deadline_for(now, minutes=0, end_at="16:15"):
    if minutes:
        if not 1 <= minutes <= 330:
            raise ValueError("minutes must be between 1 and 330")
        return now + timedelta(minutes=minutes)
    hour, minute = map(int, end_at.split(":"))
    end = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    return min(end, now + timedelta(minutes=350))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--minutes", type=int, default=0)
    parser.add_argument("--end-at", default="16:15", help="IST end time for today's session")
    args = parser.parse_args()
    token = os.environ.get("NIFTY_PUSH_TOKEN")
    if not token:
        raise SystemExit("Configure polling-repository Actions secret NIFTY_ARCHIVE_TOKEN (archive Contents: read/write)")
    deadline = deadline_for(datetime.now(IST), args.minutes, args.end_at)
    if datetime.now(IST) >= deadline:
        print("Session window has ended; nothing to collect")
        return
    with tempfile.TemporaryDirectory(prefix="nifty-runner-") as temp:
        root = Path(temp)
        publisher = GitHubPublisher(root / "git", "KayalvizhiT513/nse-nifty-option-nearest-expiry", token)
        service = CollectorService(root / "data", publisher, checkpoint_seconds=300)
        # Credential/recovery failures must fail the job before starting collection.
        publisher.recover(service.data_root)
        service.recovery_pending = False
        def stop(*_):
            service.stop_event.set()
            service.publish_event.set()
        for sig in (signal.SIGINT, signal.SIGTERM):
            signal.signal(sig, stop)
        print("Collection deadline (IST):", deadline.isoformat(), flush=True)
        service.start()
        try:
            while datetime.now(IST) < deadline and service.thread.is_alive() and not service.stop_event.is_set():
                status = service.status()
                print(json.dumps(status), flush=True)
                if service.stop_event.wait(30):
                    break
        finally:
            stop()
            service.thread.join(timeout=330)
        if service.thread.is_alive():
            raise SystemExit("Collector did not finish its final checkpoint in time")
        status = service.status()
        print(json.dumps(status), flush=True)
        if status["last_push_error"]:
            raise SystemExit("Final GitHub checkpoint failed; inspect the previous status")
        if not any(row["Saved snapshots"] for row in status["expiries"]):
            raise SystemExit("No new snapshots saved in this session; inspect NSE access probe and market hours")
        if not status["last_push_at"]:
            raise SystemExit("No verified GitHub checkpoint was completed")


if __name__ == "__main__":
    main()
