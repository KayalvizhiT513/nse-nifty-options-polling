import asyncio
import csv
import io
import subprocess
import tempfile
import threading
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import Mock, patch

from collector.nse_poller import IST
from collector.streamlit_runtime import CollectorService, GitHubPublisher, MeasuredPoller, get_service, market_open


def csv_snapshot():
    output = io.StringIO(newline="")
    writer = csv.writer(output)
    writer.writerow(["records_timestamp", "observed_at", "expiry", "strike", "ce_ltp"])
    writer.writerow(["30-Oct-2026 09:15:00", "2026-10-30 09:15:01", "03-Nov-2026", "25000", "100"])
    return output.getvalue().encode()


class StreamlitCollectorTests(unittest.TestCase):
    def test_completed_day_archived_once_and_failed_upload_retains_csv(self):
        with tempfile.TemporaryDirectory() as temp, patch("collector.streamlit_runtime.datetime") as clock:
            clock.now.return_value = datetime(2026, 10, 30, 16, 0, tzinfo=IST)
            publisher = Mock()
            publisher.publish.return_value = "a" * 40
            service = CollectorService(temp, publisher)
            path = Path(temp) / "reconstructed_2026-10-30" / "expiry_03-Nov-2026_2026-10-30_option_chain_rows.csv"
            path.parent.mkdir()
            path.write_bytes(csv_snapshot())
            publisher.publish.side_effect = RuntimeError("network unavailable")
            service._checkpoint()
            self.assertTrue(path.exists())
            self.assertIn("network unavailable", service.last_push_error)
            publisher.publish.side_effect = None
            publisher.publish.reset_mock()
            service._checkpoint()
            service._checkpoint()
            publisher.publish.assert_called_once()
            self.assertTrue(publisher.publish.call_args.kwargs["final"])
            self.assertIsNone(service.last_push_error)

    def test_market_window_in_ist(self):
        self.assertTrue(market_open(datetime(2026, 10, 30, 3, 30, tzinfo=timezone.utc)))
        self.assertFalse(market_open(datetime(2026, 10, 30, 10, 15, tzinfo=timezone.utc)))
        self.assertFalse(market_open(datetime(2026, 10, 31, 4, 0, tzinfo=timezone.utc)))

    def test_singleton_and_process_lock(self):
        async def schedule(service):
            while not service.stop_event.is_set():
                await asyncio.sleep(.01)
        with tempfile.TemporaryDirectory() as temp, patch.object(CollectorService, "_schedule", schedule), patch.object(CollectorService, "_checkpoint"):
            publisher = Mock()
            first = get_service(temp, publisher)
            second = get_service(temp, publisher)
            competing = CollectorService(temp, publisher)
            try:
                self.assertIs(first, second)
                competing.start()
                self.assertIn("Another collector", competing.status()["state"])
                self.assertTrue(first.thread.is_alive())
            finally:
                first.stop_event.set()
                first.publish_event.set()
                first.thread.join(timeout=3)
                self.assertFalse(first.thread.is_alive())

    def test_snapshot_deduplication_and_stale_day_rejection(self):
        with tempfile.TemporaryDirectory() as temp:
            owner = CollectorService(temp, Mock())
            now = datetime.now(IST)
            payload = {"records": {"timestamp": now.strftime("%d-%b-%Y %H:%M:%S"), "underlyingValue": 25000,
                "data": [{"strikePrice": 25000, "CE": {"lastPrice": 100}}]}}
            async def exercise():
                poller = MeasuredPoller(owner)
                self.assertTrue(poller.save_option_chain_rows_if_new_timestamp("03-Nov-2026", payload))
                self.assertFalse(poller.save_option_chain_rows_if_new_timestamp("03-Nov-2026", payload))
                payload["records"]["timestamp"] = "01-Jan-2000 09:15:00"
                self.assertFalse(poller.save_option_chain_rows_if_new_timestamp("03-Nov-2026", payload))
            asyncio.run(exercise())
            files = list(Path(temp).glob("reconstructed_*/*.csv"))
            self.assertEqual(len(files), 1)
            self.assertEqual(len(files[0].read_text().splitlines()), 2)

    def test_real_git_checkpoints_recovery_and_final_archive(self):
        def run(*args, cwd):
            return subprocess.check_output(["git", *args], cwd=cwd, stderr=subprocess.DEVNULL)
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            remote, seed = root / "remote.git", root / "seed"
            run("init", "--bare", "--initial-branch=main", str(remote), cwd=root)
            run("clone", str(remote), str(seed), cwd=root)
            run("config", "user.name", "Test", cwd=seed)
            run("config", "user.email", "test@example.com", cwd=seed)
            (seed / "app.py").write_text("preserve app")
            run("add", "app.py", cwd=seed)
            run("commit", "-m", "initial", cwd=seed)
            run("push", "origin", "main", cwd=seed)
            initial = run("rev-parse", "main", cwd=remote)
            publisher = GitHubPublisher(root / "publisher", "owner/repo", "test-placeholder")
            run("remote", "set-url", "origin", str(remote), cwd=publisher.repository)
            path = "reconstructed_2026-10-30/expiry_03-Nov-2026_2026-10-30_option_chain_rows.csv"
            data = csv_snapshot()
            publisher.publish("2026-10-30", {path: [data]})
            self.assertEqual(run("rev-parse", "main", cwd=remote), initial)
            recovered = root / "recovered"
            publisher.recover(recovered)
            self.assertEqual((recovered / path).read_bytes(), data)
            publisher.publish("2026-10-30", {path: [data]}, final=True)
            self.assertNotEqual(run("rev-parse", "main", cwd=remote), initial)
            self.assertEqual(run("show", "main:app.py", cwd=remote), b"preserve app")
            self.assertEqual(run("show", f"main:{path}", cwd=remote), data)
            after_archive = root / "after-archive"
            publisher.recover(after_archive)
            self.assertFalse((after_archive / path).exists())
            self.assertNotIn("test-placeholder", (publisher.root / "askpass.py").read_text())


if __name__ == "__main__":
    unittest.main()
