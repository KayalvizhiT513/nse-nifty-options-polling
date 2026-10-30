import csv
import io
import os
import subprocess
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

from collector.github_archive import merge_csv, publish_day, read_csv, verify_rows


def snapshot(timestamp, price="100", observed="2026-10-29 09:15:01"):
    out = io.StringIO(newline="")
    writer = csv.writer(out)
    writer.writerow(["records_timestamp", "observed_at", "expiry", "strike", "ce_ltp"])
    writer.writerow([timestamp, observed, "03-Nov-2026", "25000", price])
    return out.getvalue().encode()


class ExportTests(unittest.TestCase):

    def test_merge_overlapping_snapshots_and_retry(self):
        a = snapshot("29-Oct-2026 09:15:00")
        b = snapshot("29-Oct-2026 09:15:17", "101")
        overlap = snapshot("29-Oct-2026 09:15:00", observed="2026-10-29 09:15:02")
        merged = merge_csv(a, [b, overlap, b])
        self.assertEqual(len(read_csv(merged)[1]), 2)
        self.assertEqual(merge_csv(merged, [a, b]), merged)
        verify_rows(merged, [a, b, overlap])

    def test_cleanup_refused_for_changed_or_missing_source_values(self):
        a = snapshot("29-Oct-2026 09:15:00")
        with self.assertRaises(ValueError):
            verify_rows(a, [snapshot("29-Oct-2026 09:15:00", "999")])
        with self.assertRaises(ValueError):
            verify_rows(a, [snapshot("29-Oct-2026 09:15:17")])


    def test_publish_real_git_remote_preserves_checkout_and_other_files(self):
        def run(*args, cwd):
            return subprocess.check_output(["git", *args], cwd=cwd, stderr=subprocess.DEVNULL)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            remote, work = root / "remote.git", root / "work"
            run("init", "--bare", "--initial-branch=main", str(remote), cwd=root)
            run("clone", str(remote), str(work), cwd=root)
            run("config", "user.name", "Test", cwd=work)
            run("config", "user.email", "test@example.com", cwd=work)
            (work / "keep.txt").write_text("preserve me")
            run("add", "keep.txt", cwd=work)
            run("commit", "-m", "initial", cwd=work)
            run("push", "origin", "main", cwd=work)
            original = run("rev-parse", "HEAD", cwd=work)
            path = "reconstructed_2026-10-29/expiry_03-Nov-2026_2026-10-29_option_chain_rows.csv"
            a, b = snapshot("29-Oct-2026 09:15:00"), snapshot("29-Oct-2026 09:15:17")
            previous = Path.cwd()
            try:
                os.chdir(work)
                publish_day("2026-10-29", {path: [a]})
                publish_day("2026-10-29", {path: [a, b]})
                head = run("rev-parse", "FETCH_HEAD", cwd=work)
                publish_day("2026-10-29", {path: [a, b]})
                self.assertEqual(run("rev-parse", "FETCH_HEAD", cwd=work), head)
                self.assertEqual(run("rev-parse", "HEAD", cwd=work), original)
                self.assertEqual(run("status", "--porcelain", cwd=work), b"")
                self.assertEqual(run("show", "FETCH_HEAD:keep.txt", cwd=work), b"preserve me")
                verify_rows(run("show", f"FETCH_HEAD:{path}", cwd=work), [a, b])
            finally:
                os.chdir(previous)


if __name__ == "__main__":
    unittest.main()
