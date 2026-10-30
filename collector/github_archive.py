"""Merge CSV snapshots and publish verified GitHub commits without touching the app checkout."""
import csv
import io
import os
import subprocess
import tempfile
import time
from datetime import datetime
from pathlib import Path

TS_FORMAT = "%d-%b-%Y %H:%M:%S"


def read_csv(data):
    reader = csv.DictReader(io.StringIO(data.decode("utf-8"), newline=""))
    if not reader.fieldnames or not {"records_timestamp", "expiry", "strike", "observed_at"} <= set(reader.fieldnames):
        raise ValueError("Invalid CSV schema")
    return reader.fieldnames, list(reader)


def row_key(row):
    return row["records_timestamp"], row["expiry"], row["strike"]


def merge_csv(existing, snapshots):
    """Keep existing rows on overlap; add new snapshots in source-time order."""
    columns, baseline = read_csv(existing or snapshots[0])
    rows = {row_key(row): row for row in baseline}
    for snapshot in snapshots:
        fields, incoming = read_csv(snapshot)
        if fields != columns:
            raise ValueError("CSV schema changed; retain snapshots for manual review")
        for row in incoming:
            rows.setdefault(row_key(row), row)
    ordered = sorted(rows.values(), key=lambda r: (datetime.strptime(r["records_timestamp"], TS_FORMAT), float(r["strike"]) if r["strike"] else float("inf")))
    output = io.StringIO(newline="")
    writer = csv.DictWriter(output, fieldnames=columns)
    writer.writeheader()
    writer.writerows(ordered)
    return output.getvalue().encode()


def verify_rows(remote, snapshots):
    _, committed = read_csv(remote)
    lookup = {row_key(row): row for row in committed}
    for snapshot in snapshots:
        fields, rows = read_csv(snapshot)
        for row in rows:
            saved = lookup.get(row_key(row))
            # Overlapping workers observe the same source snapshot at different
            # wall times. Source values must match; observed_at may differ.
            if saved is None or any(saved[k] != row[k] for k in fields if k != "observed_at"):
                raise ValueError("Remote data differs from snapshot; cleanup refused")


def git(*args, data=None, env=None, cwd=None):
    result = subprocess.run(["git", *args], input=data, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env, cwd=cwd, timeout=300)
    if result.returncode:
        # Git errors do not contain the Supabase credential.
        raise RuntimeError(result.stderr.decode(errors="replace"))
    return result.stdout


def publish_day(day, files, branch="main", repository=None, environment=None):
    def run(*args, **kwargs):
        env = dict(os.environ)
        env.update(environment or {})
        env.update(kwargs.pop("env", {}))
        return git(*args, **kwargs, env=env, cwd=repository)

    for attempt in range(4):
        run("fetch", "--depth=1", "origin", branch)
        parent = run("rev-parse", "FETCH_HEAD").decode().strip()
        with tempfile.TemporaryDirectory(prefix="nifty-index-") as temp:
            env = dict(os.environ, GIT_INDEX_FILE=str(Path(temp) / "index"),
                       GIT_AUTHOR_NAME="github-actions[bot]", GIT_COMMITTER_NAME="github-actions[bot]",
                       GIT_AUTHOR_EMAIL="41898282+github-actions[bot]@users.noreply.github.com",
                       GIT_COMMITTER_EMAIL="41898282+github-actions[bot]@users.noreply.github.com")
            run("read-tree", parent, env=env)
            for path, snapshots in sorted(files.items()):
                exists = bool(run("ls-tree", "--name-only", parent, "--", path).strip())
                baseline = run("show", f"{parent}:{path}") if exists else None
                merged = merge_csv(baseline, snapshots)
                sha = run("hash-object", "-w", "--stdin", data=merged).decode().strip()
                run("update-index", "--add", "--cacheinfo", f"100644,{sha},{path}", env=env)
            # Historical blobs remain on origin in this partial clone. Every
            # added blob was written above; allow unchanged promised blobs so
            # Git does not lazily download the whole archive while writing a tree.
            tree = run("write-tree", "--missing-ok", env=env).decode().strip()
            if tree == run("rev-parse", f"{parent}^{{tree}}").decode().strip():
                commit = parent
            else:
                commit = run("commit-tree", tree, "-p", parent, data=f"Collect Nifty option data for {day}\n".encode(), env=env).decode().strip()
                try:
                    run("push", "origin", f"{commit}:refs/heads/{branch}")
                except RuntimeError:
                    if attempt == 3:
                        raise
                    time.sleep(2 ** attempt)
                    continue
            # Verify against a fresh read of the remote branch, not our checkout.
            run("fetch", "--depth=1", "origin", branch)
            remote = run("rev-parse", "FETCH_HEAD").decode().strip()
            for path, snapshots in files.items():
                verify_rows(run("show", f"{remote}:{path}"), snapshots)
            print(f"Verified {day}: {commit[:12]}, {len(files)} CSV files", flush=True)
            return commit
    raise RuntimeError("Unable to publish after four attempts")
