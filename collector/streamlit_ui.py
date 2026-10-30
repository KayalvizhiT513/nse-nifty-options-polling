"""Streamlit-only controls; runtime and publisher do not import Streamlit."""
import os
import tempfile
from pathlib import Path

import streamlit as st

from collector.streamlit_runtime import GitHubPublisher, get_service


@st.cache_resource
def configured_service(repository, token, root, checkpoint_seconds, checkpoint_branch, archive_branch):
    publisher = GitHubPublisher(Path(root) / "git", repository, token, checkpoint_branch, archive_branch)
    return get_service(Path(root) / "data", publisher, checkpoint_seconds)


def render_collector():
    try:
        config = dict(st.secrets.get("collector", {}))
    except (FileNotFoundError, KeyError):
        config = {}
    enabled = config.get("enabled", False) is True
    token = config.get("github_token", "")
    service = None
    with st.expander("Live collector", expanded=True):
        st.caption("Collector heartbeat")
        if not enabled:
            st.info("Automatic collection is disabled. Configure the collector section in app Secrets to enable it.")
        elif not token:
            st.warning("Collection needs a GitHub token in app Secrets so captured data can be checkpointed.")
        else:
            try:
                service = configured_service(
                    config.get("repository", "KayalvizhiT513/nse-nifty-option-nearest-expiry"),
                    token,
                    config.get("data_root", os.path.join(tempfile.gettempdir(), "nifty-streamlit")),
                    int(config.get("checkpoint_seconds", 900)),
                    config.get("checkpoint_branch", "nifty-live-data"),
                    config.get("archive_branch", "main"),
                )
            except Exception:
                st.error("Unable to initialize the collector. Check repository, branch and storage settings in Secrets.")
        if service:
            render_status(service)
    return service


@st.fragment(run_every="5s")
def render_status(service):
    service.start()
    status = service.status()
    st.write(f"**{status['state']}**")
    cols = st.columns(3)
    cols[0].metric("Expiries tracked", len(status["expiries"]))
    cols[1].metric("Saved snapshots", sum(row["Saved snapshots"] for row in status["expiries"]))
    cols[2].metric("Request errors", sum(row["Errors"] for row in status["expiries"]))
    st.caption(f"Heartbeat (IST): {status['heartbeat'] or 'Starting'} · Last GitHub checkpoint: {status['last_push_at'] or 'Pending'}")
    if status["last_commit"]:
        st.caption(f"Verified commit: {status['last_commit'][:12]}")
    if status["last_push_error"]:
        st.error(f"GitHub checkpoint failed; local files retained. {status['last_push_error']}")
    if status["last_error"]:
        st.warning(f"Most recent collection error: {status['last_error']}")
    if status["access_probe"]:
        probe = status["access_probe"]
        st.caption(f"Single-request cloud test (IST): {probe['at']} · {probe['status']} · no parallel expiry tasks")
        if probe.get("error"):
            st.error(f"Single-request result: {probe['error']}")
        elif probe.get("source_timestamp"):
            st.caption(f"Test NSE timestamp: {probe['source_timestamp']}")
    if status["expiries"]:
        st.dataframe(status["expiries"], hide_index=True, width="stretch")
    st.caption("Polling targets one request per second per expiry during weekday 09:00–15:45 IST. Fresh NSE timestamps may arrive less often.")
