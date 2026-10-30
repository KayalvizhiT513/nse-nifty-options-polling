"""Hosted one-second NIFTY collector and live snapshot monitor."""
import csv
import io
from pathlib import Path

import pandas as pd
import streamlit as st

from collector.streamlit_ui import render_collector


def latest_rows(path):
    """Read a complete final snapshot without loading an entire trading day."""
    with Path(path).open("rb") as handle:
        header = handle.readline().decode("utf-8").strip()
        handle.seek(0, 2)
        size = handle.tell()
        window = 256 * 1024
        while True:
            offset = max(0, size - window)
            handle.seek(offset)
            data = handle.read()
            if offset:
                data = data.split(b"\n", 1)[-1]
            else:
                data = data.split(b"\n", 1)[-1]
            rows = list(csv.DictReader(io.StringIO(header + "\n" + data.decode("utf-8"))))
            if not rows:
                return []
            timestamp = rows[-1]["records_timestamp"]
            if offset == 0 or rows[0]["records_timestamp"] != timestamp:
                return [row for row in rows if row["records_timestamp"] == timestamp]
            window *= 2


st.set_page_config(page_title="NIFTY Live Collector", layout="wide")
st.title("NIFTY Live Collector")
st.caption("Background collection continues independently of page refreshes while this host is running.")
service = render_collector()


@st.fragment(run_every="5s")
def render_snapshot(service):
    st.subheader("Latest captured snapshot")
    with service.file_lock:
        folders = sorted(service.data_root.glob("reconstructed_????-??-??"), reverse=True)
    if not folders:
        st.info("Snapshots will appear here after the first successful collection.")
        return
    day = st.selectbox("Trading day", [p.name.removeprefix("reconstructed_") for p in folders])
    folder = service.data_root / f"reconstructed_{day}"
    paths = sorted(folder.glob("expiry_*_option_chain_rows.csv"))
    if not paths:
        st.info("Waiting for CSV snapshots.")
        return
    options = {p.name.split("_")[1]: p for p in paths}
    expiry = st.selectbox("Expiry", list(options))
    with service.file_lock:
        rows = latest_rows(options[expiry])
    if not rows:
        st.info("Waiting for a complete snapshot.")
        return
    df = pd.DataFrame(rows)
    for column in ["strike", "underlying_value", "ce_ltp", "pe_ltp", "ce_oi", "pe_oi"]:
        if column in df:
            df[column] = pd.to_numeric(df[column], errors="coerce")
    st.caption(f"NSE timestamp: {rows[-1]['records_timestamp']} · Observed (IST): {rows[-1]['observed_at']}")
    cols = st.columns(2)
    cols[0].metric("NIFTY spot", rows[-1].get("underlying_value", "—"))
    cols[1].metric("Strikes captured", len(df))
    around_atm = st.checkbox("Show strikes around ATM", value=True)
    if around_atm and "strike" in df and "underlying_value" in df:
        spot = df["underlying_value"].dropna()
        if not spot.empty:
            df = df.assign(distance=(df["strike"] - spot.iloc[-1]).abs()).nsmallest(41, "distance").sort_values("strike")
    columns = ["strike", "ce_ltp", "pe_ltp", "ce_oi", "pe_oi", "ce_chg_oi", "pe_chg_oi", "ce_iv", "pe_iv", "ce_bid", "ce_ask", "pe_bid", "pe_ask"]
    st.dataframe(df[[c for c in columns if c in df]], hide_index=True, width="stretch")


if service:
    render_snapshot(service)
st.caption("Data destination: KayalvizhiT513/nse-nifty-option-nearest-expiry · intraday branch: nifty-live-data · final archive: main")
