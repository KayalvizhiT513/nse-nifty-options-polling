import asyncio
import os
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, time as dtime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import aiohttp
import pandas as pd
import streamlit as st

from nse_nifty_options_live_data import IST, NSEPoller, OPTION_CHAIN_URL


MARKET_OPEN = dtime(9, 0)
MARKET_CLOSE = dtime(15, 30)
DEFAULT_POLL_SECS = 20
TS_FORMAT = "%d-%b-%Y %H:%M:%S"


@dataclass(frozen=True)
class SupabaseConfig:
    url: str
    service_role_key: str
    snapshots_table: str = "option_chain_snapshots"
    rows_table: str = "option_chain_rows"

    @staticmethod
    def from_env_or_secrets() -> Optional["SupabaseConfig"]:
        def read(name: str) -> str:
            try:
                v = st.secrets.get(name)  # type: ignore[attr-defined]
            except Exception:
                v = None
            if v is None:
                v = os.getenv(name)
            return (str(v) if v is not None else "").strip()

        url = read("SUPABASE_URL")
        key = read("SUPABASE_SERVICE_ROLE_KEY")
        snapshots_table = read("SUPABASE_SNAPSHOTS_TABLE") or "option_chain_snapshots"
        rows_table = read("SUPABASE_ROWS_TABLE") or "option_chain_rows"
        if not url or not key:
            return None
        return SupabaseConfig(
            url=url.rstrip("/"),
            service_role_key=key,
            snapshots_table=snapshots_table,
            rows_table=rows_table,
        )


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_records_at(records_timestamp: Optional[str]) -> Optional[str]:
    if not records_timestamp:
        return None
    try:
        dt = datetime.strptime(records_timestamp, TS_FORMAT).replace(tzinfo=IST)
        return dt.astimezone(timezone.utc).isoformat()
    except ValueError:
        return None


class SupabaseWriter:
    def __init__(self, session: aiohttp.ClientSession, cfg: SupabaseConfig) -> None:
        self.session = session
        self.cfg = cfg

    def _headers(self) -> Dict[str, str]:
        return {
            "apikey": self.cfg.service_role_key,
            "Authorization": f"Bearer {self.cfg.service_role_key}",
            "Content-Type": "application/json",
            "Prefer": "resolution=merge-duplicates,return=minimal",
        }

    async def upsert_snapshot(self, row: Dict[str, Any]) -> Tuple[bool, str]:
        url = f"{self.cfg.url}/rest/v1/{self.cfg.snapshots_table}"
        params = {"on_conflict": "symbol,expiry,records_timestamp"}
        async with self.session.post(url, params=params, headers=self._headers(), json=row) as resp:
            if resp.status in (200, 201, 204):
                return True, f"ok:{resp.status}"
            text = await resp.text()
            return False, f"err:{resp.status}:{text[:2000]}"

    async def upsert_rows(self, rows: List[Dict[str, Any]]) -> Tuple[bool, str]:
        if not rows:
            return True, "ok:empty"
        url = f"{self.cfg.url}/rest/v1/{self.cfg.rows_table}"
        params = {"on_conflict": "symbol,expiry,records_timestamp,strike"}
        async with self.session.post(url, params=params, headers=self._headers(), json=rows) as resp:
            if resp.status in (200, 201, 204):
                return True, f"ok:{resp.status}"
            text = await resp.text()
            return False, f"err:{resp.status}:{text[:2000]}"


def now_ist_dt() -> datetime:
    return datetime.now(IST)


def is_market_hours(dt: datetime) -> bool:
    t = dt.timetz().replace(tzinfo=None)
    return MARKET_OPEN <= t <= MARKET_CLOSE


def run_coro(coro: Any) -> Any:
    """
    Streamlit scripts are synchronous, but the NSE logic is asyncio/aiohttp based.
    Run coroutines from a synchronous Streamlit script.
    """
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)

    # If we're already inside a running loop, execute in a helper thread.
    with ThreadPoolExecutor(max_workers=1) as pool:
        fut = pool.submit(asyncio.run, coro)
        return fut.result()


def fetch_expiries(*, ttl_secs: int = 15 * 60) -> List[str]:
    cache_key = "_expiry_cache"
    cached = st.session_state.get(cache_key)
    now = now_ist_dt()
    if isinstance(cached, dict):
        expires_at = cached.get("expires_at")
        expiries = cached.get("expiries")
        if isinstance(expires_at, datetime) and expires_at > now and isinstance(expiries, list):
            return [str(x) for x in expiries if x]

    async def _fetch() -> List[str]:
        poller = NSEPoller(".")
        await poller.create_session()
        try:
            return await poller.fetch_expiry_list()
        finally:
            await poller.close_session()

    expiries = run_coro(_fetch())
    st.session_state[cache_key] = {"expires_at": now + timedelta(seconds=ttl_secs), "expiries": expiries}
    return expiries


def pick_atm_strike(df: pd.DataFrame, underlying_value: Any) -> Optional[float]:
    if df.empty or "strike" not in df.columns:
        return None
    try:
        uv = float(underlying_value)
    except Exception:
        return None
    strikes = pd.to_numeric(df["strike"], errors="coerce").dropna()
    if strikes.empty:
        return None
    idx = (strikes - uv).abs().idxmin()
    try:
        return float(df.loc[idx, "strike"])
    except Exception:
        return None


def poll_one_expiry(
    *,
    expiry: str,
    write_csv: bool,
    sb_cfg: Optional[SupabaseConfig],
    write_supabase: bool,
    write_supabase_rows: bool,
) -> Dict[str, Any]:
    async def _poll() -> Dict[str, Any]:
        poller = NSEPoller(".")
        await poller.create_session()
        try:
            url = OPTION_CHAIN_URL.format(expiry=expiry)
            payload = await poller.fetch_json(url, source="option_chain", expiry=expiry)
            inserted = poller.save_option_chain_rows_if_new_timestamp(expiry, payload) if write_csv else False

            records = payload.get("records") or {}
            records_timestamp = records.get("timestamp")
            underlying_value = records.get("underlyingValue")
            rows = []
            if records_timestamp:
                rows = poller._build_option_chain_rows(
                    expiry=expiry,
                    records_timestamp=str(records_timestamp),
                    observed_at=poller._now_ist(),
                    underlying_value=underlying_value,
                    payload=payload,
                )

            sb_snapshot_status: Optional[str] = None
            sb_rows_status: Optional[str] = None
            if write_supabase and sb_cfg and records_timestamp:
                timeout = aiohttp.ClientTimeout(total=20)
                async with aiohttp.ClientSession(timeout=timeout) as sb_session:
                    writer = SupabaseWriter(sb_session, sb_cfg)
                    snapshot_row = {
                        "symbol": "NIFTY",
                        "expiry": expiry,
                        "records_timestamp": str(records_timestamp),
                        "records_at": parse_records_at(str(records_timestamp)),
                        "underlying_value": underlying_value,
                        "observed_at": utc_now_iso(),
                        "raw_json": payload,
                    }
                    ok, msg = await writer.upsert_snapshot(snapshot_row)
                    sb_snapshot_status = msg

                    if write_supabase_rows:
                        records_at = parse_records_at(str(records_timestamp))
                        observed_at = utc_now_iso()
                        out_rows: List[Dict[str, Any]] = []
                        for row in rows:
                            strike = row.get("strike")
                            if strike is None:
                                continue
                            try:
                                strike_num = float(strike)
                            except Exception:
                                continue
                            out = dict(row)
                            out["symbol"] = "NIFTY"
                            out["expiry"] = expiry
                            out["records_timestamp"] = str(records_timestamp)
                            out["records_at"] = records_at
                            out["observed_at"] = observed_at
                            out["strike"] = strike_num
                            out_rows.append(out)
                        ok2, msg2 = await writer.upsert_rows(out_rows)
                        sb_rows_status = msg2
            return {
                "expiry": expiry,
                "records_timestamp": str(records_timestamp) if records_timestamp else None,
                "underlying_value": underlying_value,
                "inserted": inserted,
                "rows": rows,
                "supabase_snapshot_status": sb_snapshot_status,
                "supabase_rows_status": sb_rows_status,
            }
        finally:
            await poller.close_session()

    res = run_coro(_poll())
    df = pd.DataFrame(res.get("rows") or [])
    if not df.empty and "strike" in df.columns:
        df = df.sort_values(["strike"], ascending=True, na_position="last")
    res["df"] = df
    return res


st.set_page_config(page_title="NIFTY Option Chain Live Polling", layout="wide")
st.title("NIFTY Option Chain Live Polling")

now = now_ist_dt()
in_hours = is_market_hours(now)

with st.sidebar:
    st.subheader("Settings")
    auto_refresh = st.toggle("Auto refresh", value=True)
    poll_secs = st.number_input("Poll interval (seconds)", min_value=5, max_value=300, value=DEFAULT_POLL_SECS, step=5)
    write_csv = st.toggle("Write CSV rows", value=True, help="Append rows into reconstructed_YYYY-MM-DD folders")
    sb_cfg = SupabaseConfig.from_env_or_secrets()
    write_supabase = st.toggle(
        "Write to Supabase",
        value=False,
        help="Uses SUPABASE_URL + SUPABASE_SERVICE_ROLE_KEY (env or Streamlit secrets).",
        disabled=sb_cfg is None,
    )
    write_supabase_rows = st.toggle(
        "Store parsed rows (Supabase)",
        value=True,
        help="Writes 1 row per strike for each new timestamp (can be large).",
        disabled=not write_supabase,
    )
    if sb_cfg is None:
        st.caption("Supabase disabled: set SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY.")
    max_strikes_each_side = st.number_input("Strikes around ATM", min_value=5, max_value=100, value=20, step=5)
    st.caption(f"Market hours (IST): {MARKET_OPEN.strftime('%H:%M')}–{MARKET_CLOSE.strftime('%H:%M')}")

expiries: List[str] = []
expiry_error: Optional[str] = None
try:
    expiries = fetch_expiries()
except Exception as e:
    expiry_error = str(e)

with st.sidebar:
    if expiry_error:
        st.error(f"Failed to load expiries: {expiry_error}")
        selected_expiries: List[str] = []
    else:
        default_expiry = expiries[0] if expiries else None
        selected_expiries = st.multiselect(
            "Expiries to poll",
            options=expiries,
            default=[default_expiry] if default_expiry else [],
        )
    poll_now = st.button("Poll now", type="primary")

status = st.empty()

if auto_refresh and in_hours:
    st.markdown(f"<meta http-equiv='refresh' content='{int(poll_secs)}'>", unsafe_allow_html=True)

if not in_hours and auto_refresh and not poll_now:
    status.info(f"Outside market hours (IST). Now: {now.strftime('%Y-%m-%d %H:%M:%S')}. Auto refresh paused.")
elif not selected_expiries and not poll_now:
    status.warning("No expiry selected.")
else:
    do_poll = poll_now or (auto_refresh and in_hours)
    if do_poll:
        status.info(f"Polling… (IST now: {now.strftime('%Y-%m-%d %H:%M:%S')})")

        results: List[Dict[str, Any]] = []
        errors: List[str] = []
        for expiry in selected_expiries:
            try:
                results.append(
                    poll_one_expiry(
                        expiry=expiry,
                        write_csv=write_csv,
                        sb_cfg=sb_cfg,
                        write_supabase=write_supabase,
                        write_supabase_rows=write_supabase_rows,
                    )
                )
            except PermissionError as e:
                errors.append(f"{expiry}: 403 from NSE ({e})")
            except Exception as e:
                errors.append(f"{expiry}: {type(e).__name__}: {e}")

        if errors:
            status.error(" | ".join(errors))
        else:
            status.success(f"Polled {len(results)} expiry(s).")

        for res in results:
            expiry = res["expiry"]
            records_ts = res["records_timestamp"]
            underlying_value = res["underlying_value"]
            inserted = res["inserted"]
            sb_snapshot_status = res.get("supabase_snapshot_status")
            sb_rows_status = res.get("supabase_rows_status")
            df: pd.DataFrame = res["df"]

            header = f"{expiry} | ts={records_ts or '—'} | NIFTY={underlying_value or '—'}"
            if write_csv:
                header += f" | saved={'yes' if inserted else 'no'}"
            if write_supabase:
                if sb_snapshot_status:
                    header += f" | sb_snap={sb_snapshot_status}"
                if write_supabase_rows and sb_rows_status:
                    header += f" | sb_rows={sb_rows_status}"

            with st.expander(header, expanded=True):
                if df.empty:
                    st.warning("No rows parsed from payload.")
                    continue

                atm = pick_atm_strike(df, underlying_value)
                if atm is not None:
                    df2 = df.copy()
                    df2["strike"] = pd.to_numeric(df2["strike"], errors="coerce")
                    df2 = df2.dropna(subset=["strike"])
                    df2 = df2.sort_values("strike", ascending=True)
                    strikes = df2["strike"].tolist()
                    try:
                        atm_idx = min(range(len(strikes)), key=lambda i: abs(strikes[i] - atm))
                    except ValueError:
                        atm_idx = None
                    if atm_idx is not None:
                        lo = max(0, atm_idx - int(max_strikes_each_side))
                        hi = min(len(df2), atm_idx + int(max_strikes_each_side) + 1)
                        df2 = df2.iloc[lo:hi]
                else:
                    df2 = df.head(int(max_strikes_each_side) * 2 + 1)

                show_cols = [
                    "strike",
                    "ce_ltp",
                    "pe_ltp",
                    "ce_oi",
                    "pe_oi",
                    "ce_chg_oi",
                    "pe_chg_oi",
                    "ce_iv",
                    "pe_iv",
                    "ce_bid",
                    "ce_ask",
                    "pe_bid",
                    "pe_ask",
                    "records_timestamp",
                    "observed_at",
                ]
                cols = [c for c in show_cols if c in df2.columns]
                st.dataframe(df2[cols], use_container_width=True, hide_index=True)
