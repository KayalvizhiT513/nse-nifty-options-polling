import asyncio
import csv
import json
import os
import random
import signal
import sys
import time
import traceback
from contextlib import suppress
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import aiohttp


BASE = "https://www.nseindia.com"
CONTRACT_INFO_URL = f"{BASE}/api/option-chain-contract-info?symbol=NIFTY"
OPTION_CHAIN_URL = f"{BASE}/api/option-chain-v3?type=Indices&symbol=NIFTY&expiry={{expiry}}"

# ---- Tuning ----
SYMBOL = "NIFTY"
IST = timezone(timedelta(hours=5, minutes=30))
REQUEST_TIMEOUT_SECS = 10
EXPIRY_REFRESH_SECS = 15 * 60   # refresh expiry list every 15 mins
OPTION_POLL_SECS = 1.0
MAX_CONCURRENT_HTTP = 8         # bounded parallelism
REINIT_SESSION_ON_403 = True
SCHEMA_CHECK_SECS = 60 * 60     # check for new NSE keys at most once per hour (per expiry)


class NSEPoller:
    TS_FORMAT = "%d-%b-%Y %H:%M:%S"
    EXPECTED_TOP_KEYS = {"records", "filtered"}
    EXPECTED_RECORDS_KEYS = {"data", "timestamp", "underlyingValue", "expiryDates", "strikePrices"}
    EXPECTED_FILTERED_KEYS = {"data", "CE", "PE"}
    EXPECTED_LEG_KEYS = {
        # pricing / OI
        "openInterest",
        "changeinOpenInterest",
        "pchangeinOpenInterest",
        "totalTradedVolume",
        "impliedVolatility",
        "lastPrice",
        "change",
        "pchange",
        # best-1 book
        "buyPrice1",
        "buyQuantity1",
        "sellPrice1",
        "sellQuantity1",
        # totals
        "totalBuyQuantity",
        "totalSellQuantity",
        # identifiers / metadata commonly present in NSE payloads
        "strikePrice",
        "expiryDate",
        "identifier",
        "optionType",
        "underlying",
        "underlyingValue",
    }
    CSV_COLUMNS = [
        "records_timestamp",
        "observed_at",
        "underlying_value",
        "symbol",
        "expiry",
        "strike",
        "ce_oi",
        "ce_chg_oi",
        "ce_vol",
        "ce_iv",
        "ce_ltp",
        "ce_chg",
        "ce_pchg",
        "ce_bid",
        "ce_bid_qty",
        "ce_ask",
        "ce_ask_qty",
        "pe_oi",
        "pe_chg_oi",
        "pe_vol",
        "pe_iv",
        "pe_ltp",
        "pe_chg",
        "pe_pchg",
        "pe_bid",
        "pe_bid_qty",
        "pe_ask",
        "pe_ask_qty",
        "ce_pchg_oi",
        "ce_total_buy_qty",
        "ce_total_sell_qty",
        "pe_pchg_oi",
        "pe_total_buy_qty",
        "pe_total_sell_qty",
    ]

    def __init__(self, output_root: str = ".") -> None:
        self.output_root = output_root
        self.session: Optional[aiohttp.ClientSession] = None
        self.http_semaphore = asyncio.Semaphore(MAX_CONCURRENT_HTTP)
        self.reinitialize_lock = asyncio.Lock()
        self.last_reinitialize_at = 0.0
        self.stop_event = asyncio.Event()

        self.expiry_tasks: Dict[str, asyncio.Task] = {}
        self.known_expiries: set[str] = set()

        # in-memory dedupe caches, keyed by (expiry, track_date)
        self.last_option_timestamp_by_key: Dict[Tuple[str, str], str] = {}
        self.max_option_timestamp_dt_by_key: Dict[Tuple[str, str], datetime] = {}
        self._loaded_last_timestamp_for_key: set[Tuple[str, str]] = set()
        self.errors_log_path = os.path.join(self.output_root, "poll_errors.jsonl")
        self._reported_schema_keys: set[Tuple[str, str]] = set()
        self._next_schema_check_at_by_expiry: Dict[str, float] = {}

    @staticmethod
    def _safe_print(msg: str) -> None:
        try:
            print(msg)
        except ValueError:
            # E.g. stdout closed (common when running under cron/pipes).
            pass
        except BrokenPipeError:
            pass

    # ---------------- CSV writing ----------------

    @classmethod
    def _parse_ts_date(cls, ts: Optional[str]) -> Optional[str]:
        if not ts:
            return None
        try:
            return datetime.strptime(ts, cls.TS_FORMAT).date().isoformat()
        except ValueError:
            return None

    @classmethod
    def _parse_ts_dt(cls, ts: Optional[str]) -> Optional[datetime]:
        if not ts:
            return None
        try:
            parsed = datetime.strptime(ts, cls.TS_FORMAT)
        except ValueError:
            return None
        return parsed.replace(tzinfo=IST)

    def _csv_path(self, expiry: str, track_date: str) -> str:
        out_dir = os.path.join(self.output_root, f"reconstructed_{track_date}")
        return os.path.join(out_dir, f"expiry_{expiry}_{track_date}_option_chain_rows.csv")

    def _read_last_records_timestamp_from_csv(self, path: str) -> Optional[str]:
        try:
            if not os.path.exists(path) or os.path.getsize(path) == 0:
                return None
        except OSError:
            return None

        # Read only the tail to avoid loading large files.
        try:
            with open(path, "rb") as f:
                f.seek(0, os.SEEK_END)
                size = f.tell()
                f.seek(max(0, size - 8192), os.SEEK_SET)
                chunk = f.read()
        except OSError:
            return None

        lines = chunk.splitlines()
        if not lines:
            return None
        # Last non-empty line.
        for raw_line in reversed(lines):
            if raw_line.strip():
                last_line = raw_line
                break
        else:
            return None

        try:
            line = last_line.decode("utf-8", errors="replace")
        except Exception:
            return None

        # If we landed on just the header or a partial line, fall back to searching for a data-like line.
        if line.startswith("records_timestamp,"):
            return None
        # records_timestamp is the first column and never includes commas.
        first_col = line.split(",", 1)[0].strip()
        return first_col or None

    def _ensure_loaded_last_timestamp(self, expiry: str, track_date: str) -> None:
        key = (expiry, track_date)
        if key in self._loaded_last_timestamp_for_key:
            return
        path = self._csv_path(expiry, track_date)
        last_ts = self._read_last_records_timestamp_from_csv(path)
        if last_ts:
            self.last_option_timestamp_by_key[key] = last_ts
            last_dt = self._parse_ts_dt(last_ts)
            if last_dt:
                self.max_option_timestamp_dt_by_key[key] = last_dt
        self._loaded_last_timestamp_for_key.add(key)

    @staticmethod
    def _extract_leg(leg: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        if not leg:
            return {}
        return {
            "oi": leg.get("openInterest"),
            "chg_oi": leg.get("changeinOpenInterest"),
            "pchg_oi": leg.get("pchangeinOpenInterest"),
            "vol": leg.get("totalTradedVolume"),
            "iv": leg.get("impliedVolatility"),
            "ltp": leg.get("lastPrice"),
            "chg": leg.get("change"),
            "pchg": leg.get("pchange"),
            "bid": leg.get("buyPrice1"),
            "bid_qty": leg.get("buyQuantity1"),
            "ask": leg.get("sellPrice1"),
            "ask_qty": leg.get("sellQuantity1"),
            "total_buy_qty": leg.get("totalBuyQuantity"),
            "total_sell_qty": leg.get("totalSellQuantity"),
        }

    def _build_option_chain_rows(
        self,
        expiry: str,
        records_timestamp: str,
        observed_at: str,
        underlying_value: Any,
        payload: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        records = payload.get("records") or {}
        data = records.get("data") or []
        if not isinstance(data, list):
            return []

        rows: List[Dict[str, Any]] = []
        for item in data:
            if not isinstance(item, dict):
                continue
            ce = item.get("CE")
            pe = item.get("PE")
            strike = item.get("strikePrice")
            if strike is None:
                if isinstance(pe, dict) and pe.get("strikePrice") is not None:
                    strike = pe.get("strikePrice")
                elif isinstance(ce, dict) and ce.get("strikePrice") is not None:
                    strike = ce.get("strikePrice")

            row: Dict[str, Any] = {
                "records_timestamp": records_timestamp,
                "observed_at": observed_at,
                "underlying_value": underlying_value,
                "symbol": SYMBOL,
                "expiry": expiry,
                "strike": strike,
            }
            ce_vals = self._extract_leg(ce if isinstance(ce, dict) else None)
            pe_vals = self._extract_leg(pe if isinstance(pe, dict) else None)
            row.update({f"ce_{k}": v for k, v in ce_vals.items()})
            row.update({f"pe_{k}": v for k, v in pe_vals.items()})
            rows.append(row)

        rows.sort(key=lambda r: (r.get("strike") is None, r.get("strike")))
        return rows

    def _report_new_schema_keys(
        self,
        *,
        expiry: str,
        records_timestamp: str,
        payload: Dict[str, Any],
    ) -> None:
        """
        Detect and log any new keys that NSE starts returning, so we can decide
        whether to add columns for them.
        """
        # top-level keys
        for key in payload.keys():
            loc = "top"
            if key not in self.EXPECTED_TOP_KEYS and (loc, str(key)) not in self._reported_schema_keys:
                self._reported_schema_keys.add((loc, str(key)))
                self.log_error(
                    source="schema_change",
                    error_type="NewTopLevelKey",
                    message=f"New top-level key in option-chain payload: {key}",
                    expiry=expiry,
                    extra={"records_timestamp": records_timestamp},
                )
                self._safe_print(f"[schema-change] new top-level key: {key}")

        records = payload.get("records") or {}
        if isinstance(records, dict):
            for key in records.keys():
                loc = "records"
                if key not in self.EXPECTED_RECORDS_KEYS and (loc, str(key)) not in self._reported_schema_keys:
                    self._reported_schema_keys.add((loc, str(key)))
                    self.log_error(
                        source="schema_change",
                        error_type="NewRecordsKey",
                        message=f"New records key in option-chain payload: {key}",
                        expiry=expiry,
                        extra={"records_timestamp": records_timestamp},
                    )
                    self._safe_print(f"[schema-change] new records key: {key}")

            data = records.get("data") or []
        else:
            data = []

        # filtered keys (if present)
        filtered = payload.get("filtered") or {}
        if isinstance(filtered, dict):
            for key in filtered.keys():
                loc = "filtered"
                if key not in self.EXPECTED_FILTERED_KEYS and (loc, str(key)) not in self._reported_schema_keys:
                    self._reported_schema_keys.add((loc, str(key)))
                    self.log_error(
                        source="schema_change",
                        error_type="NewFilteredKey",
                        message=f"New filtered key in option-chain payload: {key}",
                        expiry=expiry,
                        extra={"records_timestamp": records_timestamp},
                    )
                    self._safe_print(f"[schema-change] new filtered key: {key}")

        # leg keys
        if isinstance(data, list):
            for item in data:
                if not isinstance(item, dict):
                    continue
                for leg_name in ("CE", "PE"):
                    leg = item.get(leg_name)
                    if not isinstance(leg, dict):
                        continue
                    for key in leg.keys():
                        loc = f"{leg_name}"
                        if key not in self.EXPECTED_LEG_KEYS and (loc, str(key)) not in self._reported_schema_keys:
                            self._reported_schema_keys.add((loc, str(key)))
                            self.log_error(
                                source="schema_change",
                                error_type="NewLegKey",
                                message=f"New {leg_name} key in option-chain payload: {key}",
                                expiry=expiry,
                                extra={"records_timestamp": records_timestamp},
                            )
                            self._safe_print(f"[schema-change] new {leg_name} key: {key}")

    def save_option_chain_rows_if_new_timestamp(self, expiry: str, payload: Dict[str, Any]) -> bool:
        records = payload.get("records") or {}
        records_timestamp = records.get("timestamp")
        underlying_value = records.get("underlyingValue")
        if not records_timestamp:
            return False

        records_timestamp_str = str(records_timestamp)
        track_date = self._parse_ts_date(records_timestamp_str)
        if not track_date:
            return False

        self._ensure_loaded_last_timestamp(expiry, track_date)
        key = (expiry, track_date)
        last_ts = self.last_option_timestamp_by_key.get(key)
        if last_ts == records_timestamp_str:
            return False

        current_dt = self._parse_ts_dt(records_timestamp_str)
        if not current_dt:
            return False
        max_dt = self.max_option_timestamp_dt_by_key.get(key)
        if max_dt is not None and current_dt <= max_dt:
            return False

        now_mono = time.monotonic()
        next_check = self._next_schema_check_at_by_expiry.get(expiry, 0.0)
        if now_mono >= next_check:
            self._next_schema_check_at_by_expiry[expiry] = now_mono + SCHEMA_CHECK_SECS
            self._report_new_schema_keys(
                expiry=expiry,
                records_timestamp=str(records_timestamp),
                payload=payload,
            )

        observed_at = self._now_ist()
        rows = self._build_option_chain_rows(
            expiry=expiry,
            records_timestamp=str(records_timestamp),
            observed_at=observed_at,
            underlying_value=underlying_value,
            payload=payload,
        )
        if not rows:
            return False

        path = self._csv_path(expiry, track_date)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        file_has_data = os.path.exists(path) and os.path.getsize(path) > 0
        with open(path, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=self.CSV_COLUMNS, extrasaction="ignore")
            if not file_has_data:
                writer.writeheader()
            writer.writerows(rows)

        self.last_option_timestamp_by_key[key] = records_timestamp_str
        self.max_option_timestamp_dt_by_key[key] = current_dt
        return True

    def log_error(
        self,
        source: str,
        message: str,
        expiry: Optional[str] = None,
        error_type: Optional[str] = None,
        extra: Optional[Dict[str, Any]] = None
    ) -> None:
        record = {
            "observed_at": self._now_ist(),
            "source": source,
            "expiry": expiry,
            "error_type": error_type,
            "message": message,
            "extra": extra,
        }
        try:
            with open(self.errors_log_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(record, ensure_ascii=False) + "\n")
        except OSError:
            pass

    # ---------------- HTTP ----------------

    @staticmethod
    def _now_ist() -> str:
        return datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")

    @staticmethod
    def _headers() -> Dict[str, str]:
        return {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/123.0.0.0 Safari/537.36"
            ),
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://www.nseindia.com/option-chain",
            "Connection": "keep-alive",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
        }

    async def create_session(self) -> None:
        timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT_SECS)
        connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT_HTTP)
        self.session = aiohttp.ClientSession(
            timeout=timeout,
            connector=connector,
            headers=self._headers(),
            cookie_jar=aiohttp.CookieJar()
        )
        await self._warmup_session()

    async def close_session(self) -> None:
        if self.session:
            await self.session.close()
            self.session = None

    async def _warmup_session(self) -> None:
        """
        NSE often expects a browser-like visit first so cookies get set.
        """
        assert self.session is not None
        urls = [
            f"{BASE}/",
            f"{BASE}/option-chain",
        ]
        for url in urls:
            try:
                async with self.session.get(url) as resp:
                    await resp.text()
            except Exception as e:
                self.log_error(
                    source="warmup",
                    error_type=type(e).__name__,
                    message=str(e),
                    extra={"url": url}
                )

    async def _reinitialize_session(self) -> None:
        async with self.reinitialize_lock:
            if time.monotonic() - self.last_reinitialize_at < 10:
                return
            self.last_reinitialize_at = time.monotonic()
            await self.close_session()
            await asyncio.sleep(1)
            await self.create_session()

    async def fetch_json(self, url: str, source: str, expiry: Optional[str] = None) -> Dict[str, Any]:
        assert self.session is not None

        async with self.http_semaphore:
            async with self.session.get(url) as resp:
                if resp.status == 403 and REINIT_SESSION_ON_403:
                    raise PermissionError("403 from NSE")
                resp.raise_for_status()
                return await resp.json(content_type=None)

    # ---------------- Pollers ----------------

    async def fetch_expiry_list(self) -> List[str]:
        payload = await self.fetch_json(CONTRACT_INFO_URL, source="contract_info")
        expiries = payload.get("expiryDates") or []
        if not isinstance(expiries, list):
            raise ValueError("expiryDates missing or not a list")
        return [str(x) for x in expiries if x]

    async def expiry_manager_loop(self) -> None:
        while not self.stop_event.is_set():
            try:
                expiries = await self.fetch_expiry_list()
                await self._sync_expiry_tasks(expiries)
                self._safe_print(f"[expiry-manager] active expiries: {len(expiries)}")
            except PermissionError as e:
                self.log_error("expiry_manager", str(e), error_type=type(e).__name__)
                self._safe_print("[expiry-manager] 403, reinitializing session...")
                await self._reinitialize_session()
            except Exception as e:
                self.log_error(
                    "expiry_manager",
                    str(e),
                    error_type=type(e).__name__,
                    extra={"traceback": traceback.format_exc()},
                )
                self._safe_print(f"[expiry-manager] error: {e}")

            try:
                await asyncio.wait_for(self.stop_event.wait(), timeout=EXPIRY_REFRESH_SECS)
            except asyncio.TimeoutError:
                pass

    async def _sync_expiry_tasks(self, expiries: List[str]) -> None:
        new_set = set(expiries)

        # start new tasks
        for expiry in sorted(new_set - self.known_expiries):
            task = asyncio.create_task(self.option_chain_loop(expiry), name=f"option-{expiry}")
            self.expiry_tasks[expiry] = task
            self.known_expiries.add(expiry)
            self._safe_print(f"[expiry-manager] started task for {expiry}")

        # stop removed tasks
        for expiry in sorted(self.known_expiries - new_set):
            task = self.expiry_tasks.pop(expiry, None)
            if task:
                task.cancel()
                with suppress(asyncio.CancelledError):
                    await task
            self.known_expiries.remove(expiry)
            self._safe_print(f"[expiry-manager] stopped task for {expiry}")

    async def option_chain_loop(self, expiry: str) -> None:
        """
        Poll one expiry every second.
        Save only when records.timestamp changes.
        """
        url = OPTION_CHAIN_URL.format(expiry=expiry)

        while not self.stop_event.is_set():
            started = time.monotonic()
            try:
                payload = await self.fetch_json(url, source="option_chain", expiry=expiry)
                inserted = self.save_option_chain_rows_if_new_timestamp(expiry, payload)
                if inserted:
                    ts = (payload.get("records") or {}).get("timestamp")
                    self._safe_print(f"[option-chain] saved {expiry} @ {ts}")
            except asyncio.CancelledError:
                raise
            except PermissionError as e:
                self.log_error("option_chain", str(e), expiry=expiry, error_type=type(e).__name__)
                self._safe_print(f"[option-chain] 403 for {expiry}, reinitializing session...")
                await self._reinitialize_session()
            except Exception as e:
                self.log_error(
                    "option_chain",
                    str(e),
                    expiry=expiry,
                    error_type=type(e).__name__,
                    extra={"traceback": traceback.format_exc(), "url": url},
                )
                self._safe_print(f"[option-chain] error for {expiry}: {e}")

            # small jitter so all tasks don't hit at the exact same millisecond
            elapsed = time.monotonic() - started
            jitter = random.uniform(0.0, 0.15)
            sleep_for = max(0.0, OPTION_POLL_SECS - elapsed + jitter)

            try:
                await asyncio.wait_for(self.stop_event.wait(), timeout=sleep_for)
            except asyncio.TimeoutError:
                pass

    # ---------------- lifecycle ----------------

    async def run(self) -> None:
        await self.create_session()

        tasks = [
            asyncio.create_task(self.expiry_manager_loop(), name="expiry-manager"),
        ]
        stop_task = asyncio.create_task(self.stop_event.wait())
        try:
            done, _ = await asyncio.wait([*tasks, stop_task], return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                task.result()
        finally:
            stop_task.cancel()
            with suppress(asyncio.CancelledError):
                await stop_task
            for task in tasks:
                task.cancel()
            for task in list(self.expiry_tasks.values()):
                task.cancel()

            for task in tasks:
                with suppress(asyncio.CancelledError):
                    await task
            for task in list(self.expiry_tasks.values()):
                with suppress(asyncio.CancelledError):
                    await task

            await self.close_session()

    def request_stop(self) -> None:
        self.stop_event.set()


async def main() -> None:
    poller = NSEPoller(".")

    loop = asyncio.get_running_loop()

    def _handle_stop() -> None:
        NSEPoller._safe_print("\nStopping...")
        poller.request_stop()

    for sig in (signal.SIGINT, signal.SIGTERM):
        with suppress(NotImplementedError):
            loop.add_signal_handler(sig, _handle_stop)

    await poller.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        NSEPoller._safe_print("\nInterrupted by user, exiting...")
        sys.exit(0)
