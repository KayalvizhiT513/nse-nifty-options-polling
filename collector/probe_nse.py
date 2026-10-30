"""A single fresh-session NSE access test; no data writes or GitHub credentials."""
import asyncio
import json
import tempfile
from datetime import datetime

from collector.nse_poller import IST, NSEPoller, OPTION_CHAIN_URL


async def main():
    with tempfile.TemporaryDirectory(prefix="nifty-probe-") as temp:
        poller = NSEPoller(temp)
        try:
            print("Probe time (IST):", datetime.now(IST).isoformat(timespec="seconds"), flush=True)
            await poller.create_session()
            expiries = await poller.fetch_expiry_list()
            print("Expiry endpoint succeeded; expiry count:", len(expiries), flush=True)
            if not expiries:
                raise RuntimeError("No expiries returned")
            expiry = expiries[0]
            data = await poller.fetch_json(OPTION_CHAIN_URL.format(expiry=expiry), "single_request_probe", expiry)
            records = data.get("records") or {}
            result = {"expiry": expiry, "timestamp": records.get("timestamp"), "rows": len(records.get("data") or [])}
            print(json.dumps(result), flush=True)
            if not result["timestamp"] or not result["rows"]:
                raise RuntimeError("No option-chain data returned")
        finally:
            await poller.close_session()


if __name__ == "__main__":
    asyncio.run(main())
