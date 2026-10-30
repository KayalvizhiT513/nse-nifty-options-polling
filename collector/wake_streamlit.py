"""Open a real Streamlit session, waking hibernation if needed.

This is an availability experiment, not a guarantee against host hibernation.
"""
import os
import re
import time
from urllib.parse import urlparse

def app_frames(page, hostname):
    # Community Cloud wraps the actual Streamlit app in a same-host /~/+/ iframe.
    # Ignore third-party status widgets and inspect both the wrapper and app.
    return [frame for frame in page.frames if urlparse(frame.url).hostname == hostname]


def collector_state(text):
    if "Automatic collection is disabled" in text:
        return "disabled"
    if "Collection needs a GitHub token" in text or "Unable to initialize the collector" in text:
        return "configuration_error"
    if "GitHub checkpoint failed" in text:
        return "checkpoint_error"
    if "Stopped after error" in text or "Another collector process" in text:
        return "worker_error"
    if "Heartbeat (IST):" in text and "Heartbeat (IST): Starting" not in text:
        return "ready"
    return "starting"


def main():
    from playwright.sync_api import sync_playwright

    url = os.environ.get("STREAMLIT_APP_URL", "")
    if not re.fullmatch(r"https://[a-z0-9-]+\.streamlit\.app/?", url):
        raise SystemExit("STREAMLIT_APP_URL must be an HTTPS Streamlit Community Cloud app URL")
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(url, wait_until="domcontentloaded", timeout=90000)
        deadline = time.monotonic() + 300
        while time.monotonic() < deadline:
            for frame in app_frames(page, urlparse(url).hostname):
                try:
                    text = frame.locator("body").inner_text(timeout=3000)
                except Exception:
                    continue
                state = collector_state(text)
                if state == "disabled":
                    raise SystemExit("App opened successfully, but collection is disabled. Set [collector] enabled=true in Streamlit Secrets.")
                if state == "configuration_error":
                    raise SystemExit("App opened, but collector configuration is incomplete. Check Streamlit Secrets and the Live collector panel.")
                if state in {"checkpoint_error", "worker_error"}:
                    raise SystemExit(f"App opened; collector reported {state}. Inspect the Live collector panel.")
                if state == "ready":
                    page.wait_for_timeout(15000)
                    # Recheck after holding the browser session open.
                    text = frame.locator("body").inner_text(timeout=10000)
                    if collector_state(text) != "ready":
                        raise SystemExit("Collector started but became unhealthy during the wake-up check. Inspect app status.")
                    print("Streamlit app iframe opened; collector heartbeat is present")
                    if "403 from NSE" in text:
                        print("::warning::The app is awake, but the collector reports HTTP 403 from NSE. Check saved snapshots; waking the app does not resolve NSE access.")
                    browser.close()
                    return
                if frame.locator('[data-testid="stException"]').count():
                    raise SystemExit("Streamlit reported an application startup exception. Open the app's logs for the traceback.")
                wake = frame.get_by_role("button", name=re.compile(r"(yes.*app|get.*app.*back|wake.*app)", re.I))
                if wake.count() and wake.first.is_visible():
                    wake.first.click()
            page.wait_for_timeout(5000)
        print("Observed app frames:", [frame.url for frame in app_frames(page, urlparse(url).hostname)])
        browser.close()
        raise SystemExit("Streamlit app did not start a collector session within the wake-up timeout")


if __name__ == "__main__":
    main()
