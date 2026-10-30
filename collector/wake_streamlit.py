"""Open a real Streamlit session, waking hibernation if needed.

This is an availability experiment, not a guarantee against host hibernation.
"""
import os
import re

from playwright.sync_api import sync_playwright


def main():
    url = os.environ.get("STREAMLIT_APP_URL", "")
    if not re.fullmatch(r"https://[a-z0-9-]+\.streamlit\.app/?", url):
        raise SystemExit("STREAMLIT_APP_URL must be an HTTPS Streamlit Community Cloud app URL")
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(url, wait_until="domcontentloaded", timeout=90000)
        for _ in range(36):
            marker = page.get_by_text("Collector heartbeat", exact=True)
            if marker.count() and marker.first.is_visible():
                page.wait_for_timeout(15000)
                # Require an enabled, running worker, not merely a rendered page.
                text = page.locator("body").inner_text()
                if "Heartbeat (IST):" not in text:
                    raise SystemExit("App opened, but the collector is not enabled/configured")
                if "Heartbeat (IST): Starting" in text:
                    page.wait_for_timeout(5000)
                    continue
                if "GitHub checkpoint failed" in text:
                    raise SystemExit("Collector is awake but its GitHub checkpoint failed; inspect app status")
                print("Streamlit session opened; collector heartbeat is present")
                browser.close()
                return
            wake = page.get_by_role("button", name=re.compile(r"(yes.*app|get.*app.*back|wake.*app)", re.I))
            if wake.count() and wake.first.is_visible():
                wake.first.click()
            page.wait_for_timeout(5000)
        browser.close()
        raise SystemExit("Streamlit app did not start a collector session within the wake-up timeout")


if __name__ == "__main__":
    main()
