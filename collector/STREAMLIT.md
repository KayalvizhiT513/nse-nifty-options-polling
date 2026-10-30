# Streamlit collector

This mode runs the existing asynchronous Python collector inside the deployed
Streamlit application. Supabase is not required. It targets a one-second polling
interval per expiry with the original eight-request concurrency limit and jitter.

## Enable

1. Deploy a Streamlit Community Cloud app from
   `KayalvizhiT513/nse-nifty-options-polling`, branch `main`, entry point
   `streamlit_app_live_polling.py`. The archive viewer is a separate app.
2. Create a fine-grained GitHub token limited to the **data repository**
   `KayalvizhiT513/nse-nifty-option-nearest-expiry` with
   **Contents: Read and write**. Allow the token to create/update the
   `nifty-live-data` branch and update `main`; branch rules can prevent pushes.
   The collector does not need Actions, administration or workflow-write access.
3. In **Manage app → Settings → Secrets**, paste the contents of
   `.streamlit/secrets.toml.example`, replacing the token placeholder. Never
   commit the actual token. Save the configuration and open the app once.
4. The **Live collector** panel should show a recent heartbeat. On weekdays it
   collects between 09:00 and 15:45 IST. Outside those hours it waits. No open
   browser is needed for the Python worker to continue while its host is alive.
5. In the **polling repository**, set the Actions variable `NIFTY_STREAMLIT_URL`
   to the deployed polling app's `https://….streamlit.app/` URL. Run
   **Wake Streamlit collector** manually in GitHub Actions. It opens a real
   browser session, handles the hibernation wake button, and checks for the
   collector heartbeat. This tests more than a plain HTTP/health request.
6. After that run passes, set the repository Actions variable
   `NIFTY_STREAMLIT_ENABLED=true` to enable scheduled browser visits at 08:41 IST
   and hourly through 16:41 IST on weekdays. Both variables belong to the polling
   repository, not the private data archive.

The polling repository is public, so its standard GitHub-hosted wake jobs use
public-repository Actions. They only run for short browser visits. The app token is stored
only in Streamlit Secrets; the wake-up workflow does not receive it.

## Data and restart behavior

- One shared worker per process, with a file lock preventing a second process
  from writing the same output directory. Reruns and additional browser sessions
  reuse the existing worker. Worker threads never call Streamlit APIs.
- CSV writes and checkpoint snapshots share a lock to prevent partial snapshots.
- Every 15 minutes, CSVs are merged into `nifty-live-data`, committed by date,
  pushed, fetched again and verified in the private **data repository**. The app
  source and public workflow repository contain no captured market data or tokens.
- At the first checkpoint at or after 16:00 IST, completed days are published to
  the data repository's `main`. Data uploads cannot redeploy the collector because
  the collector's source lives in the separate polling repository.
- On reboot, the worker recovers checkpoint data not yet present on `main` before
  polling. The collector deduplicates against recovered NSE timestamps.
- Failed uploads retain local files and retry at the next checkpoint. A platform
  restart can still lose data collected since the last successful checkpoint.
- Local output defaults to `/tmp/nifty-streamlit/data`; a dedicated bare Git
  repository handles uploads without changing Streamlit's deployed checkout.
- Errors rotate across three log files of at most 2 MB each. The UI reports
  request counts, errors, successful snapshot counts and recent p95 timing.
- Before starting parallel polling, the app tests one option-chain request using
  a fresh warmed session. If that fails, it pauses full polling and retries the
  single-request test once per minute. A 403 during polling also pauses the full
  collector. The Live collector panel shows the isolated test result.
- Setting `enabled=false` in Secrets and rebooting the app stops collection.
  Set `NIFTY_STREAMLIT_ENABLED=false` to disable scheduled wake-ups.

## Validate before retiring local collection

Run cloud and local collectors together for a trading day. Compare the set of
NSE `records_timestamp` values per expiry, response errors and polling intervals.
Confirm that the next morning's scheduled browser visit starts/resumes collection
without your intervention. Check both the intraday data branch and final `main`
commit. A timer of one second does not guarantee fresh NSE data every second.

Community Cloud can hibernate or restart the app. Hourly browser visits are a
testable keep-alive strategy, not a promise of uninterrupted hosting. A failed
wake job is visible in GitHub Actions. NSE may also reject the cloud IP; errors
and request timing must be checked on the actual deployed host.

This implementation does not enable the Supabase schedule. Use one collection
backend at a time to avoid duplicate collection traffic.
