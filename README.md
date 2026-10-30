# NIFTY options polling

Streamlit-hosted Python collector targeting one request per second per expiry,
with one shared background worker, live polling metrics and automatic GitHub
checkpoints. A browser wake-up workflow visits the app hourly on weekdays.

## GitHub-hosted collection

Streamlit's option-chain requests returned HTTP 403 even in a fresh session
with no parallel tasks. A GitHub-hosted runner successfully fetched today's
option chain in the isolated [NSE access test](../../actions/workflows/test-nse-access.yml).
This does not yet prove sustained one-second polling on all expiries.

The **Collect Nifty options** workflow runs the same background collector directly
on GitHub runners, without depending on Streamlit. To configure:

1. In this polling repository's **Settings → Secrets and variables → Actions →
   Secrets**, add `NIFTY_ARCHIVE_TOKEN`: a fine-grained token scoped to the private
   `nse-nifty-option-nearest-expiry` repository with **Contents: Read and write**.
   A token in Streamlit Secrets is not accessible to GitHub Actions.
2. Run **Collect Nifty options** manually for the default five-minute test.
   Verify today's CSVs in the archive's `nifty-live-data` branch and the run's
   successful-request/saved-snapshot/poll-interval metrics.
3. After that passes, add the Actions variable
   `NIFTY_GITHUB_COLLECTOR_ENABLED=true` in this polling repository.
4. Two weekday jobs cover 08:50–14:35 and 14:20–16:15 IST, overlapping the handoff
   and remaining below the six-hour per-job limit. NSE polling runs 09:00–15:45.
   Runner checkpoints are every five minutes; completed days go to `main` after
   16:00. A manual duration of `0` runs until 16:15, capped at 350 minutes, so it
   can be used to start collection partway through the day.
5. Disable Streamlit collection with `[collector] enabled=false` and reboot the
   app after validating the runner. Set `NIFTY_STREAMLIT_ENABLED=false` to stop
   unnecessary browser wake-up jobs if Streamlit is no longer collecting.

Scheduled runner starts are best effort and runner IPs may differ between jobs.
Check each run's access test and saved snapshots. Market data and tokens are not
uploaded as public workflow artifacts; CSVs go only to the private archive.

## Repositories

| Purpose | Repository |
|---|---|
| Public app source and wake-up workflow | `KayalvizhiT513/nse-nifty-options-polling` |
| Private CSV checkpoints and final daily archive | `KayalvizhiT513/nse-nifty-option-nearest-expiry` |

Deploy `streamlit_app_live_polling.py` from this repository's `main` branch in
Streamlit Community Cloud. Then follow [the setup guide](collector/STREAMLIT.md)
and [Secrets template](.streamlit/secrets.toml.example).

The app collects weekdays 09:00–15:45 IST, checkpoints to the private data repo's
`nifty-live-data` branch every 15 minutes, and archives completed days to its
`main` after 16:00 IST. Supabase is not required.

Configuration is disabled until Streamlit Secrets are supplied. Scheduled
wake-ups are disabled until the polling repository's `NIFTY_STREAMLIT_ENABLED`
variable is `true`; set `NIFTY_STREAMLIT_URL` to the deployed polling app URL.

Hourly visits and one-second timers do not guarantee uninterrupted collection.
Validate NSE access and compare captured timestamps against local collection over
a trading day before relying on the cloud collector.

## Test

```sh
python -m pip install -r requirements.txt
python -m unittest discover -s tests -p 'test_*.py' -v
```
