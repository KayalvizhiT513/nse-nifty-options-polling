# NIFTY options polling

Streamlit-hosted Python collector targeting one request per second per expiry,
with one shared background worker, live polling metrics and automatic GitHub
checkpoints. A browser wake-up workflow visits the app hourly on weekdays.

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
