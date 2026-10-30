import unittest
from types import SimpleNamespace

from collector.wake_streamlit import app_frames, collector_state


class WakeTests(unittest.TestCase):
    def test_inspects_app_iframe_and_ignores_status_widget(self):
        host = "nse-nifty-options-polling.streamlit.app"
        wrapper = SimpleNamespace(url=f"https://{host}/")
        app = SimpleNamespace(url=f"https://{host}/~/+/")
        widget = SimpleNamespace(url="https://example.statuspage.io/embed/frame")
        page = SimpleNamespace(frames=[wrapper, widget, app])
        self.assertEqual(app_frames(page, host), [wrapper, app])

    def test_reports_configuration_and_checkpoint_errors_before_success(self):
        self.assertEqual(collector_state("Collector heartbeat Automatic collection is disabled."), "disabled")
        self.assertEqual(collector_state("Heartbeat (IST): Starting"), "starting")
        self.assertEqual(collector_state("Heartbeat (IST): 2026-10-30T09:15:00+05:30"), "ready")
        self.assertEqual(collector_state("Heartbeat (IST): 2026-10-30 GitHub checkpoint failed"), "checkpoint_error")
        self.assertEqual(collector_state("Collection needs a GitHub token in app Secrets"), "configuration_error")


if __name__ == "__main__":
    unittest.main()
