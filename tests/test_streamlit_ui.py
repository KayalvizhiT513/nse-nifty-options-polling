import unittest

from streamlit.testing.v1 import AppTest


class StreamlitCompatibilityTests(unittest.TestCase):
    def test_status_from_cached_older_collector_does_not_crash(self):
        app = AppTest.from_string('''
from collector.streamlit_ui import render_status
class LegacyService:
    def start(self): pass
    def status(self):
        return {"state":"Collecting", "heartbeat":"2026-10-30T09:15:00+05:30",
            "last_push_at":None,"last_commit":None,"last_push_error":None,
            "last_error":None,"expiries":[]}
render_status(LegacyService())
''').run(timeout=15)
        self.assertFalse(app.exception, [e.message for e in app.exception])
        self.assertTrue(any("Reboot the app" in item.value for item in app.info))


if __name__ == "__main__":
    unittest.main()
