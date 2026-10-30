import unittest
from datetime import datetime

from collector.nse_poller import IST
from collector.run_headless import deadline_for


class HeadlessTests(unittest.TestCase):
    def test_scheduled_deadline_uses_same_ist_trading_day(self):
        now = datetime(2026, 10, 30, 8, 50, tzinfo=IST)
        self.assertEqual(deadline_for(now, end_at="14:35"), datetime(2026, 10, 30, 14, 35, tzinfo=IST))

    def test_manual_duration_is_bounded(self):
        now = datetime(2026, 10, 30, 9, 30, tzinfo=IST)
        self.assertEqual(deadline_for(now, minutes=5), datetime(2026, 10, 30, 9, 35, tzinfo=IST))
        with self.assertRaises(ValueError):
            deadline_for(now, minutes=400)
        early = datetime(2026, 10, 30, 8, 0, tzinfo=IST)
        self.assertEqual(deadline_for(early), datetime(2026, 10, 30, 13, 50, tzinfo=IST))


if __name__ == "__main__":
    unittest.main()
