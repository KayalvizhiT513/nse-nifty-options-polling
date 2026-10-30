import tempfile
import unittest
from unittest.mock import AsyncMock, Mock, patch

from collector.streamlit_runtime import CollectorService


class CloudAccessTests(unittest.IsolatedAsyncioTestCase):
    async def test_single_request_denial_is_reported_without_parallel_tasks(self):
        with tempfile.TemporaryDirectory() as temp, patch("collector.streamlit_runtime.NSEPoller", autospec=True) as factory:
            probe = factory.return_value
            probe.fetch_expiry_list.return_value = ["03-Nov-2026", "09-Nov-2026"]
            probe.fetch_json.side_effect = PermissionError("403 from NSE")
            service = CollectorService(temp, Mock())
            self.assertFalse(await service._probe_access())
            probe.fetch_json.assert_awaited_once()
            probe.close_session.assert_awaited_once()
            probe._sync_expiry_tasks.assert_not_called()
            self.assertEqual(service.last_access_probe["expiry_count"], 2)
            self.assertEqual(service.last_access_probe["parallel_tasks"], 0)
            self.assertIn("403", service.last_access_probe["error"])

    async def test_failed_access_probe_stops_full_polling_and_backs_off(self):
        with tempfile.TemporaryDirectory() as temp, patch("collector.streamlit_runtime.market_open", return_value=True), patch("collector.streamlit_runtime.MeasuredPoller") as full:
            service = CollectorService(temp, Mock())
            service._probe_access = AsyncMock(return_value=False)
            waits = 0
            async def sleep(seconds):
                nonlocal waits
                waits += 1
                if waits == 3:
                    service.stop_event.set()
            with patch("collector.streamlit_runtime.asyncio.sleep", sleep):
                await service._schedule()
            service._probe_access.assert_awaited_once()
            full.assert_not_called()
            self.assertIn("Paused", service.state)


if __name__ == "__main__":
    unittest.main()
