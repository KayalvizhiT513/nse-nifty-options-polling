import asyncio
import unittest
from unittest.mock import AsyncMock, patch

from collector.nse_poller import NSEPoller


class Response:
    status = 200
    def __init__(self, entered=None, release=None):
        self.entered, self.release = entered, release
    async def __aenter__(self):
        if self.entered:
            self.entered.set()
        return self
    async def __aexit__(self, *args):
        pass
    def raise_for_status(self):
        pass
    async def json(self, **kwargs):
        if self.release:
            await self.release.wait()
        return {"ok": True}


class Session:
    closed = False
    def __init__(self, entered=None, release=None):
        self.entered, self.release = entered, release
        self.requests = 0
    def get(self, url):
        if self.closed:
            raise RuntimeError("request used a closed session")
        self.requests += 1
        return Response(self.entered, self.release)
    async def close(self):
        self.closed = True


class SessionRecoveryTests(unittest.IsolatedAsyncioTestCase):
    async def test_new_request_waits_for_session_replacement(self):
        poller = NSEPoller()
        old = Session()
        poller.session = old
        replacing, allow_replace = asyncio.Event(), asyncio.Event()
        fresh = Session()
        async def create():
            self.assertIsNone(poller.session)
            replacing.set()
            await allow_replace.wait()
            poller.session = fresh
        with patch.object(poller, "create_session", create), patch("collector.nse_poller.asyncio.sleep", new=AsyncMock()):
            refresh = asyncio.create_task(poller._reinitialize_session())
            await replacing.wait()
            request = asyncio.create_task(poller.fetch_json("test", "test"))
            await asyncio.sleep(0)
            self.assertFalse(request.done())
            allow_replace.set()
            await refresh
            self.assertEqual(await request, {"ok": True})
        self.assertTrue(old.closed)
        self.assertEqual(fresh.requests, 1)
        self.assertEqual(poller.active_requests, 0)

    async def test_refresh_drains_inflight_requests_before_close(self):
        entered, release = asyncio.Event(), asyncio.Event()
        old = Session(entered, release)
        poller = NSEPoller()
        poller.session = old
        request = asyncio.create_task(poller.fetch_json("test", "test"))
        await entered.wait()
        async def create():
            poller.session = Session()
        refresh = asyncio.create_task(poller._reinitialize_session())
        await asyncio.sleep(0)
        self.assertFalse(old.closed)
        self.assertTrue(poller.session_refreshing)
        release.set()
        await request
        with patch.object(poller, "create_session", create), patch("collector.nse_poller.asyncio.sleep", new=AsyncMock()):
            await refresh
        self.assertTrue(old.closed)
        self.assertFalse(poller.session_refreshing)

    async def test_cancelled_request_releases_active_count(self):
        entered, release = asyncio.Event(), asyncio.Event()
        poller = NSEPoller()
        poller.session = Session(entered, release)
        request = asyncio.create_task(poller.fetch_json("test", "test"))
        await entered.wait()
        request.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await request
        self.assertEqual(poller.active_requests, 0)


if __name__ == "__main__":
    unittest.main()
