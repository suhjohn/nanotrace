from __future__ import annotations

import unittest

from nanotrace import (
    AsyncNanotrace,
    Nanotrace,
    NanotraceFlushError,
    create_async_nanotrace,
    create_nanotrace,
    trace_context,
)


class RecordingTransport:
    def __init__(self) -> None:
        self.events: list[dict] = []

    def send(self, event: dict) -> None:
        self.events.append(event)


class AsyncRecordingTransport:
    def __init__(self) -> None:
        self.events: list[dict] = []

    async def send(self, event: dict) -> None:
        self.events.append(event)


class FailingTransport:
    def send(self, event: dict) -> None:
        raise RuntimeError("boom")


class AsyncFailingTransport:
    async def send(self, event: dict) -> None:
        raise RuntimeError("boom")


class NanotraceTests(unittest.TestCase):
    def test_sync_log_shapes_event(self) -> None:
        transport = RecordingTransport()
        nt = create_nanotrace(transport, service="api", environment="test")

        nt.info("hello", user_id="u_1")
        nt.flush()

        self.assertEqual(len(transport.events), 1)
        event = transport.events[0]
        self.assertIn("event_id", event)
        self.assertIn("timestamp", event)
        self.assertEqual(event["data"]["event_type"], "log")
        self.assertEqual(event["data"]["service"], "api")
        self.assertEqual(event["data"]["environment"], "test")
        self.assertEqual(event["data"]["user_id"], "u_1")
        self.assertEqual(event["data"]["body"], "hello")

    def test_sync_span_context_manager(self) -> None:
        transport = RecordingTransport()
        nt = create_nanotrace(transport, service="api")

        with nt.span("root") as span:
            nt.info("inside")
            span.set("custom", "value")
        nt.flush()

        self.assertEqual(len(transport.events), 2)
        child, end = transport.events
        self.assertEqual(child["data"]["trace_id"], span.trace_id)
        self.assertEqual(child["data"]["span_id"], span.span_id)
        self.assertEqual(end["data"]["event_type"], "span")
        self.assertEqual(end["data"]["name"], "root")
        self.assertEqual(end["data"]["custom"], "value")
        self.assertEqual(end["data"]["span_status_code"], "ok")

    def test_context_merges_into_events(self) -> None:
        transport = RecordingTransport()
        nt = Nanotrace(transport=transport, base_context={})

        with trace_context(trace_id="trace-1", span_id="span-1"):
            nt.event("clicked")
        nt.flush()

        self.assertEqual(transport.events[0]["data"]["trace_id"], "trace-1")
        self.assertEqual(transport.events[0]["data"]["span_id"], "span-1")

    def test_flush_surfaces_delivery_errors(self) -> None:
        nt = create_nanotrace(FailingTransport())

        nt.info("hello")

        with self.assertRaises(NanotraceFlushError):
            nt.flush()


class AsyncNanotraceTests(unittest.IsolatedAsyncioTestCase):
    async def test_async_log_shapes_event(self) -> None:
        transport = AsyncRecordingTransport()
        nt = create_async_nanotrace(transport, service="api", environment="test")

        nt.info("hello", user_id="u_1")
        await nt.flush()

        self.assertEqual(len(transport.events), 1)
        event = transport.events[0]
        self.assertEqual(event["data"]["event_type"], "log")
        self.assertEqual(event["data"]["service"], "api")
        self.assertEqual(event["data"]["environment"], "test")
        self.assertEqual(event["data"]["user_id"], "u_1")

    async def test_async_span_context_manager(self) -> None:
        transport = AsyncRecordingTransport()
        nt = AsyncNanotrace(transport=transport, base_context={"service": "api"})

        async with nt.span("root") as span:
            nt.info("inside")
            span.set("custom", "value")
        await nt.flush()

        self.assertEqual(len(transport.events), 2)
        child, end = transport.events
        self.assertEqual(child["data"]["trace_id"], span.trace_id)
        self.assertEqual(child["data"]["span_id"], span.span_id)
        self.assertEqual(end["data"]["event_type"], "span")
        self.assertEqual(end["data"]["custom"], "value")
        self.assertEqual(end["data"]["span_status_code"], "ok")

    async def test_async_flush_surfaces_delivery_errors(self) -> None:
        nt = create_async_nanotrace(AsyncFailingTransport())

        nt.info("hello")

        with self.assertRaises(NanotraceFlushError):
            await nt.flush()


if __name__ == "__main__":
    unittest.main()
