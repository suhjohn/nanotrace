from __future__ import annotations

import unittest

from nanotrace import (
    AsyncNanotrace,
    Nanotrace,
    NanotraceFlushError,
    create_async_nanotrace,
    create_nanotrace,
    http_transport,
    sidecar_http_transport,
    trace_context,
)


class RecordingTransport:
    def __init__(self) -> None:
        self.events: list[dict] = []

    def send(self, event: dict) -> None:
        self.events.append(event)


class BatchRecordingTransport(RecordingTransport):
    def __init__(self) -> None:
        super().__init__()
        self.batches: list[list[dict]] = []

    def send_batch(self, events: list[dict]) -> None:
        self.batches.append(events)
        self.events.extend(events)


class AsyncRecordingTransport:
    def __init__(self) -> None:
        self.events: list[dict] = []

    async def send(self, event: dict) -> None:
        self.events.append(event)


class AsyncBatchRecordingTransport(AsyncRecordingTransport):
    def __init__(self) -> None:
        super().__init__()
        self.batches: list[list[dict]] = []

    async def send_batch(self, events: list[dict]) -> None:
        self.batches.append(events)
        self.events.extend(events)


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

    def test_camel_case_fields_match_write_path_names(self) -> None:
        transport = RecordingTransport()
        nt = create_nanotrace(transport)

        nt.info(
            "hello",
            isError=True,
            requestId="req_1",
            organizationId="org_1",
            llmModel="gpt-test",
            toolName="shell",
        )
        nt.flush()

        data = transport.events[0]["data"]
        self.assertEqual(data["is_error"], True)
        self.assertEqual(data["request_id"], "req_1")
        self.assertEqual(data["organization_id"], "org_1")
        self.assertEqual(data["llm.model"], "gpt-test")
        self.assertEqual(data["tool_name"], "shell")

    def test_http_server_request_shapes_status_fields(self) -> None:
        transport = RecordingTransport()
        nt = create_nanotrace(transport)

        nt.http_server_request(
            method="GET",
            route="/users/{id}",
            path="/users/1",
            status_code=503,
            duration_ms=12.5,
            tenant_id="tenant_1",
        )
        nt.flush()

        data = transport.events[0]["data"]
        self.assertEqual(data["event_type"], "span")
        self.assertEqual(data["name"], "GET /users/{id}")
        self.assertEqual(data["span_kind"], "server")
        self.assertEqual(data["http.status_code"], 503)
        self.assertEqual(data["http.response.status_code"], 503)
        self.assertEqual(data["is_error"], 1)
        self.assertEqual(data["tenant_id"], "tenant_1")

    def test_http_transports_use_supported_ingest_paths(self) -> None:
        self.assertEqual(http_transport("https://api.example.com", "key").events_url, "https://api.example.com/v1/events")
        self.assertEqual(sidecar_http_transport("http://127.0.0.1:4320").events_url, "http://127.0.0.1:4320/events")

    def test_flush_surfaces_delivery_errors(self) -> None:
        nt = create_nanotrace(FailingTransport())

        nt.info("hello")

        with self.assertRaises(NanotraceFlushError):
            nt.flush()

    def test_flush_sends_batch_when_transport_supports_it(self) -> None:
        transport = BatchRecordingTransport()
        nt = create_nanotrace(transport, service="api")

        nt.info("one")
        nt.info("two")
        nt.flush()

        self.assertEqual(len(transport.batches), 1)
        self.assertEqual(len(transport.batches[0]), 2)
        self.assertEqual([event["data"]["body"] for event in transport.events], ["one", "two"])

    def test_batch_threshold_flushes_without_waiting_for_flush_call(self) -> None:
        transport = BatchRecordingTransport()
        nt = create_nanotrace(transport, batch_max_events=2)

        nt.info("one")
        nt.info("two")
        nt.flush()

        self.assertEqual(len(transport.batches), 1)
        self.assertEqual(len(transport.batches[0]), 2)

    def test_custom_single_event_transport_still_works(self) -> None:
        transport = RecordingTransport()
        nt = create_nanotrace(transport)

        nt.info("one")
        nt.info("two")
        nt.flush()

        self.assertEqual([event["data"]["body"] for event in transport.events], ["one", "two"])


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

    async def test_async_http_client_request_uses_shared_shape(self) -> None:
        transport = AsyncRecordingTransport()
        nt = create_async_nanotrace(transport)

        nt.http_client_request(
            method="POST",
            url="https://api.example.com/v1/events",
            statusCode=201,
            duration_ms=3.5,
            request_id="req_1",
        )
        await nt.flush()

        data = transport.events[0]["data"]
        self.assertEqual(data["event_type"], "span")
        self.assertEqual(data["name"], "POST https://api.example.com/v1/events")
        self.assertEqual(data["span_kind"], "client")
        self.assertEqual(data["http.status_code"], 201)
        self.assertEqual(data["http.response.status_code"], 201)
        self.assertEqual(data["is_error"], 0)
        self.assertEqual(data["request_id"], "req_1")

    async def test_async_flush_surfaces_delivery_errors(self) -> None:
        nt = create_async_nanotrace(AsyncFailingTransport())

        nt.info("hello")

        with self.assertRaises(NanotraceFlushError):
            await nt.flush()

    async def test_async_flush_sends_batch_when_transport_supports_it(self) -> None:
        transport = AsyncBatchRecordingTransport()
        nt = create_async_nanotrace(transport, service="api")

        nt.info("one")
        nt.info("two")
        await nt.flush()

        self.assertEqual(len(transport.batches), 1)
        self.assertEqual(len(transport.batches[0]), 2)
        self.assertEqual([event["data"]["body"] for event in transport.events], ["one", "two"])

    async def test_async_custom_single_event_transport_still_works(self) -> None:
        transport = AsyncRecordingTransport()
        nt = create_async_nanotrace(transport)

        nt.info("one")
        nt.info("two")
        await nt.flush()

        self.assertEqual([event["data"]["body"] for event in transport.events], ["one", "two"])


if __name__ == "__main__":
    unittest.main()
