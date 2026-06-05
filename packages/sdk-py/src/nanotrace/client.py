from __future__ import annotations

import secrets
import traceback
import asyncio
import json
from concurrent.futures import Future, ThreadPoolExecutor, wait
from dataclasses import dataclass, field
from datetime import datetime, timezone
from threading import Lock, Timer
from types import TracebackType
from typing import Any
from uuid import uuid4

from .context import current_context, trace_context
from .normalize import normalize_common, normalize_json, without_keys
from .types import AsyncTransport, CommonFields, Json, JsonObject, Transport


def _now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _random_hex(bytes_: int) -> str:
    return secrets.token_hex(bytes_)


def _timestamp_ms(value: datetime | str) -> float:
    if isinstance(value, datetime):
        return value.timestamp() * 1000
    return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp() * 1000


def _iso(value: datetime | str) -> str:
    if isinstance(value, datetime):
        return value.isoformat().replace("+00:00", "Z")
    return value


SEVERITY_NUMBERS = {
    "debug": 5,
    "info": 9,
    "warn": 13,
    "warning": 13,
    "error": 17,
}


def _severity_number(level: str) -> int:
    return SEVERITY_NUMBERS.get(level, 9)


def _pop_first(data: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        value = data.pop(key, None)
        if value:
            return value
    return None


def _error_fields(error: BaseException) -> JsonObject:
    return {
        "exception.type": error.__class__.__name__,
        "exception.message": str(error),
        "exception.stacktrace": "".join(
            traceback.format_exception(type(error), error, error.__traceback__)
        ),
    }


class NanotraceFlushError(Exception):
    def __init__(self, errors: list[BaseException]) -> None:
        self.errors = errors
        super().__init__(f"Nanotrace flush failed with {len(errors)} error(s).")


DEFAULT_BATCH_MAX_EVENTS = 100
DEFAULT_BATCH_MAX_BYTES = 512 * 1024
DEFAULT_BATCH_FLUSH_INTERVAL_MS = 1000


HTTP_SERVER_KEYS = {
    "method",
    "route",
    "path",
    "url",
    "status_code",
    "statusCode",
    "duration_ms",
    "durationMs",
}
HTTP_CLIENT_KEYS = {"method", "url", "status_code", "statusCode", "duration_ms", "durationMs"}
DB_QUERY_KEYS = {"system", "operation", "statement", "duration_ms", "durationMs"}
RPC_KEYS = {"system", "service", "method", "duration_ms", "durationMs"}
MESSAGE_KEYS = {"system", "destination", "duration_ms", "durationMs"}
PAGE_KEYS = {"name", "url", "path", "title", "referrer"}


def _status_fields(status_code: Any) -> JsonObject:
    if not status_code:
        return {}
    return {
        "http.status_code": status_code,
        "http.response.status_code": status_code,
    }


def _is_server_error(status_code: Any) -> int:
    return 1 if isinstance(status_code, int) and status_code >= 500 else 0


class _NanotraceClient:
    base_context: dict[str, Any]

    def emit(
        self,
        data: CommonFields,
        *,
        event_id: str | None = None,
        timestamp: datetime | str | None = None,
        observed_timestamp: datetime | str | None = None,
    ) -> None:
        event: JsonObject = {
            "event_id": event_id or str(uuid4()),
            "timestamp": _iso(timestamp) if timestamp else _now(),
            "data": {
                **normalize_common(self.base_context, current_context()),
                **normalize_common(dict(data)),
            },
        }
        if observed_timestamp is not None:
            event["observed_timestamp"] = _iso(observed_timestamp)
        self._enqueue(event)

    def event(self, name: str, **data: Any) -> None:
        self._write("analytics", {"name": name, **normalize_common(data)})

    def log(self, level: str, message: str, **data: Any) -> None:
        normalized_level = "warn" if level == "warning" else level
        self._write(
            "log",
            {
                "severity_text": normalized_level.upper(),
                "severity_number": _severity_number(normalized_level),
                "body": message,
                "is_error": 1 if normalized_level == "error" else 0,
                **normalize_common(data),
            },
        )

    def debug(self, message: str, **data: Any) -> None:
        self.log("debug", message, **data)

    def info(self, message: str, **data: Any) -> None:
        self.log("info", message, **data)

    def warn(self, message: str, **data: Any) -> None:
        self.log("warn", message, **data)

    def error(self, error_or_message: BaseException | str, **data: Any) -> None:
        if isinstance(error_or_message, BaseException):
            self.capture_exception(error_or_message, **data)
        else:
            self.log("error", str(error_or_message), **data)

    def capture_exception(self, error: BaseException, **data: Any) -> None:
        self._write(
            "log",
            {
                **normalize_common(data),
                "severity_text": "ERROR",
                "severity_number": 17,
                "body": str(error),
                "is_error": 1,
                **_error_fields(error),
            },
        )

    def span(self, name: str, **data: Any) -> Any:
        return self.start_span(name, **data)

    def start_span(self, name: str, **data: Any) -> Any:
        context = current_context()
        trace_id = _pop_first(data, "trace_id", "traceId") or context.get("trace_id") or _random_hex(16)
        span_id = _pop_first(data, "span_id", "spanId") or _random_hex(8)
        kind = _pop_first(data, "kind", "span_kind", "spanKind") or "internal"
        parent_span_id = _pop_first(data, "parent_span_id", "parentSpanId") or context.get("span_id")
        return self._make_span(
            name=name,
            trace_id=str(trace_id),
            span_id=str(span_id),
            parent_span_id=str(parent_span_id) if parent_span_id else None,
            attrs={**normalize_common(data), "span_kind": str(kind)},
        )

    def record_span(
        self,
        name: str,
        start_time: datetime | str,
        end_time: datetime | str,
        *,
        duration_ms: float | None = None,
        status_code: str = "ok",
        kind: str = "internal",
        **data: Any,
    ) -> None:
        self._write(
            "span",
            {
                **normalize_common(data),
                "name": name,
                "start_time": _iso(start_time),
                "end_time": _iso(end_time),
                "duration_ms": duration_ms
                if duration_ms is not None
                else _timestamp_ms(end_time) - _timestamp_ms(start_time),
                "span_status_code": status_code,
                "span_kind": kind,
            },
        )

    def http_server_request(self, *, method: str, duration_ms: float, **data: Any) -> None:
        route = data.get("route")
        path = data.get("path")
        url = data.get("url")
        status_code = data.get("status_code", data.get("statusCode"))
        self._write(
            "span",
            {
                "name": f"{method} {route or path or url or ''}".strip(),
                "span_kind": "server",
                "http.method": method,
                "http.request.method": method,
                **({"http.route": route} if route else {}),
                **({"url.path": path} if path else {}),
                **({"url.full": url} if url else {}),
                **_status_fields(status_code),
                "duration_ms": duration_ms,
                "is_error": _is_server_error(status_code),
                **normalize_common(without_keys(data, HTTP_SERVER_KEYS)),
            },
        )

    def http_client_request(self, *, method: str, url: str, duration_ms: float, **data: Any) -> None:
        status_code = data.get("status_code", data.get("statusCode"))
        self._write(
            "span",
            {
                "name": f"{method} {url}",
                "span_kind": "client",
                "http.method": method,
                "http.request.method": method,
                "url.full": url,
                **_status_fields(status_code),
                "duration_ms": duration_ms,
                "is_error": _is_server_error(status_code),
                **normalize_common(without_keys(data, HTTP_CLIENT_KEYS)),
            },
        )

    def db_query(
        self,
        *,
        system: str,
        duration_ms: float,
        operation: str | None = None,
        statement: str | None = None,
        **data: Any,
    ) -> None:
        self._write(
            "span",
            {
                **normalize_common(without_keys(data, DB_QUERY_KEYS)),
                "name": operation or system,
                "span_kind": "client",
                "db.system": system,
                **({"db.operation": operation} if operation else {}),
                **({"db.statement": statement} if statement else {}),
                "duration_ms": duration_ms,
            },
        )

    def rpc_call(
        self,
        *,
        system: str,
        service: str,
        method: str,
        duration_ms: float,
        **data: Any,
    ) -> None:
        self._write(
            "span",
            {
                **normalize_common(without_keys(data, RPC_KEYS)),
                "name": f"{service}/{method}",
                "span_kind": "client",
                "rpc.system": system,
                "rpc.service": service,
                "rpc.method": method,
                "duration_ms": duration_ms,
            },
        )

    def message_publish(
        self,
        *,
        system: str,
        destination: str,
        duration_ms: float | None = None,
        **data: Any,
    ) -> None:
        self._message_operation("publish", system=system, destination=destination, duration_ms=duration_ms, **data)

    def message_consume(
        self,
        *,
        system: str,
        destination: str,
        duration_ms: float | None = None,
        **data: Any,
    ) -> None:
        self._message_operation("consume", system=system, destination=destination, duration_ms=duration_ms, **data)

    def counter(self, name: str, value: float = 1, **data: Any) -> None:
        self._metric(name, "counter", value, {"metric.temporality": "delta", "metric.is_monotonic": True}, data)

    def gauge(self, name: str, value: float, **data: Any) -> None:
        self._metric(name, "gauge", value, {}, data)

    def histogram(self, name: str, value: float, **data: Any) -> None:
        self._metric(name, "histogram", value, {}, data)

    def measure(self, name: str, value: float, **data: Any) -> None:
        self.histogram(name, value, **data)

    def timing(self, name: str, duration_ms: float, **data: Any) -> None:
        self.histogram(name, duration_ms, metric_unit="ms", **data)

    def track(self, name: str, **properties: Any) -> None:
        self._write("track", {**normalize_common(properties), "name": name})

    def identify(self, user_id: str, **traits: Any) -> None:
        self._write("identify", {**normalize_common(traits), "user_id": user_id})

    def group(self, account_id: str, **traits: Any) -> None:
        self._write("group", {**normalize_common(traits), "account_id": account_id})

    def alias(self, previous_id: str, user_id: str, **data: Any) -> None:
        self._write("alias", {**normalize_common(data), "previous_id": previous_id, "user_id": user_id})

    def page(self, **data: Any) -> None:
        self._write(
            "page",
            {
                **normalize_common(without_keys(data, PAGE_KEYS)),
                **({"name": data["name"]} if data.get("name") else {}),
                **({"page_url": data["url"]} if data.get("url") else {}),
                **({"page_path": data["path"]} if data.get("path") else {}),
                **({"page_title": data["title"]} if data.get("title") else {}),
                **({"referrer": data["referrer"]} if data.get("referrer") else {}),
            },
        )

    def screen(self, name: str, **data: Any) -> None:
        self._write("screen", {**normalize_common(data), "screen_name": name, "name": name})

    def revenue(self, revenue: float, **data: Any) -> None:
        self.track("Revenue", revenue=revenue, **data)

    def experiment_viewed(self, experiment_id: str, variant: str, **data: Any) -> None:
        self.track("Experiment Viewed", experiment_id=experiment_id, variant=variant, **data)

    def feature_flag_evaluated(self, feature_flag: str, **data: Any) -> None:
        self.track("Feature Flag Evaluated", feature_flag=feature_flag, **data)

    def _message_operation(
        self,
        operation: str,
        *,
        system: str,
        destination: str,
        duration_ms: float | None,
        **data: Any,
    ) -> None:
        self._write(
            "span",
            {
                **normalize_common(without_keys(data, MESSAGE_KEYS)),
                "name": f"{operation} {destination}",
                "span_kind": "producer" if operation == "publish" else "consumer",
                "messaging.system": system,
                "messaging.destination.name": destination,
                "messaging.operation.name": operation,
                **({"duration_ms": duration_ms} if duration_ms is not None else {}),
            },
        )

    def _metric(
        self,
        name: str,
        type_: str,
        value: float,
        defaults: JsonObject,
        data: dict[str, Any],
    ) -> None:
        self._write(
            "metric",
            {
                **defaults,
                **normalize_common(data),
                "metric_name": name,
                "metric_type": type_,
                "metric_value": value,
            },
        )

    def _write(self, event_type: str, data: JsonObject) -> None:
        self.emit({"event_type": event_type, **data})

    def _make_span(
        self,
        *,
        name: str,
        trace_id: str,
        span_id: str,
        parent_span_id: str | None,
        attrs: JsonObject,
    ) -> Any:
        raise NotImplementedError

    def _enqueue(self, event: JsonObject) -> None:
        raise NotImplementedError


@dataclass
class Nanotrace(_NanotraceClient):
    transport: Transport
    base_context: dict[str, Any] = field(default_factory=dict)
    batch_max_events: int = DEFAULT_BATCH_MAX_EVENTS
    batch_max_bytes: int = DEFAULT_BATCH_MAX_BYTES
    batch_flush_interval_ms: int = DEFAULT_BATCH_FLUSH_INTERVAL_MS
    _executor: ThreadPoolExecutor = field(
        default_factory=lambda: ThreadPoolExecutor(max_workers=1, thread_name_prefix="nanotrace"),
        init=False,
        repr=False,
    )
    _queue: list[JsonObject] = field(default_factory=list, init=False, repr=False)
    _queue_bytes: int = field(default=0, init=False, repr=False)
    _timer: Timer | None = field(default=None, init=False, repr=False)
    _pending: set[Future[None]] = field(default_factory=set, init=False, repr=False)
    _errors: list[BaseException] = field(default_factory=list, init=False, repr=False)
    _lock: Lock = field(default_factory=Lock, init=False, repr=False)

    def flush(self) -> None:
        with self._lock:
            batch = self._take_batch_locked()
        self._submit_batch(batch)

        while True:
            with self._lock:
                pending = tuple(self._pending)
            if not pending:
                break
            wait(pending)

        with self._lock:
            errors = list(self._errors)
            self._errors.clear()
        if errors:
            raise NanotraceFlushError(errors)

    def _make_span(
        self,
        *,
        name: str,
        trace_id: str,
        span_id: str,
        parent_span_id: str | None,
        attrs: JsonObject,
    ) -> "Span":
        return Span(
            client=self,
            name=name,
            trace_id=trace_id,
            span_id=span_id,
            parent_span_id=parent_span_id,
            attrs=attrs,
        )

    def _enqueue(self, event: JsonObject) -> None:
        batch: list[JsonObject] = []
        with self._lock:
            self._queue.append(event)
            self._queue_bytes += _event_size(event)
            if len(self._queue) >= self.batch_max_events or self._queue_bytes >= self.batch_max_bytes:
                batch = self._take_batch_locked()
            else:
                self._schedule_flush_locked()
        self._submit_batch(batch)

    def _schedule_flush_locked(self) -> None:
        if self._timer is not None or self.batch_flush_interval_ms <= 0:
            return
        self._timer = Timer(self.batch_flush_interval_ms / 1000, self._flush_due)
        self._timer.daemon = True
        self._timer.start()

    def _flush_due(self) -> None:
        with self._lock:
            self._timer = None
            batch = self._take_batch_locked()
        self._submit_batch(batch)

    def _take_batch_locked(self) -> list[JsonObject]:
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None
        if not self._queue:
            return []
        batch = self._queue
        self._queue = []
        self._queue_bytes = 0
        return batch

    def _submit_batch(self, batch: list[JsonObject]) -> None:
        if not batch:
            return
        future: Future[None] = self._executor.submit(self._send_batch, batch)
        with self._lock:
            self._pending.add(future)

        def done(completed: Future[None]) -> None:
            with self._lock:
                self._pending.discard(completed)
            try:
                completed.result()
            except BaseException as error:
                with self._lock:
                    self._errors.append(error)

        future.add_done_callback(done)

    def _send_batch(self, batch: list[JsonObject]) -> None:
        send_batch = getattr(self.transport, "send_batch", None)
        if len(batch) > 1 and send_batch is not None:
            send_batch(batch)
            return
        for event in batch:
            self.transport.send(event)


@dataclass
class _SpanBase:
    client: _NanotraceClient
    name: str
    trace_id: str
    span_id: str
    parent_span_id: str | None
    attrs: JsonObject
    start_time: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    ended: bool = False
    _context_manager: Any = None

    def set(self, key: str, value: Any) -> None:
        self.attrs[key] = normalize_json(value)

    def event(self, name: str, **data: Any) -> None:
        self.client._write(
            "log",
            {
                **normalize_common(data),
                "trace_id": self.trace_id,
                "span_id": self.span_id,
                "name": name,
            },
        )

    def end(self, **data: Any) -> None:
        if self.ended:
            return
        self.ended = True
        end_time = datetime.now(timezone.utc)
        self.client.emit(
            {
                **self.attrs,
                **normalize_common(data),
                "event_type": "span",
                "name": self.name,
                "trace_id": self.trace_id,
                "span_id": self.span_id,
                **({"parent_span_id": self.parent_span_id} if self.parent_span_id else {}),
                "span_kind": self.attrs.get("span_kind", "internal"),
                "start_time": _iso(self.start_time),
                "end_time": _iso(end_time),
                "duration_ms": end_time.timestamp() * 1000 - self.start_time.timestamp() * 1000,
            }
        )


@dataclass
class Span(_SpanBase):
    client: Nanotrace

    def __enter__(self) -> "Span":
        self._context_manager = trace_context(trace_id=self.trace_id, span_id=self.span_id)
        self._context_manager.__enter__()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> bool:
        try:
            if exc is None:
                self.end(span_status_code="ok")
            else:
                self.end(span_status_code="error", is_error=1, **_error_fields(exc))
            return False
        finally:
            if self._context_manager is not None:
                self._context_manager.__exit__(exc_type, exc, tb)


@dataclass
class AsyncNanotrace(_NanotraceClient):
    transport: AsyncTransport
    base_context: dict[str, Any] = field(default_factory=dict)
    batch_max_events: int = DEFAULT_BATCH_MAX_EVENTS
    batch_max_bytes: int = DEFAULT_BATCH_MAX_BYTES
    batch_flush_interval_ms: int = DEFAULT_BATCH_FLUSH_INTERVAL_MS
    _queue: list[JsonObject] = field(default_factory=list, init=False, repr=False)
    _queue_bytes: int = field(default=0, init=False, repr=False)
    _flush_handle: asyncio.TimerHandle | None = field(default=None, init=False, repr=False)
    _pending: set[asyncio.Task[None]] = field(default_factory=set, init=False, repr=False)
    _errors: list[BaseException] = field(default_factory=list, init=False, repr=False)

    async def flush(self) -> None:
        self._flush_queued()
        while self._pending:
            await asyncio.gather(*tuple(self._pending), return_exceptions=True)

        if self._errors:
            errors = list(self._errors)
            self._errors.clear()
            raise NanotraceFlushError(errors)

    def _make_span(
        self,
        *,
        name: str,
        trace_id: str,
        span_id: str,
        parent_span_id: str | None,
        attrs: JsonObject,
    ) -> "AsyncSpan":
        return AsyncSpan(
            client=self,
            name=name,
            trace_id=trace_id,
            span_id=span_id,
            parent_span_id=parent_span_id,
            attrs=attrs,
        )

    def _enqueue(self, event: JsonObject) -> None:
        self._queue.append(event)
        self._queue_bytes += _event_size(event)
        if len(self._queue) >= self.batch_max_events or self._queue_bytes >= self.batch_max_bytes:
            self._flush_queued()
        else:
            self._schedule_flush()

    def _schedule_flush(self) -> None:
        if self._flush_handle is not None or self.batch_flush_interval_ms <= 0:
            return
        loop = asyncio.get_running_loop()
        self._flush_handle = loop.call_later(self.batch_flush_interval_ms / 1000, self._flush_queued)

    def _flush_queued(self) -> None:
        if self._flush_handle is not None:
            self._flush_handle.cancel()
            self._flush_handle = None
        if not self._queue:
            return
        batch = self._queue
        self._queue = []
        self._queue_bytes = 0
        task = asyncio.create_task(self._send_batch(batch))
        self._pending.add(task)

        def done(completed: asyncio.Task[None]) -> None:
            self._pending.discard(completed)
            try:
                completed.result()
            except BaseException as error:
                self._errors.append(error)

        task.add_done_callback(done)

    async def _send_batch(self, batch: list[JsonObject]) -> None:
        send_batch = getattr(self.transport, "send_batch", None)
        if len(batch) > 1 and send_batch is not None:
            await send_batch(batch)
            return
        for event in batch:
            await self.transport.send(event)


@dataclass
class AsyncSpan(_SpanBase):
    client: AsyncNanotrace

    async def __aenter__(self) -> "AsyncSpan":
        self._context_manager = trace_context(trace_id=self.trace_id, span_id=self.span_id)
        self._context_manager.__enter__()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> bool:
        try:
            if exc is None:
                self.end(span_status_code="ok")
            else:
                self.end(span_status_code="error", is_error=1, **_error_fields(exc))
            return False
        finally:
            if self._context_manager is not None:
                self._context_manager.__exit__(exc_type, exc, tb)


def create_nanotrace(
    transport: Transport,
    *,
    batch_max_events: int = DEFAULT_BATCH_MAX_EVENTS,
    batch_max_bytes: int = DEFAULT_BATCH_MAX_BYTES,
    batch_flush_interval_ms: int = DEFAULT_BATCH_FLUSH_INTERVAL_MS,
    **base_context: Any,
) -> Nanotrace:
    return Nanotrace(
        transport=transport,
        base_context=base_context,
        batch_max_events=batch_max_events,
        batch_max_bytes=batch_max_bytes,
        batch_flush_interval_ms=batch_flush_interval_ms,
    )


def create_async_nanotrace(
    transport: AsyncTransport,
    *,
    batch_max_events: int = DEFAULT_BATCH_MAX_EVENTS,
    batch_max_bytes: int = DEFAULT_BATCH_MAX_BYTES,
    batch_flush_interval_ms: int = DEFAULT_BATCH_FLUSH_INTERVAL_MS,
    **base_context: Any,
) -> AsyncNanotrace:
    return AsyncNanotrace(
        transport=transport,
        base_context=base_context,
        batch_max_events=batch_max_events,
        batch_max_bytes=batch_max_bytes,
        batch_flush_interval_ms=batch_flush_interval_ms,
    )


def _event_size(event: JsonObject) -> int:
    return len(json.dumps(event, separators=(",", ":")).encode("utf-8"))
