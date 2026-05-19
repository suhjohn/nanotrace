from __future__ import annotations

import secrets
import traceback
import asyncio
from concurrent.futures import Future, ThreadPoolExecutor, wait
from dataclasses import dataclass, field
from datetime import datetime, timezone
from threading import Lock
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


def _severity_number(level: str) -> int:
    match level:
        case "debug":
            return 5
        case "info":
            return 9
        case "warn" | "warning":
            return 13
        case "error":
            return 17
        case _:
            return 9


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


@dataclass
class Nanotrace:
    transport: Transport
    base_context: dict[str, Any] = field(default_factory=dict)
    _executor: ThreadPoolExecutor = field(
        default_factory=lambda: ThreadPoolExecutor(max_workers=1, thread_name_prefix="nanotrace"),
        init=False,
        repr=False,
    )
    _pending: set[Future[None]] = field(default_factory=set, init=False, repr=False)
    _errors: list[BaseException] = field(default_factory=list, init=False, repr=False)
    _lock: Lock = field(default_factory=Lock, init=False, repr=False)

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

    def flush(self) -> None:
        while True:
            with self._lock:
                pending = tuple(self._pending)
            if not pending:
                break
            wait(pending)

        if self._errors:
            errors = list(self._errors)
            self._errors.clear()
            raise NanotraceFlushError(errors)

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

    def span(self, name: str, **data: Any) -> "Span":
        return self.start_span(name, **data)

    def start_span(self, name: str, **data: Any) -> "Span":
        trace_id = data.pop("trace_id", None) or data.pop("traceId", None) or current_context().get("trace_id") or _random_hex(16)
        span_id = data.pop("span_id", None) or data.pop("spanId", None) or _random_hex(8)
        kind = data.pop("kind", None) or data.pop("span_kind", None) or data.pop("spanKind", None) or "internal"
        parent_span_id = (
            data.pop("parent_span_id", None)
            or data.pop("parentSpanId", None)
            or current_context().get("span_id")
        )
        return Span(
            client=self,
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
                **({"http.status_code": status_code, "http.response.status_code": status_code} if status_code else {}),
                "duration_ms": duration_ms,
                "is_error": 1 if isinstance(status_code, int) and status_code >= 500 else 0,
                **normalize_common(without_keys(data, {"method", "route", "path", "url", "status_code", "statusCode", "duration_ms", "durationMs"})),
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
                **({"http.status_code": status_code, "http.response.status_code": status_code} if status_code else {}),
                "duration_ms": duration_ms,
                "is_error": 1 if isinstance(status_code, int) and status_code >= 500 else 0,
                **normalize_common(without_keys(data, {"method", "url", "status_code", "statusCode", "duration_ms", "durationMs"})),
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
                **normalize_common(without_keys(data, {"system", "operation", "statement", "duration_ms", "durationMs"})),
                "name": operation or system,
                "span_kind": "client",
                "db.system": system,
                **({"db.operation": operation} if operation else {}),
                **({"db.statement": statement} if statement else {}),
                "duration_ms": duration_ms,
            },
        )

    def rpc_call(self, *, system: str, service: str, method: str, duration_ms: float, **data: Any) -> None:
        self._write(
            "span",
            {
                **normalize_common(without_keys(data, {"system", "service", "method", "duration_ms", "durationMs"})),
                "name": f"{service}/{method}",
                "span_kind": "client",
                "rpc.system": system,
                "rpc.service": service,
                "rpc.method": method,
                "duration_ms": duration_ms,
            },
        )

    def message_publish(self, *, system: str, destination: str, duration_ms: float | None = None, **data: Any) -> None:
        self._message_operation("publish", system=system, destination=destination, duration_ms=duration_ms, **data)

    def message_consume(self, *, system: str, destination: str, duration_ms: float | None = None, **data: Any) -> None:
        self._message_operation("consume", system=system, destination=destination, duration_ms=duration_ms, **data)

    def counter(self, name: str, value: float = 1, **data: Any) -> None:
        self._metric(name, "counter", value, {"metric.temporality": "delta", "metric.is_monotonic": True}, data)

    def gauge(self, name: str, value: float, **data: Any) -> None:
        self._metric(name, "gauge", value, {}, data)

    def histogram(self, name: str, value: float, **data: Any) -> None:
        self._metric(name, "histogram", value, {}, data)

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
                **normalize_common(without_keys(data, {"name", "url", "path", "title", "referrer"})),
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
                **normalize_common(without_keys(data, {"system", "destination", "duration_ms", "durationMs"})),
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

    def _enqueue(self, event: JsonObject) -> None:
        future: Future[None] = self._executor.submit(self.transport.send, event)
        with self._lock:
            self._pending.add(future)

        def done(completed: Future[None]) -> None:
            with self._lock:
                self._pending.discard(completed)
            try:
                completed.result()
            except BaseException as error:
                self._errors.append(error)

        future.add_done_callback(done)


@dataclass
class Span:
    client: Nanotrace
    name: str
    trace_id: str
    span_id: str
    parent_span_id: str | None
    attrs: JsonObject
    start_time: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    ended: bool = False
    _context_manager: Any = None

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
class AsyncNanotrace:
    transport: AsyncTransport
    base_context: dict[str, Any] = field(default_factory=dict)
    _pending: set[asyncio.Task[None]] = field(default_factory=set, init=False, repr=False)
    _errors: list[BaseException] = field(default_factory=list, init=False, repr=False)

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

    async def flush(self) -> None:
        while self._pending:
            await asyncio.gather(*tuple(self._pending), return_exceptions=True)

        if self._errors:
            errors = list(self._errors)
            self._errors.clear()
            raise NanotraceFlushError(errors)

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

    def span(self, name: str, **data: Any) -> "AsyncSpan":
        return self.start_span(name, **data)

    def start_span(self, name: str, **data: Any) -> "AsyncSpan":
        trace_id = data.pop("trace_id", None) or data.pop("traceId", None) or current_context().get("trace_id") or _random_hex(16)
        span_id = data.pop("span_id", None) or data.pop("spanId", None) or _random_hex(8)
        kind = data.pop("kind", None) or data.pop("span_kind", None) or data.pop("spanKind", None) or "internal"
        parent_span_id = (
            data.pop("parent_span_id", None)
            or data.pop("parentSpanId", None)
            or current_context().get("span_id")
        )
        return AsyncSpan(
            client=self,
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
                **({"http.status_code": status_code, "http.response.status_code": status_code} if status_code else {}),
                "duration_ms": duration_ms,
                "is_error": 1 if isinstance(status_code, int) and status_code >= 500 else 0,
                **normalize_common(without_keys(data, {"method", "route", "path", "url", "status_code", "statusCode", "duration_ms", "durationMs"})),
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
                **({"http.status_code": status_code, "http.response.status_code": status_code} if status_code else {}),
                "duration_ms": duration_ms,
                "is_error": 1 if isinstance(status_code, int) and status_code >= 500 else 0,
                **normalize_common(without_keys(data, {"method", "url", "status_code", "statusCode", "duration_ms", "durationMs"})),
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
                **normalize_common(without_keys(data, {"system", "operation", "statement", "duration_ms", "durationMs"})),
                "name": operation or system,
                "span_kind": "client",
                "db.system": system,
                **({"db.operation": operation} if operation else {}),
                **({"db.statement": statement} if statement else {}),
                "duration_ms": duration_ms,
            },
        )

    def rpc_call(self, *, system: str, service: str, method: str, duration_ms: float, **data: Any) -> None:
        self._write(
            "span",
            {
                **normalize_common(without_keys(data, {"system", "service", "method", "duration_ms", "durationMs"})),
                "name": f"{service}/{method}",
                "span_kind": "client",
                "rpc.system": system,
                "rpc.service": service,
                "rpc.method": method,
                "duration_ms": duration_ms,
            },
        )

    def message_publish(self, *, system: str, destination: str, duration_ms: float | None = None, **data: Any) -> None:
        self._message_operation("publish", system=system, destination=destination, duration_ms=duration_ms, **data)

    def message_consume(self, *, system: str, destination: str, duration_ms: float | None = None, **data: Any) -> None:
        self._message_operation("consume", system=system, destination=destination, duration_ms=duration_ms, **data)

    def counter(self, name: str, value: float = 1, **data: Any) -> None:
        self._metric(name, "counter", value, {"metric.temporality": "delta", "metric.is_monotonic": True}, data)

    def gauge(self, name: str, value: float, **data: Any) -> None:
        self._metric(name, "gauge", value, {}, data)

    def histogram(self, name: str, value: float, **data: Any) -> None:
        self._metric(name, "histogram", value, {}, data)

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
                **normalize_common(without_keys(data, {"name", "url", "path", "title", "referrer"})),
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
                **normalize_common(without_keys(data, {"system", "destination", "duration_ms", "durationMs"})),
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

    def _enqueue(self, event: JsonObject) -> None:
        task = asyncio.create_task(self.transport.send(event))
        self._pending.add(task)

        def done(completed: asyncio.Task[None]) -> None:
            self._pending.discard(completed)
            try:
                completed.result()
            except BaseException as error:
                self._errors.append(error)

        task.add_done_callback(done)


@dataclass
class AsyncSpan:
    client: AsyncNanotrace
    name: str
    trace_id: str
    span_id: str
    parent_span_id: str | None
    attrs: JsonObject
    start_time: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    ended: bool = False
    _context_manager: Any = None

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


def create_nanotrace(transport: Transport, **base_context: Any) -> Nanotrace:
    return Nanotrace(transport=transport, base_context=base_context)


def create_async_nanotrace(transport: AsyncTransport, **base_context: Any) -> AsyncNanotrace:
    return AsyncNanotrace(transport=transport, base_context=base_context)
