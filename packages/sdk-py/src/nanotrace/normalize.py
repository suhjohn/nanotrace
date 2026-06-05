from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any

from .types import Json, JsonObject

FIELD_ALIASES = (
    ("tenant_id", "tenant_id"),
    ("service.namespace", "service_namespace"),
    ("service.instance.id", "service_instance_id"),
    ("service_version", "service_version"),
    ("host.name", "host_name"),
    ("host.id", "host_id"),
    ("trace_id", "trace_id"),
    ("span_id", "span_id"),
    ("parent_span_id", "parent_span_id"),
    ("span_kind", "span_kind"),
    ("span_status_code", "span_status_code"),
    ("span_status_message", "span_status_message"),
    ("is_error", "is_error"),
    ("user_id", "user_id"),
    ("anonymous_id", "anonymous_id"),
    ("session_id", "session_id"),
    ("account_id", "account_id"),
    ("group_id", "group_id"),
    ("organization_id", "organization_id"),
    ("request_id", "request_id"),
    ("thread_id", "thread_id"),
    ("conversation_id", "conversation_id"),
    ("duration_ms", "duration_ms"),
    ("start_time", "start_time"),
    ("end_time", "end_time"),
    ("status_code", "status_code"),
    ("logger.name", "logger_name"),
    ("thread.name", "thread_name"),
    ("metric_name", "metric_name"),
    ("metric_type", "metric_type"),
    ("metric_value", "metric_value"),
    ("metric_unit", "metric_unit"),
    ("metric.temporality", "metric_temporality"),
    ("metric.is_monotonic", "metric_is_monotonic"),
    ("product_id", "product_id"),
    ("revenue_type", "revenue_type"),
    ("experiment_id", "experiment_id"),
    ("feature_flag", "feature_flag"),
    ("llm.model", "llm_model"),
    ("llm.provider", "llm_provider"),
    ("tool_name", "tool_name"),
)


def _camel_case(value: str) -> str:
    first, *rest = value.split("_")
    return first + "".join(part.capitalize() for part in rest)


FIELD_MAP = {
    alias: canonical
    for canonical, snake_case in FIELD_ALIASES
    for alias in (snake_case, _camel_case(snake_case))
}


def normalize_common(*items: dict[str, Any] | None) -> JsonObject:
    output: JsonObject = {}
    for item in items:
        if not item:
            continue
        for key, value in item.items():
            if value is None:
                continue
            output[FIELD_MAP.get(key, key)] = normalize_json(value)
    return output


def normalize_json(value: Any) -> Json:
    if isinstance(value, datetime):
        return _iso_datetime(value)
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if isinstance(value, (list, tuple)):
        return [normalize_json(item) for item in value]
    if isinstance(value, dict):
        return {str(key): normalize_json(child) for key, child in value.items()}
    return str(value)


def without_keys(data: dict[str, Any], keys: set[str]) -> dict[str, Any]:
    return {key: value for key, value in data.items() if key not in keys}


def _iso_datetime(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.isoformat().replace("+00:00", "Z")
