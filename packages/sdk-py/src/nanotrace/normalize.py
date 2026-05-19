from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any

from .types import Json, JsonObject

FIELD_MAP = {
    "tenant_id": "tenant_id",
    "tenantId": "tenant_id",
    "service_namespace": "service.namespace",
    "serviceNamespace": "service.namespace",
    "service_instance_id": "service.instance.id",
    "serviceInstanceId": "service.instance.id",
    "service_version": "service_version",
    "serviceVersion": "service_version",
    "host_name": "host.name",
    "hostName": "host.name",
    "host_id": "host.id",
    "hostId": "host.id",
    "trace_id": "trace_id",
    "traceId": "trace_id",
    "span_id": "span_id",
    "spanId": "span_id",
    "parent_span_id": "parent_span_id",
    "parentSpanId": "parent_span_id",
    "span_kind": "span_kind",
    "spanKind": "span_kind",
    "span_status_code": "span_status_code",
    "spanStatusCode": "span_status_code",
    "span_status_message": "span_status_message",
    "spanStatusMessage": "span_status_message",
    "is_error": "is_error",
    "isError": "is_error",
    "user_id": "user_id",
    "userId": "user_id",
    "anonymous_id": "anonymous_id",
    "anonymousId": "anonymous_id",
    "session_id": "session_id",
    "sessionId": "session_id",
    "account_id": "account_id",
    "accountId": "account_id",
    "group_id": "group_id",
    "groupId": "group_id",
    "organization_id": "organization_id",
    "organizationId": "organization_id",
    "request_id": "request_id",
    "requestId": "request_id",
    "thread_id": "thread_id",
    "threadId": "thread_id",
    "conversation_id": "conversation_id",
    "conversationId": "conversation_id",
    "duration_ms": "duration_ms",
    "durationMs": "duration_ms",
    "start_time": "start_time",
    "startTime": "start_time",
    "end_time": "end_time",
    "endTime": "end_time",
    "status_code": "status_code",
    "statusCode": "status_code",
    "logger_name": "logger.name",
    "loggerName": "logger.name",
    "thread_name": "thread.name",
    "threadName": "thread.name",
    "metric_name": "metric_name",
    "metricName": "metric_name",
    "metric_type": "metric_type",
    "metricType": "metric_type",
    "metric_value": "metric_value",
    "metricValue": "metric_value",
    "metric_unit": "metric_unit",
    "metricUnit": "metric_unit",
    "metric_temporality": "metric.temporality",
    "metricTemporality": "metric.temporality",
    "metric_is_monotonic": "metric.is_monotonic",
    "metricIsMonotonic": "metric.is_monotonic",
    "product_id": "product_id",
    "productId": "product_id",
    "revenue_type": "revenue_type",
    "revenueType": "revenue_type",
    "experiment_id": "experiment_id",
    "experimentId": "experiment_id",
    "feature_flag": "feature_flag",
    "featureFlag": "feature_flag",
    "llm_model": "llm.model",
    "llmModel": "llm.model",
    "llm_provider": "llm.provider",
    "llmProvider": "llm.provider",
    "tool_name": "tool_name",
    "toolName": "tool_name",
    "processor_name": "processor_name",
    "processorName": "processor_name",
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
