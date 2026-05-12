import type { CommonFields, Json, JsonObject } from './types.js'

const fieldMap: Record<string, string> = {
  tenantId: 'tenant_id',
  serviceNamespace: 'service.namespace',
  serviceInstanceId: 'service.instance.id',
  serviceVersion: 'service_version',
  hostName: 'host.name',
  hostId: 'host.id',
  traceId: 'trace_id',
  spanId: 'span_id',
  parentSpanId: 'parent_span_id',
  spanKind: 'span_kind',
  spanStatusCode: 'span_status_code',
  spanStatusMessage: 'span_status_message',
  isError: 'is_error',
  userId: 'user_id',
  anonymousId: 'anonymous_id',
  sessionId: 'session_id',
  accountId: 'account_id',
  durationMs: 'duration_ms',
  startTime: 'start_time',
  endTime: 'end_time',
  statusCode: 'status_code',
  loggerName: 'logger.name',
  threadName: 'thread.name',
  metricName: 'metric_name',
  metricType: 'metric_type',
  metricValue: 'metric_value',
  metricUnit: 'metric_unit',
  metricTemporality: 'metric.temporality',
  metricIsMonotonic: 'metric.is_monotonic',
  productId: 'product_id',
  revenueType: 'revenue_type',
  experimentId: 'experiment_id',
  featureFlag: 'feature_flag'
}

export function normalizeCommon(...items: Array<CommonFields | undefined>): JsonObject {
  const output: JsonObject = {}
  for (const item of items) {
    if (!item) continue
    for (const [key, value] of Object.entries(item)) {
      if (value === undefined) continue
      output[fieldMap[key] ?? key] = normalizeJson(value)
    }
  }
  return output
}

export function normalizeJson(value: Json | Date): Json {
  if (value instanceof Date) return value.toISOString()
  if (Array.isArray(value)) return value.map(item => normalizeJson(item))
  if (value && typeof value === 'object') {
    const output: JsonObject = {}
    for (const [key, child] of Object.entries(value)) {
      output[key] = normalizeJson(child as Json)
    }
    return output
  }
  return value
}

export function withoutKeys<T extends Record<string, unknown>>(
  input: T,
  keys: readonly string[]
): CommonFields {
  const skip = new Set(keys)
  const output: CommonFields = {}
  for (const [key, value] of Object.entries(input)) {
    if (!skip.has(key) && isJson(value)) {
      output[key] = value
    }
  }
  return output
}

export function isJson(value: unknown): value is Json {
  if (
    value === null ||
    typeof value === 'boolean' ||
    typeof value === 'number' ||
    typeof value === 'string'
  ) {
    return true
  }
  if (Array.isArray(value)) return value.every(isJson)
  if (value && typeof value === 'object' && value.constructor === Object) {
    return Object.values(value).every(isJson)
  }
  return false
}
