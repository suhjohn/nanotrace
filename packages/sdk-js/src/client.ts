import { randomBytes, randomUUID } from 'node:crypto'
import { contextStorage, currentContext, withContext } from './context.js'
import { normalizeCommon, normalizeJson, withoutKeys } from './normalize.js'
import type {
  CommonFields,
  DbQuery,
  EventEnvelope,
  ExperimentViewedEvent,
  FeatureFlagEvent,
  HttpClientRequest,
  HttpServerRequest,
  Json,
  JsonObject,
  LogLevel,
  MaybePromise,
  MessageOperation,
  PageEvent,
  RevenueEvent,
  RpcCall,
  SpanHandle,
  SpanOptions,
  SpanRecord,
  Transport
} from './types.js'

export type NanotraceOptions = CommonFields & {
  transport: Transport
}

export class Nanotrace {
  private readonly baseContext: CommonFields
  private readonly transport: Transport

  constructor(options: NanotraceOptions) {
    const { transport, ...baseContext } = options
    this.transport = transport
    this.baseContext = baseContext
  }

  withContext = withContext
  currentContext = currentContext

  async emit(event: EventEnvelope): Promise<void> {
    await this.transport.send({
      event_id: event.event_id ?? randomUUID(),
      timestamp: event.timestamp ?? new Date().toISOString(),
      ...(event.observed_timestamp ? { observed_timestamp: event.observed_timestamp } : {}),
      data: {
        ...normalizeCommon(this.baseContext, currentContext()),
        ...event.data
      }
    })
  }

  async event(name: string, data: CommonFields = {}): Promise<void> {
    await this.write('analytics', { name, ...normalizeCommon(data) })
  }

  async log(level: LogLevel, message: string, data: CommonFields = {}): Promise<void> {
    await this.write('log', {
      ...normalizeCommon(data),
      severity_text: level.toUpperCase(),
      severity_number: severityNumber(level),
      body: message,
      is_error: level === 'error' ? 1 : 0
    })
  }

  debug(message: string, data?: CommonFields): Promise<void> {
    return this.log('debug', message, data)
  }

  info(message: string, data?: CommonFields): Promise<void> {
    return this.log('info', message, data)
  }

  warn(message: string, data?: CommonFields): Promise<void> {
    return this.log('warn', message, data)
  }

  error(errorOrMessage: unknown, data: CommonFields = {}): Promise<void> {
    if (errorOrMessage instanceof Error) {
      return this.captureException(errorOrMessage, data)
    }
    return this.log('error', String(errorOrMessage), data)
  }

  async captureException(error: unknown, data: CommonFields = {}): Promise<void> {
    const payload = errorPayload(error)
    await this.write('log', {
      ...normalizeCommon(data),
      severity_text: 'ERROR',
      severity_number: 17,
      body: payload.message,
      is_error: 1,
      'exception.type': payload.name,
      'exception.message': payload.message,
      ...(payload.stack ? { 'exception.stacktrace': payload.stack } : {})
    })
  }

  async span<T>(
    name: string,
    fn: (span: SpanHandle) => MaybePromise<T>,
    data: SpanOptions = {}
  ): Promise<T> {
    const span = this.startSpan(name, data)
    try {
      const result = await contextStorage().run(spanContext(span), () => fn(span))
      await span.end({ spanStatusCode: 'ok' })
      return result
    } catch (error) {
      await span.end({
        spanStatusCode: 'error',
        is_error: 1,
        ...errorFields(error)
      })
      throw error
    }
  }

  startSpan(name: string, data: SpanOptions = {}): SpanHandle {
    const traceId = data.traceId ?? currentContext().traceId ?? randomHex(16)
    const spanId = data.spanId ?? randomHex(8)
    const parentSpanId = data.parentSpanId ?? currentContext().spanId
    const startTime = new Date()
    const attrs: JsonObject = {
      ...normalizeCommon(data),
      event_type: 'span',
      name,
      trace_id: traceId,
      span_id: spanId,
      ...(parentSpanId ? { parent_span_id: parentSpanId } : {}),
      span_kind: data.kind ?? 'internal',
      start_time: startTime.toISOString()
    }
    let ended = false

    return {
      traceId,
      spanId,
      set(key, value) {
        attrs[key] = normalizeJson(value)
      },
      event: async (eventName, eventData = {}) => {
        await this.write('log', {
          ...normalizeCommon(eventData),
          trace_id: traceId,
          span_id: spanId,
          name: eventName
        })
      },
      end: async (endData = {}) => {
        if (ended) return
        ended = true
        const endTime = new Date()
        await this.emit({
          timestamp: endTime.toISOString(),
          data: {
            ...attrs,
            ...normalizeCommon(endData),
            end_time: endTime.toISOString(),
            duration_ms: endTime.getTime() - startTime.getTime()
          }
        })
      }
    }
  }

  recordSpan(data: SpanRecord): Promise<void> {
    const start = dateMs(data.startTime)
    const end = dateMs(data.endTime)
    return this.write('span', {
      ...normalizeCommon(data),
      name: data.name,
      start_time: iso(data.startTime),
      end_time: iso(data.endTime),
      duration_ms: data.durationMs ?? end - start,
      span_status_code: data.statusCode ?? 'ok',
      span_kind: data.kind ?? 'internal'
    })
  }

  httpServerRequest(data: HttpServerRequest): Promise<void> {
    return this.write('span', {
      ...normalizeCommon(withoutKeys(data, ['method', 'route', 'path', 'url', 'statusCode', 'durationMs'])),
      name: `${data.method} ${data.route ?? data.path ?? data.url ?? ''}`.trim(),
      span_kind: 'server',
      'http.method': data.method,
      'http.request.method': data.method,
      ...(data.route ? { 'http.route': data.route } : {}),
      ...(data.path ? { 'url.path': data.path } : {}),
      ...(data.url ? { 'url.full': data.url } : {}),
      ...(data.statusCode ? { 'http.status_code': data.statusCode, 'http.response.status_code': data.statusCode } : {}),
      duration_ms: data.durationMs,
      is_error: data.statusCode && data.statusCode >= 500 ? 1 : 0
    })
  }

  httpClientRequest(data: HttpClientRequest): Promise<void> {
    return this.write('span', {
      ...normalizeCommon(withoutKeys(data, ['method', 'url', 'statusCode', 'durationMs'])),
      name: `${data.method} ${data.url}`,
      span_kind: 'client',
      'http.method': data.method,
      'http.request.method': data.method,
      'url.full': data.url,
      ...(data.statusCode ? { 'http.status_code': data.statusCode, 'http.response.status_code': data.statusCode } : {}),
      duration_ms: data.durationMs,
      is_error: data.statusCode && data.statusCode >= 500 ? 1 : 0
    })
  }

  dbQuery(data: DbQuery): Promise<void> {
    return this.write('span', {
      ...normalizeCommon(withoutKeys(data, ['system', 'operation', 'statement', 'durationMs'])),
      name: data.operation ?? data.system,
      span_kind: 'client',
      'db.system': data.system,
      ...(data.operation ? { 'db.operation': data.operation } : {}),
      ...(data.statement ? { 'db.statement': data.statement } : {}),
      duration_ms: data.durationMs
    })
  }

  rpcCall(data: RpcCall): Promise<void> {
    return this.write('span', {
      ...normalizeCommon(withoutKeys(data, ['system', 'service', 'method', 'durationMs'])),
      name: `${data.service}/${data.method}`,
      span_kind: 'client',
      'rpc.system': data.system,
      'rpc.service': data.service,
      'rpc.method': data.method,
      duration_ms: data.durationMs
    })
  }

  messagePublish(data: MessageOperation): Promise<void> {
    return this.messageOperation('publish', data)
  }

  messageConsume(data: MessageOperation): Promise<void> {
    return this.messageOperation('consume', data)
  }

  counter(name: string, value = 1, data?: CommonFields): Promise<void> {
    return this.metric(name, 'counter', value, { 'metric.temporality': 'delta', 'metric.is_monotonic': true }, data)
  }

  gauge(name: string, value: number, data?: CommonFields): Promise<void> {
    return this.metric(name, 'gauge', value, {}, data)
  }

  histogram(name: string, value: number, data?: CommonFields): Promise<void> {
    return this.metric(name, 'histogram', value, {}, data)
  }

  timing(name: string, durationMs: number, data?: CommonFields): Promise<void> {
    return this.histogram(name, durationMs, { metricUnit: 'ms', ...data })
  }

  track(name: string, properties: CommonFields = {}): Promise<void> {
    return this.write('track', { ...normalizeCommon(properties), name })
  }

  identify(userId: string, traits: CommonFields = {}): Promise<void> {
    return this.write('identify', { ...normalizeCommon(traits), user_id: userId })
  }

  group(accountId: string, traits: CommonFields = {}): Promise<void> {
    return this.write('group', { ...normalizeCommon(traits), account_id: accountId })
  }

  alias(previousId: string, userId: string, data: CommonFields = {}): Promise<void> {
    return this.write('alias', { ...normalizeCommon(data), previous_id: previousId, user_id: userId })
  }

  page(data: PageEvent): Promise<void> {
    return this.write('page', {
      ...normalizeCommon(withoutKeys(data, ['name', 'url', 'path', 'title', 'referrer'])),
      ...(data.name ? { name: data.name } : {}),
      ...(data.url ? { page_url: data.url } : {}),
      ...(data.path ? { page_path: data.path } : {}),
      ...(data.title ? { page_title: data.title } : {}),
      ...(data.referrer ? { referrer: data.referrer } : {})
    })
  }

  screen(name: string, data: CommonFields = {}): Promise<void> {
    return this.write('screen', { ...normalizeCommon(data), screen_name: name, name })
  }

  revenue(data: RevenueEvent): Promise<void> {
    return this.write('track', { ...normalizeCommon(data), name: 'Revenue' })
  }

  experimentViewed(data: ExperimentViewedEvent): Promise<void> {
    return this.write('track', { ...normalizeCommon(data), name: 'Experiment Viewed' })
  }

  featureFlagEvaluated(data: FeatureFlagEvent): Promise<void> {
    return this.write('track', { ...normalizeCommon(data), name: 'Feature Flag Evaluated' })
  }

  private messageOperation(operation: 'publish' | 'consume', data: MessageOperation): Promise<void> {
    return this.write('span', {
      ...normalizeCommon(withoutKeys(data, ['system', 'destination', 'durationMs'])),
      name: `${operation} ${data.destination}`,
      span_kind: operation === 'publish' ? 'producer' : 'consumer',
      'messaging.system': data.system,
      'messaging.destination.name': data.destination,
      'messaging.operation.name': operation,
      ...(data.durationMs !== undefined ? { duration_ms: data.durationMs } : {})
    })
  }

  private metric(
    name: string,
    type: string,
    value: number,
    defaults: JsonObject,
    data: CommonFields = {}
  ): Promise<void> {
    return this.write('metric', {
      ...defaults,
      ...normalizeCommon(data),
      metric_name: name,
      metric_type: type,
      metric_value: value
    })
  }

  private write(eventType: string, data: JsonObject): Promise<void> {
    return this.emit({ data: { ...data, event_type: eventType } })
  }
}

export function createNanotrace(options: NanotraceOptions): Nanotrace {
  return new Nanotrace(options)
}

function spanContext(span: SpanHandle): CommonFields {
  return { traceId: span.traceId, spanId: span.spanId }
}

function severityNumber(level: LogLevel): number {
  switch (level) {
    case 'debug':
      return 5
    case 'info':
      return 9
    case 'warn':
      return 13
    case 'error':
      return 17
  }
}

function errorPayload(error: unknown): { name: string; message: string; stack?: string } {
  if (error instanceof Error) {
    return {
      name: error.name || 'Error',
      message: error.message,
      ...(error.stack ? { stack: error.stack } : {})
    }
  }
  return { name: 'Error', message: String(error) }
}

function errorFields(error: unknown): CommonFields {
  const payload = errorPayload(error)
  return {
    'exception.type': payload.name,
    'exception.message': payload.message,
    ...(payload.stack ? { 'exception.stacktrace': payload.stack } : {})
  }
}

function randomHex(bytes: number): string {
  return randomBytes(bytes).toString('hex')
}

function iso(value: Date | string): string {
  return value instanceof Date ? value.toISOString() : value
}

function dateMs(value: Date | string): number {
  return value instanceof Date ? value.getTime() : new Date(value).getTime()
}
