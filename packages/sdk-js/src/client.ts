import { randomBytes, randomUUID } from 'node:crypto'
import { currentContext, withContext } from './context.js'
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
  batch?: BatchOptions
}

export type BatchOptions = {
  maxEvents?: number
  maxBytes?: number
  flushIntervalMs?: number
}

type ResolvedBatchOptions = Required<BatchOptions>

const DEFAULT_BATCH_OPTIONS: ResolvedBatchOptions = {
  maxEvents: 100,
  maxBytes: 512 * 1024,
  flushIntervalMs: 1000
}

const HTTP_SERVER_KEYS = ['method', 'route', 'path', 'url', 'statusCode', 'durationMs'] as const
const HTTP_CLIENT_KEYS = ['method', 'url', 'statusCode', 'durationMs'] as const
const DB_QUERY_KEYS = ['system', 'operation', 'statement', 'durationMs'] as const
const RPC_KEYS = ['system', 'service', 'method', 'durationMs'] as const
const MESSAGE_KEYS = ['system', 'destination', 'durationMs'] as const
const PAGE_KEYS = ['name', 'url', 'path', 'title', 'referrer'] as const

export class Nanotrace {
  private readonly baseContext: CommonFields
  private readonly transport: Transport
  private readonly batch: ResolvedBatchOptions
  private queue: EventEnvelope[] = []
  private queueBytes = 0
  private flushTimer: NodeJS.Timeout | undefined
  private readonly pending = new Set<Promise<void>>()
  private readonly errors: unknown[] = []

  constructor(options: NanotraceOptions) {
    const { transport, batch, ...baseContext } = options
    this.transport = transport
    this.batch = { ...DEFAULT_BATCH_OPTIONS, ...batch }
    this.baseContext = baseContext
  }

  withContext = withContext
  currentContext = currentContext

  emit(event: EventEnvelope): void {
    this.enqueue({
      event_id: event.event_id ?? randomUUID(),
      timestamp: event.timestamp ?? new Date().toISOString(),
      ...(event.observed_timestamp ? { observed_timestamp: event.observed_timestamp } : {}),
      data: {
        ...normalizeCommon(this.baseContext, currentContext()),
        ...event.data
      }
    })
  }

  async flush(): Promise<void> {
    this.flushQueued()
    while (this.pending.size > 0) {
      await Promise.allSettled([...this.pending])
    }

    if (this.errors.length > 0) {
      const errors = this.errors.splice(0)
      throw new AggregateError(errors, 'Nanotrace flush failed.')
    }
  }

  event(name: string, data: CommonFields = {}): void {
    this.write('analytics', { name, ...normalizeCommon(data) })
  }

  log(level: LogLevel, message: string, data: CommonFields = {}): void {
    this.write('log', {
      severity_text: level.toUpperCase(),
      severity_number: severityNumber(level),
      body: message,
      is_error: level === 'error' ? 1 : 0,
      ...normalizeCommon(data)
    })
  }

  debug(message: string, data?: CommonFields): void {
    this.log('debug', message, data)
  }

  info(message: string, data?: CommonFields): void {
    this.log('info', message, data)
  }

  warn(message: string, data?: CommonFields): void {
    this.log('warn', message, data)
  }

  error(errorOrMessage: unknown, data: CommonFields = {}): void {
    if (errorOrMessage instanceof Error) {
      this.captureException(errorOrMessage, data)
      return
    }
    this.log('error', String(errorOrMessage), data)
  }

  captureException(error: unknown, data: CommonFields = {}): void {
    const payload = errorPayload(error)
    this.write('log', {
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

  span(name: string, data: SpanOptions = {}): SpanHandle {
    return this.startSpan(name, data)
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
      event: (eventName, eventData = {}) => {
        this.write('log', {
          ...normalizeCommon(eventData),
          trace_id: traceId,
          span_id: spanId,
          name: eventName
        })
      },
      end: (endData = {}) => {
        if (ended) return
        ended = true
        const endTime = new Date()
        this.emit({
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

  recordSpan(data: SpanRecord): void {
    const start = dateMs(data.startTime)
    const end = dateMs(data.endTime)
    this.write('span', {
      ...normalizeCommon(data),
      name: data.name,
      start_time: iso(data.startTime),
      end_time: iso(data.endTime),
      duration_ms: data.durationMs ?? end - start,
      span_status_code: data.statusCode ?? 'ok',
      span_kind: data.kind ?? 'internal'
    })
  }

  httpServerRequest(data: HttpServerRequest): void {
    this.write('span', {
      name: `${data.method} ${data.route ?? data.path ?? data.url ?? ''}`.trim(),
      span_kind: 'server',
      'http.method': data.method,
      'http.request.method': data.method,
      ...(data.route ? { 'http.route': data.route } : {}),
      ...(data.path ? { 'url.path': data.path } : {}),
      ...(data.url ? { 'url.full': data.url } : {}),
      ...httpStatusFields(data.statusCode),
      duration_ms: data.durationMs,
      is_error: isServerError(data.statusCode),
      ...normalizeCommon(withoutKeys(data, HTTP_SERVER_KEYS))
    })
  }

  httpClientRequest(data: HttpClientRequest): void {
    this.write('span', {
      name: `${data.method} ${data.url}`,
      span_kind: 'client',
      'http.method': data.method,
      'http.request.method': data.method,
      'url.full': data.url,
      ...httpStatusFields(data.statusCode),
      duration_ms: data.durationMs,
      is_error: isServerError(data.statusCode),
      ...normalizeCommon(withoutKeys(data, HTTP_CLIENT_KEYS))
    })
  }

  dbQuery(data: DbQuery): void {
    this.write('span', {
      ...normalizeCommon(withoutKeys(data, DB_QUERY_KEYS)),
      name: data.operation ?? data.system,
      span_kind: 'client',
      'db.system': data.system,
      ...(data.operation ? { 'db.operation': data.operation } : {}),
      ...(data.statement ? { 'db.statement': data.statement } : {}),
      duration_ms: data.durationMs
    })
  }

  rpcCall(data: RpcCall): void {
    this.write('span', {
      ...normalizeCommon(withoutKeys(data, RPC_KEYS)),
      name: `${data.service}/${data.method}`,
      span_kind: 'client',
      'rpc.system': data.system,
      'rpc.service': data.service,
      'rpc.method': data.method,
      duration_ms: data.durationMs
    })
  }

  messagePublish(data: MessageOperation): void {
    this.messageOperation('publish', data)
  }

  messageConsume(data: MessageOperation): void {
    this.messageOperation('consume', data)
  }

  counter(name: string, value = 1, data?: CommonFields): void {
    this.metric(name, 'counter', value, { 'metric.temporality': 'delta', 'metric.is_monotonic': true }, data)
  }

  gauge(name: string, value: number, data?: CommonFields): void {
    this.metric(name, 'gauge', value, {}, data)
  }

  histogram(name: string, value: number, data?: CommonFields): void {
    this.metric(name, 'histogram', value, {}, data)
  }

  measure(name: string, value: number, data?: CommonFields): void {
    this.histogram(name, value, data)
  }

  timing(name: string, durationMs: number, data?: CommonFields): void {
    this.histogram(name, durationMs, { metricUnit: 'ms', ...data })
  }

  track(name: string, properties: CommonFields = {}): void {
    this.write('track', { ...normalizeCommon(properties), name })
  }

  identify(userId: string, traits: CommonFields = {}): void {
    this.write('identify', { ...normalizeCommon(traits), user_id: userId })
  }

  group(accountId: string, traits: CommonFields = {}): void {
    this.write('group', { ...normalizeCommon(traits), account_id: accountId })
  }

  alias(previousId: string, userId: string, data: CommonFields = {}): void {
    this.write('alias', { ...normalizeCommon(data), previous_id: previousId, user_id: userId })
  }

  page(data: PageEvent): void {
    this.write('page', {
      ...normalizeCommon(withoutKeys(data, PAGE_KEYS)),
      ...(data.name ? { name: data.name } : {}),
      ...(data.url ? { page_url: data.url } : {}),
      ...(data.path ? { page_path: data.path } : {}),
      ...(data.title ? { page_title: data.title } : {}),
      ...(data.referrer ? { referrer: data.referrer } : {})
    })
  }

  screen(name: string, data: CommonFields = {}): void {
    this.write('screen', { ...normalizeCommon(data), screen_name: name, name })
  }

  revenue(data: RevenueEvent): void {
    this.write('track', { ...normalizeCommon(data), name: 'Revenue' })
  }

  experimentViewed(data: ExperimentViewedEvent): void {
    this.write('track', { ...normalizeCommon(data), name: 'Experiment Viewed' })
  }

  featureFlagEvaluated(data: FeatureFlagEvent): void {
    this.write('track', { ...normalizeCommon(data), name: 'Feature Flag Evaluated' })
  }

  private enqueue(event: EventEnvelope): void {
    const eventBytes = Buffer.byteLength(JSON.stringify(event), 'utf8')
    this.queue.push(event)
    this.queueBytes += eventBytes
    if (this.queue.length >= this.batch.maxEvents || this.queueBytes >= this.batch.maxBytes) {
      this.flushQueued()
      return
    }
    this.scheduleFlush()
  }

  private scheduleFlush(): void {
    if (this.flushTimer || this.batch.flushIntervalMs <= 0) return
    this.flushTimer = setTimeout(() => {
      this.flushTimer = undefined
      this.flushQueued()
    }, this.batch.flushIntervalMs)
    this.flushTimer.unref?.()
  }

  private flushQueued(): void {
    if (this.flushTimer) {
      clearTimeout(this.flushTimer)
      this.flushTimer = undefined
    }
    if (this.queue.length === 0) return
    const events = this.queue
    this.queue = []
    this.queueBytes = 0

    let promise: Promise<void>
    promise = Promise.resolve()
      .then(() => this.sendEvents(events))
      .catch(error => {
        this.errors.push(error)
      })
      .finally(() => {
        this.pending.delete(promise)
      })
    this.pending.add(promise)
  }

  private async sendEvents(events: EventEnvelope[]): Promise<void> {
    if (events.length === 1) {
      await this.transport.send(events[0]!)
      return
    }
    if (this.transport.sendBatch) {
      await this.transport.sendBatch(events)
      return
    }
    for (const event of events) {
      await this.transport.send(event)
    }
  }

  private messageOperation(operation: 'publish' | 'consume', data: MessageOperation): void {
    this.write('span', {
      ...normalizeCommon(withoutKeys(data, MESSAGE_KEYS)),
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
  ): void {
    this.write('metric', {
      ...defaults,
      ...normalizeCommon(data),
      metric_name: name,
      metric_type: type,
      metric_value: value
    })
  }

  private write(eventType: string, data: JsonObject): void {
    this.emit({ data: { ...data, event_type: eventType } })
  }
}

export function createNanotrace(options: NanotraceOptions): Nanotrace {
  return new Nanotrace(options)
}

const SEVERITY_NUMBERS: Record<LogLevel, number> = {
  debug: 5,
  error: 17,
  info: 9,
  warn: 13
}

function severityNumber(level: LogLevel): number {
  return SEVERITY_NUMBERS[level]
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

function httpStatusFields(statusCode: number | undefined): JsonObject {
  if (!statusCode) return {}
  return {
    'http.status_code': statusCode,
    'http.response.status_code': statusCode
  }
}

function isServerError(statusCode: number | undefined): 0 | 1 {
  return statusCode && statusCode >= 500 ? 1 : 0
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
