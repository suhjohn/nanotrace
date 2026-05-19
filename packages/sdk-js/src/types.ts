export type Json =
  | null
  | boolean
  | number
  | string
  | Json[]
  | { [key: string]: Json }

export type JsonObject = { [key: string]: Json }

export type MaybePromise<T> = T | PromiseLike<T>

export type EventEnvelope = {
  event_id?: string
  timestamp?: string
  observed_timestamp?: string
  data: JsonObject
}

export type Transport = {
  send(event: EventEnvelope): void | Promise<void>
}

export type CommonFields = {
  tenantId?: string
  service?: string
  serviceNamespace?: string
  serviceInstanceId?: string
  serviceVersion?: string
  environment?: string
  hostName?: string
  hostId?: string
  traceId?: string
  spanId?: string
  parentSpanId?: string
  spanKind?: string
  spanStatusCode?: string
  spanStatusMessage?: string
  isError?: boolean | number
  userId?: string
  anonymousId?: string
  sessionId?: string
  accountId?: string
  groupId?: string
  organizationId?: string
  requestId?: string
  threadId?: string
  conversationId?: string
  loggerName?: string
  threadName?: string
  metricUnit?: string
  metricTemporality?: string
  metricIsMonotonic?: boolean
  llmModel?: string
  llmProvider?: string
  toolName?: string
  processorName?: string
  [key: string]: Json | undefined
}

export type LogLevel = 'debug' | 'info' | 'warn' | 'error'

export type SpanStatus = 'ok' | 'error' | 'unset'

export type SpanOptions = CommonFields & {
  kind?: 'internal' | 'server' | 'client' | 'producer' | 'consumer'
}

export type SpanRecord = SpanOptions & {
  name: string
  startTime: Date | string
  endTime: Date | string
  durationMs?: number
  statusCode?: SpanStatus
}

export type SpanHandle = {
  traceId: string
  spanId: string
  set(key: string, value: Json): void
  event(name: string, data?: CommonFields): void
  end(data?: CommonFields): void
}

export type HttpServerRequest = CommonFields & {
  method: string
  route?: string
  path?: string
  url?: string
  statusCode?: number
  durationMs: number
}

export type HttpClientRequest = CommonFields & {
  method: string
  url: string
  statusCode?: number
  durationMs: number
}

export type DbQuery = CommonFields & {
  system: string
  operation?: string
  statement?: string
  durationMs: number
}

export type RpcCall = CommonFields & {
  system: string
  service: string
  method: string
  durationMs: number
}

export type MessageOperation = CommonFields & {
  system: string
  destination: string
  durationMs?: number
}

export type PageEvent = CommonFields & {
  name?: string
  url?: string
  path?: string
  title?: string
  referrer?: string
}

export type RevenueEvent = CommonFields & {
  revenue: number
  currency?: string
  productId?: string
  quantity?: number
  price?: number
  revenueType?: string
}

export type ExperimentViewedEvent = CommonFields & {
  experimentId: string
  variant: string
}

export type FeatureFlagEvent = CommonFields & {
  featureFlag: string
  variant?: string
  value?: Json
}
