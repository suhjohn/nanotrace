#!/usr/bin/env node

const facetFields = [
  'tenant_id',
  'service',
  'service.namespace',
  'service_version',
  'event_type',
  'signal',
  'environment',
  'host.name',
  'name',
  'scope_name',
  'scope_version',
  'span_kind',
  'span_status_code',
  'http.method',
  'http.route',
  'http.request.method',
  'exception.type',
  'severity_text',
  'logger.name',
  'thread.name',
  'db.system',
  'db.operation',
  'rpc.system',
  'rpc.service',
  'rpc.method',
  'messaging.system',
  'messaging.destination.name',
  'messaging.operation.name',
  'metric_name',
  'metric_type',
  'metric_unit',
  'metric.temporality',
  'page_path',
  'screen_name',
  'utm_source',
  'utm_medium',
  'utm_campaign',
  'utm_term',
  'utm_content',
  'country',
  'region',
  'city',
  'continent',
  'device_type',
  'device_brand',
  'device_manufacturer',
  'device_model',
  'browser',
  'browser_version',
  'os',
  'os_version',
  'app_version',
  'locale',
  'timezone',
  'currency',
  'revenue_type',
  'experiment_id',
  'variant',
  'feature_flag'
]

const lookupFields = [
  'event_id',
  'trace_id',
  'span_id',
  'parent_span_id',
  'service.instance.id',
  'host.id',
  'trace_state',
  'url.path',
  'url.full',
  'user_agent.original',
  'client.ip',
  'server.address',
  'exception.message',
  'exception.stacktrace',
  'body',
  'db.statement',
  'user_id',
  'anonymous_id',
  'device_id',
  'session_id',
  'account_id',
  'page_url',
  'page_title',
  'referrer',
  'ip',
  'product_id'
]

const boolFacetFields = ['is_error', 'metric.is_monotonic']

const numericMeasures = [
  { path: 'duration_ms', unit: 'ms' },
  { path: 'http.status_code', unit: 'status_code' },
  { path: 'http.response.status_code', unit: 'status_code' },
  { path: 'client.port', unit: 'port' },
  { path: 'server.port', unit: 'port' },
  { path: 'severity_number', unit: 'severity' },
  { path: 'metric_value', unit: 'metric_unit' },
  { path: 'count', unit: 'count' },
  { path: 'sum', unit: 'sum' },
  { path: 'metric_count', unit: 'count' },
  { path: 'metric_sum', unit: 'sum' },
  { path: 'metric_min', unit: 'metric_unit' },
  { path: 'metric_max', unit: 'metric_unit' },
  { path: 'location_lat', unit: 'latitude' },
  { path: 'location_lng', unit: 'longitude' },
  { path: 'screen_height', unit: 'px' },
  { path: 'screen_width', unit: 'px' },
  { path: 'viewport_height', unit: 'px' },
  { path: 'viewport_width', unit: 'px' },
  { path: 'screen_dpi', unit: 'dpi' },
  { path: 'revenue', unit: 'currency' },
  { path: 'price', unit: 'currency' },
  { path: 'quantity', unit: 'count' }
]

const rollupDimensions = [
  'service',
  'environment',
  'signal',
  'event_type',
  'name',
  'host.name',
  'span_kind',
  'span_status_code',
  'metric_name',
  'metric_type',
  'page_path',
  'screen_name',
  'country',
  'device_type',
  'browser',
  'os',
  'currency',
  'experiment_id',
  'variant',
  'feature_flag'
]

const stateEntities = [
  { entityType: 'trace', entityIdPath: 'trace_id' },
  { entityType: 'session', entityIdPath: 'session_id' },
  { entityType: 'user', entityIdPath: 'user_id' },
  { entityType: 'account', entityIdPath: 'account_id' },
  { entityType: 'device', entityIdPath: 'device_id' },
  { entityType: 'service_instance', entityIdPath: 'service.instance.id' },
  { entityType: 'host', entityIdPath: 'host.id' }
]

const statePaths = [
  'environment',
  'service_version',
  'span_status_code',
  'severity_text',
  'page_path',
  'screen_name',
  'country',
  'region',
  'device_type',
  'browser',
  'os',
  'app_version',
  'currency',
  'revenue_type',
  'experiment_id',
  'variant',
  'feature_flag'
]

const sequenceEntities = [
  { entityIdPath: 'user_id', name: 'User' },
  { entityIdPath: 'session_id', name: 'Session' },
  { entityIdPath: 'account_id', name: 'Account' },
  { entityIdPath: 'device_id', name: 'Device' },
  { entityIdPath: 'trace_id', name: 'Trace' }
]

const summaryReports = [
  ['Event Volume Over Time', { bucket: '5m', group_by: ['signal'], metric: 'count', source: 'event_density_1s' }],
  ['Error Rate Over Time', { bucket: '5m', group_by: ['service'], metric: 'error_rate', source: 'event_rollups_5m' }],
  ['Top Services', { bucket: '5m', group_by: ['service'], metric: 'count', source: 'event_rollups_5m' }],
  ['Top Event Types', { bucket: '5m', group_by: ['event_type'], metric: 'count', source: 'event_rollups_5m' }],
  ['Top Names', { bucket: '5m', group_by: ['name'], metric: 'count', source: 'event_rollups_5m' }],
  ['Top Environments', { bucket: '5m', group_by: ['environment'], metric: 'count', source: 'event_rollups_5m' }],
  ['Global Density Histogram', { bucket: '1s', group_by: [], metric: 'count', source: 'event_density_1s' }],
  ['Error Density Histogram', { bucket: '1s', group_by: [], metric: 'error_count', source: 'event_density_1s' }],
  ['Trace Count Over Time', { bucket: '5m', group_by: ['service'], metric: 'trace_count', source: 'trace_summaries' }],
  ['Erroring Traces Over Time', { bucket: '5m', group_by: ['service'], metric: 'error_count', source: 'trace_summaries' }],
  ['Span Duration P95 By Service', { bucket: '5m', group_by: ['service'], metric: 'duration_ms.p95', source: 'measure_rollups' }],
  ['Span Duration P99 By Service', { bucket: '5m', group_by: ['service'], metric: 'duration_ms.p99', source: 'measure_rollups' }],
  ['Span Duration P95 By Name', { bucket: '5m', group_by: ['name'], metric: 'duration_ms.p95', source: 'measure_rollups' }],
  ['Status Code Distribution', { bucket: '5m', group_by: ['http.response.status_code'], metric: 'count', source: 'event_index' }],
  ['Request Volume By Route', { bucket: '5m', group_by: ['http.route'], metric: 'count', source: 'event_rollups_5m' }],
  ['Request Volume By Method', { bucket: '5m', group_by: ['http.method'], metric: 'count', source: 'event_rollups_5m' }],
  ['Error Rate By Route', { bucket: '5m', group_by: ['http.route'], metric: 'error_rate', source: 'event_rollups_5m' }],
  ['Log Volume By Severity', { bucket: '5m', group_by: ['severity_text'], metric: 'count', source: 'event_rollups_5m' }],
  ['Log Volume By Logger', { bucket: '5m', group_by: ['logger.name'], metric: 'count', source: 'event_rollups_5m' }],
  ['Metric Volume By Metric Name', { bucket: '5m', group_by: ['metric_name'], metric: 'count', source: 'event_rollups_5m' }],
  ['Metric Value P95 By Metric Name', { bucket: '5m', group_by: ['metric_name'], metric: 'metric_value.p95', source: 'measure_rollups' }],
  ['Active Users Over Time', { bucket: '1h', group_by: ['user_id'], metric: 'count_distinct', source: 'field_index' }],
  ['Active Sessions Over Time', { bucket: '1h', group_by: ['session_id'], metric: 'count_distinct', source: 'field_index' }],
  ['Events By Page Path', { bucket: '5m', group_by: ['page_path'], metric: 'count', source: 'event_rollups_5m' }],
  ['Events By Screen Name', { bucket: '5m', group_by: ['screen_name'], metric: 'count', source: 'event_rollups_5m' }],
  ['Revenue Over Time', { bucket: '1h', group_by: ['currency'], metric: 'revenue.sum', source: 'measure_rollups' }],
  ['Revenue By Product', { bucket: '1h', group_by: ['product_id'], metric: 'revenue.sum', source: 'event_measures' }],
  ['Variant Split', { bucket: '1h', group_by: ['experiment_id', 'variant'], metric: 'count', source: 'event_rollups_5m' }],
  ['Error Rate By Variant', { bucket: '1h', group_by: ['experiment_id', 'variant'], metric: 'error_rate', source: 'event_rollups_5m' }],
  ['Volume By Country', { bucket: '1h', group_by: ['country'], metric: 'count', source: 'event_rollups_5m' }],
  ['Volume By Device Type', { bucket: '1h', group_by: ['device_type'], metric: 'count', source: 'event_rollups_5m' }],
  ['Volume By Browser', { bucket: '1h', group_by: ['browser'], metric: 'count', source: 'event_rollups_5m' }],
  ['Volume By OS', { bucket: '1h', group_by: ['os'], metric: 'count', source: 'event_rollups_5m' }]
]

const sequenceReports = sequenceEntities.map(entity => ({
  config: {
    entity_id_path: entity.entityIdPath,
    source: 'event_index',
    steps: ['event_type', 'name'],
    window: '7d'
  },
  kind: 'sequence',
  name: `${entity.name} Default Sequence`
}))

function buildManifest() {
  const definitions = []

  for (const path of facetFields) {
    definitions.push({
      name: path,
      kind: 'field',
      mode: 'facet',
      config: { path, value_type: 'string' }
    })
  }

  for (const path of lookupFields) {
    definitions.push({
      name: path,
      kind: 'field',
      mode: 'lookup',
      config: { path, value_type: 'string' }
    })
  }

  for (const path of boolFacetFields) {
    definitions.push({
      name: path,
      kind: 'field',
      mode: 'facet',
      config: { path, value_type: 'bool' }
    })
  }

  for (const measure of numericMeasures) {
    definitions.push({
      name: measure.path,
      kind: 'measure',
      mode: 'measure',
      config: { path: measure.path, unit: measure.unit }
    })
  }

  for (const measure of numericMeasures) {
    for (const dimension of rollupDimensions) {
      definitions.push({
        name: `${measure.path}.by.${dimension}`,
        kind: 'rollup',
        mode: 'measure_rollup',
        config: {
          path: measure.path,
          unit: measure.unit,
          dimension,
          aggregates: ['count', 'sum', 'avg', 'min', 'max', 'p50', 'p95', 'p99']
        }
      })
    }
  }

  for (const entity of stateEntities) {
    for (const path of statePaths) {
      definitions.push({
        name: `${entity.entityType}.${path}`,
        kind: 'state',
        mode: 'state_transition',
        config: {
          path,
          entity_type: entity.entityType,
          entity_id_path: entity.entityIdPath,
          value_type: 'string'
        }
      })
    }
  }

  const reports = [
    ...summaryReports.map(([name, config]) => ({ name, kind: 'summary', enabled: true, config })),
    ...sequenceReports.map(report => ({ ...report, enabled: true }))
  ]

  return {
    name: 'maximal-events-pack',
    source_table: 'observatory.events',
    version: 1,
    definitions: dedupeBy(definitions, item => `${item.kind}\u0000${item.mode}\u0000${item.name}`),
    reports: dedupeBy(reports, item => `${item.kind}\u0000${item.name}`)
  }
}

function dedupeBy(items, keyFn) {
  const seen = new Set()
  const result = []
  for (const item of items) {
    const key = keyFn(item)
    if (seen.has(key)) continue
    seen.add(key)
    result.push(item)
  }
  return result
}

function parseArgs(argv) {
  const flags = new Set()
  const values = new Map()
  for (let index = 2; index < argv.length; index += 1) {
    const arg = argv[index]
    if (!arg.startsWith('--')) continue
    const [flag, inlineValue] = arg.split('=', 2)
    if (inlineValue !== undefined) {
      values.set(flag, inlineValue)
      continue
    }
    const next = argv[index + 1]
    if (next && !next.startsWith('--')) {
      values.set(flag, next)
      index += 1
    } else {
      flags.add(flag)
    }
  }
  return { flags, values }
}

function usage() {
  return [
    'Usage:',
    '  node scripts/bootstrap-maximal-events-pack.mjs --json',
    '  node scripts/bootstrap-maximal-events-pack.mjs --summary',
    '  NANOTRACE_API_KEY=... node scripts/bootstrap-maximal-events-pack.mjs --apply [--base-url http://localhost:41233]',
    '',
    'Flags:',
    '  --json       Print the full bootstrap manifest as JSON.',
    '  --summary    Print pack counts. Default when no flag is provided.',
    '  --apply      Create missing definitions and reports via the server API.',
    '  --base-url   Base Nanotrace URL. Defaults to NANOTRACE_API_BASE_URL or http://localhost:41233.'
  ].join('\n')
}

async function main() {
  const args = parseArgs(process.argv)
  const manifest = buildManifest()

  if (args.flags.has('--help')) {
    console.log(usage())
    return
  }

  if (args.flags.has('--json')) {
    console.log(JSON.stringify(manifest, null, 2))
    return
  }

  if (args.flags.has('--apply')) {
    const baseUrl = String(args.values.get('--base-url') || process.env.NANOTRACE_API_BASE_URL || 'http://localhost:41233').replace(/\/+$/, '')
    const apiKey = String(process.env.NANOTRACE_API_KEY || '').trim()
    if (!apiKey) {
      throw new Error('NANOTRACE_API_KEY is required for --apply')
    }
    await applyManifest({ apiKey, baseUrl, manifest })
    return
  }

  console.log(JSON.stringify({
    definitions: manifest.definitions.length,
    fields: manifest.definitions.filter(item => item.kind === 'field').length,
    measures: manifest.definitions.filter(item => item.kind === 'measure').length,
    reports: manifest.reports.length,
    rollups: manifest.definitions.filter(item => item.kind === 'rollup').length,
    states: manifest.definitions.filter(item => item.kind === 'state').length
  }, null, 2))
}

async function applyManifest({ apiKey, baseUrl, manifest }) {
  const headers = {
    'authorization': `Bearer ${apiKey}`,
    'content-type': 'application/json'
  }

  const existingDefinitions = await getJson(`${baseUrl}/v1/definitions`, headers)
  const existingReports = await getJson(`${baseUrl}/v1/reports`, headers)
  const definitionKeys = new Set(
    (existingDefinitions.definitions ?? []).map(item => `${item.kind}\u0000${item.mode}\u0000${item.name}`)
  )
  const reportKeys = new Set(
    (existingReports.reports ?? []).map(item => `${item.kind}\u0000${item.name}`)
  )

  let createdDefinitions = 0
  for (const definition of manifest.definitions) {
    const key = `${definition.kind}\u0000${definition.mode}\u0000${definition.name}`
    if (definitionKeys.has(key)) continue
    await postJson(`${baseUrl}/v1/definitions`, headers, definition)
    definitionKeys.add(key)
    createdDefinitions += 1
  }

  let createdReports = 0
  for (const report of manifest.reports) {
    const key = `${report.kind}\u0000${report.name}`
    if (reportKeys.has(key)) continue
    await postJson(`${baseUrl}/v1/reports`, headers, report)
    reportKeys.add(key)
    createdReports += 1
  }

  console.log(JSON.stringify({
    created_definitions: createdDefinitions,
    created_reports: createdReports,
    skipped_definitions: manifest.definitions.length - createdDefinitions,
    skipped_reports: manifest.reports.length - createdReports
  }, null, 2))
}

async function getJson(url, headers) {
  const response = await fetch(url, { headers, method: 'GET' })
  if (!response.ok) {
    throw new Error(`${response.status} ${response.statusText}: ${await response.text()}`)
  }
  return response.json()
}

async function postJson(url, headers, body) {
  const response = await fetch(url, {
    headers,
    method: 'POST',
    body: JSON.stringify(body)
  })
  if (!response.ok) {
    throw new Error(`${response.status} ${response.statusText}: ${await response.text()}`)
  }
  return response.json()
}

main().catch(error => {
  console.error(error instanceof Error ? error.message : String(error))
  process.exitCode = 1
})
