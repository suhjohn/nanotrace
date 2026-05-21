#!/usr/bin/env node
import { spawn } from 'node:child_process'

const ingestUrl = env('NANOTRACE_INGEST_URL', 'http://localhost:18473').replace(/\/+$/, '')
const apiKey = env('NANOTRACE_API_KEY', 'ntak_dev')
const clickhouseUrl = env('CLICKHOUSE_URL', 'http://localhost:18123')
const clickhouseUser = env('CLICKHOUSE_USER', 'default')
const clickhousePassword = env('CLICKHOUSE_PASSWORD', 'nanotrace')
const database = env('CLICKHOUSE_DATABASE', 'observatory')
const eventsTable = env('CLICKHOUSE_TABLE', 'events')
const runId = env('NANOTRACE_LOADTEST_RUN_ID', `materialization-${Date.now()}`)
const profile = env('NANOTRACE_LOADTEST_PROFILE', 'metrics')
const totalEvents = Number(env('NANOTRACE_LOADTEST_TOTAL_EVENTS', '128'))
const batchSizes = env('NANOTRACE_LOADTEST_BATCH_SIZES', '128')
const startRps = env('NANOTRACE_LOADTEST_START_RPS', '1')
const maxRps = env('NANOTRACE_LOADTEST_MAX_RPS', '1')
const waitMs = Number(env('NANOTRACE_MATERIALIZATION_WAIT_MS', '300000'))
const pollMs = Number(env('NANOTRACE_MATERIALIZATION_POLL_MS', '5000'))
const expectTrace = env('NANOTRACE_LOADTEST_EXPECT_TRACE', profile === 'metrics' ? '0' : '1') === '1'
const reportId = `materialization_metrics_${runId.replace(/[^A-Za-z0-9_]/g, '_')}`
const traceReportId = `trace_summary_${runId.replace(/[^A-Za-z0-9_]/g, '_')}`
const sequenceId = `metric_counter_to_gauge_${runId.replace(/[^A-Za-z0-9_]/g, '_')}`
const cohortId = `metric_gauge_names_${runId.replace(/[^A-Za-z0-9_]/g, '_')}`
const retentionId = `metric_gauge_retention_${runId.replace(/[^A-Za-z0-9_]/g, '_')}`

await seedSdkDefinitions()
await seedReportDefinition()
await seedTraceReportDefinition()
await seedSequenceDefinition()
await seedCohortDefinition()
await seedRetentionDefinition()
await runLoadtest()
await waitForMaterialization()

console.log(`materializationValidation=ok runId=${runId} profile=${profile} totalEvents=${totalEvents}`)

async function seedSdkDefinitions () {
  const response = await fetch(`${ingestUrl}/v1/definitions/sdk-defaults`, {
    method: 'POST',
    headers: {
      authorization: `Bearer ${apiKey}`,
      'content-type': 'application/json'
    }
  })
  const text = await response.text()
  if (!response.ok) {
    throw new Error(`POST /v1/definitions/sdk-defaults failed: ${response.status} ${text}`)
  }
  console.log(`seedSdkDefinitions=ok ${compact(text)}`)
}

async function seedReportDefinition () {
  const response = await fetch(`${ingestUrl}/v1/definitions`, {
    method: 'POST',
    headers: {
      authorization: `Bearer ${apiKey}`,
      'content-type': 'application/json'
    },
    body: JSON.stringify({
      name: reportId,
      kind: 'report',
      mode: 'summary',
      config: {
        match: {
          all: [
            { path: 'event_type', op: 'eq', value: 'metric' },
            { path: '_loadtest.run_id', op: 'eq', value: runId }
          ]
        },
        outputs: [
          {
            target: 'report_results',
            report_id: reportId,
            dimensions: [
              { name: 'metric_type', value: { path: 'metric_type' } }
            ],
            metrics: [
              { name: 'events', op: 'count' },
              { name: 'value_sum', op: 'sum', value: { path: 'metric_value' } }
            ],
            bucket_seconds: 60
          }
        ]
      }
    })
  })
  const text = await response.text()
  if (!response.ok) {
    throw new Error(`POST /v1/definitions report failed: ${response.status} ${text}`)
  }
  console.log(`seedReportDefinition=ok reportId=${reportId} ${compact(text)}`)
}

async function seedTraceReportDefinition () {
  const response = await fetch(`${ingestUrl}/v1/definitions`, {
    method: 'POST',
    headers: {
      authorization: `Bearer ${apiKey}`,
      'content-type': 'application/json'
    },
    body: JSON.stringify({
      name: traceReportId,
      kind: 'report',
      mode: 'trace_summary',
      config: {
        match: {
          all: [
            { path: 'trace_id', op: 'exists' },
            { path: '_loadtest.run_id', op: 'eq', value: runId }
          ]
        },
        outputs: [
          {
            target: 'report_results',
            report_id: traceReportId,
            dimensions: [
              { name: 'service', value: { path: 'service' } }
            ],
            bucket_seconds: 60
          }
        ]
      }
    })
  })
  const text = await response.text()
  if (!response.ok) {
    throw new Error(`POST /v1/definitions trace report failed: ${response.status} ${text}`)
  }
  console.log(`seedTraceReportDefinition=ok traceReportId=${traceReportId} ${compact(text)}`)
}

async function seedCohortDefinition () {
  const response = await fetch(`${ingestUrl}/v1/definitions`, {
    method: 'POST',
    headers: {
      authorization: `Bearer ${apiKey}`,
      'content-type': 'application/json'
    },
    body: JSON.stringify({
      name: cohortId,
      kind: 'cohort',
      mode: 'membership',
      config: {
        match: {
          all: [
            { path: 'event_type', op: 'eq', value: 'metric' },
            { path: '_loadtest.run_id', op: 'eq', value: runId },
            { path: 'metric_type', op: 'eq', value: 'gauge' }
          ]
        },
        outputs: [
          {
            target: 'cohort_memberships',
            cohort_id: cohortId,
            entity_type: 'metric_name',
            entity_id: { path: 'metric_name' }
          }
        ]
      }
    })
  })
  const text = await response.text()
  if (!response.ok) {
    throw new Error(`POST /v1/definitions cohort failed: ${response.status} ${text}`)
  }
  console.log(`seedCohortDefinition=ok cohortId=${cohortId} ${compact(text)}`)
}

async function seedSequenceDefinition () {
  const response = await fetch(`${ingestUrl}/v1/definitions`, {
    method: 'POST',
    headers: {
      authorization: `Bearer ${apiKey}`,
      'content-type': 'application/json'
    },
    body: JSON.stringify({
      name: sequenceId,
      kind: 'sequence',
      mode: 'funnel',
      config: {
        match: {
          all: [
            { path: 'event_type', op: 'eq', value: 'metric' },
            { path: '_loadtest.run_id', op: 'eq', value: runId }
          ]
        },
        outputs: [
          {
            target: 'sequence_report_results',
            report_id: sequenceId,
            entity_id: { path: '_loadtest.run_id' },
            dimensions: [
              { name: 'run_id', value: { path: '_loadtest.run_id' } }
            ],
            steps: [
              {
                name: 'counter',
                match: { all: [{ path: 'metric_type', op: 'eq', value: 'counter' }] }
              },
              {
                name: 'gauge',
                match: { all: [{ path: 'metric_type', op: 'eq', value: 'gauge' }] }
              }
            ],
            bucket_seconds: 60
          }
        ]
      }
    })
  })
  const text = await response.text()
  if (!response.ok) {
    throw new Error(`POST /v1/definitions sequence failed: ${response.status} ${text}`)
  }
  console.log(`seedSequenceDefinition=ok sequenceId=${sequenceId} ${compact(text)}`)
}

async function seedRetentionDefinition () {
  const response = await fetch(`${ingestUrl}/v1/definitions`, {
    method: 'POST',
    headers: {
      authorization: `Bearer ${apiKey}`,
      'content-type': 'application/json'
    },
    body: JSON.stringify({
      name: retentionId,
      kind: 'report',
      mode: 'retention',
      config: {
        match: {
          all: [
            { path: 'event_type', op: 'eq', value: 'metric' },
            { path: '_loadtest.run_id', op: 'eq', value: runId },
            { path: 'metric_type', op: 'eq', value: 'gauge' }
          ]
        },
        outputs: [
          {
            target: 'report_results',
            report_id: retentionId,
            cohort_id: cohortId,
            entity_type: 'metric_name',
            entity_id: { path: 'metric_name' },
            dimensions: [
              { name: 'run_id', value: { path: '_loadtest.run_id' } }
            ],
            retention_bucket_seconds: 60
          }
        ]
      }
    })
  })
  const text = await response.text()
  if (!response.ok) {
    throw new Error(`POST /v1/definitions retention failed: ${response.status} ${text}`)
  }
  console.log(`seedRetentionDefinition=ok retentionId=${retentionId} ${compact(text)}`)
}

async function runLoadtest () {
  await spawnChecked('cargo', ['run', '--release', '-p', 'nanotrace-loadtest', '--'], {
    ...process.env,
    NANOTRACE_INGEST_URL: ingestUrl,
    NANOTRACE_API_KEY: apiKey,
    NANOTRACE_LOADTEST_RUN_ID: runId,
    NANOTRACE_LOADTEST_PROFILE: profile,
    NANOTRACE_LOADTEST_TOTAL_EVENTS: String(totalEvents),
    NANOTRACE_LOADTEST_BATCH_SIZES: batchSizes,
    NANOTRACE_LOADTEST_START_RPS: startRps,
    NANOTRACE_LOADTEST_MAX_RPS: maxRps,
    CLICKHOUSE_URL: clickhouseUrl,
    CLICKHOUSE_USER: clickhouseUser,
    CLICKHOUSE_PASSWORD: clickhousePassword,
    CLICKHOUSE_DATABASE: database,
    CLICKHOUSE_TABLE: eventsTable
  })
}

async function waitForMaterialization () {
  const deadline = Date.now() + waitMs
  let last = null
  while (Date.now() <= deadline) {
    last = await materializationStats()
    console.log(
      `materializationStats raw=${last.rawEvents} rawMetricGroups=${last.rawMetricGroups} counters=${last.counterRollups} gauges=${last.gaugeRollups} histograms=${last.histogramRollups} reports=${last.reportResults} traces=${last.traceResults} retentions=${last.retentionResults} sequences=${last.sequenceResults} cohorts=${last.cohortMemberships} watermarks=${JSON.stringify(last.watermarks)}`
    )
    if (
      last.rawEvents >= totalEvents &&
      last.rawMetricGroups > 0 &&
      last.counterRollups > 0 &&
      last.gaugeRollups > 0 &&
      last.histogramRollups > 0 &&
      last.reportResults > 0 &&
      (!expectTrace || last.traceResults > 0) &&
      last.retentionResults > 0 &&
      last.sequenceResults > 0 &&
      last.cohortMemberships > 0
    ) {
      return
    }
    await sleep(pollMs)
  }
  throw new Error(`materialization did not catch up within ${waitMs}ms; last=${JSON.stringify(last)}`)
}

async function materializationStats () {
  const rawEvents = await scalar(`
SELECT count()
FROM ${q(database)}.${q(eventsTable)}
WHERE getSubcolumn(data, '_loadtest.run_id') = ${s(runId)}
`)
  const rawMetricGroups = await scalar(`
SELECT count()
FROM (
  SELECT ifNull(toString(data.metric_type), '') AS metric_type, count() AS events
  FROM ${q(database)}.${q(eventsTable)}
  WHERE getSubcolumn(data, '_loadtest.run_id') = ${s(runId)}
    AND ifNull(toString(data.metric_type), '') != ''
  GROUP BY metric_type
)
`)
  const counterRollups = await scalar(`
SELECT count()
FROM ${q(database)}.${q('counter_rollups')}
WHERE definition_id = 'sdk_metric_default_v1'
  AND ifNull(toString(getSubcolumn(dimensions, 'loadtest_run_id')), '') = ${s(runId)}
`)
  const gaugeRollups = await scalar(`
SELECT count()
FROM ${q(database)}.${q('gauge_rollups')}
WHERE definition_id = 'sdk_metric_default_v1'
  AND ifNull(toString(getSubcolumn(dimensions, 'loadtest_run_id')), '') = ${s(runId)}
`)
  const histogramRollups = await scalar(`
SELECT count()
FROM ${q(database)}.${q('histogram_rollups')}
WHERE definition_id = 'sdk_metric_default_v1'
  AND ifNull(toString(getSubcolumn(dimensions, 'loadtest_run_id')), '') = ${s(runId)}
`)
  const reportResults = await scalar(`
SELECT count()
FROM ${q(database)}.${q('report_results')}
WHERE report_id = ${s(reportId)}
`)
  const traceResults = await scalar(`
SELECT count()
FROM ${q(database)}.${q('report_results')}
WHERE report_id = ${s(traceReportId)}
`)
  const sequenceResults = await scalar(`
SELECT count()
FROM ${q(database)}.${q('sequence_report_results')}
WHERE report_id = ${s(sequenceId)}
`)
  const retentionResults = await scalar(`
SELECT count()
FROM ${q(database)}.${q('report_results')}
WHERE report_id = ${s(retentionId)}
`)
  const cohortMemberships = await scalar(`
SELECT count()
FROM ${q(database)}.${q('cohort_memberships')}
WHERE cohort_id = ${s(cohortId)}
`)
  const watermarks = await rows(`
SELECT serving_table, max(source_sequence_number) AS source_sequence_number
FROM ${q(database)}.${q('serving_watermarks')}
WHERE serving_table IN ('events', 'field_index', 'event_measures', 'counter_rollups', 'gauge_rollups', 'histogram_rollups', 'entity_state_updates', 'report_results', 'sequence_report_results', 'cohort_memberships')
GROUP BY serving_table
FORMAT JSON
`)
  return {
    rawEvents: Number(rawEvents),
    rawMetricGroups: Number(rawMetricGroups),
    counterRollups: Number(counterRollups),
    gaugeRollups: Number(gaugeRollups),
    histogramRollups: Number(histogramRollups),
    reportResults: Number(reportResults),
    traceResults: Number(traceResults),
    retentionResults: Number(retentionResults),
    sequenceResults: Number(sequenceResults),
    cohortMemberships: Number(cohortMemberships),
    watermarks: Object.fromEntries(
      watermarks.map(row => [row.serving_table, Number(row.source_sequence_number)])
    )
  }
}

async function scalar (sql) {
  const text = await clickhouse(sql)
  return text.trim()
}

async function rows (sql) {
  const text = await clickhouse(sql)
  const parsed = JSON.parse(text)
  return parsed.data ?? []
}

async function clickhouse (sql) {
  const response = await fetch(clickhouseUrl, {
    method: 'POST',
    headers: {
      authorization: `Basic ${Buffer.from(`${clickhouseUser}:${clickhousePassword}`).toString('base64')}`,
      'content-type': 'text/plain; charset=utf-8'
    },
    body: sql
  })
  const text = await response.text()
  if (!response.ok) {
    throw new Error(`ClickHouse query failed: ${response.status} ${text}\nSQL:\n${sql}`)
  }
  return text
}

function spawnChecked (command, args, env) {
  return new Promise((resolve, reject) => {
    const child = spawn(command, args, {
      env,
      stdio: 'inherit'
    })
    child.on('error', reject)
    child.on('exit', code => {
      if (code === 0) {
        resolve()
      } else {
        reject(new Error(`${command} ${args.join(' ')} exited with ${code}`))
      }
    })
  })
}

function env (key, fallback) {
  const value = process.env[key]?.trim()
  return value || fallback
}

function q (identifier) {
  if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(identifier)) {
    throw new Error(`invalid ClickHouse identifier: ${identifier}`)
  }
  return `\`${identifier}\``
}

function s (value) {
  return `'${String(value).replaceAll("'", "''")}'`
}

function compact (value) {
  return value.replace(/\s+/g, ' ').trim()
}

function sleep (ms) {
  return new Promise(resolve => setTimeout(resolve, ms))
}
