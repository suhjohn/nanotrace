#!/usr/bin/env node
import { createInterface } from 'node:readline'
import { createReadStream, readFileSync } from 'node:fs'
import path from 'node:path'
import { setTimeout as sleep } from 'node:timers/promises'

const root = path.resolve(new URL('..', import.meta.url).pathname)
loadEnvFile(optionValue('--env') ?? process.env.NANOTRACE_ENV_FILE ?? '.env')

const input = path.resolve(root, optionValue('--input') ?? process.env.NANOTRACE_BACKFILL_INPUT ?? 'fixtures/generated/llm_logs_7d_10m.ndjson')
const url = trimTrailingSlash(optionValue('--url') ?? process.env.NANOTRACE_INGEST_URL ?? process.env.NANOTRACE_URL ?? 'http://nanotrace-prod-alb-25e097e-2002277432.us-west-1.elb.amazonaws.com')
const apiKey = requiredEnv('NANOTRACE_API_KEY')
const target = numberOption('--count', numberEnv('NANOTRACE_BACKFILL_COUNT', Number.POSITIVE_INFINITY))
const skip = numberOption('--skip', numberEnv('NANOTRACE_BACKFILL_SKIP', 0))
const batchSize = numberOption('--batch-size', numberEnv('NANOTRACE_BACKFILL_BATCH_SIZE', 500))
const concurrency = numberOption('--concurrency', numberEnv('NANOTRACE_BACKFILL_CONCURRENCY', 8))
const progressEvery = numberOption('--progress-every', numberEnv('NANOTRACE_BACKFILL_PROGRESS_EVERY', 25_000))
const retryLimit = numberOption('--retries', numberEnv('NANOTRACE_BACKFILL_RETRIES', 5))

if (!Number.isFinite(target) && args().includes('--count')) {
  throw new Error('--count must be finite')
}
if (!Number.isSafeInteger(batchSize) || batchSize <= 0) throw new Error('--batch-size must be positive')
if (!Number.isSafeInteger(concurrency) || concurrency <= 0) throw new Error('--concurrency must be positive')

let posted = 0
let queued = 0
let read = 0
let inFlight = 0
let failed = false
const pending = new Set()
const started = Date.now()

console.log(`input=${input}`)
console.log(`url=${url}`)
console.log(`batchSize=${batchSize}`)
console.log(`concurrency=${concurrency}`)
console.log(`skip=${skip}`)
console.log(`count=${Number.isFinite(target) ? target : 'all'}`)

const reader = createInterface({
  crlfDelay: Infinity,
  input: createReadStream(input, { encoding: 'utf8' })
})

let batch = []
for await (const line of reader) {
  if (failed) break
  if (!line) continue
  read += 1
  if (read <= skip) continue
  if (queued + batch.length >= target) break

  JSON.parse(line)
  batch.push(line)
  if (batch.length >= batchSize) {
    await enqueue(batch)
    batch = []
  }
}
if (!failed && batch.length > 0) {
  await enqueue(batch)
}
await Promise.all(pending)

if (failed) {
  process.exitCode = 1
} else {
  report(true)
}

async function enqueue(lines) {
  while (pending.size >= concurrency) {
    await Promise.race(pending)
  }
  queued += lines.length
  inFlight += lines.length
  const promise = postBatch(lines)
    .then(() => {
      posted += lines.length
      inFlight -= lines.length
      if (posted % progressEvery < lines.length) report(false)
    })
    .catch(error => {
      failed = true
      throw error
    })
    .finally(() => pending.delete(promise))
  pending.add(promise)
}

async function postBatch(lines) {
  const body = `[${lines.join(',')}]`
  for (let attempt = 0; attempt <= retryLimit; attempt += 1) {
    const response = await fetch(`${url}/v1/events`, {
      body,
      headers: {
        authorization: `Bearer ${apiKey}`,
        'content-type': 'application/json'
      },
      method: 'POST'
    }).catch(error => ({ error }))

    if (!response.error && response.ok) return

    const status = response.error ? 'request_error' : response.status
    const text = response.error ? response.error.message : await response.text()
    if (attempt >= retryLimit || (typeof status === 'number' && status < 500 && status !== 429)) {
      throw new Error(`POST /v1/events failed status=${status} attempt=${attempt + 1}: ${text}`)
    }
    await sleep(Math.min(30_000, 500 * 2 ** attempt))
  }
}

function report(final) {
  const seconds = Math.max(0.001, (Date.now() - started) / 1000)
  const rate = Math.round(posted / seconds)
  console.log(`${final ? 'complete' : 'progress'} posted=${posted} inFlight=${inFlight} rate=${rate}/s elapsed=${Math.round(seconds)}s`)
}

function loadEnvFile(file) {
  if (!file) return
  const resolved = path.resolve(root, file)
  let contents
  try {
    contents = readFileSync(resolved, 'utf8')
  } catch {
    return
  }
  for (const line of contents.split(/\r?\n/)) {
    const trimmed = line.trim()
    if (!trimmed || trimmed.startsWith('#')) continue
    const match = /^([A-Za-z_][A-Za-z0-9_]*)=(.*)$/.exec(trimmed)
    if (!match) continue
    const [, key, rawValue] = match
    if (process.env[key] !== undefined) continue
    process.env[key] = unquote(rawValue.trim())
  }
}

function unquote(value) {
  if ((value.startsWith('"') && value.endsWith('"')) || (value.startsWith("'") && value.endsWith("'"))) {
    return value.slice(1, -1)
  }
  return value
}

function requiredEnv(key) {
  const value = process.env[key]
  if (!value) throw new Error(`${key} is required`)
  return value
}

function numberEnv(key, fallback) {
  const value = process.env[key]
  if (!value) return fallback
  return positiveNumber(value, key)
}

function numberOption(name, fallback) {
  const value = optionValue(name)
  if (value === undefined) return fallback
  return positiveNumber(value, name)
}

function positiveNumber(value, label) {
  const parsed = Number(value)
  if (!Number.isFinite(parsed) || parsed < 0 || !Number.isSafeInteger(parsed)) {
    throw new Error(`${label} must be a non-negative integer`)
  }
  return parsed
}

function optionValue(name) {
  const index = args().indexOf(name)
  if (index === -1) return undefined
  const value = args()[index + 1]
  if (!value || value.startsWith('--')) throw new Error(`${name} requires a value`)
  return value
}

function args() {
  return process.argv.slice(2)
}

function trimTrailingSlash(value) {
  return value.replace(/\/+$/, '')
}
