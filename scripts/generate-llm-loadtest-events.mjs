#!/usr/bin/env node
import fs from 'node:fs'
import path from 'node:path'

const count = positiveInt(process.env.NANOTRACE_LLM_EVENT_COUNT ?? process.argv[2] ?? '100000')
const output = process.env.NANOTRACE_LLM_EVENT_OUTPUT ?? process.argv[3] ?? 'fixtures/generated/llm_logs_7d.ndjson'
const runId = process.env.NANOTRACE_LLM_RUN_ID ?? `llm-loadtest-${Date.now()}`
const seed = BigInt(fnv1a64(runId))
const now = new Date()
const windowMs = 7 * 24 * 60 * 60 * 1000
const windowStartMs = now.getTime() - windowMs

fs.mkdirSync(path.dirname(output), { recursive: true })

function makeEvent(seq) {
  const rng = new TinyRng(seed ^ BigInt(seq) * 0x9e3779b97f4a7c15n)
  const timestamp = realisticTimestamp(seq, rng)
  const observed = new Date(timestamp.getTime() + rng.range(20, 5_000))
  const model = choose(rng, [
    'gpt-5.5',
    'gpt-5.5',
    'gpt-5',
    'gpt-5-mini',
    'claude-opus-4-7',
    'claude-sonnet-4-6',
    'claude-sonnet-4-6',
    'claude-haiku-4-5',
    'gemini-3.1-pro-preview',
    'gemini-3-flash-preview',
    'gemini-3-flash-preview',
    'gemini-3.1-flash-lite'
  ])
  const finishReason = choose(rng, [
    'stop',
    'stop',
    'stop',
    'stop',
    'tool-calls',
    'tool-calls',
    'length',
    'content-filter'
  ])
  const service = choose(rng, ['llm-gateway', 'llm-gateway', 'canvas-agent', 'api'])
  const env = choose(rng, ['prod', 'prod', 'prod', 'prod', 'staging', 'canary'])
  const traceIndex = Math.floor(seq / 24)
  const spanSlot = Math.floor((seq % 24) / 2)
  const rootSpanId = stableHex(`${runId}:span:${traceIndex * 1000}`, 16)
  const spanId = spanSlot === 0 ? rootSpanId : stableHex(`${runId}:span:${traceIndex * 1000 + spanSlot}`, 16)

  return {
    event_id: `${runId}-${seq}`,
    timestamp: timestamp.toISOString(),
    observed_timestamp: observed.toISOString(),
    data: {
      tenant_id: 'loadtest',
      service,
      event_type: 'log',
      environment: env,
      trace_id: stableHex(`${runId}:trace:${traceIndex}`, 32),
      span_id: spanId,
      parent_span_id: spanSlot === 0 ? '' : rootSpanId,
      name: 'POST /v1/chat/completions',
      severity_number: finishReason === 'content-filter' ? 13 : 9,
      severity_text: finishReason === 'content-filter' ? 'WARN' : 'INFO',
      is_error: finishReason === 'content-filter' ? 1 : 0,
      'http.method': 'POST',
      'http.route': '/v1/chat/completions',
      message: `${model} completion usage`,
      user_id: `user_${Math.floor(seq / (24 * 12 * 6)).toString().padStart(6, '0')}`,
      session_id: `sess_${Math.floor(seq / (24 * 12)).toString().padStart(6, '0')}`,
      account_id: `acct_${Math.floor(seq / (24 * 12 * 6 * 25)).toString().padStart(5, '0')}`,
      llm: {
        model,
        finishReason,
        messages: [],
        toolNames: finishReason === 'tool-calls' ? chooseToolNames(rng) : [],
        responses: [],
        totalUsage: llmUsage(model, finishReason, rng)
      },
      _loadtest: {
        run_id: runId,
        sequence: seq,
        fixture: 'llm_log_generated',
        profile: 'llm'
      }
    }
  }
}

function realisticTimestamp(seq, rng) {
  for (;;) {
    const day = rng.range(0, 6)
    const hour = rng.range(0, 23)
    const candidate = new Date(windowStartMs + day * 24 * 60 * 60 * 1000)
    if (rng.range(1, 100) <= trafficWeight(candidate, hour)) {
      return new Date(
        windowStartMs +
          day * 24 * 60 * 60 * 1000 +
          hour * 60 * 60 * 1000 +
          rng.range(0, 59) * 60 * 1000 +
          rng.range(0, 59) * 1000 +
          rng.range(0, 999)
      )
    }
  }
}

function trafficWeight(day, hour) {
  const hourly = hour <= 5 ? 12
    : hour <= 8 ? 55
      : hour <= 11 ? 95
        : hour <= 13 ? 75
          : hour <= 17 ? 100
            : hour <= 20 ? 70
              : 35
  const dow = day.getUTCDay()
  const factor = dow === 0 || dow === 6 ? 0.45 : dow === 1 ? 0.85 : dow === 5 ? 0.9 : 1.0
  return Math.max(5, Math.min(100, Math.round(hourly * factor)))
}

function llmUsage(model, finishReason, rng) {
  const longContext = rng.chance(8, 100)
  const cachedInputTokens = rng.chance(42, 100)
    ? 0
    : longContext
      ? rng.range(12_000, 180_000)
      : rng.range(256, 48_000)
  const noCacheTokens = longContext ? rng.range(8_000, 80_000) : rng.range(80, 14_000)
  const inputTokens = cachedInputTokens + noCacheTokens
  const smallModel = model.includes('mini') || model.includes('haiku') || model.includes('flash-lite')
  const reasoningTokens = smallModel ? rng.range(0, 3_000) : finishReason === 'tool-calls' ? rng.range(0, 8_000) : rng.range(0, 18_000)
  const textTokens = finishReason === 'length' ? rng.range(3_000, 16_000) : finishReason === 'tool-calls' ? rng.range(20, 1_800) : rng.range(40, 6_000)
  const outputTokens = reasoningTokens + textTokens
  return {
    cachedInputTokens,
    inputTokenDetails: {
      cacheReadTokens: cachedInputTokens,
      noCacheTokens
    },
    inputTokens,
    outputTokenDetails: {
      reasoningTokens,
      textTokens
    },
    outputTokens,
    reasoningTokens,
    totalTokens: inputTokens + outputTokens
  }
}

function chooseToolNames(rng) {
  const tools = ['Bash', 'Mcp_canvas_create_runtime_app_frame', 'Mcp_canvas_batch_create_elements', 'Mcp_canvas_generate_canvas_image']
  const n = rng.range(1, 2)
  const out = []
  for (let i = 0; i < n; i += 1) {
    const tool = choose(rng, tools)
    if (!out.includes(tool)) out.push(tool)
  }
  return out
}

function choose(rng, values) {
  return values[rng.range(0, values.length - 1)]
}

function positiveInt(value) {
  const parsed = Number.parseInt(value, 10)
  if (!Number.isSafeInteger(parsed) || parsed <= 0) {
    throw new Error(`expected positive integer, got ${value}`)
  }
  return parsed
}

function onceDrain(stream) {
  return new Promise(resolve => {
    stream.once('drain', resolve)
  })
}

function stableHex(value, len) {
  const rng = new TinyRng(BigInt(fnv1a64(value)))
  let out = ''
  while (out.length < len) out += rng.nextU64().toString(16).padStart(16, '0')
  return out.slice(0, len)
}

function fnv1a64(value) {
  let hash = 0xcbf29ce484222325n
  for (const byte of Buffer.from(value)) {
    hash ^= BigInt(byte)
    hash = BigInt.asUintN(64, hash * 0x100000001b3n)
  }
  return hash
}

class TinyRng {
  constructor(seed) {
    this.state = BigInt.asUintN(64, seed)
  }

  nextU64() {
    this.state = BigInt.asUintN(64, this.state + 0x9e3779b97f4a7c15n)
    let value = this.state
    value = BigInt.asUintN(64, (value ^ (value >> 30n)) * 0xbf58476d1ce4e5b9n)
    value = BigInt.asUintN(64, (value ^ (value >> 27n)) * 0x94d049bb133111ebn)
    return value ^ (value >> 31n)
  }

  range(min, max) {
    if (min >= max) return min
    return min + Number(this.nextU64() % BigInt(max - min + 1))
  }

  chance(numerator, denominator) {
    return denominator !== 0 && this.range(1, denominator) <= numerator
  }
}

const stream = fs.createWriteStream(output, { flags: 'w' })
let written = 0
let bytes = 0
for (let seq = 0; seq < count; seq += 1) {
  const row = makeEvent(seq)
  const line = JSON.stringify(row) + '\n'
  bytes += Buffer.byteLength(line)
  if (!stream.write(line)) {
    await onceDrain(stream)
  }
  written += 1
  if (written % 1_000_000 === 0) {
    process.stderr.write(`generated=${written} bytes=${bytes}\n`)
  }
}

await new Promise((resolve, reject) => {
  stream.end(error => error ? reject(error) : resolve())
})

process.stderr.write(`done output=${output} events=${written} bytes=${bytes}\n`)
