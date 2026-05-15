#!/usr/bin/env node
import { spawnSync } from 'node:child_process'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..')

const apiBaseUrl = requiredEnv('NANOTRACE_PROVISIONER_API_BASE_URL').replace(/\/+$/, '')
const apiKey = requiredEnv('NANOTRACE_PROVISIONER_API_KEY')
const workerId = process.env.NANOTRACE_PROVISIONER_WORKER_ID || `provisioner:${process.pid}`
const stackPrefix = process.env.NANOTRACE_PROVISIONER_STACK_PREFIX || 'org'
const pulumiCommand = process.env.NANOTRACE_PROVISIONER_PULUMI_COMMAND || 'up'
const poll = process.argv.includes('--poll')

do {
  const job = await claimJob()
  if (!job) {
    if (!poll) {
      console.log('No queued data-plane provisioning jobs.')
      process.exit(0)
    }
    await sleep(Number(process.env.NANOTRACE_PROVISIONER_POLL_MS || 15000))
    continue
  }
  await provision(job)
} while (poll)

async function claimJob () {
  const response = await request('/data-plane/provisioning/jobs/claim', {
    method: 'POST',
    body: { worker_id: workerId }
  })
  return response.job ?? null
}

async function provision (job) {
  const stack = stackName(job)
  console.log(`Provisioning ${job.organization_id} with stack ${stack}.`)
  try {
    runPulumi(['stack', 'select', stack, '--create'], pulumiEnv(job))
    runPulumi([pulumiCommand, '--yes', '--skip-preview'], pulumiEnv(job))
    const outputs = pulumiOutputs(pulumiEnv(job))
    await completeJob(job.id, {
      status: 'succeeded',
      result: { pulumi_stack: stack, pulumi_outputs: redactOutputs(outputs) },
      data_plane: dataPlaneFromOutputs(job, outputs)
    })
    console.log(`Provisioned ${job.organization_id}.`)
  } catch (error) {
    await completeJob(job.id, {
      status: 'failed',
      error: error instanceof Error ? error.message : String(error),
      result: { pulumi_stack: stack }
    })
    throw error
  }
}

async function completeJob (jobId, body) {
  await request(`/data-plane/provisioning/jobs/${encodeURIComponent(jobId)}/complete`, {
    method: 'POST',
    body
  })
}

function dataPlaneFromOutputs (job, outputs) {
  const publicBaseUrl = stringOutput(outputs, 'apiBaseUrlOutput') || stringOutput(outputs, 'appBaseUrlOutput') || stringOutput(outputs, 'ingestUrl')
  return {
    mode: 'dedicated',
    provider: job.provider || 'aws',
    region: job.region || process.env.AWS_REGION || 'us-west-2',
    public_base_url: publicBaseUrl,
    ingest_url: stringOutput(outputs, 'ingestUrl') || publicBaseUrl,
    query_url: stringOutput(outputs, 'apiBaseUrlOutput') || publicBaseUrl,
    internal_secret_ref: 'NANOTRACE_DATA_PLANE_SHARED_SECRET',
    s3_bucket: stringOutput(outputs, 'bucketName'),
    processor_prefix: stringOutput(outputs, 'processorPrefixOutput') || `organizations/${job.organization_id}/processors`,
    clickhouse_mode: stringOutput(outputs, 'clickhouseModeOutput') || job.clickhouse_mode || 'shared-service',
    clickhouse_provider: process.env.CLICKHOUSE_CLOUD_PROVIDER || job.provider || 'aws',
    clickhouse_region: stringOutput(outputs, 'CLICKHOUSE_CLOUD_REGION') || job.clickhouse_region || job.region || 'us-west-2',
    clickhouse_service_id: stringOutput(outputs, 'clickhouseCloudServiceIdOutput'),
    clickhouse_url: stringOutput(outputs, 'clickhouseUrlOutput'),
    clickhouse_database: stringOutput(outputs, 'clickhouseDatabaseOutput') || clickhouseDatabaseName(job.organization_id),
    kms_key_arn: stringOutput(outputs, 'dataPlaneKmsKeyArnOutput'),
    status: 'active',
    status_message: 'Dedicated data plane provisioned.'
  }
}

function pulumiOutputs (env) {
  const result = spawnSync('pulumi', ['-C', 'deploy/pulumi/nanotrace', 'stack', 'output', '--json'], {
    cwd: repoRoot,
    env,
    encoding: 'utf8'
  })
  if (result.status !== 0) {
    throw new Error((result.stderr || result.stdout || 'pulumi stack output failed').trim())
  }
  return JSON.parse(result.stdout || '{}')
}

function runPulumi (args, env) {
  const result = spawnSync('pulumi', ['-C', 'deploy/pulumi/nanotrace', ...args], {
    cwd: repoRoot,
    env,
    stdio: 'inherit'
  })
  if (result.status !== 0) {
    throw new Error(`pulumi ${args.join(' ')} failed with exit ${result.status ?? 1}`)
  }
}

function pulumiEnv (job) {
  return {
    ...process.env,
    PULUMI_CONFIG_PASSPHRASE: process.env.PULUMI_CONFIG_PASSPHRASE ?? '',
    AWS_REGION: job.region || process.env.AWS_REGION || 'us-west-2',
    CLICKHOUSE_CLOUD_REGION: job.clickhouse_region || process.env.CLICKHOUSE_CLOUD_REGION || job.region || 'us-west-2',
    CLICKHOUSE_DATABASE: process.env.NANOTRACE_PROVISIONER_CLICKHOUSE_DATABASE || clickhouseDatabaseName(job.organization_id),
    NANOTRACE_DATA_PLANE_ORGANIZATION_ID: job.organization_id,
    NANOTRACE_DATA_PLANE_PROVIDER: job.provider || 'aws',
    NANOTRACE_DATA_PLANE_REGION: job.region || process.env.AWS_REGION || 'us-west-2',
    NANOTRACE_CLICKHOUSE_MODE: normalizeClickhouseMode(job.clickhouse_mode || process.env.NANOTRACE_CLICKHOUSE_MODE || 'shared-service'),
    PROCESSOR_PREFIX: `organizations/${job.organization_id}/processors`
  }
}

function stackName (job) {
  const org = job.organization_id.replace(/[^A-Za-z0-9_.-]/g, '-')
  const region = (job.region || 'us-west-2').replace(/[^A-Za-z0-9_.-]/g, '-')
  return `${stackPrefix}-${org}-${region}`
}

async function request (pathname, { method, body }) {
  const response = await fetch(`${apiBaseUrl}${pathname}`, {
    method,
    headers: {
      Authorization: `Bearer ${apiKey}`,
      'Content-Type': 'application/json'
    },
    body: body === undefined ? undefined : JSON.stringify(body)
  })
  if (!response.ok) {
    const text = await response.text()
    throw new Error(`${method} ${pathname} failed: ${response.status} ${text || response.statusText}`)
  }
  return await response.json()
}

function stringOutput (outputs, name) {
  const value = outputs?.[name]
  if (value === undefined || value === null) return ''
  if (typeof value === 'object' && 'value' in value) return String(value.value ?? '')
  return String(value)
}

function redactOutputs (outputs) {
  const redacted = {}
  for (const [key, value] of Object.entries(outputs || {})) {
    redacted[key] = /password|secret|key|token/i.test(key) ? '[redacted]' : value
  }
  return redacted
}

function requiredEnv (key) {
  const value = process.env[key]
  if (!value) throw new Error(`${key} is required`)
  return value
}

function normalizeClickhouseMode (value) {
  if (value === 'shared-service' || value === 'dedicated-service' || value === 'external') return value
  throw new Error('clickhouse_mode must be shared-service, dedicated-service, or external')
}

function clickhouseDatabaseName (organizationId) {
  const normalized = organizationId
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9_]+/g, '_')
    .replace(/^_+|_+$/g, '')
  if (!normalized) return 'observatory'
  return /^[a-z_]/.test(normalized) ? normalized : `org_${normalized}`
}

function sleep (ms) {
  return new Promise(resolve => setTimeout(resolve, ms))
}
