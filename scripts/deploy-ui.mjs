#!/usr/bin/env node
import { spawnSync } from 'node:child_process'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..')
const distDir = path.join(repoRoot, 'apps/ui/dist')

const bucket = requiredEnv('NANOTRACE_UI_BUCKET')
const distributionId = process.env.NANOTRACE_UI_DISTRIBUTION_ID ?? ''
const region = process.env.AWS_REGION ?? process.env.AWS_DEFAULT_REGION ?? 'us-west-1'
const apiBaseUrl = uiApiBaseUrl()
const deployEnv = {
  ...process.env,
  VITE_NANOTRACE_URL: apiBaseUrl
}

run('npm', ['--workspace', '@nanotrace/ui', 'run', 'build'], {
  cwd: repoRoot,
  env: deployEnv
})

run('aws', [
  's3',
  'sync',
  distDir,
  `s3://${bucket}`,
  '--delete',
  '--exclude',
  'index.html',
  '--cache-control',
  'public,max-age=31536000,immutable',
  '--region',
  region
])

run('aws', [
  's3',
  'cp',
  path.join(distDir, 'index.html'),
  `s3://${bucket}/index.html`,
  '--cache-control',
  'no-cache',
  '--content-type',
  'text/html; charset=utf-8',
  '--region',
  region
])

if (distributionId) {
  run('aws', [
    'cloudfront',
    'create-invalidation',
    '--distribution-id',
    distributionId,
    '--paths',
    '/*',
    '--region',
    region
  ])
}

function run (command, args, options = {}) {
  const result = spawnSync(command, args, {
    cwd: options.cwd ?? repoRoot,
    env: options.env ?? process.env,
    stdio: 'inherit'
  })
  if (result.status !== 0) {
    process.exit(result.status ?? 1)
  }
}

function requiredEnv (key) {
  const value = process.env[key]
  if (!value) throw new Error(`${key} is required`)
  return value
}

function uiApiBaseUrl () {
  const configured =
    process.env.VITE_NANOTRACE_URL ||
    process.env.NANOTRACE_API_BASE_URL ||
    process.env.NANOTRACE_PUBLIC_BASE_URL ||
    domainApiBaseUrl()
  const value = String(configured || '').trim().replace(/\/+$/, '')
  if (!value) {
    throw new Error('VITE_NANOTRACE_URL, NANOTRACE_API_BASE_URL, or NANOTRACE_DOMAIN_NAME is required')
  }
  const parsed = new URL(value)
  const isLocalhost = parsed.hostname === 'localhost' || parsed.hostname === '127.0.0.1' || parsed.hostname === '::1'
  if (isLocalhost && process.env.NANOTRACE_ALLOW_LOCAL_UI_API !== 'true') {
    throw new Error(`refusing to deploy UI with local API URL: ${value}`)
  }
  return value
}

function domainApiBaseUrl () {
  const domainName = process.env.NANOTRACE_DOMAIN_NAME
  return domainName ? `https://api.${domainName.trim()}` : ''
}
