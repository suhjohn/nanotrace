#!/usr/bin/env node
import { spawnSync } from 'node:child_process'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..')
process.env.PULUMI_CONFIG_PASSPHRASE ??= ''

const result = spawnSync('pulumi', ['-C', 'deploy/pulumi/nanotrace', ...process.argv.slice(2)], {
  cwd: repoRoot,
  env: process.env,
  stdio: 'inherit'
})
process.exit(result.status ?? 1)
