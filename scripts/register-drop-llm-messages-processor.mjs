#!/usr/bin/env node
import { readFile } from 'node:fs/promises'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..')
const baseUrl = requiredEnv('NANOTRACE_URL').replace(/\/+$/, '')
const apiKey = requiredEnv('NANOTRACE_API_KEY')
const name = process.env.NANOTRACE_PROCESSOR_NAME ?? 'drop-llm-messages'
const code = await readFile(
  path.join(root, 'processors/drop_llm_messages_loader.rs'),
  'utf8'
)

const response = await fetch(`${baseUrl}/processors/${encodeURIComponent(name)}`, {
  method: 'PUT',
  headers: {
    authorization: `Bearer ${apiKey}`,
    'content-type': 'application/json'
  },
  body: JSON.stringify({
    loader: {
      code,
      config: {}
    }
  })
})

const text = await response.text()
if (!response.ok) {
  throw new Error(`PUT /processors/${name} failed: ${response.status} ${text}`)
}

console.log(text)

function requiredEnv (key) {
  const value = process.env[key]?.trim()
  if (!value) {
    throw new Error(`${key} is required`)
  }
  return value
}
