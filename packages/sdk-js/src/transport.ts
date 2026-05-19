import dgram from 'node:dgram'
import type { EventEnvelope, Transport } from './types.js'

export function httpTransport({
  url,
  key,
  fetchImpl = globalThis.fetch
}: {
  url: string
  key: string
  fetchImpl?: typeof fetch
}): Transport {
  const eventsUrl = `${url.replace(/\/+$/, '')}/v1/events`
  const auth = `Bearer ${key}`
  return {
    async send(event) {
      const response = await fetchImpl(eventsUrl, {
        method: 'POST',
        headers: {
          authorization: auth,
          'content-type': 'application/json'
        },
        body: JSON.stringify(event)
      })
      if (!response.ok) {
        throw new Error(`Nanotrace ingest failed: HTTP ${response.status}`)
      }
    }
  }
}

export function udpTransport({
  host = '127.0.0.1',
  port = 4319
}: {
  host?: string
  port?: number
} = {}): Transport {
  const socket = dgram.createSocket('udp4')
  return {
    send(event) {
      return new Promise((resolve, reject) => {
        socket.send(Buffer.from(JSON.stringify(event)), port, host, error => {
          if (error) reject(error)
          else resolve()
        })
      })
    }
  }
}

export function sidecarHttpTransport({
  url = 'http://127.0.0.1:4320',
  fetchImpl = globalThis.fetch
}: {
  url?: string
  fetchImpl?: typeof fetch
} = {}): Transport {
  const eventsUrl = `${url.replace(/\/+$/, '')}/events`
  return {
    async send(event) {
      const response = await fetchImpl(eventsUrl, {
        method: 'POST',
        headers: {
          'content-type': 'application/json'
        },
        body: JSON.stringify(event)
      })
      if (!response.ok) {
        throw new Error(`Nanotrace sidecar ingest failed: HTTP ${response.status}`)
      }
    }
  }
}
