import dgram from 'node:dgram'
import type { EventEnvelope, Transport } from './types.js'

async function postJson({
  errorPrefix,
  fetchImpl,
  headers,
  payload,
  url
}: {
  errorPrefix: string
  fetchImpl: typeof fetch
  headers: Record<string, string>
  payload: EventEnvelope | EventEnvelope[]
  url: string
}) {
  const response = await fetchImpl(url, {
    method: 'POST',
    headers,
    body: JSON.stringify(payload)
  })
  if (!response.ok) {
    throw new Error(`${errorPrefix}: HTTP ${response.status}`)
  }
}

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
      await postJson({
        errorPrefix: 'Nanotrace ingest failed',
        fetchImpl,
        headers: {
          authorization: auth,
          'content-type': 'application/json'
        },
        payload: event,
        url: eventsUrl
      })
    },
    async sendBatch(events) {
      if (events.length === 0) return
      await postJson({
        errorPrefix: 'Nanotrace ingest failed',
        fetchImpl,
        headers: {
          authorization: auth,
          'content-type': 'application/json'
        },
        payload: events,
        url: eventsUrl
      })
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
      await postJson({
        errorPrefix: 'Nanotrace sidecar ingest failed',
        fetchImpl,
        headers: {
          'content-type': 'application/json'
        },
        payload: event,
        url: eventsUrl
      })
    },
    async sendBatch(events) {
      if (events.length === 0) return
      await postJson({
        errorPrefix: 'Nanotrace sidecar ingest failed',
        fetchImpl,
        headers: {
          'content-type': 'application/json'
        },
        payload: events,
        url: eventsUrl
      })
    }
  }
}
