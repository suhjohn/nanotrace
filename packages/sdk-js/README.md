# Nanotrace TypeScript SDK

The SDK shapes application events into Nanotrace's `/v1/events` contract.

It uses camelCase public parameters and emits schema fields such as
`event_type`, `trace_id`, `duration_ms`, and `http.status_code`.
OpenTelemetry-style dotted attributes can be passed directly.

Event methods are fire-and-forget. The client batches events in process by
count, byte size, or a short flush interval, then posts a JSON array to the
ingest API. Call `flush()` when you need to wait for delivery.

```ts
import { createNanotrace, httpTransport } from '@nanotrace/sdk'

const nt = createNanotrace({
  transport: httpTransport({
    url: process.env.NANOTRACE_URL!,
    key: process.env.NANOTRACE_KEY!
  }),
  service: 'checkout-api',
  environment: 'prod'
})

const span = nt.span('POST /checkout')
try {
  nt.counter('checkout.attempts')
  nt.measure('checkout.latency', 183, {
    metricUnit: 'ms',
    plan: 'pro',
    country: 'US',
    llm: { model: 'gpt-4.1-mini' }
  })
  nt.info('checkout started')
  span.end({ spanStatusCode: 'ok' })
} catch (error) {
  span.end({ spanStatusCode: 'error', isError: 1 })
  throw error
}

await nt.flush()
```

`measure(name, value, fields)` is a convenience alias for a histogram metric
event. SDK calls emit facts only; tenant definitions choose whether those facts
materialize into metric rollups, measure cubes, funnels, cohorts, or raw/KV
query paths.

Use the UDP transport with the Rust sidecar:

```ts
import { createNanotrace, udpTransport } from '@nanotrace/sdk'

const nt = createNanotrace({
  transport: udpTransport({ port: 4319 })
})
```

Use local HTTP with the Rust sidecar when UDP is not convenient:

```ts
import { createNanotrace, sidecarHttpTransport } from '@nanotrace/sdk'

const nt = createNanotrace({
  transport: sidecarHttpTransport({ url: 'http://127.0.0.1:4320' })
})
```

Use the sidecar when you need local disk spool, host-level enrichment, egress
control, or shared fleet policy. For most server apps, direct `httpTransport`
is the easiest starting point because batching, retry surfacing through
`flush()`, and endpoint shape are handled by the SDK.
