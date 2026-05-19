# Nanotrace TypeScript SDK

The SDK shapes application events into Nanotrace's `/v1/events` contract.

It uses camelCase public parameters and emits schema fields such as
`event_type`, `trace_id`, `duration_ms`, and `http.status_code`.
OpenTelemetry-style dotted attributes can be passed directly.

Event methods are fire-and-forget. Call `flush()` when you need to wait for delivery.

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
  nt.info('checkout started')
  span.end({ spanStatusCode: 'ok' })
} catch (error) {
  span.end({ spanStatusCode: 'error', isError: 1 })
  throw error
}

await nt.flush()
```

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
