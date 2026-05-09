# Nanotrace TypeScript SDK

The SDK shapes application events into Nanotrace's `/events` contract.

It uses camelCase public parameters and emits schema fields such as
`event_type`, `trace_id`, `duration_ms`, and `http.status_code`.

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

await nt.span('POST /checkout', async () => {
  nt.counter('checkout.attempts')
  await nt.info('checkout started')
})
```

Use the UDP transport with the Rust sidecar:

```ts
import { createNanotrace, udpTransport } from '@nanotrace/sdk'

const nt = createNanotrace({
  transport: udpTransport({ port: 4319 })
})
```
