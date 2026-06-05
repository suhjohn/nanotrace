# Nanotrace Python SDK

Python SDK for sending Nanotrace events, logs, metrics, and spans to the
Nanotrace `/v1/events` write path.

## Sync

```py
from nanotrace import create_nanotrace, http_transport

nt = create_nanotrace(
    http_transport("https://api.nanotrace.dev", key="..."),
    service="checkout-api",
    environment="prod",
)

with nt.span("POST /checkout") as span:
    nt.counter("checkout.attempts")
    nt.measure(
        "checkout.latency",
        183,
        metric_unit="ms",
        plan="pro",
        country="US",
        llm={"model": "gpt-4.1-mini"},
    )
    nt.info("checkout started")
    span.set("cart.items", 3)

nt.flush()
```

`measure(name, value, **fields)` is a convenience alias for a histogram metric
event. SDK calls emit facts only; tenant definitions choose whether those facts
materialize into metric rollups, measure cubes, funnels, cohorts, or raw/KV
query paths.

## Async

```py
from nanotrace import create_async_nanotrace, async_http_transport

nt = create_async_nanotrace(
    async_http_transport("https://api.nanotrace.dev", key="..."),
    service="checkout-api",
    environment="prod",
)

async with nt.span("POST /checkout") as span:
    nt.counter("checkout.attempts")
    nt.info("checkout started")
    span.set("cart.items", 3)

await nt.flush()
```

## Transports

The default production path is direct HTTP. The SDK batches events in process by
count, byte size, or a short flush interval, then posts a JSON array to
`/v1/events`:

```py
http_transport("https://api.nanotrace.dev", key="...")
```

Use the local sidecar when you need local disk spool, host-level enrichment,
egress control, or shared fleet policy:

```py
sidecar_http_transport("http://127.0.0.1:4320")
udp_transport("127.0.0.1", 4319)
```

UDP sidecar example:

```py
from nanotrace import create_nanotrace, udp_transport

nt = create_nanotrace(
    udp_transport("127.0.0.1", 4319),
    service="checkout-api",
    environment="dev",
)

nt.info("checkout started")
nt.counter("checkout.attempts")

with nt.span("POST /checkout") as span:
    span.set("cart.items", 3)
    nt.info("charge requested")

nt.flush()
```

Async UDP sidecar example:

```py
from nanotrace import create_async_nanotrace, async_udp_transport

nt = create_async_nanotrace(
    async_udp_transport("127.0.0.1", 4319),
    service="checkout-api",
    environment="dev",
)

nt.info("checkout started")
nt.counter("checkout.attempts")

async with nt.span("POST /checkout") as span:
    span.set("cart.items", 3)
    nt.info("charge requested")

await nt.flush()
```

Direct HTTP posts to `/v1/events`. Local sidecar HTTP posts to the sidecar's
`/events` intake. CamelCase public fields are normalized to Nanotrace event
fields, and OpenTelemetry-style dotted attributes can be passed directly.

Async transports are available with the `async_` prefix. Event methods are
fire-and-forget for both sync and async clients; call `flush()` when you need to
wait for delivery.
