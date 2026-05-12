# Nanotrace Python SDK

Python SDK for sending Nanotrace events, logs, metrics, and spans.

## Sync

```py
from nanotrace import create_nanotrace, sidecar_http_transport

nt = create_nanotrace(
    sidecar_http_transport("http://127.0.0.1:4320"),
    service="checkout-api",
    environment="prod",
)

with nt.span("POST /checkout") as span:
    nt.counter("checkout.attempts")
    nt.info("checkout started")
    span.set("cart.items", 3)
```

## Async

```py
from nanotrace import create_async_nanotrace, async_sidecar_http_transport

nt = create_async_nanotrace(
    async_sidecar_http_transport("http://127.0.0.1:4320"),
    service="checkout-api",
    environment="prod",
)

async with nt.span("POST /checkout") as span:
    await nt.counter("checkout.attempts")
    await nt.info("checkout started")
    span.set("cart.items", 3)
```

## Transports

Use the local sidecar when possible:

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
```

Async UDP sidecar example:

```py
from nanotrace import create_async_nanotrace, async_udp_transport

nt = create_async_nanotrace(
    async_udp_transport("127.0.0.1", 4319),
    service="checkout-api",
    environment="dev",
)

await nt.info("checkout started")
await nt.counter("checkout.attempts")

async with nt.span("POST /checkout") as span:
    span.set("cart.items", 3)
    await nt.info("charge requested")
```

Use direct HTTP for scripts, tests, or serverless jobs:

```py
http_transport("https://api.nanotrace.dev", key="...")
```

Async variants are available with the `async_` prefix.
