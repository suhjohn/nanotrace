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

nt.flush()
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
    nt.counter("checkout.attempts")
    nt.info("checkout started")
    span.set("cart.items", 3)

await nt.flush()
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

Use direct HTTP for scripts, tests, or serverless jobs:

```py
http_transport("https://api.nanotrace.dev", key="...")
```

Async transports are available with the `async_` prefix. Event methods are fire-and-forget for both sync and async clients; call `flush()` when you need to wait for delivery.
