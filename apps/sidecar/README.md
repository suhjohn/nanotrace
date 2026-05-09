# Nanotrace Client

`nanotrace-client` is a local UDP sidecar for application servers. It is not a
general OpenTelemetry SDK or OTLP collector replacement; it is intentionally
smaller and only forwards Nanotrace event JSON to `POST /events`.

Applications send Nanotrace event JSON to UDP on localhost. The client validates
that each datagram is a JSON event object or non-empty event array, batches
events in memory, and forwards them to the configured Nanotrace HTTP cluster.
If common service metadata environment variables are present, the client fills
missing fields inside `data`. Event fields sent by the application always win.

UDP input uses the same event contract as `POST /events`:

```json
{"event_id":"evt_1","timestamp":"2026-05-11T23:00:00Z","data":{"service":"api"}}
```

Run:

```sh
NANOTRACE_URL=http://nanotrace-prod-alb.example.com \
NANOTRACE_KEY=secret \
cargo run -p nanotrace-client
```

Build and run as a container from the repo root:

```sh
docker build -f apps/sidecar/Dockerfile -t nanotrace-sidecar .
docker run --rm \
  -p 127.0.0.1:4319:4319/udp \
  -e NANOTRACE_URL=http://nanotrace-prod-alb.example.com \
  -e NANOTRACE_KEY=secret \
  nanotrace-sidecar
```

Configuration:

```text
NANOTRACE_CLIENT_BIND=127.0.0.1:4319
NANOTRACE_URL=http://...
NANOTRACE_KEY=...
NANOTRACE_CLIENT_BATCH_MAX_EVENTS=100
NANOTRACE_CLIENT_BATCH_MAX_BYTES=1048576
NANOTRACE_CLIENT_FLUSH_MS=25
NANOTRACE_CLIENT_QUEUE_CAPACITY=10000
NANOTRACE_CLIENT_UDP_MAX_BYTES=65507
NANOTRACE_CLIENT_HTTP_TIMEOUT_MS=5000
NANOTRACE_CLIENT_RETRY_ATTEMPTS=3
NANOTRACE_CLIENT_RETRY_BASE_MS=100
```

Optional enrichment fields:

```text
data.service             NANOTRACE_SERVICE, OTEL_SERVICE_NAME, DD_SERVICE, SERVICE_NAME
data.environment         NANOTRACE_ENV, DD_ENV, APP_ENV, NODE_ENV
data.service_version     NANOTRACE_VERSION, DD_VERSION, SERVICE_VERSION, APP_VERSION, GIT_SHA
data.service.instance.id NANOTRACE_INSTANCE_ID, SERVICE_INSTANCE_ID, HOSTNAME, HOST
```

The client is intentionally fire-and-forget at the UDP boundary. If the process
is down, the queue is full, or a datagram is invalid JSON, the application is
not blocked.
