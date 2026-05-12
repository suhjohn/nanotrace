# Nanotrace Client

`nanotrace-client` is a local UDP and HTTP sidecar for application servers. It
is not a general OpenTelemetry SDK or OTLP collector replacement; it is
intentionally smaller and only forwards Nanotrace event JSON to `POST /events`.

Applications send Nanotrace event JSON to UDP or local HTTP on localhost. The
client validates that each payload is a JSON event object or non-empty event
array, batches events in memory, and forwards them to the configured Nanotrace
HTTP cluster. If common service metadata environment variables are present, the
client fills missing fields inside `data`. Event fields sent by the application
always win.

UDP datagrams and local `POST /events` use the same event contract as the remote
server:

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
  -p 127.0.0.1:4320:4320/tcp \
  -e NANOTRACE_URL=http://nanotrace-prod-alb.example.com \
  -e NANOTRACE_KEY=secret \
  nanotrace-sidecar
```

Configuration:

```text
NANOTRACE_CLIENT_BIND=127.0.0.1:4319
NANOTRACE_CLIENT_HTTP_BIND=127.0.0.1:4320
NANOTRACE_URL=http://...
NANOTRACE_KEY=...
NANOTRACE_CLIENT_BATCH_MAX_EVENTS=100
NANOTRACE_CLIENT_BATCH_MAX_BYTES=1048576
NANOTRACE_CLIENT_FLUSH_MS=25
NANOTRACE_CLIENT_QUEUE_CAPACITY=10000
NANOTRACE_CLIENT_UDP_MAX_BYTES=65507
NANOTRACE_CLIENT_HTTP_MAX_BYTES=1048576
NANOTRACE_CLIENT_HTTP_TIMEOUT_MS=5000
NANOTRACE_CLIENT_RETRY_ATTEMPTS=3
NANOTRACE_CLIENT_RETRY_BASE_MS=100
```

Set `NANOTRACE_CLIENT_HTTP_BIND=disabled` to run UDP-only.

Optional enrichment fields:

```text
data.service             NANOTRACE_SERVICE, OTEL_SERVICE_NAME, DD_SERVICE, SERVICE_NAME
data.environment         NANOTRACE_ENV, DD_ENV, APP_ENV, NODE_ENV
data.service_version     NANOTRACE_VERSION, DD_VERSION, SERVICE_VERSION, APP_VERSION, GIT_SHA
data.service.instance.id NANOTRACE_INSTANCE_ID, SERVICE_INSTANCE_ID, HOSTNAME, HOST
```

The UDP path is intentionally fire-and-forget. If the process is down, the queue
is full, or a datagram is invalid JSON, the application is not blocked. The
local HTTP path returns a response once the event has been accepted into the
sidecar queue; remote persistence still happens asynchronously through the
sidecar batcher.
