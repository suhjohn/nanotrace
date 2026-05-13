use anyhow::{Context, Result, bail};
use axum::{
    Json, Router,
    body::{Body, to_bytes},
    extract::State,
    http::{Request, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use reqwest::header::AUTHORIZATION;
use serde_json::Value;
use std::{
    collections::BTreeMap,
    env,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{
    net::UdpSocket,
    sync::mpsc,
    time::{MissedTickBehavior, interval, sleep},
};
use tracing::{error, info, warn};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "nanotrace_client=info".into()),
        )
        .init();

    let cfg = Arc::new(Config::from_env()?);
    let metrics = Arc::new(Metrics::default());
    let http = reqwest::Client::builder()
        .pool_idle_timeout(Duration::from_secs(90))
        .tcp_keepalive(Duration::from_secs(60))
        .timeout(cfg.http_timeout)
        .build()
        .context("build HTTP client")?;
    let (tx, rx) = mpsc::channel(cfg.queue_capacity);

    info!(
        udp_bind = %cfg.udp_bind,
        http_bind = cfg.http_bind.map(|bind| bind.to_string()).unwrap_or_else(|| "disabled".to_owned()),
        url = %cfg.events_url,
        batch_max_events = cfg.batch_max_events,
        batch_max_bytes = cfg.batch_max_bytes,
        flush_ms = cfg.flush_interval.as_millis(),
        queue_capacity = cfg.queue_capacity,
        "starting nanotrace sidecar"
    );

    let udp_task = tokio::spawn(receive_udp(
        Arc::clone(&cfg),
        Arc::clone(&metrics),
        tx.clone(),
    ));
    let http_intake_task = match cfg.http_bind {
        Some(_) => tokio::spawn(receive_http_intake(
            Arc::clone(&cfg),
            Arc::clone(&metrics),
            tx,
        )),
        None => tokio::spawn(async { std::future::pending::<Result<()>>().await }),
    };
    let batch_task = tokio::spawn(run_batcher(
        Arc::clone(&cfg),
        Arc::clone(&metrics),
        http,
        rx,
    ));
    let metrics_task = tokio::spawn(log_metrics(Arc::clone(&metrics)));

    tokio::select! {
        result = udp_task => result.context("UDP task join failed")??,
        result = http_intake_task => result.context("HTTP intake task join failed")??,
        result = batch_task => result.context("batch task join failed")??,
        result = tokio::signal::ctrl_c() => {
            result.context("install ctrl-c handler")?;
            info!("shutdown requested");
        }
    }

    metrics_task.abort();
    Ok(())
}

#[derive(Debug)]
struct Config {
    udp_bind: SocketAddr,
    http_bind: Option<SocketAddr>,
    events_url: String,
    auth_header: String,
    enrichment: BTreeMap<String, Value>,
    batch_max_events: usize,
    batch_max_bytes: usize,
    flush_interval: Duration,
    queue_capacity: usize,
    udp_max_bytes: usize,
    http_intake_max_bytes: usize,
    http_timeout: Duration,
    retry_attempts: usize,
    retry_base_delay: Duration,
}

impl Config {
    fn from_env() -> Result<Self> {
        let udp_bind = env_or("NANOTRACE_CLIENT_BIND", "127.0.0.1:4319")
            .parse()
            .context("NANOTRACE_CLIENT_BIND must be host:port")?;
        let http_bind = optional_bind_env("NANOTRACE_CLIENT_HTTP_BIND", "127.0.0.1:4320")?;
        let url = required_env("NANOTRACE_URL")
            .or_else(|_| required_env("NANOTRACE_INGEST_URL"))
            .context("NANOTRACE_URL or NANOTRACE_INGEST_URL is required")?;
        let key = required_env("NANOTRACE_API_KEY")
            .or_else(|_| required_env("NANOTRACE_KEY"))
            .context("NANOTRACE_API_KEY is required")?;
        let batch_max_events = usize_env("NANOTRACE_CLIENT_BATCH_MAX_EVENTS", 100)?;
        let batch_max_bytes = usize_env("NANOTRACE_CLIENT_BATCH_MAX_BYTES", 1024 * 1024)?;
        let flush_ms = u64_env("NANOTRACE_CLIENT_FLUSH_MS", 25)?;
        let queue_capacity = usize_env("NANOTRACE_CLIENT_QUEUE_CAPACITY", 10_000)?;
        let udp_max_bytes = usize_env("NANOTRACE_CLIENT_UDP_MAX_BYTES", 65_507)?;
        let http_intake_max_bytes = usize_env("NANOTRACE_CLIENT_HTTP_MAX_BYTES", 1024 * 1024)?;
        let http_timeout_ms = u64_env("NANOTRACE_CLIENT_HTTP_TIMEOUT_MS", 5_000)?;
        let retry_attempts = usize_env("NANOTRACE_CLIENT_RETRY_ATTEMPTS", 3)?;
        let retry_base_ms = u64_env("NANOTRACE_CLIENT_RETRY_BASE_MS", 100)?;

        if batch_max_events == 0 {
            bail!("NANOTRACE_CLIENT_BATCH_MAX_EVENTS must be greater than zero");
        }
        if batch_max_bytes == 0 {
            bail!("NANOTRACE_CLIENT_BATCH_MAX_BYTES must be greater than zero");
        }
        if queue_capacity == 0 {
            bail!("NANOTRACE_CLIENT_QUEUE_CAPACITY must be greater than zero");
        }
        if udp_max_bytes == 0 {
            bail!("NANOTRACE_CLIENT_UDP_MAX_BYTES must be greater than zero");
        }
        if http_intake_max_bytes == 0 {
            bail!("NANOTRACE_CLIENT_HTTP_MAX_BYTES must be greater than zero");
        }
        if retry_attempts == 0 {
            bail!("NANOTRACE_CLIENT_RETRY_ATTEMPTS must be greater than zero");
        }

        Ok(Self {
            udp_bind,
            http_bind,
            events_url: format!("{}/events", trim_trailing_slash(url)),
            auth_header: format!("Bearer {key}"),
            enrichment: default_enrichment(),
            batch_max_events,
            batch_max_bytes,
            flush_interval: Duration::from_millis(flush_ms),
            queue_capacity,
            udp_max_bytes,
            http_intake_max_bytes,
            http_timeout: Duration::from_millis(http_timeout_ms),
            retry_attempts,
            retry_base_delay: Duration::from_millis(retry_base_ms),
        })
    }
}

#[derive(Default)]
struct Metrics {
    datagrams: AtomicU64,
    datagram_bytes: AtomicU64,
    http_intake_requests: AtomicU64,
    http_intake_bytes: AtomicU64,
    events_accepted: AtomicU64,
    events_sent: AtomicU64,
    events_dropped_invalid: AtomicU64,
    events_dropped_queue_full: AtomicU64,
    batches_sent: AtomicU64,
    batches_failed: AtomicU64,
    http_retries: AtomicU64,
}

#[derive(Debug)]
struct QueuedEvent {
    value: Value,
    bytes: usize,
}

async fn receive_udp(
    cfg: Arc<Config>,
    metrics: Arc<Metrics>,
    tx: mpsc::Sender<QueuedEvent>,
) -> Result<()> {
    let socket = UdpSocket::bind(cfg.udp_bind)
        .await
        .with_context(|| format!("bind UDP socket {}", cfg.udp_bind))?;
    let mut buf = vec![0_u8; cfg.udp_max_bytes];

    loop {
        let (len, peer) = socket.recv_from(&mut buf).await.context("UDP recv")?;
        metrics.datagrams.fetch_add(1, Ordering::Relaxed);
        metrics
            .datagram_bytes
            .fetch_add(len as u64, Ordering::Relaxed);

        let value = match serde_json::from_slice::<Value>(&buf[..len]) {
            Ok(value) => value,
            Err(err) => {
                metrics
                    .events_dropped_invalid
                    .fetch_add(1, Ordering::Relaxed);
                warn!(%peer, %err, "dropping invalid JSON datagram");
                continue;
            }
        };

        match prepare_queued_events(value, &cfg.enrichment) {
            Ok(events) => {
                for event in events {
                    match tx.try_send(event) {
                        Ok(()) => {
                            metrics.events_accepted.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(mpsc::error::TrySendError::Full(_)) => {
                            metrics
                                .events_dropped_queue_full
                                .fetch_add(1, Ordering::Relaxed);
                        }
                        Err(mpsc::error::TrySendError::Closed(_)) => return Ok(()),
                    }
                }
            }
            Err(err) => {
                for _ in 0..err.count {
                    metrics
                        .events_dropped_invalid
                        .fetch_add(1, Ordering::Relaxed);
                }
                warn!(%peer, reason = err.reason, "dropping invalid UDP datagram");
            }
        }
    }
}

#[derive(Clone)]
struct HttpIntakeState {
    cfg: Arc<Config>,
    metrics: Arc<Metrics>,
    tx: mpsc::Sender<QueuedEvent>,
}

async fn receive_http_intake(
    cfg: Arc<Config>,
    metrics: Arc<Metrics>,
    tx: mpsc::Sender<QueuedEvent>,
) -> Result<()> {
    let bind = cfg
        .http_bind
        .expect("HTTP intake task is only started when configured");
    let state = HttpIntakeState { cfg, metrics, tx };
    let app = Router::new()
        .route("/events", post(post_sidecar_events))
        .route("/healthz", get(sidecar_healthz))
        .with_state(state);
    let listener = tokio::net::TcpListener::bind(bind)
        .await
        .with_context(|| format!("bind HTTP intake socket {bind}"))?;
    info!(bind = %bind, "nanotrace sidecar HTTP intake listening");
    axum::serve(listener, app)
        .await
        .context("serve HTTP intake")
}

async fn post_sidecar_events(
    State(state): State<HttpIntakeState>,
    request: Request<Body>,
) -> Response {
    let body = match to_bytes(request.into_body(), state.cfg.http_intake_max_bytes).await {
        Ok(body) => body,
        Err(_) => {
            return (StatusCode::PAYLOAD_TOO_LARGE, "request body is too large").into_response();
        }
    };
    state
        .metrics
        .http_intake_requests
        .fetch_add(1, Ordering::Relaxed);
    state
        .metrics
        .http_intake_bytes
        .fetch_add(body.len() as u64, Ordering::Relaxed);

    let value = match serde_json::from_slice::<Value>(&body) {
        Ok(value) => value,
        Err(err) => {
            state
                .metrics
                .events_dropped_invalid
                .fetch_add(1, Ordering::Relaxed);
            return (StatusCode::BAD_REQUEST, format!("invalid JSON body: {err}")).into_response();
        }
    };
    let events = match prepare_queued_events(value, &state.cfg.enrichment) {
        Ok(events) => events,
        Err(err) => {
            for _ in 0..err.count {
                state
                    .metrics
                    .events_dropped_invalid
                    .fetch_add(1, Ordering::Relaxed);
            }
            return (StatusCode::BAD_REQUEST, err.reason).into_response();
        }
    };
    let accepted = events.len();
    let permits = match state.tx.try_reserve_many(accepted) {
        Ok(permits) => permits,
        Err(mpsc::error::TrySendError::Full(_)) => {
            state
                .metrics
                .events_dropped_queue_full
                .fetch_add(accepted as u64, Ordering::Relaxed);
            return (StatusCode::SERVICE_UNAVAILABLE, "sidecar queue is full").into_response();
        }
        Err(mpsc::error::TrySendError::Closed(_)) => {
            return (StatusCode::SERVICE_UNAVAILABLE, "sidecar queue is closed").into_response();
        }
    };
    for (permit, event) in permits.zip(events) {
        permit.send(event);
    }
    state
        .metrics
        .events_accepted
        .fetch_add(accepted as u64, Ordering::Relaxed);

    Json(serde_json::json!({ "accepted": accepted })).into_response()
}

async fn sidecar_healthz() -> Json<Value> {
    Json(serde_json::json!({ "ok": true }))
}

async fn run_batcher(
    cfg: Arc<Config>,
    metrics: Arc<Metrics>,
    http: reqwest::Client,
    mut rx: mpsc::Receiver<QueuedEvent>,
) -> Result<()> {
    let mut batch = Vec::with_capacity(cfg.batch_max_events);
    let mut batch_bytes = 0_usize;
    let mut ticker = interval(cfg.flush_interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            maybe_event = rx.recv() => {
                let Some(event) = maybe_event else {
                    flush_batch(&cfg, &metrics, &http, &mut batch, &mut batch_bytes).await;
                    return Ok(());
                };

                if !batch.is_empty()
                    && (batch.len() >= cfg.batch_max_events
                        || batch_bytes.saturating_add(event.bytes) > cfg.batch_max_bytes)
                {
                    flush_batch(&cfg, &metrics, &http, &mut batch, &mut batch_bytes).await;
                }

                batch_bytes = batch_bytes.saturating_add(event.bytes);
                batch.push(event.value);

                if batch.len() >= cfg.batch_max_events || batch_bytes >= cfg.batch_max_bytes {
                    flush_batch(&cfg, &metrics, &http, &mut batch, &mut batch_bytes).await;
                }
            }
            _ = ticker.tick() => {
                flush_batch(&cfg, &metrics, &http, &mut batch, &mut batch_bytes).await;
            }
        }
    }
}

async fn flush_batch(
    cfg: &Config,
    metrics: &Metrics,
    http: &reqwest::Client,
    batch: &mut Vec<Value>,
    batch_bytes: &mut usize,
) {
    if batch.is_empty() {
        return;
    }

    let events = std::mem::take(batch);
    *batch_bytes = 0;
    let event_count = events.len() as u64;
    let body = if events.len() == 1 {
        events.into_iter().next().unwrap_or(Value::Null)
    } else {
        Value::Array(events)
    };

    match send_with_retry(cfg, metrics, http, &body).await {
        Ok(()) => {
            metrics.batches_sent.fetch_add(1, Ordering::Relaxed);
            metrics
                .events_sent
                .fetch_add(event_count, Ordering::Relaxed);
        }
        Err(err) => {
            metrics.batches_failed.fetch_add(1, Ordering::Relaxed);
            error!(%err, event_count, "dropping batch after retries");
        }
    }
}

async fn send_with_retry(
    cfg: &Config,
    metrics: &Metrics,
    http: &reqwest::Client,
    body: &Value,
) -> Result<()> {
    let mut last_error = None;

    for attempt in 1..=cfg.retry_attempts {
        let result = http
            .post(&cfg.events_url)
            .header(AUTHORIZATION, cfg.auth_header.as_str())
            .json(body)
            .send()
            .await;

        match result {
            Ok(response) if response.status().is_success() => return Ok(()),
            Ok(response) => {
                let status = response.status();
                let text = response.text().await.unwrap_or_default();
                last_error = Some(anyhow::anyhow!("HTTP {status}: {text}"));
            }
            Err(err) => {
                last_error = Some(err.into());
            }
        }

        if attempt < cfg.retry_attempts {
            metrics.http_retries.fetch_add(1, Ordering::Relaxed);
            sleep(cfg.retry_base_delay * attempt as u32).await;
        }
    }

    Err(last_error.unwrap_or_else(|| anyhow::anyhow!("unknown send failure")))
}

async fn log_metrics(metrics: Arc<Metrics>) {
    let mut ticker = interval(Duration::from_secs(10));
    loop {
        ticker.tick().await;
        info!(
            datagrams = metrics.datagrams.load(Ordering::Relaxed),
            datagram_bytes = metrics.datagram_bytes.load(Ordering::Relaxed),
            http_intake_requests = metrics.http_intake_requests.load(Ordering::Relaxed),
            http_intake_bytes = metrics.http_intake_bytes.load(Ordering::Relaxed),
            events_accepted = metrics.events_accepted.load(Ordering::Relaxed),
            events_sent = metrics.events_sent.load(Ordering::Relaxed),
            events_dropped_invalid = metrics.events_dropped_invalid.load(Ordering::Relaxed),
            events_dropped_queue_full = metrics.events_dropped_queue_full.load(Ordering::Relaxed),
            batches_sent = metrics.batches_sent.load(Ordering::Relaxed),
            batches_failed = metrics.batches_failed.load(Ordering::Relaxed),
            http_retries = metrics.http_retries.load(Ordering::Relaxed),
            "client metrics"
        );
    }
}

#[derive(Debug)]
struct InvalidEvents {
    reason: &'static str,
    count: usize,
}

fn prepare_queued_events(
    value: Value,
    enrichment: &BTreeMap<String, Value>,
) -> std::result::Result<Vec<QueuedEvent>, InvalidEvents> {
    let values = expand_events(value);
    if values.is_empty() {
        return Err(InvalidEvents {
            reason: "batch must contain at least one event",
            count: 1,
        });
    }

    let mut events = Vec::with_capacity(values.len());
    for value in values {
        let event = enrich_event(value, enrichment).ok_or(InvalidEvents {
            reason: "event must have non-empty event_id, non-empty timestamp, and object data",
            count: 1,
        })?;
        let bytes = event.to_string().len();
        events.push(QueuedEvent {
            value: event,
            bytes,
        });
    }
    Ok(events)
}

fn expand_events(value: Value) -> Vec<Value> {
    match value {
        Value::Array(values) => values,
        other => vec![other],
    }
}

fn is_event_object(value: &Value) -> bool {
    let Some(object) = value.as_object() else {
        return false;
    };
    object
        .get("event_id")
        .and_then(Value::as_str)
        .is_some_and(|v| !v.is_empty())
        && object
            .get("timestamp")
            .and_then(Value::as_str)
            .is_some_and(|v| !v.is_empty())
        && object.get("data").and_then(Value::as_object).is_some()
}

fn enrich_event(mut event: Value, enrichment: &BTreeMap<String, Value>) -> Option<Value> {
    if !is_event_object(&event) {
        return None;
    }
    let data = event.get_mut("data")?.as_object_mut()?;
    for (key, value) in enrichment {
        data.entry(key.clone()).or_insert_with(|| value.clone());
    }
    Some(event)
}

fn default_enrichment() -> BTreeMap<String, Value> {
    let mut values = BTreeMap::new();
    insert_first_env(
        &mut values,
        "service",
        &[
            "NANOTRACE_SERVICE",
            "OTEL_SERVICE_NAME",
            "DD_SERVICE",
            "SERVICE_NAME",
        ],
    );
    insert_first_env(
        &mut values,
        "environment",
        &["NANOTRACE_ENV", "DD_ENV", "APP_ENV", "NODE_ENV"],
    );
    insert_first_env(
        &mut values,
        "service_version",
        &[
            "NANOTRACE_VERSION",
            "DD_VERSION",
            "SERVICE_VERSION",
            "APP_VERSION",
            "GIT_SHA",
        ],
    );
    insert_first_env(
        &mut values,
        "service.instance.id",
        &[
            "NANOTRACE_INSTANCE_ID",
            "SERVICE_INSTANCE_ID",
            "HOSTNAME",
            "HOST",
        ],
    );
    values
}

fn insert_first_env(values: &mut BTreeMap<String, Value>, field: &str, keys: &[&str]) {
    for key in keys {
        if let Ok(value) = env::var(key)
            && !value.trim().is_empty()
        {
            values.insert(field.to_owned(), Value::String(value));
            return;
        }
    }
}

fn required_env(key: &str) -> Result<String> {
    match env::var(key) {
        Ok(value) if !value.trim().is_empty() => Ok(value),
        _ => bail!("{key} is required"),
    }
}

fn env_or(key: &str, fallback: &str) -> String {
    env::var(key).unwrap_or_else(|_| fallback.to_owned())
}

fn optional_bind_env(key: &str, fallback: &str) -> Result<Option<SocketAddr>> {
    let value = env_or(key, fallback);
    let value = value.trim();
    match value.to_ascii_lowercase().as_str() {
        "" | "0" | "false" | "off" | "disabled" => Ok(None),
        _ => value
            .parse()
            .map(Some)
            .with_context(|| format!("{key} must be host:port, empty, false, off, or disabled")),
    }
}

fn usize_env(key: &str, fallback: usize) -> Result<usize> {
    parse_env(key, fallback)
}

fn u64_env(key: &str, fallback: u64) -> Result<u64> {
    parse_env(key, fallback)
}

fn parse_env<T>(key: &str, fallback: T) -> Result<T>
where
    T: std::str::FromStr,
    T::Err: std::error::Error + Send + Sync + 'static,
{
    match env::var(key) {
        Ok(value) if !value.trim().is_empty() => value
            .parse()
            .with_context(|| format!("{key} has invalid value {value:?}")),
        _ => Ok(fallback),
    }
}

fn trim_trailing_slash(value: String) -> String {
    value.trim_end_matches('/').to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn accepts_event_object_shape() {
        assert!(is_event_object(&json!({
            "event_id": "evt_1",
            "timestamp": "2026-05-11T00:00:00Z",
            "data": {}
        })));
    }

    #[test]
    fn rejects_missing_data_object() {
        assert!(!is_event_object(&json!({
            "event_id": "evt_1",
            "timestamp": "2026-05-11T00:00:00Z",
            "data": []
        })));
    }

    #[test]
    fn expands_batch_datagram() {
        let events = expand_events(json!([
            {"event_id":"a","timestamp":"t","data":{}},
            {"event_id":"b","timestamp":"t","data":{}}
        ]));
        assert_eq!(events.len(), 2);
    }

    #[test]
    fn enriches_missing_data_fields_without_overwriting() {
        let mut enrichment = BTreeMap::new();
        enrichment.insert("service".to_owned(), Value::String("from-env".to_owned()));
        enrichment.insert("environment".to_owned(), Value::String("prod".to_owned()));

        let event = enrich_event(
            json!({
                "event_id": "evt_1",
                "timestamp": "2026-05-11T00:00:00Z",
                "data": {"service": "from-app"}
            }),
            &enrichment,
        )
        .expect("valid event");

        assert_eq!(event["data"]["service"], "from-app");
        assert_eq!(event["data"]["environment"], "prod");
    }

    #[test]
    fn prepares_valid_event_batch() {
        let events = prepare_queued_events(
            json!([
                {"event_id":"a","timestamp":"t","data":{}},
                {"event_id":"b","timestamp":"t","data":{}}
            ]),
            &BTreeMap::new(),
        )
        .expect("valid batch");

        assert_eq!(events.len(), 2);
    }

    #[test]
    fn rejects_empty_batch() {
        let err = prepare_queued_events(json!([]), &BTreeMap::new()).expect_err("empty batch");

        assert_eq!(err.reason, "batch must contain at least one event");
    }
}
