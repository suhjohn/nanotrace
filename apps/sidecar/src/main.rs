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
    env, fs,
    net::SocketAddr,
    path::{Path, PathBuf},
    process,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
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
    if let Some(spool_dir) = cfg.spool_dir.as_deref() {
        fs::create_dir_all(spool_dir)
            .with_context(|| format!("create sidecar spool dir {}", spool_dir.display()))?;
        recover_spool_dir(spool_dir)
            .with_context(|| format!("recover sidecar spool dir {}", spool_dir.display()))?;
    }

    info!(
        udp_bind = %cfg.udp_bind,
        http_bind = cfg.http_bind.map(|bind| bind.to_string()).unwrap_or_else(|| "disabled".to_owned()),
        url = %cfg.events_url,
        batch_max_events = cfg.batch_max_events,
        batch_max_bytes = cfg.batch_max_bytes,
        flush_ms = cfg.flush_interval.as_millis(),
        queue_capacity = cfg.queue_capacity,
        spool_dir = cfg.spool_dir.as_ref().map(|dir| dir.display().to_string()).unwrap_or_else(|| "disabled".to_owned()),
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
            tx.clone(),
        )),
        None => tokio::spawn(async { std::future::pending::<Result<()>>().await }),
    };
    let batch_task = tokio::spawn(run_batcher(
        Arc::clone(&cfg),
        Arc::clone(&metrics),
        http,
        rx,
    ));
    let spool_task = match cfg.spool_dir.is_some() {
        true => tokio::spawn(run_spool_replayer(
            Arc::clone(&cfg),
            Arc::clone(&metrics),
            tx.clone(),
        )),
        false => tokio::spawn(async { std::future::pending::<Result<()>>().await }),
    };
    let metrics_task = tokio::spawn(log_metrics(Arc::clone(&metrics)));

    tokio::select! {
        result = udp_task => result.context("UDP task join failed")??,
        result = http_intake_task => result.context("HTTP intake task join failed")??,
        result = batch_task => result.context("batch task join failed")??,
        result = spool_task => result.context("spool task join failed")??,
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
    spool_dir: Option<PathBuf>,
    spool_poll_interval: Duration,
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
        let spool_dir = optional_path_env("NANOTRACE_CLIENT_SPOOL_DIR");
        let spool_poll_ms = u64_env("NANOTRACE_CLIENT_SPOOL_POLL_MS", 1_000)?;
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
        if spool_poll_ms == 0 {
            bail!("NANOTRACE_CLIENT_SPOOL_POLL_MS must be greater than zero");
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
            events_url: format!("{}/v1/events", trim_trailing_slash(url)),
            auth_header: format!("Bearer {key}"),
            enrichment: default_enrichment(),
            batch_max_events,
            batch_max_bytes,
            flush_interval: Duration::from_millis(flush_ms),
            queue_capacity,
            spool_dir,
            spool_poll_interval: Duration::from_millis(spool_poll_ms),
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
    events_spooled: AtomicU64,
    events_spool_replayed: AtomicU64,
    spool_errors: AtomicU64,
    batches_sent: AtomicU64,
    batches_failed: AtomicU64,
    http_retries: AtomicU64,
}

#[derive(Debug)]
struct QueuedEvent {
    value: Value,
    bytes: usize,
    spool_path: Option<PathBuf>,
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
                if let Err(err) = accept_events(&cfg, &metrics, &tx, events, false) {
                    match err {
                        AcceptEventsError::QueueClosed => return Ok(()),
                        AcceptEventsError::QueueFull(count) => {
                            metrics
                                .events_dropped_queue_full
                                .fetch_add(count as u64, Ordering::Relaxed);
                        }
                        AcceptEventsError::Spool(err) => {
                            metrics.spool_errors.fetch_add(1, Ordering::Relaxed);
                            warn!(%peer, %err, "dropping UDP event after spool write failed");
                        }
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
    let accepted = match accept_events(&state.cfg, &state.metrics, &state.tx, events, true) {
        Ok(accepted) => accepted,
        Err(AcceptEventsError::QueueFull(count)) => {
            state
                .metrics
                .events_dropped_queue_full
                .fetch_add(count as u64, Ordering::Relaxed);
            return (StatusCode::SERVICE_UNAVAILABLE, "sidecar queue is full").into_response();
        }
        Err(AcceptEventsError::QueueClosed) => {
            return (StatusCode::SERVICE_UNAVAILABLE, "sidecar queue is closed").into_response();
        }
        Err(AcceptEventsError::Spool(err)) => {
            state.metrics.spool_errors.fetch_add(1, Ordering::Relaxed);
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                format!("sidecar spool write failed: {err}"),
            )
                .into_response();
        }
    };

    Json(serde_json::json!({ "accepted": accepted })).into_response()
}

async fn sidecar_healthz() -> Json<Value> {
    Json(serde_json::json!({ "ok": true }))
}

#[derive(Debug)]
enum AcceptEventsError {
    QueueFull(usize),
    QueueClosed,
    Spool(anyhow::Error),
}

fn accept_events(
    cfg: &Config,
    metrics: &Metrics,
    tx: &mpsc::Sender<QueuedEvent>,
    events: Vec<QueuedEvent>,
    reject_queue_full: bool,
) -> std::result::Result<usize, AcceptEventsError> {
    let accepted = events.len();
    if let Some(spool_dir) = cfg.spool_dir.as_deref() {
        for event in events {
            persist_spooled_event(spool_dir, &event.value).map_err(AcceptEventsError::Spool)?;
        }
        metrics
            .events_spooled
            .fetch_add(accepted as u64, Ordering::Relaxed);
        metrics
            .events_accepted
            .fetch_add(accepted as u64, Ordering::Relaxed);
        return Ok(accepted);
    }

    if reject_queue_full {
        let permits = tx.try_reserve_many(accepted).map_err(|err| match err {
            mpsc::error::TrySendError::Full(_) => AcceptEventsError::QueueFull(accepted),
            mpsc::error::TrySendError::Closed(_) => AcceptEventsError::QueueClosed,
        })?;
        for (permit, event) in permits.zip(events) {
            permit.send(event);
        }
        metrics
            .events_accepted
            .fetch_add(accepted as u64, Ordering::Relaxed);
        return Ok(accepted);
    }

    let mut accepted_count = 0_usize;
    let mut dropped_count = 0_usize;
    for event in events {
        match tx.try_send(event) {
            Ok(()) => accepted_count += 1,
            Err(mpsc::error::TrySendError::Full(_)) => dropped_count += 1,
            Err(mpsc::error::TrySendError::Closed(_)) => {
                return Err(AcceptEventsError::QueueClosed);
            }
        }
    }
    if accepted_count > 0 {
        metrics
            .events_accepted
            .fetch_add(accepted_count as u64, Ordering::Relaxed);
    }
    if dropped_count > 0 {
        return Err(AcceptEventsError::QueueFull(dropped_count));
    }
    Ok(accepted_count)
}

async fn run_batcher(
    cfg: Arc<Config>,
    metrics: Arc<Metrics>,
    http: reqwest::Client,
    mut rx: mpsc::Receiver<QueuedEvent>,
) -> Result<()> {
    let mut batch: Vec<QueuedEvent> = Vec::with_capacity(cfg.batch_max_events);
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
                batch.push(event);

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
    batch: &mut Vec<QueuedEvent>,
    batch_bytes: &mut usize,
) {
    if batch.is_empty() {
        return;
    }

    let events = std::mem::take(batch);
    *batch_bytes = 0;
    let event_count = events.len() as u64;
    let body = if events.len() == 1 {
        events
            .first()
            .map(|event| event.value.clone())
            .unwrap_or(Value::Null)
    } else {
        Value::Array(events.iter().map(|event| event.value.clone()).collect())
    };

    match send_with_retry(cfg, metrics, http, &body).await {
        Ok(()) => {
            delete_spooled_events(&events, metrics);
            metrics.batches_sent.fetch_add(1, Ordering::Relaxed);
            metrics
                .events_sent
                .fetch_add(event_count, Ordering::Relaxed);
        }
        Err(err) => {
            restore_spooled_events(&events, metrics);
            metrics.batches_failed.fetch_add(1, Ordering::Relaxed);
            error!(%err, event_count, "dropping batch after retries");
        }
    }
}

async fn run_spool_replayer(
    cfg: Arc<Config>,
    metrics: Arc<Metrics>,
    tx: mpsc::Sender<QueuedEvent>,
) -> Result<()> {
    let Some(spool_dir) = cfg.spool_dir.as_deref() else {
        return Ok(());
    };
    let mut ticker = interval(cfg.spool_poll_interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

    loop {
        replay_spool_once(spool_dir, &metrics, &tx).await?;
        ticker.tick().await;
    }
}

async fn replay_spool_once(
    spool_dir: &Path,
    metrics: &Metrics,
    tx: &mpsc::Sender<QueuedEvent>,
) -> Result<()> {
    let mut paths = ready_spool_files(spool_dir)?;
    paths.sort();
    for path in paths {
        match take_spooled_event(&path) {
            Ok(event) => {
                metrics
                    .events_spool_replayed
                    .fetch_add(1, Ordering::Relaxed);
                if tx.send(event).await.is_err() {
                    return Ok(());
                }
            }
            Err(err) => {
                metrics.spool_errors.fetch_add(1, Ordering::Relaxed);
                warn!(path = %path.display(), %err, "quarantining unreadable sidecar spool file");
                quarantine_spool_file(&path, metrics);
                quarantine_spool_file(&path.with_extension("inflight"), metrics);
            }
        }
    }
    Ok(())
}

static SPOOL_SEQUENCE: AtomicU64 = AtomicU64::new(0);

fn persist_spooled_event(spool_dir: &Path, value: &Value) -> Result<PathBuf> {
    fs::create_dir_all(spool_dir)
        .with_context(|| format!("create sidecar spool dir {}", spool_dir.display()))?;
    let body = serde_json::to_vec(value).context("serialize sidecar spool event")?;
    for _ in 0..10 {
        let sequence = SPOOL_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let file_name = format!("event-{now}-{}-{sequence}.json", process::id());
        let ready_path = spool_dir.join(file_name);
        let tmp_path = ready_path.with_extension("tmp");
        match fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&tmp_path)
        {
            Ok(mut file) => {
                use std::io::Write;
                file.write_all(&body)
                    .with_context(|| format!("write sidecar spool file {}", tmp_path.display()))?;
                file.write_all(b"\n")
                    .with_context(|| format!("write sidecar spool file {}", tmp_path.display()))?;
                file.sync_all()
                    .with_context(|| format!("sync sidecar spool file {}", tmp_path.display()))?;
                drop(file);
                fs::rename(&tmp_path, &ready_path).with_context(|| {
                    format!(
                        "publish sidecar spool file {} to {}",
                        tmp_path.display(),
                        ready_path.display()
                    )
                })?;
                return Ok(ready_path);
            }
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(err) => {
                return Err(err)
                    .with_context(|| format!("create sidecar spool file {}", tmp_path.display()));
            }
        }
    }
    bail!("failed to allocate unique sidecar spool file name")
}

fn recover_spool_dir(spool_dir: &Path) -> Result<()> {
    if !spool_dir.exists() {
        return Ok(());
    }
    for entry in fs::read_dir(spool_dir)
        .with_context(|| format!("read sidecar spool dir {}", spool_dir.display()))?
    {
        let path = entry?.path();
        if path.extension().and_then(|ext| ext.to_str()) == Some("inflight") {
            let ready = path.with_extension("json");
            fs::rename(&path, &ready).with_context(|| {
                format!(
                    "recover sidecar spool file {} to {}",
                    path.display(),
                    ready.display()
                )
            })?;
        }
    }
    Ok(())
}

fn ready_spool_files(spool_dir: &Path) -> Result<Vec<PathBuf>> {
    if !spool_dir.exists() {
        return Ok(Vec::new());
    }
    let mut paths = Vec::new();
    for entry in fs::read_dir(spool_dir)
        .with_context(|| format!("read sidecar spool dir {}", spool_dir.display()))?
    {
        let path = entry?.path();
        if path.extension().and_then(|ext| ext.to_str()) == Some("json") {
            paths.push(path);
        }
    }
    Ok(paths)
}

fn take_spooled_event(path: &Path) -> Result<QueuedEvent> {
    let inflight = path.with_extension("inflight");
    fs::rename(path, &inflight).with_context(|| {
        format!(
            "claim sidecar spool file {} as {}",
            path.display(),
            inflight.display()
        )
    })?;
    let body = fs::read(&inflight)
        .with_context(|| format!("read sidecar spool file {}", inflight.display()))?;
    let value: Value = serde_json::from_slice(&body)
        .with_context(|| format!("parse sidecar spool file {}", inflight.display()))?;
    Ok(QueuedEvent {
        value,
        bytes: body.len(),
        spool_path: Some(inflight),
    })
}

fn delete_spooled_events(events: &[QueuedEvent], metrics: &Metrics) {
    for event in events {
        let Some(path) = event.spool_path.as_deref() else {
            continue;
        };
        if let Err(err) = fs::remove_file(path) {
            metrics.spool_errors.fetch_add(1, Ordering::Relaxed);
            warn!(path = %path.display(), %err, "failed to delete sent sidecar spool file");
        }
    }
}

fn restore_spooled_events(events: &[QueuedEvent], metrics: &Metrics) {
    for event in events {
        let Some(path) = event.spool_path.as_deref() else {
            continue;
        };
        let ready = path.with_extension("json");
        if let Err(err) = fs::rename(path, &ready) {
            metrics.spool_errors.fetch_add(1, Ordering::Relaxed);
            warn!(
                path = %path.display(),
                ready = %ready.display(),
                %err,
                "failed to restore sidecar spool file after send failure"
            );
        }
    }
}

fn quarantine_spool_file(path: &Path, metrics: &Metrics) {
    if !path.exists() {
        return;
    }
    let bad = path.with_extension("bad");
    if let Err(err) = fs::rename(path, &bad) {
        metrics.spool_errors.fetch_add(1, Ordering::Relaxed);
        warn!(
            path = %path.display(),
            bad = %bad.display(),
            %err,
            "failed to quarantine sidecar spool file"
        );
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
            events_spooled = metrics.events_spooled.load(Ordering::Relaxed),
            events_spool_replayed = metrics.events_spool_replayed.load(Ordering::Relaxed),
            spool_errors = metrics.spool_errors.load(Ordering::Relaxed),
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
            spool_path: None,
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

fn optional_path_env(key: &str) -> Option<PathBuf> {
    env::var(key)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
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

    #[test]
    fn spooled_event_round_trips_through_claim_restore_and_delete() {
        let dir = test_spool_dir("round-trip");
        let value = json!({"event_id":"evt_1","timestamp":"t","data":{"service":"api"}});
        let ready = persist_spooled_event(&dir, &value).expect("persist spool file");

        assert_eq!(
            ready_spool_files(&dir).expect("ready files"),
            vec![ready.clone()]
        );

        let claimed = take_spooled_event(&ready).expect("claim spool file");
        let inflight = claimed.spool_path.clone().expect("inflight path");
        assert_eq!(claimed.value, value);
        assert!(!ready.exists());
        assert!(inflight.exists());

        let metrics = Metrics::default();
        restore_spooled_events(&[claimed], &metrics);
        assert!(ready.exists());

        let claimed = take_spooled_event(&ready).expect("claim restored spool file");
        let inflight = claimed.spool_path.clone().expect("inflight path");
        delete_spooled_events(&[claimed], &metrics);
        assert!(!inflight.exists());
        fs::remove_dir_all(&dir).expect("remove test spool dir");
    }

    #[test]
    fn recover_spool_dir_returns_inflight_files_to_ready() {
        let dir = test_spool_dir("recover");
        let value = json!({"event_id":"evt_1","timestamp":"t","data":{}});
        let ready = persist_spooled_event(&dir, &value).expect("persist spool file");
        let claimed = take_spooled_event(&ready).expect("claim spool file");
        let inflight = claimed.spool_path.expect("inflight path");

        assert!(inflight.exists());
        recover_spool_dir(&dir).expect("recover spool dir");
        assert!(ready.exists());
        assert!(!inflight.exists());
        fs::remove_dir_all(&dir).expect("remove test spool dir");
    }

    fn test_spool_dir(name: &str) -> PathBuf {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let dir = env::temp_dir().join(format!("nanotrace-sidecar-{name}-{}-{now}", process::id()));
        fs::create_dir_all(&dir).expect("create test spool dir");
        dir
    }
}
