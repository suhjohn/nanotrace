use anyhow::{Context, Result, anyhow, bail};
use chrono::{DateTime, Datelike, Duration as ChronoDuration, Utc};
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE};
use serde_json::{Map, Value, json};
use std::{
    collections::HashMap,
    env, fs,
    path::{Path, PathBuf},
    process,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    process::Command,
    sync::mpsc,
    time::{Instant, sleep, sleep_until},
};
use uuid::Uuid;

#[tokio::main]
async fn main() -> Result<()> {
    let root = repo_root();
    if let Ok(env_file) = env::var("NANOTRACE_ENV_FILE") {
        load_env_file(&root.join(env_file))?;
    }

    let mut outputs = None;
    let ingest_url = trim_trailing_slash(match env::var("NANOTRACE_INGEST_URL") {
        Ok(value) => value,
        Err(_) => {
            let loaded_outputs = pulumi_outputs(&root).await?;
            let value = loaded_outputs
                .get("ingestUrl")
                .and_then(Value::as_str)
                .map(str::to_owned)
                .ok_or_else(|| anyhow!("Pulumi output ingestUrl is required"))?;
            outputs = Some(loaded_outputs);
            value
        }
    });
    let api_key = required_env("NANOTRACE_API_KEY")?;
    let fixtures = Arc::new(load_fixtures(&root)?);
    let clickhouse = clickhouse_config(outputs.as_ref())?;

    let config = Arc::new(Config {
        ingest_url,
        api_key,
        run_id: env::var("NANOTRACE_LOADTEST_RUN_ID").unwrap_or_else(|_| default_run_id()),
        batch_sizes: list_env("NANOTRACE_LOADTEST_BATCH_SIZES", &[1, 10, 100])?,
        step_seconds: number_env("NANOTRACE_LOADTEST_STEP_SECONDS", 30.0)?,
        cooldown_ms: number_env("NANOTRACE_LOADTEST_COOLDOWN_MS", 2_000.0)?,
        start_rps: integer_env("NANOTRACE_LOADTEST_START_RPS", 1)?,
        max_rps: integer_env("NANOTRACE_LOADTEST_MAX_RPS", 2_000)?,
        binary_rounds: integer_env("NANOTRACE_LOADTEST_BINARY_ROUNDS", 6)?,
        max_error_rate: number_env("NANOTRACE_LOADTEST_MAX_ERROR_RATE", 0.01)?,
        max_p95_ms: number_env("NANOTRACE_LOADTEST_MAX_P95_MS", 2_000.0)?,
        max_in_flight: integer_env("NANOTRACE_LOADTEST_MAX_IN_FLIGHT", 2_000)?,
        log_ratio: number_env("NANOTRACE_LOADTEST_LOG_RATIO", 0.10)?,
        profile: load_profile_env()?,
        trace_depth: integer_env("NANOTRACE_LOADTEST_TRACE_DEPTH", 96)?,
        total_events: optional_integer_env("NANOTRACE_LOADTEST_TOTAL_EVENTS")?,
        sequence_offset: integer_env_allow_zero("NANOTRACE_LOADTEST_SEQUENCE_OFFSET", 0)?,
        sequence_stride: integer_env("NANOTRACE_LOADTEST_SEQUENCE_STRIDE", 1)?,
        clickhouse_wait_ms: number_env("NANOTRACE_LOADTEST_CLICKHOUSE_WAIT_MS", 300_000.0)?,
        clickhouse_poll_ms: number_env("NANOTRACE_LOADTEST_CLICKHOUSE_POLL_MS", 5_000.0)?,
        event_seq: AtomicU64::new(0),
        accepted_events: AtomicU64::new(0),
    });

    if !(0.0..=1.0).contains(&config.log_ratio) {
        bail!("NANOTRACE_LOADTEST_LOG_RATIO must be between 0 and 1");
    }

    let client = reqwest::Client::builder()
        .pool_idle_timeout(Duration::from_secs(90))
        .build()
        .context("build HTTP client")?;
    let context = Arc::new(LoadContext {
        config,
        fixtures,
        client,
    });

    println!("ingestUrl={}", context.config.ingest_url);
    println!("runId={}", context.config.run_id);
    println!(
        "batchSizes={}",
        context
            .config
            .batch_sizes
            .iter()
            .map(u64::to_string)
            .collect::<Vec<_>>()
            .join(",")
    );
    println!("stepSeconds={}", context.config.step_seconds);
    println!("profile={}", context.config.profile.as_str());
    println!(
        "successCriteria=errorRate<={},p95Ms<={}",
        context.config.max_error_rate, context.config.max_p95_ms
    );

    if let Some(total_events) = context.config.total_events {
        let batch_size = *context
            .config
            .batch_sizes
            .first()
            .ok_or_else(|| anyhow!("at least one batch size is required"))?;
        let stats = run_fixed_events(Arc::clone(&context), batch_size, total_events).await?;
        print_step(&stats);

        let accepted_events = context.config.accepted_events.load(Ordering::Relaxed);
        println!("acceptedEvents={accepted_events}");
        if let Some(clickhouse) = clickhouse {
            wait_for_clickhouse(&context, &clickhouse, accepted_events).await?;
        } else {
            println!(
                "clickhouseObservation=skipped reason=CLICKHOUSE_URL,CLICKHOUSE_USER,CLICKHOUSE_PASSWORD not all set"
            );
        }
        return Ok(());
    }

    let mut summary = Vec::new();
    for &batch_size in &context.config.batch_sizes {
        println!("\n=== batchSize={batch_size} events/request ===");
        summary.push(find_max_rps(Arc::clone(&context), batch_size).await?);
    }

    println!("\n=== summary ===");
    for result in summary {
        println!(
            "batchSize={} maxReqPerSec={} maxEventPerSec={} bodyBytesPerSec={} bodyMiBPerSec={} avgBodyBytesPerReq={} p50Ms={} p95Ms={} p99Ms={} errorRate={}",
            result.batch_size,
            result.max_req_per_sec,
            result.max_req_per_sec * result.batch_size,
            result.stats.achieved_body_bytes_per_sec,
            result.stats.achieved_body_mib_per_sec,
            result.stats.average_body_bytes_per_request,
            result.stats.p50_ms,
            result.stats.p95_ms,
            result.stats.p99_ms,
            result.stats.error_rate,
        );
    }

    let accepted_events = context.config.accepted_events.load(Ordering::Relaxed);
    println!("acceptedEvents={accepted_events}");
    if let Some(clickhouse) = clickhouse {
        wait_for_clickhouse(&context, &clickhouse, accepted_events).await?;
    } else {
        println!(
            "clickhouseObservation=skipped reason=CLICKHOUSE_URL,CLICKHOUSE_USER,CLICKHOUSE_PASSWORD not all set"
        );
    }

    Ok(())
}

struct Config {
    ingest_url: String,
    api_key: String,
    run_id: String,
    batch_sizes: Vec<u64>,
    step_seconds: f64,
    cooldown_ms: f64,
    start_rps: u64,
    max_rps: u64,
    binary_rounds: u64,
    max_error_rate: f64,
    max_p95_ms: f64,
    max_in_flight: u64,
    log_ratio: f64,
    profile: LoadProfile,
    trace_depth: u64,
    total_events: Option<u64>,
    sequence_offset: u64,
    sequence_stride: u64,
    clickhouse_wait_ms: f64,
    clickhouse_poll_ms: f64,
    event_seq: AtomicU64,
    accepted_events: AtomicU64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LoadProfile {
    Atlas,
    Codex,
    Fixture,
    Realistic,
    Llm,
    Trace,
    Metrics,
    Logs,
    Product,
    Agent,
    Pipeline,
}

impl LoadProfile {
    fn as_str(self) -> &'static str {
        match self {
            Self::Atlas => "atlas",
            Self::Codex => "codex",
            Self::Fixture => "fixture",
            Self::Realistic => "realistic",
            Self::Llm => "llm",
            Self::Trace => "trace",
            Self::Metrics => "metrics",
            Self::Logs => "logs",
            Self::Product => "product",
            Self::Agent => "agent",
            Self::Pipeline => "pipeline",
        }
    }

    fn is_realistic(self) -> bool {
        matches!(
            self,
            Self::Atlas
                | Self::Codex
                | Self::Realistic
                | Self::Llm
                | Self::Trace
                | Self::Metrics
                | Self::Logs
                | Self::Product
                | Self::Agent
                | Self::Pipeline
        )
    }
}

struct ClickHouseConfig {
    url: String,
    user: String,
    password: String,
    database: String,
    table: String,
}

struct LoadContext {
    config: Arc<Config>,
    fixtures: Arc<Fixtures>,
    client: reqwest::Client,
}

struct Fixtures {
    log: Fixture,
    log_variations: Vec<Fixture>,
    metric_fixtures: Vec<Fixture>,
    trace_fixtures: Vec<Fixture>,
    product_fixtures: Vec<Fixture>,
    agent_fixtures: Vec<Fixture>,
    pipeline_fixtures: Vec<Fixture>,
    rest: Vec<Fixture>,
}

#[derive(Clone)]
struct Fixture {
    name: String,
    body: Value,
}

struct SearchResult {
    batch_size: u64,
    max_req_per_sec: u64,
    stats: StepStats,
}

#[derive(Clone)]
struct StepStats {
    batch_size: u64,
    target_rps: u64,
    attempted: u64,
    ok: u64,
    failed: u64,
    accepted_events: u64,
    attempted_body_bytes: u64,
    accepted_body_bytes: u64,
    error_rate: f64,
    achieved_req_per_sec: f64,
    achieved_event_per_sec: f64,
    achieved_body_bytes_per_sec: f64,
    achieved_body_mib_per_sec: f64,
    average_body_bytes_per_request: f64,
    p50_ms: f64,
    p95_ms: f64,
    p99_ms: f64,
    max_observed_in_flight: u64,
    statuses: HashMap<String, u64>,
}

struct RequestResult {
    status: String,
    ok: bool,
    latency_ms: f64,
    body_bytes: u64,
}

async fn find_max_rps(context: Arc<LoadContext>, batch_size: u64) -> Result<SearchResult> {
    let mut low = 0;
    let mut high = context.config.start_rps;
    let mut best = None;

    while high <= context.config.max_rps {
        let stats = run_step(Arc::clone(&context), batch_size, high).await?;
        print_step(&stats);
        if !is_pass(&context.config, &stats) {
            break;
        }
        best = Some(stats);
        low = high;
        high = high.saturating_mul(2);
        sleep(Duration::from_secs_f64(
            context.config.cooldown_ms / 1_000.0,
        ))
        .await;
    }

    high = high.min(context.config.max_rps);
    for _ in 0..context.config.binary_rounds {
        if high.saturating_sub(low) <= 1 {
            break;
        }
        let target = (low + high) / 2;
        let stats = run_step(Arc::clone(&context), batch_size, target).await?;
        print_step(&stats);
        if is_pass(&context.config, &stats) {
            low = target;
            best = Some(stats);
        } else {
            high = target;
        }
        sleep(Duration::from_secs_f64(
            context.config.cooldown_ms / 1_000.0,
        ))
        .await;
    }

    let stats = match best {
        Some(stats) => stats,
        None => {
            let stats = run_step(
                Arc::clone(&context),
                batch_size,
                low.max(context.config.start_rps),
            )
            .await?;
            print_step(&stats);
            stats
        }
    };

    Ok(SearchResult {
        batch_size,
        max_req_per_sec: stats.target_rps,
        stats,
    })
}

async fn run_step(
    context: Arc<LoadContext>,
    batch_size: u64,
    target_rps: u64,
) -> Result<StepStats> {
    if target_rps == 0 {
        bail!("target_rps must be positive");
    }

    let started_at = Instant::now();
    let deadline = started_at + Duration::from_secs_f64(context.config.step_seconds);
    let interval = Duration::from_secs_f64(1.0 / target_rps as f64);
    let (tx, mut rx) = mpsc::unbounded_channel();

    let mut attempted = 0;
    let mut failed = 0;
    let mut in_flight = 0;
    let mut max_observed_in_flight = 0;
    let mut latencies = Vec::new();
    let mut statuses = HashMap::new();
    let mut attempted_body_bytes = 0;
    let mut accepted_body_bytes = 0;
    let mut next_at = started_at;

    while Instant::now() < deadline {
        drain_results(
            &mut rx,
            &mut in_flight,
            &mut failed,
            &mut latencies,
            &mut statuses,
            &mut attempted_body_bytes,
            &mut accepted_body_bytes,
        );

        let now = Instant::now();
        if next_at > now {
            sleep_until(next_at).await;
        }
        next_at += interval;

        if in_flight >= context.config.max_in_flight {
            failed += 1;
            *statuses
                .entry("client_backpressure".to_owned())
                .or_insert(0) += 1;
            continue;
        }

        attempted += 1;
        in_flight += 1;
        max_observed_in_flight = max_observed_in_flight.max(in_flight);

        let tx = tx.clone();
        let context = Arc::clone(&context);
        tokio::spawn(async move {
            let result = post_batch(context, batch_size).await;
            let _ = tx.send(result);
        });
    }
    drop(tx);

    while in_flight > 0 {
        if let Some(result) = rx.recv().await {
            record_result(
                result,
                &mut in_flight,
                &mut failed,
                &mut latencies,
                &mut statuses,
                &mut attempted_body_bytes,
                &mut accepted_body_bytes,
            );
        } else {
            break;
        }
    }

    let elapsed_seconds = started_at.elapsed().as_secs_f64();
    let ok = statuses
        .iter()
        .filter_map(|(status, count)| status.parse::<u16>().ok().map(|code| (code, count)))
        .filter(|(code, _)| (200..300).contains(code))
        .map(|(_, count)| *count)
        .sum::<u64>();
    let accepted_events = ok * batch_size;
    context
        .config
        .accepted_events
        .fetch_add(accepted_events, Ordering::Relaxed);
    let total = ok + failed;

    Ok(StepStats {
        batch_size,
        target_rps,
        attempted,
        ok,
        failed,
        accepted_events,
        attempted_body_bytes,
        accepted_body_bytes,
        error_rate: if total == 0 {
            1.0
        } else {
            round(failed as f64 / total as f64, 4)
        },
        achieved_req_per_sec: round(ok as f64 / elapsed_seconds, 2),
        achieved_event_per_sec: round((ok * batch_size) as f64 / elapsed_seconds, 2),
        achieved_body_bytes_per_sec: round(accepted_body_bytes as f64 / elapsed_seconds, 2),
        achieved_body_mib_per_sec: round(
            accepted_body_bytes as f64 / elapsed_seconds / 1024.0 / 1024.0,
            3,
        ),
        average_body_bytes_per_request: if ok == 0 {
            0.0
        } else {
            round(accepted_body_bytes as f64 / ok as f64, 1)
        },
        p50_ms: percentile(&latencies, 0.50),
        p95_ms: percentile(&latencies, 0.95),
        p99_ms: percentile(&latencies, 0.99),
        max_observed_in_flight,
        statuses,
    })
}

async fn run_fixed_events(
    context: Arc<LoadContext>,
    batch_size: u64,
    total_events: u64,
) -> Result<StepStats> {
    if batch_size == 0 {
        bail!("batch_size must be positive");
    }
    if total_events == 0 {
        bail!("total_events must be positive");
    }

    let started_at = Instant::now();
    let mut attempted = 0;
    let mut failed = 0;
    let mut ok = 0;
    let mut accepted_events = 0;
    let mut latencies = Vec::new();
    let mut statuses = HashMap::new();
    let mut attempted_body_bytes = 0;
    let mut accepted_body_bytes = 0;
    let mut remaining = total_events;

    while remaining > 0 {
        let request_events = remaining.min(batch_size);
        attempted += 1;
        let result = post_batch(Arc::clone(&context), request_events).await;
        latencies.push(result.latency_ms);
        *statuses.entry(result.status).or_insert(0) += 1;
        attempted_body_bytes += result.body_bytes;
        if result.ok {
            ok += 1;
            accepted_events += request_events;
            accepted_body_bytes += result.body_bytes;
        } else {
            failed += 1;
        }
        remaining -= request_events;
    }

    context
        .config
        .accepted_events
        .fetch_add(accepted_events, Ordering::Relaxed);

    let elapsed_seconds = started_at.elapsed().as_secs_f64().max(0.001);
    let total = ok + failed;
    Ok(StepStats {
        batch_size,
        target_rps: 0,
        attempted,
        ok,
        failed,
        accepted_events,
        attempted_body_bytes,
        accepted_body_bytes,
        error_rate: if total == 0 {
            1.0
        } else {
            round(failed as f64 / total as f64, 4)
        },
        achieved_req_per_sec: round(ok as f64 / elapsed_seconds, 2),
        achieved_event_per_sec: round(accepted_events as f64 / elapsed_seconds, 2),
        achieved_body_bytes_per_sec: round(accepted_body_bytes as f64 / elapsed_seconds, 2),
        achieved_body_mib_per_sec: round(
            accepted_body_bytes as f64 / elapsed_seconds / 1024.0 / 1024.0,
            3,
        ),
        average_body_bytes_per_request: if ok == 0 {
            0.0
        } else {
            round(accepted_body_bytes as f64 / ok as f64, 1)
        },
        p50_ms: percentile(&latencies, 0.50),
        p95_ms: percentile(&latencies, 0.95),
        p99_ms: percentile(&latencies, 0.99),
        max_observed_in_flight: 1,
        statuses,
    })
}

fn drain_results(
    rx: &mut mpsc::UnboundedReceiver<RequestResult>,
    in_flight: &mut u64,
    failed: &mut u64,
    latencies: &mut Vec<f64>,
    statuses: &mut HashMap<String, u64>,
    attempted_body_bytes: &mut u64,
    accepted_body_bytes: &mut u64,
) {
    while let Ok(result) = rx.try_recv() {
        record_result(
            result,
            in_flight,
            failed,
            latencies,
            statuses,
            attempted_body_bytes,
            accepted_body_bytes,
        );
    }
}

fn record_result(
    result: RequestResult,
    in_flight: &mut u64,
    failed: &mut u64,
    latencies: &mut Vec<f64>,
    statuses: &mut HashMap<String, u64>,
    attempted_body_bytes: &mut u64,
    accepted_body_bytes: &mut u64,
) {
    *in_flight = in_flight.saturating_sub(1);
    latencies.push(result.latency_ms);
    *statuses.entry(result.status).or_insert(0) += 1;
    *attempted_body_bytes += result.body_bytes;
    if result.ok {
        *accepted_body_bytes += result.body_bytes;
    }
    if !result.ok {
        *failed += 1;
    }
}

async fn post_batch(context: Arc<LoadContext>, batch_size: u64) -> RequestResult {
    let started_at = Instant::now();
    let result = async {
        let body = if batch_size == 1 {
            serde_json::to_vec(&make_event(&context, "10_percent_log_90_percent_rest")?)?
        } else {
            let events = (0..batch_size)
                .map(|_| make_event(&context, "10_percent_log_90_percent_rest"))
                .collect::<Result<Vec<_>>>()?;
            serde_json::to_vec(&events)?
        };
        let body_bytes = body.len() as u64;

        let response = context
            .client
            .post(format!("{}/v1/events", context.config.ingest_url))
            .header(AUTHORIZATION, format!("Bearer {}", context.config.api_key))
            .header(CONTENT_TYPE, "application/json")
            .body(body)
            .send()
            .await?;
        let status = response.status().as_u16();
        let _ = response.bytes().await;
        Ok::<_, anyhow::Error>((status, body_bytes))
    }
    .await;

    match result {
        Ok((status, body_bytes)) => RequestResult {
            status: status.to_string(),
            ok: (200..300).contains(&status),
            latency_ms: started_at.elapsed().as_secs_f64() * 1_000.0,
            body_bytes,
        },
        Err(error) => RequestResult {
            status: error
                .root_cause()
                .to_string()
                .split(':')
                .next()
                .unwrap_or("request_error")
                .to_owned(),
            ok: false,
            latency_ms: started_at.elapsed().as_secs_f64() * 1_000.0,
            body_bytes: 0,
        },
    }
}

fn make_event(context: &LoadContext, batch_mix: &str) -> Result<Value> {
    let local_seq = context.config.event_seq.fetch_add(1, Ordering::Relaxed);
    let seq = context
        .config
        .sequence_offset
        .saturating_add(local_seq.saturating_mul(context.config.sequence_stride));
    let fixture = choose_fixture(
        &context.fixtures,
        seq,
        context.config.log_ratio,
        context.config.profile,
    );
    let (timestamp, observed_timestamp) =
        event_timestamps(context.config.profile, &context.config.run_id, seq);

    let mut event = fixture
        .body
        .as_object()
        .cloned()
        .ok_or_else(|| anyhow!("fixture {} must be a JSON object", fixture.name))?;
    let mut data = event
        .get("data")
        .and_then(Value::as_object)
        .cloned()
        .ok_or_else(|| anyhow!("fixture {} must contain object data", fixture.name))?;
    if context.config.profile.is_realistic() {
        randomize_realistic_data(
            &mut data,
            &fixture.name,
            &context.config.run_id,
            seq,
            &timestamp,
            context.config.trace_depth,
            context.config.profile == LoadProfile::Trace,
        );
    }
    if context.config.profile == LoadProfile::Llm {
        apply_llm_loadtest_data(&mut data, &fixture.name, &context.config.run_id, seq);
    }

    event.insert(
        "event_id".to_owned(),
        Value::String(Uuid::now_v7().to_string()),
    );
    event.insert("timestamp".to_owned(), Value::String(timestamp));
    event.insert(
        "observed_timestamp".to_owned(),
        Value::String(observed_timestamp),
    );
    data.insert("tenant_id".to_owned(), Value::String("loadtest".to_owned()));
    data.insert(
        "service".to_owned(),
        data.get("service")
            .cloned()
            .unwrap_or_else(|| Value::String("nanotrace-loadtest".to_owned())),
    );
    data.insert(
        "event_type".to_owned(),
        data.get("event_type")
            .cloned()
            .unwrap_or_else(|| Value::String(fixture.name.clone())),
    );
    data.insert(
        "_loadtest".to_owned(),
        json!({
            "run_id": context.config.run_id,
            "sequence": seq,
            "fixture": fixture.name,
            "batch_mix": batch_mix,
        }),
    );
    event.insert("data".to_owned(), Value::Object(data));

    Ok(Value::Object(event))
}

fn choose_fixture(fixtures: &Fixtures, seq: u64, log_ratio: f64, profile: LoadProfile) -> &Fixture {
    match profile {
        LoadProfile::Logs | LoadProfile::Llm => log_fixture(fixtures, seq),
        LoadProfile::Metrics => fixture_at(&fixtures.metric_fixtures, seq),
        LoadProfile::Trace => fixture_at(&fixtures.trace_fixtures, seq),
        LoadProfile::Product => fixture_at(&fixtures.product_fixtures, seq),
        LoadProfile::Agent => fixture_at(&fixtures.agent_fixtures, seq),
        LoadProfile::Pipeline => fixture_at(&fixtures.pipeline_fixtures, seq),
        LoadProfile::Atlas => atlas_fixture(fixtures, seq, log_ratio),
        LoadProfile::Codex => codex_fixture(fixtures, seq),
        LoadProfile::Realistic => mixed_fixture(fixtures, seq, log_ratio, true),
        LoadProfile::Fixture => mixed_fixture(fixtures, seq, log_ratio, false),
    }
}

fn atlas_fixture(fixtures: &Fixtures, seq: u64, log_ratio: f64) -> &Fixture {
    let bucket = seq % 100;
    if bucket < log_ratio_percent(log_ratio) {
        return log_fixture(fixtures, seq);
    }
    if bucket < 48 {
        return fixture_at(&fixtures.product_fixtures, seq);
    }
    if bucket < 78 {
        return fixture_at(&fixtures.agent_fixtures, seq);
    }
    if bucket < 92 {
        return fixture_at(&fixtures.metric_fixtures, seq);
    }
    if bucket < 98 {
        return fixture_at(&fixtures.trace_fixtures, seq);
    }
    fixture_at(&fixtures.pipeline_fixtures, seq)
}

fn mixed_fixture(
    fixtures: &Fixtures,
    seq: u64,
    log_ratio: f64,
    use_log_variations: bool,
) -> &Fixture {
    if (seq % 100) < (log_ratio * 100.0).round() as u64 {
        if use_log_variations {
            log_fixture(fixtures, seq)
        } else {
            &fixtures.log
        }
    } else {
        let index = seq as usize % fixtures.rest.len();
        &fixtures.rest[index]
    }
}

fn log_fixture(fixtures: &Fixtures, seq: u64) -> &Fixture {
    if fixtures.log_variations.is_empty() {
        return &fixtures.log;
    }
    let index = seq as usize % (fixtures.log_variations.len() + 1);
    if index == 0 {
        &fixtures.log
    } else {
        &fixtures.log_variations[index - 1]
    }
}

fn fixture_at(fixtures: &[Fixture], seq: u64) -> &Fixture {
    let index = seq as usize % fixtures.len();
    &fixtures[index]
}

fn log_ratio_percent(log_ratio: f64) -> u64 {
    (log_ratio * 100.0).round() as u64
}

fn codex_fixture(fixtures: &Fixtures, seq: u64) -> &Fixture {
    const EVENTS_PER_CODEX_TRACE: u64 = 24;

    let group = seq / EVENTS_PER_CODEX_TRACE;
    let slot = seq % EVENTS_PER_CODEX_TRACE;
    let workflow = CodexWorkflow::for_group(group);

    match slot {
        0 => named_fixture_or_first(&fixtures.trace_fixtures, "span_start"),
        1 => named_fixture_or_first(&fixtures.agent_fixtures, "agent_request"),
        2 => workflow_log_fixture(fixtures, workflow),
        3 | 16 => named_fixture_or_first(&fixtures.agent_fixtures, "retrieval_step"),
        4 | 10 | 13 | 19 => named_fixture_or_first(&fixtures.agent_fixtures, "llm_call"),
        5 => named_fixture_or_first(&fixtures.metric_fixtures, "metric_histogram"),
        6 | 9 | 15 => named_fixture_or_first(&fixtures.agent_fixtures, "tool_call"),
        7 | 12 | 17 | 21 => workflow_log_fixture(fixtures, workflow),
        8 => named_fixture_or_first(&fixtures.metric_fixtures, "metric_counter"),
        11 => named_fixture_or_first(&fixtures.agent_fixtures, "agent_decision"),
        14 => named_fixture_or_first(&fixtures.metric_fixtures, "metric_runtime"),
        18 => match group % 4 {
            0 => named_fixture_or_first(&fixtures.agent_fixtures, "safety_event"),
            1 => named_fixture_or_first(&fixtures.agent_fixtures, "eval_score"),
            _ => named_fixture_or_first(&fixtures.metric_fixtures, "metric"),
        },
        20 => named_fixture_or_first(&fixtures.metric_fixtures, "metric"),
        22 => match group % 6 {
            0 => &fixtures.pipeline_fixtures[group as usize % fixtures.pipeline_fixtures.len()],
            1 => named_fixture_or_first(&fixtures.agent_fixtures, "eval_score"),
            2 => named_fixture_or_first(&fixtures.agent_fixtures, "safety_event"),
            _ => workflow_log_fixture(fixtures, workflow),
        },
        23 => named_fixture_or_first(&fixtures.trace_fixtures, "span_end"),
        _ => &fixtures.log,
    }
}

fn workflow_log_fixture(fixtures: &Fixtures, workflow: CodexWorkflow) -> &Fixture {
    let name = match workflow {
        CodexWorkflow::CodeEdit | CodexWorkflow::DebugFailure | CodexWorkflow::ReviewFix => {
            "log_variation_code_success"
        }
        CodexWorkflow::CanvasEdit => "log_variation_excalidraw",
        CodexWorkflow::ImageGeneration => "log_variation_image_gen",
        CodexWorkflow::DocsLookup => "log",
    };
    if name == "log" {
        &fixtures.log
    } else {
        named_fixture_or_first(&fixtures.log_variations, name)
    }
}

fn named_fixture_or_first<'a>(fixtures: &'a [Fixture], name: &str) -> &'a Fixture {
    fixtures
        .iter()
        .find(|fixture| fixture.name == name)
        .unwrap_or(&fixtures[0])
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CodexWorkflow {
    CodeEdit,
    DebugFailure,
    CanvasEdit,
    DocsLookup,
    ImageGeneration,
    ReviewFix,
}

impl CodexWorkflow {
    fn for_group(group: u64) -> Self {
        match group % 10 {
            0..=2 => Self::CodeEdit,
            3..=4 => Self::DebugFailure,
            5 => Self::CanvasEdit,
            6 => Self::DocsLookup,
            7 => Self::ImageGeneration,
            _ => Self::ReviewFix,
        }
    }

    fn conversation_mode(self) -> &'static str {
        match self {
            Self::CodeEdit | Self::DebugFailure | Self::ReviewFix => "coding",
            Self::CanvasEdit => "canvas",
            Self::DocsLookup => "research",
            Self::ImageGeneration => "multimodal",
        }
    }

    fn agent_type(self) -> &'static str {
        match self {
            Self::CanvasEdit => "canvas",
            Self::DocsLookup => "research",
            Self::ImageGeneration => "multimodal",
            _ => "coding",
        }
    }

    fn user_prompt(self) -> &'static str {
        match self {
            Self::CodeEdit => "Update the parser to handle the new schema and add a focused test.",
            Self::DebugFailure => {
                "Figure out why the deploy is failing after the ClickHouse upgrade and patch it."
            }
            Self::CanvasEdit => {
                "Tighten the canvas layout and fix the overlapping labels in the visualization."
            }
            Self::DocsLookup => {
                "Search the repo and docs for how tool execution and MCP requests are wired."
            }
            Self::ImageGeneration => {
                "Generate a new hero image from the product screenshot and match the existing style."
            }
            Self::ReviewFix => "Address the PR review comments and explain the behavior change.",
        }
    }

    fn system_prompt(self) -> &'static str {
        match self {
            Self::CanvasEdit => {
                "You are Codex. Inspect the canvas code, keep edits scoped, and preserve interaction behavior."
            }
            Self::DocsLookup => {
                "You are Codex. Read the repo first, ground answers in source, and avoid unsupported claims."
            }
            Self::ImageGeneration => {
                "You are Codex. Use existing assets and tools, prefer concrete outputs, and keep revisions deterministic."
            }
            _ => {
                "You are Codex, a coding agent. Read the repo, reason about the task, and make grounded code changes."
            }
        }
    }

    fn assistant_summary(self) -> &'static str {
        match self {
            Self::CodeEdit => "I inspected the relevant files and I am preparing a narrow patch.",
            Self::DebugFailure => {
                "I found the failure path and I am validating a schema-compatible fix."
            }
            Self::CanvasEdit => {
                "I identified the layout collision and I am updating the visualization component."
            }
            Self::DocsLookup => {
                "I gathered the relevant repo paths and I am tracing the execution flow."
            }
            Self::ImageGeneration => {
                "I am iterating on the requested visual and checking the output against the existing UI."
            }
            Self::ReviewFix => {
                "I reproduced the review concern and I am applying the requested fix."
            }
        }
    }
}

fn event_timestamps(profile: LoadProfile, run_id: &str, seq: u64) -> (String, String) {
    match profile {
        LoadProfile::Llm => {
            let timestamp = realistic_llm_timestamp(run_id, seq);
            let mut rng = TinyRng::new(seed_for(run_id, "llm-observed-timestamp", seq));
            let observed = timestamp + ChronoDuration::milliseconds(rng.range(20, 5_000) as i64);
            (
                timestamp.to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
                observed.to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
            )
        }
        LoadProfile::Atlas
        | LoadProfile::Codex
        | LoadProfile::Realistic
        | LoadProfile::Trace
        | LoadProfile::Metrics
        | LoadProfile::Logs
        | LoadProfile::Product
        | LoadProfile::Agent
        | LoadProfile::Pipeline => {
            let timestamp = historical_loadtest_timestamp(run_id, seq);
            let mut rng = TinyRng::new(seed_for(run_id, "observed-timestamp", seq));
            let observed = timestamp + ChronoDuration::milliseconds(rng.range(1, 500) as i64);
            (
                timestamp.to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
                observed.to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
            )
        }
        LoadProfile::Fixture => {
            let now = Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
            (now.clone(), now)
        }
    }
}

fn historical_loadtest_timestamp(run_id: &str, seq: u64) -> DateTime<Utc> {
    const WINDOW_DAYS: i64 = 60;
    const EVENTS_PER_SYNTHETIC_TRACE: u64 = 24;

    let now = Utc::now();
    let group = seq / EVENTS_PER_SYNTHETIC_TRACE;
    let within_group = seq % EVENTS_PER_SYNTHETIC_TRACE;
    let mut rng = TinyRng::new(seed_for(run_id, "timestamp-group", group));
    let day = weighted_history_day(&mut rng, WINDOW_DAYS as u64);
    let hour = weighted_business_hour(&mut rng);
    let minute = rng.range(0, 59);
    let second = rng.range(0, 59);
    let date = (now - ChronoDuration::days(day as i64)).date_naive();
    let base = DateTime::<Utc>::from_naive_utc_and_offset(
        date.and_hms_opt(hour as u32, minute as u32, second as u32)
            .expect("generated timestamp components are valid"),
        Utc,
    );
    let base = if base > now {
        base - ChronoDuration::days(1)
    } else {
        base
    };
    let jitter = TinyRng::new(seed_for(run_id, "timestamp-jitter", seq)).range(0, 20);

    base + ChronoDuration::milliseconds((within_group * 50 + jitter) as i64)
}

fn realistic_llm_timestamp(run_id: &str, seq: u64) -> DateTime<Utc> {
    const WINDOW_DAYS: u64 = 60;

    let window_end = Utc::now();
    let window_start = window_end - ChronoDuration::days(WINDOW_DAYS as i64);
    let mut rng = TinyRng::new(seed_for(run_id, "llm-timestamp", seq));

    let (day, hour) = loop {
        let day = weighted_history_day(&mut rng, WINDOW_DAYS) as i64;
        let hour = rng.range(0, 23);
        let candidate = window_start + ChronoDuration::days(day);
        let weight = llm_traffic_weight(candidate, hour);
        if rng.range(1, 100) <= weight {
            break (day, hour);
        }
    };

    window_start
        + ChronoDuration::days(day)
        + ChronoDuration::hours(hour as i64)
        + ChronoDuration::minutes(rng.range(0, 59) as i64)
        + ChronoDuration::seconds(rng.range(0, 59) as i64)
        + ChronoDuration::milliseconds(rng.range(0, 999) as i64)
}

fn weighted_history_day(rng: &mut TinyRng, window_days: u64) -> u64 {
    loop {
        let day = rng.range(0, window_days.saturating_sub(1));
        let age_factor = 1.0 - ((day as f64 / window_days.max(1) as f64) * 0.35);
        let weekday_factor = match day % 7 {
            5 | 6 => 0.55,
            _ => 1.0,
        };
        let weight = (100.0 * age_factor * weekday_factor)
            .round()
            .clamp(10.0, 100.0) as u64;
        if rng.range(1, 100) <= weight {
            return day;
        }
    }
}

fn weighted_business_hour(rng: &mut TinyRng) -> u64 {
    loop {
        let hour = rng.range(0, 23);
        let weight = match hour {
            0..=5 => 8,
            6..=8 => 45,
            9..=11 => 95,
            12..=13 => 72,
            14..=17 => 100,
            18..=20 => 58,
            _ => 24,
        };
        if rng.range(1, 100) <= weight {
            return hour;
        }
    }
}

fn llm_traffic_weight(day: DateTime<Utc>, hour: u64) -> u64 {
    let hourly = match hour {
        0..=5 => 12,
        6..=8 => 55,
        9..=11 => 95,
        12..=13 => 75,
        14..=17 => 100,
        18..=20 => 70,
        _ => 35,
    };
    let weekday_factor = match day.weekday().number_from_monday() {
        6 | 7 => 0.45,
        1 => 0.85,
        5 => 0.9,
        _ => 1.0,
    };
    ((hourly as f64) * weekday_factor).round().clamp(5.0, 100.0) as u64
}

fn randomize_realistic_data(
    data: &mut Map<String, Value>,
    fixture_name: &str,
    run_id: &str,
    seq: u64,
    now: &str,
    trace_depth: u64,
    deep_trace: bool,
) {
    let mut rng = TinyRng::new(seed_for(run_id, fixture_name, seq));
    let scenario = Scenario::new(
        &mut rng,
        fixture_name,
        run_id,
        seq,
        now,
        trace_depth,
        deep_trace,
    );
    apply_realistic_object(data, "", &scenario, &mut rng);
    enrich_realistic_data(data, fixture_name, &scenario, &mut rng);
}

fn enrich_realistic_data(
    data: &mut Map<String, Value>,
    fixture_name: &str,
    scenario: &Scenario,
    rng: &mut TinyRng,
) {
    let event_type = data
        .get("event_type")
        .and_then(Value::as_str)
        .unwrap_or(fixture_name)
        .to_owned();
    let request_id = format!(
        "req_{}",
        stable_hex(&scenario.run_id, "request", rng.range(0, 1_000_000), 16)
    );

    data.insert("request_id".to_owned(), Value::String(request_id));
    data.insert(
        "user_id".to_owned(),
        Value::String(scenario.user_id.clone()),
    );
    data.insert(
        "session_id".to_owned(),
        Value::String(scenario.session_id.clone()),
    );
    data.insert(
        "account_id".to_owned(),
        Value::String(scenario.account_id.clone()),
    );
    data.insert(
        "conversation_id".to_owned(),
        Value::String(scenario.conversation_id.clone()),
    );
    data.insert(
        "thread_id".to_owned(),
        Value::String(scenario.thread_id.clone()),
    );

    match event_type.as_str() {
        "log" => {
            data.insert("signal".to_owned(), Value::String("log".to_owned()));
            data.insert(
                "service".to_owned(),
                Value::String(scenario.service.to_owned()),
            );
            data.insert(
                "conversationMode".to_owned(),
                Value::String(scenario.workflow.conversation_mode().to_owned()),
            );
            data.insert(
                "agentType".to_owned(),
                Value::String(scenario.workflow.agent_type().to_owned()),
            );
            data.insert(
                "agentPhase".to_owned(),
                Value::String(log_agent_phase(scenario, rng).to_owned()),
            );
            data.insert(
                "name".to_owned(),
                Value::String(log_event_name(scenario, fixture_name).to_owned()),
            );
            data.insert(
                "message".to_owned(),
                Value::String(log_event_message(scenario, fixture_name, rng)),
            );
            data.insert("llm".to_owned(), codex_llm_log_payload(scenario, rng));
            if fixture_name.contains("image_gen") {
                data.insert(
                    "asset".to_owned(),
                    json!({
                        "kind": "generated-image",
                        "mime_type": "image/png",
                        "width": 1536,
                        "height": 1024
                    }),
                );
            }
            if fixture_name.contains("code_success") {
                data.insert("tool".to_owned(), codex_tool_payload(scenario, rng, false));
            }
        }
        "checkout.started" | "checkout.completed" | "subscription.renewed" => {
            data.insert("signal".to_owned(), Value::String("analytics".to_owned()));
            data.insert("service".to_owned(), Value::String("billing".to_owned()));
            data.insert("name".to_owned(), Value::String(event_type.clone()));
            let plan = rng.choose_str(&["free", "free", "plus", "team", "team", "enterprise"]);
            let previous_plan = match plan {
                "free" => "trial",
                "plus" => "free",
                "team" => rng.choose_str(&["free", "plus"]),
                "enterprise" => "team",
                _ => "free",
            };
            let amount = round(rng.float_range(9.0, 499.0), 2);
            data.insert("revenue".to_owned(), number_value(amount));
            data.insert(
                "credits_used".to_owned(),
                number_value(round(amount * 12.0, 2)),
            );
            data.insert(
                "checkout".to_owned(),
                json!({
                    "id": format!("chk_{}", stable_hex(&scenario.run_id, "checkout", rng.range(0, 1_000_000), 12)),
                    "currency": "USD",
                    "amount": amount,
                    "payment_method": rng.choose_str(&["card", "ach", "wire", "apple_pay"]),
                    "plan_before": previous_plan,
                    "plan_after": plan
                }),
            );
        }
        "order.submitted" | "order.filled" | "order.cancelled" | "order.rejected" => {
            data.insert("signal".to_owned(), Value::String("analytics".to_owned()));
            data.insert("service".to_owned(), Value::String("trading".to_owned()));
            data.insert("name".to_owned(), Value::String(event_type.clone()));
            let amount = round(rng.float_range(9.0, 499.0), 2);
            let symbol = rng.choose_str(&["AAPL", "NVDA", "MSFT", "META", "BTC-USD", "ETH-USD"]);
            let order_side = rng.choose_str(&["buy", "sell"]);
            let order_status = match event_type.as_str() {
                "order.submitted" => "submitted",
                "order.filled" => "filled",
                "order.cancelled" => "cancelled",
                _ => rng.choose_str(&["submitted", "filled", "cancelled", "rejected"]),
            };
            let quantity = round(rng.float_range(0.01, 250.0), 4);
            let price = round(rng.float_range(4.0, 950.0), 2);
            let order_id = format!(
                "ord_{}",
                stable_hex(&scenario.run_id, "order", rng.range(0, 1_000_000), 12)
            );
            data.insert(
                "revenue".to_owned(),
                number_value(round(amount * 0.0025, 4)),
            );
            data.insert(
                "order".to_owned(),
                json!({
                    "id": order_id,
                    "symbol": symbol,
                    "asset_class": if symbol.ends_with("-USD") { "crypto" } else { "equity" },
                    "side": order_side,
                    "status": order_status,
                    "quantity": quantity,
                    "price": price,
                    "notional": round(quantity * price, 2),
                    "venue": rng.choose_str(&["nasdaq", "nyse", "arca", "crypto-router"])
                }),
            );
        }
        "account.plan_changed" => {
            data.insert("signal".to_owned(), Value::String("analytics".to_owned()));
            data.insert("service".to_owned(), Value::String("billing".to_owned()));
            let plan = rng.choose_str(&["free", "free", "plus", "team", "team", "enterprise"]);
            let previous_plan = match plan {
                "free" => "trial",
                "plus" => "free",
                "team" => rng.choose_str(&["free", "plus"]),
                "enterprise" => "team",
                _ => "free",
            };
            data.insert(
                "name".to_owned(),
                Value::String("account.plan_changed".to_owned()),
            );
            data.insert(
                "change".to_owned(),
                json!({
                    "field": "account.plan",
                    "from": previous_plan,
                    "to": plan,
                    "reason": rng.choose_str(&["self_serve_upgrade", "sales_upgrade", "trial_expired", "admin_change"])
                }),
            );
        }
        "account.risk_tier_changed" => {
            data.insert("signal".to_owned(), Value::String("analytics".to_owned()));
            data.insert("service".to_owned(), Value::String("risk".to_owned()));
            let risk_tier = rng.choose_str(&["low", "low", "medium", "medium", "high"]);
            data.insert(
                "name".to_owned(),
                Value::String("account.risk_tier_changed".to_owned()),
            );
            data.insert(
                "change".to_owned(),
                json!({
                    "field": "account.risk_tier",
                    "from": rng.choose_str(&["low", "medium", "high"]),
                    "to": risk_tier,
                    "reason": rng.choose_str(&["kyc_review", "velocity_rule", "manual_review", "model_score"])
                }),
            );
        }
        "span_start" | "span_end" => {
            data.insert("signal".to_owned(), Value::String("trace".to_owned()));
            data.insert("name".to_owned(), Value::String(scenario.span_name()));
            data.insert("duration_ms".to_owned(), Value::from(scenario.duration_ms));
            data.insert(
                "trace_id".to_owned(),
                Value::String(scenario.trace_id.clone()),
            );
            data.insert(
                "span_id".to_owned(),
                Value::String(scenario.span_id.clone()),
            );
            data.insert(
                "parent_span_id".to_owned(),
                Value::String(scenario.parent_span_id.clone()),
            );
        }
        "agent.request" | "agent.decision" => {
            data.insert("signal".to_owned(), Value::String("trace".to_owned()));
            data.insert(
                "service".to_owned(),
                Value::String("codex-orchestrator".to_owned()),
            );
            data.insert("name".to_owned(), Value::String(event_type.clone()));
            data.insert("duration_ms".to_owned(), Value::from(scenario.duration_ms));
            data.insert(
                "agent".to_owned(),
                json!({
                    "type": scenario.workflow.agent_type(),
                    "workflow_state": rng.choose_str(&["classified", "planned", "tool_wait", "patching", "responding"]),
                    "intent": workflow_intent(scenario.workflow),
                    "decision": rng.choose_str(&["answer", "retrieve", "call_tool", "patch_files", "refuse"])
                }),
            );
        }
        "llm.call" => {
            let input_tokens = rng.range(400, 42_000);
            let output_tokens = rng.range(80, 8_000);
            let total_tokens = input_tokens + output_tokens;
            data.insert("signal".to_owned(), Value::String("trace".to_owned()));
            data.insert(
                "service".to_owned(),
                Value::String("llm-gateway".to_owned()),
            );
            data.insert("name".to_owned(), Value::String("llm.call".to_owned()));
            data.insert("model".to_owned(), Value::String(scenario.model.to_owned()));
            data.insert("input_tokens".to_owned(), Value::from(input_tokens));
            data.insert("output_tokens".to_owned(), Value::from(output_tokens));
            data.insert("total_tokens".to_owned(), Value::from(total_tokens));
            data.insert(
                "cost_usd".to_owned(),
                number_value(round(total_tokens as f64 * 0.0000025, 5)),
            );
            data.insert(
                "duration_ms".to_owned(),
                Value::from(scenario.duration_ms.max(500)),
            );
            data.insert(
                "llm".to_owned(),
                codex_llm_call_payload(scenario, input_tokens, output_tokens, total_tokens, rng),
            );
        }
        "tool.call" => {
            data.insert("signal".to_owned(), Value::String("trace".to_owned()));
            data.insert(
                "service".to_owned(),
                Value::String("tool-runner".to_owned()),
            );
            data.insert("name".to_owned(), Value::String("tool.call".to_owned()));
            data.insert("duration_ms".to_owned(), Value::from(scenario.duration_ms));
            data.insert(
                "credits_used".to_owned(),
                number_value(round(rng.float_range(0.1, 18.0), 2)),
            );
            data.insert("tool".to_owned(), codex_tool_payload(scenario, rng, true));
        }
        "retrieval.step" => {
            data.insert("signal".to_owned(), Value::String("trace".to_owned()));
            data.insert("service".to_owned(), Value::String("retrieval".to_owned()));
            data.insert(
                "name".to_owned(),
                Value::String("retrieval.step".to_owned()),
            );
            data.insert("duration_ms".to_owned(), Value::from(rng.range(20, 1_200)));
            data.insert(
                "retrieval".to_owned(),
                json!({
                    "index": scenario.retrieval_index,
                    "query_type": rng.choose_str(&["hybrid", "vector", "keyword"]),
                    "query": retrieval_query(scenario.workflow, rng),
                    "top_k": rng.range(3, 30),
                    "hit_count": rng.range(0, 30),
                    "max_score": round(rng.float_range(0.15, 0.98), 4),
                    "reranked": rng.chance(72, 100),
                    "namespace": rng.choose_str(&["workspace", "repo", "memory", "docs"])
                }),
            );
        }
        "eval.score" => {
            data.insert("signal".to_owned(), Value::String("analytics".to_owned()));
            data.insert("service".to_owned(), Value::String("evals".to_owned()));
            data.insert("name".to_owned(), Value::String("eval.score".to_owned()));
            data.insert(
                "eval_score".to_owned(),
                number_value(round(rng.float_range(0.0, 1.0), 4)),
            );
            data.insert(
                "eval".to_owned(),
                json!({
                    "name": rng.choose_str(&["answer_groundedness", "tool_correctness", "latency_budget", "policy_compliance", "patch_quality", "canvas_layout"]),
                    "score": round(rng.float_range(0.0, 1.0), 4),
                    "passed": !scenario.is_error
                }),
            );
        }
        "safety.event" => {
            data.insert("signal".to_owned(), Value::String("analytics".to_owned()));
            data.insert("service".to_owned(), Value::String("safety".to_owned()));
            data.insert("name".to_owned(), Value::String("safety.event".to_owned()));
            data.insert(
                "is_error".to_owned(),
                Value::from(if scenario.is_error { 1 } else { 0 }),
            );
            data.insert(
                "safety".to_owned(),
                json!({
                    "policy": rng.choose_str(&["prompt_injection", "secrets_exposure", "exfiltration", "unsafe_shell", "copyright"]),
                    "action": rng.choose_str(&["allow", "warn", "block", "escalate"]),
                    "severity": rng.choose_str(&["low", "medium", "high"]),
                    "surface": rng.choose_str(&["prompt", "tool_output", "file_read", "image_upload", "browser_result"])
                }),
            );
        }
        "materializer.run" | "materializer.backfill_slice" | "materializer.report_materialized" => {
            data.insert("signal".to_owned(), Value::String("pipeline".to_owned()));
            data.insert(
                "service".to_owned(),
                Value::String("materializer".to_owned()),
            );
            data.insert("name".to_owned(), Value::String(event_type.clone()));
            data.insert("duration_ms".to_owned(), Value::from(rng.range(50, 30_000)));
            data.insert(
                "rows_scanned".to_owned(),
                Value::from(rng.range(10_000, 2_000_000_000)),
            );
            data.insert(
                "rows_written".to_owned(),
                Value::from(rng.range(1_000, 50_000_000)),
            );
            data.insert(
                "materializer".to_owned(),
                json!({
                    "id": format!("mat_{}", rng.range(1, 32)),
                    "kind": rng.choose_str(&["session_rollup", "trace_summary", "token_rollup", "field_indexer", "conversation_compaction"]),
                    "status": if scenario.is_error { "failed" } else { rng.choose_str(&["completed", "completed", "running"]) },
                    "definition_id": format!("def_{}", rng.choose_str(&["service", "model", "duration_ms", "total_tokens", "tool_name"])),
                    "slice_seconds": rng.choose_str(&["60", "300", "900"])
                }),
            );
        }
        _ => {}
    }
}

fn apply_llm_loadtest_data(
    data: &mut Map<String, Value>,
    fixture_name: &str,
    run_id: &str,
    seq: u64,
) {
    let mut rng = TinyRng::new(seed_for(run_id, "llm-loadtest", seq));
    let model = llm_model(&mut rng);
    let finish_reason = rng.choose_str(&[
        "stop",
        "stop",
        "stop",
        "stop",
        "tool-calls",
        "tool-calls",
        "length",
        "content-filter",
    ]);
    let service = rng.choose_str(&["llm-gateway", "llm-gateway", "canvas-agent", "api"]);

    data.insert("service".to_owned(), Value::String(service.to_owned()));
    data.insert("event_type".to_owned(), Value::String("log".to_owned()));
    data.insert(
        "name".to_owned(),
        Value::String("POST /v1/chat/completions".to_owned()),
    );
    data.insert(
        "http.route".to_owned(),
        Value::String("/v1/chat/completions".to_owned()),
    );
    data.insert("http.method".to_owned(), Value::String("POST".to_owned()));
    data.insert(
        "message".to_owned(),
        Value::String(format!(
            "{model} completion usage generated from {fixture_name}"
        )),
    );

    let usage = llm_total_usage(model, finish_reason, &mut rng);
    let llm_value = data
        .entry("llm".to_owned())
        .or_insert_with(|| Value::Object(Map::new()));
    if !llm_value.is_object() {
        *llm_value = Value::Object(Map::new());
    }
    let llm = llm_value
        .as_object_mut()
        .expect("llm value was normalized to object");
    llm.insert("model".to_owned(), Value::String(model.to_owned()));
    llm.insert(
        "finishReason".to_owned(),
        Value::String(finish_reason.to_owned()),
    );
    llm.insert(
        "messages".to_owned(),
        Value::Array(vec![
            json!({
                "role": "system",
                "content": "You are Codex, a coding agent. Read the repo, reason about the task, and make grounded code changes."
            }),
            json!({
                "role": "user",
                "content": llm_profile_prompt(seq)
            }),
        ]),
    );
    llm.insert(
        "request".to_owned(),
        json!({
            "endpoint": "/v1/chat/completions",
            "stream": true,
            "service_tier": rng.choose_str(&["priority", "default", "flex"]),
            "parallel_tool_calls": rng.chance(35, 100),
            "reasoning_effort": rng.choose_str(&["none", "low", "medium", "high"]),
            "temperature": round(rng.float_range(0.0, 0.8), 2),
            "max_output_tokens": rng.range(512, 8_192)
        }),
    );
    llm.insert(
        "response".to_owned(),
        json!({
            "provider_request_id": format!("oai_{}", stable_hex(run_id, "provider-request", seq, 16)),
            "finish_reason": finish_reason,
            "tool_call_count": if finish_reason == "tool-calls" { rng.range(1, 4) } else { 0 }
        }),
    );
    llm.insert("totalUsage".to_owned(), usage);
}

fn llm_profile_prompt(seq: u64) -> &'static str {
    match seq % 6 {
        0 => "Find the root cause of the flaky test and patch it.",
        1 => "Inspect the UI route and explain why the histogram is hidden.",
        2 => "Search the codebase for tool execution flow and summarize it.",
        3 => "Update the canvas layout to avoid label overlap on mobile.",
        4 => "Review the deploy failure after the ClickHouse schema change.",
        _ => "Generate a screenshot-ready image that matches the product UI.",
    }
}

fn llm_model(rng: &mut TinyRng) -> &'static str {
    rng.choose_str(&[
        "gpt-5.5",
        "gpt-5.5",
        "gpt-5",
        "gpt-5-mini",
        "claude-opus-4-7",
        "claude-sonnet-4-6",
        "claude-sonnet-4-6",
        "claude-haiku-4-5",
        "gemini-3.1-pro-preview",
        "gemini-3-flash-preview",
        "gemini-3-flash-preview",
        "gemini-3.1-flash-lite",
    ])
}

fn llm_total_usage(model: &str, finish_reason: &str, rng: &mut TinyRng) -> Value {
    let long_context = rng.chance(8, 100);
    let cached_input_tokens = if rng.chance(42, 100) {
        0
    } else if long_context {
        rng.range(12_000, 180_000)
    } else {
        rng.range(256, 48_000)
    };
    let no_cache_tokens = if long_context {
        rng.range(8_000, 80_000)
    } else {
        rng.range(80, 14_000)
    };
    let input_tokens = cached_input_tokens + no_cache_tokens;

    let reasoning_tokens =
        if model.contains("mini") || model.contains("haiku") || model.contains("flash-lite") {
            rng.range(0, 3_000)
        } else if finish_reason == "tool-calls" {
            rng.range(0, 8_000)
        } else {
            rng.range(0, 18_000)
        };
    let text_tokens = if finish_reason == "length" {
        rng.range(3_000, 16_000)
    } else if finish_reason == "tool-calls" {
        rng.range(20, 1_800)
    } else {
        rng.range(40, 6_000)
    };
    let output_tokens = reasoning_tokens + text_tokens;
    let total_tokens = input_tokens + output_tokens;

    json!({
        "cachedInputTokens": cached_input_tokens,
        "inputTokenDetails": {
            "cacheReadTokens": cached_input_tokens,
            "noCacheTokens": no_cache_tokens,
        },
        "inputTokens": input_tokens,
        "outputTokenDetails": {
            "reasoningTokens": reasoning_tokens,
            "textTokens": text_tokens,
        },
        "outputTokens": output_tokens,
        "reasoningTokens": reasoning_tokens,
        "totalTokens": total_tokens,
    })
}

struct Scenario {
    service: &'static str,
    environment: &'static str,
    method: &'static str,
    route: &'static str,
    workflow: CodexWorkflow,
    status_code: u16,
    duration_ms: u64,
    is_error: bool,
    user_id: String,
    session_id: String,
    account_id: String,
    conversation_id: String,
    trace_id: String,
    span_id: String,
    parent_span_id: String,
    run_id: String,
    thread_id: String,
    canvas_id: String,
    workspace_id: String,
    file_id: String,
    message_id: String,
    tool_name: &'static str,
    tool_sandbox: &'static str,
    retrieval_index: &'static str,
    model: &'static str,
    finish_reason: &'static str,
    turn: u64,
    start_time: String,
    end_time: String,
    metric_name: &'static str,
    metric_type: &'static str,
    metric_unit: &'static str,
    metric_value: f64,
    queue_depth: u64,
    memory_bytes: u64,
    pid: u64,
}

impl Scenario {
    fn new(
        rng: &mut TinyRng,
        fixture_name: &str,
        run_id: &str,
        seq: u64,
        now: &str,
        trace_depth: u64,
        deep_trace: bool,
    ) -> Self {
        const EVENTS_PER_CODEX_TRACE: u64 = 24;

        let hierarchy = Hierarchy::new(run_id, seq, trace_depth, deep_trace);
        let trace_group = seq / EVENTS_PER_CODEX_TRACE;
        let mut trace_rng = TinyRng::new(seed_for(run_id, "codex-trace", trace_group));
        let workflow = CodexWorkflow::for_group(trace_group);
        let route = route_for_workflow(workflow, &mut trace_rng);
        let method = match route {
            "/healthz" => "GET",
            "/v1/chat/completions"
            | "/v1/responses"
            | "/api/traces"
            | "/api/canvas-events"
            | "/api/tools/execute" => "POST",
            _ => trace_rng.choose_str(&["GET", "POST", "PATCH"]),
        };
        let error_rate = match route {
            "/api/canvases/{canvas_id}" | "/api/canvas/{document_id}" => 1,
            "/api/canvas-events" => 1,
            "/v1/chat/completions" | "/v1/responses" => 2,
            "/api/tools/execute" => 3,
            "/healthz" => 1,
            _ => 2,
        };
        let is_error = rng.chance(error_rate, 100);
        let status_code = if is_error {
            *rng.choose(&[400_u16, 400, 429, 429, 500, 500, 500, 503])
        } else if rng.chance(8, 100) {
            if method == "POST" {
                *rng.choose(&[200_u16, 201, 202, 204])
            } else {
                *rng.choose(&[200_u16, 200, 200, 304])
            }
        } else {
            200
        };
        let duration_ms = if is_error {
            rng.range(250, 4_500)
        } else {
            match route {
                "/healthz" => rng.range(3, 30),
                "/v1/chat/completions" | "/v1/responses" => rng.range(700, 9_000),
                "/api/tools/execute" => rng.range(40, 2_400),
                "/api/canvas/{document_id}" | "/api/canvases/{canvas_id}" => rng.range(80, 1_800),
                _ => rng.range(20, 650),
            }
        };
        let queue_depth = rng.range(0, 2_000);
        let memory_bytes = rng.range(120_000_000, 3_200_000_000);
        let (metric_name, metric_type, metric_unit, metric_value) = metric_scenario(
            fixture_name,
            route,
            duration_ms,
            queue_depth,
            memory_bytes,
            rng,
        );
        let (start_time, end_time) = span_times(now, duration_ms, rng);
        let tool_name = match workflow {
            CodexWorkflow::CanvasEdit => {
                trace_rng.choose_str(&["browser.open", "fs.write", "search.code"])
            }
            CodexWorkflow::DocsLookup => {
                trace_rng.choose_str(&["search.code", "fs.read", "mcp.query"])
            }
            CodexWorkflow::ImageGeneration => {
                trace_rng.choose_str(&["image.generate", "fs.read", "browser.open"])
            }
            _ => trace_rng.choose_str(&[
                "shell.exec",
                "fs.read",
                "fs.write",
                "git.status",
                "search.code",
                "mcp.query",
            ]),
        };
        let tool_sandbox = match tool_name {
            "image.generate" => "image",
            "browser.open" => "browser",
            "shell.exec" => "workspace-write",
            _ => "default",
        };
        let retrieval_index = match workflow {
            CodexWorkflow::DocsLookup => {
                trace_rng.choose_str(&["openai-docs", "tool-docs", "conversation-memory"])
            }
            CodexWorkflow::CanvasEdit => {
                trace_rng.choose_str(&["workspace-files", "repo-code", "conversation-memory"])
            }
            _ => trace_rng.choose_str(&[
                "workspace-files",
                "repo-code",
                "conversation-memory",
                "tool-docs",
            ]),
        };

        Self {
            service: service_for_route(route, &mut trace_rng),
            environment: trace_rng.choose_str(&["prod", "prod", "prod", "staging", "canary"]),
            method,
            route,
            workflow,
            status_code,
            duration_ms,
            is_error,
            user_id: hierarchy.user_id,
            session_id: hierarchy.session_id,
            account_id: hierarchy.account_id,
            conversation_id: uuid_like(&mut trace_rng),
            trace_id: hierarchy.trace_id,
            span_id: hierarchy.span_id,
            parent_span_id: hierarchy.parent_span_id,
            run_id: uuid_like(rng),
            thread_id: uuid_like(&mut trace_rng),
            canvas_id: uuid_like(&mut trace_rng),
            workspace_id: uuid_like(&mut trace_rng),
            file_id: uuid_like(&mut trace_rng),
            message_id: uuid_like(rng),
            tool_name,
            tool_sandbox,
            retrieval_index,
            model: llm_model(&mut trace_rng),
            finish_reason: trace_rng.choose_str(&[
                "stop",
                "tool-calls",
                "length",
                "content-filter",
            ]),
            turn: rng.range(1, 80),
            start_time,
            end_time,
            metric_name,
            metric_type,
            metric_unit,
            metric_value,
            queue_depth,
            memory_bytes,
            pid: rng.range(1_000, 65_000),
        }
    }

    fn span_name(&self) -> String {
        format!("{} {}", self.method, self.route)
    }

    fn log_message(&self) -> String {
        if self.is_error {
            format!(
                "{} {} failed with status {} after {}ms",
                self.method, self.route, self.status_code, self.duration_ms
            )
        } else {
            format!(
                "{} {} completed with status {} in {}ms",
                self.method, self.route, self.status_code, self.duration_ms
            )
        }
    }
}

fn route_for_workflow(workflow: CodexWorkflow, rng: &mut TinyRng) -> &'static str {
    match workflow {
        CodexWorkflow::CodeEdit | CodexWorkflow::DebugFailure | CodexWorkflow::ReviewFix => {
            rng.choose_str(&["/v1/responses", "/api/workspaces/{workspace_id}/patch"])
        }
        CodexWorkflow::CanvasEdit => {
            rng.choose_str(&["/api/canvases/{canvas_id}", "/api/canvas-events"])
        }
        CodexWorkflow::DocsLookup => rng.choose_str(&["/api/threads/{thread_id}", "/v1/responses"]),
        CodexWorkflow::ImageGeneration => {
            rng.choose_str(&["/v1/responses", "/api/files/{file_id}"])
        }
    }
}

fn workflow_intent(workflow: CodexWorkflow) -> &'static str {
    match workflow {
        CodexWorkflow::CodeEdit => "edit_code",
        CodexWorkflow::DebugFailure => "debug_failure",
        CodexWorkflow::CanvasEdit => "generate_ui",
        CodexWorkflow::DocsLookup => "explain_repo",
        CodexWorkflow::ImageGeneration => "generate_image",
        CodexWorkflow::ReviewFix => "address_review",
    }
}

fn log_agent_phase(scenario: &Scenario, rng: &mut TinyRng) -> &'static str {
    match scenario.workflow {
        CodexWorkflow::CodeEdit | CodexWorkflow::ReviewFix => {
            rng.choose_str(&["planning", "patching", "verifying", "responding"])
        }
        CodexWorkflow::DebugFailure => {
            rng.choose_str(&["investigating", "tool_wait", "patching", "responding"])
        }
        CodexWorkflow::CanvasEdit => {
            rng.choose_str(&["planning", "editing", "previewing", "responding"])
        }
        CodexWorkflow::DocsLookup => rng.choose_str(&["searching", "reading", "summarizing"]),
        CodexWorkflow::ImageGeneration => rng.choose_str(&["prompting", "rendering", "reviewing"]),
    }
}

fn log_event_name(scenario: &Scenario, fixture_name: &str) -> &'static str {
    match fixture_name {
        "log_variation_code_success" => "patch.apply.completed",
        "log_variation_excalidraw" => "canvas.snapshot.saved",
        "log_variation_image_gen" => "image.generation.completed",
        _ => {
            if scenario.route == "/v1/responses" || scenario.route == "/v1/chat/completions" {
                "response.stream.completed"
            } else {
                "agent.step.completed"
            }
        }
    }
}

fn log_event_message(scenario: &Scenario, fixture_name: &str, rng: &mut TinyRng) -> String {
    match fixture_name {
        "log_variation_code_success" => format!(
            "applied patch to {} and verified the focused test path",
            rng.choose(&[
                "event schema",
                "query builder",
                "UI route",
                "loadtest generator"
            ])
        ),
        "log_variation_excalidraw" => {
            "saved updated canvas layout after fixing overlap and spacing regressions".to_owned()
        }
        "log_variation_image_gen" => {
            "completed image generation pass and attached a new bitmap artifact".to_owned()
        }
        _ => match scenario.workflow {
            CodexWorkflow::DocsLookup => {
                "retrieved the relevant repo and docs context for the current request".to_owned()
            }
            CodexWorkflow::DebugFailure => {
                "captured the failure path and prepared a schema-compatible fix".to_owned()
            }
            _ => "response stream finished after tool-assisted generation".to_owned(),
        },
    }
}

fn codex_tool_payload(scenario: &Scenario, rng: &mut TinyRng, include_io: bool) -> Value {
    let exit_code = if scenario.is_error {
        rng.range(1, 2)
    } else {
        0
    };
    let mut payload = json!({
        "name": scenario.tool_name,
        "status": if scenario.is_error { "error" } else { "ok" },
        "attempt": rng.range(1, 3),
        "result_count": rng.range(0, 50),
        "latency_class": rng.choose_str(&["fast", "medium", "slow"]),
        "sandbox": scenario.tool_sandbox,
        "workspace_id": scenario.workspace_id,
        "arguments": tool_arguments(scenario),
        "exit_code": exit_code
    });
    if include_io {
        payload["bytes_read"] = Value::from(rng.range(256, 2_500_000));
        payload["bytes_written"] = Value::from(rng.range(0, 400_000));
    }
    payload
}

fn tool_arguments(scenario: &Scenario) -> Value {
    match scenario.tool_name {
        "shell.exec" => json!({"cmd": "cargo test -p nanotrace-loadtest", "workdir": "/workspace"}),
        "fs.read" => json!({"path": format!("apps/ui/src/routes/{}.tsx", scenario.file_id)}),
        "fs.write" => json!({"path": format!("apps/server/src/{}.rs", scenario.file_id)}),
        "git.status" => json!({}),
        "search.code" => json!({"query": "fetchDensity histogram groupBy", "path": "/workspace"}),
        "browser.open" => json!({"url": "http://localhost:3000"}),
        "image.generate" => {
            json!({"prompt": "Generate a product hero image matching the current UI"})
        }
        "mcp.query" => json!({"server": "openaiDeveloperDocs", "topic": "responses api"}),
        _ => json!({}),
    }
}

fn retrieval_query(workflow: CodexWorkflow, rng: &mut TinyRng) -> &'static str {
    match workflow {
        CodexWorkflow::CodeEdit => rng.choose_str(&[
            "applyPatch helper",
            "event schema recreate",
            "trace density query",
        ]),
        CodexWorkflow::DebugFailure => rng.choose_str(&[
            "ClickHouse ORDER BY duplicate",
            "Pulumi launch template refresh",
            "Kafka lag investigation",
        ]),
        CodexWorkflow::CanvasEdit => rng.choose_str(&[
            "DensityHistogramCanvas",
            "canvas overlap mobile",
            "iframe visualization sizing",
        ]),
        CodexWorkflow::DocsLookup => rng.choose_str(&[
            "tool execution flow",
            "MCP query path",
            "responses api reasoning effort",
        ]),
        CodexWorkflow::ImageGeneration => rng.choose_str(&[
            "image generation asset pipeline",
            "bitmap hero asset",
            "product screenshot prompt",
        ]),
        CodexWorkflow::ReviewFix => rng.choose_str(&[
            "requested changes failing tests",
            "PR review comment",
            "behavior regression summary",
        ]),
    }
}

fn codex_llm_call_payload(
    scenario: &Scenario,
    input_tokens: u64,
    output_tokens: u64,
    total_tokens: u64,
    rng: &mut TinyRng,
) -> Value {
    json!({
        "provider": rng.choose_str(&["openai", "anthropic", "google"]),
        "model": scenario.model,
        "finish_reason": scenario.finish_reason,
        "service_tier": rng.choose_str(&["priority", "default", "flex"]),
        "stream": rng.chance(82, 100),
        "parallel_tool_calls": rng.chance(35, 100),
        "reasoning_effort": rng.choose_str(&["none", "low", "medium", "high"]),
        "cached_input_tokens": rng.range(0, input_tokens / 2),
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "total_tokens": total_tokens,
        "provider_request_id": format!("req_{}", stable_hex(&scenario.run_id, "provider", scenario.turn, 18)),
        "temperature": round(rng.float_range(0.0, 0.8), 2),
        "max_output_tokens": rng.range(512, 8_192),
        "tool_choice": if scenario.workflow == CodexWorkflow::DocsLookup { "auto" } else { rng.choose_str(&["auto", "required", "none"]) },
        "messages": [
            {
                "role": "system",
                "content": scenario.workflow.system_prompt()
            },
            {
                "role": "user",
                "content": scenario.workflow.user_prompt()
            },
            {
                "role": "assistant",
                "content": scenario.workflow.assistant_summary()
            }
        ],
        "tools": [
            {
                "name": scenario.tool_name,
                "type": "function"
            }
        ],
        "response": {
            "status": if scenario.is_error { "error" } else { "completed" },
            "output_items": if scenario.finish_reason == "tool-calls" { rng.range(1, 4) } else { 1 }
        }
    })
}

fn codex_llm_log_payload(scenario: &Scenario, rng: &mut TinyRng) -> Value {
    let usage = llm_total_usage(scenario.model, scenario.finish_reason, rng);
    json!({
        "provider": "openai",
        "model": scenario.model,
        "finishReason": scenario.finish_reason,
        "messages": [
            {
                "role": "system",
                "content": scenario.workflow.system_prompt()
            },
            {
                "role": "user",
                "content": scenario.workflow.user_prompt()
            }
        ],
        "request": {
            "endpoint": if scenario.route == "/v1/responses" { "/v1/responses" } else { "/v1/chat/completions" },
            "stream": true,
            "serviceTier": rng.choose_str(&["priority", "default", "flex"]),
            "parallelToolCalls": rng.chance(35, 100),
            "reasoningEffort": rng.choose_str(&["none", "low", "medium", "high"]),
            "toolChoice": rng.choose_str(&["auto", "required", "none"])
        },
        "response": {
            "providerRequestId": format!("oai_{}", stable_hex(&scenario.run_id, "log-provider", scenario.turn, 16)),
            "status": if scenario.is_error { "error" } else { "completed" },
            "finishReason": scenario.finish_reason
        },
        "totalUsage": usage
    })
}

struct Hierarchy {
    user_id: String,
    session_id: String,
    account_id: String,
    trace_id: String,
    span_id: String,
    parent_span_id: String,
}

impl Hierarchy {
    fn new(run_id: &str, seq: u64, trace_depth: u64, deep_trace: bool) -> Self {
        const EVENTS_PER_TRACE: u64 = 24;
        const TRACES_PER_SESSION: u64 = 12;
        const SESSIONS_PER_USER: u64 = 6;
        const USERS_PER_ACCOUNT: u64 = 25;

        let events_per_trace = if deep_trace {
            trace_depth.max(1).saturating_mul(2)
        } else {
            EVENTS_PER_TRACE
        };
        let trace_index = seq / events_per_trace;
        let session_index = trace_index / TRACES_PER_SESSION;
        let user_index = session_index / SESSIONS_PER_USER;
        let account_index = user_index / USERS_PER_ACCOUNT;
        let session_in_user = session_index % SESSIONS_PER_USER;
        let span_slot = (seq % events_per_trace) / 2;

        let trace_id = stable_hex(run_id, "trace", trace_index, 32);
        let root_span_id = stable_hex(run_id, "span", trace_index * 1_000, 16);
        let span_id = if span_slot == 0 {
            root_span_id.clone()
        } else {
            stable_hex(run_id, "span", trace_index * 1_000 + span_slot, 16)
        };
        let parent_span_id = if span_slot == 0 {
            String::new()
        } else if deep_trace {
            stable_hex(run_id, "span", trace_index * 1_000 + span_slot - 1, 16)
        } else if span_slot <= 3 {
            root_span_id
        } else {
            stable_hex(
                run_id,
                "span",
                trace_index * 1_000 + ((span_slot - 1) / 2),
                16,
            )
        };

        Self {
            user_id: format!("user_{user_index:06}"),
            session_id: format!("sess_{user_index:06}_{session_in_user:02}"),
            account_id: format!("acct_{account_index:05}"),
            trace_id,
            span_id,
            parent_span_id,
        }
    }
}

fn apply_realistic_object(
    object: &mut Map<String, Value>,
    parent_key: &str,
    scenario: &Scenario,
    rng: &mut TinyRng,
) {
    for (key, value) in object.iter_mut() {
        let lower_key = key.to_ascii_lowercase();
        match value {
            Value::Object(child) => apply_realistic_object(child, &lower_key, scenario, rng),
            Value::Array(items) => apply_realistic_array(&lower_key, items, scenario, rng),
            _ => {
                if let Some(new_value) =
                    realistic_scalar_value(&lower_key, parent_key, value, scenario, rng)
                {
                    *value = new_value;
                }
            }
        }
    }
}

fn apply_realistic_array(key: &str, items: &mut [Value], scenario: &Scenario, rng: &mut TinyRng) {
    if key == "bucket_counts" {
        let mut remaining = scenario.duration_ms.max(8);
        for item in items {
            let count = rng.range(0, remaining.min(80));
            remaining = remaining.saturating_sub(count);
            *item = Value::from(count);
        }
        return;
    }

    if key == "explicit_bounds" {
        let mut next = 25;
        for item in items {
            next += rng.range(25, 175);
            *item = Value::from(next);
        }
        return;
    }

    for item in items {
        match item {
            Value::Object(child) => apply_realistic_object(child, key, scenario, rng),
            Value::Array(child) => apply_realistic_array(key, child, scenario, rng),
            _ => {
                if let Some(new_value) = realistic_scalar_value(key, key, item, scenario, rng) {
                    *item = new_value;
                }
            }
        }
    }
}

fn realistic_scalar_value(
    key: &str,
    parent_key: &str,
    current: &Value,
    scenario: &Scenario,
    rng: &mut TinyRng,
) -> Option<Value> {
    let value = match key {
        "service" | "service.name" => Value::String(scenario.service.to_owned()),
        "environment" | "deployment.environment" => Value::String(scenario.environment.to_owned()),
        "signal" => Value::String(signal_from_existing(current)),
        "trace_id" | "traceid" | "trace_id_hex" => Value::String(scenario.trace_id.clone()),
        "span_id" | "spanid" => Value::String(scenario.span_id.clone()),
        "parent_span_id" | "parentspanid" => Value::String(scenario.parent_span_id.clone()),
        "http.method" | "method" if parent_key != "llm" => {
            Value::String(scenario.method.to_owned())
        }
        "http.route" | "route" | "url.path" | "path" => Value::String(scenario.route.to_owned()),
        "http.status_code" | "status_code" | "status" if current.is_number() => {
            Value::from(scenario.status_code)
        }
        "duration_ms" | "durationms" | "elapsed_ms" | "latency_ms" => {
            number_like(current, scenario.duration_ms as f64)
        }
        "start_time" | "startedat" | "started_at" => Value::String(scenario.start_time.clone()),
        "end_time" | "endedat" | "ended_at" => Value::String(scenario.end_time.clone()),
        "is_error" | "error" if current.is_boolean() => Value::Bool(scenario.is_error),
        "is_error" | "error" => number_like(current, if scenario.is_error { 1.0 } else { 0.0 }),
        "span_status_code" => {
            Value::String(if scenario.is_error { "error" } else { "ok" }.to_owned())
        }
        "severity_text" | "level" => Value::String(if scenario.is_error {
            rng.choose(&["ERROR", "WARN"]).to_string()
        } else {
            rng.choose(&["INFO", "DEBUG"]).to_string()
        }),
        "severity_number" => Value::from(if scenario.is_error { 17 } else { 9 }),
        "name" if parent_key.is_empty() => Value::String(scenario.span_name()),
        "message" => Value::String(scenario.log_message()),
        "metric_value" => number_like(current, scenario.metric_value),
        "value" if parent_key.contains("metric") => number_like(current, scenario.metric_value),
        "queue.depth" | "queue_depth" => Value::from(scenario.queue_depth),
        "memory.bytes" | "memory_bytes" | "process.memory.bytes" => {
            Value::from(scenario.memory_bytes)
        }
        "count" => Value::from(rng.range(1, 300)),
        "sum" => number_like(current, scenario.metric_value * rng.float_range(8.0, 80.0)),
        "process.pid" | "pid" => Value::from(scenario.pid),
        "queue.name" => Value::String(
            rng.choose(&["parquet-flush", "ingest", "embeddings", "exports"])
                .to_string(),
        ),
        "partition" => Value::String(format!("p={:04}", rng.range(0, 64))),
        "memory.type" => Value::String(rng.choose(&["rss", "heap", "external"]).to_string()),
        "user_id" | "userid" => Value::String(scenario.user_id.clone()),
        "session_id" | "sessionid" => Value::String(scenario.session_id.clone()),
        "account_id" | "accountid" | "organization_id" | "org_id" => {
            Value::String(scenario.account_id.clone())
        }
        "canvasid" | "canvas_id" | "documentid" | "document_id" => {
            Value::String(scenario.canvas_id.clone())
        }
        "conversationmode" | "conversation_mode" => Value::String(
            rng.choose(&["teaching", "editing", "debugging", "review"])
                .to_string(),
        ),
        "agentphase" | "agent_phase" => {
            Value::String(rng.choose(&["plan", "run", "verify", "repair"]).to_string())
        }
        "agenttype" | "agent_type" => {
            Value::String(rng.choose(&["canvas", "code", "research"]).to_string())
        }
        "caller" => Value::String(format!(
            "src/{}/{}.ts:{}",
            scenario.service.replace('-', "_"),
            rng.choose(&["handler", "worker", "tracer", "client"]),
            rng.range(20, 900)
        )),
        "runid" | "run_id" if parent_key != "_loadtest" => Value::String(scenario.run_id.clone()),
        "threadid" | "thread_id" => Value::String(scenario.thread_id.clone()),
        "usermessageid" | "user_message_id" | "messageid" | "message_id" => {
            Value::String(scenario.message_id.clone())
        }
        "turn" | "steeringgeneration" | "steering_generation" => Value::from(scenario.turn),
        "model" | "modelid" | "model_id" => Value::String(scenario.model.to_owned()),
        "finishreason" | "finish_reason" => Value::String(scenario.finish_reason.to_owned()),
        "role" => Value::String(
            rng.choose(&["system", "user", "assistant", "tool"])
                .to_string(),
        ),
        "content" if current.is_string() => Value::String(llm_content(parent_key, scenario, rng)),
        "title" if parent_key.is_empty() => Value::String(
            rng.choose(&[
                "Patch review for event schema",
                "Canvas layout revision",
                "Latency investigation for tool execution",
                "Ingest smoke test for responses API",
            ])
            .to_string(),
        ),
        "metric_name" => Value::String(scenario.metric_name.to_owned()),
        "metric_unit" => Value::String(scenario.metric_unit.to_owned()),
        "metric_type" => Value::String(scenario.metric_type.to_owned()),
        _ => return None,
    };
    Some(value)
}

fn metric_scenario(
    fixture_name: &str,
    route: &str,
    duration_ms: u64,
    queue_depth: u64,
    memory_bytes: u64,
    rng: &mut TinyRng,
) -> (&'static str, &'static str, &'static str, f64) {
    let metric_name = match fixture_name {
        "metric_counter" => "http.server.requests",
        "metric_gauge" => "process.memory.usage",
        "metric_histogram" | "metric" => "http.server.duration",
        "metric_runtime" => "runtime.queue.depth",
        _ => match route {
            "/v1/chat/completions" | "/v1/responses" => rng.choose_str(&[
                "llm.tokens",
                "llm.duration",
                "llm.requests",
                "llm.stream_chunks",
            ]),
            "/api/canvas/{document_id}" => {
                rng.choose_str(&["canvas.tool.duration", "canvas.elements.changed"])
            }
            "/api/tools/execute" => {
                rng.choose_str(&["tool.duration", "tool.requests", "tool.result_count"])
            }
            _ => rng.choose_str(&[
                "http.server.duration",
                "http.server.requests",
                "runtime.queue.depth",
                "process.memory.usage",
            ]),
        },
    };

    let (metric_name, metric_type, metric_unit, mut metric_value) = match metric_name {
        "http.server.duration" | "llm.duration" | "canvas.tool.duration" => {
            (metric_name, "histogram", "ms", duration_ms as f64)
        }
        "tool.duration" => (metric_name, "histogram", "ms", duration_ms as f64),
        "http.server.requests" | "llm.requests" => {
            (metric_name, "counter", "1", rng.range(1, 20) as f64)
        }
        "tool.requests" | "llm.stream_chunks" => {
            (metric_name, "counter", "1", rng.range(1, 80) as f64)
        }
        "runtime.queue.depth" | "canvas.elements.changed" => {
            (metric_name, "gauge", "1", queue_depth.max(1) as f64)
        }
        "tool.result_count" => (metric_name, "gauge", "1", rng.range(0, 120) as f64),
        "process.memory.usage" => (metric_name, "gauge", "By", memory_bytes as f64),
        "llm.tokens" => (
            metric_name,
            "histogram",
            "tokens",
            rng.range(100, 50_000) as f64,
        ),
        _ => (metric_name, "gauge", "1", rng.float_range(1.0, 1_200.0)),
    };
    if metric_value.round() as u64 == 231 {
        metric_value += 17.0;
    }
    (metric_name, metric_type, metric_unit, metric_value)
}

fn service_for_route(route: &str, rng: &mut TinyRng) -> &'static str {
    match route {
        "/v1/chat/completions" | "/v1/responses" => {
            rng.choose_str(&["llm-gateway", "codex-orchestrator", "canvas-agent"])
        }
        "/api/canvas/{document_id}" | "/api/canvases/{canvas_id}" | "/api/canvas-events" => {
            rng.choose_str(&["api", "canvas-agent", "document-sync"])
        }
        "/api/tools/execute" => rng.choose_str(&["tool-runner", "workspace-exec"]),
        "/api/threads/{thread_id}" => rng.choose_str(&["conversation-store", "codex-orchestrator"]),
        "/api/files/{file_id}" => rng.choose_str(&["workspace-files", "artifact-store"]),
        "/api/workspaces/{workspace_id}/patch" => {
            rng.choose_str(&["workspace-patcher", "tool-runner"])
        }
        "/api/traces" => rng.choose_str(&["api", "ingest"]),
        _ => rng.choose_str(&["api", "worker"]),
    }
}

fn signal_from_existing(current: &Value) -> String {
    match current.as_str().unwrap_or_default() {
        "span" | "span_start" | "span_end" | "trace" => "trace".to_owned(),
        "metric" => "metric".to_owned(),
        "log" => "log".to_owned(),
        "analytics" | "track" | "page" | "screen" | "identify" | "group" | "alias" => {
            "analytics".to_owned()
        }
        value if !value.is_empty() => value.to_owned(),
        _ => "other".to_owned(),
    }
}

fn span_times(now: &str, duration_ms: u64, rng: &mut TinyRng) -> (String, String) {
    let end = chrono::DateTime::parse_from_rfc3339(now)
        .map(|time| time.with_timezone(&Utc))
        .unwrap_or_else(|_| Utc::now())
        - ChronoDuration::milliseconds(rng.range(2, 250) as i64);
    let start = end - ChronoDuration::milliseconds(duration_ms as i64);
    (
        start.to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
        end.to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
    )
}

fn llm_content(parent_key: &str, scenario: &Scenario, rng: &mut TinyRng) -> String {
    match parent_key {
        "messages" => format!(
            "{} request for {} on {}",
            rng.choose(&["Trace", "Canvas", "Loadtest", "Debug"]),
            scenario.service,
            scenario.route
        ),
        _ => scenario.log_message(),
    }
}

fn number_like(current: &Value, value: f64) -> Value {
    if current.as_i64().is_some() || current.as_u64().is_some() {
        Value::from(value.round() as u64)
    } else {
        number_value(round(value, 3))
    }
}

fn number_value(value: f64) -> Value {
    serde_json::Number::from_f64(value)
        .map(Value::Number)
        .unwrap_or_else(|| Value::from(0))
}

fn seed_for(run_id: &str, fixture_name: &str, seq: u64) -> u64 {
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in run_id.bytes().chain(fixture_name.bytes()) {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash ^ seq.wrapping_mul(0x9e3779b97f4a7c15)
}

fn stable_hex(run_id: &str, namespace: &str, index: u64, len: usize) -> String {
    TinyRng::new(seed_for(run_id, namespace, index)).hex(len)
}

struct TinyRng {
    state: u64,
}

impl TinyRng {
    fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9e3779b97f4a7c15);
        let mut value = self.state;
        value = (value ^ (value >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94d049bb133111eb);
        value ^ (value >> 31)
    }

    fn range(&mut self, min: u64, max: u64) -> u64 {
        if min >= max {
            return min;
        }
        min + (self.next_u64() % (max - min + 1))
    }

    fn chance(&mut self, numerator: u64, denominator: u64) -> bool {
        denominator != 0 && self.range(1, denominator) <= numerator
    }

    fn choose<'a, T>(&mut self, items: &'a [T]) -> &'a T {
        &items[self.range(0, items.len().saturating_sub(1) as u64) as usize]
    }

    fn choose_str(&mut self, items: &'static [&'static str]) -> &'static str {
        items[self.range(0, items.len().saturating_sub(1) as u64) as usize]
    }

    fn float_range(&mut self, min: f64, max: f64) -> f64 {
        let unit = (self.next_u64() >> 11) as f64 / ((1_u64 << 53) as f64);
        min + ((max - min) * unit)
    }

    fn hex(&mut self, len: usize) -> String {
        let mut output = String::with_capacity(len);
        while output.len() < len {
            output.push_str(&format!("{:016x}", self.next_u64()));
        }
        output.truncate(len);
        output
    }
}

fn uuid_like(rng: &mut TinyRng) -> String {
    let hex = rng.hex(32);
    format!(
        "{}-{}-{}-{}-{}",
        &hex[0..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..32]
    )
}

fn is_pass(config: &Config, stats: &StepStats) -> bool {
    stats.error_rate <= config.max_error_rate && stats.p95_ms <= config.max_p95_ms
}

fn print_step(stats: &StepStats) {
    println!(
        "targetRps={} batchSize={} attempted={} ok={} failed={} acceptedEvents={} achievedRps={} eventsPerSec={} bodyBytesPerSec={} bodyMiBPerSec={} avgBodyBytesPerReq={} attemptedBodyBytes={} acceptedBodyBytes={} p50={}ms p95={}ms p99={}ms errorRate={} inFlightMax={} statuses={}",
        stats.target_rps,
        stats.batch_size,
        stats.attempted,
        stats.ok,
        stats.failed,
        stats.accepted_events,
        stats.achieved_req_per_sec,
        stats.achieved_event_per_sec,
        stats.achieved_body_bytes_per_sec,
        stats.achieved_body_mib_per_sec,
        stats.average_body_bytes_per_request,
        stats.attempted_body_bytes,
        stats.accepted_body_bytes,
        stats.p50_ms,
        stats.p95_ms,
        stats.p99_ms,
        stats.error_rate,
        stats.max_observed_in_flight,
        Value::Object(
            stats
                .statuses
                .iter()
                .map(|(key, value)| (key.clone(), Value::from(*value)))
                .collect::<Map<_, _>>()
        )
    );
}

fn load_fixtures(root: &Path) -> Result<Fixtures> {
    let events_dir = root.join("fixtures/events");
    let base_names = [
        "log",
        "log_variation_code_success",
        "log_variation_excalidraw",
        "log_variation_image_gen",
        "metric",
        "metric_counter",
        "metric_gauge",
        "metric_histogram",
        "metric_runtime",
        "span_end",
        "span_start",
    ];
    let mut names = base_names
        .iter()
        .map(|name| (*name).to_owned())
        .collect::<Vec<_>>();
    let mut discovered = fs::read_dir(&events_dir)
        .with_context(|| format!("read {}", events_dir.display()))?
        .map(|entry| {
            let entry = entry?;
            Ok(entry.path())
        })
        .collect::<std::io::Result<Vec<_>>>()
        .with_context(|| format!("read {}", events_dir.display()))?;
    discovered.sort();
    for path in discovered {
        if path.extension().and_then(|extension| extension.to_str()) != Some("json") {
            continue;
        }
        let Some(stem) = path.file_stem().and_then(|stem| stem.to_str()) else {
            continue;
        };
        if !names.iter().any(|name| name == stem) {
            names.push(stem.to_owned());
        }
    }

    let mut loaded = Vec::new();
    for name in names {
        let path = events_dir.join(format!("{name}.json"));
        let body = serde_json::from_slice::<Value>(
            &fs::read(&path).with_context(|| format!("read {}", path.display()))?,
        )
        .with_context(|| format!("parse {}", path.display()))?;
        validate_fixture(&name, &body)?;
        loaded.push(Fixture {
            name: name.to_owned(),
            body,
        });
    }

    let log = loaded
        .iter()
        .find(|fixture| fixture.name == "log")
        .cloned()
        .ok_or_else(|| anyhow!("expected log fixture"))?;
    let rest = loaded
        .iter()
        .filter(|fixture| fixture.name != "log")
        .filter(|fixture| !fixture.name.starts_with("log_variation_"))
        .cloned()
        .collect::<Vec<_>>();
    let metric_fixtures = loaded
        .iter()
        .filter(|fixture| fixture.name.starts_with("metric"))
        .cloned()
        .collect::<Vec<_>>();
    let trace_fixtures = loaded
        .iter()
        .filter(|fixture| is_trace_fixture(&fixture.name))
        .cloned()
        .collect::<Vec<_>>();
    let product_fixtures = loaded
        .iter()
        .filter(|fixture| is_product_fixture(&fixture.name))
        .cloned()
        .collect::<Vec<_>>();
    let agent_fixtures = loaded
        .iter()
        .filter(|fixture| is_agent_fixture(&fixture.name))
        .cloned()
        .collect::<Vec<_>>();
    let pipeline_fixtures = loaded
        .iter()
        .filter(|fixture| is_pipeline_fixture(&fixture.name))
        .cloned()
        .collect::<Vec<_>>();
    let log_variations = loaded
        .into_iter()
        .filter(|fixture| fixture.name.starts_with("log_variation_"))
        .collect::<Vec<_>>();
    if rest.is_empty() {
        bail!("expected at least one non-log fixture");
    }
    if metric_fixtures.is_empty() {
        bail!("expected at least one metric fixture");
    }
    if trace_fixtures.is_empty() {
        bail!("expected at least one trace fixture");
    }
    if product_fixtures.is_empty() {
        bail!("expected at least one product fixture");
    }
    if agent_fixtures.is_empty() {
        bail!("expected at least one agent fixture");
    }
    if pipeline_fixtures.is_empty() {
        bail!("expected at least one pipeline fixture");
    }
    Ok(Fixtures {
        log,
        log_variations,
        metric_fixtures,
        trace_fixtures,
        product_fixtures,
        agent_fixtures,
        pipeline_fixtures,
        rest,
    })
}

fn is_product_fixture(name: &str) -> bool {
    name.starts_with("product_") || name.starts_with("state_")
}

fn is_trace_fixture(name: &str) -> bool {
    matches!(name, "span_start" | "span_end") || name.starts_with("span_")
}

fn is_agent_fixture(name: &str) -> bool {
    name.starts_with("agent_")
        || matches!(
            name,
            "llm_call" | "tool_call" | "retrieval_step" | "eval_score" | "safety_event"
        )
}

fn is_pipeline_fixture(name: &str) -> bool {
    name.starts_with("materializer_") || name.starts_with("pipeline_")
}

fn validate_fixture(name: &str, fixture: &Value) -> Result<()> {
    let object = fixture
        .as_object()
        .ok_or_else(|| anyhow!("fixture {name} must be a JSON object"))?;
    for key in ["event_id", "timestamp", "data"] {
        if !object.contains_key(key) {
            bail!("fixture {name} is missing {key}");
        }
    }
    if !object.get("data").is_some_and(Value::is_object) {
        bail!("fixture {name} data must be an object");
    }
    for key in [
        "source_file",
        "source_offset",
        "source_length",
        "ingested_timestamp",
    ] {
        if object.contains_key(key) {
            bail!("fixture {name} should not set server/ClickHouse-owned field {key}");
        }
    }
    Ok(())
}

fn clickhouse_config(outputs: Option<&Value>) -> Result<Option<ClickHouseConfig>> {
    let url = env::var("CLICKHOUSE_URL").ok();
    let user = env::var("CLICKHOUSE_USER").ok();
    let password = env::var("CLICKHOUSE_PASSWORD").ok();

    if url.is_none() && user.is_none() && password.is_none() {
        return Ok(None);
    }

    let database = env::var("CLICKHOUSE_DATABASE")
        .ok()
        .or_else(|| output_string(outputs, "clickhouseDatabase"))
        .or_else(|| output_string(outputs, "clickhouseDatabaseOutput"))
        .unwrap_or_else(|| "observatory".to_owned());
    let table = env::var("CLICKHOUSE_TABLE")
        .ok()
        .or_else(|| output_string(outputs, "clickhouseTable"))
        .or_else(|| output_string(outputs, "clickhouseTableOutput"))
        .unwrap_or_else(|| "events".to_owned());

    validate_identifier(&database, "CLICKHOUSE_DATABASE")?;
    validate_identifier(&table, "CLICKHOUSE_TABLE")?;

    Ok(Some(ClickHouseConfig {
        url: url.ok_or_else(|| anyhow!("CLICKHOUSE_URL is required for ClickHouse observation"))?,
        user: user
            .ok_or_else(|| anyhow!("CLICKHOUSE_USER is required for ClickHouse observation"))?,
        password: password
            .ok_or_else(|| anyhow!("CLICKHOUSE_PASSWORD is required for ClickHouse observation"))?,
        database,
        table,
    }))
}

async fn wait_for_clickhouse(
    context: &LoadContext,
    clickhouse: &ClickHouseConfig,
    accepted_events: u64,
) -> Result<()> {
    if accepted_events == 0 {
        println!("clickhouseObservation=skipped reason=no accepted events");
        return Ok(());
    }

    println!(
        "clickhouseObservation=enabled database={} table={} targetEvents={} waitMs={} pollMs={}",
        clickhouse.database,
        clickhouse.table,
        accepted_events,
        context.config.clickhouse_wait_ms,
        context.config.clickhouse_poll_ms
    );

    let deadline =
        Instant::now() + Duration::from_secs_f64(context.config.clickhouse_wait_ms / 1_000.0);
    let poll = Duration::from_secs_f64(context.config.clickhouse_poll_ms / 1_000.0);
    let mut last_error = None;
    let mut last_stats = None;

    while Instant::now() < deadline {
        match clickhouse_stats(context, clickhouse).await {
            Ok(stats) => {
                println!(
                    "clickhouseVisibleEvents={} targetEvents={} p50IngestLagMs={} p95IngestLagMs={} maxIngestLagMs={}",
                    stats.visible_events,
                    accepted_events,
                    stats.p50_ingest_lag_ms,
                    stats.p95_ingest_lag_ms,
                    stats.max_ingest_lag_ms
                );
                if stats.visible_events >= accepted_events {
                    println!(
                        "clickhouseObservation=complete visibleEvents={} targetEvents={} firstIngestedTimestamp={} lastIngestedTimestamp={}",
                        stats.visible_events,
                        accepted_events,
                        stats.first_ingested_timestamp,
                        stats.last_ingested_timestamp
                    );
                    return Ok(());
                }
                last_stats = Some(stats);
                last_error = None;
            }
            Err(error) => {
                last_error = Some(error.to_string());
            }
        }
        sleep(poll).await;
    }

    match (last_stats, last_error) {
        (Some(stats), _) => bail!(
            "timed out waiting for ClickHouse visibility: visibleEvents={} targetEvents={}",
            stats.visible_events,
            accepted_events
        ),
        (_, Some(error)) => bail!("timed out waiting for ClickHouse visibility: {error}"),
        _ => bail!("timed out waiting for ClickHouse visibility"),
    }
}

struct ClickHouseStats {
    visible_events: u64,
    first_ingested_timestamp: String,
    last_ingested_timestamp: String,
    p50_ingest_lag_ms: f64,
    p95_ingest_lag_ms: f64,
    max_ingest_lag_ms: f64,
}

async fn clickhouse_stats(
    context: &LoadContext,
    clickhouse: &ClickHouseConfig,
) -> Result<ClickHouseStats> {
    let ingest_lag =
        "toUnixTimestamp64Milli(ingested_timestamp) - toUnixTimestamp64Milli(timestamp)";
    let sql = format!(
        "\
SELECT
    count() AS visible_events,
    min(toString(ingested_timestamp)) AS first_ingested_timestamp,
    max(toString(ingested_timestamp)) AS last_ingested_timestamp,
    quantileExact(0.50)({ingest_lag}) AS p50_ingest_lag_ms,
    quantileExact(0.95)({ingest_lag}) AS p95_ingest_lag_ms,
    max({ingest_lag}) AS max_ingest_lag_ms
FROM {}.{}
WHERE getSubcolumn(data, '_loadtest.run_id') = '{}'
FORMAT JSON",
        quote_identifier(&clickhouse.database),
        quote_identifier(&clickhouse.table),
        escape_sql_string(&context.config.run_id)
    );

    let response = context
        .client
        .post(&clickhouse.url)
        .basic_auth(&clickhouse.user, Some(&clickhouse.password))
        .header(CONTENT_TYPE, "text/plain; charset=utf-8")
        .body(sql)
        .send()
        .await
        .context("query ClickHouse")?;
    let status = response.status();
    let text = response.text().await.context("read ClickHouse response")?;
    if !status.is_success() {
        bail!("ClickHouse query failed: {status} {text}");
    }

    let parsed = serde_json::from_str::<Value>(&text).context("parse ClickHouse JSON response")?;
    let row = parsed
        .get("data")
        .and_then(Value::as_array)
        .and_then(|rows| rows.first())
        .and_then(Value::as_object)
        .ok_or_else(|| anyhow!("ClickHouse response did not include a data row"))?;

    Ok(ClickHouseStats {
        visible_events: value_to_u64(row.get("visible_events")),
        first_ingested_timestamp: value_to_string(row.get("first_ingested_timestamp")),
        last_ingested_timestamp: value_to_string(row.get("last_ingested_timestamp")),
        p50_ingest_lag_ms: value_to_f64(row.get("p50_ingest_lag_ms")),
        p95_ingest_lag_ms: value_to_f64(row.get("p95_ingest_lag_ms")),
        max_ingest_lag_ms: value_to_f64(row.get("max_ingest_lag_ms")),
    })
}

fn output_string(outputs: Option<&Value>, key: &str) -> Option<String> {
    outputs?.get(key).and_then(Value::as_str).map(str::to_owned)
}

fn validate_identifier(value: &str, env_key: &str) -> Result<()> {
    let valid = value.chars().enumerate().all(|(index, character)| {
        matches!(
            (index, character),
            (0, 'A'..='Z' | 'a'..='z' | '_')
                | (_, 'A'..='Z' | 'a'..='z' | '0'..='9' | '_')
        )
    });
    if !valid {
        bail!("{env_key} must be a simple ClickHouse identifier");
    }
    Ok(())
}

fn quote_identifier(value: &str) -> String {
    format!("`{}`", value.replace('`', "``"))
}

fn escape_sql_string(value: &str) -> String {
    value.replace('\\', "\\\\").replace('\'', "\\'")
}

fn value_to_u64(value: Option<&Value>) -> u64 {
    match value {
        Some(Value::Number(number)) => number.as_u64().unwrap_or(0),
        Some(Value::String(string)) => string.parse().unwrap_or(0),
        _ => 0,
    }
}

fn value_to_f64(value: Option<&Value>) -> f64 {
    match value {
        Some(Value::Number(number)) => number.as_f64().unwrap_or(0.0),
        Some(Value::String(string)) => string.parse().unwrap_or(0.0),
        _ => 0.0,
    }
}

fn value_to_string(value: Option<&Value>) -> String {
    match value {
        Some(Value::String(string)) => string.clone(),
        Some(value) if !value.is_null() => value.to_string(),
        _ => String::new(),
    }
}

async fn pulumi_outputs(root: &Path) -> Result<Value> {
    let output = Command::new("pulumi")
        .args(["stack", "output", "--json"])
        .current_dir(root.join("deploy/pulumi/nanotrace"))
        .output()
        .await
        .context("run pulumi stack output --json")?;
    if !output.status.success() {
        bail!(
            "pulumi stack output --json failed with {}\n{}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        );
    }
    serde_json::from_slice(&output.stdout).context("parse Pulumi outputs")
}

fn load_env_file(path: &Path) -> Result<()> {
    let text = match fs::read_to_string(path) {
        Ok(text) => text,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error).with_context(|| format!("read {}", path.display())),
    };

    for line in text.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        let trimmed = trimmed.strip_prefix("export ").unwrap_or(trimmed);
        let Some((key, value)) = trimmed.split_once('=') else {
            continue;
        };
        if env::var_os(key).is_none() {
            // SAFETY: this process sets env vars during single-threaded startup, before any
            // async tasks are spawned or other threads are intentionally used by this binary.
            unsafe {
                env::set_var(key, parse_env_value(value));
            }
        }
    }
    Ok(())
}

fn parse_env_value(value: &str) -> String {
    let trimmed = value.trim();
    if (trimmed.starts_with('"') && trimmed.ends_with('"'))
        || (trimmed.starts_with('\'') && trimmed.ends_with('\''))
    {
        trimmed[1..trimmed.len() - 1].to_owned()
    } else {
        trimmed.to_owned()
    }
}

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("tools/loadtest must live under repo root")
        .to_owned()
}

fn required_env(key: &str) -> Result<String> {
    env::var(key).with_context(|| format!("{key} is required"))
}

fn number_env(key: &str, fallback: f64) -> Result<f64> {
    env::var(key)
        .map(|value| parse_positive_f64(key, &value))
        .unwrap_or(Ok(fallback))
}

fn integer_env(key: &str, fallback: u64) -> Result<u64> {
    env::var(key)
        .map(|value| parse_positive_u64(key, &value))
        .unwrap_or(Ok(fallback))
}

fn integer_env_allow_zero(key: &str, fallback: u64) -> Result<u64> {
    env::var(key)
        .map(|value| parse_u64(key, &value))
        .unwrap_or(Ok(fallback))
}

fn optional_integer_env(key: &str) -> Result<Option<u64>> {
    env::var(key)
        .map(|value| parse_positive_u64(key, &value).map(Some))
        .unwrap_or(Ok(None))
}

fn list_env(key: &str, fallback: &[u64]) -> Result<Vec<u64>> {
    env::var(key)
        .map(|value| parse_positive_u64_list(key, &value))
        .unwrap_or_else(|_| Ok(fallback.to_vec()))
}

fn parse_positive_f64(key: &str, value: &str) -> Result<f64> {
    let parsed = value
        .parse::<f64>()
        .with_context(|| format!("{key} must be a number"))?;
    if parsed <= 0.0 {
        bail!("{key} must be positive");
    }
    Ok(parsed)
}

fn parse_u64(key: &str, value: &str) -> Result<u64> {
    value
        .parse::<u64>()
        .with_context(|| format!("{key} must be an integer"))
}

fn parse_positive_u64(key: &str, value: &str) -> Result<u64> {
    let parsed = parse_u64(key, value)?;
    if parsed == 0 {
        bail!("{key} must be positive");
    }
    Ok(parsed)
}

fn parse_positive_u64_list(key: &str, value: &str) -> Result<Vec<u64>> {
    value
        .split(',')
        .map(str::trim)
        .map(|item| {
            let parsed =
                parse_u64(key, item).with_context(|| format!("{key} must contain integers"))?;
            if parsed == 0 {
                bail!("{key} values must be positive");
            }
            Ok(parsed)
        })
        .collect()
}

fn load_profile_env() -> Result<LoadProfile> {
    match env::var("NANOTRACE_LOADTEST_PROFILE") {
        Ok(value) => parse_load_profile(&value).ok_or_else(|| {
            anyhow::anyhow!(
                "NANOTRACE_LOADTEST_PROFILE must be codex, atlas, llm, realistic, trace, metrics, logs, product, agent, pipeline, fixture, default, or static; got {value}"
            )
        }),
        Err(_) => Ok(LoadProfile::Codex),
    }
}

fn parse_load_profile(value: &str) -> Option<LoadProfile> {
    let normalized = value.to_ascii_lowercase();
    let profile = match normalized.as_str() {
        "codex" | "codex_mixed" | "mixed" => LoadProfile::Codex,
        "atlas" | "atlas_mixed" => LoadProfile::Atlas,
        "realistic" => LoadProfile::Realistic,
        "trace" | "traces" | "realistic_trace" | "realistic_traces" => LoadProfile::Trace,
        "metrics" | "metric" | "pure_metrics" => LoadProfile::Metrics,
        "logs" | "log" | "pure_logs" => LoadProfile::Logs,
        "product" | "products" | "analytics" => LoadProfile::Product,
        "agent" | "agents" | "agentic" | "agent_traces" => LoadProfile::Agent,
        "pipeline" | "pipelines" | "materializer" | "materializers" => LoadProfile::Pipeline,
        "llm" | "realistic_llm" | "llm_realistic" => LoadProfile::Llm,
        "fixture" | "default" | "static" => LoadProfile::Fixture,
        _ => return None,
    };
    Some(profile)
}

fn trim_trailing_slash(mut value: String) -> String {
    while value.ends_with('/') {
        value.pop();
    }
    value
}

fn default_run_id() -> String {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    format!("load-{millis}-{}", process::id())
}

fn percentile(values: &[f64], p: f64) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(f64::total_cmp);
    let index = ((sorted.len() as f64 * p).ceil() as usize).saturating_sub(1);
    round(sorted[index.min(sorted.len() - 1)], 1)
}

fn round(value: f64, digits: u32) -> f64 {
    let scale = 10_f64.powi(digits as i32);
    (value * scale).round() / scale
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_positive_env_numbers() {
        assert_eq!(parse_positive_u64("COUNT", "42").unwrap(), 42);
        assert_eq!(parse_positive_f64("SECONDS", "0.5").unwrap(), 0.5);
        assert!(parse_positive_u64("COUNT", "0").is_err());
        assert!(parse_positive_f64("SECONDS", "0").is_err());
    }

    #[test]
    fn parses_positive_env_integer_lists() {
        assert_eq!(
            parse_positive_u64_list("BATCH_SIZES", "1, 10,100").unwrap(),
            vec![1, 10, 100]
        );
        assert!(parse_positive_u64_list("BATCH_SIZES", "1,0,10").is_err());
        assert!(parse_positive_u64_list("BATCH_SIZES", "1,nope,10").is_err());
    }

    #[test]
    fn generated_event_preserves_fixture_shape_without_double_wrapping() {
        let context = test_context();

        let event = make_event(&context, "test_mix").expect("event");
        let object = event.as_object().expect("event object");

        assert!(Uuid::parse_str(object["event_id"].as_str().unwrap()).is_ok());
        assert!(object.get("tenant_id").is_none());
        assert!(object.get("service").is_none());
        assert!(object.get("event_type").is_none());
        assert!(object["data"].get("data").is_none());
        assert_eq!(object["data"]["tenant_id"], "loadtest");
        assert_eq!(object["data"]["service"], "api");
        assert_eq!(object["data"]["event_type"], "log");
        assert_eq!(object["data"]["message"], "hello");
        assert_eq!(object["data"]["_loadtest"]["run_id"], "test-run");
        assert_eq!(object["data"]["_loadtest"]["fixture"], "log");
    }

    #[test]
    fn generated_event_sequence_can_be_sharded() {
        let context = test_context_with_sequence(2, 4);

        let first = make_event(&context, "test_mix").expect("first event");
        let second = make_event(&context, "test_mix").expect("second event");

        assert!(Uuid::parse_str(first["event_id"].as_str().expect("first event id")).is_ok());
        assert_eq!(first["data"]["_loadtest"]["sequence"], 2);
        assert!(Uuid::parse_str(second["event_id"].as_str().expect("second event id")).is_ok());
        assert_ne!(first["event_id"], second["event_id"]);
        assert_eq!(second["data"]["_loadtest"]["sequence"], 6);
    }

    #[test]
    fn fixture_selection_uses_log_ratio_then_rest() {
        let context = test_context();

        assert_eq!(
            choose_fixture(&context.fixtures, 0, 0.10, LoadProfile::Fixture).name,
            "log"
        );
        assert_eq!(
            choose_fixture(&context.fixtures, 9, 0.10, LoadProfile::Fixture).name,
            "log"
        );
        assert_eq!(
            choose_fixture(&context.fixtures, 10, 0.10, LoadProfile::Fixture).name,
            "metric"
        );
        assert_eq!(
            choose_fixture(&context.fixtures, 1, 0.10, LoadProfile::Realistic).name,
            "log_variation_code_success"
        );
        assert_eq!(
            choose_fixture(&context.fixtures, 0, 0.10, LoadProfile::Logs).name,
            "log"
        );
        assert_eq!(
            choose_fixture(&context.fixtures, 1, 0.10, LoadProfile::Logs).name,
            "log_variation_code_success"
        );
        assert_eq!(
            choose_fixture(&context.fixtures, 0, 0.10, LoadProfile::Metrics).name,
            "metric"
        );
        assert_eq!(
            choose_fixture(&context.fixtures, 0, 0.10, LoadProfile::Trace).name,
            "span_start"
        );
    }

    #[test]
    fn load_profile_aliases_parse_to_explicit_profiles() {
        assert_eq!(parse_load_profile("mixed"), Some(LoadProfile::Codex));
        assert_eq!(
            parse_load_profile("REALISTIC_TRACES"),
            Some(LoadProfile::Trace)
        );
        assert_eq!(
            parse_load_profile("materializers"),
            Some(LoadProfile::Pipeline)
        );
        assert_eq!(parse_load_profile("static"), Some(LoadProfile::Fixture));
        assert_eq!(parse_load_profile("unknown"), None);
    }

    #[test]
    fn bundled_fixtures_include_realistic_log_variations() {
        let fixtures = load_fixtures(&repo_root()).expect("load fixtures");

        assert_eq!(fixtures.log.name, "log");
        assert!(fixtures.log_variations.len() >= 3);
        assert!(
            fixtures
                .rest
                .iter()
                .any(|fixture| fixture.name == "span_start")
        );
        assert!(
            fixtures
                .rest
                .iter()
                .any(|fixture| fixture.name == "metric_runtime")
        );
        assert!(
            fixtures
                .metric_fixtures
                .iter()
                .all(|fixture| fixture.name.starts_with("metric"))
        );
        assert!(
            fixtures
                .trace_fixtures
                .iter()
                .all(|fixture| fixture.name == "span_start" || fixture.name == "span_end")
        );
        assert!(
            fixtures
                .product_fixtures
                .iter()
                .any(|fixture| fixture.name == "product_checkout_completed")
        );
        assert!(
            fixtures
                .agent_fixtures
                .iter()
                .any(|fixture| fixture.name == "agent_request")
        );
        assert!(
            fixtures
                .pipeline_fixtures
                .iter()
                .any(|fixture| fixture.name == "materializer_backfill_slice")
        );
    }

    #[test]
    fn new_profiles_select_their_fixture_families() {
        let fixtures = load_fixtures(&repo_root()).expect("load fixtures");

        assert!(is_product_fixture(
            &choose_fixture(&fixtures, 0, 0.10, LoadProfile::Product).name
        ));
        assert!(is_agent_fixture(
            &choose_fixture(&fixtures, 0, 0.10, LoadProfile::Agent).name
        ));
        assert!(is_pipeline_fixture(
            &choose_fixture(&fixtures, 0, 0.10, LoadProfile::Pipeline).name
        ));
        assert_eq!(
            choose_fixture(&fixtures, 4, 0.10, LoadProfile::Codex).name,
            "llm_call"
        );
        assert_eq!(
            choose_fixture(&fixtures, 6, 0.10, LoadProfile::Codex).name,
            "tool_call"
        );
        assert_eq!(
            choose_fixture(&fixtures, 1, 0.10, LoadProfile::Codex).name,
            "agent_request"
        );
        assert_eq!(
            choose_fixture(&fixtures, 23, 0.10, LoadProfile::Codex).name,
            "span_end"
        );
        assert_eq!(
            choose_fixture(&fixtures, 18, 0.10, LoadProfile::Codex).name,
            "safety_event"
        );
        assert_eq!(
            choose_fixture(&fixtures, 46, 0.10, LoadProfile::Codex).name,
            "eval_score"
        );
        assert!(matches!(
            choose_fixture(&fixtures, 50, 0.10, LoadProfile::Atlas)
                .body
                .get("data")
                .and_then(Value::as_object)
                .and_then(|data| data.get("event_type"))
                .and_then(Value::as_str),
            Some(
                "agent.request"
                    | "agent.decision"
                    | "llm.call"
                    | "tool.call"
                    | "retrieval.step"
                    | "eval.score"
                    | "safety.event"
            )
        ));
    }

    #[test]
    fn realistic_profile_randomizes_log_data_but_preserves_shape() {
        let context = test_context_with_profile(LoadProfile::Realistic);

        let event = make_event(&context, "test_mix").expect("event");
        let data = event["data"].as_object().expect("data object");

        assert_eq!(data["tenant_id"], "loadtest");
        assert_eq!(data["event_type"], "log");
        assert_eq!(data["_loadtest"]["run_id"], "test-run");
        assert_ne!(data["message"], "hello");
        assert_ne!(data["trace_id"], "4bf92f3577b34da6a3ce929d0e0e4736");
        assert_eq!(data["trace_id"].as_str().unwrap().len(), 32);
        assert_eq!(data["span_id"].as_str().unwrap().len(), 16);
        assert_ne!(data["canvasId"], "9dc05d76-e919-4c53-9274-c8060774ee8a");
        assert!(data["llm"].is_object());
        assert!(data["llm"]["messages"].is_array());
        assert_ne!(data["llm"]["messages"][0]["content"], "fixture prompt");
    }

    #[test]
    fn realistic_profile_randomizes_metric_values_and_http_fields() {
        let context = test_context_with_profile(LoadProfile::Realistic);
        context.config.event_seq.store(10, Ordering::Relaxed);

        let event = make_event(&context, "test_mix").expect("event");
        let data = event["data"].as_object().expect("data object");

        assert_eq!(data["event_type"], "metric");
        assert_ne!(data["metric_value"], 231);
        assert!(data["metric_value"].as_f64().unwrap() > 0.0);
        assert!(data["http.route"].as_str().unwrap().starts_with('/'));
        assert!(data["http.status_code"].as_u64().unwrap() >= 200);
        assert!(data["duration_ms"].as_u64().unwrap() > 0);
    }

    #[test]
    fn realistic_profile_is_deterministic_for_run_fixture_and_sequence() {
        let first = test_context_with_profile(LoadProfile::Realistic);
        let second = test_context_with_profile(LoadProfile::Realistic);

        let first_event = make_event(&first, "test_mix").expect("first event");
        let second_event = make_event(&second, "test_mix").expect("second event");

        assert_eq!(
            first_event["data"]["trace_id"],
            second_event["data"]["trace_id"]
        );
        assert_eq!(
            first_event["data"]["message"],
            second_event["data"]["message"]
        );
        assert_eq!(
            first_event["data"]["llm"]["messages"][0]["content"],
            second_event["data"]["llm"]["messages"][0]["content"]
        );
    }

    #[test]
    fn llm_profile_generates_coherent_usage_and_historical_timestamp() {
        let context = test_context_with_profile(LoadProfile::Llm);

        let event = make_event(&context, "test_mix").expect("event");
        let timestamp = DateTime::parse_from_rfc3339(event["timestamp"].as_str().unwrap())
            .expect("timestamp")
            .with_timezone(&Utc);
        let observed = DateTime::parse_from_rfc3339(event["observed_timestamp"].as_str().unwrap())
            .expect("observed timestamp")
            .with_timezone(&Utc);
        let now = Utc::now();
        assert!(timestamp <= now);
        assert!(timestamp >= now - ChronoDuration::days(60) - ChronoDuration::seconds(1));
        assert!(observed > timestamp);

        let data = event["data"].as_object().expect("data object");
        let llm = data["llm"].as_object().expect("llm object");
        let usage = llm["totalUsage"].as_object().expect("usage object");
        let cached = usage["cachedInputTokens"].as_u64().unwrap();
        let no_cache = usage["inputTokenDetails"]["noCacheTokens"]
            .as_u64()
            .unwrap();
        let input = usage["inputTokens"].as_u64().unwrap();
        let reasoning = usage["reasoningTokens"].as_u64().unwrap();
        let text = usage["outputTokenDetails"]["textTokens"].as_u64().unwrap();
        let output = usage["outputTokens"].as_u64().unwrap();
        let total = usage["totalTokens"].as_u64().unwrap();

        assert_eq!(data["event_type"], "log");
        assert_eq!(llm["messages"].as_array().unwrap().len(), 2);
        assert!(llm["request"].is_object());
        assert!(llm["response"].is_object());
        assert_eq!(
            usage["inputTokenDetails"]["cacheReadTokens"]
                .as_u64()
                .unwrap(),
            cached
        );
        assert_eq!(input, cached + no_cache);
        assert_eq!(
            usage["outputTokenDetails"]["reasoningTokens"]
                .as_u64()
                .unwrap(),
            reasoning
        );
        assert_eq!(output, reasoning + text);
        assert_eq!(total, input + output);
    }

    #[test]
    fn codex_profile_generates_correlated_workflow_events() {
        let context = test_context_with_profile(LoadProfile::Codex);

        let first = make_event(&context, "codex_mix").expect("first event");
        let second = make_event(&context, "codex_mix").expect("second event");
        let mut llm = None;
        for _ in 0..3 {
            llm = Some(make_event(&context, "codex_mix").expect("event"));
        }
        let llm = llm.expect("llm event");
        let first_data = first["data"].as_object().expect("first data");
        let second_data = second["data"].as_object().expect("second data");
        let llm_data = llm["data"].as_object().expect("llm data");

        assert_eq!(first_data["event_type"], "span_start");
        assert_eq!(second_data["event_type"], "agent.request");
        assert_eq!(first_data["trace_id"], second_data["trace_id"]);
        assert_eq!(llm_data["event_type"], "llm.call");
        assert_eq!(llm_data["trace_id"], first_data["trace_id"]);
        assert!(llm_data["llm"]["messages"].is_array());
        assert!(llm_data["llm"]["tools"].is_array());
    }

    #[test]
    fn codex_profile_keeps_route_method_and_environment_consistent_within_trace() {
        let context = test_context_with_profile(LoadProfile::Codex);
        let events = (0..24)
            .map(|_| make_event(&context, "codex_mix").expect("event"))
            .collect::<Vec<_>>();

        let trace_id = events[0]["data"]["trace_id"].clone();
        let environment = events
            .iter()
            .find_map(|event| event["data"].get("environment").cloned())
            .expect("environment present");
        let route = events
            .iter()
            .find_map(|event| event["data"].get("http.route").cloned())
            .expect("route present");
        let method = events
            .iter()
            .find_map(|event| event["data"].get("http.method").cloned())
            .expect("method present");

        for event in &events {
            let data = event["data"].as_object().expect("data object");
            if let Some(value) = data.get("trace_id") {
                assert_eq!(value, &trace_id);
            }
            if let Some(value) = data.get("environment") {
                assert_eq!(value, &environment);
            }
            if let Some(value) = data.get("http.route") {
                assert_eq!(value, &route);
            }
            if let Some(value) = data.get("http.method") {
                assert_eq!(value, &method);
            }
        }
    }

    #[test]
    fn realistic_mutation_does_not_overwrite_nested_tool_name_with_route_name() {
        let context = test_context_with_profile(LoadProfile::Codex);
        context.config.event_seq.store(2, Ordering::Relaxed);

        let event = make_event(&context, "codex_mix").expect("event");
        let tool_name = event["data"]["tool"]["name"]
            .as_str()
            .expect("tool name string");
        assert!(!tool_name.contains("/api/"));
        assert!(!tool_name.contains("/v1/"));
    }

    #[test]
    fn bundled_codex_fixtures_preserve_shape_under_realistic_mutation() {
        let fixtures = load_fixtures(&repo_root()).expect("load fixtures");
        let names = [
            "log",
            "log_variation_code_success",
            "log_variation_excalidraw",
            "log_variation_image_gen",
            "llm_call",
            "tool_call",
            "retrieval_step",
            "agent_request",
            "agent_decision",
            "safety_event",
            "eval_score",
            "metric",
            "metric_counter",
            "metric_histogram",
            "metric_runtime",
            "span_start",
            "span_end",
        ];

        for (index, name) in names.iter().enumerate() {
            let fixture =
                find_fixture(&fixtures, name).unwrap_or_else(|| panic!("missing fixture {name}"));
            let original = fixture.body["data"]
                .as_object()
                .expect("fixture data object");
            let mut mutated = original.clone();

            randomize_realistic_data(
                &mut mutated,
                &fixture.name,
                "test-run",
                index as u64,
                "2026-05-12T00:00:00.000Z",
                96,
                false,
            );

            assert_shape_preserved(original, &mutated, &format!("fixture {name}"));
        }
    }

    #[test]
    fn llm_timestamp_weights_favor_weekday_business_hours() {
        let weekday = DateTime::parse_from_rfc3339("2026-05-13T00:00:00.000Z")
            .expect("weekday")
            .with_timezone(&Utc);
        let weekend = DateTime::parse_from_rfc3339("2026-05-16T00:00:00.000Z")
            .expect("weekend")
            .with_timezone(&Utc);

        assert!(llm_traffic_weight(weekday, 14) > llm_traffic_weight(weekday, 2));
        assert!(llm_traffic_weight(weekday, 14) > llm_traffic_weight(weekend, 14));
    }

    #[test]
    fn realistic_profile_builds_user_session_trace_hierarchy() {
        let first = realistic_data_for_seq(0);
        let same_trace_child = realistic_data_for_seq(7);
        let next_trace_same_session = realistic_data_for_seq(24);
        let next_session_same_user = realistic_data_for_seq(24 * 12);
        let next_user = realistic_data_for_seq(24 * 12 * 6);

        assert_eq!(first["user_id"], same_trace_child["user_id"]);
        assert_eq!(first["session_id"], same_trace_child["session_id"]);
        assert_eq!(first["trace_id"], same_trace_child["trace_id"]);
        assert_ne!(first["span_id"], same_trace_child["span_id"]);
        assert_eq!(same_trace_child["parent_span_id"], first["span_id"]);

        assert_eq!(first["user_id"], next_trace_same_session["user_id"]);
        assert_eq!(first["session_id"], next_trace_same_session["session_id"]);
        assert_ne!(first["trace_id"], next_trace_same_session["trace_id"]);

        assert_eq!(first["user_id"], next_session_same_user["user_id"]);
        assert_ne!(first["session_id"], next_session_same_user["session_id"]);

        assert_ne!(first["user_id"], next_user["user_id"]);
        assert_ne!(first["session_id"], next_user["session_id"]);
    }

    fn test_context() -> LoadContext {
        test_context_with_profile(LoadProfile::Fixture)
    }

    fn find_fixture<'a>(fixtures: &'a Fixtures, name: &str) -> Option<&'a Fixture> {
        fixtures
            .log_variations
            .iter()
            .chain(fixtures.metric_fixtures.iter())
            .chain(fixtures.trace_fixtures.iter())
            .chain(fixtures.product_fixtures.iter())
            .chain(fixtures.agent_fixtures.iter())
            .chain(fixtures.pipeline_fixtures.iter())
            .chain(fixtures.rest.iter())
            .chain(std::iter::once(&fixtures.log))
            .find(|fixture| fixture.name == name)
    }

    fn assert_shape_preserved(
        original: &Map<String, Value>,
        mutated: &Map<String, Value>,
        context: &str,
    ) {
        for (key, original_value) in original {
            let Some(mutated_value) = mutated.get(key) else {
                panic!("missing key {key} in {context}");
            };
            assert_json_shape_preserved(original_value, mutated_value, &format!("{context}.{key}"));
        }
    }

    fn assert_json_shape_preserved(original: &Value, mutated: &Value, path: &str) {
        match (original, mutated) {
            (Value::Object(original_map), Value::Object(mutated_map)) => {
                assert_shape_preserved(original_map, mutated_map, path);
            }
            (Value::Array(original_items), Value::Array(mutated_items)) => {
                assert_eq!(
                    original_items.len(),
                    mutated_items.len(),
                    "array length mismatch at {path}"
                );
                for (index, (original_item, mutated_item)) in
                    original_items.iter().zip(mutated_items.iter()).enumerate()
                {
                    assert_json_shape_preserved(
                        original_item,
                        mutated_item,
                        &format!("{path}[{index}]"),
                    );
                }
            }
            _ => assert_eq!(
                json_kind(original),
                json_kind(mutated),
                "type mismatch at {path}"
            ),
        }
    }

    fn json_kind(value: &Value) -> &'static str {
        match value {
            Value::Null => "null",
            Value::Bool(_) => "bool",
            Value::Number(_) => "number",
            Value::String(_) => "string",
            Value::Array(_) => "array",
            Value::Object(_) => "object",
        }
    }

    fn realistic_data_for_seq(seq: u64) -> Map<String, Value> {
        let mut data = json!({
            "tenant_id": "fixture",
            "service": "api",
            "event_type": "span_start",
            "trace_id": "4bf92f3577b34da6a3ce929d0e0e4736",
            "span_id": "00f067aa0ba902b7",
            "parent_span_id": "",
            "user_id": "user_fixture",
            "session_id": "sess_fixture",
            "account_id": "acct_fixture"
        })
        .as_object()
        .cloned()
        .expect("data object");
        randomize_realistic_data(
            &mut data,
            "span_start",
            "test-run",
            seq,
            "2026-05-12T00:00:00.000Z",
            96,
            false,
        );
        data
    }

    fn test_context_with_profile(profile: LoadProfile) -> LoadContext {
        test_context_with_profile_and_sequence(profile, 0, 1)
    }

    fn test_context_with_sequence(sequence_offset: u64, sequence_stride: u64) -> LoadContext {
        test_context_with_profile_and_sequence(
            LoadProfile::Fixture,
            sequence_offset,
            sequence_stride,
        )
    }

    fn test_context_with_profile_and_sequence(
        profile: LoadProfile,
        sequence_offset: u64,
        sequence_stride: u64,
    ) -> LoadContext {
        LoadContext {
            config: Arc::new(Config {
                ingest_url: "http://127.0.0.1:3000".to_owned(),
                api_key: "ntak_test".to_owned(),
                run_id: "test-run".to_owned(),
                batch_sizes: vec![1],
                step_seconds: 1.0,
                cooldown_ms: 1.0,
                start_rps: 1,
                max_rps: 1,
                binary_rounds: 1,
                max_error_rate: 0.01,
                max_p95_ms: 2_000.0,
                max_in_flight: 1,
                log_ratio: 0.10,
                profile,
                trace_depth: 96,
                total_events: None,
                sequence_offset,
                sequence_stride,
                clickhouse_wait_ms: 1_000.0,
                clickhouse_poll_ms: 100.0,
                event_seq: AtomicU64::new(0),
                accepted_events: AtomicU64::new(0),
            }),
            fixtures: Arc::new(Fixtures {
                log: Fixture {
                    name: "log".to_owned(),
                    body: json!({
                        "event_id": "fixture-log",
                        "timestamp": "2026-05-08T01:23:45.123Z",
                        "observed_timestamp": "2026-05-08T01:23:45.130Z",
                        "data": {
                            "tenant_id": "fixture",
                            "service": "api",
                            "event_type": "log",
                            "message": "hello",
                            "environment": "prod",
                            "trace_id": "4bf92f3577b34da6a3ce929d0e0e4736",
                            "span_id": "00f067aa0ba902b7",
                            "canvasId": "9dc05d76-e919-4c53-9274-c8060774ee8a",
                            "llm": {
                                "finishReason": "tool-calls",
                                "messages": [
                                    {
                                        "role": "user",
                                        "content": "fixture prompt"
                                    }
                                ],
                                "model": "gpt-5.5"
                            }
                        }
                    }),
                },
                log_variations: vec![Fixture {
                    name: "log_variation_code_success".to_owned(),
                    body: json!({
                        "event_id": "fixture-log-variation",
                        "timestamp": "2026-05-08T01:23:45.123Z",
                        "observed_timestamp": "2026-05-08T01:23:45.130Z",
                        "data": {
                            "tenant_id": "fixture",
                            "service": "api",
                            "event_type": "log",
                            "message": "variation",
                            "environment": "prod",
                            "trace_id": "4bf92f3577b34da6a3ce929d0e0e4736",
                            "span_id": "00f067aa0ba902b7"
                        }
                    }),
                }],
                metric_fixtures: vec![
                    Fixture {
                        name: "metric".to_owned(),
                        body: json!({
                            "event_id": "fixture-metric",
                            "timestamp": "2026-05-08T01:23:45.123Z",
                            "observed_timestamp": "2026-05-08T01:23:45.130Z",
                            "data": {
                                "tenant_id": "fixture",
                                "service": "api",
                                "event_type": "metric",
                                "metric_name": "http.server.duration",
                                "metric_value": 231,
                                "http.method": "POST",
                                "http.route": "/checkout",
                                "http.status_code": 200,
                                "duration_ms": 231
                            }
                        }),
                    },
                    Fixture {
                        name: "metric_counter".to_owned(),
                        body: json!({
                            "event_id": "fixture-metric-counter",
                            "timestamp": "2026-05-08T01:23:45.123Z",
                            "observed_timestamp": "2026-05-08T01:23:45.130Z",
                            "data": {
                                "tenant_id": "fixture",
                                "service": "api",
                                "event_type": "metric",
                                "metric_name": "http.server.requests",
                                "metric_type": "counter",
                                "metric_value": 12
                            }
                        }),
                    },
                    Fixture {
                        name: "metric_histogram".to_owned(),
                        body: json!({
                            "event_id": "fixture-metric-histogram",
                            "timestamp": "2026-05-08T01:23:45.123Z",
                            "observed_timestamp": "2026-05-08T01:23:45.130Z",
                            "data": {
                                "tenant_id": "fixture",
                                "service": "api",
                                "event_type": "metric",
                                "metric_name": "http.server.duration",
                                "metric_type": "histogram",
                                "metric_value": 231
                            }
                        }),
                    },
                    Fixture {
                        name: "metric_runtime".to_owned(),
                        body: json!({
                            "event_id": "fixture-metric-runtime",
                            "timestamp": "2026-05-08T01:23:45.123Z",
                            "observed_timestamp": "2026-05-08T01:23:45.130Z",
                            "data": {
                                "tenant_id": "fixture",
                                "service": "api",
                                "event_type": "metric",
                                "metric_name": "runtime.queue.depth",
                                "metric_type": "gauge",
                                "metric_value": 4
                            }
                        }),
                    },
                ],
                trace_fixtures: vec![
                    Fixture {
                        name: "span_start".to_owned(),
                        body: json!({
                            "event_id": "fixture-span-start",
                            "timestamp": "2026-05-08T01:23:45.123Z",
                            "observed_timestamp": "2026-05-08T01:23:45.130Z",
                            "data": {
                                "tenant_id": "fixture",
                                "service": "api",
                                "event_type": "span_start",
                                "trace_id": "4bf92f3577b34da6a3ce929d0e0e4736",
                                "span_id": "00f067aa0ba902b7",
                                "parent_span_id": "",
                                "name": "POST /checkout",
                                "start_time": "2026-05-08T01:23:45.000Z",
                                "end_time": "2026-05-08T01:23:45.123Z",
                                "duration_ms": 123
                            }
                        }),
                    },
                    Fixture {
                        name: "span_end".to_owned(),
                        body: json!({
                            "event_id": "fixture-span-end",
                            "timestamp": "2026-05-08T01:23:45.223Z",
                            "observed_timestamp": "2026-05-08T01:23:45.230Z",
                            "data": {
                                "tenant_id": "fixture",
                                "service": "api",
                                "event_type": "span_end",
                                "trace_id": "4bf92f3577b34da6a3ce929d0e0e4736",
                                "span_id": "00f067aa0ba902b8",
                                "parent_span_id": "00f067aa0ba902b7",
                                "name": "POST /checkout",
                                "start_time": "2026-05-08T01:23:45.000Z",
                                "end_time": "2026-05-08T01:23:45.223Z",
                                "duration_ms": 223
                            }
                        }),
                    },
                ],
                product_fixtures: vec![Fixture {
                    name: "product_checkout_completed".to_owned(),
                    body: json!({
                        "event_id": "fixture-product-checkout-completed",
                        "timestamp": "2026-05-08T01:23:45.123Z",
                        "observed_timestamp": "2026-05-08T01:23:45.130Z",
                        "data": {
                            "tenant_id": "fixture",
                            "service": "billing",
                            "event_type": "checkout.completed",
                            "signal": "analytics",
                            "environment": "prod",
                            "user_id": "user_fixture",
                            "account": {
                                "id": "acct_fixture",
                                "plan": "pro"
                            },
                            "revenue": 49
                        }
                    }),
                }],
                agent_fixtures: vec![
                    Fixture {
                        name: "agent_request".to_owned(),
                        body: json!({
                            "event_id": "fixture-agent-request",
                            "timestamp": "2026-05-08T01:23:45.123Z",
                            "observed_timestamp": "2026-05-08T01:23:45.130Z",
                            "data": {
                                "tenant_id": "fixture",
                                "service": "atlas-agent",
                                "event_type": "agent.request",
                                "signal": "trace",
                                "environment": "prod",
                                "trace_id": "4bf92f3577b34da6a3ce929d0e0e4736",
                                "span_id": "00f067aa0ba902b7",
                                "parent_span_id": "",
                                "duration_ms": 120
                            }
                        }),
                    },
                    Fixture {
                        name: "agent_decision".to_owned(),
                        body: json!({
                            "event_id": "fixture-agent-decision",
                            "timestamp": "2026-05-08T01:23:45.123Z",
                            "observed_timestamp": "2026-05-08T01:23:45.130Z",
                            "data": {
                                "tenant_id": "fixture",
                                "service": "atlas-agent",
                                "event_type": "agent.decision",
                                "signal": "trace",
                                "environment": "prod",
                                "trace_id": "4bf92f3577b34da6a3ce929d0e0e4736",
                                "span_id": "00f067aa0ba902b7",
                                "parent_span_id": "",
                                "duration_ms": 120
                            }
                        }),
                    },
                    Fixture {
                        name: "llm_call".to_owned(),
                        body: json!({
                            "event_id": "fixture-llm-call",
                            "timestamp": "2026-05-08T01:23:45.123Z",
                            "observed_timestamp": "2026-05-08T01:23:45.130Z",
                            "data": {
                                "tenant_id": "fixture",
                                "service": "llm-gateway",
                                "event_type": "llm.call",
                                "signal": "trace",
                                "environment": "prod",
                                "trace_id": "4bf92f3577b34da6a3ce929d0e0e4736",
                                "span_id": "00f067aa0ba902b8",
                                "parent_span_id": "00f067aa0ba902b7"
                            }
                        }),
                    },
                    Fixture {
                        name: "tool_call".to_owned(),
                        body: json!({
                            "event_id": "fixture-tool-call",
                            "timestamp": "2026-05-08T01:23:45.123Z",
                            "observed_timestamp": "2026-05-08T01:23:45.130Z",
                            "data": {
                                "tenant_id": "fixture",
                                "service": "tool-runner",
                                "event_type": "tool.call",
                                "signal": "trace",
                                "environment": "prod",
                                "trace_id": "4bf92f3577b34da6a3ce929d0e0e4736",
                                "span_id": "00f067aa0ba902b8",
                                "parent_span_id": "00f067aa0ba902b7"
                            }
                        }),
                    },
                    Fixture {
                        name: "retrieval_step".to_owned(),
                        body: json!({
                            "event_id": "fixture-retrieval-step",
                            "timestamp": "2026-05-08T01:23:45.123Z",
                            "observed_timestamp": "2026-05-08T01:23:45.130Z",
                            "data": {
                                "tenant_id": "fixture",
                                "service": "retrieval",
                                "event_type": "retrieval.step",
                                "signal": "trace",
                                "environment": "prod",
                                "trace_id": "4bf92f3577b34da6a3ce929d0e0e4736",
                                "span_id": "00f067aa0ba902b8",
                                "parent_span_id": "00f067aa0ba902b7"
                            }
                        }),
                    },
                    Fixture {
                        name: "safety_event".to_owned(),
                        body: json!({
                            "event_id": "fixture-safety-event",
                            "timestamp": "2026-05-08T01:23:45.123Z",
                            "observed_timestamp": "2026-05-08T01:23:45.130Z",
                            "data": {
                                "tenant_id": "fixture",
                                "service": "safety",
                                "event_type": "safety.event",
                                "signal": "analytics",
                                "environment": "prod"
                            }
                        }),
                    },
                    Fixture {
                        name: "eval_score".to_owned(),
                        body: json!({
                            "event_id": "fixture-eval-score",
                            "timestamp": "2026-05-08T01:23:45.123Z",
                            "observed_timestamp": "2026-05-08T01:23:45.130Z",
                            "data": {
                                "tenant_id": "fixture",
                                "service": "evals",
                                "event_type": "eval.score",
                                "signal": "analytics",
                                "environment": "prod"
                            }
                        }),
                    },
                ],
                pipeline_fixtures: vec![Fixture {
                    name: "materializer_backfill_slice".to_owned(),
                    body: json!({
                        "event_id": "fixture-materializer-backfill-slice",
                        "timestamp": "2026-05-08T01:23:45.123Z",
                        "observed_timestamp": "2026-05-08T01:23:45.130Z",
                        "data": {
                            "tenant_id": "fixture",
                            "service": "materializer",
                            "event_type": "materializer.backfill_slice",
                            "signal": "pipeline",
                            "environment": "prod",
                            "rows_scanned": 1000,
                            "rows_written": 100
                        }
                    }),
                }],
                rest: vec![Fixture {
                    name: "metric".to_owned(),
                    body: json!({
                        "event_id": "fixture-metric",
                        "timestamp": "2026-05-08T01:23:45.123Z",
                        "observed_timestamp": "2026-05-08T01:23:45.130Z",
                        "data": {
                            "tenant_id": "fixture",
                            "service": "api",
                            "event_type": "metric",
                            "metric_name": "http.server.duration",
                            "metric_value": 231,
                            "http.method": "POST",
                            "http.route": "/checkout",
                            "http.status_code": 200,
                            "duration_ms": 231
                        }
                    }),
                }],
            }),
            client: reqwest::Client::new(),
        }
    }
}
