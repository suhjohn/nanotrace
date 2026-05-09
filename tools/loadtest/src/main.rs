use anyhow::{Context, Result, anyhow, bail};
use chrono::Utc;
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
    let secret_key = required_env("SECRET_KEY")?;
    let fixtures = Arc::new(load_fixtures(&root)?);
    let clickhouse = clickhouse_config(outputs.as_ref())?;

    let config = Arc::new(Config {
        ingest_url,
        secret_key,
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
    println!(
        "successCriteria=errorRate<={},p95Ms<={}",
        context.config.max_error_rate, context.config.max_p95_ms
    );

    let mut summary = Vec::new();
    for &batch_size in &context.config.batch_sizes {
        println!("\n=== batchSize={batch_size} events/request ===");
        summary.push(find_max_rps(Arc::clone(&context), batch_size).await?);
    }

    println!("\n=== summary ===");
    for result in summary {
        println!(
            "batchSize={} maxReqPerSec={} maxEventPerSec={} p50Ms={} p95Ms={} p99Ms={} errorRate={}",
            result.batch_size,
            result.max_req_per_sec,
            result.max_req_per_sec * result.batch_size,
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
    secret_key: String,
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
    clickhouse_wait_ms: f64,
    clickhouse_poll_ms: f64,
    event_seq: AtomicU64,
    accepted_events: AtomicU64,
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
    error_rate: f64,
    achieved_req_per_sec: f64,
    achieved_event_per_sec: f64,
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
    let mut next_at = started_at;

    while Instant::now() < deadline {
        drain_results(
            &mut rx,
            &mut in_flight,
            &mut failed,
            &mut latencies,
            &mut statuses,
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
        error_rate: if total == 0 {
            1.0
        } else {
            round(failed as f64 / total as f64, 4)
        },
        achieved_req_per_sec: round(ok as f64 / elapsed_seconds, 2),
        achieved_event_per_sec: round((ok * batch_size) as f64 / elapsed_seconds, 2),
        p50_ms: percentile(&latencies, 0.50),
        p95_ms: percentile(&latencies, 0.95),
        p99_ms: percentile(&latencies, 0.99),
        max_observed_in_flight,
        statuses,
    })
}

fn drain_results(
    rx: &mut mpsc::UnboundedReceiver<RequestResult>,
    in_flight: &mut u64,
    failed: &mut u64,
    latencies: &mut Vec<f64>,
    statuses: &mut HashMap<String, u64>,
) {
    while let Ok(result) = rx.try_recv() {
        record_result(result, in_flight, failed, latencies, statuses);
    }
}

fn record_result(
    result: RequestResult,
    in_flight: &mut u64,
    failed: &mut u64,
    latencies: &mut Vec<f64>,
    statuses: &mut HashMap<String, u64>,
) {
    *in_flight = in_flight.saturating_sub(1);
    latencies.push(result.latency_ms);
    *statuses.entry(result.status).or_insert(0) += 1;
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

        let response = context
            .client
            .post(format!("{}/events", context.config.ingest_url))
            .header(
                AUTHORIZATION,
                format!("Bearer {}", context.config.secret_key),
            )
            .header(CONTENT_TYPE, "application/json")
            .body(body)
            .send()
            .await?;
        let status = response.status().as_u16();
        let _ = response.bytes().await;
        Ok::<_, anyhow::Error>(status)
    }
    .await;

    match result {
        Ok(status) => RequestResult {
            status: status.to_string(),
            ok: (200..300).contains(&status),
            latency_ms: started_at.elapsed().as_secs_f64() * 1_000.0,
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
        },
    }
}

fn make_event(context: &LoadContext, batch_mix: &str) -> Result<Value> {
    let seq = context.config.event_seq.fetch_add(1, Ordering::Relaxed);
    let fixture = choose_fixture(&context.fixtures, seq, context.config.log_ratio);
    let now = Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true);

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

    event.insert(
        "event_id".to_owned(),
        Value::String(format!("{}-{seq}", context.config.run_id)),
    );
    event.insert("timestamp".to_owned(), Value::String(now.clone()));
    event.insert("observed_timestamp".to_owned(), Value::String(now));
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

fn choose_fixture(fixtures: &Fixtures, seq: u64, log_ratio: f64) -> &Fixture {
    if (seq % 100) < (log_ratio * 100.0).round() as u64 {
        &fixtures.log
    } else {
        let index = seq as usize % fixtures.rest.len();
        &fixtures.rest[index]
    }
}

fn is_pass(config: &Config, stats: &StepStats) -> bool {
    stats.error_rate <= config.max_error_rate && stats.p95_ms <= config.max_p95_ms
}

fn print_step(stats: &StepStats) {
    println!(
        "targetRps={} batchSize={} attempted={} ok={} failed={} acceptedEvents={} achievedRps={} eventsPerSec={} p50={}ms p95={}ms p99={}ms errorRate={} inFlightMax={} statuses={}",
        stats.target_rps,
        stats.batch_size,
        stats.attempted,
        stats.ok,
        stats.failed,
        stats.accepted_events,
        stats.achieved_req_per_sec,
        stats.achieved_event_per_sec,
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
    let names = [
        "log",
        "metric",
        "metric_counter",
        "metric_gauge",
        "metric_histogram",
        "metric_runtime",
        "span_end",
        "span_start",
    ];
    let mut loaded = Vec::new();
    for name in names {
        let path = events_dir.join(format!("{name}.json"));
        let body = serde_json::from_slice::<Value>(
            &fs::read(&path).with_context(|| format!("read {}", path.display()))?,
        )
        .with_context(|| format!("parse {}", path.display()))?;
        validate_fixture(name, &body)?;
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
        .into_iter()
        .filter(|fixture| fixture.name != "log")
        .collect::<Vec<_>>();
    if rest.is_empty() {
        bail!("expected at least one non-log fixture");
    }
    Ok(Fixtures { log, rest })
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
    let event_prefix = format!("{}-", context.config.run_id);
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
WHERE tenant_id = 'loadtest'
  AND startsWith(event_id, '{}')
FORMAT JSON",
        quote_identifier(&clickhouse.database),
        quote_identifier(&clickhouse.table),
        escape_sql_string(&event_prefix)
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
    let valid = value
        .chars()
        .enumerate()
        .all(|(index, character)| match (index, character) {
            (0, 'A'..='Z' | 'a'..='z' | '_') => true,
            (_, 'A'..='Z' | 'a'..='z' | '0'..='9' | '_') => true,
            _ => false,
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
    match env::var(key) {
        Ok(value) => {
            let parsed = value
                .parse::<f64>()
                .with_context(|| format!("{key} must be a number"))?;
            if parsed <= 0.0 {
                bail!("{key} must be positive");
            }
            Ok(parsed)
        }
        Err(_) => Ok(fallback),
    }
}

fn integer_env(key: &str, fallback: u64) -> Result<u64> {
    match env::var(key) {
        Ok(value) => {
            let parsed = value
                .parse::<u64>()
                .with_context(|| format!("{key} must be an integer"))?;
            if parsed == 0 {
                bail!("{key} must be positive");
            }
            Ok(parsed)
        }
        Err(_) => Ok(fallback),
    }
}

fn list_env(key: &str, fallback: &[u64]) -> Result<Vec<u64>> {
    match env::var(key) {
        Ok(value) => value
            .split(',')
            .map(str::trim)
            .map(|item| {
                let parsed = item
                    .parse::<u64>()
                    .with_context(|| format!("{key} must contain integers"))?;
                if parsed == 0 {
                    bail!("{key} values must be positive");
                }
                Ok(parsed)
            })
            .collect(),
        Err(_) => Ok(fallback.to_vec()),
    }
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
    fn generated_event_preserves_fixture_shape_without_double_wrapping() {
        let context = test_context();

        let event = make_event(&context, "test_mix").expect("event");
        let object = event.as_object().expect("event object");

        assert!(
            object["event_id"]
                .as_str()
                .unwrap()
                .starts_with("test-run-")
        );
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
    fn fixture_selection_uses_log_ratio_then_rest() {
        let context = test_context();

        assert_eq!(choose_fixture(&context.fixtures, 0, 0.10).name, "log");
        assert_eq!(choose_fixture(&context.fixtures, 9, 0.10).name, "log");
        assert_eq!(choose_fixture(&context.fixtures, 10, 0.10).name, "metric");
    }

    fn test_context() -> LoadContext {
        LoadContext {
            config: Arc::new(Config {
                ingest_url: "http://127.0.0.1:3000".to_owned(),
                secret_key: "secret".to_owned(),
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
                            "message": "hello"
                        }
                    }),
                },
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
                            "metric_name": "requests"
                        }
                    }),
                }],
            }),
            client: reqwest::Client::new(),
        }
    }
}
