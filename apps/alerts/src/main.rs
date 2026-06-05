use std::{
    collections::{BTreeMap, HashMap, hash_map::Entry},
    env,
    time::Duration,
};

use anyhow::{Context, Result, anyhow, bail};
use chrono::{DateTime, SecondsFormat, Utc};
use nanotrace_ingest::{
    DEFAULT_NORMALIZED_TOPIC, consumer, count_ndjson_rows, header_value, subscribe,
};
use rdkafka::{
    Message,
    consumer::{CommitMode, Consumer},
};
use regex::Regex;
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use tokio::time::{Instant, interval};
use tracing::{error, info, warn};
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Debug, Clone)]
struct Config {
    brokers: String,
    normalized_topic: String,
    group_id: String,
    client_id: String,
    clickhouse_url: String,
    clickhouse_database: String,
    clickhouse_definitions_table: String,
    clickhouse_alert_events_table: String,
    clickhouse_alert_notifications_table: String,
    clickhouse_user: Option<String>,
    clickhouse_password: Option<String>,
    request_timeout: Duration,
    definition_refresh: Duration,
    max_dedupe_keys: usize,
    notifier_loop: bool,
    notify_poll_interval: Duration,
    notify_batch_size: usize,
}

#[derive(Debug, Deserialize)]
struct ClickHouseJson<T> {
    data: Vec<T>,
}

#[derive(Debug, Deserialize)]
struct DefinitionRow {
    tenant_id: String,
    definition_id: String,
    name: String,
    config: Value,
    version: u64,
}

#[derive(Debug, Clone)]
struct AlertRule {
    tenant_id: String,
    alert_id: String,
    name: String,
    version: u64,
    severity: String,
    message: String,
    dedupe_seconds: i64,
    dedupe_key_path: Option<String>,
    matcher: Matcher,
    text: Option<String>,
    regex: Option<Regex>,
    notifications: Vec<NotificationTarget>,
}

#[derive(Debug, Clone, Default)]
struct Matcher {
    all: Vec<Condition>,
    any: Vec<Condition>,
}

#[derive(Debug, Clone)]
struct Condition {
    path: String,
    op: String,
    value: Option<Value>,
    regex: Option<Regex>,
}

#[derive(Debug, Clone)]
struct NotificationTarget {
    id: String,
    channel: String,
    target: String,
    headers: BTreeMap<String, String>,
    max_attempts: u32,
}

#[derive(Debug, Serialize)]
struct AlertEventRow {
    tenant_id: String,
    alert_id: String,
    alert_version: u64,
    alert_name: String,
    severity: String,
    triggered_at: String,
    event_timestamp: String,
    event_id: String,
    event_type: String,
    dedupe_key: String,
    source_file: String,
    matched: Value,
    data: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AlertNotificationRow {
    tenant_id: String,
    notification_id: String,
    alert_id: String,
    alert_version: u64,
    alert_name: String,
    channel: String,
    target: String,
    headers: Value,
    status: String,
    attempt: u32,
    max_attempts: u32,
    next_attempt_at: String,
    delivered_at: Option<String>,
    updated_at: String,
    last_error: String,
    event_id: String,
    triggered_at: String,
    payload: Value,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .with(tracing_subscriber::fmt::layer())
        .init();

    let cfg = Config::from_env()?;
    let http = reqwest::Client::builder()
        .timeout(cfg.request_timeout)
        .build()
        .context("build HTTP client")?;
    if cfg.notifier_loop {
        run_notifier_loop(&cfg, &http).await?;
        return Ok(());
    }

    let consumer =
        consumer(&cfg.brokers, &cfg.group_id, &cfg.client_id).context("create Kafka consumer")?;
    subscribe(&consumer, &cfg.normalized_topic).context("subscribe to normalized topic")?;
    let mut rules = load_alert_rules(&cfg, &http).await?;
    let mut refresh = interval(cfg.definition_refresh);
    refresh.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut dedupe = HashMap::<String, DateTime<Utc>>::new();

    info!(
        brokers = cfg.brokers,
        normalized_topic = cfg.normalized_topic,
        alert_rules = rules.len(),
        "nanotrace alerts starting"
    );

    loop {
        tokio::select! {
            _ = refresh.tick() => {
                match load_alert_rules(&cfg, &http).await {
                    Ok(next_rules) => {
                        info!(alert_rules = next_rules.len(), "refreshed alert definitions");
                        rules = next_rules;
                    }
                    Err(err) => warn!(error = %err, "failed to refresh alert definitions"),
                }
            }
            message = consumer.recv() => {
                match message {
                    Ok(message) => {
                        if let Err(err) = process_message(&cfg, &http, &message, &rules, &mut dedupe).await {
                            error!(error = %err, "failed to process normalized message for alerts");
                        } else {
                            consumer.commit_message(&message, CommitMode::Sync)
                                .context("commit Kafka offset")?;
                        }
                    }
                    Err(err) => warn!(error = %err, "Kafka receive failed"),
                }
            }
            _ = shutdown_signal() => {
                info!("shutdown signal received");
                return Ok(());
            }
        }
    }
}

async fn process_message(
    cfg: &Config,
    http: &reqwest::Client,
    message: &rdkafka::message::BorrowedMessage<'_>,
    rules: &[AlertRule],
    dedupe: &mut HashMap<String, DateTime<Utc>>,
) -> Result<()> {
    let started = Instant::now();
    let payload = message.payload().unwrap_or_default();
    if payload.is_empty() || rules.is_empty() {
        return Ok(());
    }

    let tenant_hint = header_value(message, nanotrace_ingest::HEADER_TENANT_ID);
    let mut rows = Vec::new();
    let mut notifications = Vec::new();
    for (index, line) in payload.split(|byte| *byte == b'\n').enumerate() {
        if line.iter().all(u8::is_ascii_whitespace) {
            continue;
        }
        let event: Value = serde_json::from_slice(line)
            .with_context(|| format!("parse normalized event at line {}", index + 1))?;
        let tenant_id = event_string(&event, "tenant_id")
            .filter(|value| !value.is_empty())
            .or_else(|| tenant_hint.clone())
            .unwrap_or_default();
        if tenant_id.is_empty() {
            continue;
        }
        for rule in rules.iter().filter(|rule| rule.tenant_id == tenant_id) {
            if let Some(row) = evaluate_alert(rule, &event, dedupe, cfg.max_dedupe_keys)? {
                notifications.extend(notification_rows(rule, &row));
                rows.push(row);
            }
        }
    }

    if !rows.is_empty() {
        let mut body = Vec::new();
        for row in &rows {
            serde_json::to_writer(&mut body, row).context("serialize alert event row")?;
            body.push(b'\n');
        }
        let token = format!(
            "alerts:{}:{}:{}:{}",
            message.topic(),
            message.partition(),
            message.offset(),
            sha256_hex(&body)
        );
        insert_clickhouse(
            cfg,
            http,
            &cfg.clickhouse_alert_events_table,
            &body,
            Some(&token),
        )
        .await?;
    }
    if !notifications.is_empty() {
        let mut body = Vec::new();
        for row in &notifications {
            serde_json::to_writer(&mut body, row).context("serialize alert notification row")?;
            body.push(b'\n');
        }
        let token = format!(
            "alert-notifications:{}:{}:{}:{}",
            message.topic(),
            message.partition(),
            message.offset(),
            sha256_hex(&body)
        );
        insert_clickhouse(
            cfg,
            http,
            &cfg.clickhouse_alert_notifications_table,
            &body,
            Some(&token),
        )
        .await?;
    }

    info!(
        topic = message.topic(),
        partition = message.partition(),
        offset = message.offset(),
        matched_alerts = rows.len(),
        pending_notifications = notifications.len(),
        elapsed_ms = started.elapsed().as_millis(),
        "processed normalized message for alerts"
    );
    Ok(())
}

fn notification_rows(rule: &AlertRule, alert: &AlertEventRow) -> Vec<AlertNotificationRow> {
    let now = Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true);
    rule.notifications
        .iter()
        .map(|target| {
            let notification_id = notification_id(rule, alert, target);
            AlertNotificationRow {
                tenant_id: alert.tenant_id.clone(),
                notification_id,
                alert_id: alert.alert_id.clone(),
                alert_version: alert.alert_version,
                alert_name: alert.alert_name.clone(),
                channel: target.channel.clone(),
                target: target.target.clone(),
                headers: serde_json::to_value(&target.headers).unwrap_or(Value::Null),
                status: "pending".to_string(),
                attempt: 0,
                max_attempts: target.max_attempts,
                next_attempt_at: now.clone(),
                delivered_at: None,
                updated_at: now.clone(),
                last_error: String::new(),
                event_id: alert.event_id.clone(),
                triggered_at: alert.triggered_at.clone(),
                payload: serde_json::json!({
                    "tenant_id": alert.tenant_id,
                    "alert": {
                        "id": alert.alert_id,
                        "version": alert.alert_version,
                        "name": alert.alert_name,
                        "severity": alert.severity,
                        "message": alert.matched.get("message").cloned().unwrap_or(Value::Null)
                    },
                    "event": {
                        "id": alert.event_id,
                        "timestamp": alert.event_timestamp,
                        "type": alert.event_type,
                        "source_file": alert.source_file,
                        "data": alert.data
                    },
                    "matched": alert.matched
                }),
            }
        })
        .collect()
}

fn notification_id(rule: &AlertRule, alert: &AlertEventRow, target: &NotificationTarget) -> String {
    sha256_hex(
        format!(
            "{}\0{}\0{}\0{}\0{}\0{}",
            rule.tenant_id,
            rule.alert_id,
            rule.version,
            target.id,
            alert.dedupe_key,
            alert.event_id
        )
        .as_bytes(),
    )
}

fn evaluate_alert(
    rule: &AlertRule,
    event: &Value,
    dedupe: &mut HashMap<String, DateTime<Utc>>,
    max_dedupe_keys: usize,
) -> Result<Option<AlertEventRow>> {
    if !matcher_matches(&rule.matcher, event)? {
        return Ok(None);
    }
    if let Some(text) = rule.text.as_deref() {
        let haystack = alert_haystack(event).to_ascii_lowercase();
        if !haystack.contains(text) {
            return Ok(None);
        }
    }
    if let Some(regex) = rule.regex.as_ref()
        && !regex.is_match(&alert_haystack(event))
    {
        return Ok(None);
    }

    let now = Utc::now();
    let event_timestamp = event_string(event, "timestamp")
        .unwrap_or_else(|| now.to_rfc3339_opts(SecondsFormat::Millis, true));
    let event_id = event_string(event, "event_id").unwrap_or_default();
    let dedupe_key = rule
        .dedupe_key_path
        .as_deref()
        .and_then(|path| value_at_path(event, path))
        .and_then(value_to_string)
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| {
            if event_id.is_empty() {
                "event".to_string()
            } else {
                event_id.clone()
            }
        });
    let dedupe_bucket = if rule.dedupe_seconds <= 0 {
        now.timestamp_millis()
    } else {
        now.timestamp() / rule.dedupe_seconds
    };
    let dedupe_token = format!(
        "{}:{}:{}:{}",
        rule.tenant_id, rule.alert_id, dedupe_key, dedupe_bucket
    );
    if let Entry::Occupied(existing) = dedupe.entry(dedupe_token.clone())
        && rule.dedupe_seconds > 0
    {
        let age = now.signed_duration_since(*existing.get()).num_seconds();
        if age < rule.dedupe_seconds {
            return Ok(None);
        }
    }
    dedupe.insert(dedupe_token.clone(), now);
    if dedupe.len() > max_dedupe_keys {
        prune_dedupe(dedupe, now, rule.dedupe_seconds.max(60));
    }

    Ok(Some(AlertEventRow {
        tenant_id: rule.tenant_id.clone(),
        alert_id: rule.alert_id.clone(),
        alert_version: rule.version,
        alert_name: rule.name.clone(),
        severity: rule.severity.clone(),
        triggered_at: now.to_rfc3339_opts(SecondsFormat::Millis, true),
        event_timestamp,
        event_id,
        event_type: event_string(event, "event_type").unwrap_or_default(),
        dedupe_key: dedupe_token,
        source_file: event_string(event, "source_file").unwrap_or_default(),
        matched: serde_json::json!({
            "message": rule.message,
            "dedupe_key": dedupe_key
        }),
        data: event.get("data").cloned().unwrap_or(Value::Null),
    }))
}

fn matcher_matches(matcher: &Matcher, event: &Value) -> Result<bool> {
    for condition in &matcher.all {
        if !condition_matches(condition, event)? {
            return Ok(false);
        }
    }
    if matcher.any.is_empty() {
        return Ok(true);
    }
    for condition in &matcher.any {
        if condition_matches(condition, event)? {
            return Ok(true);
        }
    }
    if !matcher.any.is_empty() {
        return Ok(false);
    }
    Ok(true)
}

fn condition_matches(condition: &Condition, event: &Value) -> Result<bool> {
    let value = value_at_path(event, &condition.path);
    Ok(match condition.op.as_str() {
        "exists" => value.is_some_and(|value| !value.is_null()),
        "not_exists" => value.is_none_or(Value::is_null),
        "eq" => values_equal(value, condition.value.as_ref()),
        "ne" | "neq" => !values_equal(value, condition.value.as_ref()),
        "contains" => value
            .and_then(value_to_string)
            .zip(condition.value.as_ref().and_then(value_to_string))
            .is_some_and(|(left, right)| left.contains(&right)),
        "gt" | "gte" | "lt" | "lte" => {
            numeric_condition(condition.op.as_str(), value, condition.value.as_ref())
        }
        "regex" => {
            let Some(regex) = condition.regex.as_ref() else {
                return Ok(false);
            };
            value
                .and_then(value_to_string)
                .is_some_and(|value| regex.is_match(&value))
        }
        "is_number" => value.is_some_and(|value| value.as_f64().is_some()),
        "is_error" => {
            event_boolish(event, "is_error")
                || event_string(event, "event_type")
                    .is_some_and(|event_type| event_type.to_ascii_lowercase().ends_with("_error"))
        }
        _ => {
            return Err(anyhow!(
                "unsupported alert condition operator: {}",
                condition.op
            ));
        }
    })
}

fn numeric_condition(op: &str, left: Option<&Value>, right: Option<&Value>) -> bool {
    let Some(left) = left.and_then(value_to_f64) else {
        return false;
    };
    let Some(right) = right.and_then(value_to_f64) else {
        return false;
    };
    match op {
        "gt" => left > right,
        "gte" => left >= right,
        "lt" => left < right,
        "lte" => left <= right,
        _ => false,
    }
}

async fn load_alert_rules(cfg: &Config, http: &reqwest::Client) -> Result<Vec<AlertRule>> {
    let query = format!(
        "SELECT tenant_id, definition_id, name, config, version FROM {} FINAL WHERE kind = 'alert' AND enabled = 1 AND isNull(deleted_at)",
        qualified_table(&cfg.clickhouse_database, &cfg.clickhouse_definitions_table)
    );
    let body = clickhouse_select(cfg, http, &query).await?;
    let response: ClickHouseJson<DefinitionRow> =
        serde_json::from_str(&body).context("parse alert definitions response")?;
    response.data.into_iter().map(AlertRule::try_from).collect()
}

async fn run_notifier_loop(cfg: &Config, http: &reqwest::Client) -> Result<()> {
    let mut poll = interval(cfg.notify_poll_interval);
    poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    info!(
        batch_size = cfg.notify_batch_size,
        poll_secs = cfg.notify_poll_interval.as_secs(),
        "nanotrace alert notifier starting"
    );
    loop {
        tokio::select! {
            _ = poll.tick() => {
                match deliver_pending_notifications(cfg, http).await {
                    Ok(count) => {
                        if count > 0 {
                            info!(notifications = count, "processed alert notifications");
                        }
                    }
                    Err(err) => warn!(error = %err, "failed to deliver alert notifications"),
                }
            }
            _ = shutdown_signal() => {
                info!("shutdown signal received");
                return Ok(());
            }
        }
    }
}

async fn deliver_pending_notifications(cfg: &Config, http: &reqwest::Client) -> Result<usize> {
    let table = qualified_table(
        &cfg.clickhouse_database,
        &cfg.clickhouse_alert_notifications_table,
    );
    let query = format!(
        "SELECT tenant_id, notification_id, alert_id, alert_version, alert_name, channel, target, headers, status, attempt, max_attempts, next_attempt_at, delivered_at, updated_at, last_error, event_id, triggered_at, payload FROM {table} FINAL WHERE status IN ('pending', 'retry') AND attempt < max_attempts AND next_attempt_at <= now64(3) ORDER BY next_attempt_at ASC LIMIT {}",
        cfg.notify_batch_size
    );
    let body = clickhouse_select(cfg, http, &query).await?;
    let response: ClickHouseJson<AlertNotificationRow> =
        serde_json::from_str(&body).context("parse alert notification response")?;
    if response.data.is_empty() {
        return Ok(0);
    }

    let mut updates = Vec::with_capacity(response.data.len());
    for notification in response.data {
        updates.push(deliver_notification(http, notification).await);
    }
    let mut body = Vec::new();
    for row in &updates {
        serde_json::to_writer(&mut body, row).context("serialize alert notification update")?;
        body.push(b'\n');
    }
    insert_clickhouse(
        cfg,
        http,
        &cfg.clickhouse_alert_notifications_table,
        &body,
        None,
    )
    .await?;
    Ok(updates.len())
}

async fn deliver_notification(
    http: &reqwest::Client,
    notification: AlertNotificationRow,
) -> AlertNotificationRow {
    let mut next = notification.clone();
    next.attempt = notification.attempt.saturating_add(1);
    next.updated_at = Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true);
    next.delivered_at = None;

    let result = match notification.channel.as_str() {
        "webhook" => post_webhook(http, &notification).await,
        other => Err(anyhow!("unsupported alert notification channel: {other}")),
    };
    match result {
        Ok(()) => {
            next.status = "delivered".to_string();
            next.delivered_at = Some(next.updated_at.clone());
            next.last_error.clear();
        }
        Err(err) => {
            next.last_error = err.to_string();
            if next.attempt >= next.max_attempts {
                next.status = "failed".to_string();
                next.next_attempt_at = next.updated_at.clone();
            } else {
                next.status = "retry".to_string();
                next.next_attempt_at = retry_at(next.attempt);
            }
        }
    }
    next
}

async fn post_webhook(http: &reqwest::Client, notification: &AlertNotificationRow) -> Result<()> {
    let mut request = http.post(&notification.target).json(&notification.payload);
    if let Some(headers) = notification.headers.as_object() {
        for (name, value) in headers {
            if let Some(value) = value.as_str() {
                request = request.header(name, value);
            }
        }
    }
    let response = request.send().await.context("send alert webhook")?;
    let status = response.status();
    let text = response.text().await.unwrap_or_default();
    if !status.is_success() {
        bail!("alert webhook failed: {status} {text}");
    }
    Ok(())
}

fn retry_at(attempt: u32) -> String {
    let shift = attempt.min(6);
    let delay = 30_i64.saturating_mul(1_i64 << shift);
    (Utc::now() + chrono::Duration::seconds(delay)).to_rfc3339_opts(SecondsFormat::Millis, true)
}

impl TryFrom<DefinitionRow> for AlertRule {
    type Error = anyhow::Error;

    fn try_from(row: DefinitionRow) -> Result<Self> {
        let matcher = parse_matcher(row.config.get("match"))?;
        let severity = row
            .config
            .get("severity")
            .and_then(Value::as_str)
            .unwrap_or("warning")
            .to_string();
        if !matches!(severity.as_str(), "info" | "warning" | "critical") {
            bail!("invalid alert severity for {}", row.definition_id);
        }
        let regex = row
            .config
            .get("regex")
            .and_then(Value::as_str)
            .filter(|value| !value.trim().is_empty())
            .map(Regex::new)
            .transpose()
            .with_context(|| format!("compile alert regex {}", row.definition_id))?;
        let notifications = parse_notifications(&row.config)
            .with_context(|| format!("parse alert notifications {}", row.definition_id))?;
        Ok(Self {
            tenant_id: row.tenant_id,
            alert_id: row.definition_id,
            name: row.name.clone(),
            version: row.version,
            severity,
            message: row
                .config
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or(row.name.as_str())
                .to_string(),
            dedupe_seconds: row
                .config
                .get("dedupe_seconds")
                .and_then(Value::as_i64)
                .unwrap_or(60)
                .clamp(0, 86_400),
            dedupe_key_path: row
                .config
                .get("dedupe_key")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_string),
            matcher,
            text: row
                .config
                .get("text")
                .and_then(Value::as_str)
                .map(|value| value.to_ascii_lowercase()),
            regex,
            notifications,
        })
    }
}

fn parse_notifications(config: &Value) -> Result<Vec<NotificationTarget>> {
    let mut notifications = Vec::new();
    if let Some(url) = config
        .get("webhook_url")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        notifications.push(NotificationTarget {
            id: "webhook".to_string(),
            channel: "webhook".to_string(),
            target: url.to_string(),
            headers: BTreeMap::new(),
            max_attempts: 5,
        });
    }
    let Some(webhooks) = config
        .get("notifications")
        .and_then(|value| value.get("webhooks"))
    else {
        return Ok(notifications);
    };
    let webhooks = webhooks
        .as_array()
        .ok_or_else(|| anyhow!("alert notification webhooks must be an array"))?;
    for (index, webhook) in webhooks.iter().enumerate() {
        let object = webhook
            .as_object()
            .ok_or_else(|| anyhow!("alert notification webhook must be an object"))?;
        let target = object
            .get("url")
            .or_else(|| object.get("target"))
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| anyhow!("alert notification webhook url is required"))?;
        let id = object
            .get("id")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string)
            .unwrap_or_else(|| format!("webhook_{index}"));
        let headers = parse_webhook_headers(object.get("headers"))?;
        let max_attempts = object
            .get("max_attempts")
            .and_then(Value::as_u64)
            .and_then(|value| u32::try_from(value).ok())
            .unwrap_or(5)
            .clamp(1, 20);
        notifications.push(NotificationTarget {
            id,
            channel: "webhook".to_string(),
            target: target.to_string(),
            headers,
            max_attempts,
        });
    }
    Ok(notifications)
}

fn parse_webhook_headers(value: Option<&Value>) -> Result<BTreeMap<String, String>> {
    let Some(value) = value else {
        return Ok(BTreeMap::new());
    };
    let object = value
        .as_object()
        .ok_or_else(|| anyhow!("alert notification headers must be an object"))?;
    let mut headers = BTreeMap::new();
    for (name, value) in object {
        let Some(value) = value.as_str() else {
            bail!("alert notification header values must be strings");
        };
        headers.insert(name.clone(), value.to_string());
    }
    Ok(headers)
}

fn parse_matcher(value: Option<&Value>) -> Result<Matcher> {
    let Some(value) = value else {
        return Ok(Matcher::default());
    };
    let object = value
        .as_object()
        .ok_or_else(|| anyhow!("alert match must be an object"))?;
    Ok(Matcher {
        all: parse_conditions(object.get("all"))?,
        any: parse_conditions(object.get("any"))?,
    })
}

fn parse_conditions(value: Option<&Value>) -> Result<Vec<Condition>> {
    let Some(value) = value else {
        return Ok(Vec::new());
    };
    let values = value
        .as_array()
        .ok_or_else(|| anyhow!("alert match clause must be an array"))?;
    values.iter().map(parse_condition).collect()
}

fn parse_condition(value: &Value) -> Result<Condition> {
    let object = value
        .as_object()
        .ok_or_else(|| anyhow!("alert condition must be an object"))?;
    let path = object
        .get("path")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow!("alert condition path is required"))?
        .to_string();
    let op = object
        .get("op")
        .or_else(|| object.get("operator"))
        .and_then(Value::as_str)
        .unwrap_or("eq")
        .to_string();
    let pattern = object
        .get("regex")
        .or_else(|| object.get("pattern"))
        .and_then(Value::as_str);
    let regex = if op == "regex" {
        Some(
            Regex::new(pattern.ok_or_else(|| anyhow!("regex alert condition requires pattern"))?)
                .context("compile alert condition regex")?,
        )
    } else {
        None
    };
    Ok(Condition {
        path,
        op,
        value: object.get("value").cloned(),
        regex,
    })
}

fn value_at_path<'a>(event: &'a Value, path: &str) -> Option<&'a Value> {
    if path == "data" {
        return event.get("data");
    }
    if let Some(rest) = path.strip_prefix("data.") {
        return dotted_value(event.get("data")?, rest);
    }
    match path {
        "tenant_id" | "event_id" | "timestamp" | "observed_timestamp" | "ingested_timestamp"
        | "source_file" | "event_type" | "signal" | "trace_id" | "span_id" => event
            .get(path)
            .or_else(|| event.get("data").and_then(|data| dotted_value(data, path))),
        _ => event.get("data").and_then(|data| dotted_value(data, path)),
    }
}

fn dotted_value<'a>(value: &'a Value, path: &str) -> Option<&'a Value> {
    let mut current = value;
    for segment in path.split('.') {
        let object = current.as_object()?;
        current = object.get(segment)?;
    }
    Some(current)
}

fn event_string(event: &Value, path: &str) -> Option<String> {
    value_at_path(event, path).and_then(value_to_string)
}

fn event_boolish(event: &Value, path: &str) -> bool {
    value_at_path(event, path).is_some_and(|value| match value {
        Value::Bool(value) => *value,
        Value::Number(value) => value.as_u64().is_some_and(|value| value != 0),
        Value::String(value) => matches!(
            value.to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "error"
        ),
        _ => false,
    })
}

fn value_to_string(value: &Value) -> Option<String> {
    match value {
        Value::String(value) => Some(value.clone()),
        Value::Number(value) => Some(value.to_string()),
        Value::Bool(value) => Some(value.to_string()),
        Value::Null => None,
        other => Some(other.to_string()),
    }
}

fn value_to_f64(value: &Value) -> Option<f64> {
    match value {
        Value::Number(value) => value.as_f64(),
        Value::String(value) => value.parse::<f64>().ok(),
        _ => None,
    }
}

fn values_equal(left: Option<&Value>, right: Option<&Value>) -> bool {
    match (left, right) {
        (Some(left), Some(right)) if left == right => true,
        (Some(left), Some(right)) => value_to_string(left) == value_to_string(right),
        _ => false,
    }
}

fn alert_haystack(event: &Value) -> String {
    serde_json::to_string(event).unwrap_or_default()
}

fn prune_dedupe(
    dedupe: &mut HashMap<String, DateTime<Utc>>,
    now: DateTime<Utc>,
    older_than_seconds: i64,
) {
    dedupe.retain(|_, timestamp| {
        now.signed_duration_since(*timestamp).num_seconds() <= older_than_seconds
    });
}

async fn clickhouse_select(cfg: &Config, http: &reqwest::Client, query: &str) -> Result<String> {
    let mut request = http.post(&cfg.clickhouse_url).query(&[
        ("database", cfg.clickhouse_database.as_str()),
        ("query", query),
        ("default_format", "JSON"),
        ("date_time_output_format", "iso"),
        ("readonly", "1"),
    ]);
    if let Some(user) = cfg.clickhouse_user.as_deref() {
        request = request.basic_auth(user, cfg.clickhouse_password.as_deref());
    }
    let response = request.send().await.context("send ClickHouse query")?;
    let status = response.status();
    let text = response.text().await.context("read ClickHouse response")?;
    if status != StatusCode::OK {
        bail!("ClickHouse query failed: {status} {text}");
    }
    Ok(text)
}

async fn insert_clickhouse(
    cfg: &Config,
    http: &reqwest::Client,
    table: &str,
    body: &[u8],
    dedupe_token: Option<&str>,
) -> Result<()> {
    if body.is_empty() || count_ndjson_rows(body) == 0 {
        return Ok(());
    }
    let full_table = qualified_table(&cfg.clickhouse_database, table);
    let query = format!("INSERT INTO {full_table} FORMAT JSONEachRow");
    let mut request = http
        .post(&cfg.clickhouse_url)
        .query(&[
            ("database", cfg.clickhouse_database.as_str()),
            ("query", query.as_str()),
            ("date_time_input_format", "best_effort"),
            ("type_json_skip_duplicated_paths", "1"),
            ("insert_deduplicate", "1"),
        ])
        .body(body.to_vec());
    if let Some(dedupe_token) = dedupe_token {
        request = request.query(&[("insert_deduplication_token", dedupe_token)]);
    }
    if let Some(user) = cfg.clickhouse_user.as_deref() {
        request = request.basic_auth(user, cfg.clickhouse_password.as_deref());
    }
    let response = request.send().await.context("send ClickHouse insert")?;
    let status = response.status();
    let text = response.text().await.context("read ClickHouse response")?;
    if status != StatusCode::OK {
        bail!("ClickHouse insert into {full_table} failed: {status} {text}");
    }
    Ok(())
}

impl Config {
    fn from_env() -> Result<Self> {
        Ok(Self {
            brokers: required("NANOTRACE_KAFKA_BROKERS")?,
            normalized_topic: env_or("NANOTRACE_KAFKA_NORMALIZED_TOPIC", DEFAULT_NORMALIZED_TOPIC),
            group_id: env_or("NANOTRACE_ALERTS_GROUP_ID", "nanotrace-alerts"),
            client_id: env_or("NANOTRACE_ALERTS_CLIENT_ID", "nanotrace-alerts"),
            clickhouse_url: required("CLICKHOUSE_URL")?,
            clickhouse_database: env_or("CLICKHOUSE_DATABASE", "observatory"),
            clickhouse_definitions_table: env_or("CLICKHOUSE_DEFINITIONS_TABLE", "definitions"),
            clickhouse_alert_events_table: env_or("CLICKHOUSE_ALERT_EVENTS_TABLE", "alert_events"),
            clickhouse_alert_notifications_table: env_or(
                "CLICKHOUSE_ALERT_NOTIFICATIONS_TABLE",
                "alert_notifications",
            ),
            clickhouse_user: optional("CLICKHOUSE_USER")
                .or_else(|| optional("CLICKHOUSE_USERNAME")),
            clickhouse_password: optional("CLICKHOUSE_PASSWORD"),
            request_timeout: Duration::from_secs(parse_env(
                "NANOTRACE_ALERTS_REQUEST_TIMEOUT_SECS",
                30_u64,
            )?),
            definition_refresh: Duration::from_secs(parse_env(
                "NANOTRACE_ALERTS_DEFINITION_REFRESH_SECS",
                10_u64,
            )?),
            max_dedupe_keys: parse_env("NANOTRACE_ALERTS_MAX_DEDUPE_KEYS", 100_000_usize)?,
            notifier_loop: env_bool("NANOTRACE_ALERTS_NOTIFIER"),
            notify_poll_interval: Duration::from_secs(parse_env(
                "NANOTRACE_ALERTS_NOTIFY_POLL_SECS",
                5_u64,
            )?),
            notify_batch_size: parse_env("NANOTRACE_ALERTS_NOTIFY_BATCH_SIZE", 100_usize)?,
        })
    }
}

fn qualified_table(database: &str, table: &str) -> String {
    if table.contains('.') {
        table.to_string()
    } else {
        format!("{database}.{table}")
    }
}

fn required(key: &str) -> Result<String> {
    optional(key).ok_or_else(|| anyhow!("{key} is required"))
}

fn optional(key: &str) -> Option<String> {
    env::var(key)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn env_or(key: &str, fallback: &str) -> String {
    optional(key).unwrap_or_else(|| fallback.to_string())
}

fn env_bool(key: &str) -> bool {
    optional(key)
        .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
        .unwrap_or(false)
}

fn parse_env<T>(key: &str, fallback: T) -> Result<T>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    optional(key)
        .map(|value| value.parse().map_err(|err| anyhow!("invalid {key}: {err}")))
        .unwrap_or(Ok(fallback))
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("{:x}", hasher.finalize())
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn alert_rule_matches_path_numeric_and_regex_conditions() {
        let row = serde_json::json!({
            "tenant_id": "org_1",
            "event_id": "evt_1",
            "timestamp": "2026-06-05T01:00:00Z",
            "source_file": "kafka://events.normalized.v1/0/1",
            "data": {
                "event_type": "llm.call",
                "duration_ms": 1200,
                "llm": { "model": "gpt-5.5" }
            }
        });
        let definition = DefinitionRow {
            tenant_id: "org_1".to_string(),
            definition_id: "slow_llm".to_string(),
            name: "slow_llm".to_string(),
            version: 3,
            config: serde_json::json!({
                "severity": "critical",
                "dedupe_key": "llm.model",
                "notifications": {
                    "webhooks": [
                        {
                            "id": "pager",
                            "url": "https://alerts.example.com/nanotrace",
                            "headers": { "x-alert-source": "nanotrace" },
                            "max_attempts": 3
                        }
                    ]
                },
                "match": {
                    "all": [
                        { "path": "event_type", "op": "eq", "value": "llm.call" },
                        { "path": "duration_ms", "op": "gt", "value": 1000 },
                        { "path": "llm.model", "op": "regex", "pattern": "^gpt-" }
                    ]
                }
            }),
        };
        let rule = AlertRule::try_from(definition).expect("alert rule");
        let mut dedupe = HashMap::new();

        let first = evaluate_alert(&rule, &row, &mut dedupe, 100).expect("evaluate");
        assert!(first.is_some());
        let notifications = notification_rows(&rule, first.as_ref().expect("alert event"));
        assert_eq!(notifications.len(), 1);
        assert_eq!(notifications[0].channel, "webhook");
        assert_eq!(
            notifications[0].target,
            "https://alerts.example.com/nanotrace"
        );
        assert_eq!(notifications[0].headers["x-alert-source"], "nanotrace");
        assert_eq!(notifications[0].max_attempts, 3);
        let second = evaluate_alert(&rule, &row, &mut dedupe, 100).expect("evaluate");
        assert!(second.is_none());
    }

    #[test]
    fn alert_rule_rejects_non_matching_any_clause() {
        let row = serde_json::json!({
            "tenant_id": "org_1",
            "event_id": "evt_1",
            "timestamp": "2026-06-05T01:00:00Z",
            "data": { "event_type": "track", "plan": "free" }
        });
        let matcher = parse_matcher(Some(&serde_json::json!({
            "all": [{ "path": "event_type", "op": "eq", "value": "track" }],
            "any": [
                { "path": "plan", "op": "eq", "value": "enterprise" },
                { "path": "plan", "op": "eq", "value": "pro" }
            ]
        })))
        .expect("matcher");

        assert!(!matcher_matches(&matcher, &row).expect("match"));
    }

    #[tokio::test]
    async fn unsupported_notification_channel_becomes_failed_status() {
        let notification = AlertNotificationRow {
            tenant_id: "org_1".to_string(),
            notification_id: "note_1".to_string(),
            alert_id: "alert_1".to_string(),
            alert_version: 1,
            alert_name: "alert".to_string(),
            channel: "sms".to_string(),
            target: "unused".to_string(),
            headers: serde_json::json!({}),
            status: "pending".to_string(),
            attempt: 0,
            max_attempts: 1,
            next_attempt_at: "2026-06-05T01:00:00.000Z".to_string(),
            delivered_at: None,
            updated_at: "2026-06-05T01:00:00.000Z".to_string(),
            last_error: String::new(),
            event_id: "evt_1".to_string(),
            triggered_at: "2026-06-05T01:00:00.000Z".to_string(),
            payload: serde_json::json!({ "ok": true }),
        };
        let http = reqwest::Client::new();

        let updated = deliver_notification(&http, notification).await;

        assert_eq!(updated.status, "failed");
        assert_eq!(updated.attempt, 1);
        assert!(updated.delivered_at.is_none());
        assert!(
            updated
                .last_error
                .contains("unsupported alert notification channel")
        );
    }
}
