use std::{
    collections::BTreeMap,
    sync::{
        Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use axum::http::StatusCode;
use nanotrace_auth::AuthStore;

#[derive(Debug)]
pub struct ServerMetrics {
    started_at: Instant,
    http: Mutex<BTreeMap<HttpMetricKey, DurationStats>>,
    account_failures: Mutex<BTreeMap<AccountFailureMetricKey, u64>>,
    queries: Mutex<BTreeMap<QueryMetricKey, DurationStats>>,
    ingest_batches_accepted: AtomicU64,
    ingest_bytes_accepted: AtomicU64,
    backfills_created: AtomicU64,
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
struct HttpMetricKey {
    method: String,
    route: String,
    status: u16,
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
struct AccountFailureMetricKey {
    area: &'static str,
    status_class: u16,
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
struct QueryMetricKey {
    query_type: &'static str,
    result: &'static str,
}

#[derive(Debug, Default)]
struct DurationStats {
    count: u64,
    sum: Duration,
}

impl ServerMetrics {
    pub fn new() -> Self {
        Self {
            started_at: Instant::now(),
            http: Mutex::new(BTreeMap::new()),
            account_failures: Mutex::new(BTreeMap::new()),
            queries: Mutex::new(BTreeMap::new()),
            ingest_batches_accepted: AtomicU64::new(0),
            ingest_bytes_accepted: AtomicU64::new(0),
            backfills_created: AtomicU64::new(0),
        }
    }

    pub fn record_http(&self, method: &str, route: &str, status: StatusCode, elapsed: Duration) {
        let key = HttpMetricKey {
            method: method.to_string(),
            route: route.to_string(),
            status: status.as_u16(),
        };
        if let Ok(mut metrics) = self.http.lock() {
            metrics.entry(key).or_default().record(elapsed);
        }
        if status.is_client_error() || status.is_server_error() {
            if let Some(area) = account_route_area(route) {
                let key = AccountFailureMetricKey {
                    area,
                    status_class: status.as_u16() / 100,
                };
                if let Ok(mut metrics) = self.account_failures.lock() {
                    *metrics.entry(key).or_default() += 1;
                }
            }
        }
    }

    pub fn record_ingest_accepted(&self, bytes: usize) {
        self.ingest_batches_accepted.fetch_add(1, Ordering::Relaxed);
        self.ingest_bytes_accepted
            .fetch_add(bytes as u64, Ordering::Relaxed);
    }

    pub fn record_query(&self, query_type: &'static str, result: &'static str, elapsed: Duration) {
        let key = QueryMetricKey { query_type, result };
        if let Ok(mut metrics) = self.queries.lock() {
            metrics.entry(key).or_default().record(elapsed);
        }
    }

    pub fn record_backfill_created(&self) {
        self.backfills_created.fetch_add(1, Ordering::Relaxed);
    }

    pub fn render_prometheus(&self, auth: Option<&AuthStore>) -> String {
        let mut output = String::new();
        push_metric_help(
            &mut output,
            "nanotrace_server_info",
            "Nanotrace server metadata.",
            "gauge",
        );
        output.push_str("nanotrace_server_info{ingest=\"kafka\"} 1\n");

        push_metric_help(
            &mut output,
            "nanotrace_server_uptime_seconds",
            "Seconds since the server process started.",
            "gauge",
        );
        output.push_str(&format!(
            "nanotrace_server_uptime_seconds {:.3}\n",
            self.started_at.elapsed().as_secs_f64()
        ));

        self.render_http_metrics(&mut output);
        self.render_ingest_metrics(&mut output);
        self.render_query_metrics(&mut output);
        self.render_backfill_metrics(&mut output);
        self.render_account_failure_metrics(&mut output);
        render_auth_metrics(&mut output, auth);
        output
    }

    fn render_http_metrics(&self, output: &mut String) {
        push_metric_help(
            output,
            "nanotrace_http_requests_total",
            "HTTP requests by method, route template, and response status.",
            "counter",
        );
        push_metric_help(
            output,
            "nanotrace_http_request_duration_seconds",
            "HTTP request duration by method, route template, and response status.",
            "summary",
        );
        let Ok(metrics) = self.http.lock() else {
            return;
        };
        for (key, stats) in metrics.iter() {
            let labels = format!(
                "method=\"{}\",route=\"{}\",status=\"{}\"",
                label_value(&key.method),
                label_value(&key.route),
                key.status
            );
            output.push_str(&format!(
                "nanotrace_http_requests_total{{{labels}}} {}\n",
                stats.count
            ));
            push_duration_stats(
                output,
                "nanotrace_http_request_duration_seconds",
                &labels,
                stats,
            );
        }
    }

    fn render_ingest_metrics(&self, output: &mut String) {
        push_metric_help(
            output,
            "nanotrace_ingest_batches_total",
            "Accepted ingest batches.",
            "counter",
        );
        output.push_str(&format!(
            "nanotrace_ingest_batches_total{{result=\"accepted\"}} {}\n",
            self.ingest_batches_accepted.load(Ordering::Relaxed)
        ));
        push_metric_help(
            output,
            "nanotrace_ingest_bytes_total",
            "Accepted ingest request bytes.",
            "counter",
        );
        output.push_str(&format!(
            "nanotrace_ingest_bytes_total{{result=\"accepted\"}} {}\n",
            self.ingest_bytes_accepted.load(Ordering::Relaxed)
        ));
    }

    fn render_query_metrics(&self, output: &mut String) {
        push_metric_help(
            output,
            "nanotrace_query_requests_total",
            "Structured query requests by query type and result.",
            "counter",
        );
        push_metric_help(
            output,
            "nanotrace_query_duration_seconds",
            "Structured query execution duration by query type and result.",
            "summary",
        );
        let Ok(metrics) = self.queries.lock() else {
            return;
        };
        for (key, stats) in metrics.iter() {
            let labels = format!(
                "query_type=\"{}\",result=\"{}\"",
                key.query_type, key.result
            );
            output.push_str(&format!(
                "nanotrace_query_requests_total{{{labels}}} {}\n",
                stats.count
            ));
            push_duration_stats(output, "nanotrace_query_duration_seconds", &labels, stats);
        }
    }

    fn render_backfill_metrics(&self, output: &mut String) {
        push_metric_help(
            output,
            "nanotrace_backfill_jobs_created_total",
            "Definition backfill jobs created through the server API.",
            "counter",
        );
        output.push_str(&format!(
            "nanotrace_backfill_jobs_created_total {}\n",
            self.backfills_created.load(Ordering::Relaxed)
        ));
    }

    fn render_account_failure_metrics(&self, output: &mut String) {
        push_metric_help(
            output,
            "nanotrace_account_api_failures_total",
            "Account and organization API failures by area and status class.",
            "counter",
        );
        let Ok(metrics) = self.account_failures.lock() else {
            return;
        };
        for (key, count) in metrics.iter() {
            output.push_str(&format!(
                "nanotrace_account_api_failures_total{{area=\"{}\",status_class=\"{}xx\"}} {}\n",
                key.area, key.status_class, count
            ));
        }
    }
}

impl DurationStats {
    fn record(&mut self, elapsed: Duration) {
        self.count += 1;
        self.sum += elapsed;
    }
}

fn render_auth_metrics(output: &mut String, auth: Option<&AuthStore>) {
    push_metric_help(
        output,
        "nanotrace_auth_api_key_cache_loaded",
        "Whether the API key cache is loaded in this server process.",
        "gauge",
    );
    push_metric_help(
        output,
        "nanotrace_auth_api_key_cache_entries",
        "Number of API keys in the in-process auth cache.",
        "gauge",
    );
    push_metric_help(
        output,
        "nanotrace_auth_api_key_cache_age_seconds",
        "Seconds since the API key cache was last loaded.",
        "gauge",
    );
    let Some(auth) = auth else {
        output.push_str("nanotrace_auth_api_key_cache_loaded 0\n");
        output.push_str("nanotrace_auth_api_key_cache_entries 0\n");
        return;
    };
    let stats = auth.api_key_cache_stats();
    output.push_str(&format!(
        "nanotrace_auth_api_key_cache_loaded {}\n",
        u8::from(stats.loaded)
    ));
    output.push_str(&format!(
        "nanotrace_auth_api_key_cache_entries {}\n",
        stats.entries
    ));
    if let Some(age) = stats.age {
        output.push_str(&format!(
            "nanotrace_auth_api_key_cache_age_seconds {:.3}\n",
            age.as_secs_f64()
        ));
    }
}

fn account_route_area(route: &str) -> Option<&'static str> {
    if route.starts_with("/v1/organizations") || route.starts_with("/v1/organization-invitations") {
        Some("organizations")
    } else if route.starts_with("/v1/api-keys") {
        Some("api_keys")
    } else if route.starts_with("/auth/") || route.starts_with("/v1/auth/") {
        Some("auth")
    } else {
        None
    }
}

fn push_metric_help(output: &mut String, name: &str, help: &str, metric_type: &str) {
    output.push_str(&format!(
        "# HELP {name} {help}\n# TYPE {name} {metric_type}\n"
    ));
}

fn push_duration_stats(output: &mut String, name: &str, labels: &str, stats: &DurationStats) {
    output.push_str(&format!("{name}_count{{{labels}}} {}\n", stats.count));
    output.push_str(&format!(
        "{name}_sum{{{labels}}} {:.6}\n",
        stats.sum.as_secs_f64()
    ));
}

fn label_value(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('\n', "\\n")
        .replace('"', "\\\"")
}
