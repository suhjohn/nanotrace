use std::{
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use chrono::{Datelike, Timelike, Utc};
use tokio::{
    fs::{self, File, OpenOptions},
    io::AsyncWriteExt,
    sync::{mpsc, oneshot},
};

use crate::{config::Config, event};

#[derive(Debug, serde::Serialize)]
pub struct WriteReceipt {
    pub event_id: String,
    pub source_file: String,
    pub source_offset: u64,
    pub source_length: u32,
}

#[derive(Debug)]
pub struct WriteReceipts {
    pub is_batch: bool,
    pub receipts: Vec<WriteReceipt>,
}

#[derive(Debug, thiserror::Error)]
pub enum EventLogError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("event error: {0}")]
    Event(#[from] event::EventError),
    #[error("writer queue is closed")]
    QueueClosed,
    #[error("writer dropped the response")]
    WriterDropped,
    #[error("writer commit failed: {0}")]
    CommitFailed(String),
}

pub struct EventLogWriter {
    cfg: Arc<Config>,
    lanes: Vec<WriterLaneHandle>,
    metrics: Arc<WriterMetrics>,
    next_lane: AtomicU64,
}

struct WriterLaneHandle {
    tx: mpsc::Sender<WriteCommand>,
}

enum WriteCommand {
    Append {
        events: event::PreparedEvents,
        enqueued_at: Instant,
        response: oneshot::Sender<Result<WriteReceipts, EventLogError>>,
    },
    RotateIfOld {
        response: oneshot::Sender<Result<(), EventLogError>>,
    },
    Flush {
        response: oneshot::Sender<Result<(), EventLogError>>,
    },
}

struct WriterLane {
    id: usize,
    cfg: Arc<Config>,
    host: String,
    state: WriterState,
    rx: mpsc::Receiver<WriteCommand>,
    metrics: Arc<WriterMetrics>,
    pending_flush_bytes: u64,
    pending_commits: Vec<PendingCommit>,
}

struct WriterState {
    file: File,
    tmp_path: PathBuf,
    s3_key: String,
    opened_at: Instant,
    bytes: u64,
}

struct PendingCommit {
    response: oneshot::Sender<Result<WriteReceipts, EventLogError>>,
    receipts: WriteReceipts,
    event_count: usize,
}

struct AppendedBatch {
    receipts: WriteReceipts,
    event_count: usize,
}

#[derive(Default)]
struct WriterMetrics {
    requests_total: AtomicU64,
    events_total: AtomicU64,
    bytes_written_total: AtomicU64,
    write_errors_total: AtomicU64,
    parse_errors_total: AtomicU64,
    queued_requests: AtomicU64,
    request_read_us_total: AtomicU64,
    request_read_count: AtomicU64,
    queue_wait_us_total: AtomicU64,
    serialize_us_total: AtomicU64,
    write_us_total: AtomicU64,
    flush_us_total: AtomicU64,
    rotate_us_total: AtomicU64,
    append_us_total: AtomicU64,
}

impl EventLogWriter {
    pub async fn new(cfg: Arc<Config>) -> Result<Self, EventLogError> {
        fs::create_dir_all(&cfg.data_dir).await?;
        recover_uploading_files(&cfg.data_dir).await?;
        recover_tmp_files(&cfg.data_dir).await?;

        let host = hostname();
        let metrics = Arc::new(WriterMetrics::default());
        let mut lanes = Vec::with_capacity(cfg.writer_lanes);

        for id in 0..cfg.writer_lanes {
            let state = open_new_file(&cfg, &host, id).await?;
            let (tx, rx) = mpsc::channel(cfg.writer_queue_capacity);
            let lane = WriterLane {
                id,
                cfg: cfg.clone(),
                host: host.clone(),
                state,
                rx,
                metrics: metrics.clone(),
                pending_flush_bytes: 0,
                pending_commits: Vec::new(),
            };
            tokio::spawn(lane.run());
            lanes.push(WriterLaneHandle { tx });
        }

        Ok(Self {
            cfg,
            lanes,
            metrics,
            next_lane: AtomicU64::new(0),
        })
    }

    pub async fn append_bytes(
        &self,
        body: &[u8],
        tenant_id: &str,
    ) -> Result<WriteReceipts, EventLogError> {
        let started_at = Instant::now();
        let mut events = match event::prepare_events(body) {
            Ok(events) => events,
            Err(err) => {
                self.metrics
                    .parse_errors_total
                    .fetch_add(1, Ordering::Relaxed);
                return Err(err.into());
            }
        };
        events.stamp_tenant(tenant_id);

        let lane = self.next_lane.fetch_add(1, Ordering::Relaxed) as usize % self.lanes.len();
        let (tx, rx) = oneshot::channel();
        self.metrics.queued_requests.fetch_add(1, Ordering::Relaxed);
        if self.lanes[lane]
            .tx
            .send(WriteCommand::Append {
                events,
                enqueued_at: Instant::now(),
                response: tx,
            })
            .await
            .is_err()
        {
            self.metrics.queued_requests.fetch_sub(1, Ordering::Relaxed);
            return Err(EventLogError::QueueClosed);
        }

        let result = match rx.await {
            Ok(result) => result,
            Err(_) => {
                self.metrics.queued_requests.fetch_sub(1, Ordering::Relaxed);
                self.metrics
                    .append_us_total
                    .fetch_add(elapsed_us(started_at.elapsed()), Ordering::Relaxed);
                return Err(EventLogError::WriterDropped);
            }
        };
        self.metrics.queued_requests.fetch_sub(1, Ordering::Relaxed);
        self.metrics
            .append_us_total
            .fetch_add(elapsed_us(started_at.elapsed()), Ordering::Relaxed);
        result
    }

    pub async fn rotate_if_old(&self) -> Result<(), EventLogError> {
        let mut responses = Vec::with_capacity(self.lanes.len());
        for lane in &self.lanes {
            let (tx, rx) = oneshot::channel();
            lane.tx
                .send(WriteCommand::RotateIfOld { response: tx })
                .await
                .map_err(|_| EventLogError::QueueClosed)?;
            responses.push(rx);
        }

        for response in responses {
            response.await.map_err(|_| EventLogError::WriterDropped)??;
        }
        Ok(())
    }

    pub async fn flush(&self) -> Result<(), EventLogError> {
        let mut responses = Vec::with_capacity(self.lanes.len());
        for lane in &self.lanes {
            let (tx, rx) = oneshot::channel();
            lane.tx
                .send(WriteCommand::Flush { response: tx })
                .await
                .map_err(|_| EventLogError::QueueClosed)?;
            responses.push(rx);
        }

        for response in responses {
            response.await.map_err(|_| EventLogError::WriterDropped)??;
        }
        Ok(())
    }

    pub fn record_request_read(&self, elapsed: Duration) {
        self.metrics
            .request_read_us_total
            .fetch_add(elapsed_us(elapsed), Ordering::Relaxed);
        self.metrics
            .request_read_count
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn metrics_text(&self) -> String {
        self.metrics.render(self.cfg.writer_lanes)
    }
}

impl WriterLane {
    async fn run(mut self) {
        let mut flush_interval = tokio::time::interval(self.cfg.writer_flush_interval);

        loop {
            tokio::select! {
                maybe_cmd = self.rx.recv() => {
                    let Some(cmd) = maybe_cmd else {
                        let _ = self.commit_pending().await;
                        return;
                    };
                    match cmd {
                        WriteCommand::Append { events, enqueued_at, response } => {
                            self.metrics.queue_wait_us_total.fetch_add(
                                elapsed_us(enqueued_at.elapsed()),
                                Ordering::Relaxed,
                            );
                            match self.append(events).await {
                                Ok(appended) => {
                                    self.pending_commits.push(PendingCommit {
                                        response,
                                        receipts: appended.receipts,
                                        event_count: appended.event_count,
                                    });
                                    let result = self.commit_or_rotate_if_needed().await;
                                    if result.is_err() {
                                        self.metrics.write_errors_total.fetch_add(1, Ordering::Relaxed);
                                    }
                                }
                                Err(err) => {
                                    self.metrics.write_errors_total.fetch_add(1, Ordering::Relaxed);
                                    let _ = response.send(Err(err));
                                }
                            }
                        }
                        WriteCommand::RotateIfOld { response } => {
                            let result = self.rotate_if_old().await;
                            if result.is_err() {
                                self.metrics.write_errors_total.fetch_add(1, Ordering::Relaxed);
                            }
                            let _ = response.send(result);
                        }
                        WriteCommand::Flush { response } => {
                            let result = self.commit_pending().await;
                            if result.is_err() {
                                self.metrics.write_errors_total.fetch_add(1, Ordering::Relaxed);
                            }
                            let _ = response.send(result);
                        }
                    }
                }
                _ = flush_interval.tick() => {
                    if let Err(_err) = self.commit_pending().await {
                        self.metrics.write_errors_total.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }
    }

    async fn append(
        &mut self,
        events: event::PreparedEvents,
    ) -> Result<AppendedBatch, EventLogError> {
        let source_file = self.state.s3_key.clone();
        let mut offset = self.state.bytes;
        let mut lines = Vec::new();
        let mut receipts = Vec::with_capacity(events.len());
        let serialize_started_at = Instant::now();

        for event in &events.events {
            let (line, receipt) = event::serialize_line(
                event,
                source_file.clone(),
                offset,
                self.cfg.max_event_bytes,
            )?;
            offset += line.len() as u64;
            lines.extend_from_slice(&line);
            receipts.push(receipt);
        }

        self.metrics.serialize_us_total.fetch_add(
            elapsed_us(serialize_started_at.elapsed()),
            Ordering::Relaxed,
        );

        if !lines.is_empty() {
            let write_started_at = Instant::now();
            self.state.file.write_all(&lines).await?;
            self.metrics
                .write_us_total
                .fetch_add(elapsed_us(write_started_at.elapsed()), Ordering::Relaxed);

            self.state.bytes = offset;
            self.pending_flush_bytes += lines.len() as u64;
            self.metrics
                .bytes_written_total
                .fetch_add(lines.len() as u64, Ordering::Relaxed);
        }

        Ok(AppendedBatch {
            event_count: receipts.len(),
            receipts: WriteReceipts {
                is_batch: events.is_batch,
                receipts,
            },
        })
    }

    async fn commit_or_rotate_if_needed(&mut self) -> Result<(), EventLogError> {
        if self.state.bytes >= self.cfg.rotate_bytes {
            self.rotate().await
        } else if self.pending_flush_bytes >= self.cfg.writer_flush_bytes {
            self.commit_pending().await
        } else {
            Ok(())
        }
    }

    async fn rotate_if_old(&mut self) -> Result<(), EventLogError> {
        if self.state.bytes > 0 && self.state.opened_at.elapsed() >= self.cfg.rotate_after {
            self.rotate().await?;
        }
        Ok(())
    }

    async fn commit_pending(&mut self) -> Result<(), EventLogError> {
        if self.pending_flush_bytes == 0 {
            return Ok(());
        }

        let started_at = Instant::now();
        if let Err(err) = self.state.file.flush().await {
            self.fail_pending(err.to_string());
            self.pending_flush_bytes = 0;
            return Err(err.into());
        }
        if let Err(err) = self.state.file.sync_data().await {
            self.fail_pending(err.to_string());
            self.pending_flush_bytes = 0;
            return Err(err.into());
        }
        self.pending_flush_bytes = 0;
        self.metrics
            .flush_us_total
            .fetch_add(elapsed_us(started_at.elapsed()), Ordering::Relaxed);
        self.complete_pending();
        Ok(())
    }

    async fn rotate(&mut self) -> Result<(), EventLogError> {
        let started_at = Instant::now();
        self.commit_pending().await?;
        self.state.file.sync_data().await?;
        let ready_path = with_status(&self.state.tmp_path, "ready");
        fs::rename(&self.state.tmp_path, ready_path).await?;
        self.state = open_new_file(&self.cfg, &self.host, self.id).await?;
        self.metrics
            .rotate_us_total
            .fetch_add(elapsed_us(started_at.elapsed()), Ordering::Relaxed);
        Ok(())
    }

    fn complete_pending(&mut self) {
        for pending in self.pending_commits.drain(..) {
            self.metrics.requests_total.fetch_add(1, Ordering::Relaxed);
            self.metrics
                .events_total
                .fetch_add(pending.event_count as u64, Ordering::Relaxed);
            let _ = pending.response.send(Ok(pending.receipts));
        }
    }

    fn fail_pending(&mut self, message: String) {
        for pending in self.pending_commits.drain(..) {
            let _ = pending
                .response
                .send(Err(EventLogError::CommitFailed(message.clone())));
        }
    }
}

impl WriterMetrics {
    fn render(&self, lanes: usize) -> String {
        let request_read_count = self.request_read_count.load(Ordering::Relaxed);
        format!(
            "# HELP nanotrace_writer_lanes Configured writer lane count\n\
             # TYPE nanotrace_writer_lanes gauge\n\
             nanotrace_writer_lanes {lanes}\n\
             # HELP nanotrace_writer_queued_requests Requests waiting for writer responses\n\
             # TYPE nanotrace_writer_queued_requests gauge\n\
             nanotrace_writer_queued_requests {}\n\
             # HELP nanotrace_writer_requests_total Successfully committed requests\n\
             # TYPE nanotrace_writer_requests_total counter\n\
             nanotrace_writer_requests_total {}\n\
             # HELP nanotrace_writer_events_total Successfully committed events\n\
             # TYPE nanotrace_writer_events_total counter\n\
             nanotrace_writer_events_total {}\n\
             # HELP nanotrace_writer_bytes_total Event log bytes written\n\
             # TYPE nanotrace_writer_bytes_total counter\n\
             nanotrace_writer_bytes_total {}\n\
             # HELP nanotrace_writer_parse_errors_total Invalid event bodies\n\
             # TYPE nanotrace_writer_parse_errors_total counter\n\
             nanotrace_writer_parse_errors_total {}\n\
             # HELP nanotrace_writer_errors_total Writer failures\n\
             # TYPE nanotrace_writer_errors_total counter\n\
             nanotrace_writer_errors_total {}\n\
             # HELP nanotrace_request_read_seconds Request body read time\n\
             # TYPE nanotrace_request_read_seconds summary\n\
             nanotrace_request_read_seconds_count {request_read_count}\n\
             nanotrace_request_read_seconds_sum {:.6}\n\
             # HELP nanotrace_writer_queue_wait_seconds Time spent waiting inside writer queues\n\
             # TYPE nanotrace_writer_queue_wait_seconds counter\n\
             nanotrace_writer_queue_wait_seconds_total {:.6}\n\
             # HELP nanotrace_writer_serialize_seconds Time spent serializing ClickHouse lines\n\
             # TYPE nanotrace_writer_serialize_seconds counter\n\
             nanotrace_writer_serialize_seconds_total {:.6}\n\
             # HELP nanotrace_writer_write_seconds Time spent in file write calls\n\
             # TYPE nanotrace_writer_write_seconds counter\n\
             nanotrace_writer_write_seconds_total {:.6}\n\
             # HELP nanotrace_writer_flush_seconds Time spent flushing or syncing files\n\
             # TYPE nanotrace_writer_flush_seconds counter\n\
             nanotrace_writer_flush_seconds_total {:.6}\n\
             # HELP nanotrace_writer_rotate_seconds Time spent rotating files\n\
             # TYPE nanotrace_writer_rotate_seconds counter\n\
             nanotrace_writer_rotate_seconds_total {:.6}\n\
             # HELP nanotrace_writer_append_seconds End-to-end append latency inside server\n\
             # TYPE nanotrace_writer_append_seconds counter\n\
             nanotrace_writer_append_seconds_total {:.6}\n",
            self.queued_requests.load(Ordering::Relaxed),
            self.requests_total.load(Ordering::Relaxed),
            self.events_total.load(Ordering::Relaxed),
            self.bytes_written_total.load(Ordering::Relaxed),
            self.parse_errors_total.load(Ordering::Relaxed),
            self.write_errors_total.load(Ordering::Relaxed),
            seconds(self.request_read_us_total.load(Ordering::Relaxed)),
            seconds(self.queue_wait_us_total.load(Ordering::Relaxed)),
            seconds(self.serialize_us_total.load(Ordering::Relaxed)),
            seconds(self.write_us_total.load(Ordering::Relaxed)),
            seconds(self.flush_us_total.load(Ordering::Relaxed)),
            seconds(self.rotate_us_total.load(Ordering::Relaxed)),
            seconds(self.append_us_total.load(Ordering::Relaxed)),
        )
    }
}

async fn open_new_file(
    cfg: &Config,
    host: &str,
    lane_id: usize,
) -> Result<WriterState, EventLogError> {
    let s3_key = new_s3_key(cfg, host, lane_id);
    let tmp_path = cfg.data_dir.join(format!("{s3_key}.tmp"));

    if let Some(parent) = tmp_path.parent() {
        fs::create_dir_all(parent).await?;
    }

    let file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&tmp_path)
        .await?;

    Ok(WriterState {
        file,
        tmp_path,
        s3_key,
        opened_at: Instant::now(),
        bytes: 0,
    })
}

fn new_s3_key(cfg: &Config, host: &str, lane_id: usize) -> String {
    let now = Utc::now();
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or(0);
    let part = format!("part-{millis}.ndjson");
    let prefix = cfg.s3_prefix.trim_matches('/');
    let key = format!(
        "dt={:04}-{:02}-{:02}/hour={:02}/host={host}/lane={lane_id}/{part}",
        now.year(),
        now.month(),
        now.day(),
        now.hour()
    );

    if prefix.is_empty() {
        key
    } else {
        format!("{prefix}/{key}")
    }
}

pub fn status_path(path: &Path, from: &str, to: &str) -> PathBuf {
    let path = path.to_string_lossy();
    PathBuf::from(path.strip_suffix(from).unwrap_or(&path).to_string() + to)
}

fn with_status(path: &Path, status: &str) -> PathBuf {
    status_path(path, ".tmp", &format!(".{status}"))
}

async fn recover_tmp_files(root: &Path) -> Result<(), EventLogError> {
    let files = find_with_suffix(root, ".tmp").await?;
    for tmp in files {
        if !truncate_to_complete_lines(&tmp).await? {
            fs::remove_file(tmp).await?;
            continue;
        }
        let ready = status_path(&tmp, ".tmp", ".ready");
        fs::rename(tmp, ready).await?;
    }
    Ok(())
}

async fn truncate_to_complete_lines(path: &Path) -> Result<bool, EventLogError> {
    let contents = fs::read(path).await?;
    let Some(last_newline) = contents.iter().rposition(|byte| *byte == b'\n') else {
        return Ok(false);
    };
    let keep_len = (last_newline + 1) as u64;
    let file = OpenOptions::new().write(true).open(path).await?;
    if keep_len < contents.len() as u64 {
        file.set_len(keep_len).await?;
    }
    file.sync_data().await?;
    Ok(keep_len > 0)
}

async fn recover_uploading_files(root: &Path) -> Result<(), EventLogError> {
    let files = find_with_suffix(root, ".uploading").await?;
    for uploading in files {
        let ready = status_path(&uploading, ".uploading", ".ready");
        fs::rename(uploading, ready).await?;
    }
    Ok(())
}

pub async fn find_with_suffix(
    root: &Path,
    suffix: &'static str,
) -> Result<Vec<PathBuf>, std::io::Error> {
    let root = root.to_path_buf();
    tokio::task::spawn_blocking(move || {
        let mut out = Vec::new();
        collect_with_suffix(&root, suffix, &mut out)?;
        Ok(out)
    })
    .await
    .map_err(std::io::Error::other)?
}

fn collect_with_suffix(
    root: &Path,
    suffix: &str,
    out: &mut Vec<PathBuf>,
) -> Result<(), std::io::Error> {
    if !root.exists() {
        return Ok(());
    }

    for entry in std::fs::read_dir(root)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            collect_with_suffix(&path, suffix, out)?;
        } else if path.to_string_lossy().ends_with(suffix) {
            out.push(path);
        }
    }
    out.sort();
    Ok(())
}

fn elapsed_us(elapsed: Duration) -> u64 {
    elapsed.as_micros().try_into().unwrap_or(u64::MAX)
}

fn seconds(micros: u64) -> f64 {
    micros as f64 / 1_000_000.0
}

fn hostname() -> String {
    std::env::var("HOSTNAME")
        .ok()
        .or_else(|| std::fs::read_to_string("/etc/hostname").ok())
        .map(|value| sanitize_host(&value))
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "unknown".to_string())
}

fn sanitize_host(value: &str) -> String {
    value
        .trim()
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' || ch == '.' {
                ch
            } else {
                '-'
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{EventLogWriter, truncate_to_complete_lines};
    use crate::config::Config;
    use nanotrace_auth::AuthConfig;
    use std::{sync::Arc, time::Duration};
    use tokio::fs;

    #[tokio::test]
    async fn append_returns_after_group_commit() {
        let temp =
            std::env::temp_dir().join(format!("nanotrace-writer-commit-{}", std::process::id()));
        let cfg = Arc::new(test_config(temp.clone()));
        let writer = EventLogWriter::new(cfg).await.expect("create writer");

        let receipts = writer
            .append_bytes(
                br#"{
                    "event_id": "evt_1",
                    "timestamp": "2026-05-10T12:34:56.789Z",
                    "data": {"tenant_id": "tenant-a", "event_type": "log"}
                }"#,
                "org_default",
            )
            .await
            .expect("append commits");

        assert_eq!(receipts.receipts.len(), 1);
        assert_eq!(receipts.receipts[0].source_offset, 0);
        assert!(receipts.receipts[0].source_length > 0);

        let files = super::find_with_suffix(&temp, ".tmp")
            .await
            .expect("find tmp files");
        assert_eq!(files.len(), 1);
        let contents = fs::read_to_string(&files[0]).await.expect("read tmp");
        assert!(contents.contains("\"event_id\":\"evt_1\""));

        let _ = fs::remove_dir_all(temp).await;
    }

    #[tokio::test]
    async fn recovery_truncates_partial_trailing_line() {
        let temp =
            std::env::temp_dir().join(format!("nanotrace-writer-truncate-{}", std::process::id()));
        fs::create_dir_all(&temp).await.expect("create temp dir");
        let path = temp.join("part.ndjson.tmp");
        fs::write(&path, b"{\"ok\":true}\n{\"partial\"")
            .await
            .expect("write tmp");

        assert!(
            truncate_to_complete_lines(&path)
                .await
                .expect("truncate succeeds")
        );
        let contents = fs::read(&path).await.expect("read truncated tmp");
        assert_eq!(contents, b"{\"ok\":true}\n");

        let _ = fs::remove_dir_all(temp).await;
    }

    fn test_config(data_dir: std::path::PathBuf) -> Config {
        Config {
            port: 18473,
            data_dir,
            ui_dir: std::path::PathBuf::from("/tmp/nanotrace-ui"),
            s3_bucket: Some("bucket".to_owned()),
            s3_prefix: "events".to_owned(),
            clickhouse_url: None,
            clickhouse_user: None,
            clickhouse_password: None,
            clickhouse_database: "observatory".to_owned(),
            clickhouse_table: "events".to_owned(),
            clickhouse_facets_table: "event_facets".to_owned(),
            clickhouse_event_index_table: "event_facet_index".to_owned(),
            clickhouse_hot_dimensions_table: "hot_dimensions".to_owned(),
            clickhouse_max_result_rows: 100_000,
            clickhouse_max_execution_secs: 30,
            clickhouse_max_bytes_to_read: 1_000_000_000,
            max_request_bytes: 1024 * 1024,
            max_event_bytes: 1024 * 1024,
            rotate_bytes: 64 * 1024 * 1024,
            rotate_after: Duration::from_secs(60),
            upload_poll_interval: Duration::from_millis(500),
            done_retention: Some(Duration::from_secs(60)),
            done_cleanup_interval: Duration::from_secs(60),
            writer_lanes: 1,
            writer_queue_capacity: 16,
            writer_flush_interval: Duration::from_millis(5),
            writer_flush_bytes: 1024 * 1024,
            compact_batch_receipts: false,
            processor_poll_interval: Duration::from_secs(30),
            processor_builder_cmd: "true".to_string(),
            processor_prefix: "organizations/org_default/processors".to_string(),
            cloud_provider: "aws".to_string(),
            region: "us-west-2".to_string(),
            supported_regions: vec!["us-west-2".to_string()],
            clickhouse_mode: "external".to_string(),
            clickhouse_region: "us-west-2".to_string(),
            clickhouse_service_id: None,
            data_plane_kms_key_arn: None,
            data_plane_organization_id: None,
            data_plane_shared_secret: None,
            shared_data_plane_ingest_url: None,
            shared_data_plane_query_url: None,
            shared_data_plane_secret: None,
            auth: test_auth_config(),
            email_from: None,
            cors_allowed_origins: Vec::new(),
            app_base_url: None,
        }
    }

    fn test_auth_config() -> AuthConfig {
        AuthConfig {
            postgres_url: None,
            bootstrap_api_key: None,
            public_base_url: None,
            session_cookie_name: "nanotrace_session".to_string(),
            session_ttl: Duration::from_secs(3600),
            session_secure: false,
            magic_link_ttl: Duration::from_secs(600),
            allowed_emails: Vec::new(),
            admin_emails: Vec::new(),
        }
    }
}
