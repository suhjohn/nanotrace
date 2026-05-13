use std::{path::Path, sync::Arc, time::SystemTime};

use tokio::fs;
use tracing::{error, info, warn};

use crate::{config::Config, event_log::find_with_suffix};

#[derive(Debug, thiserror::Error)]
pub enum RetentionError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

pub async fn run(cfg: Arc<Config>) {
    let Some(retain_for) = cfg.done_retention else {
        info!("local done-file cleanup disabled");
        return;
    };

    let interval_duration = cfg
        .done_cleanup_interval
        .min(retain_for)
        .max(std::time::Duration::from_secs(1));
    let mut interval = tokio::time::interval(interval_duration);

    loop {
        interval.tick().await;
        if let Err(err) = cleanup_done_files(&cfg).await {
            error!(error = %err, "done-file cleanup pass failed");
        }
    }
}

async fn cleanup_done_files(cfg: &Config) -> Result<(), RetentionError> {
    let done_files = find_with_suffix(&cfg.data_dir, ".done").await?;
    for path in done_files {
        match should_delete(&path, cfg).await {
            Ok(true) => {
                let bytes = file_len(&path).await.unwrap_or(0);
                fs::remove_file(&path).await?;
                info!(path = %path.display(), bytes, "deleted uploaded event part from local disk");
            }
            Ok(false) => {}
            Err(err) => {
                warn!(path = %path.display(), error = %err, "failed to evaluate done-file retention")
            }
        }
    }
    Ok(())
}

async fn should_delete(path: &Path, cfg: &Config) -> Result<bool, std::io::Error> {
    let Some(retain_for) = cfg.done_retention else {
        return Ok(false);
    };
    let metadata = fs::metadata(path).await?;
    let modified = metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH);
    match modified.elapsed() {
        Ok(age) => Ok(age >= retain_for),
        Err(_) => Ok(false),
    }
}

async fn file_len(path: &Path) -> Result<u64, std::io::Error> {
    Ok(fs::metadata(path).await?.len())
}

#[cfg(test)]
mod tests {
    use std::{fs, time::Duration};

    use super::should_delete;
    use crate::config::Config;
    use nanotrace_auth::AuthConfig;

    #[tokio::test]
    async fn retention_disabled_never_deletes() {
        let temp = std::env::temp_dir().join(format!(
            "nanotrace-retention-disabled-{}",
            std::process::id()
        ));
        fs::create_dir_all(&temp).expect("create temp dir");
        let file = temp.join("part.ndjson.done");
        fs::write(&file, b"ok").expect("write done file");

        let cfg = test_config(temp.clone(), None);
        assert!(!should_delete(&file, &cfg).await.expect("check retention"));

        let _ = fs::remove_dir_all(temp);
    }

    #[tokio::test]
    async fn old_done_file_is_eligible_for_delete() {
        let temp =
            std::env::temp_dir().join(format!("nanotrace-retention-old-{}", std::process::id()));
        fs::create_dir_all(&temp).expect("create temp dir");
        let file = temp.join("part.ndjson.done");
        fs::write(&file, b"ok").expect("write done file");

        let cfg = test_config(temp.clone(), Some(Duration::from_secs(0)));
        assert!(should_delete(&file, &cfg).await.expect("check retention"));

        let _ = fs::remove_dir_all(temp);
    }

    fn test_config(data_dir: std::path::PathBuf, done_retention: Option<Duration>) -> Config {
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
            max_request_bytes: 1024,
            max_event_bytes: 1024,
            rotate_bytes: 1024,
            rotate_after: Duration::from_secs(1),
            upload_poll_interval: Duration::from_millis(500),
            done_retention,
            done_cleanup_interval: Duration::from_secs(60),
            writer_lanes: 1,
            writer_queue_capacity: 16,
            writer_flush_interval: Duration::from_millis(100),
            writer_flush_bytes: 1024,
            compact_batch_receipts: false,
            processor_poll_interval: Duration::from_secs(30),
            processor_builder_cmd: "true".to_string(),
            auth: test_auth_config(),
            email_from: None,
            cors_allowed_origins: Vec::new(),
        }
    }

    fn test_auth_config() -> AuthConfig {
        AuthConfig {
            database_url: None,
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
