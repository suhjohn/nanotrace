use std::{path::Path, sync::Arc, time::Duration};

use aws_sdk_s3::{Client, primitives::ByteStream};
use nanotrace_processor_runtime::ProcessorRuntime;
use tokio::fs;
use tracing::{error, info, warn};

use crate::{
    config::Config,
    event,
    event_log::{find_with_suffix, status_path},
};

#[derive(Debug, thiserror::Error)]
pub enum UploadError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("S3 upload failed: {0}")]
    S3(String),
    #[error("S3 upload timed out after {0:?}")]
    S3Timeout(Duration),
    #[error("event error: {0}")]
    Event(#[from] event::EventError),
    #[error("processor failed: {0}")]
    Processor(String),
}

pub async fn run(cfg: Arc<Config>, processors: ProcessorRuntime) {
    let Some(bucket) = cfg.s3_bucket.clone() else {
        warn!("NANOTRACE_S3_BUCKET/S3_BUCKET not set; uploader disabled");
        return;
    };

    let aws_config = aws_config::load_from_env().await;
    let client = s3_client(&aws_config);
    let mut interval = tokio::time::interval(cfg.upload_poll_interval);

    loop {
        interval.tick().await;
        if let Err(err) = upload_ready_files(&cfg, &client, &bucket, &processors).await {
            error!(error = %err, "uploader pass failed");
        }
    }
}

fn s3_client(config: &aws_config::SdkConfig) -> Client {
    let mut builder = aws_sdk_s3::config::Builder::from(config);
    if env_bool("AWS_S3_FORCE_PATH_STYLE") || env_bool("AWS_S3_PATH_STYLE") {
        builder.set_force_path_style(Some(true));
    }
    Client::from_conf(builder.build())
}

fn env_bool(key: &str) -> bool {
    std::env::var(key)
        .ok()
        .is_some_and(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
}

async fn upload_ready_files(
    cfg: &Config,
    client: &Client,
    bucket: &str,
    processors: &ProcessorRuntime,
) -> Result<(), UploadError> {
    let ready_files = find_with_suffix(&cfg.data_dir, ".ready").await?;

    for ready in ready_files {
        let uploading = status_path(&ready, ".ready", ".uploading");
        if fs::rename(&ready, &uploading).await.is_err() {
            continue;
        }

        match upload_one(cfg, client, bucket, &uploading, processors).await {
            Ok(Some(key)) => {
                let done = status_path(&uploading, ".uploading", ".done");
                fs::rename(&uploading, done).await?;
                info!(bucket, key, "uploaded event part");
            }
            Ok(None) => {
                let done = status_path(&uploading, ".uploading", ".done");
                fs::rename(&uploading, done).await?;
                info!(bucket, path = %uploading.display(), "upload processor dropped event part");
            }
            Err(err) => {
                let failed = status_path(&uploading, ".uploading", ".failed");
                let _ = fs::rename(&uploading, failed).await;
                error!(path = %uploading.display(), error = %err, "failed to upload event part");
            }
        }
    }

    Ok(())
}

async fn upload_one(
    cfg: &Config,
    client: &Client,
    bucket: &str,
    path: &Path,
    processors: &ProcessorRuntime,
) -> Result<Option<String>, UploadError> {
    let key = object_key(cfg, path)?;
    let bytes = fs::read(path).await?;
    let body_bytes = if processors.has_processors() {
        let transformed = processors
            .transform_ndjson(&bytes)
            .map_err(|err| UploadError::Processor(err.to_string()))?;
        let restamped = event::restamp_ndjson(&transformed, &key, cfg.max_event_bytes)?;
        if restamped.is_empty() {
            return Ok(None);
        }
        restamped
    } else {
        bytes
    };

    let timeout = Duration::from_secs(60);
    tokio::time::timeout(
        timeout,
        client
            .put_object()
            .bucket(bucket)
            .key(&key)
            .content_type("application/x-ndjson")
            .content_length(body_bytes.len() as i64)
            .body(ByteStream::from(body_bytes))
            .send(),
    )
    .await
    .map_err(|_| UploadError::S3Timeout(timeout))?
    .map_err(|err| UploadError::S3(err.to_string()))?;

    Ok(Some(key))
}

fn object_key(cfg: &Config, path: &Path) -> Result<String, UploadError> {
    let relative = path
        .strip_prefix(&cfg.data_dir)
        .map_err(std::io::Error::other)?;
    let key = relative.to_string_lossy();
    let key = key.trim_start_matches('/');
    Ok(key.strip_suffix(".uploading").unwrap_or(key).to_string())
}
