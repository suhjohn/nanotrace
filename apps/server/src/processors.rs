use std::{collections::BTreeMap, process::Stdio, sync::Arc};

use aws_sdk_s3::{Client as S3Client, primitives::ByteStream};
use chrono::Utc;
use nanotrace_processor_runtime::{ProcessorArtifact, ProcessorIndex, ProcessorManifest};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::process::Command;
use tracing::{error, info};

use crate::config::Config;

#[derive(Clone)]
pub struct ProcessorStore {
    cfg: Arc<Config>,
    s3: S3Client,
}

#[derive(Debug, Deserialize)]
pub struct PutProcessorRequest {
    #[serde(default)]
    pub upload: Option<ProcessorStageRequest>,
    #[serde(default)]
    pub loader: Option<ProcessorStageRequest>,
}

#[derive(Debug, Deserialize)]
pub struct ProcessorStageRequest {
    pub code: String,
    #[serde(default)]
    pub config: Value,
}

#[derive(Debug, Serialize)]
pub struct ProcessorListResponse {
    pub processors: Vec<ProcessorManifest>,
}

#[derive(Debug, thiserror::Error)]
pub enum ProcessorStoreError {
    #[error("S3 bucket is not configured")]
    S3NotConfigured,
    #[error("invalid processor name")]
    InvalidName,
    #[error("at least one of upload or loader is required")]
    MissingStage,
    #[error("{0} code is required")]
    MissingCode(&'static str),
    #[error("S3 error: {0}")]
    S3(String),
    #[error("invalid processor JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("failed to spawn processor builder: {0}")]
    Spawn(std::io::Error),
}

impl ProcessorStore {
    pub fn new(cfg: Arc<Config>, s3: S3Client) -> Self {
        Self { cfg, s3 }
    }

    pub async fn put(
        &self,
        name: &str,
        request: PutProcessorRequest,
    ) -> Result<ProcessorManifest, ProcessorStoreError> {
        validate_name(name)?;
        let bucket = self.bucket()?;
        let stages = request.stages()?;
        let mut configs = BTreeMap::new();

        for stage in &stages {
            let stage_request = request
                .stage(stage)
                .ok_or(ProcessorStoreError::MissingStage)?;
            let code = stage_request.code.trim();
            if code.is_empty() {
                return Err(ProcessorStoreError::MissingCode(stage));
            }
            configs.insert(stage.to_string(), stage_request.config.clone());
            self.put_bytes(
                bucket,
                &format!("processors/{name}/{stage}/source.rs"),
                code.as_bytes().to_vec(),
            )
            .await?;
            self.put_json(
                bucket,
                &format!("processors/{name}/{stage}/config.json"),
                &stage_request.config,
            )
            .await?;
        }

        let manifest = ProcessorManifest {
            name: name.to_string(),
            stages: stages.iter().map(|stage| stage.to_string()).collect(),
            status: "building".to_string(),
            config: Value::Null,
            artifact_key: None,
            artifact_sha256: None,
            configs,
            artifacts: BTreeMap::<String, ProcessorArtifact>::new(),
            error: None,
            updated_at: Some(Utc::now().to_rfc3339()),
        };
        self.put_json(bucket, &manifest_key(name), &manifest)
            .await?;
        self.upsert_index(bucket, name).await?;
        self.spawn_builder(name)?;

        Ok(manifest)
    }

    pub async fn list(&self) -> Result<Vec<ProcessorManifest>, ProcessorStoreError> {
        let bucket = self.bucket()?;
        let index = self.get_index(bucket).await?;
        let mut processors = Vec::new();
        for name in index.processors {
            if let Ok(manifest) = self
                .get_json::<ProcessorManifest>(bucket, &manifest_key(&name))
                .await
            {
                if manifest.status != "deleted" {
                    processors.push(manifest);
                }
            }
        }
        Ok(processors)
    }

    pub async fn delete(&self, name: &str) -> Result<ProcessorManifest, ProcessorStoreError> {
        validate_name(name)?;
        let bucket = self.bucket()?;
        let mut index = self.get_index(bucket).await?;
        index.processors.retain(|candidate| candidate != name);
        self.put_json(bucket, "processors/index.json", &index)
            .await?;

        let manifest = ProcessorManifest {
            name: name.to_string(),
            stages: Vec::new(),
            status: "deleted".to_string(),
            config: Value::Null,
            artifact_key: None,
            artifact_sha256: None,
            configs: BTreeMap::new(),
            artifacts: BTreeMap::new(),
            error: None,
            updated_at: Some(Utc::now().to_rfc3339()),
        };
        self.put_json(bucket, &manifest_key(name), &manifest)
            .await?;
        Ok(manifest)
    }

    fn bucket(&self) -> Result<&str, ProcessorStoreError> {
        self.cfg
            .s3_bucket
            .as_deref()
            .ok_or(ProcessorStoreError::S3NotConfigured)
    }

    async fn upsert_index(&self, bucket: &str, name: &str) -> Result<(), ProcessorStoreError> {
        let mut index = self.get_index(bucket).await?;
        if !index.processors.iter().any(|candidate| candidate == name) {
            index.processors.push(name.to_string());
        }
        index.processors.sort();
        index.processors.dedup();
        self.put_json(bucket, "processors/index.json", &index).await
    }

    async fn get_index(&self, bucket: &str) -> Result<ProcessorIndex, ProcessorStoreError> {
        match self
            .get_json::<ProcessorIndex>(bucket, "processors/index.json")
            .await
        {
            Ok(index) => Ok(index),
            Err(ProcessorStoreError::S3(_)) => Ok(ProcessorIndex {
                processors: Vec::new(),
            }),
            Err(err) => Err(err),
        }
    }

    async fn get_json<T>(&self, bucket: &str, key: &str) -> Result<T, ProcessorStoreError>
    where
        T: for<'de> Deserialize<'de>,
    {
        let output = self
            .s3
            .get_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .map_err(|err| ProcessorStoreError::S3(err.to_string()))?;
        let bytes = output
            .body
            .collect()
            .await
            .map_err(|err| ProcessorStoreError::S3(err.to_string()))?
            .into_bytes();
        serde_json::from_slice(&bytes).map_err(ProcessorStoreError::Json)
    }

    async fn put_json<T: Serialize>(
        &self,
        bucket: &str,
        key: &str,
        value: &T,
    ) -> Result<(), ProcessorStoreError> {
        let bytes = serde_json::to_vec_pretty(value)?;
        self.put_bytes(bucket, key, bytes).await
    }

    async fn put_bytes(
        &self,
        bucket: &str,
        key: &str,
        bytes: Vec<u8>,
    ) -> Result<(), ProcessorStoreError> {
        self.s3
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(ByteStream::from(bytes))
            .send()
            .await
            .map_err(|err| ProcessorStoreError::S3(err.to_string()))?;
        Ok(())
    }

    fn spawn_builder(&self, name: &str) -> Result<(), ProcessorStoreError> {
        let Some(bucket) = self.cfg.s3_bucket.clone() else {
            return Err(ProcessorStoreError::S3NotConfigured);
        };
        let mut parts = self.cfg.processor_builder_cmd.split_whitespace();
        let Some(program) = parts.next() else {
            return Err(ProcessorStoreError::Spawn(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "PROCESSOR_BUILDER_CMD is empty",
            )));
        };

        let mut command = Command::new(program);
        command
            .args(parts)
            .env("PROCESSOR_BUCKET", bucket)
            .env("PROCESSOR_NAME", name)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        let mut child = command.spawn().map_err(ProcessorStoreError::Spawn)?;
        let processor_name = name.to_string();
        tokio::spawn(async move {
            match child.wait().await {
                Ok(status) if status.success() => {
                    info!(processor = processor_name, "processor builder finished");
                }
                Ok(status) => {
                    error!(processor = processor_name, status = %status, "processor builder failed");
                }
                Err(err) => {
                    error!(processor = processor_name, error = %err, "processor builder wait failed");
                }
            }
        });
        Ok(())
    }
}

impl PutProcessorRequest {
    fn stages(&self) -> Result<Vec<&'static str>, ProcessorStoreError> {
        let mut stages = Vec::new();
        if self.upload.is_some() {
            stages.push("upload");
        }
        if self.loader.is_some() {
            stages.push("loader");
        }
        if stages.is_empty() {
            return Err(ProcessorStoreError::MissingStage);
        }
        Ok(stages)
    }

    fn stage(&self, stage: &str) -> Option<&ProcessorStageRequest> {
        match stage {
            "upload" => self.upload.as_ref(),
            "loader" => self.loader.as_ref(),
            _ => None,
        }
    }
}

fn manifest_key(name: &str) -> String {
    format!("processors/{name}/manifest.json")
}

fn validate_name(name: &str) -> Result<(), ProcessorStoreError> {
    let valid = !name.is_empty()
        && name.len() <= 128
        && name
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_' || ch == '-');
    if valid {
        Ok(())
    } else {
        Err(ProcessorStoreError::InvalidName)
    }
}
