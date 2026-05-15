use std::{ffi::c_int, path::PathBuf, sync::Arc, time::Duration};

use aws_sdk_s3::Client as S3Client;
use libloading::{Library, Symbol};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use tokio::sync::watch;
use tracing::{error, info, warn};

#[derive(Clone)]
pub struct ProcessorRuntime {
    tx: watch::Sender<Arc<Vec<LoadedProcessor>>>,
    rx: watch::Receiver<Arc<Vec<LoadedProcessor>>>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ProcessorManifest {
    pub name: String,
    pub stages: Vec<String>,
    pub status: String,
    #[serde(default)]
    pub config: Value,
    #[serde(default)]
    pub artifact_key: Option<String>,
    #[serde(default)]
    pub artifact_sha256: Option<String>,
    #[serde(default)]
    pub configs: BTreeMap<String, Value>,
    #[serde(default)]
    pub artifacts: BTreeMap<String, ProcessorArtifact>,
    #[serde(default)]
    pub error: Option<String>,
    #[serde(default)]
    pub updated_at: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ProcessorArtifact {
    pub key: String,
    pub sha256: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ProcessorIndex {
    pub processors: Vec<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum ProcessorError {
    #[error("S3 error: {0}")]
    S3(String),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("load error: {0}")]
    Load(String),
    #[error("processor {name} failed: {message}")]
    Transform { name: String, message: String },
}

#[derive(Clone)]
pub struct ProcessorSyncConfig {
    pub bucket: String,
    pub prefix: String,
    pub interval: Duration,
    pub root: PathBuf,
    pub stage: String,
}

impl ProcessorRuntime {
    pub fn identity() -> Self {
        let (tx, rx) = watch::channel(Arc::new(Vec::new()));
        Self { tx, rx }
    }

    pub fn start(s3: S3Client, cfg: ProcessorSyncConfig) -> Self {
        let runtime = Self::identity();
        let tx = runtime.tx.clone();
        tokio::spawn(async move {
            loop {
                match load_processors(&s3, &cfg).await {
                    Ok(processors) => {
                        let count = processors.len();
                        if tx.send(Arc::new(processors)).is_ok() {
                            info!(stage = cfg.stage, count, "processor registry synced");
                        }
                    }
                    Err(err) => {
                        error!(stage = cfg.stage, error = %err, "processor registry sync failed");
                    }
                }
                tokio::time::sleep(cfg.interval).await;
            }
        });
        runtime
    }

    pub fn has_processors(&self) -> bool {
        !self.rx.borrow().is_empty()
    }

    pub fn transform_ndjson(&self, input: &[u8]) -> Result<Vec<u8>, ProcessorError> {
        let processors = self.rx.borrow().clone();
        let mut current = input.to_vec();
        for processor in processors.iter() {
            current = processor.transform(&current)?;
        }
        Ok(current)
    }
}

#[derive(Clone)]
struct LoadedProcessor {
    name: String,
    config_json: Vec<u8>,
    inner: Arc<ProcessorLibrary>,
}

impl std::fmt::Debug for LoadedProcessor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LoadedProcessor")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

struct ProcessorLibrary {
    _library: Library,
    transform: TransformFn,
    free: FreeFn,
}

type TransformFn =
    unsafe extern "C" fn(NtBytes, NtBytes, *mut NtOwnedBytes, *mut NtOwnedBytes) -> c_int;
type FreeFn = unsafe extern "C" fn(NtOwnedBytes);

unsafe impl Send for ProcessorLibrary {}
unsafe impl Sync for ProcessorLibrary {}

#[repr(C)]
#[derive(Clone, Copy)]
struct NtBytes {
    ptr: *const u8,
    len: usize,
}

#[repr(C)]
#[derive(Clone, Copy)]
struct NtOwnedBytes {
    ptr: *mut u8,
    len: usize,
    cap: usize,
}

impl LoadedProcessor {
    fn transform(&self, input: &[u8]) -> Result<Vec<u8>, ProcessorError> {
        let input_bytes = NtBytes {
            ptr: input.as_ptr(),
            len: input.len(),
        };
        let config_bytes = NtBytes {
            ptr: self.config_json.as_ptr(),
            len: self.config_json.len(),
        };
        let mut output = NtOwnedBytes {
            ptr: std::ptr::null_mut(),
            len: 0,
            cap: 0,
        };
        let mut error = NtOwnedBytes {
            ptr: std::ptr::null_mut(),
            len: 0,
            cap: 0,
        };

        let code =
            unsafe { (self.inner.transform)(input_bytes, config_bytes, &mut output, &mut error) };
        if code == 0 {
            return unsafe { take_owned(&self.inner, output) };
        }

        let message = unsafe { take_owned(&self.inner, error) }
            .ok()
            .and_then(|bytes| String::from_utf8(bytes).ok())
            .unwrap_or_else(|| format!("processor returned {code}"));
        Err(ProcessorError::Transform {
            name: self.name.clone(),
            message,
        })
    }
}

unsafe fn take_owned(
    library: &ProcessorLibrary,
    value: NtOwnedBytes,
) -> Result<Vec<u8>, ProcessorError> {
    if value.ptr.is_null() {
        return Ok(Vec::new());
    }
    let bytes = unsafe { std::slice::from_raw_parts(value.ptr, value.len) }.to_vec();
    unsafe { (library.free)(value) };
    Ok(bytes)
}

async fn load_processors(
    s3: &S3Client,
    cfg: &ProcessorSyncConfig,
) -> Result<Vec<LoadedProcessor>, ProcessorError> {
    let index_key = processor_key(&cfg.prefix, "index.json");
    let index = match get_json::<ProcessorIndex>(s3, &cfg.bucket, &index_key).await {
        Ok(index) => index,
        Err(err) => {
            warn!(error = %err, "processor index unavailable; using identity processors");
            return Ok(Vec::new());
        }
    };

    let mut processors = Vec::new();
    let mut names = index.processors;
    names.sort();
    names.dedup();

    for name in names {
        let key = processor_key(&cfg.prefix, &format!("{name}/manifest.json"));
        let manifest = match get_json::<ProcessorManifest>(s3, &cfg.bucket, &key).await {
            Ok(manifest) => manifest,
            Err(err) => {
                warn!(name, error = %err, "failed to load processor manifest");
                continue;
            }
        };
        if manifest.status != "ready" || !manifest.stages.iter().any(|stage| stage == &cfg.stage) {
            continue;
        }
        let Some(artifact) = artifact_for_stage(&manifest, &cfg.stage) else {
            continue;
        };
        let processor = load_processor(s3, cfg, manifest, &artifact.key, &artifact.sha256).await?;
        processors.push(processor);
    }

    Ok(processors)
}

fn artifact_for_stage(manifest: &ProcessorManifest, stage: &str) -> Option<ProcessorArtifact> {
    if let Some(artifact) = manifest.artifacts.get(stage) {
        return Some(artifact.clone());
    }
    let key = manifest.artifact_key.clone()?;
    let sha256 = manifest.artifact_sha256.clone()?;
    Some(ProcessorArtifact { key, sha256 })
}

async fn load_processor(
    s3: &S3Client,
    cfg: &ProcessorSyncConfig,
    manifest: ProcessorManifest,
    artifact_key: &str,
    expected_sha: &str,
) -> Result<LoadedProcessor, ProcessorError> {
    tokio::fs::create_dir_all(&cfg.root).await?;
    let bytes = get_object(s3, &cfg.bucket, artifact_key).await?;
    let actual_sha = hex::encode(Sha256::digest(&bytes));
    if actual_sha != expected_sha {
        return Err(ProcessorError::Load(format!(
            "sha256 mismatch for {artifact_key}: expected {expected_sha}, got {actual_sha}"
        )));
    }
    let path = processor_path(&cfg.root, &cfg.stage, &manifest.name, &actual_sha);
    if tokio::fs::metadata(&path).await.is_err() {
        let tmp = path.with_extension(format!("{}.tmp", std::process::id()));
        tokio::fs::write(&tmp, bytes).await?;
        tokio::fs::rename(&tmp, &path).await?;
    }

    let library =
        unsafe { Library::new(&path) }.map_err(|err| ProcessorError::Load(err.to_string()))?;
    let transform = {
        let symbol: Symbol<TransformFn> = unsafe { library.get(b"nanotrace_transform_v1") }
            .map_err(|err| ProcessorError::Load(err.to_string()))?;
        *symbol
    };
    let free = {
        let symbol: Symbol<FreeFn> = unsafe { library.get(b"nanotrace_free_v1") }
            .map_err(|err| ProcessorError::Load(err.to_string()))?;
        *symbol
    };

    let config = manifest
        .configs
        .get(&cfg.stage)
        .cloned()
        .unwrap_or(manifest.config);
    let config_json = serde_json::to_vec(&config)?;
    Ok(LoadedProcessor {
        name: manifest.name,
        config_json,
        inner: Arc::new(ProcessorLibrary {
            _library: library,
            transform,
            free,
        }),
    })
}

fn processor_path(root: &std::path::Path, stage: &str, name: &str, sha: &str) -> PathBuf {
    let short_sha = sha.get(..16).unwrap_or(sha);
    root.join(format!("{stage}-{name}-{short_sha}.so"))
}

fn processor_key(prefix: &str, key: &str) -> String {
    let prefix = prefix.trim_matches('/');
    if prefix.is_empty() {
        key.to_string()
    } else {
        format!("{prefix}/{key}")
    }
}

async fn get_json<T>(s3: &S3Client, bucket: &str, key: &str) -> Result<T, ProcessorError>
where
    T: for<'de> Deserialize<'de>,
{
    let bytes = get_object(s3, bucket, key).await?;
    Ok(serde_json::from_slice(&bytes)?)
}

async fn get_object(s3: &S3Client, bucket: &str, key: &str) -> Result<Vec<u8>, ProcessorError> {
    let output = s3
        .get_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .map_err(|err| ProcessorError::S3(err.to_string()))?;
    let bytes = output
        .body
        .collect()
        .await
        .map_err(|err| ProcessorError::S3(err.to_string()))?
        .into_bytes();
    Ok(bytes.to_vec())
}
