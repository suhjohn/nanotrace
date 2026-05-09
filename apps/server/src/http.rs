use std::sync::Arc;

use axum::{
    Json, Router,
    body::{Body, to_bytes},
    extract::{Path, State},
    http::{HeaderMap, Request, StatusCode, header::AUTHORIZATION},
    response::{IntoResponse, Response},
    routing::{get, post, put},
};
use serde::Serialize;
use std::time::Instant;
use tower_http::limit::RequestBodyLimitLayer;

use crate::{
    config::Config,
    event_log::{EventLogError, EventLogWriter, WriteReceipt},
    processors::{ProcessorListResponse, ProcessorStore, ProcessorStoreError, PutProcessorRequest},
    read::{QueryRequest, ReadError, ReadStore},
};

#[derive(Clone)]
pub struct AppState {
    pub cfg: Arc<Config>,
    pub processors: Arc<ProcessorStore>,
    pub read: Arc<ReadStore>,
    pub writer: Arc<EventLogWriter>,
}

pub fn router(state: AppState) -> Router {
    let limit = state.cfg.max_request_bytes;

    Router::new()
        .route("/events", post(post_events))
        .route("/events/{event_id}", get(get_event))
        .route("/processors", get(list_processors))
        .route(
            "/processors/{name}",
            put(put_processor).delete(delete_processor),
        )
        .route("/query", post(post_query))
        .route("/healthz", get(healthz))
        .route("/metrics", get(metrics))
        .route("/readyz", get(readyz))
        .layer(RequestBodyLimitLayer::new(limit))
        .with_state(state)
}

async fn post_events(
    State(state): State<AppState>,
    request: Request<Body>,
) -> Result<Json<impl Serialize>, ApiError> {
    let (parts, body) = request.into_parts();
    let headers = parts.headers;
    authorize(&state.cfg, &headers)?;
    let read_started_at = Instant::now();
    let body = to_bytes(body, state.cfg.max_request_bytes)
        .await
        .map_err(|_| ApiError::PayloadTooLarge)?;
    state.writer.record_request_read(read_started_at.elapsed());
    let receipts = state.writer.append_bytes(&body).await?;
    let response = if receipts.is_batch {
        if state.cfg.compact_batch_receipts {
            PostEventsResponse::CompactBatch(CompactBatchReceipt::from_receipts(&receipts.receipts))
        } else {
            PostEventsResponse::Batch(receipts.receipts)
        }
    } else {
        let receipt = receipts
            .receipts
            .into_iter()
            .next()
            .ok_or(ApiError::EmptyWrite)?;
        PostEventsResponse::Single(receipt)
    };
    Ok(Json(response))
}

async fn post_query(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<QueryRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    authorize(&state.cfg, &headers)?;
    let response = state.read.query(request).await?;
    Ok(Json(response))
}

async fn list_processors(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<ProcessorListResponse>, ApiError> {
    authorize(&state.cfg, &headers)?;
    let processors = state.processors.list().await?;
    Ok(Json(ProcessorListResponse { processors }))
}

async fn put_processor(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(name): Path<String>,
    Json(request): Json<PutProcessorRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    authorize(&state.cfg, &headers)?;
    let manifest = state.processors.put(&name, request).await?;
    Ok(Json(serde_json::json!({ "processor": manifest })))
}

async fn delete_processor(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(name): Path<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    authorize(&state.cfg, &headers)?;
    let manifest = state.processors.delete(&name).await?;
    Ok(Json(serde_json::json!({ "processor": manifest })))
}

async fn get_event(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(event_id): Path<String>,
) -> Result<Response, ApiError> {
    authorize(&state.cfg, &headers)?;
    let bytes = state.read.event_bytes(&event_id).await?;
    Ok(([("content-type", "application/json")], Body::from(bytes)).into_response())
}

async fn metrics(State(state): State<AppState>) -> Response {
    (
        [("content-type", "text/plain; version=0.0.4; charset=utf-8")],
        state.writer.metrics_text(),
    )
        .into_response()
}

async fn healthz() -> Json<serde_json::Value> {
    Json(serde_json::json!({ "ok": true }))
}

async fn readyz(State(state): State<AppState>) -> Result<Json<serde_json::Value>, ApiError> {
    if state.cfg.s3_bucket.is_none() {
        return Err(ApiError::Unavailable("S3 bucket is not configured"));
    }
    Ok(Json(serde_json::json!({ "ok": true })))
}

fn authorize(cfg: &Config, headers: &HeaderMap) -> Result<(), ApiError> {
    let expected = format!("Bearer {}", cfg.secret_key);
    let actual = headers
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();

    if actual == expected {
        Ok(())
    } else {
        Err(ApiError::Unauthorized)
    }
}

#[derive(Debug)]
pub enum ApiError {
    Unauthorized,
    PayloadTooLarge,
    EmptyWrite,
    Unavailable(&'static str),
    EventLog(crate::event_log::EventLogError),
    Processor(crate::processors::ProcessorStoreError),
    Read(crate::read::ReadError),
}

#[derive(Debug, Serialize)]
#[serde(untagged)]
enum PostEventsResponse {
    Single(WriteReceipt),
    Batch(Vec<WriteReceipt>),
    CompactBatch(CompactBatchReceipt),
}

#[derive(Debug, Serialize)]
struct CompactBatchReceipt {
    accepted: usize,
    source_file: Option<String>,
    source_offset: Option<u64>,
    source_length: u64,
}

impl CompactBatchReceipt {
    fn from_receipts(receipts: &[WriteReceipt]) -> Self {
        let source_file = receipts.first().map(|receipt| receipt.source_file.clone());
        let source_offset = receipts.first().map(|receipt| receipt.source_offset);
        let source_length = receipts
            .iter()
            .map(|receipt| u64::from(receipt.source_length))
            .sum();

        Self {
            accepted: receipts.len(),
            source_file,
            source_offset,
            source_length,
        }
    }
}

impl From<crate::event_log::EventLogError> for ApiError {
    fn from(value: crate::event_log::EventLogError) -> Self {
        Self::EventLog(value)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            Self::Unauthorized => (StatusCode::UNAUTHORIZED, "unauthorized".to_string()),
            Self::PayloadTooLarge => (
                StatusCode::PAYLOAD_TOO_LARGE,
                "request body is too large".to_string(),
            ),
            Self::EmptyWrite => (StatusCode::INTERNAL_SERVER_ERROR, "empty write".to_string()),
            Self::Unavailable(message) => (StatusCode::SERVICE_UNAVAILABLE, message.to_string()),
            Self::EventLog(EventLogError::Event(err)) => (StatusCode::BAD_REQUEST, err.to_string()),
            Self::EventLog(err) => (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()),
            Self::Processor(err @ ProcessorStoreError::InvalidName)
            | Self::Processor(err @ ProcessorStoreError::MissingStage)
            | Self::Processor(err @ ProcessorStoreError::MissingCode(_))
            | Self::Processor(err @ ProcessorStoreError::Json(_)) => {
                (StatusCode::BAD_REQUEST, err.to_string())
            }
            Self::Processor(ProcessorStoreError::S3NotConfigured) => (
                StatusCode::SERVICE_UNAVAILABLE,
                "S3 bucket is not configured".to_string(),
            ),
            Self::Processor(err) => (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()),
            Self::Read(ReadError::InvalidQuery(err)) => (StatusCode::BAD_REQUEST, err),
            Self::Read(ReadError::ClickHouseResponse { status, body }) => {
                let status = if status.is_client_error() {
                    StatusCode::BAD_REQUEST
                } else {
                    StatusCode::BAD_GATEWAY
                };
                (status, body)
            }
            Self::Read(ReadError::NotFound) => (StatusCode::NOT_FOUND, "not_found".to_string()),
            Self::Read(ReadError::ClickHouseNotConfigured) => (
                StatusCode::SERVICE_UNAVAILABLE,
                "ClickHouse is not configured".to_string(),
            ),
            Self::Read(ReadError::S3NotConfigured) => (
                StatusCode::SERVICE_UNAVAILABLE,
                "S3 bucket is not configured".to_string(),
            ),
            Self::Read(err) => (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()),
        };

        (status, Json(serde_json::json!({ "error": message }))).into_response()
    }
}

impl From<crate::read::ReadError> for ApiError {
    fn from(value: crate::read::ReadError) -> Self {
        Self::Read(value)
    }
}

impl From<crate::processors::ProcessorStoreError> for ApiError {
    fn from(value: crate::processors::ProcessorStoreError) -> Self {
        Self::Processor(value)
    }
}
