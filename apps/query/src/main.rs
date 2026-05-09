mod config;
mod read;

use std::{net::SocketAddr, sync::Arc};

use axum::{
    Json, Router,
    extract::{Path, State},
    http::{HeaderMap, StatusCode, header::AUTHORIZATION},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use config::Config;
use read::{QueryRequest, ReadError, ReadStore};
use tokio::net::TcpListener;
use tower_http::limit::RequestBodyLimitLayer;
use tracing::info;
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Clone)]
struct AppState {
    cfg: Arc<Config>,
    read: Arc<ReadStore>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .with(tracing_subscriber::fmt::layer())
        .init();

    let cfg = Arc::new(Config::from_env()?);
    let aws_config = aws_config::load_from_env().await;
    let s3 = aws_sdk_s3::Client::new(&aws_config);
    let state = AppState {
        cfg: cfg.clone(),
        read: Arc::new(ReadStore::new(cfg.clone(), s3)),
    };
    let address = SocketAddr::from(([0, 0, 0, 0], cfg.port));
    let listener = TcpListener::bind(address).await?;
    info!(%address, "nanotrace query starting");

    axum::serve(listener, router(state))
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    Ok(())
}

fn router(state: AppState) -> Router {
    let limit = state.cfg.max_request_bytes;
    Router::new()
        .route("/query", post(post_query))
        .route("/events/{event_id}", get(get_event))
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .layer(RequestBodyLimitLayer::new(limit))
        .with_state(state)
}

async fn post_query(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<QueryRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    authorize(&state.cfg, &headers)?;
    Ok(Json(state.read.query(request).await?))
}

async fn get_event(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(event_id): Path<String>,
) -> Result<Response, ApiError> {
    authorize(&state.cfg, &headers)?;
    let bytes = state.read.event_bytes(&event_id).await?;
    Ok(([("content-type", "application/json")], bytes).into_response())
}

async fn healthz() -> Json<serde_json::Value> {
    Json(serde_json::json!({ "ok": true }))
}

async fn readyz(State(state): State<AppState>) -> Result<Json<serde_json::Value>, ApiError> {
    if state.cfg.s3_bucket.is_none() {
        return Err(ApiError::Unavailable("S3 bucket is not configured"));
    }
    if state.cfg.clickhouse_url.is_none() {
        return Err(ApiError::Unavailable("ClickHouse is not configured"));
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
enum ApiError {
    Unauthorized,
    Unavailable(&'static str),
    Read(ReadError),
}

impl From<ReadError> for ApiError {
    fn from(value: ReadError) -> Self {
        Self::Read(value)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            Self::Unauthorized => (StatusCode::UNAUTHORIZED, "unauthorized".to_string()),
            Self::Unavailable(message) => (StatusCode::SERVICE_UNAVAILABLE, message.to_string()),
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

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl-C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}
