mod config;
mod read;

use std::{net::SocketAddr, sync::Arc};

use axum::{
    Json, Router,
    body::Body,
    extract::{Path, State},
    http::{
        HeaderMap, HeaderValue, Method, StatusCode,
        header::{AUTHORIZATION, CONTENT_TYPE},
    },
    response::{IntoResponse, Response},
    routing::{get, post},
};
use config::Config;
use nanotrace_auth::{AuthError, AuthIdentity, AuthRole, AuthStore, AuthType};
use read::{QueryRequest, ReadError, ReadStore};
use tokio::net::TcpListener;
use tower_http::{
    cors::{AllowOrigin, CorsLayer},
    limit::RequestBodyLimitLayer,
};
use tracing::info;
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Clone)]
struct AppState {
    cfg: Arc<Config>,
    auth: Option<Arc<AuthStore>>,
    read: Arc<ReadStore>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .with(tracing_subscriber::fmt::layer())
        .init();

    let cfg = Arc::new(Config::from_env()?);
    let auth = nanotrace_auth::AuthStore::connect(cfg.auth.clone())
        .await?
        .map(Arc::new);
    let aws_config = aws_config::load_from_env().await;
    let s3 = aws_sdk_s3::Client::new(&aws_config);
    let state = AppState {
        cfg: cfg.clone(),
        auth,
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

    let router = Router::new()
        .route("/internal/query", post(post_query))
        .route("/internal/events/{event_id}", get(get_event))
        .route("/query", post(post_query))
        .route("/events/{event_id}", get(get_event))
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz));

    let router = router
        .layer(RequestBodyLimitLayer::new(limit))
        .with_state(state.clone());

    match cors_layer(&state.cfg.cors_allowed_origins) {
        Some(layer) => router.layer(layer),
        None => router,
    }
}

fn cors_layer(origins: &[String]) -> Option<CorsLayer> {
    let allowed_origins = origins
        .iter()
        .filter_map(|origin| HeaderValue::from_str(origin).ok())
        .collect::<Vec<_>>();
    if allowed_origins.is_empty() {
        return None;
    }

    Some(
        CorsLayer::new()
            .allow_origin(AllowOrigin::list(allowed_origins))
            .allow_methods([Method::GET, Method::POST])
            .allow_headers([
                AUTHORIZATION,
                CONTENT_TYPE,
                axum::http::header::HeaderName::from_static("x-nanotrace-organization-id"),
                axum::http::header::HeaderName::from_static("x-nanotrace-internal-organization-id"),
                axum::http::header::HeaderName::from_static("x-nanotrace-internal-secret"),
            ])
            .allow_credentials(true),
    )
}

async fn post_query(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<QueryRequest>,
) -> Result<Response, ApiError> {
    let identity = authorize(&state, &headers).await?;
    require_scope(&identity, "query:read")?;
    let body = serde_json::to_vec(&request).map_err(|err| ApiError::BadGateway(err.to_string()))?;
    if let Some(response) = proxy_to_data_plane(
        &state,
        &identity,
        Method::POST,
        "/internal/query",
        Some(body),
    )
    .await?
    {
        return Ok(response);
    }
    Ok(Json(state.read.query(request, &identity.tenant_id).await?).into_response())
}

async fn get_event(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(event_id): Path<String>,
) -> Result<Response, ApiError> {
    let identity = authorize(&state, &headers).await?;
    require_scope(&identity, "query:read")?;
    if let Some(response) = proxy_to_data_plane(
        &state,
        &identity,
        Method::GET,
        &format!("/internal/events/{}", event_id),
        None,
    )
    .await?
    {
        return Ok(response);
    }
    let bytes = state
        .read
        .event_bytes(&event_id, &identity.tenant_id)
        .await?;
    Ok(([("content-type", "application/json")], bytes).into_response())
}

fn require_scope(identity: &AuthIdentity, scope: &str) -> Result<(), ApiError> {
    if nanotrace_auth::has_scope(identity, scope) {
        Ok(())
    } else {
        Err(ApiError::Auth(AuthError::Forbidden))
    }
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

async fn proxy_to_data_plane(
    state: &AppState,
    identity: &AuthIdentity,
    method: Method,
    path: &str,
    body: Option<Vec<u8>>,
) -> Result<Option<Response>, ApiError> {
    if state.cfg.data_plane_organization_id.is_some() || identity.subject == "internal:gateway" {
        return Ok(None);
    }
    let base_url = state
        .cfg
        .shared_data_plane_query_url
        .as_deref()
        .unwrap_or("");
    let internal_secret = state.cfg.shared_data_plane_secret.as_deref().unwrap_or("");
    if base_url.is_empty() || is_local_data_plane_base(state, base_url) {
        return Ok(None);
    }
    if internal_secret.is_empty() {
        return Ok(None);
    }
    let url = format!(
        "{}/{}",
        base_url.trim_end_matches('/'),
        path.trim_start_matches('/')
    );
    let method = method
        .as_str()
        .parse::<reqwest::Method>()
        .map_err(|err| ApiError::BadGateway(err.to_string()))?;
    let mut request = reqwest::Client::new()
        .request(method, url)
        .header("x-nanotrace-internal-organization-id", &identity.tenant_id)
        .header("x-nanotrace-internal-secret", internal_secret);
    if let Some(body) = body {
        request = request
            .header("content-type", "application/json")
            .body(body);
    }
    let response = request
        .send()
        .await
        .map_err(|err| ApiError::BadGateway(err.to_string()))?;
    let status = response.status();
    let content_type = response
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .map(str::to_string);
    let body = response
        .bytes()
        .await
        .map_err(|err| ApiError::BadGateway(err.to_string()))?;
    let mut builder = Response::builder().status(status);
    if let Some(content_type) = content_type {
        builder = builder.header(CONTENT_TYPE, content_type);
    }
    builder
        .body(Body::from(body))
        .map(Some)
        .map_err(|err| ApiError::BadGateway(err.to_string()))
}

fn is_local_data_plane_base(state: &AppState, base_url: &str) -> bool {
    state
        .cfg
        .auth
        .public_base_url
        .as_deref()
        .is_some_and(|public_base_url| {
            public_base_url.trim_end_matches('/') == base_url.trim_end_matches('/')
        })
}

async fn authorize(state: &AppState, headers: &HeaderMap) -> Result<AuthIdentity, ApiError> {
    if let Some(identity) = authorize_internal(state, headers) {
        return Ok(identity);
    }
    let identity = nanotrace_auth::authorize_headers(headers, state.auth.as_deref()).await?;
    if state
        .cfg
        .data_plane_organization_id
        .as_deref()
        .is_some_and(|organization_id| organization_id != identity.tenant_id)
    {
        return Err(ApiError::Auth(AuthError::Forbidden));
    }
    Ok(identity)
}

fn authorize_internal(state: &AppState, headers: &HeaderMap) -> Option<AuthIdentity> {
    let configured_secret = state.cfg.data_plane_shared_secret.as_deref()?;
    let provided_secret = headers
        .get("x-nanotrace-internal-secret")
        .and_then(|value| value.to_str().ok())?;
    if configured_secret != provided_secret {
        return None;
    }
    let organization_id = headers
        .get("x-nanotrace-internal-organization-id")
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())?;
    if state
        .cfg
        .data_plane_organization_id
        .as_deref()
        .is_some_and(|expected| expected != organization_id)
    {
        return None;
    }
    Some(AuthIdentity {
        auth_type: AuthType::ApiKey,
        subject: "internal:gateway".to_string(),
        email: None,
        name: Some("internal gateway".to_string()),
        role: AuthRole::Service,
        tenant_id: organization_id.to_string(),
        organization_id: organization_id.to_string(),
        organization_name: organization_id.to_string(),
        scopes: vec!["query:read".to_string()],
    })
}

#[derive(Debug)]
enum ApiError {
    Unauthorized,
    BadGateway(String),
    Unavailable(&'static str),
    Read(ReadError),
    Auth(AuthError),
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
            Self::BadGateway(message) => (StatusCode::BAD_GATEWAY, message),
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
            Self::Auth(err) => (err.status_code(), err.to_string()),
        };
        (status, Json(serde_json::json!({ "error": message }))).into_response()
    }
}

impl From<AuthError> for ApiError {
    fn from(value: AuthError) -> Self {
        match value {
            AuthError::Unauthorized => Self::Unauthorized,
            other => Self::Auth(other),
        }
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
