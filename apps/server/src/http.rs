use std::sync::Arc;

use aws_sdk_sesv2::{
    error::ProvideErrorMetadata,
    types::{Body as EmailBody, Content, Destination, EmailContent, Message},
};
use axum::{
    Json, Router,
    body::{Body, to_bytes},
    extract::{Path, Query, State},
    http::{
        HeaderMap, HeaderValue, Method, Request, StatusCode,
        header::{AUTHORIZATION, CONTENT_TYPE, LOCATION, SET_COOKIE},
    },
    response::{Html, IntoResponse, Response},
    routing::{delete, get, post, put},
};
use chrono::{DateTime, Utc};
use nanotrace_auth::{AuthError, AuthIdentity, AuthRole, AuthStore};
use serde::{Deserialize, Serialize};
use std::time::Instant;
use tower_http::{
    cors::{AllowOrigin, CorsLayer},
    limit::RequestBodyLimitLayer,
    services::{ServeDir, ServeFile},
};

use crate::{
    config::Config,
    event_log::{EventLogError, EventLogWriter, WriteReceipt},
    facets::{
        FacetBackfillListResponse, FacetError, FacetListResponse, FacetStore, PutFacetRequest,
    },
    processors::{ProcessorListResponse, ProcessorStore, ProcessorStoreError, PutProcessorRequest},
    read::{QueryRequest, ReadError, ReadStore},
};

#[derive(Clone)]
pub struct AppState {
    pub cfg: Arc<Config>,
    pub auth: Option<Arc<AuthStore>>,
    pub facets: Arc<FacetStore>,
    pub processors: Arc<ProcessorStore>,
    pub read: Arc<ReadStore>,
    pub ses: aws_sdk_sesv2::Client,
    pub writer: Arc<EventLogWriter>,
}

pub fn router(state: AppState) -> Router {
    let limit = state.cfg.max_request_bytes;

    let ui_index = state.cfg.ui_dir.join("index.html");
    let ui_assets =
        ServeDir::new(state.cfg.ui_dir.clone()).not_found_service(ServeFile::new(ui_index));

    let router = Router::new()
        .route("/events", post(post_events))
        .route("/events/{event_id}", get(get_event))
        .route("/facets", get(list_facets).post(put_facet))
        .route("/facets/backfills", get(list_facet_backfills))
        .route("/facets/backfills/{job_id}", get(get_facet_backfill))
        .route("/facets/{path}/backfill", post(backfill_facet))
        .route("/facets/{path}", delete(delete_facet))
        .route("/processors", get(list_processors))
        .route(
            "/processors/{name}",
            put(put_processor).delete(delete_processor),
        )
        .route("/query", post(post_query))
        .route("/auth/login", get(auth_login_form).post(auth_login))
        .route("/auth/callback", get(auth_callback))
        .route("/auth/logout", post(auth_logout))
        .route("/auth/me", get(auth_me))
        .route("/api-keys", get(list_api_keys).post(create_api_key))
        .route("/api-keys/{id}", delete(revoke_api_key))
        .route("/healthz", get(healthz))
        .route("/metrics", get(metrics))
        .route("/readyz", get(readyz))
        .fallback_service(ui_assets)
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
            .allow_methods([Method::GET, Method::POST, Method::PUT, Method::DELETE])
            .allow_headers([AUTHORIZATION, CONTENT_TYPE])
            .allow_credentials(true),
    )
}

async fn post_events(
    State(state): State<AppState>,
    request: Request<Body>,
) -> Result<Json<impl Serialize>, ApiError> {
    let (parts, body) = request.into_parts();
    let headers = parts.headers;
    authorize_service(&state, &headers).await?;
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
    authorize_any(&state, &headers).await?;
    let response = state.read.query(request).await?;
    Ok(Json(response))
}

async fn list_processors(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<ProcessorListResponse>, ApiError> {
    authorize_admin(&state, &headers).await?;
    let processors = state.processors.list().await?;
    Ok(Json(ProcessorListResponse { processors }))
}

async fn list_facets(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<FacetListResponse>, ApiError> {
    authorize_any(&state, &headers).await?;
    let facets = state.facets.list().await?;
    Ok(Json(FacetListResponse { facets }))
}

async fn put_facet(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<PutFacetRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    authorize_admin(&state, &headers).await?;
    let facet = state.facets.put(request).await?;
    Ok(Json(serde_json::json!({ "facet": facet })))
}

async fn delete_facet(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(path): Path<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    authorize_admin(&state, &headers).await?;
    let facet = state.facets.delete(&path).await?;
    Ok(Json(serde_json::json!({ "facet": facet })))
}

async fn backfill_facet(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(path): Path<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    authorize_admin(&state, &headers).await?;
    let backfill = state.facets.enqueue_backfill(&path).await?;
    Ok(Json(serde_json::json!({ "backfill": backfill })))
}

async fn list_facet_backfills(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<FacetBackfillListResponse>, ApiError> {
    authorize_admin(&state, &headers).await?;
    let backfills = state.facets.backfill_list().await?;
    Ok(Json(FacetBackfillListResponse { backfills }))
}

async fn get_facet_backfill(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(job_id): Path<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    authorize_admin(&state, &headers).await?;
    let backfill = state.facets.backfill_status(&job_id).await?;
    Ok(Json(serde_json::json!({ "backfill": backfill })))
}

async fn put_processor(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(name): Path<String>,
    Json(request): Json<PutProcessorRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    authorize_admin(&state, &headers).await?;
    let manifest = state.processors.put(&name, request).await?;
    Ok(Json(serde_json::json!({ "processor": manifest })))
}

async fn delete_processor(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(name): Path<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    authorize_admin(&state, &headers).await?;
    let manifest = state.processors.delete(&name).await?;
    Ok(Json(serde_json::json!({ "processor": manifest })))
}

async fn get_event(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(event_id): Path<String>,
) -> Result<Response, ApiError> {
    authorize_any(&state, &headers).await?;
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
        return Err(ApiError::Unavailable(
            "S3 bucket is not configured".to_string(),
        ));
    }
    Ok(Json(serde_json::json!({ "ok": true })))
}

#[derive(Debug, Deserialize)]
struct LoginParams {
    return_to: Option<String>,
}

#[derive(Debug, Deserialize)]
struct LoginRequest {
    email: String,
    return_to: Option<String>,
}

#[derive(Debug, Serialize)]
struct LoginResponse {
    ok: bool,
}

#[derive(Debug, Deserialize)]
struct CallbackParams {
    token: String,
}

#[derive(Debug, Deserialize)]
struct CreateApiKeyRequest {
    name: String,
    role: Option<String>,
    expires_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Serialize)]
struct ApiKeysResponse {
    api_keys: Vec<nanotrace_auth::ApiKeyRecord>,
}

async fn auth_login_form(Query(params): Query<LoginParams>) -> Html<String> {
    let return_to = html_escape(&params.return_to.unwrap_or_else(|| "/".to_string()));
    Html(format!(
        "<!doctype html><html><head><meta charset=\"utf-8\"><title>Nanotrace Login</title></head>\
         <body style=\"font-family:system-ui,sans-serif;background:#000;color:#fff;padding:24px\">\
         <form id=\"login-form\">\
         <input type=\"hidden\" name=\"return_to\" value=\"{return_to}\">\
         <label>Email <input name=\"email\" type=\"email\" autocomplete=\"email\" autofocus required></label>\
         <button type=\"submit\">Send login link</button>\
         </form><p id=\"status\"></p><script>\
         document.getElementById('login-form').addEventListener('submit', async event => {{\
           event.preventDefault();\
           const form = event.currentTarget;\
           const response = await fetch('/auth/login', {{\
             method: 'POST',\
             headers: {{ 'Content-Type': 'application/json' }},\
             body: JSON.stringify({{ email: form.email.value, return_to: form.return_to.value }})\
           }});\
           document.getElementById('status').textContent = response.ok ? 'Check your email.' : await response.text();\
         }});\
         </script></body></html>"
    ))
}

async fn auth_login(
    State(state): State<AppState>,
    Json(request): Json<LoginRequest>,
) -> Result<Json<LoginResponse>, ApiError> {
    let auth = auth_store(&state)?;
    let login = auth
        .start_login(&request.email, request.return_to.as_deref())
        .await?;
    send_login_email(&state, &login.email, &login.login_url).await?;
    Ok(Json(LoginResponse { ok: true }))
}

async fn auth_callback(
    State(state): State<AppState>,
    Query(params): Query<CallbackParams>,
) -> Result<Response, ApiError> {
    let auth = auth_store(&state)?;
    let login = auth.complete_login(&params.token).await?;
    let mut response = StatusCode::FOUND.into_response();
    response.headers_mut().insert(
        LOCATION,
        login
            .return_to
            .parse()
            .map_err(|err| ApiError::Header(format!("invalid return_to header: {err}")))?,
    );
    response.headers_mut().append(
        SET_COOKIE,
        login
            .session_cookie
            .parse()
            .map_err(|err| ApiError::Header(format!("invalid session cookie header: {err}")))?,
    );
    Ok(response)
}

async fn send_login_email(state: &AppState, email: &str, login_url: &str) -> Result<(), ApiError> {
    let from = state.cfg.email_from.as_deref().ok_or_else(|| {
        ApiError::Unavailable("NANOTRACE_EMAIL_FROM is not configured".to_string())
    })?;
    let subject = Content::builder()
        .data("Your Nanotrace login link")
        .charset("UTF-8")
        .build()
        .map_err(|err| ApiError::Email(err.to_string()))?;
    let text = Content::builder()
        .data(format!(
            "Use this link to sign in to Nanotrace:\n\n{login_url}\n\nThis link expires soon and can only be used once."
        ))
        .charset("UTF-8")
        .build()
        .map_err(|err| ApiError::Email(err.to_string()))?;
    let body = EmailBody::builder().text(text).build();
    let message = Message::builder().subject(subject).body(body).build();
    let content = EmailContent::builder().simple(message).build();
    let destination = Destination::builder().to_addresses(email).build();

    state
        .ses
        .send_email()
        .from_email_address(from)
        .destination(destination)
        .content(content)
        .send()
        .await
        .map_err(|err| {
            let code = err.code().unwrap_or("SesSendError");
            let message = err.message().unwrap_or("failed to send login email");
            ApiError::Email(format!("{code}: {message}"))
        })?;
    Ok(())
}

fn html_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('"', "&quot;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

async fn auth_logout(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let Some(auth) = state.auth.as_deref() else {
        return Ok(Json(serde_json::json!({ "ok": true })).into_response());
    };
    let cookie = auth.logout(&headers).await?;
    let mut response = Json(serde_json::json!({ "ok": true })).into_response();
    response.headers_mut().append(
        SET_COOKIE,
        cookie
            .parse()
            .map_err(|err| ApiError::Header(format!("invalid logout cookie header: {err}")))?,
    );
    Ok(response)
}

async fn auth_me(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<AuthIdentity>, ApiError> {
    Ok(Json(authorize_any(&state, &headers).await?))
}

async fn list_api_keys(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<ApiKeysResponse>, ApiError> {
    authorize_admin(&state, &headers).await?;
    let api_keys = auth_store(&state)?.list_api_keys().await?;
    Ok(Json(ApiKeysResponse { api_keys }))
}

async fn create_api_key(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<CreateApiKeyRequest>,
) -> Result<Json<nanotrace_auth::CreatedApiKey>, ApiError> {
    let identity = authorize_admin(&state, &headers).await?;
    let role = parse_requested_role(request.role.as_deref())?;
    let created = auth_store(&state)?
        .create_api_key(&request.name, role, &identity.subject, request.expires_at)
        .await?;
    Ok(Json(created))
}

async fn revoke_api_key(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<i64>,
) -> Result<Json<serde_json::Value>, ApiError> {
    authorize_admin(&state, &headers).await?;
    let api_key = auth_store(&state)?.revoke_api_key(id).await?;
    Ok(Json(serde_json::json!({ "api_key": api_key })))
}

fn auth_store(state: &AppState) -> Result<&AuthStore, ApiError> {
    state.auth.as_deref().ok_or(ApiError::AuthNotConfigured)
}

async fn authorize_any(state: &AppState, headers: &HeaderMap) -> Result<AuthIdentity, ApiError> {
    Ok(nanotrace_auth::authorize_headers(headers, state.auth.as_deref()).await?)
}

async fn authorize_admin(state: &AppState, headers: &HeaderMap) -> Result<AuthIdentity, ApiError> {
    let identity = authorize_any(state, headers).await?;
    if nanotrace_auth::is_admin(&identity) {
        Ok(identity)
    } else {
        Err(ApiError::Forbidden)
    }
}

async fn authorize_service(
    state: &AppState,
    headers: &HeaderMap,
) -> Result<AuthIdentity, ApiError> {
    let identity = authorize_any(state, headers).await?;
    if nanotrace_auth::is_service_or_admin(&identity) {
        Ok(identity)
    } else {
        Err(ApiError::Forbidden)
    }
}

fn parse_requested_role(value: Option<&str>) -> Result<AuthRole, ApiError> {
    match value
        .unwrap_or("service")
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "admin" => Ok(AuthRole::Admin),
        "service" => Ok(AuthRole::Service),
        "viewer" => Ok(AuthRole::Viewer),
        other => Err(ApiError::BadRequest(format!(
            "invalid API key role: {other}"
        ))),
    }
}

#[derive(Debug)]
pub enum ApiError {
    Unauthorized,
    Forbidden,
    AuthNotConfigured,
    BadRequest(String),
    Header(String),
    PayloadTooLarge,
    EmptyWrite,
    Unavailable(String),
    Email(String),
    EventLog(crate::event_log::EventLogError),
    Processor(crate::processors::ProcessorStoreError),
    Facet(crate::facets::FacetError),
    Read(crate::read::ReadError),
    Auth(AuthError),
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
            Self::Forbidden => (StatusCode::FORBIDDEN, "forbidden".to_string()),
            Self::AuthNotConfigured => (
                StatusCode::SERVICE_UNAVAILABLE,
                "auth database is not configured".to_string(),
            ),
            Self::BadRequest(message) => (StatusCode::BAD_REQUEST, message),
            Self::Header(message) => (StatusCode::INTERNAL_SERVER_ERROR, message),
            Self::PayloadTooLarge => (
                StatusCode::PAYLOAD_TOO_LARGE,
                "request body is too large".to_string(),
            ),
            Self::EmptyWrite => (StatusCode::INTERNAL_SERVER_ERROR, "empty write".to_string()),
            Self::Unavailable(message) => (StatusCode::SERVICE_UNAVAILABLE, message),
            Self::Email(message) => (StatusCode::BAD_GATEWAY, message),
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
            Self::Facet(err @ FacetError::InvalidPath)
            | Self::Facet(err @ FacetError::InvalidValueType)
            | Self::Facet(err @ FacetError::BuiltinFacet) => {
                (StatusCode::BAD_REQUEST, err.to_string())
            }
            Self::Facet(FacetError::ClickHouseNotConfigured) => (
                StatusCode::SERVICE_UNAVAILABLE,
                "ClickHouse is not configured".to_string(),
            ),
            Self::Facet(FacetError::ClickHouseResponse { status, body }) => {
                let status = if status.is_client_error() {
                    StatusCode::BAD_REQUEST
                } else {
                    StatusCode::BAD_GATEWAY
                };
                (status, body)
            }
            Self::Facet(err) => (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()),
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

impl From<crate::facets::FacetError> for ApiError {
    fn from(value: crate::facets::FacetError) -> Self {
        Self::Facet(value)
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
