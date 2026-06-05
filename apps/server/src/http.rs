use std::sync::Arc;

use aws_sdk_sesv2::{
    error::ProvideErrorMetadata,
    types::{Body as EmailBody, Content, Destination, EmailContent, Message},
};
use axum::{
    Json, Router,
    body::{Body, to_bytes},
    extract::{MatchedPath, Path, Query, State},
    http::{
        HeaderMap, HeaderValue, Method, Request, StatusCode,
        header::{AUTHORIZATION, CONTENT_TYPE, LOCATION, SET_COOKIE},
    },
    middleware::{self, Next},
    response::{Html, IntoResponse, Response},
    routing::{delete, get, post},
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
    definitions::{
        BackfillRequest, CreateDefinitionRequest, DefinitionGetResponse, DefinitionListResponse,
        DefinitionStore, DefinitionStoreError,
    },
    materializations::{
        BackfillJobListResponse, BackfillJobResponse, CreateBackfillRequest, MaterializationStore,
        MaterializationStoreError,
    },
    metrics::ServerMetrics,
    read::{QueryApiRequest, QueryRecommendationListResponse, ReadError, ReadStore},
};

use utoipa::ToSchema;

#[derive(Clone)]
pub struct AppState {
    pub cfg: Arc<Config>,
    pub auth: Option<Arc<AuthStore>>,
    pub definitions: Arc<DefinitionStore>,
    pub materializations: Arc<MaterializationStore>,
    pub read: Arc<ReadStore>,
    pub raw_ingest: Arc<nanotrace_ingest::RawBatchProducer>,
    pub ses: aws_sdk_sesv2::Client,
    pub metrics: Arc<ServerMetrics>,
}

pub fn router(state: AppState) -> Router {
    let limit = state.cfg.max_request_bytes;

    let router = Router::new()
        .route("/v1/events", post(post_events))
        .route("/v1/events/{event_id}", get(get_event))
        .route(
            "/v1/definitions",
            get(list_definitions).post(create_definition),
        )
        .route(
            "/v1/definitions/{definition_id}",
            get(get_definition).delete(delete_definition),
        )
        .route(
            "/v1/definitions/{definition_id}/backfill",
            post(backfill_definition),
        )
        .route(
            "/v1/definitions/{definition_id}/backfills",
            post(create_definition_backfill),
        )
        .route("/v1/backfills", get(list_backfill_jobs))
        .route("/v1/backfills/{job_id}", get(get_backfill_job))
        .route("/v1/query/recommendations", get(list_query_recommendations))
        .route("/v1/query", post(post_query))
        .route("/auth/login", get(auth_login_form).post(auth_login))
        .route("/auth/callback", get(auth_callback))
        .route("/auth/logout", post(auth_logout))
        .route("/auth/me", get(auth_me))
        .route("/v1/auth/me", get(auth_me))
        .route("/v1/api-keys", get(list_api_keys).post(create_api_key))
        .route("/v1/api-keys/{id}", delete(revoke_api_key))
        .route("/openapi.json", get(openapi_json))
        .route("/healthz", get(healthz))
        .route("/v1/healthz", get(healthz))
        .route("/metrics", get(metrics))
        .route("/readyz", get(readyz))
        .route("/v1/readyz", get(readyz))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            record_http_metrics,
        ))
        .layer(RequestBodyLimitLayer::new(limit))
        .with_state(state.clone());

    let router = match &state.cfg.ui_dir {
        Some(ui_dir) => {
            let ui_index = ui_dir.join("index.html");
            let ui_assets =
                ServeDir::new(ui_dir.clone()).not_found_service(ServeFile::new(ui_index));
            router.fallback_service(ui_assets)
        }
        None => router.fallback(not_found),
    };

    match cors_layer(&state.cfg.cors_allowed_origins) {
        Some(layer) => router.layer(layer),
        None => router,
    }
}

async fn not_found() -> StatusCode {
    StatusCode::NOT_FOUND
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

async fn record_http_metrics(
    State(state): State<AppState>,
    request: Request<Body>,
    next: Next,
) -> Response {
    let method = request.method().as_str().to_string();
    let route = request
        .extensions()
        .get::<MatchedPath>()
        .map(|matched| matched.as_str().to_string())
        .unwrap_or_else(|| "unmatched".to_string());
    let started_at = Instant::now();
    let response = next.run(request).await;
    state
        .metrics
        .record_http(&method, &route, response.status(), started_at.elapsed());
    response
}

#[utoipa::path(
    post,
    path = "/v1/events",
    request_body = serde_json::Value,
    responses((status = 202, description = "Accepted Kafka event batch.", body = KafkaAcceptedResponse)),
    security(("bearerAuth" = [])),
    tag = "Events"
)]
pub(crate) async fn post_events(
    State(state): State<AppState>,
    request: Request<Body>,
) -> Result<Response, ApiError> {
    let (parts, body) = request.into_parts();
    let headers = parts.headers;
    let identity = authorize_service(&state, &headers).await?;
    require_scope(&identity, "ingest:write")?;
    let body = to_bytes(body, state.cfg.max_request_bytes)
        .await
        .map_err(|_| ApiError::PayloadTooLarge)?;
    let content_type = headers
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("application/json");
    let body_len = body.len();
    let produced = state
        .raw_ingest
        .produce_raw_batch(
            &identity.tenant_id,
            &identity.organization_id,
            content_type,
            &body,
        )
        .await?;
    state.metrics.record_ingest_accepted(body_len);
    Ok((
        StatusCode::ACCEPTED,
        Json(KafkaAcceptedResponse {
            accepted: true,
            mode: "kafka",
            topic: produced.topic,
            partition: produced.partition,
            offset: produced.offset,
        }),
    )
        .into_response())
}

#[utoipa::path(
    post,
    path = "/v1/query",
    request_body = serde_json::Value,
    responses((status = 200, description = "Structured query response.", body = serde_json::Value)),
    security(("bearerAuth" = [])),
    tag = "Query"
)]
pub(crate) async fn post_query(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<serde_json::Value>,
) -> Result<Response, ApiError> {
    let identity = authorize_scope(&state, &headers, "query:read").await?;
    let request = serde_json::from_value::<QueryApiRequest>(request)
        .map_err(|err| ApiError::BadRequest(err.to_string()))?;
    let query_type = query_type_name(&request);
    let started_at = Instant::now();
    match state.read.api_query(request, &identity.tenant_id).await {
        Ok(response) => {
            state
                .metrics
                .record_query(query_type, "success", started_at.elapsed());
            Ok(Json(response).into_response())
        }
        Err(err) => {
            state
                .metrics
                .record_query(query_type, "error", started_at.elapsed());
            Err(err.into())
        }
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct QueryRecommendationsParams {
    #[serde(default)]
    limit: Option<u64>,
}

#[utoipa::path(
    get,
    path = "/v1/query/recommendations",
    params(
        ("limit" = Option<u64>, Query, description = "Maximum number of recent query recommendation records to return. Clamped to 1..100.")
    ),
    responses((status = 200, description = "Recent successful queries with planner recommendations.", body = QueryRecommendationListResponse)),
    security(("bearerAuth" = [])),
    tag = "Query"
)]
pub(crate) async fn list_query_recommendations(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(params): Query<QueryRecommendationsParams>,
) -> Result<Json<QueryRecommendationListResponse>, ApiError> {
    let identity = authorize_scope(&state, &headers, "query:read").await?;
    let response = state
        .read
        .recent_query_recommendations(&identity.tenant_id, params.limit.unwrap_or(50))
        .await?;
    Ok(Json(response))
}

#[utoipa::path(
    get,
    path = "/v1/definitions",
    responses((status = 200, description = "Definition list.", body = DefinitionListResponse)),
    security(("bearerAuth" = [])),
    tag = "Definitions"
)]
pub(crate) async fn list_definitions(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<DefinitionListResponse>, ApiError> {
    let identity = authorize_scope(&state, &headers, "query:read").await?;
    let definitions = state.definitions.list(&identity.tenant_id).await?;
    Ok(Json(DefinitionListResponse { definitions }))
}

#[utoipa::path(
    get,
    path = "/v1/definitions/{definition_id}",
    params(("definition_id" = String, Path, description = "Definition id.")),
    responses((status = 200, description = "Definition envelope.", body = DefinitionGetResponse)),
    security(("bearerAuth" = [])),
    tag = "Definitions"
)]
pub(crate) async fn get_definition(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(definition_id): Path<String>,
) -> Result<Json<DefinitionGetResponse>, ApiError> {
    let identity = authorize_scope(&state, &headers, "query:read").await?;
    let definition = state
        .definitions
        .get(&identity.tenant_id, &definition_id)
        .await?;
    Ok(Json(DefinitionGetResponse { definition }))
}

#[utoipa::path(
    post,
    path = "/v1/definitions",
    request_body = CreateDefinitionRequest,
    responses((status = 200, description = "Created definition.", body = crate::definitions::DefinitionMutationResponse)),
    security(("bearerAuth" = [])),
    tag = "Definitions"
)]
pub(crate) async fn create_definition(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<CreateDefinitionRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let identity = authorize_admin_scope(&state, &headers, "definitions:write").await?;
    let response = state
        .definitions
        .create(&identity.tenant_id, request)
        .await?;
    Ok(Json(
        serde_json::to_value(response).map_err(|err| ApiError::BadRequest(err.to_string()))?,
    ))
}

#[utoipa::path(
    delete,
    path = "/v1/definitions/{definition_id}",
    params(("definition_id" = String, Path, description = "Definition id.")),
    responses((status = 200, description = "Deleted definition envelope.", body = serde_json::Value)),
    security(("bearerAuth" = [])),
    tag = "Definitions"
)]
pub(crate) async fn delete_definition(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(definition_id): Path<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let identity = authorize_admin_scope(&state, &headers, "definitions:write").await?;
    let definition = state
        .definitions
        .delete(&identity.tenant_id, &definition_id)
        .await?;
    Ok(Json(serde_json::json!({ "definition": definition })))
}

#[utoipa::path(
    post,
    path = "/v1/definitions/{definition_id}/backfill",
    params(("definition_id" = String, Path, description = "Definition id.")),
    request_body = BackfillRequest,
    responses((status = 200, description = "Backfill response envelope.", body = serde_json::Value)),
    security(("bearerAuth" = [])),
    tag = "Definitions"
)]
pub(crate) async fn backfill_definition(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(definition_id): Path<String>,
    Json(request): Json<BackfillRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let identity = authorize_admin_scope(&state, &headers, "definitions:write").await?;
    let backfill = state
        .definitions
        .backfill(&identity.tenant_id, &definition_id, request)
        .await?;
    Ok(Json(serde_json::json!({ "backfill": backfill })))
}

#[utoipa::path(
    get,
    path = "/v1/backfills",
    responses((status = 200, description = "Backfill jobs.", body = BackfillJobListResponse)),
    security(("bearerAuth" = [])),
    tag = "Backfills"
)]
pub(crate) async fn list_backfill_jobs(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<BackfillJobListResponse>, ApiError> {
    let identity = authorize_scope(&state, &headers, "query:read").await?;
    let backfills = state
        .materializations
        .list_backfills(&identity.tenant_id)
        .await?;
    Ok(Json(BackfillJobListResponse { backfills }))
}

#[utoipa::path(
    post,
    path = "/v1/definitions/{definition_id}/backfills",
    params(("definition_id" = String, Path, description = "Definition id.")),
    request_body = CreateBackfillRequest,
    responses((status = 200, description = "Created backfill job.", body = BackfillJobResponse)),
    security(("bearerAuth" = [])),
    tag = "Backfills"
)]
pub(crate) async fn create_definition_backfill(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(definition_id): Path<String>,
    Json(request): Json<CreateBackfillRequest>,
) -> Result<Json<BackfillJobResponse>, ApiError> {
    let identity = authorize_admin_scope(&state, &headers, "definitions:write").await?;
    let response = state
        .materializations
        .create_backfill(&identity.tenant_id, &definition_id, request)
        .await?;
    state.metrics.record_backfill_created();
    Ok(Json(response))
}

#[utoipa::path(
    get,
    path = "/v1/backfills/{job_id}",
    params(("job_id" = String, Path, description = "Backfill job id.")),
    responses((status = 200, description = "Backfill job with chunks.", body = BackfillJobResponse)),
    security(("bearerAuth" = [])),
    tag = "Backfills"
)]
pub(crate) async fn get_backfill_job(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(job_id): Path<String>,
) -> Result<Json<BackfillJobResponse>, ApiError> {
    let identity = authorize_scope(&state, &headers, "query:read").await?;
    let response = state
        .materializations
        .get_backfill(&identity.tenant_id, &job_id)
        .await?;
    Ok(Json(response))
}

#[utoipa::path(
    get,
    path = "/v1/events/{event_id}",
    params(("event_id" = String, Path, description = "Event id.")),
    responses((status = 200, description = "Raw event JSON.", body = serde_json::Value)),
    security(("bearerAuth" = [])),
    tag = "Events"
)]
pub(crate) async fn get_event(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(event_id): Path<String>,
) -> Result<Response, ApiError> {
    let identity = authorize_scope(&state, &headers, "query:read").await?;
    let bytes = state
        .read
        .event_bytes(&event_id, &identity.tenant_id)
        .await?;
    Ok(([("content-type", "application/json")], Body::from(bytes)).into_response())
}

pub(crate) async fn metrics(State(state): State<AppState>) -> Response {
    (
        [("content-type", "text/plain; version=0.0.4; charset=utf-8")],
        state.metrics.render_prometheus(state.auth.as_deref()),
    )
        .into_response()
}

#[utoipa::path(
    get,
    path = "/healthz",
    responses((status = 200, description = "Liveness response.", body = serde_json::Value)),
    tag = "Health"
)]
pub(crate) async fn healthz() -> Json<serde_json::Value> {
    Json(serde_json::json!({ "ok": true }))
}

#[utoipa::path(
    get,
    path = "/openapi.json",
    responses((status = 200, description = "OpenAPI document.", body = serde_json::Value)),
    tag = "OpenAPI"
)]
pub(crate) async fn openapi_json() -> Json<utoipa::openapi::OpenApi> {
    Json(crate::openapi::spec())
}

#[utoipa::path(
    get,
    path = "/readyz",
    responses((status = 200, description = "Readiness response.", body = serde_json::Value)),
    tag = "Health"
)]
pub(crate) async fn readyz(
    State(state): State<AppState>,
) -> Result<Json<serde_json::Value>, ApiError> {
    Ok(Json(serde_json::json!({
        "ok": true,
        "ingest": "kafka",
        "topic": state.cfg.kafka_ingest_topic
    })))
}

#[derive(Debug, Deserialize)]
struct LoginParams {
    return_to: Option<String>,
}

#[derive(Debug, Deserialize, ToSchema)]
pub(crate) struct LoginRequest {
    email: String,
    return_to: Option<String>,
}

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct LoginResponse {
    ok: bool,
}

#[derive(Debug, Deserialize)]
struct CallbackParams {
    token: String,
}

#[derive(Debug, Deserialize, ToSchema)]
pub(crate) struct CreateApiKeyRequest {
    name: String,
    role: Option<String>,
    #[serde(default)]
    scopes: Vec<String>,
    expires_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct ApiKeysResponse {
    #[schema(value_type = Vec<serde_json::Value>)]
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

#[utoipa::path(
    post,
    path = "/auth/login",
    request_body = LoginRequest,
    responses((status = 200, description = "Login started.", body = LoginResponse)),
    tag = "Auth"
)]
pub(crate) async fn auth_login(
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
    let return_to = callback_return_to(&state.cfg, &login.return_to);
    response.headers_mut().insert(
        LOCATION,
        return_to
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

fn callback_return_to(cfg: &Config, return_to: &str) -> String {
    let return_to = return_to.trim();
    if return_to.starts_with('/')
        && !return_to.starts_with("//")
        && let Some(app_base_url) = cfg.app_base_url.as_deref()
    {
        return format!("{}{}", app_base_url.trim_end_matches('/'), return_to);
    }
    return_to.to_string()
}

async fn send_login_email(state: &AppState, email: &str, login_url: &str) -> Result<(), ApiError> {
    send_text_email(
        state,
        email,
        "Your Nanotrace login link",
        &format!(
            "Use this link to sign in to Nanotrace:\n\n{login_url}\n\nThis link expires soon and can only be used once."
        ),
    )
    .await
}

async fn send_text_email(
    state: &AppState,
    email: &str,
    subject_text: &str,
    text_body: &str,
) -> Result<(), ApiError> {
    let from = state.cfg.email_from.as_deref().ok_or_else(|| {
        ApiError::Unavailable("NANOTRACE_EMAIL_FROM is not configured".to_string())
    })?;
    let subject = Content::builder()
        .data(subject_text)
        .charset("UTF-8")
        .build()
        .map_err(|err| ApiError::Email(err.to_string()))?;
    let text = Content::builder()
        .data(text_body)
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

#[utoipa::path(
    post,
    path = "/auth/logout",
    responses((status = 200, description = "Logout response.", body = serde_json::Value)),
    security(("bearerAuth" = [])),
    tag = "Auth"
)]
pub(crate) async fn auth_logout(
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

#[utoipa::path(
    get,
    path = "/v1/auth/me",
    responses((status = 200, description = "Authenticated identity.", body = serde_json::Value)),
    security(("bearerAuth" = [])),
    tag = "Auth"
)]
pub(crate) async fn auth_me(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<AuthIdentity>, ApiError> {
    Ok(Json(authorize_any(&state, &headers).await?))
}

#[utoipa::path(
    get,
    path = "/v1/api-keys",
    responses((status = 200, description = "API key list.", body = ApiKeysResponse)),
    security(("bearerAuth" = [])),
    tag = "API Keys"
)]
pub(crate) async fn list_api_keys(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<ApiKeysResponse>, ApiError> {
    let identity = authorize_admin_scope(&state, &headers, "api_keys:write").await?;
    let api_keys = auth_store(&state)?
        .list_api_keys(&identity.organization_id)
        .await?;
    Ok(Json(ApiKeysResponse { api_keys }))
}

#[utoipa::path(
    post,
    path = "/v1/api-keys",
    request_body = CreateApiKeyRequest,
    responses((status = 200, description = "Created API key.", body = serde_json::Value)),
    security(("bearerAuth" = [])),
    tag = "API Keys"
)]
pub(crate) async fn create_api_key(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<CreateApiKeyRequest>,
) -> Result<Json<nanotrace_auth::CreatedApiKey>, ApiError> {
    let identity = authorize_admin_scope(&state, &headers, "api_keys:write").await?;
    let role = parse_requested_role(request.role.as_deref())?;
    let created = auth_store(&state)?
        .create_api_key(
            &identity.organization_id,
            &request.name,
            role,
            &request.scopes,
            &identity.subject,
            request.expires_at,
        )
        .await?;
    Ok(Json(created))
}

#[utoipa::path(
    delete,
    path = "/v1/api-keys/{id}",
    params(("id" = i64, Path, description = "API key id.")),
    responses((status = 200, description = "Revoked API key envelope.", body = serde_json::Value)),
    security(("bearerAuth" = [])),
    tag = "API Keys"
)]
pub(crate) async fn revoke_api_key(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<i64>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let identity = authorize_admin(&state, &headers).await?;
    let api_key = auth_store(&state)?
        .revoke_api_key(&identity.organization_id, id)
        .await?;
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

async fn authorize_scope(
    state: &AppState,
    headers: &HeaderMap,
    scope: &str,
) -> Result<AuthIdentity, ApiError> {
    let identity = authorize_any(state, headers).await?;
    require_scope(&identity, scope)?;
    Ok(identity)
}

async fn authorize_admin_scope(
    state: &AppState,
    headers: &HeaderMap,
    scope: &str,
) -> Result<AuthIdentity, ApiError> {
    let identity = authorize_admin(state, headers).await?;
    require_scope(&identity, scope)?;
    Ok(identity)
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

fn require_scope(identity: &AuthIdentity, scope: &str) -> Result<(), ApiError> {
    if nanotrace_auth::has_scope(identity, scope) {
        Ok(())
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

fn query_type_name(request: &QueryApiRequest) -> &'static str {
    match request {
        QueryApiRequest::Events(_) => "events",
        QueryApiRequest::Search(_) => "search",
        QueryApiRequest::Measure(_) => "measure",
        QueryApiRequest::Funnel(_) => "funnel",
        QueryApiRequest::Cohort(_) => "cohort",
        QueryApiRequest::Report(_) => "report",
        QueryApiRequest::State(_) => "state",
        QueryApiRequest::Alerts(_) => "alerts",
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
    Unavailable(String),
    Email(String),
    Kafka(nanotrace_ingest::IngestError),
    Definition(crate::definitions::DefinitionStoreError),
    Materialization(crate::materializations::MaterializationStoreError),
    Read(crate::read::ReadError),
    Auth(AuthError),
}

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct KafkaAcceptedResponse {
    accepted: bool,
    mode: &'static str,
    topic: String,
    partition: i32,
    offset: i64,
}

impl From<nanotrace_ingest::IngestError> for ApiError {
    fn from(value: nanotrace_ingest::IngestError) -> Self {
        Self::Kafka(value)
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
            Self::Unavailable(message) => (StatusCode::SERVICE_UNAVAILABLE, message),
            Self::Email(message) => (StatusCode::SERVICE_UNAVAILABLE, message),
            Self::Kafka(err) => (StatusCode::BAD_GATEWAY, err.to_string()),
            Self::Definition(
                err @ (DefinitionStoreError::InvalidName
                | DefinitionStoreError::InvalidKind
                | DefinitionStoreError::InvalidMode
                | DefinitionStoreError::InvalidPath
                | DefinitionStoreError::InvalidConfig
                | DefinitionStoreError::UnsupportedSynchronousBackfillKind { .. }),
            ) => (StatusCode::BAD_REQUEST, err.to_string()),
            Self::Definition(DefinitionStoreError::NotFound) => {
                (StatusCode::NOT_FOUND, "not_found".to_string())
            }
            Self::Definition(DefinitionStoreError::ClickHouseNotConfigured) => (
                StatusCode::SERVICE_UNAVAILABLE,
                "ClickHouse is not configured".to_string(),
            ),
            Self::Definition(DefinitionStoreError::ClickHouseResponse { status, body }) => {
                let status = if status.is_client_error() {
                    StatusCode::BAD_REQUEST
                } else {
                    StatusCode::BAD_GATEWAY
                };
                (status, body)
            }
            Self::Definition(err) => (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()),
            Self::Materialization(
                err @ (MaterializationStoreError::InvalidRequest
                | MaterializationStoreError::InvalidTarget
                | MaterializationStoreError::UnsupportedQueuedBackfillKind { .. }),
            ) => (StatusCode::BAD_REQUEST, err.to_string()),
            Self::Materialization(MaterializationStoreError::DefinitionNotFound)
            | Self::Materialization(MaterializationStoreError::NotFound) => {
                (StatusCode::NOT_FOUND, "not_found".to_string())
            }
            Self::Materialization(MaterializationStoreError::ClickHouseNotConfigured) => (
                StatusCode::SERVICE_UNAVAILABLE,
                "ClickHouse is not configured".to_string(),
            ),
            Self::Materialization(MaterializationStoreError::ClickHouseResponse {
                status,
                body,
            }) => {
                let status = if status.is_client_error() {
                    StatusCode::BAD_REQUEST
                } else {
                    StatusCode::BAD_GATEWAY
                };
                (status, body)
            }
            Self::Materialization(err) => (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()),
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

impl From<crate::definitions::DefinitionStoreError> for ApiError {
    fn from(value: crate::definitions::DefinitionStoreError) -> Self {
        Self::Definition(value)
    }
}

impl From<crate::materializations::MaterializationStoreError> for ApiError {
    fn from(value: crate::materializations::MaterializationStoreError) -> Self {
        Self::Materialization(value)
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
