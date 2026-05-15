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
use bytes::Bytes;
use chrono::{DateTime, Utc};
use nanotrace_auth::{
    AuthError, AuthIdentity, AuthRole, AuthStore, AuthType, CompleteDataPlaneProvisionJobInput,
    CreateDataPlaneProvisionJobInput, CreateOrganizationInput, DEFAULT_ORGANIZATION_ID,
    OrganizationDataPlaneRecord, UpsertOrganizationDataPlaneInput,
};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::time::Instant;
use tower_http::{
    cors::{AllowOrigin, CorsLayer},
    limit::RequestBodyLimitLayer,
    services::{ServeDir, ServeFile},
};

use crate::{
    config::Config,
    dashboards::{
        CreateVisualizationRequest, DashboardError, DashboardStore,
        DashboardVisualizationsResponse, UpdateVisualizationRequest,
    },
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
    pub dashboards: Arc<DashboardStore>,
    pub facets: Arc<FacetStore>,
    pub processors: Arc<ProcessorStore>,
    pub read: Arc<ReadStore>,
    pub ses: aws_sdk_sesv2::Client,
    pub writer: Arc<EventLogWriter>,
}

pub fn router(state: AppState) -> Router {
    let limit = state.cfg.max_request_bytes;

    let router = Router::new()
        .route("/events", post(post_events))
        .route("/internal/events", post(post_events))
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
        .route("/internal/query", post(post_query))
        .route(
            "/dashboards/{dashboard_id}/visualizations",
            get(list_dashboard_visualizations)
                .post(create_dashboard_visualization)
                .delete(clear_dashboard_visualizations),
        )
        .route(
            "/dashboards/{dashboard_id}/visualizations/{id}",
            put(update_dashboard_visualization).delete(delete_dashboard_visualization),
        )
        .route("/auth/login", get(auth_login_form).post(auth_login))
        .route("/auth/callback", get(auth_callback))
        .route("/auth/invite/accept", get(auth_accept_invite))
        .route("/auth/logout", post(auth_logout))
        .route("/auth/me", get(auth_me))
        .route("/regions", get(list_regions))
        .route(
            "/data-plane/provisioning/jobs/claim",
            post(claim_data_plane_provision_job),
        )
        .route(
            "/data-plane/provisioning/jobs/{job_id}/complete",
            post(complete_data_plane_provision_job),
        )
        .route(
            "/organizations",
            get(list_organizations).post(create_organization),
        )
        .route(
            "/organizations/{organization_id}/invites",
            get(list_organization_invites).post(create_organization_invite),
        )
        .route(
            "/organizations/{organization_id}/data-plane",
            get(get_organization_data_plane).put(upsert_organization_data_plane),
        )
        .route(
            "/organizations/{organization_id}/data-plane/provision",
            post(provision_organization_data_plane),
        )
        .route(
            "/organizations/{organization_id}/data-plane/jobs",
            get(list_organization_data_plane_jobs),
        )
        .route(
            "/organizations/{organization_id}/data-plane/jobs/{job_id}",
            get(get_organization_data_plane_job),
        )
        .route(
            "/organizations/{organization_id}/invites/{invite_id}",
            delete(revoke_organization_invite),
        )
        .route("/api-keys", get(list_api_keys).post(create_api_key))
        .route("/api-keys/{id}", delete(revoke_api_key))
        .route("/healthz", get(healthz))
        .route("/metrics", get(metrics))
        .route("/readyz", get(readyz))
        .layer(RequestBodyLimitLayer::new(limit))
        .with_state(state.clone());

    let router = match &state.cfg.ui_dir {
        Some(ui_dir) => {
            let ui_index = ui_dir.join("index.html");
            let ui_assets = ServeDir::new(ui_dir.clone()).not_found_service(ServeFile::new(ui_index));
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

async fn post_events(
    State(state): State<AppState>,
    request: Request<Body>,
) -> Result<Response, ApiError> {
    let (parts, body) = request.into_parts();
    let headers = parts.headers;
    let identity = authorize_service(&state, &headers).await?;
    require_scope(&identity, "ingest:write")?;
    let read_started_at = Instant::now();
    let body = to_bytes(body, state.cfg.max_request_bytes)
        .await
        .map_err(|_| ApiError::PayloadTooLarge)?;
    state.writer.record_request_read(read_started_at.elapsed());
    if let Some(response) = proxy_to_data_plane(
        &state,
        &identity,
        "ingest",
        "/internal/events",
        body.clone(),
    )
    .await?
    {
        return Ok(response);
    }
    let receipts = state
        .writer
        .append_bytes(&body, &identity.tenant_id)
        .await?;
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
    Ok(Json(response).into_response())
}

async fn post_query(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<QueryRequest>,
) -> Result<Response, ApiError> {
    let identity = authorize_scope(&state, &headers, "query:read").await?;
    let body = serde_json::to_vec(&request).map_err(|err| ApiError::BadRequest(err.to_string()))?;
    if let Some(response) =
        proxy_to_data_plane(&state, &identity, "query", "/internal/query", body.into()).await?
    {
        return Ok(response);
    }
    let response = state.read.query(request, &identity.tenant_id).await?;
    Ok(Json(response).into_response())
}

async fn list_processors(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<ProcessorListResponse>, ApiError> {
    let identity = authorize_admin_scope(&state, &headers, "processors:write").await?;
    let processors = state.processors.list(&identity.tenant_id).await?;
    Ok(Json(ProcessorListResponse { processors }))
}

async fn list_facets(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<FacetListResponse>, ApiError> {
    let identity = authorize_scope(&state, &headers, "query:read").await?;
    let facets = state.facets.list(&identity.tenant_id).await?;
    Ok(Json(FacetListResponse { facets }))
}

async fn put_facet(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<PutFacetRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let identity = authorize_admin_scope(&state, &headers, "facets:write").await?;
    let facet = state.facets.put(&identity.tenant_id, request).await?;
    Ok(Json(serde_json::json!({ "facet": facet })))
}

async fn delete_facet(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(path): Path<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let identity = authorize_admin_scope(&state, &headers, "facets:write").await?;
    let facet = state.facets.delete(&identity.tenant_id, &path).await?;
    Ok(Json(serde_json::json!({ "facet": facet })))
}

async fn backfill_facet(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(path): Path<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let identity = authorize_admin_scope(&state, &headers, "facets:write").await?;
    let backfill = state
        .facets
        .enqueue_backfill(&identity.tenant_id, &path)
        .await?;
    Ok(Json(serde_json::json!({ "backfill": backfill })))
}

async fn list_facet_backfills(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<FacetBackfillListResponse>, ApiError> {
    let identity = authorize_admin_scope(&state, &headers, "facets:write").await?;
    let backfills = state.facets.backfill_list(&identity.tenant_id).await?;
    Ok(Json(FacetBackfillListResponse { backfills }))
}

async fn get_facet_backfill(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(job_id): Path<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let identity = authorize_admin_scope(&state, &headers, "facets:write").await?;
    let backfill = state
        .facets
        .backfill_status(&identity.tenant_id, &job_id)
        .await?;
    Ok(Json(serde_json::json!({ "backfill": backfill })))
}

async fn put_processor(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(name): Path<String>,
    Json(request): Json<PutProcessorRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let identity = authorize_admin_scope(&state, &headers, "processors:write").await?;
    let manifest = state
        .processors
        .put(&identity.tenant_id, &name, request)
        .await?;
    Ok(Json(serde_json::json!({ "processor": manifest })))
}

async fn delete_processor(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(name): Path<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let identity = authorize_admin_scope(&state, &headers, "processors:write").await?;
    let manifest = state.processors.delete(&identity.tenant_id, &name).await?;
    Ok(Json(serde_json::json!({ "processor": manifest })))
}

async fn list_dashboard_visualizations(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(dashboard_id): Path<String>,
) -> Result<Json<DashboardVisualizationsResponse>, ApiError> {
    let identity = authorize_scope(&state, &headers, "query:read").await?;
    let visualizations = state
        .dashboards
        .list(&identity.tenant_id, &dashboard_id)
        .await?;
    Ok(Json(DashboardVisualizationsResponse { visualizations }))
}

async fn create_dashboard_visualization(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(dashboard_id): Path<String>,
    Json(request): Json<CreateVisualizationRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let identity = authorize_admin_scope(&state, &headers, "dashboards:write").await?;
    let visualization = state
        .dashboards
        .create(&identity.tenant_id, &dashboard_id, request)
        .await?;
    Ok(Json(serde_json::json!({ "visualization": visualization })))
}

async fn clear_dashboard_visualizations(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(dashboard_id): Path<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let identity = authorize_admin_scope(&state, &headers, "dashboards:write").await?;
    state
        .dashboards
        .clear(&identity.tenant_id, &dashboard_id)
        .await?;
    Ok(Json(serde_json::json!({ "ok": true })))
}

async fn update_dashboard_visualization(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((dashboard_id, id)): Path<(String, String)>,
    Json(request): Json<UpdateVisualizationRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let identity = authorize_admin_scope(&state, &headers, "dashboards:write").await?;
    let visualization = state
        .dashboards
        .update(&identity.tenant_id, &dashboard_id, &id, request)
        .await?;
    Ok(Json(serde_json::json!({ "visualization": visualization })))
}

async fn delete_dashboard_visualization(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((dashboard_id, id)): Path<(String, String)>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let identity = authorize_admin_scope(&state, &headers, "dashboards:write").await?;
    let visualization = state
        .dashboards
        .delete(&identity.tenant_id, &dashboard_id, &id)
        .await?;
    Ok(Json(serde_json::json!({ "visualization": visualization })))
}

async fn get_event(
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
    #[serde(default)]
    scopes: Vec<String>,
    expires_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Serialize)]
struct ApiKeysResponse {
    api_keys: Vec<nanotrace_auth::ApiKeyRecord>,
}

#[derive(Debug, Serialize)]
struct OrganizationsResponse {
    organizations: Vec<nanotrace_auth::OrganizationRecord>,
}

#[derive(Debug, Serialize)]
struct RegionsResponse {
    regions: Vec<RegionOption>,
}

#[derive(Debug, Serialize)]
struct RegionOption {
    provider: String,
    region: String,
    clickhouse_provider: String,
    clickhouse_region: String,
    current: bool,
}

#[derive(Debug, Deserialize)]
struct CreateOrganizationRequest {
    slug: String,
    name: String,
    #[serde(default)]
    plan: String,
}

#[derive(Debug, Deserialize)]
struct UpsertOrganizationDataPlaneRequest {
    #[serde(default)]
    mode: String,
    #[serde(default)]
    provider: String,
    #[serde(default)]
    region: String,
    public_base_url: String,
    ingest_url: String,
    query_url: String,
    internal_secret_ref: String,
    s3_bucket: String,
    processor_prefix: String,
    #[serde(default)]
    clickhouse_mode: String,
    #[serde(default)]
    clickhouse_provider: String,
    #[serde(default)]
    clickhouse_region: String,
    #[serde(default)]
    clickhouse_service_id: String,
    clickhouse_url: String,
    #[serde(default)]
    clickhouse_database: String,
    #[serde(default)]
    kms_key_arn: String,
    #[serde(default)]
    status: String,
    #[serde(default)]
    status_message: String,
    #[serde(default)]
    last_provisioning_job_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ProvisionDataPlaneRequest {
    #[serde(default)]
    provider: String,
    #[serde(default)]
    region: String,
    #[serde(default)]
    clickhouse_mode: String,
    #[serde(default)]
    clickhouse_region: String,
}

#[derive(Debug, Deserialize)]
struct ClaimProvisionJobRequest {
    #[serde(default)]
    worker_id: String,
}

#[derive(Debug, Serialize)]
struct ClaimProvisionJobResponse {
    job: Option<nanotrace_auth::DataPlaneProvisionJobRecord>,
}

#[derive(Debug, Deserialize)]
struct CompleteProvisionJobRequest {
    #[serde(default)]
    status: String,
    #[serde(default)]
    result: Option<JsonValue>,
    #[serde(default)]
    error: Option<String>,
    #[serde(default)]
    data_plane: Option<UpsertOrganizationDataPlaneRequest>,
}

#[derive(Debug, Serialize)]
struct DataPlaneJobsResponse {
    jobs: Vec<nanotrace_auth::DataPlaneProvisionJobRecord>,
}

#[derive(Debug, Serialize)]
struct OrganizationInvitesResponse {
    invites: Vec<nanotrace_auth::OrganizationInviteRecord>,
}

#[derive(Debug, Deserialize)]
struct CreateOrganizationInviteRequest {
    email: String,
    #[serde(default)]
    role: Option<String>,
}

#[derive(Debug, Deserialize)]
struct AcceptInviteParams {
    token: String,
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

async fn auth_accept_invite(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(params): Query<AcceptInviteParams>,
) -> Result<Response, ApiError> {
    let auth = auth_store(&state)?;
    let identity = match auth.validate_session(&headers).await {
        Ok(identity) => identity,
        Err(AuthError::Unauthorized) => {
            let return_to = format!(
                "/auth/invite/accept?token={}",
                url_percent_encode(&params.token)
            );
            let login_url = format!("/auth/login?return_to={}", url_percent_encode(&return_to));
            let mut response = StatusCode::FOUND.into_response();
            response.headers_mut().insert(
                LOCATION,
                login_url.parse().map_err(|err| {
                    ApiError::Header(format!("invalid invite login redirect: {err}"))
                })?,
            );
            return Ok(response);
        }
        Err(error) => return Err(error.into()),
    };
    let email = identity.email.as_deref().ok_or(ApiError::Unauthorized)?;
    auth.accept_organization_invite(&params.token, &identity.subject, email)
        .await?;
    let mut response = StatusCode::FOUND.into_response();
    response.headers_mut().insert(
        LOCATION,
        callback_return_to(&state.cfg, "/settings/organizations")
            .parse()
            .map_err(|err| ApiError::Header(format!("invalid invite redirect: {err}")))?,
    );
    Ok(response)
}

fn callback_return_to(cfg: &Config, return_to: &str) -> String {
    let return_to = return_to.trim();
    if return_to.starts_with('/') && !return_to.starts_with("//") {
        if let Some(app_base_url) = cfg.app_base_url.as_deref() {
            return format!("{}{}", app_base_url.trim_end_matches('/'), return_to);
        }
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

async fn send_invite_email(
    state: &AppState,
    email: &str,
    organization_name: &str,
    invite_url: &str,
) -> Result<(), ApiError> {
    send_text_email(
        state,
        email,
        &format!("You have been invited to {organization_name} on Nanotrace"),
        &format!(
            "You have been invited to join {organization_name} on Nanotrace.\n\nAccept the invite:\n\n{invite_url}\n\nThis invite expires soon and can only be used once."
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

async fn list_organizations(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<OrganizationsResponse>, ApiError> {
    let identity = authorize_any(&state, &headers).await?;
    let organizations = auth_store(&state)?
        .list_organizations(&identity.organization_id, &identity.subject)
        .await?;
    Ok(Json(OrganizationsResponse { organizations }))
}

async fn create_organization(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<CreateOrganizationRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let identity = authorize_admin_scope(&state, &headers, "organizations:write").await?;
    if matches!(identity.auth_type, AuthType::ApiKey) {
        require_platform_admin(&identity)?;
    }
    let organization = auth_store(&state)?
        .create_organization(
            &identity.subject,
            CreateOrganizationInput {
                slug: request.slug,
                name: request.name,
                plan: request.plan,
            },
        )
        .await?;
    Ok(Json(serde_json::json!({ "organization": organization })))
}

async fn list_regions(State(state): State<AppState>) -> Result<Json<RegionsResponse>, ApiError> {
    let regions = state
        .cfg
        .supported_regions
        .iter()
        .map(|region| RegionOption {
            provider: state.cfg.cloud_provider.clone(),
            region: region.clone(),
            clickhouse_provider: state.cfg.cloud_provider.clone(),
            clickhouse_region: region.clone(),
            current: region == &state.cfg.region,
        })
        .collect();
    Ok(Json(RegionsResponse { regions }))
}

async fn claim_data_plane_provision_job(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<ClaimProvisionJobRequest>,
) -> Result<Json<ClaimProvisionJobResponse>, ApiError> {
    let identity = authorize_platform_admin_scope(&state, &headers, "organizations:write").await?;
    let worker_id = if request.worker_id.trim().is_empty() {
        format!("provisioner:{}", identity.subject)
    } else {
        request.worker_id
    };
    let job = auth_store(&state)?
        .claim_next_data_plane_provision_job(&worker_id)
        .await?;
    Ok(Json(ClaimProvisionJobResponse { job }))
}

async fn complete_data_plane_provision_job(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(job_id): Path<String>,
    Json(request): Json<CompleteProvisionJobRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    authorize_platform_admin_scope(&state, &headers, "organizations:write").await?;
    let job = auth_store(&state)?
        .complete_data_plane_provision_job(
            &job_id,
            CompleteDataPlaneProvisionJobInput {
                status: request.status,
                result: request.result,
                error: request.error,
                data_plane: request.data_plane.map(upsert_data_plane_input),
            },
        )
        .await?;
    Ok(Json(serde_json::json!({ "job": job })))
}

async fn get_organization_data_plane(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(organization_id): Path<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let identity = authorize_any(&state, &headers).await?;
    ensure_api_key_organization(&identity, &organization_id)?;
    let data_plane = match auth_store(&state)?
        .get_organization_data_plane(&organization_id, &identity.subject)
        .await
    {
        Ok(data_plane) => data_plane,
        Err(AuthError::NotFound) => shared_data_plane_record(&state, &organization_id),
        Err(err) => return Err(err.into()),
    };
    Ok(Json(serde_json::json!({ "data_plane": data_plane })))
}

async fn upsert_organization_data_plane(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(organization_id): Path<String>,
    Json(request): Json<UpsertOrganizationDataPlaneRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let identity = authorize_admin_scope(&state, &headers, "organizations:write").await?;
    ensure_api_key_organization(&identity, &organization_id)?;
    let data_plane = auth_store(&state)?
        .upsert_organization_data_plane(
            &organization_id,
            &identity.subject,
            upsert_data_plane_input(request),
        )
        .await?;
    Ok(Json(serde_json::json!({ "data_plane": data_plane })))
}

async fn provision_organization_data_plane(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(organization_id): Path<String>,
    Json(request): Json<ProvisionDataPlaneRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let identity = authorize_admin_scope(&state, &headers, "organizations:write").await?;
    ensure_api_key_organization(&identity, &organization_id)?;
    let job = auth_store(&state)?
        .create_data_plane_provision_job(
            &organization_id,
            &identity.subject,
            CreateDataPlaneProvisionJobInput {
                provider: request.provider,
                region: request.region,
                clickhouse_mode: request.clickhouse_mode,
                clickhouse_region: request.clickhouse_region,
            },
        )
        .await?;
    let data_plane = auth_store(&state)?
        .get_organization_data_plane(&organization_id, &identity.subject)
        .await?;
    Ok(Json(
        serde_json::json!({ "job": job, "data_plane": data_plane }),
    ))
}

async fn list_organization_data_plane_jobs(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(organization_id): Path<String>,
) -> Result<Json<DataPlaneJobsResponse>, ApiError> {
    let identity = authorize_admin_scope(&state, &headers, "organizations:write").await?;
    ensure_api_key_organization(&identity, &organization_id)?;
    let jobs = auth_store(&state)?
        .list_data_plane_jobs(&organization_id, &identity.subject)
        .await?;
    Ok(Json(DataPlaneJobsResponse { jobs }))
}

async fn get_organization_data_plane_job(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((organization_id, job_id)): Path<(String, String)>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let identity = authorize_admin_scope(&state, &headers, "organizations:write").await?;
    ensure_api_key_organization(&identity, &organization_id)?;
    let job = auth_store(&state)?
        .get_data_plane_job(&organization_id, &identity.subject, &job_id)
        .await?;
    Ok(Json(serde_json::json!({ "job": job })))
}

async fn list_organization_invites(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(organization_id): Path<String>,
) -> Result<Json<OrganizationInvitesResponse>, ApiError> {
    let identity = authorize_admin_scope(&state, &headers, "organizations:write").await?;
    ensure_api_key_organization(&identity, &organization_id)?;
    let invites = auth_store(&state)?
        .list_organization_invites(&organization_id, &identity.subject)
        .await?;
    Ok(Json(OrganizationInvitesResponse { invites }))
}

async fn create_organization_invite(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(organization_id): Path<String>,
    Json(request): Json<CreateOrganizationInviteRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let identity = authorize_admin_scope(&state, &headers, "organizations:write").await?;
    ensure_api_key_organization(&identity, &organization_id)?;
    let role = parse_invite_role(request.role.as_deref())?;
    let created = auth_store(&state)?
        .create_organization_invite(&organization_id, &identity.subject, &request.email, role)
        .await?;
    let invite_url = invite_accept_url(&state.cfg, &created.token);
    send_invite_email(
        &state,
        &created.invite.email,
        &identity.organization_name,
        &invite_url,
    )
    .await?;
    Ok(Json(serde_json::json!({
        "invite": created.invite,
        "sent": true
    })))
}

async fn revoke_organization_invite(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((organization_id, invite_id)): Path<(String, i64)>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let identity = authorize_admin_scope(&state, &headers, "organizations:write").await?;
    ensure_api_key_organization(&identity, &organization_id)?;
    let invite = auth_store(&state)?
        .revoke_organization_invite(&organization_id, invite_id, &identity.subject)
        .await?;
    Ok(Json(serde_json::json!({ "invite": invite })))
}

async fn list_api_keys(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<ApiKeysResponse>, ApiError> {
    let identity = authorize_admin_scope(&state, &headers, "api_keys:write").await?;
    let api_keys = auth_store(&state)?
        .list_api_keys(&identity.organization_id)
        .await?;
    Ok(Json(ApiKeysResponse { api_keys }))
}

async fn create_api_key(
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

async fn revoke_api_key(
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

async fn proxy_to_data_plane(
    state: &AppState,
    identity: &AuthIdentity,
    endpoint_kind: &str,
    path: &str,
    body: Bytes,
) -> Result<Option<Response>, ApiError> {
    if state.cfg.data_plane_organization_id.is_some() || identity.subject == "internal:gateway" {
        return Ok(None);
    }
    let base_url = data_plane_base_url(state, identity, endpoint_kind).await;
    let internal_secret = state.cfg.shared_data_plane_secret.as_deref().unwrap_or("");
    if base_url.is_empty() || is_local_data_plane_base(state, &base_url) {
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
    let response = reqwest::Client::new()
        .post(url)
        .header("content-type", "application/json")
        .header("x-nanotrace-internal-organization-id", &identity.tenant_id)
        .header("x-nanotrace-internal-secret", internal_secret)
        .body(body)
        .send()
        .await
        .map_err(|err| ApiError::BadGateway(err.to_string()))?;
    let status = response.status();
    let content_type = response
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("application/json")
        .to_string();
    let bytes = response
        .bytes()
        .await
        .map_err(|err| ApiError::BadGateway(err.to_string()))?;
    let mut builder = Response::builder().status(status);
    builder = builder.header(CONTENT_TYPE, content_type);
    builder
        .body(Body::from(bytes))
        .map(Some)
        .map_err(|err| ApiError::BadGateway(err.to_string()))
}

async fn data_plane_base_url(
    state: &AppState,
    identity: &AuthIdentity,
    endpoint_kind: &str,
) -> String {
    if let Some(auth) = state.auth.as_deref() {
        if let Ok(data_plane) = auth
            .get_organization_data_plane(&identity.organization_id, &identity.subject)
            .await
        {
            if data_plane.mode == "dedicated" && data_plane.status == "active" {
                let url = match endpoint_kind {
                    "ingest" => &data_plane.ingest_url,
                    "query" => &data_plane.query_url,
                    _ => "",
                };
                if !url.trim().is_empty() {
                    return url.trim().to_string();
                }
            }
        }
    }
    shared_data_plane_base_url(state, endpoint_kind).to_string()
}

fn shared_data_plane_base_url<'a>(state: &'a AppState, endpoint_kind: &str) -> &'a str {
    match endpoint_kind {
        "ingest" => state
            .cfg
            .shared_data_plane_ingest_url
            .as_deref()
            .unwrap_or(""),
        "query" => state
            .cfg
            .shared_data_plane_query_url
            .as_deref()
            .unwrap_or(""),
        _ => "",
    }
}

fn shared_data_plane_record(
    state: &AppState,
    organization_id: &str,
) -> OrganizationDataPlaneRecord {
    let public_base_url = state
        .cfg
        .auth
        .public_base_url
        .as_deref()
        .or(state.cfg.app_base_url.as_deref())
        .unwrap_or("")
        .trim_end_matches('/')
        .to_string();
    let ingest_url = state
        .cfg
        .shared_data_plane_ingest_url
        .as_deref()
        .unwrap_or(&public_base_url)
        .trim_end_matches('/')
        .to_string();
    let query_url = state
        .cfg
        .shared_data_plane_query_url
        .as_deref()
        .unwrap_or(&public_base_url)
        .trim_end_matches('/')
        .to_string();
    let now = Utc::now();
    OrganizationDataPlaneRecord {
        organization_id: organization_id.to_string(),
        mode: "shared".to_string(),
        provider: state.cfg.cloud_provider.clone(),
        region: state.cfg.region.clone(),
        public_base_url,
        ingest_url,
        query_url,
        internal_secret_ref: if state.cfg.shared_data_plane_secret.is_some() {
            "NANOTRACE_SHARED_DATA_PLANE_SECRET".to_string()
        } else {
            String::new()
        },
        s3_bucket: state.cfg.s3_bucket.clone().unwrap_or_default(),
        processor_prefix: state.cfg.processor_prefix.clone(),
        clickhouse_mode: state.cfg.clickhouse_mode.clone(),
        clickhouse_provider: state.cfg.cloud_provider.clone(),
        clickhouse_region: state.cfg.clickhouse_region.clone(),
        clickhouse_service_id: state.cfg.clickhouse_service_id.clone().unwrap_or_default(),
        clickhouse_url: state.cfg.clickhouse_url.clone().unwrap_or_default(),
        clickhouse_database: state.cfg.clickhouse_database.clone(),
        kms_key_arn: state.cfg.data_plane_kms_key_arn.clone().unwrap_or_default(),
        status: "active".to_string(),
        status_message: "Using the shared Nanotrace data plane.".to_string(),
        last_provisioning_job_id: None,
        created_at: now,
        updated_at: now,
    }
}

fn upsert_data_plane_input(
    request: UpsertOrganizationDataPlaneRequest,
) -> UpsertOrganizationDataPlaneInput {
    UpsertOrganizationDataPlaneInput {
        mode: request.mode,
        provider: request.provider,
        region: request.region,
        public_base_url: request.public_base_url,
        ingest_url: request.ingest_url,
        query_url: request.query_url,
        internal_secret_ref: request.internal_secret_ref,
        s3_bucket: request.s3_bucket,
        processor_prefix: request.processor_prefix,
        clickhouse_mode: request.clickhouse_mode,
        clickhouse_provider: request.clickhouse_provider,
        clickhouse_region: request.clickhouse_region,
        clickhouse_service_id: request.clickhouse_service_id,
        clickhouse_url: request.clickhouse_url,
        clickhouse_database: request.clickhouse_database,
        kms_key_arn: request.kms_key_arn,
        status: request.status,
        status_message: request.status_message,
        last_provisioning_job_id: request.last_provisioning_job_id,
    }
}

fn is_local_data_plane_base(state: &AppState, base_url: &str) -> bool {
    let normalized = base_url.trim_end_matches('/');
    state
        .cfg
        .auth
        .public_base_url
        .as_deref()
        .is_some_and(|value| value.trim_end_matches('/') == normalized)
}

fn auth_store(state: &AppState) -> Result<&AuthStore, ApiError> {
    state.auth.as_deref().ok_or(ApiError::AuthNotConfigured)
}

async fn authorize_any(state: &AppState, headers: &HeaderMap) -> Result<AuthIdentity, ApiError> {
    if let Some(identity) = authorize_internal(state, headers) {
        return Ok(identity);
    }
    let identity = nanotrace_auth::authorize_headers(headers, state.auth.as_deref()).await?;
    ensure_data_plane_organization(state, &identity)?;
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
        scopes: vec!["ingest:write".to_string(), "query:read".to_string()],
    })
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

async fn authorize_platform_admin_scope(
    state: &AppState,
    headers: &HeaderMap,
    scope: &str,
) -> Result<AuthIdentity, ApiError> {
    let identity = authorize_admin_scope(state, headers, scope).await?;
    require_platform_admin(&identity)?;
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

fn ensure_data_plane_organization(
    state: &AppState,
    identity: &AuthIdentity,
) -> Result<(), ApiError> {
    if state
        .cfg
        .data_plane_organization_id
        .as_deref()
        .is_some_and(|organization_id| organization_id != identity.tenant_id)
    {
        Err(ApiError::Forbidden)
    } else {
        Ok(())
    }
}

fn require_platform_admin(identity: &AuthIdentity) -> Result<(), ApiError> {
    if identity.role != AuthRole::Admin {
        return Err(ApiError::Forbidden);
    }
    if matches!(identity.auth_type, AuthType::ApiKey)
        && identity.organization_id != DEFAULT_ORGANIZATION_ID
    {
        return Err(ApiError::Forbidden);
    }
    Ok(())
}

fn ensure_api_key_organization(
    identity: &AuthIdentity,
    organization_id: &str,
) -> Result<(), ApiError> {
    if matches!(identity.auth_type, AuthType::ApiKey)
        && identity.subject != "internal:gateway"
        && identity.organization_id != organization_id
    {
        return Err(ApiError::Forbidden);
    }
    Ok(())
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

fn parse_invite_role(value: Option<&str>) -> Result<AuthRole, ApiError> {
    match value
        .unwrap_or("viewer")
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "admin" => Ok(AuthRole::Admin),
        "viewer" => Ok(AuthRole::Viewer),
        "service" => Err(ApiError::BadRequest(
            "organization invites cannot use service role".to_string(),
        )),
        other => Err(ApiError::BadRequest(format!(
            "invalid organization invite role: {other}"
        ))),
    }
}

fn invite_accept_url(cfg: &Config, token: &str) -> String {
    let base = cfg
        .auth
        .public_base_url
        .as_deref()
        .or(cfg.app_base_url.as_deref())
        .unwrap_or("")
        .trim_end_matches('/');
    let path = format!("/auth/invite/accept?token={}", url_percent_encode(token));
    if base.is_empty() {
        path
    } else {
        format!("{base}{path}")
    }
}

fn url_percent_encode(value: &str) -> String {
    let mut encoded = String::new();
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            encoded.push(byte as char);
        } else {
            encoded.push_str(&format!("%{byte:02X}"));
        }
    }
    encoded
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
    BadGateway(String),
    Unavailable(String),
    Email(String),
    EventLog(crate::event_log::EventLogError),
    Processor(crate::processors::ProcessorStoreError),
    Facet(crate::facets::FacetError),
    Dashboard(crate::dashboards::DashboardError),
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
            Self::BadGateway(message) => (StatusCode::BAD_GATEWAY, message),
            Self::Unavailable(message) => (StatusCode::SERVICE_UNAVAILABLE, message),
            Self::Email(message) => (StatusCode::SERVICE_UNAVAILABLE, message),
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
            Self::Dashboard(
                err @ (DashboardError::InvalidId
                | DashboardError::InvalidDashboardId
                | DashboardError::MissingTitle
                | DashboardError::MissingSourceCode
                | DashboardError::InvalidDimensions),
            ) => (StatusCode::BAD_REQUEST, err.to_string()),
            Self::Dashboard(DashboardError::PostgresNotConfigured) => (
                StatusCode::SERVICE_UNAVAILABLE,
                "Postgres is not configured".to_string(),
            ),
            Self::Dashboard(DashboardError::NotFound) => {
                (StatusCode::NOT_FOUND, "not_found".to_string())
            }
            Self::Dashboard(err) => (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()),
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

impl From<crate::dashboards::DashboardError> for ApiError {
    fn from(value: crate::dashboards::DashboardError) -> Self {
        Self::Dashboard(value)
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
