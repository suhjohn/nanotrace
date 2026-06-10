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
        HeaderMap, HeaderName, HeaderValue, Method, Request, StatusCode,
        header::{AUTHORIZATION, CONTENT_TYPE, LOCATION, SET_COOKIE},
    },
    middleware::{self, Next},
    response::{Html, IntoResponse, Response},
    routing::{delete, get, patch, post},
};
use chrono::{DateTime, Utc};
use nanotrace_auth::{AuthError, AuthIdentity, AuthRole, AuthStore};
use nanotrace_ingest::HEADER_PROJECT_ID;
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
    deletions::{
        CreateDeletionRequest, DeletionJobListResponse, DeletionJobResponse, DeletionStore,
        DeletionStoreError,
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
    pub deletions: Arc<DeletionStore>,
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
        .route("/v1/deletions", get(list_deletions).post(create_deletion))
        .route("/v1/deletions/{deletion_id}", get(get_deletion))
        .route("/v1/query/recommendations", get(list_query_recommendations))
        .route("/v1/query", post(post_query))
        .route("/auth/login", get(auth_login_form).post(auth_login))
        .route("/auth/providers", get(auth_providers))
        .route("/auth/google", get(auth_google_start))
        .route("/auth/google/callback", get(auth_google_callback))
        .route("/auth/callback", get(auth_callback))
        .route("/auth/logout", post(auth_logout))
        .route("/auth/me", get(auth_me))
        .route("/v1/auth/me", get(auth_me))
        .route(
            "/v1/organizations",
            get(list_organizations).post(create_organization),
        )
        .route(
            "/v1/organizations/{organization_id}",
            patch(update_organization).delete(archive_organization),
        )
        .route(
            "/v1/organizations/{organization_id}/switch",
            post(switch_organization),
        )
        .route("/v1/projects", get(list_projects).post(create_project))
        .route(
            "/v1/projects/{project_id}",
            patch(update_project).delete(archive_project),
        )
        .route(
            "/v1/organizations/{organization_id}/leave",
            post(leave_organization),
        )
        .route(
            "/v1/organizations/{organization_id}/members",
            get(list_organization_members),
        )
        .route(
            "/v1/organizations/{organization_id}/members/{subject}",
            patch(update_organization_member).delete(remove_organization_member),
        )
        .route(
            "/v1/organizations/{organization_id}/invitations",
            get(list_organization_invitations).post(create_organization_invitation),
        )
        .route(
            "/v1/organizations/{organization_id}/invitations/{invitation_id}",
            delete(revoke_organization_invitation),
        )
        .route(
            "/v1/organizations/{organization_id}/invitations/{invitation_id}/resend",
            post(resend_organization_invitation),
        )
        .route(
            "/v1/organization-invitations/accept",
            post(accept_organization_invitation),
        )
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
            .allow_headers([
                AUTHORIZATION,
                CONTENT_TYPE,
                HeaderName::from_static(HEADER_PROJECT_ID),
            ])
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
    let requested_project_id = headers
        .get(HeaderName::from_static(HEADER_PROJECT_ID))
        .and_then(|value| value.to_str().ok());
    let project_id = auth_store(&state)?
        .resolve_ingest_project(&identity, requested_project_id)
        .await?;
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
            &project_id,
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
    let mut request = serde_json::from_value::<QueryApiRequest>(request)
        .map_err(|err| ApiError::BadRequest(err.to_string()))?;
    apply_authorized_project_scope(&identity, &mut request)?;
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
    post,
    path = "/v1/deletions",
    request_body = CreateDeletionRequest,
    responses((status = 200, description = "Created deletion job.", body = DeletionJobResponse)),
    security(("bearerAuth" = [])),
    tag = "Deletions"
)]
pub(crate) async fn create_deletion(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<CreateDeletionRequest>,
) -> Result<Json<DeletionJobResponse>, ApiError> {
    let identity = authorize_admin_scope(&state, &headers, "data:delete").await?;
    require_explicit_scope(&identity, "data:delete")?;
    authorize_deletion_project_scope(&identity, &request.project_scope)?;
    let response = state
        .deletions
        .create_deletion(&identity.tenant_id, &identity.subject, request)
        .await?;
    Ok(Json(response))
}

#[utoipa::path(
    get,
    path = "/v1/deletions",
    responses((status = 200, description = "Deletion jobs.", body = DeletionJobListResponse)),
    security(("bearerAuth" = [])),
    tag = "Deletions"
)]
pub(crate) async fn list_deletions(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<DeletionJobListResponse>, ApiError> {
    let identity = authorize_admin_scope(&state, &headers, "data:delete").await?;
    require_explicit_scope(&identity, "data:delete")?;
    let deletions = state.deletions.list_deletions(&identity.tenant_id).await?;
    Ok(Json(DeletionJobListResponse { deletions }))
}

#[utoipa::path(
    get,
    path = "/v1/deletions/{deletion_id}",
    params(("deletion_id" = String, Path, description = "Deletion job id.")),
    responses((status = 200, description = "Deletion job.", body = DeletionJobResponse)),
    security(("bearerAuth" = [])),
    tag = "Deletions"
)]
pub(crate) async fn get_deletion(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(deletion_id): Path<String>,
) -> Result<Json<DeletionJobResponse>, ApiError> {
    let identity = authorize_admin_scope(&state, &headers, "data:delete").await?;
    require_explicit_scope(&identity, "data:delete")?;
    let deletion = state
        .deletions
        .get_deletion(&identity.tenant_id, &deletion_id)
        .await?;
    Ok(Json(DeletionJobResponse { deletion }))
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
        .event_bytes_scoped(&event_id, &identity.tenant_id, &identity.project_ids)
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

#[derive(Debug, Deserialize)]
struct OAuthCallbackParams {
    code: Option<String>,
    state: Option<String>,
    error: Option<String>,
}

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct AuthProvidersResponse {
    google: bool,
}

#[derive(Debug, Deserialize, ToSchema)]
pub(crate) struct CreateApiKeyRequest {
    name: String,
    role: Option<String>,
    #[serde(default)]
    scopes: Vec<String>,
    #[serde(default)]
    project_ids: Vec<String>,
    default_project_id: Option<String>,
    expires_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Deserialize, ToSchema)]
pub(crate) struct CreateOrganizationRequest {
    name: String,
    slug: Option<String>,
}

#[derive(Debug, Deserialize, ToSchema)]
pub(crate) struct UpdateOrganizationRequest {
    name: Option<String>,
    slug: Option<String>,
}

#[derive(Debug, Deserialize, ToSchema)]
pub(crate) struct CreateProjectRequest {
    name: String,
    slug: Option<String>,
}

#[derive(Debug, Deserialize, ToSchema)]
pub(crate) struct UpdateProjectRequest {
    name: Option<String>,
    slug: Option<String>,
}

#[derive(Debug, Deserialize, ToSchema)]
pub(crate) struct UpdateOrganizationMemberRequest {
    role: String,
}

#[derive(Debug, Deserialize, ToSchema)]
pub(crate) struct CreateOrganizationInvitationRequest {
    email: String,
    role: String,
}

#[derive(Debug, Deserialize, ToSchema)]
pub(crate) struct AcceptOrganizationInvitationRequest {
    token: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct OrganizationListItem {
    organization_id: String,
    organization_name: String,
    slug: Option<String>,
    role: String,
    created_at: Option<DateTime<Utc>>,
    updated_at: Option<DateTime<Utc>>,
}

impl From<nanotrace_auth::OrganizationMembershipSummary> for OrganizationListItem {
    fn from(value: nanotrace_auth::OrganizationMembershipSummary) -> Self {
        Self {
            organization_id: value.organization_id,
            organization_name: value.organization_name,
            slug: Some(value.slug),
            role: role_name(value.role).to_string(),
            created_at: Some(value.created_at),
            updated_at: Some(value.updated_at),
        }
    }
}

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct OrganizationListApiResponse {
    active_organization_id: Option<String>,
    organizations: Vec<OrganizationListItem>,
}

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct OrganizationResponse {
    #[schema(value_type = serde_json::Value)]
    organization: nanotrace_auth::OrganizationRecord,
}

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct ProjectsResponse {
    #[schema(value_type = Vec<serde_json::Value>)]
    projects: Vec<nanotrace_auth::ProjectRecord>,
}

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct ProjectResponse {
    #[schema(value_type = serde_json::Value)]
    project: nanotrace_auth::ProjectRecord,
}

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct OrganizationMemberResponse {
    #[schema(value_type = serde_json::Value)]
    member: nanotrace_auth::OrganizationMemberRecord,
}

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct OrganizationMembersResponse {
    #[schema(value_type = Vec<serde_json::Value>)]
    members: Vec<nanotrace_auth::OrganizationMemberRecord>,
}

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct OrganizationMembershipResponse {
    #[schema(value_type = serde_json::Value)]
    organization: nanotrace_auth::OrganizationMembershipSummary,
}

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct OrganizationInvitationResponse {
    #[schema(value_type = serde_json::Value)]
    invitation: nanotrace_auth::OrganizationInvitationRecord,
}

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct OrganizationInvitationsResponse {
    #[schema(value_type = Vec<serde_json::Value>)]
    invitations: Vec<nanotrace_auth::OrganizationInvitationRecord>,
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
    get,
    path = "/auth/providers",
    responses((status = 200, description = "Configured browser auth providers.", body = AuthProvidersResponse)),
    tag = "Auth"
)]
pub(crate) async fn auth_providers(State(state): State<AppState>) -> Json<AuthProvidersResponse> {
    Json(AuthProvidersResponse {
        google: state.cfg.google_oauth.is_some(),
    })
}

async fn auth_google_start(
    State(state): State<AppState>,
    Query(params): Query<LoginParams>,
) -> Result<Response, ApiError> {
    let auth = auth_store(&state)?;
    let google = state
        .cfg
        .google_oauth
        .as_ref()
        .ok_or_else(|| ApiError::Unavailable("Google OAuth is not configured".to_string()))?;
    let redirect_uri = google_redirect_uri(&state.cfg)?;
    let oauth = auth
        .start_oauth_login("google", params.return_to.as_deref())
        .await?;
    let authorize_url = google_authorize_url(&google.client_id, &redirect_uri, &oauth.state);
    let mut response = StatusCode::FOUND.into_response();
    response.headers_mut().insert(
        LOCATION,
        authorize_url
            .parse()
            .map_err(|err| ApiError::Header(format!("invalid google authorize URL: {err}")))?,
    );
    Ok(response)
}

async fn auth_google_callback(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(params): Query<OAuthCallbackParams>,
) -> Result<Response, ApiError> {
    if let Some(error) = params.error.as_deref() {
        return Err(ApiError::BadRequest(format!("Google OAuth error: {error}")));
    }
    let code = params
        .code
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| ApiError::BadRequest("Google OAuth code is required".to_string()))?;
    let state_token = params
        .state
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| ApiError::BadRequest("Google OAuth state is required".to_string()))?;
    let auth = auth_store(&state)?;
    let return_to = auth.consume_oauth_state("google", state_token).await?;
    let google = state
        .cfg
        .google_oauth
        .as_ref()
        .ok_or_else(|| ApiError::Unavailable("Google OAuth is not configured".to_string()))?;
    let redirect_uri = google_redirect_uri(&state.cfg)?;
    let token = exchange_google_code(google, &redirect_uri, code).await?;
    let profile = verify_google_id_token(&google.client_id, &token.id_token).await?;
    let login = auth
        .complete_external_login(
            &profile.email,
            profile.name.as_deref(),
            &return_to,
            Some(&headers),
            "google_oauth",
        )
        .await?;
    browser_login_response(&state, login).await
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
    headers: HeaderMap,
    Query(params): Query<CallbackParams>,
) -> Result<Response, ApiError> {
    let auth = auth_store(&state)?;
    let login = auth.complete_login(&params.token, Some(&headers)).await?;
    browser_login_response(&state, login).await
}

async fn browser_login_response(
    state: &AppState,
    login: nanotrace_auth::LoginComplete,
) -> Result<Response, ApiError> {
    if let Some(organization_id) = login.created_organization_id.as_deref()
        && state.cfg.clickhouse_url.is_some()
    {
        state.definitions.seed_sdk_defaults(organization_id).await?;
    }
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

fn google_redirect_uri(cfg: &Config) -> Result<String, ApiError> {
    if let Some(redirect_uri) = cfg
        .google_oauth
        .as_ref()
        .and_then(|config| config.redirect_uri.as_deref())
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        return Ok(redirect_uri.to_string());
    }
    let base_url = cfg.auth.public_base_url.as_deref().ok_or_else(|| {
        ApiError::Unavailable("NANOTRACE_PUBLIC_BASE_URL is required for Google OAuth".to_string())
    })?;
    Ok(format!(
        "{}/auth/google/callback",
        base_url.trim_end_matches('/')
    ))
}

fn google_authorize_url(client_id: &str, redirect_uri: &str, state: &str) -> String {
    let params = [
        ("client_id", client_id),
        ("redirect_uri", redirect_uri),
        ("response_type", "code"),
        ("scope", "openid email profile"),
        ("state", state),
        ("prompt", "select_account"),
    ];
    let query = params
        .iter()
        .map(|(key, value)| format!("{key}={}", urlencoding::encode(value)))
        .collect::<Vec<_>>()
        .join("&");
    format!("https://accounts.google.com/o/oauth2/v2/auth?{query}")
}

#[derive(Debug, Deserialize)]
struct GoogleTokenResponse {
    id_token: String,
}

#[derive(Debug, Deserialize)]
struct GoogleTokenInfo {
    aud: String,
    email: String,
    #[serde(default)]
    email_verified: serde_json::Value,
    name: Option<String>,
}

struct GoogleProfile {
    email: String,
    name: Option<String>,
}

async fn exchange_google_code(
    google: &crate::config::GoogleOAuthConfig,
    redirect_uri: &str,
    code: &str,
) -> Result<GoogleTokenResponse, ApiError> {
    let response = reqwest::Client::new()
        .post("https://oauth2.googleapis.com/token")
        .form(&[
            ("client_id", google.client_id.as_str()),
            ("client_secret", google.client_secret.as_str()),
            ("code", code),
            ("grant_type", "authorization_code"),
            ("redirect_uri", redirect_uri),
        ])
        .send()
        .await
        .map_err(|err| {
            ApiError::Unavailable(format!("Google OAuth token exchange failed: {err}"))
        })?;
    let status = response.status();
    let body = response.text().await.map_err(|err| {
        ApiError::Unavailable(format!("Google OAuth token response failed: {err}"))
    })?;
    if !status.is_success() {
        return Err(ApiError::BadRequest(format!(
            "Google OAuth token exchange failed: {body}"
        )));
    }
    serde_json::from_str::<GoogleTokenResponse>(&body).map_err(|err| {
        ApiError::Unavailable(format!("Google OAuth token response was invalid: {err}"))
    })
}

async fn verify_google_id_token(
    client_id: &str,
    id_token: &str,
) -> Result<GoogleProfile, ApiError> {
    let response = reqwest::Client::new()
        .get("https://oauth2.googleapis.com/tokeninfo")
        .query(&[("id_token", id_token)])
        .send()
        .await
        .map_err(|err| {
            ApiError::Unavailable(format!("Google OAuth token verification failed: {err}"))
        })?;
    let status = response.status();
    let body = response.text().await.map_err(|err| {
        ApiError::Unavailable(format!("Google OAuth tokeninfo response failed: {err}"))
    })?;
    if !status.is_success() {
        return Err(ApiError::BadRequest(format!(
            "Google OAuth token verification failed: {body}"
        )));
    }
    let token = serde_json::from_str::<GoogleTokenInfo>(&body).map_err(|err| {
        ApiError::Unavailable(format!(
            "Google OAuth tokeninfo response was invalid: {err}"
        ))
    })?;
    if token.aud != client_id {
        return Err(ApiError::Unauthorized);
    }
    if !json_truthy(&token.email_verified) {
        return Err(ApiError::Forbidden);
    }
    Ok(GoogleProfile {
        email: token.email,
        name: token.name,
    })
}

fn json_truthy(value: &serde_json::Value) -> bool {
    match value {
        serde_json::Value::Bool(value) => *value,
        serde_json::Value::String(value) => value.eq_ignore_ascii_case("true") || value == "1",
        serde_json::Value::Number(value) => value.as_i64() == Some(1),
        _ => false,
    }
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

async fn send_invitation_email(state: &AppState, email: &str, token: &str) -> Result<(), ApiError> {
    let base_url = state
        .cfg
        .app_base_url
        .as_deref()
        .or(state.cfg.auth.public_base_url.as_deref())
        .unwrap_or("")
        .trim_end_matches('/');
    let path = format!("/settings/organization?invite_token={token}");
    let accept_url = if base_url.is_empty() {
        path
    } else {
        format!("{base_url}{path}")
    };
    if state.cfg.email_from.is_none() {
        tracing::info!(
            target_email = %email,
            accept_url = %accept_url,
            "NANOTRACE_EMAIL_FROM is not configured; logging invitation accept link"
        );
        return Ok(());
    }
    send_text_email(
        state,
        email,
        "You have been invited to Nanotrace",
        &format!(
            "Use this link to accept your Nanotrace organization invitation:\n\n{accept_url}\n\nSign in with this email address before accepting."
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
    path = "/v1/organizations",
    responses((status = 200, description = "Organizations for the authenticated identity.", body = OrganizationListApiResponse)),
    security(("bearerAuth" = [])),
    tag = "Organizations"
)]
pub(crate) async fn list_organizations(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<OrganizationListApiResponse>, ApiError> {
    let identity = authorize_any(&state, &headers).await?;
    if matches!(identity.auth_type, nanotrace_auth::AuthType::ApiKey) {
        return Ok(Json(OrganizationListApiResponse {
            active_organization_id: Some(identity.organization_id.clone()),
            organizations: vec![OrganizationListItem {
                organization_id: identity.organization_id,
                organization_name: identity.organization_name,
                slug: None,
                role: role_name(identity.role).to_string(),
                created_at: None,
                updated_at: None,
            }],
        }));
    }
    let response = auth_store(&state)?
        .list_session_organizations(
            &identity.subject,
            (!identity.organization_id.is_empty()).then_some(identity.organization_id.as_str()),
        )
        .await?;
    Ok(Json(OrganizationListApiResponse {
        active_organization_id: response.active_organization_id,
        organizations: response
            .organizations
            .into_iter()
            .map(OrganizationListItem::from)
            .collect(),
    }))
}

#[utoipa::path(
    post,
    path = "/v1/organizations",
    request_body = CreateOrganizationRequest,
    responses((status = 200, description = "Created organization.", body = OrganizationResponse)),
    security(("bearerAuth" = [])),
    tag = "Organizations"
)]
pub(crate) async fn create_organization(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<CreateOrganizationRequest>,
) -> Result<Json<OrganizationResponse>, ApiError> {
    let identity = authorize_session(&state, &headers).await?;
    let organization = auth_store(&state)?
        .create_organization_for_session(
            &headers,
            &identity.subject,
            request.name,
            request.slug.as_deref(),
        )
        .await?;
    if state.cfg.clickhouse_url.is_some() {
        state
            .definitions
            .seed_sdk_defaults(&organization.id)
            .await?;
    }
    audit_account_event(
        &state,
        "organization.created",
        &identity,
        Some(&organization.id),
        None,
        None,
        serde_json::json!({ "slug": &organization.slug, "name": &organization.name }),
    )
    .await?;
    tracing::info!(
        actor_subject = %identity.subject,
        organization_id = %organization.id,
        "organization created"
    );
    Ok(Json(OrganizationResponse { organization }))
}

#[utoipa::path(
    patch,
    path = "/v1/organizations/{organization_id}",
    params(("organization_id" = String, Path, description = "Organization id.")),
    request_body = UpdateOrganizationRequest,
    responses((status = 200, description = "Updated organization.", body = OrganizationResponse)),
    security(("bearerAuth" = [])),
    tag = "Organizations"
)]
pub(crate) async fn update_organization(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(organization_id): Path<String>,
    Json(request): Json<UpdateOrganizationRequest>,
) -> Result<Json<OrganizationResponse>, ApiError> {
    let identity = require_session_organization_admin(&state, &headers, &organization_id).await?;
    let organization = auth_store(&state)?
        .update_organization(&organization_id, request.name, request.slug.as_deref())
        .await?;
    audit_account_event(
        &state,
        "organization.updated",
        &identity,
        Some(&organization.id),
        None,
        None,
        serde_json::json!({ "slug": &organization.slug, "name": &organization.name }),
    )
    .await?;
    tracing::info!(
        actor_subject = %identity.subject,
        organization_id = %organization.id,
        "organization updated"
    );
    Ok(Json(OrganizationResponse { organization }))
}

#[utoipa::path(
    delete,
    path = "/v1/organizations/{organization_id}",
    params(("organization_id" = String, Path, description = "Organization id.")),
    responses((status = 200, description = "Archived organization.", body = OrganizationResponse)),
    security(("bearerAuth" = [])),
    tag = "Organizations"
)]
pub(crate) async fn archive_organization(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(organization_id): Path<String>,
) -> Result<Json<OrganizationResponse>, ApiError> {
    let identity = require_session_organization_admin(&state, &headers, &organization_id).await?;
    let organization = auth_store(&state)?
        .archive_organization(&organization_id)
        .await?;
    audit_account_event(
        &state,
        "organization.archived",
        &identity,
        Some(&organization.id),
        None,
        None,
        serde_json::json!({ "slug": &organization.slug }),
    )
    .await?;
    tracing::info!(
        actor_subject = %identity.subject,
        organization_id = %organization.id,
        "organization archived"
    );
    Ok(Json(OrganizationResponse { organization }))
}

#[utoipa::path(
    post,
    path = "/v1/organizations/{organization_id}/switch",
    params(("organization_id" = String, Path, description = "Organization id.")),
    responses((status = 200, description = "Switched active organization.", body = OrganizationMembershipResponse)),
    security(("bearerAuth" = [])),
    tag = "Organizations"
)]
pub(crate) async fn switch_organization(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(organization_id): Path<String>,
) -> Result<Json<OrganizationMembershipResponse>, ApiError> {
    let identity = authorize_session(&state, &headers).await?;
    let organization = auth_store(&state)?
        .switch_session_organization(&headers, &organization_id)
        .await?;
    audit_account_event(
        &state,
        "organization.switched",
        &identity,
        Some(&organization.organization_id),
        None,
        None,
        serde_json::json!({}),
    )
    .await?;
    tracing::info!(
        actor_subject = %identity.subject,
        organization_id = %organization.organization_id,
        "organization switched"
    );
    Ok(Json(OrganizationMembershipResponse { organization }))
}

#[utoipa::path(
    get,
    path = "/v1/projects",
    responses((status = 200, description = "Project list.", body = ProjectsResponse)),
    security(("bearerAuth" = [])),
    tag = "Projects"
)]
pub(crate) async fn list_projects(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<ProjectsResponse>, ApiError> {
    let identity = authorize_scope(&state, &headers, "query:read").await?;
    let projects = auth_store(&state)?
        .list_projects(&identity.organization_id, false)
        .await?;
    Ok(Json(ProjectsResponse { projects }))
}

#[utoipa::path(
    post,
    path = "/v1/projects",
    request_body = CreateProjectRequest,
    responses((status = 200, description = "Created project.", body = ProjectResponse)),
    security(("bearerAuth" = [])),
    tag = "Projects"
)]
pub(crate) async fn create_project(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<CreateProjectRequest>,
) -> Result<Json<ProjectResponse>, ApiError> {
    let identity = authorize_admin(&state, &headers).await?;
    let project = auth_store(&state)?
        .create_project(
            &identity.organization_id,
            request.name,
            request.slug.as_deref(),
        )
        .await?;
    audit_account_event(
        &state,
        "project.created",
        &identity,
        Some(&identity.organization_id),
        None,
        None,
        serde_json::json!({ "project_id": &project.id, "slug": &project.slug }),
    )
    .await?;
    Ok(Json(ProjectResponse { project }))
}

#[utoipa::path(
    patch,
    path = "/v1/projects/{project_id}",
    params(("project_id" = String, Path, description = "Project id.")),
    request_body = UpdateProjectRequest,
    responses((status = 200, description = "Updated project.", body = ProjectResponse)),
    security(("bearerAuth" = [])),
    tag = "Projects"
)]
pub(crate) async fn update_project(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(project_id): Path<String>,
    Json(request): Json<UpdateProjectRequest>,
) -> Result<Json<ProjectResponse>, ApiError> {
    let identity = authorize_admin(&state, &headers).await?;
    let project = auth_store(&state)?
        .update_project(
            &identity.organization_id,
            &project_id,
            request.name,
            request.slug.as_deref(),
        )
        .await?;
    audit_account_event(
        &state,
        "project.updated",
        &identity,
        Some(&identity.organization_id),
        None,
        None,
        serde_json::json!({ "project_id": &project.id, "slug": &project.slug }),
    )
    .await?;
    Ok(Json(ProjectResponse { project }))
}

#[utoipa::path(
    delete,
    path = "/v1/projects/{project_id}",
    params(("project_id" = String, Path, description = "Project id.")),
    responses((status = 200, description = "Archived project.", body = ProjectResponse)),
    security(("bearerAuth" = [])),
    tag = "Projects"
)]
pub(crate) async fn archive_project(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(project_id): Path<String>,
) -> Result<Json<ProjectResponse>, ApiError> {
    let identity = authorize_admin(&state, &headers).await?;
    let project = auth_store(&state)?
        .archive_project(&identity.organization_id, &project_id)
        .await?;
    audit_account_event(
        &state,
        "project.archived",
        &identity,
        Some(&identity.organization_id),
        None,
        None,
        serde_json::json!({ "project_id": &project.id, "slug": &project.slug }),
    )
    .await?;
    Ok(Json(ProjectResponse { project }))
}

#[utoipa::path(
    post,
    path = "/v1/organizations/{organization_id}/leave",
    params(("organization_id" = String, Path, description = "Organization id.")),
    responses((status = 200, description = "Left organization.", body = OrganizationMemberResponse)),
    security(("bearerAuth" = [])),
    tag = "Organizations"
)]
pub(crate) async fn leave_organization(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(organization_id): Path<String>,
) -> Result<Json<OrganizationMemberResponse>, ApiError> {
    let identity = authorize_session(&state, &headers).await?;
    let member = auth_store(&state)?
        .remove_organization_member(&organization_id, &identity.subject)
        .await?;
    audit_account_event(
        &state,
        "organization.left",
        &identity,
        Some(&organization_id),
        Some(&identity.subject),
        identity.email.as_deref(),
        serde_json::json!({}),
    )
    .await?;
    tracing::info!(
        actor_subject = %identity.subject,
        organization_id = %organization_id,
        "organization left"
    );
    Ok(Json(OrganizationMemberResponse { member }))
}

#[utoipa::path(
    get,
    path = "/v1/organizations/{organization_id}/members",
    params(("organization_id" = String, Path, description = "Organization id.")),
    responses((status = 200, description = "Organization members.", body = OrganizationMembersResponse)),
    security(("bearerAuth" = [])),
    tag = "Organizations"
)]
pub(crate) async fn list_organization_members(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(organization_id): Path<String>,
) -> Result<Json<OrganizationMembersResponse>, ApiError> {
    require_session_organization_admin(&state, &headers, &organization_id).await?;
    let members = auth_store(&state)?
        .list_organization_members(&organization_id)
        .await?;
    Ok(Json(OrganizationMembersResponse { members }))
}

#[utoipa::path(
    patch,
    path = "/v1/organizations/{organization_id}/members/{subject}",
    params(
        ("organization_id" = String, Path, description = "Organization id."),
        ("subject" = String, Path, description = "Member subject.")
    ),
    request_body = UpdateOrganizationMemberRequest,
    responses((status = 200, description = "Updated organization member.", body = OrganizationMemberResponse)),
    security(("bearerAuth" = [])),
    tag = "Organizations"
)]
pub(crate) async fn update_organization_member(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((organization_id, subject)): Path<(String, String)>,
    Json(request): Json<UpdateOrganizationMemberRequest>,
) -> Result<Json<OrganizationMemberResponse>, ApiError> {
    let identity = require_session_organization_admin(&state, &headers, &organization_id).await?;
    let role = parse_member_role(&request.role)?;
    let member = auth_store(&state)?
        .set_organization_member_role(&organization_id, &subject, role)
        .await?;
    audit_account_event(
        &state,
        "organization.member_role_updated",
        &identity,
        Some(&organization_id),
        Some(&member.subject),
        Some(&member.email),
        serde_json::json!({ "role": role_name(member.role) }),
    )
    .await?;
    tracing::info!(
        actor_subject = %identity.subject,
        organization_id = %organization_id,
        target_subject = %member.subject,
        role = role_name(member.role),
        "organization member role updated"
    );
    Ok(Json(OrganizationMemberResponse { member }))
}

#[utoipa::path(
    delete,
    path = "/v1/organizations/{organization_id}/members/{subject}",
    params(
        ("organization_id" = String, Path, description = "Organization id."),
        ("subject" = String, Path, description = "Member subject.")
    ),
    responses((status = 200, description = "Removed organization member.", body = OrganizationMemberResponse)),
    security(("bearerAuth" = [])),
    tag = "Organizations"
)]
pub(crate) async fn remove_organization_member(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((organization_id, subject)): Path<(String, String)>,
) -> Result<Json<OrganizationMemberResponse>, ApiError> {
    let identity = require_session_organization_admin(&state, &headers, &organization_id).await?;
    let member = auth_store(&state)?
        .remove_organization_member(&organization_id, &subject)
        .await?;
    audit_account_event(
        &state,
        "organization.member_removed",
        &identity,
        Some(&organization_id),
        Some(&member.subject),
        Some(&member.email),
        serde_json::json!({}),
    )
    .await?;
    tracing::info!(
        actor_subject = %identity.subject,
        organization_id = %organization_id,
        target_subject = %member.subject,
        "organization member removed"
    );
    Ok(Json(OrganizationMemberResponse { member }))
}

#[utoipa::path(
    get,
    path = "/v1/organizations/{organization_id}/invitations",
    params(("organization_id" = String, Path, description = "Organization id.")),
    responses((status = 200, description = "Organization invitations.", body = OrganizationInvitationsResponse)),
    security(("bearerAuth" = [])),
    tag = "Organizations"
)]
pub(crate) async fn list_organization_invitations(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(organization_id): Path<String>,
) -> Result<Json<OrganizationInvitationsResponse>, ApiError> {
    require_session_organization_admin(&state, &headers, &organization_id).await?;
    let invitations = auth_store(&state)?
        .list_organization_invitations(&organization_id)
        .await?;
    Ok(Json(OrganizationInvitationsResponse { invitations }))
}

#[utoipa::path(
    post,
    path = "/v1/organizations/{organization_id}/invitations",
    params(("organization_id" = String, Path, description = "Organization id.")),
    request_body = CreateOrganizationInvitationRequest,
    responses((status = 200, description = "Created organization invitation.", body = OrganizationInvitationResponse)),
    security(("bearerAuth" = [])),
    tag = "Organizations"
)]
pub(crate) async fn create_organization_invitation(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(organization_id): Path<String>,
    Json(request): Json<CreateOrganizationInvitationRequest>,
) -> Result<Json<OrganizationInvitationResponse>, ApiError> {
    let identity = require_session_organization_admin(&state, &headers, &organization_id).await?;
    let role = parse_member_role(&request.role)?;
    let created = auth_store(&state)?
        .create_organization_invitation(&organization_id, &request.email, role, &identity.subject)
        .await?;
    if let Some(token) = created.token.as_deref() {
        send_invitation_email(&state, &created.invitation.email, token).await?;
    }
    audit_account_event(
        &state,
        "organization.invitation_created",
        &identity,
        Some(&organization_id),
        None,
        Some(&created.invitation.email),
        serde_json::json!({ "role": role_name(created.invitation.role), "invitation_id": created.invitation.id }),
    )
    .await?;
    tracing::info!(
        actor_subject = %identity.subject,
        organization_id = %organization_id,
        target_email = %created.invitation.email,
        "organization invitation created"
    );
    Ok(Json(OrganizationInvitationResponse {
        invitation: created.invitation,
    }))
}

#[utoipa::path(
    delete,
    path = "/v1/organizations/{organization_id}/invitations/{invitation_id}",
    params(
        ("organization_id" = String, Path, description = "Organization id."),
        ("invitation_id" = i64, Path, description = "Invitation id.")
    ),
    responses((status = 200, description = "Revoked organization invitation.", body = OrganizationInvitationResponse)),
    security(("bearerAuth" = [])),
    tag = "Organizations"
)]
pub(crate) async fn revoke_organization_invitation(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((organization_id, invitation_id)): Path<(String, i64)>,
) -> Result<Json<OrganizationInvitationResponse>, ApiError> {
    let identity = require_session_organization_admin(&state, &headers, &organization_id).await?;
    let invitation = auth_store(&state)?
        .revoke_organization_invitation(&organization_id, invitation_id)
        .await?;
    audit_account_event(
        &state,
        "organization.invitation_revoked",
        &identity,
        Some(&organization_id),
        None,
        Some(&invitation.email),
        serde_json::json!({ "invitation_id": invitation.id }),
    )
    .await?;
    tracing::info!(
        actor_subject = %identity.subject,
        organization_id = %organization_id,
        target_email = %invitation.email,
        "organization invitation revoked"
    );
    Ok(Json(OrganizationInvitationResponse { invitation }))
}

#[utoipa::path(
    post,
    path = "/v1/organizations/{organization_id}/invitations/{invitation_id}/resend",
    params(
        ("organization_id" = String, Path, description = "Organization id."),
        ("invitation_id" = i64, Path, description = "Invitation id.")
    ),
    responses((status = 200, description = "Resent organization invitation.", body = OrganizationInvitationResponse)),
    security(("bearerAuth" = [])),
    tag = "Organizations"
)]
pub(crate) async fn resend_organization_invitation(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((organization_id, invitation_id)): Path<(String, i64)>,
) -> Result<Json<OrganizationInvitationResponse>, ApiError> {
    let identity = require_session_organization_admin(&state, &headers, &organization_id).await?;
    let resent = auth_store(&state)?
        .resend_organization_invitation(&organization_id, invitation_id)
        .await?;
    if let Some(token) = resent.token.as_deref() {
        send_invitation_email(&state, &resent.invitation.email, token).await?;
    }
    audit_account_event(
        &state,
        "organization.invitation_resent",
        &identity,
        Some(&organization_id),
        None,
        Some(&resent.invitation.email),
        serde_json::json!({ "invitation_id": resent.invitation.id }),
    )
    .await?;
    tracing::info!(
        actor_subject = %identity.subject,
        organization_id = %organization_id,
        target_email = %resent.invitation.email,
        "organization invitation resent"
    );
    Ok(Json(OrganizationInvitationResponse {
        invitation: resent.invitation,
    }))
}

#[utoipa::path(
    post,
    path = "/v1/organization-invitations/accept",
    request_body = AcceptOrganizationInvitationRequest,
    responses((status = 200, description = "Accepted organization invitation.", body = OrganizationMembershipResponse)),
    security(("bearerAuth" = [])),
    tag = "Organizations"
)]
pub(crate) async fn accept_organization_invitation(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<AcceptOrganizationInvitationRequest>,
) -> Result<Json<OrganizationMembershipResponse>, ApiError> {
    let identity = authorize_session(&state, &headers).await?;
    let organization = auth_store(&state)?
        .accept_organization_invitation(&headers, &request.token)
        .await?;
    audit_account_event(
        &state,
        "organization.invitation_accepted",
        &identity,
        Some(&organization.organization_id),
        Some(&identity.subject),
        identity.email.as_deref(),
        serde_json::json!({ "role": role_name(organization.role) }),
    )
    .await?;
    tracing::info!(
        actor_subject = %identity.subject,
        organization_id = %organization.organization_id,
        "organization invitation accepted"
    );
    Ok(Json(OrganizationMembershipResponse { organization }))
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
            &request.project_ids,
            request.default_project_id.as_deref(),
            &identity.subject,
            request.expires_at,
        )
        .await?;
    audit_account_event(
        &state,
        "api_key.created",
        &identity,
        Some(&identity.organization_id),
        None,
        None,
        serde_json::json!({ "api_key_id": created.api_key.id, "role": role_name(created.api_key.role) }),
    )
    .await?;
    tracing::info!(
        actor_subject = %identity.subject,
        organization_id = %identity.organization_id,
        api_key_id = created.api_key.id,
        "api key created"
    );
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
    audit_account_event(
        &state,
        "api_key.revoked",
        &identity,
        Some(&identity.organization_id),
        None,
        None,
        serde_json::json!({ "api_key_id": api_key.id }),
    )
    .await?;
    tracing::info!(
        actor_subject = %identity.subject,
        organization_id = %identity.organization_id,
        api_key_id = api_key.id,
        "api key revoked"
    );
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

async fn authorize_session(
    state: &AppState,
    headers: &HeaderMap,
) -> Result<AuthIdentity, ApiError> {
    let identity = authorize_any(state, headers).await?;
    if matches!(identity.auth_type, nanotrace_auth::AuthType::Session) {
        Ok(identity)
    } else {
        Err(ApiError::Forbidden)
    }
}

async fn require_session_organization_admin(
    state: &AppState,
    headers: &HeaderMap,
    organization_id: &str,
) -> Result<AuthIdentity, ApiError> {
    let identity = authorize_session(state, headers).await?;
    if auth_store(state)?
        .is_organization_admin(organization_id, &identity.subject)
        .await?
    {
        Ok(identity)
    } else {
        Err(ApiError::Forbidden)
    }
}

async fn audit_account_event(
    state: &AppState,
    event_type: &str,
    identity: &AuthIdentity,
    organization_id: Option<&str>,
    target_subject: Option<&str>,
    target_email: Option<&str>,
    metadata: serde_json::Value,
) -> Result<(), ApiError> {
    auth_store(state)?
        .record_account_audit_event(
            event_type,
            &identity.subject,
            auth_type_name(identity.auth_type),
            organization_id,
            target_subject,
            target_email,
            metadata,
        )
        .await?;
    Ok(())
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

fn require_explicit_scope(identity: &AuthIdentity, scope: &str) -> Result<(), ApiError> {
    if identity.scopes.iter().any(|candidate| candidate == scope) {
        Ok(())
    } else {
        Err(ApiError::Forbidden)
    }
}

fn apply_authorized_project_scope(
    identity: &AuthIdentity,
    request: &mut QueryApiRequest,
) -> Result<(), ApiError> {
    let Some(requested_project_ids) = request.project_scope_project_ids() else {
        if identity.project_ids.is_empty() {
            return Ok(());
        }
        return Err(ApiError::Forbidden);
    };
    if identity.project_ids.is_empty() {
        return Ok(());
    }
    if requested_project_ids.is_empty() {
        request.set_project_scope_project_ids(identity.project_ids.clone());
        return Ok(());
    }
    if requested_project_ids.iter().all(|project_id| {
        identity
            .project_ids
            .iter()
            .any(|allowed| allowed == project_id)
    }) {
        Ok(())
    } else {
        Err(ApiError::Forbidden)
    }
}

fn authorize_deletion_project_scope(
    identity: &AuthIdentity,
    project_scope: &crate::read::ProjectScope,
) -> Result<(), ApiError> {
    if identity.project_ids.is_empty() {
        return Ok(());
    }
    let requested_project_ids = project_scope
        .project_ids
        .iter()
        .map(|project_id| project_id.trim())
        .filter(|project_id| !project_id.is_empty())
        .collect::<Vec<_>>();
    if requested_project_ids.is_empty() {
        return Err(ApiError::Forbidden);
    }
    if requested_project_ids.iter().all(|project_id| {
        identity
            .project_ids
            .iter()
            .any(|allowed| allowed == project_id)
    }) {
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

fn parse_member_role(value: &str) -> Result<AuthRole, ApiError> {
    match value.trim().to_ascii_lowercase().as_str() {
        "admin" => Ok(AuthRole::Admin),
        "viewer" => Ok(AuthRole::Viewer),
        other => Err(ApiError::BadRequest(format!(
            "invalid organization member role: {other}"
        ))),
    }
}

fn role_name(role: AuthRole) -> &'static str {
    match role {
        AuthRole::Admin => "admin",
        AuthRole::Service => "service",
        AuthRole::Viewer => "viewer",
    }
}

fn auth_type_name(auth_type: nanotrace_auth::AuthType) -> &'static str {
    match auth_type {
        nanotrace_auth::AuthType::ApiKey => "api_key",
        nanotrace_auth::AuthType::Session => "session",
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
    Deletion(crate::deletions::DeletionStoreError),
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
            Self::Deletion(DeletionStoreError::InvalidRequest(message)) => {
                (StatusCode::BAD_REQUEST, message)
            }
            Self::Deletion(DeletionStoreError::NotFound) => {
                (StatusCode::NOT_FOUND, "not_found".to_string())
            }
            Self::Deletion(DeletionStoreError::ClickHouseNotConfigured) => (
                StatusCode::SERVICE_UNAVAILABLE,
                "ClickHouse is not configured".to_string(),
            ),
            Self::Deletion(DeletionStoreError::ClickHouseResponse { status, body }) => {
                let status = if status.is_client_error() {
                    StatusCode::BAD_REQUEST
                } else {
                    StatusCode::BAD_GATEWAY
                };
                (status, body)
            }
            Self::Deletion(err) => (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()),
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

impl From<crate::deletions::DeletionStoreError> for ApiError {
    fn from(value: crate::deletions::DeletionStoreError) -> Self {
        Self::Deletion(value)
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
