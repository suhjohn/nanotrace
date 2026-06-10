use serde::Serialize;
use utoipa::{
    Modify, OpenApi, ToSchema,
    openapi::{
        Components,
        security::{HttpAuthScheme, HttpBuilder, SecurityScheme},
    },
};

#[derive(OpenApi)]
#[openapi(
    paths(
        crate::http::openapi_json,
        crate::http::healthz,
        crate::http::readyz,
        crate::http::post_events,
        crate::http::get_event,
        crate::http::post_query,
        crate::http::list_query_recommendations,
        crate::http::list_definitions,
        crate::http::get_definition,
        crate::http::create_definition,
        crate::http::delete_definition,
        crate::http::backfill_definition,
        crate::http::list_backfill_jobs,
        crate::http::create_definition_backfill,
        crate::http::get_backfill_job,
        crate::http::create_deletion,
        crate::http::list_deletions,
        crate::http::get_deletion,
        crate::http::auth_providers,
        crate::http::auth_login,
        crate::http::auth_logout,
        crate::http::auth_me,
        crate::http::list_organizations,
        crate::http::create_organization,
        crate::http::update_organization,
        crate::http::archive_organization,
        crate::http::switch_organization,
        crate::http::list_projects,
        crate::http::create_project,
        crate::http::update_project,
        crate::http::archive_project,
        crate::http::leave_organization,
        crate::http::list_organization_members,
        crate::http::update_organization_member,
        crate::http::remove_organization_member,
        crate::http::list_organization_invitations,
        crate::http::create_organization_invitation,
        crate::http::revoke_organization_invitation,
        crate::http::resend_organization_invitation,
        crate::http::accept_organization_invitation,
        crate::http::list_api_keys,
        crate::http::create_api_key,
        crate::http::revoke_api_key,
    ),
    components(schemas(
        ErrorResponse,
        crate::http::KafkaAcceptedResponse,
        crate::read::MeasureQueryRequest,
        crate::read::FunnelQueryRequest,
        crate::read::CohortQueryRequest,
        crate::read::ReportQueryRequest,
        crate::read::StateQueryRequest,
        crate::read::AlertQueryRequest,
        crate::read::AlertQueryMode,
        crate::read::SearchQueryRequest,
        crate::read::EventsQueryRequest,
        crate::read::EventFilter,
        crate::read::EventFacetFilter,
        crate::read::EventFacetJoin,
        crate::read::EventFacetOperator,
        crate::read::EventTimeRange,
        crate::read::EventPage,
        crate::read::EventsQuerySort,
        crate::read::EventSortDirection,
        crate::read::GroupSortKey,
        crate::read::QueryRecommendationRecord,
        crate::read::QueryRecommendationListResponse,
        crate::definitions::DefinitionKind,
        crate::definitions::DefinitionMode,
        crate::definitions::CreateDefinitionRequest,
        crate::definitions::BackfillRequest,
        crate::definitions::DefinitionRecord,
        crate::definitions::DefinitionListResponse,
        crate::definitions::DefinitionGetResponse,
        crate::definitions::DefinitionMutationResponse,
        crate::definitions::BackfillResponse,
        crate::materializations::CreateBackfillRequest,
        crate::materializations::MaterializationJobRecord,
        crate::materializations::MaterializationChunkRecord,
        crate::materializations::BackfillJobResponse,
        crate::materializations::BackfillJobListResponse,
        crate::deletions::CreateDeletionRequest,
        crate::deletions::DeletionJobRecord,
        crate::deletions::DeletionJobResponse,
        crate::deletions::DeletionJobListResponse,
        crate::http::AuthProvidersResponse,
        crate::http::LoginRequest,
        crate::http::LoginResponse,
        crate::http::CreateOrganizationRequest,
        crate::http::UpdateOrganizationRequest,
        crate::http::CreateProjectRequest,
        crate::http::UpdateProjectRequest,
        crate::http::ProjectsResponse,
        crate::http::ProjectResponse,
        crate::http::OrganizationListApiResponse,
        crate::http::OrganizationResponse,
        crate::http::OrganizationMemberResponse,
        crate::http::OrganizationMembersResponse,
        crate::http::OrganizationMembershipResponse,
        crate::http::OrganizationInvitationResponse,
        crate::http::OrganizationInvitationsResponse,
        crate::http::UpdateOrganizationMemberRequest,
        crate::http::CreateOrganizationInvitationRequest,
        crate::http::AcceptOrganizationInvitationRequest,
        crate::http::CreateApiKeyRequest,
        crate::http::ApiKeysResponse,
    )),
    modifiers(&SecurityAddon),
    tags(
        (name = "OpenAPI", description = "OpenAPI document"),
        (name = "Health", description = "Health endpoints"),
        (name = "Events", description = "Event ingest and structured event queries"),
        (name = "Query", description = "Structured read query endpoint"),
        (name = "Definitions", description = "Definition management and backfills"),
        (name = "Backfills", description = "Historical processing jobs for definition outputs"),
        (name = "Deletions", description = "Hard-delete jobs for project-scoped event data"),
        (name = "Auth", description = "Browser session authentication"),
        (name = "Organizations", description = "Organization lifecycle and membership management"),
        (name = "Projects", description = "Organization project management"),
        (name = "API Keys", description = "API key management")
    )
)]
struct ApiDoc;

pub fn spec() -> utoipa::openapi::OpenApi {
    ApiDoc::openapi()
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ErrorResponse {
    pub error: String,
}

struct SecurityAddon;

impl Modify for SecurityAddon {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        let components = openapi.components.get_or_insert_with(Components::new);
        components.add_security_scheme(
            "bearerAuth",
            SecurityScheme::Http(
                HttpBuilder::new()
                    .scheme(HttpAuthScheme::Bearer)
                    .bearer_format("Nanotrace API key or session token")
                    .build(),
            ),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use utoipa::OpenApi;

    #[test]
    fn generated_spec_contains_core_routes() {
        let spec = ApiDoc::openapi();
        let paths = spec.paths.paths;
        assert!(paths.contains_key("/openapi.json"));
        assert!(!paths.contains_key("/v1/openapi.json"));
        assert!(!paths.contains_key("/metrics"));
        assert!(paths.contains_key("/v1/events"));
        assert!(paths.contains_key("/v1/query"));
        assert!(paths.contains_key("/v1/query/recommendations"));
        assert!(!paths.contains_key("/v1/admin/query"));
        assert!(!paths.contains_key("/v1/sql/query"));
        assert!(!paths.contains_key("/v1/events/query"));
        assert!(!paths.contains_key("/v1/measures/query"));
        assert!(!paths.contains_key("/v1/funnels/query"));
        assert!(!paths.contains_key("/v1/cohorts/query"));
        assert!(paths.contains_key("/v1/definitions"));
        assert!(paths.contains_key("/v1/definitions/{definition_id}"));
        assert!(paths.contains_key("/v1/definitions/{definition_id}/backfills"));
        assert!(!paths.contains_key("/v1/definitions/sdk-defaults"));
        assert!(paths.contains_key("/v1/backfills"));
        assert!(paths.contains_key("/v1/backfills/{job_id}"));
        assert!(paths.contains_key("/v1/deletions"));
        assert!(paths.contains_key("/v1/deletions/{deletion_id}"));
        assert!(!paths.contains_key("/v1/materializations"));
        assert!(!paths.contains_key("/v1/materializations/{job_id}"));
        assert!(!paths.contains_key("/v1/reports"));
        assert!(!paths.contains_key("/dashboards/{dashboard_id}/visualizations"));
        assert!(paths.contains_key("/v1/api-keys"));
        assert!(paths.contains_key("/auth/providers"));
        assert!(paths.contains_key("/v1/organizations"));
        assert!(paths.contains_key("/v1/organizations/{organization_id}"));
        assert!(paths.contains_key("/v1/organizations/{organization_id}/switch"));
        assert!(paths.contains_key("/v1/projects"));
        assert!(paths.contains_key("/v1/projects/{project_id}"));
        assert!(paths.contains_key("/v1/organizations/{organization_id}/leave"));
        assert!(paths.contains_key(
            "/v1/organizations/{organization_id}/invitations/{invitation_id}/resend"
        ));
        assert!(paths.contains_key("/v1/organization-invitations/accept"));
        assert!(!paths.contains_key("/api-keys"));
        let components = spec.components.unwrap();
        assert!(components.security_schemes.contains_key("bearerAuth"));
        assert!(components.schemas.contains_key("DefinitionKind"));
        assert!(components.schemas.contains_key("DefinitionMode"));
    }
}
