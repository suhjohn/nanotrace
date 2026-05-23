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
        crate::http::metrics,
        crate::http::post_events,
        crate::http::post_events_query,
        crate::http::get_event,
        crate::http::post_query,
        crate::http::list_definitions,
        crate::http::create_definition,
        crate::http::seed_sdk_definitions,
        crate::http::delete_definition,
        crate::http::backfill_definition,
        crate::http::list_reports,
        crate::http::create_report,
        crate::http::delete_report,
        crate::http::list_processors,
        crate::http::put_processor,
        crate::http::delete_processor,
        crate::http::list_dashboard_visualizations,
        crate::http::create_dashboard_visualization,
        crate::http::clear_dashboard_visualizations,
        crate::http::update_dashboard_visualization,
        crate::http::delete_dashboard_visualization,
        crate::http::auth_login,
        crate::http::auth_logout,
        crate::http::auth_me,
        crate::http::list_api_keys,
        crate::http::create_api_key,
        crate::http::revoke_api_key,
    ),
    components(schemas(
        ErrorResponse,
        crate::event_log::WriteReceipt,
        crate::http::PostEventsResponse,
        crate::http::CompactBatchReceipt,
        crate::read::QueryRequest,
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
        crate::definitions::CreateDefinitionRequest,
        crate::definitions::BackfillRequest,
        crate::definitions::DefinitionRecord,
        crate::definitions::DefinitionListResponse,
        crate::definitions::DefinitionMutationResponse,
        crate::definitions::BackfillResponse,
        crate::reports::CreateReportRequest,
        crate::reports::ReportRecord,
        crate::reports::ReportListResponse,
        crate::processors::PutProcessorRequest,
        crate::processors::ProcessorStageRequest,
        crate::processors::ProcessorListResponse,
        crate::dashboards::CreateVisualizationRequest,
        crate::dashboards::UpdateVisualizationRequest,
        crate::dashboards::DashboardVisualization,
        crate::dashboards::DashboardVisualizationsResponse,
        crate::http::LoginRequest,
        crate::http::LoginResponse,
        crate::http::CreateApiKeyRequest,
        crate::http::ApiKeysResponse,
    )),
    modifiers(&SecurityAddon),
    tags(
        (name = "OpenAPI", description = "OpenAPI document"),
        (name = "Health", description = "Health and metrics endpoints"),
        (name = "Events", description = "Event ingest and structured event queries"),
        (name = "Query", description = "Read-only SQL query endpoint"),
        (name = "Definitions", description = "Definition management and backfills"),
        (name = "Reports", description = "Report definition management"),
        (name = "Processors", description = "Processor management"),
        (name = "Dashboards", description = "Dashboard visualization persistence"),
        (name = "Auth", description = "Browser session authentication"),
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
        assert!(paths.contains_key("/v1/events"));
        assert!(paths.contains_key("/v1/events/query"));
        assert!(paths.contains_key("/v1/definitions"));
        assert!(
            spec.components
                .unwrap()
                .security_schemes
                .contains_key("bearerAuth")
        );
    }
}
