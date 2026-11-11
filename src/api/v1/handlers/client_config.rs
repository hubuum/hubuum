use actix_web::{HttpRequest, Responder, get, http::StatusCode, web};

use crate::api::openapi::ApiErrorResponse;
use crate::api::response::ApiResponse;
use crate::config::running::{ClientConfig, RunningConfig};
use crate::errors::ApiError;
use crate::pagination::PageLimits;
use crate::permissions::AppContext;

#[utoipa::path(
    get,
    path = "/api/v1/config",
    tag = "config",
    responses(
        (status = 200, description = "Effective client-safe configuration, including runtime-managed page limits applied by the responding replica", body = ClientConfig),
        (status = 500, description = "Internal server error", body = ApiErrorResponse)
    )
)]
#[get("/config")]
pub async fn get_client_config(
    config: web::Data<RunningConfig>,
    request: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let mut client = ClientConfig::from(config.get_ref());
    let policy = request
        .app_data::<web::Data<AppContext>>()
        .and_then(|context| {
            context
                .backend()
                .applied_runtime_settings()
                .snapshot()
                .and_then(|snapshot| snapshot.pagination())
        });
    let limits = PageLimits::new(
        client.pagination.default_page_limit,
        client.pagination.max_page_limit,
    )?
    .with_runtime_policy(policy);
    client.pagination.default_page_limit = limits.default_limit();
    client.pagination.max_page_limit = limits.maximum_limit();
    if let Some(context) = request.app_data::<web::Data<AppContext>>() {
        let effective = crate::config::runtime::for_storage(context.backend())?;
        client.authentication.default_token_lifetime_hours = effective.token_lifetime_hours;
    }
    Ok(ApiResponse::new(client, StatusCode::OK))
}

#[cfg(test)]
mod tests {
    use actix_web::{App, http::StatusCode, test, web};
    use clap::Parser;

    use super::*;
    use crate::config::AppConfig;

    #[actix_web::test]
    async fn client_config_exposes_effective_client_defaults_without_authentication() {
        let app_config = AppConfig::parse_from([
            "hubuum",
            "--default-page-limit",
            "125",
            "--max-page-limit",
            "500",
            "--token-lifetime-hours",
            "72",
            "--max-token-lifetime-hours",
            "720",
        ]);
        let running_config = RunningConfig::from(&app_config);
        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(running_config))
                .service(get_client_config),
        )
        .await;

        let response =
            test::call_service(&app, test::TestRequest::get().uri("/config").to_request()).await;

        assert_eq!(response.status(), StatusCode::OK);
        let body: serde_json::Value = test::read_body_json(response).await;
        assert_eq!(body["pagination"]["default_page_limit"], 125);
        assert_eq!(body["pagination"]["max_page_limit"], 500);
        assert_eq!(body["authentication"]["default_token_lifetime_hours"], 72);
        assert_eq!(body["authentication"]["max_token_lifetime_hours"], 720);
    }
}
