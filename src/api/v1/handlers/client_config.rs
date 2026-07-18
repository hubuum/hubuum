use actix_web::{Responder, get, http::StatusCode, web};

use crate::api::openapi::ApiErrorResponse;
use crate::api::response::ApiResponse;
use crate::config::running::{ClientConfig, RunningConfig};
use crate::errors::ApiError;

#[utoipa::path(
    get,
    path = "/api/v1/config",
    tag = "config",
    responses(
        (status = 200, description = "Effective client-safe configuration", body = ClientConfig),
        (status = 500, description = "Internal server error", body = ApiErrorResponse)
    )
)]
#[get("/config")]
pub async fn get_client_config(
    config: web::Data<RunningConfig>,
) -> Result<impl Responder, ApiError> {
    Ok(ApiResponse::new(
        ClientConfig::from(config.get_ref()),
        StatusCode::OK,
    ))
}

#[cfg(test)]
mod tests {
    use actix_web::{App, http::StatusCode, test, web};
    use clap::Parser;

    use super::*;
    use crate::config::AppConfig;

    #[actix_web::test]
    async fn client_config_exposes_effective_pagination_limits_without_authentication() {
        let app_config = AppConfig::parse_from([
            "hubuum",
            "--default-page-limit",
            "125",
            "--max-page-limit",
            "500",
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
    }
}
