use actix_web::{App, http::StatusCode, test};
use serde_json::Value;

use crate::api as prod_api;
use crate::tests::get_test_pool;

#[actix_web::test]
async fn test_healthz_returns_ok_without_database_pool() {
    let app = test::init_service(App::new().configure(prod_api::config)).await;

    let req = test::TestRequest::get().uri("/healthz").to_request();
    let response = test::call_service(&app, req).await;

    assert_eq!(response.status(), StatusCode::OK);

    let body: Value = test::read_body_json(response).await;
    assert_eq!(body["status"], "ok");
}

#[actix_web::test]
async fn test_readyz_checks_database_connectivity() {
    let app = test::init_service(
        App::new()
            .app_data(get_test_pool())
            .configure(prod_api::config),
    )
    .await;

    let req = test::TestRequest::get().uri("/readyz").to_request();
    let response = test::call_service(&app, req).await;

    assert_eq!(response.status(), StatusCode::OK);

    let body: Value = test::read_body_json(response).await;
    assert_eq!(body["status"], "ready");
}
