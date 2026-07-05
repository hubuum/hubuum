use actix_web::{App, http::StatusCode, test, web};
use rstest::rstest;

use crate::observability::metrics;
use crate::tests::{TestContext, test_context};

#[rstest]
#[actix_web::test]
async fn metrics_endpoint_exports_prometheus_text(#[future(awt)] test_context: TestContext) {
    let context = test_context;
    metrics::init().unwrap();
    metrics::http_request_started();
    metrics::http_request_finished("GET", "/test", 200, std::time::Duration::from_millis(1));

    let app = test::init_service(
        App::new()
            .app_data(context.pool.clone())
            .route("/metrics", web::get().to(metrics::scrape)),
    )
    .await;

    let req = test::TestRequest::get().uri("/metrics").to_request();
    let response = test::call_service(&app, req).await;

    assert_eq!(response.status(), StatusCode::OK);
    let body = test::read_body(response).await;
    let body = std::str::from_utf8(&body).unwrap();

    assert!(body.contains("# TYPE hubuum_http_requests_total counter"));
    assert!(body.contains("hubuum_http_requests_total"));
    assert!(body.contains("hubuum_inventory_entities"));
    assert!(body.contains("entity_type=\"namespaces\""));
}
