use crate::tests::TestContext;
use actix_web::{App, http::StatusCode, middleware::from_fn, test};
use hubuum_storage_postgres::capture_queries;
use std::time::Instant;

#[actix_web::test]
async fn complete_authenticated_requests_reuse_authentication_without_read_audit_transactions() {
    let context = TestContext::new().await;
    let fixture = context
        .collection_fixture("authenticated_performance")
        .await;
    let app = test::init_service(
        App::new()
            .app_data(crate::tests::app_context(&context.pool))
            .wrap(from_fn(crate::middlewares::actor_context))
            .configure(crate::api::config),
    )
    .await;
    let uri = format!("/api/v1/collections/{}", fixture.collection.id);
    let mut durations = Vec::new();
    let mut total_queries = 0;
    for sample in 0..21 {
        let request = test::TestRequest::get()
            .uri(&uri)
            .insert_header(("Authorization", format!("Bearer {}", context.admin_token)))
            .to_request();
        let started = Instant::now();
        let (response, queries) = capture_queries(test::call_service(&app, request)).await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = test::read_body(response).await;
        assert!(!body.is_empty());
        if sample > 0 {
            durations.push(started.elapsed().as_micros());
            total_queries += queries.total_queries();
            assert_eq!(queries.control_queries(), 0, "{:?}", queries.query_counts());
        }
    }
    durations.sort_unstable();
    eprintln!(
        "PERFORMANCE_EVIDENCE {}",
        serde_json::json!({"scenario":"authenticated_collection_http", "samples":durations.len(), "p50_us":durations[10], "p95_us":durations[19], "total_queries":total_queries})
    );
    fixture.cleanup().await.unwrap();
    context
        .admin_user
        .delete_without_events(&context.pool)
        .await
        .unwrap();
    context
        .normal_user
        .delete_without_events(&context.pool)
        .await
        .unwrap();
}

#[actix_web::test]
async fn complete_invalid_authentication_is_not_retried_by_the_handler_extractor() {
    let context = TestContext::new().await;
    let app = test::init_service(
        App::new()
            .app_data(crate::tests::app_context(&context.pool))
            .wrap(from_fn(crate::middlewares::actor_context))
            .configure(crate::api::config),
    )
    .await;
    let request = test::TestRequest::get()
        .uri("/api/v1/collections/1")
        .insert_header(("Authorization", "Bearer invalid-performance-token"))
        .to_request();
    let (response, queries) = capture_queries(test::call_service(&app, request)).await;
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(
        queries.connection_checkouts(),
        1,
        "{:?}",
        queries.query_counts()
    );
    context
        .admin_user
        .delete_without_events(&context.pool)
        .await
        .unwrap();
    context
        .normal_user
        .delete_without_events(&context.pool)
        .await
        .unwrap();
}
