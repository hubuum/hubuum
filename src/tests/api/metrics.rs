use std::sync::LazyLock;
use std::time::Duration;

use actix_web::{App, http::StatusCode, test, web};
use diesel::PgConnection;
use diesel::r2d2::{ConnectionManager, Pool};
use rstest::rstest;
use tokio::sync::Mutex;

use crate::db::DbPool;
use crate::models::{TaskKind, TaskStatus};
use crate::observability::metrics;
use crate::tests::{TestContext, test_context};

static METRICS_TEST_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

#[rstest]
#[actix_web::test]
async fn metrics_endpoint_exports_prometheus_text(#[future(awt)] test_context: TestContext) {
    let _lock = METRICS_TEST_LOCK.lock().await;
    let context = test_context;
    metrics::init().unwrap();
    metrics::clear_scrape_cache_for_tests();
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

#[actix_web::test]
async fn metrics_endpoint_is_best_effort_when_database_refresh_fails() {
    let _lock = METRICS_TEST_LOCK.lock().await;
    metrics::init().unwrap();
    metrics::clear_scrape_cache_for_tests();

    let app = test::init_service(
        App::new()
            .app_data(web::Data::new(unreachable_pool()))
            .route("/metrics", web::get().to(metrics::scrape)),
    )
    .await;

    let req = test::TestRequest::get().uri("/metrics").to_request();
    let response = test::call_service(&app, req).await;

    assert_eq!(response.status(), StatusCode::OK);
    let body = test::read_body(response).await;
    let body = std::str::from_utf8(&body).unwrap();

    assert!(body.contains("hubuum_metrics_refresh_failures_total"));
    assert!(body.contains("source=\"inventory\""));
    assert!(body.contains("source=\"tasks\""));
    assert!(body.contains("hubuum_tasks{kind=\"import\",status=\"queued\"} 0"));
}

#[actix_web::test]
async fn task_queue_wait_uses_kind_only_labels() {
    let _lock = METRICS_TEST_LOCK.lock().await;
    metrics::init().unwrap();
    metrics::clear_scrape_cache_for_tests();

    metrics::task_claimed("remote_call", Some(Duration::from_millis(25)));
    metrics::task_completed("remote_call", "succeeded", Some(Duration::from_millis(5)));

    let app = test::init_service(
        App::new()
            .app_data(web::Data::new(unreachable_pool()))
            .route("/metrics", web::get().to(metrics::scrape)),
    )
    .await;
    let req = test::TestRequest::get().uri("/metrics").to_request();
    let response = test::call_service(&app, req).await;
    let body = test::read_body(response).await;
    let body = std::str::from_utf8(&body).unwrap();

    assert!(body.contains("hubuum_task_queue_wait_duration_seconds_bucket{kind=\"remote_call\""));
    assert!(!body.contains("hubuum_task_queue_wait_duration_seconds_bucket{final_status="));
    assert!(!body.contains(
        "hubuum_task_queue_wait_duration_seconds_bucket{kind=\"remote_call\",final_status="
    ));
}

#[rstest]
#[actix_web::test]
async fn task_gauges_export_zero_for_bounded_kind_status_pairs(
    #[future(awt)] test_context: TestContext,
) {
    let _lock = METRICS_TEST_LOCK.lock().await;
    let context = test_context;
    metrics::init().unwrap();
    metrics::clear_scrape_cache_for_tests();

    let app = test::init_service(
        App::new()
            .app_data(context.pool.clone())
            .route("/metrics", web::get().to(metrics::scrape)),
    )
    .await;
    let req = test::TestRequest::get().uri("/metrics").to_request();
    let response = test::call_service(&app, req).await;
    let body = test::read_body(response).await;
    let body = std::str::from_utf8(&body).unwrap();

    for kind in TaskKind::ALL {
        for status in TaskStatus::ALL {
            let line = format!(
                "hubuum_tasks{{kind=\"{}\",status=\"{}\"}}",
                kind.as_str(),
                status.as_str()
            );
            assert!(body.contains(&line), "missing metrics line: {line}");
        }
    }
}

fn unreachable_pool() -> DbPool {
    let manager = ConnectionManager::<PgConnection>::new(
        "postgres://hubuum:hubuum@127.0.0.1:1/hubuum_metrics_unreachable",
    );
    Pool::builder()
        .max_size(1)
        .connection_timeout(Duration::from_millis(5))
        .build_unchecked(manager)
}
