use std::sync::LazyLock;
use std::time::Duration;

use actix_web::{App, http::StatusCode, test, web};
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::pooled_connection::bb8::Pool;
use rstest::rstest;
use tokio::sync::Mutex;

use crate::db::{DbConnection, DbPool};
use crate::models::{TaskKind, TaskStatus};
use crate::observability::metrics;
use crate::test_support::clear_metrics_scrape_cache;
use crate::tests::{TestContext, test_context};

static METRICS_TEST_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

#[rstest]
#[actix_web::test]
async fn metrics_endpoint_exports_prometheus_text(#[future(awt)] test_context: TestContext) {
    let _lock = METRICS_TEST_LOCK.lock().await;
    let context = test_context;
    metrics::init().unwrap();
    clear_metrics_scrape_cache();
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
    assert!(body.contains("entity_type=\"collections\""));
    assert!(body.contains("hubuum_event_queue_items"));
    assert!(body.contains("queue=\"fanout\""));
}

#[actix_web::test]
async fn metrics_endpoint_is_best_effort_when_database_refresh_fails() {
    let _lock = METRICS_TEST_LOCK.lock().await;
    metrics::init().unwrap();
    clear_metrics_scrape_cache();

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
    assert!(body.contains("source=\"events\""));
    assert!(body.contains("hubuum_tasks{kind=\"import\",status=\"queued\"} 0"));
}

#[actix_web::test]
async fn metrics_endpoint_exports_representative_bounded_families() {
    let _lock = METRICS_TEST_LOCK.lock().await;
    metrics::init().unwrap();
    clear_metrics_scrape_cache();

    metrics::export_completed("objects_in_class", "application/json");
    metrics::export_truncated("objects_in_class", "application/json");
    metrics::export_warnings("objects_in_class", "application/json", 2);
    metrics::import_phase_duration("planning", Duration::from_millis(5));
    metrics::import_items(3, 2, 1);
    metrics::login_lockout("subnet");
    metrics::client_allowlist_rejected("disallowed_ip");
    metrics::remote_call_finished("GET", "none", "timeout", Duration::from_millis(10));

    let app = test::init_service(
        App::new()
            .app_data(web::Data::new(unreachable_pool()))
            .route("/metrics", web::get().to(metrics::scrape)),
    )
    .await;
    let response =
        test::call_service(&app, test::TestRequest::get().uri("/metrics").to_request()).await;
    let body = test::read_body(response).await;
    let body = std::str::from_utf8(&body).unwrap();

    for metric_name in [
        "hubuum_export_completions_total",
        "hubuum_export_truncations_total",
        "hubuum_export_warnings_total",
        "hubuum_import_phase_duration_seconds",
        "hubuum_import_processed_items_total",
        "hubuum_import_succeeded_items_total",
        "hubuum_import_failed_items_total",
        "hubuum_login_lockouts_total",
        "hubuum_client_allowlist_rejections_total",
        "hubuum_remote_call_results_total",
    ] {
        assert!(body.contains(metric_name), "missing metric: {metric_name}");
    }
    assert!(body.contains("outcome=\"timeout\""));
}

#[actix_web::test]
async fn task_queue_wait_uses_kind_only_labels() {
    let _lock = METRICS_TEST_LOCK.lock().await;
    metrics::init().unwrap();
    clear_metrics_scrape_cache();

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
    clear_metrics_scrape_cache();

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
    let manager = AsyncDieselConnectionManager::<DbConnection>::new(
        "postgres://hubuum:hubuum@127.0.0.1:1/hubuum_metrics_unreachable",
    );
    Pool::builder()
        .max_size(1)
        .connection_timeout(Duration::from_millis(5))
        .build_unchecked(manager)
}
