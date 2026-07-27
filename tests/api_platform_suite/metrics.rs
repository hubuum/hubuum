use std::sync::LazyLock;
use std::time::Duration;

use actix_web::{App, HttpResponse, http::StatusCode, test, web};
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::pooled_connection::bb8::Pool;
use rstest::rstest;
use tokio::sync::Mutex;

use crate::config::RuntimeRole;
use crate::db::{DbConnection, DbPool};
use crate::middlewares::TracingMiddleware;
use crate::models::{ExportTemplateID, TaskKind, TaskStatus};
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
    metrics::runtime_identity(RuntimeRole::Worker);
    clear_metrics_scrape_cache();
    let _in_flight = metrics::http_request_started_for_route("/test");
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
    assert!(body.contains(
        "hubuum_db_connection_acquire_duration_seconds_bucket{caller=\"metrics_refresh\""
    ));
    assert!(body.contains(
        "hubuum_db_operation_duration_seconds_bucket{caller=\"metrics_refresh\",operation=\"connection\",result=\"ok\""
    ));
    assert!(body.contains("hubuum_build_info{git_sha="));
    assert!(body.contains("hubuum_runtime_info{role=\"worker\"} 1"));
    assert!(body.contains("hubuum_process_start_time_seconds"));
    assert!(body.contains("hubuum_metrics_refresh_last_success_timestamp_seconds"));
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
    let template_id = ExportTemplateID::new(42).unwrap();
    metrics::export_phase_timer(metrics::ExportMetricPhase::Render, Some(template_id))
        .finish(metrics::ExportMetricOutcome::Success);
    metrics::export_phase_timer(metrics::ExportMetricPhase::Total, Some(template_id))
        .finish(metrics::ExportMetricOutcome::Success);
    metrics::import_phase_duration(
        metrics::ImportMetricPhase::Planning,
        Duration::from_millis(5),
    );
    metrics::import_items(3, 2, 1);
    metrics::login_lockout("subnet");
    metrics::client_allowlist_rejected("disallowed_ip");
    metrics::remote_call_finished("GET", "none", "timeout", Duration::from_millis(10));
    metrics::event_worker_wakeup("fanout", "poll");
    metrics::task_worker_config(0, Duration::from_millis(200));

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
        "hubuum_export_duration_seconds",
        "hubuum_export_phase_duration_seconds",
        "hubuum_export_truncations_total",
        "hubuum_export_warnings_total",
        "hubuum_import_phase_duration_seconds",
        "hubuum_import_processed_items_total",
        "hubuum_import_succeeded_items_total",
        "hubuum_import_failed_items_total",
        "hubuum_login_lockouts_total",
        "hubuum_client_allowlist_rejections_total",
        "hubuum_remote_call_results_total",
        "hubuum_event_worker_wakeups_total",
        "hubuum_task_workers_configured",
        "hubuum_task_poll_interval_seconds",
    ] {
        assert!(body.contains(metric_name), "missing metric: {metric_name}");
    }
    assert!(body.contains("template_id=\"42\""));
    assert!(body.contains("phase=\"render\""));
    assert!(body.contains("outcome=\"success\""));
    assert!(body.contains("outcome=\"timeout\""));
    assert!(body.contains("hubuum_event_worker_wakeups_total{kind=\"poll\",worker=\"fanout\"}"));
    assert!(body.contains("hubuum_task_workers_configured 0"));
    assert!(body.contains("hubuum_task_poll_interval_seconds 0.2"));
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
        for state in ["queued", "active"] {
            let line = format!(
                "hubuum_task_oldest_age_seconds{{kind=\"{}\",state=\"{state}\"}}",
                kind.as_str(),
            );
            assert!(body.contains(&line), "missing metrics line: {line}");
        }
    }
}

#[actix_web::test]
async fn tracing_metrics_keep_stable_route_templates() {
    let _lock = METRICS_TEST_LOCK.lock().await;
    metrics::init().unwrap();
    clear_metrics_scrape_cache();

    async fn ok() -> HttpResponse {
        HttpResponse::Ok().finish()
    }

    let app = test::init_service(
        App::new()
            .wrap(TracingMiddleware::new())
            .app_data(web::Data::new(unreachable_pool()))
            .route(
                "/api/v1/classes/{class_id}/objects/{object_id}",
                web::get().to(ok),
            )
            .route("/metrics", web::get().to(metrics::scrape)),
    )
    .await;
    let response = test::call_service(
        &app,
        test::TestRequest::get()
            .uri("/api/v1/classes/42/objects/99")
            .to_request(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);

    let response =
        test::call_service(&app, test::TestRequest::get().uri("/metrics").to_request()).await;
    let body = test::read_body(response).await;
    let body = std::str::from_utf8(&body).unwrap();

    assert!(body.contains(
        "hubuum_http_requests_total{method=\"GET\",route=\"/api/v1/classes/{class_id}/objects/{object_id}\",status_code=\"200\",status_family=\"2xx\"}"
    ));
    assert!(!body.contains("route=\"/api/v1/classes/42/objects/99\""));
    assert!(body.contains("hubuum_http_requests_in_flight{route=\"/metrics\"} 1"));
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
