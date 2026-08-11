use std::sync::LazyLock;
use std::time::Duration;

use actix_web::{App, HttpResponse, http::StatusCode, test, web};
use chrono::{NaiveDate, NaiveDateTime};
use diesel::{ExpressionMethods, QueryDsl};
use diesel_async::RunQueryDsl;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::pooled_connection::bb8::Pool;
use rstest::rstest;
use tokio::sync::Mutex;

use crate::config::RuntimeRole;
use crate::middlewares::TracingMiddleware;
use crate::models::{ExportTemplateID, NewTaskRecord, TaskKind, TaskStatus};
use crate::observability::metrics;
use crate::schema::tasks;
use crate::storage::postgres::{PostgresConnection, PostgresPool, with_connection};
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
            .app_data(crate::tests::app_context(&context.pool))
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
    assert!(
        body.contains("hubuum_metrics_refresh_last_success_timestamp_seconds{source=\"process\"}")
    );
    assert!(body.contains(
        "hubuum_task_last_terminal_timestamp_seconds{kind=\"export\",status=\"failed\"}"
    ));

    for metric_name in [
        "process_cpu_seconds_total",
        "process_max_fds",
        "process_open_fds",
        "process_resident_memory_bytes",
        "process_start_time_seconds",
        "process_virtual_memory_bytes",
    ] {
        assert!(
            body.contains(metric_name),
            "missing process metric: {metric_name}"
        );
    }
}

#[actix_web::test]
async fn metrics_endpoint_is_best_effort_when_database_refresh_fails() {
    let _lock = METRICS_TEST_LOCK.lock().await;
    metrics::init().unwrap();
    clear_metrics_scrape_cache();

    let pool = web::Data::new(unreachable_pool());
    let app = test::init_service(
        App::new()
            .app_data(pool.clone())
            .app_data(crate::tests::app_context(&pool))
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
async fn storage_metrics_export_backend_identity_and_bounded_operation_labels() {
    let body = scrape_recorded_metrics(|| {
        metrics::storage_backend_identity(
            "postgresql",
            hubuum_storage_core::STORAGE_CONTRACT_VERSION,
        );
        metrics::storage_operation_finished(
            "postgresql",
            "objects",
            "update",
            "conflict",
            Duration::from_millis(5),
        );
    })
    .await;

    assert!(
        body.contains(
            "hubuum_storage_backend_info{backend=\"postgresql\",contract_version=\"16\"} 1"
        )
    );
    assert!(body.contains(
        "hubuum_storage_operation_duration_seconds_bucket{backend=\"postgresql\",capability=\"objects\",operation=\"update\",result=\"conflict\""
    ));
    assert!(body.contains(
        "hubuum_storage_operation_errors_total{backend=\"postgresql\",capability=\"objects\",operation=\"update\",result=\"conflict\"} 1"
    ));
}

#[actix_web::test]
async fn export_metrics_use_bounded_phase_and_template_labels() {
    let body = scrape_recorded_metrics(|| {
        metrics::export_completed("objects_in_class", "application/json");
        metrics::export_truncated("objects_in_class", "application/json");
        metrics::export_warnings("objects_in_class", "application/json", 2);
        let template_id = ExportTemplateID::new(42).unwrap();
        metrics::export_phase_timer(metrics::ExportMetricPhase::Render, Some(template_id))
            .finish(metrics::ExportMetricOutcome::Success);
        metrics::export_phase_timer(metrics::ExportMetricPhase::Total, Some(template_id))
            .finish(metrics::ExportMetricOutcome::Success);
    })
    .await;

    for metric_name in [
        "hubuum_export_completions_total",
        "hubuum_export_duration_seconds",
        "hubuum_export_phase_duration_seconds",
        "hubuum_export_truncations_total",
        "hubuum_export_warnings_total",
    ] {
        assert!(body.contains(metric_name), "missing metric: {metric_name}");
    }
    assert!(body.contains("template_id=\"42\""));
    assert!(body.contains("phase=\"render\""));
    assert!(body.contains("outcome=\"success\""));
}

#[actix_web::test]
async fn import_metrics_export_phase_and_item_families() {
    let body = scrape_recorded_metrics(|| {
        metrics::import_phase_duration("planning", Duration::from_millis(5));
        metrics::import_items(3, 2, 1);
    })
    .await;

    for metric_name in [
        "hubuum_import_phase_duration_seconds",
        "hubuum_import_processed_items_total",
        "hubuum_import_succeeded_items_total",
        "hubuum_import_failed_items_total",
    ] {
        assert!(body.contains(metric_name), "missing metric: {metric_name}");
    }
    assert!(body.contains("phase=\"planning\""));
    assert!(body.contains("outcome=\"success\""));
}

#[actix_web::test]
async fn login_lockout_metrics_export_the_scope() {
    let body = scrape_recorded_metrics(|| metrics::login_lockout("subnet")).await;

    assert!(body.contains("hubuum_login_lockouts_total{scope=\"subnet\"}"));
}

#[actix_web::test]
async fn client_allowlist_metrics_export_the_rejection_reason() {
    let body =
        scrape_recorded_metrics(|| metrics::client_allowlist_rejected("disallowed_ip")).await;

    assert!(body.contains("hubuum_client_allowlist_rejections_total{reason=\"disallowed_ip\"}"));
}

#[actix_web::test]
async fn remote_call_metrics_export_the_terminal_outcome() {
    let body = scrape_recorded_metrics(|| {
        metrics::remote_call_finished("GET", "none", "timeout", Duration::from_millis(10));
    })
    .await;

    assert!(body.contains("hubuum_remote_call_duration_seconds"));
    assert!(body.contains("hubuum_remote_call_results_total"));
    assert!(body.contains("outcome=\"timeout\""));
}

#[actix_web::test]
async fn event_worker_metrics_export_bounded_wakeup_labels() {
    let body = scrape_recorded_metrics(|| metrics::event_worker_wakeup("fanout", "poll")).await;

    assert!(body.contains("hubuum_event_worker_wakeups_total{kind=\"poll\",worker=\"fanout\"}"));
}

#[actix_web::test]
async fn task_worker_config_metrics_use_dimensionally_typed_families() {
    let body =
        scrape_recorded_metrics(|| metrics::task_worker_config(0, Duration::from_millis(200)))
            .await;

    assert!(body.contains("hubuum_task_workers_configured 0"));
    assert!(body.contains("hubuum_task_poll_interval_seconds 0.2"));
}

#[actix_web::test]
async fn task_queue_wait_uses_kind_only_labels() {
    let body = scrape_recorded_metrics(|| {
        metrics::task_claimed("remote_call", Some(Duration::from_millis(25)));
        metrics::task_completed("remote_call", "succeeded", Some(Duration::from_millis(5)));
    })
    .await;

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
            .app_data(crate::tests::app_context(&context.pool))
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

#[rstest]
#[actix_web::test]
async fn task_terminal_gauge_exports_latest_finished_timestamp(
    #[future(awt)] test_context: TestContext,
) {
    let _lock = METRICS_TEST_LOCK.lock().await;
    let context = test_context;
    metrics::init().unwrap();
    clear_metrics_scrape_cache();

    let older = NaiveDate::from_ymd_opt(9998, 1, 1)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap();
    let latest = NaiveDate::from_ymd_opt(9998, 1, 2)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap();
    let records = [
        terminal_task_record(&context, "older", older),
        terminal_task_record(&context, "latest", latest),
    ];
    let task_ids = with_connection(&context.pool, async |conn| {
        diesel::insert_into(tasks::table)
            .values(&records)
            .returning(tasks::id)
            .get_results::<i32>(conn)
            .await
    })
    .await
    .unwrap();

    let app = test::init_service(
        App::new()
            .app_data(context.pool.clone())
            .app_data(crate::tests::app_context(&context.pool))
            .route("/metrics", web::get().to(metrics::scrape)),
    )
    .await;
    let response =
        test::call_service(&app, test::TestRequest::get().uri("/metrics").to_request()).await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = String::from_utf8(test::read_body(response).await.to_vec()).unwrap();

    with_connection(&context.pool, async |conn| {
        diesel::delete(tasks::table.filter(tasks::id.eq_any(task_ids)))
            .execute(conn)
            .await
    })
    .await
    .unwrap();
    clear_metrics_scrape_cache();

    let metric = body
        .lines()
        .find(|line| {
            line.starts_with(
                "hubuum_task_last_terminal_timestamp_seconds{kind=\"backup\",status=\"failed\"}",
            )
        })
        .expect("missing backup/failed terminal timestamp metric");
    let exported = metric
        .split_whitespace()
        .nth(1)
        .expect("metric value")
        .parse::<f64>()
        .expect("numeric metric value");
    assert_eq!(exported, latest.and_utc().timestamp() as f64);
}

#[actix_web::test]
async fn tracing_metrics_keep_stable_route_templates() {
    let _lock = METRICS_TEST_LOCK.lock().await;
    metrics::init().unwrap();
    clear_metrics_scrape_cache();

    async fn ok() -> HttpResponse {
        HttpResponse::Ok().finish()
    }

    let pool = web::Data::new(unreachable_pool());
    let app = test::init_service(
        App::new()
            .wrap(TracingMiddleware::new())
            .app_data(pool.clone())
            .app_data(crate::tests::app_context(&pool))
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

fn terminal_task_record(
    context: &TestContext,
    label: &str,
    finished_at: NaiveDateTime,
) -> NewTaskRecord {
    NewTaskRecord {
        kind: TaskKind::Backup.as_str().to_string(),
        status: TaskStatus::Failed.as_str().to_string(),
        submitted_by: Some(context.admin_user.id),
        idempotency_key: Some(context.scoped_name(&format!("terminal-metric-{label}"))),
        request_hash: None,
        request_payload: None,
        summary: Some("terminal metric fixture".to_string()),
        total_items: 1,
        processed_items: 1,
        success_items: 0,
        failed_items: 1,
        submitted_token_id: None,
        submitted_token_scoped: false,
        submitted_token_scopes: serde_json::json!([]),
        request_redacted_at: Some(finished_at),
        started_at: Some(finished_at),
        finished_at: Some(finished_at),
    }
}

fn unreachable_pool() -> PostgresPool {
    let manager = AsyncDieselConnectionManager::<PostgresConnection>::new(
        "postgres://hubuum:hubuum@127.0.0.1:1/hubuum_metrics_unreachable",
    );
    Pool::builder()
        .max_size(1)
        .connection_timeout(Duration::from_millis(5))
        .build_unchecked(manager)
}

async fn scrape_recorded_metrics(record: impl FnOnce()) -> String {
    let _lock = METRICS_TEST_LOCK.lock().await;
    metrics::init().unwrap();
    clear_metrics_scrape_cache();
    record();

    let pool = web::Data::new(unreachable_pool());
    let app = test::init_service(
        App::new()
            .app_data(pool.clone())
            .app_data(crate::tests::app_context(&pool))
            .route("/metrics", web::get().to(metrics::scrape)),
    )
    .await;
    let response =
        test::call_service(&app, test::TestRequest::get().uri("/metrics").to_request()).await;
    assert_eq!(response.status(), StatusCode::OK);
    String::from_utf8(test::read_body(response).await.to_vec()).unwrap()
}
