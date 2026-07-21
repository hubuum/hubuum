#[cfg(test)]
mod tests {
    use rstest::rstest;

    use actix_web::{
        App, HttpRequest, HttpResponse,
        http::{StatusCode, header::HeaderValue},
        test, web,
    };
    use serde_json::json;
    use std::{future::Future, str::FromStr};
    use tracing::instrument::WithSubscriber;
    use tracing_subscriber::layer::SubscriberExt;

    use crate::config::ClientAllowlist;
    use crate::events::RequestProvenance;
    use crate::logger::HubuumLoggingFormat;
    use crate::middlewares::actor_context;
    use crate::middlewares::{ClientAllowlistMiddleware, TracingMiddleware};
    use crate::test_support::{JsonLogWriter, record_principal_on_current_span};
    use crate::tests::api_operations::get_request_with_correlation;
    use crate::tests::asserts::assert_response_status;
    use crate::tests::{TestContext, test_context};

    const ENDPOINT: &str = "/api/v1/classes/";

    #[rstest]
    #[case::with_correlation_id(Some("test-correlation-id"), true)]
    #[case::with_empty_correlation_id(Some(""), false)]
    #[case::with_whitespace_correlation_id(Some("correlation id"), false)]
    #[case::with_overlong_correlation_id(
        Some(
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        ),
        false
    )]
    #[case::without_correlation_id(None, false)]
    #[actix_web::test]
    async fn test_with_correlation_id(
        #[case] correlation_target: Option<&str>,
        #[case] echoed: bool,
        #[future(awt)] test_context: TestContext,
    ) {
        let resp = get_request_with_correlation(
            &test_context.pool,
            &test_context.admin_token,
            ENDPOINT,
            correlation_target,
        )
        .await;

        let resp = assert_response_status(resp, StatusCode::OK).await;

        match (correlation_target, echoed) {
            (Some(correlation_id), true) => {
                assert_eq!(
                    resp.headers().get("x-correlation-id"),
                    Some(&HeaderValue::from_str(correlation_id).unwrap())
                );
            }
            _ => {
                assert!(resp.headers().get("x-correlation-id").is_none());
            }
        }

        assert!(
            resp.headers().get("x-request-id").is_some(),
            "Expected x-request-id header to be present"
        );
    }

    #[actix_web::test]
    async fn tracing_middleware_exposes_request_provenance_to_handlers() {
        async fn handler(req: HttpRequest) -> HttpResponse {
            let provenance = RequestProvenance::from_request(&req).expect("request provenance");
            HttpResponse::Ok().json(json!({
                "request_id": provenance.request_id().to_string(),
                "correlation_id": provenance.correlation_id(),
            }))
        }

        let app = test::init_service(
            App::new()
                .wrap(TracingMiddleware::new())
                .route("/probe", web::get().to(handler)),
        )
        .await;

        let resp = test::TestRequest::get()
            .insert_header(("x-correlation-id", "context-correlation"))
            .uri("/probe")
            .send_request(&app)
            .await;
        let response_request_id = resp
            .headers()
            .get("x-request-id")
            .expect("response request id")
            .to_str()
            .expect("valid request id header")
            .to_string();
        let body: serde_json::Value = test::read_body_json(resp).await;

        assert_eq!(body["request_id"], response_request_id);
        assert_eq!(body["correlation_id"], "context-correlation");
    }

    async fn capture_request_logs<T>(future: impl Future<Output = T>) -> (T, JsonLogWriter) {
        let writer = JsonLogWriter::default();
        let subscriber = tracing_subscriber::registry().with(
            tracing_subscriber::fmt::layer()
                .json()
                .with_writer(writer.clone())
                .event_format(HubuumLoggingFormat),
        );
        let output = future.with_subscriber(subscriber).await;
        (output, writer)
    }

    async fn ok_handler() -> HttpResponse {
        HttpResponse::Ok().finish()
    }

    async fn bad_request_handler() -> HttpResponse {
        HttpResponse::BadRequest().finish()
    }

    async fn server_error_handler() -> HttpResponse {
        HttpResponse::InternalServerError().finish()
    }

    async fn service_unavailable_handler() -> HttpResponse {
        HttpResponse::ServiceUnavailable().finish()
    }

    async fn principal_handler() -> HttpResponse {
        record_principal_on_current_span(77);
        HttpResponse::Ok().finish()
    }

    #[rstest]
    #[case::success("/ok", StatusCode::OK, "INFO")]
    #[case::client_error("/bad-request", StatusCode::BAD_REQUEST, "WARN")]
    #[case::server_error("/server-error", StatusCode::INTERNAL_SERVER_ERROR, "ERROR")]
    #[actix_web::test]
    async fn tracing_middleware_logs_request_completion_by_status(
        #[case] path: &str,
        #[case] expected_status: StatusCode,
        #[case] expected_severity: &str,
    ) {
        let (resp, writer) = capture_request_logs(async {
            let app = test::init_service(
                App::new()
                    .wrap(TracingMiddleware::new())
                    .route("/ok", web::get().to(ok_handler))
                    .route("/bad-request", web::get().to(bad_request_handler))
                    .route("/server-error", web::get().to(server_error_handler)),
            )
            .await;

            test::TestRequest::get()
                .insert_header(("x-correlation-id", "request-log-correlation"))
                .uri(path)
                .send_request(&app)
                .await
        })
        .await;
        assert_eq!(resp.status(), expected_status);

        let logs = writer.output();
        let event = logs
            .iter()
            .find(|event| event["message"] == "request complete")
            .expect("request completion log");
        assert_eq!(event["severity"], expected_severity);
        assert_eq!(event["method"], "GET");
        assert_eq!(event["path"], path);
        assert_eq!(event["status"], expected_status.as_u16());
        assert_eq!(event["correlation_id"], "request-log-correlation");
        assert!(event["request_id"].as_str().is_some());
        assert!(event["elapsed_ms"].as_u64().is_some());
    }

    #[rstest]
    #[case::liveness("/healthz")]
    #[case::readiness("/readyz")]
    #[actix_web::test]
    async fn tracing_middleware_logs_successful_probes_at_debug(#[case] path: &str) {
        let (resp, writer) = capture_request_logs(async {
            let app = test::init_service(
                App::new()
                    .wrap(TracingMiddleware::new())
                    .route("/healthz", web::get().to(ok_handler))
                    .route("/readyz", web::get().to(ok_handler)),
            )
            .await;

            test::TestRequest::get().uri(path).send_request(&app).await
        })
        .await;
        assert_eq!(resp.status(), StatusCode::OK);

        let event = writer
            .output()
            .into_iter()
            .find(|event| event["message"] == "request complete")
            .expect("request completion log");
        assert_eq!(event["severity"], "DEBUG");
        assert_eq!(event["path"], path);
    }

    #[actix_web::test]
    async fn tracing_middleware_keeps_failed_readiness_probes_at_error() {
        let (resp, writer) = capture_request_logs(async {
            let app = test::init_service(
                App::new()
                    .wrap(TracingMiddleware::new())
                    .route("/readyz", web::get().to(service_unavailable_handler)),
            )
            .await;

            test::TestRequest::get()
                .uri("/readyz")
                .send_request(&app)
                .await
        })
        .await;
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);

        let event = writer
            .output()
            .into_iter()
            .find(|event| event["message"] == "request complete")
            .expect("request completion log");
        assert_eq!(event["severity"], "ERROR");
        assert_eq!(event["path"], "/readyz");
    }

    #[actix_web::test]
    async fn tracing_middleware_includes_recorded_principal_on_request_logs() {
        let (resp, writer) = capture_request_logs(async {
            let app = test::init_service(
                App::new()
                    .wrap(TracingMiddleware::new())
                    .route("/principal", web::get().to(principal_handler)),
            )
            .await;

            test::TestRequest::get()
                .uri("/principal")
                .send_request(&app)
                .await
        })
        .await;
        assert_eq!(resp.status(), StatusCode::OK);

        let logs = writer.output();
        let event = logs
            .iter()
            .find(|event| event["message"] == "request complete")
            .expect("request completion log");
        assert_eq!(event["principal_id"], 77);
    }

    #[actix_web::test]
    async fn production_middleware_stack_records_authenticated_principal_on_request_logs() {
        let test_context = TestContext::new().await;
        let (resp, writer) = capture_request_logs(async {
            let app = test::init_service(
                App::new()
                    .wrap(actix_web::middleware::from_fn(actor_context))
                    .wrap(TracingMiddleware::new())
                    .app_data(test_context.pool.clone())
                    .app_data(crate::tests::app_context(&test_context.pool))
                    .route("/principal", web::get().to(ok_handler)),
            )
            .await;

            test::TestRequest::get()
                .insert_header((
                    actix_web::http::header::AUTHORIZATION,
                    format!("Bearer {}", test_context.admin_token),
                ))
                .uri("/principal")
                .send_request(&app)
                .await
        })
        .await;
        assert_eq!(resp.status(), StatusCode::OK);

        let logs = writer.output();
        let event = logs
            .iter()
            .find(|event| event["message"] == "request complete")
            .expect("request completion log");
        assert_eq!(event["principal_id"], test_context.admin_user.id);
    }

    #[actix_web::test]
    async fn production_middleware_stack_traces_allowlist_rejections() {
        let (resp, writer) = capture_request_logs(async {
            let app = test::init_service(
                App::new()
                    .wrap(actix_web::middleware::from_fn(actor_context))
                    .wrap(ClientAllowlistMiddleware::new(
                        ClientAllowlist::from_str("10.0.0.0/24").expect("allowlist"),
                    ))
                    .wrap(TracingMiddleware::new())
                    .route("/denied", web::get().to(ok_handler)),
            )
            .await;

            test::TestRequest::get()
                .uri("/denied")
                .peer_addr("192.0.2.10:8000".parse().expect("peer address"))
                .send_request(&app)
                .await
        })
        .await;

        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
        assert!(resp.headers().get("x-request-id").is_some());

        let event = writer
            .output()
            .into_iter()
            .find(|event| event["message"] == "request complete")
            .expect("request completion log");
        assert_eq!(event["severity"], "WARN");
        assert_eq!(event["status"], StatusCode::FORBIDDEN.as_u16());
        assert_eq!(event["path"], "/denied");
        assert!(event["request_id"].as_str().is_some());
    }
}
