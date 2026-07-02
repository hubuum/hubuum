#[cfg(test)]
mod tests {
    use rstest::rstest;

    use actix_web::{
        App, HttpRequest, HttpResponse,
        http::{StatusCode, header::HeaderValue},
        test, web,
    };
    use serde_json::json;

    use crate::events::RequestProvenance;
    use crate::middlewares::TracingMiddleware;
    use crate::tests::api_operations::get_request_with_correlation;
    use crate::tests::asserts::assert_response_status;
    use crate::tests::{TestContext, test_context};

    const ENDPOINT: &str = "/api/v1/classes/";

    #[rstest]
    #[case::with_correlation_id(Some("test-correlation-id"))]
    #[case::with_empty_correlation_id(Some(""))]
    #[case::with_long_correlation_id(Some(
        "test-correlation-id-long with spaces & weird characters"
    ))]
    #[case::without_correlation_id(None)]
    #[actix_web::test]
    async fn test_with_correlation_id(
        #[case] correlation_target: Option<&str>,
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

        match correlation_target {
            Some(correlation_id) => {
                assert_eq!(
                    resp.headers().get("x-correlation-id"),
                    Some(&HeaderValue::from_str(correlation_id).unwrap())
                );
            }
            None => {
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
}
