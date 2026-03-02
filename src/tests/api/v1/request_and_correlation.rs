#[cfg(test)]
mod tests {
    use rstest::rstest;

    use actix_web::http::{header::HeaderValue, StatusCode};

    use crate::tests::api_operations::get_request_with_correlation;
    use crate::tests::asserts::assert_response_status;
    use crate::tests::{test_context, TestContext};

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
}
