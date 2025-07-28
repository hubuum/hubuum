#[cfg(test)]
mod tests {
    use yare::parameterized;

    use actix_web::http::{header::HeaderValue, StatusCode};

    use crate::tests::api_operations::get_request_with_correlation;
    use crate::tests::asserts::assert_response_status;
    use crate::tests::setup_pool_and_tokens;

    const ENDPOINT: &str = "/api/v1/classes/";

    #[parameterized(
        with_correlation_id = { Some("test-correlation-id") },
        with_empty_correlation_id = { Some("") }, // This means we get an empty x-correlation-id header in the response
        with_long_correlation_id = { Some("test-correlation-id-long with spaces & weird characters") },
        without_correlation_id = { None }

    )]
    #[test_macro(actix_web::test)]
    async fn test_with_correlation_id(correlation_target: Option<&str>) {
        let (pool, admin_token, _) = setup_pool_and_tokens().await;

        let resp =
            get_request_with_correlation(&pool, &admin_token, ENDPOINT, correlation_target).await;

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
