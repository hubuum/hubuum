#[cfg(test)]
mod tests {
    use actix_web::{http::StatusCode, test};
    use rstest::rstest;
    use serde_json::Value;

    use crate::tests::api_operations::get_request;
    use crate::tests::asserts::assert_response_status;
    use crate::tests::{TestContext, test_context};

    #[rstest]
    #[actix_web::test]
    async fn test_task_queue_meta_endpoint_returns_worker_and_queue_state(
        #[future(awt)] test_context: TestContext,
    ) {
        let context = test_context;
        let resp = get_request(&context.pool, &context.admin_token, "/api/v0/meta/tasks").await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let body: Value = test::read_body_json(resp).await;

        assert!(body["actix_workers"].is_number());
        assert!(body["configured_task_workers"].is_number());
        assert!(body["task_poll_interval_ms"].is_number());
        assert!(body["total_tasks"].is_number());
        assert!(body["queued_tasks"].is_number());
        assert!(body["active_tasks"].is_number());
        assert!(body["total_task_events"].is_number());
        assert!(body["total_import_result_rows"].is_number());
    }

    #[rstest]
    #[actix_web::test]
    async fn test_task_queue_meta_endpoint_requires_admin(
        #[future(awt)] test_context: TestContext,
    ) {
        let context = test_context;
        let resp = get_request(&context.pool, &context.normal_token, "/api/v0/meta/tasks").await;
        assert_response_status(resp, StatusCode::FORBIDDEN).await;
    }
}
