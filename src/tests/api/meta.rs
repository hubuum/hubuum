#[cfg(test)]
mod tests {
    use std::net::IpAddr;

    use actix_web::{http::StatusCode, test};
    use rstest::rstest;
    use serde_json::Value;

    use crate::middlewares::rate_limit::{
        LOGIN_RATE_LIMIT_TEST_LOCK, record_login_failure, reset_login_rate_limit_for_tests,
    };
    use crate::tests::api_operations::{delete_request, get_request};
    use crate::tests::asserts::assert_response_status;
    use crate::tests::{TestContext, lock_test_mutex, test_context};

    const LOGIN_RATE_LIMIT_ENDPOINT: &str = "/api/v0/meta/login-rate-limit";

    /// Drive enough failures to lock the `(username, ip)` scope (default threshold is 5).
    async fn lock_user_ip(username: &str, ip: IpAddr) {
        for _ in 0..5 {
            record_login_failure("local", username, Some(ip)).await;
        }
    }

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

    #[rstest]
    #[actix_web::test]
    async fn test_login_rate_limit_meta_endpoint_returns_config(
        #[future(awt)] test_context: TestContext,
    ) {
        let _guard = lock_test_mutex(&LOGIN_RATE_LIMIT_TEST_LOCK).await;
        reset_login_rate_limit_for_tests().await;
        let context = test_context;

        let resp = get_request(
            &context.pool,
            &context.admin_token,
            LOGIN_RATE_LIMIT_ENDPOINT,
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let body: Value = test::read_body_json(resp).await;

        assert!(body["config"]["enabled"].is_boolean());
        assert!(body["config"]["max_attempts"].is_number());
        assert!(body["config"]["max_attempts_per_ip"].is_number());
        assert!(body["config"]["max_attempts_per_subnet"].is_number());
        assert!(body["tracked_entries"].is_number());
        assert!(body["locked_entries"].is_number());
        assert!(body["entries"].is_array());
    }

    #[rstest]
    #[actix_web::test]
    async fn test_login_rate_limit_meta_endpoint_requires_admin(
        #[future(awt)] test_context: TestContext,
    ) {
        let context = test_context;
        let resp = get_request(
            &context.pool,
            &context.normal_token,
            LOGIN_RATE_LIMIT_ENDPOINT,
        )
        .await;
        assert_response_status(resp, StatusCode::FORBIDDEN).await;
    }

    #[rstest]
    #[actix_web::test]
    async fn test_login_rate_limit_release_entry(#[future(awt)] test_context: TestContext) {
        let _guard = lock_test_mutex(&LOGIN_RATE_LIMIT_TEST_LOCK).await;
        reset_login_rate_limit_for_tests().await;
        let context = test_context;

        let ip: IpAddr = "203.0.113.55".parse().unwrap();
        let identifier = "local/meta-release-user|203.0.113.55";
        lock_user_ip("meta-release-user", ip).await;

        // The locked scope is visible in the default (locked-only) listing.
        let resp = get_request(
            &context.pool,
            &context.admin_token,
            LOGIN_RATE_LIMIT_ENDPOINT,
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let body: Value = test::read_body_json(resp).await;
        let entry = body["entries"]
            .as_array()
            .unwrap()
            .iter()
            .find(|entry| entry["identifier"] == identifier)
            .expect("locked user_ip scope should be listed");
        assert_eq!(entry["scope"], "user_ip");
        assert_eq!(entry["locked"], true);
        let id = entry["id"].as_str().unwrap().to_string();

        // Releasing it by id reports success.
        let resp = delete_request(
            &context.pool,
            &context.admin_token,
            &format!("{LOGIN_RATE_LIMIT_ENDPOINT}/{id}"),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let body: Value = test::read_body_json(resp).await;
        assert_eq!(body["released"], true);

        // It is gone afterwards, even from the full listing.
        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &format!("{LOGIN_RATE_LIMIT_ENDPOINT}?include=all"),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let body: Value = test::read_body_json(resp).await;
        assert!(
            !body["entries"]
                .as_array()
                .unwrap()
                .iter()
                .any(|entry| entry["identifier"] == identifier),
            "released scope should no longer be tracked"
        );

        // A stale id now returns 404.
        let resp = delete_request(
            &context.pool,
            &context.admin_token,
            &format!("{LOGIN_RATE_LIMIT_ENDPOINT}/{id}"),
        )
        .await;
        assert_response_status(resp, StatusCode::NOT_FOUND).await;
    }

    #[rstest]
    #[actix_web::test]
    async fn test_login_rate_limit_filter_by_scope_and_query(
        #[future(awt)] test_context: TestContext,
    ) {
        let _guard = lock_test_mutex(&LOGIN_RATE_LIMIT_TEST_LOCK).await;
        reset_login_rate_limit_for_tests().await;
        let context = test_context;

        // One failure seeds three scopes: user_ip, ip, and subnet.
        record_login_failure(
            "local",
            "filter-alice",
            Some("203.0.113.77".parse().unwrap()),
        )
        .await;

        // scope=user_ip with a username substring returns only the user_ip scope.
        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &format!("{LOGIN_RATE_LIMIT_ENDPOINT}?include=all&scope=user_ip&q=filter-alice"),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let body: Value = test::read_body_json(resp).await;
        let entries = body["entries"].as_array().unwrap();
        assert_eq!(body["returned_entries"], entries.len() as u64);
        assert!(!entries.is_empty());
        assert!(entries.iter().all(|entry| {
            entry["scope"] == "user_ip"
                && entry["identifier"]
                    .as_str()
                    .unwrap()
                    .contains("filter-alice")
        }));

        // scope=ip narrows to the IP scope.
        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &format!("{LOGIN_RATE_LIMIT_ENDPOINT}?include=all&scope=ip"),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let body: Value = test::read_body_json(resp).await;
        let entries = body["entries"].as_array().unwrap();
        assert!(entries.iter().all(|entry| entry["scope"] == "ip"));
        assert!(
            entries
                .iter()
                .any(|entry| entry["identifier"] == "203.0.113.77")
        );

        // A non-matching query returns nothing.
        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &format!("{LOGIN_RATE_LIMIT_ENDPOINT}?include=all&q=no-such-identifier"),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let body: Value = test::read_body_json(resp).await;
        assert_eq!(body["returned_entries"], 0);
        assert!(body["entries"].as_array().unwrap().is_empty());
    }

    #[rstest]
    #[actix_web::test]
    async fn test_login_rate_limit_clear_all(#[future(awt)] test_context: TestContext) {
        let _guard = lock_test_mutex(&LOGIN_RATE_LIMIT_TEST_LOCK).await;
        reset_login_rate_limit_for_tests().await;
        let context = test_context;

        lock_user_ip("meta-clear-user", "203.0.113.66".parse().unwrap()).await;

        let resp = delete_request(
            &context.pool,
            &context.admin_token,
            LOGIN_RATE_LIMIT_ENDPOINT,
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let body: Value = test::read_body_json(resp).await;
        assert!(body["cleared"].as_u64().unwrap() >= 1);

        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &format!("{LOGIN_RATE_LIMIT_ENDPOINT}?include=all"),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let body: Value = test::read_body_json(resp).await;
        let entries = body["entries"].as_array().unwrap();
        let cleared_identifiers = [
            "meta-clear-user|203.0.113.66",
            "203.0.113.66",
            "203.0.113.0/24",
        ];
        assert!(
            entries.iter().all(|entry| {
                let identifier = entry["identifier"].as_str().unwrap();
                !cleared_identifiers.contains(&identifier)
            }),
            "clear-all should remove every scope created by this test: {entries:?}",
        );
    }
}
