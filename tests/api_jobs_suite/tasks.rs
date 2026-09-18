#[cfg(test)]
mod tests {
    use actix_web::{http::StatusCode, test};
    use rstest::rstest;

    use crate::models::{TaskKind, TaskResponse, TaskStatus};
    use crate::pagination::NEXT_CURSOR_HEADER;
    use crate::tests::api_operations::get_request;
    use crate::tests::asserts::{
        assert_paginated_collection_total_count, assert_response_status, header_value,
    };
    use crate::tests::{TestContext, create_test_user, test_context};

    const TASKS_ENDPOINT: &str = "/api/v1/tasks";

    async fn create_synthetic_task(
        context: &TestContext,
        submitted_by: i32,
        kind: TaskKind,
        status: TaskStatus,
        label: &str,
    ) -> i32 {
        let task = crate::test_support::create_persisted_test_task(
            context.pool.get_ref(),
            crate::test_support::persisted_test_task_request(kind, status, submitted_by)
                .expect("test task request must be valid")
                .summary(Some(context.scoped_name(label))),
        )
        .await
        .expect("synthetic task should be persisted");

        task.id
    }

    async fn list_visible_task_ids(
        context: &TestContext,
        token: &str,
        max_pages: usize,
    ) -> Vec<i32> {
        let mut cursor = None;
        let mut collected = Vec::new();

        for _ in 0..max_pages {
            let url = match cursor.as_deref() {
                Some(c) => format!("{TASKS_ENDPOINT}?sort=id.desc&limit=50&cursor={c}"),
                None => format!("{TASKS_ENDPOINT}?sort=id.desc&limit=50"),
            };

            let resp = get_request(&context.pool, token, &url).await;
            let resp = assert_response_status(resp, StatusCode::OK).await;
            cursor = header_value(&resp, NEXT_CURSOR_HEADER);
            let tasks: Vec<TaskResponse> = test::read_body_json(resp).await;
            collected.extend(tasks.into_iter().map(|task| task.id));

            if cursor.is_none() {
                break;
            }
        }

        collected
    }

    async fn get_tasks(context: &TestContext, token: &str, url: &str) -> Vec<TaskResponse> {
        let resp = get_request(&context.pool, token, url).await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        test::read_body_json(resp).await
    }

    #[rstest]
    #[actix_web::test]
    async fn test_list_tasks_non_admin_sees_only_own(#[future(awt)] test_context: TestContext) {
        let context = test_context;
        let other_user = create_test_user(&context.pool).await;

        let foreign_task_id = create_synthetic_task(
            &context,
            other_user.id,
            TaskKind::Export,
            TaskStatus::Succeeded,
            "tasks_list_foreign",
        )
        .await;
        let own_task_id = create_synthetic_task(
            &context,
            context.normal_user.id,
            TaskKind::Export,
            TaskStatus::Succeeded,
            "tasks_list_own",
        )
        .await;

        let visible_ids = list_visible_task_ids(&context, &context.normal_token, 20).await;

        assert!(visible_ids.contains(&own_task_id));
        assert!(!visible_ids.contains(&foreign_task_id));
    }

    #[rstest]
    #[actix_web::test]
    async fn test_list_tasks_admin_sees_all_users(#[future(awt)] test_context: TestContext) {
        let context = test_context;
        let other_user = create_test_user(&context.pool).await;

        let normal_user_task_id = create_synthetic_task(
            &context,
            context.normal_user.id,
            TaskKind::Export,
            TaskStatus::Succeeded,
            "tasks_list_admin_normal",
        )
        .await;
        let other_user_task_id = create_synthetic_task(
            &context,
            other_user.id,
            TaskKind::Export,
            TaskStatus::Succeeded,
            "tasks_list_admin_other",
        )
        .await;

        let visible_ids = list_visible_task_ids(&context, &context.admin_token, 20).await;

        assert!(visible_ids.contains(&normal_user_task_id));
        assert!(visible_ids.contains(&other_user_task_id));
    }

    #[rstest]
    #[actix_web::test]
    async fn test_list_tasks_admin_filters_by_kind_status_and_submitted_by(
        #[future(awt)] test_context: TestContext,
    ) {
        let context = test_context;
        let other_user = create_test_user(&context.pool).await;

        let expected = create_synthetic_task(
            &context,
            other_user.id,
            TaskKind::Import,
            TaskStatus::Running,
            "tasks_filter_expected",
        )
        .await;
        let _ = create_synthetic_task(
            &context,
            other_user.id,
            TaskKind::Import,
            TaskStatus::Failed,
            "tasks_filter_wrong_status",
        )
        .await;
        let _ = create_synthetic_task(
            &context,
            other_user.id,
            TaskKind::Export,
            TaskStatus::Running,
            "tasks_filter_wrong_kind",
        )
        .await;
        let _ = create_synthetic_task(
            &context,
            context.normal_user.id,
            TaskKind::Import,
            TaskStatus::Running,
            "tasks_filter_wrong_submitter",
        )
        .await;

        let url = format!(
            "{TASKS_ENDPOINT}?kind=import&status=running&submitted_by={}&sort=id.desc&limit=50",
            other_user.id
        );
        let resp = get_request(&context.pool, &context.admin_token, &url).await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let tasks: Vec<TaskResponse> = test::read_body_json(resp).await;

        assert!(!tasks.is_empty());
        assert!(tasks.iter().any(|task| task.id == expected));
        assert!(tasks.iter().all(|task| task.kind == TaskKind::Import));
        assert!(tasks.iter().all(|task| task.status == TaskStatus::Running));
        assert!(
            tasks
                .iter()
                .all(|task| task.submitted_by == Some(other_user.id))
        );
    }

    #[rstest]
    #[actix_web::test]
    async fn task_search_retains_combined_filters_across_cursors(
        #[future(awt)] test_context: TestContext,
    ) {
        let context = test_context;
        let expected = create_synthetic_task(
            &context,
            context.normal_user.id,
            TaskKind::Import,
            TaskStatus::Succeeded,
            "search_first",
        )
        .await;
        let second = create_synthetic_task(
            &context,
            context.normal_user.id,
            TaskKind::Export,
            TaskStatus::Failed,
            "search_second",
        )
        .await;
        create_synthetic_task(
            &context,
            context.normal_user.id,
            TaskKind::Backup,
            TaskStatus::Succeeded,
            "search_excluded_kind",
        )
        .await;
        create_synthetic_task(
            &context,
            context.normal_user.id,
            TaskKind::Import,
            TaskStatus::Cancelled,
            "search_excluded_status",
        )
        .await;
        let url = format!(
            "{TASKS_ENDPOINT}?kind=import,export&status=succeeded,failed&terminal=true&cancel_requested=false&submitted_by={}&sort=id.asc&limit=1",
            context.normal_user.id
        );
        let response = get_request(&context.pool, &context.admin_token, &url).await;
        let response = assert_response_status(response, StatusCode::OK).await;
        assert_eq!(
            header_value(&response, "X-Total-Count").as_deref(),
            Some("2")
        );
        let cursor = header_value(&response, NEXT_CURSOR_HEADER).expect("second page");
        let first: Vec<TaskResponse> = test::read_body_json(response).await;
        let response = get_request(
            &context.pool,
            &context.admin_token,
            &format!("{url}&cursor={cursor}"),
        )
        .await;
        let response = assert_response_status(response, StatusCode::OK).await;
        let next: Vec<TaskResponse> = test::read_body_json(response).await;
        assert_eq!(
            first
                .into_iter()
                .chain(next)
                .map(|t| t.id)
                .collect::<Vec<_>>(),
            vec![expected, second]
        );
    }

    #[rstest]
    #[actix_web::test]
    async fn test_list_tasks_admin_sorts_by_kind(#[future(awt)] test_context: TestContext) {
        let context = test_context;
        let other_user = create_test_user(&context.pool).await;

        let import_id = create_synthetic_task(
            &context,
            other_user.id,
            TaskKind::Import,
            TaskStatus::Succeeded,
            "tasks_sort_kind_import",
        )
        .await;
        let export_id = create_synthetic_task(
            &context,
            other_user.id,
            TaskKind::Export,
            TaskStatus::Succeeded,
            "tasks_sort_kind_export",
        )
        .await;
        let reindex_id = create_synthetic_task(
            &context,
            other_user.id,
            TaskKind::Reindex,
            TaskStatus::Succeeded,
            "tasks_sort_kind_reindex",
        )
        .await;

        let url = format!(
            "{TASKS_ENDPOINT}?submitted_by={}&sort=kind.asc&limit=50",
            other_user.id
        );
        let tasks = get_tasks(&context, &context.admin_token, &url).await;
        let ids = tasks.iter().map(|task| task.id).collect::<Vec<_>>();

        assert_eq!(ids, vec![export_id, import_id, reindex_id]);
    }

    #[rstest]
    #[actix_web::test]
    async fn test_list_tasks_admin_supports_multi_field_sort(
        #[future(awt)] test_context: TestContext,
    ) {
        let context = test_context;
        let other_user = create_test_user(&context.pool).await;

        let export_id_one = create_synthetic_task(
            &context,
            other_user.id,
            TaskKind::Export,
            TaskStatus::Succeeded,
            "tasks_sort_multi_export_one",
        )
        .await;
        let export_id_two = create_synthetic_task(
            &context,
            other_user.id,
            TaskKind::Export,
            TaskStatus::Succeeded,
            "tasks_sort_multi_export_two",
        )
        .await;
        let import_id = create_synthetic_task(
            &context,
            other_user.id,
            TaskKind::Import,
            TaskStatus::Succeeded,
            "tasks_sort_multi_import",
        )
        .await;

        let url = format!(
            "{TASKS_ENDPOINT}?submitted_by={}&sort=kind.asc,id.desc&limit=50",
            other_user.id
        );
        let tasks = get_tasks(&context, &context.admin_token, &url).await;
        let ids = tasks.iter().map(|task| task.id).collect::<Vec<_>>();

        assert_eq!(ids, vec![export_id_two, export_id_one, import_id]);
    }

    #[rstest]
    #[actix_web::test]
    async fn test_list_tasks_total_count_matches_paginated_results(
        #[future(awt)] test_context: TestContext,
    ) {
        let context = test_context;
        let other_user = create_test_user(&context.pool).await;

        let expected_ids = vec![
            create_synthetic_task(
                &context,
                other_user.id,
                TaskKind::Import,
                TaskStatus::Succeeded,
                "tasks_total_count_one",
            )
            .await,
            create_synthetic_task(
                &context,
                other_user.id,
                TaskKind::Import,
                TaskStatus::Succeeded,
                "tasks_total_count_two",
            )
            .await,
            create_synthetic_task(
                &context,
                other_user.id,
                TaskKind::Import,
                TaskStatus::Succeeded,
                "tasks_total_count_three",
            )
            .await,
        ];

        let (tasks, total_count): (Vec<TaskResponse>, i64) = assert_paginated_collection_total_count(
            &context.pool,
            &context.admin_token,
            10,
            |cursor| match cursor {
                Some(cursor) => format!(
                    "{TASKS_ENDPOINT}?submitted_by={}&kind=import&status=succeeded&sort=id&limit=2&cursor={cursor}",
                    other_user.id
                ),
                None => format!(
                    "{TASKS_ENDPOINT}?submitted_by={}&kind=import&status=succeeded&sort=id&limit=2",
                    other_user.id
                ),
            },
        )
        .await;

        assert_eq!(total_count, expected_ids.len() as i64);
        assert_eq!(
            tasks.iter().map(|task| task.id).collect::<Vec<_>>(),
            expected_ids
        );
    }
    #[rstest]
    #[actix_web::test]
    async fn external_task_list_pages_before_policy_checks(
        #[values(false, true)] include_total: bool,
    ) {
        use crate::permissions::test_support::mock_treetop::MockTreetopBackend;
        use crate::tests::api_operations::get_request_with_permission_backend;
        use std::sync::Arc;

        let context = TestContext::new().await;
        let fixture = context.collection_fixture("task_candidate_pages").await;
        let backend = Arc::new(MockTreetopBackend::new());
        backend.add_admin_rule(fixture.owner_group.id);
        backend.add_task_read_rule(fixture.owner_group.id, None);
        let mut ids = Vec::new();
        for index in 0..8 {
            ids.push(
                create_synthetic_task(
                    &context,
                    context.admin_user.id,
                    TaskKind::Reindex,
                    TaskStatus::Succeeded,
                    &format!("candidate_{index}"),
                )
                .await,
            );
        }
        let mut cursor = String::new();
        for expected_id in ids.iter().take(2) {
            let before = backend.task_authorization_batch_sizes().len();
            let response = get_request_with_permission_backend(
                &context.pool,
                &context.admin_token,
                &format!(
                    "/api/v1/tasks?submitted_by={}&limit=1&include_total={include_total}{cursor}",
                    context.admin_user.id
                ),
                backend.clone(),
            )
            .await;
            let response = assert_response_status(response, StatusCode::OK).await;
            assert_eq!(
                header_value(&response, crate::pagination::TOTAL_COUNT_HEADER),
                include_total.then(|| "8".to_string())
            );
            cursor = format!(
                "&cursor={}",
                header_value(&response, NEXT_CURSOR_HEADER).unwrap()
            );
            let rows: Vec<TaskResponse> = test::read_body_json(response).await;
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].id, *expected_id);
            assert_eq!(
                backend.task_authorization_batch_sizes()[before..],
                if include_total { vec![8] } else { vec![2] }
            );
        }
        hubuum_storage_postgres::test_support::delete_tasks(
            &context.pool,
            &ids.into_iter()
                .map(|id| crate::models::TaskID::new(id).unwrap())
                .collect::<Vec<_>>(),
        )
        .await
        .unwrap();
        fixture.cleanup().await.unwrap();
    }
    #[rstest]
    #[case::owner(false)]
    #[case::administrator(true)]
    #[actix_web::test]
    async fn cancel_endpoint_allows_owner_or_unscoped_admin(#[case] admin: bool) {
        use crate::tests::api_operations::post_request;
        let context = TestContext::new().await;
        let task_id = create_synthetic_task(
            &context,
            context.normal_user.id,
            TaskKind::Import,
            TaskStatus::Running,
            "cancel_authorized",
        )
        .await;
        let (token, actor) = if admin {
            (&context.admin_token, context.admin_user.id)
        } else {
            (&context.normal_token, context.normal_user.id)
        };
        let response = post_request(
            &context.pool,
            token,
            &format!("{TASKS_ENDPOINT}/{task_id}/cancel"),
            serde_json::json!({"reason":"Withdraw this request"}),
        )
        .await;
        let response = assert_response_status(response, StatusCode::ACCEPTED).await;
        let task: TaskResponse = test::read_body_json(response).await;
        assert_eq!(task.status, TaskStatus::Running);
        assert_eq!(task.cancel_requested_by, Some(actor));
        assert!(task.cancel_requested_at.is_some());
        assert!(task.terminal_reason.is_none());
        hubuum_storage_postgres::test_support::delete_task(
            &context.pool,
            hubuum_domain::TaskId::new(task_id).unwrap(),
        )
        .await
        .unwrap();
    }

    #[rstest]
    #[case::blank(serde_json::json!({"reason":" "}))]
    #[case::multiline(serde_json::json!({"reason":"first\nsecond"}))]
    #[case::oversized(serde_json::json!({"reason":"x".repeat(513)}))]
    #[case::unknown_field(serde_json::json!({"timeout":2}))]
    #[actix_web::test]
    async fn cancel_endpoint_validates_before_mutation(#[case] body: serde_json::Value) {
        use crate::tests::api_operations::post_request;
        let context = TestContext::new().await;
        let task_id = create_synthetic_task(
            &context,
            context.normal_user.id,
            TaskKind::Import,
            TaskStatus::Queued,
            "cancel_bad_input",
        )
        .await;
        let response = post_request(
            &context.pool,
            &context.normal_token,
            &format!("{TASKS_ENDPOINT}/{task_id}/cancel"),
            body,
        )
        .await;
        assert_response_status(response, StatusCode::BAD_REQUEST).await;
        hubuum_storage_postgres::test_support::delete_task(
            &context.pool,
            hubuum_domain::TaskId::new(task_id).unwrap(),
        )
        .await
        .unwrap();
    }

    #[rstest]
    #[case::ordinary(TaskKind::Import)]
    #[case::internal(TaskKind::Reindex)]
    #[actix_web::test]
    async fn cancel_endpoint_hides_another_principals_task(#[case] kind: TaskKind) {
        use crate::tests::api_operations::post_request;
        let context = TestContext::new().await;
        let task_id = create_synthetic_task(
            &context,
            context.admin_user.id,
            kind,
            TaskStatus::Queued,
            "cancel_foreign",
        )
        .await;
        let response = post_request(
            &context.pool,
            &context.normal_token,
            &format!("{TASKS_ENDPOINT}/{task_id}/cancel"),
            serde_json::json!({}),
        )
        .await;
        assert_response_status(response, StatusCode::NOT_FOUND).await;
        hubuum_storage_postgres::test_support::delete_task(
            &context.pool,
            hubuum_domain::TaskId::new(task_id).unwrap(),
        )
        .await
        .unwrap();
    }

    #[actix_web::test]
    async fn cancel_endpoint_requires_admin_for_internal_reindex() {
        use crate::tests::api_operations::post_request;
        let context = TestContext::new().await;
        let task_id = create_synthetic_task(
            &context,
            context.normal_user.id,
            TaskKind::Reindex,
            TaskStatus::Queued,
            "cancel_internal",
        )
        .await;
        let response = post_request(
            &context.pool,
            &context.normal_token,
            &format!("{TASKS_ENDPOINT}/{task_id}/cancel"),
            serde_json::json!({}),
        )
        .await;
        assert_response_status(response, StatusCode::FORBIDDEN).await;
        hubuum_storage_postgres::test_support::delete_task(
            &context.pool,
            hubuum_domain::TaskId::new(task_id).unwrap(),
        )
        .await
        .unwrap();
    }

    #[rstest]
    #[case::same_credential(true, StatusCode::ACCEPTED)]
    #[case::another_credential(false, StatusCode::FORBIDDEN)]
    #[actix_web::test]
    async fn scoped_admin_can_withdraw_only_its_own_token_submission(
        #[case] same_credential: bool,
        #[case] expected: StatusCode,
    ) {
        use crate::models::{Permissions, TokenScope};
        use crate::tests::api_operations::post_request;
        use crate::tests::{persisted_test_token, scoped_token};
        use hubuum_storage_core::StorageTaskScopeSnapshot;
        let context = TestContext::new().await;
        let bearer = scoped_token(
            &context.pool,
            context.admin_user.id,
            &[Permissions::ReadCollection],
        )
        .await;
        let token = persisted_test_token(&context.pool, &bearer).await;
        let scope = TokenScope::from_request_parts(Some(vec![Permissions::ReadCollection]), None)
            .unwrap()
            .unwrap();
        let snapshot = if same_credential {
            StorageTaskScopeSnapshot::new(
                Some(hubuum_domain::TokenId::new(token.id).unwrap()),
                true,
                scope.snapshot_json(),
            )
        } else {
            StorageTaskScopeSnapshot::unscoped()
        };
        let task = crate::test_support::create_persisted_test_task(
            &context.pool,
            crate::test_support::persisted_test_task_request(
                TaskKind::Import,
                TaskStatus::Running,
                context.admin_user.id,
            )
            .unwrap()
            .scope_snapshot(snapshot),
        )
        .await
        .unwrap();
        let response = post_request(
            &context.pool,
            &bearer,
            &format!("{TASKS_ENDPOINT}/{}/cancel", task.id),
            serde_json::json!({}),
        )
        .await;
        assert_response_status(response, expected).await;
        hubuum_storage_postgres::test_support::delete_task(
            &context.pool,
            hubuum_domain::TaskId::new(task.id).unwrap(),
        )
        .await
        .unwrap();
    }

    #[rstest]
    #[case::human_owner(false, false)]
    #[case::disabled_account_human_owner(true, false)]
    #[case::service_account_self(false, true)]
    #[actix_web::test]
    async fn service_account_cancellation_follows_principal_and_owner_group_policy(
        #[case] disabled: bool,
        #[case] self_cancel: bool,
    ) {
        use crate::events::EventContext;
        use crate::tests::api_operations::post_request;
        use crate::tests::{create_test_group, create_test_service_account, service_account_token};
        let context = TestContext::new().await;
        let group = create_test_group(&context.pool).await;
        group
            .add_member_without_events(&context.pool, &context.normal_user)
            .await
            .unwrap();
        let account =
            create_test_service_account(&context.pool, &group, Some(context.admin_user.id)).await;
        let task_id = create_synthetic_task(
            &context,
            account.id,
            TaskKind::Import,
            TaskStatus::Running,
            "cancel_service_owner",
        )
        .await;
        if disabled {
            crate::services::identity::disable_service_account(
                &context.pool,
                account.id,
                &EventContext::system(),
            )
            .await
            .unwrap();
        }
        let (bearer, actor_id) = if self_cancel {
            (
                service_account_token(&context.pool, &account, None, None).await,
                account.id,
            )
        } else {
            (context.normal_token.clone(), context.normal_user.id)
        };
        let response = post_request(
            &context.pool,
            &bearer,
            &format!("{TASKS_ENDPOINT}/{task_id}/cancel"),
            serde_json::json!({}),
        )
        .await;
        let requested: TaskResponse =
            test::read_body_json(assert_response_status(response, StatusCode::ACCEPTED).await)
                .await;
        assert_eq!(requested.cancel_requested_by, Some(actor_id));
        hubuum_storage_postgres::test_support::delete_task(
            &context.pool,
            hubuum_domain::TaskId::new(task_id).unwrap(),
        )
        .await
        .unwrap();
        crate::services::identity::delete_service_account(
            &context.pool,
            account.id,
            &EventContext::system(),
        )
        .await
        .unwrap();
        group.delete_without_events(&context.pool).await.unwrap();
    }

    #[actix_web::test]
    async fn task_read_policy_does_not_grant_cancellation() {
        use crate::permissions::test_support::mock_treetop::MockTreetopBackend;
        use crate::tests::api_operations::post_request_with_permission_backend;
        use std::sync::Arc;
        let context = TestContext::new().await;
        let fixture = context.collection_fixture("task_cancel_policy").await;
        let backend = Arc::new(MockTreetopBackend::new());
        backend.add_admin_rule(fixture.owner_group.id);
        backend.add_task_read_rule(fixture.owner_group.id, None);
        let task_id = create_synthetic_task(
            &context,
            context.admin_user.id,
            TaskKind::Import,
            TaskStatus::Queued,
            "cancel_policy",
        )
        .await;
        let response = post_request_with_permission_backend(
            &context.pool,
            &context.admin_token,
            &format!("{TASKS_ENDPOINT}/{task_id}/cancel"),
            serde_json::json!({}),
            backend,
        )
        .await;
        assert_response_status(response, StatusCode::NOT_FOUND).await;
        hubuum_storage_postgres::test_support::delete_task(
            &context.pool,
            hubuum_domain::TaskId::new(task_id).unwrap(),
        )
        .await
        .unwrap();
        fixture.cleanup().await.unwrap();
    }
    #[rstest]
    #[case::local_search(false, true, false)]
    #[case::delegated_search(true, true, false)]
    #[case::local_details(false, false, false)]
    #[case::local_cancellation(false, false, true)]
    #[case::delegated_details(true, false, false)]
    #[actix_web::test]
    async fn task_discovery_requires_resource_access(
        #[future(awt)] test_context: TestContext,
        #[case] delegated: bool,
        #[case] search: bool,
        #[case] cancel: bool,
    ) {
        use crate::permissions::test_support::mock_treetop::MockTreetopBackend;
        use crate::tests::api_operations::get_request_with_permission_backend;
        use hubuum_storage_core::{
            StorageTaskCreateRequest, StorageTaskKind, StorageTaskMetadata, TaskExplicitTarget,
            TaskMetadataDetails, TaskQueueStorage,
        };
        use std::sync::Arc;
        let context = test_context;
        let fixture = context.collection_fixture("task_discovery_hidden").await;
        let (actor, token) = if delegated {
            (context.admin_user.id, &context.admin_token)
        } else {
            (context.normal_user.id, &context.normal_token)
        };
        let storage =
            hubuum_storage_postgres::PostgresStorage::unobserved(context.pool.get_ref().clone());
        let metadata = StorageTaskMetadata::new(TaskMetadataDetails::RemoteCall {
            remote_target_id: None,
            target: Some(TaskExplicitTarget::Collection {
                collection_id: hubuum_domain::CollectionId::new(fixture.collection.id).unwrap(),
            }),
        })
        .unwrap();
        let task = storage
            .create_task(
                StorageTaskCreateRequest::builder(
                    StorageTaskKind::RemoteCall,
                    hubuum_domain::PrincipalId::new(actor).unwrap(),
                    serde_json::json!({}),
                    1,
                )
                .metadata(Some(metadata))
                .try_build(100)
                .unwrap(),
            )
            .await
            .unwrap();
        let url = if cancel {
            format!("/api/v1/tasks/{}/cancel", task.id().id())
        } else if search {
            format!(
                "/api/v1/tasks?collection_id={}&include_total=true",
                fixture.collection.id
            )
        } else {
            format!("/api/v1/tasks/{}", task.id().id())
        };
        let response = if delegated {
            let backend = Arc::new(MockTreetopBackend::new());
            backend.add_task_read_rule(fixture.owner_group.id, None);
            get_request_with_permission_backend(&context.pool, token, &url, backend).await
        } else if cancel {
            crate::tests::api_operations::post_request(
                &context.pool,
                token,
                &url,
                serde_json::json!({}),
            )
            .await
        } else {
            get_request(&context.pool, token, &url).await
        };
        if search {
            assert_response_status(response, StatusCode::FORBIDDEN).await;
        } else {
            if cancel {
                assert!(matches!(
                    response.status(),
                    StatusCode::OK | StatusCode::ACCEPTED
                ));
            } else {
                assert_eq!(response.status(), StatusCode::OK);
            }
            let task: TaskResponse = test::read_body_json(response).await;
            assert!(task.details.is_none());
        }
        hubuum_storage_postgres::test_support::delete_task(context.pool.get_ref(), task.id())
            .await
            .unwrap();
        fixture.cleanup().await.unwrap();
    }
    #[rstest]
    #[case::local_admin(false, true)]
    #[case::local_non_admin(false, false)]
    #[case::delegated(true, false)]
    #[actix_web::test]
    async fn mixed_task_pages_have_a_fixed_authorization_query_budget(
        #[future(awt)] test_context: TestContext,
        #[case] delegated: bool,
        #[case] admin: bool,
    ) {
        use crate::models::{
            ExportContentType, ExportScopeKind, ExportTemplateKind, NewExportTemplate,
            NewHubuumClass, NewHubuumClassRelation, NewHubuumObject, NewHubuumObjectRelation,
            Permissions,
        };
        use crate::permissions::test_support::mock_treetop::{MockAllowRule, MockTreetopBackend};
        use crate::permissions::{AuthzTarget, ResourceRef};
        use crate::tests::api_operations::get_request_with_permission_backend;
        use crate::traits::CanSave;
        use hubuum_storage_core::{
            StorageTaskCreateRequest, StorageTaskKind, StorageTaskMetadata, TaskQueueStorage,
        };
        use hubuum_storage_postgres::{PostgresStorage, capture_queries};
        use std::sync::Arc;
        let context = test_context;
        let (actor, token) = if admin {
            (context.admin_user.id, &context.admin_token)
        } else {
            (context.normal_user.id, &context.normal_token)
        };
        let storage = PostgresStorage::unobserved(context.pool.get_ref().clone());
        let policy = Arc::new(MockTreetopBackend::new());
        let trace_id = uuid::Uuid::new_v4().simple().to_string();
        let trace =
            hubuum_events_core::TraceLink::new(&trace_id, "00f067aa0ba902b7", 1, 0).unwrap();
        let mut fixtures = Vec::new();
        let mut task_ids = Vec::new();
        let mut small_budget = None;
        // Five tasks per set cover all seven kinds of resource reference. Every
        // set uses distinct configuration IDs, collections and relation endpoints.
        for index in 0..12 {
            let mut objects = Vec::new();
            for side in ["from", "to"] {
                let fixture = context
                    .collection_fixture(&format!("batch_{index}_{side}"))
                    .await;
                fixture
                    .owner_group
                    .add_member_without_events(&context.pool, &context.normal_user)
                    .await
                    .unwrap();
                let class = NewHubuumClass {
                    name: context.scoped_name(&format!("batch_class_{index}_{side}")),
                    description: String::new(),
                    collection_id: fixture.collection.id,
                    json_schema: None,
                    validate_schema: Some(false),
                }
                .save_without_events(&context.pool)
                .await
                .unwrap();
                let object = NewHubuumObject {
                    name: context.scoped_name(&format!("batch_object_{index}_{side}")),
                    description: String::new(),
                    collection_id: fixture.collection.id,
                    hubuum_class_id: class.id,
                    data: serde_json::json!({}),
                }
                .save_without_events(&context.pool)
                .await
                .unwrap();
                objects.push((class, object));
                fixtures.push(fixture);
            }
            let fixture = &fixtures[index * 2];
            let (class, object) = &objects[0];
            let class_relation = NewHubuumClassRelation {
                from_hubuum_class_id: class.id,
                to_hubuum_class_id: objects[1].0.id,
                forward_template_alias: None,
                reverse_template_alias: None,
                from_max_relations: None,
                to_max_relations: None,
            }
            .save_without_events(&context.pool)
            .await
            .unwrap();
            let object_relation = NewHubuumObjectRelation {
                from_hubuum_object_id: object.id,
                to_hubuum_object_id: objects[1].1.id,
                class_relation_id: class_relation.id,
            }
            .save_without_events(&context.pool)
            .await
            .unwrap();
            let template = NewExportTemplate {
                collection_id: fixture.collection.id,
                name: context.scoped_name(&format!("batch_template_{index}")),
                description: String::new(),
                content_type: ExportContentType::TextPlain,
                template: "{{ name }}".into(),
                kind: ExportTemplateKind::Export,
                scope_kind: Some(ExportScopeKind::ObjectsInClass),
                class_id: Some(class.id),
                default_query: None,
                include: None,
                relation_context: None,
                default_missing_data_policy: None,
                default_limits: None,
            }
            .save_without_events(&context.pool)
            .await
            .unwrap();
            let response = crate::tests::api_operations::post_request(&context.pool, &context.admin_token, "/api/v1/remote-targets", serde_json::json!({
                "collection_id":fixture.collection.id, "name":context.scoped_name(&format!("batch_remote_{index}")),
                "description":"batch fixture", "method":"post", "url_template":"https://example.com/",
                "allowed_subject_types":["collection", "class_relation", "object_relation"],
            })).await;
            let target: crate::models::RemoteTarget =
                test::read_body_json(assert_response_status(response, StatusCode::CREATED).await)
                    .await;
            policy.add_task_read_rule(fixture.owner_group.id, None);
            // Exact facts make delegated authorization detect lost names or
            // either endpoint, as well as a wrong configuration owner.
            for (resource, permission) in [
                (
                    fixture
                        .collection
                        .to_resource_ref(&context.pool)
                        .await
                        .unwrap(),
                    Permissions::ReadCollection,
                ),
                (
                    class.to_resource_ref(&context.pool).await.unwrap(),
                    Permissions::ReadClass,
                ),
                (
                    object.to_resource_ref(&context.pool).await.unwrap(),
                    Permissions::ReadObject,
                ),
                (
                    class_relation.to_resource_ref(&context.pool).await.unwrap(),
                    Permissions::ReadClassRelation,
                ),
                (
                    object_relation
                        .to_resource_ref(&context.pool)
                        .await
                        .unwrap(),
                    Permissions::ReadObjectRelation,
                ),
                (
                    template.to_resource_ref(&context.pool).await.unwrap(),
                    Permissions::ReadTemplate,
                ),
                (
                    ResourceRef::remote_target(target.id, target.collection_id, Some(target.name)),
                    Permissions::ReadRemoteTarget,
                ),
            ] {
                let resource = resource.normalized_for_permission(permission);
                policy.add_rule(MockAllowRule {
                    group_id: fixture.owner_group.id,
                    action: permission,
                    resource_kind: resource.kind(),
                    resource_id: resource.id(),
                    attrs: resource.fields(),
                });
            }
            let metadata = [
                (
                    StorageTaskKind::Export,
                    serde_json::json!({"kind":"export", "scope_kind":"objects_in_class", "target":{"type":"class", "class_id":class.id}, "template_id":template.id}),
                ),
                (
                    StorageTaskKind::RemoteCall,
                    serde_json::json!({"kind":"remote_call", "remote_target_id":target.id, "target":{"type":"collection", "collection_id":fixture.collection.id}}),
                ),
                (
                    StorageTaskKind::RemoteCall,
                    serde_json::json!({"kind":"remote_call", "remote_target_id":target.id, "target":{"type":"object", "object_id":object.id, "class_id":class.id}}),
                ),
                (
                    StorageTaskKind::RemoteCall,
                    serde_json::json!({"kind":"remote_call", "remote_target_id":target.id, "target":{"type":"class_relation", "relation_id":class_relation.id}}),
                ),
                (
                    StorageTaskKind::RemoteCall,
                    serde_json::json!({"kind":"remote_call", "remote_target_id":target.id, "target":{"type":"object_relation", "relation_id":object_relation.id}}),
                ),
            ];
            for (kind, data) in metadata {
                let metadata = StorageTaskMetadata::from_persisted(
                    kind,
                    serde_json::json!({"version":1,"data":data}),
                )
                .unwrap();
                let task = storage
                    .create_task(
                        StorageTaskCreateRequest::builder(
                            kind,
                            hubuum_domain::PrincipalId::new(actor).unwrap(),
                            serde_json::json!({}),
                            1,
                        )
                        .metadata(Some(metadata))
                        .trace_link(Some(trace.clone()))
                        .try_build(100)
                        .unwrap(),
                    )
                    .await
                    .unwrap();
                task_ids.push(task.id());
            }
            if index != 0 && index != 11 {
                continue;
            }
            let url = format!(
                "{TASKS_ENDPOINT}?submitted_by={}&kind=export,remote_call&limit=100&include_total=true&trace_id={trace_id}",
                actor
            );
            // Warm token activity bookkeeping before measuring steady-state polling.
            let warmup = if delegated {
                get_request_with_permission_backend(&context.pool, token, &url, policy.clone())
                    .await
            } else {
                get_request(&context.pool, token, &url).await
            };
            assert_response_status(warmup, StatusCode::OK).await;
            let (response, queries) = if delegated {
                capture_queries(get_request_with_permission_backend(
                    &context.pool,
                    token,
                    &url,
                    policy.clone(),
                ))
                .await
            } else {
                capture_queries(get_request(&context.pool, token, &url)).await
            };
            let response = assert_response_status(response, StatusCode::OK).await;
            let tasks: Vec<TaskResponse> = test::read_body_json(response).await;
            assert_eq!(tasks.len(), task_ids.len());
            assert!(tasks.iter().all(|task| task.details.is_some()));
            assert!(
                queries.domain_queries() <= 20,
                "mixed-page query budget exceeded: {:?}",
                queries.query_counts()
            );
            if let Some(budget) = small_budget {
                assert_eq!(
                    queries.domain_queries(),
                    budget,
                    "query count grew with distinct references: {:?}",
                    queries.query_counts()
                );
            } else {
                small_budget = Some(queries.domain_queries());
            }
            assert_eq!(queries.queries_matching("schema_repair_reports"), 0);
            assert_eq!(queries.queries_matching("schema_impact_findings"), 0);
        }
        for id in task_ids {
            hubuum_storage_postgres::test_support::delete_task(&context.pool, id)
                .await
                .unwrap();
        }
        // Collection cleanup removes contained resources and associated relations.
        for fixture in fixtures.iter().rev() {
            fixture.cleanup().await.unwrap();
        }
    }
}
