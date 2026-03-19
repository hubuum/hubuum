#[cfg(test)]
mod tests {
    use actix_rt::time::sleep;
    use actix_web::{http::StatusCode, test};
    use chrono::Utc;
    use diesel::{ExpressionMethods, QueryDsl, RunQueryDsl};
    use futures::join;
    use rstest::rstest;
    use std::time::Duration;

    use crate::db::traits::task::create_task_record;
    use crate::db::with_connection;
    use crate::models::{
        GroupKey, ImportAtomicity, ImportClassInput, ImportCollisionPolicy, ImportGraph,
        ImportMode, ImportNamespaceInput, ImportNamespacePermissionInput, ImportObjectInput,
        ImportPermissionPolicy, ImportRequest, ImportTaskResultResponse, NamespaceKey,
        NewTaskRecord, Permissions, TaskEventResponse, TaskKind, TaskResponse, TaskStatus,
    };
    use crate::pagination::{NEXT_CURSOR_HEADER, TOTAL_COUNT_HEADER};
    use crate::schema::hubuumclass::dsl::{hubuumclass, name as class_name_col, namespace_id};
    use crate::schema::namespaces::dsl::{
        description as namespace_description, id as namespace_id_field, namespaces,
    };
    use crate::schema::tasks::dsl::{
        id as task_id_field, request_payload, request_redacted_at, tasks,
    };
    use crate::tests::api_operations::{get_request, post_request_with_headers};
    use crate::tests::asserts::{assert_response_status, header_value};
    use crate::tests::{TestContext, create_test_group, test_context};

    const IMPORTS_ENDPOINT: &str = "/api/v1/imports";

    async fn wait_for_task(
        context: &TestContext,
        task_id: i32,
        expected_terminal_statuses: &[TaskStatus],
    ) -> TaskResponse {
        wait_for_task_with_token(
            &context.pool,
            &context.admin_token,
            task_id,
            expected_terminal_statuses,
        )
        .await
    }

    async fn wait_for_task_with_token(
        pool: &crate::db::DbPool,
        token: &str,
        task_id: i32,
        expected_terminal_statuses: &[TaskStatus],
    ) -> TaskResponse {
        for _ in 0..50 {
            let resp = get_request(pool, token, &format!("/api/v1/tasks/{task_id}")).await;
            let resp = assert_response_status(resp, StatusCode::OK).await;
            let task: TaskResponse = test::read_body_json(resp).await;
            if expected_terminal_statuses.contains(&task.status) {
                return task;
            }
            sleep(Duration::from_millis(100)).await;
        }

        panic!("Task {task_id} did not reach a terminal status in time");
    }

    fn namespace_import_request(
        name: String,
        description: &str,
        mode: ImportMode,
    ) -> ImportRequest {
        ImportRequest {
            version: 1,
            dry_run: Some(false),
            mode: Some(mode),
            graph: ImportGraph {
                namespaces: vec![ImportNamespaceInput {
                    ref_: Some("ns:primary".to_string()),
                    name,
                    description: description.to_string(),
                }],
                ..ImportGraph::default()
            },
        }
    }

    #[rstest]
    #[actix_web::test]
    async fn test_import_creates_task_events_results_and_redacts_payload(
        #[future(awt)] test_context: TestContext,
    ) {
        let context = test_context;
        let delegate_group = create_test_group(&context.pool).await;
        let import_namespace_name = context.scoped_name("import_ns");
        let import_class_name = context.scoped_name("import_class");
        let object_name = context.scoped_name("import_object");

        let body = ImportRequest {
            version: 1,
            dry_run: Some(false),
            mode: Some(ImportMode {
                atomicity: Some(ImportAtomicity::Strict),
                collision_policy: None,
                permission_policy: None,
            }),
            graph: ImportGraph {
                namespaces: vec![ImportNamespaceInput {
                    ref_: Some("ns:primary".to_string()),
                    name: import_namespace_name.clone(),
                    description: "Imported namespace".to_string(),
                }],
                classes: vec![ImportClassInput {
                    ref_: Some("class:primary".to_string()),
                    name: import_class_name.clone(),
                    description: "Imported class".to_string(),
                    json_schema: None,
                    validate_schema: Some(false),
                    namespace_ref: Some("ns:primary".to_string()),
                    namespace_key: None,
                }],
                objects: vec![ImportObjectInput {
                    ref_: Some("object:primary".to_string()),
                    name: object_name.clone(),
                    description: "Imported object".to_string(),
                    data: serde_json::json!({"hostname": object_name}),
                    class_ref: Some("class:primary".to_string()),
                    class_key: None,
                }],
                namespace_permissions: vec![ImportNamespacePermissionInput {
                    ref_: Some("acl:primary".to_string()),
                    namespace_ref: Some("ns:primary".to_string()),
                    namespace_key: None,
                    group_key: GroupKey {
                        groupname: delegate_group.groupname.clone(),
                    },
                    permissions: vec![Permissions::ReadCollection, Permissions::ReadClass],
                    replace_existing: Some(false),
                }],
                ..ImportGraph::default()
            },
        };

        let resp = post_request_with_headers(
            &context.pool,
            &context.admin_token,
            IMPORTS_ENDPOINT,
            &body,
            vec![(
                actix_web::http::header::HeaderName::from_static("idempotency-key"),
                context.scoped_name("import-idempotency"),
            )],
        )
        .await;

        let resp = assert_response_status(resp, StatusCode::ACCEPTED).await;
        let location = header_value(&resp, "Location");
        let task: TaskResponse = test::read_body_json(resp).await;
        assert_eq!(location, Some(format!("/api/v1/tasks/{}", task.id)));
        assert_eq!(task.kind, TaskKind::Import);
        assert!(matches!(
            task.status,
            TaskStatus::Queued
                | TaskStatus::Validating
                | TaskStatus::Running
                | TaskStatus::Succeeded
        ));
        assert!(task.links.import.is_some());

        let completed = wait_for_task(
            &context,
            task.id,
            &[TaskStatus::Succeeded, TaskStatus::PartiallySucceeded],
        )
        .await;
        assert_eq!(completed.status, TaskStatus::Succeeded);
        assert!(completed.request_redacted_at.is_some());
        let finished_at = completed
            .finished_at
            .expect("completed import task should have finished_at");

        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &format!("/api/v1/tasks/{}/events", task.id),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let events: Vec<TaskEventResponse> = test::read_body_json(resp).await;
        let event_types = events
            .iter()
            .map(|event| event.event_type.as_str())
            .collect::<Vec<_>>();
        assert!(event_types.contains(&"queued"));
        assert!(event_types.contains(&"validating"));
        assert!(event_types.contains(&"running"));
        assert!(event_types.contains(&"succeeded"));
        let last_event_at = events
            .iter()
            .map(|event| event.created_at)
            .max()
            .expect("task should have events");
        assert!(finished_at >= last_event_at);

        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &format!("/api/v1/imports/{}/results", task.id),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let results: Vec<ImportTaskResultResponse> = test::read_body_json(resp).await;
        assert_eq!(results.len(), 4);
        assert!(results.iter().all(|result| result.outcome == "succeeded"));
        let last_result_at = results
            .iter()
            .map(|result| result.created_at)
            .max()
            .expect("import should have results");
        assert!(finished_at >= last_result_at);

        let stored_payload = with_connection(&context.pool, |conn| {
            tasks
                .filter(task_id_field.eq(task.id))
                .select((request_payload, request_redacted_at))
                .first::<(Option<serde_json::Value>, Option<chrono::NaiveDateTime>)>(conn)
        })
        .unwrap();
        assert!(stored_payload.0.is_none());
        assert!(stored_payload.1.is_some());
    }

    #[rstest]
    #[actix_web::test]
    async fn test_generic_task_endpoint_supports_non_import_kind(
        #[future(awt)] test_context: TestContext,
    ) {
        let context = test_context;
        let task = create_task_record(
            &context.pool,
            NewTaskRecord {
                kind: TaskKind::Report.as_str().to_string(),
                status: TaskStatus::Succeeded.as_str().to_string(),
                submitted_by: Some(context.admin_user.id),
                idempotency_key: None,
                request_hash: None,
                request_payload: None,
                summary: Some("Synthetic report task".to_string()),
                total_items: 0,
                processed_items: 0,
                success_items: 0,
                failed_items: 0,
                request_redacted_at: Some(Utc::now().naive_utc()),
                started_at: Some(Utc::now().naive_utc()),
                finished_at: Some(Utc::now().naive_utc()),
            },
        )
        .await
        .unwrap();

        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &format!("/api/v1/tasks/{}", task.id),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let task_response: TaskResponse = test::read_body_json(resp).await;
        assert_eq!(task_response.kind, TaskKind::Report);
        assert_eq!(task_response.status, TaskStatus::Succeeded);
        assert!(task_response.links.import.is_none());
        assert!(task_response.details.is_none());
    }

    #[rstest]
    #[actix_web::test]
    async fn test_import_idempotency_returns_same_task(#[future(awt)] test_context: TestContext) {
        let context = test_context;
        let import_namespace_name = context.scoped_name("idempotent_import_ns");
        let idempotency = context.scoped_name("same-task");

        let body = ImportRequest {
            version: 1,
            dry_run: Some(true),
            mode: None,
            graph: ImportGraph {
                namespaces: vec![ImportNamespaceInput {
                    ref_: Some("ns:dry".to_string()),
                    name: import_namespace_name,
                    description: "Dry run namespace".to_string(),
                }],
                ..ImportGraph::default()
            },
        };

        let first = post_request_with_headers(
            &context.pool,
            &context.admin_token,
            IMPORTS_ENDPOINT,
            &body,
            vec![(
                actix_web::http::header::HeaderName::from_static("idempotency-key"),
                idempotency.clone(),
            )],
        )
        .await;
        let first = assert_response_status(first, StatusCode::ACCEPTED).await;
        let first_task: TaskResponse = test::read_body_json(first).await;

        let second = post_request_with_headers(
            &context.pool,
            &context.admin_token,
            IMPORTS_ENDPOINT,
            &body,
            vec![(
                actix_web::http::header::HeaderName::from_static("idempotency-key"),
                idempotency,
            )],
        )
        .await;
        let second = assert_response_status(second, StatusCode::ACCEPTED).await;
        let second_task: TaskResponse = test::read_body_json(second).await;

        assert_eq!(first_task.id, second_task.id);
    }

    #[rstest]
    #[actix_web::test]
    async fn test_import_idempotency_reuses_task_under_concurrent_submissions(
        #[future(awt)] test_context: TestContext,
    ) {
        let context = test_context;

        for iteration in 0..10 {
            let idempotency = context.scoped_name(&format!("same-task-concurrent-{iteration}"));
            let body = ImportRequest {
                version: 1,
                dry_run: Some(true),
                mode: None,
                graph: ImportGraph {
                    namespaces: vec![ImportNamespaceInput {
                        ref_: Some("ns:dry".to_string()),
                        name: context
                            .scoped_name(&format!("idempotent_import_ns_concurrent_{iteration}")),
                        description: "Dry run namespace".to_string(),
                    }],
                    ..ImportGraph::default()
                },
            };

            let first = post_request_with_headers(
                &context.pool,
                &context.admin_token,
                IMPORTS_ENDPOINT,
                body.clone(),
                vec![(
                    actix_web::http::header::HeaderName::from_static("idempotency-key"),
                    idempotency.clone(),
                )],
            );
            let second = post_request_with_headers(
                &context.pool,
                &context.admin_token,
                IMPORTS_ENDPOINT,
                body,
                vec![(
                    actix_web::http::header::HeaderName::from_static("idempotency-key"),
                    idempotency,
                )],
            );

            let (first, second) = join!(first, second);
            let first = assert_response_status(first, StatusCode::ACCEPTED).await;
            let second = assert_response_status(second, StatusCode::ACCEPTED).await;
            let first_task: TaskResponse = test::read_body_json(first).await;
            let second_task: TaskResponse = test::read_body_json(second).await;

            assert_eq!(first_task.id, second_task.id);
        }
    }

    #[rstest]
    #[actix_web::test]
    async fn test_import_idempotency_conflicts_for_non_import_task_or_changed_payload(
        #[future(awt)] test_context: TestContext,
    ) {
        let context = test_context;
        let report_key = context.scoped_name("report-task-idempotency");
        let report_task = create_task_record(
            &context.pool,
            NewTaskRecord {
                kind: TaskKind::Report.as_str().to_string(),
                status: TaskStatus::Queued.as_str().to_string(),
                submitted_by: Some(context.admin_user.id),
                idempotency_key: Some(report_key.clone()),
                request_hash: Some(context.scoped_name("report-task-hash")),
                request_payload: None,
                summary: None,
                total_items: 0,
                processed_items: 0,
                success_items: 0,
                failed_items: 0,
                request_redacted_at: None,
                started_at: None,
                finished_at: None,
            },
        )
        .await
        .unwrap();

        let body = ImportRequest {
            version: 1,
            dry_run: Some(true),
            mode: None,
            graph: ImportGraph {
                namespaces: vec![ImportNamespaceInput {
                    ref_: Some("ns:conflict".to_string()),
                    name: context.scoped_name("idempotency_conflict_namespace"),
                    description: "Dry run namespace".to_string(),
                }],
                ..ImportGraph::default()
            },
        };

        let resp = post_request_with_headers(
            &context.pool,
            &context.admin_token,
            IMPORTS_ENDPOINT,
            &body,
            vec![(
                actix_web::http::header::HeaderName::from_static("idempotency-key"),
                report_key,
            )],
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::CONFLICT).await;
        let error: serde_json::Value = test::read_body_json(resp).await;
        assert!(
            error["message"]
                .as_str()
                .unwrap_or_default()
                .contains("Idempotency-Key")
        );

        let reused = get_request(
            &context.pool,
            &context.admin_token,
            &format!("/api/v1/tasks/{}", report_task.id),
        )
        .await;
        assert_response_status(reused, StatusCode::OK).await;

        let import_key = context.scoped_name("import-task-idempotency");
        let first = post_request_with_headers(
            &context.pool,
            &context.admin_token,
            IMPORTS_ENDPOINT,
            &body,
            vec![(
                actix_web::http::header::HeaderName::from_static("idempotency-key"),
                import_key.clone(),
            )],
        )
        .await;
        let first = assert_response_status(first, StatusCode::ACCEPTED).await;
        let first_task: TaskResponse = test::read_body_json(first).await;

        let changed_body = ImportRequest {
            version: 1,
            dry_run: Some(true),
            mode: None,
            graph: ImportGraph {
                namespaces: vec![ImportNamespaceInput {
                    ref_: Some("ns:conflict".to_string()),
                    name: context.scoped_name("idempotency_conflict_namespace_changed"),
                    description: "Changed dry run namespace".to_string(),
                }],
                ..ImportGraph::default()
            },
        };

        let second = post_request_with_headers(
            &context.pool,
            &context.admin_token,
            IMPORTS_ENDPOINT,
            &changed_body,
            vec![(
                actix_web::http::header::HeaderName::from_static("idempotency-key"),
                import_key,
            )],
        )
        .await;
        assert_response_status(second, StatusCode::CONFLICT).await;

        let completed = wait_for_task(&context, first_task.id, &[TaskStatus::Succeeded]).await;
        assert_eq!(completed.status, TaskStatus::Succeeded);
    }

    #[rstest]
    #[actix_web::test]
    async fn test_import_rejects_unsupported_version(#[future(awt)] test_context: TestContext) {
        let context = test_context;
        let body = ImportRequest {
            version: 2,
            dry_run: Some(true),
            mode: None,
            graph: ImportGraph {
                namespaces: vec![ImportNamespaceInput {
                    ref_: Some("ns:unsupported".to_string()),
                    name: context.scoped_name("unsupported_import_version"),
                    description: "unsupported".to_string(),
                }],
                ..ImportGraph::default()
            },
        };

        let resp = post_request_with_headers(
            &context.pool,
            &context.admin_token,
            IMPORTS_ENDPOINT,
            &body,
            vec![],
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::BAD_REQUEST).await;
        let error: serde_json::Value = test::read_body_json(resp).await;
        assert!(
            error["message"]
                .as_str()
                .unwrap_or_default()
                .contains("Unsupported import version")
        );
    }

    #[rstest]
    #[actix_web::test]
    async fn test_task_events_and_import_results_cursor_pagination(
        #[future(awt)] test_context: TestContext,
    ) {
        let context = test_context;
        let delegate_group = create_test_group(&context.pool).await;
        let body = ImportRequest {
            version: 1,
            dry_run: Some(false),
            mode: Some(ImportMode {
                atomicity: Some(ImportAtomicity::Strict),
                collision_policy: None,
                permission_policy: None,
            }),
            graph: ImportGraph {
                namespaces: vec![ImportNamespaceInput {
                    ref_: Some("ns:page".to_string()),
                    name: context.scoped_name("paged_import_ns"),
                    description: "Imported namespace".to_string(),
                }],
                classes: vec![ImportClassInput {
                    ref_: Some("class:page".to_string()),
                    name: context.scoped_name("paged_import_class"),
                    description: "Imported class".to_string(),
                    json_schema: None,
                    validate_schema: Some(false),
                    namespace_ref: Some("ns:page".to_string()),
                    namespace_key: None,
                }],
                objects: vec![ImportObjectInput {
                    ref_: Some("object:page".to_string()),
                    name: context.scoped_name("paged_import_object"),
                    description: "Imported object".to_string(),
                    data: serde_json::json!({"hostname": "paged"}),
                    class_ref: Some("class:page".to_string()),
                    class_key: None,
                }],
                namespace_permissions: vec![ImportNamespacePermissionInput {
                    ref_: Some("acl:page".to_string()),
                    namespace_ref: Some("ns:page".to_string()),
                    namespace_key: None,
                    group_key: GroupKey {
                        groupname: delegate_group.groupname.clone(),
                    },
                    permissions: vec![Permissions::ReadCollection],
                    replace_existing: Some(false),
                }],
                ..ImportGraph::default()
            },
        };

        let resp = post_request_with_headers(
            &context.pool,
            &context.admin_token,
            IMPORTS_ENDPOINT,
            &body,
            vec![],
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::ACCEPTED).await;
        let task: TaskResponse = test::read_body_json(resp).await;
        let _ = wait_for_task(&context, task.id, &[TaskStatus::Succeeded]).await;

        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &format!("/api/v1/tasks/{}/events?limit=2&sort=id", task.id),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let next_cursor = header_value(&resp, NEXT_CURSOR_HEADER);
        let first_event_total =
            header_value(&resp, TOTAL_COUNT_HEADER).and_then(|value| value.parse::<i64>().ok());
        let first_events: Vec<TaskEventResponse> = test::read_body_json(resp).await;
        assert_eq!(first_events.len(), 2);
        assert!(first_event_total.unwrap_or_default() >= first_events.len() as i64);
        assert!(next_cursor.is_some());

        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &format!(
                "/api/v1/tasks/{}/events?limit=2&sort=id&cursor={}",
                task.id,
                next_cursor.clone().unwrap()
            ),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let second_event_total =
            header_value(&resp, TOTAL_COUNT_HEADER).and_then(|value| value.parse::<i64>().ok());
        let second_events: Vec<TaskEventResponse> = test::read_body_json(resp).await;
        assert_eq!(second_event_total, first_event_total);
        assert!(!second_events.is_empty());
        assert!(second_events[0].id > first_events.last().unwrap().id);

        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &format!("/api/v1/imports/{}/results?limit=2&sort=id", task.id),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let next_cursor = header_value(&resp, NEXT_CURSOR_HEADER);
        let first_result_total =
            header_value(&resp, TOTAL_COUNT_HEADER).and_then(|value| value.parse::<i64>().ok());
        let first_results: Vec<ImportTaskResultResponse> = test::read_body_json(resp).await;
        assert_eq!(first_results.len(), 2);
        assert!(first_result_total.unwrap_or_default() >= first_results.len() as i64);
        assert!(next_cursor.is_some());

        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &format!(
                "/api/v1/imports/{}/results?limit=2&sort=id&cursor={}",
                task.id,
                next_cursor.unwrap()
            ),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let second_result_total =
            header_value(&resp, TOTAL_COUNT_HEADER).and_then(|value| value.parse::<i64>().ok());
        let second_results: Vec<ImportTaskResultResponse> = test::read_body_json(resp).await;
        assert_eq!(second_result_total, first_result_total);
        assert!(!second_results.is_empty());
        assert!(second_results[0].id > first_results.last().unwrap().id);
    }

    #[rstest]
    #[actix_web::test]
    async fn test_import_collision_abort_keeps_existing_data(
        #[future(awt)] test_context: TestContext,
    ) {
        let context = test_context;
        let fixture = context.namespace_fixture("collision_abort").await;

        let body = namespace_import_request(
            fixture.namespace.name.clone(),
            "updated-by-import",
            ImportMode {
                atomicity: Some(ImportAtomicity::Strict),
                collision_policy: Some(ImportCollisionPolicy::Abort),
                permission_policy: Some(ImportPermissionPolicy::Abort),
            },
        );

        let resp = post_request_with_headers(
            &context.pool,
            &context.admin_token,
            IMPORTS_ENDPOINT,
            &body,
            vec![],
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::ACCEPTED).await;
        let task: TaskResponse = test::read_body_json(resp).await;
        let completed = wait_for_task(&context, task.id, &[TaskStatus::Failed]).await;
        assert_eq!(completed.status, TaskStatus::Failed);

        let description = with_connection(&context.pool, |conn| {
            namespaces
                .filter(namespace_id_field.eq(fixture.namespace.id))
                .select(namespace_description)
                .first::<String>(conn)
        })
        .unwrap();
        assert_eq!(description, fixture.namespace.description);
    }

    #[rstest]
    #[actix_web::test]
    async fn test_import_collision_overwrite_updates_existing_data(
        #[future(awt)] test_context: TestContext,
    ) {
        let context = test_context;
        let fixture = context.namespace_fixture("collision_overwrite").await;
        let updated_description = context.scoped_name("collision_overwrite_description");

        let body = namespace_import_request(
            fixture.namespace.name.clone(),
            &updated_description,
            ImportMode {
                atomicity: Some(ImportAtomicity::Strict),
                collision_policy: Some(ImportCollisionPolicy::Overwrite),
                permission_policy: Some(ImportPermissionPolicy::Abort),
            },
        );

        let resp = post_request_with_headers(
            &context.pool,
            &context.admin_token,
            IMPORTS_ENDPOINT,
            &body,
            vec![],
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::ACCEPTED).await;
        let task: TaskResponse = test::read_body_json(resp).await;
        let completed = wait_for_task(&context, task.id, &[TaskStatus::Succeeded]).await;
        assert_eq!(completed.status, TaskStatus::Succeeded);

        let description = with_connection(&context.pool, |conn| {
            namespaces
                .filter(namespace_id_field.eq(fixture.namespace.id))
                .select(namespace_description)
                .first::<String>(conn)
        })
        .unwrap();
        assert_eq!(description, updated_description);
    }

    #[rstest]
    #[actix_web::test]
    async fn test_import_permission_continue_allows_partial_success(
        #[future(awt)] test_context: TestContext,
    ) {
        let context = test_context;
        let allowed = context
            .namespace_fixture("permission_continue_allowed")
            .await;
        let forbidden = context
            .namespace_fixture("permission_continue_forbidden")
            .await;
        allowed
            .owner_group
            .add_member(&context.pool, &context.normal_user)
            .await
            .unwrap();

        let allowed_class = context.scoped_name("permission_continue_allowed_class");
        let forbidden_class = context.scoped_name("permission_continue_forbidden_class");
        let body = ImportRequest {
            version: 1,
            dry_run: Some(false),
            mode: Some(ImportMode {
                atomicity: Some(ImportAtomicity::BestEffort),
                collision_policy: Some(ImportCollisionPolicy::Abort),
                permission_policy: Some(ImportPermissionPolicy::Continue),
            }),
            graph: ImportGraph {
                classes: vec![
                    ImportClassInput {
                        ref_: Some("class:allowed".to_string()),
                        name: allowed_class.clone(),
                        description: "allowed".to_string(),
                        json_schema: None,
                        validate_schema: Some(false),
                        namespace_ref: None,
                        namespace_key: Some(NamespaceKey {
                            name: allowed.namespace.name.clone(),
                        }),
                    },
                    ImportClassInput {
                        ref_: Some("class:forbidden".to_string()),
                        name: forbidden_class.clone(),
                        description: "forbidden".to_string(),
                        json_schema: None,
                        validate_schema: Some(false),
                        namespace_ref: None,
                        namespace_key: Some(NamespaceKey {
                            name: forbidden.namespace.name.clone(),
                        }),
                    },
                ],
                ..ImportGraph::default()
            },
        };

        let resp = post_request_with_headers(
            &context.pool,
            &context.normal_token,
            IMPORTS_ENDPOINT,
            &body,
            vec![],
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::ACCEPTED).await;
        let task: TaskResponse = test::read_body_json(resp).await;
        let completed = wait_for_task_with_token(
            &context.pool,
            &context.normal_token,
            task.id,
            &[TaskStatus::PartiallySucceeded],
        )
        .await;
        assert_eq!(completed.status, TaskStatus::PartiallySucceeded);

        let created = with_connection(&context.pool, |conn| {
            hubuumclass
                .filter(class_name_col.eq(&allowed_class))
                .filter(namespace_id.eq(allowed.namespace.id))
                .count()
                .get_result::<i64>(conn)
        })
        .unwrap();
        let blocked = with_connection(&context.pool, |conn| {
            hubuumclass
                .filter(class_name_col.eq(&forbidden_class))
                .filter(namespace_id.eq(forbidden.namespace.id))
                .count()
                .get_result::<i64>(conn)
        })
        .unwrap();
        assert_eq!(created, 1);
        assert_eq!(blocked, 0);

        let resp = get_request(
            &context.pool,
            &context.normal_token,
            &format!("/api/v1/imports/{}/results", task.id),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let results: Vec<ImportTaskResultResponse> = test::read_body_json(resp).await;
        assert_eq!(results.len(), 2);
        assert_eq!(
            results
                .iter()
                .filter(|result| result.outcome == "succeeded")
                .count(),
            1
        );
        assert_eq!(
            results
                .iter()
                .filter(|result| result.outcome == "failed")
                .count(),
            1
        );
    }

    #[rstest]
    #[actix_web::test]
    async fn test_import_permission_abort_prevents_any_mutation(
        #[future(awt)] test_context: TestContext,
    ) {
        let context = test_context;
        let allowed = context.namespace_fixture("permission_abort_allowed").await;
        let forbidden = context
            .namespace_fixture("permission_abort_forbidden")
            .await;
        allowed
            .owner_group
            .add_member(&context.pool, &context.normal_user)
            .await
            .unwrap();

        let allowed_class = context.scoped_name("permission_abort_allowed_class");
        let forbidden_class = context.scoped_name("permission_abort_forbidden_class");
        let body = ImportRequest {
            version: 1,
            dry_run: Some(false),
            mode: Some(ImportMode {
                atomicity: Some(ImportAtomicity::BestEffort),
                collision_policy: Some(ImportCollisionPolicy::Abort),
                permission_policy: Some(ImportPermissionPolicy::Abort),
            }),
            graph: ImportGraph {
                classes: vec![
                    ImportClassInput {
                        ref_: Some("class:allowed".to_string()),
                        name: allowed_class.clone(),
                        description: "allowed".to_string(),
                        json_schema: None,
                        validate_schema: Some(false),
                        namespace_ref: None,
                        namespace_key: Some(NamespaceKey {
                            name: allowed.namespace.name.clone(),
                        }),
                    },
                    ImportClassInput {
                        ref_: Some("class:forbidden".to_string()),
                        name: forbidden_class.clone(),
                        description: "forbidden".to_string(),
                        json_schema: None,
                        validate_schema: Some(false),
                        namespace_ref: None,
                        namespace_key: Some(NamespaceKey {
                            name: forbidden.namespace.name.clone(),
                        }),
                    },
                ],
                ..ImportGraph::default()
            },
        };

        let resp = post_request_with_headers(
            &context.pool,
            &context.normal_token,
            IMPORTS_ENDPOINT,
            &body,
            vec![],
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::ACCEPTED).await;
        let task: TaskResponse = test::read_body_json(resp).await;
        let completed = wait_for_task_with_token(
            &context.pool,
            &context.normal_token,
            task.id,
            &[TaskStatus::Failed],
        )
        .await;
        assert_eq!(completed.status, TaskStatus::Failed);

        let created = with_connection(&context.pool, |conn| {
            hubuumclass
                .filter(class_name_col.eq_any([allowed_class.clone(), forbidden_class.clone()]))
                .count()
                .get_result::<i64>(conn)
        })
        .unwrap();
        assert_eq!(created, 0);
    }

    #[rstest]
    #[actix_web::test]
    async fn test_task_and_import_endpoints_forbid_non_owner_access(
        #[future(awt)] test_context: TestContext,
    ) {
        let context = test_context;
        let body = ImportRequest {
            version: 1,
            dry_run: Some(true),
            mode: None,
            graph: ImportGraph {
                namespaces: vec![ImportNamespaceInput {
                    ref_: Some("ns:private".to_string()),
                    name: context.scoped_name("private_task_namespace"),
                    description: "private".to_string(),
                }],
                ..ImportGraph::default()
            },
        };

        let resp = post_request_with_headers(
            &context.pool,
            &context.admin_token,
            IMPORTS_ENDPOINT,
            &body,
            vec![],
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::ACCEPTED).await;
        let task: TaskResponse = test::read_body_json(resp).await;
        let _ = wait_for_task(&context, task.id, &[TaskStatus::Succeeded]).await;

        for endpoint in [
            format!("/api/v1/tasks/{}", task.id),
            format!("/api/v1/tasks/{}/events", task.id),
            format!("/api/v1/imports/{}", task.id),
            format!("/api/v1/imports/{}/results", task.id),
        ] {
            let resp = get_request(&context.pool, &context.normal_token, &endpoint).await;
            assert_response_status(resp, StatusCode::NOT_FOUND).await;
        }
    }
}
