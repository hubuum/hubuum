#[cfg(test)]
mod tests {
    use std::time::Duration;

    use actix_rt::time::sleep;
    use actix_web::{http::StatusCode, test};
    use rstest::rstest;
    use sha2::{Digest, Sha256};

    use crate::models::{
        BackupDocument, BackupRequest, Permissions, TaskKind, TaskResponse, TaskStatus,
    };
    use crate::tests::api_operations::{get_request, post_request};
    use crate::tests::asserts::{assert_response_status, header_value};
    use crate::tests::{
        TestContext, TestMutex, lock_test_mutex, scoped_token, test_context, test_mutex,
    };

    static BACKUP_TASK_TEST_LOCK: TestMutex = test_mutex();

    #[derive(Clone, Copy)]
    enum RejectedBackupCaller {
        NormalUser,
        ScopedAdministrator,
    }

    async fn wait_for_backup(context: &TestContext, task_id: i32) -> TaskResponse {
        let mut last_task = None;
        for _ in 0..200 {
            let response = get_request(
                &context.pool,
                &context.admin_token,
                &format!("/api/v1/backups/{task_id}"),
            )
            .await;
            let response = assert_response_status(response, StatusCode::OK).await;
            let task: TaskResponse = test::read_body_json(response).await;
            if task.status == TaskStatus::Succeeded {
                return task;
            }
            if matches!(task.status, TaskStatus::Failed | TaskStatus::Cancelled) {
                panic!(
                    "Backup task {task_id} reached {:?}: {:?}",
                    task.status, task.summary
                );
            }
            last_task = Some(task);
            sleep(Duration::from_millis(100)).await;
        }
        panic!(
            "Backup task {task_id} did not finish; last state: {:?}",
            last_task.map(|task| task.status)
        );
    }

    #[rstest]
    #[actix_web::test]
    async fn full_backup_is_consistent_and_refetchable(#[future(awt)] test_context: TestContext) {
        let _guard = lock_test_mutex(&BACKUP_TASK_TEST_LOCK).await;
        let context = test_context;
        let request = BackupRequest {
            include_history: false,
        };

        let response = post_request(
            &context.pool,
            &context.admin_token,
            "/api/v1/backups",
            &request,
        )
        .await;
        let response = assert_response_status(response, StatusCode::ACCEPTED).await;
        let accepted: TaskResponse = test::read_body_json(response).await;
        assert_eq!(accepted.kind, TaskKind::Backup);

        let completed = wait_for_backup(&context, accepted.id).await;
        let details = completed
            .details
            .and_then(|details| details.backup)
            .expect("completed backup should expose output details");
        assert!(details.output_available);

        let response = get_request(
            &context.pool,
            &context.admin_token,
            &format!("/api/v1/backups/{}/output", accepted.id),
        )
        .await;
        let response = assert_response_status(response, StatusCode::OK).await;
        let advertised_sha = header_value(&response, "X-Hubuum-Backup-SHA256")
            .expect("backup output should advertise its SHA-256");
        let cache_control =
            header_value(&response, "Cache-Control").expect("backup output cache policy");
        let content_disposition =
            header_value(&response, "Content-Disposition").expect("backup attachment policy");
        let bytes = test::read_body(response).await;
        let actual_sha = Sha256::digest(&bytes)
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        assert_eq!(
            (advertised_sha, cache_control, content_disposition),
            (
                actual_sha,
                "no-store".to_string(),
                format!(
                    "attachment; filename=\"hubuum-backup-{}.json\"",
                    accepted.id
                ),
            )
        );

        let document: BackupDocument = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(
            (
                document.history.is_none(),
                document.state.sections.contains_key("identity_scopes"),
                document.state.sections.contains_key("collections"),
                document.state.sections.contains_key("tokens"),
                document.state.sections.contains_key("backup_task_outputs"),
            ),
            (true, true, true, false, false)
        );
    }

    #[rstest]
    #[actix_web::test]
    async fn generic_task_projections_include_backup_output_summaries(
        #[future(awt)] test_context: TestContext,
    ) {
        let _guard = lock_test_mutex(&BACKUP_TASK_TEST_LOCK).await;
        let context = test_context;
        let response = post_request(
            &context.pool,
            &context.admin_token,
            "/api/v1/backups",
            &BackupRequest {
                include_history: false,
            },
        )
        .await;
        let response = assert_response_status(response, StatusCode::ACCEPTED).await;
        let accepted: TaskResponse = test::read_body_json(response).await;
        wait_for_backup(&context, accepted.id).await;

        let response = get_request(
            &context.pool,
            &context.admin_token,
            &format!("/api/v1/tasks/{}", accepted.id),
        )
        .await;
        let response = assert_response_status(response, StatusCode::OK).await;
        let task: TaskResponse = test::read_body_json(response).await;
        let details = task.details.and_then(|details| details.backup).unwrap();
        assert_eq!(
            (
                details.output_available,
                details.byte_size.is_some(),
                details.sha256.is_some(),
            ),
            (true, true, true)
        );

        let response = get_request(
            &context.pool,
            &context.admin_token,
            &format!(
                "/api/v1/tasks?kind=backup&submitted_by={}&sort=id.desc&limit=10",
                context.admin_user.id
            ),
        )
        .await;
        let response = assert_response_status(response, StatusCode::OK).await;
        let tasks: Vec<TaskResponse> = test::read_body_json(response).await;
        let listed = tasks
            .iter()
            .find(|task| task.id == accepted.id)
            .expect("backup task in generic task list");
        assert!(
            listed
                .details
                .as_ref()
                .and_then(|details| details.backup.as_ref())
                .is_some_and(|details| details.output_available)
        );
    }

    #[rstest]
    #[case::normal_user(RejectedBackupCaller::NormalUser)]
    #[case::scoped_administrator(RejectedBackupCaller::ScopedAdministrator)]
    #[actix_web::test]
    async fn full_backup_rejects_non_administrator(
        #[future(awt)] test_context: TestContext,
        #[case] caller: RejectedBackupCaller,
    ) {
        let context = test_context;
        let request = BackupRequest {
            include_history: false,
        };
        let token = match caller {
            RejectedBackupCaller::NormalUser => context.normal_token.clone(),
            RejectedBackupCaller::ScopedAdministrator => {
                scoped_token(
                    &context.pool,
                    context.admin_user.id,
                    &[Permissions::ReadCollection],
                )
                .await
            }
        };

        let response = post_request(&context.pool, &token, "/api/v1/backups", &request).await;

        assert_response_status(response, StatusCode::FORBIDDEN).await;
    }

    #[rstest]
    #[actix_web::test]
    async fn backup_request_rejects_partial_scope_fields(#[future(awt)] test_context: TestContext) {
        let context = test_context;
        let response = post_request(
            &context.pool,
            &context.admin_token,
            "/api/v1/backups",
            &serde_json::json!({
                "scope": {
                    "kind": "collections",
                    "collection_ids": [1]
                }
            }),
        )
        .await;

        assert_response_status(response, StatusCode::BAD_REQUEST).await;
    }

    #[rstest]
    #[case::status("")]
    #[case::output("/output")]
    #[actix_web::test]
    async fn backup_artifacts_require_an_administrator(
        #[future(awt)] test_context: TestContext,
        #[case] suffix: &str,
    ) {
        let _guard = lock_test_mutex(&BACKUP_TASK_TEST_LOCK).await;
        let context = test_context;
        let response = post_request(
            &context.pool,
            &context.admin_token,
            "/api/v1/backups",
            &BackupRequest {
                include_history: false,
            },
        )
        .await;
        let response = assert_response_status(response, StatusCode::ACCEPTED).await;
        let accepted: TaskResponse = test::read_body_json(response).await;
        wait_for_backup(&context, accepted.id).await;

        let response = get_request(
            &context.pool,
            &context.normal_token,
            &format!("/api/v1/backups/{}{suffix}", accepted.id),
        )
        .await;

        assert_response_status(response, StatusCode::FORBIDDEN).await;
    }
}
