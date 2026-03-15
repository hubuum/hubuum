#[cfg(test)]
mod tests {
    use actix_web::{http::StatusCode, test};
    use chrono::Utc;
    use rstest::rstest;

    use crate::db::traits::task::create_task_record;
    use crate::models::{NewTaskRecord, TaskKind, TaskResponse, TaskStatus};
    use crate::pagination::NEXT_CURSOR_HEADER;
    use crate::tests::api_operations::get_request;
    use crate::tests::asserts::{assert_response_status, header_value};
    use crate::tests::{TestContext, create_test_user, test_context};

    const TASKS_ENDPOINT: &str = "/api/v1/tasks";

    async fn create_synthetic_task(
        context: &TestContext,
        submitted_by: i32,
        kind: TaskKind,
        status: TaskStatus,
        label: &str,
    ) -> i32 {
        let task = create_task_record(
            &context.pool,
            NewTaskRecord {
                kind: kind.as_str().to_string(),
                status: status.as_str().to_string(),
                submitted_by: Some(submitted_by),
                idempotency_key: None,
                request_hash: None,
                request_payload: None,
                summary: Some(context.scoped_name(label)),
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
            TaskKind::Report,
            TaskStatus::Succeeded,
            "tasks_list_foreign",
        )
        .await;
        let own_task_id = create_synthetic_task(
            &context,
            context.normal_user.id,
            TaskKind::Report,
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
            TaskKind::Report,
            TaskStatus::Succeeded,
            "tasks_list_admin_normal",
        )
        .await;
        let other_user_task_id = create_synthetic_task(
            &context,
            other_user.id,
            TaskKind::Report,
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
            TaskKind::Report,
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
        let report_id = create_synthetic_task(
            &context,
            other_user.id,
            TaskKind::Report,
            TaskStatus::Succeeded,
            "tasks_sort_kind_report",
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

        assert_eq!(ids, vec![import_id, reindex_id, report_id]);
    }

    #[rstest]
    #[actix_web::test]
    async fn test_list_tasks_admin_supports_multi_field_sort(
        #[future(awt)] test_context: TestContext,
    ) {
        let context = test_context;
        let other_user = create_test_user(&context.pool).await;

        let report_id_one = create_synthetic_task(
            &context,
            other_user.id,
            TaskKind::Report,
            TaskStatus::Succeeded,
            "tasks_sort_multi_report_one",
        )
        .await;
        let report_id_two = create_synthetic_task(
            &context,
            other_user.id,
            TaskKind::Report,
            TaskStatus::Succeeded,
            "tasks_sort_multi_report_two",
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

        assert_eq!(ids, vec![import_id, report_id_two, report_id_one]);
    }
}
