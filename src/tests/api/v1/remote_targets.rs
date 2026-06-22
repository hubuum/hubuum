#[cfg(test)]
mod tests {
    use actix_rt::time::sleep;
    use actix_web::{
        http::{StatusCode, header},
        test,
    };
    use std::time::Duration;

    use crate::models::{
        NewHubuumClass, NewHubuumObject, Permissions, PermissionsList, RemoteTarget, TaskResponse,
        TaskStatus,
    };
    use crate::tests::TestContext;
    use crate::tests::api_operations::{
        delete_request, get_request, patch_request, post_request, post_request_with_headers,
    };
    use crate::tests::asserts::{assert_response_status, header_value};
    use crate::tests::{create_test_group, create_test_user};
    use crate::traits::{PermissionController, SelfAccessors};

    const RT_ENDPOINT: &str = "/api/v1/remote-targets";

    /// Create a namespace + class + object owned by the admin group so the admin token holds
    /// every remote-target permission for it. Returns (namespace_id, class_id, object_id).
    async fn setup_object(context: &TestContext, label: &str) -> (i32, i32, i32) {
        let fixture = context
            .object_fixture(
                label,
                NewHubuumClass {
                    name: context.scoped_name(&format!("{label}-class")),
                    description: "remote target test class".to_string(),
                    namespace_id: 0,
                    json_schema: None,
                    validate_schema: Some(false),
                },
                vec![NewHubuumObject {
                    name: context.scoped_name(&format!("{label}-object")),
                    description: "remote target test object".to_string(),
                    namespace_id: 0,
                    hubuum_class_id: 0,
                    data: serde_json::json!({"hostname": "host-01"}),
                }],
            )
            .await
            .unwrap();

        (
            fixture.namespace_id(),
            fixture.class_id(),
            fixture.objects[0].id,
        )
    }

    fn target_payload(namespace_id: i32, name: &str, url_template: &str) -> serde_json::Value {
        serde_json::json!({
            "namespace_id": namespace_id,
            "name": name,
            "description": "test target",
            "method": "post",
            "url_template": url_template,
        })
    }

    async fn create_target(
        context: &TestContext,
        namespace_id: i32,
        name: &str,
        url_template: &str,
    ) -> RemoteTarget {
        let resp = post_request(
            &context.pool,
            &context.admin_token,
            RT_ENDPOINT,
            target_payload(namespace_id, name, url_template),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::CREATED).await;
        test::read_body_json(resp).await
    }

    async fn wait_for_task(
        context: &TestContext,
        task_id: i32,
        expected: TaskStatus,
    ) -> TaskResponse {
        for _ in 0..50 {
            let resp = get_request(
                &context.pool,
                &context.admin_token,
                &format!("/api/v1/tasks/{task_id}"),
            )
            .await;
            let resp = assert_response_status(resp, StatusCode::OK).await;
            let task: TaskResponse = test::read_body_json(resp).await;
            if task.status == expected {
                return task;
            }
            if matches!(
                task.status,
                TaskStatus::Succeeded | TaskStatus::Failed | TaskStatus::Cancelled
            ) {
                panic!(
                    "task {task_id} reached {:?} (summary {:?}), expected {:?}",
                    task.status, task.summary, expected
                );
            }
            sleep(Duration::from_millis(100)).await;
        }
        panic!("task {task_id} did not reach {expected:?} in time");
    }

    #[actix_web::test]
    async fn crud_lifecycle_as_admin() {
        let context = TestContext::new().await;
        let (namespace_id, _class_id, _object_id) = setup_object(&context, "rt_crud").await;

        // Create
        let create = serde_json::json!({
            "namespace_id": namespace_id,
            "name": "crud-target",
            "description": "created",
            "method": "post",
            "url_template": "https://service.example.com/hook/{{ object.id }}",
            "body_template": "{\"id\": {{ object.id }}}",
        });
        let resp = post_request(&context.pool, &context.admin_token, RT_ENDPOINT, create).await;
        let resp = assert_response_status(resp, StatusCode::CREATED).await;
        let location = header_value(&resp, "Location").expect("Location header");
        let created: RemoteTarget = test::read_body_json(resp).await;
        assert_eq!(created.namespace_id, namespace_id);
        assert_eq!(location, format!("{RT_ENDPOINT}/{}", created.id));
        assert_eq!(
            created.body_template.as_deref(),
            Some("{\"id\": {{ object.id }}}")
        );

        // Read
        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &format!("{RT_ENDPOINT}/{}", created.id),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let fetched: RemoteTarget = test::read_body_json(resp).await;
        assert_eq!(fetched.id, created.id);

        // List
        let resp = get_request(&context.pool, &context.admin_token, RT_ENDPOINT).await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let listed: Vec<RemoteTarget> = test::read_body_json(resp).await;
        assert!(listed.iter().any(|target| target.id == created.id));

        // Patch: clear the body template with an explicit null, change description.
        let resp = patch_request(
            &context.pool,
            &context.admin_token,
            &format!("{RT_ENDPOINT}/{}", created.id),
            serde_json::json!({ "description": "updated", "body_template": null }),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let updated: RemoteTarget = test::read_body_json(resp).await;
        assert_eq!(updated.description, "updated");
        assert_eq!(updated.body_template, None);

        // Delete then confirm gone.
        let resp = delete_request(
            &context.pool,
            &context.admin_token,
            &format!("{RT_ENDPOINT}/{}", created.id),
        )
        .await;
        assert_response_status(resp, StatusCode::NO_CONTENT).await;

        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &format!("{RT_ENDPOINT}/{}", created.id),
        )
        .await;
        assert_response_status(resp, StatusCode::NOT_FOUND).await;
    }

    #[actix_web::test]
    async fn create_requires_create_permission() {
        let context = TestContext::new().await;
        let (namespace_id, _, _) = setup_object(&context, "rt_perm").await;

        // Normal user with no permission is forbidden.
        let resp = post_request(
            &context.pool,
            &context.normal_token,
            RT_ENDPOINT,
            target_payload(
                namespace_id,
                "perm-target",
                "https://service.example.com/hook",
            ),
        )
        .await;
        assert_response_status(resp, StatusCode::FORBIDDEN).await;

        // After granting CreateRemoteTarget on the object's namespace, the request succeeds.
        let group = create_test_group(&context.pool).await;
        group
            .add_member(&context.pool, &context.normal_user)
            .await
            .unwrap();
        let namespace = crate::models::NamespaceID::new(namespace_id)
            .unwrap()
            .instance(&context.pool)
            .await
            .unwrap();
        namespace
            .grant(
                &context.pool,
                group.id,
                PermissionsList::new([Permissions::CreateRemoteTarget]),
            )
            .await
            .unwrap();

        let resp = post_request(
            &context.pool,
            &context.normal_token,
            RT_ENDPOINT,
            target_payload(
                namespace_id,
                "perm-target",
                "https://service.example.com/hook",
            ),
        )
        .await;
        assert_response_status(resp, StatusCode::CREATED).await;
    }

    #[actix_web::test]
    async fn create_rejects_invalid_template_and_secret() {
        let context = TestContext::new().await;
        let (namespace_id, _, _) = setup_object(&context, "rt_invalid").await;

        // Broken minijinja template.
        let resp = post_request(
            &context.pool,
            &context.admin_token,
            RT_ENDPOINT,
            target_payload(namespace_id, "bad-template", "https://x.example.com/{{"),
        )
        .await;
        assert_response_status(resp, StatusCode::BAD_REQUEST).await;

        // Auth secret reference with an illegal character.
        let payload = serde_json::json!({
            "namespace_id": namespace_id,
            "name": "bad-secret",
            "description": "test",
            "method": "get",
            "url_template": "https://x.example.com/hook",
            "auth_config": { "type": "bearer_secret", "secret": "not-valid" },
        });
        let resp = post_request(&context.pool, &context.admin_token, RT_ENDPOINT, payload).await;
        assert_response_status(resp, StatusCode::BAD_REQUEST).await;
    }

    #[actix_web::test]
    async fn move_requires_create_on_target_namespace() {
        let context = TestContext::new().await;
        let (source_ns, _, _) = setup_object(&context, "rt_move_src").await;
        let target_namespace = context.namespace_fixture("rt_move_dst").await;
        let target_ns = target_namespace.namespace.id;

        let created = create_target(
            &context,
            source_ns,
            "move-target",
            "https://x.example.com/h",
        )
        .await;

        // A user with UpdateRemoteTarget on the source but no CreateRemoteTarget on the target
        // cannot move the target.
        let group = create_test_group(&context.pool).await;
        let user = create_test_user(&context.pool).await;
        group.add_member(&context.pool, &user).await.unwrap();
        let user_token = user.create_token(&context.pool).await.unwrap().get_token();

        let source_namespace = crate::models::NamespaceID::new(source_ns)
            .unwrap()
            .instance(&context.pool)
            .await
            .unwrap();
        source_namespace
            .grant(
                &context.pool,
                group.id,
                PermissionsList::new([Permissions::UpdateRemoteTarget]),
            )
            .await
            .unwrap();

        let move_payload = serde_json::json!({ "namespace_id": target_ns });
        let resp = patch_request(
            &context.pool,
            &user_token,
            &format!("{RT_ENDPOINT}/{}", created.id),
            &move_payload,
        )
        .await;
        assert_response_status(resp, StatusCode::FORBIDDEN).await;

        // Granting CreateRemoteTarget on the target namespace unblocks the move.
        target_namespace
            .namespace
            .grant(
                &context.pool,
                group.id,
                PermissionsList::new([Permissions::CreateRemoteTarget]),
            )
            .await
            .unwrap();
        let resp = patch_request(
            &context.pool,
            &user_token,
            &format!("{RT_ENDPOINT}/{}", created.id),
            &move_payload,
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let moved: RemoteTarget = test::read_body_json(resp).await;
        assert_eq!(moved.namespace_id, target_ns);
    }

    #[actix_web::test]
    async fn invoke_creates_task_and_ssrf_guard_fails_private_target() {
        let context = TestContext::new().await;
        let (namespace_id, class_id, object_id) = setup_object(&context, "rt_invoke").await;
        // The rendered URL points at loopback, which the SSRF guard must refuse.
        let target = create_target(
            &context,
            namespace_id,
            "invoke-target",
            "https://127.0.0.1/hook",
        )
        .await;

        let resp = post_request(
            &context.pool,
            &context.admin_token,
            &format!(
                "/api/v1/classes/{class_id}/objects/{object_id}/remote-targets/{}/invoke",
                target.id
            ),
            serde_json::json!({}),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::ACCEPTED).await;
        let location = header_value(&resp, "Location").expect("Location header");
        let task: TaskResponse = test::read_body_json(resp).await;
        assert_eq!(location, format!("/api/v1/tasks/{}", task.id));

        // The worker screens the loopback address and finalizes the task as failed.
        let finished = wait_for_task(&context, task.id, TaskStatus::Failed).await;
        assert_eq!(finished.status, TaskStatus::Failed);
    }

    #[actix_web::test]
    async fn invoke_disabled_target_returns_400() {
        let context = TestContext::new().await;
        let (namespace_id, class_id, object_id) = setup_object(&context, "rt_disabled").await;
        let payload = serde_json::json!({
            "namespace_id": namespace_id,
            "name": "disabled-target",
            "description": "test",
            "method": "post",
            "url_template": "https://service.example.com/hook",
            "enabled": false,
        });
        let resp = post_request(&context.pool, &context.admin_token, RT_ENDPOINT, payload).await;
        let resp = assert_response_status(resp, StatusCode::CREATED).await;
        let target: RemoteTarget = test::read_body_json(resp).await;

        let resp = post_request(
            &context.pool,
            &context.admin_token,
            &format!(
                "/api/v1/classes/{class_id}/objects/{object_id}/remote-targets/{}/invoke",
                target.id
            ),
            serde_json::json!({}),
        )
        .await;
        assert_response_status(resp, StatusCode::BAD_REQUEST).await;
    }

    #[actix_web::test]
    async fn invoke_class_mismatch_returns_404() {
        let context = TestContext::new().await;
        let (namespace_id, _class_id, object_id) = setup_object(&context, "rt_mismatch").await;
        let target = create_target(
            &context,
            namespace_id,
            "mismatch-target",
            "https://x.example.com/h",
        )
        .await;

        // Use a class id that does not own the object.
        let resp = post_request(
            &context.pool,
            &context.admin_token,
            &format!(
                "/api/v1/classes/{}/objects/{object_id}/remote-targets/{}/invoke",
                999_999, target.id
            ),
            serde_json::json!({}),
        )
        .await;
        assert_response_status(resp, StatusCode::NOT_FOUND).await;
    }

    #[actix_web::test]
    async fn invoke_requires_execute_permission() {
        let context = TestContext::new().await;
        let (namespace_id, class_id, object_id) = setup_object(&context, "rt_exec").await;
        let target = create_target(
            &context,
            namespace_id,
            "exec-target",
            "https://x.example.com/h",
        )
        .await;

        // Grant only ReadObject (not ExecuteRemoteTarget) to a normal user's group.
        let group = create_test_group(&context.pool).await;
        group
            .add_member(&context.pool, &context.normal_user)
            .await
            .unwrap();
        let namespace = crate::models::NamespaceID::new(namespace_id)
            .unwrap()
            .instance(&context.pool)
            .await
            .unwrap();
        namespace
            .grant(
                &context.pool,
                group.id,
                PermissionsList::new([Permissions::ReadObject]),
            )
            .await
            .unwrap();

        let resp = post_request(
            &context.pool,
            &context.normal_token,
            &format!(
                "/api/v1/classes/{class_id}/objects/{object_id}/remote-targets/{}/invoke",
                target.id
            ),
            serde_json::json!({}),
        )
        .await;
        assert_response_status(resp, StatusCode::FORBIDDEN).await;
    }

    #[actix_web::test]
    async fn invoke_idempotency_key_conflict() {
        let context = TestContext::new().await;
        let (namespace_id, class_id, object_id) = setup_object(&context, "rt_idem").await;
        let target = create_target(
            &context,
            namespace_id,
            "idem-target",
            "https://127.0.0.1/hook",
        )
        .await;
        let endpoint = format!(
            "/api/v1/classes/{class_id}/objects/{object_id}/remote-targets/{}/invoke",
            target.id
        );
        let key = vec![(
            header::HeaderName::from_static("idempotency-key"),
            "remote-key-1".to_string(),
        )];

        // First submission creates a task.
        let resp = post_request_with_headers(
            &context.pool,
            &context.admin_token,
            &endpoint,
            serde_json::json!({ "parameters": { "a": 1 } }),
            key.clone(),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::ACCEPTED).await;
        let first: TaskResponse = test::read_body_json(resp).await;

        // Same key + same body returns the same task.
        let resp = post_request_with_headers(
            &context.pool,
            &context.admin_token,
            &endpoint,
            serde_json::json!({ "parameters": { "a": 1 } }),
            key.clone(),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::ACCEPTED).await;
        let again: TaskResponse = test::read_body_json(resp).await;
        assert_eq!(first.id, again.id);

        // Same key + different body is a conflict.
        let resp = post_request_with_headers(
            &context.pool,
            &context.admin_token,
            &endpoint,
            serde_json::json!({ "parameters": { "a": 2 } }),
            key,
        )
        .await;
        assert_response_status(resp, StatusCode::CONFLICT).await;
    }
}
