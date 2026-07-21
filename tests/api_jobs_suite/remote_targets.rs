#[cfg(test)]
mod tests {
    use crate::db::prelude::*;
    use actix_rt::time::sleep;
    use actix_web::{
        http::{StatusCode, header},
        test,
    };
    use base64::Engine;
    use futures::join;
    use rstest::rstest;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tokio::sync::oneshot;
    use tokio_rustls::TlsAcceptor;
    use tokio_rustls::rustls::{
        ServerConfig,
        pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer},
    };

    use crate::db::with_connection;
    use crate::models::{
        HubuumClassRelation, HubuumObjectRelation, NewHubuumClass, NewHubuumClassRelation,
        NewHubuumObject, NewHubuumObjectRelation, Permissions, PermissionsList, RemoteCallResult,
        RemoteTarget, TaskResponse, TaskStatus,
    };
    use crate::tests::TestContext;
    use crate::tests::api_operations::{
        delete_request, get_request, patch_request, post_request, post_request_with_headers,
    };
    use crate::tests::asserts::{assert_response_status, header_value};
    use crate::tests::{create_test_group, create_test_user};
    use crate::traits::{CanSave, PermissionController, SelfAccessors};

    const RT_ENDPOINT: &str = "/api/v1/remote-targets";
    const LOCALHOST_CERT_DER_B64: &str = "MIIDHzCCAgegAwIBAgIUT7YypqM2YgvdrXLHby8OFyeNEEIwDQYJKoZIhvcNAQELBQAwFDESMBAGA1UEAwwJbG9jYWxob3N0MB4XDTI2MDYyMzA0MDEyMloXDTI2MDYyNDA0MDEyMlowFDESMBAGA1UEAwwJbG9jYWxob3N0MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAn3A378veyRzeP7MSS/S61EPpE+v9Z+fGlFC4qB8SOUHvO1D6+QZrqcKkUJZb/HKnQyDydMNMBJfjswid5l18ogPVFmfGInGp50T3ceH8i1DAnN1Bj6g6h/QgKe64elkYDukaoHkqLGiQ7Nwsllm8UqwdgFa+B1hYD6uoYAcd/4gv5ClxOx6bkwganvWas+PXyHEEdYW7YBRAyPrJHIInWjck5k5UJPn5Vy551ptGpurvUqf2M7VcmnxjHAldTnc9br+chIvLtyulWg8pBAdFwu+4ZM0jWQpTRhVi5lWB+q7mmI8Da4izV0/K2a1bDnSN6j4rmAzEknok0fMoGXzWjQIDAQABo2kwZzAdBgNVHQ4EFgQUDp9XEjhqPBb8Ef0vyJXXDqLjcDwwHwYDVR0jBBgwFoAUDp9XEjhqPBb8Ef0vyJXXDqLjcDwwDwYDVR0TAQH/BAUwAwEB/zAUBgNVHREEDTALgglsb2NhbGhvc3QwDQYJKoZIhvcNAQELBQADggEBAJFxe1GtT9g/PI0Ht912WKwCJc8Oj0U49zUK8TRe9VZHMaJriozeS+4P6I6RhmMR4RV2bPtvjQjzv9ZCHoGoiPUupHd+PUGn8oyezDWoGLuwlPE0dQyn3OAdV1no6q/HI6PFThHTd2o/cLl3nfyIu56sCRLiwrMg6xH3UZ6VJ4qjtxTuyYloMNrb09Uyo7G1Qpw7qfiOB8whyJcjC8Gx1H1JTmF/h/CU2u79yAcVIRA4N6zJLAdtsseUjyTb5CAagmvZ6wZBqB+XNCwXzV09+56zt5fFtopF7mBgQcE21wtlzoKKLUyivc5FzgOHPv3YDJiooYyFXcOOobY1B0k8ih8=";
    const LOCALHOST_KEY_DER_B64: &str = "MIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQCfcDfvy97JHN4/sxJL9LrUQ+kT6/1n58aUULioHxI5Qe87UPr5BmupwqRQllv8cqdDIPJ0w0wEl+OzCJ3mXXyiA9UWZ8YicannRPdx4fyLUMCc3UGPqDqH9CAp7rh6WRgO6RqgeSosaJDs3CyWWbxSrB2AVr4HWFgPq6hgBx3/iC/kKXE7HpuTCBqe9Zqz49fIcQR1hbtgFEDI+skcgidaNyTmTlQk+flXLnnWm0am6u9Sp/YztVyafGMcCV1Odz1uv5yEi8u3K6VaDykEB0XC77hkzSNZClNGFWLmVYH6ruaYjwNriLNXT8rZrVsOdI3qPiuYDMSSeiTR8ygZfNaNAgMBAAECggEAAQH66ebA1Y9whamibqggtQiyrd6HAohCnR1CEhpOWCcaXPbuAtJNkUapRSf72gAAND4v3j2ikL1S+P9Yxhc7lBclbMoV+3uxk5+qFYVxzNlzsz1RoLUMs0IkCtEt6L/UyIaLDjLGUCavrIAKuxNKlM0/EOOgCcyljFuUUAIKIwOcOKv7rG/t7GC+wZMTT3oyICgihwsN7D527BTKRlk6zcSCj38B21drfgLAMreGRt8NGcByhzo3BuazRkYyEw8SP9LCEbDQKwWGR2xJtxwnSHcrvYvSklhDAB3EP29URstGUxapRg4re25e3MRVIjVdYtCeGt8Ie71UZgO/lgwYAQKBgQDPL192FKjTUwqfhjICpXYiNbbseXw7dvvNfLOZvuE20zPTkwwEWkpF2dxQX44RfYS625jzj9GHRijKwL6HlV89i+pNw+N2OWLUdWkkeMVqqknSPgJavZ4O3WKpk+cSgVm0VgaxNfvwoNi+TnLQblP6YFoXMG/luY3wYg0CviHzAQKBgQDFAPGIU/G6SYAnD5SJcojUXKzH3ivvciBYuLJt4FGUlfym9fnkQNbGNJAL4c3otPTcR/r0br2JIrxod5/w4c93Q4EKmXEwMdW26npxDR8uO/caSvFGZweikqxIj0Im5UlGV3cuanFb+u0jZWjCjFxMO2sWGRMdwrgQm+GyG7z/jQKBgA+vxIiKM+YcKXe+j1bH9FPOwVTSNefCsHn0cRy46RBfmVLxlT1XILx9LEMhmP4WBNCpA8GdJ/4X/8qqIULeumFMkKbmp/gxjBwN77IFOt1Cm2hBraf1J1x0wp2YRyyNgp82zDbqoXKsmvx9sA+76rvQQ8Hxtucrz2Vd5yJIBwYBAoGAaLd7q8+TKkZvjFPHzNfIy7kHTqZWDE1JzF9A2Q7nzmd7iPQvBJlCkNDX0LkSTqQBlCXey5chwIdqRs1vgwdE1ExZh1zQwaF7zGMO+pDTBixxyNQVNCsH7+6vDVK5AxvVu0I6471IzG+xJaN98AvT8+GRpollk+gxFwMFETuVVvECgYAJ8qBnL/YnusNmORCdItqG6adl+0H4ohikxNurIP8cBRjKGJ6XSC2Qs3BmljiqL9aLluKTcbhOBKlH6iq63vA8KxF7JjVBj2NXClDh6MO6hr/4gWTi7VMpC3CWT80IijoMAth37y+MImdaJhG2kut+XcT14KFakVJM1JCbe0Ygdw==";

    /// Create a collection + class + object owned by the admin group so the admin token holds
    /// every remote-target permission for it. Returns (collection_id, class_id, object_id).
    async fn setup_object(context: &TestContext, label: &str) -> (i32, i32, i32) {
        let fixture = context
            .object_fixture(
                label,
                NewHubuumClass {
                    name: context.scoped_name(&format!("{label}-class")),
                    description: "remote target test class".to_string(),
                    collection_id: 0,
                    json_schema: None,
                    validate_schema: Some(false),
                },
                vec![NewHubuumObject {
                    name: context.scoped_name(&format!("{label}-object")),
                    description: "remote target test object".to_string(),
                    collection_id: 0,
                    hubuum_class_id: 0,
                    data: serde_json::json!({"hostname": "host-01"}),
                }],
            )
            .await
            .unwrap();

        (
            fixture.collection_id(),
            fixture.class_id(),
            fixture.objects[0].id,
        )
    }

    async fn create_object_in_collection(
        context: &TestContext,
        collection_id: i32,
        label: &str,
    ) -> (i32, i32) {
        let class = NewHubuumClass {
            name: context.scoped_name(&format!("{label}-class")),
            description: "remote target alternate class".to_string(),
            collection_id,
            json_schema: None,
            validate_schema: Some(false),
        }
        .save_without_events(&context.pool)
        .await
        .unwrap();
        let object = NewHubuumObject {
            name: context.scoped_name(&format!("{label}-object")),
            description: "remote target alternate object".to_string(),
            collection_id,
            hubuum_class_id: class.id,
            data: serde_json::json!({"hostname": "other-host"}),
        }
        .save_without_events(&context.pool)
        .await
        .unwrap();

        (class.id, object.id)
    }

    fn target_payload(
        collection_id: i32,
        class_id: i32,
        name: &str,
        url_template: &str,
    ) -> serde_json::Value {
        target_payload_with_subjects(
            collection_id,
            Some(class_id),
            name,
            url_template,
            serde_json::json!(["object"]),
        )
    }

    fn target_payload_with_subjects(
        collection_id: i32,
        class_id: Option<i32>,
        name: &str,
        url_template: &str,
        allowed_subject_types: serde_json::Value,
    ) -> serde_json::Value {
        serde_json::json!({
            "collection_id": collection_id,
            "class_id": class_id,
            "name": name,
            "description": "test target",
            "method": "post",
            "url_template": url_template,
            "allowed_subject_types": allowed_subject_types,
        })
    }

    fn object_invoke_body(class_id: i32, object_id: i32) -> serde_json::Value {
        serde_json::json!({
            "subject": {
                "type": "object",
                "class_id": class_id,
                "object_id": object_id,
            }
        })
    }

    fn object_invoke_body_with_payload(
        class_id: i32,
        object_id: i32,
        parameters: serde_json::Value,
        body_override: serde_json::Value,
    ) -> serde_json::Value {
        serde_json::json!({
            "subject": {
                "type": "object",
                "class_id": class_id,
                "object_id": object_id,
            },
            "parameters": parameters,
            "body_override": body_override,
        })
    }

    fn invoke_endpoint(target_id: i32) -> String {
        format!("{RT_ENDPOINT}/{target_id}/invoke")
    }

    fn collection_invoke_body(collection_id: i32) -> serde_json::Value {
        serde_json::json!({
            "subject": {
                "type": "collection",
                "collection_id": collection_id,
            }
        })
    }

    fn class_invoke_body(class_id: i32) -> serde_json::Value {
        serde_json::json!({
            "subject": {
                "type": "class",
                "class_id": class_id,
            }
        })
    }

    fn object_relation_invoke_body(relation_id: i32) -> serde_json::Value {
        serde_json::json!({
            "subject": {
                "type": "object_relation",
                "relation_id": relation_id,
            }
        })
    }

    async fn setup_cross_collection_object_relation(
        context: &TestContext,
        label: &str,
    ) -> (i32, i32, i32, HubuumClassRelation, HubuumObjectRelation) {
        let (from_collection_id, from_class_id, from_object_id) =
            setup_object(context, &format!("{label}_from")).await;
        let (to_collection_id, to_class_id, to_object_id) =
            setup_object(context, &format!("{label}_to")).await;
        let class_relation = NewHubuumClassRelation {
            from_hubuum_class_id: from_class_id,
            to_hubuum_class_id: to_class_id,
            forward_template_alias: None,
            reverse_template_alias: None,
        }
        .save_without_events(&context.pool)
        .await
        .unwrap();
        let object_relation = NewHubuumObjectRelation {
            from_hubuum_object_id: from_object_id,
            to_hubuum_object_id: to_object_id,
            class_relation_id: class_relation.id,
        }
        .save_without_events(&context.pool)
        .await
        .unwrap();

        (
            from_collection_id,
            to_collection_id,
            from_class_id,
            class_relation,
            object_relation,
        )
    }

    async fn create_target(
        context: &TestContext,
        collection_id: i32,
        class_id: i32,
        name: &str,
        url_template: &str,
    ) -> RemoteTarget {
        let resp = post_request(
            &context.pool,
            &context.admin_token,
            RT_ENDPOINT,
            target_payload(collection_id, class_id, name, url_template),
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

    async fn spawn_https_remote_server() -> (u16, oneshot::Receiver<String>) {
        spawn_https_remote_server_with_body(b"remote-ok-body".to_vec()).await
    }

    async fn spawn_https_remote_server_with_body(
        body: Vec<u8>,
    ) -> (u16, oneshot::Receiver<String>) {
        let _ = tokio_rustls::rustls::crypto::aws_lc_rs::default_provider().install_default();
        let cert_der = base64::engine::general_purpose::STANDARD
            .decode(LOCALHOST_CERT_DER_B64)
            .unwrap();
        let key_der = base64::engine::general_purpose::STANDARD
            .decode(LOCALHOST_KEY_DER_B64)
            .unwrap();
        let config = ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(
                vec![CertificateDer::from(cert_der)],
                PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(key_der)),
            )
            .unwrap();
        let acceptor = TlsAcceptor::from(Arc::new(config));
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let (request_tx, request_rx) = oneshot::channel();

        actix_rt::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut stream = acceptor.accept(stream).await.unwrap();
            let mut request = Vec::new();
            let header_end;
            loop {
                let mut chunk = [0_u8; 1024];
                let read = stream.read(&mut chunk).await.unwrap();
                assert!(read > 0, "client closed before sending request headers");
                request.extend_from_slice(&chunk[..read]);
                if let Some(index) = request.windows(4).position(|window| window == b"\r\n\r\n") {
                    header_end = index + 4;
                    break;
                }
            }

            let headers = String::from_utf8_lossy(&request[..header_end]).into_owned();
            let content_length = headers
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().unwrap())
                })
                .unwrap_or(0);
            while request.len() < header_end + content_length {
                let mut chunk = [0_u8; 1024];
                let read = stream.read(&mut chunk).await.unwrap();
                assert!(read > 0, "client closed before sending request body");
                request.extend_from_slice(&chunk[..read]);
            }

            let request_text = String::from_utf8_lossy(&request).into_owned();
            request_tx.send(request_text).unwrap();
            let response = format!(
                "HTTP/1.1 201 Created\r\nContent-Type: text/plain\r\nX-Remote-Result: accepted\r\nSet-Cookie: session=secret\r\nContent-Length: {}\r\n\r\n",
                body.len()
            );
            stream.write_all(response.as_bytes()).await.unwrap();
            stream.write_all(&body).await.unwrap();
            stream.shutdown().await.unwrap();
        });

        (port, request_rx)
    }

    async fn remote_call_result(context: &TestContext, task_id_value: i32) -> RemoteCallResult {
        use crate::schema::remote_call_results::dsl::{remote_call_results, task_id};

        with_connection(&context.pool, async |conn| {
            remote_call_results
                .filter(task_id.eq(task_id_value))
                .first::<RemoteCallResult>(conn)
                .await
        })
        .await
        .unwrap()
    }

    #[actix_web::test]
    async fn crud_lifecycle_as_admin() {
        let context = TestContext::new().await;
        let (collection_id, class_id, _object_id) = setup_object(&context, "rt_crud").await;

        // Create
        let create = serde_json::json!({
            "collection_id": collection_id,
            "class_id": class_id,
            "name": "crud-target",
            "description": "created",
            "method": "post",
            "url_template": "https://service.example.com/hook/{{ object.id }}",
            "body_template": "{\"id\": {{ object.id }}}",
            "allowed_subject_types": ["object"],
        });
        let resp = post_request(&context.pool, &context.admin_token, RT_ENDPOINT, create).await;
        let resp = assert_response_status(resp, StatusCode::CREATED).await;
        let location = header_value(&resp, "Location").expect("Location header");
        let created: RemoteTarget = test::read_body_json(resp).await;
        assert_eq!(created.collection_id, collection_id);
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
        let (collection_id, class_id, _) = setup_object(&context, "rt_perm").await;

        // Normal user with no permission is forbidden.
        let resp = post_request(
            &context.pool,
            &context.normal_token,
            RT_ENDPOINT,
            target_payload(
                collection_id,
                class_id,
                "perm-target",
                "https://service.example.com/hook",
            ),
        )
        .await;
        assert_response_status(resp, StatusCode::FORBIDDEN).await;

        // After granting CreateRemoteTarget on the object's collection, the request succeeds.
        let group = create_test_group(&context.pool).await;
        group
            .add_member_without_events(&context.pool, &context.normal_user)
            .await
            .unwrap();
        let collection = crate::models::CollectionID::new(collection_id)
            .unwrap()
            .instance(&context.pool)
            .await
            .unwrap();
        collection
            .grant_without_events(
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
                collection_id,
                class_id,
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
        let (collection_id, class_id, _) = setup_object(&context, "rt_invalid").await;

        // Broken minijinja template.
        let resp = post_request(
            &context.pool,
            &context.admin_token,
            RT_ENDPOINT,
            target_payload(
                collection_id,
                class_id,
                "bad-template",
                "https://x.example.com/{{",
            ),
        )
        .await;
        assert_response_status(resp, StatusCode::BAD_REQUEST).await;

        // Auth secret reference with an illegal character.
        let payload = serde_json::json!({
            "collection_id": collection_id,
            "class_id": class_id,
            "name": "bad-secret",
            "description": "test",
            "method": "get",
            "url_template": "https://x.example.com/hook",
            "allowed_subject_types": ["object"],
            "auth_config": { "type": "bearer_secret", "secret": "not-valid" },
        });
        let resp = post_request(&context.pool, &context.admin_token, RT_ENDPOINT, payload).await;
        assert_response_status(resp, StatusCode::BAD_REQUEST).await;
    }

    #[actix_web::test]
    async fn create_rejects_empty_or_duplicate_allowed_subject_types() {
        let context = TestContext::new().await;
        let (collection_id, class_id, _) = setup_object(&context, "rt_subject_validation").await;

        let resp = post_request(
            &context.pool,
            &context.admin_token,
            RT_ENDPOINT,
            target_payload_with_subjects(
                collection_id,
                None,
                "empty-subjects",
                "https://x.example.com/hook",
                serde_json::json!([]),
            ),
        )
        .await;
        assert_response_status(resp, StatusCode::BAD_REQUEST).await;

        let resp = post_request(
            &context.pool,
            &context.admin_token,
            RT_ENDPOINT,
            target_payload_with_subjects(
                collection_id,
                Some(class_id),
                "duplicate-subjects",
                "https://x.example.com/hook",
                serde_json::json!(["object", "object"]),
            ),
        )
        .await;
        assert_response_status(resp, StatusCode::BAD_REQUEST).await;
    }

    #[actix_web::test]
    async fn create_object_target_requires_class_scope_in_target_collection() {
        let context = TestContext::new().await;
        let (collection_id, _class_id, _) = setup_object(&context, "rt_class_scope").await;
        let (other_collection_id, other_class_id, _) =
            setup_object(&context, "rt_class_scope_other").await;

        let payload_without_class = serde_json::json!({
            "collection_id": collection_id,
            "name": "object-without-class",
            "description": "test",
            "method": "post",
            "url_template": "https://x.example.com/hook",
            "allowed_subject_types": ["object"],
        });
        let resp = post_request(
            &context.pool,
            &context.admin_token,
            RT_ENDPOINT,
            payload_without_class,
        )
        .await;
        assert_response_status(resp, StatusCode::BAD_REQUEST).await;

        let payload_with_foreign_class = serde_json::json!({
            "collection_id": collection_id,
            "class_id": other_class_id,
            "name": "object-with-foreign-class",
            "description": "test",
            "method": "post",
            "url_template": "https://x.example.com/hook",
            "allowed_subject_types": ["object"],
        });
        assert_ne!(collection_id, other_collection_id);
        let resp = post_request(
            &context.pool,
            &context.admin_token,
            RT_ENDPOINT,
            payload_with_foreign_class,
        )
        .await;
        assert_response_status(resp, StatusCode::BAD_REQUEST).await;
    }

    #[actix_web::test]
    async fn move_requires_create_on_target_collection() {
        let context = TestContext::new().await;
        let (source_collection, class_id, _) = setup_object(&context, "rt_move_src").await;
        let target_collection = context.collection_fixture("rt_move_dst").await;
        let target_collection_id = target_collection.collection.id;

        let created = create_target(
            &context,
            source_collection,
            class_id,
            "move-target",
            "https://x.example.com/h",
        )
        .await;

        // A user with UpdateRemoteTarget on the source but no CreateRemoteTarget on the target
        // cannot move the target.
        let group = create_test_group(&context.pool).await;
        let user = create_test_user(&context.pool).await;
        group
            .add_member_without_events(&context.pool, &user)
            .await
            .unwrap();
        let user_token = user.create_token(&context.pool).await.unwrap().get_token();

        let source_collection = crate::models::CollectionID::new(source_collection)
            .unwrap()
            .instance(&context.pool)
            .await
            .unwrap();
        source_collection
            .grant_without_events(
                &context.pool,
                group.id,
                PermissionsList::new([Permissions::UpdateRemoteTarget]),
            )
            .await
            .unwrap();

        let move_payload = serde_json::json!({
            "collection_id": target_collection_id,
            "class_id": null,
            "allowed_subject_types": ["collection"],
        });
        let resp = patch_request(
            &context.pool,
            &user_token,
            &format!("{RT_ENDPOINT}/{}", created.id),
            &move_payload,
        )
        .await;
        assert_response_status(resp, StatusCode::FORBIDDEN).await;

        // Granting CreateRemoteTarget on the target collection unblocks the move.
        target_collection
            .collection
            .grant_without_events(
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
        assert_eq!(moved.collection_id, target_collection_id);
    }

    #[actix_web::test]
    async fn invoke_creates_task_and_ssrf_guard_fails_private_target() {
        let context = TestContext::new().await;
        let (collection_id, class_id, object_id) = setup_object(&context, "rt_invoke").await;
        // The rendered URL points at loopback, which the SSRF guard must refuse.
        let target = create_target(
            &context,
            collection_id,
            class_id,
            "invoke-target",
            "https://127.0.0.1/hook",
        )
        .await;

        let resp = post_request(
            &context.pool,
            &context.admin_token,
            &invoke_endpoint(target.id),
            object_invoke_body(class_id, object_id),
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
    async fn invoke_accepts_collection_and_class_subjects() {
        let context = TestContext::new().await;
        let (collection_id, class_id, _) = setup_object(&context, "rt_scope_subjects").await;
        let resp = post_request(
            &context.pool,
            &context.admin_token,
            RT_ENDPOINT,
            target_payload_with_subjects(
                collection_id,
                None,
                "scope-subject-target",
                "https://127.0.0.1/hook",
                serde_json::json!(["collection", "class"]),
            ),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::CREATED).await;
        let target: RemoteTarget = test::read_body_json(resp).await;

        let resp = post_request(
            &context.pool,
            &context.admin_token,
            &invoke_endpoint(target.id),
            collection_invoke_body(collection_id),
        )
        .await;
        assert_response_status(resp, StatusCode::ACCEPTED).await;

        let resp = post_request(
            &context.pool,
            &context.admin_token,
            &invoke_endpoint(target.id),
            class_invoke_body(class_id),
        )
        .await;
        assert_response_status(resp, StatusCode::ACCEPTED).await;
    }

    #[actix_web::test]
    async fn invoke_rejects_subject_type_not_allowed_by_target() {
        let context = TestContext::new().await;
        let (collection_id, class_id, object_id) =
            setup_object(&context, "rt_subject_allowed").await;
        let resp = post_request(
            &context.pool,
            &context.admin_token,
            RT_ENDPOINT,
            target_payload_with_subjects(
                collection_id,
                None,
                "class-only-target",
                "https://x.example.com/hook",
                serde_json::json!(["class"]),
            ),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::CREATED).await;
        let target: RemoteTarget = test::read_body_json(resp).await;

        let resp = post_request(
            &context.pool,
            &context.admin_token,
            &invoke_endpoint(target.id),
            object_invoke_body(class_id, object_id),
        )
        .await;
        assert_response_status(resp, StatusCode::BAD_REQUEST).await;
    }

    #[actix_web::test]
    async fn invoke_object_requires_target_class_scope() {
        let context = TestContext::new().await;
        let (collection_id, target_class_id, _target_object_id) =
            setup_object(&context, "rt_target_class").await;
        let (other_class_id, other_object_id) =
            create_object_in_collection(&context, collection_id, "rt_other_class").await;
        assert_ne!(target_class_id, other_class_id);

        let target = create_target(
            &context,
            collection_id,
            target_class_id,
            "class-scoped-object-target",
            "https://x.example.com/h",
        )
        .await;

        let resp = post_request(
            &context.pool,
            &context.admin_token,
            &invoke_endpoint(target.id),
            object_invoke_body(other_class_id, other_object_id),
        )
        .await;
        assert_response_status(resp, StatusCode::NOT_FOUND).await;
    }

    #[actix_web::test]
    async fn invoke_rejects_non_object_parameters_and_body_override() {
        let context = TestContext::new().await;
        let (collection_id, class_id, object_id) = setup_object(&context, "rt_newtypes").await;
        let target = create_target(
            &context,
            collection_id,
            class_id,
            "newtype-target",
            "https://x.example.com/h",
        )
        .await;

        let mut non_object_parameters = object_invoke_body(class_id, object_id);
        non_object_parameters["parameters"] = serde_json::json!("not-an-object");
        let resp = post_request(
            &context.pool,
            &context.admin_token,
            &invoke_endpoint(target.id),
            non_object_parameters,
        )
        .await;
        assert_response_status(resp, StatusCode::BAD_REQUEST).await;

        let mut non_object_body_override = object_invoke_body(class_id, object_id);
        non_object_body_override["body_override"] = serde_json::json!(["not", "object"]);
        let resp = post_request(
            &context.pool,
            &context.admin_token,
            &invoke_endpoint(target.id),
            non_object_body_override,
        )
        .await;
        assert_response_status(resp, StatusCode::BAD_REQUEST).await;
    }

    #[actix_web::test]
    async fn invoke_success_records_sanitized_https_result_and_sends_auth() {
        let _local_target = crate::test_support::allow_local_remote_target();
        unsafe {
            std::env::set_var(
                "HUBUUM_REMOTE_SECRET_REMOTE_SUCCESS_TOKEN",
                "expected-token",
            );
        }
        let context = TestContext::new().await;
        let (port, request_rx) = spawn_https_remote_server().await;
        let (collection_id, class_id, object_id) = setup_object(&context, "rt_success").await;
        let payload = serde_json::json!({
            "collection_id": collection_id,
            "class_id": class_id,
            "name": "success-target",
            "description": "test",
            "method": "post",
            "url_template": format!("https://localhost:{port}/hook/{{{{ object.data.hostname }}}}"),
            "headers_template": {
                "X-Object": "{{ object.name }}",
                "X-Trace": "{{ parameters.trace }}",
            },
            "body_template": "{\"host\": {{ object.data.hostname | tojson }}, \"trace\": {{ parameters.trace | tojson }}, \"override\": {{ body_override | tojson }}}",
            "auth_config": { "type": "bearer_secret", "secret": "remote_success_token" },
            "allowed_subject_types": ["object"],
        });
        let resp = post_request(&context.pool, &context.admin_token, RT_ENDPOINT, payload).await;
        let resp = assert_response_status(resp, StatusCode::CREATED).await;
        let target: RemoteTarget = test::read_body_json(resp).await;

        let resp = post_request(
            &context.pool,
            &context.admin_token,
            &invoke_endpoint(target.id),
            object_invoke_body_with_payload(
                class_id,
                object_id,
                serde_json::json!({ "trace": "trace-123" }),
                serde_json::json!({ "force": true }),
            ),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::ACCEPTED).await;
        let task: TaskResponse = test::read_body_json(resp).await;
        let finished = wait_for_task(&context, task.id, TaskStatus::Succeeded).await;
        assert_eq!(finished.status, TaskStatus::Succeeded);

        let request = request_rx.await.unwrap();
        assert!(request.starts_with("POST /hook/host-01 HTTP/1.1"));
        assert!(request.contains("authorization: Bearer expected-token"));
        assert!(request.contains("x-object: "));
        assert!(request.contains("x-trace: trace-123"));
        assert!(request.contains("\"host\": \"host-01\""));
        assert!(request.contains("\"trace\": \"trace-123\""));
        assert!(request.contains("\"force\":true"));

        let result = remote_call_result(&context, task.id).await;
        assert!(result.success);
        assert_eq!(result.target_id, Some(target.id));
        assert_eq!(result.subject_type, "object");
        assert_eq!(result.subject_id, object_id);
        assert_eq!(result.method, "post");
        assert_eq!(
            result.rendered_url,
            format!("https://localhost:{port}/hook/host-01")
        );
        assert_eq!(result.response_status, Some(201));
        assert_eq!(
            result.response_body_preview.as_deref(),
            Some("remote-ok-body")
        );
        let headers = result.response_headers.unwrap();
        assert_eq!(headers["x-remote-result"], "accepted");
        assert_eq!(headers["set-cookie"], "[redacted]");
        assert_eq!(result.error, None);
    }

    #[actix_web::test]
    async fn invoke_stores_sanitized_response_body_preview_with_nul() {
        let _local_target = crate::test_support::allow_local_remote_target();
        let context = TestContext::new().await;
        let (port, request_rx) =
            spawn_https_remote_server_with_body(b"before\0after".to_vec()).await;
        let (collection_id, class_id, object_id) = setup_object(&context, "rt_nul_preview").await;
        let target = create_target(
            &context,
            collection_id,
            class_id,
            "nul-preview-target",
            &format!("https://localhost:{port}/hook"),
        )
        .await;

        let resp = post_request(
            &context.pool,
            &context.admin_token,
            &invoke_endpoint(target.id),
            object_invoke_body(class_id, object_id),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::ACCEPTED).await;
        let task: TaskResponse = test::read_body_json(resp).await;
        let finished = wait_for_task(&context, task.id, TaskStatus::Succeeded).await;
        assert_eq!(finished.status, TaskStatus::Succeeded);

        let _request = request_rx.await.unwrap();
        let result = remote_call_result(&context, task.id).await;
        assert!(result.success);
        assert_eq!(
            result.response_body_preview.as_deref(),
            Some("before\u{FFFD}after")
        );
        assert!(
            !result
                .response_body_preview
                .as_deref()
                .unwrap_or_default()
                .contains('\0')
        );
    }

    #[actix_web::test]
    async fn invoke_disabled_target_returns_400() {
        let context = TestContext::new().await;
        let (collection_id, class_id, object_id) = setup_object(&context, "rt_disabled").await;
        let payload = serde_json::json!({
            "collection_id": collection_id,
            "class_id": class_id,
            "name": "disabled-target",
            "description": "test",
            "method": "post",
            "url_template": "https://service.example.com/hook",
            "allowed_subject_types": ["object"],
            "enabled": false,
        });
        let resp = post_request(&context.pool, &context.admin_token, RT_ENDPOINT, payload).await;
        let resp = assert_response_status(resp, StatusCode::CREATED).await;
        let target: RemoteTarget = test::read_body_json(resp).await;

        let resp = post_request(
            &context.pool,
            &context.admin_token,
            &invoke_endpoint(target.id),
            object_invoke_body(class_id, object_id),
        )
        .await;
        assert_response_status(resp, StatusCode::BAD_REQUEST).await;
    }

    #[actix_web::test]
    async fn invoke_checks_execute_before_disclosing_disabled_target() {
        let context = TestContext::new().await;
        let (collection_id, class_id, object_id) =
            setup_object(&context, "rt_disabled_forbidden").await;
        let payload = serde_json::json!({
            "collection_id": collection_id,
            "class_id": class_id,
            "name": "disabled-forbidden-target",
            "description": "test",
            "method": "post",
            "url_template": "https://service.example.com/hook",
            "allowed_subject_types": ["object"],
            "enabled": false,
        });
        let resp = post_request(&context.pool, &context.admin_token, RT_ENDPOINT, payload).await;
        let resp = assert_response_status(resp, StatusCode::CREATED).await;
        let target: RemoteTarget = test::read_body_json(resp).await;

        let resp = post_request(
            &context.pool,
            &context.normal_token,
            &invoke_endpoint(target.id),
            object_invoke_body(class_id, object_id),
        )
        .await;
        assert_response_status(resp, StatusCode::FORBIDDEN).await;
    }

    #[rstest]
    #[case::header_template(serde_json::json!({
        "headers_template": { "Host": "internal.example" }
    }))]
    #[case::api_key_auth(serde_json::json!({
        "auth_config": {
            "type": "api_key_secret",
            "header": "Content-Length",
            "secret": "inventory_api_key"
        }
    }))]
    #[actix_web::test]
    async fn create_rejects_transport_controlled_headers(
        #[case] override_fields: serde_json::Value,
    ) {
        let context = TestContext::new().await;
        let (collection_id, class_id, _) = setup_object(&context, "rt_transport_headers").await;
        let mut payload = target_payload(
            collection_id,
            class_id,
            "transport-header-target",
            "https://service.example.com/hook",
        );
        payload
            .as_object_mut()
            .unwrap()
            .extend(override_fields.as_object().unwrap().clone());

        let response =
            post_request(&context.pool, &context.admin_token, RT_ENDPOINT, payload).await;

        assert_response_status(response, StatusCode::BAD_REQUEST).await;
    }

    #[actix_web::test]
    async fn invoke_class_mismatch_returns_404() {
        let context = TestContext::new().await;
        let (collection_id, class_id, object_id) = setup_object(&context, "rt_mismatch").await;
        let target = create_target(
            &context,
            collection_id,
            class_id,
            "mismatch-target",
            "https://x.example.com/h",
        )
        .await;

        // Use a class id that does not own the object.
        let resp = post_request(
            &context.pool,
            &context.admin_token,
            &invoke_endpoint(target.id),
            object_invoke_body(999_999, object_id),
        )
        .await;
        assert_response_status(resp, StatusCode::NOT_FOUND).await;
    }

    #[actix_web::test]
    async fn invoke_requires_execute_permission() {
        let context = TestContext::new().await;
        let (collection_id, class_id, object_id) = setup_object(&context, "rt_exec").await;
        let target = create_target(
            &context,
            collection_id,
            class_id,
            "exec-target",
            "https://x.example.com/h",
        )
        .await;

        // Grant only ReadObject (not ExecuteRemoteTarget) to a normal user's group.
        let group = create_test_group(&context.pool).await;
        group
            .add_member_without_events(&context.pool, &context.normal_user)
            .await
            .unwrap();
        let collection = crate::models::CollectionID::new(collection_id)
            .unwrap()
            .instance(&context.pool)
            .await
            .unwrap();
        collection
            .grant_without_events(
                &context.pool,
                group.id,
                PermissionsList::new([Permissions::ReadObject]),
            )
            .await
            .unwrap();

        let resp = post_request(
            &context.pool,
            &context.normal_token,
            &invoke_endpoint(target.id),
            object_invoke_body(class_id, object_id),
        )
        .await;
        assert_response_status(resp, StatusCode::FORBIDDEN).await;
    }

    #[actix_web::test]
    async fn invoke_accepts_cross_collection_relations_when_target_is_anchored_on_subject_collection()
     {
        let context = TestContext::new().await;
        let (
            from_collection_id,
            _to_collection_id,
            _from_class_id,
            class_relation,
            object_relation,
        ) = setup_cross_collection_object_relation(&context, "rt_relation_accept").await;
        let resp = post_request(
            &context.pool,
            &context.admin_token,
            RT_ENDPOINT,
            target_payload_with_subjects(
                from_collection_id,
                None,
                "relation-target",
                "https://127.0.0.1/hook",
                serde_json::json!(["class_relation", "object_relation"]),
            ),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::CREATED).await;
        let target: RemoteTarget = test::read_body_json(resp).await;

        let resp = post_request(
            &context.pool,
            &context.admin_token,
            &invoke_endpoint(target.id),
            serde_json::json!({
                "subject": {
                    "type": "class_relation",
                    "relation_id": class_relation.id,
                }
            }),
        )
        .await;
        assert_response_status(resp, StatusCode::ACCEPTED).await;

        let resp = post_request(
            &context.pool,
            &context.admin_token,
            &invoke_endpoint(target.id),
            object_relation_invoke_body(object_relation.id),
        )
        .await;
        assert_response_status(resp, StatusCode::ACCEPTED).await;
    }

    #[actix_web::test]
    async fn invoke_relation_requires_read_on_both_collections() {
        let context = TestContext::new().await;
        let (
            from_collection_id,
            to_collection_id,
            _from_class_id,
            _class_relation,
            object_relation,
        ) = setup_cross_collection_object_relation(&context, "rt_relation_read").await;
        let resp = post_request(
            &context.pool,
            &context.admin_token,
            RT_ENDPOINT,
            target_payload_with_subjects(
                from_collection_id,
                None,
                "relation-read-target",
                "https://127.0.0.1/hook",
                serde_json::json!(["object_relation"]),
            ),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::CREATED).await;
        let target: RemoteTarget = test::read_body_json(resp).await;

        let group = create_test_group(&context.pool).await;
        group
            .add_member_without_events(&context.pool, &context.normal_user)
            .await
            .unwrap();
        let from_collection = crate::models::CollectionID::new(from_collection_id)
            .unwrap()
            .instance(&context.pool)
            .await
            .unwrap();
        from_collection
            .grant_without_events(
                &context.pool,
                group.id,
                PermissionsList::new([
                    Permissions::ReadObjectRelation,
                    Permissions::ExecuteRemoteTarget,
                ]),
            )
            .await
            .unwrap();

        let resp = post_request(
            &context.pool,
            &context.normal_token,
            &invoke_endpoint(target.id),
            object_relation_invoke_body(object_relation.id),
        )
        .await;
        assert_response_status(resp, StatusCode::FORBIDDEN).await;

        let to_collection = crate::models::CollectionID::new(to_collection_id)
            .unwrap()
            .instance(&context.pool)
            .await
            .unwrap();
        to_collection
            .grant_one(&context.pool, group.id, Permissions::ReadObjectRelation)
            .await
            .unwrap();

        let resp = post_request(
            &context.pool,
            &context.normal_token,
            &invoke_endpoint(target.id),
            object_relation_invoke_body(object_relation.id),
        )
        .await;
        assert_response_status(resp, StatusCode::ACCEPTED).await;
    }

    #[actix_web::test]
    async fn invoke_relation_returns_404_when_target_collection_is_not_part_of_subject() {
        let context = TestContext::new().await;
        let (
            _from_collection_id,
            _to_collection_id,
            _from_class_id,
            _class_relation,
            object_relation,
        ) = setup_cross_collection_object_relation(&context, "rt_relation_scope").await;
        let unrelated_collection = context.collection_fixture("rt_relation_unrelated").await;
        let resp = post_request(
            &context.pool,
            &context.admin_token,
            RT_ENDPOINT,
            target_payload_with_subjects(
                unrelated_collection.collection.id,
                None,
                "relation-unrelated-target",
                "https://x.example.com/hook",
                serde_json::json!(["object_relation"]),
            ),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::CREATED).await;
        let target: RemoteTarget = test::read_body_json(resp).await;

        let resp = post_request(
            &context.pool,
            &context.admin_token,
            &invoke_endpoint(target.id),
            object_relation_invoke_body(object_relation.id),
        )
        .await;
        assert_response_status(resp, StatusCode::NOT_FOUND).await;
    }

    #[actix_web::test]
    async fn invoke_idempotency_key_conflict() {
        let context = TestContext::new().await;
        let (collection_id, class_id, object_id) = setup_object(&context, "rt_idem").await;
        let target = create_target(
            &context,
            collection_id,
            class_id,
            "idem-target",
            "https://127.0.0.1/hook",
        )
        .await;
        let endpoint = invoke_endpoint(target.id);
        let key = vec![(
            header::HeaderName::from_static("idempotency-key"),
            "remote-key-1".to_string(),
        )];

        // First submission creates a task.
        let resp = post_request_with_headers(
            &context.pool,
            &context.admin_token,
            &endpoint,
            object_invoke_body_with_payload(
                class_id,
                object_id,
                serde_json::json!({ "a": 1 }),
                serde_json::json!({}),
            ),
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
            object_invoke_body_with_payload(
                class_id,
                object_id,
                serde_json::json!({ "a": 1 }),
                serde_json::json!({}),
            ),
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
            object_invoke_body_with_payload(
                class_id,
                object_id,
                serde_json::json!({ "a": 2 }),
                serde_json::json!({}),
            ),
            key,
        )
        .await;
        assert_response_status(resp, StatusCode::CONFLICT).await;
    }

    #[actix_web::test]
    async fn invoke_idempotency_key_reuses_task_under_concurrent_submissions() {
        let context = TestContext::new().await;
        let (collection_id, class_id, object_id) =
            setup_object(&context, "rt_idem_concurrent").await;
        let target = create_target(
            &context,
            collection_id,
            class_id,
            "idem-concurrent-target",
            "https://127.0.0.1/hook",
        )
        .await;
        let endpoint = invoke_endpoint(target.id);
        let key = context.scoped_name("remote-same-task-concurrent");
        let body = object_invoke_body_with_payload(
            class_id,
            object_id,
            serde_json::json!({ "a": 1 }),
            serde_json::json!({}),
        );

        let first = post_request_with_headers(
            &context.pool,
            &context.admin_token,
            &endpoint,
            body.clone(),
            vec![(
                header::HeaderName::from_static("idempotency-key"),
                key.clone(),
            )],
        );
        let second = post_request_with_headers(
            &context.pool,
            &context.admin_token,
            &endpoint,
            body,
            vec![(header::HeaderName::from_static("idempotency-key"), key)],
        );

        let (first, second) = join!(first, second);
        let first = assert_response_status(first, StatusCode::ACCEPTED).await;
        let second = assert_response_status(second, StatusCode::ACCEPTED).await;
        let first_task: TaskResponse = test::read_body_json(first).await;
        let second_task: TaskResponse = test::read_body_json(second).await;

        assert_eq!(first_task.id, second_task.id);
    }

    #[actix_web::test]
    async fn test_api_remote_target_history_list_and_as_of() {
        let context = TestContext::new().await;
        let (collection_id, class_id, _object_id) =
            setup_object(&context, "remote_target_history_api").await;

        // Create a remote target.
        let created = create_target(
            &context,
            collection_id,
            class_id,
            "remote_target_history_api",
            "https://example.com/v1",
        )
        .await;

        // Update it to create a second version.
        let update_payload = serde_json::json!({
            "description": "v2"
        });
        let resp = patch_request(
            &context.pool,
            &context.admin_token,
            &format!("{}/{}", RT_ENDPOINT, created.id),
            &update_payload,
        )
        .await;
        assert_response_status(resp, StatusCode::OK).await;

        // List history newest-first.
        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &format!("{}/{}/history", RT_ENDPOINT, created.id),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let body: Vec<serde_json::Value> = test::read_body_json(resp).await;
        assert_eq!(body.len(), 2, "expected two versions");
        assert_eq!(body[0]["op"], "U");
        assert_eq!(body[0]["description"], "v2");
        assert_eq!(body[1]["op"], "I");
        assert!(
            body[0].get("actor_username").is_some(),
            "actor_username key present"
        );

        // as-of just after the insert (before the update) -> v1.
        let v1_from = body[1]["valid_from"].as_str().unwrap().to_string();
        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &format!(
                "{}/{}/history/as-of?at={}",
                RT_ENDPOINT, created.id, v1_from
            ),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let snap: serde_json::Value = test::read_body_json(resp).await;
        assert_eq!(snap["description"], created.description);
    }

    #[actix_web::test]
    async fn test_api_remote_target_history_404_for_missing() {
        let context = TestContext::new().await;
        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &format!("{}/2147483647/history", RT_ENDPOINT),
        )
        .await;
        assert_response_status(resp, StatusCode::NOT_FOUND).await;
    }
}
