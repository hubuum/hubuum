#[cfg(test)]
mod tests {
    use actix_rt::time::sleep;
    use actix_web::{
        http::{StatusCode, header},
        test,
    };
    use rstest::rstest;
    use std::time::Duration;

    use crate::db::traits::task::{create_task_record, purge_expired_report_outputs};
    use crate::models::{
        HubuumClass, HubuumClassRelation, HubuumObjectRelation, NewHubuumClass,
        NewHubuumClassRelation, NewHubuumObject, NewHubuumObjectRelation, NewReportTemplate,
        NewTaskRecord, ReportContentType, ReportJsonResponse, ReportRequest, ReportScope,
        ReportScopeKind, TaskEventResponse, TaskKind, TaskResponse, TaskStatus,
        UpdateReportTemplate,
    };
    use crate::tests::api::v1::classes::tests::{cleanup, create_test_classes};
    use crate::tests::api_operations::{get_request, post_request_with_headers};
    use crate::tests::asserts::{assert_response_status, header_value};
    use crate::tests::{TestContext, create_test_user, test_context};
    use crate::traits::CanSave;
    const REPORTS_ENDPOINT: &str = "/api/v1/reports";

    async fn wait_for_task(
        context: &TestContext,
        task_id: i32,
        expected_terminal_statuses: &[TaskStatus],
    ) -> TaskResponse {
        let mut last_task = None;
        for _ in 0..50 {
            let resp = get_request(
                &context.pool,
                &context.admin_token,
                &format!("/api/v1/tasks/{task_id}"),
            )
            .await;
            let resp = assert_response_status(resp, StatusCode::OK).await;
            let task: TaskResponse = test::read_body_json(resp).await;
            if matches!(
                task.status,
                TaskStatus::Succeeded | TaskStatus::Failed | TaskStatus::Cancelled
            ) && !expected_terminal_statuses.contains(&task.status)
            {
                panic!(
                    "Task {task_id} reached terminal status {:?} with summary {:?}",
                    task.status, task.summary
                );
            }
            if expected_terminal_statuses.contains(&task.status) {
                return task;
            }
            last_task = Some(task);
            sleep(Duration::from_millis(100)).await;
        }

        panic!(
            "Task {task_id} did not reach a terminal status in time; last observed state: {:?}",
            last_task.as_ref().map(|task| (&task.status, &task.summary))
        );
    }

    async fn create_report_objects(
        pool: &crate::db::DbPool,
        class: &HubuumClass,
    ) -> Vec<crate::models::HubuumObject> {
        let objects = vec![
            NewHubuumObject {
                name: "report-app-01".to_string(),
                description: "App server".to_string(),
                namespace_id: class.namespace_id,
                hubuum_class_id: class.id,
                data: serde_json::json!({"hostname": "report-app-01", "owner": "alice"}),
            },
            NewHubuumObject {
                name: "report-db-01".to_string(),
                description: "Database server".to_string(),
                namespace_id: class.namespace_id,
                hubuum_class_id: class.id,
                data: serde_json::json!({"hostname": "report-db-01", "owner": "bob"}),
            },
        ];

        let mut created = Vec::new();
        for object in objects {
            created.push(object.save(pool).await.unwrap());
        }
        created
    }

    async fn create_class_relation(
        pool: &crate::db::DbPool,
        from_class_id: i32,
        to_class_id: i32,
    ) -> HubuumClassRelation {
        NewHubuumClassRelation {
            from_hubuum_class_id: from_class_id,
            to_hubuum_class_id: to_class_id,
            forward_template_alias: None,
            reverse_template_alias: None,
        }
        .save(pool)
        .await
        .unwrap()
    }

    async fn create_named_class(
        pool: &crate::db::DbPool,
        namespace_id: i32,
        name: &str,
    ) -> HubuumClass {
        NewHubuumClass {
            name: name.to_string(),
            description: format!("{name} description"),
            namespace_id,
            json_schema: None,
            validate_schema: Some(false),
        }
        .save(pool)
        .await
        .unwrap()
    }

    async fn create_object_relation(
        pool: &crate::db::DbPool,
        from_object_id: i32,
        to_object_id: i32,
        class_relation_id: i32,
    ) -> HubuumObjectRelation {
        NewHubuumObjectRelation {
            from_hubuum_object_id: from_object_id,
            to_hubuum_object_id: to_object_id,
            class_relation_id,
        }
        .save(pool)
        .await
        .unwrap()
    }

    async fn create_template(
        pool: &crate::db::DbPool,
        namespace_id: i32,
        name: &str,
        content_type: ReportContentType,
        template: &str,
    ) -> i32 {
        let template = crate::models::report_template::create_report_template(
            pool,
            NewReportTemplate {
                namespace_id,
                name: name.to_string(),
                description: "report template".to_string(),
                content_type,
                template: template.to_string(),
            },
        )
        .await
        .unwrap();

        template.id
    }

    #[rstest]
    #[actix_web::test]
    async fn test_report_submission_returns_task_and_json_output_is_refetchable(
        #[future(awt)] test_context: TestContext,
    ) {
        let context = test_context;
        let classes = create_test_classes(&context, "report_async_json").await;
        let class = classes[0].clone();
        let created_objects = create_report_objects(&context.pool, &class).await;

        let body = ReportRequest {
            scope: ReportScope {
                kind: ReportScopeKind::ObjectsInClass,
                class_id: Some(class.id),
                object_id: None,
            },
            query: Some("name__contains=report-&sort=name".to_string()),
            output: None,
            missing_data_policy: None,
            limits: None,
            relation_context: None,
        };

        let resp = post_request_with_headers(
            &context.pool,
            &context.admin_token,
            REPORTS_ENDPOINT,
            &body,
            vec![(
                header::HeaderName::from_static("idempotency-key"),
                context.scoped_name("report-json-idempotency"),
            )],
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::ACCEPTED).await;
        let location = header_value(&resp, "Location");
        let task: TaskResponse = test::read_body_json(resp).await;

        assert_eq!(location, Some(format!("/api/v1/tasks/{}", task.id)));
        assert_eq!(task.kind, TaskKind::Report);
        assert!(task.links.report.is_some());
        assert!(task.links.report_output.is_some());
        assert!(
            task.details
                .as_ref()
                .and_then(|details| details.report.as_ref())
                .is_some()
        );

        let completed = wait_for_task(&context, task.id, &[TaskStatus::Succeeded]).await;
        assert_eq!(completed.status, TaskStatus::Succeeded);
        assert!(completed.request_redacted_at.is_some());

        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &format!("/api/v1/reports/{}/output", task.id),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let report: ReportJsonResponse = test::read_body_json(resp).await;

        assert_eq!(report.meta.count, created_objects.len());
        assert_eq!(report.meta.scope.kind, ReportScopeKind::ObjectsInClass);
        assert_eq!(report.meta.content_type, ReportContentType::ApplicationJson);
        assert_eq!(report.items.len(), 2);
        assert_eq!(report.items[0]["name"], "report-app-01");
        assert_eq!(report.items[1]["name"], "report-db-01");

        cleanup(&classes).await;
    }

    #[rstest]
    #[actix_web::test]
    async fn test_report_output_stays_stable_after_template_and_data_changes(
        #[future(awt)] test_context: TestContext,
    ) {
        let context = test_context;
        let classes = create_test_classes(&context, "report_output_stable").await;
        let class = classes[0].clone();
        let _ = create_report_objects(&context.pool, &class).await;
        let template_id = create_template(
            &context.pool,
            class.namespace_id,
            "stable-template",
            ReportContentType::TextPlain,
            "{% for item in items %}{{ item.name }}={{ item.data.owner }}\n{% endfor %}",
        )
        .await;

        let body = serde_json::json!({
            "scope": {
                "kind": "objects_in_class",
                "class_id": class.id
            },
            "query": "name__contains=report-&sort=name",
            "output": {
                "template_id": template_id
            }
        });

        let resp = post_request_with_headers(
            &context.pool,
            &context.admin_token,
            REPORTS_ENDPOINT,
            &body,
            vec![],
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::ACCEPTED).await;
        let task: TaskResponse = test::read_body_json(resp).await;

        let _ = wait_for_task(&context, task.id, &[TaskStatus::Succeeded]).await;

        let first_output = get_request(
            &context.pool,
            &context.admin_token,
            &format!("/api/v1/reports/{}/output", task.id),
        )
        .await;
        let first_output = assert_response_status(first_output, StatusCode::OK).await;
        let first_body = String::from_utf8(test::read_body(first_output).await.to_vec()).unwrap();
        assert_eq!(first_body, "report-app-01=alice\nreport-db-01=bob\n");

        crate::models::report_template::update_report_template(
            &context.pool,
            template_id,
            UpdateReportTemplate {
                namespace_id: None,
                name: None,
                description: None,
                template: Some("changed output".to_string()),
            },
        )
        .await
        .unwrap();

        let _ = NewHubuumObject {
            name: "report-cache-01".to_string(),
            description: "new object".to_string(),
            namespace_id: class.namespace_id,
            hubuum_class_id: class.id,
            data: serde_json::json!({"hostname": "report-cache-01", "owner": "carol"}),
        }
        .save(&context.pool)
        .await
        .unwrap();

        let second_output = get_request(
            &context.pool,
            &context.admin_token,
            &format!("/api/v1/reports/{}/output", task.id),
        )
        .await;
        let second_output = assert_response_status(second_output, StatusCode::OK).await;
        let second_body = String::from_utf8(test::read_body(second_output).await.to_vec()).unwrap();

        assert_eq!(second_body, first_body);

        cleanup(&classes).await;
    }

    #[rstest]
    #[actix_web::test]
    async fn test_report_output_counts_template_missing_value_warnings(
        #[future(awt)] test_context: TestContext,
    ) {
        let context = test_context;
        let classes = create_test_classes(&context, "report_warning_count").await;
        let class = classes[0].clone();
        let _ = create_report_objects(&context.pool, &class).await;
        let template_id = create_template(
            &context.pool,
            class.namespace_id,
            "warning-template",
            ReportContentType::TextPlain,
            "{% for item in items %}{{ item.name }}={{ item.data.primary_contact }}\n{% endfor %}",
        )
        .await;

        let body = ReportRequest {
            scope: ReportScope {
                kind: ReportScopeKind::ObjectsInClass,
                class_id: Some(class.id),
                object_id: None,
            },
            query: Some("sort=name".to_string()),
            output: Some(crate::models::ReportOutputRequest {
                template_id: Some(template_id),
            }),
            missing_data_policy: Some(crate::models::ReportMissingDataPolicy::Omit),
            limits: None,
            relation_context: None,
        };

        let resp = post_request_with_headers(
            &context.pool,
            &context.admin_token,
            REPORTS_ENDPOINT,
            &body,
            vec![],
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::ACCEPTED).await;
        let task: TaskResponse = test::read_body_json(resp).await;

        let completed = wait_for_task(&context, task.id, &[TaskStatus::Succeeded]).await;
        assert_eq!(completed.status, TaskStatus::Succeeded);

        let output = get_request(
            &context.pool,
            &context.admin_token,
            &format!("/api/v1/reports/{}/output", task.id),
        )
        .await;
        let output = assert_response_status(output, StatusCode::OK).await;
        assert_eq!(
            header_value(&output, "X-Hubuum-Report-Warnings"),
            Some("1".to_string())
        );
        let body = String::from_utf8(test::read_body(output).await.to_vec()).unwrap();
        assert_eq!(body, "report-app-01=\nreport-db-01=\n");

        let projection = get_request(
            &context.pool,
            &context.admin_token,
            &format!("/api/v1/reports/{}", task.id),
        )
        .await;
        let projection = assert_response_status(projection, StatusCode::OK).await;
        let projection: TaskResponse = test::read_body_json(projection).await;
        let details = projection
            .details
            .as_ref()
            .and_then(|details| details.report.as_ref())
            .expect("report details");
        assert!(details.output_available);
        assert_eq!(details.template_name.as_deref(), Some("warning-template"));
        assert_eq!(
            details.output_content_type.as_deref(),
            Some(ReportContentType::TextPlain.as_mime())
        );
        assert_eq!(details.warning_count, Some(1));
        assert_eq!(details.truncated, Some(false));
        assert!(details.output_expires_at.is_some());

        cleanup(&classes).await;
    }

    #[rstest]
    #[actix_web::test]
    async fn test_report_idempotency_returns_same_task(#[future(awt)] test_context: TestContext) {
        let context = test_context;
        let classes = create_test_classes(&context, "report_same_task").await;
        let class = classes[0].clone();
        let _ = create_report_objects(&context.pool, &class).await;
        let idempotency_key = context.scoped_name("same-task");

        let body = ReportRequest {
            scope: ReportScope {
                kind: ReportScopeKind::ObjectsInClass,
                class_id: Some(class.id),
                object_id: None,
            },
            query: Some("sort=name".to_string()),
            output: None,
            missing_data_policy: None,
            limits: None,
            relation_context: None,
        };

        let first = post_request_with_headers(
            &context.pool,
            &context.admin_token,
            REPORTS_ENDPOINT,
            &body,
            vec![(
                header::HeaderName::from_static("idempotency-key"),
                idempotency_key.clone(),
            )],
        )
        .await;
        let first = assert_response_status(first, StatusCode::ACCEPTED).await;
        let first_task: TaskResponse = test::read_body_json(first).await;

        let second = post_request_with_headers(
            &context.pool,
            &context.admin_token,
            REPORTS_ENDPOINT,
            &body,
            vec![(
                header::HeaderName::from_static("idempotency-key"),
                idempotency_key,
            )],
        )
        .await;
        let second = assert_response_status(second, StatusCode::ACCEPTED).await;
        let second_task: TaskResponse = test::read_body_json(second).await;

        assert_eq!(first_task.id, second_task.id);

        cleanup(&classes).await;
    }

    #[rstest]
    #[actix_web::test]
    async fn test_report_idempotency_conflicts_for_non_report_task_or_changed_payload(
        #[future(awt)] test_context: TestContext,
    ) {
        let context = test_context;
        let report_key = context.scoped_name("foreign-task-idempotency");
        let report_task = create_task_record(
            &context.pool,
            NewTaskRecord {
                kind: TaskKind::Import.as_str().to_string(),
                status: TaskStatus::Queued.as_str().to_string(),
                submitted_by: Some(context.admin_user.id),
                idempotency_key: Some(report_key.clone()),
                request_hash: Some(context.scoped_name("foreign-task-hash")),
                request_payload: None,
                summary: None,
                total_items: 1,
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

        let classes = create_test_classes(&context, "report_conflict").await;
        let class = classes[0].clone();

        let body = ReportRequest {
            scope: ReportScope {
                kind: ReportScopeKind::ObjectsInClass,
                class_id: Some(class.id),
                object_id: None,
            },
            query: Some("sort=name".to_string()),
            output: None,
            missing_data_policy: None,
            limits: None,
            relation_context: None,
        };

        let resp = post_request_with_headers(
            &context.pool,
            &context.admin_token,
            REPORTS_ENDPOINT,
            &body,
            vec![(
                header::HeaderName::from_static("idempotency-key"),
                report_key,
            )],
        )
        .await;
        assert_response_status(resp, StatusCode::CONFLICT).await;

        let reused = get_request(
            &context.pool,
            &context.admin_token,
            &format!("/api/v1/tasks/{}", report_task.id),
        )
        .await;
        assert_response_status(reused, StatusCode::OK).await;

        let report_idempotency = context.scoped_name("report-task-idempotency");
        let first = post_request_with_headers(
            &context.pool,
            &context.admin_token,
            REPORTS_ENDPOINT,
            &body,
            vec![(
                header::HeaderName::from_static("idempotency-key"),
                report_idempotency.clone(),
            )],
        )
        .await;
        let first = assert_response_status(first, StatusCode::ACCEPTED).await;
        let first_task: TaskResponse = test::read_body_json(first).await;

        let changed_body = ReportRequest {
            query: Some("sort=name.desc".to_string()),
            ..body
        };

        let second = post_request_with_headers(
            &context.pool,
            &context.admin_token,
            REPORTS_ENDPOINT,
            &changed_body,
            vec![(
                header::HeaderName::from_static("idempotency-key"),
                report_idempotency,
            )],
        )
        .await;
        assert_response_status(second, StatusCode::CONFLICT).await;

        let completed = wait_for_task(&context, first_task.id, &[TaskStatus::Succeeded]).await;
        assert_eq!(completed.status, TaskStatus::Succeeded);

        cleanup(&classes).await;
    }

    #[rstest]
    #[actix_web::test]
    async fn test_report_rejects_template_permission_failure_before_task_creation(
        #[future(awt)] test_context: TestContext,
    ) {
        let context = test_context;
        let classes = create_test_classes(&context, "report_template_permission").await;
        let class = classes[0].clone();
        let template_id = create_template(
            &context.pool,
            class.namespace_id,
            "restricted-template",
            ReportContentType::TextPlain,
            "{{ items|length }}",
        )
        .await;

        let body = serde_json::json!({
            "scope": {
                "kind": "objects_in_class",
                "class_id": class.id
            },
            "output": {
                "template_id": template_id
            }
        });

        let resp = post_request_with_headers(
            &context.pool,
            &context.normal_token,
            REPORTS_ENDPOINT,
            &body,
            vec![],
        )
        .await;
        assert_response_status(resp, StatusCode::FORBIDDEN).await;

        cleanup(&classes).await;
    }

    #[rstest]
    #[actix_web::test]
    async fn test_report_projection_and_output_hide_foreign_tasks(
        #[future(awt)] test_context: TestContext,
    ) {
        let context = test_context;
        let classes = create_test_classes(&context, "report_visibility").await;
        let class = classes[0].clone();
        let _ = create_report_objects(&context.pool, &class).await;

        let body = ReportRequest {
            scope: ReportScope {
                kind: ReportScopeKind::ObjectsInClass,
                class_id: Some(class.id),
                object_id: None,
            },
            query: Some("sort=name".to_string()),
            output: None,
            missing_data_policy: None,
            limits: None,
            relation_context: None,
        };

        let resp = post_request_with_headers(
            &context.pool,
            &context.admin_token,
            REPORTS_ENDPOINT,
            &body,
            vec![],
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::ACCEPTED).await;
        let task: TaskResponse = test::read_body_json(resp).await;
        let _ = wait_for_task(&context, task.id, &[TaskStatus::Succeeded]).await;

        let other_user = create_test_user(&context.pool).await;
        let other_token = other_user
            .create_token(&context.pool)
            .await
            .unwrap()
            .get_token();

        let report_resp = get_request(
            &context.pool,
            &other_token,
            &format!("/api/v1/reports/{}", task.id),
        )
        .await;
        assert_response_status(report_resp, StatusCode::NOT_FOUND).await;

        let output_resp = get_request(
            &context.pool,
            &other_token,
            &format!("/api/v1/reports/{}/output", task.id),
        )
        .await;
        assert_response_status(output_resp, StatusCode::NOT_FOUND).await;

        cleanup(&classes).await;
        other_user.delete(&context.pool).await.unwrap();
    }

    #[rstest]
    #[actix_web::test]
    async fn test_report_events_include_running_steps_and_related_output(
        #[future(awt)] test_context: TestContext,
    ) {
        let context = test_context;
        let namespace = context.namespace_fixture("report_related_output").await;
        let host_class = create_named_class(
            &context.pool,
            namespace.namespace.id,
            &context.scoped_name("Host"),
        )
        .await;
        let room_class = create_named_class(
            &context.pool,
            namespace.namespace.id,
            &context.scoped_name("Room"),
        )
        .await;
        let person_class = create_named_class(
            &context.pool,
            namespace.namespace.id,
            &context.scoped_name("Person"),
        )
        .await;

        let host = NewHubuumObject {
            name: "host-01".to_string(),
            description: "host".to_string(),
            namespace_id: namespace.namespace.id,
            hubuum_class_id: host_class.id,
            data: serde_json::json!({}),
        }
        .save(&context.pool)
        .await
        .unwrap();
        let room = NewHubuumObject {
            name: "room-101".to_string(),
            description: "room".to_string(),
            namespace_id: namespace.namespace.id,
            hubuum_class_id: room_class.id,
            data: serde_json::json!({}),
        }
        .save(&context.pool)
        .await
        .unwrap();
        let person = NewHubuumObject {
            name: "alice".to_string(),
            description: "person".to_string(),
            namespace_id: namespace.namespace.id,
            hubuum_class_id: person_class.id,
            data: serde_json::json!({}),
        }
        .save(&context.pool)
        .await
        .unwrap();

        let host_room_relation = NewHubuumClassRelation {
            from_hubuum_class_id: host_class.id,
            to_hubuum_class_id: room_class.id,
            forward_template_alias: Some("rooms".to_string()),
            reverse_template_alias: Some("hosts".to_string()),
        }
        .save(&context.pool)
        .await
        .unwrap();
        let room_person_relation = NewHubuumClassRelation {
            from_hubuum_class_id: room_class.id,
            to_hubuum_class_id: person_class.id,
            forward_template_alias: Some("persons".to_string()),
            reverse_template_alias: Some("rooms".to_string()),
        }
        .save(&context.pool)
        .await
        .unwrap();
        let _ =
            create_object_relation(&context.pool, host.id, room.id, host_room_relation.id).await;
        let _ = create_object_relation(&context.pool, room.id, person.id, room_person_relation.id)
            .await;

        let template_id = create_template(
            &context.pool,
            namespace.namespace.id,
            "reachable-template",
            ReportContentType::TextPlain,
            "{% for host in items %}Host: {{ host.name }} {% for person in host.reachable.persons %}{{ person.name }}{% endfor %}{% endfor %}",
        )
        .await;

        let body = ReportRequest {
            scope: ReportScope {
                kind: ReportScopeKind::RelatedObjects,
                class_id: Some(host_class.id),
                object_id: Some(host.id),
            },
            query: None,
            output: Some(crate::models::ReportOutputRequest {
                template_id: Some(template_id),
            }),
            missing_data_policy: None,
            limits: None,
            relation_context: None,
        };

        let resp = post_request_with_headers(
            &context.pool,
            &context.admin_token,
            REPORTS_ENDPOINT,
            &body,
            vec![],
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::ACCEPTED).await;
        let task: TaskResponse = test::read_body_json(resp).await;

        let completed = wait_for_task(&context, task.id, &[TaskStatus::Succeeded]).await;
        assert_eq!(completed.status, TaskStatus::Succeeded);

        let output_resp = get_request(
            &context.pool,
            &context.admin_token,
            &format!("/api/v1/reports/{}/output", task.id),
        )
        .await;
        let output_resp = assert_response_status(output_resp, StatusCode::OK).await;
        let rendered = String::from_utf8(test::read_body(output_resp).await.to_vec()).unwrap();
        assert_eq!(rendered, "Host: host-01 alice");

        let events_resp = get_request(
            &context.pool,
            &context.admin_token,
            &format!("/api/v1/tasks/{}/events", task.id),
        )
        .await;
        let events_resp = assert_response_status(events_resp, StatusCode::OK).await;
        let events: Vec<TaskEventResponse> = test::read_body_json(events_resp).await;
        let messages = events
            .iter()
            .map(|event| event.message.as_str())
            .collect::<Vec<_>>();

        assert!(messages.contains(&"Report execution started"));
        assert!(messages.contains(&"Query execution started"));
        assert!(messages.contains(&"Hydrating relation-aware template context"));
        assert!(messages.contains(&"Rendering report output"));
        assert!(messages.contains(&"Persisting report output"));

        namespace.cleanup().await.unwrap();
    }

    #[rstest]
    #[actix_web::test]
    async fn test_report_relation_aliases_and_paths_are_available_in_templates(
        #[future(awt)] test_context: TestContext,
    ) {
        let context = test_context;
        let namespace = context.namespace_fixture("report_paths_aliases").await;
        let host_class = create_named_class(
            &context.pool,
            namespace.namespace.id,
            &context.scoped_name("Host"),
        )
        .await;
        let room_class = create_named_class(
            &context.pool,
            namespace.namespace.id,
            &context.scoped_name("Room"),
        )
        .await;
        let person_class = create_named_class(
            &context.pool,
            namespace.namespace.id,
            &context.scoped_name("Person"),
        )
        .await;

        let host = NewHubuumObject {
            name: "host-01".to_string(),
            description: "host".to_string(),
            namespace_id: namespace.namespace.id,
            hubuum_class_id: host_class.id,
            data: serde_json::json!({}),
        }
        .save(&context.pool)
        .await
        .unwrap();
        let room_a = NewHubuumObject {
            name: "room-101".to_string(),
            description: "room".to_string(),
            namespace_id: namespace.namespace.id,
            hubuum_class_id: room_class.id,
            data: serde_json::json!({}),
        }
        .save(&context.pool)
        .await
        .unwrap();
        let room_b = NewHubuumObject {
            name: "room-102".to_string(),
            description: "room".to_string(),
            namespace_id: namespace.namespace.id,
            hubuum_class_id: room_class.id,
            data: serde_json::json!({}),
        }
        .save(&context.pool)
        .await
        .unwrap();
        let person = NewHubuumObject {
            name: "alice".to_string(),
            description: "person".to_string(),
            namespace_id: namespace.namespace.id,
            hubuum_class_id: person_class.id,
            data: serde_json::json!({}),
        }
        .save(&context.pool)
        .await
        .unwrap();

        let host_room_relation = NewHubuumClassRelation {
            from_hubuum_class_id: host_class.id,
            to_hubuum_class_id: room_class.id,
            forward_template_alias: Some("rooms".to_string()),
            reverse_template_alias: Some("hosts".to_string()),
        }
        .save(&context.pool)
        .await
        .unwrap();
        let room_person_relation = NewHubuumClassRelation {
            from_hubuum_class_id: room_class.id,
            to_hubuum_class_id: person_class.id,
            forward_template_alias: Some("persons".to_string()),
            reverse_template_alias: Some("rooms".to_string()),
        }
        .save(&context.pool)
        .await
        .unwrap();

        let _ =
            create_object_relation(&context.pool, host.id, room_a.id, host_room_relation.id).await;
        let _ =
            create_object_relation(&context.pool, host.id, room_b.id, host_room_relation.id).await;
        let _ =
            create_object_relation(&context.pool, room_a.id, person.id, room_person_relation.id)
                .await;
        let _ =
            create_object_relation(&context.pool, room_b.id, person.id, room_person_relation.id)
                .await;

        let template_id = create_template(
            &context.pool,
            namespace.namespace.id,
            "paths-template",
            ReportContentType::TextPlain,
            "{% for host in items %}rooms={% for room in host.related.rooms %}{{ room.name }} {% endfor %}|reachable={% for person in host.reachable.persons %}{{ person.name }} {% endfor %}|paths={% for person in host.paths.persons %}[{{ person.name }} via {{ person.path_objects[1].name }}]{% endfor %}{% endfor %}",
        )
        .await;

        let body = ReportRequest {
            scope: ReportScope {
                kind: ReportScopeKind::RelatedObjects,
                class_id: Some(host_class.id),
                object_id: Some(host.id),
            },
            query: None,
            output: Some(crate::models::ReportOutputRequest {
                template_id: Some(template_id),
            }),
            missing_data_policy: None,
            limits: None,
            relation_context: None,
        };

        let resp = post_request_with_headers(
            &context.pool,
            &context.admin_token,
            REPORTS_ENDPOINT,
            &body,
            vec![],
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::ACCEPTED).await;
        let task: TaskResponse = test::read_body_json(resp).await;
        let _ = wait_for_task(&context, task.id, &[TaskStatus::Succeeded]).await;

        let output = get_request(
            &context.pool,
            &context.admin_token,
            &format!("/api/v1/reports/{}/output", task.id),
        )
        .await;
        let output = assert_response_status(output, StatusCode::OK).await;
        let rendered = String::from_utf8(test::read_body(output).await.to_vec()).unwrap();

        assert_eq!(
            rendered,
            "rooms=room-101 room-102 |reachable=alice |paths=[alice via room-101][alice via room-102]"
        );

        namespace.cleanup().await.unwrap();
    }

    #[rstest]
    #[actix_web::test]
    async fn test_report_output_cleanup_removes_expired_artifacts(
        #[future(awt)] test_context: TestContext,
    ) {
        use diesel::prelude::*;

        let context = test_context;
        let classes = create_test_classes(&context, "report_cleanup").await;
        let class = classes[0].clone();
        let _ = create_report_objects(&context.pool, &class).await;
        let template_id = create_template(
            &context.pool,
            class.namespace_id,
            "cleanup-template",
            ReportContentType::TextPlain,
            "{% for item in items %}{{ item.name }}\n{% endfor %}",
        )
        .await;

        let body = ReportRequest {
            scope: ReportScope {
                kind: ReportScopeKind::ObjectsInClass,
                class_id: Some(class.id),
                object_id: None,
            },
            query: Some("sort=name".to_string()),
            output: Some(crate::models::ReportOutputRequest {
                template_id: Some(template_id),
            }),
            missing_data_policy: None,
            limits: None,
            relation_context: None,
        };

        let resp = post_request_with_headers(
            &context.pool,
            &context.admin_token,
            REPORTS_ENDPOINT,
            &body,
            vec![],
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::ACCEPTED).await;
        let task: TaskResponse = test::read_body_json(resp).await;
        let _ = wait_for_task(&context, task.id, &[TaskStatus::Succeeded]).await;

        crate::db::with_connection(&context.pool, |conn| {
            use crate::schema::report_task_outputs::dsl::{
                output_expires_at, report_task_outputs, task_id,
            };

            diesel::update(report_task_outputs.filter(task_id.eq(task.id)))
                .set(
                    output_expires_at
                        .eq(chrono::Utc::now().naive_utc() - chrono::Duration::hours(1)),
                )
                .execute(conn)
        })
        .unwrap();

        let cleaned = purge_expired_report_outputs(&context.pool).await.unwrap();
        assert_eq!(cleaned, vec![task.id]);

        let projection = get_request(
            &context.pool,
            &context.admin_token,
            &format!("/api/v1/reports/{}", task.id),
        )
        .await;
        let projection = assert_response_status(projection, StatusCode::OK).await;
        let projection: TaskResponse = test::read_body_json(projection).await;
        let details = projection
            .details
            .as_ref()
            .and_then(|details| details.report.as_ref())
            .expect("report details");
        assert!(!details.output_available);
        assert_eq!(details.output_expires_at, None);

        let output = get_request(
            &context.pool,
            &context.admin_token,
            &format!("/api/v1/reports/{}/output", task.id),
        )
        .await;
        assert_response_status(output, StatusCode::NOT_FOUND).await;

        let events_resp = get_request(
            &context.pool,
            &context.admin_token,
            &format!("/api/v1/tasks/{}/events", task.id),
        )
        .await;
        let events_resp = assert_response_status(events_resp, StatusCode::OK).await;
        let events: Vec<TaskEventResponse> = test::read_body_json(events_resp).await;
        assert!(
            events
                .iter()
                .any(|event| event.event_type == "cleanup" && event.message.contains("cleaned up"))
        );

        cleanup(&classes).await;
    }
}
