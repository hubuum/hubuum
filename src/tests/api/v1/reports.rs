#[cfg(test)]
mod tests {
    use actix_web::{
        http::{StatusCode, header},
        test,
    };

    use crate::models::{
        HubuumClass, HubuumClassRelation, HubuumObject, HubuumObjectRelation,
        NewHubuumClassRelation, NewHubuumObject, NewHubuumObjectRelation, NewReportTemplate,
        ReportContentType, ReportJsonResponse, ReportRequest, ReportScope, ReportScopeKind,
    };
    use crate::tests::api::v1::classes::tests::{cleanup, create_test_classes};
    use crate::tests::api_operations::post_request_with_headers;
    use crate::tests::asserts::assert_response_status;
    use crate::tests::{TestContext, test_context};
    use crate::traits::CanSave;
    use rstest::rstest;

    const REPORTS_ENDPOINT: &str = "/api/v1/reports";

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

    async fn create_report_relation_fixture(
        pool: &crate::db::DbPool,
        classes: &[HubuumClass],
    ) -> (
        Vec<HubuumObject>,
        Vec<HubuumClassRelation>,
        Vec<HubuumObjectRelation>,
    ) {
        let objects = vec![
            NewHubuumObject {
                name: "report-root-01".to_string(),
                description: "Report root object".to_string(),
                namespace_id: classes[0].namespace_id,
                hubuum_class_id: classes[0].id,
                data: serde_json::json!({"hostname": "report-root-01", "role": "root"}),
            },
            NewHubuumObject {
                name: "report-mid-01".to_string(),
                description: "Report middle object".to_string(),
                namespace_id: classes[1].namespace_id,
                hubuum_class_id: classes[1].id,
                data: serde_json::json!({"hostname": "report-mid-01", "role": "middle"}),
            },
            NewHubuumObject {
                name: "report-leaf-01".to_string(),
                description: "Report leaf object".to_string(),
                namespace_id: classes[2].namespace_id,
                hubuum_class_id: classes[2].id,
                data: serde_json::json!({"hostname": "report-leaf-01", "role": "leaf"}),
            },
        ];

        let mut created_objects = Vec::new();
        for object in objects {
            created_objects.push(object.save(pool).await.unwrap());
        }

        let class_relations = vec![
            NewHubuumClassRelation {
                from_hubuum_class_id: classes[0].id,
                to_hubuum_class_id: classes[1].id,
            }
            .save(pool)
            .await
            .unwrap(),
            NewHubuumClassRelation {
                from_hubuum_class_id: classes[1].id,
                to_hubuum_class_id: classes[2].id,
            }
            .save(pool)
            .await
            .unwrap(),
        ];

        let object_relations = vec![
            NewHubuumObjectRelation {
                from_hubuum_object_id: created_objects[0].id,
                to_hubuum_object_id: created_objects[1].id,
                class_relation_id: class_relations[0].id,
            }
            .save(pool)
            .await
            .unwrap(),
            NewHubuumObjectRelation {
                from_hubuum_object_id: created_objects[1].id,
                to_hubuum_object_id: created_objects[2].id,
                class_relation_id: class_relations[1].id,
            }
            .save(pool)
            .await
            .unwrap(),
        ];

        (created_objects, class_relations, object_relations)
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
    async fn test_run_report_returns_json_envelope(#[future(awt)] test_context: TestContext) {
        let context = test_context;
        let pool = &context.pool;
        let admin_token = &context.admin_token;
        let classes = create_test_classes(&context, "report_json").await;
        let class = classes[0].clone();
        let created_objects = create_report_objects(pool, &class).await;

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
            include: None,
        };

        let resp = post_request_with_headers(
            pool,
            admin_token,
            REPORTS_ENDPOINT,
            &body,
            vec![(header::ACCEPT, "application/json".to_string())],
        )
        .await;

        let resp = assert_response_status(resp, StatusCode::OK).await;
        let headers = resp.headers().clone();
        let report: ReportJsonResponse = test::read_body_json(resp).await;

        assert_eq!(report.meta.count, created_objects.len());
        assert_eq!(report.meta.scope.kind, ReportScopeKind::ObjectsInClass);
        assert_eq!(report.warnings.len(), 0);
        assert_eq!(
            headers
                .get("X-Hubuum-Report-Warnings")
                .unwrap()
                .to_str()
                .unwrap(),
            "0"
        );
        assert_eq!(report.items.len(), 2);
        assert_eq!(report.items[0]["name"], "report-app-01");
        assert_eq!(report.items[1]["name"], "report-db-01");

        cleanup(&classes).await;
    }

    #[rstest]
    #[actix_web::test]
    async fn docs_report_class_relations_example_runs(#[future(awt)] test_context: TestContext) {
        let context = test_context;
        let pool = &context.pool;
        let admin_token = &context.admin_token;
        let classes = create_test_classes(
            &context,
            &context.scoped_name("report_class_relations_docs"),
        )
        .await;
        let (_objects, class_relations, _object_relations) =
            create_report_relation_fixture(pool, &classes).await;

        let body = serde_json::json!({
            "scope": {
                "kind": "class_relations"
            },
            "query": format!("from_classes={}&sort=created_at.desc", classes[0].id),
            "limits": {
                "max_items": 50
            }
        });

        let resp = post_request_with_headers(
            pool,
            admin_token,
            REPORTS_ENDPOINT,
            &body,
            vec![(header::ACCEPT, "application/json".to_string())],
        )
        .await;

        let resp = assert_response_status(resp, StatusCode::OK).await;
        let report: ReportJsonResponse = test::read_body_json(resp).await;

        assert_eq!(report.meta.scope.kind, ReportScopeKind::ClassRelations);
        assert_eq!(report.items.len(), 1);
        assert_eq!(report.items[0]["id"], class_relations[0].id);
        assert_eq!(
            report.items[0]["from_hubuum_class_id"],
            class_relations[0].from_hubuum_class_id
        );
        assert_eq!(
            report.items[0]["to_hubuum_class_id"],
            class_relations[0].to_hubuum_class_id
        );

        cleanup(&classes).await;
    }

    #[rstest]
    #[actix_web::test]
    async fn docs_report_object_relations_example_runs(#[future(awt)] test_context: TestContext) {
        let context = test_context;
        let pool = &context.pool;
        let admin_token = &context.admin_token;
        let classes = create_test_classes(
            &context,
            &context.scoped_name("report_object_relations_docs"),
        )
        .await;
        let (objects, _class_relations, object_relations) =
            create_report_relation_fixture(pool, &classes).await;

        let body = serde_json::json!({
            "scope": {
                "kind": "object_relations"
            },
            "query": format!("to_objects={}&sort=created_at.desc", objects[1].id)
        });

        let resp = post_request_with_headers(
            pool,
            admin_token,
            REPORTS_ENDPOINT,
            &body,
            vec![(header::ACCEPT, "application/json".to_string())],
        )
        .await;

        let resp = assert_response_status(resp, StatusCode::OK).await;
        let report: ReportJsonResponse = test::read_body_json(resp).await;

        assert_eq!(report.meta.scope.kind, ReportScopeKind::ObjectRelations);
        assert_eq!(report.items.len(), 1);
        assert_eq!(report.items[0]["id"], object_relations[0].id);
        assert_eq!(
            report.items[0]["from_hubuum_object_id"],
            object_relations[0].from_hubuum_object_id
        );
        assert_eq!(
            report.items[0]["to_hubuum_object_id"],
            object_relations[0].to_hubuum_object_id
        );

        cleanup(&classes).await;
    }

    #[rstest]
    #[actix_web::test]
    async fn docs_report_related_objects_example_runs(#[future(awt)] test_context: TestContext) {
        let context = test_context;
        let pool = &context.pool;
        let admin_token = &context.admin_token;
        let classes = create_test_classes(
            &context,
            &context.scoped_name("report_related_objects_docs"),
        )
        .await;
        let (objects, _class_relations, _object_relations) =
            create_report_relation_fixture(pool, &classes).await;

        let body = serde_json::json!({
            "scope": {
                "kind": "related_objects",
                "class_id": classes[0].id,
                "object_id": objects[0].id
            },
            "query": format!("depth__lte=2&to_classes={}&sort=path", classes[2].id)
        });

        let resp = post_request_with_headers(
            pool,
            admin_token,
            REPORTS_ENDPOINT,
            &body,
            vec![(header::ACCEPT, "application/json".to_string())],
        )
        .await;

        let resp = assert_response_status(resp, StatusCode::OK).await;
        let report: ReportJsonResponse = test::read_body_json(resp).await;

        assert_eq!(report.meta.scope.kind, ReportScopeKind::RelatedObjects);
        assert_eq!(report.items.len(), 1);
        assert_eq!(report.items[0]["id"], objects[2].id);
        assert_eq!(report.items[0]["name"], "report-leaf-01");
        assert_eq!(report.items[0]["data"]["hostname"], "report-leaf-01");
        assert!(
            report.items[0]["path"].as_array().is_some(),
            "expected related object report item to include path array, got {}",
            report.items[0]
        );

        cleanup(&classes).await;
    }

    #[rstest]
    #[actix_web::test]
    async fn test_run_report_objects_in_class_includes_related_objects(
        #[future(awt)] test_context: TestContext,
    ) {
        let context = test_context;
        let pool = &context.pool;
        let admin_token = &context.admin_token;
        let classes = create_test_classes(
            &context,
            &context.scoped_name("report_include_related_json"),
        )
        .await;
        let (objects, class_relations, _object_relations) =
            create_report_relation_fixture(pool, &classes).await;

        let second_room = NewHubuumObject {
            name: "report-mid-02".to_string(),
            description: "Second related object".to_string(),
            namespace_id: classes[1].namespace_id,
            hubuum_class_id: classes[1].id,
            data: serde_json::json!({"hostname": "report-mid-02", "role": "middle"}),
        }
        .save(pool)
        .await
        .unwrap();
        NewHubuumObjectRelation {
            from_hubuum_object_id: objects[0].id,
            to_hubuum_object_id: second_room.id,
            class_relation_id: class_relations[0].id,
        }
        .save(pool)
        .await
        .unwrap();

        let body = serde_json::json!({
            "scope": {
                "kind": "objects_in_class",
                "class_id": classes[0].id
            },
            "query": "name__equals=report-root-01",
            "include": {
                "related_objects": {
                    "room": {
                        "class_id": classes[1].id,
                        "limit": 1
                    }
                }
            }
        });

        let resp = post_request_with_headers(
            pool,
            admin_token,
            REPORTS_ENDPOINT,
            &body,
            vec![(header::ACCEPT, "application/json".to_string())],
        )
        .await;

        let resp = assert_response_status(resp, StatusCode::OK).await;
        let report: ReportJsonResponse = test::read_body_json(resp).await;

        assert_eq!(report.items.len(), 1);
        assert_eq!(report.items[0]["name"], "report-root-01");
        let room = report.items[0]["related"]["room"].as_array().unwrap();
        assert_eq!(room.len(), 1);
        assert_eq!(room[0]["name"], "report-mid-01");
        assert_eq!(room[0]["data"]["hostname"], "report-mid-01");
        assert!(room[0]["path"].as_array().is_some());

        cleanup(&classes).await;
    }

    #[rstest]
    #[actix_web::test]
    async fn test_run_report_template_renders_included_related_objects(
        #[future(awt)] test_context: TestContext,
    ) {
        let context = test_context;
        let pool = &context.pool;
        let admin_token = &context.admin_token;
        let classes = create_test_classes(
            &context,
            &context.scoped_name("report_include_related_template"),
        )
        .await;
        let _fixture = create_report_relation_fixture(pool, &classes).await;
        let template_id = create_template(
            pool,
            classes[0].namespace_id,
            "included-related-template",
            ReportContentType::TextPlain,
            "{{#each items}}{{this.name}} is in {{this.related.room[0].name}}\\n{{/each}}",
        )
        .await;

        let body = serde_json::json!({
            "scope": {
                "kind": "objects_in_class",
                "class_id": classes[0].id
            },
            "query": "name__equals=report-root-01",
            "output": {
                "template_id": template_id
            },
            "include": {
                "related_objects": {
                    "room": {
                        "class_id": classes[1].id
                    }
                }
            }
        });

        let resp =
            post_request_with_headers(pool, admin_token, REPORTS_ENDPOINT, &body, vec![]).await;

        let resp = assert_response_status(resp, StatusCode::OK).await;
        let rendered = String::from_utf8(test::read_body(resp).await.to_vec()).unwrap();
        assert_eq!(rendered, "report-root-01 is in report-mid-01\\n");

        cleanup(&classes).await;
    }

    #[rstest]
    #[actix_web::test]
    async fn test_run_report_included_related_objects_empty_when_missing(
        #[future(awt)] test_context: TestContext,
    ) {
        let context = test_context;
        let pool = &context.pool;
        let admin_token = &context.admin_token;
        let classes = create_test_classes(
            &context,
            &context.scoped_name("report_include_related_empty"),
        )
        .await;
        let _fixture = create_report_relation_fixture(pool, &classes).await;
        let unlinked = NewHubuumObject {
            name: "report-root-02".to_string(),
            description: "Unlinked root object".to_string(),
            namespace_id: classes[0].namespace_id,
            hubuum_class_id: classes[0].id,
            data: serde_json::json!({"hostname": "report-root-02", "role": "root"}),
        }
        .save(pool)
        .await
        .unwrap();

        let body = serde_json::json!({
            "scope": {
                "kind": "objects_in_class",
                "class_id": classes[0].id
            },
            "query": format!("id={}", unlinked.id),
            "include": {
                "related_objects": {
                    "room": {
                        "class_id": classes[1].id
                    }
                }
            }
        });

        let resp = post_request_with_headers(
            pool,
            admin_token,
            REPORTS_ENDPOINT,
            &body,
            vec![(header::ACCEPT, "application/json".to_string())],
        )
        .await;

        let resp = assert_response_status(resp, StatusCode::OK).await;
        let report: ReportJsonResponse = test::read_body_json(resp).await;

        assert_eq!(report.items.len(), 1);
        assert_eq!(
            report.items[0]["related"]["room"].as_array().unwrap().len(),
            0
        );

        cleanup(&classes).await;
    }

    #[rstest]
    #[actix_web::test]
    async fn test_run_report_included_related_objects_respects_max_depth(
        #[future(awt)] test_context: TestContext,
    ) {
        let context = test_context;
        let pool = &context.pool;
        let admin_token = &context.admin_token;
        let classes = create_test_classes(
            &context,
            &context.scoped_name("report_include_related_depth"),
        )
        .await;
        let _fixture = create_report_relation_fixture(pool, &classes).await;

        let direct_body = serde_json::json!({
            "scope": {
                "kind": "objects_in_class",
                "class_id": classes[0].id
            },
            "query": "name__equals=report-root-01",
            "include": {
                "related_objects": {
                    "leaf": {
                        "class_id": classes[2].id,
                        "max_depth": 1
                    }
                }
            }
        });
        let transitive_body = serde_json::json!({
            "scope": {
                "kind": "objects_in_class",
                "class_id": classes[0].id
            },
            "query": "name__equals=report-root-01",
            "include": {
                "related_objects": {
                    "leaf": {
                        "class_id": classes[2].id,
                        "max_depth": 2
                    }
                }
            }
        });

        let direct_resp = post_request_with_headers(
            pool,
            admin_token,
            REPORTS_ENDPOINT,
            &direct_body,
            vec![(header::ACCEPT, "application/json".to_string())],
        )
        .await;
        let direct_resp = assert_response_status(direct_resp, StatusCode::OK).await;
        let direct_report: ReportJsonResponse = test::read_body_json(direct_resp).await;
        assert_eq!(
            direct_report.items[0]["related"]["leaf"]
                .as_array()
                .unwrap()
                .len(),
            0
        );

        let transitive_resp = post_request_with_headers(
            pool,
            admin_token,
            REPORTS_ENDPOINT,
            &transitive_body,
            vec![(header::ACCEPT, "application/json".to_string())],
        )
        .await;
        let transitive_resp = assert_response_status(transitive_resp, StatusCode::OK).await;
        let transitive_report: ReportJsonResponse = test::read_body_json(transitive_resp).await;
        assert_eq!(
            transitive_report.items[0]["related"]["leaf"][0]["name"],
            "report-leaf-01"
        );

        cleanup(&classes).await;
    }

    #[rstest]
    #[actix_web::test]
    async fn test_run_report_rejects_related_include_on_other_scopes(
        #[future(awt)] test_context: TestContext,
    ) {
        let context = test_context;
        let pool = &context.pool;
        let admin_token = &context.admin_token;

        let body = serde_json::json!({
            "scope": {
                "kind": "classes"
            },
            "include": {
                "related_objects": {
                    "room": {
                        "class_id": 1
                    }
                }
            }
        });

        let resp =
            post_request_with_headers(pool, admin_token, REPORTS_ENDPOINT, &body, vec![]).await;

        assert_response_status(resp, StatusCode::BAD_REQUEST).await;
    }

    #[rstest]
    #[actix_web::test]
    async fn test_run_report_rejects_invalid_related_include_options(
        #[future(awt)] test_context: TestContext,
    ) {
        let context = test_context;
        let pool = &context.pool;
        let admin_token = &context.admin_token;

        let invalid_includes = vec![
            serde_json::json!({
                "bad-alias": {
                    "class_id": 1
                }
            }),
            serde_json::json!({
                "room": {
                    "class_id": 0
                }
            }),
            serde_json::json!({
                "room": {
                    "class_id": 1,
                    "max_depth": 0
                }
            }),
            serde_json::json!({
                "room": {
                    "class_id": 1,
                    "max_depth": 11
                }
            }),
            serde_json::json!({
                "room": {
                    "class_id": 1,
                    "limit": 0
                }
            }),
            serde_json::json!({
                "room": {
                    "class_id": 1,
                    "limit": 51
                }
            }),
        ];

        for related_objects in invalid_includes {
            let body = serde_json::json!({
                "scope": {
                    "kind": "objects_in_class",
                    "class_id": 1
                },
                "include": {
                    "related_objects": related_objects
                }
            });

            let resp =
                post_request_with_headers(pool, admin_token, REPORTS_ENDPOINT, &body, vec![]).await;

            assert_response_status(resp, StatusCode::BAD_REQUEST).await;
        }
    }

    #[rstest]
    #[actix_web::test]
    async fn test_run_report_renders_text_template_from_stored_template(
        #[future(awt)] test_context: TestContext,
    ) {
        let context = test_context;
        let pool = &context.pool;
        let admin_token = &context.admin_token;
        let classes = create_test_classes(&context, "report_text").await;
        let class = classes[0].clone();
        let _created_objects = create_report_objects(pool, &class).await;
        let template_id = create_template(
            pool,
            class.namespace_id,
            "stored-report-template",
            ReportContentType::TextPlain,
            "{{#each items}}{{this.name}}={{this.data.owner}}\\n{{/each}}",
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

        let resp =
            post_request_with_headers(pool, admin_token, REPORTS_ENDPOINT, &body, vec![]).await;

        let resp = assert_response_status(resp, StatusCode::OK).await;
        let headers = resp.headers().clone();
        let body = test::read_body(resp).await;
        let rendered = String::from_utf8(body.to_vec()).unwrap();

        assert_eq!(headers.get(header::CONTENT_TYPE).unwrap(), "text/plain");
        assert_eq!(rendered, "report-app-01=alice\\nreport-db-01=bob\\n");

        cleanup(&classes).await;
    }

    #[rstest]
    #[actix_web::test]
    async fn test_run_report_rejects_output_content_type_field(
        #[future(awt)] test_context: TestContext,
    ) {
        let context = test_context;
        let pool = &context.pool;
        let admin_token = &context.admin_token;
        let classes = create_test_classes(&context, "report_content_type_rejected").await;
        let class = classes[0].clone();
        let template_id = create_template(
            pool,
            class.namespace_id,
            "template-without-content-type",
            ReportContentType::TextPlain,
            "{{#each items}}{{this.name}}={{this.data.owner}}\\n{{/each}}",
        )
        .await;

        let body = serde_json::json!({
            "scope": { "kind": "classes" },
            "output": {
                "template_id": template_id,
                "content_type": "text/plain"
            }
        });

        let resp =
            post_request_with_headers(pool, admin_token, REPORTS_ENDPOINT, &body, vec![]).await;

        assert_response_status(resp, StatusCode::BAD_REQUEST).await;
        cleanup(&classes).await;
    }

    #[rstest]
    #[actix_web::test]
    async fn test_run_report_requires_template_for_non_json_output(
        #[future(awt)] test_context: TestContext,
    ) {
        let context = test_context;
        let pool = &context.pool;
        let admin_token = &context.admin_token;

        let body = serde_json::json!({
            "scope": {
                "kind": "classes"
            }
        });

        let resp = post_request_with_headers(
            pool,
            admin_token,
            REPORTS_ENDPOINT,
            &body,
            vec![(header::ACCEPT, "text/plain".to_string())],
        )
        .await;

        assert_response_status(resp, StatusCode::BAD_REQUEST).await;
    }

    #[rstest]
    #[actix_web::test]
    async fn test_run_report_rejects_accept_mismatch_for_template(
        #[future(awt)] test_context: TestContext,
    ) {
        let context = test_context;
        let pool = &context.pool;
        let admin_token = &context.admin_token;
        let classes = create_test_classes(&context, "report_accept_mismatch").await;
        let class = classes[0].clone();
        let template_id = create_template(
            pool,
            class.namespace_id,
            "template-accept-mismatch",
            ReportContentType::TextPlain,
            "{{#each items}}{{this.name}}={{this.data.owner}}\\n{{/each}}",
        )
        .await;

        let body = serde_json::json!({
            "scope": {
                "kind": "classes"
            },
            "output": {
                "template_id": template_id
            }
        });

        let resp = post_request_with_headers(
            pool,
            admin_token,
            REPORTS_ENDPOINT,
            body,
            vec![(header::ACCEPT, "text/html".to_string())],
        )
        .await;

        assert_response_status(resp, StatusCode::NOT_ACCEPTABLE).await;
        cleanup(&classes).await;
    }

    #[rstest]
    #[actix_web::test]
    async fn test_run_report_requires_read_template_permission(
        #[future(awt)] test_context: TestContext,
    ) {
        let context = test_context;
        let pool = &context.pool;
        let normal_token = &context.normal_token;
        let classes = create_test_classes(&context, "report_template_permission").await;
        let class = classes[0].clone();
        let template_id = create_template(
            pool,
            class.namespace_id,
            "template-read-permission",
            ReportContentType::TextPlain,
            "{{#each items}}{{this.name}}={{this.data.owner}}\\n{{/each}}",
        )
        .await;

        let body = serde_json::json!({
            "scope": { "kind": "classes" },
            "output": {
                "template_id": template_id
            }
        });

        let resp =
            post_request_with_headers(pool, normal_token, REPORTS_ENDPOINT, body, vec![]).await;

        assert_response_status(resp, StatusCode::FORBIDDEN).await;
        cleanup(&classes).await;
    }

    #[rstest]
    #[actix_web::test]
    async fn test_run_report_succeeds_with_read_template_permission(
        #[future(awt)] test_context: TestContext,
    ) {
        use crate::models::{Permissions, PermissionsList};
        use crate::tests::create_test_user;
        use crate::traits::PermissionController;

        let context = test_context;
        let pool = &context.pool;
        let classes = create_test_classes(&context, "report_template_read_granted").await;
        let class = classes[0].clone();
        let _created_objects = create_report_objects(pool, &class).await;
        let template_id = create_template(
            pool,
            class.namespace_id,
            "template-with-permission",
            ReportContentType::TextPlain,
            "{{#each items}}{{this.name}}={{this.data.owner}}\\n{{/each}}",
        )
        .await;

        // Create a user with only ReadTemplate permission
        let test_user = create_test_user(pool).await;
        let user_token = test_user.create_token(pool).await.unwrap().get_token();

        // Grant ReadTemplate permission to the namespace
        classes
            .namespace
            .namespace
            .grant(
                pool,
                classes.namespace.owner_group.id,
                PermissionsList::new([Permissions::ReadTemplate, Permissions::ReadObject]),
            )
            .await
            .unwrap();

        // Add test user to the group with permissions
        classes
            .namespace
            .owner_group
            .add_member(pool, &test_user)
            .await
            .unwrap();

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

        let resp =
            post_request_with_headers(pool, &user_token, REPORTS_ENDPOINT, body, vec![]).await;

        let resp = assert_response_status(resp, StatusCode::OK).await;
        let rendered = String::from_utf8(test::read_body(resp).await.to_vec()).unwrap();
        assert_eq!(rendered, "report-app-01=alice\\nreport-db-01=bob\\n");

        cleanup(&classes).await;
        test_user.delete(pool).await.unwrap();
    }

    #[rstest]
    #[actix_web::test]
    async fn test_run_report_nonexistent_template_returns_not_found(
        #[future(awt)] test_context: TestContext,
    ) {
        let context = test_context;
        let pool = &context.pool;
        let admin_token = &context.admin_token;

        let body = serde_json::json!({
            "scope": { "kind": "classes" },
            "output": {
                "template_id": 999_999_999
            }
        });

        let resp =
            post_request_with_headers(pool, admin_token, REPORTS_ENDPOINT, body, vec![]).await;

        assert_response_status(resp, StatusCode::NOT_FOUND).await;
    }

    #[rstest]
    #[actix_web::test]
    async fn test_run_report_rejects_accept_application_json_for_template(
        #[future(awt)] test_context: TestContext,
    ) {
        let context = test_context;
        let pool = &context.pool;
        let admin_token = &context.admin_token;
        let classes = create_test_classes(&context, "report_accept_json_mismatch").await;
        let class = classes[0].clone();

        let template_id = create_template(
            pool,
            class.namespace_id,
            "template-accept-json-mismatch",
            ReportContentType::TextPlain,
            "{{#each items}}{{this.name}}={{this.data.owner}}\\n{{/each}}",
        )
        .await;

        let body = serde_json::json!({
            "scope": { "kind": "classes" },
            "output": {
                "template_id": template_id
            }
        });

        let resp = post_request_with_headers(
            pool,
            admin_token,
            REPORTS_ENDPOINT,
            body,
            vec![(header::ACCEPT, "application/json".to_string())],
        )
        .await;

        assert_response_status(resp, StatusCode::NOT_ACCEPTABLE).await;
        cleanup(&classes).await;
    }

    #[rstest]
    #[actix_web::test]
    async fn test_run_report_renders_html_template_from_stored_template(
        #[future(awt)] test_context: TestContext,
    ) {
        let context = test_context;
        let pool = &context.pool;
        let admin_token = &context.admin_token;
        let classes = create_test_classes(&context, "report_html").await;
        let class = classes[0].clone();
        let _created_objects = create_report_objects(pool, &class).await;

        let template_id = create_template(
            pool,
            class.namespace_id,
            "stored-html-report-template",
            ReportContentType::TextHtml,
            "<ul>{{#each items}}<li>{{this.name}}:{{this.data.owner}}</li>{{/each}}</ul>",
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
            pool,
            admin_token,
            REPORTS_ENDPOINT,
            body,
            vec![(header::ACCEPT, "text/html".to_string())],
        )
        .await;

        let resp = assert_response_status(resp, StatusCode::OK).await;
        let headers = resp.headers().clone();
        let body = test::read_body(resp).await;
        let rendered = String::from_utf8(body.to_vec()).unwrap();

        assert!(
            headers
                .get(header::CONTENT_TYPE)
                .unwrap()
                .to_str()
                .unwrap()
                .starts_with("text/html")
        );
        assert_eq!(
            rendered,
            "<ul><li>report-app-01:alice</li><li>report-db-01:bob</li></ul>"
        );

        cleanup(&classes).await;
    }

    #[rstest]
    #[actix_web::test]
    async fn test_run_report_renders_csv_template_from_stored_template(
        #[future(awt)] test_context: TestContext,
    ) {
        let context = test_context;
        let pool = &context.pool;
        let admin_token = &context.admin_token;
        let classes = create_test_classes(&context, "report_csv").await;
        let class = classes[0].clone();
        let _created_objects = create_report_objects(pool, &class).await;

        let template_id = create_template(
            pool,
            class.namespace_id,
            "stored-csv-report-template",
            ReportContentType::TextCsv,
            "name,owner\\n{{#each items}}{{this.name}},{{this.data.owner}}\\n{{/each}}",
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
            pool,
            admin_token,
            REPORTS_ENDPOINT,
            body,
            vec![(header::ACCEPT, "text/csv".to_string())],
        )
        .await;

        let resp = assert_response_status(resp, StatusCode::OK).await;
        let headers = resp.headers().clone();
        let body = test::read_body(resp).await;
        let rendered = String::from_utf8(body.to_vec()).unwrap();

        assert!(
            headers
                .get(header::CONTENT_TYPE)
                .unwrap()
                .to_str()
                .unwrap()
                .starts_with("text/csv")
        );
        assert_eq!(
            rendered,
            "name,owner\\nreport-app-01,alice\\nreport-db-01,bob\\n"
        );

        cleanup(&classes).await;
    }

    #[rstest]
    #[actix_web::test]
    async fn test_run_report_respects_accept_q_values(#[future(awt)] test_context: TestContext) {
        let context = test_context;
        let pool = &context.pool;
        let admin_token = &context.admin_token;
        let classes = create_test_classes(&context, "report_q_values").await;
        let class = classes[0].clone();
        let _created_objects = create_report_objects(pool, &class).await;

        // Accept header with q-values preferring text/plain over application/json
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
            include: None,
        };

        let resp = post_request_with_headers(
            pool,
            admin_token,
            REPORTS_ENDPOINT,
            &body,
            vec![(
                header::ACCEPT,
                "application/json;q=0.5, text/plain;q=1.0".to_string(),
            )],
        )
        .await;

        assert_response_status(resp, StatusCode::BAD_REQUEST).await;

        cleanup(&classes).await;
    }

    #[rstest]
    #[actix_web::test]
    async fn test_run_report_excludes_q_zero(#[future(awt)] test_context: TestContext) {
        let context = test_context;
        let pool = &context.pool;
        let admin_token = &context.admin_token;
        let classes = create_test_classes(&context, "report_q_zero_exclude").await;
        let class = classes[0].clone();
        let _created_objects = create_report_objects(pool, &class).await;

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
            include: None,
        };

        // Accept header explicitly excluding application/json with q=0
        let resp = post_request_with_headers(
            pool,
            admin_token,
            REPORTS_ENDPOINT,
            &body,
            vec![(
                header::ACCEPT,
                "application/json;q=0, text/plain;q=0".to_string(),
            )],
        )
        .await;

        assert_response_status(resp, StatusCode::NOT_ACCEPTABLE).await;

        cleanup(&classes).await;
    }

    #[rstest]
    #[actix_web::test]
    async fn test_run_report_wildcard_with_q_values(#[future(awt)] test_context: TestContext) {
        let context = test_context;
        let pool = &context.pool;
        let admin_token = &context.admin_token;
        let classes = create_test_classes(&context, "report_wildcard_qvalue").await;
        let class = classes[0].clone();
        let _created_objects = create_report_objects(pool, &class).await;

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
            include: None,
        };

        // Accept: application/*;q=0.9, text/*;q=0.5 should pick application/json
        let resp = post_request_with_headers(
            pool,
            admin_token,
            REPORTS_ENDPOINT,
            &body,
            vec![(
                header::ACCEPT,
                "application/*;q=0.9, text/*;q=0.5".to_string(),
            )],
        )
        .await;

        let resp = assert_response_status(resp, StatusCode::OK).await;
        let content_type = resp.headers().get(header::CONTENT_TYPE).unwrap();
        assert!(
            content_type
                .to_str()
                .unwrap()
                .starts_with("application/json")
        );

        cleanup(&classes).await;
    }

    #[rstest]
    #[actix_web::test]
    async fn test_run_report_prefers_higher_q_value(#[future(awt)] test_context: TestContext) {
        let context = test_context;
        let pool = &context.pool;
        let admin_token = &context.admin_token;
        let classes = create_test_classes(&context, "report_prefer_q").await;
        let class = classes[0].clone();
        let _created_objects = create_report_objects(pool, &class).await;

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
            include: None,
        };

        // Should pick application/json with higher q-value
        let resp = post_request_with_headers(
            pool,
            admin_token,
            REPORTS_ENDPOINT,
            &body,
            vec![(
                header::ACCEPT,
                "text/html;q=0.3, application/json;q=0.9, text/plain;q=0.5".to_string(),
            )],
        )
        .await;

        let resp = assert_response_status(resp, StatusCode::OK).await;
        let report: ReportJsonResponse = test::read_body_json(resp).await;
        assert_eq!(report.items.len(), 2);

        cleanup(&classes).await;
    }
}
