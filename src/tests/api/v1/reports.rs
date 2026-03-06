#[cfg(test)]
mod tests {
    use actix_web::{
        App,
        http::{StatusCode, header},
        test,
    };

    use crate::api as prod_api;
    use crate::middlewares::tracing::TracingMiddleware;
    use crate::models::{
        HubuumClass, NewHubuumObject, NewReportTemplate, ReportContentType, ReportJsonResponse,
        ReportRequest, ReportScope, ReportScopeKind,
    };
    use crate::tests::api::v1::classes::tests::{cleanup, create_test_classes};
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
        let created_objects = create_report_objects(&pool, &class).await;

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
        };

        let app = test::init_service(
            App::new()
                .wrap(TracingMiddleware)
                .app_data(context.pool.clone())
                .configure(prod_api::config),
        )
        .await;

        let resp = test::TestRequest::post()
            .insert_header((header::AUTHORIZATION, format!("Bearer {admin_token}")))
            .insert_header((header::ACCEPT, "application/json"))
            .uri(REPORTS_ENDPOINT)
            .set_json(&body)
            .send_request(&app)
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
    async fn test_run_report_renders_text_template_from_stored_template(
        #[future(awt)] test_context: TestContext,
    ) {
        let context = test_context;
        let pool = &context.pool;
        let admin_token = &context.admin_token;
        let classes = create_test_classes(&context, "report_text").await;
        let class = classes[0].clone();
        let _created_objects = create_report_objects(&pool, &class).await;
        let template_id = create_template(
            &pool,
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

        let app = test::init_service(
            App::new()
                .wrap(TracingMiddleware)
                .app_data(context.pool.clone())
                .configure(prod_api::config),
        )
        .await;

        let resp = test::TestRequest::post()
            .insert_header((header::AUTHORIZATION, format!("Bearer {admin_token}")))
            .uri(REPORTS_ENDPOINT)
            .set_json(&body)
            .send_request(&app)
            .await;

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
            &pool,
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

        let app = test::init_service(
            App::new()
                .wrap(TracingMiddleware)
                .app_data(context.pool.clone())
                .configure(prod_api::config),
        )
        .await;

        let resp = test::TestRequest::post()
            .insert_header((header::AUTHORIZATION, format!("Bearer {admin_token}")))
            .uri(REPORTS_ENDPOINT)
            .set_json(&body)
            .send_request(&app)
            .await;

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

        let app = test::init_service(
            App::new()
                .wrap(TracingMiddleware)
                .app_data(context.pool.clone())
                .configure(prod_api::config),
        )
        .await;

        let resp = test::TestRequest::post()
            .insert_header((header::AUTHORIZATION, format!("Bearer {admin_token}")))
            .insert_header((header::ACCEPT, "text/plain"))
            .uri(REPORTS_ENDPOINT)
            .set_json(&body)
            .send_request(&app)
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
            &pool,
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

        let app = test::init_service(
            App::new()
                .wrap(TracingMiddleware)
                .app_data(context.pool.clone())
                .configure(prod_api::config),
        )
        .await;

        let resp = test::TestRequest::post()
            .insert_header((header::AUTHORIZATION, format!("Bearer {admin_token}")))
            .insert_header((header::ACCEPT, "text/html"))
            .uri(REPORTS_ENDPOINT)
            .set_json(&body)
            .send_request(&app)
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
            &pool,
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

        let app = test::init_service(
            App::new()
                .wrap(TracingMiddleware)
                .app_data(context.pool.clone())
                .configure(prod_api::config),
        )
        .await;

        let resp = test::TestRequest::post()
            .insert_header((header::AUTHORIZATION, format!("Bearer {normal_token}")))
            .uri(REPORTS_ENDPOINT)
            .set_json(&body)
            .send_request(&app)
            .await;

        assert_response_status(resp, StatusCode::FORBIDDEN).await;
        cleanup(&classes).await;
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

        let app = test::init_service(
            App::new()
                .wrap(TracingMiddleware)
                .app_data(context.pool.clone())
                .configure(prod_api::config),
        )
        .await;

        let resp = test::TestRequest::post()
            .insert_header((header::AUTHORIZATION, format!("Bearer {admin_token}")))
            .uri(REPORTS_ENDPOINT)
            .set_json(&body)
            .send_request(&app)
            .await;

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
            &pool,
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

        let app = test::init_service(
            App::new()
                .wrap(TracingMiddleware)
                .app_data(context.pool.clone())
                .configure(prod_api::config),
        )
        .await;

        let resp = test::TestRequest::post()
            .insert_header((header::AUTHORIZATION, format!("Bearer {admin_token}")))
            .insert_header((header::ACCEPT, "application/json"))
            .uri(REPORTS_ENDPOINT)
            .set_json(&body)
            .send_request(&app)
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
        let _created_objects = create_report_objects(&pool, &class).await;

        let template_id = create_template(
            &pool,
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

        let app = test::init_service(
            App::new()
                .wrap(TracingMiddleware)
                .app_data(context.pool.clone())
                .configure(prod_api::config),
        )
        .await;

        let resp = test::TestRequest::post()
            .insert_header((header::AUTHORIZATION, format!("Bearer {admin_token}")))
            .insert_header((header::ACCEPT, "text/html"))
            .uri(REPORTS_ENDPOINT)
            .set_json(&body)
            .send_request(&app)
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
        let _created_objects = create_report_objects(&pool, &class).await;

        let template_id = create_template(
            &pool,
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

        let app = test::init_service(
            App::new()
                .wrap(TracingMiddleware)
                .app_data(context.pool.clone())
                .configure(prod_api::config),
        )
        .await;

        let resp = test::TestRequest::post()
            .insert_header((header::AUTHORIZATION, format!("Bearer {admin_token}")))
            .insert_header((header::ACCEPT, "text/csv"))
            .uri(REPORTS_ENDPOINT)
            .set_json(&body)
            .send_request(&app)
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
}
