#[cfg(test)]
mod tests {
    use actix_web::{
        http::{header, StatusCode},
        test,
        web::Data,
        App,
    };

    use crate::api as prod_api;
    use crate::middlewares::tracing::TracingMiddleware;
    use crate::models::{
        HubuumClass, NewHubuumObject, ReportJsonResponse, ReportRequest, ReportScope,
        ReportScopeKind,
    };
    use crate::tests::api::v1::classes::tests::{cleanup, create_test_classes};
    use crate::tests::asserts::assert_response_status;
    use crate::tests::setup_pool_and_tokens;
    use crate::traits::CanSave;

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

    #[actix_web::test]
    async fn test_run_report_returns_json_envelope() {
        let (pool, admin_token, _) = setup_pool_and_tokens().await;
        let classes = create_test_classes("report_json").await;
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
                .app_data(Data::new(pool.as_ref().clone()))
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

    #[actix_web::test]
    async fn test_run_report_renders_text_template() {
        let (pool, admin_token, _) = setup_pool_and_tokens().await;
        let classes = create_test_classes("report_text").await;
        let class = classes[0].clone();
        let _created_objects = create_report_objects(&pool, &class).await;

        let body = serde_json::json!({
            "scope": {
                "kind": "objects_in_class",
                "class_id": class.id
            },
            "query": "name__contains=report-&sort=name",
            "output": {
                "content_type": "text/plain",
                "template": "{{#each items}}{{this.name}}={{this.data.owner}}\n{{/each}}"
            }
        });

        let app = test::init_service(
            App::new()
                .wrap(TracingMiddleware)
                .app_data(Data::new(pool.as_ref().clone()))
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
        assert_eq!(rendered, "report-app-01=alice\nreport-db-01=bob\n");

        cleanup(&classes).await;
    }

    #[actix_web::test]
    async fn test_run_report_rejects_unsupported_accept_header() {
        let (pool, admin_token, _) = setup_pool_and_tokens().await;

        let body = serde_json::json!({
            "scope": {
                "kind": "classes"
            }
        });

        let app = test::init_service(
            App::new()
                .wrap(TracingMiddleware)
                .app_data(Data::new(pool.as_ref().clone()))
                .configure(prod_api::config),
        )
        .await;

        let resp = test::TestRequest::post()
            .insert_header((header::AUTHORIZATION, format!("Bearer {admin_token}")))
            .insert_header((header::ACCEPT, "application/xml"))
            .uri(REPORTS_ENDPOINT)
            .set_json(&body)
            .send_request(&app)
            .await;

        assert_response_status(resp, StatusCode::NOT_ACCEPTABLE).await;
    }

    #[actix_web::test]
    async fn test_run_report_returns_payload_too_large_for_small_limit() {
        let (pool, admin_token, _) = setup_pool_and_tokens().await;
        let classes = create_test_classes("report_too_large").await;
        let class = classes[0].clone();
        let _created_objects = create_report_objects(&pool, &class).await;

        let body = serde_json::json!({
            "scope": {
                "kind": "objects_in_class",
                "class_id": class.id
            },
            "query": "name__contains=report-&sort=name",
            "limits": {
                "max_output_bytes": 32
            }
        });

        let app = test::init_service(
            App::new()
                .wrap(TracingMiddleware)
                .app_data(Data::new(pool.as_ref().clone()))
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

        assert_response_status(resp, StatusCode::PAYLOAD_TOO_LARGE).await;
        cleanup(&classes).await;
    }
}
