#[cfg(test)]
mod tests {
    use actix_web::{http::StatusCode, test};

    use crate::models::{
        Namespace, NewNamespaceWithAssignee, NewReportTemplate, Permissions, PermissionsList,
        ReportContentType, ReportTemplate, UpdateReportTemplate,
    };
    use crate::tests::api_operations::{delete_request, get_request, patch_request, post_request};
    use crate::tests::asserts::{assert_paginated_collection_total_count, assert_response_status};
    use crate::tests::{
        create_test_group, create_test_user, ensure_admin_group, setup_pool_and_tokens,
    };
    use crate::traits::{CanDelete, CanSave, PermissionController};

    const TEMPLATES_ENDPOINT: &str = "/api/v1/templates";

    async fn create_namespace(pool: &crate::db::DbPool, suffix: &str) -> Namespace {
        let admin_group = ensure_admin_group(pool).await;

        NewNamespaceWithAssignee {
            name: format!("template_ns_{suffix}"),
            description: "template test namespace".to_string(),
            group_id: admin_group.id,
        }
        .save(pool)
        .await
        .unwrap()
    }

    fn new_template_payload(namespace_id: i32, name: &str) -> NewReportTemplate {
        NewReportTemplate {
            namespace_id,
            name: name.to_string(),
            description: "template description".to_string(),
            content_type: ReportContentType::TextPlain,
            template: "{% for item in items %}{{ item.name }}\n{% endfor %}".to_string(),
        }
    }

    fn new_template_payload_with_content_type(
        namespace_id: i32,
        name: &str,
        content_type: ReportContentType,
    ) -> NewReportTemplate {
        NewReportTemplate {
            namespace_id,
            name: name.to_string(),
            description: "template description".to_string(),
            content_type,
            template: "{% for item in items %}{{ item.name }}\n{% endfor %}".to_string(),
        }
    }

    #[actix_web::test]
    async fn test_template_crud_admin() {
        let (pool, admin_token, _) = setup_pool_and_tokens().await;
        let namespace = create_namespace(&pool, "crud").await;

        let create_payload = new_template_payload(namespace.id, "tmpl-crud");
        let resp = post_request(&pool, &admin_token, TEMPLATES_ENDPOINT, &create_payload).await;
        let resp = assert_response_status(resp, StatusCode::CREATED).await;
        let created: ReportTemplate = test::read_body_json(resp).await;

        assert_eq!(created.namespace_id, namespace.id);
        assert_eq!(created.name, "tmpl-crud");
        assert_eq!(created.content_type, ReportContentType::TextPlain);

        let resp = get_request(
            &pool,
            &admin_token,
            &format!("{TEMPLATES_ENDPOINT}/{}", created.id),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let fetched: ReportTemplate = test::read_body_json(resp).await;
        assert_eq!(fetched.id, created.id);

        let patch_payload = UpdateReportTemplate {
            namespace_id: None,
            name: Some("tmpl-crud-v2".to_string()),
            description: Some("updated".to_string()),
            template: Some(
                "{% for item in items %}{{ item.name }}={{ item.id }}\n{% endfor %}".to_string(),
            ),
        };
        let resp = patch_request(
            &pool,
            &admin_token,
            &format!("{TEMPLATES_ENDPOINT}/{}", created.id),
            &patch_payload,
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let patched: ReportTemplate = test::read_body_json(resp).await;
        assert_eq!(patched.name, "tmpl-crud-v2");

        let resp = get_request(&pool, &admin_token, TEMPLATES_ENDPOINT).await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let listed: Vec<ReportTemplate> = test::read_body_json(resp).await;
        assert!(listed.iter().any(|template| template.id == created.id));

        let resp = delete_request(
            &pool,
            &admin_token,
            &format!("{TEMPLATES_ENDPOINT}/{}", created.id),
        )
        .await;
        assert_response_status(resp, StatusCode::NO_CONTENT).await;

        let resp = get_request(
            &pool,
            &admin_token,
            &format!("{TEMPLATES_ENDPOINT}/{}", created.id),
        )
        .await;
        assert_response_status(resp, StatusCode::NOT_FOUND).await;

        namespace.delete(&pool).await.unwrap();
    }

    #[actix_web::test]
    async fn test_template_list_total_count_matches_paginated_results() {
        let (pool, admin_token, _) = setup_pool_and_tokens().await;
        let namespace = create_namespace(&pool, "pagination").await;

        let expected_ids = vec![
            post_request(
                &pool,
                &admin_token,
                TEMPLATES_ENDPOINT,
                &new_template_payload(namespace.id, "tmpl-page-a"),
            )
            .await,
            post_request(
                &pool,
                &admin_token,
                TEMPLATES_ENDPOINT,
                &new_template_payload(namespace.id, "tmpl-page-b"),
            )
            .await,
            post_request(
                &pool,
                &admin_token,
                TEMPLATES_ENDPOINT,
                &new_template_payload(namespace.id, "tmpl-page-c"),
            )
            .await,
        ];

        let mut created_ids = Vec::new();
        for resp in expected_ids {
            let resp = assert_response_status(resp, StatusCode::CREATED).await;
            let created: ReportTemplate = test::read_body_json(resp).await;
            created_ids.push(created.id);
        }

        let (templates, total_count): (Vec<ReportTemplate>, i64) =
            assert_paginated_collection_total_count(
                &pool,
                &admin_token,
                10,
                |cursor| match cursor {
                    Some(cursor) => format!(
                        "{TEMPLATES_ENDPOINT}?namespace_id={}&sort=id&limit=2&cursor={cursor}",
                        namespace.id
                    ),
                    None => format!(
                        "{TEMPLATES_ENDPOINT}?namespace_id={}&sort=id&limit=2",
                        namespace.id
                    ),
                },
            )
            .await;

        assert_eq!(total_count, created_ids.len() as i64);
        assert_eq!(
            templates
                .iter()
                .map(|template| template.id)
                .collect::<Vec<_>>(),
            created_ids
        );

        namespace.delete(&pool).await.unwrap();
    }

    #[actix_web::test]
    async fn test_template_create_requires_permission() {
        let (pool, _admin_token, normal_token) = setup_pool_and_tokens().await;
        let namespace = create_namespace(&pool, "forbidden_create").await;

        let create_payload = new_template_payload(namespace.id, "tmpl-forbidden");
        let resp = post_request(&pool, &normal_token, TEMPLATES_ENDPOINT, &create_payload).await;
        assert_response_status(resp, StatusCode::FORBIDDEN).await;

        namespace.delete(&pool).await.unwrap();
    }

    #[actix_web::test]
    async fn test_template_create_rejects_legacy_handlebars_syntax() {
        let (pool, admin_token, _) = setup_pool_and_tokens().await;
        let namespace = create_namespace(&pool, "legacy_syntax").await;

        let payload = NewReportTemplate {
            namespace_id: namespace.id,
            name: "tmpl-legacy".to_string(),
            description: "legacy syntax".to_string(),
            content_type: ReportContentType::TextPlain,
            template: "{{#each items}}{{this.name}}\\n{{/each}}".to_string(),
        };

        let resp = post_request(&pool, &admin_token, TEMPLATES_ENDPOINT, &payload).await;
        assert_response_status(resp, StatusCode::BAD_REQUEST).await;

        namespace.delete(&pool).await.unwrap();
    }

    #[actix_web::test]
    async fn test_template_create_accepts_same_namespace_composition() {
        let (pool, admin_token, _) = setup_pool_and_tokens().await;
        let namespace = create_namespace(&pool, "same_namespace_composition").await;

        let layout = NewReportTemplate {
            namespace_id: namespace.id,
            name: "layout.html".to_string(),
            description: "layout".to_string(),
            content_type: ReportContentType::TextHtml,
            template: "<ul>{% block body %}{% endblock %}</ul>".to_string(),
        };
        let resp = post_request(&pool, &admin_token, TEMPLATES_ENDPOINT, &layout).await;
        assert_response_status(resp, StatusCode::CREATED).await;

        let child = NewReportTemplate {
            namespace_id: namespace.id,
            name: "child.html".to_string(),
            description: "child".to_string(),
            content_type: ReportContentType::TextHtml,
            template:
                "{% extends \"layout.html\" %}{% block body %}<li>{{ items[0].name }}</li>{% endblock %}"
                    .to_string(),
        };
        let resp = post_request(&pool, &admin_token, TEMPLATES_ENDPOINT, &child).await;
        assert_response_status(resp, StatusCode::CREATED).await;

        namespace.delete(&pool).await.unwrap();
    }

    #[actix_web::test]
    async fn test_template_create_accepts_curated_helper_filters_and_functions() {
        let (pool, admin_token, _) = setup_pool_and_tokens().await;
        let namespace = create_namespace(&pool, "helper_validation").await;

        let payload = NewReportTemplate {
            namespace_id: namespace.id,
            name: "report.hosts".to_string(),
            description: "helper coverage".to_string(),
            content_type: ReportContentType::TextPlain,
            template: "{{ csv|csv_cell }} {{ payload|tojson }} {{ coalesce(primary, fallback, \"owner\") }} {{ values|join_nonempty(\"; \") }} {{ when|format_datetime(\"date\") }}".to_string(),
        };

        let resp = post_request(&pool, &admin_token, TEMPLATES_ENDPOINT, &payload).await;
        assert_response_status(resp, StatusCode::CREATED).await;

        namespace.delete(&pool).await.unwrap();
    }

    #[actix_web::test]
    async fn test_template_create_rejects_cross_namespace_composition() {
        let (pool, admin_token, _) = setup_pool_and_tokens().await;
        let source_namespace = create_namespace(&pool, "cross_namespace_source").await;
        let target_namespace = create_namespace(&pool, "cross_namespace_target").await;

        let layout = NewReportTemplate {
            namespace_id: source_namespace.id,
            name: "layout.html".to_string(),
            description: "layout".to_string(),
            content_type: ReportContentType::TextHtml,
            template: "<ul>{% block body %}{% endblock %}</ul>".to_string(),
        };
        let resp = post_request(&pool, &admin_token, TEMPLATES_ENDPOINT, &layout).await;
        assert_response_status(resp, StatusCode::CREATED).await;

        let child = NewReportTemplate {
            namespace_id: target_namespace.id,
            name: "child.html".to_string(),
            description: "child".to_string(),
            content_type: ReportContentType::TextHtml,
            template:
                "{% extends \"layout.html\" %}{% block body %}<li>{{ items[0].name }}</li>{% endblock %}"
                    .to_string(),
        };
        let resp = post_request(&pool, &admin_token, TEMPLATES_ENDPOINT, &child).await;
        assert_response_status(resp, StatusCode::BAD_REQUEST).await;

        source_namespace.delete(&pool).await.unwrap();
        target_namespace.delete(&pool).await.unwrap();
    }

    #[actix_web::test]
    async fn test_template_move_requires_create_on_target_namespace() {
        let (pool, admin_token, _normal_token) = setup_pool_and_tokens().await;
        let source_namespace = create_namespace(&pool, "move_src").await;
        let target_namespace = create_namespace(&pool, "move_dst").await;

        let create_payload = new_template_payload(source_namespace.id, "tmpl-move");
        let resp = post_request(&pool, &admin_token, TEMPLATES_ENDPOINT, &create_payload).await;
        let resp = assert_response_status(resp, StatusCode::CREATED).await;
        let created: ReportTemplate = test::read_body_json(resp).await;

        let test_user = create_test_user(&pool).await;
        let test_group = create_test_group(&pool).await;
        test_group.add_member(&pool, &test_user).await.unwrap();
        let user_token = test_user.create_token(&pool).await.unwrap().get_token();

        source_namespace
            .grant(
                &pool,
                test_group.id,
                PermissionsList::new([Permissions::UpdateTemplate]),
            )
            .await
            .unwrap();

        let move_payload = UpdateReportTemplate {
            namespace_id: Some(target_namespace.id),
            name: None,
            description: None,
            template: None,
        };

        let resp = patch_request(
            &pool,
            &user_token,
            &format!("{TEMPLATES_ENDPOINT}/{}", created.id),
            &move_payload,
        )
        .await;
        assert_response_status(resp, StatusCode::FORBIDDEN).await;

        target_namespace
            .grant(
                &pool,
                test_group.id,
                PermissionsList::new([Permissions::CreateTemplate]),
            )
            .await
            .unwrap();

        let resp = patch_request(
            &pool,
            &user_token,
            &format!("{TEMPLATES_ENDPOINT}/{}", created.id),
            &move_payload,
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let moved: ReportTemplate = test::read_body_json(resp).await;
        assert_eq!(moved.namespace_id, target_namespace.id);

        source_namespace.delete(&pool).await.unwrap();
        target_namespace.delete(&pool).await.unwrap();
    }

    #[actix_web::test]
    async fn test_template_move_conflict_on_target_name() {
        let (pool, admin_token, _) = setup_pool_and_tokens().await;
        let source_namespace = create_namespace(&pool, "conflict_src").await;
        let target_namespace = create_namespace(&pool, "conflict_dst").await;

        let src_payload = new_template_payload(source_namespace.id, "shared-name");
        let dst_payload = new_template_payload(target_namespace.id, "shared-name");

        let resp = post_request(&pool, &admin_token, TEMPLATES_ENDPOINT, &src_payload).await;
        let resp = assert_response_status(resp, StatusCode::CREATED).await;
        let source_template: ReportTemplate = test::read_body_json(resp).await;

        let resp = post_request(&pool, &admin_token, TEMPLATES_ENDPOINT, &dst_payload).await;
        assert_response_status(resp, StatusCode::CREATED).await;

        let move_payload = UpdateReportTemplate {
            namespace_id: Some(target_namespace.id),
            name: None,
            description: None,
            template: None,
        };

        let resp = patch_request(
            &pool,
            &admin_token,
            &format!("{TEMPLATES_ENDPOINT}/{}", source_template.id),
            &move_payload,
        )
        .await;
        assert_response_status(resp, StatusCode::CONFLICT).await;

        source_namespace.delete(&pool).await.unwrap();
        target_namespace.delete(&pool).await.unwrap();
    }

    #[actix_web::test]
    async fn test_template_create_duplicate_name_in_namespace_returns_conflict() {
        let (pool, admin_token, _) = setup_pool_and_tokens().await;
        let namespace = create_namespace(&pool, "duplicate_create").await;

        let payload = new_template_payload(namespace.id, "tmpl-duplicate");
        let resp = post_request(&pool, &admin_token, TEMPLATES_ENDPOINT, &payload).await;
        assert_response_status(resp, StatusCode::CREATED).await;

        let resp = post_request(&pool, &admin_token, TEMPLATES_ENDPOINT, &payload).await;
        assert_response_status(resp, StatusCode::CONFLICT).await;

        namespace.delete(&pool).await.unwrap();
    }

    #[actix_web::test]
    async fn test_template_rename_conflict_in_same_namespace_returns_conflict() {
        let (pool, admin_token, _) = setup_pool_and_tokens().await;
        let namespace = create_namespace(&pool, "rename_conflict").await;

        let payload_a = new_template_payload(namespace.id, "tmpl-rename-a");
        let payload_b = new_template_payload(namespace.id, "tmpl-rename-b");

        let resp = post_request(&pool, &admin_token, TEMPLATES_ENDPOINT, &payload_a).await;
        let resp = assert_response_status(resp, StatusCode::CREATED).await;
        let created_a: ReportTemplate = test::read_body_json(resp).await;

        let resp = post_request(&pool, &admin_token, TEMPLATES_ENDPOINT, &payload_b).await;
        let resp = assert_response_status(resp, StatusCode::CREATED).await;
        let created_b: ReportTemplate = test::read_body_json(resp).await;

        let rename_payload = UpdateReportTemplate {
            namespace_id: None,
            name: Some(created_a.name),
            description: None,
            template: None,
        };

        let resp = patch_request(
            &pool,
            &admin_token,
            &format!("{TEMPLATES_ENDPOINT}/{}", created_b.id),
            &rename_payload,
        )
        .await;
        assert_response_status(resp, StatusCode::CONFLICT).await;

        namespace.delete(&pool).await.unwrap();
    }

    #[actix_web::test]
    async fn test_template_move_requires_update_on_source_namespace() {
        let (pool, admin_token, _normal_token) = setup_pool_and_tokens().await;
        let source_namespace = create_namespace(&pool, "move_missing_source_update_src").await;
        let target_namespace = create_namespace(&pool, "move_missing_source_update_dst").await;

        let create_payload = new_template_payload(source_namespace.id, "tmpl-move-no-update");
        let resp = post_request(&pool, &admin_token, TEMPLATES_ENDPOINT, &create_payload).await;
        let resp = assert_response_status(resp, StatusCode::CREATED).await;
        let created: ReportTemplate = test::read_body_json(resp).await;

        let test_user = create_test_user(&pool).await;
        let test_group = create_test_group(&pool).await;
        test_group.add_member(&pool, &test_user).await.unwrap();
        let user_token = test_user.create_token(&pool).await.unwrap().get_token();

        target_namespace
            .grant(
                &pool,
                test_group.id,
                PermissionsList::new([Permissions::CreateTemplate]),
            )
            .await
            .unwrap();

        let move_payload = UpdateReportTemplate {
            namespace_id: Some(target_namespace.id),
            name: None,
            description: None,
            template: None,
        };

        let resp = patch_request(
            &pool,
            &user_token,
            &format!("{TEMPLATES_ENDPOINT}/{}", created.id),
            &move_payload,
        )
        .await;
        assert_response_status(resp, StatusCode::FORBIDDEN).await;

        source_namespace.delete(&pool).await.unwrap();
        target_namespace.delete(&pool).await.unwrap();
        test_group.delete(&pool).await.unwrap();
        test_user.delete(&pool).await.unwrap();
    }

    #[actix_web::test]
    async fn test_template_list_filters_by_read_template_permission() {
        let (pool, admin_token, _normal_token) = setup_pool_and_tokens().await;
        let visible_namespace = create_namespace(&pool, "list_visible").await;
        let hidden_namespace = create_namespace(&pool, "list_hidden").await;

        let visible_payload = new_template_payload(visible_namespace.id, "tmpl-visible");
        let hidden_payload = new_template_payload(hidden_namespace.id, "tmpl-hidden");

        let resp = post_request(&pool, &admin_token, TEMPLATES_ENDPOINT, &visible_payload).await;
        let resp = assert_response_status(resp, StatusCode::CREATED).await;
        let visible_template: ReportTemplate = test::read_body_json(resp).await;

        let resp = post_request(&pool, &admin_token, TEMPLATES_ENDPOINT, &hidden_payload).await;
        let resp = assert_response_status(resp, StatusCode::CREATED).await;
        let hidden_template: ReportTemplate = test::read_body_json(resp).await;

        let test_user = create_test_user(&pool).await;
        let test_group = create_test_group(&pool).await;
        test_group.add_member(&pool, &test_user).await.unwrap();
        let user_token = test_user.create_token(&pool).await.unwrap().get_token();

        visible_namespace
            .grant(
                &pool,
                test_group.id,
                PermissionsList::new([Permissions::ReadTemplate]),
            )
            .await
            .unwrap();

        let resp = get_request(&pool, &user_token, TEMPLATES_ENDPOINT).await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let listed: Vec<ReportTemplate> = test::read_body_json(resp).await;

        assert!(
            listed
                .iter()
                .any(|template| template.id == visible_template.id)
        );
        assert!(
            !listed
                .iter()
                .any(|template| template.id == hidden_template.id)
        );

        visible_namespace.delete(&pool).await.unwrap();
        hidden_namespace.delete(&pool).await.unwrap();
        test_group.delete(&pool).await.unwrap();
        test_user.delete(&pool).await.unwrap();
    }

    #[actix_web::test]
    async fn test_template_list_keeps_admin_visibility_without_template_permission_rows() {
        let (pool, admin_token, _) = setup_pool_and_tokens().await;
        let namespace = create_namespace(&pool, "admin_list_visibility").await;
        let admin_group = ensure_admin_group(&pool).await;

        let payload = new_template_payload(namespace.id, "tmpl-admin-visible");
        let resp = post_request(&pool, &admin_token, TEMPLATES_ENDPOINT, &payload).await;
        let resp = assert_response_status(resp, StatusCode::CREATED).await;
        let created: ReportTemplate = test::read_body_json(resp).await;

        namespace
            .revoke(
                &pool,
                admin_group.id,
                PermissionsList::new([
                    Permissions::ReadTemplate,
                    Permissions::CreateTemplate,
                    Permissions::UpdateTemplate,
                    Permissions::DeleteTemplate,
                ]),
            )
            .await
            .unwrap();

        let resp = get_request(
            &pool,
            &admin_token,
            &format!("{TEMPLATES_ENDPOINT}/{}", created.id),
        )
        .await;
        assert_response_status(resp, StatusCode::OK).await;

        let resp = get_request(&pool, &admin_token, TEMPLATES_ENDPOINT).await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let listed: Vec<ReportTemplate> = test::read_body_json(resp).await;

        assert!(listed.iter().any(|template| template.id == created.id));

        namespace.delete(&pool).await.unwrap();
    }

    #[actix_web::test]
    async fn test_template_get_and_delete_require_permissions() {
        let (pool, admin_token, normal_token) = setup_pool_and_tokens().await;
        let namespace = create_namespace(&pool, "get_delete_forbidden").await;

        let payload = new_template_payload(namespace.id, "tmpl-get-delete-forbidden");
        let resp = post_request(&pool, &admin_token, TEMPLATES_ENDPOINT, &payload).await;
        let resp = assert_response_status(resp, StatusCode::CREATED).await;
        let created: ReportTemplate = test::read_body_json(resp).await;

        let resp = get_request(
            &pool,
            &normal_token,
            &format!("{TEMPLATES_ENDPOINT}/{}", created.id),
        )
        .await;
        assert_response_status(resp, StatusCode::FORBIDDEN).await;

        let resp = delete_request(
            &pool,
            &normal_token,
            &format!("{TEMPLATES_ENDPOINT}/{}", created.id),
        )
        .await;
        assert_response_status(resp, StatusCode::FORBIDDEN).await;

        namespace.delete(&pool).await.unwrap();
    }

    #[actix_web::test]
    async fn test_template_create_rejects_invalid_content_type() {
        let (pool, admin_token, _) = setup_pool_and_tokens().await;
        let namespace = create_namespace(&pool, "invalid_content_type").await;

        let payload = new_template_payload_with_content_type(
            namespace.id,
            "tmpl-invalid-content-type",
            ReportContentType::ApplicationJson,
        );
        let resp = post_request(&pool, &admin_token, TEMPLATES_ENDPOINT, &payload).await;
        assert_response_status(resp, StatusCode::BAD_REQUEST).await;

        namespace.delete(&pool).await.unwrap();
    }

    #[actix_web::test]
    async fn test_template_update_content_requires_update_permission() {
        let (pool, admin_token, _normal_token) = setup_pool_and_tokens().await;
        let namespace = create_namespace(&pool, "update_content_forbidden").await;

        let create_payload = new_template_payload(namespace.id, "tmpl-update-test");
        let resp = post_request(&pool, &admin_token, TEMPLATES_ENDPOINT, &create_payload).await;
        let resp = assert_response_status(resp, StatusCode::CREATED).await;
        let created: ReportTemplate = test::read_body_json(resp).await;

        let test_user = create_test_user(&pool).await;
        let test_group = create_test_group(&pool).await;
        test_group.add_member(&pool, &test_user).await.unwrap();
        let user_token = test_user.create_token(&pool).await.unwrap().get_token();

        // Grant only ReadTemplate, not UpdateTemplate
        namespace
            .grant(
                &pool,
                test_group.id,
                PermissionsList::new([Permissions::ReadTemplate]),
            )
            .await
            .unwrap();

        let update_payload = UpdateReportTemplate {
            namespace_id: None,
            name: None,
            description: Some("updated description".to_string()),
            template: None,
        };

        let resp = patch_request(
            &pool,
            &user_token,
            &format!("{TEMPLATES_ENDPOINT}/{}", created.id),
            &update_payload,
        )
        .await;
        assert_response_status(resp, StatusCode::FORBIDDEN).await;

        // Now grant UpdateTemplate and verify it works
        namespace
            .grant(
                &pool,
                test_group.id,
                PermissionsList::new([Permissions::UpdateTemplate]),
            )
            .await
            .unwrap();

        let resp = patch_request(
            &pool,
            &user_token,
            &format!("{TEMPLATES_ENDPOINT}/{}", created.id),
            &update_payload,
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let updated: ReportTemplate = test::read_body_json(resp).await;
        assert_eq!(updated.description, "updated description");

        namespace.delete(&pool).await.unwrap();
        test_group.delete(&pool).await.unwrap();
        test_user.delete(&pool).await.unwrap();
    }

    #[actix_web::test]
    async fn test_template_delete_requires_delete_permission() {
        let (pool, admin_token, _normal_token) = setup_pool_and_tokens().await;
        let namespace = create_namespace(&pool, "delete_forbidden").await;

        let create_payload = new_template_payload(namespace.id, "tmpl-delete-test");
        let resp = post_request(&pool, &admin_token, TEMPLATES_ENDPOINT, &create_payload).await;
        let resp = assert_response_status(resp, StatusCode::CREATED).await;
        let created: ReportTemplate = test::read_body_json(resp).await;

        let test_user = create_test_user(&pool).await;
        let test_group = create_test_group(&pool).await;
        test_group.add_member(&pool, &test_user).await.unwrap();
        let user_token = test_user.create_token(&pool).await.unwrap().get_token();

        // Grant only ReadTemplate and UpdateTemplate, not DeleteTemplate
        namespace
            .grant(
                &pool,
                test_group.id,
                PermissionsList::new([Permissions::ReadTemplate, Permissions::UpdateTemplate]),
            )
            .await
            .unwrap();

        let resp = delete_request(
            &pool,
            &user_token,
            &format!("{TEMPLATES_ENDPOINT}/{}", created.id),
        )
        .await;
        assert_response_status(resp, StatusCode::FORBIDDEN).await;

        // Now grant DeleteTemplate and verify it works
        namespace
            .grant(
                &pool,
                test_group.id,
                PermissionsList::new([Permissions::DeleteTemplate]),
            )
            .await
            .unwrap();

        let resp = delete_request(
            &pool,
            &user_token,
            &format!("{TEMPLATES_ENDPOINT}/{}", created.id),
        )
        .await;
        assert_response_status(resp, StatusCode::NO_CONTENT).await;

        namespace.delete(&pool).await.unwrap();
        test_group.delete(&pool).await.unwrap();
        test_user.delete(&pool).await.unwrap();
    }
}
