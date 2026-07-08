#[cfg(test)]
mod tests {
    use actix_web::{http::StatusCode, test};

    use crate::models::{
        Collection, ExportContentType, ExportLimits, ExportMissingDataPolicy, ExportScopeKind,
        ExportTemplate, ExportTemplateKind, NewCollectionWithAssignee, NewExportTemplate,
        NewHubuumClass, Permissions, PermissionsList, UpdateExportTemplate,
    };
    use crate::tests::api_operations::{delete_request, get_request, patch_request, post_request};
    use crate::tests::asserts::{assert_paginated_collection_total_count, assert_response_status};
    use crate::tests::{
        create_test_group, create_test_user, ensure_admin_group, setup_pool_and_tokens,
    };
    use crate::traits::{CanDelete, CanSave, PermissionController};

    const TEMPLATES_ENDPOINT: &str = "/api/v1/export-templates";

    async fn create_collection(pool: &crate::db::DbPool, suffix: &str) -> Collection {
        let admin_group = ensure_admin_group(pool).await;

        NewCollectionWithAssignee {
            name: format!("template_collection_{suffix}"),
            description: "template test collection".to_string(),
            group_id: admin_group.id,
            parent_collection_id: None,
        }
        .save_without_events(pool)
        .await
        .unwrap()
    }

    fn new_template_payload(collection_id: i32, name: &str) -> NewExportTemplate {
        NewExportTemplate {
            collection_id,
            name: name.to_string(),
            description: "template description".to_string(),
            content_type: ExportContentType::TextPlain,
            template: "{% for item in items %}{{ item.name }}\n{% endfor %}".to_string(),
            kind: ExportTemplateKind::Fragment,
            scope_kind: None,
            class_id: None,
            default_query: None,
            include: None,
            relation_context: None,
            default_missing_data_policy: None,
            default_limits: None,
        }
    }

    fn new_template_payload_with_content_type(
        collection_id: i32,
        name: &str,
        content_type: ExportContentType,
    ) -> NewExportTemplate {
        NewExportTemplate {
            collection_id,
            name: name.to_string(),
            description: "template description".to_string(),
            content_type,
            template: "{% for item in items %}{{ item.name }}\n{% endfor %}".to_string(),
            kind: ExportTemplateKind::Fragment,
            scope_kind: None,
            class_id: None,
            default_query: None,
            include: None,
            relation_context: None,
            default_missing_data_policy: None,
            default_limits: None,
        }
    }

    fn empty_update_template_payload() -> UpdateExportTemplate {
        UpdateExportTemplate {
            collection_id: None,
            name: None,
            description: None,
            template: None,
            kind: None,
            scope_kind: None,
            class_id: None,
            default_query: None,
            include: None,
            relation_context: None,
            default_missing_data_policy: None,
            default_limits: None,
        }
    }

    #[actix_web::test]
    async fn test_template_crud_admin() {
        let (pool, admin_token, _) = setup_pool_and_tokens().await;
        let collection = create_collection(&pool, "crud").await;

        let create_payload = new_template_payload(collection.id, "tmpl-crud");
        let resp = post_request(&pool, &admin_token, TEMPLATES_ENDPOINT, &create_payload).await;
        let resp = assert_response_status(resp, StatusCode::CREATED).await;
        let created: ExportTemplate = test::read_body_json(resp).await;

        assert_eq!(created.collection_id, collection.id);
        assert_eq!(created.name, "tmpl-crud");
        assert_eq!(created.content_type, ExportContentType::TextPlain);

        let resp = get_request(
            &pool,
            &admin_token,
            &format!("{TEMPLATES_ENDPOINT}/{}", created.id),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let fetched: ExportTemplate = test::read_body_json(resp).await;
        assert_eq!(fetched.id, created.id);

        let patch_payload = UpdateExportTemplate {
            collection_id: None,
            name: Some("tmpl-crud-v2".to_string()),
            description: Some("updated".to_string()),
            template: Some(
                "{% for item in items %}{{ item.name }}={{ item.id }}\n{% endfor %}".to_string(),
            ),
            ..empty_update_template_payload()
        };
        let resp = patch_request(
            &pool,
            &admin_token,
            &format!("{TEMPLATES_ENDPOINT}/{}", created.id),
            &patch_payload,
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let patched: ExportTemplate = test::read_body_json(resp).await;
        assert_eq!(patched.name, "tmpl-crud-v2");

        let resp = get_request(&pool, &admin_token, TEMPLATES_ENDPOINT).await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let listed: Vec<ExportTemplate> = test::read_body_json(resp).await;
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

        collection.delete_without_events(&pool).await.unwrap();
    }

    #[actix_web::test]
    async fn test_template_list_total_count_matches_paginated_results() {
        let (pool, admin_token, _) = setup_pool_and_tokens().await;
        let collection = create_collection(&pool, "pagination").await;

        let expected_ids = vec![
            post_request(
                &pool,
                &admin_token,
                TEMPLATES_ENDPOINT,
                &new_template_payload(collection.id, "tmpl-page-a"),
            )
            .await,
            post_request(
                &pool,
                &admin_token,
                TEMPLATES_ENDPOINT,
                &new_template_payload(collection.id, "tmpl-page-b"),
            )
            .await,
            post_request(
                &pool,
                &admin_token,
                TEMPLATES_ENDPOINT,
                &new_template_payload(collection.id, "tmpl-page-c"),
            )
            .await,
        ];

        let mut created_ids = Vec::new();
        for resp in expected_ids {
            let resp = assert_response_status(resp, StatusCode::CREATED).await;
            let created: ExportTemplate = test::read_body_json(resp).await;
            created_ids.push(created.id);
        }

        let (export_templates, total_count): (Vec<ExportTemplate>, i64) =
            assert_paginated_collection_total_count(
                &pool,
                &admin_token,
                10,
                |cursor| match cursor {
                    Some(cursor) => format!(
                        "{TEMPLATES_ENDPOINT}?collection_id={}&sort=id&limit=2&cursor={cursor}",
                        collection.id
                    ),
                    None => format!(
                        "{TEMPLATES_ENDPOINT}?collection_id={}&sort=id&limit=2",
                        collection.id
                    ),
                },
            )
            .await;

        assert_eq!(total_count, created_ids.len() as i64);
        assert_eq!(
            export_templates
                .iter()
                .map(|template| template.id)
                .collect::<Vec<_>>(),
            created_ids
        );

        collection.delete_without_events(&pool).await.unwrap();
    }

    #[actix_web::test]
    async fn test_template_create_requires_permission() {
        let (pool, _admin_token, normal_token) = setup_pool_and_tokens().await;
        let collection = create_collection(&pool, "forbidden_create").await;

        let create_payload = new_template_payload(collection.id, "tmpl-forbidden");
        let resp = post_request(&pool, &normal_token, TEMPLATES_ENDPOINT, &create_payload).await;
        assert_response_status(resp, StatusCode::FORBIDDEN).await;

        collection.delete_without_events(&pool).await.unwrap();
    }

    #[actix_web::test]
    async fn test_template_create_rejects_legacy_handlebars_syntax() {
        let (pool, admin_token, _) = setup_pool_and_tokens().await;
        let collection = create_collection(&pool, "legacy_syntax").await;

        let payload = NewExportTemplate {
            collection_id: collection.id,
            name: "tmpl-legacy".to_string(),
            description: "legacy syntax".to_string(),
            content_type: ExportContentType::TextPlain,
            template: "{{#each items}}{{this.name}}\\n{{/each}}".to_string(),
            kind: ExportTemplateKind::Fragment,
            scope_kind: None,
            class_id: None,
            default_query: None,
            include: None,
            relation_context: None,
            default_missing_data_policy: None,
            default_limits: None,
        };

        let resp = post_request(&pool, &admin_token, TEMPLATES_ENDPOINT, &payload).await;
        assert_response_status(resp, StatusCode::BAD_REQUEST).await;

        collection.delete_without_events(&pool).await.unwrap();
    }

    #[actix_web::test]
    async fn test_template_create_accepts_same_collection_composition() {
        let (pool, admin_token, _) = setup_pool_and_tokens().await;
        let collection = create_collection(&pool, "same_collection_composition").await;

        let layout = NewExportTemplate {
            collection_id: collection.id,
            name: "layout.html".to_string(),
            description: "layout".to_string(),
            content_type: ExportContentType::TextHtml,
            template: "<ul>{% block body %}{% endblock %}</ul>".to_string(),
            kind: ExportTemplateKind::Fragment,
            scope_kind: None,
            class_id: None,
            default_query: None,
            include: None,
            relation_context: None,
            default_missing_data_policy: None,
            default_limits: None,
        };
        let resp = post_request(&pool, &admin_token, TEMPLATES_ENDPOINT, &layout).await;
        assert_response_status(resp, StatusCode::CREATED).await;

        let child = NewExportTemplate {
            collection_id: collection.id,
            name: "child.html".to_string(),
            description: "child".to_string(),
            content_type: ExportContentType::TextHtml,
            template:
                "{% extends \"layout.html\" %}{% block body %}<li>{{ items[0].name }}</li>{% endblock %}"
                    .to_string(),
            kind: ExportTemplateKind::Fragment,
            scope_kind: None,
            class_id: None,
            default_query: None,
            include: None,
            relation_context: None,
            default_missing_data_policy: None,
            default_limits: None,
        };
        let resp = post_request(&pool, &admin_token, TEMPLATES_ENDPOINT, &child).await;
        assert_response_status(resp, StatusCode::CREATED).await;

        collection.delete_without_events(&pool).await.unwrap();
    }

    #[actix_web::test]
    async fn test_template_create_accepts_curated_helper_filters_and_functions() {
        let (pool, admin_token, _) = setup_pool_and_tokens().await;
        let collection = create_collection(&pool, "helper_validation").await;

        let payload = NewExportTemplate {
            collection_id: collection.id,
            name: "export.hosts".to_string(),
            description: "helper coverage".to_string(),
            content_type: ExportContentType::TextPlain,
            template: "{{ csv|csv_cell }} {{ payload|tojson }} {{ coalesce(primary, fallback, \"owner\") }} {{ values|join_nonempty(\"; \") }} {{ when|format_datetime(\"date\") }}".to_string(),
            kind: ExportTemplateKind::Fragment,
            scope_kind: None,
            class_id: None,
            default_query: None,
            include: None,
            relation_context: None,
            default_missing_data_policy: None,
            default_limits: None,
        };

        let resp = post_request(&pool, &admin_token, TEMPLATES_ENDPOINT, &payload).await;
        assert_response_status(resp, StatusCode::CREATED).await;

        collection.delete_without_events(&pool).await.unwrap();
    }

    #[actix_web::test]
    async fn test_template_create_rejects_cross_collection_composition() {
        let (pool, admin_token, _) = setup_pool_and_tokens().await;
        let source_collection = create_collection(&pool, "cross_collection_source").await;
        let target_collection = create_collection(&pool, "cross_collection_target").await;

        let layout = NewExportTemplate {
            collection_id: source_collection.id,
            name: "layout.html".to_string(),
            description: "layout".to_string(),
            content_type: ExportContentType::TextHtml,
            template: "<ul>{% block body %}{% endblock %}</ul>".to_string(),
            kind: ExportTemplateKind::Fragment,
            scope_kind: None,
            class_id: None,
            default_query: None,
            include: None,
            relation_context: None,
            default_missing_data_policy: None,
            default_limits: None,
        };
        let resp = post_request(&pool, &admin_token, TEMPLATES_ENDPOINT, &layout).await;
        assert_response_status(resp, StatusCode::CREATED).await;

        let child = NewExportTemplate {
            collection_id: target_collection.id,
            name: "child.html".to_string(),
            description: "child".to_string(),
            content_type: ExportContentType::TextHtml,
            template:
                "{% extends \"layout.html\" %}{% block body %}<li>{{ items[0].name }}</li>{% endblock %}"
                    .to_string(),
            kind: ExportTemplateKind::Fragment,
            scope_kind: None,
            class_id: None,
            default_query: None,
            include: None,
            relation_context: None,
            default_missing_data_policy: None,
            default_limits: None,
        };
        let resp = post_request(&pool, &admin_token, TEMPLATES_ENDPOINT, &child).await;
        assert_response_status(resp, StatusCode::BAD_REQUEST).await;

        source_collection
            .delete_without_events(&pool)
            .await
            .unwrap();
        target_collection
            .delete_without_events(&pool)
            .await
            .unwrap();
    }

    #[actix_web::test]
    async fn test_template_move_requires_create_on_target_collection() {
        let (pool, admin_token, _normal_token) = setup_pool_and_tokens().await;
        let source_collection = create_collection(&pool, "move_src").await;
        let target_collection = create_collection(&pool, "move_dst").await;

        let create_payload = new_template_payload(source_collection.id, "tmpl-move");
        let resp = post_request(&pool, &admin_token, TEMPLATES_ENDPOINT, &create_payload).await;
        let resp = assert_response_status(resp, StatusCode::CREATED).await;
        let created: ExportTemplate = test::read_body_json(resp).await;

        let test_user = create_test_user(&pool).await;
        let test_group = create_test_group(&pool).await;
        test_group
            .add_member_without_events(&pool, &test_user)
            .await
            .unwrap();
        let user_token = test_user.create_token(&pool).await.unwrap().get_token();

        source_collection
            .grant_without_events(
                &pool,
                test_group.id,
                PermissionsList::new([Permissions::UpdateTemplate]),
            )
            .await
            .unwrap();

        let move_payload = UpdateExportTemplate {
            collection_id: Some(target_collection.id),
            name: None,
            description: None,
            template: None,
            ..empty_update_template_payload()
        };

        let resp = patch_request(
            &pool,
            &user_token,
            &format!("{TEMPLATES_ENDPOINT}/{}", created.id),
            &move_payload,
        )
        .await;
        assert_response_status(resp, StatusCode::FORBIDDEN).await;

        target_collection
            .grant_without_events(
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
        let moved: ExportTemplate = test::read_body_json(resp).await;
        assert_eq!(moved.collection_id, target_collection.id);

        source_collection
            .delete_without_events(&pool)
            .await
            .unwrap();
        target_collection
            .delete_without_events(&pool)
            .await
            .unwrap();
    }

    #[actix_web::test]
    async fn test_template_move_conflict_on_target_name() {
        let (pool, admin_token, _) = setup_pool_and_tokens().await;
        let source_collection = create_collection(&pool, "conflict_src").await;
        let target_collection = create_collection(&pool, "conflict_dst").await;

        let src_payload = new_template_payload(source_collection.id, "shared-name");
        let dst_payload = new_template_payload(target_collection.id, "shared-name");

        let resp = post_request(&pool, &admin_token, TEMPLATES_ENDPOINT, &src_payload).await;
        let resp = assert_response_status(resp, StatusCode::CREATED).await;
        let source_template: ExportTemplate = test::read_body_json(resp).await;

        let resp = post_request(&pool, &admin_token, TEMPLATES_ENDPOINT, &dst_payload).await;
        assert_response_status(resp, StatusCode::CREATED).await;

        let move_payload = UpdateExportTemplate {
            collection_id: Some(target_collection.id),
            name: None,
            description: None,
            template: None,
            ..empty_update_template_payload()
        };

        let resp = patch_request(
            &pool,
            &admin_token,
            &format!("{TEMPLATES_ENDPOINT}/{}", source_template.id),
            &move_payload,
        )
        .await;
        assert_response_status(resp, StatusCode::CONFLICT).await;

        source_collection
            .delete_without_events(&pool)
            .await
            .unwrap();
        target_collection
            .delete_without_events(&pool)
            .await
            .unwrap();
    }

    #[actix_web::test]
    async fn test_template_create_duplicate_name_in_collection_returns_conflict() {
        let (pool, admin_token, _) = setup_pool_and_tokens().await;
        let collection = create_collection(&pool, "duplicate_create").await;

        let payload = new_template_payload(collection.id, "tmpl-duplicate");
        let resp = post_request(&pool, &admin_token, TEMPLATES_ENDPOINT, &payload).await;
        assert_response_status(resp, StatusCode::CREATED).await;

        let resp = post_request(&pool, &admin_token, TEMPLATES_ENDPOINT, &payload).await;
        assert_response_status(resp, StatusCode::CONFLICT).await;

        collection.delete_without_events(&pool).await.unwrap();
    }

    #[actix_web::test]
    async fn test_template_rename_conflict_in_same_collection_returns_conflict() {
        let (pool, admin_token, _) = setup_pool_and_tokens().await;
        let collection = create_collection(&pool, "rename_conflict").await;

        let payload_a = new_template_payload(collection.id, "tmpl-rename-a");
        let payload_b = new_template_payload(collection.id, "tmpl-rename-b");

        let resp = post_request(&pool, &admin_token, TEMPLATES_ENDPOINT, &payload_a).await;
        let resp = assert_response_status(resp, StatusCode::CREATED).await;
        let created_a: ExportTemplate = test::read_body_json(resp).await;

        let resp = post_request(&pool, &admin_token, TEMPLATES_ENDPOINT, &payload_b).await;
        let resp = assert_response_status(resp, StatusCode::CREATED).await;
        let created_b: ExportTemplate = test::read_body_json(resp).await;

        let rename_payload = UpdateExportTemplate {
            collection_id: None,
            name: Some(created_a.name),
            description: None,
            template: None,
            ..empty_update_template_payload()
        };

        let resp = patch_request(
            &pool,
            &admin_token,
            &format!("{TEMPLATES_ENDPOINT}/{}", created_b.id),
            &rename_payload,
        )
        .await;
        assert_response_status(resp, StatusCode::CONFLICT).await;

        collection.delete_without_events(&pool).await.unwrap();
    }

    #[actix_web::test]
    async fn test_template_move_requires_update_on_source_collection() {
        let (pool, admin_token, _normal_token) = setup_pool_and_tokens().await;
        let source_collection = create_collection(&pool, "move_missing_source_update_src").await;
        let target_collection = create_collection(&pool, "move_missing_source_update_dst").await;

        let create_payload = new_template_payload(source_collection.id, "tmpl-move-no-update");
        let resp = post_request(&pool, &admin_token, TEMPLATES_ENDPOINT, &create_payload).await;
        let resp = assert_response_status(resp, StatusCode::CREATED).await;
        let created: ExportTemplate = test::read_body_json(resp).await;

        let test_user = create_test_user(&pool).await;
        let test_group = create_test_group(&pool).await;
        test_group
            .add_member_without_events(&pool, &test_user)
            .await
            .unwrap();
        let user_token = test_user.create_token(&pool).await.unwrap().get_token();

        target_collection
            .grant_without_events(
                &pool,
                test_group.id,
                PermissionsList::new([Permissions::CreateTemplate]),
            )
            .await
            .unwrap();

        let move_payload = UpdateExportTemplate {
            collection_id: Some(target_collection.id),
            name: None,
            description: None,
            template: None,
            ..empty_update_template_payload()
        };

        let resp = patch_request(
            &pool,
            &user_token,
            &format!("{TEMPLATES_ENDPOINT}/{}", created.id),
            &move_payload,
        )
        .await;
        assert_response_status(resp, StatusCode::FORBIDDEN).await;

        source_collection
            .delete_without_events(&pool)
            .await
            .unwrap();
        target_collection
            .delete_without_events(&pool)
            .await
            .unwrap();
        test_group.delete_without_events(&pool).await.unwrap();
        test_user.delete_without_events(&pool).await.unwrap();
    }

    #[actix_web::test]
    async fn test_template_list_filters_by_read_template_permission() {
        let (pool, admin_token, _normal_token) = setup_pool_and_tokens().await;
        let visible_collection = create_collection(&pool, "list_visible").await;
        let hidden_collection = create_collection(&pool, "list_hidden").await;

        let visible_payload = new_template_payload(visible_collection.id, "tmpl-visible");
        let hidden_payload = new_template_payload(hidden_collection.id, "tmpl-hidden");

        let resp = post_request(&pool, &admin_token, TEMPLATES_ENDPOINT, &visible_payload).await;
        let resp = assert_response_status(resp, StatusCode::CREATED).await;
        let visible_template: ExportTemplate = test::read_body_json(resp).await;

        let resp = post_request(&pool, &admin_token, TEMPLATES_ENDPOINT, &hidden_payload).await;
        let resp = assert_response_status(resp, StatusCode::CREATED).await;
        let hidden_template: ExportTemplate = test::read_body_json(resp).await;

        let test_user = create_test_user(&pool).await;
        let test_group = create_test_group(&pool).await;
        test_group
            .add_member_without_events(&pool, &test_user)
            .await
            .unwrap();
        let user_token = test_user.create_token(&pool).await.unwrap().get_token();

        visible_collection
            .grant_without_events(
                &pool,
                test_group.id,
                PermissionsList::new([Permissions::ReadTemplate]),
            )
            .await
            .unwrap();

        let resp = get_request(&pool, &user_token, TEMPLATES_ENDPOINT).await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let listed: Vec<ExportTemplate> = test::read_body_json(resp).await;

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

        visible_collection
            .delete_without_events(&pool)
            .await
            .unwrap();
        hidden_collection
            .delete_without_events(&pool)
            .await
            .unwrap();
        test_group.delete_without_events(&pool).await.unwrap();
        test_user.delete_without_events(&pool).await.unwrap();
    }

    #[actix_web::test]
    async fn test_template_list_keeps_admin_visibility_without_template_permission_rows() {
        let (pool, admin_token, _) = setup_pool_and_tokens().await;
        let collection = create_collection(&pool, "admin_list_visibility").await;
        let admin_group = ensure_admin_group(&pool).await;

        let payload = new_template_payload(collection.id, "tmpl-admin-visible");
        let resp = post_request(&pool, &admin_token, TEMPLATES_ENDPOINT, &payload).await;
        let resp = assert_response_status(resp, StatusCode::CREATED).await;
        let created: ExportTemplate = test::read_body_json(resp).await;

        collection
            .revoke_without_events(
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
        let listed: Vec<ExportTemplate> = test::read_body_json(resp).await;

        assert!(listed.iter().any(|template| template.id == created.id));

        collection.delete_without_events(&pool).await.unwrap();
    }

    #[actix_web::test]
    async fn test_template_get_and_delete_require_permissions() {
        let (pool, admin_token, normal_token) = setup_pool_and_tokens().await;
        let collection = create_collection(&pool, "get_delete_forbidden").await;

        let payload = new_template_payload(collection.id, "tmpl-get-delete-forbidden");
        let resp = post_request(&pool, &admin_token, TEMPLATES_ENDPOINT, &payload).await;
        let resp = assert_response_status(resp, StatusCode::CREATED).await;
        let created: ExportTemplate = test::read_body_json(resp).await;

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

        collection.delete_without_events(&pool).await.unwrap();
    }

    #[actix_web::test]
    async fn test_template_create_rejects_invalid_content_type() {
        let (pool, admin_token, _) = setup_pool_and_tokens().await;
        let collection = create_collection(&pool, "invalid_content_type").await;

        let payload = new_template_payload_with_content_type(
            collection.id,
            "tmpl-invalid-content-type",
            ExportContentType::ApplicationJson,
        );
        let resp = post_request(&pool, &admin_token, TEMPLATES_ENDPOINT, &payload).await;
        assert_response_status(resp, StatusCode::BAD_REQUEST).await;

        collection.delete_without_events(&pool).await.unwrap();
    }

    #[actix_web::test]
    async fn test_fragment_template_cannot_be_executed() {
        let (pool, admin_token, _) = setup_pool_and_tokens().await;
        let collection = create_collection(&pool, "fragment_execution").await;

        let payload = new_template_payload(collection.id, "partial.not-executable");
        let resp = post_request(&pool, &admin_token, TEMPLATES_ENDPOINT, &payload).await;
        let resp = assert_response_status(resp, StatusCode::CREATED).await;
        let created: ExportTemplate = test::read_body_json(resp).await;

        let resp = post_request(
            &pool,
            &admin_token,
            &format!("{TEMPLATES_ENDPOINT}/{}/exports", created.id),
            &serde_json::json!({}),
        )
        .await;
        assert_response_status(resp, StatusCode::BAD_REQUEST).await;

        collection.delete_without_events(&pool).await.unwrap();
    }

    #[actix_web::test]
    async fn test_export_template_rejects_class_in_another_collection() {
        let (pool, admin_token, _) = setup_pool_and_tokens().await;
        let template_collection =
            create_collection(&pool, "export_class_template_collection").await;
        let class_collection = create_collection(&pool, "export_class_target_collection").await;
        let class = NewHubuumClass {
            name: "foreign-template-class".to_string(),
            collection_id: class_collection.id,
            json_schema: None,
            validate_schema: Some(false),
            description: "foreign class".to_string(),
        }
        .save_without_events(&pool)
        .await
        .unwrap();

        let payload = NewExportTemplate {
            collection_id: template_collection.id,
            name: "export.foreign-class".to_string(),
            description: "bad export template".to_string(),
            content_type: ExportContentType::TextPlain,
            template: "{{ items|length }}".to_string(),
            kind: ExportTemplateKind::Export,
            scope_kind: Some(ExportScopeKind::ObjectsInClass),
            class_id: Some(class.id),
            default_query: None,
            include: None,
            relation_context: None,
            default_missing_data_policy: None,
            default_limits: None,
        };

        let resp = post_request(&pool, &admin_token, TEMPLATES_ENDPOINT, &payload).await;
        assert_response_status(resp, StatusCode::BAD_REQUEST).await;

        template_collection
            .delete_without_events(&pool)
            .await
            .unwrap();
        class_collection.delete_without_events(&pool).await.unwrap();
    }

    #[actix_web::test]
    async fn test_patch_export_template_class_scope_to_collection_scope() {
        let (pool, admin_token, _) = setup_pool_and_tokens().await;
        let collection = create_collection(&pool, "patch_scope_change").await;
        let class = NewHubuumClass {
            name: "patch-scope-class".to_string(),
            collection_id: collection.id,
            json_schema: None,
            validate_schema: Some(false),
            description: "class".to_string(),
        }
        .save_without_events(&pool)
        .await
        .unwrap();

        // Start as an objects_in_class export bound to a class.
        let create_payload = NewExportTemplate {
            collection_id: collection.id,
            name: "export.scope-change".to_string(),
            description: "scope change export".to_string(),
            content_type: ExportContentType::TextPlain,
            template: "{% for item in items %}{{ item.name }}{% endfor %}".to_string(),
            kind: ExportTemplateKind::Export,
            scope_kind: Some(ExportScopeKind::ObjectsInClass),
            class_id: Some(class.id),
            default_query: None,
            include: None,
            relation_context: None,
            default_missing_data_policy: None,
            default_limits: None,
        };
        let resp = post_request(&pool, &admin_token, TEMPLATES_ENDPOINT, &create_payload).await;
        let resp = assert_response_status(resp, StatusCode::CREATED).await;
        let created: ExportTemplate = test::read_body_json(resp).await;

        // PATCH to a collection scope without clearing class_id explicitly; the carried-forward
        // class_id must be dropped rather than rejected.
        let patch = UpdateExportTemplate {
            scope_kind: Some(ExportScopeKind::Collections),
            ..empty_update_template_payload()
        };
        let resp = patch_request(
            &pool,
            &admin_token,
            &format!("{TEMPLATES_ENDPOINT}/{}", created.id),
            &patch,
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let patched: ExportTemplate = test::read_body_json(resp).await;
        assert_eq!(patched.scope_kind, Some(ExportScopeKind::Collections));
        assert_eq!(patched.class_id, None);

        collection.delete_without_events(&pool).await.unwrap();
    }

    #[actix_web::test]
    async fn test_patch_export_template_clears_nullable_defaults() {
        let (pool, admin_token, _) = setup_pool_and_tokens().await;
        let collection = create_collection(&pool, "patch_clear_defaults").await;
        let class = NewHubuumClass {
            name: "patch-clear-class".to_string(),
            collection_id: collection.id,
            json_schema: None,
            validate_schema: Some(false),
            description: "class".to_string(),
        }
        .save_without_events(&pool)
        .await
        .unwrap();

        let created = NewExportTemplate {
            collection_id: collection.id,
            name: "export.clear-defaults".to_string(),
            description: "clear defaults export".to_string(),
            content_type: ExportContentType::TextPlain,
            template: "{% for item in items %}{{ item.name }}{% endfor %}".to_string(),
            kind: ExportTemplateKind::Export,
            scope_kind: Some(ExportScopeKind::ObjectsInClass),
            class_id: Some(class.id),
            default_query: Some("sort=name".to_string()),
            include: None,
            relation_context: None,
            default_missing_data_policy: Some(ExportMissingDataPolicy::Strict),
            default_limits: Some(ExportLimits {
                max_items: Some(100),
                max_output_bytes: Some(262_144),
            }),
        }
        .save_without_events(&pool)
        .await
        .unwrap();

        // An unrelated PATCH that omits the default fields must leave them untouched.
        let resp = patch_request(
            &pool,
            &admin_token,
            &format!("{TEMPLATES_ENDPOINT}/{}", created.id),
            &serde_json::json!({ "description": "still has defaults" }),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let patched: ExportTemplate = test::read_body_json(resp).await;
        assert_eq!(patched.default_query.as_deref(), Some("sort=name"));
        assert_eq!(
            patched.default_missing_data_policy,
            Some(ExportMissingDataPolicy::Strict)
        );
        assert!(patched.default_limits.is_some());

        // Explicit JSON null clears the nullable defaults while keeping the scope intact.
        let resp = patch_request(
            &pool,
            &admin_token,
            &format!("{TEMPLATES_ENDPOINT}/{}", created.id),
            &serde_json::json!({
                "default_query": null,
                "default_missing_data_policy": null,
                "default_limits": null
            }),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let patched: ExportTemplate = test::read_body_json(resp).await;
        assert_eq!(patched.default_query, None);
        assert_eq!(patched.default_missing_data_policy, None);
        assert_eq!(patched.default_limits, None);
        assert_eq!(patched.scope_kind, Some(ExportScopeKind::ObjectsInClass));
        assert_eq!(patched.class_id, Some(class.id));

        collection.delete_without_events(&pool).await.unwrap();
    }

    #[actix_web::test]
    async fn test_export_template_rejects_class_id_for_collection_scope() {
        let (pool, admin_token, _) = setup_pool_and_tokens().await;
        let collection = create_collection(&pool, "collection_scope_class_id").await;
        let class = NewHubuumClass {
            name: "collection-scope-class".to_string(),
            collection_id: collection.id,
            json_schema: None,
            validate_schema: Some(false),
            description: "class".to_string(),
        }
        .save_without_events(&pool)
        .await
        .unwrap();

        let payload = NewExportTemplate {
            collection_id: collection.id,
            name: "export.collections-with-class".to_string(),
            description: "invalid collection export".to_string(),
            content_type: ExportContentType::TextPlain,
            template: "{% for item in items %}{{ item.name }}{% endfor %}".to_string(),
            kind: ExportTemplateKind::Export,
            scope_kind: Some(ExportScopeKind::Collections),
            class_id: Some(class.id),
            default_query: None,
            include: None,
            relation_context: None,
            default_missing_data_policy: None,
            default_limits: None,
        };

        let resp = post_request(&pool, &admin_token, TEMPLATES_ENDPOINT, &payload).await;
        assert_response_status(resp, StatusCode::BAD_REQUEST).await;

        collection.delete_without_events(&pool).await.unwrap();
    }

    #[actix_web::test]
    async fn test_template_list_can_filter_by_kind() {
        let (pool, admin_token, _) = setup_pool_and_tokens().await;
        let collection = create_collection(&pool, "kind_filter").await;

        let class = NewHubuumClass {
            name: "kind-filter-class".to_string(),
            collection_id: collection.id,
            json_schema: None,
            validate_schema: Some(false),
            description: "class for kind filtering".to_string(),
        }
        .save_without_events(&pool)
        .await
        .unwrap();

        let fragment = new_template_payload(collection.id, "partial.kind-fragment");
        let resp = post_request(&pool, &admin_token, TEMPLATES_ENDPOINT, &fragment).await;
        assert_response_status(resp, StatusCode::CREATED).await;

        let export = NewExportTemplate {
            collection_id: collection.id,
            name: "export.kind-export".to_string(),
            description: "export template".to_string(),
            content_type: ExportContentType::TextPlain,
            template: "{% for item in items %}{{ item.name }}{% endfor %}".to_string(),
            kind: ExportTemplateKind::Export,
            scope_kind: Some(ExportScopeKind::ObjectsInClass),
            class_id: Some(class.id),
            default_query: None,
            include: None,
            relation_context: None,
            default_missing_data_policy: None,
            default_limits: None,
        };
        let resp = post_request(&pool, &admin_token, TEMPLATES_ENDPOINT, &export).await;
        assert_response_status(resp, StatusCode::CREATED).await;

        let resp = get_request(
            &pool,
            &admin_token,
            &format!(
                "{TEMPLATES_ENDPOINT}?collection_id={}&kind=export",
                collection.id
            ),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let exports: Vec<ExportTemplate> = test::read_body_json(resp).await;
        assert_eq!(exports.len(), 1);
        assert!(exports.iter().all(|t| t.kind == ExportTemplateKind::Export));
        assert_eq!(exports[0].name, "export.kind-export");

        let resp = get_request(
            &pool,
            &admin_token,
            &format!(
                "{TEMPLATES_ENDPOINT}?collection_id={}&kind=fragment",
                collection.id
            ),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let fragments: Vec<ExportTemplate> = test::read_body_json(resp).await;
        assert_eq!(fragments.len(), 1);
        assert!(
            fragments
                .iter()
                .all(|t| t.kind == ExportTemplateKind::Fragment)
        );
        assert_eq!(fragments[0].name, "partial.kind-fragment");

        collection.delete_without_events(&pool).await.unwrap();
    }

    #[actix_web::test]
    async fn test_template_update_content_requires_update_permission() {
        let (pool, admin_token, _normal_token) = setup_pool_and_tokens().await;
        let collection = create_collection(&pool, "update_content_forbidden").await;

        let create_payload = new_template_payload(collection.id, "tmpl-update-test");
        let resp = post_request(&pool, &admin_token, TEMPLATES_ENDPOINT, &create_payload).await;
        let resp = assert_response_status(resp, StatusCode::CREATED).await;
        let created: ExportTemplate = test::read_body_json(resp).await;

        let test_user = create_test_user(&pool).await;
        let test_group = create_test_group(&pool).await;
        test_group
            .add_member_without_events(&pool, &test_user)
            .await
            .unwrap();
        let user_token = test_user.create_token(&pool).await.unwrap().get_token();

        // Grant only ReadTemplate, not UpdateTemplate
        collection
            .grant_without_events(
                &pool,
                test_group.id,
                PermissionsList::new([Permissions::ReadTemplate]),
            )
            .await
            .unwrap();

        let update_payload = UpdateExportTemplate {
            collection_id: None,
            name: None,
            description: Some("updated description".to_string()),
            template: None,
            ..empty_update_template_payload()
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
        collection
            .grant_without_events(
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
        let updated: ExportTemplate = test::read_body_json(resp).await;
        assert_eq!(updated.description, "updated description");

        collection.delete_without_events(&pool).await.unwrap();
        test_group.delete_without_events(&pool).await.unwrap();
        test_user.delete_without_events(&pool).await.unwrap();
    }

    #[actix_web::test]
    async fn test_template_delete_requires_delete_permission() {
        let (pool, admin_token, _normal_token) = setup_pool_and_tokens().await;
        let collection = create_collection(&pool, "delete_forbidden").await;

        let create_payload = new_template_payload(collection.id, "tmpl-delete-test");
        let resp = post_request(&pool, &admin_token, TEMPLATES_ENDPOINT, &create_payload).await;
        let resp = assert_response_status(resp, StatusCode::CREATED).await;
        let created: ExportTemplate = test::read_body_json(resp).await;

        let test_user = create_test_user(&pool).await;
        let test_group = create_test_group(&pool).await;
        test_group
            .add_member_without_events(&pool, &test_user)
            .await
            .unwrap();
        let user_token = test_user.create_token(&pool).await.unwrap().get_token();

        // Grant only ReadTemplate and UpdateTemplate, not DeleteTemplate
        collection
            .grant_without_events(
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
        collection
            .grant_without_events(
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

        collection.delete_without_events(&pool).await.unwrap();
        test_group.delete_without_events(&pool).await.unwrap();
        test_user.delete_without_events(&pool).await.unwrap();
    }

    #[actix_web::test]
    async fn test_api_template_history_list_and_as_of() {
        use crate::models::{NewExportTemplate, UpdateExportTemplate};
        use crate::traits::{CanSave, CanUpdate};

        let (pool, admin_token, _normal_token) = setup_pool_and_tokens().await;
        let collection = create_collection(&pool, "template_history_api").await;
        let event_context = hubuum_events_core::EventContext::system();

        // Create then update so there are two versions.
        let created = NewExportTemplate {
            collection_id: collection.id,
            name: "template_history_api".to_string(),
            description: "v1".to_string(),
            content_type: crate::models::ExportContentType::TextPlain,
            template: "content".to_string(),
            kind: crate::models::ExportTemplateKind::Export,
            scope_kind: Some(crate::models::ExportScopeKind::Collections),
            class_id: None,
            default_query: None,
            include: None,
            relation_context: None,
            default_missing_data_policy: None,
            default_limits: None,
        }
        .save(&pool, &event_context)
        .await
        .unwrap();

        UpdateExportTemplate {
            collection_id: None,
            name: None,
            description: Some("v2".to_string()),
            template: None,
            kind: None,
            scope_kind: None,
            class_id: None,
            default_query: None,
            include: None,
            relation_context: None,
            default_missing_data_policy: None,
            default_limits: None,
        }
        .update(&pool, created.id, &event_context)
        .await
        .unwrap();

        // List history newest-first.
        let resp = get_request(
            &pool,
            &admin_token,
            &format!("{}/{}/history", TEMPLATES_ENDPOINT, created.id),
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
            &pool,
            &admin_token,
            &format!(
                "{}/{}/history/as-of?at={}",
                TEMPLATES_ENDPOINT, created.id, &v1_from
            ),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let snap: serde_json::Value = test::read_body_json(resp).await;
        assert_eq!(snap["description"], "v1");

        collection.delete(&pool, &event_context).await.unwrap();
    }

    #[actix_web::test]
    async fn test_api_template_history_404_for_missing() {
        let (pool, admin_token, _normal_token) = setup_pool_and_tokens().await;
        let resp = get_request(
            &pool,
            &admin_token,
            &format!("{}/2147483647/history", TEMPLATES_ENDPOINT),
        )
        .await;
        assert_response_status(resp, StatusCode::NOT_FOUND).await;
    }
}
