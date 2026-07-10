#[cfg(test)]
mod tests {
    use actix_web::{http::StatusCode, test};
    use diesel::prelude::*;
    use rstest::rstest;

    use crate::db::with_connection;
    use crate::events::{Action, EntityType, Event};
    use crate::models::{Permissions, PrincipalID};
    use crate::tests::api_operations::{delete_request, get_request, patch_request, put_request};
    use crate::tests::{
        TestContext, create_test_group, create_test_service_account, ensure_admin_group,
        scoped_token, service_account_token,
    };

    const ME_SETTINGS: &str = "/api/v1/iam/me/settings";
    const PRINCIPALS: &str = "/api/v1/iam/principals";

    #[derive(Clone, Copy, Debug)]
    enum RouteFamily {
        Me,
        Principal,
    }

    fn settings_endpoint(family: RouteFamily, principal_id: i32) -> String {
        match family {
            RouteFamily::Me => ME_SETTINGS.to_string(),
            RouteFamily::Principal => format!("{PRINCIPALS}/{principal_id}/settings"),
        }
    }

    #[rstest]
    #[case::me(RouteFamily::Me)]
    #[case::principal(RouteFamily::Principal)]
    #[actix_web::test]
    async fn new_principal_settings_are_empty(#[case] family: RouteFamily) {
        let context = TestContext::new().await;
        let endpoint = settings_endpoint(family, context.normal_user.id);

        let response = get_request(&context.pool, &context.normal_token, &endpoint).await;

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            test::read_body_json::<serde_json::Value, _>(response).await,
            serde_json::json!({})
        );
    }

    #[actix_web::test]
    async fn service_account_settings_default_to_empty() {
        let context = TestContext::new().await;
        let owner_group = create_test_group(&context.pool).await;
        let account = create_test_service_account(&context.pool, &owner_group, None).await;

        let settings = PrincipalID::new(account.id)
            .unwrap()
            .settings(&context.pool)
            .await
            .unwrap();

        assert_eq!(settings.as_value(), &serde_json::json!({}));
    }

    #[actix_web::test]
    async fn database_rejects_non_object_settings() {
        let context = TestContext::new().await;

        let result = with_connection(&context.pool, |conn| {
            diesel::update(
                crate::schema::principals::table
                    .filter(crate::schema::principals::id.eq(context.normal_user.id)),
            )
            .set(crate::schema::principals::settings.eq(serde_json::json!(["invalid"])))
            .execute(conn)
        });

        assert!(matches!(
            result,
            Err(crate::errors::ApiError::BadRequest(_))
        ));
    }

    #[rstest]
    #[case::me(RouteFamily::Me)]
    #[case::principal(RouteFamily::Principal)]
    #[actix_web::test]
    async fn put_replaces_the_complete_settings_document(#[case] family: RouteFamily) {
        let context = TestContext::new().await;
        let endpoint = settings_endpoint(family, context.normal_user.id);
        let replacement = serde_json::json!({
            "theme": "dark",
            "dashboard": { "columns": 3 },
            "nullable_preference": null,
            "shortcuts": ["search", "create"]
        });

        let response = put_request(
            &context.pool,
            &context.normal_token,
            &endpoint,
            &replacement,
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            test::read_body_json::<serde_json::Value, _>(response).await,
            replacement
        );
    }

    #[rstest]
    #[case::me(RouteFamily::Me)]
    #[case::principal(RouteFamily::Principal)]
    #[actix_web::test]
    async fn patch_recursively_merges_objects_and_replaces_other_values(
        #[case] family: RouteFamily,
    ) {
        let context = TestContext::new().await;
        let endpoint = settings_endpoint(family, context.normal_user.id);
        let initial = serde_json::json!({
            "nested": { "keep": true, "change": "old" },
            "scalar": "old",
            "unchanged": 7
        });
        let response = put_request(&context.pool, &context.normal_token, &endpoint, &initial).await;
        assert_eq!(response.status(), StatusCode::OK);

        let response = patch_request(
            &context.pool,
            &context.normal_token,
            &endpoint,
            serde_json::json!({
                "nested": { "change": "new", "added": [1, 2] },
                "scalar": { "now": "an object" }
            }),
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            test::read_body_json::<serde_json::Value, _>(response).await,
            serde_json::json!({
                "nested": { "keep": true, "change": "new", "added": [1, 2] },
                "scalar": { "now": "an object" },
                "unchanged": 7
            })
        );
    }

    #[rstest]
    #[case::me(RouteFamily::Me)]
    #[case::principal(RouteFamily::Principal)]
    #[actix_web::test]
    async fn patch_null_removes_keys_at_each_depth(#[case] family: RouteFamily) {
        let context = TestContext::new().await;
        let endpoint = settings_endpoint(family, context.normal_user.id);
        let response = put_request(
            &context.pool,
            &context.normal_token,
            &endpoint,
            serde_json::json!({
                "remove": true,
                "nested": { "remove": true, "keep": true }
            }),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);

        let response = patch_request(
            &context.pool,
            &context.normal_token,
            &endpoint,
            serde_json::json!({
                "remove": null,
                "nested": { "remove": null }
            }),
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            test::read_body_json::<serde_json::Value, _>(response).await,
            serde_json::json!({ "nested": { "keep": true } })
        );
    }

    #[rstest]
    #[case::me(RouteFamily::Me)]
    #[case::principal(RouteFamily::Principal)]
    #[actix_web::test]
    async fn delete_resets_settings(#[case] family: RouteFamily) {
        let context = TestContext::new().await;
        let endpoint = settings_endpoint(family, context.normal_user.id);
        let response = put_request(
            &context.pool,
            &context.normal_token,
            &endpoint,
            serde_json::json!({ "theme": "dark" }),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);

        let response = delete_request(&context.pool, &context.normal_token, &endpoint).await;

        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        let settings = PrincipalID::new(context.normal_user.id)
            .unwrap()
            .settings(&context.pool)
            .await
            .unwrap();
        assert_eq!(settings.as_value(), &serde_json::json!({}));
    }

    #[derive(Clone, Copy, Debug)]
    enum WriteMethod {
        Put,
        Patch,
    }

    #[rstest]
    #[case::put_array(WriteMethod::Put, serde_json::json!([]))]
    #[case::put_string(WriteMethod::Put, serde_json::json!("invalid"))]
    #[case::put_number(WriteMethod::Put, serde_json::json!(1))]
    #[case::put_boolean(WriteMethod::Put, serde_json::json!(true))]
    #[case::put_null(WriteMethod::Put, serde_json::Value::Null)]
    #[case::patch_array(WriteMethod::Patch, serde_json::json!([]))]
    #[case::patch_string(WriteMethod::Patch, serde_json::json!("invalid"))]
    #[case::patch_number(WriteMethod::Patch, serde_json::json!(1))]
    #[case::patch_boolean(WriteMethod::Patch, serde_json::json!(true))]
    #[case::patch_null(WriteMethod::Patch, serde_json::Value::Null)]
    #[actix_web::test]
    async fn write_rejects_non_object_roots(
        #[case] method: WriteMethod,
        #[case] body: serde_json::Value,
    ) {
        let context = TestContext::new().await;
        let response = match method {
            WriteMethod::Put => {
                put_request(&context.pool, &context.normal_token, ME_SETTINGS, body).await
            }
            WriteMethod::Patch => {
                patch_request(&context.pool, &context.normal_token, ME_SETTINGS, body).await
            }
        };

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[derive(Clone, Copy, Debug)]
    enum SelfCaller {
        Human,
        ScopedHuman,
        ServiceAccount,
        ProviderManagedHuman,
    }

    #[rstest]
    #[case::human(SelfCaller::Human)]
    #[case::scoped_human(SelfCaller::ScopedHuman)]
    #[case::service_account(SelfCaller::ServiceAccount)]
    #[case::provider_managed(SelfCaller::ProviderManagedHuman)]
    #[actix_web::test]
    async fn every_valid_principal_token_can_manage_its_own_settings(#[case] caller: SelfCaller) {
        let context = TestContext::new().await;
        let (principal_id, token) = match caller {
            SelfCaller::Human => (context.normal_user.id, context.normal_token.clone()),
            SelfCaller::ScopedHuman => (
                context.normal_user.id,
                scoped_token(
                    &context.pool,
                    context.normal_user.id,
                    &[Permissions::ReadCollection],
                )
                .await,
            ),
            SelfCaller::ServiceAccount => {
                let owner_group = create_test_group(&context.pool).await;
                let account = create_test_service_account(&context.pool, &owner_group, None).await;
                let token =
                    scoped_token(&context.pool, account.id, &[Permissions::ReadCollection]).await;
                (account.id, token)
            }
            SelfCaller::ProviderManagedHuman => {
                with_connection(&context.pool, |conn| {
                    diesel::update(
                        crate::schema::principals::table
                            .filter(crate::schema::principals::id.eq(context.normal_user.id)),
                    )
                    .set(crate::schema::principals::provider_managed.eq(true))
                    .execute(conn)
                })
                .unwrap();
                (context.normal_user.id, context.normal_token.clone())
            }
        };
        let endpoint = format!("{PRINCIPALS}/{principal_id}/settings");

        let response = put_request(
            &context.pool,
            &token,
            &endpoint,
            serde_json::json!({ "allowed": true }),
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[derive(Clone, Copy, Debug)]
    enum CrossCaller {
        HumanAdmin,
        HumanNonAdmin,
        ScopedHumanAdmin,
        ServiceAccountInAdminGroup,
    }

    #[rstest]
    #[case::human_admin(CrossCaller::HumanAdmin, StatusCode::OK)]
    #[case::human_non_admin(CrossCaller::HumanNonAdmin, StatusCode::NOT_FOUND)]
    #[case::scoped_human_admin(CrossCaller::ScopedHumanAdmin, StatusCode::NOT_FOUND)]
    #[case::service_account_admin_group(
        CrossCaller::ServiceAccountInAdminGroup,
        StatusCode::NOT_FOUND
    )]
    #[actix_web::test]
    async fn cross_principal_access_requires_an_unscoped_human_admin(
        #[case] caller: CrossCaller,
        #[case] expected: StatusCode,
    ) {
        let context = TestContext::new().await;
        let token = match caller {
            CrossCaller::HumanAdmin => context.admin_token.clone(),
            CrossCaller::HumanNonAdmin => context.normal_token.clone(),
            CrossCaller::ScopedHumanAdmin => {
                scoped_token(
                    &context.pool,
                    context.admin_user.id,
                    &[Permissions::ReadCollection],
                )
                .await
            }
            CrossCaller::ServiceAccountInAdminGroup => {
                let admin_group = ensure_admin_group(&context.pool).await;
                let owner_group = create_test_group(&context.pool).await;
                let account = create_test_service_account(&context.pool, &owner_group, None).await;
                admin_group
                    .add_member_without_events(&context.pool, &account)
                    .await
                    .unwrap();
                service_account_token(&context.pool, &account, None, None).await
            }
        };
        let target = context.scope.scoped_name("settings-cross-target");
        let target =
            crate::tests::create_user_with_params(&context.pool, &target, "password").await;
        let endpoint = format!("{PRINCIPALS}/{}/settings", target.id);

        let response = get_request(&context.pool, &token, &endpoint).await;

        assert_eq!(response.status(), expected);
    }

    #[derive(Clone, Copy, Debug)]
    enum TargetKind {
        Human,
        ServiceAccount,
    }

    #[derive(Clone, Copy, Debug)]
    enum Mutation {
        Put,
        Patch,
        Delete,
    }

    #[rstest]
    #[case::human_put(TargetKind::Human, Mutation::Put)]
    #[case::human_patch(TargetKind::Human, Mutation::Patch)]
    #[case::human_delete(TargetKind::Human, Mutation::Delete)]
    #[case::service_account_put(TargetKind::ServiceAccount, Mutation::Put)]
    #[case::service_account_patch(TargetKind::ServiceAccount, Mutation::Patch)]
    #[case::service_account_delete(TargetKind::ServiceAccount, Mutation::Delete)]
    #[actix_web::test]
    async fn mutations_emit_complete_settings_snapshots_for_the_target_kind(
        #[case] target_kind: TargetKind,
        #[case] mutation: Mutation,
    ) {
        let context = TestContext::new().await;
        let (target_id, entity_type) = match target_kind {
            TargetKind::Human => (context.normal_user.id, EntityType::User),
            TargetKind::ServiceAccount => {
                let owner_group = create_test_group(&context.pool).await;
                let account = create_test_service_account(&context.pool, &owner_group, None).await;
                (account.id, EntityType::ServiceAccount)
            }
        };
        let endpoint = format!("{PRINCIPALS}/{target_id}/settings");
        let initial = serde_json::json!({ "nested": { "keep": true, "change": "old" } });
        let (before, after, response) = match mutation {
            Mutation::Put => {
                let replacement = serde_json::json!({ "theme": "dark" });
                let response =
                    put_request(&context.pool, &context.admin_token, &endpoint, &replacement).await;
                (serde_json::json!({}), replacement, response)
            }
            Mutation::Patch => {
                let setup =
                    put_request(&context.pool, &context.admin_token, &endpoint, &initial).await;
                assert_eq!(setup.status(), StatusCode::OK);
                let after = serde_json::json!({
                    "nested": { "keep": true, "change": "new", "added": 2 }
                });
                let response = patch_request(
                    &context.pool,
                    &context.admin_token,
                    &endpoint,
                    serde_json::json!({
                        "nested": { "change": "new", "added": 2 }
                    }),
                )
                .await;
                (initial, after, response)
            }
            Mutation::Delete => {
                let setup =
                    put_request(&context.pool, &context.admin_token, &endpoint, &initial).await;
                assert_eq!(setup.status(), StatusCode::OK);
                let response = delete_request(&context.pool, &context.admin_token, &endpoint).await;
                (initial, serde_json::json!({}), response)
            }
        };
        let expected_status = match mutation {
            Mutation::Delete => StatusCode::NO_CONTENT,
            Mutation::Put | Mutation::Patch => StatusCode::OK,
        };
        assert_eq!(response.status(), expected_status);

        let event = with_connection(&context.pool, |conn| {
            crate::schema::events::table
                .filter(crate::schema::events::entity_type.eq(entity_type.as_str()))
                .filter(crate::schema::events::entity_id.eq(target_id))
                .filter(crate::schema::events::action.eq(Action::Updated.as_str()))
                .order(crate::schema::events::id.desc())
                .first::<Event>(conn)
        })
        .unwrap();

        assert_eq!(
            event.before,
            Some(serde_json::json!({ "settings": before }))
        );
        assert_eq!(event.after, Some(serde_json::json!({ "settings": after })));
    }

    #[actix_web::test]
    async fn concurrent_patches_preserve_unrelated_changes() {
        let context = TestContext::new().await;
        let endpoint = format!("{PRINCIPALS}/{}/settings", context.normal_user.id);
        let response = put_request(
            &context.pool,
            &context.normal_token,
            &endpoint,
            serde_json::json!({ "base": true }),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);

        let (left, right) = tokio::join!(
            patch_request(
                &context.pool,
                &context.normal_token,
                &endpoint,
                serde_json::json!({ "left": 1 }),
            ),
            patch_request(
                &context.pool,
                &context.normal_token,
                &endpoint,
                serde_json::json!({ "right": 2 }),
            )
        );
        assert_eq!(left.status(), StatusCode::OK);
        assert_eq!(right.status(), StatusCode::OK);

        let response = get_request(&context.pool, &context.normal_token, &endpoint).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            test::read_body_json::<serde_json::Value, _>(response).await,
            serde_json::json!({ "base": true, "left": 1, "right": 2 })
        );
    }
}
