#[cfg(test)]
mod tests {
    use crate::storage::postgres::prelude::*;
    use actix_web::{http::StatusCode, test};
    use chrono::NaiveDateTime;
    use rstest::rstest;

    use crate::errors::ApiError;
    use crate::events::{Action, EntityType, Event};
    use crate::models::{
        MAX_PRINCIPAL_SETTINGS_PATCH_BYTES, MAX_PRINCIPAL_SETTINGS_PATCH_OPERATIONS, Permissions,
        PrincipalID, PrincipalSettingsResponse, ResourceRevision,
    };
    use crate::storage::postgres::{PostgresPool, with_connection};
    use crate::tests::api_operations::{
        get_request, patch_request_with_content_type, patch_request_with_raw_body, put_request,
    };
    use crate::tests::{TestContext, create_test_group, create_test_service_account, scoped_token};

    const JSON_PATCH_MEDIA_TYPE: &str = "application/json-patch+json";
    const JSON_MERGE_PATCH_MEDIA_TYPE: &str = "application/merge-patch+json";
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

    async fn patch_settings(
        context: &TestContext,
        token: &str,
        endpoint: &str,
        patch: serde_json::Value,
    ) -> actix_web::dev::ServiceResponse {
        patch_request_with_content_type(
            &context.pool,
            token,
            endpoint,
            patch,
            JSON_PATCH_MEDIA_TYPE,
        )
        .await
    }

    async fn current_settings(
        context: &TestContext,
        token: &str,
        endpoint: &str,
    ) -> PrincipalSettingsResponse {
        let response = get_request(&context.pool, token, endpoint).await;
        assert_eq!(response.status(), StatusCode::OK);
        test::read_body_json(response).await
    }

    #[derive(Debug, PartialEq, Eq)]
    struct MutationState {
        revision: ResourceRevision,
        updated_at: NaiveDateTime,
        event_count: i64,
    }

    async fn mutation_state(pool: &PostgresPool, principal_id: i32) -> MutationState {
        use crate::schema::{events, principals};

        let (revision, updated_at, event_count) = with_connection(pool, async |conn| {
            let (revision, updated_at) = principals::table
                .filter(principals::id.eq(principal_id))
                .select((principals::revision, principals::updated_at))
                .first::<(ResourceRevision, NaiveDateTime)>(conn)
                .await?;
            let event_count = events::table
                .filter(events::entity_type.eq(EntityType::User.as_str()))
                .filter(events::entity_id.eq(principal_id))
                .filter(events::action.eq(Action::Updated.as_str()))
                .count()
                .get_result::<i64>(conn)
                .await?;
            Ok::<_, ApiError>((revision, updated_at, event_count))
        })
        .await
        .unwrap();
        MutationState {
            revision,
            updated_at,
            event_count,
        }
    }

    #[rstest]
    #[case::me(RouteFamily::Me)]
    #[case::principal(RouteFamily::Principal)]
    #[actix_web::test]
    async fn both_settings_route_families_accept_json_patch(#[case] family: RouteFamily) {
        let context = TestContext::new().await;
        let endpoint = settings_endpoint(family, context.normal_user.id);
        let setup = put_request(
            &context.pool,
            &context.normal_token,
            &endpoint,
            serde_json::json!({"theme": "light"}),
        )
        .await;
        assert_eq!(setup.status(), StatusCode::OK);

        let response = patch_settings(
            &context,
            &context.normal_token,
            &endpoint,
            serde_json::json!([
                {"op": "replace", "path": "/theme", "value": "dark"}
            ]),
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK);
        let settings: PrincipalSettingsResponse = test::read_body_json(response).await;
        assert_eq!(settings.as_value(), &serde_json::json!({"theme": "dark"}));
    }

    #[rstest]
    #[case::add(
        serde_json::json!({"source": 1}),
        serde_json::json!([{"op": "add", "path": "/added", "value": true}]),
        serde_json::json!({"source": 1, "added": true})
    )]
    #[case::remove(
        serde_json::json!({"remove": 1, "keep": 2}),
        serde_json::json!([{"op": "remove", "path": "/remove"}]),
        serde_json::json!({"keep": 2})
    )]
    #[case::replace(
        serde_json::json!({"value": "before"}),
        serde_json::json!([{"op": "replace", "path": "/value", "value": "after"}]),
        serde_json::json!({"value": "after"})
    )]
    #[case::move_value(
        serde_json::json!({"source": {"value": 1}}),
        serde_json::json!([{"op": "move", "from": "/source", "path": "/moved"}]),
        serde_json::json!({"moved": {"value": 1}})
    )]
    #[case::copy(
        serde_json::json!({"source": {"value": 1}}),
        serde_json::json!([{"op": "copy", "from": "/source", "path": "/copied"}]),
        serde_json::json!({"source": {"value": 1}, "copied": {"value": 1}})
    )]
    #[case::test_success(
        serde_json::json!({"value": 1}),
        serde_json::json!([{"op": "test", "path": "/value", "value": 1}]),
        serde_json::json!({"value": 1})
    )]
    #[actix_web::test]
    async fn principal_settings_support_each_rfc_6902_operation(
        #[case] initial: serde_json::Value,
        #[case] patch: serde_json::Value,
        #[case] expected: serde_json::Value,
    ) {
        let context = TestContext::new().await;
        let setup = put_request(&context.pool, &context.normal_token, ME_SETTINGS, initial).await;
        assert_eq!(setup.status(), StatusCode::OK);

        let response = patch_settings(&context, &context.normal_token, ME_SETTINGS, patch).await;

        assert_eq!(response.status(), StatusCode::OK);
        let settings: PrincipalSettingsResponse = test::read_body_json(response).await;
        assert_eq!(settings.as_value(), &expected);
    }

    #[rstest]
    #[case::json("application/json")]
    #[case::merge_patch(JSON_MERGE_PATCH_MEDIA_TYPE)]
    #[actix_web::test]
    async fn object_content_types_retain_json_merge_patch_semantics(#[case] content_type: &str) {
        let context = TestContext::new().await;
        let initial = serde_json::json!({
            "nested": {"keep": true, "remove": true},
            "array": [1, 2]
        });
        let setup = put_request(&context.pool, &context.normal_token, ME_SETTINGS, &initial).await;
        assert_eq!(setup.status(), StatusCode::OK);

        let response = patch_request_with_content_type(
            &context.pool,
            &context.normal_token,
            ME_SETTINGS,
            serde_json::json!({
                "nested": {"remove": null, "added": 2},
                "array": [3]
            }),
            content_type,
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK);
        let settings: PrincipalSettingsResponse = test::read_body_json(response).await;
        assert_eq!(
            settings.as_value(),
            &serde_json::json!({
                "nested": {"keep": true, "added": 2},
                "array": [3]
            })
        );
    }

    #[actix_web::test]
    async fn json_patch_can_insert_an_individual_array_element() {
        let context = TestContext::new().await;
        let setup = put_request(
            &context.pool,
            &context.normal_token,
            ME_SETTINGS,
            serde_json::json!({"items": ["first", "third"]}),
        )
        .await;
        assert_eq!(setup.status(), StatusCode::OK);

        let response = patch_settings(
            &context,
            &context.normal_token,
            ME_SETTINGS,
            serde_json::json!([
                {"op": "add", "path": "/items/1", "value": "second"}
            ]),
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK);
        let settings: PrincipalSettingsResponse = test::read_body_json(response).await;
        assert_eq!(
            settings.as_value(),
            &serde_json::json!({"items": ["first", "second", "third"]})
        );
    }

    #[actix_web::test]
    async fn json_patch_can_store_a_literal_null() {
        let context = TestContext::new().await;

        let response = patch_settings(
            &context,
            &context.normal_token,
            ME_SETTINGS,
            serde_json::json!([
                {"op": "add", "path": "/nullable", "value": null}
            ]),
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK);
        let settings: PrincipalSettingsResponse = test::read_body_json(response).await;
        assert_eq!(settings.as_value(), &serde_json::json!({"nullable": null}));
    }

    #[actix_web::test]
    async fn a_failed_test_rolls_back_prior_operations_and_audit_side_effects() {
        let context = TestContext::new().await;
        let principal_id = context.normal_user.id;
        let initial = serde_json::json!({"version": 1, "state": "before"});
        let setup = put_request(&context.pool, &context.normal_token, ME_SETTINGS, &initial).await;
        assert_eq!(setup.status(), StatusCode::OK);
        let before = mutation_state(&context.pool, principal_id).await;

        let response = patch_settings(
            &context,
            &context.normal_token,
            ME_SETTINGS,
            serde_json::json!([
                {"op": "replace", "path": "/state", "value": "intermediate"},
                {"op": "test", "path": "/version", "value": 2}
            ]),
        )
        .await;

        assert_eq!(response.status(), StatusCode::CONFLICT);
        assert_eq!(mutation_state(&context.pool, principal_id).await, before);
        assert_eq!(
            current_settings(&context, &context.normal_token, ME_SETTINGS)
                .await
                .as_value(),
            &initial
        );
    }

    #[actix_web::test]
    async fn json_patch_test_compares_numbers_by_rfc_value() {
        let context = TestContext::new().await;

        let response = patch_request_with_raw_body(
            &context.pool,
            &context.normal_token,
            ME_SETTINGS,
            br#"[
                {"op":"add","path":"/value","value":{"direct":1.0,"nested":[2e0]}},
                {"op":"test","path":"/value","value":{"nested":[2.00],"direct":1}},
                {"op":"add","path":"/test_passed","value":true}
            ]"#
            .as_slice(),
            JSON_PATCH_MEDIA_TYPE,
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK);
        let settings: PrincipalSettingsResponse = test::read_body_json(response).await;
        assert_eq!(settings.as_value()["test_passed"], true);
    }

    #[actix_web::test]
    async fn a_json_patch_no_op_does_not_advance_revision_or_emit_an_event() {
        let context = TestContext::new().await;
        let principal_id = context.normal_user.id;
        let setup = put_request(
            &context.pool,
            &context.normal_token,
            ME_SETTINGS,
            serde_json::json!({"version": 1}),
        )
        .await;
        assert_eq!(setup.status(), StatusCode::OK);
        let before = mutation_state(&context.pool, principal_id).await;

        let response = patch_settings(
            &context,
            &context.normal_token,
            ME_SETTINGS,
            serde_json::json!([
                {"op": "test", "path": "/version", "value": 1}
            ]),
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK);
        let settings: PrincipalSettingsResponse = test::read_body_json(response).await;
        assert_eq!(settings.revision, before.revision);
        assert_eq!(mutation_state(&context.pool, principal_id).await, before);
    }

    #[actix_web::test]
    async fn concurrent_json_patches_apply_to_the_latest_row_locked_settings() {
        let context = TestContext::new().await;
        let setup = put_request(
            &context.pool,
            &context.normal_token,
            ME_SETTINGS,
            serde_json::json!({"base": true}),
        )
        .await;
        assert_eq!(setup.status(), StatusCode::OK);
        let left_patch = serde_json::json!([
            {"op": "add", "path": "/left", "value": 1}
        ]);
        let right_patch = serde_json::json!([
            {"op": "add", "path": "/right", "value": 2}
        ]);

        let (left, right) = tokio::join!(
            patch_settings(&context, &context.normal_token, ME_SETTINGS, left_patch,),
            patch_settings(&context, &context.normal_token, ME_SETTINGS, right_patch,)
        );

        assert_eq!(left.status(), StatusCode::OK);
        assert_eq!(right.status(), StatusCode::OK);
        assert_eq!(
            current_settings(&context, &context.normal_token, ME_SETTINGS)
                .await
                .as_value(),
            &serde_json::json!({"base": true, "left": 1, "right": 2})
        );
    }

    #[actix_web::test]
    async fn json_patch_rejects_a_non_object_final_root_without_persisting_it() {
        let context = TestContext::new().await;
        let initial = serde_json::json!({"keep": true});
        let setup = put_request(&context.pool, &context.normal_token, ME_SETTINGS, &initial).await;
        assert_eq!(setup.status(), StatusCode::OK);

        let response = patch_settings(
            &context,
            &context.normal_token,
            ME_SETTINGS,
            serde_json::json!([
                {"op": "replace", "path": "", "value": ["invalid"]}
            ]),
        )
        .await;

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            current_settings(&context, &context.normal_token, ME_SETTINGS)
                .await
                .as_value(),
            &initial
        );
    }

    #[actix_web::test]
    async fn json_patch_rejects_an_oversized_request() {
        let context = TestContext::new().await;
        let body = format!("[]{}", " ".repeat(MAX_PRINCIPAL_SETTINGS_PATCH_BYTES));

        let response = patch_request_with_raw_body(
            &context.pool,
            &context.normal_token,
            ME_SETTINGS,
            body,
            JSON_PATCH_MEDIA_TYPE,
        )
        .await;

        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    }

    #[actix_web::test]
    async fn json_patch_rejects_an_oversized_result() {
        let context = TestContext::new().await;
        let setup = put_request(
            &context.pool,
            &context.normal_token,
            ME_SETTINGS,
            serde_json::json!({
                "blob": "x".repeat(MAX_PRINCIPAL_SETTINGS_PATCH_BYTES / 2 + 1)
            }),
        )
        .await;
        assert_eq!(setup.status(), StatusCode::OK);

        let response = patch_settings(
            &context,
            &context.normal_token,
            ME_SETTINGS,
            serde_json::json!([
                {"op": "copy", "from": "/blob", "path": "/copy"}
            ]),
        )
        .await;

        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    }

    #[actix_web::test]
    async fn malformed_json_patch_documents_are_rejected() {
        let context = TestContext::new().await;

        let response = patch_settings(
            &context,
            &context.normal_token,
            ME_SETTINGS,
            serde_json::json!({"op": "add", "path": "/invalid", "value": true}),
        )
        .await;

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[actix_web::test]
    async fn json_patch_rejects_too_many_operations() {
        let context = TestContext::new().await;
        let operations = (0..=MAX_PRINCIPAL_SETTINGS_PATCH_OPERATIONS)
            .map(|_| serde_json::json!({"op": "test", "path": "", "value": {}}))
            .collect::<Vec<_>>();

        let response = patch_settings(
            &context,
            &context.normal_token,
            ME_SETTINGS,
            serde_json::Value::Array(operations),
        )
        .await;

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[actix_web::test]
    async fn json_patch_rejects_an_excessively_deep_pointer() {
        let context = TestContext::new().await;
        let path = format!("/{}", vec!["segment"; 129].join("/"));

        let response = patch_settings(
            &context,
            &context.normal_token,
            ME_SETTINGS,
            serde_json::json!([
                {"op": "remove", "path": path}
            ]),
        )
        .await;

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[actix_web::test]
    async fn json_patch_rejects_an_excessively_nested_intermediate_result() {
        let context = TestContext::new().await;
        let nested = (0..=64).fold(serde_json::Value::Null, |value, _| {
            serde_json::Value::Array(vec![value])
        });

        let response = patch_settings(
            &context,
            &context.normal_token,
            ME_SETTINGS,
            serde_json::json!([
                {"op": "add", "path": "/nested", "value": nested},
                {"op": "remove", "path": "/nested"}
            ]),
        )
        .await;

        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
        assert_eq!(
            current_settings(&context, &context.normal_token, ME_SETTINGS)
                .await
                .as_value(),
            &serde_json::json!({})
        );
    }

    #[actix_web::test]
    async fn json_patch_bounds_cumulative_application_work() {
        let context = TestContext::new().await;
        let setup = put_request(
            &context.pool,
            &context.normal_token,
            ME_SETTINGS,
            serde_json::json!({
                "padding": "x".repeat(40 * 1024),
                "value": true
            }),
        )
        .await;
        assert_eq!(setup.status(), StatusCode::OK);
        let operations = (0..MAX_PRINCIPAL_SETTINGS_PATCH_OPERATIONS)
            .map(|_| serde_json::json!({"op": "test", "path": "/value", "value": true}))
            .collect::<Vec<_>>();

        let response = patch_settings(
            &context,
            &context.normal_token,
            ME_SETTINGS,
            serde_json::Value::Array(operations),
        )
        .await;

        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    }

    #[actix_web::test]
    async fn json_patch_rejects_results_postgresql_jsonb_cannot_represent() {
        let context = TestContext::new().await;

        let response = patch_settings(
            &context,
            &context.normal_token,
            ME_SETTINGS,
            serde_json::json!([
                {"op": "add", "path": "/invalid", "value": "contains\u{0}null"}
            ]),
        )
        .await;

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            current_settings(&context, &context.normal_token, ME_SETTINGS)
                .await
                .as_value(),
            &serde_json::json!({})
        );
    }

    #[actix_web::test]
    async fn service_accounts_can_json_patch_their_own_settings() {
        let context = TestContext::new().await;
        let owner_group = create_test_group(&context.pool).await;
        let account = create_test_service_account(&context.pool, &owner_group, None).await;
        let token = scoped_token(&context.pool, account.id, &[Permissions::ReadCollection]).await;

        let response = patch_settings(
            &context,
            &token,
            ME_SETTINGS,
            serde_json::json!([
                {"op": "add", "path": "/automation", "value": true}
            ]),
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK);
        let settings: PrincipalSettingsResponse = test::read_body_json(response).await;
        assert_eq!(
            settings.as_value(),
            &serde_json::json!({"automation": true})
        );
    }

    #[actix_web::test]
    async fn cross_principal_json_patch_preserves_existing_authorization_rules() {
        let context = TestContext::new().await;
        let endpoint = format!("{PRINCIPALS}/{}/settings", context.admin_user.id);

        let response = patch_settings(
            &context,
            &context.normal_token,
            &endpoint,
            serde_json::json!([
                {"op": "add", "path": "/unauthorized", "value": true}
            ]),
        )
        .await;

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert_eq!(
            PrincipalID::new(context.admin_user.id)
                .unwrap()
                .settings(&context.pool)
                .await
                .unwrap()
                .as_value(),
            &serde_json::json!({})
        );
    }

    #[actix_web::test]
    async fn successful_json_patch_emits_complete_service_account_snapshots() {
        let context = TestContext::new().await;
        let owner_group = create_test_group(&context.pool).await;
        let account = create_test_service_account(&context.pool, &owner_group, None).await;
        let endpoint = format!("{PRINCIPALS}/{}/settings", account.id);
        let initial = serde_json::json!({"mode": "before", "keep": true});
        let setup = put_request(&context.pool, &context.admin_token, &endpoint, &initial).await;
        assert_eq!(setup.status(), StatusCode::OK);

        let response = patch_settings(
            &context,
            &context.admin_token,
            &endpoint,
            serde_json::json!([
                {"op": "replace", "path": "/mode", "value": "after"}
            ]),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let updated: PrincipalSettingsResponse = test::read_body_json(response).await;

        let event = with_connection(&context.pool, async |conn| {
            crate::schema::events::table
                .filter(crate::schema::events::entity_type.eq(EntityType::ServiceAccount.as_str()))
                .filter(crate::schema::events::entity_id.eq(account.id))
                .filter(crate::schema::events::action.eq(Action::Updated.as_str()))
                .order(crate::schema::events::id.desc())
                .first::<Event>(conn)
                .await
        })
        .await
        .unwrap();

        assert_eq!(
            event.before,
            Some(serde_json::json!({
                "revision": event.before_revision.unwrap(),
                "settings": initial
            }))
        );
        assert_eq!(
            event.after,
            Some(serde_json::json!({
                "revision": updated.revision,
                "settings": {"mode": "after", "keep": true}
            }))
        );
    }

    #[actix_web::test]
    async fn unsupported_principal_settings_patch_media_types_are_rejected() {
        let context = TestContext::new().await;

        let response = patch_request_with_content_type(
            &context.pool,
            &context.normal_token,
            ME_SETTINGS,
            serde_json::json!({"theme": "dark"}),
            "application/problem+json",
        )
        .await;

        assert_eq!(response.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);
    }
}
