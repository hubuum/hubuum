#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::db::prelude::*;
    use actix_web::{http::StatusCode, test};

    use crate::db::traits::computed_field::{
        class_computation_state_for, create_personal_definition, create_shared_definition,
        enrich_objects_with_computed_query_snapshot, execute_computed_reindex_task,
        request_class_rebuild, resolve_computed_query_fields, source_data_sha256,
    };
    use crate::db::traits::task::recover_expired_task_leases;
    use crate::db::{capture_queries, with_connection};
    use crate::events::EventContext;
    use crate::models::search::parse_query_parameter;
    use crate::models::{
        ComputedFieldDefinitionRequest, HubuumClassID, NewHubuumClass, NewHubuumObject,
        Permissions, TaskID, TaskStatus,
    };
    use crate::pagination::{NEXT_CURSOR_HEADER, TOTAL_COUNT_HEADER, finalize_page};
    use crate::permissions::test_support::mock_treetop::{MockAllowRule, MockTreetopBackend};
    use crate::permissions::{ResourceAttrs, ResourceKind};
    use crate::tests::api_operations::{
        get_request, get_request_with_permission_backend, patch_request, post_request,
    };
    use crate::tests::asserts::{assert_response_status, header_value};
    use crate::tests::{
        TestContext, create_test_group, create_test_service_account, get_test_pool, scoped_token,
        service_account_token, test_context,
    };
    use crate::traits::{CanDelete, CanSave, PermissionController, SelfAccessors};

    #[derive(QueryableByName)]
    struct ComputedQuerySqlValue {
        #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Jsonb>)]
        value: Option<serde_json::Value>,
    }

    #[derive(QueryableByName)]
    struct ComputedSourceHashSqlValue {
        #[diesel(sql_type = diesel::sql_types::Text)]
        value: String,
    }

    async fn evaluate_sql_scope(
        context: &TestContext,
        data: &serde_json::Value,
        definitions: &[hubuum_computed_fields::Definition],
    ) -> serde_json::Value {
        let definitions = serde_json::to_value(definitions).unwrap();
        with_connection(&context.pool, async |conn| {
            diesel::sql_query("SELECT hubuum_computed_evaluate_scope($1, $2) AS value")
                .bind::<diesel::sql_types::Jsonb, _>(data)
                .bind::<diesel::sql_types::Jsonb, _>(&definitions)
                .get_result::<ComputedQuerySqlValue>(conn)
                .await
        })
        .await
        .unwrap()
        .value
        .expect("scope evaluator always returns JSON")
    }

    fn evaluator_definition(value: serde_json::Value) -> hubuum_computed_fields::Definition {
        serde_json::from_value(value).unwrap()
    }

    fn definition(key: &str) -> serde_json::Value {
        serde_json::json!({
            "key": key,
            "label": "Display name",
            "description": "",
            "operation": {
                "type": "first_non_null",
                "paths": ["/inventory/hostname", "/manual/hostname"]
            },
            "result_type": "string",
            "enabled": true
        })
    }

    fn definition_request(key: &str) -> ComputedFieldDefinitionRequest {
        serde_json::from_value(definition(key)).unwrap()
    }

    async fn fixture(context: &TestContext, label: &str) -> crate::tests::ObjectFixture {
        context
            .object_fixture(
                label,
                NewHubuumClass {
                    collection_id: 0,
                    name: context.scoped_name("computed class"),
                    description: "Computed field test class".to_string(),
                    json_schema: None,
                    validate_schema: Some(false),
                },
                vec![NewHubuumObject {
                    collection_id: 0,
                    hubuum_class_id: 0,
                    name: context.scoped_name("computed object"),
                    description: "Computed field test object".to_string(),
                    data: serde_json::json!({
                        "inventory": {"hostname": "inventory.example"},
                        "manual": {"hostname": "manual.example"}
                    }),
                }],
            )
            .await
            .unwrap()
    }

    async fn active_rebuild_task(
        context: &TestContext,
        class_id: i32,
    ) -> crate::models::TaskRecord {
        for _ in 0..50 {
            let state = class_computation_state_for(&context.pool, class_id)
                .await
                .unwrap();
            let task_id = match state.active_task_id {
                Some(task_id) => task_id,
                None => {
                    let class = HubuumClassID::new(class_id)
                        .unwrap()
                        .instance(&context.pool)
                        .await
                        .unwrap();
                    request_class_rebuild(
                        &context.pool,
                        class_id,
                        class.collection_id,
                        Some(context.admin_user.id),
                    )
                    .await
                    .unwrap()
                    .active_task_id
                    .expect("manual rebuild task")
                }
            };
            if let Ok(task) = TaskID::new(task_id).unwrap().instance(&context.pool).await {
                return task;
            }
            tokio::task::yield_now().await;
        }
        panic!("could not load an active computed-field rebuild task");
    }

    async fn finish_active_rebuild(context: &TestContext, class_id: i32) {
        for _ in 0..50 {
            let state = class_computation_state_for(&context.pool, class_id)
                .await
                .unwrap();
            if state.active_task_id.is_none() && state.rebuild_status == "ready" {
                return;
            }
            let task = active_rebuild_task(context, class_id).await;
            let _ = execute_computed_reindex_task(&context.pool, &task).await;
            tokio::task::yield_now().await;
        }
        panic!("computed-field rebuild did not reach ready state");
    }

    #[rstest::rstest]
    #[case::first_non_null(
        serde_json::json!({"type": "first_non_null", "paths": ["/missing", "/value"]}),
        "string",
        serde_json::json!({"value": "chosen"})
    )]
    #[case::sum(
        serde_json::json!({"type": "sum", "paths": ["/left", "/right"]}),
        "number",
        serde_json::json!({"left": 2.25, "right": 3.75})
    )]
    #[case::sum_rounds_a_power_boundary(
        serde_json::json!({"type": "sum", "paths": ["/left", "/right"]}),
        "number",
        serde_json::from_str(
            r#"{"left": 9999999999999999999999999999999999, "right": 6}"#
        )
        .unwrap()
    )]
    #[case::sum_preserves_precision(
        serde_json::json!({"type": "sum", "paths": ["/value"]}),
        "number",
        serde_json::from_str(r#"{"value": 9.999999999999999999999999999999999}"#).unwrap()
    )]
    #[case::average(
        serde_json::json!({"type": "average", "paths": ["/left", "/right"]}),
        "number",
        serde_json::json!({"left": 2, "right": 5})
    )]
    #[case::repeating_average(
        serde_json::json!({"type": "average", "paths": ["/left", "/right", "/third"]}),
        "number",
        serde_json::json!({"left": 1, "right": 0, "third": 0})
    )]
    #[case::minimum(
        serde_json::json!({"type": "min", "paths": ["/left", "/right"]}),
        "integer",
        serde_json::json!({"left": -4, "right": 9})
    )]
    #[case::maximum(
        serde_json::json!({"type": "max", "paths": ["/left", "/right"]}),
        "integer",
        serde_json::json!({"left": -4, "right": 9})
    )]
    #[case::all_present(
        serde_json::json!({"type": "all_present", "paths": ["/false", "/zero"]}),
        "boolean",
        serde_json::json!({"false": false, "zero": 0})
    )]
    #[case::any_present(
        serde_json::json!({"type": "any_present", "paths": ["/missing", "/empty"]}),
        "boolean",
        serde_json::json!({"empty": ""})
    )]
    #[case::count_present(
        serde_json::json!({"type": "count_present", "paths": ["/missing", "/null", "/array"]}),
        "integer",
        serde_json::json!({"null": null, "array": []})
    )]
    #[case::all_present_and_equal(
        serde_json::json!({"type": "all_present_and_equal", "paths": ["/left", "/right"]}),
        "boolean",
        serde_json::json!({"left": {"a": 1, "b": [2]}, "right": {"b": [2], "a": 1.0}})
    )]
    #[case::object_result(
        serde_json::json!({"type": "first_non_null", "paths": ["/value"]}),
        "object",
        serde_json::json!({"value": {"nested": true}})
    )]
    #[case::array_result(
        serde_json::json!({"type": "first_non_null", "paths": ["/value"]}),
        "array",
        serde_json::json!({"value": [3, 2, 1]})
    )]
    #[case::non_numeric_operand(
        serde_json::json!({"type": "sum", "paths": ["/left", "/right"]}),
        "number",
        serde_json::json!({"left": 2, "right": "invalid"})
    )]
    #[tokio::test]
    async fn database_scope_evaluator_matches_the_domain_operation_catalog(
        #[case] operation: serde_json::Value,
        #[case] result_type: &str,
        #[case] data: serde_json::Value,
    ) {
        let definition: hubuum_computed_fields::Definition =
            serde_json::from_value(serde_json::json!({
                "key": "sort_value",
                "label": "Sort value",
                "operation": operation.clone(),
                "result_type": result_type,
                "enabled": true
            }))
            .unwrap();
        let expected = hubuum_computed_fields::evaluate(
            &data,
            std::slice::from_ref(&definition),
            1,
            hubuum_computed_fields::EvaluationLimits::standard(),
        )
        .unwrap()
        .values
        .remove("sort_value")
        .unwrap();
        let pool = get_test_pool();
        let definitions = serde_json::to_value([definition]).unwrap();
        let actual = with_connection(pool.get_ref(), async |conn| {
            diesel::sql_query(
                "SELECT NULLIF(\
                    hubuum_computed_evaluate_scope($1, $2) \
                        -> 'values' -> 'sort_value', \
                    'null'::jsonb\
                ) AS value",
            )
            .bind::<diesel::sql_types::Jsonb, _>(&data)
            .bind::<diesel::sql_types::Jsonb, _>(&definitions)
            .get_result::<ComputedQuerySqlValue>(conn)
            .await
        })
        .await
        .unwrap()
        .value
        .unwrap_or(serde_json::Value::Null);
        match (actual.as_number(), expected.as_number()) {
            (Some(actual), Some(expected)) => assert_eq!(
                hubuum_computed_fields::compare_decimal_strings(
                    &actual.to_string(),
                    &expected.to_string()
                ),
                Some(std::cmp::Ordering::Equal),
                "operation: {operation}, data: {data}, actual: {actual}, expected: {expected}"
            ),
            _ => assert_eq!(actual, expected, "operation: {operation}, data: {data}"),
        }
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn database_scope_evaluator_preserves_the_empty_pointer_token(
        #[future(awt)] test_context: TestContext,
    ) {
        let definition = evaluator_definition(serde_json::json!({
            "key": "empty_key",
            "label": "Empty key",
            "operation": {"type": "first_non_null", "paths": ["/"]},
            "result_type": "string"
        }));
        let data = serde_json::json!({"": "selected"});
        let actual = evaluate_sql_scope(&test_context, &data, &[definition]).await;

        assert_eq!(actual["values"]["empty_key"], "selected");
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn database_source_hash_matches_the_domain_canonical_json(
        #[future(awt)] test_context: TestContext,
    ) {
        let data = serde_json::json!({
            "z": [1, {"quote": "'\"", "unicode": "blåbær 🫐"}],
            "a": {"nested": true, "control": "line\nbreak"}
        });
        let expected = source_data_sha256(&data).unwrap();

        let actual = with_connection(&test_context.pool, async |conn| {
            diesel::sql_query("SELECT hubuum_computed_source_sha256($1) AS value")
                .bind::<diesel::sql_types::Jsonb, _>(&data)
                .get_result::<ComputedSourceHashSqlValue>(conn)
                .await
        })
        .await
        .unwrap();

        assert_eq!(actual.value, expected);
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn database_scope_evaluator_measures_compact_json_input(
        #[future(awt)] test_context: TestContext,
    ) {
        let definition = evaluator_definition(serde_json::json!({
            "key": "present",
            "label": "Present",
            "operation": {"type": "count_present", "paths": ["/value"]},
            "result_type": "integer"
        }));
        let data = serde_json::json!({
            "value": "x".repeat(hubuum_computed_fields::MAX_INPUT_BYTES - 12)
        });
        assert_eq!(
            serde_json::to_vec(&data).unwrap().len(),
            hubuum_computed_fields::MAX_INPUT_BYTES
        );

        let actual = evaluate_sql_scope(&test_context, &data, &[definition]).await;

        assert_eq!(actual["values"]["present"], 1);
        assert_eq!(actual["errors"], serde_json::json!({}));
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn database_scope_evaluator_preserves_scope_output_limits(
        #[future(awt)] test_context: TestContext,
    ) {
        let definitions = (0..5)
            .map(|index| {
                evaluator_definition(serde_json::json!({
                    "key": format!("value_{index}"),
                    "label": format!("Value {index}"),
                    "operation": {"type": "first_non_null", "paths": ["/value"]},
                    "result_type": "string"
                }))
            })
            .collect::<Vec<_>>();
        let data = serde_json::json!({"value": "x".repeat(60_000)});
        let expected = hubuum_computed_fields::evaluate(
            &data,
            &definitions,
            definitions.len(),
            hubuum_computed_fields::EvaluationLimits::standard(),
        )
        .unwrap();

        let actual = evaluate_sql_scope(&test_context, &data, &definitions).await;

        assert_eq!(actual, serde_json::to_value(expected).unwrap());
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn database_scope_evaluator_preserves_scope_work_limits(
        #[future(awt)] test_context: TestContext,
    ) {
        let definitions = (0..9)
            .map(|index| {
                evaluator_definition(serde_json::json!({
                    "key": format!("equal_{index}"),
                    "label": format!("Equal {index}"),
                    "operation": {
                        "type": "all_present_and_equal",
                        "paths": ["/left", "/right"]
                    },
                    "result_type": "boolean"
                }))
            })
            .collect::<Vec<_>>();
        let values = (0..6_000).collect::<Vec<_>>();
        let data = serde_json::json!({"left": values.clone(), "right": values});
        let expected = hubuum_computed_fields::evaluate(
            &data,
            &definitions,
            definitions.len(),
            hubuum_computed_fields::EvaluationLimits::standard(),
        )
        .unwrap();

        let actual = evaluate_sql_scope(&test_context, &data, &definitions).await;

        assert_eq!(actual, serde_json::to_value(expected).unwrap());
    }

    async fn grant_normal_user(
        context: &TestContext,
        fixture: &crate::tests::ObjectFixture,
        permissions: &[Permissions],
    ) -> crate::models::Group {
        let group = create_test_group(&context.pool).await;
        group
            .add_member_without_events(&context.pool, &context.normal_user)
            .await
            .unwrap();
        for permission in permissions {
            fixture
                .collection
                .collection
                .grant_one(&context.pool, group.id, *permission)
                .await
                .unwrap();
        }
        group
    }

    #[derive(Clone, Copy)]
    enum ComputedQueryDenial {
        Scope,
        ListVisibility,
    }

    mod authorization;
    mod consistency;
    mod definitions;
    mod lifecycle;
    mod querying;
}
