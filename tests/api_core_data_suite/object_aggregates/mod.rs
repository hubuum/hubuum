use std::sync::Arc;
use std::time::Duration;

use actix_web::{App, http::StatusCode, test, web::Data};
use base64::Engine;

use crate::db::traits::computed_field::{
    class_computation_state_for, create_personal_definition, create_shared_definition,
    execute_computed_reindex_task, update_shared_definition,
};
use crate::events::EventContext;
use crate::models::{
    ComputedFieldDefinitionPatch, ComputedFieldDefinitionRequest, HubuumObject,
    MAX_OBJECT_AGGREGATE_CURSOR_LENGTH, NewHubuumClass, NewHubuumObject, Permissions,
    ServiceAccountID, TaskID, UpdateHubuumObject,
};
use crate::pagination::{NEXT_CURSOR_HEADER, TOTAL_COUNT_HEADER};
use crate::permissions::test_support::mock_treetop::{MockAllowRule, MockTreetopBackend};
use crate::permissions::{AppContext, PermissionBackend, ResourceAttrs, ResourceKind};
use crate::tests::api_operations::get_request;
use crate::tests::asserts::{assert_response_status, header_value};
use crate::tests::{
    ObjectFixture, TestContext, create_test_group, create_test_service_account, scoped_token,
    service_account_token, test_context,
};
use crate::traits::{CanDelete, CanUpdate, PermissionController, SelfAccessors};

async fn fixture(context: &TestContext, label: &str) -> ObjectFixture {
    let object = |name: &str, description: &str, data: serde_json::Value| NewHubuumObject {
        collection_id: 0,
        hubuum_class_id: 0,
        name: context.scoped_name(name),
        description: description.to_string(),
        data,
    };
    context
        .object_fixture(
            label,
            NewHubuumClass {
                collection_id: 0,
                name: context.scoped_name(&format!("object aggregate class {label}")),
                description: "Object aggregate test class".to_string(),
                json_schema: None,
                validate_schema: Some(false),
            },
            vec![
                object(
                    "group object one",
                    "alpha",
                    serde_json::json!({
                        "status": "active",
                        "location": {"country": "NO"},
                        "typed": "text",
                        "nullable": "present",
                        "bucket": "a",
                        "amount": 10
                    }),
                ),
                object(
                    "group object two",
                    "alpha",
                    serde_json::json!({
                        "status": "active",
                        "location": {"country": "NO"},
                        "typed": 7,
                        "nullable": null,
                        "bucket": null,
                        "amount": 20.5
                    }),
                ),
                object(
                    "group object three",
                    "beta",
                    serde_json::json!({
                        "status": "inactive",
                        "location": {"country": "SE"},
                        "typed": true,
                        "bucket": 12,
                        "amount": null
                    }),
                ),
                object(
                    "group object four",
                    "beta",
                    serde_json::json!({
                        "status": "active",
                        "location": {"country": ["NO"]},
                        "typed": ["x"],
                        "amount": "not numeric"
                    }),
                ),
                object(
                    "group object five",
                    "gamma",
                    serde_json::json!({
                        "status": "active",
                        "typed": {"nested_null": null},
                        "bucket": "a"
                    }),
                ),
            ],
        )
        .await
        .unwrap()
}

#[derive(Debug, PartialEq)]
struct AggregatePage {
    rows: Vec<serde_json::Value>,
    total_count: Option<String>,
    cache_control: Option<String>,
}

async fn aggregate_rows(
    context: &TestContext,
    fixture: &ObjectFixture,
    token: &str,
    query: &str,
) -> AggregatePage {
    aggregate_rows_at_path(
        context,
        token,
        &format!("/api/v1/classes/{}/object-aggregates", fixture.class.id),
        query,
    )
    .await
}

async fn aggregate_rows_at_path(
    context: &TestContext,
    token: &str,
    path: &str,
    query: &str,
) -> AggregatePage {
    let response = get_request(&context.pool, token, &format!("{path}?{query}")).await;
    let response = assert_response_status(response, StatusCode::OK).await;
    let total_count = header_value(&response, TOTAL_COUNT_HEADER);
    let cache_control = header_value(&response, "Cache-Control");
    let rows = test::read_body_json(response).await;
    AggregatePage {
        rows,
        total_count,
        cache_control,
    }
}

fn summed_count(rows: &[serde_json::Value]) -> i64 {
    rows.iter()
        .map(|row| row["object_count"].as_i64().unwrap())
        .sum()
}

fn encoded_aggregate_cursor(sort_key: serde_json::Value, object_count: i64) -> String {
    let token = serde_json::json!({
        "version": 1,
        "dimensions": ["name"],
        "sort": "dimensions_ascending",
        "sort_key": sort_key,
        "object_count": object_count,
    });
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(serde_json::to_vec(&token).unwrap())
}

fn computed_definition(key: &str, path: &str, enabled: bool) -> ComputedFieldDefinitionRequest {
    serde_json::from_value(serde_json::json!({
        "key": key,
        "label": key,
        "description": "",
        "operation": {"type": "first_non_null", "paths": [path]},
        "result_type": "string",
        "enabled": enabled
    }))
    .unwrap()
}

fn numeric_computed_definition(
    key: &str,
    path: &str,
    enabled: bool,
) -> ComputedFieldDefinitionRequest {
    serde_json::from_value(serde_json::json!({
        "key": key,
        "label": key,
        "description": "",
        "operation": {"type": "first_non_null", "paths": [path]},
        "result_type": "number",
        "enabled": enabled
    }))
    .unwrap()
}

async fn finish_active_rebuild(context: &TestContext, class_id: i32) {
    for _ in 0..20 {
        let state = class_computation_state_for(&context.pool, class_id)
            .await
            .unwrap();
        if state.active_task_id.is_none() {
            return;
        }
        let task = TaskID::new(state.active_task_id.unwrap())
            .unwrap()
            .instance(&context.pool)
            .await
            .unwrap();
        let _ = execute_computed_reindex_task(&context.pool, &task).await;
        tokio::task::yield_now().await;
    }
    panic!("computed-field rebuild did not finish");
}

async fn grant_normal_user_read_access(
    context: &TestContext,
    fixture: &ObjectFixture,
) -> crate::models::Group {
    let group = create_test_group(&context.pool).await;
    group
        .add_member_without_events(&context.pool, &context.normal_user)
        .await
        .unwrap();
    for permission in [
        Permissions::ReadCollection,
        Permissions::ReadClass,
        Permissions::ReadObject,
    ] {
        fixture
            .collection
            .collection
            .grant_one(&context.pool, group.id, permission)
            .await
            .unwrap();
    }
    group
}

mod basic;
mod computed;
mod contracts;
mod cursor;
mod external_authorization;
