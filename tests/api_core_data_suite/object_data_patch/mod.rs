#![cfg(test)]

use std::time::Duration;

use actix_web::{http::StatusCode, test};
use rstest::rstest;

use crate::db::prelude::*;
use crate::db::{with_connection, with_transaction};
use crate::events::{Event, EventContext};
use crate::models::traits::{PatchObjectData, ResolveObjectTarget};
use crate::models::{
    HubuumClassID, HubuumObject, HubuumObjectHistory, HubuumObjectID, MAX_OBJECT_DATA_PATCH_BYTES,
    NewHubuumClass, NewHubuumObject, NewObjectComputedData, ObjectComputedData,
    ObjectDataPatchDocument, ObjectSelector,
};
use crate::tests::api_operations::{
    patch_request, patch_request_with_content_type, patch_request_with_raw_body, post_request,
};
use crate::tests::asserts::assert_response_status;
use crate::tests::{TestContext, create_test_classes, test_context};
use crate::traits::{CanSave, SelfAccessors};

const JSON_PATCH_MEDIA_TYPE: &str = "application/json-patch+json";

fn data_patch_endpoint(class_id: i32, object_id: i32) -> String {
    format!("/api/v1/classes/{class_id}/{object_id}/data")
}

fn data_patch_by_name_endpoint(class_name: &str, object_name: &str) -> String {
    let encoded_class_name =
        percent_encoding::utf8_percent_encode(class_name, percent_encoding::NON_ALPHANUMERIC);
    let encoded_object_name =
        percent_encoding::utf8_percent_encode(object_name, percent_encoding::NON_ALPHANUMERIC);
    format!(
        "/api/v1/classes/by-name/{encoded_class_name}/objects/by-name/{encoded_object_name}/data"
    )
}

fn object_endpoint(class_id: i32, object_id: i32) -> String {
    format!("/api/v1/classes/{class_id}/{object_id}")
}

async fn object_fixture(
    context: &TestContext,
    label: &str,
    data: serde_json::Value,
) -> crate::tests::ObjectFixture {
    context
        .object_fixture(
            label,
            NewHubuumClass {
                collection_id: 0,
                name: context.scoped_name("JSON Patch class"),
                description: "JSON Patch class".to_string(),
                json_schema: None,
                validate_schema: Some(false),
            },
            vec![NewHubuumObject {
                collection_id: 0,
                hubuum_class_id: 0,
                name: context.scoped_name("JSON Patch object"),
                description: "JSON Patch object".to_string(),
                data,
            }],
        )
        .await
        .unwrap()
}

async fn object_history_count(context: &TestContext, object_id: i32) -> i64 {
    with_connection(&context.pool, async |conn| {
        use crate::schema::hubuumobject_history::dsl::{hubuumobject_history, id};
        hubuumobject_history
            .filter(id.eq(object_id))
            .count()
            .get_result(conn)
            .await
    })
    .await
    .unwrap()
}

async fn object_event_count(context: &TestContext, object_id: i32) -> i64 {
    with_connection(&context.pool, async |conn| {
        use crate::schema::events::dsl::{entity_id, entity_type, events};
        events
            .filter(entity_type.eq("object"))
            .filter(entity_id.eq(object_id))
            .count()
            .get_result(conn)
            .await
    })
    .await
    .unwrap()
}

async fn current_object(context: &TestContext, object_id: i32) -> HubuumObject {
    HubuumObjectID::new(object_id)
        .unwrap()
        .instance(&context.pool)
        .await
        .unwrap()
}

mod atomicity;
mod semantics;
mod validation;
