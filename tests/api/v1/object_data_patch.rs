#[cfg(test)]
mod tests {
    use std::time::Duration;

    use actix_web::{http::StatusCode, test};
    use rstest::rstest;

    use crate::db::prelude::*;
    use crate::db::{with_connection, with_transaction};
    use crate::events::{Event, EventContext};
    use crate::models::traits::{PatchObjectData, ResolveObjectTarget};
    use crate::models::{
        HubuumClassID, HubuumObject, HubuumObjectHistory, HubuumObjectID,
        MAX_OBJECT_DATA_PATCH_BYTES, NewHubuumClass, NewHubuumObject, NewObjectComputedData,
        ObjectComputedData, ObjectDataPatchDocument, ObjectSelector,
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

    #[rstest]
    #[case::missing_member(serde_json::json!({"keep": true}))]
    #[case::existing_member(serde_json::json!({
        "keep": true,
        "facts": {"source": "old", "hostname": "srv-01"}
    }))]
    #[actix_web::test]
    async fn add_facts_creates_or_completely_replaces_the_member(
        #[case] initial_data: serde_json::Value,
        #[future(awt)] test_context: TestContext,
    ) {
        let fixture = object_fixture(&test_context, "patch add facts", initial_data).await;
        let object = &fixture.objects[0];

        let response = patch_request_with_content_type(
            &test_context.pool,
            &test_context.admin_token,
            &data_patch_endpoint(fixture.class.id, object.id),
            serde_json::json!([
                {"op": "add", "path": "/facts", "value": {"source": "inventory"}}
            ]),
            JSON_PATCH_MEDIA_TYPE,
        )
        .await;
        let response = assert_response_status(response, StatusCode::OK).await;
        let updated: HubuumObject = test::read_body_json(response).await;

        assert_eq!(
            updated.data["facts"],
            serde_json::json!({"source": "inventory"})
        );
        assert_eq!(updated.data["keep"], true);
        fixture.cleanup().await.unwrap();
    }

    #[rstest]
    #[actix_web::test]
    async fn empty_path_replaces_the_complete_data_document(
        #[future(awt)] test_context: TestContext,
    ) {
        let fixture = object_fixture(
            &test_context,
            "patch root replacement",
            serde_json::json!({"old": true}),
        )
        .await;
        let object = &fixture.objects[0];

        let response = patch_request_with_content_type(
            &test_context.pool,
            &test_context.admin_token,
            &data_patch_endpoint(fixture.class.id, object.id),
            serde_json::json!([
                {"op": "replace", "path": "", "value": ["complete", "replacement"]}
            ]),
            JSON_PATCH_MEDIA_TYPE,
        )
        .await;
        let response = assert_response_status(response, StatusCode::OK).await;
        let updated: HubuumObject = test::read_body_json(response).await;

        assert_eq!(updated.data, serde_json::json!(["complete", "replacement"]));
        fixture.cleanup().await.unwrap();
    }

    #[rstest]
    #[actix_web::test]
    async fn by_name_path_percent_decodes_and_updates_the_named_object(
        #[future(awt)] test_context: TestContext,
    ) {
        let fixture = object_fixture(
            &test_context,
            "patch by encoded name",
            serde_json::json!({"state": "before"}),
        )
        .await;
        let mut object = fixture.objects[0].clone();
        object.name = test_context.scoped_name("rack/a b");
        object = object
            .save_without_events(&test_context.pool)
            .await
            .unwrap();

        let response = patch_request_with_content_type(
            &test_context.pool,
            &test_context.admin_token,
            &data_patch_by_name_endpoint(&fixture.class.name, &object.name),
            serde_json::json!([
                {"op": "replace", "path": "/state", "value": "after"}
            ]),
            JSON_PATCH_MEDIA_TYPE,
        )
        .await;
        let response = assert_response_status(response, StatusCode::OK).await;
        let updated: HubuumObject = test::read_body_json(response).await;

        assert_eq!(updated.id, object.id);
        assert_eq!(updated.data["state"], "after");
        fixture.cleanup().await.unwrap();
    }

    #[rstest]
    #[actix_web::test]
    async fn by_name_path_is_scoped_to_the_class(#[future(awt)] test_context: TestContext) {
        let classes = create_test_classes(&test_context, "JSON Patch name class scope").await;
        let shared_name = test_context.scoped_name("shared object name");
        let first = NewHubuumObject {
            collection_id: classes[0].collection_id,
            hubuum_class_id: classes[0].id,
            name: shared_name.clone(),
            description: "first class".to_string(),
            data: serde_json::json!({"class": "first"}),
        }
        .save_without_events(&test_context.pool)
        .await
        .unwrap();
        let second = NewHubuumObject {
            collection_id: classes[1].collection_id,
            hubuum_class_id: classes[1].id,
            name: shared_name.clone(),
            description: "second class".to_string(),
            data: serde_json::json!({"class": "second"}),
        }
        .save_without_events(&test_context.pool)
        .await
        .unwrap();

        let response = patch_request_with_content_type(
            &test_context.pool,
            &test_context.admin_token,
            &data_patch_by_name_endpoint(&classes[0].name, &shared_name),
            serde_json::json!([
                {"op": "replace", "path": "/class", "value": "patched"}
            ]),
            JSON_PATCH_MEDIA_TYPE,
        )
        .await;
        assert_response_status(response, StatusCode::OK).await;

        assert_eq!(
            current_object(&test_context, first.id).await.data["class"],
            "patched"
        );
        assert_eq!(
            current_object(&test_context, second.id).await.data["class"],
            "second"
        );
        crate::tests::cleanup_test_classes(&classes).await;
    }

    #[rstest]
    #[actix_web::test]
    async fn by_name_path_never_interprets_numeric_names_as_ids(
        #[future(awt)] test_context: TestContext,
    ) {
        let fixture = object_fixture(
            &test_context,
            "patch numeric names",
            serde_json::json!({"state": "before"}),
        )
        .await;
        let mut class = fixture.class.clone();
        class.name = (i32::MAX - class.id).to_string();
        class = class.save_without_events(&test_context.pool).await.unwrap();
        let mut object = fixture.objects[0].clone();
        object.name = (i32::MAX - object.id).to_string();
        object = object
            .save_without_events(&test_context.pool)
            .await
            .unwrap();

        let response = patch_request_with_content_type(
            &test_context.pool,
            &test_context.admin_token,
            &data_patch_by_name_endpoint(&class.name, &object.name),
            serde_json::json!([
                {"op": "replace", "path": "/state", "value": "after"}
            ]),
            JSON_PATCH_MEDIA_TYPE,
        )
        .await;
        let response = assert_response_status(response, StatusCode::OK).await;
        let updated: HubuumObject = test::read_body_json(response).await;

        assert_eq!(updated.id, object.id);
        assert_eq!(updated.data["state"], "after");
        fixture.cleanup().await.unwrap();
    }

    #[rstest]
    #[actix_web::test]
    async fn existing_object_patch_keeps_whole_data_replacement_semantics(
        #[future(awt)] test_context: TestContext,
    ) {
        let fixture = object_fixture(
            &test_context,
            "existing patch replacement",
            serde_json::json!({"nested": {"keep": true, "replace": "before"}}),
        )
        .await;
        let object = &fixture.objects[0];

        let response = patch_request(
            &test_context.pool,
            &test_context.admin_token,
            &object_endpoint(fixture.class.id, object.id),
            serde_json::json!({"data": {"nested": {"replace": "after"}}}),
        )
        .await;
        let response = assert_response_status(response, StatusCode::OK).await;
        let updated: HubuumObject = test::read_body_json(response).await;

        assert_eq!(
            updated.data,
            serde_json::json!({"nested": {"replace": "after"}})
        );
        fixture.cleanup().await.unwrap();
    }

    #[rstest]
    #[actix_web::test]
    async fn later_operation_failure_persists_none_of_the_patch(
        #[future(awt)] test_context: TestContext,
    ) {
        let fixture = object_fixture(
            &test_context,
            "later patch failure",
            serde_json::json!({"state": "before"}),
        )
        .await;
        let object = fixture.objects[0].clone();
        let history_before = object_history_count(&test_context, object.id).await;
        let events_before = object_event_count(&test_context, object.id).await;

        let response = patch_request_with_content_type(
            &test_context.pool,
            &test_context.admin_token,
            &data_patch_endpoint(fixture.class.id, object.id),
            serde_json::json!([
                {"op": "replace", "path": "/state", "value": "intermediate"},
                {"op": "remove", "path": "/missing"}
            ]),
            JSON_PATCH_MEDIA_TYPE,
        )
        .await;
        assert_response_status(response, StatusCode::CONFLICT).await;

        assert_eq!(current_object(&test_context, object.id).await, object);
        assert_eq!(
            object_history_count(&test_context, object.id).await,
            history_before
        );
        assert_eq!(
            object_event_count(&test_context, object.id).await,
            events_before
        );
        fixture.cleanup().await.unwrap();
    }

    #[rstest]
    #[actix_web::test]
    async fn failed_test_operation_leaves_the_object_unchanged(
        #[future(awt)] test_context: TestContext,
    ) {
        let fixture = object_fixture(
            &test_context,
            "failed test operation",
            serde_json::json!({"version": 1}),
        )
        .await;
        let object = fixture.objects[0].clone();

        let response = patch_request_with_content_type(
            &test_context.pool,
            &test_context.admin_token,
            &data_patch_endpoint(fixture.class.id, object.id),
            serde_json::json!([
                {"op": "test", "path": "/version", "value": 2},
                {"op": "replace", "path": "/version", "value": 3}
            ]),
            JSON_PATCH_MEDIA_TYPE,
        )
        .await;
        assert_response_status(response, StatusCode::CONFLICT).await;

        assert_eq!(current_object(&test_context, object.id).await, object);
        fixture.cleanup().await.unwrap();
    }

    #[rstest]
    #[actix_web::test]
    async fn schema_failure_preserves_data_computed_history_and_events(
        #[future(awt)] test_context: TestContext,
    ) {
        let collection = test_context
            .collection_fixture("JSON Patch schema rollback")
            .await;
        let class = NewHubuumClass {
            collection_id: collection.collection.id,
            name: test_context.scoped_name("JSON Patch schema class"),
            description: "JSON Patch schema class".to_string(),
            json_schema: Some(serde_json::json!({
                "type": "object",
                "required": ["latitude"],
                "properties": {"latitude": {"type": "number"}}
            })),
            validate_schema: Some(true),
        }
        .save_without_events(&test_context.pool)
        .await
        .unwrap();
        let object = NewHubuumObject {
            collection_id: collection.collection.id,
            hubuum_class_id: class.id,
            name: test_context.scoped_name("JSON Patch schema object"),
            description: "JSON Patch schema object".to_string(),
            data: serde_json::json!({"latitude": 59.9}),
        }
        .save_without_events(&test_context.pool)
        .await
        .unwrap();
        let computed_before = NewObjectComputedData {
            object_id: object.id,
            class_id: class.id,
            evaluation_revision: 7,
            source_data_sha256: "0".repeat(64),
            values: serde_json::json!({"stable": "value"}),
            errors: serde_json::json!({}),
        };
        with_connection(&test_context.pool, async |conn| {
            use crate::schema::object_computed_data::dsl::object_computed_data;
            diesel::insert_into(object_computed_data)
                .values(&computed_before)
                .execute(conn)
                .await
        })
        .await
        .unwrap();
        let history_before = object_history_count(&test_context, object.id).await;
        let events_before = object_event_count(&test_context, object.id).await;

        let response = patch_request_with_content_type(
            &test_context.pool,
            &test_context.admin_token,
            &data_patch_endpoint(class.id, object.id),
            serde_json::json!([
                {"op": "replace", "path": "/latitude", "value": "north"}
            ]),
            JSON_PATCH_MEDIA_TYPE,
        )
        .await;
        assert_response_status(response, StatusCode::NOT_ACCEPTABLE).await;

        assert_eq!(current_object(&test_context, object.id).await, object);
        let computed_after = with_connection(&test_context.pool, async |conn| {
            use crate::schema::object_computed_data::dsl::{object_computed_data, object_id};
            object_computed_data
                .filter(object_id.eq(object.id))
                .first::<ObjectComputedData>(conn)
                .await
        })
        .await
        .unwrap();
        assert_eq!(computed_after.evaluation_revision, 7);
        assert_eq!(computed_after.source_data_sha256, "0".repeat(64));
        assert_eq!(
            computed_after.values,
            serde_json::json!({"stable": "value"})
        );
        assert_eq!(
            object_history_count(&test_context, object.id).await,
            history_before
        );
        assert_eq!(
            object_event_count(&test_context, object.id).await,
            events_before
        );
        collection.cleanup().await.unwrap();
    }

    #[rstest]
    #[actix_web::test]
    async fn computed_materialization_failure_rolls_back_data_history_and_events(
        #[future(awt)] test_context: TestContext,
    ) {
        let fixture = object_fixture(
            &test_context,
            "computed materialization rollback",
            serde_json::json!({"inventory": {"hostname": "before.example"}}),
        )
        .await;
        let response = post_request(
            &test_context.pool,
            &test_context.admin_token,
            &format!("/api/v1/classes/{}/computed-fields", fixture.class.id),
            serde_json::json!({
                "key": "display_name",
                "label": "Display name",
                "description": "",
                "operation": {
                    "type": "first_non_null",
                    "paths": ["/inventory/hostname"]
                },
                "result_type": "string",
                "enabled": true
            }),
        )
        .await;
        assert_response_status(response, StatusCode::CREATED).await;
        let before = fixture.objects[0]
            .save_without_events(&test_context.pool)
            .await
            .unwrap();
        let computed_before = with_connection(&test_context.pool, async |conn| {
            use crate::schema::object_computed_data::dsl::{object_computed_data, object_id};
            object_computed_data
                .filter(object_id.eq(before.id))
                .first::<ObjectComputedData>(conn)
                .await
        })
        .await
        .unwrap();
        with_connection(&test_context.pool, async |conn| {
            use crate::schema::computed_field_definitions::dsl::{
                class_id, computed_field_definitions, operation,
            };
            diesel::update(computed_field_definitions.filter(class_id.eq(fixture.class.id)))
                .set(operation.eq(serde_json::json!({"type": "invalid"})))
                .execute(conn)
                .await
        })
        .await
        .unwrap();
        let history_before = object_history_count(&test_context, before.id).await;
        let events_before = object_event_count(&test_context, before.id).await;

        let response = patch_request_with_content_type(
            &test_context.pool,
            &test_context.admin_token,
            &data_patch_endpoint(fixture.class.id, before.id),
            serde_json::json!([
                {"op": "replace", "path": "/inventory/hostname", "value": "after.example"}
            ]),
            JSON_PATCH_MEDIA_TYPE,
        )
        .await;
        assert_response_status(response, StatusCode::INTERNAL_SERVER_ERROR).await;

        assert_eq!(current_object(&test_context, before.id).await, before);
        let computed_after = with_connection(&test_context.pool, async |conn| {
            use crate::schema::object_computed_data::dsl::{object_computed_data, object_id};
            object_computed_data
                .filter(object_id.eq(before.id))
                .first::<ObjectComputedData>(conn)
                .await
        })
        .await
        .unwrap();
        assert_eq!(computed_after.class_id, computed_before.class_id);
        assert_eq!(
            computed_after.evaluation_revision,
            computed_before.evaluation_revision
        );
        assert_eq!(
            computed_after.source_data_sha256,
            computed_before.source_data_sha256
        );
        assert_eq!(computed_after.values, computed_before.values);
        assert_eq!(computed_after.errors, computed_before.errors);
        assert_eq!(computed_after.computed_at, computed_before.computed_at);
        assert_eq!(
            object_history_count(&test_context, before.id).await,
            history_before
        );
        assert_eq!(
            object_event_count(&test_context, before.id).await,
            events_before
        );
        fixture.cleanup().await.unwrap();
    }

    #[rstest]
    #[actix_web::test]
    async fn successful_patch_updates_computed_history_event_and_timestamp_together(
        #[future(awt)] test_context: TestContext,
    ) {
        let fixture = object_fixture(
            &test_context,
            "successful patch side effects",
            serde_json::json!({"inventory": {"hostname": "before.example"}}),
        )
        .await;
        let before = fixture.objects[0].clone();
        let definition_response = post_request(
            &test_context.pool,
            &test_context.admin_token,
            &format!("/api/v1/classes/{}/computed-fields", fixture.class.id),
            serde_json::json!({
                "key": "display_name",
                "label": "Display name",
                "description": "",
                "operation": {
                    "type": "first_non_null",
                    "paths": ["/inventory/hostname"]
                },
                "result_type": "string",
                "enabled": true
            }),
        )
        .await;
        assert_response_status(definition_response, StatusCode::CREATED).await;
        let history_before = object_history_count(&test_context, before.id).await;
        tokio::time::sleep(Duration::from_millis(2)).await;

        let response = patch_request_with_content_type(
            &test_context.pool,
            &test_context.admin_token,
            &data_patch_endpoint(fixture.class.id, before.id),
            serde_json::json!([
                {"op": "replace", "path": "/inventory/hostname", "value": "after.example"}
            ]),
            JSON_PATCH_MEDIA_TYPE,
        )
        .await;
        let response = assert_response_status(response, StatusCode::OK).await;
        let updated: HubuumObject = test::read_body_json(response).await;

        assert!(updated.updated_at > before.updated_at);
        let (computed, history, events) = with_connection(&test_context.pool, async |conn| {
            use crate::schema::events::dsl as event;
            use crate::schema::hubuumobject_history::dsl as history;
            use crate::schema::object_computed_data::dsl as computed;

            let computed = computed::object_computed_data
                .filter(computed::object_id.eq(before.id))
                .first::<ObjectComputedData>(conn)
                .await?;
            let history = history::hubuumobject_history
                .filter(history::id.eq(before.id))
                .order(history::history_id.asc())
                .load::<HubuumObjectHistory>(conn)
                .await?;
            let events = event::events
                .filter(event::entity_type.eq("object"))
                .filter(event::entity_id.eq(before.id))
                .order(event::id.asc())
                .select(Event::as_select())
                .load::<Event>(conn)
                .await?;
            Ok::<_, diesel::result::Error>((computed, history, events))
        })
        .await
        .unwrap();

        assert_eq!(computed.values["display_name"], "after.example");
        assert_eq!(
            computed.source_data_sha256,
            crate::db::traits::computed_field::source_data_sha256(&updated.data).unwrap()
        );
        assert_eq!(history.len() as i64, history_before + 1);
        assert_eq!(history.last().unwrap().data, updated.data);
        assert_eq!(history.last().unwrap().op, "U");
        assert_eq!(
            history.last().unwrap().actor_id,
            Some(test_context.admin_user.id)
        );
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].before.as_ref().unwrap()["data"], before.data);
        assert_eq!(events[0].after.as_ref().unwrap()["data"], updated.data);
        fixture.cleanup().await.unwrap();
    }

    #[rstest]
    #[actix_web::test]
    async fn no_op_patch_keeps_timestamp_history_and_events_unchanged(
        #[future(awt)] test_context: TestContext,
    ) {
        let fixture = object_fixture(
            &test_context,
            "no-op patch",
            serde_json::json!({"state": "same"}),
        )
        .await;
        let before = fixture.objects[0].clone();
        let history_before = object_history_count(&test_context, before.id).await;
        let events_before = object_event_count(&test_context, before.id).await;

        let response = patch_request_with_content_type(
            &test_context.pool,
            &test_context.admin_token,
            &data_patch_endpoint(fixture.class.id, before.id),
            serde_json::json!([]),
            JSON_PATCH_MEDIA_TYPE,
        )
        .await;
        let response = assert_response_status(response, StatusCode::OK).await;
        let unchanged: HubuumObject = test::read_body_json(response).await;

        assert_eq!(unchanged, before);
        assert_eq!(
            object_history_count(&test_context, before.id).await,
            history_before
        );
        assert_eq!(
            object_event_count(&test_context, before.id).await,
            events_before
        );
        fixture.cleanup().await.unwrap();
    }

    #[rstest]
    #[actix_web::test]
    async fn concurrent_patches_compose_from_the_latest_row_locked_data(
        #[future(awt)] test_context: TestContext,
    ) {
        let fixture = object_fixture(
            &test_context,
            "concurrent patches",
            serde_json::json!({"base": true}),
        )
        .await;
        let object = fixture.objects[0].clone();
        let object_id = object.id;
        let first_pool = test_context.pool.clone();
        let second_pool = test_context.pool.clone();
        let actor_id = test_context.admin_user.id;
        let first_patch: ObjectDataPatchDocument = serde_json::from_value(serde_json::json!([
            {"op": "add", "path": "/first", "value": 1}
        ]))
        .unwrap();
        let second_patch: ObjectDataPatchDocument = serde_json::from_value(serde_json::json!([
            {"op": "add", "path": "/second", "value": 2}
        ]))
        .unwrap();
        let target = ObjectSelector::by_id(
            HubuumClassID::new(fixture.class.id).unwrap(),
            HubuumObjectID::new(object_id).unwrap(),
        )
        .resolve_object_target(&test_context.pool)
        .await
        .unwrap();
        let first_target = target.clone();

        let (first, second) = with_transaction(&test_context.pool, async |conn| {
            use crate::schema::hubuumobject::dsl::{hubuumobject, id};
            hubuumobject
                .filter(id.eq(object_id))
                .for_update()
                .first::<HubuumObject>(conn)
                .await?;

            let first = tokio::spawn(async move {
                first_patch
                    .patch_object_data(
                        &first_pool,
                        &first_target,
                        &EventContext::user(actor_id, None, None),
                    )
                    .await
            });
            let second = tokio::spawn(async move {
                second_patch
                    .patch_object_data(
                        &second_pool,
                        &target,
                        &EventContext::user(actor_id, None, None),
                    )
                    .await
            });
            tokio::time::sleep(Duration::from_millis(100)).await;
            Ok::<_, diesel::result::Error>((first, second))
        })
        .await
        .unwrap();

        first.await.unwrap().unwrap();
        second.await.unwrap().unwrap();
        assert_eq!(
            current_object(&test_context, object.id).await.data,
            serde_json::json!({"base": true, "first": 1, "second": 2})
        );
        fixture.cleanup().await.unwrap();
    }

    #[rstest]
    #[actix_web::test]
    async fn class_mismatch_returns_not_found(#[future(awt)] test_context: TestContext) {
        let classes = create_test_classes(&test_context, "JSON Patch class mismatch").await;
        let object = NewHubuumObject {
            collection_id: classes[0].collection_id,
            hubuum_class_id: classes[0].id,
            name: test_context.scoped_name("JSON Patch mismatch object"),
            description: "JSON Patch mismatch object".to_string(),
            data: serde_json::json!({}),
        }
        .save_without_events(&test_context.pool)
        .await
        .unwrap();

        let response = patch_request_with_content_type(
            &test_context.pool,
            &test_context.admin_token,
            &data_patch_endpoint(classes[1].id, object.id),
            serde_json::json!([]),
            JSON_PATCH_MEDIA_TYPE,
        )
        .await;

        assert_response_status(response, StatusCode::NOT_FOUND).await;
        crate::tests::cleanup_test_classes(&classes).await;
    }

    #[rstest]
    #[actix_web::test]
    async fn missing_object_returns_not_found(#[future(awt)] test_context: TestContext) {
        let fixture =
            object_fixture(&test_context, "missing patch object", serde_json::json!({})).await;

        let response = patch_request_with_content_type(
            &test_context.pool,
            &test_context.admin_token,
            &data_patch_endpoint(fixture.class.id, i32::MAX),
            serde_json::json!([]),
            JSON_PATCH_MEDIA_TYPE,
        )
        .await;

        assert_response_status(response, StatusCode::NOT_FOUND).await;
        fixture.cleanup().await.unwrap();
    }

    #[rstest]
    #[actix_web::test]
    async fn missing_authentication_returns_unauthorized(#[future(awt)] test_context: TestContext) {
        let fixture = object_fixture(
            &test_context,
            "unauthenticated patch",
            serde_json::json!({}),
        )
        .await;

        let response = patch_request_with_content_type(
            &test_context.pool,
            "not-a-valid-token",
            &data_patch_endpoint(fixture.class.id, fixture.objects[0].id),
            serde_json::json!([]),
            JSON_PATCH_MEDIA_TYPE,
        )
        .await;

        assert_response_status(response, StatusCode::UNAUTHORIZED).await;
        fixture.cleanup().await.unwrap();
    }

    #[rstest]
    #[actix_web::test]
    async fn insufficient_permission_returns_forbidden(#[future(awt)] test_context: TestContext) {
        let fixture = object_fixture(&test_context, "forbidden patch", serde_json::json!({})).await;

        let response = patch_request_with_content_type(
            &test_context.pool,
            &test_context.normal_token,
            &data_patch_endpoint(fixture.class.id, fixture.objects[0].id),
            serde_json::json!([]),
            JSON_PATCH_MEDIA_TYPE,
        )
        .await;

        assert_response_status(response, StatusCode::FORBIDDEN).await;
        fixture.cleanup().await.unwrap();
    }

    #[rstest]
    #[actix_web::test]
    async fn malformed_json_returns_bad_request(#[future(awt)] test_context: TestContext) {
        let fixture = object_fixture(&test_context, "malformed patch", serde_json::json!({})).await;

        let response = patch_request_with_raw_body(
            &test_context.pool,
            &test_context.admin_token,
            &data_patch_endpoint(fixture.class.id, fixture.objects[0].id),
            br#"[{"op":]"#.as_slice(),
            JSON_PATCH_MEDIA_TYPE,
        )
        .await;

        assert_response_status(response, StatusCode::BAD_REQUEST).await;
        fixture.cleanup().await.unwrap();
    }

    #[rstest]
    #[actix_web::test]
    async fn invalid_patch_structure_returns_bad_request(#[future(awt)] test_context: TestContext) {
        let fixture = object_fixture(&test_context, "invalid patch", serde_json::json!({})).await;

        let response = patch_request_with_content_type(
            &test_context.pool,
            &test_context.admin_token,
            &data_patch_endpoint(fixture.class.id, fixture.objects[0].id),
            serde_json::json!({"op": "add", "path": "/value", "value": 1}),
            JSON_PATCH_MEDIA_TYPE,
        )
        .await;

        assert_response_status(response, StatusCode::BAD_REQUEST).await;
        fixture.cleanup().await.unwrap();
    }

    #[rstest]
    #[actix_web::test]
    async fn incorrect_content_type_returns_unsupported_media_type(
        #[future(awt)] test_context: TestContext,
    ) {
        let fixture = object_fixture(
            &test_context,
            "incorrect content type",
            serde_json::json!({}),
        )
        .await;

        let response = patch_request_with_content_type(
            &test_context.pool,
            &test_context.admin_token,
            &data_patch_endpoint(fixture.class.id, fixture.objects[0].id),
            serde_json::json!([]),
            "application/json",
        )
        .await;

        assert_response_status(response, StatusCode::UNSUPPORTED_MEDIA_TYPE).await;
        fixture.cleanup().await.unwrap();
    }

    #[rstest]
    #[actix_web::test]
    async fn oversized_patch_returns_payload_too_large(#[future(awt)] test_context: TestContext) {
        let fixture = object_fixture(&test_context, "oversized patch", serde_json::json!({})).await;
        let body = vec![b' '; 2_097_153];

        let response = patch_request_with_raw_body(
            &test_context.pool,
            &test_context.admin_token,
            &data_patch_endpoint(fixture.class.id, fixture.objects[0].id),
            body,
            JSON_PATCH_MEDIA_TYPE,
        )
        .await;

        assert_response_status(response, StatusCode::PAYLOAD_TOO_LARGE).await;
        fixture.cleanup().await.unwrap();
    }

    #[rstest]
    #[actix_web::test]
    async fn patch_result_larger_than_object_data_limit_returns_payload_too_large(
        #[future(awt)] test_context: TestContext,
    ) {
        let blob = "x".repeat(MAX_OBJECT_DATA_PATCH_BYTES / 2 + 1);
        let fixture = object_fixture(
            &test_context,
            "oversized patch result",
            serde_json::json!({"blob": blob}),
        )
        .await;

        let response = patch_request_with_content_type(
            &test_context.pool,
            &test_context.admin_token,
            &data_patch_endpoint(fixture.class.id, fixture.objects[0].id),
            serde_json::json!([
                {"op": "copy", "from": "/blob", "path": "/copy"}
            ]),
            JSON_PATCH_MEDIA_TYPE,
        )
        .await;

        assert_response_status(response, StatusCode::PAYLOAD_TOO_LARGE).await;
        fixture.cleanup().await.unwrap();
    }
}
