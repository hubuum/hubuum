use super::*;

use diesel::sql_types::{Bool, Integer};

use crate::db::DbPool;

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

const COMPUTED_CLASS_LOCK_NAMESPACE: i32 = 1_133_113;

#[derive(QueryableByName)]
struct WaitingLock {
    #[diesel(sql_type = Bool)]
    waiting: bool,
}

async fn wait_for_computed_lock_waiter(pool: &DbPool, class_id: i32) {
    for _ in 0..100 {
        let waiting = with_connection(pool, async |conn| {
            diesel::sql_query(
                "SELECT EXISTS (\
                    SELECT 1 FROM pg_locks \
                    WHERE locktype = 'advisory' \
                      AND classid = $1::oid \
                      AND objid = $2::oid \
                      AND objsubid = 2 \
                      AND NOT granted\
                ) AS waiting",
            )
            .bind::<Integer, _>(COMPUTED_CLASS_LOCK_NAMESPACE)
            .bind::<Integer, _>(class_id)
            .get_result::<WaitingLock>(conn)
            .await
        })
        .await
        .unwrap()
        .waiting;
        if waiting {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("JSON Patch did not reach computed materialization");
}

#[rstest]
#[actix_web::test]
async fn object_data_patch_holds_the_class_schema_lock_until_commit(
    #[future(awt)] test_context: TestContext,
) {
    let fixture = test_context
        .object_fixture(
            "object patch class lock",
            NewHubuumClass {
                collection_id: 0,
                name: test_context.scoped_name("object patch class lock"),
                description: String::new(),
                json_schema: Some(serde_json::json!({
                    "type": "object",
                    "additionalProperties": true
                })),
                validate_schema: Some(true),
            },
            vec![NewHubuumObject {
                collection_id: 0,
                hubuum_class_id: 0,
                name: test_context.scoped_name("object patch class lock"),
                description: String::new(),
                data: serde_json::json!({"before": true}),
            }],
        )
        .await
        .unwrap();
    let object = fixture.objects[0].clone();
    let target = ObjectSelector::by_id(
        HubuumClassID::new(fixture.class.id).unwrap(),
        crate::models::HubuumObjectID::new(object.id).unwrap(),
    )
    .resolve_object_target(&test_context.pool)
    .await
    .unwrap();
    let patch: ObjectDataPatchDocument = serde_json::from_value(serde_json::json!([
        {"op": "add", "path": "/after", "value": true}
    ]))
    .unwrap();
    let patch_pool = test_context.pool.clone();
    let schema_pool = test_context.pool.clone();
    let class_id = fixture.class.id;

    let (patch_task, schema_task) = with_transaction(&test_context.pool, async |conn| {
        diesel::sql_query("SELECT pg_advisory_xact_lock($1, $2)")
            .bind::<Integer, _>(COMPUTED_CLASS_LOCK_NAMESPACE)
            .bind::<Integer, _>(class_id)
            .execute(conn)
            .await?;

        let patch_task = tokio::spawn(async move {
            patch
                .patch_object_data(&patch_pool, &target, &EventContext::system())
                .await
        });
        wait_for_computed_lock_waiter(&test_context.pool, class_id).await;

        let mut schema_task = tokio::spawn(async move {
            with_connection(&schema_pool, async |conn| {
                use crate::schema::hubuumclass::dsl::{hubuumclass, id, json_schema};

                diesel::update(hubuumclass.filter(id.eq(class_id)))
                    .set(json_schema.eq(Some(serde_json::json!({
                        "type": "object",
                        "additionalProperties": false,
                        "properties": {"before": {"type": "boolean"}}
                    }))))
                    .execute(conn)
                    .await
            })
            .await
        });
        assert!(
            tokio::time::timeout(Duration::from_millis(150), &mut schema_task)
                .await
                .is_err(),
            "class schema update must wait for the object patch transaction"
        );
        Ok::<_, diesel::result::Error>((patch_task, schema_task))
    })
    .await
    .unwrap();

    patch_task.await.unwrap().unwrap();
    schema_task.await.unwrap().unwrap();
    fixture.cleanup().await.unwrap();
}
