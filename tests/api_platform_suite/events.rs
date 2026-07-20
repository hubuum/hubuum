#[cfg(test)]
mod tests {
    use actix_web::{http::StatusCode, test};
    use rstest::rstest;
    use serde_json::json;

    use crate::db::with_connection;
    use crate::events::{Action, ActorKind, EntityType, EventResponse, NewEvent, emit_event};
    use crate::models::{NewHubuumClass, NewHubuumObject, Permissions, PermissionsList};
    use crate::tests::TestContext;
    use crate::tests::api_operations::get_request;
    use crate::tests::asserts::{assert_response_status, header_value};
    use crate::tests::{create_test_group, create_test_user};
    use crate::traits::{CanSave, PermissionController};

    const EVENTS_ENDPOINT: &str = "/api/v1/events";

    async fn emit_test_event(pool: &crate::db::DbPool, event: &NewEvent) -> EventResponse {
        with_connection(pool, async |conn| emit_event(conn, event).await)
            .await
            .expect("failed to emit test event")
            .into()
    }

    #[actix_web::test]
    async fn test_events_endpoint_requires_read_audit_permission() {
        let context = TestContext::new().await;
        let collection = context
            .collection_fixture("audit_requires_permission")
            .await;

        let event = emit_test_event(
            &context.pool,
            &NewEvent::new(
                EntityType::Collection,
                Action::Created,
                ActorKind::System,
                "collection audit permission test",
            )
            .unwrap()
            .with_collection_id(collection.collection.id)
            .with_entity_id(collection.collection.id)
            .with_entity_name(&collection.collection.name),
        )
        .await;

        let resp = get_request(&context.pool, &context.normal_token, EVENTS_ENDPOINT).await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let rows: Vec<EventResponse> = test::read_body_json(resp).await;
        assert!(rows.is_empty());

        let group = create_test_group(&context.pool).await;
        group
            .add_member_without_events(&context.pool, &context.normal_user)
            .await
            .unwrap();
        collection
            .collection
            .grant_without_events(
                &context.pool,
                group.id,
                PermissionsList::new([Permissions::ReadAudit]),
            )
            .await
            .unwrap();

        let resp = get_request(&context.pool, &context.normal_token, EVENTS_ENDPOINT).await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let rows: Vec<EventResponse> = test::read_body_json(resp).await;
        assert!(rows.iter().any(|row| row.id == event.id));
    }

    #[actix_web::test]
    async fn events_endpoint_rejects_computed_filters() {
        let context = TestContext::new().await;

        let response = get_request(
            &context.pool,
            &context.admin_token,
            &format!("{EVENTS_ENDPOINT}?computed.shared.rank=1"),
        )
        .await;

        assert_response_status(response, StatusCode::BAD_REQUEST).await;
    }

    #[actix_web::test]
    async fn test_events_endpoint_filters_and_hides_collection_less_for_non_admin() {
        let context = TestContext::new().await;
        let collection = context.collection_fixture("audit_filters").await;

        let user = create_test_user(&context.pool).await;
        let user_token = user.create_token(&context.pool).await.unwrap().get_token();
        let group = create_test_group(&context.pool).await;
        group
            .add_member_without_events(&context.pool, &user)
            .await
            .unwrap();
        collection
            .collection
            .grant_without_events(
                &context.pool,
                group.id,
                PermissionsList::new([Permissions::ReadAudit]),
            )
            .await
            .unwrap();

        let collectiond_event = emit_test_event(
            &context.pool,
            &NewEvent::new(
                EntityType::Collection,
                Action::Created,
                ActorKind::System,
                "collection audit filter test",
            )
            .unwrap()
            .with_collection_id(collection.collection.id)
            .with_entity_id(collection.collection.id)
            .with_entity_name(&collection.collection.name),
        )
        .await;
        let collection_less_event = emit_test_event(
            &context.pool,
            &NewEvent::new(
                EntityType::Token,
                Action::Created,
                ActorKind::System,
                "collection-less audit filter test",
            )
            .unwrap(),
        )
        .await;

        let endpoint = format!(
            "{EVENTS_ENDPOINT}?entity_type=collection&action=created&collection_id={}",
            collection.collection.id
        );
        let resp = get_request(&context.pool, &user_token, &endpoint).await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let total_count = header_value(&resp, "X-Total-Count")
            .and_then(|value| value.parse::<i64>().ok())
            .expect("events endpoint should include a valid X-Total-Count header");
        let rows: Vec<EventResponse> = test::read_body_json(resp).await;
        assert_eq!(total_count, 1);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].id, collectiond_event.id);

        let resp = get_request(&context.pool, &user_token, EVENTS_ENDPOINT).await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let rows: Vec<EventResponse> = test::read_body_json(resp).await;
        assert!(!rows.iter().any(|row| row.id == collection_less_event.id));

        let admin_endpoint = format!("{EVENTS_ENDPOINT}?entity_type=token&action=created");
        let resp = get_request(&context.pool, &context.admin_token, &admin_endpoint).await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let rows: Vec<EventResponse> = test::read_body_json(resp).await;
        assert!(rows.iter().any(|row| row.id == collection_less_event.id));
    }

    #[actix_web::test]
    async fn test_entity_events_endpoint_applies_route_entity_filter() {
        let context = TestContext::new().await;
        let collection = context.collection_fixture("audit_entity_route").await;

        let group = create_test_group(&context.pool).await;
        group
            .add_member_without_events(&context.pool, &context.normal_user)
            .await
            .unwrap();
        collection
            .collection
            .grant_without_events(
                &context.pool,
                group.id,
                PermissionsList::new([Permissions::ReadAudit]),
            )
            .await
            .unwrap();

        let matching_event = emit_test_event(
            &context.pool,
            &NewEvent::new(
                EntityType::Collection,
                Action::Created,
                ActorKind::System,
                "collection route audit test",
            )
            .unwrap()
            .with_collection_id(collection.collection.id)
            .with_entity_id(collection.collection.id)
            .with_entity_name(&collection.collection.name),
        )
        .await;
        let other_event = emit_test_event(
            &context.pool,
            &NewEvent::new(
                EntityType::Object,
                Action::Created,
                ActorKind::System,
                "object should not appear in collection route",
            )
            .unwrap()
            .with_collection_id(collection.collection.id)
            .with_entity_id(collection.collection.id)
            .with_entity_name("not-the-collection"),
        )
        .await;

        let endpoint = format!("/api/v1/collections/{}/events", collection.collection.id);
        let resp = get_request(&context.pool, &context.normal_token, &endpoint).await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let rows: Vec<EventResponse> = test::read_body_json(resp).await;
        assert!(rows.iter().any(|row| row.id == matching_event.id));
        assert!(!rows.iter().any(|row| row.id == other_event.id));

        let endpoint = format!(
            "/api/v1/collections/{}/events?entity_type=object",
            collection.collection.id
        );
        let resp = get_request(&context.pool, &context.normal_token, &endpoint).await;
        assert_response_status(resp, StatusCode::BAD_REQUEST).await;
    }

    #[rstest]
    #[case::numeric(false)]
    #[case::legacy_string(true)]
    #[actix_web::test]
    async fn test_events_endpoint_includes_related_collection_events(
        #[case] encode_as_string: bool,
    ) {
        let context = TestContext::new().await;
        let collections = context.collection_fixtures("audit_related", 2).await;
        let source_collection = &collections[0].collection;
        let related_collection = &collections[1].collection;

        let user = create_test_user(&context.pool).await;
        let user_token = user.create_token(&context.pool).await.unwrap().get_token();
        let group = create_test_group(&context.pool).await;
        group
            .add_member_without_events(&context.pool, &user)
            .await
            .unwrap();
        related_collection
            .grant_without_events(
                &context.pool,
                group.id,
                PermissionsList::new([Permissions::ReadAudit]),
            )
            .await
            .unwrap();

        let related_collection_id = if encode_as_string {
            json!(related_collection.id.to_string())
        } else {
            json!(related_collection.id)
        };
        let event = emit_test_event(
            &context.pool,
            &NewEvent::new(
                EntityType::ClassRelation,
                Action::Created,
                ActorKind::System,
                "related collection audit test",
            )
            .unwrap()
            .with_collection_id(source_collection.id)
            .with_entity_id(source_collection.id)
            .with_before(json!({"secret": "source-before"}))
            .with_after(json!({"secret": "source-after"}))
            .with_metadata(json!({
                "related_collection_ids": [related_collection_id],
            })),
        )
        .await;

        let resp = get_request(&context.pool, &user_token, EVENTS_ENDPOINT).await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let rows: Vec<EventResponse> = test::read_body_json(resp).await;
        let related_event = rows
            .iter()
            .find(|row| row.id == event.id)
            .expect("related collection audit event should be visible");
        assert!(related_event.before.is_none());
        assert!(related_event.after.is_none());
    }

    #[rstest]
    #[case::matching(true, StatusCode::OK)]
    #[case::mismatching(false, StatusCode::NOT_FOUND)]
    #[actix_web::test]
    async fn object_event_route_enforces_path_class(
        #[case] matching_class: bool,
        #[case] expected_status: StatusCode,
    ) {
        let context = TestContext::new().await;
        let collection = context.collection_fixture("object_event_path_class").await;
        let class = NewHubuumClass {
            name: format!("event_class_{}", uuid::Uuid::new_v4().simple()),
            collection_id: collection.collection.id,
            json_schema: None,
            validate_schema: Some(false),
            description: "event route class".to_string(),
        }
        .save_without_events(&context.pool)
        .await
        .unwrap();
        let other_class = NewHubuumClass {
            name: format!("other_event_class_{}", uuid::Uuid::new_v4().simple()),
            collection_id: collection.collection.id,
            json_schema: None,
            validate_schema: Some(false),
            description: "other event route class".to_string(),
        }
        .save_without_events(&context.pool)
        .await
        .unwrap();
        let object = NewHubuumObject {
            name: format!("event_object_{}", uuid::Uuid::new_v4().simple()),
            collection_id: collection.collection.id,
            hubuum_class_id: class.id,
            data: json!({}),
            description: "event route object".to_string(),
        }
        .save_without_events(&context.pool)
        .await
        .unwrap();
        let path_class_id = if matching_class {
            class.id
        } else {
            other_class.id
        };

        let response = get_request(
            &context.pool,
            &context.admin_token,
            &format!("/api/v1/classes/{}/{}/events", path_class_id, object.id),
        )
        .await;
        assert_response_status(response, expected_status).await;
    }
}
