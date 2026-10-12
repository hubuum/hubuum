#[cfg(test)]
mod tests {
    use actix_web::{http::StatusCode, test};
    use hubuum_events_core::AuditDocument;
    use rstest::rstest;
    use serde_json::json;

    use crate::events::{
        Action, ActorKind, CollectionId, EntityType, EventEntityId, EventResponse, NewEvent,
    };
    use crate::models::{GroupID, NewHubuumClass, NewHubuumObject, Permissions, PermissionsList};
    use crate::test_support::create_audit_event;
    use crate::tests::api_operations::get_request;
    use crate::tests::asserts::{assert_response_status, header_value};
    use crate::tests::{ObjectFixture, TestContext};
    use crate::tests::{create_test_group, create_test_user};
    use crate::traits::{CanSave, PermissionController};

    const EVENTS_ENDPOINT: &str = "/api/v1/events";

    fn collection_id(id: i32) -> CollectionId {
        CollectionId::new(id).expect("test collection id must be positive")
    }

    fn event_entity_id(id: i32) -> EventEntityId {
        EventEntityId::new(id).expect("test event entity id must be positive")
    }

    async fn emit_test_event(
        pool: &impl crate::test_support::AuditEventFixture,
        event: &NewEvent,
    ) -> EventResponse {
        create_audit_event(pool, event)
            .await
            .expect("failed to emit test event")
    }

    struct RelationAuditFixture {
        resources: ObjectFixture,
        route: String,
        generic_route: String,
        events: Vec<EventResponse>,
    }

    impl RelationAuditFixture {
        async fn new(context: &TestContext, object: bool, from: bool) -> Self {
            let resources = context
                .object_fixture(
                    "relation_audit",
                    NewHubuumClass {
                        name: context.scoped_name("audit_class"),
                        collection_id: 1,
                        json_schema: None,
                        validate_schema: Some(false),
                        description: String::new(),
                    },
                    vec![NewHubuumObject {
                        name: context.scoped_name("audit_object"),
                        collection_id: 1,
                        hubuum_class_id: 1,
                        data: json!({}),
                        description: String::new(),
                    }],
                )
                .await
                .unwrap();
            let (entity_type, relation_type, id, route) = if object {
                (
                    EntityType::Object,
                    EntityType::ObjectRelation,
                    resources[0].id,
                    format!(
                        "/api/v1/classes/{}/{}/events",
                        resources.class.id, resources[0].id
                    ),
                )
            } else {
                (
                    EntityType::Class,
                    EntityType::ClassRelation,
                    resources.class.id,
                    format!("/api/v1/classes/{}/events", resources.class.id),
                )
            };
            let response = get_request(&context.pool, &context.admin_token, &route).await;
            let response = assert_response_status(response, StatusCode::OK).await;
            let mut events: Vec<EventResponse> = test::read_body_json(response).await;
            assert_eq!(events.len(), 1, "fixture creation emits one entity event");
            let endpoint_key = match (object, from) {
                (false, true) => "from_class_id",
                (false, false) => "to_class_id",
                (true, true) => "from_object_id",
                (true, false) => "to_object_id",
            };
            // No live relation exists: the read must use the recorded endpoint.
            for (kind, action, endpoint_id) in [
                (relation_type, Action::Created, id),
                (relation_type, Action::Deleted, id),
                (relation_type, Action::Created, i32::MAX),
                (
                    if object {
                        EntityType::ClassRelation
                    } else {
                        EntityType::ObjectRelation
                    },
                    Action::Created,
                    id,
                ),
            ] {
                let document = AuditDocument::try_new(
                    "relation changed",
                    Some(json!({"private": "before"})),
                    Some(json!({"private": "after"})),
                    json!({endpoint_key: endpoint_id, "related_collection_ids": [resources.collection_id()]}),
                ).unwrap();
                let event = emit_test_event(
                    context.pool.get_ref(),
                    &NewEvent::from_document(kind, action, ActorKind::System, document)
                        .unwrap()
                        .with_entity_id(event_entity_id(id)),
                )
                .await;
                if kind == relation_type && endpoint_id == id {
                    events.push(event);
                }
            }
            Self {
                resources,
                route,
                generic_route: format!(
                    "{EVENTS_ENDPOINT}?entity_type={}&entity_id={id}",
                    entity_type.as_str()
                ),
                events,
            }
        }
    }

    #[rstest]
    #[case::class_from(false, true)]
    #[case::class_to(false, false)]
    #[case::object_from(true, true)]
    #[case::object_to(true, false)]
    #[actix_web::test]
    async fn entity_audit_relation_inclusion_is_opt_in(
        #[case] object: bool,
        #[case] from: bool,
        #[values(
            "",
            "include_relations=false",
            "include_relations=true",
            "include_relations=true&action=deleted"
        )]
        query: &str,
    ) {
        let context = TestContext::new().await;
        let fixture = RelationAuditFixture::new(&context, object, from).await;
        let mut endpoint = format!("{}?sort=id", fixture.route);
        if !query.is_empty() {
            endpoint.push('&');
            endpoint.push_str(query);
        }
        let response = get_request(&context.pool, &context.admin_token, &endpoint).await;
        let response = assert_response_status(response, StatusCode::OK).await;
        let total = header_value(&response, "X-Total-Count").unwrap();
        let rows: Vec<EventResponse> = test::read_body_json(response).await;
        let expected = fixture
            .events
            .iter()
            .filter(|event| {
                if query.contains("action=deleted") {
                    event.action == "deleted"
                } else {
                    query.contains("include_relations=true") || event.id == fixture.events[0].id
                }
            })
            .map(|event| event.id)
            .collect::<Vec<_>>();
        assert_eq!(
            (rows.iter().map(|event| event.id).collect::<Vec<_>>(), total),
            (expected.clone(), expected.len().to_string())
        );
        fixture.resources.cleanup().await.unwrap();
    }

    #[rstest]
    #[case::class(false)]
    #[case::object(true)]
    #[actix_web::test]
    async fn relation_audit_cursor_pages_share_the_combined_total(#[case] object: bool) {
        let context = TestContext::new().await;
        let fixture = RelationAuditFixture::new(&context, object, true).await;
        let base = format!(
            "{}&include_relations=true&sort=id&limit=1",
            fixture.generic_route
        );
        let mut endpoint = base.clone();
        let mut ids = Vec::new();
        loop {
            let response = get_request(&context.pool, &context.admin_token, &endpoint).await;
            let response = assert_response_status(response, StatusCode::OK).await;
            assert_eq!(
                header_value(&response, "X-Total-Count").as_deref(),
                Some("3")
            );
            let next = header_value(&response, "X-Next-Cursor");
            let rows: Vec<EventResponse> = test::read_body_json(response).await;
            ids.extend(rows.iter().map(|event| event.id));
            assert!(
                ids.len() <= 3,
                "cursor must advance without duplicating events"
            );
            let Some(cursor) = next else { break };
            endpoint = format!("{base}&cursor={cursor}");
        }
        assert_eq!(
            ids,
            fixture
                .events
                .iter()
                .map(|event| event.id)
                .collect::<Vec<_>>()
        );
        fixture.resources.cleanup().await.unwrap();
    }

    #[rstest]
    #[case::class_denied(false, false)]
    #[case::class_allowed(false, true)]
    #[case::object_denied(true, false)]
    #[case::object_allowed(true, true)]
    #[actix_web::test]
    async fn relation_audit_inclusion_preserves_visibility_and_redaction(
        #[case] object: bool,
        #[case] grant: bool,
    ) {
        let context = TestContext::new().await;
        let fixture = RelationAuditFixture::new(&context, object, false).await;
        if grant {
            fixture
                .resources
                .collection
                .owner_group
                .add_member_without_events(&context.pool, &context.normal_user)
                .await
                .unwrap();
        }
        let response = get_request(
            &context.pool,
            &context.normal_token,
            &format!("{}?include_relations=true", fixture.route),
        )
        .await;
        let response = assert_response_status(response, StatusCode::OK).await;
        let rows: Vec<EventResponse> = test::read_body_json(response).await;
        assert_eq!(rows.len(), if grant { 3 } else { 0 });
        for relation in rows
            .iter()
            .filter(|event| event.entity_type.ends_with("_relation"))
        {
            assert_eq!((&relation.before, &relation.after), (&None, &None));
        }
        fixture.resources.cleanup().await.unwrap();
    }

    #[rstest]
    #[case::invalid_boolean("include_relations=yes")]
    #[case::duplicate("include_relations=true&include_relations=false")]
    #[case::missing_subject("include_relations=true")]
    #[case::missing_id("include_relations=true&entity_type=object")]
    #[case::unsupported_subject("include_relations=true&entity_type=collection&entity_id=1")]
    #[actix_web::test]
    async fn relation_audit_rejects_invalid_selection(#[case] query: &str) {
        let context = TestContext::new().await;
        let response = get_request(
            &context.pool,
            &context.admin_token,
            &format!("{EVENTS_ENDPOINT}?{query}"),
        )
        .await;
        assert_response_status(response, StatusCode::BAD_REQUEST).await;
    }

    #[actix_web::test]
    async fn test_events_endpoint_requires_read_audit_permission() {
        let context = TestContext::new().await;
        let collection = context
            .collection_fixture("audit_requires_permission")
            .await;

        let event = emit_test_event(
            context.pool.get_ref(),
            &NewEvent::new(
                EntityType::Collection,
                Action::Created,
                ActorKind::System,
                "collection audit permission test",
            )
            .unwrap()
            .with_collection_id(collection_id(collection.collection.id))
            .with_entity_id(event_entity_id(collection.collection.id))
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
                GroupID::new(group.id).unwrap(),
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
                GroupID::new(group.id).unwrap(),
                PermissionsList::new([Permissions::ReadAudit]),
            )
            .await
            .unwrap();

        let collectiond_event = emit_test_event(
            context.pool.get_ref(),
            &NewEvent::new(
                EntityType::Collection,
                Action::Updated,
                ActorKind::System,
                "collection audit filter test",
            )
            .unwrap()
            .with_collection_id(collection_id(collection.collection.id))
            .with_entity_id(event_entity_id(collection.collection.id))
            .with_entity_name(&collection.collection.name),
        )
        .await;
        let collection_less_event = emit_test_event(
            context.pool.get_ref(),
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
            "{EVENTS_ENDPOINT}?entity_type=collection&action=updated&collection_id={}",
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
                GroupID::new(group.id).unwrap(),
                PermissionsList::new([Permissions::ReadAudit]),
            )
            .await
            .unwrap();

        let matching_event = emit_test_event(
            context.pool.get_ref(),
            &NewEvent::new(
                EntityType::Collection,
                Action::Created,
                ActorKind::System,
                "collection route audit test",
            )
            .unwrap()
            .with_collection_id(collection_id(collection.collection.id))
            .with_entity_id(event_entity_id(collection.collection.id))
            .with_entity_name(&collection.collection.name),
        )
        .await;
        let other_event = emit_test_event(
            context.pool.get_ref(),
            &NewEvent::new(
                EntityType::Object,
                Action::Created,
                ActorKind::System,
                "object should not appear in collection route",
            )
            .unwrap()
            .with_collection_id(collection_id(collection.collection.id))
            .with_entity_id(event_entity_id(collection.collection.id))
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
                GroupID::new(group.id).unwrap(),
                PermissionsList::new([Permissions::ReadAudit]),
            )
            .await
            .unwrap();

        let related_collection_id = if encode_as_string {
            json!(related_collection.id.to_string())
        } else {
            json!(related_collection.id)
        };
        let document = AuditDocument::try_new(
            "related collection audit test",
            Some(json!({"secret": "source-before"})),
            Some(json!({"secret": "source-after"})),
            json!({
                "related_collection_ids": [related_collection_id],
            }),
        )
        .unwrap();
        let event = emit_test_event(
            context.pool.get_ref(),
            &NewEvent::from_document(
                EntityType::ClassRelation,
                Action::Created,
                ActorKind::System,
                document,
            )
            .unwrap()
            .with_collection_id(collection_id(source_collection.id))
            .with_entity_id(event_entity_id(source_collection.id)),
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
        #[values(false, true)] include_relations: bool,
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
            &format!(
                "/api/v1/classes/{}/{}/events?include_relations={include_relations}",
                path_class_id, object.id
            ),
        )
        .await;
        assert_response_status(response, expected_status).await;
    }
}
