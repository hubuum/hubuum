use std::sync::Arc;

use actix_web::{http, test};

use crate::models::{
    HubuumClass, HubuumObject, NewHubuumClass, NewHubuumClassRelation, NewHubuumObject,
    NewHubuumObjectRelation, Permissions,
};
use crate::permissions::test_support::{MockAllowRule, MockTreetopBackend};
use crate::permissions::{ResourceFields, ResourceKind};
use crate::tests::api_operations::{get_request, get_request_with_permission_backend};
use crate::tests::asserts::{assert_response_status, header_value};
use crate::tests::{CollectionFixture, TestContext, create_test_group};
use crate::traits::CanSave;

struct RelatedFilterFixture {
    collection: CollectionFixture,
    host_class: HubuumClass,
    room_class: HubuumClass,
    person_class: HubuumClass,
    host_complete: HubuumObject,
    host_missing_person: HubuumObject,
    room_foo: HubuumObject,
    person_bar: HubuumObject,
    person_zoot: HubuumObject,
}

async fn save_class(
    context: &TestContext,
    collection: &CollectionFixture,
    label: &str,
) -> HubuumClass {
    NewHubuumClass {
        name: context.scoped_name(label),
        description: label.to_string(),
        collection_id: collection.collection.id,
        json_schema: None,
        validate_schema: Some(false),
    }
    .save_without_events(&context.pool)
    .await
    .unwrap()
}

async fn save_object(
    context: &TestContext,
    collection: &CollectionFixture,
    class: &HubuumClass,
    label: &str,
    data: serde_json::Value,
) -> HubuumObject {
    NewHubuumObject {
        name: context.scoped_name(label),
        description: label.to_string(),
        collection_id: collection.collection.id,
        hubuum_class_id: class.id,
        data,
    }
    .save_without_events(&context.pool)
    .await
    .unwrap()
}

async fn relate_classes(
    context: &TestContext,
    left: &HubuumClass,
    right: &HubuumClass,
) -> crate::models::HubuumClassRelation {
    NewHubuumClassRelation {
        from_hubuum_class_id: left.id,
        to_hubuum_class_id: right.id,
        forward_template_alias: None,
        reverse_template_alias: None,
        from_max_relations: None,
        to_max_relations: None,
    }
    .save_without_events(&context.pool)
    .await
    .unwrap()
}

async fn relate_objects(
    context: &TestContext,
    left: &HubuumObject,
    right: &HubuumObject,
    class_relation: &crate::models::HubuumClassRelation,
) {
    NewHubuumObjectRelation {
        from_hubuum_object_id: left.id,
        to_hubuum_object_id: right.id,
        class_relation_id: class_relation.id,
    }
    .save_without_events(&context.pool)
    .await
    .unwrap();
}

async fn related_filter_fixture(context: &TestContext) -> RelatedFilterFixture {
    let collection = context.collection_fixture("related_filter").await;
    let host_class = save_class(context, &collection, "Host").await;
    let room_class = save_class(context, &collection, "Room").await;
    let person_class = save_class(context, &collection, "Person").await;
    let host_room = relate_classes(context, &host_class, &room_class).await;
    let host_person = relate_classes(context, &host_class, &person_class).await;

    let host_complete = save_object(
        context,
        &collection,
        &host_class,
        "host_complete",
        serde_json::json!({}),
    )
    .await;
    let host_missing_person = save_object(
        context,
        &collection,
        &host_class,
        "host_missing_person",
        serde_json::json!({}),
    )
    .await;
    let host_wrong_room = save_object(
        context,
        &collection,
        &host_class,
        "host_wrong_room",
        serde_json::json!({}),
    )
    .await;
    let room_foo = save_object(
        context,
        &collection,
        &room_class,
        "foo",
        serde_json::json!({"tier": "gold"}),
    )
    .await;
    let room_other = save_object(
        context,
        &collection,
        &room_class,
        "other_room",
        serde_json::json!({"tier": "silver"}),
    )
    .await;
    let person_bar = save_object(
        context,
        &collection,
        &person_class,
        "bar",
        serde_json::json!({}),
    )
    .await;
    let person_zoot = save_object(
        context,
        &collection,
        &person_class,
        "zoot",
        serde_json::json!({}),
    )
    .await;

    relate_objects(context, &host_complete, &room_foo, &host_room).await;
    relate_objects(context, &host_complete, &person_bar, &host_person).await;
    relate_objects(context, &host_complete, &person_zoot, &host_person).await;
    relate_objects(context, &host_missing_person, &room_foo, &host_room).await;
    relate_objects(context, &host_missing_person, &person_bar, &host_person).await;
    relate_objects(context, &host_wrong_room, &room_other, &host_room).await;
    relate_objects(context, &host_wrong_room, &person_bar, &host_person).await;
    relate_objects(context, &host_wrong_room, &person_zoot, &host_person).await;

    RelatedFilterFixture {
        collection,
        host_class,
        room_class,
        person_class,
        host_complete,
        host_missing_person,
        room_foo,
        person_bar,
        person_zoot,
    }
}

#[actix_web::test]
async fn related_groups_are_independent_existentials_combined_with_and() {
    let context = TestContext::new().await;
    let fixture = related_filter_fixture(&context).await;
    let endpoint = format!(
        "/api/v1/classes/{}/?related.room.class.name={}&related.room.object.name={}&related.bar.class.name={}&related.bar.object.name={}&related.zoot.class.name={}&related.zoot.object.name={}",
        fixture.host_class.id,
        fixture.room_class.name,
        fixture.room_foo.name,
        fixture.person_class.name,
        fixture.person_bar.name,
        fixture.person_class.name,
        fixture.person_zoot.name,
    );

    let response = get_request(&context.pool, &context.admin_token, &endpoint).await;
    let response = assert_response_status(response, http::StatusCode::OK).await;
    assert_eq!(
        header_value(&response, "x-total-count").as_deref(),
        Some("1")
    );
    let objects: Vec<HubuumObject> = test::read_body_json(response).await;

    assert_eq!(objects, vec![fixture.host_complete]);
    fixture.collection.cleanup().await.unwrap();
}

#[actix_web::test]
async fn related_target_object_filters_support_json_and_revision_fields() {
    let context = TestContext::new().await;
    let fixture = related_filter_fixture(&context).await;
    let endpoint = format!(
        "/api/v1/classes/by-name/{}/objects?related.room.class.name={}&related.room.object.json_data__equals=tier=gold&related.room.object.revision__gte={}",
        fixture.host_class.name, fixture.room_class.name, fixture.room_foo.revision,
    );

    let response = get_request(&context.pool, &context.admin_token, &endpoint).await;
    let response = assert_response_status(response, http::StatusCode::OK).await;
    let objects: Vec<HubuumObject> = test::read_body_json(response).await;

    assert_eq!(
        objects.iter().map(|object| object.id).collect::<Vec<_>>(),
        vec![fixture.host_complete.id, fixture.host_missing_person.id]
    );
    fixture.collection.cleanup().await.unwrap();
}

#[actix_web::test]
async fn related_depth_is_bounded_per_group() {
    let context = TestContext::new().await;
    let collection = context.collection_fixture("related_filter_depth").await;
    let host_class = save_class(&context, &collection, "depth_host").await;
    let bridge_class = save_class(&context, &collection, "depth_bridge").await;
    let room_class = save_class(&context, &collection, "depth_room").await;
    let host_bridge = relate_classes(&context, &host_class, &bridge_class).await;
    let bridge_room = relate_classes(&context, &bridge_class, &room_class).await;
    let host = save_object(
        &context,
        &collection,
        &host_class,
        "depth_host_object",
        serde_json::json!({}),
    )
    .await;
    let bridge = save_object(
        &context,
        &collection,
        &bridge_class,
        "depth_bridge_object",
        serde_json::json!({}),
    )
    .await;
    let room = save_object(
        &context,
        &collection,
        &room_class,
        "depth_room_object",
        serde_json::json!({}),
    )
    .await;
    relate_objects(&context, &host, &bridge, &host_bridge).await;
    relate_objects(&context, &bridge, &room, &bridge_room).await;

    let default_endpoint = format!(
        "/api/v1/classes/{}/?related.room.class.name={}&related.room.object.name={}",
        host_class.id, room_class.name, room.name,
    );
    let response = get_request(&context.pool, &context.admin_token, &default_endpoint).await;
    let response = assert_response_status(response, http::StatusCode::OK).await;
    let default_objects: Vec<HubuumObject> = test::read_body_json(response).await;
    assert!(default_objects.is_empty());

    let depth_two_endpoint = format!("{default_endpoint}&related.room.depth__lte=2");
    let response = get_request(&context.pool, &context.admin_token, &depth_two_endpoint).await;
    let response = assert_response_status(response, http::StatusCode::OK).await;
    let depth_two_objects: Vec<HubuumObject> = test::read_body_json(response).await;
    assert_eq!(depth_two_objects, vec![host]);

    collection.cleanup().await.unwrap();
}

#[actix_web::test]
async fn malformed_related_filter_returns_bad_request() {
    let context = TestContext::new().await;
    let fixture = related_filter_fixture(&context).await;
    let endpoint = format!(
        "/api/v1/classes/{}/?related.room.object.name={}",
        fixture.host_class.id, fixture.room_foo.name,
    );

    let response = get_request(&context.pool, &context.admin_token, &endpoint).await;
    assert_response_status(response, http::StatusCode::BAD_REQUEST).await;

    fixture.collection.cleanup().await.unwrap();
}

#[actix_web::test]
async fn external_policy_requires_a_visible_path_and_preserves_allowed_alternatives() {
    let context = TestContext::new().await;
    let collection = context.collection_fixture("related_filter_external").await;
    let group = create_test_group(&context.pool).await;
    group
        .add_member_without_events(&context.pool, &context.normal_user)
        .await
        .unwrap();
    let host_class = save_class(&context, &collection, "external_host").await;
    let bridge_class = save_class(&context, &collection, "external_bridge").await;
    let room_class = save_class(&context, &collection, "external_room").await;
    let room_bridge = relate_classes(&context, &room_class, &bridge_class).await;
    let bridge_host = relate_classes(&context, &bridge_class, &host_class).await;
    let room = save_object(
        &context,
        &collection,
        &room_class,
        "external_room_foo",
        serde_json::json!({}),
    )
    .await;
    let allowed_bridge = save_object(
        &context,
        &collection,
        &bridge_class,
        "external_allowed_bridge",
        serde_json::json!({}),
    )
    .await;
    let hidden_bridge = save_object(
        &context,
        &collection,
        &bridge_class,
        "external_hidden_bridge",
        serde_json::json!({}),
    )
    .await;
    let host_with_alternative = save_object(
        &context,
        &collection,
        &host_class,
        "external_host_with_alternative",
        serde_json::json!({}),
    )
    .await;
    let host_hidden_path_only = save_object(
        &context,
        &collection,
        &host_class,
        "external_host_hidden_path_only",
        serde_json::json!({}),
    )
    .await;

    let allowed_first = NewHubuumObjectRelation {
        from_hubuum_object_id: room.id,
        to_hubuum_object_id: allowed_bridge.id,
        class_relation_id: room_bridge.id,
    }
    .save_without_events(&context.pool)
    .await
    .unwrap();
    let allowed_second = NewHubuumObjectRelation {
        from_hubuum_object_id: allowed_bridge.id,
        to_hubuum_object_id: host_with_alternative.id,
        class_relation_id: bridge_host.id,
    }
    .save_without_events(&context.pool)
    .await
    .unwrap();
    relate_objects(&context, &room, &hidden_bridge, &room_bridge).await;
    relate_objects(
        &context,
        &hidden_bridge,
        &host_with_alternative,
        &bridge_host,
    )
    .await;
    relate_objects(
        &context,
        &hidden_bridge,
        &host_hidden_path_only,
        &bridge_host,
    )
    .await;

    let permission_backend = MockTreetopBackend::new();
    permission_backend.add_rule(MockAllowRule {
        group_id: group.id,
        action: Permissions::ReadClass,
        resource_kind: ResourceKind::Class,
        resource_id: Some(room_class.id),
        attrs: ResourceFields::default(),
    });
    permission_backend.add_rule(MockAllowRule {
        group_id: group.id,
        action: Permissions::ReadCollection,
        resource_kind: ResourceKind::Collection,
        resource_id: Some(collection.collection.id),
        attrs: ResourceFields::default(),
    });
    for object_id in [
        room.id,
        allowed_bridge.id,
        host_with_alternative.id,
        host_hidden_path_only.id,
    ] {
        permission_backend.add_rule(MockAllowRule {
            group_id: group.id,
            action: Permissions::ReadObject,
            resource_kind: ResourceKind::Object,
            resource_id: Some(object_id),
            attrs: ResourceFields::default(),
        });
    }
    for relation_id in [allowed_first.id, allowed_second.id] {
        permission_backend.add_rule(MockAllowRule {
            group_id: group.id,
            action: Permissions::ReadObjectRelation,
            resource_kind: ResourceKind::ObjectRelation,
            resource_id: Some(relation_id),
            attrs: ResourceFields::default(),
        });
    }

    let endpoint = format!(
        "/api/v1/classes/{}/?related.room.class.name={}&related.room.object.name={}&related.room.depth__lte=2",
        host_class.id, room_class.name, room.name,
    );
    let response = get_request_with_permission_backend(
        &context.pool,
        &context.normal_token,
        &endpoint,
        Arc::new(permission_backend),
    )
    .await;
    let response = assert_response_status(response, http::StatusCode::OK).await;
    let objects: Vec<HubuumObject> = test::read_body_json(response).await;

    assert_eq!(objects, vec![host_with_alternative]);
    collection.cleanup().await.unwrap();
    group.delete_without_events(&context.pool).await.unwrap();
}

#[rstest::rstest]
#[case::allowed(true, None, false, false, false)]
#[case::denied_descendant(true, Some(2), false, false, false)]
#[case::denied_intermediate(true, Some(1), false, false, false)]
#[case::denied_edge(true, None, true, false, false)]
#[case::external_scoped_descendant(true, Some(2), false, true, false)]
#[case::external_scoped_intermediate(true, Some(1), false, true, false)]
#[case::local_scoped_descendant(false, Some(2), false, true, false)]
#[case::local_scoped_intermediate(false, Some(1), false, true, false)]
#[case::local_allowed(false, None, false, false, false)]
#[case::denied_graph_chord(true, None, false, false, true)]
#[actix_web::test]
async fn traversal_authorizes_complete_paths(
    #[case] external: bool,
    #[case] denied_vertex: Option<usize>,
    #[case] denied_edge: bool,
    #[case] scoped: bool,
    #[case] star: bool,
    #[values(false, true)] classes: bool,
    #[values(false, true)] graph: bool,
) {
    use crate::models::TokenResourceScope;
    use crate::tests::resource_scoped_token;

    let context = TestContext::new().await;
    let collection = context.collection_fixture("traversal_policy").await;
    let group = create_test_group(&context.pool).await;
    group
        .add_member_without_events(&context.pool, &context.admin_user)
        .await
        .unwrap();
    let policy = MockTreetopBackend::new();
    let allow = |permission, kind, id| {
        policy.add_rule(MockAllowRule {
            group_id: group.id,
            action: permission,
            resource_kind: kind,
            resource_id: Some(id),
            attrs: ResourceFields::default(),
        })
    };
    allow(
        Permissions::ReadCollection,
        ResourceKind::Collection,
        collection.collection.id,
    );
    let mut vertices = Vec::new();
    for index in 0..3 {
        let class = save_class(&context, &collection, &format!("path_class_{index}")).await;
        let object = save_object(
            &context,
            &collection,
            &class,
            &format!("path_object_{index}"),
            serde_json::json!({"secret": index}),
        )
        .await;
        if scoped || denied_vertex != Some(index) {
            allow(Permissions::ReadClass, ResourceKind::Class, class.id);
            allow(Permissions::ReadObject, ResourceKind::Object, object.id);
        }
        vertices.push((class, object));
    }
    let edges = if star {
        vec![(0, 1), (0, 2), (1, 2)]
    } else {
        vec![(0, 1), (1, 2)]
    };
    for (index, (from, to)) in edges.into_iter().enumerate() {
        let pair = [&vertices[from], &vertices[to]];
        let relation = relate_classes(&context, &pair[0].0, &pair[1].0).await;
        let object_relation = NewHubuumObjectRelation {
            from_hubuum_object_id: pair[0].1.id,
            to_hubuum_object_id: pair[1].1.id,
            class_relation_id: relation.id,
        }
        .save_without_events(&context.pool)
        .await
        .unwrap();
        if (!denied_edge || index != 0) && (!star || index != 2) {
            allow(
                Permissions::ReadClassRelation,
                ResourceKind::ClassRelation,
                relation.id,
            );
            allow(
                Permissions::ReadObjectRelation,
                ResourceKind::ObjectRelation,
                object_relation.id,
            );
        }
    }
    let token = if scoped {
        let scopes = vertices
            .iter()
            .enumerate()
            .filter(|(index, _)| Some(*index) != denied_vertex)
            .map(|(_, (class, _))| {
                TokenResourceScope::Class(crate::models::HubuumClassID::new(class.id).unwrap())
            })
            .collect();
        resource_scoped_token(&context.pool, context.admin_user.id, scopes).await
    } else {
        context.admin_token.clone()
    };
    let endpoint = if classes {
        format!(
            "/api/v1/classes/{}/related/{}",
            vertices[0].0.id,
            if graph { "graph" } else { "classes" }
        )
    } else {
        format!(
            "/api/v1/classes/{}/objects/{}/related/{}",
            vertices[0].0.id,
            vertices[0].1.id,
            if graph { "graph" } else { "objects" }
        )
    };
    let expected_count = if denied_edge || denied_vertex == Some(1) {
        0
    } else if denied_vertex == Some(2) {
        1
    } else {
        2
    };
    let endpoint = format!(
        "{endpoint}?include_total=true&limit={}",
        if graph { 10 } else { 1 }
    );
    let policy = Arc::new(policy);
    if external
        && !scoped
        && let Some(index) = denied_vertex
    {
        let (class, object) = &vertices[index];
        let direct = if classes {
            format!("/api/v1/classes/{}", class.id)
        } else {
            format!("/api/v1/classes/{}/{}", class.id, object.id)
        };
        let response =
            get_request_with_permission_backend(&context.pool, &token, &direct, policy.clone())
                .await;
        assert_response_status(response, http::StatusCode::FORBIDDEN).await;
    }
    let mut next_endpoint = endpoint.clone();
    let mut actual = Vec::new();
    loop {
        let response = if external {
            get_request_with_permission_backend(
                &context.pool,
                &token,
                &next_endpoint,
                policy.clone(),
            )
            .await
        } else {
            get_request(&context.pool, &token, &next_endpoint).await
        };
        let response = assert_response_status(response, http::StatusCode::OK).await;
        let total = header_value(&response, "X-Total-Count");
        let cursor = header_value(&response, "X-Next-Cursor");
        let body: serde_json::Value = test::read_body_json(response).await;
        let rows = if graph {
            assert_eq!(body["relations"].as_array().unwrap().len(), expected_count);
            body[if classes { "classes" } else { "objects" }]
                .as_array()
                .unwrap()
        } else {
            assert_eq!(total, Some(expected_count.to_string()));
            body.as_array().unwrap()
        };
        actual.extend(rows.iter().map(|row| row["id"].as_i64().unwrap() as i32));
        if let Some(cursor) = cursor {
            assert!(
                !graph && actual.len() < expected_count,
                "cursor exposes denied or nonexistent rows"
            );
            next_endpoint = format!("{endpoint}&cursor={cursor}");
        } else {
            break;
        }
    }
    let expected = vertices
        .iter()
        .take(expected_count + 1)
        .skip(usize::from(!graph))
        .map(|(class, object)| if classes { class.id } else { object.id })
        .collect::<Vec<_>>();
    assert_eq!(actual, expected);
    collection.cleanup().await.unwrap();
    group.delete_without_events(&context.pool).await.unwrap();
}
