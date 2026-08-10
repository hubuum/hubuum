use std::sync::Arc;

use actix_web::{http, test};
use serde_json::{Value, json};

use crate::db::with_transaction;
use crate::events::{Action, ActorKind, EntityType, EventResponse, NewEvent, emit_event};
use crate::models::{
    Collection, GroupResponse, HubuumClass, HubuumClassExpanded, HubuumObject, NewHubuumClass,
    NewHubuumClassRelation, NewHubuumObject, NewHubuumObjectRelation, Permissions,
    ServiceAccountResponse, StructuredSearchResourceKind, StructuredSearchResponse,
    StructuredSearchResult, UserResponse,
};
use crate::permissions::test_support::{MockAllowRule, MockTreetopBackend};
use crate::permissions::{ResourceAttrs, ResourceKind};
use crate::tests::api_operations::{post_request, post_request_with_permission_backend};
use crate::tests::asserts::assert_response_status;
use crate::tests::{
    CollectionFixture, TestContext, create_test_group, create_test_service_account,
};
use crate::traits::CanSave;

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
    description: &str,
    data: Value,
) -> HubuumObject {
    NewHubuumObject {
        name: context.scoped_name(label),
        description: description.to_string(),
        collection_id: collection.collection.id,
        hubuum_class_id: class.id,
        data,
    }
    .save_without_events(&context.pool)
    .await
    .unwrap()
}

fn request_for_class(class: &HubuumClass, filter: Value) -> Value {
    json!({
        "version": 1,
        "target": {"kind": "object", "class": {"id": class.id}},
        "filter": filter,
        "sort": [{"field": "name", "direction": "asc"}],
        "include_total": true
    })
}

fn object_results(response: StructuredSearchResponse) -> Vec<HubuumObject> {
    assert_eq!(response.kind, StructuredSearchResourceKind::Object);
    response
        .results
        .into_iter()
        .map(|result| match result {
            StructuredSearchResult::Object(object) => object,
            other => panic!("expected object result, got {other:?}"),
        })
        .collect()
}

fn collection_results(response: StructuredSearchResponse) -> Vec<Collection> {
    assert_eq!(response.kind, StructuredSearchResourceKind::Collection);
    response
        .results
        .into_iter()
        .map(|result| match result {
            StructuredSearchResult::Collection(collection) => collection,
            other => panic!("expected collection result, got {other:?}"),
        })
        .collect()
}

fn class_results(response: StructuredSearchResponse) -> Vec<HubuumClassExpanded> {
    assert_eq!(response.kind, StructuredSearchResourceKind::Class);
    response
        .results
        .into_iter()
        .map(|result| match result {
            StructuredSearchResult::Class(class) => class,
            other => panic!("expected class result, got {other:?}"),
        })
        .collect()
}

fn user_results(response: StructuredSearchResponse) -> Vec<UserResponse> {
    assert_eq!(response.kind, StructuredSearchResourceKind::User);
    response
        .results
        .into_iter()
        .map(|result| match result {
            StructuredSearchResult::User(user) => user,
            other => panic!("expected user result, got {other:?}"),
        })
        .collect()
}

fn audit_event_results(response: StructuredSearchResponse) -> Vec<EventResponse> {
    assert_eq!(response.kind, StructuredSearchResourceKind::AuditEvent);
    response
        .results
        .into_iter()
        .map(|result| match result {
            StructuredSearchResult::AuditEvent(event) => *event,
            other => panic!("expected audit event result, got {other:?}"),
        })
        .collect()
}

fn group_results(response: StructuredSearchResponse) -> Vec<GroupResponse> {
    assert_eq!(response.kind, StructuredSearchResourceKind::Group);
    response
        .results
        .into_iter()
        .map(|result| match result {
            StructuredSearchResult::Group(group) => group,
            other => panic!("expected group result, got {other:?}"),
        })
        .collect()
}

fn service_account_results(response: StructuredSearchResponse) -> Vec<ServiceAccountResponse> {
    assert_eq!(response.kind, StructuredSearchResourceKind::ServiceAccount);
    response
        .results
        .into_iter()
        .map(|result| match result {
            StructuredSearchResult::ServiceAccount(account) => account,
            other => panic!("expected service account result, got {other:?}"),
        })
        .collect()
}

#[actix_web::test]
async fn structured_search_targets_collections() {
    let context = TestContext::new().await;
    let collection = context.collection_fixture("structured_collection").await;
    let request = json!({
        "version": 1,
        "target": {"kind": "collection"},
        "filter": {
            "op": "field",
            "predicate": {
                "field": "name",
                "operator": "equals",
                "value": collection.collection.name
            }
        },
        "include_total": true
    });

    let response = post_request(
        &context.pool,
        &context.admin_token,
        "/api/v1/search",
        request,
    )
    .await;
    let response = assert_response_status(response, http::StatusCode::OK).await;
    let body: StructuredSearchResponse = test::read_body_json(response).await;

    assert_eq!(
        collection_results(body),
        vec![collection.collection.clone()]
    );
    collection.cleanup().await.unwrap();
}

#[actix_web::test]
async fn structured_search_targets_classes() {
    let context = TestContext::new().await;
    let collection = context.collection_fixture("structured_class").await;
    let class = save_class(&context, &collection, "structured_class_target").await;
    let request = json!({
        "version": 1,
        "target": {"kind": "class"},
        "filter": {
            "op": "and",
            "args": [
                {
                    "op": "field",
                    "predicate": {
                        "field": "name",
                        "operator": "equals",
                        "value": class.name
                    }
                },
                {
                    "op": "field",
                    "predicate": {
                        "field": "validate_schema",
                        "operator": "equals",
                        "value": false
                    }
                }
            ]
        }
    });

    let response = post_request(
        &context.pool,
        &context.admin_token,
        "/api/v1/search",
        request,
    )
    .await;
    let response = assert_response_status(response, http::StatusCode::OK).await;
    let body: StructuredSearchResponse = test::read_body_json(response).await;

    assert_eq!(class_results(body)[0].id, class.id);
    collection.cleanup().await.unwrap();
}

#[actix_web::test]
async fn structured_search_targets_users_for_admins() {
    let context = TestContext::new().await;
    let request = json!({
        "version": 1,
        "target": {"kind": "user"},
        "filter": {
            "op": "field",
            "predicate": {
                "field": "id",
                "operator": "equals",
                "value": context.admin_user.id
            }
        }
    });

    let response = post_request(
        &context.pool,
        &context.admin_token,
        "/api/v1/search",
        request,
    )
    .await;
    let response = assert_response_status(response, http::StatusCode::OK).await;
    let body: StructuredSearchResponse = test::read_body_json(response).await;

    assert_eq!(user_results(body)[0].id, context.admin_user.id);
}

#[actix_web::test]
async fn structured_search_targets_groups_for_human_principals() {
    let context = TestContext::new().await;
    let group = create_test_group(&context.pool).await;
    let request = json!({
        "version": 1,
        "target": {"kind": "group"},
        "filter": {
            "op": "and",
            "args": [
                {
                    "op": "field",
                    "predicate": {
                        "field": "name",
                        "operator": "equals",
                        "value": group.groupname
                    }
                },
                {
                    "op": "field",
                    "predicate": {
                        "field": "managed_by",
                        "operator": "equals",
                        "value": group.managed_by
                    }
                }
            ]
        }
    });

    let response = post_request(
        &context.pool,
        &context.normal_token,
        "/api/v1/search",
        request,
    )
    .await;
    let response = assert_response_status(response, http::StatusCode::OK).await;
    let body: StructuredSearchResponse = test::read_body_json(response).await;

    assert_eq!(group_results(body)[0].id, group.id);
}

#[actix_web::test]
async fn structured_service_account_search_reuses_owner_group_visibility() {
    let context = TestContext::new().await;
    let owner_group = create_test_group(&context.pool).await;
    owner_group
        .add_member_without_events(&context.pool, &context.normal_user)
        .await
        .unwrap();
    let account =
        create_test_service_account(&context.pool, &owner_group, Some(context.normal_user.id))
            .await;
    let request = json!({
        "version": 1,
        "target": {"kind": "service_account"},
        "filter": {
            "op": "and",
            "args": [
                {
                    "op": "field",
                    "predicate": {
                        "field": "id",
                        "operator": "equals",
                        "value": account.id
                    }
                },
                {
                    "op": "field",
                    "predicate": {
                        "field": "owner_group_id",
                        "operator": "equals",
                        "value": owner_group.id
                    }
                }
            ]
        }
    });

    let response = post_request(
        &context.pool,
        &context.normal_token,
        "/api/v1/search",
        request,
    )
    .await;
    let response = assert_response_status(response, http::StatusCode::OK).await;
    let body: StructuredSearchResponse = test::read_body_json(response).await;

    assert_eq!(service_account_results(body)[0].id, account.id);
}

#[actix_web::test]
async fn structured_not_is_the_complement_for_nullable_fields() {
    let context = TestContext::new().await;
    assert!(context.admin_user.email.is_none());
    let request = json!({
        "version": 1,
        "target": {"kind": "user"},
        "filter": {
            "op": "and",
            "args": [
                {
                    "op": "field",
                    "predicate": {
                        "field": "id",
                        "operator": "equals",
                        "value": context.admin_user.id
                    }
                },
                {
                    "op": "not",
                    "arg": {
                        "op": "field",
                        "predicate": {
                            "field": "email",
                            "operator": "equals",
                            "value": "absent@example.invalid"
                        }
                    }
                }
            ]
        }
    });

    let response = post_request(
        &context.pool,
        &context.admin_token,
        "/api/v1/search",
        request,
    )
    .await;
    let response = assert_response_status(response, http::StatusCode::OK).await;
    let body: StructuredSearchResponse = test::read_body_json(response).await;

    assert_eq!(user_results(body)[0].id, context.admin_user.id);
}

#[actix_web::test]
async fn structured_user_search_rejects_non_admins() {
    let context = TestContext::new().await;
    let request = json!({
        "version": 1,
        "target": {"kind": "user"}
    });

    let response = post_request(
        &context.pool,
        &context.normal_token,
        "/api/v1/search",
        request,
    )
    .await;

    assert_response_status(response, http::StatusCode::FORBIDDEN).await;
}

#[actix_web::test]
async fn structured_search_targets_audit_events() {
    let context = TestContext::new().await;
    let summary = context.scoped_name("structured_audit_summary");
    let event = NewEvent::new(
        EntityType::Collection,
        Action::Created,
        ActorKind::System,
        summary.clone(),
    )
    .unwrap();
    let persisted = with_transaction(&context.pool, async |conn| emit_event(conn, &event).await)
        .await
        .unwrap();
    let request = json!({
        "version": 1,
        "target": {"kind": "audit_event"},
        "filter": {
            "op": "field",
            "predicate": {
                "field": "summary",
                "operator": "equals",
                "value": summary
            }
        }
    });

    let response = post_request(
        &context.pool,
        &context.admin_token,
        "/api/v1/search",
        request,
    )
    .await;
    let response = assert_response_status(response, http::StatusCode::OK).await;
    let body: StructuredSearchResponse = test::read_body_json(response).await;

    assert_eq!(audit_event_results(body)[0].id, persisted.id);
}

#[actix_web::test]
async fn structured_search_composes_nested_boolean_field_predicates() {
    let context = TestContext::new().await;
    let collection = context.collection_fixture("structured_boolean").await;
    let class = save_class(&context, &collection, "service").await;
    let alpha = save_object(
        &context,
        &collection,
        &class,
        "alpha",
        "production",
        json!({"tier": "gold"}),
    )
    .await;
    let beta = save_object(
        &context,
        &collection,
        &class,
        "beta",
        "production",
        json!({"tier": "silver"}),
    )
    .await;
    save_object(
        &context,
        &collection,
        &class,
        "gamma",
        "development",
        json!({"tier": "gold"}),
    )
    .await;
    let request = request_for_class(
        &class,
        json!({
            "op": "and",
            "args": [
                {
                    "op": "field",
                    "predicate": {
                        "field": "description",
                        "operator": "equals",
                        "value": "production"
                    }
                },
                {
                    "op": "or",
                    "args": [
                        {
                            "op": "field",
                            "predicate": {
                                "field": "json_data",
                                "path": "tier",
                                "operator": "equals",
                                "value": "gold"
                            }
                        },
                        {
                            "op": "field",
                            "predicate": {
                                "field": "name",
                                "operator": "equals",
                                "value": beta.name
                            }
                        }
                    ]
                }
            ]
        }),
    );

    let response = post_request(
        &context.pool,
        &context.admin_token,
        "/api/v1/search",
        request,
    )
    .await;
    let response = assert_response_status(response, http::StatusCode::OK).await;
    let body: StructuredSearchResponse = test::read_body_json(response).await;

    assert_eq!(
        object_results(body)
            .iter()
            .map(|object| object.id)
            .collect::<Vec<_>>(),
        vec![alpha.id, beta.id]
    );
    collection.cleanup().await.unwrap();
}

#[actix_web::test]
async fn not_related_means_no_visible_related_target_matches() {
    let context = TestContext::new().await;
    let collection = context.collection_fixture("structured_not_related").await;
    let host_class = save_class(&context, &collection, "host").await;
    let room_class = save_class(&context, &collection, "room").await;
    let class_relation = NewHubuumClassRelation {
        from_hubuum_class_id: host_class.id,
        to_hubuum_class_id: room_class.id,
        forward_template_alias: None,
        reverse_template_alias: None,
        from_max_relations: None,
        to_max_relations: None,
    }
    .save_without_events(&context.pool)
    .await
    .unwrap();
    let related_host = save_object(
        &context,
        &collection,
        &host_class,
        "related_host",
        "host",
        json!({}),
    )
    .await;
    let unrelated_host = save_object(
        &context,
        &collection,
        &host_class,
        "unrelated_host",
        "host",
        json!({}),
    )
    .await;
    let room = save_object(
        &context,
        &collection,
        &room_class,
        "gold_room",
        "room",
        json!({}),
    )
    .await;
    NewHubuumObjectRelation {
        from_hubuum_object_id: related_host.id,
        to_hubuum_object_id: room.id,
        class_relation_id: class_relation.id,
    }
    .save_without_events(&context.pool)
    .await
    .unwrap();
    let request = request_for_class(
        &host_class,
        json!({
            "op": "not",
            "arg": {
                "op": "related",
                "predicate": {
                    "class": {"name": room_class.name},
                    "filters": [{
                        "field": "name",
                        "operator": "equals",
                        "value": room.name
                    }],
                    "depth": 1
                }
            }
        }),
    );

    let response = post_request(
        &context.pool,
        &context.admin_token,
        "/api/v1/search",
        request,
    )
    .await;
    let response = assert_response_status(response, http::StatusCode::OK).await;
    let body: StructuredSearchResponse = test::read_body_json(response).await;

    assert_eq!(object_results(body), vec![unrelated_host]);
    collection.cleanup().await.unwrap();
}

#[actix_web::test]
async fn structured_cursor_is_rejected_after_the_sort_changes() {
    let context = TestContext::new().await;
    let collection = context.collection_fixture("structured_cursor").await;
    let class = save_class(&context, &collection, "cursor_class").await;
    for label in ["first", "second"] {
        save_object(&context, &collection, &class, label, label, json!({})).await;
    }
    let mut request = json!({
        "version": 1,
        "target": {"kind": "object", "class": {"name": class.name}},
        "sort": [{"field": "name", "direction": "asc"}],
        "limit": 1,
        "include_total": false
    });
    let response = post_request(
        &context.pool,
        &context.admin_token,
        "/api/v1/search",
        &request,
    )
    .await;
    let response = assert_response_status(response, http::StatusCode::OK).await;
    let first_page: StructuredSearchResponse = test::read_body_json(response).await;
    request["cursor"] = json!(first_page.next.unwrap());
    request["sort"][0]["direction"] = json!("desc");

    let response = post_request(
        &context.pool,
        &context.admin_token,
        "/api/v1/search",
        request,
    )
    .await;

    assert_response_status(response, http::StatusCode::BAD_REQUEST).await;
    collection.cleanup().await.unwrap();
}

#[actix_web::test]
async fn external_policy_structured_search_only_uses_visible_paths() {
    let context = TestContext::new().await;
    let collection = context.collection_fixture("structured_external").await;
    let group = create_test_group(&context.pool).await;
    group
        .add_member_without_events(&context.pool, &context.normal_user)
        .await
        .unwrap();
    let host_class = save_class(&context, &collection, "external_host").await;
    let room_class = save_class(&context, &collection, "external_room").await;
    let class_relation = NewHubuumClassRelation {
        from_hubuum_class_id: host_class.id,
        to_hubuum_class_id: room_class.id,
        forward_template_alias: None,
        reverse_template_alias: None,
        from_max_relations: None,
        to_max_relations: None,
    }
    .save_without_events(&context.pool)
    .await
    .unwrap();
    let visible_host = save_object(
        &context,
        &collection,
        &host_class,
        "visible_host",
        "host",
        json!({}),
    )
    .await;
    let hidden_host = save_object(
        &context,
        &collection,
        &host_class,
        "hidden_host",
        "host",
        json!({}),
    )
    .await;
    let room = save_object(
        &context,
        &collection,
        &room_class,
        "visible_room",
        "room",
        json!({}),
    )
    .await;
    let visible_relation = NewHubuumObjectRelation {
        from_hubuum_object_id: visible_host.id,
        to_hubuum_object_id: room.id,
        class_relation_id: class_relation.id,
    }
    .save_without_events(&context.pool)
    .await
    .unwrap();
    NewHubuumObjectRelation {
        from_hubuum_object_id: hidden_host.id,
        to_hubuum_object_id: room.id,
        class_relation_id: class_relation.id,
    }
    .save_without_events(&context.pool)
    .await
    .unwrap();

    let permission_backend = MockTreetopBackend::new();
    permission_backend.add_rule(MockAllowRule {
        group_id: group.id,
        action: Permissions::ReadCollection,
        resource_kind: ResourceKind::Collection,
        resource_id: Some(collection.collection.id),
        attrs: ResourceAttrs::default(),
    });
    for class_id in [host_class.id, room_class.id] {
        permission_backend.add_rule(MockAllowRule {
            group_id: group.id,
            action: Permissions::ReadClass,
            resource_kind: ResourceKind::Class,
            resource_id: Some(class_id),
            attrs: ResourceAttrs::default(),
        });
    }
    for object_id in [visible_host.id, room.id] {
        permission_backend.add_rule(MockAllowRule {
            group_id: group.id,
            action: Permissions::ReadObject,
            resource_kind: ResourceKind::Object,
            resource_id: Some(object_id),
            attrs: ResourceAttrs::default(),
        });
    }
    permission_backend.add_rule(MockAllowRule {
        group_id: group.id,
        action: Permissions::ReadObjectRelation,
        resource_kind: ResourceKind::ObjectRelation,
        resource_id: Some(visible_relation.id),
        attrs: ResourceAttrs::default(),
    });
    let request = request_for_class(
        &host_class,
        json!({
            "op": "related",
            "predicate": {
                "class": {"id": room_class.id},
                "filters": [{
                    "field": "name",
                    "operator": "equals",
                    "value": room.name
                }]
            }
        }),
    );

    let response = post_request_with_permission_backend(
        &context.pool,
        &context.normal_token,
        "/api/v1/search",
        request,
        Arc::new(permission_backend),
    )
    .await;
    let response = assert_response_status(response, http::StatusCode::OK).await;
    let body: StructuredSearchResponse = test::read_body_json(response).await;

    assert_eq!(object_results(body), vec![visible_host]);
    collection.cleanup().await.unwrap();
    group.delete_without_events(&context.pool).await.unwrap();
}
