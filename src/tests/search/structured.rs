use hubuum_domain::ClassId;
use std::sync::Arc;

use actix_web::{http, test};
use rstest::rstest;
use serde_json::{Value, json};

use crate::events::EventResponse;
use crate::models::{
    Collection, GroupResponse, HubuumClass, HubuumClassExpanded, HubuumObject, NewHubuumClass,
    NewHubuumClassRelation, NewHubuumObject, NewHubuumObjectRelation, Permissions,
    ServiceAccountResponse, StructuredSearchResourceKind, StructuredSearchResponse,
    StructuredSearchResult, UserResponse,
};
use crate::permissions::test_support::{MockAllowRule, MockTreetopBackend};
use crate::permissions::{ResourceFields, ResourceKind};
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

async fn hydration_fixture(
    context: &TestContext,
    collection: &CollectionFixture,
) -> (
    HubuumClass,
    HubuumClass,
    Vec<HubuumObject>,
    Vec<crate::models::HubuumObjectRelation>,
) {
    let hosts = save_class(context, collection, "hydration_hosts").await;
    let sites = save_class(context, collection, "hydration_sites").await;
    let class_relation = NewHubuumClassRelation {
        from_hubuum_class_id: ClassId::new(hosts.id).unwrap(),
        to_hubuum_class_id: ClassId::new(sites.id).unwrap(),
        forward_label: None,
        reverse_label: None,
        forward_template_alias: None,
        reverse_template_alias: None,
        from_max_relations: None,
        to_max_relations: None,
    }
    .save_without_events(&context.pool)
    .await
    .unwrap();
    let mut objects = Vec::new();
    for (index, class) in [&hosts, &sites, &hosts, &sites].into_iter().enumerate() {
        objects.push(
            save_object(
                context,
                collection,
                class,
                &format!("hydration_{index}"),
                "hydration",
                json!({}),
            )
            .await,
        );
    }
    let mut relations = Vec::new();
    for (from, to) in [(0, 1), (2, 1), (2, 3), (0, 3)] {
        relations.push(
            NewHubuumObjectRelation {
                from_hubuum_object_id: objects[from].id,
                to_hubuum_object_id: objects[to].id,
                class_relation_id: class_relation.id.id(),
            }
            .save_without_events(&context.pool)
            .await
            .unwrap(),
        );
    }
    (hosts, sites, objects, relations)
}

fn hydration_request(root: &HubuumObject, target: &HubuumClass, limit: u8) -> Value {
    json!({ "version": 1, "root_class_id": root.hubuum_class_id, "root_object_id": root.id,
        "class": {"id": target.id}, "depth": 3, "limit": limit })
}

#[actix_web::test]
async fn hydration_deduplicates_shared_resources_and_bounds_cycles() {
    use crate::models::SearchHydrationResponse;
    use std::collections::HashSet;
    let context = TestContext::new().await;
    let collection = context.collection_fixture("hydration_cycles").await;
    let (_, sites, objects, _) = hydration_fixture(&context, &collection).await;
    let response = post_request(
        &context.pool,
        &context.admin_token,
        "/api/v1/search/hydrate",
        hydration_request(&objects[0], &sites, 50),
    )
    .await;
    let response = assert_response_status(response, http::StatusCode::OK).await;
    assert_eq!(
        response.headers().get("Cache-Control").unwrap(),
        "private, no-store"
    );
    let body: SearchHydrationResponse = test::read_body_json(response).await;
    assert!(!body.truncated);
    assert!(!body.paths.is_empty());
    assert_eq!(
        body.objects
            .iter()
            .map(|object| object.id)
            .collect::<HashSet<_>>()
            .len(),
        body.objects.len()
    );
    assert_eq!(
        body.relations
            .iter()
            .map(|relation| relation.id)
            .collect::<HashSet<_>>()
            .len(),
        body.relations.len()
    );
    for path in &body.paths {
        assert_eq!(path.object_ids.first(), Some(&objects[0].id));
        assert_eq!(
            path.object_ids.len(),
            path.object_ids.iter().collect::<HashSet<_>>().len()
        );
        assert!(path.object_ids.len() <= 4);
        assert_eq!(path.relation_ids.len() + 1, path.object_ids.len());
        for (pair, id) in path.object_ids.windows(2).zip(&path.relation_ids) {
            let relation = body
                .relations
                .iter()
                .find(|relation| relation.id == *id)
                .unwrap();
            assert_eq!(
                HashSet::from([pair[0], pair[1]]),
                HashSet::from([relation.from_hubuum_object_id, relation.to_hubuum_object_id])
            );
        }
    }
    collection.cleanup().await.unwrap();
}

#[actix_web::test]
async fn hydration_truncation_does_not_change_search_page_or_cursor() {
    use crate::models::SearchHydrationResponse;
    let context = TestContext::new().await;
    let collection = context.collection_fixture("hydration_page").await;
    let (hosts, sites, objects, _) = hydration_fixture(&context, &collection).await;
    let mut request = request_for_class(
        &hosts,
        json!({"op":"field","predicate":{"field":"description","operator":"equals","value":"hydration"}}),
    );
    request["limit"] = json!(1);
    let before = post_request(
        &context.pool,
        &context.admin_token,
        "/api/v1/search",
        request.clone(),
    )
    .await;
    let before: Value =
        test::read_body_json(assert_response_status(before, http::StatusCode::OK).await).await;
    let response = post_request(
        &context.pool,
        &context.admin_token,
        "/api/v1/search/hydrate",
        hydration_request(&objects[0], &sites, 1),
    )
    .await;
    let body: SearchHydrationResponse =
        test::read_body_json(assert_response_status(response, http::StatusCode::OK).await).await;
    assert!(body.truncated);
    assert_eq!(body.paths.len(), 1);
    let after = post_request(
        &context.pool,
        &context.admin_token,
        "/api/v1/search",
        request.clone(),
    )
    .await;
    let after: Value =
        test::read_body_json(assert_response_status(after, http::StatusCode::OK).await).await;
    assert_eq!(before, after);
    request["cursor"] = before["next"].clone();
    let next = post_request(
        &context.pool,
        &context.admin_token,
        "/api/v1/search",
        request,
    )
    .await;
    let next: Value =
        test::read_body_json(assert_response_status(next, http::StatusCode::OK).await).await;
    assert_ne!(before["results"], next["results"]);
    collection.cleanup().await.unwrap();
}

#[rstest]
#[case::hidden_relation(false, true)]
#[case::hidden_object(true, false)]
#[case::visible(true, true)]
#[actix_web::test]
async fn hydration_obeys_external_object_and_edge_permissions(
    #[case] allow_relation: bool,
    #[case] allow_object: bool,
) {
    use crate::models::SearchHydrationResponse;
    let context = TestContext::new().await;
    let collection = context.collection_fixture("hydration_permissions").await;
    let group = create_test_group(&context.pool).await;
    group
        .add_member_without_events(&context.pool, &context.normal_user)
        .await
        .unwrap();
    let (hosts, sites, objects, relations) = hydration_fixture(&context, &collection).await;
    let backend = MockTreetopBackend::new();
    let allow = |action, resource_kind, id| {
        backend.add_rule(MockAllowRule {
            group_id: group.id,
            action,
            resource_kind,
            resource_id: Some(id),
            attrs: ResourceFields::default(),
        })
    };
    allow(
        Permissions::ReadCollection,
        ResourceKind::Collection,
        collection.collection.id,
    );
    for id in [hosts.id, sites.id] {
        allow(Permissions::ReadClass, ResourceKind::Class, id);
    }
    allow(Permissions::ReadObject, ResourceKind::Object, objects[0].id);
    if allow_object {
        allow(Permissions::ReadObject, ResourceKind::Object, objects[1].id);
    }
    if allow_relation {
        allow(
            Permissions::ReadObjectRelation,
            ResourceKind::ObjectRelation,
            relations[0].id,
        );
    }
    let response = post_request_with_permission_backend(
        &context.pool,
        &context.normal_token,
        "/api/v1/search/hydrate",
        hydration_request(&objects[0], &sites, 50),
        Arc::new(backend),
    )
    .await;
    let body: SearchHydrationResponse =
        test::read_body_json(assert_response_status(response, http::StatusCode::OK).await).await;
    assert_eq!(body.paths.is_empty(), !(allow_relation && allow_object));
    assert!(!body.truncated);
    assert!(
        body.objects.iter().all(
            |object| object.id == objects[0].id || (allow_object && object.id == objects[1].id)
        )
    );
    assert!(
        body.relations
            .iter()
            .all(|relation| allow_relation && relation.id == relations[0].id)
    );
    collection.cleanup().await.unwrap();
}

#[rstest]
#[case::unicode_folding("(?i)^k$", "K", Some(true))]
#[case::newline("router.$", "router\n", Some(false))]
#[case::case_sensitive("ROUTER", "router", Some(false))]
#[case::empty("", "router", Some(true))]
#[case::invalid("[", "router", None)]
#[case::native_boundary(r"\yrouter\y", "router", None)]
#[actix_web::test]
async fn structured_search_uses_portable_regex(
    #[case] pattern: &str,
    #[case] value: &str,
    #[case] matched: Option<bool>,
    #[values("description", "json_data")] field: &str,
) {
    let context = TestContext::new().await;
    let collection = context.collection_fixture("structured_regex").await;
    let class = save_class(&context, &collection, "regex").await;
    save_object(
        &context,
        &collection,
        &class,
        "regex",
        value,
        json!({"value": value}),
    )
    .await;
    let mut predicate = json!({"field": field, "operator": "regex", "value": pattern});
    if field == "json_data" {
        predicate["path"] = json!("value");
    }
    let request = request_for_class(&class, json!({"op": "field", "predicate": predicate}));
    let response = post_request(
        &context.pool,
        &context.admin_token,
        "/api/v1/search",
        request,
    )
    .await;
    let status = if matched.is_none() {
        http::StatusCode::BAD_REQUEST
    } else {
        http::StatusCode::OK
    };
    let response = assert_response_status(response, status).await;
    if let Some(matched) = matched {
        let body: StructuredSearchResponse = test::read_body_json(response).await;
        assert_eq!(object_results(body).len(), usize::from(matched));
    }
    collection.cleanup().await.unwrap();
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
async fn structured_search_streams_the_same_tagged_collection_results() {
    let context = TestContext::new().await;
    let collection = context
        .collection_fixture("structured_collection_stream")
        .await;
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
        "/api/v1/search/stream",
        request,
    )
    .await;
    let response = assert_response_status(response, http::StatusCode::OK).await;
    assert_eq!(
        response.headers().get(http::header::CONTENT_TYPE).unwrap(),
        "text/event-stream; charset=utf-8"
    );
    let body = String::from_utf8(test::read_body(response).await.to_vec()).unwrap();
    let started = body.find("event: started").unwrap();
    let result = body.find("event: result").unwrap();
    let done = body.find("event: done").unwrap();

    assert!(started < result && result < done);
    assert!(body.contains("\"kind\":\"collection\""));
    assert!(body.contains(&collection.collection.name));
    assert!(body.contains("\"total\":1"));
    collection.cleanup().await.unwrap();
}

#[actix_web::test]
async fn structured_search_stream_reports_execution_authorization_errors_as_terminal_events() {
    let context = TestContext::new().await;
    let request = json!({
        "version": 1,
        "target": {"kind": "user"}
    });

    let response = post_request(
        &context.pool,
        &context.normal_token,
        "/api/v1/search/stream",
        request,
    )
    .await;
    let response = assert_response_status(response, http::StatusCode::OK).await;
    let body = String::from_utf8(test::read_body(response).await.to_vec()).unwrap();

    assert!(body.contains("event: started"));
    assert!(body.contains("event: error"));
    assert!(body.contains("user search requires administrator access"));
    assert!(!body.contains("event: result"));
    assert!(!body.contains("event: done"));
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
    let collection = context.collection_fixture("structured_audit").await;
    let summary = format!("Collection '{}' created", collection.collection.name);
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

    assert_eq!(audit_event_results(body)[0].summary, summary);
    collection.cleanup().await.unwrap();
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
async fn structured_search_accepts_dotted_json_paths() {
    let context = TestContext::new().await;
    let collection = context.collection_fixture("structured_dotted_path").await;
    let class = save_class(&context, &collection, "dotted_path_class").await;
    let matching = save_object(
        &context,
        &collection,
        &class,
        "dotted_path_match",
        "matching",
        json!({"hardware": {"cpu": {"count": 8}}}),
    )
    .await;
    save_object(
        &context,
        &collection,
        &class,
        "dotted_path_other",
        "other",
        json!({"hardware": {"cpu": {"count": 4}}}),
    )
    .await;
    let request = request_for_class(
        &class,
        json!({
            "op": "field",
            "predicate": {
                "field": "json_data",
                "operator": "equals",
                "path": "hardware.cpu.count",
                "value": 8
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

    assert_eq!(object_results(body), vec![matching]);
    collection.cleanup().await.unwrap();
}

#[actix_web::test]
async fn not_related_means_no_visible_related_target_matches() {
    let context = TestContext::new().await;
    let collection = context.collection_fixture("structured_not_related").await;
    let host_class = save_class(&context, &collection, "host").await;
    let room_class = save_class(&context, &collection, "room").await;
    let class_relation = NewHubuumClassRelation {
        from_hubuum_class_id: ClassId::new(host_class.id).unwrap(),
        to_hubuum_class_id: ClassId::new(room_class.id).unwrap(),
        forward_label: None,
        reverse_label: None,
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
        class_relation_id: class_relation.id.id(),
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
        from_hubuum_class_id: ClassId::new(host_class.id).unwrap(),
        to_hubuum_class_id: ClassId::new(room_class.id).unwrap(),
        forward_label: None,
        reverse_label: None,
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
        class_relation_id: class_relation.id.id(),
    }
    .save_without_events(&context.pool)
    .await
    .unwrap();
    NewHubuumObjectRelation {
        from_hubuum_object_id: hidden_host.id,
        to_hubuum_object_id: room.id,
        class_relation_id: class_relation.id.id(),
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
        attrs: ResourceFields::default(),
    });
    for class_id in [host_class.id, room_class.id] {
        permission_backend.add_rule(MockAllowRule {
            group_id: group.id,
            action: Permissions::ReadClass,
            resource_kind: ResourceKind::Class,
            resource_id: Some(class_id),
            attrs: ResourceFields::default(),
        });
    }
    for object_id in [visible_host.id, room.id] {
        permission_backend.add_rule(MockAllowRule {
            group_id: group.id,
            action: Permissions::ReadObject,
            resource_kind: ResourceKind::Object,
            resource_id: Some(object_id),
            attrs: ResourceFields::default(),
        });
    }
    permission_backend.add_rule(MockAllowRule {
        group_id: group.id,
        action: Permissions::ReadObjectRelation,
        resource_kind: ResourceKind::ObjectRelation,
        resource_id: Some(visible_relation.id),
        attrs: ResourceFields::default(),
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

#[actix_web::test]
async fn external_policy_object_search_requires_parent_collection_visibility() {
    let context = TestContext::new().await;
    let collection = context
        .collection_fixture("structured_external_object_collection")
        .await;
    let group = create_test_group(&context.pool).await;
    group
        .add_member_without_events(&context.pool, &context.normal_user)
        .await
        .unwrap();
    let class = save_class(&context, &collection, "external_object_class").await;
    let object = save_object(
        &context,
        &collection,
        &class,
        "external_object",
        "object",
        json!({}),
    )
    .await;
    let permission_backend = MockTreetopBackend::new();
    permission_backend.add_rule(MockAllowRule {
        group_id: group.id,
        action: Permissions::ReadObject,
        resource_kind: ResourceKind::Object,
        resource_id: Some(object.id),
        attrs: ResourceFields::default(),
    });
    let request = json!({
        "version": 1,
        "target": {"kind": "object"},
        "filter": {
            "op": "field",
            "predicate": {
                "field": "id",
                "operator": "equals",
                "value": object.id
            }
        },
        "include_total": true
    });

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
    let total = body.total;

    assert!(object_results(body).is_empty());
    assert_eq!(total, Some(0));
    collection.cleanup().await.unwrap();
    group.delete_without_events(&context.pool).await.unwrap();
}

#[actix_web::test]
async fn external_policy_class_search_requires_parent_collection_visibility() {
    let context = TestContext::new().await;
    let collection = context
        .collection_fixture("structured_external_class_collection")
        .await;
    let group = create_test_group(&context.pool).await;
    group
        .add_member_without_events(&context.pool, &context.normal_user)
        .await
        .unwrap();
    let class = save_class(&context, &collection, "external_visible_class").await;
    let permission_backend = MockTreetopBackend::new();
    permission_backend.add_rule(MockAllowRule {
        group_id: group.id,
        action: Permissions::ReadClass,
        resource_kind: ResourceKind::Class,
        resource_id: Some(class.id),
        attrs: ResourceFields::default(),
    });
    let request = json!({
        "version": 1,
        "target": {"kind": "class"},
        "filter": {
            "op": "field",
            "predicate": {
                "field": "id",
                "operator": "equals",
                "value": class.id
            }
        },
        "include_total": true
    });

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
    let total = body.total;

    assert!(class_results(body).is_empty());
    assert_eq!(total, Some(0));
    collection.cleanup().await.unwrap();
    group.delete_without_events(&context.pool).await.unwrap();
}
