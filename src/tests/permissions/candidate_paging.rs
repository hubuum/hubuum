//! Request-level row and policy-work regressions for external cursor pagination.

use std::sync::Arc;

use actix_web::{http::StatusCode, test};
use rstest::rstest;
use serde_json::Value;

use crate::models::{
    HubuumClassID, NewHubuumClass, NewHubuumObject, Permissions, UpdateHubuumClass,
};
use crate::pagination::{NEXT_CURSOR_HEADER, TOTAL_COUNT_HEADER};
use crate::permissions::test_support::{MockAllowRule, MockTreetopBackend};
use crate::permissions::visibility::capture_candidate_fetches;
use crate::permissions::{ResourceFields, ResourceKind};
use crate::tests::TestContext;
use crate::tests::api_operations::get_request_with_permission_backend;
use crate::tests::asserts::assert_response_status;
use crate::traits::{CanSave, CanUpdate};

#[derive(Clone, Copy, Debug)]
enum Listing {
    Classes,
    Objects,
    ClassHistory,
}

#[rstest]
#[actix_web::test]
async fn external_list_bounds_fetched_rows_and_authorization_work(
    #[values(Listing::Classes, Listing::Objects, Listing::ClassHistory)] listing: Listing,
    #[values(false, true)] include_total: bool,
    #[values(false, true)] after_cursor: bool,
    #[values(false, true)] sparse: bool,
) {
    let context = TestContext::new().await;
    let fixture = context.collection_fixture("candidate_paging").await;
    let group_id = fixture.owner_group.id;
    let collection_id = fixture.collection.id;
    let backend = Arc::new(MockTreetopBackend::new());
    let mut ids = Vec::new();
    let mut names = Vec::new();
    let mut parent_class = None;
    for index in 0..140 {
        let name = context.scoped_name(&format!("candidate_{index:03}"));
        if matches!(listing, Listing::Classes) || index == 0 {
            let class = NewHubuumClass {
                name: name.clone(),
                collection_id,
                json_schema: None,
                validate_schema: Some(false),
                description: "candidate".to_string(),
            }
            .save_without_events(&context.pool)
            .await
            .unwrap();
            if matches!(listing, Listing::Classes) {
                ids.push(class.id);
            }
            parent_class = Some(class.id);
        }
        let class_id = parent_class.unwrap();
        match listing {
            Listing::Objects => {
                let object = NewHubuumObject {
                    name: name.clone(),
                    collection_id,
                    hubuum_class_id: class_id,
                    description: "candidate".to_string(),
                    data: serde_json::json!({}),
                }
                .save_without_events(&context.pool)
                .await
                .unwrap();
                ids.push(object.id);
            }
            Listing::ClassHistory if index > 0 => {
                UpdateHubuumClass {
                    name: Some(name.clone()),
                    collection_id: None,
                    json_schema: None,
                    validate_schema: None,
                    description: None,
                }
                .update_without_events(&context.pool, HubuumClassID::new(class_id).unwrap())
                .await
                .unwrap();
            }
            _ => {}
        }
        names.push(name);
    }
    let (action, resource_kind) = match listing {
        Listing::Objects => (Permissions::ReadObject, ResourceKind::Object),
        _ => (Permissions::ReadClass, ResourceKind::Class),
    };
    if sparse {
        for index in [130, 135, 139] {
            backend.add_rule(MockAllowRule {
                group_id,
                action,
                resource_kind: resource_kind.clone(),
                resource_id: if matches!(listing, Listing::ClassHistory) {
                    parent_class
                } else {
                    Some(ids[index])
                },
                attrs: ResourceFields {
                    name: Some(names[index].clone()),
                    ..Default::default()
                },
            });
        }
    } else {
        backend.add_rule(MockAllowRule {
            group_id,
            action,
            resource_kind,
            resource_id: None,
            attrs: ResourceFields {
                collection_id: Some(collection_id),
                ..Default::default()
            },
        });
    }
    let endpoint = match listing {
        Listing::Classes => format!("/api/v1/classes?collections={collection_id}&sort=name"),
        Listing::Objects => format!("/api/v1/classes/{}/?sort=name", parent_class.unwrap()),
        Listing::ClassHistory => format!(
            "/api/v1/classes/{}/history?sort=history_id",
            parent_class.unwrap()
        ),
    };
    let mut endpoint = format!("{endpoint}&limit=1&include_total={include_total}");
    if after_cursor {
        let response = get_request_with_permission_backend(
            &context.pool,
            &context.admin_token,
            &endpoint,
            backend.clone(),
        )
        .await;
        let response = assert_response_status(response, StatusCode::OK).await;
        let cursor = response
            .headers()
            .get(NEXT_CURSOR_HEADER)
            .unwrap()
            .to_str()
            .unwrap();
        endpoint.push_str(&format!("&cursor={cursor}"));
    }
    let before = backend.authorization_batch_sizes().len();
    let ((response, fetched), queries) = hubuum_storage_postgres::capture_queries(
        capture_candidate_fetches(get_request_with_permission_backend(
            &context.pool,
            &context.admin_token,
            &endpoint,
            backend.clone(),
        )),
    )
    .await;
    let response = assert_response_status(response, StatusCode::OK).await;
    let table = match listing {
        Listing::Classes => "hubuumclass",
        Listing::Objects => "hubuumobject",
        Listing::ClassHistory => "hubuumclass_history",
    };
    let candidate_selects = queries
        .query_counts()
        .keys()
        .filter(|sql| sql.contains(&format!("FROM \"{table}\"")))
        .collect::<Vec<_>>();
    assert!(
        !candidate_selects.is_empty(),
        "missing storage query evidence: {queries:?}"
    );
    assert!(
        candidate_selects.iter().all(|sql| sql.contains(" LIMIT ")),
        "unbounded candidate SELECT: {candidate_selects:?}"
    );
    if include_total {
        assert_eq!(
            response.headers().get(TOTAL_COUNT_HEADER).unwrap(),
            if sparse { "3" } else { "140" }
        );
        assert_eq!(fetched, vec![129, 12]);
    } else {
        assert!(response.headers().get(TOTAL_COUNT_HEADER).is_none());
        assert!(!fetched.is_empty());
        assert!(
            fetched.iter().all(|count| *count <= 3),
            "fetched rows: {fetched:?}"
        );
        if !sparse {
            assert_eq!(fetched, vec![3]);
        }
    }
    assert!(response.headers().contains_key(NEXT_CURSOR_HEADER));
    let rows: Vec<Value> = test::read_body_json(response).await;
    let expected_index = match (sparse, after_cursor) {
        (false, false) => 0,
        (false, true) => 1,
        (true, false) => 130,
        (true, true) => 135,
    };
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["name"], names[expected_index]);
    let expected_candidates = if include_total {
        140
    } else if sparse {
        if after_cursor { 9 } else { 136 }
    } else {
        2
    };
    // History also authorizes the current resource once at the handler boundary.
    let boundary_checks = usize::from(matches!(listing, Listing::ClassHistory));
    let batches = backend.authorization_batch_sizes();
    assert_eq!(
        batches[before..].iter().sum::<usize>(),
        expected_candidates + boundary_checks
    );
    fixture.cleanup().await.unwrap();
}

#[derive(Clone, Copy, Debug)]
enum EquivalentListing {
    ClassRelations,
    ObjectRelations,
    TouchingClasses,
    TouchingObjects,
    Templates,
    StructuredClasses,
    StructuredObjects,
    ComputedObjects,
}

#[rstest]
#[actix_web::test]
async fn equivalent_external_lists_page_before_authorization(
    #[values(
        EquivalentListing::ClassRelations,
        EquivalentListing::ObjectRelations,
        EquivalentListing::TouchingClasses,
        EquivalentListing::TouchingObjects,
        EquivalentListing::Templates,
        EquivalentListing::StructuredClasses,
        EquivalentListing::StructuredObjects,
        EquivalentListing::ComputedObjects
    )]
    listing: EquivalentListing,
    #[values(false, true)] include_total: bool,
) {
    use crate::models::{
        ExportContentType, ExportTemplateKind, NewExportTemplate, NewHubuumClassRelation,
        NewHubuumObjectRelation,
    };
    use crate::tests::api_operations::{post_request, post_request_with_permission_backend};
    use serde_json::json;

    let context = TestContext::new().await;
    let fixture = context
        .collection_fixture("equivalent_candidate_pages")
        .await;
    let collection_id = fixture.collection.id;
    let backend = Arc::new(MockTreetopBackend::new());
    for (action, resource_kind) in [
        (Permissions::ReadCollection, ResourceKind::Collection),
        (Permissions::ReadClass, ResourceKind::Class),
        (Permissions::ReadObject, ResourceKind::Object),
        (Permissions::ReadClassRelation, ResourceKind::ClassRelation),
        (
            Permissions::ReadObjectRelation,
            ResourceKind::ObjectRelation,
        ),
        (Permissions::ReadTemplate, ResourceKind::Template),
    ] {
        backend.add_rule(MockAllowRule {
            group_id: fixture.owner_group.id,
            action,
            resource_kind,
            resource_id: None,
            attrs: ResourceFields::default(),
        });
    }
    let source = NewHubuumClass {
        name: context.scoped_name("anchor_class"),
        collection_id,
        json_schema: None,
        validate_schema: Some(false),
        description: String::new(),
    }
    .save_without_events(&context.pool)
    .await
    .unwrap();
    let anchor = NewHubuumObject {
        name: context.scoped_name("anchor_object"),
        collection_id,
        hubuum_class_id: source.id,
        description: String::new(),
        data: json!({"order": -1}),
    }
    .save_without_events(&context.pool)
    .await
    .unwrap();
    if matches!(listing, EquivalentListing::ComputedObjects) {
        let response = post_request(&context.pool, &context.admin_token,
            &format!("/api/v1/classes/{}/computed-fields", source.id),
            json!({"key": "order", "label": "Order", "operation": {"type": "first_non_null", "paths": ["/order"]}, "result_type": "number"}),
        ).await;
        assert_response_status(response, StatusCode::CREATED).await;
    }
    let prefix = context.scoped_name("selected");
    for index in 0..8 {
        let name = format!("{prefix}_{index:03}");
        match listing {
            EquivalentListing::Templates => {
                NewExportTemplate {
                    name,
                    collection_id,
                    description: String::new(),
                    content_type: ExportContentType::TextPlain,
                    template: "hello".to_string(),
                    kind: ExportTemplateKind::Fragment,
                    scope_kind: None,
                    class_id: None,
                    default_query: None,
                    include: None,
                    relation_context: None,
                    default_missing_data_policy: None,
                    default_limits: None,
                }
                .save_without_events(&context.pool)
                .await
                .unwrap();
            }
            EquivalentListing::StructuredObjects | EquivalentListing::ComputedObjects => {
                NewHubuumObject {
                    name,
                    collection_id,
                    hubuum_class_id: source.id,
                    description: String::new(),
                    data: json!({"order": index}),
                }
                .save_without_events(&context.pool)
                .await
                .unwrap();
            }
            _ => {
                let class = NewHubuumClass {
                    name: name.clone(),
                    collection_id,
                    json_schema: None,
                    validate_schema: Some(false),
                    description: String::new(),
                }
                .save_without_events(&context.pool)
                .await
                .unwrap();
                if !matches!(listing, EquivalentListing::StructuredClasses) {
                    let relation = NewHubuumClassRelation {
                        from_hubuum_class_id: source.id,
                        to_hubuum_class_id: class.id,
                        forward_template_alias: None,
                        reverse_template_alias: None,
                        from_max_relations: None,
                        to_max_relations: None,
                    }
                    .save_without_events(&context.pool)
                    .await
                    .unwrap();
                    if matches!(
                        listing,
                        EquivalentListing::ObjectRelations | EquivalentListing::TouchingObjects
                    ) {
                        let object = NewHubuumObject {
                            name,
                            collection_id,
                            hubuum_class_id: class.id,
                            description: String::new(),
                            data: json!({}),
                        }
                        .save_without_events(&context.pool)
                        .await
                        .unwrap();
                        NewHubuumObjectRelation {
                            from_hubuum_object_id: anchor.id,
                            to_hubuum_object_id: object.id,
                            class_relation_id: relation.id,
                        }
                        .save_without_events(&context.pool)
                        .await
                        .unwrap();
                    }
                }
            }
        }
    }
    // Walk two response pages: the second must retain the same global total
    // while only authorizing the next response and look-ahead when totals skip.
    let mut cursor: Option<String> = None;
    for page_number in 0..2 {
        let before = backend.authorization_batch_sizes().len();
        let structured = matches!(
            listing,
            EquivalentListing::StructuredClasses | EquivalentListing::StructuredObjects
        );
        let (response, fetched) = capture_candidate_fetches(async {
            if structured {
                post_request_with_permission_backend(&context.pool, &context.admin_token, "/api/v1/search",
                    json!({"version": 1, "target": {"kind": if matches!(listing, EquivalentListing::StructuredClasses) { "class" } else { "object" }},
                        "filter": {"op": "field", "predicate": {"field": "name", "operator": "startswith", "value": prefix}},
                        "sort": [{"field": "name", "direction": "asc"}],
                        "limit": 1, "include_total": include_total, "cursor": cursor}), backend.clone(),
                ).await
            } else {
                let endpoint = match listing {
                    EquivalentListing::ClassRelations => format!("/api/v1/relations/classes?from_classes={}", source.id),
                    EquivalentListing::ObjectRelations => format!("/api/v1/relations/objects?from_objects={}", anchor.id),
                    EquivalentListing::TouchingClasses => format!("/api/v1/classes/{}/related/relations?sort=id", source.id),
                    EquivalentListing::TouchingObjects => format!("/api/v1/classes/{}/objects/{}/related/relations?sort=id", source.id, anchor.id),
                    EquivalentListing::Templates => format!("/api/v1/export-templates?name__startswith={prefix}"),
                    EquivalentListing::ComputedObjects => format!("/api/v1/classes/{}/?computed.shared.order__gte=0&sort=computed.shared.order.desc,name", source.id),
                    _ => unreachable!(),
                };
                let cursor_query = cursor.as_ref().map(|cursor| format!("&cursor={cursor}")).unwrap_or_default();
                get_request_with_permission_backend(&context.pool, &context.admin_token,
                    &format!("{endpoint}&limit=1&include_total={include_total}{cursor_query}"), backend.clone(),
                ).await
            }
        }).await;
        let response = assert_response_status(response, StatusCode::OK).await;
        assert_eq!(fetched, if include_total { vec![8] } else { vec![3] });
        assert_eq!(
            response
                .headers()
                .get(TOTAL_COUNT_HEADER)
                .map(|header| header.to_str().unwrap()),
            include_total.then_some("8")
        );
        cursor = Some(
            response
                .headers()
                .get(NEXT_CURSOR_HEADER)
                .unwrap()
                .to_str()
                .unwrap()
                .to_string(),
        );
        let value: Value = test::read_body_json(response).await;
        let rows = if structured {
            value["results"].as_array().unwrap()
        } else {
            value.as_array().unwrap()
        };
        assert_eq!(rows.len(), 1);
        if matches!(listing, EquivalentListing::ComputedObjects) {
            assert_eq!(rows[0]["name"], format!("{prefix}_{:03}", 7 - page_number));
            assert!(rows[0].get("computed").is_none());
        }
        let boundary = usize::from(matches!(
            listing,
            EquivalentListing::TouchingClasses
                | EquivalentListing::TouchingObjects
                | EquivalentListing::ComputedObjects
        ));
        let permissions_per_candidate = if structured { 2 } else { 1 };
        let batches = backend.authorization_batch_sizes();
        assert_eq!(
            batches[before..].iter().sum::<usize>(),
            boundary + permissions_per_candidate * if include_total { 8 } else { 2 }
        );
    }
    fixture.cleanup().await.unwrap();
}
