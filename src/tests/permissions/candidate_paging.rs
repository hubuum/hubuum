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
async fn external_text_pages_preserve_byte_order_through_the_last_cursor(
    #[values(Listing::Classes, Listing::Objects)] listing: Listing,
    #[values(false, true)] include_total: bool,
) {
    let context = TestContext::new().await;
    let fixture = context.collection_fixture("text_candidate_pages").await;
    let collection_id = fixture.collection.id;
    let backend = Arc::new(MockTreetopBackend::new());
    let mut names = Vec::new();
    let mut parent_class = None;
    let prefix = context.scoped_name("text_sort");
    for suffix in ["a", "Z", "é", "z"] {
        let name = format!("{prefix}_{suffix}");
        if matches!(listing, Listing::Classes) || parent_class.is_none() {
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
            parent_class = Some(class.id);
        }
        if matches!(listing, Listing::Objects) {
            NewHubuumObject {
                name: name.clone(),
                collection_id,
                hubuum_class_id: parent_class.unwrap(),
                description: String::new(),
                data: serde_json::json!({}),
            }
            .save_without_events(&context.pool)
            .await
            .unwrap();
        }
        names.push(name);
    }
    names.sort();
    backend.add_rule(MockAllowRule {
        group_id: fixture.owner_group.id,
        action: if matches!(listing, Listing::Classes) {
            Permissions::ReadClass
        } else {
            Permissions::ReadObject
        },
        resource_kind: if matches!(listing, Listing::Classes) {
            ResourceKind::Class
        } else {
            ResourceKind::Object
        },
        resource_id: None,
        attrs: ResourceFields {
            collection_id: Some(collection_id),
            ..Default::default()
        },
    });
    let endpoint = if matches!(listing, Listing::Classes) {
        format!("/api/v1/classes?collections={collection_id}")
    } else {
        format!(
            "/api/v1/classes/{}/?collections={collection_id}",
            parent_class.unwrap()
        )
    };
    let mut cursor = None;
    for (index, expected) in names.iter().enumerate() {
        let cursor_query = cursor
            .as_ref()
            .map(|cursor| format!("&cursor={cursor}"))
            .unwrap_or_default();
        let response = get_request_with_permission_backend(
            &context.pool,
            &context.admin_token,
            &format!("{endpoint}&sort=name&limit=1&include_total={include_total}{cursor_query}"),
            backend.clone(),
        )
        .await;
        let response = assert_response_status(response, StatusCode::OK).await;
        assert_eq!(
            response
                .headers()
                .get(TOTAL_COUNT_HEADER)
                .map(|value| value.to_str().unwrap()),
            include_total.then_some("4")
        );
        cursor = response
            .headers()
            .get(NEXT_CURSOR_HEADER)
            .map(|value| value.to_str().unwrap().to_string());
        assert_eq!(cursor.is_some(), index + 1 < names.len());
        let rows: Vec<Value> = test::read_body_json(response).await;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["name"], *expected);
    }
    fixture.cleanup().await.unwrap();
}

#[actix_web::test]
async fn external_object_page_does_not_encode_its_oversized_look_ahead() {
    let context = TestContext::new().await;
    let fixture = context
        .collection_fixture("oversized_candidate_cursor")
        .await;
    let collection_id = fixture.collection.id;
    let class = NewHubuumClass {
        name: context.scoped_name("oversized_cursor_class"),
        collection_id,
        json_schema: None,
        validate_schema: Some(false),
        description: String::new(),
    }
    .save_without_events(&context.pool)
    .await
    .unwrap();
    for (index, description) in ["a".to_string(), "b".repeat(50_000), "c".to_string()]
        .into_iter()
        .enumerate()
    {
        NewHubuumObject {
            name: context.scoped_name(&format!("oversized_cursor_{index}")),
            collection_id,
            hubuum_class_id: class.id,
            description,
            data: serde_json::json!({}),
        }
        .save_without_events(&context.pool)
        .await
        .unwrap();
    }
    let backend = Arc::new(MockTreetopBackend::new());
    backend.add_rule(MockAllowRule {
        group_id: fixture.owner_group.id,
        action: Permissions::ReadObject,
        resource_kind: ResourceKind::Object,
        resource_id: None,
        attrs: ResourceFields {
            collection_id: Some(collection_id),
            ..Default::default()
        },
    });
    let response = get_request_with_permission_backend(
        &context.pool,
        &context.admin_token,
        &format!(
            "/api/v1/classes/{}/?sort=description&limit=1&include_total=false",
            class.id
        ),
        backend,
    )
    .await;
    let response = assert_response_status(response, StatusCode::OK).await;
    assert!(response.headers().contains_key(NEXT_CURSOR_HEADER));
    let rows: Vec<Value> = test::read_body_json(response).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["description"], "a");
    fixture.cleanup().await.unwrap();
}

#[rstest]
#[case::classes(Listing::Classes, false)]
#[case::objects(Listing::Objects, false)]
#[case::computed_arrays(Listing::Objects, true)]
#[actix_web::test]
async fn external_lists_continue_past_oversized_denied_sort_values(
    #[case] listing: Listing,
    #[case] computed: bool,
    #[values(false, true)] include_total: bool,
    #[values(false, true)] descending: bool,
) {
    use crate::tests::api_operations::post_request;
    use serde_json::json;

    let context = TestContext::new().await;
    let fixture = context.collection_fixture("large_denied_sort_values").await;
    let collection_id = fixture.collection.id;
    let class = NewHubuumClass {
        name: context.scoped_name("large_denied_sort_class"),
        collection_id,
        json_schema: None,
        validate_schema: Some(false),
        description: String::new(),
    }
    .save_without_events(&context.pool)
    .await
    .unwrap();
    if computed {
        let response = post_request(&context.pool, &context.admin_token,
            &format!("/api/v1/classes/{}/computed-fields", class.id),
            json!({"key": "order", "label": "Order", "operation": {"type": "first_non_null", "paths": ["/order"]}, "result_type": "array"}),
        ).await;
        assert_response_status(response, StatusCode::CREATED).await;
    }
    let backend = Arc::new(MockTreetopBackend::new());
    let prefix = context.scoped_name("selected_large_sort");
    let mut visible_ids = Vec::new();
    for index in 0..129 {
        let mut value = format!("{:03}", if descending { 128 - index } else { index });
        // Force a continuation at an oversized denied row in each count mode.
        if index == if include_total { 127 } else { 1 } {
            value.push_str(&"x".repeat(50_000));
        }
        let name = format!("{prefix}_{index:03}");
        let (id, action, resource_kind) = if matches!(listing, Listing::Classes) {
            let row = NewHubuumClass {
                name,
                collection_id,
                json_schema: None,
                validate_schema: Some(false),
                description: value,
            }
            .save_without_events(&context.pool)
            .await
            .unwrap();
            (row.id, Permissions::ReadClass, ResourceKind::Class)
        } else {
            let row = NewHubuumObject {
                name,
                collection_id,
                hubuum_class_id: class.id,
                description: value.clone(),
                data: json!({"order": [value]}),
            }
            .save_without_events(&context.pool)
            .await
            .unwrap();
            (row.id, Permissions::ReadObject, ResourceKind::Object)
        };
        if index == 0 || index == 128 {
            visible_ids.push(id);
            backend.add_rule(MockAllowRule {
                group_id: fixture.owner_group.id,
                action,
                resource_kind,
                resource_id: Some(id),
                attrs: ResourceFields::default(),
            });
        }
    }
    let endpoint = if matches!(listing, Listing::Classes) {
        format!("/api/v1/classes?collections={collection_id}&name__startswith={prefix}")
    } else {
        format!("/api/v1/classes/{}/?name__startswith={prefix}", class.id)
    };
    let sort = if computed {
        "computed.shared.order"
    } else {
        "description"
    };
    let direction = if descending { ".desc" } else { "" };
    let mut cursor = None;
    for (page_index, expected_id) in visible_ids.iter().enumerate() {
        let cursor_query = cursor
            .as_ref()
            .map(|value| format!("&cursor={value}"))
            .unwrap_or_default();
        let (response, fetched) = capture_candidate_fetches(get_request_with_permission_backend(
            &context.pool, &context.admin_token,
            &format!("{endpoint}&sort={sort}{direction}&limit=1&include_total={include_total}{cursor_query}"),
            backend.clone(),
        )).await;
        let response = assert_response_status(response, StatusCode::OK).await;
        assert_eq!(
            response
                .headers()
                .get(TOTAL_COUNT_HEADER)
                .map(|value| value.to_str().unwrap()),
            include_total.then_some("2")
        );
        cursor = response
            .headers()
            .get(NEXT_CURSOR_HEADER)
            .map(|value| value.to_str().unwrap().to_string());
        assert_eq!(cursor.is_some(), page_index == 0);
        let rows: Vec<Value> = test::read_body_json(response).await;
        assert_eq!(
            rows.iter()
                .map(|row| row["id"].as_i64().unwrap())
                .collect::<Vec<_>>(),
            vec![i64::from(*expected_id)]
        );
        // Continued JSON pages with exact totals need two count batches in
        // addition to the bounded storage scan that locates the response.
        let maximum_fetches = if computed && include_total && page_index > 0 {
            9
        } else {
            7
        };
        assert!(
            fetched.len() <= maximum_fetches,
            "sparse paging work: {fetched:?}"
        );
        assert!(fetched.iter().all(|rows| *rows <= 129));
    }
    fixture.cleanup().await.unwrap();
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
    assert_eq!(
        queries.queries_matching("SELECT \"collections\".\"id\" FROM \"collections\""),
        0,
        "candidate enumeration must not load all collection IDs: {queries:?}"
    );
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
            fetched.iter().all(|count| *count <= 129),
            "fetched rows: {fetched:?}"
        );
        if sparse {
            assert!(
                fetched.len() <= 7,
                "too many sparse candidate fetches: {fetched:?}"
            );
        } else {
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
        if after_cursor { 9 } else { 140 }
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

#[rstest]
#[case::classes("class", Listing::Classes)]
#[case::objects("object", Listing::Objects)]
#[actix_web::test]
async fn denied_ranked_search_bounds_database_and_policy_round_trips(
    #[case] kind: &str,
    #[case] listing: Listing,
) {
    use crate::models::UnifiedSearchResponse;
    let context = TestContext::new().await;
    let fixture = context.collection_fixture("denied_ranked_search").await;
    let class = NewHubuumClass {
        name: context.scoped_name("ranked_parent"),
        collection_id: fixture.collection.id,
        json_schema: None,
        validate_schema: Some(false),
        description: String::new(),
    }
    .save_without_events(&context.pool)
    .await
    .unwrap();
    let needle = context.scoped_name("deniedranked").replace('_', "");
    for index in 0..300 {
        let name = format!("{needle}{index:03}");
        if matches!(listing, Listing::Classes) {
            NewHubuumClass {
                name,
                collection_id: fixture.collection.id,
                json_schema: None,
                validate_schema: Some(false),
                description: String::new(),
            }
            .save_without_events(&context.pool)
            .await
            .unwrap();
        } else {
            NewHubuumObject {
                name,
                collection_id: fixture.collection.id,
                hubuum_class_id: class.id,
                description: String::new(),
                data: serde_json::json!({}),
            }
            .save_without_events(&context.pool)
            .await
            .unwrap();
        }
    }
    let backend = Arc::new(MockTreetopBackend::new());
    let (response, queries) =
        hubuum_storage_postgres::capture_queries(get_request_with_permission_backend(
            &context.pool,
            &context.admin_token,
            &format!("/api/v1/search?q={needle}&kinds={kind}&limit_per_kind=1"),
            backend.clone(),
        ))
        .await;
    let response = assert_response_status(response, StatusCode::OK).await;
    let response: UnifiedSearchResponse = test::read_body_json(response).await;
    assert!(response.results.classes.is_empty() && response.results.objects.is_empty());
    let batches = backend.authorization_batch_sizes();
    assert_eq!(batches.iter().sum::<usize>(), 300);
    assert!(batches.len() <= 10, "policy round trips: {batches:?}");
    // Class pages include snapshot transaction control statements and their
    // collection expansion; the budget includes those database round trips.
    assert!(
        queries.total_queries() <= 50,
        "database round trips: {queries:?}"
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
async fn external_computed_filter_preserves_projection_query_budget(
    #[values(false, true)] include_total: bool,
    #[values(false, true)] sparse: bool,
    #[values(false, true)] include_computed: bool,
) {
    use crate::tests::api_operations::post_request;
    use serde_json::json;

    let context = TestContext::new().await;
    let fixture = context
        .collection_fixture("computed_candidate_budget")
        .await;
    let collection_id = fixture.collection.id;
    let class = NewHubuumClass {
        name: context.scoped_name("computed_budget_class"),
        collection_id,
        json_schema: None,
        validate_schema: Some(false),
        description: String::new(),
    }
    .save_without_events(&context.pool)
    .await
    .unwrap();
    let response = post_request(&context.pool, &context.admin_token,
        &format!("/api/v1/classes/{}/computed-fields", class.id),
        json!({"key": "order", "label": "Order", "operation": {"type": "first_non_null", "paths": ["/order"]}, "result_type": "number"}),
    ).await;
    assert_response_status(response, StatusCode::CREATED).await;
    let response = post_request(&context.pool, &context.admin_token,
        "/api/v1/iam/me/computed-fields",
        json!({"class_id": class.id, "key": "personal_order", "label": "Personal order", "operation": {"type": "first_non_null", "paths": ["/order"]}, "result_type": "number"}),
    ).await;
    assert_response_status(response, StatusCode::CREATED).await;
    let backend = Arc::new(MockTreetopBackend::new());
    backend.add_rule(MockAllowRule {
        group_id: fixture.owner_group.id,
        action: Permissions::ReadClass,
        resource_kind: ResourceKind::Class,
        resource_id: Some(class.id),
        attrs: ResourceFields::default(),
    });
    let mut allowed = Vec::new();
    for index in 0..140 {
        let object = NewHubuumObject {
            name: context.scoped_name(&format!("computed_budget_{index:03}")),
            collection_id,
            hubuum_class_id: class.id,
            description: String::new(),
            data: json!({"order": index}),
        }
        .save_without_events(&context.pool)
        .await
        .unwrap();
        if !sparse || [130, 135, 139].contains(&index) {
            allowed.push((object.id, index));
            backend.add_rule(MockAllowRule {
                group_id: fixture.owner_group.id,
                action: Permissions::ReadObject,
                resource_kind: ResourceKind::Object,
                resource_id: Some(object.id),
                attrs: ResourceFields::default(),
            });
        }
    }
    let include = if include_computed {
        "&include=computed"
    } else {
        ""
    };
    let mut cursor = None;
    for (page_index, (expected_id, expected_value)) in allowed.iter().take(2).enumerate() {
        let cursor_query = cursor
            .as_ref()
            .map(|cursor| format!("&cursor={cursor}"))
            .unwrap_or_default();
        let ((response, fetched), queries) = hubuum_storage_postgres::capture_queries(capture_candidate_fetches(
            get_request_with_permission_backend(&context.pool, &context.admin_token,
                &format!("/api/v1/classes/{}/?computed.shared.order__gte=0&sort=id&limit=1&include_total={include_total}{include}{cursor_query}", class.id), backend.clone()),
        )).await;
        let response = assert_response_status(response, StatusCode::OK).await;
        assert_eq!(
            response
                .headers()
                .get(TOTAL_COUNT_HEADER)
                .map(|value| value.to_str().unwrap().to_string()),
            include_total.then(|| allowed.len().to_string())
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
        let rows: Vec<Value> = test::read_body_json(response).await;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["id"], *expected_id);
        if include_computed {
            assert_eq!(
                rows[0]["computed"]["shared"]["values"]["order"],
                *expected_value
            );
            assert_eq!(
                rows[0]["computed"]["personal"]["values"]["personal_order"],
                *expected_value
            );
        } else {
            assert!(rows[0].get("computed").is_none());
        }
        assert_eq!(
            queries.queries_matching("SELECT \"object_computed_data\"."),
            if include_computed { fetched.len() } else { 0 },
            "{queries:?}"
        );
        assert_eq!(
            queries.queries_matching("SELECT \"hubuumobject\"."),
            fetched.len(),
            "{queries:?}"
        );
        assert_eq!(
            queries.queries_matching("SELECT \"computed_field_definitions\"."),
            fetched.len(),
            "{queries:?}"
        );
        if include_total {
            assert_eq!(fetched, vec![129, 12]);
        } else if sparse {
            assert!(
                fetched.len() <= if page_index == 0 { 7 } else { 3 },
                "sparse policies must amortize candidate queries: {fetched:?}"
            );
        } else {
            assert_eq!(fetched, vec![3]);
        }
    }
    fixture.cleanup().await.unwrap();
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
        let ((response, fetched), queries) = hubuum_storage_postgres::capture_queries(capture_candidate_fetches(async {
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
        })).await;
        let response = assert_response_status(response, StatusCode::OK).await;
        assert_eq!(
            queries.queries_matching("SELECT \"collections\".\"id\" FROM \"collections\""),
            0,
            "candidate enumeration must not load all collection IDs: {queries:?}"
        );
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
