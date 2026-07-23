//! Pre-refactor PostgreSQL query budgets for the storage boundary.
//!
//! These are deliberately model/storage-level checks rather than HTTP tests:
//! authentication and routing should not hide an extra pool checkout or an
//! N+1 introduced while storage capabilities are extracted.

use diesel::sql_types::{Integer, Json, Text};
use serde_json::Value;

use crate::db::prelude::{QueryableByName, RunQueryDsl};
use crate::db::traits::history::{
    HistoryCollectionFilter, collection_history_paginated_with_total_count, resolve_actor_usernames,
};
use crate::db::traits::user::UserSearchBackend;
use crate::db::with_actor_scope;
use crate::db::{DbPool, capture_queries, with_connection};
use crate::events::EventContext;
use crate::models::collection::effective_group_on;
use crate::models::search::parse_query_parameter;
use crate::models::{
    CollectionID, NewCollectionWithAssignee, NewHubuumClass, NewHubuumClassRelation,
    NewHubuumObject, NewHubuumObjectRelation, UpdateCollection, UserID, collection_ancestors,
};
use crate::tests::{TestScope, ensure_admin_user};
use crate::traits::{CanDelete, CanSave, CanUpdate, CollectionAccessors};

const REPRESENTATIVE_COLLECTION_ROWS: i32 = 2_000;

#[derive(QueryableByName)]
struct ExplainPlanRow {
    #[diesel(sql_type = Json)]
    #[diesel(column_name = "QUERY PLAN")]
    query_plan: Value,
}

fn plan_uses_index(plan: &Value, index_name_prefix: &str) -> bool {
    match plan {
        Value::Array(values) => values
            .iter()
            .any(|value| plan_uses_index(value, index_name_prefix)),
        Value::Object(fields) => {
            fields
                .get("Index Name")
                .and_then(Value::as_str)
                .is_some_and(|name| name.starts_with(index_name_prefix))
                || fields
                    .values()
                    .any(|value| plan_uses_index(value, index_name_prefix))
        }
        _ => false,
    }
}

fn root_plan(plan: &Value) -> &Value {
    plan.as_array()
        .and_then(|plans| plans.as_slice().first())
        .and_then(|explain| explain.get("Plan"))
        .expect("EXPLAIN JSON should contain a root plan")
}

fn root_shared_blocks(plan: &Value) -> u64 {
    let root = root_plan(plan);
    root.get("Shared Hit Blocks")
        .and_then(Value::as_u64)
        .unwrap_or_default()
        + root
            .get("Shared Read Blocks")
            .and_then(Value::as_u64)
            .unwrap_or_default()
}

async fn add_representative_collection_rows(pool: &DbPool, name_prefix: &str, parent_id: i32) {
    with_connection(pool, async |conn| {
        diesel::sql_query(
            "WITH inserted AS (\
                INSERT INTO collections (name, description, parent_collection_id) \
                SELECT $1 || '-' || sequence::text, 'query plan scale row', $2 \
                FROM generate_series(1, $3) AS sequence \
                RETURNING id\
            ) \
            INSERT INTO collection_closure \
                (ancestor_collection_id, descendant_collection_id, depth) \
            SELECT id, id, 0 FROM inserted",
        )
        .bind::<Text, _>(name_prefix)
        .bind::<Integer, _>(parent_id)
        .bind::<Integer, _>(REPRESENTATIVE_COLLECTION_ROWS)
        .execute(conn)
        .await?;
        diesel::sql_query("ANALYZE collections")
            .execute(conn)
            .await?;
        diesel::sql_query("ANALYZE collection_closure")
            .execute(conn)
            .await
    })
    .await
    .expect("representative query-plan rows should be created");
}

async fn remove_representative_collection_rows(pool: &DbPool, name_prefix: &str) {
    with_connection(pool, async |conn| {
        diesel::sql_query("DELETE FROM collections WHERE left(name, length($1)) = $1")
            .bind::<Text, _>(name_prefix)
            .execute(conn)
            .await
    })
    .await
    .expect("representative query-plan rows should be removed");
}

async fn explain_storage_query(pool: &DbPool, query: &'static str, collection_id: i32) -> Value {
    with_connection(pool, async |conn| {
        diesel::sql_query(query)
            .bind::<Integer, _>(collection_id)
            .get_result::<ExplainPlanRow>(conn)
            .await
    })
    .await
    .expect("storage query should produce an EXPLAIN plan")
    .query_plan
}

fn assert_same_query_shape(
    smaller: &crate::db::QueryCaptureSnapshot,
    larger: &crate::db::QueryCaptureSnapshot,
) {
    assert_eq!(
        larger.total_queries(),
        smaller.total_queries(),
        "small: {:#?}\nlarge: {:#?}",
        smaller.query_counts(),
        larger.query_counts()
    );
    assert_eq!(larger.domain_queries(), smaller.domain_queries());
    assert_eq!(larger.control_queries(), smaller.control_queries());
    assert_eq!(
        larger.connection_checkouts(),
        smaller.connection_checkouts()
    );
    assert_eq!(larger.query_counts(), smaller.query_counts());
}

#[actix_web::test]
async fn collection_point_read_uses_one_query() {
    let scope = TestScope::new();
    let fixture = scope.collection_fixture("query_budget_point_read").await;
    let collection_id = CollectionID::new(fixture.collection.id).expect("valid collection id");

    let (loaded, queries) = capture_queries(collection_id.collection(&scope.pool)).await;
    assert_eq!(loaded.expect("collection should load"), fixture.collection);
    assert_eq!(queries.total_queries(), 1, "{:#?}", queries.query_counts());
    assert_eq!(queries.domain_queries(), 1);
    assert_eq!(queries.control_queries(), 0);
    assert_eq!(queries.connection_checkouts(), 1);
    assert_eq!(queries.queries_matching("FROM \"collections\""), 1);

    fixture.cleanup().await.expect("fixture cleanup");
}

#[actix_web::test]
async fn collection_point_read_plan_has_bounded_logical_work_at_representative_scale() {
    let scope = TestScope::new();
    let fixture = scope.collection_fixture("query_plan_point_read").await;
    let scale_prefix = scope.scoped_name("query_plan_point_read_scale");
    add_representative_collection_rows(scope.pool.get_ref(), &scale_prefix, fixture.collection.id)
        .await;

    let plan = explain_storage_query(
        scope.pool.get_ref(),
        "EXPLAIN (ANALYZE, BUFFERS, TIMING OFF, SUMMARY OFF, FORMAT JSON) \
         SELECT id, name, description, created_at, updated_at, parent_collection_id \
         FROM collections WHERE id = $1",
        fixture.collection.id,
    )
    .await;

    assert!(
        plan_uses_index(&plan, "collections_pkey"),
        "point read should use the collections primary-key index: {plan:#}"
    );
    assert_eq!(root_plan(&plan)["Actual Rows"].as_f64(), Some(1.0));
    assert!(
        root_shared_blocks(&plan) <= 8,
        "point read touched too many shared blocks: {plan:#}"
    );

    remove_representative_collection_rows(scope.pool.get_ref(), &scale_prefix).await;
    fixture.cleanup().await.expect("fixture cleanup");
}

#[actix_web::test]
async fn collection_ancestor_query_count_is_constant_with_depth() {
    let scope = TestScope::new();
    let root_fixture = scope.collection_fixture("query_budget_ancestors").await;
    let mut collections = vec![root_fixture.collection.clone()];

    for depth in 1..=32 {
        let parent = collections.last().expect("parent collection");
        let collection = NewCollectionWithAssignee {
            name: scope.scoped_name(&format!("query_budget_ancestor_{depth}")),
            description: format!("query budget ancestor level {depth}"),
            group_id: root_fixture.owner_group.id,
            parent_collection_id: Some(
                CollectionID::new(parent.id).expect("valid parent collection id"),
            ),
        }
        .save_without_events(&scope.pool)
        .await
        .expect("collection should save");
        collections.push(collection);
    }

    let shallow_id = CollectionID::new(collections[1].id).expect("valid shallow collection id");
    let (shallow_ancestors, shallow_queries) =
        capture_queries(collection_ancestors(&scope.pool, shallow_id)).await;
    assert_eq!(shallow_ancestors.expect("shallow ancestors").len(), 2);

    let leaf_id =
        CollectionID::new(collections.last().expect("leaf").id).expect("valid leaf collection id");
    let (ancestors, queries) = capture_queries(collection_ancestors(&scope.pool, leaf_id)).await;
    let ancestors = ancestors.expect("ancestors should load");

    assert_eq!(ancestors.len(), 33);
    assert_eq!(queries.total_queries(), shallow_queries.total_queries());
    assert_eq!(queries.domain_queries(), shallow_queries.domain_queries());
    assert_eq!(queries.total_queries(), 1, "{:#?}", queries.query_counts());
    assert_eq!(queries.domain_queries(), 1);
    assert_eq!(queries.control_queries(), 0);
    assert_eq!(queries.connection_checkouts(), 1);
    assert_eq!(queries.queries_matching("collection_closure"), 1);

    for collection in collections.iter().skip(1).rev() {
        collection
            .delete_without_events(&scope.pool)
            .await
            .expect("nested collection cleanup");
    }
    root_fixture.cleanup().await.expect("root fixture cleanup");
}

#[actix_web::test]
async fn collection_ancestor_plan_has_bounded_logical_work_at_representative_scale() {
    let scope = TestScope::new();
    let fixture = scope.collection_fixture("query_plan_ancestors").await;
    let mut collections = vec![fixture.collection.clone()];

    for depth in 1..=16 {
        let parent_id = collections.last().expect("parent collection").id;
        let collection = NewCollectionWithAssignee {
            name: scope.scoped_name(&format!("query_plan_ancestor_{depth}")),
            description: format!("query plan ancestor level {depth}"),
            group_id: fixture.owner_group.id,
            parent_collection_id: Some(
                CollectionID::new(parent_id).expect("valid parent collection id"),
            ),
        }
        .save_without_events(&scope.pool)
        .await
        .expect("nested collection should save");
        collections.push(collection);
    }

    let scale_prefix = scope.scoped_name("query_plan_ancestor_scale");
    add_representative_collection_rows(scope.pool.get_ref(), &scale_prefix, fixture.collection.id)
        .await;
    let leaf_id = collections.last().expect("leaf collection").id;
    let plan = explain_storage_query(
        scope.pool.get_ref(),
        "EXPLAIN (ANALYZE, BUFFERS, TIMING OFF, SUMMARY OFF, FORMAT JSON) \
         SELECT collections.id, collections.name, collections.description, \
                collections.created_at, collections.updated_at, \
                collections.parent_collection_id \
         FROM collection_closure \
         INNER JOIN collections \
             ON collections.id = collection_closure.ancestor_collection_id \
         WHERE collection_closure.descendant_collection_id = $1 \
           AND collection_closure.depth > 0 \
         ORDER BY collection_closure.depth ASC",
        leaf_id,
    )
    .await;

    assert!(
        plan_uses_index(&plan, "collection_closure_descendant"),
        "ancestor read should use a descendant-first closure index: {plan:#}"
    );
    assert_eq!(root_plan(&plan)["Actual Rows"].as_f64(), Some(17.0));
    assert!(
        root_shared_blocks(&plan) <= 128,
        "ancestor read touched too many shared blocks: {plan:#}"
    );

    remove_representative_collection_rows(scope.pool.get_ref(), &scale_prefix).await;
    for collection in collections.iter().skip(1).rev() {
        collection
            .delete_without_events(&scope.pool)
            .await
            .expect("nested collection cleanup");
    }
    fixture.cleanup().await.expect("fixture cleanup");
}

#[actix_web::test]
async fn collection_create_with_event_has_a_fixed_query_budget() {
    let scope = TestScope::new();
    let parent = scope.collection_fixture("query_budget_create_parent").await;
    let command = NewCollectionWithAssignee {
        name: scope.scoped_name("query_budget_create_child"),
        description: "query budget create child".to_string(),
        group_id: parent.owner_group.id,
        parent_collection_id: Some(
            CollectionID::new(parent.collection.id).expect("valid parent collection id"),
        ),
    };

    let (created, queries) =
        capture_queries(command.save(&scope.pool, &EventContext::system())).await;
    let created = created.expect("collection should save with an event");

    assert_eq!(queries.total_queries(), 7, "{:#?}", queries.query_counts());
    assert_eq!(queries.domain_queries(), 5, "{:#?}", queries.query_counts());
    assert_eq!(queries.control_queries(), 2);
    assert_eq!(queries.connection_checkouts(), 1);
    assert_eq!(queries.queries_matching("SELECT \"collections\".\"id\""), 1);
    assert_eq!(queries.queries_matching("INSERT INTO \"collections\""), 1);
    assert_eq!(
        queries.queries_matching("INSERT INTO collection_closure"),
        1
    );
    assert_eq!(queries.queries_matching("INSERT INTO \"permissions\""), 1);
    assert_eq!(queries.queries_matching("INSERT INTO \"events\""), 1);

    created
        .delete_without_events(&scope.pool)
        .await
        .expect("created collection cleanup");
    parent.cleanup().await.expect("parent fixture cleanup");
}

#[actix_web::test]
async fn collection_no_op_update_does_not_write_or_emit_an_event() {
    let scope = TestScope::new();
    let fixture = scope.collection_fixture("query_budget_no_op_update").await;
    let update = UpdateCollection {
        name: Some(fixture.collection.name.clone()),
        description: Some(fixture.collection.description.clone()),
    };

    let (updated, queries) =
        capture_queries(update.update(&scope.pool, fixture.collection.id, &EventContext::system()))
            .await;
    assert_eq!(
        updated.expect("no-op update should return current row"),
        fixture.collection
    );

    assert_eq!(queries.total_queries(), 3, "{:#?}", queries.query_counts());
    assert_eq!(queries.domain_queries(), 1, "{:#?}", queries.query_counts());
    assert_eq!(queries.control_queries(), 2);
    assert_eq!(queries.connection_checkouts(), 1);
    assert_eq!(queries.queries_matching("UPDATE \"collections\""), 0);
    assert_eq!(queries.queries_matching("INSERT INTO \"events\""), 0);

    fixture.cleanup().await.expect("fixture cleanup");
}

#[actix_web::test]
async fn object_page_query_count_is_constant_with_page_size() {
    let scope = TestScope::new();
    let fixture = scope
        .object_fixture(
            "query_budget_object_page",
            NewHubuumClass {
                collection_id: 0,
                name: scope.scoped_name("query_budget_object_page_class"),
                description: "query budget object page class".to_string(),
                json_schema: None,
                validate_schema: None,
            },
            (0..20)
                .map(|index| NewHubuumObject {
                    collection_id: 0,
                    hubuum_class_id: 0,
                    name: scope.scoped_name(&format!("query_budget_object_{index:02}")),
                    description: "query budget object".to_string(),
                    data: serde_json::json!({"index": index}),
                })
                .collect(),
        )
        .await
        .expect("object fixture should save");
    let subject = UserID::new(1).expect("valid synthetic runtime-admin subject id");

    let run_page = |limit: usize| {
        let query = parse_query_parameter(&format!(
            "classes={}&sort=id&limit={limit}",
            fixture.class.id
        ))
        .expect("valid object page query");
        async {
            let total = subject
                .count_objects_from_backend_with_admin_status(
                    &scope.pool,
                    query.clone(),
                    true,
                    None,
                )
                .await?;
            let rows = subject
                .search_objects_from_backend_with_admin_status(&scope.pool, query, true, None)
                .await?;
            Ok::<_, crate::errors::ApiError>((rows, total))
        }
    };

    let (small_page, small_queries) = capture_queries(run_page(1)).await;
    let (small_rows, small_total) = small_page.expect("small object page should load");
    assert_eq!(small_rows.len(), 1);
    assert_eq!(small_total, 20);

    let (large_page, large_queries) = capture_queries(run_page(20)).await;
    let (large_rows, large_total) = large_page.expect("large object page should load");
    assert_eq!(large_rows.len(), 20);
    assert_eq!(large_total, 20);

    assert_same_query_shape(&small_queries, &large_queries);
    assert_eq!(large_queries.total_queries(), 4);
    assert_eq!(large_queries.domain_queries(), 4);
    assert_eq!(large_queries.control_queries(), 0);
    assert_eq!(large_queries.connection_checkouts(), 4);

    fixture.cleanup().await.expect("object fixture cleanup");
}

#[actix_web::test]
async fn effective_permission_query_count_is_constant_with_collection_depth() {
    let scope = TestScope::new();
    let root = scope
        .collection_fixture("query_budget_effective_permission")
        .await;
    let mut collections = vec![root.collection.clone()];

    for depth in 1..=16 {
        let parent = collections.last().expect("parent collection");
        let collection = NewCollectionWithAssignee {
            name: scope.scoped_name(&format!("query_budget_permission_depth_{depth}")),
            description: format!("query budget permission depth {depth}"),
            group_id: root.owner_group.id,
            parent_collection_id: Some(
                CollectionID::new(parent.id).expect("valid parent collection id"),
            ),
        }
        .save_without_events(&scope.pool)
        .await
        .expect("collection should save");
        collections.push(collection);
    }

    let (shallow_permissions, shallow_queries) = capture_queries(effective_group_on(
        &scope.pool,
        collections[1].id,
        root.owner_group.id,
    ))
    .await;
    assert!(
        !shallow_permissions
            .expect("shallow permissions should load")
            .is_empty()
    );

    let (deep_permissions, deep_queries) = capture_queries(effective_group_on(
        &scope.pool,
        collections.last().expect("deep collection").id,
        root.owner_group.id,
    ))
    .await;
    assert!(
        !deep_permissions
            .expect("deep permissions should load")
            .is_empty()
    );

    assert_same_query_shape(&shallow_queries, &deep_queries);
    assert_eq!(deep_queries.total_queries(), 3);
    assert_eq!(deep_queries.domain_queries(), 3);
    assert_eq!(deep_queries.control_queries(), 0);
    assert_eq!(deep_queries.connection_checkouts(), 1);

    for collection in collections.iter().skip(1).rev() {
        collection
            .delete_without_events(&scope.pool)
            .await
            .expect("nested collection cleanup");
    }
    root.cleanup().await.expect("root fixture cleanup");
}

#[actix_web::test]
async fn changed_collection_update_writes_once_and_emits_one_event() {
    let scope = TestScope::new();
    let fixture = scope
        .collection_fixture("query_budget_changed_update")
        .await;
    let update = UpdateCollection {
        name: None,
        description: Some("changed query budget description".to_string()),
    };

    let (updated, queries) =
        capture_queries(update.update(&scope.pool, fixture.collection.id, &EventContext::system()))
            .await;
    assert_eq!(
        updated.expect("changed update should succeed").description,
        "changed query budget description"
    );

    assert_eq!(queries.total_queries(), 5, "{:#?}", queries.query_counts());
    assert_eq!(queries.domain_queries(), 3, "{:#?}", queries.query_counts());
    assert_eq!(queries.control_queries(), 2);
    assert_eq!(queries.connection_checkouts(), 1);
    assert_eq!(queries.queries_matching("UPDATE \"collections\""), 1);
    assert_eq!(queries.queries_matching("INSERT INTO \"events\""), 1);

    fixture.cleanup().await.expect("fixture cleanup");
}

#[actix_web::test]
async fn object_relation_create_has_a_fixed_query_and_checkout_budget() {
    let scope = TestScope::new();
    let fixture = scope
        .collection_fixture("query_budget_object_relation")
        .await;
    let class_one = NewHubuumClass {
        collection_id: fixture.collection.id,
        name: scope.scoped_name("query_budget_relation_class_one"),
        description: "query budget relation class one".to_string(),
        json_schema: None,
        validate_schema: None,
    }
    .save_without_events(&scope.pool)
    .await
    .expect("first class should save");
    let class_two = NewHubuumClass {
        collection_id: fixture.collection.id,
        name: scope.scoped_name("query_budget_relation_class_two"),
        description: "query budget relation class two".to_string(),
        json_schema: None,
        validate_schema: None,
    }
    .save_without_events(&scope.pool)
    .await
    .expect("second class should save");
    let class_relation = NewHubuumClassRelation {
        from_hubuum_class_id: class_one.id,
        to_hubuum_class_id: class_two.id,
        forward_template_alias: Some("seconds".to_string()),
        reverse_template_alias: Some("firsts".to_string()),
    }
    .save_without_events(&scope.pool)
    .await
    .expect("class relation should save");
    let object_one = NewHubuumObject {
        collection_id: fixture.collection.id,
        hubuum_class_id: class_one.id,
        name: scope.scoped_name("query_budget_relation_object_one"),
        description: "query budget relation object one".to_string(),
        data: serde_json::json!({}),
    }
    .save_without_events(&scope.pool)
    .await
    .expect("first object should save");
    let object_two = NewHubuumObject {
        collection_id: fixture.collection.id,
        hubuum_class_id: class_two.id,
        name: scope.scoped_name("query_budget_relation_object_two"),
        description: "query budget relation object two".to_string(),
        data: serde_json::json!({}),
    }
    .save_without_events(&scope.pool)
    .await
    .expect("second object should save");

    let relation = NewHubuumObjectRelation {
        from_hubuum_object_id: object_one.id,
        to_hubuum_object_id: object_two.id,
        class_relation_id: class_relation.id,
    };
    let (saved, queries) =
        capture_queries(relation.save(&scope.pool, &EventContext::system())).await;
    saved.expect("object relation should save with an event");

    assert_eq!(queries.total_queries(), 6, "{:#?}", queries.query_counts());
    assert_eq!(queries.domain_queries(), 4, "{:#?}", queries.query_counts());
    assert_eq!(queries.control_queries(), 2);
    assert_eq!(queries.connection_checkouts(), 3);
    assert_eq!(queries.queries_matching("FROM \"hubuumobject\""), 2);
    assert_eq!(
        queries.queries_matching("INSERT INTO \"hubuumobject_relation\""),
        1
    );
    assert_eq!(queries.queries_matching("INSERT INTO \"events\""), 1);

    fixture.cleanup().await.expect("fixture cleanup");
}

#[actix_web::test]
async fn collection_history_query_count_is_constant_with_page_size() {
    let scope = TestScope::new();
    let fixture = scope.collection_fixture("query_budget_history_page").await;
    let actor = ensure_admin_user(&scope.pool).await;

    for version in 0..12 {
        with_actor_scope(
            Some(actor.id),
            UpdateCollection {
                name: None,
                description: Some(format!("query budget history version {version}")),
            }
            .update_without_events(&scope.pool, fixture.collection.id),
        )
        .await
        .expect("history-generating update should succeed");
    }

    let history_pool = scope.pool.clone();
    let history_collection_id = fixture.collection.id;
    let load_history = |limit: usize| {
        let pool = history_pool.clone();
        let query = parse_query_parameter(&format!("limit={limit}&sort=history_id.desc"))
            .expect("valid history query");
        async move {
            let (rows, total) = collection_history_paginated_with_total_count(
                history_collection_id,
                &pool,
                &query,
                HistoryCollectionFilter::All,
            )
            .await?;
            let actor_ids = rows.iter().filter_map(|row| row.actor_id).collect();
            let actors = resolve_actor_usernames(&pool, actor_ids).await?;
            Ok::<_, crate::errors::ApiError>((rows, total, actors))
        }
    };

    let (small_page, small_queries) = capture_queries(load_history(1)).await;
    let (small_rows, small_total, small_actors) =
        small_page.expect("small history page should load");
    assert_eq!(small_rows.len(), 1);
    assert!(small_total >= 12);
    assert!(small_actors.contains_key(&actor.id));

    let (large_page, large_queries) = capture_queries(load_history(20)).await;
    let (large_rows, large_total, large_actors) =
        large_page.expect("large history page should load");
    assert!(large_rows.len() >= 12);
    assert_eq!(large_total, small_total);
    assert!(large_actors.contains_key(&actor.id));

    assert_same_query_shape(&small_queries, &large_queries);
    assert_eq!(large_queries.total_queries(), 3);
    assert_eq!(large_queries.domain_queries(), 3);
    assert_eq!(large_queries.control_queries(), 0);
    assert_eq!(large_queries.connection_checkouts(), 3);

    fixture.cleanup().await.expect("fixture cleanup");
}
