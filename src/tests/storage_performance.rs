//! PostgreSQL query budgets for the storage boundary.
//!
//! These are deliberately model/storage-level checks rather than HTTP tests:
//! authentication and routing should not hide an extra pool checkout or an
//! N+1 introduced while storage capabilities are extracted.

use diesel::sql_types::{Integer, Json, Text};
use hubuum_domain::PrincipalId;
use serde_json::Value;

use crate::events::{EventContext, MutationProvenance};
use crate::models::collection::effective_group_on;
use crate::models::search::parse_query_parameter;
use crate::models::{
    ClassSelector, CollectionID, GroupID, HubuumClassID, HubuumClassRelationID, HubuumObjectID,
    HubuumObjectRelationID, NewCollectionWithAssignee, NewHubuumClass, NewHubuumClassRelation,
    NewHubuumObject, NewHubuumObjectRelation, ObjectAggregateAuthorization,
    ObjectAggregateCursorBudget, ObjectAggregateRequest, ObjectAggregateTarget,
    ObjectRelationCreateSelector, ObjectRelationSelector, ObjectSelector, Permissions,
    UpdateCollection, UpdateHubuumClass, UpdateHubuumObject, UserID, parse_object_aggregate_query,
};
use crate::services::Services;
use crate::services::history::{
    HistoryCollectionFilter, collection_history_paginated_with_total_count, resolve_principal_names,
};
use crate::services::storage_boundary::{object_create_to_storage, resolved_class_to_storage};
use crate::storage::postgres::prelude::{QueryableByName, RunQueryDsl};
use crate::storage::postgres::{PostgresPool, capture_queries, with_connection};
use crate::storage::{StorageHandle, TransactionalStorage, with_mutation_provenance};
use crate::tests::{CollectionFixture, TestScope, ensure_admin_user};
use crate::traits::{CanDelete, CanSave, CanUpdate};

const REPRESENTATIVE_COLLECTION_ROWS: i32 = 2_000;

async fn object_relation_budget_fixture(
    scope: &TestScope,
    label: &str,
) -> (CollectionFixture, Services, ObjectRelationCreateSelector) {
    let fixture = scope.collection_fixture(label).await;
    let services = crate::tests::services_for_postgres(scope.pool.get_ref().clone());
    let class_one = NewHubuumClass {
        collection_id: fixture.collection.id,
        name: scope.scoped_name(&format!("{label}_class_one")),
        description: "object relation budget class one".to_string(),
        json_schema: None,
        validate_schema: None,
    }
    .save_without_events(&scope.pool)
    .await
    .expect("first class should save");
    let class_two = NewHubuumClass {
        collection_id: fixture.collection.id,
        name: scope.scoped_name(&format!("{label}_class_two")),
        description: "object relation budget class two".to_string(),
        json_schema: None,
        validate_schema: None,
    }
    .save_without_events(&scope.pool)
    .await
    .expect("second class should save");
    let class_relation = NewHubuumClassRelation {
        from_hubuum_class_id: class_one.id,
        to_hubuum_class_id: class_two.id,
        forward_template_alias: None,
        reverse_template_alias: None,
        from_max_relations: None,
        to_max_relations: None,
    }
    .save_without_events(&scope.pool)
    .await
    .expect("class relation should save");
    let object_one = NewHubuumObject {
        collection_id: fixture.collection.id,
        hubuum_class_id: class_one.id,
        name: scope.scoped_name(&format!("{label}_object_one")),
        description: "object relation budget object one".to_string(),
        data: serde_json::json!({}),
    }
    .save_without_events(&scope.pool)
    .await
    .expect("first object should save");
    let object_two = NewHubuumObject {
        collection_id: fixture.collection.id,
        hubuum_class_id: class_two.id,
        name: scope.scoped_name(&format!("{label}_object_two")),
        description: "object relation budget object two".to_string(),
        data: serde_json::json!({}),
    }
    .save_without_events(&scope.pool)
    .await
    .expect("second object should save");
    let selector = ObjectRelationCreateSelector::explicit(NewHubuumObjectRelation {
        from_hubuum_object_id: object_one.id,
        to_hubuum_object_id: object_two.id,
        class_relation_id: class_relation.id,
    });
    (fixture, services, selector)
}

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

async fn add_representative_collection_rows(
    pool: &PostgresPool,
    name_prefix: &str,
    parent_id: i32,
) {
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

async fn remove_representative_collection_rows(pool: &PostgresPool, name_prefix: &str) {
    with_connection(pool, async |conn| {
        diesel::sql_query("DELETE FROM collections WHERE left(name, length($1)) = $1")
            .bind::<Text, _>(name_prefix)
            .execute(conn)
            .await
    })
    .await
    .expect("representative query-plan rows should be removed");
}

async fn explain_storage_query(
    pool: &PostgresPool,
    query: &'static str,
    collection_id: i32,
) -> Value {
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
    smaller: &crate::storage::postgres::QueryCaptureSnapshot,
    larger: &crate::storage::postgres::QueryCaptureSnapshot,
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
async fn token_metadata_batch_query_count_is_constant_with_batch_size() {
    use crate::models::{
        Permissions, PrincipalID, PrincipalToken, PrincipalTokenCreateRequest,
        PrincipalTokenMetadata, TokenScope,
    };
    use crate::storage::postgres::operations::token::PrincipalTokenRow;
    use crate::storage::postgres::prelude::*;

    let scope = TestScope::new();
    let user = crate::tests::create_test_user(&scope.pool).await;
    let token_scope =
        TokenScope::from_request_parts(Some(vec![Permissions::ReadCollection]), None).unwrap();
    let raw = PrincipalTokenCreateRequest::new(PrincipalID::new(user.id).unwrap())
        .scope(token_scope)
        .create(&scope.pool, &crate::events::EventContext::system())
        .await
        .expect("scoped token should be created");
    let token_hash = raw.storage_hash();
    let token = with_connection(&scope.pool, async |connection| {
        crate::schema::tokens::table
            .filter(crate::schema::tokens::token.eq(token_hash))
            .first::<PrincipalTokenRow>(connection)
            .await
            .map(PrincipalToken::from)
    })
    .await
    .expect("created token should be persisted");

    let (small, small_queries) = capture_queries(PrincipalTokenMetadata::load_for_tokens(
        &scope.pool,
        std::slice::from_ref(&token),
    ))
    .await;
    let repeated = vec![token; 20];
    let (large, large_queries) = capture_queries(PrincipalTokenMetadata::load_for_tokens(
        &scope.pool,
        &repeated,
    ))
    .await;

    assert_eq!(small.expect("single-token batch should load").len(), 1);
    assert_eq!(large.expect("repeated-token batch should load").len(), 20);
    assert_same_query_shape(&small_queries, &large_queries);
    assert_eq!(large_queries.total_queries(), 5, "{large_queries:#?}");
    assert_eq!(large_queries.domain_queries(), 2);
    assert_eq!(large_queries.control_queries(), 3);
    assert_eq!(large_queries.connection_checkouts(), 1);
    assert_eq!(large_queries.queries_matching("FROM \"token_scopes\""), 1);

    user.delete_without_events(&scope.pool).await.unwrap();
}

#[actix_web::test]
async fn collection_point_read_uses_one_query() {
    let scope = TestScope::new();
    let fixture = scope.collection_fixture("query_budget_point_read").await;
    let collection_id = CollectionID::new(fixture.collection.id).expect("valid collection id");
    let services = crate::tests::services_for_postgres(scope.pool.get_ref().clone());

    let (loaded, queries) = capture_queries(services.collections().get(collection_id)).await;
    assert_eq!(loaded.expect("collection should load"), fixture.collection);
    assert_eq!(queries.total_queries(), 1, "{:#?}", queries.query_counts());
    assert_eq!(queries.domain_queries(), 1);
    assert_eq!(queries.control_queries(), 0);
    assert_eq!(queries.connection_checkouts(), 1);
    assert_eq!(queries.queries_matching("FROM \"collections\""), 1);

    fixture.cleanup().await.expect("fixture cleanup");
}

#[actix_web::test]
async fn class_storage_query_budget_point_read_uses_one_query() {
    let scope = TestScope::new();
    let fixture = scope
        .collection_fixture("query_budget_class_point_read")
        .await;
    let services = crate::tests::services_for_postgres(scope.pool.get_ref().clone());
    let class = NewHubuumClass {
        name: scope.scoped_name("query_budget_class_point_read"),
        collection_id: fixture.collection.id,
        json_schema: None,
        validate_schema: None,
        description: "class point read query budget".to_string(),
    }
    .save_without_events(&scope.pool)
    .await
    .expect("class fixture should save");

    let (loaded, queries) = capture_queries(services.classes().resolve(ClassSelector::by_id(
        HubuumClassID::new(class.id).expect("valid class id"),
    )))
    .await;
    assert_eq!(loaded.expect("class should load").class(), &class);
    assert_eq!(queries.total_queries(), 1, "{:#?}", queries.query_counts());
    assert_eq!(queries.domain_queries(), 1);
    assert_eq!(queries.control_queries(), 0);
    assert_eq!(queries.connection_checkouts(), 1);
    assert_eq!(queries.queries_matching("FROM \"hubuumclass\""), 1);

    class
        .delete_without_events(&scope.pool)
        .await
        .expect("class fixture cleanup");
    fixture.cleanup().await.expect("collection fixture cleanup");
}

#[actix_web::test]
async fn class_storage_query_budget_create_with_event_is_fixed() {
    let scope = TestScope::new();
    let fixture = scope.collection_fixture("query_budget_class_create").await;
    let services = crate::tests::services_for_postgres(scope.pool.get_ref().clone());
    let command = NewHubuumClass {
        name: scope.scoped_name("query_budget_class_create"),
        collection_id: fixture.collection.id,
        json_schema: None,
        validate_schema: None,
        description: "class create query budget".to_string(),
    };

    let (created, queries) =
        capture_queries(services.classes().create(command, &EventContext::system())).await;
    let created = created.expect("class should save with an event");
    assert_eq!(queries.total_queries(), 4, "{:#?}", queries.query_counts());
    assert_eq!(queries.domain_queries(), 2);
    assert_eq!(queries.control_queries(), 2);
    assert_eq!(queries.connection_checkouts(), 1);
    assert_eq!(queries.queries_matching("INSERT INTO \"hubuumclass\""), 1);
    assert_eq!(queries.queries_matching("INSERT INTO \"events\""), 1);

    created
        .delete_without_events(&scope.pool)
        .await
        .expect("class cleanup");
    fixture.cleanup().await.expect("collection fixture cleanup");
}

#[actix_web::test]
async fn class_storage_query_budget_no_op_avoids_writes_and_events() {
    let scope = TestScope::new();
    let fixture = scope.collection_fixture("query_budget_class_no_op").await;
    let services = crate::tests::services_for_postgres(scope.pool.get_ref().clone());
    let class = NewHubuumClass {
        name: scope.scoped_name("query_budget_class_no_op"),
        collection_id: fixture.collection.id,
        json_schema: None,
        validate_schema: None,
        description: "class no-op query budget".to_string(),
    }
    .save_without_events(&scope.pool)
    .await
    .expect("class fixture should save");
    let target = services
        .classes()
        .resolve(ClassSelector::by_id(
            HubuumClassID::new(class.id).expect("valid class id"),
        ))
        .await
        .expect("class fixture should resolve");
    let update = UpdateHubuumClass {
        name: Some(class.name.clone()),
        collection_id: Some(class.collection_id),
        json_schema: None,
        validate_schema: Some(class.validate_schema),
        description: Some(class.description.clone()),
    };

    let (updated, queries) = capture_queries(services.classes().update(
        &target,
        update,
        &EventContext::system(),
    ))
    .await;
    assert_eq!(updated.expect("no-op update should succeed"), class);
    assert_eq!(queries.total_queries(), 4, "{:#?}", queries.query_counts());
    assert_eq!(queries.domain_queries(), 2);
    assert_eq!(queries.control_queries(), 2);
    assert_eq!(queries.connection_checkouts(), 1);
    assert_eq!(
        queries.queries_matching("SELECT hubuum_assert_revision_precondition"),
        1
    );
    assert_eq!(queries.queries_matching("UPDATE \"hubuumclass\""), 0);
    assert_eq!(queries.queries_matching("INSERT INTO \"events\""), 0);

    class
        .delete_without_events(&scope.pool)
        .await
        .expect("class cleanup");
    fixture.cleanup().await.expect("collection fixture cleanup");
}

#[actix_web::test]
async fn class_relation_storage_query_budget_point_resolution_is_fixed() {
    let scope = TestScope::new();
    let fixture = scope
        .collection_fixture("query_budget_class_relation_point")
        .await;
    let services = crate::tests::services_for_postgres(scope.pool.get_ref().clone());
    let from_class = NewHubuumClass {
        name: scope.scoped_name("query_budget_class_relation_point_from"),
        collection_id: fixture.collection.id,
        json_schema: None,
        validate_schema: None,
        description: "class relation point budget from class".to_string(),
    }
    .save_without_events(&scope.pool)
    .await
    .expect("from class fixture should save");
    let to_class = NewHubuumClass {
        name: scope.scoped_name("query_budget_class_relation_point_to"),
        collection_id: fixture.collection.id,
        json_schema: None,
        validate_schema: None,
        description: "class relation point budget to class".to_string(),
    }
    .save_without_events(&scope.pool)
    .await
    .expect("to class fixture should save");
    let relation = NewHubuumClassRelation {
        from_hubuum_class_id: from_class.id,
        to_hubuum_class_id: to_class.id,
        forward_template_alias: None,
        reverse_template_alias: None,
        from_max_relations: None,
        to_max_relations: None,
    }
    .save_without_events(&scope.pool)
    .await
    .expect("relation fixture should save");

    let (loaded, queries) = capture_queries(
        services
            .class_relations()
            .resolve(HubuumClassRelationID::new(relation.id).expect("valid class relation id")),
    )
    .await;
    assert_eq!(
        loaded.expect("relation should resolve").relation(),
        &relation
    );
    assert_eq!(queries.total_queries(), 2, "{:#?}", queries.query_counts());
    assert_eq!(queries.domain_queries(), 2);
    assert_eq!(queries.control_queries(), 0);
    assert_eq!(queries.connection_checkouts(), 1);
    assert_eq!(queries.queries_matching("FROM \"hubuumclass_relation\""), 1);
    assert_eq!(queries.queries_matching("FROM \"hubuumclass\""), 1);

    fixture.cleanup().await.expect("collection fixture cleanup");
}

#[actix_web::test]
async fn class_relation_storage_query_budget_create_with_event_is_fixed() {
    let scope = TestScope::new();
    let fixture = scope
        .collection_fixture("query_budget_class_relation_create")
        .await;
    let services = crate::tests::services_for_postgres(scope.pool.get_ref().clone());
    let from_class = NewHubuumClass {
        name: scope.scoped_name("query_budget_class_relation_create_from"),
        collection_id: fixture.collection.id,
        json_schema: None,
        validate_schema: None,
        description: "class relation create budget from class".to_string(),
    }
    .save_without_events(&scope.pool)
    .await
    .expect("from class fixture should save");
    let to_class = NewHubuumClass {
        name: scope.scoped_name("query_budget_class_relation_create_to"),
        collection_id: fixture.collection.id,
        json_schema: None,
        validate_schema: None,
        description: "class relation create budget to class".to_string(),
    }
    .save_without_events(&scope.pool)
    .await
    .expect("to class fixture should save");
    let prepared = services
        .class_relations()
        .prepare_create(NewHubuumClassRelation {
            from_hubuum_class_id: from_class.id,
            to_hubuum_class_id: to_class.id,
            forward_template_alias: None,
            reverse_template_alias: None,
            from_max_relations: None,
            to_max_relations: None,
        })
        .await
        .expect("relation should prepare");

    let (created, queries) = capture_queries(
        services
            .class_relations()
            .create(&prepared, &EventContext::system()),
    )
    .await;
    created.expect("relation should create with an event");
    assert_eq!(queries.total_queries(), 6, "{:#?}", queries.query_counts());
    assert_eq!(queries.domain_queries(), 4);
    assert_eq!(queries.control_queries(), 2);
    assert_eq!(queries.connection_checkouts(), 1);
    assert_eq!(
        queries.queries_matching("INSERT INTO \"hubuumclass_relation\""),
        1
    );
    assert_eq!(queries.queries_matching("INSERT INTO \"events\""), 1);

    fixture.cleanup().await.expect("collection fixture cleanup");
}

#[actix_web::test]
async fn class_relation_storage_query_budget_delete_with_event_is_fixed() {
    let scope = TestScope::new();
    let fixture = scope
        .collection_fixture("query_budget_class_relation_delete")
        .await;
    let services = crate::tests::services_for_postgres(scope.pool.get_ref().clone());
    let from_class = NewHubuumClass {
        name: scope.scoped_name("query_budget_class_relation_delete_from"),
        collection_id: fixture.collection.id,
        json_schema: None,
        validate_schema: None,
        description: "class relation delete budget from class".to_string(),
    }
    .save_without_events(&scope.pool)
    .await
    .expect("from class fixture should save");
    let to_class = NewHubuumClass {
        name: scope.scoped_name("query_budget_class_relation_delete_to"),
        collection_id: fixture.collection.id,
        json_schema: None,
        validate_schema: None,
        description: "class relation delete budget to class".to_string(),
    }
    .save_without_events(&scope.pool)
    .await
    .expect("to class fixture should save");
    let prepared = services
        .class_relations()
        .prepare_create(NewHubuumClassRelation {
            from_hubuum_class_id: from_class.id,
            to_hubuum_class_id: to_class.id,
            forward_template_alias: None,
            reverse_template_alias: None,
            from_max_relations: None,
            to_max_relations: None,
        })
        .await
        .expect("relation should prepare");
    let target = services
        .class_relations()
        .create(&prepared, &EventContext::system())
        .await
        .expect("relation should create");

    let (deleted, queries) = capture_queries(
        services
            .class_relations()
            .delete(&target, &EventContext::system()),
    )
    .await;
    deleted.expect("relation should delete with an event");
    assert_eq!(queries.total_queries(), 7, "{:#?}", queries.query_counts());
    assert_eq!(queries.domain_queries(), 5);
    assert_eq!(queries.control_queries(), 2);
    assert_eq!(queries.connection_checkouts(), 1);
    assert_eq!(
        queries.queries_matching("DELETE FROM \"hubuumclass_relation\""),
        1
    );
    assert_eq!(queries.queries_matching("INSERT INTO \"events\""), 1);

    fixture.cleanup().await.expect("collection fixture cleanup");
}

#[actix_web::test]
async fn object_storage_query_budget_point_read_uses_one_query() {
    let scope = TestScope::new();
    let fixture = scope
        .collection_fixture("query_budget_object_point_read")
        .await;
    let services = crate::tests::services_for_postgres(scope.pool.get_ref().clone());
    let class = NewHubuumClass {
        name: scope.scoped_name("query_budget_object_point_read_class"),
        collection_id: fixture.collection.id,
        json_schema: None,
        validate_schema: None,
        description: "object point read query budget class".to_string(),
    }
    .save_without_events(&scope.pool)
    .await
    .expect("class fixture should save");
    let object = NewHubuumObject {
        name: scope.scoped_name("query_budget_object_point_read"),
        collection_id: fixture.collection.id,
        hubuum_class_id: class.id,
        data: serde_json::json!({"value": 1}),
        description: "object point read query budget".to_string(),
    }
    .save_without_events(&scope.pool)
    .await
    .expect("object fixture should save");

    let (loaded, queries) = capture_queries(services.objects().resolve(ObjectSelector::by_id(
        HubuumClassID::new(class.id).expect("valid class id"),
        HubuumObjectID::new(object.id).expect("valid object id"),
    )))
    .await;
    assert_eq!(loaded.expect("object should load").object(), &object);
    assert_eq!(queries.total_queries(), 1, "{:#?}", queries.query_counts());
    assert_eq!(queries.domain_queries(), 1);
    assert_eq!(queries.control_queries(), 0);
    assert_eq!(queries.connection_checkouts(), 1);
    assert_eq!(queries.queries_matching("INNER JOIN \"hubuumclass\""), 1);

    fixture.cleanup().await.expect("collection fixture cleanup");
}

#[actix_web::test]
async fn object_storage_query_budget_create_with_event_is_fixed() {
    let scope = TestScope::new();
    let fixture = scope.collection_fixture("query_budget_object_create").await;
    let services = crate::tests::services_for_postgres(scope.pool.get_ref().clone());
    let class = NewHubuumClass {
        name: scope.scoped_name("query_budget_object_create_class"),
        collection_id: fixture.collection.id,
        json_schema: None,
        validate_schema: None,
        description: "object create query budget class".to_string(),
    }
    .save_without_events(&scope.pool)
    .await
    .expect("class fixture should save");
    let class_target = services
        .classes()
        .resolve(ClassSelector::by_id(
            HubuumClassID::new(class.id).expect("valid class id"),
        ))
        .await
        .expect("class fixture should resolve");
    let command = NewHubuumObject {
        name: scope.scoped_name("query_budget_object_create"),
        collection_id: fixture.collection.id,
        hubuum_class_id: class.id,
        data: serde_json::json!({"value": 1}),
        description: "object create query budget".to_string(),
    };

    let (created, queries) = capture_queries(services.objects().create(
        &class_target,
        command,
        &EventContext::system(),
    ))
    .await;
    created.expect("object should save with an event");
    assert_eq!(queries.total_queries(), 10, "{:#?}", queries.query_counts());
    assert_eq!(queries.domain_queries(), 8);
    assert_eq!(queries.control_queries(), 2);
    assert_eq!(queries.connection_checkouts(), 1);
    assert_eq!(queries.queries_matching("INSERT INTO \"hubuumobject\""), 1);
    assert_eq!(queries.queries_matching("INSERT INTO \"events\""), 1);

    fixture.cleanup().await.expect("collection fixture cleanup");
}

#[actix_web::test]
async fn object_transaction_reuses_the_direct_create_round_trip_budget() {
    let scope = TestScope::new();
    let fixture = scope
        .collection_fixture("query_budget_object_transaction_create")
        .await;
    let services = crate::tests::services_for_postgres(scope.pool.get_ref().clone());
    let storage = StorageHandle::postgres(scope.pool.get_ref().clone());
    let class = NewHubuumClass {
        name: scope.scoped_name("query_budget_object_transaction_create_class"),
        collection_id: fixture.collection.id,
        json_schema: None,
        validate_schema: None,
        description: "object transaction query budget class".to_string(),
    }
    .save_without_events(&scope.pool)
    .await
    .expect("class fixture should save");
    let class_target = services
        .classes()
        .resolve(ClassSelector::by_id(
            HubuumClassID::new(class.id).expect("valid class id"),
        ))
        .await
        .expect("class fixture should resolve");
    let storage_class = resolved_class_to_storage(&class_target);
    let command = object_create_to_storage(NewHubuumObject {
        name: scope.scoped_name("query_budget_object_transaction_create"),
        collection_id: fixture.collection.id,
        hubuum_class_id: class.id,
        data: serde_json::json!({"value": 1}),
        description: "object transaction query budget".to_string(),
    });

    let (created, queries) = capture_queries(storage.transaction(
        EventContext::system(),
        move |transaction| {
            Box::pin(async move { transaction.objects().create(&storage_class, command).await })
        },
    ))
    .await;
    let _ = created
        .expect("transactional object should save with an event")
        .into_value();
    assert_eq!(queries.total_queries(), 10, "{:#?}", queries.query_counts());
    assert_eq!(queries.domain_queries(), 8);
    assert_eq!(queries.control_queries(), 2);
    assert_eq!(queries.connection_checkouts(), 1);
    assert_eq!(queries.queries_matching("INSERT INTO \"hubuumobject\""), 1);
    assert_eq!(queries.queries_matching("INSERT INTO \"events\""), 1);

    fixture.cleanup().await.expect("collection fixture cleanup");
}

#[actix_web::test]
async fn object_storage_query_budget_no_op_avoids_object_writes_and_events() {
    let scope = TestScope::new();
    let fixture = scope.collection_fixture("query_budget_object_no_op").await;
    let services = crate::tests::services_for_postgres(scope.pool.get_ref().clone());
    let class = NewHubuumClass {
        name: scope.scoped_name("query_budget_object_no_op_class"),
        collection_id: fixture.collection.id,
        json_schema: None,
        validate_schema: None,
        description: "object no-op query budget class".to_string(),
    }
    .save_without_events(&scope.pool)
    .await
    .expect("class fixture should save");
    let object = NewHubuumObject {
        name: scope.scoped_name("query_budget_object_no_op"),
        collection_id: fixture.collection.id,
        hubuum_class_id: class.id,
        data: serde_json::json!({"value": 1}),
        description: "object no-op query budget".to_string(),
    }
    .save_without_events(&scope.pool)
    .await
    .expect("object fixture should save");
    let target = services
        .objects()
        .resolve(ObjectSelector::by_id(
            HubuumClassID::new(class.id).expect("valid class id"),
            HubuumObjectID::new(object.id).expect("valid object id"),
        ))
        .await
        .expect("object fixture should resolve");
    let update = UpdateHubuumObject {
        name: Some(object.name.clone()),
        collection_id: Some(object.collection_id),
        hubuum_class_id: Some(object.hubuum_class_id),
        data: Some(object.data.clone()),
        description: Some(object.description.clone()),
    };

    let (updated, queries) = capture_queries(services.objects().update(
        &target,
        update,
        &EventContext::system(),
    ))
    .await;
    assert_eq!(updated.expect("no-op update should succeed"), object);
    assert_eq!(queries.total_queries(), 9, "{:#?}", queries.query_counts());
    assert_eq!(queries.domain_queries(), 7);
    assert_eq!(queries.control_queries(), 2);
    assert_eq!(queries.connection_checkouts(), 1);
    assert_eq!(
        queries.queries_matching("SELECT hubuum_assert_revision_precondition"),
        1
    );
    assert_eq!(queries.queries_matching("UPDATE \"hubuumobject\""), 0);
    assert_eq!(queries.queries_matching("INSERT INTO \"events\""), 0);

    fixture.cleanup().await.expect("collection fixture cleanup");
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
    let services = crate::tests::services_for_postgres(scope.pool.get_ref().clone());
    let root_fixture = scope.collection_fixture("query_budget_ancestors").await;
    let mut collections = vec![root_fixture.collection.clone()];

    for depth in 1..=32 {
        let parent = collections.last().expect("parent collection");
        let collection = NewCollectionWithAssignee {
            name: scope.scoped_name(&format!("query_budget_ancestor_{depth}")),
            description: format!("query budget ancestor level {depth}"),
            group_id: GroupID::new(root_fixture.owner_group.id).unwrap(),
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
        capture_queries(services.collections().ancestors(shallow_id)).await;
    assert_eq!(shallow_ancestors.expect("shallow ancestors").len(), 2);

    let leaf_id =
        CollectionID::new(collections.last().expect("leaf").id).expect("valid leaf collection id");
    let (ancestors, queries) = capture_queries(services.collections().ancestors(leaf_id)).await;
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
            group_id: GroupID::new(fixture.owner_group.id).unwrap(),
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
    let services = crate::tests::services_for_postgres(scope.pool.get_ref().clone());
    let parent = scope.collection_fixture("query_budget_create_parent").await;
    let command = NewCollectionWithAssignee {
        name: scope.scoped_name("query_budget_create_child"),
        description: "query budget create child".to_string(),
        group_id: GroupID::new(parent.owner_group.id).unwrap(),
        parent_collection_id: Some(
            CollectionID::new(parent.collection.id).expect("valid parent collection id"),
        ),
    };

    let (created, queries) = capture_queries(
        services
            .collections()
            .create(command, &EventContext::system()),
    )
    .await;
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
    let services = crate::tests::services_for_postgres(scope.pool.get_ref().clone());
    let fixture = scope.collection_fixture("query_budget_no_op_update").await;
    let update = UpdateCollection {
        name: Some(fixture.collection.name.clone()),
        description: Some(fixture.collection.description.clone()),
    };

    let (updated, queries) = capture_queries(services.collections().update(
        CollectionID::new(fixture.collection.id).unwrap(),
        update,
        &EventContext::system(),
    ))
    .await;
    assert_eq!(
        updated.expect("no-op update should return current row"),
        fixture.collection
    );

    assert_eq!(queries.total_queries(), 4, "{:#?}", queries.query_counts());
    assert_eq!(queries.domain_queries(), 2, "{:#?}", queries.query_counts());
    assert_eq!(queries.control_queries(), 2);
    assert_eq!(queries.connection_checkouts(), 1);
    assert_eq!(
        queries.queries_matching("SELECT hubuum_assert_revision_precondition"),
        1
    );
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
            let (rows, total) = crate::services::catalog::list_objects(
                &scope.pool,
                subject.id(),
                true,
                None,
                query,
            )
            .await?;
            Ok::<_, crate::errors::ApiError>((
                rows,
                total.expect("catalog query requested an exact total"),
            ))
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
    assert_eq!(
        large_queries.total_queries(),
        6,
        "{:#?}",
        large_queries.query_counts()
    );
    assert_eq!(large_queries.domain_queries(), 3);
    assert_eq!(large_queries.control_queries(), 3);
    assert_eq!(large_queries.connection_checkouts(), 1);

    fixture.cleanup().await.expect("object fixture cleanup");
}

#[actix_web::test]
async fn object_aggregate_query_count_is_constant_with_page_size() {
    let scope = TestScope::new();
    let fixture = scope
        .object_fixture(
            "query_budget_object_aggregate",
            NewHubuumClass {
                collection_id: 0,
                name: scope.scoped_name("query_budget_object_aggregate_class"),
                description: "query budget object aggregate class".to_string(),
                json_schema: None,
                validate_schema: None,
            },
            (0..20)
                .map(|index| NewHubuumObject {
                    collection_id: 0,
                    hubuum_class_id: 0,
                    name: scope.scoped_name(&format!("query_budget_group_{index:02}")),
                    description: "query budget object aggregate".to_string(),
                    data: serde_json::json!({"index": index}),
                })
                .collect(),
        )
        .await
        .expect("object aggregate fixture should save");
    let actor = ensure_admin_user(&scope.pool).await;

    let run_page = |limit: usize| {
        let query_string = format!("group_by=name&limit={limit}&include_total=true");
        let request = ObjectAggregateRequest::builder(
            ObjectAggregateTarget::from_class(&fixture.class)
                .expect("aggregate target should be valid"),
            parse_object_aggregate_query(&query_string).expect("aggregate query should be valid"),
        )
        .authorization(
            ObjectAggregateAuthorization::new(
                vec![Permissions::ReadObject, Permissions::ReadCollection],
                None,
            )
            .expect("aggregate authorization should be valid"),
        )
        .cursor_budget(
            ObjectAggregateCursorBudget::for_request_target(
                &format!("/api/v1/classes/{}/object-aggregates", fixture.class.id),
                &query_string,
            )
            .expect("aggregate cursor budget should be valid"),
        )
        .build()
        .expect("aggregate request should be valid");
        crate::services::object_aggregates::aggregate_objects(&scope.pool, &actor, request)
    };

    let (small_page, small_queries) = capture_queries(run_page(1)).await;
    let (small_rows, small_total, small_cursor) = small_page
        .expect("small aggregate page should load")
        .into_parts();
    assert_eq!(small_rows.len(), 1);
    assert_eq!(small_total, 20);
    assert!(small_cursor.is_some());

    let (large_page, large_queries) = capture_queries(run_page(20)).await;
    let (large_rows, large_total, large_cursor) = large_page
        .expect("large aggregate page should load")
        .into_parts();
    assert_eq!(large_rows.len(), 20);
    assert_eq!(large_total, 20);
    assert!(large_cursor.is_none());

    assert_same_query_shape(&small_queries, &large_queries);
    assert_eq!(
        large_queries.connection_checkouts(),
        2,
        "one admin lookup and one aggregate transaction are expected: {:#?}",
        large_queries.query_counts()
    );
    assert_eq!(large_queries.total_queries(), 6);
    assert_eq!(large_queries.domain_queries(), 4);
    assert_eq!(large_queries.control_queries(), 2);
    assert_eq!(
        large_queries.queries_matching("count( DISTINCT jsonb_build_array"),
        1
    );
    assert_eq!(
        large_queries.queries_matching("GROUP BY jsonb_build_array"),
        1,
        "{:#?}",
        large_queries.query_counts()
    );

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
            group_id: GroupID::new(root.owner_group.id).unwrap(),
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
    assert_eq!(
        deep_queries.total_queries(),
        2,
        "{:#?}",
        deep_queries.query_counts()
    );
    assert_eq!(deep_queries.domain_queries(), 2);
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
    let services = crate::tests::services_for_postgres(scope.pool.get_ref().clone());
    let fixture = scope
        .collection_fixture("query_budget_changed_update")
        .await;
    let update = UpdateCollection {
        name: None,
        description: Some("changed query budget description".to_string()),
    };

    let (updated, queries) = capture_queries(services.collections().update(
        CollectionID::new(fixture.collection.id).unwrap(),
        update,
        &EventContext::system(),
    ))
    .await;
    assert_eq!(
        updated.expect("changed update should succeed").description,
        "changed query budget description"
    );

    assert_eq!(queries.total_queries(), 6, "{:#?}", queries.query_counts());
    assert_eq!(queries.domain_queries(), 4, "{:#?}", queries.query_counts());
    assert_eq!(queries.control_queries(), 2);
    assert_eq!(queries.connection_checkouts(), 1);
    assert_eq!(
        queries.queries_matching("SELECT hubuum_assert_revision_precondition"),
        1
    );
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
        from_max_relations: None,
        to_max_relations: None,
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

    assert_eq!(queries.total_queries(), 5, "{:#?}", queries.query_counts());
    assert_eq!(queries.domain_queries(), 3, "{:#?}", queries.query_counts());
    assert_eq!(queries.control_queries(), 2);
    assert_eq!(queries.connection_checkouts(), 1);
    assert_eq!(queries.queries_matching("FROM \"hubuumobject\""), 1);
    assert_eq!(
        queries.queries_matching("INSERT INTO \"hubuumobject_relation\""),
        1
    );
    assert_eq!(queries.queries_matching("INSERT INTO \"events\""), 1);

    fixture.cleanup().await.expect("fixture cleanup");
}

#[actix_web::test]
async fn object_relation_storage_query_budget_preparation_is_fixed() {
    let scope = TestScope::new();
    let (fixture, services, selector) =
        object_relation_budget_fixture(&scope, "query_budget_object_relation_prepare").await;

    let (prepared, queries) =
        capture_queries(services.object_relations().prepare_create(selector)).await;
    prepared.expect("object relation should prepare");
    assert_eq!(queries.total_queries(), 3, "{:#?}", queries.query_counts());
    assert_eq!(queries.domain_queries(), 3);
    assert_eq!(queries.control_queries(), 0);
    assert_eq!(queries.connection_checkouts(), 1);
    assert_eq!(queries.queries_matching("FROM \"hubuumobject\""), 1);
    assert_eq!(queries.queries_matching("FROM \"hubuumclass_relation\""), 1);
    assert_eq!(queries.queries_matching("FROM \"hubuumclass\""), 1);

    fixture.cleanup().await.expect("fixture cleanup");
}

#[actix_web::test]
async fn object_relation_storage_query_budget_point_resolution_is_fixed() {
    let scope = TestScope::new();
    let (fixture, services, selector) =
        object_relation_budget_fixture(&scope, "query_budget_object_relation_resolve").await;
    let prepared = services
        .object_relations()
        .prepare_create(selector)
        .await
        .expect("object relation should prepare");
    let created = services
        .object_relations()
        .create(&prepared, &EventContext::system())
        .await
        .expect("object relation should create");

    let (resolved, queries) = capture_queries(services.object_relations().resolve(
        ObjectRelationSelector::by_id(
            HubuumObjectRelationID::new(created.relation().id).expect("valid object relation id"),
        ),
    ))
    .await;
    assert_eq!(
        resolved.expect("object relation should resolve").relation(),
        created.relation()
    );
    assert_eq!(queries.total_queries(), 4, "{:#?}", queries.query_counts());
    assert_eq!(queries.domain_queries(), 4);
    assert_eq!(queries.control_queries(), 0);
    assert_eq!(queries.connection_checkouts(), 1);
    assert_eq!(
        queries.queries_matching("FROM \"hubuumobject_relation\""),
        1
    );
    assert_eq!(queries.queries_matching("FROM \"hubuumobject\""), 1);

    fixture.cleanup().await.expect("fixture cleanup");
}

#[actix_web::test]
async fn object_relation_storage_query_budget_create_with_event_is_fixed() {
    let scope = TestScope::new();
    let (fixture, services, selector) =
        object_relation_budget_fixture(&scope, "query_budget_object_relation_service_create").await;
    let prepared = services
        .object_relations()
        .prepare_create(selector)
        .await
        .expect("object relation should prepare");

    let (created, queries) = capture_queries(
        services
            .object_relations()
            .create(&prepared, &EventContext::system()),
    )
    .await;
    created.expect("object relation should create with an event");
    assert_eq!(queries.total_queries(), 6, "{:#?}", queries.query_counts());
    assert_eq!(queries.domain_queries(), 4);
    assert_eq!(queries.control_queries(), 2);
    assert_eq!(queries.connection_checkouts(), 1);
    assert_eq!(
        queries.queries_matching("INSERT INTO \"hubuumobject_relation\""),
        1
    );
    assert_eq!(queries.queries_matching("INSERT INTO \"events\""), 1);

    fixture.cleanup().await.expect("fixture cleanup");
}

#[actix_web::test]
async fn object_relation_storage_query_budget_delete_with_event_is_fixed() {
    let scope = TestScope::new();
    let (fixture, services, selector) =
        object_relation_budget_fixture(&scope, "query_budget_object_relation_service_delete").await;
    let prepared = services
        .object_relations()
        .prepare_create(selector)
        .await
        .expect("object relation should prepare");
    let target = services
        .object_relations()
        .create(&prepared, &EventContext::system())
        .await
        .expect("object relation should create");

    let (deleted, queries) = capture_queries(
        services
            .object_relations()
            .delete(&target, &EventContext::system()),
    )
    .await;
    deleted.expect("object relation should delete with an event");
    assert_eq!(queries.total_queries(), 6, "{:#?}", queries.query_counts());
    assert_eq!(queries.domain_queries(), 4);
    assert_eq!(queries.control_queries(), 2);
    assert_eq!(queries.connection_checkouts(), 1);
    assert_eq!(
        queries.queries_matching("DELETE FROM \"hubuumobject_relation\""),
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
        with_mutation_provenance(
            &scope.pool,
            Some(MutationProvenance::user(
                PrincipalId::new(actor.id).expect("persisted principal id must be positive"),
            )),
            UpdateCollection {
                name: None,
                description: Some(format!("query budget history version {version}")),
            }
            .update_without_events(
                &scope.pool,
                CollectionID::new(fixture.collection.id).unwrap(),
            ),
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
            let principal_ids = rows
                .iter()
                .flat_map(|row| [row.actor_id, row.initiator_user_id])
                .flatten()
                .collect();
            let principal_names = resolve_principal_names(&pool, principal_ids).await?;
            Ok::<_, crate::errors::ApiError>((rows, total, principal_names))
        }
    };

    let (small_page, small_queries) = capture_queries(load_history(1)).await;
    let (small_rows, small_total, small_principal_names) =
        small_page.expect("small history page should load");
    assert_eq!(small_rows.len(), 1);
    assert!(small_total >= 12);
    assert!(small_principal_names.contains(actor.id));

    let (large_page, large_queries) = capture_queries(load_history(20)).await;
    let (large_rows, large_total, large_principal_names) =
        large_page.expect("large history page should load");
    assert!(large_rows.len() >= 12);
    assert_eq!(large_total, small_total);
    assert!(large_principal_names.contains(actor.id));

    assert_same_query_shape(&small_queries, &large_queries);
    assert_eq!(large_queries.total_queries(), 6);
    assert_eq!(large_queries.domain_queries(), 3);
    assert_eq!(large_queries.control_queries(), 3);
    assert_eq!(large_queries.connection_checkouts(), 2);

    fixture.cleanup().await.expect("fixture cleanup");
}

#[actix_web::test]
async fn external_identity_sync_query_count_is_constant_with_group_count() {
    use hubuum_storage_core::{StorageExternalGroup, StorageExternalUserSync};
    use hubuum_storage_postgres::PostgresRuntime;

    let scope = TestScope::new();
    let runtime = PostgresRuntime::unobserved(scope.pool.get_ref().clone());
    let request = |label: &str, group_count: usize| {
        StorageExternalUserSync::builder(
            scope.scoped_name(&format!("external_budget_scope_{label}")),
            "query_budget_provider",
            scope.scoped_name(&format!("external_budget_subject_{label}")),
            scope.scoped_name(&format!("external_budget_user_{label}")),
        )
        .groups(
            (0..group_count)
                .map(|index| {
                    StorageExternalGroup::new(
                        scope.scoped_name(&format!("external_budget_key_{label}_{index}")),
                        scope.scoped_name(&format!("external_budget_group_{label}_{index}")),
                        Some("query-budget external group".to_string()),
                    )
                })
                .collect(),
        )
        .build()
    };

    let (small_result, small_queries) = capture_queries(
        hubuum_storage_postgres::operations::external_identity::sync_external_user(
            &runtime,
            request("small", 1),
        ),
    )
    .await;
    small_result.expect("one-group external sync should succeed");
    let (large_result, large_queries) = capture_queries(
        hubuum_storage_postgres::operations::external_identity::sync_external_user(
            &runtime,
            request("large", 20),
        ),
    )
    .await;
    large_result.expect("twenty-group external sync should succeed");

    assert_eq!(large_queries.total_queries(), small_queries.total_queries());
    assert_eq!(
        large_queries.domain_queries(),
        small_queries.domain_queries()
    );
    assert_eq!(
        large_queries.control_queries(),
        small_queries.control_queries()
    );
    assert_eq!(
        large_queries.connection_checkouts(),
        small_queries.connection_checkouts()
    );
    assert_eq!(
        small_queries.total_queries(),
        16,
        "{:#?}",
        small_queries.query_counts()
    );
    assert_eq!(small_queries.domain_queries(), 14);
    assert_eq!(small_queries.control_queries(), 2);
    assert_eq!(small_queries.connection_checkouts(), 1);
    for statement in [
        "INSERT INTO \"groups\"",
        "INSERT INTO \"group_memberships\"",
        "INSERT INTO \"group_membership_sources\"",
        "DELETE FROM \"group_membership_sources\"",
        "DELETE FROM \"group_memberships\"",
    ] {
        assert_eq!(small_queries.queries_matching(statement), 1);
        assert_eq!(large_queries.queries_matching(statement), 1);
    }
}
