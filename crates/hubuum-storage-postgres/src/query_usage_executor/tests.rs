use super::*;
use crate::{
    PostgresStorage,
    test_support::{
        database_role_tests_enabled, integration_test_database_roles,
        integration_test_migration_pool, integration_test_pool,
    },
};
use diesel::sql_types::Json;
use hubuum_domain::CollectionId;
use hubuum_events_core::EventContext;
use hubuum_storage_core::{
    ClassStorage, QueryUsageStorage, StorageClassSelector, StorageQueryUsageCreate,
    StorageQueryUsageDelete, StorageQueryUsageReplace, StorageQueryUsageScope,
};
use serde_json::json;
use tokio::sync::Mutex;

// Native resources are database-wide; these tests serialize only their executor
// operations while other adapter contracts retain ordinary parallel coverage.
static EXECUTION_TESTS: Mutex<()> = Mutex::const_new(());

struct Fixture {
    executor: QueryUsageExecutor,
    storage: PostgresStorage,
    scope: StorageQueryUsageScope,
    path: String,
}
impl Fixture {
    async fn new() -> Self {
        let executor = executor();
        let path = format!("usage_{}", Uuid::new_v4().simple());
        let (collection,class) = executor.runtime.with_transaction::<_,_,PostgresStorageError>(async |connection| {
            assume_fixture_owner(&executor, connection).await?;
            let collection = diesel::sql_query("INSERT INTO collections(name,description,parent_collection_id) SELECT $1,'native query usage test',id FROM collections WHERE parent_collection_id IS NULL RETURNING id::bigint AS id").bind::<Text,_>(&path).get_result::<IdRow>(connection).await?.id as i32;
            let class = diesel::sql_query("INSERT INTO hubuumclass(name,collection_id,description,validate_schema) VALUES($1,$2,'native query usage test',false) RETURNING id::bigint AS id").bind::<Text,_>(&path).bind::<Integer,_>(collection).get_result::<IdRow>(connection).await?.id as i32;
            Ok((collection,class))
        }).await.unwrap();
        Self {
            executor,
            storage: PostgresStorage::unobserved(integration_test_pool(2)),
            scope: StorageQueryUsageScope::new(
                ClassId::new(class).unwrap(),
                CollectionId::new(collection).unwrap(),
            ),
            path,
        }
    }
    async fn declare(&self, path: &str) -> StorageQueryUsageDeclaration {
        self.storage
            .create_query_usage(StorageQueryUsageCreate::new(
                self.scope,
                pattern(path),
                EventContext::system(),
            ))
            .await
            .unwrap()
            .into_value()
    }
    async fn withdraw(&self, declaration: &StorageQueryUsageDeclaration) {
        let _ = self
            .storage
            .delete_query_usage(StorageQueryUsageDelete::new(
                self.scope,
                declaration.metadata().id(),
                declaration.metadata().revision(),
                EventContext::system(),
            ))
            .await
            .unwrap();
    }
    async fn resource(&self) -> Option<Resource> {
        self.executor
            .runtime
            .with_transaction::<_, _, PostgresStorageError>(async |connection| {
                assume_fixture_owner(&self.executor, connection).await?;
                Ok(resources(connection)
                    .await?
                    .into_iter()
                    .find(|resource| resource.path == self.path))
            })
            .await
            .unwrap()
    }
    async fn cleanup(self) {
        self.executor
            .runtime
            .with_transaction::<_, _, PostgresStorageError>(async |connection| {
                assume_fixture_owner(&self.executor, connection).await?;
                diesel::sql_query("DELETE FROM hubuumobject WHERE hubuum_class_id=$1")
                    .bind::<Integer, _>(self.scope.class_id().id())
                    .execute(connection)
                    .await?;
                diesel::sql_query("DELETE FROM hubuumclass WHERE id=$1")
                    .bind::<Integer, _>(self.scope.class_id().id())
                    .execute(connection)
                    .await?;
                diesel::sql_query("DELETE FROM collections WHERE id=$1")
                    .bind::<Integer, _>(self.scope.authorized_collection().id())
                    .execute(connection)
                    .await?;
                Ok(())
            })
            .await
            .unwrap();
        self.executor
            .reconcile(Some(self.scope.class_id()))
            .await
            .unwrap();
    }
}
// Fixture setup and administrator edits use the pool's normal timeouts. Only
// executor operations should apply its deliberately short native-action budget.
async fn assume_fixture_owner(
    executor: &QueryUsageExecutor,
    connection: &mut PostgresConnection,
) -> Result<(), PostgresStorageError> {
    if let Some(owner) = &executor.owner {
        diesel::sql_query("SELECT set_config('role',$1,true)")
            .bind::<Text, _>(owner.as_str())
            .execute(connection)
            .await?;
    }
    Ok(())
}

fn executor() -> QueryUsageExecutor {
    let owner =
        database_role_tests_enabled().then(|| integration_test_database_roles().owner().clone());
    let pool = if owner.is_some() {
        integration_test_migration_pool(2)
    } else {
        integration_test_pool(2)
    };
    QueryUsageExecutor::new(pool, owner)
}
fn pattern(path: &str) -> StorageQueryUsagePattern {
    use hubuum_storage_core::{StorageQueryUsageOperation, StorageQueryUsageValueType};
    StorageQueryUsagePattern::try_new(
        path,
        StorageQueryUsageValueType::String,
        vec![StorageQueryUsageOperation::Equals],
    )
    .unwrap()
}

#[tokio::test]
async fn shared_resource_survives_until_the_last_owner_withdraws() {
    let _permit = EXECUTION_TESTS.lock().await;
    let first = Fixture::new().await;
    let second = Fixture::new().await;
    let a = first.declare(&first.path).await;
    let b = second.declare(&first.path).await;
    first
        .executor
        .reconcile(Some(first.scope.class_id()))
        .await
        .unwrap();
    second
        .executor
        .reconcile(Some(second.scope.class_id()))
        .await
        .unwrap();
    let shared = first.resource().await.unwrap();
    assert_eq!(shared.owners, 2);
    assert_eq!(shared.state, "ready");
    first.withdraw(&a).await;
    first
        .executor
        .reconcile(Some(first.scope.class_id()))
        .await
        .unwrap();
    assert_eq!(first.resource().await.unwrap().index_oid, shared.index_oid);
    second.withdraw(&b).await;
    let pending = first.resource().await.unwrap();
    assert_eq!(pending.state, "cleanup");
    first
        .executor
        .reconcile(Some(first.scope.class_id()))
        .await
        .unwrap();
    assert!(first.resource().await.is_none());
    second.cleanup().await;
    first.cleanup().await;
}

#[tokio::test]
async fn restart_rechecks_withdrawn_pending_intent_before_native_creation() {
    let _permit = EXECUTION_TESTS.lock().await;
    let fixture = Fixture::new().await;
    let declaration = fixture.declare(&fixture.path).await;
    fixture
        .executor
        .plan(Some(fixture.scope.class_id()))
        .await
        .unwrap();
    let pending = fixture.resource().await.unwrap();
    assert!(pending.index_oid.is_none());
    fixture.withdraw(&declaration).await;
    let restarted = executor();
    assert!(matches!(
        restarted.apply(pending.id).await.unwrap(),
        Action::Removed
    ));
    assert!(fixture.resource().await.is_none());
    fixture.cleanup().await;
}

#[tokio::test]
async fn pattern_replacement_withdraws_the_old_physical_owner() {
    let _permit = EXECUTION_TESTS.lock().await;
    let fixture = Fixture::new().await;
    let declaration = fixture.declare(&fixture.path).await;
    fixture
        .executor
        .reconcile(Some(fixture.scope.class_id()))
        .await
        .unwrap();
    let _ = fixture
        .storage
        .replace_query_usage(StorageQueryUsageReplace::new(
            StorageQueryUsageCreate::new(
                fixture.scope,
                pattern(&format!("{}_new", fixture.path)),
                EventContext::system(),
            ),
            declaration.metadata().id(),
            declaration.metadata().revision(),
        ))
        .await
        .unwrap();
    assert_eq!(fixture.resource().await.unwrap().state, "cleanup");
    fixture
        .executor
        .reconcile(Some(fixture.scope.class_id()))
        .await
        .unwrap();
    assert!(fixture.resource().await.is_none());
    fixture.cleanup().await;
}

#[tokio::test]
async fn independently_created_index_is_never_claimed_or_dropped() {
    let _permit = EXECUTION_TESTS.lock().await;
    let fixture = Fixture::new().await;
    let name = format!("external_{}", Uuid::new_v4().simple());
    fixture
        .executor
        .runtime
        .with_transaction::<_, _, PostgresStorageError>(async |connection| {
            assume_fixture_owner(&fixture.executor, connection).await?;
            diesel::sql_query(format!(
                "CREATE INDEX {name} ON hubuumobject USING hash ((data #>> '{{{}}}'))",
                fixture.path
            ))
            .execute(connection)
            .await?;
            Ok(())
        })
        .await
        .unwrap();
    let declaration = fixture.declare(&fixture.path).await;
    fixture
        .executor
        .reconcile(Some(fixture.scope.class_id()))
        .await
        .unwrap();
    assert!(fixture.resource().await.is_none());
    fixture.withdraw(&declaration).await;
    fixture
        .executor
        .reconcile(Some(fixture.scope.class_id()))
        .await
        .unwrap();
    // DROP without IF EXISTS proves the executor preserved the external object.
    fixture
        .executor
        .runtime
        .with_transaction::<_, _, PostgresStorageError>(async |connection| {
            assume_fixture_owner(&fixture.executor, connection).await?;
            diesel::sql_query(format!("DROP INDEX {name}"))
                .execute(connection)
                .await?;
            Ok(())
        })
        .await
        .unwrap();
    fixture.cleanup().await;
}

#[tokio::test]
async fn renamed_index_keeps_cleanup_evidence_and_is_not_recreated() {
    let _permit = EXECUTION_TESTS.lock().await;
    let fixture = Fixture::new().await;
    let declaration = fixture.declare(&fixture.path).await;
    fixture
        .executor
        .reconcile(Some(fixture.scope.class_id()))
        .await
        .unwrap();
    let resource = fixture.resource().await.unwrap();
    let name = format!("renamed_{}", Uuid::new_v4().simple());
    fixture
        .executor
        .runtime
        .with_transaction::<_, _, PostgresStorageError>(async |connection| {
            assume_fixture_owner(&fixture.executor, connection).await?;
            diesel::sql_query(format!("ALTER INDEX {} RENAME TO {name}", resource.name()))
                .execute(connection)
                .await?;
            Ok(())
        })
        .await
        .unwrap();
    fixture.withdraw(&declaration).await;
    let report = fixture
        .executor
        .reconcile(Some(fixture.scope.class_id()))
        .await
        .unwrap();
    assert_eq!(report.deferred, 1);
    assert_eq!(
        fixture.resource().await.unwrap().last_error.as_deref(),
        Some("identity_mismatch")
    );
    // Manual cleanup is intentionally required after ownership identity changes.
    fixture
        .executor
        .runtime
        .with_transaction::<_, _, PostgresStorageError>(async |connection| {
            assume_fixture_owner(&fixture.executor, connection).await?;
            diesel::sql_query(format!("DROP INDEX {name}"))
                .execute(connection)
                .await?;
            Ok(())
        })
        .await
        .unwrap();
    fixture.cleanup().await;
}

#[tokio::test]
async fn failed_transaction_leaves_no_native_orphan_and_retry_succeeds() {
    let _permit = EXECUTION_TESTS.lock().await;
    let fixture = Fixture::new().await;
    fixture.declare(&fixture.path).await;
    fixture
        .executor
        .plan(Some(fixture.scope.class_id()))
        .await
        .unwrap();
    let resource = fixture.resource().await.unwrap();
    // Hold a conflicting object lock on an independent privileged connection.
    let pool = integration_test_migration_pool(1);
    let mut connection = pool.get().await.unwrap();
    if database_role_tests_enabled() {
        diesel::sql_query("SELECT set_config('role',$1,false)")
            .bind::<Text, _>(integration_test_database_roles().owner().as_str())
            .execute(&mut connection)
            .await
            .unwrap();
    }
    diesel::sql_query("BEGIN")
        .execute(&mut connection)
        .await
        .unwrap();
    diesel::sql_query("LOCK TABLE hubuumobject IN ROW EXCLUSIVE MODE")
        .execute(&mut connection)
        .await
        .unwrap();
    let attempted = fixture.executor.apply(resource.id).await;
    diesel::sql_query("ROLLBACK")
        .execute(&mut connection)
        .await
        .unwrap();
    assert!(attempted.is_err());
    assert!(fixture.resource().await.unwrap().index_oid.is_none());
    assert!(matches!(
        fixture.executor.apply(resource.id).await.unwrap(),
        Action::Prepared
    ));
    fixture.cleanup().await;
}

#[tokio::test]
async fn prepared_expression_supports_the_real_filter_without_rejecting_long_text() {
    use crate::operations::json_filter::json_filter_sql;
    use hubuum_query::parse_query_parameter;
    let _permit = EXECUTION_TESTS.lock().await;
    let fixture = Fixture::new().await;
    fixture.declare(&fixture.path).await;
    fixture
        .executor
        .reconcile(Some(fixture.scope.class_id()))
        .await
        .unwrap();
    let resource = fixture.resource().await.unwrap();
    let options =
        parse_query_parameter(&format!("json_data={}=sample-text", fixture.path)).unwrap();
    let predicate = json_filter_sql(&options.filters()[0], "data").unwrap();
    let sql = format!(
        "EXPLAIN (FORMAT JSON) SELECT id FROM hubuumobject WHERE {}",
        predicate.sql.replace('?', "$1")
    );
    let plan = fixture.executor.runtime.with_transaction::<_,_,PostgresStorageError>(async |connection| {
        assume_fixture_owner(&fixture.executor, connection).await?;
        diesel::sql_query("SET LOCAL enable_seqscan=off").execute(connection).await?;
        #[derive(QueryableByName)] struct Plan { #[diesel(sql_type=Json,column_name="QUERY PLAN")] value: Value }
        let plan = diesel::sql_query(sql).bind::<Text,_>("sample-text").get_result::<Plan>(connection).await?.value;
        let data = json!({fixture.path.clone(): "x".repeat(100_000)});
        diesel::sql_query("INSERT INTO hubuumobject(name,description,collection_id,hubuum_class_id,data) VALUES('long-text','query usage test',$1,$2,$3)").bind::<Integer,_>(fixture.scope.authorized_collection().id()).bind::<Integer,_>(fixture.scope.class_id().id()).bind::<Jsonb,_>(data).execute(connection).await?;
        Ok(plan)
    }).await.unwrap();
    assert!(plan.to_string().contains(&resource.name()));
    fixture.cleanup().await;
}

#[tokio::test]
async fn concurrent_last_owner_withdrawals_leave_durable_cleanup_state() {
    let _permit = EXECUTION_TESTS.lock().await;
    let first = Fixture::new().await;
    let second = Fixture::new().await;
    let a = first.declare(&first.path).await;
    let b = second.declare(&first.path).await;
    first
        .executor
        .reconcile(Some(first.scope.class_id()))
        .await
        .unwrap();
    second
        .executor
        .reconcile(Some(second.scope.class_id()))
        .await
        .unwrap();
    tokio::join!(first.withdraw(&a), second.withdraw(&b));
    let resource = first.resource().await.unwrap();
    assert_eq!((resource.owners, resource.state.as_str()), (0, "cleanup"));
    second.cleanup().await;
    first.cleanup().await;
}

#[tokio::test]
async fn class_deletion_withdraws_ownership_without_losing_cleanup_identity() {
    let _permit = EXECUTION_TESTS.lock().await;
    let fixture = Fixture::new().await;
    fixture.declare(&fixture.path).await;
    fixture
        .executor
        .reconcile(Some(fixture.scope.class_id()))
        .await
        .unwrap();
    let oid = fixture.resource().await.unwrap().index_oid;
    let class = fixture
        .storage
        .resolve_class(StorageClassSelector::Id(fixture.scope.class_id()))
        .await
        .unwrap();
    let _ = fixture
        .storage
        .delete_class(&class, &EventContext::system())
        .await
        .unwrap();
    let resource = fixture.resource().await.unwrap();
    assert_eq!(
        (resource.owners, resource.state.as_str(), resource.index_oid),
        (0, "cleanup", oid)
    );
    fixture.cleanup().await;
}

#[tokio::test]
async fn restore_withdraws_owners_and_reconciles_preserved_local_resources() {
    let _permit = EXECUTION_TESTS.lock().await;
    let fixture = Fixture::new().await;
    fixture.declare(&fixture.path).await;
    fixture
        .executor
        .reconcile(Some(fixture.scope.class_id()))
        .await
        .unwrap();
    let oid = fixture.resource().await.unwrap().index_oid;
    fixture
        .executor
        .runtime
        .with_transaction::<_, _, PostgresStorageError>(async |connection| {
            assume_fixture_owner(&fixture.executor, connection).await?;
            // Restore uses this table's TRUNCATE trigger, never serializes its rows.
            diesel::sql_query("TRUNCATE query_usage_resource_owners")
                .execute(connection)
                .await?;
            Ok(())
        })
        .await
        .unwrap();
    let pending = fixture.resource().await.unwrap();
    assert_eq!(
        (pending.owners, pending.state.as_str(), pending.index_oid),
        (0, "cleanup", oid)
    );
    fixture
        .executor
        .reconcile(Some(fixture.scope.class_id()))
        .await
        .unwrap();
    let restored = fixture.resource().await.unwrap();
    assert_eq!(
        (restored.owners, restored.state.as_str(), restored.index_oid),
        (1, "ready", oid)
    );
    fixture.cleanup().await;
}

#[rstest::rstest]
#[case("query_usage_resources", "state='cleanup'")]
#[case("query_usage_resource_owners", "resource_id=resource_id")]
#[case("query_usage_executor_cursor", "after_declaration_id=0")]
#[tokio::test]
async fn runtime_role_cannot_forge_native_ownership(#[case] table: &str, #[case] assignment: &str) {
    if !database_role_tests_enabled() {
        return;
    }
    let pool = integration_test_pool(1);
    let result = crate::with_transaction(&pool, async |connection| {
        diesel::sql_query(format!("UPDATE {table} SET {assignment}"))
            .execute(connection)
            .await
    })
    .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn resource_limit_defers_until_cleanup_frees_capacity() {
    let _permit = EXECUTION_TESTS.lock().await;
    let fixture = Fixture::new().await;
    let first = fixture.declare(&format!("{}_first", fixture.path)).await;
    for index in 1..MAX_RESOURCES {
        fixture
            .declare(&format!("{}_{}", fixture.path, index))
            .await;
    }
    // This ninth path is the one returned by Fixture::resource.
    fixture.declare(&fixture.path).await;
    let full = fixture
        .executor
        .reconcile(Some(fixture.scope.class_id()))
        .await
        .unwrap();
    assert_eq!(
        (full.prepared, full.deferred, full.resources.len()),
        (MAX_RESOURCES, 1, MAX_RESOURCES)
    );
    assert!(fixture.resource().await.is_none());

    fixture.withdraw(&first).await;
    let cleanup = fixture
        .executor
        .reconcile(Some(fixture.scope.class_id()))
        .await
        .unwrap();
    assert_eq!(
        (cleanup.removed, cleanup.resources.len()),
        (1, MAX_RESOURCES - 1)
    );
    let admitted = fixture
        .executor
        .reconcile(Some(fixture.scope.class_id()))
        .await
        .unwrap();
    assert_eq!(
        (
            admitted.prepared,
            admitted.deferred,
            admitted.resources.len()
        ),
        (1, 0, MAX_RESOURCES)
    );
    let resource = fixture.resource().await.unwrap();
    assert_eq!(resource.state, "ready");
    assert!(resource.index_oid.is_some());
    fixture.cleanup().await;
}

#[tokio::test]
async fn restart_advances_past_a_full_batch_of_externally_covered_declarations() {
    let _permit = EXECUTION_TESTS.lock().await;
    let first = Fixture::new().await;
    let second = Fixture::new().await;
    let tail = Fixture::new().await;
    let paths = (0..32)
        .map(|index| format!("{}_{}", first.path, index))
        .collect::<Vec<_>>();
    let mut start = None;
    // Two classes reuse 32 external indexes: the first 64 candidates stay
    // unowned and would occupy every pass without the persisted scan cursor.
    for fixture in [&first, &second] {
        for path in &paths {
            let declaration = fixture.declare(path).await;
            start.get_or_insert(declaration.metadata().id().id() - 1);
        }
    }
    tail.declare(&tail.path).await;
    first.executor.runtime.with_transaction::<_, _, PostgresStorageError>(async |connection| {
        assume_fixture_owner(&first.executor, connection).await?;
        for path in &paths {
            diesel::sql_query(format!("CREATE INDEX external_{path} ON hubuumobject USING hash ((data #>> '{{{path}}}'))"))
                .execute(connection).await?;
        }
        diesel::sql_query("UPDATE query_usage_executor_cursor SET after_declaration_id=$1 WHERE singleton")
            .bind::<Integer,_>(start.unwrap()).execute(connection).await?;
        Ok(())
    }).await.unwrap();

    let covered = first.executor.reconcile(None).await.unwrap();
    assert_eq!(covered.prepared, 0);
    assert!(tail.resource().await.is_none());
    let restarted = executor();
    let progressed = restarted.reconcile(None).await.unwrap();
    assert_eq!(progressed.prepared, 1);
    assert_eq!(tail.resource().await.unwrap().state, "ready");

    first.cleanup().await;
    second.cleanup().await;
    // Each external index must still exist after declaration withdrawal.
    tail.executor
        .runtime
        .with_transaction::<_, _, PostgresStorageError>(async |connection| {
            assume_fixture_owner(&tail.executor, connection).await?;
            for path in &paths {
                diesel::sql_query(format!("DROP INDEX external_{path}"))
                    .execute(connection)
                    .await?;
            }
            Ok(())
        })
        .await
        .unwrap();
    tail.cleanup().await;
}
