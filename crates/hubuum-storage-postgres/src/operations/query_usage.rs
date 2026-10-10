//! Atomic, audited storage of advisory class workload intent.
use diesel::{
    prelude::*,
    sql_types::{BigInt, Integer, Jsonb, Nullable},
};
use diesel_async::RunQueryDsl;
use hubuum_domain::{CollectionId, ResourceId};
use hubuum_events_core::{
    Action, AuditDocument, EntityType, EventContext, EventEntityId, NewEvent,
};
use hubuum_storage_core::{
    MAX_QUERY_USAGE_DECLARATIONS, StorageAuditReceipt, StorageMutationOutcome,
    StorageQueryUsageCreate, StorageQueryUsageDeclaration, StorageQueryUsageDelete,
    StorageQueryUsageReplace, StorageQueryUsageScope,
};
use serde_json::{Value, json};

use super::{class::ClassRow, event_record::append_event};
use crate::{PostgresConnection, PostgresRuntime, PostgresStorageError};

#[derive(QueryableByName)]
struct CountRow {
    #[diesel(sql_type=BigInt)]
    count: i64,
}

#[derive(QueryableByName)]
struct JsonRow {
    #[diesel(sql_type=Jsonb)]
    value: Value,
}
impl JsonRow {
    fn record(self) -> Result<StorageQueryUsageDeclaration, PostgresStorageError> {
        StorageQueryUsageDeclaration::from_snapshot(self.value).map_err(|error| {
            PostgresStorageError::invalid_persisted_value("query usage declaration", error)
        })
    }
}

async fn lock_class(
    connection: &mut PostgresConnection,
    scope: StorageQueryUsageScope,
) -> Result<ClassRow, PostgresStorageError> {
    use crate::schema::hubuumclass::dsl as classes;
    classes::hubuumclass
        .filter(classes::id.eq(scope.class_id().id()))
        .filter(classes::collection_id.eq(scope.authorized_collection().id()))
        .for_update()
        .select(ClassRow::as_select())
        .first(connection)
        .await
        .map_err(PostgresStorageError::from)
}

async fn current(
    connection: &mut PostgresConnection,
    scope: StorageQueryUsageScope,
    id: ResourceId,
) -> Result<StorageQueryUsageDeclaration, PostgresStorageError> {
    diesel::sql_query(
        "SELECT to_jsonb(d) AS value FROM query_usage_declarations d WHERE class_id=$1 AND id=$2",
    )
    .bind::<Integer, _>(scope.class_id().id())
    .bind::<Integer, _>(id.id())
    .get_result::<JsonRow>(connection)
    .await?
    .record()
}

async fn audit(
    connection: &mut PostgresConnection,
    scope: StorageQueryUsageScope,
    context: &EventContext,
    before: Option<&StorageQueryUsageDeclaration>,
    after: Option<&StorageQueryUsageDeclaration>,
) -> Result<StorageAuditReceipt, PostgresStorageError> {
    let value = after.or(before).expect("mutation has a declaration");
    let action = match (before, after) {
        (None, _) => Action::Created,
        (_, None) => Action::Deleted,
        _ => Action::Updated,
    };
    let document = AuditDocument::try_new(
        "Query usage declaration changed",
        before.map(StorageQueryUsageDeclaration::snapshot),
        after.map(StorageQueryUsageDeclaration::snapshot),
        json!({"class_id":scope.class_id()}),
    )?;
    let event = NewEvent::from_document(
        EntityType::QueryUsageDeclaration,
        action,
        context.actor_kind(),
        document,
    )
    .map_err(|error| PostgresStorageError::invalid_persisted_value("query usage event", error))?
    .with_context(context)
    .with_entity_id(EventEntityId::new(value.metadata().id().id())?)
    .with_collection_id(scope.authorized_collection());
    Ok(append_event(connection, &event).await?.into_audit_receipt())
}

pub async fn list_query_usage(
    runtime: &PostgresRuntime,
    scope: StorageQueryUsageScope,
) -> Result<Vec<StorageQueryUsageDeclaration>, PostgresStorageError> {
    runtime.with_transaction::<_, _, PostgresStorageError>(async move |connection| {
        lock_class(connection, scope).await?;
        diesel::sql_query("SELECT to_jsonb(d) AS value FROM query_usage_declarations d WHERE class_id=$1 ORDER BY id").bind::<Integer,_>(scope.class_id().id()).load::<JsonRow>(connection).await?.into_iter().map(JsonRow::record).collect()
    }).await
}

pub async fn create_query_usage(
    runtime: &PostgresRuntime,
    request: StorageQueryUsageCreate,
) -> Result<StorageMutationOutcome<StorageQueryUsageDeclaration>, PostgresStorageError> {
    runtime.with_transaction::<_, _, PostgresStorageError>(async move |connection| {
        lock_class(connection, request.scope()).await?;
        let count = diesel::sql_query("SELECT count(*) AS count FROM query_usage_declarations WHERE class_id=$1").bind::<Integer,_>(request.scope().class_id().id()).get_result::<CountRow>(connection).await?.count;
        if count >= MAX_QUERY_USAGE_DECLARATIONS as i64 { return Err(PostgresStorageError::conflict("Class query usage declaration limit reached")); }
        let record = diesel::sql_query("INSERT INTO query_usage_declarations AS d(class_id,pattern,created_by,updated_by) VALUES($1,$2,$3,$3) RETURNING to_jsonb(d) AS value").bind::<Integer,_>(request.scope().class_id().id()).bind::<Jsonb,_>(json!(request.pattern())).bind::<Nullable<Integer>,_>(request.context().actor_user_id().map(|id|id.id())).get_result::<JsonRow>(connection).await?.record()?;
        let receipt = audit(connection, request.scope(), request.context(), None, Some(&record)).await?;
        Ok(StorageMutationOutcome::committed(record, receipt))
    }).await
}

pub async fn replace_query_usage(
    runtime: &PostgresRuntime,
    request: StorageQueryUsageReplace,
) -> Result<StorageMutationOutcome<StorageQueryUsageDeclaration>, PostgresStorageError> {
    runtime.with_transaction::<_, _, PostgresStorageError>(async move |connection| {
        lock_class(connection, request.scope()).await?;
        let before = current(connection, request.scope(), request.id()).await?;
        if before.metadata().revision() != request.expected_revision() { return Err(PostgresStorageError::conflict("Query usage declaration revision changed")); }
        if before.pattern() == request.pattern() { return Ok(StorageMutationOutcome::unchanged(before)); }
        let record = diesel::sql_query("UPDATE query_usage_declarations AS d SET pattern=$1,revision=revision+1,updated_at=greatest(clock_timestamp(),updated_at),updated_by=$2 WHERE id=$3 RETURNING to_jsonb(d) AS value").bind::<Jsonb,_>(json!(request.pattern())).bind::<Nullable<Integer>,_>(request.context().actor_user_id().map(|id|id.id())).bind::<Integer,_>(request.id().id()).get_result::<JsonRow>(connection).await?.record()?;
        let receipt = audit(connection, request.scope(), request.context(), Some(&before), Some(&record)).await?;
        Ok(StorageMutationOutcome::committed(record, receipt))
    }).await
}

pub async fn delete_query_usage(
    runtime: &PostgresRuntime,
    request: StorageQueryUsageDelete,
) -> Result<StorageMutationOutcome<()>, PostgresStorageError> {
    runtime
        .with_transaction::<_, _, PostgresStorageError>(async move |connection| {
            lock_class(connection, request.scope()).await?;
            let before = current(connection, request.scope(), request.id()).await?;
            if before.metadata().revision() != request.expected_revision() {
                return Err(PostgresStorageError::conflict(
                    "Query usage declaration revision changed",
                ));
            }
            diesel::sql_query("DELETE FROM query_usage_declarations WHERE id=$1")
                .bind::<Integer, _>(request.id().id())
                .execute(connection)
                .await?;
            let receipt = audit(
                connection,
                request.scope(),
                request.context(),
                Some(&before),
                None,
            )
            .await?;
            Ok(StorageMutationOutcome::committed((), receipt))
        })
        .await
}

pub(crate) async fn delete_class_query_usage(
    connection: &mut PostgresConnection,
    class: &ClassRow,
    context: &EventContext,
) -> Result<(), PostgresStorageError> {
    let scope = StorageQueryUsageScope::new(
        hubuum_domain::ClassId::new(class.id)?,
        CollectionId::new(class.collection_id)?,
    );
    let records = diesel::sql_query("DELETE FROM query_usage_declarations AS d WHERE class_id=$1 RETURNING to_jsonb(d) AS value").bind::<Integer,_>(class.id).load::<JsonRow>(connection).await?;
    for row in records {
        audit(connection, scope, context, Some(&row.record()?), None).await?;
    }
    Ok(())
}
