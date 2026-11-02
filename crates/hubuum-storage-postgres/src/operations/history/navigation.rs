use diesel::{
    dsl::sql,
    sql_types::{BigInt, Bool, Timestamptz},
};
use hubuum_storage_core::{
    StorageHistoryEntry, StorageHistoryEntryQuery, StorageHistoryMarker,
    StorageHistoryNavigationQuery, StorageHistoryResource, StorageHistorySelection,
};

use super::*;

// PostgreSQL stores microseconds. Adjust comparisons, rather than rounding an
// exclusive/inclusive boundary into a different set of entries.
fn timestamp_floor(at: DateTime<Utc>) -> DateTime<Utc> {
    at - chrono::Duration::nanoseconds(i64::from(at.timestamp_subsec_nanos() % 1_000))
}

#[derive(diesel::Queryable)]
struct MarkerRow {
    collection_id: i32,
    name: String,
    op: String,
    valid_from: DateTime<Utc>,
    valid_to: Option<DateTime<Utc>>,
    actor_id: Option<i32>,
    history_id: i64,
    actor_kind: Option<String>,
    initiator_user_id: Option<i32>,
    task_id: Option<i32>,
    revision: PostgresRevision,
}

impl MarkerRow {
    fn into_marker(
        self,
        resource: StorageHistoryResource,
    ) -> Result<StorageHistoryMarker, PostgresStorageError> {
        let metadata = project_history_metadata(
            history_operation(&self.op)?,
            self.valid_from,
            self.valid_to,
            HistoryRecordId::new(self.history_id)?,
            self.revision.into_domain(),
        )?
        .actor(
            self.actor_id.map(PrincipalId::new).transpose()?,
            self.actor_kind.clone(),
        )
        .initiator_principal_id(self.initiator_user_id.map(PrincipalId::new).transpose()?)
        .task_id(self.task_id.map(TaskId::new).transpose()?);
        Ok(StorageHistoryMarker::new(
            resource,
            CollectionId::new(self.collection_id)?,
            self.name,
            metadata,
        ))
    }
}

// Each table has a statically checked projection. Large JSON fields never enter this query.
macro_rules! marker_page {
    ($runtime:expr, $query:expr, $table:ident, $predicate:expr) => {{
        use crate::schema::$table::dsl as h;
        let query = $query;
        let options = query.options();
        let fields = vec![
            CursorSqlField {
                column: "valid_from",
                sql_type: CursorSqlType::DateTimeUtc,
                nullable: false,
            },
            CursorSqlField {
                column: "history_id",
                sql_type: CursorSqlType::BigInt,
                nullable: false,
            },
        ];
        $runtime
            .with_read_only_snapshot(async |connection| {
                let base_query = || {
                    let mut records = h::$table.filter($predicate).into_boxed();
                    if let Some(ids) = visible_collection_ids(query.scope()) {
                        records = records.filter(h::collection_id.eq_any(ids));
                    }
                    if let Some(from) = query.time_window().from() {
                        let floor = timestamp_floor(from);
                        records = if from == floor {
                            records.filter(h::valid_from.ge(floor))
                        } else {
                            records.filter(h::valid_from.gt(floor))
                        };
                    }
                    if let Some(until) = query.time_window().until() {
                        let floor = timestamp_floor(until);
                        records = if until == floor {
                            records.filter(h::valid_from.lt(floor))
                        } else {
                            records.filter(h::valid_from.le(floor))
                        };
                    }
                    records = match query.selected() {
                        StorageHistorySelection::All => records,
                        StorageHistorySelection::Entry(id) => {
                            records.filter(h::history_id.eq(id.id()))
                        }
                        StorageHistorySelection::AtOrBefore(at) => {
                            records.filter(h::valid_from.le(timestamp_floor(at)))
                        }
                        StorageHistorySelection::After(at) => {
                            records.filter(h::valid_from.gt(timestamp_floor(at)))
                        }
                        StorageHistorySelection::BeforeEntry(anchor)
                            if anchor.at() != timestamp_floor(anchor.at()) =>
                        {
                            records.filter(h::valid_from.le(timestamp_floor(anchor.at())))
                        }
                        StorageHistorySelection::AfterEntry(anchor)
                            if anchor.at() != timestamp_floor(anchor.at()) =>
                        {
                            records.filter(h::valid_from.gt(timestamp_floor(anchor.at())))
                        }
                        StorageHistorySelection::BeforeEntry(anchor) => records.filter(
                            sql::<Bool>("(valid_from, history_id) < (")
                                .bind::<Timestamptz, _>(anchor.at())
                                .sql(", ")
                                .bind::<BigInt, _>(anchor.history_id().id())
                                .sql(")"),
                        ),
                        StorageHistorySelection::AfterEntry(anchor) => records.filter(
                            sql::<Bool>("(valid_from, history_id) > (")
                                .bind::<Timestamptz, _>(anchor.at())
                                .sql(", ")
                                .bind::<BigInt, _>(anchor.history_id().id())
                                .sql(")"),
                        ),
                    };
                    records
                };
                let total = if options.include_total() {
                    Some(base_query().count().get_result::<i64>(connection).await?)
                } else {
                    None
                };
                let mut records = base_query().select((
                    h::collection_id,
                    h::name,
                    h::op,
                    h::valid_from,
                    h::valid_to,
                    h::actor_id,
                    h::history_id,
                    h::actor_kind,
                    h::initiator_user_id,
                    h::task_id,
                    h::revision,
                ));
                crate::apply_query_options_with_fields!(
                    records,
                    options,
                    fields,
                    crate::cursor::CursorTieBreaker::new(
                        FilterField::HistoryId,
                        options.sort()[0].descending,
                        CursorSqlField {
                            column: "history_id",
                            sql_type: CursorSqlType::BigInt,
                            nullable: false
                        }
                    )
                );
                let rows = records
                    .load::<MarkerRow>(connection)
                    .await?
                    .into_iter()
                    .map(|row| row.into_marker(query.resource()))
                    .collect::<Result<Vec<_>, _>>()?;
                crate::persisted_page(rows, total)
            })
            .await
    }};
}

pub(crate) async fn list_history_markers(
    runtime: &PostgresRuntime,
    query: StorageHistoryNavigationQuery,
) -> Result<StoragePage<StorageHistoryMarker>, PostgresStorageError> {
    match query.resource() {
        StorageHistoryResource::Class(id) => {
            marker_page!(runtime, &query, hubuumclass_history, h::id.eq(id.id()))
        }
        StorageHistoryResource::Object {
            class_id,
            object_id,
        } => marker_page!(
            runtime,
            &query,
            hubuumobject_history,
            h::id
                .eq(object_id.id())
                .and(h::hubuum_class_id.eq(class_id.id()))
        ),
    }
}

pub(crate) async fn get_history_entry(
    runtime: &PostgresRuntime,
    query: StorageHistoryEntryQuery,
) -> Result<Option<StorageHistoryEntry>, PostgresStorageError> {
    runtime
        .with_connection(async |connection| match query.resource() {
            StorageHistoryResource::Class(id) => {
                use crate::schema::hubuumclass_history::dsl as h;
                h::hubuumclass_history
                    .filter(h::id.eq(id.id()))
                    .filter(h::history_id.eq(query.history_id().id()))
                    .first::<ClassHistoryRow>(connection)
                    .await
                    .optional()?
                    .map(StorageClassHistoryRecord::try_from)
                    .transpose()
                    .map(|row| row.map(StorageHistoryEntry::Class))
            }
            StorageHistoryResource::Object {
                class_id,
                object_id,
            } => {
                use crate::schema::hubuumobject_history::dsl as h;
                h::hubuumobject_history
                    .filter(h::id.eq(object_id.id()))
                    .filter(h::hubuum_class_id.eq(class_id.id()))
                    .filter(h::history_id.eq(query.history_id().id()))
                    .first::<ObjectHistoryRow>(connection)
                    .await
                    .optional()?
                    .map(StorageObjectHistoryRecord::try_from)
                    .transpose()
                    .map(|row| row.map(StorageHistoryEntry::Object))
            }
        })
        .await
}
