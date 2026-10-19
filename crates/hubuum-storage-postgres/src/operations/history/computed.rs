use diesel::sql_types::{Integer, Jsonb, Timestamptz};
use diesel::{OptionalExtension, QueryableByName, sql_query};
use diesel_async::RunQueryDsl;
use hubuum_storage_core::{
    StorageComputedFieldHistory, StorageComputedFieldHistoryQuery, StorageHistoricalComputedFields,
};
use serde_json::Value;

use crate::{PostgresRuntime, PostgresStorageError};

#[derive(QueryableByName)]
struct HistoryRow {
    #[diesel(sql_type = Jsonb)]
    snapshot: Value,
}

pub(crate) async fn get_computed_field_history(
    runtime: &PostgresRuntime,
    query: StorageComputedFieldHistoryQuery,
) -> Result<Option<StorageHistoricalComputedFields>, PostgresStorageError> {
    runtime
        .with_connection(async |connection| {
            // First bound by class and start time using the unique index. Check the
            // selected interval afterward so gaps cannot force an unbounded scan.
            let row = sql_query(
                "SELECT (to_jsonb(h) - 'history_id') || jsonb_build_object(\
            'history_entry_id', h.history_id) AS snapshot FROM computed_field_history h \
            WHERE class_id = $1 AND valid_from <= $2 ORDER BY valid_from DESC LIMIT 1",
            )
            .bind::<Integer, _>(query.class_id().id())
            .bind::<Timestamptz, _>(query.at())
            .get_result::<HistoryRow>(connection)
            .await
            .optional()?;
            let Some(row) = row else {
                return Ok(None);
            };
            let record: StorageComputedFieldHistory = serde_json::from_value(row.snapshot)
                .map_err(|error| {
                    PostgresStorageError::invalid_persisted_value("computed history", error)
                })?;
            if !record.is_effective_at(query.at()) {
                return Ok(None);
            }
            record.select(query).map(Some).map_err(|error| {
                PostgresStorageError::invalid_persisted_value("computed history selection", error)
            })
        })
        .await
}

#[cfg(test)]
mod tests;
