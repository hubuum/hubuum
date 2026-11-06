use chrono::{DateTime, Utc};
use diesel::{ExpressionMethods, OptionalExtension, QueryDsl, Queryable};
use diesel_async::RunQueryDsl;
use hubuum_domain::{ClassId, HistoryRecordId};
use hubuum_storage_core::{
    StorageComputedFieldHistory, StorageComputedFieldHistoryQuery, StorageHistoricalComputedFields,
};
use serde_json::Value;

use super::history_as_of_visible;
use crate::error::validate_persisted;
use crate::schema::computed_field_history as h;
use crate::{PostgresRuntime, PostgresStorageError};

#[derive(Queryable)]
struct HistoryRow {
    history_id: i64,
    class_id: i32,
    valid_from: DateTime<Utc>,
    valid_to: Option<DateTime<Utc>>,
    evaluator_version: i16,
    definitions: Value,
    logical_identity: Option<Value>,
    visible: bool,
}

impl HistoryRow {
    fn project(self) -> Result<StorageComputedFieldHistory, PostgresStorageError> {
        validate_persisted(
            "computed history",
            StorageComputedFieldHistory::builder(
                validate_persisted("computed history class", ClassId::new(self.class_id))?,
                validate_persisted("computed history ID", HistoryRecordId::new(self.history_id))?,
                self.valid_from,
            )
            .valid_until(self.valid_to)
            .evaluator_version(self.evaluator_version)
            .definitions(validate_persisted(
                "computed history definitions",
                serde_json::from_value(self.definitions),
            )?)
            .logical_identity(validate_persisted(
                "computed history identity",
                self.logical_identity
                    .map(serde_json::from_value)
                    .transpose(),
            )?)
            .build(),
        )
    }
}

macro_rules! computed_history_query {
    ($class:expr, $at:expr) => {
        h::table
            .filter(h::class_id.eq($class))
            .filter(h::valid_from.le($at))
            .order(h::valid_from.desc())
            .limit(1)
            .select((
                h::history_id,
                h::class_id,
                h::valid_from,
                h::valid_to,
                h::evaluator_version,
                h::definitions,
                h::logical_identity,
                history_as_of_visible(
                    "computed_definitions",
                    h::logical_identity,
                    $at,
                    h::valid_from,
                ),
            ))
    };
}

pub(crate) async fn get_computed_field_history(
    runtime: &PostgresRuntime,
    query: StorageComputedFieldHistoryQuery,
) -> Result<Option<StorageHistoricalComputedFields>, PostgresStorageError> {
    runtime
        .with_connection(async |connection| {
            // First bound by class and start time using the unique index. Check the
            // selected interval afterward so gaps cannot force an unbounded scan.
            let row = computed_history_query!(query.class_id().id(), query.at())
                .get_result::<HistoryRow>(connection)
                .await
                .optional()?;
            let Some(row) = row else {
                return Ok(None);
            };
            let visible = row.visible;
            let record = row.project()?;
            if !record.is_effective_at(query.at()) {
                return Ok(None);
            }
            if !visible {
                return Err(PostgresStorageError::not_found(
                    "Historical computed version is expired",
                ));
            }
            record.select(query).map(Some).map_err(|error| {
                PostgresStorageError::invalid_persisted_value("computed history selection", error)
            })
        })
        .await
}

#[cfg(test)]
mod tests;
