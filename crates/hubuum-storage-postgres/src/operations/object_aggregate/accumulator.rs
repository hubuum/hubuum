use diesel::sql_types::{BigInt, Jsonb};
use diesel_async::RunQueryDsl;
use futures_util::TryStreamExt;

use super::ObjectAggregatePaging;
use super::bounded_json::{MAX_OBJECT_AGGREGATE_ACCUMULATOR_BYTES, ObjectAggregateJsonBound};
use super::sql::{
    ObjectAggregateBindValue, ObjectAggregateSqlSpec, append_page_options,
    bind_object_aggregate_query, grouped_measure_state_sql, measure_response_sql,
    merged_measure_state_sql,
};
use hubuum_storage_core::{
    StorageObjectAggregateMeasureState, StorageObjectAggregateMeasureValue,
    StorageObjectAggregatePage, StorageObjectAggregateRow, StorageObjectAggregateSpec,
};

use crate::{PostgresConnection, PostgresStorageError};

const OBJECT_AGGREGATE_ACCUMULATOR_COMPACT_BYTES: usize = 1024 * 1024;

#[derive(diesel::QueryableByName, serde::Serialize)]
pub(super) struct ObjectAggregateDatabaseRow {
    #[diesel(sql_type = Jsonb)]
    pub(super) sort_key: serde_json::Value,
    #[diesel(sql_type = Jsonb)]
    pub(super) measures: serde_json::Value,
    #[diesel(sql_type = BigInt)]
    pub(super) object_count: i64,
}

#[derive(diesel::QueryableByName, serde::Serialize)]
pub(super) struct PartialObjectAggregateRow {
    #[diesel(sql_type = Jsonb)]
    pub(super) sort_key: serde_json::Value,
    #[diesel(sql_type = Jsonb)]
    pub(super) measure_state: serde_json::Value,
    #[diesel(sql_type = BigInt)]
    pub(super) object_count: i64,
}

pub(super) struct AggregateRows {
    rows: Vec<PartialObjectAggregateRow>,
    serialized_bytes: usize,
}

impl Default for AggregateRows {
    fn default() -> Self {
        Self {
            rows: Vec::new(),
            serialized_bytes: 2,
        }
    }
}

impl AggregateRows {
    pub(super) fn push_bounded(
        &mut self,
        row: PartialObjectAggregateRow,
    ) -> Result<(), PostgresStorageError> {
        let row_bytes = serialized_row_len(&row)?;
        self.push_measured(row, row_bytes)
    }

    fn push_measured(
        &mut self,
        row: PartialObjectAggregateRow,
        row_bytes: usize,
    ) -> Result<(), PostgresStorageError> {
        let serialized_bytes = self
            .serialized_bytes
            .checked_add(row_bytes.saturating_add(1))
            .ok_or_else(accumulator_too_large)?;
        if serialized_bytes > MAX_OBJECT_AGGREGATE_ACCUMULATOR_BYTES {
            return Err(accumulator_too_large());
        }
        self.rows.push(row);
        self.serialized_bytes = serialized_bytes;
        Ok(())
    }

    pub(super) fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    fn into_rows(self) -> Vec<PartialObjectAggregateRow> {
        self.rows
    }

    fn len(&self) -> usize {
        self.rows.len()
    }
}

#[derive(Default)]
pub(super) struct ExternalAggregateAccumulator {
    compacted: AggregateRows,
    pending: AggregateRows,
}

impl ExternalAggregateAccumulator {
    pub(super) async fn add_rows(
        &mut self,
        connection: &mut PostgresConnection,
        rows: AggregateRows,
        spec: &StorageObjectAggregateSpec,
    ) -> Result<(), PostgresStorageError> {
        for row in rows.into_rows() {
            let row_bytes = serialized_row_len(&row)?;
            if self.total_bytes().saturating_add(row_bytes) > MAX_OBJECT_AGGREGATE_ACCUMULATOR_BYTES
            {
                self.compact_pending(connection, spec).await?;
            }
            if self.compacted.serialized_bytes.saturating_add(row_bytes)
                > MAX_OBJECT_AGGREGATE_ACCUMULATOR_BYTES
            {
                let mut incoming = AggregateRows::default();
                incoming.push_measured(row, row_bytes)?;
                self.compacted = compact_aggregate_rows(
                    connection,
                    std::mem::take(&mut self.compacted),
                    incoming,
                    spec,
                )
                .await?;
                continue;
            }
            self.pending.push_measured(row, row_bytes)?;
            if self.pending.serialized_bytes >= OBJECT_AGGREGATE_ACCUMULATOR_COMPACT_BYTES {
                self.compact_pending(connection, spec).await?;
            }
        }
        Ok(())
    }

    pub(super) async fn finish(
        mut self,
        connection: &mut PostgresConnection,
        spec: &StorageObjectAggregateSpec,
    ) -> Result<AggregateRows, PostgresStorageError> {
        self.compact_pending(connection, spec).await?;
        Ok(self.compacted)
    }

    fn total_bytes(&self) -> usize {
        self.compacted
            .serialized_bytes
            .saturating_add(self.pending.serialized_bytes)
    }

    async fn compact_pending(
        &mut self,
        connection: &mut PostgresConnection,
        spec: &StorageObjectAggregateSpec,
    ) -> Result<(), PostgresStorageError> {
        if self.pending.is_empty() {
            return Ok(());
        }
        self.compacted = compact_aggregate_rows(
            connection,
            std::mem::take(&mut self.compacted),
            std::mem::take(&mut self.pending),
            spec,
        )
        .await?;
        Ok(())
    }
}

pub(super) async fn create_aggregate_accumulator(
    connection: &mut PostgresConnection,
) -> Result<(), PostgresStorageError> {
    diesel::sql_query(
        "CREATE TEMP TABLE object_aggregate_accumulator (
            sort_key jsonb NOT NULL,
            measure_state jsonb NOT NULL,
            object_count bigint NOT NULL CHECK (object_count > 0)
        ) ON COMMIT DROP",
    )
    .execute(connection)
    .await?;
    diesel::sql_query(
        "CREATE UNIQUE INDEX object_aggregate_accumulator_sort_key_idx
            ON object_aggregate_accumulator (sort_key)",
    )
    .execute(connection)
    .await?;
    Ok(())
}

pub(super) async fn merge_aggregate_rows(
    connection: &mut PostgresConnection,
    groups: AggregateRows,
    spec: &StorageObjectAggregateSpec,
) -> Result<(), PostgresStorageError> {
    if groups.is_empty() {
        return Ok(());
    }
    let merge = ObjectAggregateSqlSpec {
        sql: "INSERT INTO object_aggregate_accumulator (
    sort_key,
    measure_state,
    object_count
)
SELECT incoming.sort_key, incoming.measure_state, incoming.object_count
FROM jsonb_to_recordset(?::jsonb) AS incoming(
    sort_key jsonb,
    measure_state jsonb,
    object_count bigint
)
ON CONFLICT (sort_key) DO UPDATE
SET object_count = object_aggregate_accumulator.object_count + EXCLUDED.object_count,
    measure_state = {measure_state}"
            .replace(
                "{measure_state}",
                &merged_measure_state_sql(
                    spec,
                    "object_aggregate_accumulator.measure_state",
                    "EXCLUDED.measure_state",
                ),
            ),
        binds: vec![ObjectAggregateBindValue::Json(aggregate_rows_payload(
            groups,
        ))],
    };
    bind_object_aggregate_query!(merge)
        .execute(connection)
        .await?;
    Ok(())
}

async fn compact_aggregate_rows(
    connection: &mut PostgresConnection,
    compacted: AggregateRows,
    pending: AggregateRows,
    spec: &StorageObjectAggregateSpec,
) -> Result<AggregateRows, PostgresStorageError> {
    let measure_state = grouped_measure_state_sql(spec, "measure_state");
    let query = ObjectAggregateSqlSpec {
        sql: format!(
            "WITH incoming AS (
    SELECT *
    FROM jsonb_to_recordset(?::jsonb) AS rows(
        sort_key jsonb,
        measure_state jsonb,
        object_count bigint
    )
    UNION ALL
    SELECT *
    FROM jsonb_to_recordset(?::jsonb) AS rows(
        sort_key jsonb,
        measure_state jsonb,
        object_count bigint
    )
)
SELECT
    sort_key,
    {measure_state} AS measure_state,
    SUM(object_count)::bigint AS object_count
FROM incoming
GROUP BY sort_key"
        ),
        binds: vec![
            ObjectAggregateBindValue::Json(aggregate_rows_payload(compacted)),
            ObjectAggregateBindValue::Json(aggregate_rows_payload(pending)),
        ],
    };
    let stream = bind_object_aggregate_query!(query)
        .load_stream::<PartialObjectAggregateRow>(connection)
        .await?;
    futures_util::pin_mut!(stream);
    let mut groups = AggregateRows::default();
    while let Some(row) = stream.try_next().await? {
        groups.push_bounded(row)?;
    }
    Ok(groups)
}

pub(super) async fn page_external_aggregates(
    connection: &mut PostgresConnection,
    groups: AggregateRows,
    paging: &ObjectAggregatePaging,
) -> Result<StorageObjectAggregatePage, PostgresStorageError> {
    let total = if paging.query_options.include_total() {
        Some(i64::try_from(groups.len()).map_err(|_| accumulator_too_large())?)
    } else {
        None
    };
    let database_rows = page_aggregate_rows(connection, groups, paging).await?;
    finish_aggregate_page(database_rows, total, paging)
}

async fn page_aggregate_rows(
    connection: &mut PostgresConnection,
    groups: AggregateRows,
    paging: &ObjectAggregatePaging,
) -> Result<Vec<ObjectAggregateDatabaseRow>, PostgresStorageError> {
    let measures = measure_response_sql(&paging.spec, "measure_state", "object_count");
    let mut page_spec = ObjectAggregateSqlSpec {
        sql: format!(
            "WITH object_aggregate_accumulator AS (
    SELECT *
    FROM jsonb_to_recordset(?::jsonb) AS rows(
        sort_key jsonb,
        measure_state jsonb,
        object_count bigint
    )
)
SELECT sort_key, {measures} AS measures, object_count
FROM object_aggregate_accumulator"
        ),
        binds: vec![ObjectAggregateBindValue::Json(aggregate_rows_payload(
            groups,
        ))],
    };
    append_page_options(
        &mut page_spec,
        paging.spec.sort(),
        paging.decoded_cursor.as_ref(),
        paging.effective_limit,
    )?;
    Ok(bind_object_aggregate_query!(page_spec)
        .load::<ObjectAggregateDatabaseRow>(connection)
        .await?)
}

pub(super) async fn page_accumulated_aggregates(
    connection: &mut PostgresConnection,
    paging: &ObjectAggregatePaging,
) -> Result<StorageObjectAggregatePage, PostgresStorageError> {
    let total = if paging.query_options.include_total() {
        Some(
            diesel::sql_query("SELECT COUNT(*) AS count FROM object_aggregate_accumulator")
                .get_result::<ObjectAggregateCountRow>(connection)
                .await?
                .count,
        )
    } else {
        None
    };
    let measures = measure_response_sql(&paging.spec, "measure_state", "object_count");
    let mut page_spec = ObjectAggregateSqlSpec {
        sql: format!(
            "SELECT sort_key, {measures} AS measures, object_count
FROM object_aggregate_accumulator"
        ),
        binds: Vec::new(),
    };
    append_page_options(
        &mut page_spec,
        paging.spec.sort(),
        paging.decoded_cursor.as_ref(),
        paging.effective_limit,
    )?;
    let database_rows = bind_object_aggregate_query!(page_spec)
        .load::<ObjectAggregateDatabaseRow>(connection)
        .await?;
    finish_aggregate_page(database_rows, total, paging)
}

pub(super) fn finish_aggregate_page(
    database_rows: Vec<ObjectAggregateDatabaseRow>,
    total: Option<i64>,
    paging: &ObjectAggregatePaging,
) -> Result<StorageObjectAggregatePage, PostgresStorageError> {
    let mut rows = database_rows
        .into_iter()
        .map(|row| {
            storage_row_from_database(&paging.spec, row.measures, row.object_count, row.sort_key)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let has_more = rows.len() > paging.effective_limit;
    if has_more {
        rows.truncate(paging.effective_limit);
    }
    let next_cursor = if has_more {
        rows.last()
            .map(|row| {
                paging
                    .spec
                    .encode_cursor(row, paging.cursor_max_encoded_bytes)
            })
            .transpose()?
    } else {
        None
    };
    crate::validate_persisted(
        "object aggregate page",
        StorageObjectAggregatePage::try_new(rows, total, next_cursor),
    )
}

#[derive(serde::Deserialize)]
struct DatabaseMeasureValue {
    state: StorageObjectAggregateMeasureState,
    value_count: i64,
    skipped_count: i64,
    value: Option<serde_json::Value>,
}

fn storage_row_from_database(
    spec: &StorageObjectAggregateSpec,
    measures: serde_json::Value,
    object_count: i64,
    sort_key: serde_json::Value,
) -> Result<StorageObjectAggregateRow, PostgresStorageError> {
    let measures =
        serde_json::from_value::<Vec<DatabaseMeasureValue>>(measures).map_err(|error| {
            PostgresStorageError::database(format!(
                "Database returned invalid object aggregate measures: {error}"
            ))
        })?;
    if measures.len() != spec.measures().len() {
        return Err(PostgresStorageError::database(
            "Database returned an object aggregate row with the wrong measure count",
        ));
    }
    if object_count <= 0 || !sort_key.is_array() {
        return Err(PostgresStorageError::database(
            "Database returned invalid object aggregate ordering data",
        ));
    }
    if measures.iter().any(|measure| {
        measure.value_count < 0
            || measure.skipped_count < 0
            || measure.value_count + measure.skipped_count != object_count
            || match measure.state {
                StorageObjectAggregateMeasureState::Value => {
                    measure.value_count == 0
                        || !measure
                            .value
                            .as_ref()
                            .is_some_and(serde_json::Value::is_number)
                }
                StorageObjectAggregateMeasureState::Empty => {
                    measure.value_count != 0 || measure.value.is_some()
                }
            }
    }) {
        return Err(PostgresStorageError::database(
            "Database returned invalid object aggregate measure data",
        ));
    }
    let measures = measures
        .into_iter()
        .map(|measure| {
            crate::validate_persisted(
                "object aggregate measure",
                StorageObjectAggregateMeasureValue::try_new(
                    measure.state,
                    measure.value_count,
                    measure.skipped_count,
                    measure.value,
                ),
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    crate::validate_persisted(
        "object aggregate row",
        StorageObjectAggregateRow::try_new(measures, object_count, sort_key),
    )
}

fn aggregate_rows_payload(groups: AggregateRows) -> serde_json::Value {
    serde_json::Value::Array(
        groups
            .into_rows()
            .into_iter()
            .map(|row| {
                serde_json::json!({
                    "sort_key": row.sort_key,
                    "measure_state": row.measure_state,
                    "object_count": row.object_count,
                })
            })
            .collect(),
    )
}

fn serialized_row_len(row: &PartialObjectAggregateRow) -> Result<usize, PostgresStorageError> {
    ObjectAggregateJsonBound::Accumulator.measure(row)
}

fn accumulator_too_large() -> PostgresStorageError {
    ObjectAggregateJsonBound::Accumulator.overflow_error()
}

#[derive(diesel::QueryableByName)]
struct ObjectAggregateCountRow {
    #[diesel(sql_type = BigInt)]
    count: i64,
}
