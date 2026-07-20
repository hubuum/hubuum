use diesel::sql_types::{BigInt, Jsonb};
use diesel_async::RunQueryDsl;
use futures::TryStreamExt;

use super::ObjectGroupPaging;
use super::bounded_json::{MAX_OBJECT_GROUP_ACCUMULATOR_BYTES, ObjectGroupJsonBound};
use super::sql::{
    ObjectGroupBindValue, ObjectGroupSqlSpec, append_page_options, bind_object_group_query,
};
use crate::db::{DbConnection, DbPool, with_connection};
use crate::errors::ApiError;
use crate::models::object_group::{ObjectGroupPage, ObjectGroupRow};
use crate::pagination::SKIPPED_TOTAL_COUNT;

const OBJECT_GROUP_ACCUMULATOR_COMPACT_BYTES: usize = 1024 * 1024;

#[derive(diesel::QueryableByName, serde::Serialize)]
pub(super) struct ObjectGroupDatabaseRow {
    #[diesel(sql_type = Jsonb)]
    pub(super) dimensions: serde_json::Value,
    #[diesel(sql_type = Jsonb)]
    pub(super) sort_key: serde_json::Value,
    #[diesel(sql_type = BigInt)]
    pub(super) object_count: i64,
}

pub(super) struct GroupRows {
    rows: Vec<ObjectGroupDatabaseRow>,
    serialized_bytes: usize,
}

impl Default for GroupRows {
    fn default() -> Self {
        Self {
            rows: Vec::new(),
            serialized_bytes: 2,
        }
    }
}

impl GroupRows {
    pub(super) fn push_bounded(&mut self, row: ObjectGroupDatabaseRow) -> Result<(), ApiError> {
        let row_bytes = serialized_row_len(&row)?;
        self.push_measured(row, row_bytes)
    }

    fn push_measured(
        &mut self,
        row: ObjectGroupDatabaseRow,
        row_bytes: usize,
    ) -> Result<(), ApiError> {
        let serialized_bytes = self
            .serialized_bytes
            .checked_add(row_bytes.saturating_add(1))
            .ok_or_else(accumulator_too_large)?;
        if serialized_bytes > MAX_OBJECT_GROUP_ACCUMULATOR_BYTES {
            return Err(accumulator_too_large());
        }
        self.rows.push(row);
        self.serialized_bytes = serialized_bytes;
        Ok(())
    }

    fn into_rows(self) -> Vec<ObjectGroupDatabaseRow> {
        self.rows
    }

    fn len(&self) -> usize {
        self.rows.len()
    }

    pub(super) fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
}

#[derive(Default)]
pub(super) struct ExternalGroupAccumulator {
    compacted: GroupRows,
    pending: GroupRows,
}

impl ExternalGroupAccumulator {
    pub(super) async fn add_rows(
        &mut self,
        pool: &DbPool,
        rows: GroupRows,
    ) -> Result<(), ApiError> {
        for row in rows.into_rows() {
            let row_bytes = serialized_row_len(&row)?;
            if self.total_bytes().saturating_add(row_bytes) > MAX_OBJECT_GROUP_ACCUMULATOR_BYTES {
                self.compact_pending(pool).await?;
            }
            if self.compacted.serialized_bytes.saturating_add(row_bytes)
                > MAX_OBJECT_GROUP_ACCUMULATOR_BYTES
            {
                let mut incoming = GroupRows::default();
                incoming.push_measured(row, row_bytes)?;
                self.compacted =
                    compact_group_rows(pool, std::mem::take(&mut self.compacted), incoming).await?;
                continue;
            }
            self.pending.push_measured(row, row_bytes)?;
            if self.pending.serialized_bytes >= OBJECT_GROUP_ACCUMULATOR_COMPACT_BYTES {
                self.compact_pending(pool).await?;
            }
        }
        Ok(())
    }

    pub(super) async fn finish(mut self, pool: &DbPool) -> Result<GroupRows, ApiError> {
        self.compact_pending(pool).await?;
        Ok(self.compacted)
    }

    fn total_bytes(&self) -> usize {
        self.compacted
            .serialized_bytes
            .saturating_add(self.pending.serialized_bytes)
    }

    async fn compact_pending(&mut self, pool: &DbPool) -> Result<(), ApiError> {
        if self.pending.is_empty() {
            return Ok(());
        }
        self.compacted = compact_group_rows(
            pool,
            std::mem::take(&mut self.compacted),
            std::mem::take(&mut self.pending),
        )
        .await?;
        Ok(())
    }
}

pub(super) async fn create_group_accumulator(
    connection: &mut DbConnection,
) -> Result<(), ApiError> {
    diesel::sql_query(
        "CREATE TEMP TABLE object_group_accumulator (
            sort_key jsonb NOT NULL,
            dimensions jsonb NOT NULL,
            object_count bigint NOT NULL CHECK (object_count > 0)
        ) ON COMMIT DROP",
    )
    .execute(connection)
    .await?;
    diesel::sql_query(
        "CREATE UNIQUE INDEX object_group_accumulator_sort_key_idx
            ON object_group_accumulator (sort_key)",
    )
    .execute(connection)
    .await?;
    Ok(())
}

pub(super) async fn merge_group_rows(
    connection: &mut DbConnection,
    groups: GroupRows,
) -> Result<(), ApiError> {
    if groups.is_empty() {
        return Ok(());
    }
    let merge = ObjectGroupSqlSpec {
        sql: "INSERT INTO object_group_accumulator (
    dimensions,
    sort_key,
    object_count
)
SELECT incoming.dimensions, incoming.sort_key, incoming.object_count
FROM jsonb_to_recordset(?::jsonb) AS incoming(
    dimensions jsonb,
    sort_key jsonb,
    object_count bigint
)
ON CONFLICT (sort_key) DO UPDATE
SET object_count = object_group_accumulator.object_count + EXCLUDED.object_count"
            .to_string(),
        binds: vec![ObjectGroupBindValue::Json(group_rows_payload(groups))],
    };
    bind_object_group_query!(merge).execute(connection).await?;
    Ok(())
}

async fn compact_group_rows(
    pool: &DbPool,
    compacted: GroupRows,
    pending: GroupRows,
) -> Result<GroupRows, ApiError> {
    with_connection(pool, async |connection| -> Result<GroupRows, ApiError> {
        let query = ObjectGroupSqlSpec {
            sql: "WITH incoming AS (
    SELECT *
    FROM jsonb_to_recordset(?::jsonb) AS rows(
        dimensions jsonb,
        sort_key jsonb,
        object_count bigint
    )
    UNION ALL
    SELECT *
    FROM jsonb_to_recordset(?::jsonb) AS rows(
        dimensions jsonb,
        sort_key jsonb,
        object_count bigint
    )
)
SELECT
    (array_agg(dimensions))[1] AS dimensions,
    sort_key,
    SUM(object_count)::bigint AS object_count
FROM incoming
GROUP BY sort_key"
                .to_string(),
            binds: vec![
                ObjectGroupBindValue::Json(group_rows_payload(compacted)),
                ObjectGroupBindValue::Json(group_rows_payload(pending)),
            ],
        };
        let stream = bind_object_group_query!(query)
            .load_stream::<ObjectGroupDatabaseRow>(connection)
            .await?;
        futures::pin_mut!(stream);
        let mut groups = GroupRows::default();
        while let Some(row) = stream.try_next().await? {
            groups.push_bounded(row)?;
        }
        Ok(groups)
    })
    .await
}

pub(super) async fn page_external_groups(
    pool: &DbPool,
    groups: GroupRows,
    paging: &ObjectGroupPaging<'_>,
) -> Result<ObjectGroupPage, ApiError> {
    let total_count = if paging.query_options.include_total {
        i64::try_from(groups.len()).map_err(|_| accumulator_too_large())?
    } else {
        SKIPPED_TOTAL_COUNT
    };
    let database_rows = with_connection(pool, async |connection| {
        page_group_rows(connection, groups, paging).await
    })
    .await?;
    finish_group_page(database_rows, total_count, paging)
}

async fn page_group_rows(
    connection: &mut DbConnection,
    groups: GroupRows,
    paging: &ObjectGroupPaging<'_>,
) -> Result<Vec<ObjectGroupDatabaseRow>, ApiError> {
    let mut page_spec = ObjectGroupSqlSpec {
        sql: "WITH object_group_accumulator AS (
    SELECT *
    FROM jsonb_to_recordset(?::jsonb) AS rows(
        dimensions jsonb,
        sort_key jsonb,
        object_count bigint
    )
)
SELECT dimensions, sort_key, object_count
FROM object_group_accumulator"
            .to_string(),
        binds: vec![ObjectGroupBindValue::Json(group_rows_payload(groups))],
    };
    append_page_options(
        &mut page_spec,
        paging.spec.sort(),
        paging.decoded_cursor.as_ref(),
        paging.effective_limit,
    )?;
    Ok(bind_object_group_query!(page_spec)
        .load::<ObjectGroupDatabaseRow>(connection)
        .await?)
}

pub(super) async fn page_accumulated_groups(
    connection: &mut DbConnection,
    paging: &ObjectGroupPaging<'_>,
) -> Result<ObjectGroupPage, ApiError> {
    let total_count = if paging.query_options.include_total {
        diesel::sql_query("SELECT COUNT(*) AS count FROM object_group_accumulator")
            .get_result::<ObjectGroupCountRow>(connection)
            .await?
            .count
    } else {
        SKIPPED_TOTAL_COUNT
    };
    let mut page_spec = ObjectGroupSqlSpec {
        sql: "SELECT dimensions, sort_key, object_count
FROM object_group_accumulator"
            .to_string(),
        binds: Vec::new(),
    };
    append_page_options(
        &mut page_spec,
        paging.spec.sort(),
        paging.decoded_cursor.as_ref(),
        paging.effective_limit,
    )?;
    let database_rows = bind_object_group_query!(page_spec)
        .load::<ObjectGroupDatabaseRow>(connection)
        .await?;
    finish_group_page(database_rows, total_count, paging)
}

pub(super) fn finish_group_page(
    database_rows: Vec<ObjectGroupDatabaseRow>,
    total_count: i64,
    paging: &ObjectGroupPaging<'_>,
) -> Result<ObjectGroupPage, ApiError> {
    let mut rows = database_rows
        .into_iter()
        .map(|row| ObjectGroupRow::from_database(row.dimensions, row.object_count, row.sort_key))
        .collect::<Result<Vec<_>, _>>()?;
    let has_more = rows.len() > paging.effective_limit;
    if has_more {
        rows.truncate(paging.effective_limit);
    }
    let next_cursor = if has_more {
        rows.last()
            .map(|row| paging.spec.encode_cursor(row, paging.cursor_budget))
            .transpose()?
    } else {
        None
    };
    Ok(ObjectGroupPage::new(rows, total_count, next_cursor))
}

fn group_rows_payload(groups: GroupRows) -> serde_json::Value {
    serde_json::Value::Array(
        groups
            .into_rows()
            .into_iter()
            .map(|row| {
                serde_json::json!({
                    "dimensions": row.dimensions,
                    "sort_key": row.sort_key,
                    "object_count": row.object_count,
                })
            })
            .collect(),
    )
}

fn serialized_row_len(row: &ObjectGroupDatabaseRow) -> Result<usize, ApiError> {
    ObjectGroupJsonBound::Accumulator.measure(row)
}

fn accumulator_too_large() -> ApiError {
    ObjectGroupJsonBound::Accumulator.overflow_error()
}

#[derive(diesel::QueryableByName)]
struct ObjectGroupCountRow {
    #[diesel(sql_type = BigInt)]
    count: i64,
}
