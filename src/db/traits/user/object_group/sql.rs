use std::io::{self, Write};

use diesel::dsl::{count, sql};
use diesel::prelude::*;
use diesel::sql_types::{BigInt, Bool, Jsonb};
use diesel_async::RunQueryDsl;
use futures::TryStreamExt;

use super::ObjectGroupExecution;
use super::accumulator::{GroupRows, ObjectGroupDatabaseRow, finish_group_page};
use super::computed::{ComputedGroupDefinitions, computed_group_payload};
use crate::db::traits::search::JsonPredicateExt;
use crate::db::{DbConnection, with_connection};
use crate::errors::ApiError;
use crate::models::object::HubuumObject;
use crate::models::object_group::{
    DecodedObjectGroupCursor, ObjectGroupDimension, ObjectGroupScalarField, ObjectGroupSort,
    ObjectGroupSpec,
};
use crate::models::search::{FilterField, QueryOptions, QueryParamsExt};
use crate::pagination::{Page, SKIPPED_TOTAL_COUNT, finalize_page, finalize_partial_page};
use crate::utilities::extensions::CustomStringExtensions;

#[cfg(not(feature = "integration-test-support"))]
const MAX_OBJECT_GROUP_CANDIDATE_BATCH_BYTES: usize = 8 * 1024 * 1024;
#[cfg(feature = "integration-test-support")]
const MAX_OBJECT_GROUP_CANDIDATE_BATCH_BYTES: usize = 4 * 1024;

#[derive(Debug, Clone)]
pub(super) enum ObjectGroupBindValue {
    Json(serde_json::Value),
    BigInt(i64),
}

#[derive(Debug, Clone)]
pub(super) struct ObjectGroupSqlSpec {
    pub(super) sql: String,
    pub(super) binds: Vec<ObjectGroupBindValue>,
}

pub(super) struct ObjectGroupCandidateBatch {
    items: Vec<HubuumObject>,
    stopped_by_size: bool,
}

impl ObjectGroupCandidateBatch {
    pub(super) fn into_page(
        self,
        query_options: &QueryOptions,
    ) -> Result<Page<HubuumObject>, ApiError> {
        if self.stopped_by_size {
            finalize_partial_page(self.items, query_options, true)
        } else {
            finalize_page(self.items, query_options)
        }
    }
}

impl ObjectGroupSqlSpec {
    pub(super) fn indexed(self) -> Self {
        Self {
            sql: self.sql.replace_question_mark_with_indexed_n(),
            binds: self.binds,
        }
    }
}

macro_rules! bind_object_group_query {
    ($spec:expr) => {{
        let spec = $spec.indexed();
        let mut query = diesel::sql_query(spec.sql).into_boxed();
        for bind in spec.binds {
            query = match bind {
                ObjectGroupBindValue::Json(value) => query.bind::<Jsonb, _>(value),
                ObjectGroupBindValue::BigInt(value) => query.bind::<BigInt, _>(value),
            };
        }
        query
    }};
}

pub(super) use bind_object_group_query;

macro_rules! apply_visible_object_filters {
    ($query:ident, $query_options:expr) => {{
        let query_params = $query_options.filters.clone();
        for param in query_params.json_datas(FilterField::JsonData)? {
            $query = $query.filter(param.as_json_predicate()?);
        }
        for param in query_params {
            let operator = param.operator.clone();
            match param.field {
                FilterField::Id => crate::numeric_search!($query, param, operator, object_id),
                FilterField::Collections | FilterField::CollectionId => {
                    crate::numeric_search!($query, param, operator, object_collection_id)
                }
                FilterField::CreatedAt => {
                    crate::date_search!($query, param, operator, object_created_at)
                }
                FilterField::UpdatedAt => {
                    crate::date_search!($query, param, operator, object_updated_at)
                }
                FilterField::Name => crate::string_search!($query, param, operator, object_name),
                FilterField::Description => {
                    crate::string_search!($query, param, operator, object_description)
                }
                FilterField::Classes | FilterField::ClassId => {
                    crate::numeric_search!($query, param, operator, hubuum_class_id)
                }
                FilterField::JsonData | FilterField::Permissions => {}
                _ => {
                    return Err(ApiError::BadRequest(format!(
                        "Field '{}' isn't searchable (or does not exist) for objects",
                        param.field
                    )));
                }
            }
        }
    }};
}

macro_rules! visible_filtered_object_query {
    ($collection_id:expr, $query_options:expr) => {{
        use crate::schema::hubuumobject::dsl::{
            collection_id as object_collection_id, created_at as object_created_at,
            description as object_description, hubuum_class_id, hubuumobject, id as object_id,
            name as object_name, updated_at as object_updated_at,
        };

        let mut query = hubuumobject
            .filter(object_collection_id.eq($collection_id))
            .into_boxed();
        apply_visible_object_filters!(query, $query_options);
        query
    }};
}

macro_rules! visible_filtered_group_query {
    ($collection_id:expr, $query_options:expr, $sort_key_sql:expr) => {{
        use crate::schema::hubuumobject::dsl::{
            collection_id as object_collection_id, created_at as object_created_at,
            description as object_description, hubuum_class_id, hubuumobject, id as object_id,
            name as object_name, updated_at as object_updated_at,
        };

        let mut query = hubuumobject
            .group_by(sql::<Jsonb>($sort_key_sql))
            .select((sql::<Jsonb>($sort_key_sql), sql::<BigInt>("COUNT(*)")))
            .into_boxed()
            .filter(object_collection_id.eq($collection_id));
        apply_visible_object_filters!(query, $query_options);
        query
    }};
}

pub(super) async fn group_visible_filtered_objects_with_sql(
    execution: ObjectGroupExecution<'_>,
) -> Result<crate::models::object_group::ObjectGroupPage, ApiError> {
    let ObjectGroupExecution {
        pool,
        target,
        query_options,
        spec,
        decoded_cursor,
        effective_limit,
        ..
    } = execution;
    let sort_key_sql = direct_group_sort_key(spec);
    let total_count = if query_options.include_total {
        let query = visible_filtered_object_query!(target.collection_id, query_options);
        with_connection(pool, async |connection| {
            query
                .select(count(sql::<Jsonb>(&sort_key_sql)).aggregate_distinct())
                .get_result::<i64>(connection)
                .await
        })
        .await?
    } else {
        SKIPPED_TOTAL_COUNT
    };

    let mut query =
        visible_filtered_group_query!(target.collection_id, query_options, &sort_key_sql);
    if let Some(cursor) = decoded_cursor {
        query = query.having(sql::<Bool>(&inline_cursor_clause(
            spec.sort(),
            &cursor,
            &sort_key_sql,
        )?));
    }
    query = match spec.sort() {
        ObjectGroupSort::DimensionsAscending => {
            query.order_by(sql::<Jsonb>(&format!("{sort_key_sql} ASC")))
        }
        ObjectGroupSort::DimensionsDescending => {
            query.order_by(sql::<Jsonb>(&format!("{sort_key_sql} DESC")))
        }
        ObjectGroupSort::ObjectCountAscending => query
            .order_by(sql::<BigInt>("COUNT(*) ASC"))
            .then_order_by(sql::<Jsonb>(&format!("{sort_key_sql} ASC"))),
        ObjectGroupSort::ObjectCountDescending => query
            .order_by(sql::<BigInt>("COUNT(*) DESC"))
            .then_order_by(sql::<Jsonb>(&format!("{sort_key_sql} ASC"))),
    };
    query = query.limit(page_query_limit(effective_limit)?);
    let database_rows = with_connection(pool, async |connection| {
        query.load::<(serde_json::Value, i64)>(connection).await
    })
    .await?
    .into_iter()
    .map(|(sort_key, object_count)| {
        Ok(ObjectGroupDatabaseRow {
            dimensions: dimensions_from_sort_key(spec, &sort_key)?,
            sort_key,
            object_count,
        })
    })
    .collect::<Result<Vec<_>, ApiError>>()?;
    finish_group_page(database_rows, total_count, effective_limit, spec)
}

pub(super) async fn load_group_candidate_batch(
    connection: &mut DbConnection,
    query_options: &QueryOptions,
    collection_id: i32,
) -> Result<ObjectGroupCandidateBatch, ApiError> {
    use crate::schema::hubuumobject::dsl::{
        collection_id as object_collection_id, created_at as object_created_at,
        description as object_description, hubuum_class_id, hubuumobject, id as object_id,
        name as object_name, updated_at as object_updated_at,
    };

    let mut query = hubuumobject
        .filter(object_collection_id.eq(collection_id))
        .into_boxed();
    apply_visible_object_filters!(query, query_options);
    crate::apply_query_options!(query, query_options, HubuumObject);
    let stream = query
        .select(hubuumobject::all_columns())
        .distinct()
        .load_stream::<HubuumObject>(connection)
        .await?;
    futures::pin_mut!(stream);
    let mut items = Vec::new();
    let mut serialized_bytes = 2_usize;
    let mut stopped_by_size = false;
    while let Some(candidate) = stream.try_next().await? {
        let candidate_bytes = serialized_candidate_len(&candidate)?;
        let next_size = serialized_bytes
            .checked_add(candidate_bytes.saturating_add(1))
            .ok_or_else(candidate_batch_too_large)?;
        if next_size > MAX_OBJECT_GROUP_CANDIDATE_BATCH_BYTES {
            if items.is_empty() {
                return Err(candidate_batch_too_large());
            }
            stopped_by_size = true;
            break;
        }
        items.push(candidate);
        serialized_bytes = next_size;
    }
    Ok(ObjectGroupCandidateBatch {
        items,
        stopped_by_size,
    })
}

fn serialized_candidate_len(candidate: &HubuumObject) -> Result<usize, ApiError> {
    let mut writer = CandidateSizeWriter::default();
    match serde_json::to_writer(&mut writer, candidate) {
        Ok(()) => Ok(writer.bytes),
        Err(_) if writer.exceeded => Err(candidate_batch_too_large()),
        Err(error) => Err(ApiError::InternalServerError(format!(
            "Failed to measure object group candidate: {error}"
        ))),
    }
}

#[derive(Default)]
struct CandidateSizeWriter {
    bytes: usize,
    exceeded: bool,
}

impl Write for CandidateSizeWriter {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        let Some(bytes) = self.bytes.checked_add(buffer.len()) else {
            self.exceeded = true;
            return Err(io::Error::other("object group candidate size overflowed"));
        };
        if bytes > MAX_OBJECT_GROUP_CANDIDATE_BATCH_BYTES {
            self.exceeded = true;
            return Err(io::Error::other(
                "object group candidate exceeds the source batch bound",
            ));
        }
        self.bytes = bytes;
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn candidate_batch_too_large() -> ApiError {
    ApiError::PayloadTooLarge(format!(
        "An object snapshot exceeds the {MAX_OBJECT_GROUP_CANDIDATE_BATCH_BYTES}-byte grouped-query source batch limit"
    ))
}

pub(super) async fn grouped_snapshot_rows(
    connection: &mut DbConnection,
    candidates: Vec<HubuumObject>,
    spec: &ObjectGroupSpec,
    computed_definitions: &ComputedGroupDefinitions,
) -> Result<GroupRows, ApiError> {
    if candidates.is_empty() {
        return Ok(GroupRows::default());
    }
    let (candidates, computed_payload) = if spec.has_computed_dimension() {
        let (candidates, payload) = computed_group_payload(candidates, spec, computed_definitions)?;
        (candidates, Some(payload))
    } else {
        (candidates, None)
    };
    let mut query = build_group_ctes(candidates, computed_payload, spec)?;
    query
        .sql
        .push_str("\nSELECT dimensions, sort_key, object_count FROM group_rows");
    let stream = bind_object_group_query!(query)
        .load_stream::<ObjectGroupDatabaseRow>(connection)
        .await?;
    futures::pin_mut!(stream);
    let mut groups = GroupRows::default();
    while let Some(row) = stream.try_next().await? {
        groups.push_bounded(row)?;
    }
    Ok(groups)
}

fn build_group_ctes(
    candidates: Vec<HubuumObject>,
    computed_payload: Option<serde_json::Value>,
    spec: &ObjectGroupSpec,
) -> Result<ObjectGroupSqlSpec, ApiError> {
    let candidates = serde_json::to_value(candidates).map_err(|error| {
        ApiError::InternalServerError(format!(
            "Failed to serialize authorized object snapshots: {error}"
        ))
    })?;
    let mut binds = vec![ObjectGroupBindValue::Json(candidates)];
    let computed_input = if let Some(payload) = computed_payload {
        binds.push(ObjectGroupBindValue::Json(payload));
        ", ?::jsonb AS computed_values"
    } else {
        ""
    };
    let computed_column = if computed_input.is_empty() {
        ""
    } else {
        ", input.computed_values"
    };
    let expressions = spec
        .dimensions()
        .iter()
        .enumerate()
        .map(|(index, dimension)| dimension_sql(index, dimension))
        .collect::<Vec<_>>();
    let dimension_select = expressions
        .iter()
        .enumerate()
        .flat_map(|(index, (state, value))| {
            [
                format!("{state} AS d{index}_state"),
                format!("{value} AS d{index}_value"),
            ]
        })
        .collect::<Vec<_>>()
        .join(",\n        ");
    let group_columns = (0..expressions.len())
        .flat_map(|index| [format!("d{index}_state"), format!("d{index}_value")])
        .collect::<Vec<_>>()
        .join(", ");
    let response_dimensions = spec
        .dimensions()
        .iter()
        .enumerate()
        .map(|(index, dimension)| response_dimension_sql(index, dimension))
        .collect::<Vec<_>>()
        .join(",\n            ");
    let sort_dimensions = (0..expressions.len())
        .map(|index| format!("jsonb_build_array(d{index}_state, d{index}_value)"))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        r#"WITH query_input AS (
    SELECT ?::jsonb AS objects{computed_input}
),
visible_filtered_objects AS (
    SELECT object.*{computed_column}
    FROM query_input AS input
    CROSS JOIN LATERAL jsonb_populate_recordset(NULL::hubuumobject, input.objects) AS object
),
dimensioned_objects AS (
    SELECT
        {dimension_select}
    FROM visible_filtered_objects AS object
),
grouped_objects AS (
    SELECT
        {group_columns},
        COUNT(*) AS object_count
    FROM dimensioned_objects
    GROUP BY {group_columns}
),
group_rows AS (
    SELECT
        jsonb_build_array(
            {response_dimensions}
        ) AS dimensions,
        jsonb_build_array({sort_dimensions}) AS sort_key,
        object_count
    FROM grouped_objects
)"#
    );
    Ok(ObjectGroupSqlSpec { sql, binds })
}

fn dimension_sql(index: usize, dimension: &ObjectGroupDimension) -> (String, String) {
    dimension_sql_for_source(index, dimension, "object")
}

fn dimension_sql_for_source(
    index: usize,
    dimension: &ObjectGroupDimension,
    source: &str,
) -> (String, String) {
    match dimension {
        ObjectGroupDimension::Scalar(field) => {
            let column = match field {
                ObjectGroupScalarField::Name => "name",
                ObjectGroupScalarField::Description => "description",
                ObjectGroupScalarField::CollectionId => "collection_id",
                ObjectGroupScalarField::CreatedAt => "created_at",
                ObjectGroupScalarField::UpdatedAt => "updated_at",
            };
            (
                "0::smallint".to_string(),
                format!("to_jsonb({source}.{column})"),
            )
        }
        ObjectGroupDimension::JsonData(path) => {
            let path = path
                .segments()
                .iter()
                .map(|segment| sql_string_literal(segment))
                .collect::<Vec<_>>()
                .join(", ");
            let value = format!("{source}.data #> ARRAY[{path}]::text[]");
            (
                format!(
                    "CASE WHEN {value} IS NULL THEN 2::smallint WHEN {value} = 'null'::jsonb THEN 1::smallint ELSE 0::smallint END"
                ),
                format!("COALESCE({value}, 'null'::jsonb)"),
            )
        }
        ObjectGroupDimension::Computed(_) => {
            let value = format!("{source}.computed_values -> {source}.id::text -> {index}");
            (
                format!("(({value}) ->> 'state')::smallint"),
                format!("COALESCE(({value}) -> 'value', 'null'::jsonb)"),
            )
        }
    }
}

fn direct_group_sort_key(spec: &ObjectGroupSpec) -> String {
    let sort_key = spec
        .dimensions()
        .iter()
        .enumerate()
        .map(|(index, dimension)| dimension_sql_for_source(index, dimension, "hubuumobject"))
        .map(|(state, value)| format!("jsonb_build_array({state}, {value})"))
        .collect::<Vec<_>>()
        .join(", ");
    format!("jsonb_build_array({sort_key})")
}

fn dimensions_from_sort_key(
    spec: &ObjectGroupSpec,
    sort_key: &serde_json::Value,
) -> Result<serde_json::Value, ApiError> {
    let values = sort_key.as_array().ok_or_else(|| {
        ApiError::InternalServerError(
            "Database returned a non-array object group sort key".to_string(),
        )
    })?;
    if values.len() != spec.dimensions().len() {
        return Err(ApiError::InternalServerError(
            "Database returned an object group sort key with the wrong dimension count".to_string(),
        ));
    }
    let dimensions = spec
        .dimensions()
        .iter()
        .zip(values)
        .map(|(dimension, item)| dimension_from_sort_item(dimension, item))
        .collect::<Result<Vec<_>, ApiError>>()?;
    Ok(serde_json::Value::Array(dimensions))
}

fn dimension_from_sort_item(
    dimension: &ObjectGroupDimension,
    item: &serde_json::Value,
) -> Result<serde_json::Value, ApiError> {
    let pair = item
        .as_array()
        .filter(|pair| pair.len() == 2)
        .ok_or_else(|| {
            ApiError::InternalServerError(
                "Database returned an invalid object group sort key".to_string(),
            )
        })?;
    let state = pair[0].as_i64().ok_or_else(|| {
        ApiError::InternalServerError(
            "Database returned an invalid object group value state".to_string(),
        )
    })?;
    let field = dimension.canonical();
    Ok(match state {
        0 => serde_json::json!({"field": field, "state": "value", "value": pair[1].clone()}),
        1 => serde_json::json!({"field": field, "state": "null"}),
        2 => serde_json::json!({"field": field, "state": "missing"}),
        3 => serde_json::json!({"field": field, "state": "unavailable"}),
        _ => {
            return Err(ApiError::InternalServerError(
                "Database returned an unknown object group value state".to_string(),
            ));
        }
    })
}

fn response_dimension_sql(index: usize, dimension: &ObjectGroupDimension) -> String {
    response_dimension_expression(
        dimension,
        &format!("d{index}_state"),
        &format!("d{index}_value"),
    )
}

fn response_dimension_expression(
    dimension: &ObjectGroupDimension,
    state: &str,
    value: &str,
) -> String {
    let field = sql_string_literal(&dimension.canonical());
    format!(
        "CASE {state} WHEN 0 THEN jsonb_build_object('field', {field}, 'state', 'value', 'value', {value}) WHEN 1 THEN jsonb_build_object('field', {field}, 'state', 'null') WHEN 2 THEN jsonb_build_object('field', {field}, 'state', 'missing') ELSE jsonb_build_object('field', {field}, 'state', 'unavailable') END"
    )
}

fn sql_string_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn inline_cursor_clause(
    sort: ObjectGroupSort,
    cursor: &DecodedObjectGroupCursor,
    sort_key_sql: &str,
) -> Result<String, ApiError> {
    let sort_key = serde_json::to_string(&cursor.sort_key).map_err(|error| {
        ApiError::BadRequest(format!(
            "group cursor contains invalid ordering values: {error}"
        ))
    })?;
    let sort_key = format!("{}::jsonb", sql_string_literal(&sort_key));
    Ok(match sort {
        ObjectGroupSort::DimensionsAscending => format!("{sort_key_sql} > {sort_key}"),
        ObjectGroupSort::DimensionsDescending => format!("{sort_key_sql} < {sort_key}"),
        ObjectGroupSort::ObjectCountAscending => format!(
            "(COUNT(*) > {count} OR (COUNT(*) = {count} AND {sort_key_sql} > {sort_key}))",
            count = cursor.object_count,
        ),
        ObjectGroupSort::ObjectCountDescending => format!(
            "(COUNT(*) < {count} OR (COUNT(*) = {count} AND {sort_key_sql} > {sort_key}))",
            count = cursor.object_count,
        ),
    })
}

pub(super) fn append_page_options(
    spec: &mut ObjectGroupSqlSpec,
    sort: ObjectGroupSort,
    cursor: Option<DecodedObjectGroupCursor>,
    effective_limit: usize,
) -> Result<(), ApiError> {
    if let Some(cursor) = cursor {
        append_cursor_clause(spec, sort, cursor);
    }
    spec.sql.push_str("\nORDER BY ");
    spec.sql.push_str(order_clause(sort));
    spec.sql.push_str("\nLIMIT ?");
    spec.binds
        .push(ObjectGroupBindValue::BigInt(page_query_limit(
            effective_limit,
        )?));
    Ok(())
}

fn append_cursor_clause(
    spec: &mut ObjectGroupSqlSpec,
    sort: ObjectGroupSort,
    cursor: DecodedObjectGroupCursor,
) {
    spec.sql.push_str("\nWHERE ");
    match sort {
        ObjectGroupSort::DimensionsAscending => {
            spec.sql.push_str("sort_key > ?::jsonb");
            spec.binds.push(ObjectGroupBindValue::Json(cursor.sort_key));
        }
        ObjectGroupSort::DimensionsDescending => {
            spec.sql.push_str("sort_key < ?::jsonb");
            spec.binds.push(ObjectGroupBindValue::Json(cursor.sort_key));
        }
        ObjectGroupSort::ObjectCountAscending => {
            spec.sql
                .push_str("(object_count > ? OR (object_count = ? AND sort_key > ?::jsonb))");
            spec.binds
                .push(ObjectGroupBindValue::BigInt(cursor.object_count));
            spec.binds
                .push(ObjectGroupBindValue::BigInt(cursor.object_count));
            spec.binds.push(ObjectGroupBindValue::Json(cursor.sort_key));
        }
        ObjectGroupSort::ObjectCountDescending => {
            spec.sql
                .push_str("(object_count < ? OR (object_count = ? AND sort_key > ?::jsonb))");
            spec.binds
                .push(ObjectGroupBindValue::BigInt(cursor.object_count));
            spec.binds
                .push(ObjectGroupBindValue::BigInt(cursor.object_count));
            spec.binds.push(ObjectGroupBindValue::Json(cursor.sort_key));
        }
    }
}

const fn order_clause(sort: ObjectGroupSort) -> &'static str {
    match sort {
        ObjectGroupSort::DimensionsAscending => "sort_key ASC",
        ObjectGroupSort::DimensionsDescending => "sort_key DESC",
        ObjectGroupSort::ObjectCountAscending => "object_count ASC, sort_key ASC",
        ObjectGroupSort::ObjectCountDescending => "object_count DESC, sort_key ASC",
    }
}

fn page_query_limit(effective_limit: usize) -> Result<i64, ApiError> {
    i64::try_from(effective_limit.saturating_add(1))
        .map_err(|_| ApiError::BadRequest("Object group page limit is too large".to_string()))
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    #[test]
    fn json_dimension_sql_distinguishes_value_null_and_missing() {
        let dimension = ObjectGroupDimension::from_str("json_data.location,country").unwrap();
        let (state, value) = dimension_sql(0, &dimension);
        assert!(state.contains("IS NULL THEN 2"));
        assert!(state.contains("= 'null'::jsonb THEN 1"));
        assert!(value.contains("COALESCE"));
    }

    #[test]
    fn count_sort_always_uses_complete_dimension_tie_breaker() {
        assert_eq!(
            order_clause(ObjectGroupSort::ObjectCountDescending),
            "object_count DESC, sort_key ASC"
        );
    }
}
