use diesel::dsl::{count, sql};
use diesel::prelude::*;
use diesel::sql_types::{BigInt, Bool, Jsonb};
use diesel_async::RunQueryDsl;
use futures::TryStreamExt;

use super::ObjectAggregateExecution;
use super::accumulator::{AggregateRows, ObjectAggregateDatabaseRow, finish_aggregate_page};
use super::candidate::ObjectAggregateCandidate;
use super::computed::{ComputedAggregateDefinitions, computed_aggregate_payload};
use super::filters::apply_object_aggregate_source_filters;
use crate::db::traits::computed_field::{ComputedQuerySnapshot, computed_filter_sql_component};
use crate::db::traits::search::JsonPredicateExt;
use crate::db::{DbConnection, with_connection};
use crate::errors::ApiError;
use crate::models::object_aggregate::{
    DecodedObjectAggregateCursor, ObjectAggregateDimension, ObjectAggregateScalarField,
    ObjectAggregateSort, ObjectAggregateSpec,
};
use crate::models::search::{FilterField, QueryOptions, QueryParamsExt, SQLValue};
use crate::pagination::SKIPPED_TOTAL_COUNT;
use crate::utilities::extensions::CustomStringExtensions;

#[derive(Debug, Clone)]
pub(super) enum ObjectAggregateBindValue {
    Json(serde_json::Value),
    BigInt(i64),
    Query(SQLValue),
}

#[derive(Debug, Clone)]
pub(super) struct ObjectAggregateSqlSpec {
    pub(super) sql: String,
    pub(super) binds: Vec<ObjectAggregateBindValue>,
}

impl ObjectAggregateSqlSpec {
    pub(super) fn indexed(self) -> Self {
        Self {
            sql: self.sql.replace_question_mark_with_indexed_n(),
            binds: self.binds,
        }
    }
}

macro_rules! bind_object_aggregate_query {
    ($spec:expr) => {{
        let spec = $spec.indexed();
        let mut query = diesel::sql_query(spec.sql).into_boxed();
        for bind in spec.binds {
            query = match bind {
                ObjectAggregateBindValue::Json(value) => query.bind::<Jsonb, _>(value),
                ObjectAggregateBindValue::BigInt(value) => query.bind::<BigInt, _>(value),
                ObjectAggregateBindValue::Query(crate::models::search::SQLValue::String(value)) => {
                    query.bind::<diesel::sql_types::Text, _>(value)
                }
                ObjectAggregateBindValue::Query(crate::models::search::SQLValue::Integer(
                    value,
                )) => query.bind::<diesel::sql_types::Integer, _>(value),
                ObjectAggregateBindValue::Query(crate::models::search::SQLValue::Date(value)) => {
                    query.bind::<diesel::sql_types::Timestamp, _>(value)
                }
                ObjectAggregateBindValue::Query(crate::models::search::SQLValue::Boolean(
                    value,
                )) => query.bind::<diesel::sql_types::Bool, _>(value),
            };
        }
        query
    }};
}

pub(super) use bind_object_aggregate_query;

macro_rules! visible_filtered_object_query {
    ($collection_id:expr, $query_options:expr, $computed_filter_snapshot:expr) => {{
        use crate::schema::hubuumobject::dsl::{
            collection_id as object_collection_id, created_at as object_created_at,
            description as object_description, hubuum_class_id, hubuumobject, id as object_id,
            name as object_name, updated_at as object_updated_at,
        };

        let mut query = hubuumobject
            .filter(object_collection_id.eq($collection_id))
            .into_boxed();
        apply_object_aggregate_source_filters!(query, $query_options, $computed_filter_snapshot);
        query
    }};
}

macro_rules! visible_filtered_aggregate_query {
    ($collection_id:expr, $query_options:expr, $sort_key_sql:expr, $computed_filter_snapshot:expr) => {{
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
        apply_object_aggregate_source_filters!(query, $query_options, $computed_filter_snapshot);
        query
    }};
}

pub(super) async fn aggregate_visible_filtered_objects_with_sql(
    execution: ObjectAggregateExecution<'_>,
) -> Result<crate::models::object_aggregate::ObjectAggregatePage, ApiError> {
    let ObjectAggregateExecution {
        pool,
        target,
        paging,
        ..
    } = execution;
    let query_options = &paging.query_options;
    let spec = &paging.spec;
    let computed_filter_snapshot = paging.computed_filter_snapshot.as_ref();
    let sort_key_sql = direct_aggregate_sort_key(spec);
    let total_count = if query_options.include_total {
        let query = visible_filtered_object_query!(
            target.collection_id,
            query_options,
            computed_filter_snapshot
        );
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

    let mut query = visible_filtered_aggregate_query!(
        target.collection_id,
        query_options,
        &sort_key_sql,
        computed_filter_snapshot
    );
    if let Some(cursor) = paging.decoded_cursor.as_ref() {
        query = query.having(sql::<Bool>(&inline_cursor_clause(
            spec.sort(),
            cursor,
            &sort_key_sql,
        )?));
    }
    query = match spec.sort() {
        ObjectAggregateSort::DimensionsAscending => {
            query.order_by(sql::<Jsonb>(&format!("{sort_key_sql} ASC")))
        }
        ObjectAggregateSort::DimensionsDescending => {
            query.order_by(sql::<Jsonb>(&format!("{sort_key_sql} DESC")))
        }
        ObjectAggregateSort::ObjectCountAscending => query
            .order_by(sql::<BigInt>("COUNT(*) ASC"))
            .then_order_by(sql::<Jsonb>(&format!("{sort_key_sql} ASC"))),
        ObjectAggregateSort::ObjectCountDescending => query
            .order_by(sql::<BigInt>("COUNT(*) DESC"))
            .then_order_by(sql::<Jsonb>(&format!("{sort_key_sql} ASC"))),
    };
    query = query.limit(page_query_limit(paging.effective_limit)?);
    let database_rows = with_connection(pool, async |connection| {
        query.load::<(serde_json::Value, i64)>(connection).await
    })
    .await?
    .into_iter()
    .map(|(sort_key, object_count)| {
        Ok(ObjectAggregateDatabaseRow {
            dimensions: dimensions_from_sort_key(spec, &sort_key)?,
            sort_key,
            object_count,
        })
    })
    .collect::<Result<Vec<_>, ApiError>>()?;
    finish_aggregate_page(database_rows, total_count, &paging)
}

pub(super) async fn aggregate_snapshot_rows(
    connection: &mut DbConnection,
    candidates: Vec<ObjectAggregateCandidate>,
    plan: SnapshotAggregatePlan<'_>,
) -> Result<AggregateRows, ApiError> {
    if candidates.is_empty() {
        return Ok(AggregateRows::default());
    }
    let (candidates, computed_payload) = if plan.spec.has_computed_dimension() {
        let (candidates, payload) =
            computed_aggregate_payload(candidates, plan.spec, plan.computed_definitions)?;
        (candidates, Some(payload))
    } else {
        (candidates, None)
    };
    let mut query = build_aggregate_ctes(candidates, computed_payload, &plan)?;
    query
        .sql
        .push_str("\nSELECT dimensions, sort_key, object_count FROM aggregate_rows");
    let stream = bind_object_aggregate_query!(query)
        .load_stream::<ObjectAggregateDatabaseRow>(connection)
        .await?;
    futures::pin_mut!(stream);
    let mut groups = AggregateRows::default();
    while let Some(row) = stream.try_next().await? {
        groups.push_bounded(row)?;
    }
    Ok(groups)
}

pub(super) struct SnapshotAggregatePlan<'a> {
    spec: &'a ObjectAggregateSpec,
    computed_definitions: &'a ComputedAggregateDefinitions,
    computed_filters: Option<(&'a QueryOptions, &'a ComputedQuerySnapshot)>,
}

impl<'a> SnapshotAggregatePlan<'a> {
    pub(super) fn new(
        spec: &'a ObjectAggregateSpec,
        computed_definitions: &'a ComputedAggregateDefinitions,
    ) -> Self {
        Self {
            spec,
            computed_definitions,
            computed_filters: None,
        }
    }

    pub(super) fn computed_filters(
        mut self,
        query_options: &'a QueryOptions,
        snapshot: &'a ComputedQuerySnapshot,
    ) -> Self {
        self.computed_filters = Some((query_options, snapshot));
        self
    }
}

fn build_aggregate_ctes(
    candidates: Vec<ObjectAggregateCandidate>,
    computed_payload: Option<serde_json::Value>,
    plan: &SnapshotAggregatePlan<'_>,
) -> Result<ObjectAggregateSqlSpec, ApiError> {
    let candidates = serde_json::to_value(candidates).map_err(|error| {
        ApiError::InternalServerError(format!(
            "Failed to serialize authorized object snapshots: {error}"
        ))
    })?;
    let mut binds = vec![ObjectAggregateBindValue::Json(candidates)];
    let computed_input = if let Some(payload) = computed_payload {
        binds.push(ObjectAggregateBindValue::Json(payload));
        ", ?::jsonb AS computed_values"
    } else {
        ""
    };
    let computed_column = if computed_input.is_empty() {
        ""
    } else {
        ", input.computed_values"
    };
    let (computed_filter_clause, computed_filter_binds) = plan
        .computed_filters
        .map(|(query_options, snapshot)| computed_filter_clause(query_options, snapshot))
        .transpose()?
        .unwrap_or_default();
    binds.extend(computed_filter_binds);
    let expressions = plan
        .spec
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
    let response_dimensions = plan
        .spec
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
    SELECT hubuumobject.*{computed_column}
    FROM query_input AS input
    CROSS JOIN LATERAL jsonb_populate_recordset(NULL::hubuumobject, input.objects) AS hubuumobject{computed_filter_clause}
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
aggregate_rows AS (
    SELECT
        jsonb_build_array(
            {response_dimensions}
        ) AS dimensions,
        jsonb_build_array({sort_dimensions}) AS sort_key,
        object_count
    FROM grouped_objects
)"#
    );
    Ok(ObjectAggregateSqlSpec { sql, binds })
}

fn computed_filter_clause(
    query_options: &QueryOptions,
    snapshot: &ComputedQuerySnapshot,
) -> Result<(String, Vec<ObjectAggregateBindValue>), ApiError> {
    let mut clauses = Vec::new();
    let mut binds = Vec::new();
    for filter in query_options
        .filters
        .iter()
        .filter(|filter| filter.field.computed_query().is_some())
    {
        let component = computed_filter_sql_component(filter, snapshot)?;
        clauses.push(format!("({})", component.sql));
        binds.extend(
            component
                .bind_variables
                .into_iter()
                .map(ObjectAggregateBindValue::Query),
        );
    }
    let clause = if clauses.is_empty() {
        String::new()
    } else {
        format!("\n    WHERE {}", clauses.join(" AND "))
    };
    Ok((clause, binds))
}

fn dimension_sql(index: usize, dimension: &ObjectAggregateDimension) -> (String, String) {
    dimension_sql_for_source(index, dimension, "object")
}

fn dimension_sql_for_source(
    index: usize,
    dimension: &ObjectAggregateDimension,
    source: &str,
) -> (String, String) {
    match dimension {
        ObjectAggregateDimension::Scalar(field) => {
            let column = match field {
                ObjectAggregateScalarField::Name => "name",
                ObjectAggregateScalarField::Description => "description",
                ObjectAggregateScalarField::CollectionId => "collection_id",
                ObjectAggregateScalarField::CreatedAt => "created_at",
                ObjectAggregateScalarField::UpdatedAt => "updated_at",
            };
            (
                "0::smallint".to_string(),
                format!("to_jsonb({source}.{column})"),
            )
        }
        ObjectAggregateDimension::JsonData(path) => {
            let path = path
                .segments()
                .map(sql_string_literal)
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
        ObjectAggregateDimension::Computed(_) => {
            let value = format!("{source}.computed_values -> {source}.id::text -> {index}");
            (
                format!("(({value}) ->> 'state')::smallint"),
                format!("COALESCE(({value}) -> 'value', 'null'::jsonb)"),
            )
        }
    }
}

fn direct_aggregate_sort_key(spec: &ObjectAggregateSpec) -> String {
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
    spec: &ObjectAggregateSpec,
    sort_key: &serde_json::Value,
) -> Result<serde_json::Value, ApiError> {
    let values = sort_key.as_array().ok_or_else(|| {
        ApiError::InternalServerError(
            "Database returned a non-array object aggregate sort key".to_string(),
        )
    })?;
    if values.len() != spec.dimensions().len() {
        return Err(ApiError::InternalServerError(
            "Database returned an object aggregate sort key with the wrong dimension count"
                .to_string(),
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
    dimension: &ObjectAggregateDimension,
    item: &serde_json::Value,
) -> Result<serde_json::Value, ApiError> {
    let pair = item
        .as_array()
        .filter(|pair| pair.len() == 2)
        .ok_or_else(|| {
            ApiError::InternalServerError(
                "Database returned an invalid object aggregate sort key".to_string(),
            )
        })?;
    let state = pair[0].as_i64().ok_or_else(|| {
        ApiError::InternalServerError(
            "Database returned an invalid object aggregate value state".to_string(),
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
                "Database returned an unknown object aggregate value state".to_string(),
            ));
        }
    })
}

fn response_dimension_sql(index: usize, dimension: &ObjectAggregateDimension) -> String {
    response_dimension_expression(
        dimension,
        &format!("d{index}_state"),
        &format!("d{index}_value"),
    )
}

fn response_dimension_expression(
    dimension: &ObjectAggregateDimension,
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
    sort: ObjectAggregateSort,
    cursor: &DecodedObjectAggregateCursor,
    sort_key_sql: &str,
) -> Result<String, ApiError> {
    let sort_key = serde_json::to_string(&cursor.sort_key).map_err(|error| {
        ApiError::BadRequest(format!(
            "aggregate cursor contains invalid ordering values: {error}"
        ))
    })?;
    let sort_key = format!("{}::jsonb", sql_string_literal(&sort_key));
    Ok(match sort {
        ObjectAggregateSort::DimensionsAscending => format!("{sort_key_sql} > {sort_key}"),
        ObjectAggregateSort::DimensionsDescending => format!("{sort_key_sql} < {sort_key}"),
        ObjectAggregateSort::ObjectCountAscending => format!(
            "(COUNT(*) > {count} OR (COUNT(*) = {count} AND {sort_key_sql} > {sort_key}))",
            count = cursor.object_count,
        ),
        ObjectAggregateSort::ObjectCountDescending => format!(
            "(COUNT(*) < {count} OR (COUNT(*) = {count} AND {sort_key_sql} > {sort_key}))",
            count = cursor.object_count,
        ),
    })
}

pub(super) fn append_page_options(
    spec: &mut ObjectAggregateSqlSpec,
    sort: ObjectAggregateSort,
    cursor: Option<&DecodedObjectAggregateCursor>,
    effective_limit: usize,
) -> Result<(), ApiError> {
    if let Some(cursor) = cursor {
        append_cursor_clause(spec, sort, cursor);
    }
    spec.sql.push_str("\nORDER BY ");
    spec.sql.push_str(order_clause(sort));
    spec.sql.push_str("\nLIMIT ?");
    spec.binds
        .push(ObjectAggregateBindValue::BigInt(page_query_limit(
            effective_limit,
        )?));
    Ok(())
}

fn append_cursor_clause(
    spec: &mut ObjectAggregateSqlSpec,
    sort: ObjectAggregateSort,
    cursor: &DecodedObjectAggregateCursor,
) {
    spec.sql.push_str("\nWHERE ");
    match sort {
        ObjectAggregateSort::DimensionsAscending => {
            spec.sql.push_str("sort_key > ?::jsonb");
            spec.binds
                .push(ObjectAggregateBindValue::Json(cursor.sort_key.clone()));
        }
        ObjectAggregateSort::DimensionsDescending => {
            spec.sql.push_str("sort_key < ?::jsonb");
            spec.binds
                .push(ObjectAggregateBindValue::Json(cursor.sort_key.clone()));
        }
        ObjectAggregateSort::ObjectCountAscending => {
            spec.sql
                .push_str("(object_count > ? OR (object_count = ? AND sort_key > ?::jsonb))");
            spec.binds
                .push(ObjectAggregateBindValue::BigInt(cursor.object_count));
            spec.binds
                .push(ObjectAggregateBindValue::BigInt(cursor.object_count));
            spec.binds
                .push(ObjectAggregateBindValue::Json(cursor.sort_key.clone()));
        }
        ObjectAggregateSort::ObjectCountDescending => {
            spec.sql
                .push_str("(object_count < ? OR (object_count = ? AND sort_key > ?::jsonb))");
            spec.binds
                .push(ObjectAggregateBindValue::BigInt(cursor.object_count));
            spec.binds
                .push(ObjectAggregateBindValue::BigInt(cursor.object_count));
            spec.binds
                .push(ObjectAggregateBindValue::Json(cursor.sort_key.clone()));
        }
    }
}

const fn order_clause(sort: ObjectAggregateSort) -> &'static str {
    match sort {
        ObjectAggregateSort::DimensionsAscending => "sort_key ASC",
        ObjectAggregateSort::DimensionsDescending => "sort_key DESC",
        ObjectAggregateSort::ObjectCountAscending => "object_count ASC, sort_key ASC",
        ObjectAggregateSort::ObjectCountDescending => "object_count DESC, sort_key ASC",
    }
}

fn page_query_limit(effective_limit: usize) -> Result<i64, ApiError> {
    i64::try_from(effective_limit.saturating_add(1))
        .map_err(|_| ApiError::BadRequest("Object aggregate page limit is too large".to_string()))
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    #[test]
    fn json_dimension_sql_distinguishes_value_null_and_missing() {
        let dimension = ObjectAggregateDimension::from_str("json_data.location,country").unwrap();
        let (state, value) = dimension_sql(0, &dimension);
        assert!(state.contains("IS NULL THEN 2"));
        assert!(state.contains("= 'null'::jsonb THEN 1"));
        assert!(value.contains("COALESCE"));
    }

    #[test]
    fn count_sort_always_uses_complete_dimension_tie_breaker() {
        assert_eq!(
            order_clause(ObjectAggregateSort::ObjectCountDescending),
            "object_count DESC, sort_key ASC"
        );
    }
}
