use diesel::dsl::{count, sql};
use diesel::prelude::*;
use diesel::sql_types::{BigInt, Bool, Jsonb};
use diesel_async::RunQueryDsl;
use futures_util::TryStreamExt;

use super::ObjectAggregateExecution;
use super::accumulator::{
    AggregateRows, ObjectAggregateDatabaseRow, PartialObjectAggregateRow, finish_aggregate_page,
};
use super::candidate::ObjectAggregateCandidate;
use super::computed::{ComputedAggregateDefinitions, computed_aggregate_payload};
use super::filters::apply_object_aggregate_source_filters;
use hubuum_query::{JsonFieldPath, QueryOptions};
use hubuum_storage_core::{
    StorageComputedFieldSelector, StorageObjectAggregateCursor, StorageObjectAggregateDimension,
    StorageObjectAggregateMeasure, StorageObjectAggregateMeasureField,
    StorageObjectAggregateMeasureOperation, StorageObjectAggregateScalarField,
    StorageObjectAggregateSort, StorageObjectAggregateSpec,
};

use crate::operations::computed_objects::query::{
    ComputedQuerySnapshot, computed_filter_sql_component,
};
use crate::operations::dynamic_sql::{SqlValue, indexed_bind_placeholders};
use crate::{PostgresConnection, PostgresStorageError};

#[derive(Debug, Clone)]
pub(super) enum ObjectAggregateBindValue {
    Json(serde_json::Value),
    BigInt(i64),
    Query(SqlValue),
}

#[derive(Debug, Clone)]
pub(super) struct ObjectAggregateSqlSpec {
    pub(super) sql: String,
    pub(super) binds: Vec<ObjectAggregateBindValue>,
}

impl ObjectAggregateSqlSpec {
    pub(super) fn indexed(self) -> Self {
        Self {
            sql: indexed_bind_placeholders(&self.sql),
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
                ObjectAggregateBindValue::Query(
                    $crate::operations::dynamic_sql::SqlValue::String(value),
                ) => query.bind::<diesel::sql_types::Text, _>(value),
                ObjectAggregateBindValue::Query(
                    $crate::operations::dynamic_sql::SqlValue::Integer(value),
                ) => query.bind::<diesel::sql_types::Integer, _>(value),
                ObjectAggregateBindValue::Query(
                    $crate::operations::dynamic_sql::SqlValue::BigInteger(value),
                ) => query.bind::<diesel::sql_types::BigInt, _>(value),
                ObjectAggregateBindValue::Query(
                    $crate::operations::dynamic_sql::SqlValue::DateTime(value),
                ) => query.bind::<diesel::sql_types::Timestamp, _>(value),
                ObjectAggregateBindValue::Query(
                    $crate::operations::dynamic_sql::SqlValue::Boolean(value),
                ) => query.bind::<diesel::sql_types::Bool, _>(value),
            };
        }
        query
    }};
}

pub(super) use bind_object_aggregate_query;

macro_rules! visible_filtered_object_query {
    ($collection_id:expr, $resource_scope:expr, $query_options:expr, $computed_filter_snapshot:expr) => {{
        use crate::schema::hubuumobject::dsl::{
            collection_id as object_collection_id, created_at as object_created_at,
            description as object_description, hubuum_class_id, hubuumobject, id as object_id,
            name as object_name, updated_at as object_updated_at,
        };

        let mut query = hubuumobject
            .filter(object_collection_id.eq($collection_id))
            .into_boxed();
        if let Some(scope) = $resource_scope {
            query = query.filter(
                object_collection_id
                    .eq_any(
                        scope
                            .collection_ids()
                            .iter()
                            .map(|id| id.id())
                            .collect::<Vec<_>>(),
                    )
                    .or(hubuum_class_id.eq_any(
                        scope
                            .class_ids()
                            .iter()
                            .map(|id| id.id())
                            .collect::<Vec<_>>(),
                    ))
                    .or(object_id.eq_any(
                        scope
                            .object_ids()
                            .iter()
                            .map(|id| id.id())
                            .collect::<Vec<_>>(),
                    )),
            );
        }
        apply_object_aggregate_source_filters!(query, $query_options, $computed_filter_snapshot);
        query
    }};
}

macro_rules! visible_filtered_aggregate_query {
    ($collection_id:expr, $resource_scope:expr, $query_options:expr, $sort_key_sql:expr, $measures_sql:expr, $computed_filter_snapshot:expr) => {{
        use crate::schema::hubuumobject::dsl::{
            collection_id as object_collection_id, created_at as object_created_at,
            description as object_description, hubuum_class_id, hubuumobject, id as object_id,
            name as object_name, updated_at as object_updated_at,
        };

        let mut query = hubuumobject
            .group_by(sql::<Jsonb>($sort_key_sql))
            .select((
                sql::<Jsonb>($sort_key_sql),
                sql::<BigInt>("COUNT(*)"),
                sql::<Jsonb>($measures_sql),
            ))
            .into_boxed()
            .filter(object_collection_id.eq($collection_id));
        if let Some(scope) = $resource_scope {
            query = query.filter(
                object_collection_id
                    .eq_any(
                        scope
                            .collection_ids()
                            .iter()
                            .map(|id| id.id())
                            .collect::<Vec<_>>(),
                    )
                    .or(hubuum_class_id.eq_any(
                        scope
                            .class_ids()
                            .iter()
                            .map(|id| id.id())
                            .collect::<Vec<_>>(),
                    ))
                    .or(object_id.eq_any(
                        scope
                            .object_ids()
                            .iter()
                            .map(|id| id.id())
                            .collect::<Vec<_>>(),
                    )),
            );
        }
        apply_object_aggregate_source_filters!(query, $query_options, $computed_filter_snapshot);
        query
    }};
}

pub(super) async fn aggregate_visible_filtered_objects_with_sql(
    connection: &mut PostgresConnection,
    execution: ObjectAggregateExecution<'_>,
) -> Result<hubuum_storage_core::StorageObjectAggregatePage, PostgresStorageError> {
    let ObjectAggregateExecution {
        target,
        paging,
        visibility,
        ..
    } = execution;
    let query_options = &paging.query_options;
    let spec = &paging.spec;
    let computed_filter_snapshot = paging.computed_filter_snapshot.as_ref();
    let sort_key_sql = direct_aggregate_sort_key(spec);
    let measures_sql = direct_measure_response_sql(spec, "hubuumobject");
    let total = if query_options.include_total() {
        let query = visible_filtered_object_query!(
            target.collection_id,
            visibility.resources(),
            query_options,
            computed_filter_snapshot
        );
        Some(
            query
                .select(count(sql::<Jsonb>(&sort_key_sql)).aggregate_distinct())
                .get_result::<i64>(connection)
                .await?,
        )
    } else {
        None
    };

    let mut query = visible_filtered_aggregate_query!(
        target.collection_id,
        visibility.resources(),
        query_options,
        &sort_key_sql,
        &measures_sql,
        computed_filter_snapshot
    );
    if let Some(cursor) = paging.decoded_cursor.as_ref() {
        query = match spec.sort() {
            StorageObjectAggregateSort::DimensionsAscending => query.having(
                sql::<Bool>(&format!("{sort_key_sql} > "))
                    .bind::<Jsonb, _>(cursor.sort_key().clone()),
            ),
            StorageObjectAggregateSort::DimensionsDescending => query.having(
                sql::<Bool>(&format!("{sort_key_sql} < "))
                    .bind::<Jsonb, _>(cursor.sort_key().clone()),
            ),
            StorageObjectAggregateSort::ObjectCountAscending => query.having(
                sql::<Bool>("(COUNT(*) > ")
                    .bind::<BigInt, _>(cursor.object_count())
                    .sql(" OR (COUNT(*) = ")
                    .bind::<BigInt, _>(cursor.object_count())
                    .sql(&format!(" AND {sort_key_sql} > "))
                    .bind::<Jsonb, _>(cursor.sort_key().clone())
                    .sql("))"),
            ),
            StorageObjectAggregateSort::ObjectCountDescending => query.having(
                sql::<Bool>("(COUNT(*) < ")
                    .bind::<BigInt, _>(cursor.object_count())
                    .sql(" OR (COUNT(*) = ")
                    .bind::<BigInt, _>(cursor.object_count())
                    .sql(&format!(" AND {sort_key_sql} > "))
                    .bind::<Jsonb, _>(cursor.sort_key().clone())
                    .sql("))"),
            ),
        };
    }
    query = match spec.sort() {
        StorageObjectAggregateSort::DimensionsAscending => {
            query.order_by(sql::<Jsonb>(&format!("{sort_key_sql} ASC")))
        }
        StorageObjectAggregateSort::DimensionsDescending => {
            query.order_by(sql::<Jsonb>(&format!("{sort_key_sql} DESC")))
        }
        StorageObjectAggregateSort::ObjectCountAscending => query
            .order_by(sql::<BigInt>("COUNT(*) ASC"))
            .then_order_by(sql::<Jsonb>(&format!("{sort_key_sql} ASC"))),
        StorageObjectAggregateSort::ObjectCountDescending => query
            .order_by(sql::<BigInt>("COUNT(*) DESC"))
            .then_order_by(sql::<Jsonb>(&format!("{sort_key_sql} ASC"))),
    };
    query = query.limit(page_query_limit(paging.effective_limit)?);
    let database_rows = query
        .load::<(serde_json::Value, i64, serde_json::Value)>(connection)
        .await?
        .into_iter()
        .map(|(sort_key, object_count, measures)| {
            Ok::<_, PostgresStorageError>(ObjectAggregateDatabaseRow {
                sort_key,
                measures,
                object_count,
            })
        })
        .collect::<Result<Vec<_>, PostgresStorageError>>()?;
    finish_aggregate_page(database_rows, total, &paging)
}

pub(super) async fn aggregate_snapshot_rows(
    connection: &mut PostgresConnection,
    candidates: Vec<ObjectAggregateCandidate>,
    plan: SnapshotAggregatePlan<'_>,
) -> Result<AggregateRows, PostgresStorageError> {
    if candidates.is_empty() {
        return Ok(AggregateRows::default());
    }
    let (candidates, computed_payload) = if plan.spec.has_computed_field() {
        let (candidates, payload) = computed_aggregate_payload(
            plan.runtime,
            candidates,
            plan.spec,
            plan.computed_definitions,
        )?;
        (candidates, Some(payload))
    } else {
        (candidates, None)
    };
    let mut query = build_aggregate_ctes(candidates, computed_payload, &plan)?;
    query
        .sql
        .push_str("\nSELECT sort_key, measure_state, object_count FROM aggregate_rows");
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

pub(super) struct SnapshotAggregatePlan<'a> {
    runtime: &'a crate::PostgresRuntime,
    spec: &'a StorageObjectAggregateSpec,
    computed_definitions: &'a ComputedAggregateDefinitions,
    computed_filters: Option<(&'a QueryOptions, &'a ComputedQuerySnapshot)>,
}

impl<'a> SnapshotAggregatePlan<'a> {
    pub(super) fn new(
        runtime: &'a crate::PostgresRuntime,
        spec: &'a StorageObjectAggregateSpec,
        computed_definitions: &'a ComputedAggregateDefinitions,
    ) -> Self {
        Self {
            runtime,
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
) -> Result<ObjectAggregateSqlSpec, PostgresStorageError> {
    let candidates = serde_json::to_value(candidates).map_err(|error| {
        PostgresStorageError::internal(format!(
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
        .collect::<Vec<_>>();
    let measure_select = plan
        .spec
        .measures()
        .iter()
        .enumerate()
        .map(|(index, measure)| {
            format!(
                "{} AS m{index}_value",
                measure_numeric_sql(measure, "object")
            )
        });
    let projected_values = dimension_select
        .into_iter()
        .chain(measure_select)
        .collect::<Vec<_>>()
        .join(",\n        ");
    let group_columns = (0..expressions.len())
        .flat_map(|index| [format!("d{index}_state"), format!("d{index}_value")])
        .collect::<Vec<_>>();
    let sort_dimensions = (0..expressions.len())
        .map(|index| format!("jsonb_build_array(d{index}_state, d{index}_value)"))
        .collect::<Vec<_>>()
        .join(", ");
    let grouped_dimensions = if group_columns.is_empty() {
        String::new()
    } else {
        format!("{},\n        ", group_columns.join(", "))
    };
    let group_by = if group_columns.is_empty() {
        "GROUP BY ()\n    HAVING COUNT(*) > 0".to_string()
    } else {
        format!("GROUP BY {}", group_columns.join(", "))
    };
    let measure_state = partial_measure_state_sql(plan.spec, "m");
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
        {projected_values}
    FROM visible_filtered_objects AS object
),
grouped_objects AS (
    SELECT
        {grouped_dimensions}COUNT(*) AS object_count,
        {measure_state} AS measure_state
    FROM dimensioned_objects
    {group_by}
),
aggregate_rows AS (
    SELECT
        jsonb_build_array({sort_dimensions}) AS sort_key,
        measure_state,
        object_count
    FROM grouped_objects
)"#
    );
    Ok(ObjectAggregateSqlSpec { sql, binds })
}

fn computed_filter_clause(
    query_options: &QueryOptions,
    snapshot: &ComputedQuerySnapshot,
) -> Result<(String, Vec<ObjectAggregateBindValue>), PostgresStorageError> {
    let mut clauses = Vec::new();
    let mut binds = Vec::new();
    for filter in query_options
        .filters()
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

fn dimension_sql(index: usize, dimension: &StorageObjectAggregateDimension) -> (String, String) {
    dimension_sql_for_source(index, dimension, "object")
}

fn dimension_sql_for_source(
    _index: usize,
    dimension: &StorageObjectAggregateDimension,
    source: &str,
) -> (String, String) {
    match dimension {
        StorageObjectAggregateDimension::Scalar(field) => {
            let column = match field {
                StorageObjectAggregateScalarField::Name => "name",
                StorageObjectAggregateScalarField::Description => "description",
                StorageObjectAggregateScalarField::CollectionId => "collection_id",
                StorageObjectAggregateScalarField::CreatedAt => "created_at",
                StorageObjectAggregateScalarField::UpdatedAt => "updated_at",
            };
            (
                "0::smallint".to_string(),
                format!("to_jsonb({source}.{column})"),
            )
        }
        StorageObjectAggregateDimension::JsonData(path) => {
            let path = json_path_sql(path);
            let value = format!("{source}.data #> ARRAY[{path}]::text[]");
            (
                format!(
                    "CASE WHEN {value} IS NULL THEN 2::smallint WHEN {value} = 'null'::jsonb THEN 1::smallint ELSE 0::smallint END"
                ),
                format!("COALESCE({value}, 'null'::jsonb)"),
            )
        }
        StorageObjectAggregateDimension::Computed(selector) => {
            let key = computed_selector_sql(selector);
            let value = format!("{source}.computed_values -> {source}.id::text -> {key}");
            (
                format!("(({value}) ->> 'state')::smallint"),
                format!("COALESCE(({value}) -> 'value', 'null'::jsonb)"),
            )
        }
    }
}

fn measure_numeric_sql(measure: &StorageObjectAggregateMeasure, source: &str) -> String {
    let value = match measure.field() {
        StorageObjectAggregateMeasureField::JsonData(path) => {
            let path = json_path_sql(path);
            format!("{source}.data #> ARRAY[{path}]::text[]")
        }
        StorageObjectAggregateMeasureField::Computed(selector) => {
            let key = computed_selector_sql(selector);
            format!("{source}.computed_values -> {source}.id::text -> {key} -> 'value'")
        }
    };
    format!("hubuum_computed_numeric({value})")
}

fn partial_measure_state_sql(spec: &StorageObjectAggregateSpec, prefix: &str) -> String {
    let values = spec
        .measures()
        .iter()
        .enumerate()
        .map(|(index, measure)| {
            let source = format!("{prefix}{index}_value");
            let value = grouped_measure_value_sql(measure.operation(), &source);
            format!(
                "jsonb_build_object('value_count', COUNT({source}), 'value', to_jsonb(trim_scale({value})))"
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!("jsonb_build_array({values})")
}

fn direct_measure_response_sql(spec: &StorageObjectAggregateSpec, source: &str) -> String {
    let values = spec
        .measures()
        .iter()
        .map(|measure| {
            let numeric = measure_numeric_sql(measure, source);
            let value_count = format!("COUNT({numeric})");
            let aggregate_value = grouped_measure_value_sql(measure.operation(), &numeric);
            let value = match measure.operation() {
                StorageObjectAggregateMeasureOperation::Average => {
                    format!("{aggregate_value} / NULLIF({value_count}, 0)::numeric")
                }
                StorageObjectAggregateMeasureOperation::Sum
                | StorageObjectAggregateMeasureOperation::Min
                | StorageObjectAggregateMeasureOperation::Max => aggregate_value,
            };
            response_measure_sql(&value_count, &value, "COUNT(*)")
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!("jsonb_build_array({values})")
}

fn grouped_measure_value_sql(
    operation: StorageObjectAggregateMeasureOperation,
    source: &str,
) -> String {
    match operation {
        StorageObjectAggregateMeasureOperation::Sum
        | StorageObjectAggregateMeasureOperation::Average => {
            format!("SUM({source})")
        }
        StorageObjectAggregateMeasureOperation::Min => format!("MIN({source})"),
        StorageObjectAggregateMeasureOperation::Max => format!("MAX({source})"),
    }
}

pub(super) fn merged_measure_state_sql(
    spec: &StorageObjectAggregateSpec,
    left: &str,
    right: &str,
) -> String {
    let values = spec
        .measures()
        .iter()
        .enumerate()
        .map(|(index, measure)| {
            let left_count = format!("({left} #>> '{{{index},value_count}}')::bigint");
            let right_count = format!("({right} #>> '{{{index},value_count}}')::bigint");
            let total_count = format!("({left_count} + {right_count})");
            let left_value = aggregate_state_numeric_sql(left, index);
            let right_value = aggregate_state_numeric_sql(right, index);
            let merged = match measure.operation() {
                StorageObjectAggregateMeasureOperation::Sum
                | StorageObjectAggregateMeasureOperation::Average => format!(
                    "COALESCE({left_value}, 0::numeric) + COALESCE({right_value}, 0::numeric)"
                ),
                StorageObjectAggregateMeasureOperation::Min => {
                    format!("LEAST({left_value}, {right_value})")
                }
                StorageObjectAggregateMeasureOperation::Max => {
                    format!("GREATEST({left_value}, {right_value})")
                }
            };
            format!(
                "jsonb_build_object('value_count', {total_count}, 'value', CASE WHEN {total_count} = 0 THEN 'null'::jsonb ELSE to_jsonb(trim_scale({merged})) END)"
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!("jsonb_build_array({values})")
}

pub(super) fn grouped_measure_state_sql(spec: &StorageObjectAggregateSpec, source: &str) -> String {
    let values = spec
        .measures()
        .iter()
        .enumerate()
        .map(|(index, measure)| {
            let value_count = format!("SUM(({source} #>> '{{{index},value_count}}')::bigint)");
            let source_value = aggregate_state_numeric_sql(source, index);
            let value = grouped_measure_value_sql(measure.operation(), &source_value);
            format!(
                "jsonb_build_object('value_count', {value_count}, 'value', to_jsonb(trim_scale({value})))"
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!("jsonb_build_array({values})")
}

pub(super) fn measure_response_sql(
    spec: &StorageObjectAggregateSpec,
    state: &str,
    object_count: &str,
) -> String {
    let values = spec
        .measures()
        .iter()
        .enumerate()
        .map(|(index, measure)| {
            let value_count = format!("({state} #>> '{{{index},value_count}}')::bigint");
            let stored_value = aggregate_state_numeric_sql(state, index);
            let value = match measure.operation() {
                StorageObjectAggregateMeasureOperation::Average => {
                    format!("{stored_value} / NULLIF({value_count}, 0)::numeric")
                }
                StorageObjectAggregateMeasureOperation::Sum
                | StorageObjectAggregateMeasureOperation::Min
                | StorageObjectAggregateMeasureOperation::Max => stored_value,
            };
            response_measure_sql(&value_count, &value, object_count)
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!("jsonb_build_array({values})")
}

fn response_measure_sql(value_count: &str, value: &str, object_count: &str) -> String {
    format!(
        "jsonb_strip_nulls(jsonb_build_object('state', CASE WHEN {value_count} = 0 THEN 'empty' ELSE 'value' END, 'value_count', {value_count}, 'skipped_count', {object_count} - {value_count}, 'value', CASE WHEN {value_count} = 0 THEN NULL ELSE to_jsonb(trim_scale({value})) END))"
    )
}

fn aggregate_state_numeric_sql(state: &str, index: usize) -> String {
    format!("({state} #>> '{{{index},value}}')::numeric")
}

fn direct_aggregate_sort_key(spec: &StorageObjectAggregateSpec) -> String {
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

fn json_path_sql(path: &JsonFieldPath) -> String {
    path.segments()
        .map(|segment| {
            assert!(
                segment
                    .bytes()
                    .all(|byte| { byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'$') })
            );
            format!("'{segment}'")
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn computed_selector_sql(selector: &StorageComputedFieldSelector) -> String {
    assert!(selector.key().bytes().enumerate().all(|(index, byte)| {
        matches!((index, byte), (0, b'a'..=b'z') | (_, b'a'..=b'z' | b'0'..=b'9' | b'_'))
    }));
    format!(
        "'computed.{}.{}'",
        selector.scope().as_str(),
        selector.key()
    )
}

pub(super) fn append_page_options(
    spec: &mut ObjectAggregateSqlSpec,
    sort: StorageObjectAggregateSort,
    cursor: Option<&StorageObjectAggregateCursor>,
    effective_limit: usize,
) -> Result<(), PostgresStorageError> {
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
    sort: StorageObjectAggregateSort,
    cursor: &StorageObjectAggregateCursor,
) {
    spec.sql.push_str("\nWHERE ");
    match sort {
        StorageObjectAggregateSort::DimensionsAscending => {
            spec.sql.push_str("sort_key > ?::jsonb");
            spec.binds
                .push(ObjectAggregateBindValue::Json(cursor.sort_key().clone()));
        }
        StorageObjectAggregateSort::DimensionsDescending => {
            spec.sql.push_str("sort_key < ?::jsonb");
            spec.binds
                .push(ObjectAggregateBindValue::Json(cursor.sort_key().clone()));
        }
        StorageObjectAggregateSort::ObjectCountAscending => {
            spec.sql
                .push_str("(object_count > ? OR (object_count = ? AND sort_key > ?::jsonb))");
            spec.binds
                .push(ObjectAggregateBindValue::BigInt(cursor.object_count()));
            spec.binds
                .push(ObjectAggregateBindValue::BigInt(cursor.object_count()));
            spec.binds
                .push(ObjectAggregateBindValue::Json(cursor.sort_key().clone()));
        }
        StorageObjectAggregateSort::ObjectCountDescending => {
            spec.sql
                .push_str("(object_count < ? OR (object_count = ? AND sort_key > ?::jsonb))");
            spec.binds
                .push(ObjectAggregateBindValue::BigInt(cursor.object_count()));
            spec.binds
                .push(ObjectAggregateBindValue::BigInt(cursor.object_count()));
            spec.binds
                .push(ObjectAggregateBindValue::Json(cursor.sort_key().clone()));
        }
    }
}

const fn order_clause(sort: StorageObjectAggregateSort) -> &'static str {
    match sort {
        StorageObjectAggregateSort::DimensionsAscending => "sort_key ASC",
        StorageObjectAggregateSort::DimensionsDescending => "sort_key DESC",
        StorageObjectAggregateSort::ObjectCountAscending => "object_count ASC, sort_key ASC",
        StorageObjectAggregateSort::ObjectCountDescending => "object_count DESC, sort_key ASC",
    }
}

fn page_query_limit(effective_limit: usize) -> Result<i64, PostgresStorageError> {
    i64::try_from(effective_limit.saturating_add(1)).map_err(|_| {
        PostgresStorageError::invalid_input("Object aggregate page limit is too large")
    })
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    #[test]
    fn json_dimension_sql_distinguishes_value_null_and_missing() {
        let dimension =
            StorageObjectAggregateDimension::from_str("json_data.location,country").unwrap();
        let (state, value) = dimension_sql(0, &dimension);
        assert!(state.contains("IS NULL THEN 2"));
        assert!(state.contains("= 'null'::jsonb THEN 1"));
        assert!(value.contains("COALESCE"));
    }

    #[test]
    fn count_sort_always_uses_complete_dimension_tie_breaker() {
        assert_eq!(
            order_clause(StorageObjectAggregateSort::ObjectCountDescending),
            "object_count DESC, sort_key ASC"
        );
    }
}
