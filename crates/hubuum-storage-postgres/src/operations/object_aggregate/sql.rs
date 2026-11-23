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
use hubuum_query::QueryOptions;
use hubuum_storage_core::{StorageObjectAggregateSort, StorageObjectAggregateSpec};

use super::expressions::{
    ObjectAggregateBindValue, ObjectAggregateSqlSpec, bind_object_aggregate_query, dimension_sql,
    direct_aggregate_sort_key, direct_measure_response_sql, measure_numeric_sql, page_query_limit,
    partial_measure_state_sql,
};
use crate::operations::computed_objects::query::{
    ComputedQuerySnapshot, computed_filter_sql_component,
};
use crate::{PostgresConnection, PostgresStorageError};

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
        .filter(|filter| filter.field().computed_query().is_some())
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
