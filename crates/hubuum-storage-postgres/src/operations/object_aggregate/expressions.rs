use crate::PostgresStorageError;
use crate::operations::dynamic_sql::{SqlValue, indexed_bind_placeholders};
use hubuum_query::JsonFieldPath;
use hubuum_storage_core::{
    StorageComputedFieldSelector, StorageObjectAggregateCursor, StorageObjectAggregateDimension,
    StorageObjectAggregateMeasure, StorageObjectAggregateMeasureField,
    StorageObjectAggregateMeasureOperation, StorageObjectAggregateScalarField,
    StorageObjectAggregateSort, StorageObjectAggregateSpec,
};

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

pub(super) fn dimension_sql(
    index: usize,
    dimension: &StorageObjectAggregateDimension,
) -> (String, String) {
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

pub(super) fn measure_numeric_sql(measure: &StorageObjectAggregateMeasure, source: &str) -> String {
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

pub(super) fn partial_measure_state_sql(spec: &StorageObjectAggregateSpec, prefix: &str) -> String {
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

pub(super) fn direct_measure_response_sql(
    spec: &StorageObjectAggregateSpec,
    source: &str,
) -> String {
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

pub(super) fn direct_aggregate_sort_key(spec: &StorageObjectAggregateSpec) -> String {
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

pub(super) fn page_query_limit(effective_limit: usize) -> Result<i64, PostgresStorageError> {
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
