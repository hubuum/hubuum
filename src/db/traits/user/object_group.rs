use std::collections::HashSet;

use diesel::sql_types::{Array, BigInt, Integer, Jsonb};
use diesel_async::RunQueryDsl;

use super::UserCollectionAccessors;
use crate::db::prelude::*;
use crate::db::traits::computed_field::enrich_objects_with_computed;
use crate::db::{DbPool, with_connection};
use crate::errors::ApiError;
use crate::models::computed_field::{
    COMPUTED_FIELD_VISIBILITY_PERSONAL, COMPUTED_FIELD_VISIBILITY_SHARED, ComputedFieldDefinition,
    HubuumObjectComputedResponse,
};
use crate::models::object::HubuumObject;
use crate::models::object_group::{
    ComputedFieldScope, ObjectGroupBackendRequest, ObjectGroupDimension, ObjectGroupPage,
    ObjectGroupRow, ObjectGroupScalarField, ObjectGroupSort, ObjectGroupSpec,
};
use crate::models::search::QueryOptions;
use crate::pagination::{SKIPPED_TOTAL_COUNT, effective_page_limit};
use crate::utilities::extensions::CustomStringExtensions;

#[derive(Debug, Clone)]
enum ObjectGroupBindValue {
    IntegerArray(Vec<i32>),
    Json(serde_json::Value),
    BigInt(i64),
}

#[derive(Debug, Clone)]
struct ObjectGroupSqlSpec {
    sql: String,
    binds: Vec<ObjectGroupBindValue>,
}

impl ObjectGroupSqlSpec {
    fn indexed(self) -> Self {
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
                ObjectGroupBindValue::IntegerArray(values) => {
                    query.bind::<Array<Integer>, _>(values)
                }
                ObjectGroupBindValue::Json(value) => query.bind::<Jsonb, _>(value),
                ObjectGroupBindValue::BigInt(value) => query.bind::<BigInt, _>(value),
            };
        }
        query
    }};
}

#[derive(diesel::QueryableByName)]
struct ObjectGroupDatabaseRow {
    #[diesel(sql_type = Jsonb)]
    dimensions: serde_json::Value,
    #[diesel(sql_type = Jsonb)]
    sort_key: serde_json::Value,
    #[diesel(sql_type = BigInt)]
    object_count: i64,
}

#[derive(diesel::QueryableByName)]
struct ObjectGroupCountRow {
    #[diesel(sql_type = BigInt)]
    count: i64,
}

pub trait ObjectGroupBackend: UserCollectionAccessors {
    async fn group_objects_from_backend(
        &self,
        pool: &DbPool,
        request: ObjectGroupBackendRequest,
    ) -> Result<ObjectGroupPage, ApiError> {
        let (class_id, candidates, query_options, spec, personal_owner_id) = request.into_parts();
        tracing::debug!(
            message = "Grouping visible filtered objects",
            user_id = self.principal_id(),
            candidate_count = candidates.len(),
            dimensions = ?spec
                .dimensions()
                .iter()
                .map(ObjectGroupDimension::canonical)
                .collect::<Vec<_>>()
        );
        group_visible_filtered_objects(
            pool,
            class_id,
            candidates,
            &query_options,
            &spec,
            personal_owner_id,
        )
        .await
    }
}

impl<T> ObjectGroupBackend for T where T: UserCollectionAccessors + ?Sized {}

async fn group_visible_filtered_objects(
    pool: &DbPool,
    class_id: i32,
    candidates: Vec<HubuumObject>,
    query_options: &QueryOptions,
    spec: &ObjectGroupSpec,
    personal_owner_id: Option<i32>,
) -> Result<ObjectGroupPage, ApiError> {
    let effective_limit = effective_page_limit(query_options)?;
    validate_computed_selectors(pool, class_id, spec, personal_owner_id).await?;
    if candidates.is_empty() {
        return Ok(ObjectGroupPage::new(
            Vec::new(),
            if query_options.include_total {
                0
            } else {
                SKIPPED_TOTAL_COUNT
            },
            None,
        ));
    }

    let class_ids = candidates
        .iter()
        .map(|object| object.hubuum_class_id)
        .collect::<HashSet<_>>();
    if class_ids.len() != 1 {
        return Err(ApiError::InternalServerError(
            "Object group candidates must belong to exactly one class".to_string(),
        ));
    }
    if !class_ids.contains(&class_id) {
        return Err(ApiError::InternalServerError(
            "Object group candidates do not belong to the requested class".to_string(),
        ));
    }

    let (object_ids, computed_payload) = if spec.has_computed_dimension() {
        computed_group_payload(pool, candidates, spec, personal_owner_id).await?
    } else {
        (candidates.iter().map(|object| object.id).collect(), None)
    };
    let base = build_group_ctes(object_ids, computed_payload, spec);

    let total_count = if query_options.include_total {
        let count_spec = ObjectGroupSqlSpec {
            sql: format!("{}\nSELECT COUNT(*) AS count FROM group_rows", base.sql),
            binds: base.binds.clone(),
        };
        with_connection(pool, async |conn| {
            bind_object_group_query!(count_spec)
                .get_result::<ObjectGroupCountRow>(conn)
                .await
        })
        .await?
        .count
    } else {
        SKIPPED_TOTAL_COUNT
    };

    let mut page_spec = base;
    page_spec
        .sql
        .push_str("\nSELECT dimensions, sort_key, object_count\nFROM group_rows");
    if let Some(cursor) = query_options.cursor.as_deref() {
        let cursor = spec.decode_cursor(cursor)?;
        append_cursor_clause(&mut page_spec, spec.sort(), cursor);
    }
    page_spec.sql.push_str("\nORDER BY ");
    page_spec.sql.push_str(order_clause(spec.sort()));
    page_spec.sql.push_str("\nLIMIT ?");
    page_spec.binds.push(ObjectGroupBindValue::BigInt(
        i64::try_from(effective_limit.saturating_add(1)).map_err(|_| {
            ApiError::BadRequest("Object group page limit is too large".to_string())
        })?,
    ));

    let database_rows = with_connection(pool, async |conn| {
        bind_object_group_query!(page_spec)
            .load::<ObjectGroupDatabaseRow>(conn)
            .await
    })
    .await?;
    let mut rows = database_rows
        .into_iter()
        .map(|row| ObjectGroupRow::from_database(row.dimensions, row.object_count, row.sort_key))
        .collect::<Result<Vec<_>, _>>()?;
    let has_more = rows.len() > effective_limit;
    if has_more {
        rows.truncate(effective_limit);
    }
    let next_cursor = if has_more {
        rows.last().map(|row| spec.encode_cursor(row)).transpose()?
    } else {
        None
    };

    Ok(ObjectGroupPage::new(rows, total_count, next_cursor))
}

async fn validate_computed_selectors(
    pool: &DbPool,
    class_id_value: i32,
    spec: &ObjectGroupSpec,
    personal_owner_id: Option<i32>,
) -> Result<(), ApiError> {
    let selectors = spec
        .dimensions()
        .iter()
        .filter_map(ObjectGroupDimension::computed_selector)
        .collect::<Vec<_>>();
    if selectors.is_empty() {
        return Ok(());
    }

    let definitions = with_connection(pool, async |conn| {
        use crate::schema::computed_field_definitions::dsl::{
            class_id, computed_field_definitions, id,
        };
        computed_field_definitions
            .filter(class_id.eq(class_id_value))
            .order(id.asc())
            .select(ComputedFieldDefinition::as_select())
            .load::<ComputedFieldDefinition>(conn)
            .await
    })
    .await?;

    for selector in selectors {
        let definition = definitions
            .iter()
            .find(|definition| match selector.scope() {
                ComputedFieldScope::Shared => {
                    definition.visibility == COMPUTED_FIELD_VISIBILITY_SHARED
                        && definition.key == selector.key()
                }
                ComputedFieldScope::Personal => {
                    definition.visibility == COMPUTED_FIELD_VISIBILITY_PERSONAL
                        && definition.owner_user_id == personal_owner_id
                        && definition.key == selector.key()
                }
            });
        let Some(definition) = definition else {
            return Err(ApiError::BadRequest(format!(
                "Computed group dimension '{}' does not name an accessible field in class {class_id_value}",
                selector.canonical()
            )));
        };
        if !definition.enabled {
            return Err(ApiError::BadRequest(format!(
                "Computed group dimension '{}' is disabled",
                selector.canonical()
            )));
        }
        definition.evaluator_definition()?;
    }
    Ok(())
}

async fn computed_group_payload(
    pool: &DbPool,
    candidates: Vec<HubuumObject>,
    spec: &ObjectGroupSpec,
    personal_owner_id: Option<i32>,
) -> Result<(Vec<i32>, Option<serde_json::Value>), ApiError> {
    let enriched = enrich_objects_with_computed(pool, candidates, personal_owner_id).await?;
    let object_ids = enriched.iter().map(|row| row.object.id).collect::<Vec<_>>();
    let payload = enriched
        .iter()
        .map(|row| {
            let values = spec
                .dimensions()
                .iter()
                .map(|dimension| computed_dimension_value(row, dimension))
                .collect::<Vec<_>>();
            (row.object.id.to_string(), serde_json::Value::Array(values))
        })
        .collect::<serde_json::Map<_, _>>();
    Ok((object_ids, Some(serde_json::Value::Object(payload))))
}

fn computed_dimension_value(
    row: &HubuumObjectComputedResponse,
    dimension: &ObjectGroupDimension,
) -> serde_json::Value {
    let Some(selector) = dimension.computed_selector() else {
        return serde_json::Value::Null;
    };
    let (values, has_error) = match selector.scope() {
        ComputedFieldScope::Shared => (
            &row.computed.shared.values,
            row.computed.shared.errors.contains_key(selector.key()),
        ),
        ComputedFieldScope::Personal => match row.computed.personal.as_ref() {
            Some(personal) => (
                &personal.values,
                personal.errors.contains_key(selector.key()),
            ),
            None => return serde_json::json!({"state": 3, "value": null}),
        },
    };
    if has_error {
        return serde_json::json!({"state": 3, "value": null});
    }
    match values.get(selector.key()) {
        Some(serde_json::Value::Null) => serde_json::json!({"state": 1, "value": null}),
        Some(value) => serde_json::json!({"state": 0, "value": value}),
        None => serde_json::json!({"state": 3, "value": null}),
    }
}

fn build_group_ctes(
    object_ids: Vec<i32>,
    computed_payload: Option<serde_json::Value>,
    spec: &ObjectGroupSpec,
) -> ObjectGroupSqlSpec {
    let mut binds = vec![ObjectGroupBindValue::IntegerArray(object_ids)];
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
    SELECT ?::integer[] AS object_ids{computed_input}
),
visible_filtered_objects AS (
    SELECT object.*{computed_column}
    FROM hubuumobject AS object
    CROSS JOIN query_input AS input
    WHERE object.id = ANY(input.object_ids)
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
    ObjectGroupSqlSpec { sql, binds }
}

fn dimension_sql(index: usize, dimension: &ObjectGroupDimension) -> (String, String) {
    match dimension {
        ObjectGroupDimension::Scalar(field) => {
            let column = match field {
                ObjectGroupScalarField::Name => "object.name",
                ObjectGroupScalarField::Description => "object.description",
                ObjectGroupScalarField::CollectionId => "object.collection_id",
                ObjectGroupScalarField::CreatedAt => "object.created_at",
                ObjectGroupScalarField::UpdatedAt => "object.updated_at",
            };
            ("0::smallint".to_string(), format!("to_jsonb({column})"))
        }
        ObjectGroupDimension::JsonData(path) => {
            let path = path
                .segments()
                .iter()
                .map(|segment| sql_string_literal(segment))
                .collect::<Vec<_>>()
                .join(", ");
            let value = format!("object.data #> ARRAY[{path}]::text[]");
            (
                format!(
                    "CASE WHEN {value} IS NULL THEN 2::smallint WHEN {value} = 'null'::jsonb THEN 1::smallint ELSE 0::smallint END"
                ),
                format!("COALESCE({value}, 'null'::jsonb)"),
            )
        }
        ObjectGroupDimension::Computed(_) => {
            let value = format!("object.computed_values -> object.id::text -> {index}");
            (
                format!("(({value}) ->> 'state')::smallint"),
                format!("COALESCE(({value}) -> 'value', 'null'::jsonb)"),
            )
        }
    }
}

fn response_dimension_sql(index: usize, dimension: &ObjectGroupDimension) -> String {
    let field = sql_string_literal(&dimension.canonical());
    let state = format!("d{index}_state");
    let value = format!("d{index}_value");
    format!(
        "CASE {state} WHEN 0 THEN jsonb_build_object('field', {field}, 'state', 'value', 'value', {value}) WHEN 1 THEN jsonb_build_object('field', {field}, 'state', 'null') WHEN 2 THEN jsonb_build_object('field', {field}, 'state', 'missing') ELSE jsonb_build_object('field', {field}, 'state', 'unavailable') END"
    )
}

fn sql_string_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn append_cursor_clause(
    spec: &mut ObjectGroupSqlSpec,
    sort: ObjectGroupSort,
    cursor: crate::models::object_group::DecodedObjectGroupCursor,
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
