//! PostgreSQL materialization used by canonical object writes.

use diesel::prelude::{ExpressionMethods, QueryDsl};
use diesel::{Insertable, Queryable, Selectable, SelectableHelper};
use diesel_async::RunQueryDsl;
use hubuum_computed_fields::MAX_SHARED_DEFINITIONS;
use sha2::{Digest, Sha256};

use crate::operations::computed_definition::{
    ComputedDefinitionRow, SHARED_VISIBILITY, evaluate_definitions,
};
use crate::{PostgresConnection, PostgresStorageError};

const COMPUTED_CLASS_LOCK_NAMESPACE: i32 = 1_133_113;

/// Borrowed canonical object data required for computed materialization.
#[derive(Clone, Copy, Debug)]
pub(crate) struct ObjectMaterializationInput<'value> {
    object_id: i32,
    class_id: i32,
    data: &'value serde_json::Value,
}

impl<'value> ObjectMaterializationInput<'value> {
    pub(crate) const fn new(
        object_id: i32,
        class_id: i32,
        data: &'value serde_json::Value,
    ) -> Self {
        Self {
            object_id,
            class_id,
            data,
        }
    }
}

/// Bounded labels recorded after one computed evaluation.
#[derive(Debug, Default)]
pub(crate) struct ComputedEvaluationSummary {
    error_codes: Vec<&'static str>,
}

impl ComputedEvaluationSummary {
    pub(crate) fn error_codes(&self) -> &[&'static str] {
        &self.error_codes
    }
}

#[derive(Queryable, Selectable)]
#[diesel(table_name = crate::schema::class_computation_state)]
struct ComputationStateRow {
    evaluation_revision: i64,
}

#[derive(Insertable)]
#[diesel(table_name = crate::schema::object_computed_data)]
struct NewMaterializedObjectRow {
    object_id: i32,
    class_id: i32,
    evaluation_revision: i64,
    source_data_sha256: String,
    values: serde_json::Value,
    errors: serde_json::Value,
}

/// Acquire the class lock shared by object writes and computed-definition work.
#[doc(hidden)]
pub async fn acquire_computed_class_shared_lock(
    connection: &mut PostgresConnection,
    class_id: i32,
) -> Result<(), PostgresStorageError> {
    use diesel::sql_types::Integer;

    diesel::sql_query("SELECT pg_advisory_xact_lock_shared($1, $2)")
        .bind::<Integer, _>(COMPUTED_CLASS_LOCK_NAMESPACE)
        .bind::<Integer, _>(class_id)
        .execute(connection)
        .await?;
    Ok(())
}

/// Acquire the class lock used by computed-definition mutations.
#[doc(hidden)]
pub async fn acquire_computed_class_exclusive_lock(
    connection: &mut PostgresConnection,
    class_id: i32,
) -> Result<(), PostgresStorageError> {
    use diesel::sql_types::Integer;

    diesel::sql_query("SELECT pg_advisory_xact_lock($1, $2)")
        .bind::<Integer, _>(COMPUTED_CLASS_LOCK_NAMESPACE)
        .bind::<Integer, _>(class_id)
        .execute(connection)
        .await?;
    Ok(())
}

/// Materialize one canonical object inside the caller's write transaction.
pub(crate) async fn materialize_object(
    connection: &mut PostgresConnection,
    object: ObjectMaterializationInput<'_>,
) -> Result<Option<ComputedEvaluationSummary>, PostgresStorageError> {
    acquire_computed_class_shared_lock(connection, object.class_id).await?;
    let definitions = shared_definitions(connection, object.class_id).await?;
    if definitions.is_empty() {
        diesel::delete(
            crate::schema::object_computed_data::table
                .filter(crate::schema::object_computed_data::object_id.eq(object.object_id)),
        )
        .execute(connection)
        .await?;
        return Ok(None);
    }
    let state = ensure_computation_state(connection, object.class_id).await?;
    let (input, summary) =
        evaluate_materialized_object(object, state.evaluation_revision, &definitions)?;
    upsert_materialized_object(connection, &input).await?;
    Ok(Some(summary))
}

/// Materialize one object in a caller-owned PostgreSQL transaction.
///
/// This narrow integration hook exists for PostgreSQL-owned import workflows
/// that have not yet moved into this crate. Backend-neutral consumers use the
/// object lifecycle traits instead.
#[doc(hidden)]
pub async fn materialize_object_on_connection(
    connection: &mut PostgresConnection,
    object_id: i32,
    class_id: i32,
    data: &serde_json::Value,
) -> Result<Option<Vec<&'static str>>, PostgresStorageError> {
    materialize_object(
        connection,
        ObjectMaterializationInput::new(object_id, class_id, data),
    )
    .await
    .map(|summary| summary.map(|summary| summary.error_codes))
}

/// Rebuild a bounded object batch after the caller validates the task lease
/// and acquires the shared computed-class lock.
pub(crate) async fn rebuild_objects(
    connection: &mut PostgresConnection,
    class_id: i32,
    evaluation_revision: i64,
    objects: &[ObjectMaterializationInput<'_>],
) -> Result<Vec<ComputedEvaluationSummary>, PostgresStorageError> {
    if objects.iter().any(|object| object.class_id != class_id) {
        return Err(PostgresStorageError::internal(
            "Computed rebuild batch contains an object from another class",
        ));
    }
    let definitions = shared_definitions(connection, class_id).await?;
    if definitions.is_empty() {
        let object_ids = objects
            .iter()
            .map(|object| object.object_id)
            .collect::<Vec<_>>();
        diesel::delete(
            crate::schema::object_computed_data::table
                .filter(crate::schema::object_computed_data::object_id.eq_any(object_ids)),
        )
        .execute(connection)
        .await?;
        return Ok(Vec::new());
    }
    let mut summaries = Vec::with_capacity(objects.len());
    for object in objects {
        let (input, summary) =
            evaluate_materialized_object(*object, evaluation_revision, &definitions)?;
        upsert_materialized_object(connection, &input).await?;
        summaries.push(summary);
    }
    Ok(summaries)
}

fn evaluate_materialized_object(
    object: ObjectMaterializationInput<'_>,
    evaluation_revision: i64,
    definitions: &[ComputedDefinitionRow],
) -> Result<(NewMaterializedObjectRow, ComputedEvaluationSummary), PostgresStorageError> {
    let result = evaluate_definitions(object.data, definitions, MAX_SHARED_DEFINITIONS)?;
    let summary = ComputedEvaluationSummary {
        error_codes: result
            .errors
            .values()
            .map(|error| error.code.as_str())
            .collect(),
    };
    Ok((
        NewMaterializedObjectRow {
            object_id: object.object_id,
            class_id: object.class_id,
            evaluation_revision,
            source_data_sha256: source_data_sha256(object.data)?,
            values: serde_json::to_value(result.values)
                .map_err(|error| PostgresStorageError::internal(error.to_string()))?,
            errors: serde_json::to_value(result.errors)
                .map_err(|error| PostgresStorageError::internal(error.to_string()))?,
        },
        summary,
    ))
}

async fn upsert_materialized_object(
    connection: &mut PostgresConnection,
    input: &NewMaterializedObjectRow,
) -> Result<(), PostgresStorageError> {
    diesel::insert_into(crate::schema::object_computed_data::table)
        .values(input)
        .on_conflict(crate::schema::object_computed_data::object_id)
        .do_update()
        .set((
            crate::schema::object_computed_data::class_id.eq(input.class_id),
            crate::schema::object_computed_data::evaluation_revision.eq(input.evaluation_revision),
            crate::schema::object_computed_data::source_data_sha256.eq(&input.source_data_sha256),
            crate::schema::object_computed_data::values.eq(&input.values),
            crate::schema::object_computed_data::errors.eq(&input.errors),
            crate::schema::object_computed_data::computed_at.eq(diesel::dsl::now),
        ))
        .execute(connection)
        .await?;
    Ok(())
}

async fn shared_definitions(
    connection: &mut PostgresConnection,
    class_id: i32,
) -> Result<Vec<ComputedDefinitionRow>, PostgresStorageError> {
    crate::schema::computed_field_definitions::table
        .filter(crate::schema::computed_field_definitions::class_id.eq(class_id))
        .filter(crate::schema::computed_field_definitions::visibility.eq(SHARED_VISIBILITY))
        .order(crate::schema::computed_field_definitions::id.asc())
        .select(ComputedDefinitionRow::as_select())
        .load(connection)
        .await
        .map_err(PostgresStorageError::from)
}

async fn ensure_computation_state(
    connection: &mut PostgresConnection,
    target_class_id: i32,
) -> Result<ComputationStateRow, PostgresStorageError> {
    diesel::insert_into(crate::schema::class_computation_state::table)
        .values(crate::schema::class_computation_state::class_id.eq(target_class_id))
        .on_conflict(crate::schema::class_computation_state::class_id)
        .do_nothing()
        .execute(connection)
        .await?;
    crate::schema::class_computation_state::table
        .filter(crate::schema::class_computation_state::class_id.eq(target_class_id))
        .select(ComputationStateRow::as_select())
        .first(connection)
        .await
        .map_err(PostgresStorageError::from)
}

/// Return the canonical hash stored beside materialized computed values.
#[doc(hidden)]
pub fn source_data_sha256(data: &serde_json::Value) -> Result<String, PostgresStorageError> {
    let mut canonical = String::new();
    canonical_json(data, &mut canonical)?;
    Ok(Sha256::digest(canonical.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect())
}

fn canonical_json(
    value: &serde_json::Value,
    output: &mut String,
) -> Result<(), PostgresStorageError> {
    match value {
        serde_json::Value::Object(values) => {
            output.push('{');
            let mut keys = values.keys().collect::<Vec<_>>();
            keys.sort_unstable();
            for (index, key) in keys.into_iter().enumerate() {
                if index > 0 {
                    output.push(',');
                }
                output.push_str(
                    &serde_json::to_string(key)
                        .map_err(|error| PostgresStorageError::internal(error.to_string()))?,
                );
                output.push(':');
                canonical_json(&values[key], output)?;
            }
            output.push('}');
        }
        serde_json::Value::Array(values) => {
            output.push('[');
            for (index, value) in values.iter().enumerate() {
                if index > 0 {
                    output.push(',');
                }
                canonical_json(value, output)?;
            }
            output.push(']');
        }
        _ => output.push_str(
            &serde_json::to_string(value)
                .map_err(|error| PostgresStorageError::internal(error.to_string()))?,
        ),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_hash_ignores_object_key_order() {
        let left = serde_json::json!({"a": 1, "b": 2});
        let right = serde_json::json!({"b": 2, "a": 1});

        assert_eq!(
            source_data_sha256(&left).unwrap(),
            source_data_sha256(&right).unwrap()
        );
    }
}
