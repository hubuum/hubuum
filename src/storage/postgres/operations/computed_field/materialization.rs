use super::*;

fn shared_definitions_from_rows(
    definitions: &[ComputedFieldDefinition],
) -> Result<Vec<hubuum_computed_fields::Definition>, ApiError> {
    definitions
        .iter()
        .filter(|definition| definition.enabled)
        .map(ComputedFieldDefinition::evaluator_definition)
        .collect()
}

pub(crate) fn evaluate_definitions(
    data: &serde_json::Value,
    definitions: &[ComputedFieldDefinition],
    maximum: usize,
    scope: &'static str,
) -> Result<EvaluationResult, ApiError> {
    let definitions = shared_definitions_from_rows(definitions)?;
    let result =
        evaluate(data, &definitions, maximum, EvaluationLimits::standard()).map_err(|error| {
            ApiError::InternalServerError(format!("Computed-field evaluation failed: {error}"))
        })?;
    crate::observability::metrics::computed_evaluation(scope, &result);
    Ok(result)
}

pub fn source_data_sha256(data: &serde_json::Value) -> Result<String, ApiError> {
    hubuum_storage_postgres::operations::computed_materialization::source_data_sha256(data)
        .map_err(hubuum_storage_core::StorageError::from)
        .map_err(ApiError::from)
}

pub(super) async fn upsert_materialized(
    conn: &mut PostgresConnection,
    object: &HubuumObject,
    revision: i64,
    result: EvaluationResult,
) -> Result<(), ApiError> {
    use crate::schema::object_computed_data::dsl::{
        class_id, computed_at, errors, evaluation_revision, object_computed_data, object_id,
        source_data_sha256 as stored_hash, values,
    };
    let input = NewObjectComputedData {
        object_id: object.id,
        class_id: object.hubuum_class_id,
        evaluation_revision: revision,
        source_data_sha256: source_data_sha256(&object.data)?,
        values: serde_json::to_value(result.values)?,
        errors: serde_json::to_value(result.errors)?,
    };
    diesel::insert_into(object_computed_data)
        .values(&input)
        .on_conflict(object_id)
        .do_update()
        .set((
            class_id.eq(input.class_id),
            evaluation_revision.eq(input.evaluation_revision),
            stored_hash.eq(&input.source_data_sha256),
            values.eq(&input.values),
            errors.eq(&input.errors),
            computed_at.eq(diesel::dsl::now),
        ))
        .execute(conn)
        .await?;
    Ok(())
}

pub(super) async fn shared_definitions_conn(
    conn: &mut PostgresConnection,
    target_class_id: i32,
) -> Result<Vec<ComputedFieldDefinition>, ApiError> {
    use crate::schema::computed_field_definitions::dsl::{
        class_id, computed_field_definitions, id, visibility,
    };
    Ok(computed_field_definitions
        .filter(class_id.eq(target_class_id))
        .filter(visibility.eq(COMPUTED_FIELD_VISIBILITY_SHARED))
        .order(id.asc())
        .select(ComputedFieldDefinition::as_select())
        .load(conn)
        .await?)
}

/// Materialize one canonical object inside the caller's write transaction.
pub(crate) async fn materialize_object_in_transaction(
    conn: &mut PostgresConnection,
    object: &HubuumObject,
) -> Result<(), ApiError> {
    acquire_computed_class_shared_lock(conn, object.hubuum_class_id).await?;
    let definitions = shared_definitions_conn(conn, object.hubuum_class_id).await?;
    if definitions.is_empty() {
        use crate::schema::object_computed_data::dsl::{object_computed_data, object_id};
        diesel::delete(object_computed_data.filter(object_id.eq(object.id)))
            .execute(conn)
            .await?;
        return Ok(());
    }
    let state = ensure_computation_state(conn, object.hubuum_class_id).await?;
    let result =
        evaluate_definitions(&object.data, &definitions, MAX_SHARED_DEFINITIONS, "shared")?;
    upsert_materialized(conn, object, state.evaluation_revision, result).await
}
