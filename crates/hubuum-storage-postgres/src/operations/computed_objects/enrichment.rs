use std::collections::{BTreeMap, BTreeSet, HashMap};

use diesel::prelude::{BoolExpressionMethods, ExpressionMethods, QueryDsl};
use diesel::{Queryable, Selectable, SelectableHelper};
use diesel_async::RunQueryDsl;
use hubuum_computed_fields::{
    EvaluationResult, FieldError, MAX_PERSONAL_DEFINITIONS, MAX_SHARED_DEFINITIONS,
};
use hubuum_query::ComputedQueryValueType;
use hubuum_storage_core::{
    ComputedObjectEnrichmentQuery, StorageComputationRevision, StorageComputedFieldError,
    StorageComputedObject, StorageComputedScope, StorageObject, StorageSharedComputedScope,
};

use super::query::ComputedQuerySnapshot;
use crate::operations::computed_definition::{
    ComputedDefinitionRow, PERSONAL_VISIBILITY, SHARED_VISIBILITY, evaluate_definitions,
};
use crate::operations::computed_materialization::{
    ObjectMaterializationInput, acquire_computed_class_shared_lock, materialize_object,
    source_data_sha256,
};
use crate::operations::object::ObjectRow;
use crate::{PostgresConnection, PostgresRuntime, PostgresStorageError};

#[derive(Clone, Queryable, Selectable)]
#[diesel(table_name = crate::schema::class_computation_state)]
struct ComputationStateRow {
    class_id: i32,
    evaluation_revision: i64,
}

#[derive(Clone, Queryable, Selectable)]
#[diesel(table_name = crate::schema::object_computed_data)]
struct MaterializedObjectRow {
    object_id: i32,
    class_id: i32,
    evaluation_revision: i64,
    source_data_sha256: String,
    values: serde_json::Value,
    errors: serde_json::Value,
}

type EvaluationMaps = (
    BTreeMap<String, serde_json::Value>,
    BTreeMap<String, StorageComputedFieldError>,
);

pub(super) async fn enrich_objects(
    runtime: &PostgresRuntime,
    query: ComputedObjectEnrichmentQuery,
) -> Result<Vec<StorageComputedObject>, PostgresStorageError> {
    let (objects, personal_owner_id) = query.into_parts();
    if objects.is_empty() {
        return Ok(Vec::new());
    }
    let personal_owner_id = personal_owner_id.map(|id| id.id());
    validate_owner_id(personal_owner_id)?;
    let snapshot_runtime = runtime.clone();
    let (enriched, stale_objects) = runtime
        .with_read_only_snapshot(async move |connection| {
            let class_ids = objects
                .iter()
                .map(|object| object.class_id().id())
                .collect::<BTreeSet<_>>();
            for class_id in &class_ids {
                acquire_computed_class_shared_lock(connection, *class_id).await?;
            }
            let object_ids = objects
                .iter()
                .map(|object| object.id().id())
                .collect::<Vec<_>>();
            let class_ids = class_ids.into_iter().collect::<Vec<_>>();
            let definitions = load_definitions(connection, &class_ids, personal_owner_id).await?;
            let states = load_states(connection, &class_ids).await?;
            let materialized = load_materialized(connection, &object_ids).await?;
            enrich_from_rows(
                &snapshot_runtime,
                objects,
                personal_owner_id,
                definitions,
                states,
                materialized,
                true,
            )
        })
        .await?;

    if !stale_objects.is_empty() {
        match repair_stale_materializations(runtime, stale_objects).await {
            Ok(()) => runtime.record_computed_read_repair("success"),
            Err(error) => {
                runtime.record_computed_read_repair("failure");
                tracing::warn!(
                    operation = "computed_read_repair",
                    backend = "postgresql",
                    error_kind = error.kind().as_str(),
                    "computed-field read repair failed"
                );
            }
        }
    }
    Ok(enriched)
}

pub(super) async fn enrich_with_query_snapshot(
    runtime: &PostgresRuntime,
    connection: &mut PostgresConnection,
    objects: Vec<StorageObject>,
    personal_owner_id: Option<i32>,
    snapshot: &ComputedQuerySnapshot,
) -> Result<Vec<StorageComputedObject>, PostgresStorageError> {
    if objects.is_empty() {
        return Ok(Vec::new());
    }
    if objects
        .iter()
        .any(|object| object.class_id().id() != snapshot.class_id())
    {
        return Err(PostgresStorageError::internal(
            "Computed sort snapshot cannot enrich objects from another class",
        ));
    }
    let object_ids = objects
        .iter()
        .map(|object| object.id().id())
        .collect::<Vec<_>>();
    let materialized = load_materialized(connection, &object_ids).await?;
    let states = vec![ComputationStateRow {
        class_id: snapshot.class_id(),
        evaluation_revision: snapshot.evaluation_revision(),
    }];
    enrich_from_rows(
        runtime,
        objects,
        personal_owner_id,
        snapshot.definitions().to_vec(),
        states,
        materialized,
        false,
    )
    .map(|(objects, _)| objects)
}

fn enrich_from_rows(
    runtime: &PostgresRuntime,
    objects: Vec<StorageObject>,
    personal_owner_id: Option<i32>,
    definitions: Vec<ComputedDefinitionRow>,
    states: Vec<ComputationStateRow>,
    materialized: Vec<MaterializedObjectRow>,
    collect_stale: bool,
) -> Result<(Vec<StorageComputedObject>, Vec<StorageObject>), PostgresStorageError> {
    let mut shared_by_class = HashMap::<i32, Vec<ComputedDefinitionRow>>::new();
    let mut personal_by_class = HashMap::<i32, Vec<ComputedDefinitionRow>>::new();
    for definition in definitions {
        if definition.is_shared() {
            shared_by_class
                .entry(definition.class_id())
                .or_default()
                .push(definition);
        } else if personal_owner_id.is_some_and(|owner| definition.is_personal_for(owner)) {
            personal_by_class
                .entry(definition.class_id())
                .or_default()
                .push(definition);
        }
    }
    let states = states
        .into_iter()
        .map(|state| (state.class_id, state.evaluation_revision))
        .collect::<HashMap<_, _>>();
    let materialized = materialized
        .into_iter()
        .map(|row| (row.object_id, row))
        .collect::<HashMap<_, _>>();

    let mut stale_objects = Vec::new();
    let mut enriched = Vec::with_capacity(objects.len());
    for object in objects {
        let evaluation_revision = states.get(&object.class_id().id()).copied().unwrap_or(0);
        let shared_definitions = shared_by_class
            .get(&object.class_id().id())
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        let hash = source_data_sha256(object.data())?;
        let stored = materialized.get(&object.id().id());
        let has_enabled_definitions = shared_definitions
            .iter()
            .any(ComputedDefinitionRow::enabled);
        let stored_maps = match stored {
            Some(row)
                if row.class_id == object.class_id().id()
                    && row.evaluation_revision == evaluation_revision
                    && row.source_data_sha256 == hash =>
            {
                valid_stored_evaluation_maps(row, shared_definitions)?
            }
            _ => None,
        };
        let fresh = !has_enabled_definitions || stored_maps.is_some();
        let shared = if !has_enabled_definitions {
            StorageComputedScope::default()
        } else if let Some((values, errors)) = stored_maps {
            StorageComputedScope::new(values, errors)
        } else {
            if collect_stale {
                stale_objects.push(object.clone());
            }
            runtime.record_computed_live_fallback();
            let result = evaluate_scope(
                runtime,
                object.data(),
                shared_definitions,
                MAX_SHARED_DEFINITIONS,
                "shared",
            )?;
            scope_from_evaluation(result)
        };

        let personal = personal_owner_id.map(|_| {
            let definitions = personal_by_class
                .get(&object.class_id().id())
                .map(Vec::as_slice)
                .unwrap_or(&[]);
            evaluate_scope(
                runtime,
                object.data(),
                definitions,
                MAX_PERSONAL_DEFINITIONS,
                "personal",
            )
            .map(scope_from_evaluation)
        });
        let personal = personal.transpose()?;
        enriched.push(StorageComputedObject::new(
            object,
            StorageSharedComputedScope::new(
                StorageComputationRevision::new(evaluation_revision)?,
                !fresh,
                shared,
            ),
            personal,
        ));
    }
    Ok((enriched, stale_objects))
}

fn evaluate_scope(
    runtime: &PostgresRuntime,
    data: &serde_json::Value,
    definitions: &[ComputedDefinitionRow],
    maximum: usize,
    scope: &'static str,
) -> Result<EvaluationResult, PostgresStorageError> {
    let result = evaluate_definitions(data, definitions, maximum)?;
    let error_codes = result
        .errors
        .values()
        .map(|error| error.code.as_str())
        .collect::<Vec<_>>();
    runtime.record_computed_evaluation(scope, &error_codes);
    Ok(result)
}

fn scope_from_evaluation(result: EvaluationResult) -> StorageComputedScope {
    StorageComputedScope::new(
        result.values,
        result
            .errors
            .into_iter()
            .map(|(key, error)| (key, storage_error(error)))
            .collect(),
    )
}

fn valid_stored_evaluation_maps(
    row: &MaterializedObjectRow,
    definitions: &[ComputedDefinitionRow],
) -> Result<Option<EvaluationMaps>, PostgresStorageError> {
    let Ok(values) =
        serde_json::from_value::<BTreeMap<String, serde_json::Value>>(row.values.clone())
    else {
        return Ok(None);
    };
    let Ok(errors) = serde_json::from_value::<BTreeMap<String, FieldError>>(row.errors.clone())
    else {
        return Ok(None);
    };
    let enabled = definitions
        .iter()
        .filter(|definition| definition.enabled())
        .collect::<Vec<_>>();
    if values.len() != enabled.len() {
        return Ok(None);
    }
    for definition in &enabled {
        let Some(value) = values.get(definition.key()) else {
            return Ok(None);
        };
        if !computed_value_matches_result_type(value, definition.query_value_type()?) {
            return Ok(None);
        }
    }
    let enabled_keys = enabled
        .iter()
        .map(|definition| definition.key())
        .collect::<BTreeSet<_>>();
    if errors.iter().any(|(key, _)| {
        !enabled_keys.contains(key.as_str())
            || !values.get(key).is_some_and(serde_json::Value::is_null)
    }) {
        return Ok(None);
    }
    Ok(Some((
        values,
        errors
            .into_iter()
            .map(|(key, error)| (key, storage_error(error)))
            .collect(),
    )))
}

fn computed_value_matches_result_type(
    value: &serde_json::Value,
    result_type: ComputedQueryValueType,
) -> bool {
    if value.is_null() {
        return true;
    }
    match result_type {
        ComputedQueryValueType::String => value.is_string(),
        ComputedQueryValueType::Number => value.as_number().is_some_and(|number| {
            hubuum_computed_fields::canonical_decimal_string(&number.to_string()).is_some()
        }),
        ComputedQueryValueType::Integer => value.as_number().is_some_and(|number| {
            hubuum_computed_fields::canonical_integer_string(&number.to_string()).is_some()
        }),
        ComputedQueryValueType::Boolean => value.is_boolean(),
        ComputedQueryValueType::Object => value.is_object(),
        ComputedQueryValueType::Array => value.is_array(),
    }
}

fn storage_error(error: FieldError) -> StorageComputedFieldError {
    StorageComputedFieldError::new(error.code.as_str().to_string(), error.path, error.message)
}

async fn load_definitions(
    connection: &mut PostgresConnection,
    class_ids: &[i32],
    personal_owner_id: Option<i32>,
) -> Result<Vec<ComputedDefinitionRow>, PostgresStorageError> {
    use crate::schema::computed_field_definitions::dsl as definition;

    let mut query = definition::computed_field_definitions
        .filter(definition::class_id.eq_any(class_ids))
        .into_boxed();
    query = match personal_owner_id {
        Some(owner_id) => query.filter(
            definition::visibility
                .eq(SHARED_VISIBILITY)
                .or(definition::visibility
                    .eq(PERSONAL_VISIBILITY)
                    .and(definition::owner_user_id.eq(Some(owner_id)))),
        ),
        None => query.filter(definition::visibility.eq(SHARED_VISIBILITY)),
    };
    query
        .order((definition::class_id.asc(), definition::id.asc()))
        .select(ComputedDefinitionRow::as_select())
        .load(connection)
        .await
        .map_err(PostgresStorageError::from)
}

async fn load_states(
    connection: &mut PostgresConnection,
    class_ids: &[i32],
) -> Result<Vec<ComputationStateRow>, PostgresStorageError> {
    use crate::schema::class_computation_state::dsl as state;

    state::class_computation_state
        .filter(state::class_id.eq_any(class_ids))
        .select(ComputationStateRow::as_select())
        .load(connection)
        .await
        .map_err(PostgresStorageError::from)
}

async fn load_materialized(
    connection: &mut PostgresConnection,
    object_ids: &[i32],
) -> Result<Vec<MaterializedObjectRow>, PostgresStorageError> {
    use crate::schema::object_computed_data::dsl as computed;

    computed::object_computed_data
        .filter(computed::object_id.eq_any(object_ids))
        .select(MaterializedObjectRow::as_select())
        .load(connection)
        .await
        .map_err(PostgresStorageError::from)
}

async fn repair_stale_materializations(
    runtime: &PostgresRuntime,
    mut stale_objects: Vec<StorageObject>,
) -> Result<(), PostgresStorageError> {
    stale_objects.sort_by_key(|object| object.id().id());
    let object_ids = stale_objects
        .iter()
        .map(|object| object.id().id())
        .collect::<Vec<_>>();
    let evaluations = runtime
        .with_transaction(async move |connection| {
            use crate::schema::hubuumobject::dsl::{hubuumobject, id};

            let current_objects = hubuumobject
                .filter(id.eq_any(object_ids))
                .order(id.asc())
                .for_update()
                .select(ObjectRow::as_select())
                .load::<ObjectRow>(connection)
                .await?;
            let mut evaluations = Vec::with_capacity(current_objects.len());
            for object in &current_objects {
                evaluations.push(
                    materialize_object(
                        connection,
                        ObjectMaterializationInput::new(
                            object.id,
                            object.hubuum_class_id,
                            &object.data,
                        ),
                    )
                    .await?,
                );
            }
            Ok::<_, PostgresStorageError>(evaluations)
        })
        .await?;
    for evaluation in evaluations.iter().flatten() {
        runtime.record_computed_evaluation("shared", evaluation.error_codes());
    }
    Ok(())
}

fn validate_owner_id(personal_owner_id: Option<i32>) -> Result<(), PostgresStorageError> {
    if personal_owner_id.is_some_and(|owner_id| owner_id <= 0) {
        return Err(PostgresStorageError::invalid_input(
            "computed field owner id must be greater than zero",
        ));
    }
    Ok(())
}
