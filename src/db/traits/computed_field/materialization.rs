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

pub(super) fn evaluate_definitions(
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

pub fn preview_computed_definition(
    data: &serde_json::Value,
    request: &ComputedFieldDefinitionRequest,
) -> Result<crate::models::ComputedFieldPreviewResponse, ApiError> {
    let definition = request.validate()?;
    let result =
        evaluate(data, &[definition], 1, EvaluationLimits::standard()).map_err(|error| {
            ApiError::InternalServerError(format!("Computed-field preview failed: {error}"))
        })?;
    crate::observability::metrics::computed_evaluation("preview", &result);
    Ok(crate::models::ComputedFieldPreviewResponse {
        value: result
            .values
            .get(&request.key)
            .cloned()
            .unwrap_or(serde_json::Value::Null),
        error: result
            .errors
            .get(&request.key)
            .cloned()
            .map(ComputedFieldErrorResponse::from),
    })
}

fn canonical_json(value: &serde_json::Value, output: &mut String) -> Result<(), ApiError> {
    match value {
        serde_json::Value::Object(values) => {
            output.push('{');
            let mut keys = values.keys().collect::<Vec<_>>();
            keys.sort_unstable();
            for (index, key) in keys.into_iter().enumerate() {
                if index > 0 {
                    output.push(',');
                }
                output.push_str(&serde_json::to_string(key)?);
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
        _ => output.push_str(&serde_json::to_string(value)?),
    }
    Ok(())
}

pub fn source_data_sha256(data: &serde_json::Value) -> Result<String, ApiError> {
    let mut canonical = String::new();
    canonical_json(data, &mut canonical)?;
    Ok(Sha256::digest(canonical.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect())
}

pub(super) async fn upsert_materialized(
    conn: &mut DbConnection,
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
    conn: &mut DbConnection,
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
    conn: &mut DbConnection,
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

type ResponseEvaluationMaps = (
    BTreeMap<String, serde_json::Value>,
    BTreeMap<String, ComputedFieldErrorResponse>,
);

#[derive(Clone, Copy)]
enum MaterializationRepair {
    Apply,
    Defer,
}

fn evaluation_maps(result: EvaluationResult) -> ResponseEvaluationMaps {
    (
        result.values,
        result
            .errors
            .into_iter()
            .map(|(key, error)| (key, error.into()))
            .collect(),
    )
}

fn valid_stored_evaluation_maps(
    row: &ObjectComputedData,
    definitions: &[ComputedFieldDefinition],
) -> Result<Option<ResponseEvaluationMaps>, ApiError> {
    let Ok(values) =
        serde_json::from_value::<BTreeMap<String, serde_json::Value>>(row.values.clone())
    else {
        return Ok(None);
    };
    let Ok(errors) = serde_json::from_value::<BTreeMap<String, hubuum_computed_fields::FieldError>>(
        row.errors.clone(),
    ) else {
        return Ok(None);
    };
    let enabled = definitions
        .iter()
        .filter(|definition| definition.enabled)
        .collect::<Vec<_>>();
    if values.len() != enabled.len() {
        return Ok(None);
    }
    for definition in &enabled {
        let Some(value) = values.get(&definition.key) else {
            return Ok(None);
        };
        let result_type = ComputedResultType::from_db(&definition.result_type)?;
        if !computed_value_matches_result_type(value, result_type) {
            return Ok(None);
        }
    }
    let enabled_keys = enabled
        .iter()
        .map(|definition| definition.key.as_str())
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
            .map(|(key, error)| (key, error.into()))
            .collect(),
    )))
}

fn computed_value_matches_result_type(
    value: &serde_json::Value,
    result_type: ComputedResultType,
) -> bool {
    if value.is_null() {
        return true;
    }
    match result_type {
        ComputedResultType::String => value.is_string(),
        ComputedResultType::Number => value.as_number().is_some_and(|number| {
            hubuum_computed_fields::canonical_decimal_string(&number.to_string()).is_some()
        }),
        ComputedResultType::Integer => value.as_number().is_some_and(|number| {
            hubuum_computed_fields::canonical_integer_string(&number.to_string()).is_some()
        }),
        ComputedResultType::Boolean => value.is_boolean(),
        ComputedResultType::Object => value.is_object(),
        ComputedResultType::Array => value.is_array(),
    }
}

pub async fn enrich_objects_with_computed(
    pool: &DbPool,
    objects: Vec<HubuumObject>,
    personal_owner_id: Option<i32>,
) -> Result<Vec<HubuumObjectComputedResponse>, ApiError> {
    if objects.is_empty() {
        return Ok(Vec::new());
    }
    let class_ids = objects
        .iter()
        .map(|object| object.hubuum_class_id)
        .collect::<BTreeSet<_>>();
    let object_ids = objects.iter().map(|object| object.id).collect::<Vec<_>>();
    let class_id_values = class_ids.iter().copied().collect::<Vec<_>>();

    let (definitions, states, materialized) =
        with_transaction(pool, async |conn| -> Result<_, ApiError> {
            for target_class_id in &class_ids {
                acquire_computed_class_shared_lock(conn, *target_class_id).await?;
            }
            use crate::schema::class_computation_state::dsl as state;
            use crate::schema::computed_field_definitions::dsl as definition;
            use crate::schema::object_computed_data::dsl as computed;
            let mut definition_query = definition::computed_field_definitions
                .filter(definition::class_id.eq_any(&class_id_values))
                .into_boxed();
            definition_query = match personal_owner_id {
                Some(owner_id) => definition_query.filter(
                    definition::visibility
                        .eq(COMPUTED_FIELD_VISIBILITY_SHARED)
                        .or(definition::visibility
                            .eq(COMPUTED_FIELD_VISIBILITY_PERSONAL)
                            .and(definition::owner_user_id.eq(Some(owner_id)))),
                ),
                None => definition_query
                    .filter(definition::visibility.eq(COMPUTED_FIELD_VISIBILITY_SHARED)),
            };
            let definitions = definition_query
                .order((definition::class_id.asc(), definition::id.asc()))
                .select(ComputedFieldDefinition::as_select())
                .load::<ComputedFieldDefinition>(conn)
                .await?;
            let states = state::class_computation_state
                .filter(state::class_id.eq_any(&class_id_values))
                .select(ClassComputationState::as_select())
                .load::<ClassComputationState>(conn)
                .await?;
            let materialized = computed::object_computed_data
                .filter(computed::object_id.eq_any(&object_ids))
                .select(ObjectComputedData::as_select())
                .load::<ObjectComputedData>(conn)
                .await?;
            Ok((definitions, states, materialized))
        })
        .await?;

    enrich_objects_from_snapshot(
        pool,
        objects,
        personal_owner_id,
        definitions,
        states,
        materialized,
        MaterializationRepair::Apply,
    )
    .await
}

pub async fn enrich_objects_with_computed_query_snapshot(
    pool: &DbPool,
    objects: Vec<HubuumObject>,
    personal_owner_id: Option<i32>,
    snapshot: &ComputedQuerySnapshot,
) -> Result<Vec<HubuumObjectComputedResponse>, ApiError> {
    if objects.is_empty() {
        return Ok(Vec::new());
    }
    if objects
        .iter()
        .any(|object| object.hubuum_class_id != snapshot.class_id)
    {
        return Err(ApiError::InternalServerError(
            "Computed sort snapshot cannot enrich objects from another class".to_string(),
        ));
    }
    let object_ids = objects.iter().map(|object| object.id).collect::<Vec<_>>();
    let materialized = with_connection(pool, async |conn| {
        use crate::schema::object_computed_data::dsl as computed;
        computed::object_computed_data
            .filter(computed::object_id.eq_any(&object_ids))
            .select(ObjectComputedData::as_select())
            .load::<ObjectComputedData>(conn)
            .await
    })
    .await?;

    enrich_objects_from_snapshot(
        pool,
        objects,
        personal_owner_id,
        snapshot.definitions.clone(),
        vec![snapshot.state.clone()],
        materialized,
        MaterializationRepair::Defer,
    )
    .await
}

async fn enrich_objects_from_snapshot(
    pool: &DbPool,
    objects: Vec<HubuumObject>,
    personal_owner_id: Option<i32>,
    definitions: Vec<ComputedFieldDefinition>,
    states: Vec<ClassComputationState>,
    materialized: Vec<ObjectComputedData>,
    repair: MaterializationRepair,
) -> Result<Vec<HubuumObjectComputedResponse>, ApiError> {
    let mut shared_by_class: HashMap<i32, Vec<ComputedFieldDefinition>> = HashMap::new();
    let mut personal_by_class: HashMap<i32, Vec<ComputedFieldDefinition>> = HashMap::new();
    for definition in definitions {
        if definition.is_shared() {
            shared_by_class
                .entry(definition.class_id)
                .or_default()
                .push(definition);
        } else if personal_owner_id.is_some_and(|owner| definition.is_personal_for(owner)) {
            personal_by_class
                .entry(definition.class_id)
                .or_default()
                .push(definition);
        }
    }
    let states = states
        .into_iter()
        .map(|state| (state.class_id, state))
        .collect::<HashMap<_, _>>();
    let materialized = materialized
        .into_iter()
        .map(|row| (row.object_id, row))
        .collect::<HashMap<_, _>>();

    let mut stale_objects = Vec::new();
    let mut enriched = Vec::with_capacity(objects.len());
    for object in objects {
        let state = states
            .get(&object.hubuum_class_id)
            .cloned()
            .unwrap_or_else(|| {
                ClassComputationState::ready_without_definitions(object.hubuum_class_id)
            });
        let definitions = shared_by_class
            .get(&object.hubuum_class_id)
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        let hash = source_data_sha256(&object.data)?;
        let stored = materialized.get(&object.id);
        let has_enabled_definitions = definitions.iter().any(|definition| definition.enabled);
        let stored_maps = match stored {
            Some(row)
                if row.class_id == object.hubuum_class_id
                    && row.evaluation_revision == state.evaluation_revision
                    && row.source_data_sha256 == hash =>
            {
                valid_stored_evaluation_maps(row, definitions)?
            }
            _ => None,
        };
        let fresh = !has_enabled_definitions || stored_maps.is_some();
        let (shared_values, shared_errors) = if !has_enabled_definitions {
            (BTreeMap::new(), BTreeMap::new())
        } else if let Some(maps) = stored_maps {
            maps
        } else {
            if matches!(repair, MaterializationRepair::Apply) {
                stale_objects.push(object.clone());
            }
            crate::observability::metrics::computed_live_fallback();
            evaluation_maps(evaluate_definitions(
                &object.data,
                definitions,
                MAX_SHARED_DEFINITIONS,
                "shared",
            )?)
        };

        let personal = if personal_owner_id.is_some() {
            let definitions = personal_by_class
                .get(&object.hubuum_class_id)
                .map(Vec::as_slice)
                .unwrap_or(&[]);
            let result = evaluate_definitions(
                &object.data,
                definitions,
                MAX_PERSONAL_DEFINITIONS,
                "personal",
            )?;
            let (values, errors) = evaluation_maps(result);
            Some(ComputedScopeResponse { values, errors })
        } else {
            None
        };
        enriched.push(HubuumObjectComputedResponse {
            object,
            computed: ComputedObjectScopesResponse {
                shared: SharedComputedScopeResponse {
                    revision: state.evaluation_revision,
                    materialization_stale: !fresh,
                    values: shared_values,
                    errors: shared_errors,
                },
                personal,
            },
        });
    }

    if !stale_objects.is_empty() {
        match repair_stale_materializations(pool, stale_objects).await {
            Ok(()) => crate::observability::metrics::computed_read_repair("success"),
            Err(error) => {
                crate::observability::metrics::computed_read_repair("failure");
                warn!(message = "Computed-field read repair failed", error = %error);
            }
        }
    }
    Ok(enriched)
}

async fn repair_stale_materializations(
    pool: &DbPool,
    mut stale_objects: Vec<HubuumObject>,
) -> Result<(), ApiError> {
    stale_objects.sort_by_key(|object| object.id);
    let object_ids = stale_objects
        .iter()
        .map(|object| object.id)
        .collect::<Vec<_>>();
    with_transaction(pool, async |conn| -> Result<_, ApiError> {
        use crate::schema::hubuumobject::dsl::{hubuumobject, id};
        let current_objects = hubuumobject
            .filter(id.eq_any(&object_ids))
            .order(id.asc())
            .for_update()
            .load::<HubuumObject>(conn)
            .await?;

        // Object writes lock their row before taking the class advisory lock.
        // Repairs and rebuild batches use the same order so an exclusive
        // definition update cannot form a row/advisory-lock cycle.
        let class_ids = current_objects
            .iter()
            .map(|object| object.hubuum_class_id)
            .collect::<BTreeSet<_>>();
        for class_id in &class_ids {
            acquire_computed_class_shared_lock(conn, *class_id).await?;
        }

        let class_id_values = class_ids.iter().copied().collect::<Vec<_>>();
        use crate::schema::computed_field_definitions::dsl as definition;
        let definitions = definition::computed_field_definitions
            .filter(definition::class_id.eq_any(&class_id_values))
            .filter(definition::visibility.eq(COMPUTED_FIELD_VISIBILITY_SHARED))
            .order((definition::class_id.asc(), definition::id.asc()))
            .select(ComputedFieldDefinition::as_select())
            .load::<ComputedFieldDefinition>(conn)
            .await?;
        let mut definitions_by_class: HashMap<i32, Vec<ComputedFieldDefinition>> = HashMap::new();
        for definition in definitions {
            definitions_by_class
                .entry(definition.class_id)
                .or_default()
                .push(definition);
        }
        let mut revisions = HashMap::new();
        for class_id in class_ids {
            let state = ensure_computation_state(conn, class_id).await?;
            revisions.insert(class_id, state.evaluation_revision);
        }

        let mut without_definitions = Vec::new();
        for object in current_objects {
            let definitions = definitions_by_class
                .get(&object.hubuum_class_id)
                .map(Vec::as_slice)
                .unwrap_or(&[]);
            if definitions.is_empty() {
                without_definitions.push(object.id);
                continue;
            }
            let result =
                evaluate_definitions(&object.data, definitions, MAX_SHARED_DEFINITIONS, "shared")?;
            upsert_materialized(conn, &object, revisions[&object.hubuum_class_id], result).await?;
        }
        if !without_definitions.is_empty() {
            use crate::schema::object_computed_data::dsl::{object_computed_data, object_id};
            diesel::delete(object_computed_data.filter(object_id.eq_any(without_definitions)))
                .execute(conn)
                .await?;
        }
        Ok(())
    })
    .await
}
