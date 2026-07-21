use diesel::prelude::*;
use diesel_async::RunQueryDsl;
use hubuum_computed_fields::{EvaluationResult, MAX_PERSONAL_DEFINITIONS, MAX_SHARED_DEFINITIONS};

use super::candidate::ObjectAggregateCandidate;
use crate::db::DbConnection;
use crate::db::traits::computed_field::{ComputedQuerySnapshot, evaluate_definitions};
use crate::errors::ApiError;
use crate::models::computed_field::{
    COMPUTED_FIELD_VISIBILITY_PERSONAL, COMPUTED_FIELD_VISIBILITY_SHARED, ComputedFieldDefinition,
    ComputedResultType,
};
use crate::models::object_aggregate::{ComputedFieldSelector, ObjectAggregateSpec};
use crate::models::search::ComputedFieldScope;

#[derive(Default)]
pub(super) struct ComputedAggregateDefinitions {
    shared: Vec<ComputedFieldDefinition>,
    personal: Vec<ComputedFieldDefinition>,
}

pub(super) async fn load_computed_aggregate_definitions(
    connection: &mut DbConnection,
    class_id_value: i32,
    spec: &ObjectAggregateSpec,
    personal_owner_id: Option<i32>,
    computed_filter_snapshot: Option<&ComputedQuerySnapshot>,
) -> Result<ComputedAggregateDefinitions, ApiError> {
    let selectors = spec.computed_selectors().collect::<Vec<_>>();
    if selectors.is_empty() {
        return Ok(ComputedAggregateDefinitions::default());
    }

    let shared_keys = selectors
        .iter()
        .filter(|selector| selector.scope() == ComputedFieldScope::Shared)
        .map(|selector| selector.key().to_string())
        .collect::<Vec<_>>();
    let personal_keys = selectors
        .iter()
        .filter(|selector| selector.scope() == ComputedFieldScope::Personal)
        .map(|selector| selector.key().to_string())
        .collect::<Vec<_>>();
    if computed_filter_snapshot.is_some_and(|snapshot| snapshot.class_id() != class_id_value) {
        return Err(ApiError::InternalServerError(
            "Computed object aggregate filter snapshot belongs to a different class".to_string(),
        ));
    }
    let loaded_definitions = if computed_filter_snapshot.is_none() {
        Some(
            load_selected_definitions(
                connection,
                class_id_value,
                &shared_keys,
                &personal_keys,
                personal_owner_id,
            )
            .await?,
        )
    } else {
        None
    };
    let definitions = computed_filter_snapshot
        .map(ComputedQuerySnapshot::definitions)
        .or(loaded_definitions.as_deref())
        .unwrap_or_default();

    let mut selected = ComputedAggregateDefinitions::default();
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
            })
            .ok_or_else(|| {
                ApiError::BadRequest(format!(
                    "Computed aggregate field '{}' does not name an accessible field in class {class_id_value}",
                    selector.canonical()
                ))
            })?;
        if !definition.enabled {
            return Err(ApiError::BadRequest(format!(
                "Computed aggregate field '{}' is disabled",
                selector.canonical()
            )));
        }
        let is_measure = spec.measures().iter().any(|measure| {
            measure
                .computed_selector()
                .is_some_and(|candidate| candidate.canonical() == selector.canonical())
        });
        if is_measure
            && !matches!(
                ComputedResultType::from_db(&definition.result_type)?,
                ComputedResultType::Number | ComputedResultType::Integer
            )
        {
            return Err(ApiError::BadRequest(format!(
                "Computed aggregate measure '{}' must select a numeric field",
                selector.canonical()
            )));
        }
        definition.evaluator_definition()?;
        let target = match selector.scope() {
            ComputedFieldScope::Shared => &mut selected.shared,
            ComputedFieldScope::Personal => &mut selected.personal,
        };
        if !target
            .iter()
            .any(|selected_definition| selected_definition.id == definition.id)
        {
            target.push(definition.clone());
        }
    }
    Ok(selected)
}

async fn load_selected_definitions(
    connection: &mut DbConnection,
    class_id_value: i32,
    shared_keys: &[String],
    personal_keys: &[String],
    personal_owner_id: Option<i32>,
) -> Result<Vec<ComputedFieldDefinition>, ApiError> {
    use crate::schema::computed_field_definitions::dsl::{
        class_id, computed_field_definitions, id, key, owner_user_id, visibility,
    };

    let mut definitions = Vec::with_capacity(shared_keys.len() + personal_keys.len());
    if !shared_keys.is_empty() {
        definitions.extend(
            computed_field_definitions
                .filter(class_id.eq(class_id_value))
                .filter(visibility.eq(COMPUTED_FIELD_VISIBILITY_SHARED))
                .filter(key.eq_any(shared_keys))
                .order(id.asc())
                .select(ComputedFieldDefinition::as_select())
                .load::<ComputedFieldDefinition>(connection)
                .await?,
        );
    }
    if !personal_keys.is_empty() {
        let owner_id = personal_owner_id.ok_or_else(|| {
            ApiError::InternalServerError(
                "Personal computed grouping requires an owner".to_string(),
            )
        })?;
        definitions.extend(
            computed_field_definitions
                .filter(class_id.eq(class_id_value))
                .filter(visibility.eq(COMPUTED_FIELD_VISIBILITY_PERSONAL))
                .filter(owner_user_id.eq(Some(owner_id)))
                .filter(key.eq_any(personal_keys))
                .order(id.asc())
                .select(ComputedFieldDefinition::as_select())
                .load::<ComputedFieldDefinition>(connection)
                .await?,
        );
    }
    Ok(definitions)
}

pub(super) fn computed_aggregate_payload(
    candidates: Vec<ObjectAggregateCandidate>,
    spec: &ObjectAggregateSpec,
    definitions: &ComputedAggregateDefinitions,
) -> Result<(Vec<ObjectAggregateCandidate>, serde_json::Value), ApiError> {
    let mut payload = serde_json::Map::new();
    for object in &candidates {
        let data = object.data.as_ref().ok_or_else(|| {
            ApiError::InternalServerError(
                "Computed aggregation candidate is missing its JSON data snapshot".to_string(),
            )
        })?;
        let shared = evaluate_aggregate_definitions(
            data,
            &definitions.shared,
            MAX_SHARED_DEFINITIONS,
            "shared_group",
        )?;
        let personal = evaluate_aggregate_definitions(
            data,
            &definitions.personal,
            MAX_PERSONAL_DEFINITIONS,
            "personal_group",
        )?;
        let values = spec
            .computed_selectors()
            .map(|selector| {
                (
                    selector.canonical(),
                    computed_selector_value(shared.as_ref(), personal.as_ref(), selector),
                )
            })
            .collect::<serde_json::Map<_, _>>();
        payload.insert(object.id.to_string(), serde_json::Value::Object(values));
    }
    Ok((candidates, serde_json::Value::Object(payload)))
}

fn evaluate_aggregate_definitions(
    data: &serde_json::Value,
    definitions: &[ComputedFieldDefinition],
    limit: usize,
    context: &'static str,
) -> Result<Option<EvaluationResult>, ApiError> {
    (!definitions.is_empty())
        .then(|| evaluate_definitions(data, definitions, limit, context))
        .transpose()
}

fn computed_selector_value(
    shared: Option<&EvaluationResult>,
    personal: Option<&EvaluationResult>,
    selector: &ComputedFieldSelector,
) -> serde_json::Value {
    let result = match selector.scope() {
        ComputedFieldScope::Shared => shared,
        ComputedFieldScope::Personal => personal,
    };
    let Some(result) = result else {
        return serde_json::json!({"state": 3, "value": null});
    };
    if result.errors.contains_key(selector.key()) {
        return serde_json::json!({"state": 3, "value": null});
    }
    match result.values.get(selector.key()) {
        Some(serde_json::Value::Null) => serde_json::json!({"state": 1, "value": null}),
        Some(value) => serde_json::json!({"state": 0, "value": value}),
        None => serde_json::json!({"state": 3, "value": null}),
    }
}
