use chrono::{DateTime, Utc};
use hubuum_computed_fields::{EvaluationLimits, MAX_SHARED_DEFINITIONS, evaluate};
use hubuum_domain::{ClassId, ObjectId};
use hubuum_storage_core::{
    HISTORICAL_COMPUTED_EVALUATOR_VERSION, HistoryStorage, StorageComputedFieldHistoryQuery,
    StorageHistoricalComputedFields, StorageHistoryResource,
};

use super::navigation::{HistoryEntry, HistoryReader};
use crate::errors::ApiError;
use crate::models::{
    HistoricalComputationMode, HistoricalComputedField, HistoricalComputedFieldResult,
    HistoricalComputedObjectSource, HistoricalComputedUnavailableReason, HistoricalComputedValues,
    HistoricalPersonalFields, HistoricalSharedComputedFields, HubuumObjectHistory, TokenScope,
};
use crate::permissions::AppContext;
use crate::storage::storage_handle;
use crate::traits::AuthzSubject;

pub(crate) async fn as_of<S: AuthzSubject>(
    context: &AppContext,
    subject: &S,
    scopes: Option<&TokenScope>,
    class_id: ClassId,
    object_id: ObjectId,
    at: DateTime<Utc>,
) -> Result<HistoricalComputedValues, ApiError> {
    // Both current permission gates precede any stored input or definition read.
    let object_reader = HistoryReader::authorize(
        context,
        subject,
        scopes,
        StorageHistoryResource::Object {
            class_id,
            object_id,
        },
    )
    .await?;
    let class_reader = HistoryReader::authorize(
        context,
        subject,
        scopes,
        StorageHistoryResource::Class(class_id),
    )
    .await?;
    let HistoryEntry::Object(object) = object_reader.as_of(at).await? else {
        return Err(ApiError::InternalServerError(
            "History returned the wrong resource kind".into(),
        ));
    };
    let shared = match class_reader.as_of(at).await {
        Err(ApiError::NotFound(_)) => HistoricalSharedComputedFields::Unavailable {
            reason: HistoricalComputedUnavailableReason::ClassHistoryUnavailable,
        },
        Err(error) => return Err(error),
        Ok(_) => match storage_handle(context)
            .get_computed_field_history(StorageComputedFieldHistoryQuery::new(class_id, at))
            .await?
        {
            Some(definitions) => recompute(&object, definitions)?,
            None => HistoricalSharedComputedFields::Unavailable {
                reason: HistoricalComputedUnavailableReason::DefinitionHistoryUnavailable,
            },
        },
    };
    Ok(HistoricalComputedValues {
        mode: HistoricalComputationMode::HistoricalRecomputation,
        at,
        object: HistoricalComputedObjectSource {
            object_id: object.id,
            class_id: object.hubuum_class_id,
            history_id: object.history_id,
            revision: object.revision,
            valid_from: object.valid_from,
            valid_to: object.valid_to,
        },
        shared,
        personal: HistoricalPersonalFields::Unsupported,
    })
}

fn recompute(
    object: &HubuumObjectHistory,
    selected: StorageHistoricalComputedFields,
) -> Result<HistoricalSharedComputedFields, ApiError> {
    let record = selected.record();
    let unsupported = if record.evaluator_version() != HISTORICAL_COMPUTED_EVALUATOR_VERSION {
        Some(HistoricalComputedUnavailableReason::UnsupportedEvaluatorVersion)
    } else if record
        .definitions()
        .iter()
        .any(|field| field.enabled() && field.evaluator_definition().is_none())
    {
        // Unknown operations can consume the shared work budget. Evaluating only
        // the known subset would silently change other fields' historical results.
        Some(HistoricalComputedUnavailableReason::UnsupportedDefinitionSemantics)
    } else {
        None
    };
    let mut output = if unsupported.is_none() {
        let definitions = record
            .definitions()
            .iter()
            .filter_map(|field| field.evaluator_definition().cloned())
            .collect::<Vec<_>>();
        evaluate(
            &object.data,
            &definitions,
            MAX_SHARED_DEFINITIONS,
            EvaluationLimits::historical_version_one(),
        )
        .map_err(|error| ApiError::InternalServerError(error.to_string()))?
    } else {
        Default::default()
    };
    let fields = record
        .definitions()
        .iter()
        .map(|field| {
            let result = if !field.enabled() {
                HistoricalComputedFieldResult::Disabled
            } else if let Some(reason) = unsupported {
                HistoricalComputedFieldResult::Unavailable { reason }
            } else if let Some(error) = output.errors.remove(field.key()) {
                HistoricalComputedFieldResult::Error {
                    error: error.into(),
                }
            } else {
                HistoricalComputedFieldResult::Evaluated {
                    value: output.values.remove(field.key()).ok_or_else(|| {
                        ApiError::InternalServerError(
                            "Historical evaluator omitted an enabled field".into(),
                        )
                    })?,
                }
            };
            Ok(HistoricalComputedField {
                definition_id: field.id().id(),
                revision: field.revision(),
                key: field.key().to_owned(),
                label: field.label().to_owned(),
                semantics_version: field.semantics_version(),
                result,
            })
        })
        .collect::<Result<Vec<_>, ApiError>>()?;
    Ok(HistoricalSharedComputedFields::Available {
        definition_history_id: record.history_entry_id().id(),
        valid_from: record.valid_from(),
        valid_to: record.valid_to(),
        evaluator_version: record.evaluator_version(),
        fields,
    })
}

#[cfg(test)]
mod tests;
