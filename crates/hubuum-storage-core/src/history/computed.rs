use std::collections::BTreeSet;

use chrono::{DateTime, Utc};
use hubuum_computed_fields::{
    Definition, FieldKey, MAX_DESCRIPTION_BYTES, MAX_LABEL_BYTES, MAX_SHARED_DEFINITIONS,
    SEMANTICS_VERSION,
};
use hubuum_domain::{ClassId, ComputedFieldDefinitionId, HistoryRecordId, ResourceRevision};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;

use crate::{
    StorageComputedFieldDefinition, StorageComputedFieldVisibility, StorageValidationError,
};

/// Versions the historical evaluator's ordering, operation catalog and safety limits.
pub const HISTORICAL_COMPUTED_EVALUATOR_VERSION: i16 = 1;
const MAX_DEFINITION_BYTES: usize = 64 * 1024;

#[derive(Clone, Copy)]
pub struct StorageComputedFieldHistoryQuery {
    class_id: ClassId,
    at: DateTime<Utc>,
}

impl StorageComputedFieldHistoryQuery {
    pub const fn new(class_id: ClassId, at: DateTime<Utc>) -> Self {
        Self { class_id, at }
    }
    pub const fn class_id(self) -> ClassId {
        self.class_id
    }
    pub const fn at(self) -> DateTime<Utc> {
        self.at
    }
}

#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DefinitionWire {
    key: FieldKey,
    label: String,
    description: String,
    operation: Value,
    result_type: String,
    enabled: bool,
    semantics_version: i16,
}

#[derive(Clone, PartialEq, Serialize)]
#[serde(untagged)]
enum HistoricalDefinition {
    Supported(Definition),
    Unavailable(DefinitionWire),
}

/// A shared definition revision. Personal ownership and actor fields are never retained.
#[derive(Clone, PartialEq, Serialize)]
pub struct StorageHistoricalComputedField {
    id: ComputedFieldDefinitionId,
    class_id: ClassId,
    revision: ResourceRevision,
    definition: HistoricalDefinition,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FieldWire {
    id: ComputedFieldDefinitionId,
    class_id: ClassId,
    revision: ResourceRevision,
    definition: DefinitionWire,
}

impl<'de> Deserialize<'de> for StorageHistoricalComputedField {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let wire = FieldWire::deserialize(deserializer)?;
        let definition = wire.definition;
        if definition.semantics_version <= 0
            || definition.label.is_empty()
            || definition.label.len() > MAX_LABEL_BYTES
            || definition.description.len() > MAX_DESCRIPTION_BYTES
            || definition.result_type.is_empty()
            || definition.result_type.len() > 32
            || !definition.operation.is_object()
        {
            return Err(serde::de::Error::custom(
                "Invalid historical computed definition",
            ));
        }
        let value = serde_json::to_value(&definition).map_err(serde::de::Error::custom)?;
        if serde_json::to_vec(&value)
            .map_err(serde::de::Error::custom)?
            .len()
            > MAX_DEFINITION_BYTES
        {
            return Err(serde::de::Error::custom(
                "Historical computed definition exceeds its size limit",
            ));
        }
        let definition = if definition.semantics_version == SEMANTICS_VERSION {
            HistoricalDefinition::Supported(
                serde_json::from_value(value).map_err(serde::de::Error::custom)?,
            )
        } else {
            // Preserve unsupported historical semantics without interpreting them
            // using today's operation catalog.
            HistoricalDefinition::Unavailable(definition)
        };
        Ok(Self {
            id: wire.id,
            class_id: wire.class_id,
            revision: wire.revision,
            definition,
        })
    }
}

impl StorageHistoricalComputedField {
    pub fn from_shared(
        value: &StorageComputedFieldDefinition,
    ) -> Result<Self, StorageValidationError> {
        if value.visibility() != StorageComputedFieldVisibility::Shared {
            return Err(StorageValidationError::invalid(
                "Personal computed definitions cannot enter shared history",
            ));
        }
        Ok(Self {
            id: ComputedFieldDefinitionId::from(value.metadata().id()),
            class_id: value.class_id(),
            revision: value.metadata().revision(),
            definition: HistoricalDefinition::Supported(value.evaluator_definition().clone()),
        })
    }
    pub const fn id(&self) -> ComputedFieldDefinitionId {
        self.id
    }
    pub const fn class_id(&self) -> ClassId {
        self.class_id
    }
    pub const fn revision(&self) -> ResourceRevision {
        self.revision
    }
    pub fn key(&self) -> &str {
        match &self.definition {
            HistoricalDefinition::Supported(value) => value.key().as_str(),
            HistoricalDefinition::Unavailable(value) => value.key.as_str(),
        }
    }
    pub fn label(&self) -> &str {
        match &self.definition {
            HistoricalDefinition::Supported(value) => value.label(),
            HistoricalDefinition::Unavailable(value) => &value.label,
        }
    }
    pub fn enabled(&self) -> bool {
        match &self.definition {
            HistoricalDefinition::Supported(value) => value.enabled(),
            HistoricalDefinition::Unavailable(value) => value.enabled,
        }
    }
    pub fn semantics_version(&self) -> i16 {
        match &self.definition {
            HistoricalDefinition::Supported(value) => value.semantics_version(),
            HistoricalDefinition::Unavailable(value) => value.semantics_version,
        }
    }
    pub fn evaluator_definition(&self) -> Option<&Definition> {
        match &self.definition {
            HistoricalDefinition::Supported(value) => Some(value),
            HistoricalDefinition::Unavailable(_) => None,
        }
    }
}

/// An authoritative complete shared definition set, including an observed empty set.
/// The interval describes availability of this exact set, not a materialization.
#[derive(Clone, PartialEq, Serialize)]
pub struct StorageComputedFieldHistory {
    class_id: ClassId,
    history_entry_id: HistoryRecordId,
    valid_from: DateTime<Utc>,
    valid_to: Option<DateTime<Utc>>,
    evaluator_version: i16,
    definitions: Vec<StorageHistoricalComputedField>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct HistoryWire {
    class_id: ClassId,
    history_entry_id: HistoryRecordId,
    valid_from: DateTime<Utc>,
    valid_to: Option<DateTime<Utc>>,
    evaluator_version: i16,
    definitions: Vec<StorageHistoricalComputedField>,
}

impl<'de> Deserialize<'de> for StorageComputedFieldHistory {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let wire = HistoryWire::deserialize(deserializer)?;
        Self::builder(wire.class_id, wire.history_entry_id, wire.valid_from)
            .valid_until(wire.valid_to)
            .evaluator_version(wire.evaluator_version)
            .definitions(wire.definitions)
            .build()
            .map_err(serde::de::Error::custom)
    }
}

pub struct StorageComputedFieldHistoryBuilder(StorageComputedFieldHistory);

impl StorageComputedFieldHistoryBuilder {
    pub fn valid_until(mut self, at: Option<DateTime<Utc>>) -> Self {
        self.0.valid_to = at;
        self
    }
    pub fn evaluator_version(mut self, version: i16) -> Self {
        self.0.evaluator_version = version;
        self
    }
    pub fn definitions(mut self, values: Vec<StorageHistoricalComputedField>) -> Self {
        self.0.definitions = values;
        self
    }
    pub fn build(mut self) -> Result<StorageComputedFieldHistory, StorageValidationError> {
        let row = &mut self.0;
        if row.evaluator_version <= 0 || row.valid_to.is_some_and(|end| end <= row.valid_from) {
            return Err(StorageValidationError::invalid(
                "Invalid computed history version or validity interval",
            ));
        }
        if row.definitions.len() > MAX_SHARED_DEFINITIONS {
            return Err(StorageValidationError::invalid(
                "Historical shared definition set exceeds its field limit",
            ));
        }
        let mut ids = BTreeSet::new();
        let mut keys = BTreeSet::new();
        for field in &row.definitions {
            if field.class_id() != row.class_id
                || !ids.insert(field.id())
                || !keys.insert(field.key())
            {
                return Err(StorageValidationError::invalid(
                    "Historical shared definitions must have matching class and unique identities and keys",
                ));
            }
        }
        // Version 1 evaluates fields in stable identity order, including after a rename.
        row.definitions
            .sort_by_key(StorageHistoricalComputedField::id);
        Ok(self.0)
    }
}

impl StorageComputedFieldHistory {
    pub fn builder(
        class_id: ClassId,
        history_entry_id: HistoryRecordId,
        valid_from: DateTime<Utc>,
    ) -> StorageComputedFieldHistoryBuilder {
        StorageComputedFieldHistoryBuilder(Self {
            class_id,
            history_entry_id,
            valid_from,
            valid_to: None,
            evaluator_version: HISTORICAL_COMPUTED_EVALUATOR_VERSION,
            definitions: Vec::new(),
        })
    }
    pub const fn class_id(&self) -> ClassId {
        self.class_id
    }
    pub const fn history_entry_id(&self) -> HistoryRecordId {
        self.history_entry_id
    }
    pub const fn valid_from(&self) -> DateTime<Utc> {
        self.valid_from
    }
    pub const fn valid_to(&self) -> Option<DateTime<Utc>> {
        self.valid_to
    }
    pub const fn evaluator_version(&self) -> i16 {
        self.evaluator_version
    }
    pub fn definitions(&self) -> &[StorageHistoricalComputedField] {
        &self.definitions
    }
    pub fn close_at(&mut self, at: DateTime<Utc>) -> Result<(), StorageValidationError> {
        if self.valid_to.is_some() || at <= self.valid_from {
            return Err(StorageValidationError::invalid(
                "Only an open computed definition interval can be closed after its start",
            ));
        }
        self.valid_to = Some(at);
        Ok(())
    }
    pub fn is_effective_at(&self, at: DateTime<Utc>) -> bool {
        self.valid_from <= at && self.valid_to.is_none_or(|end| at < end)
    }
    pub fn select(
        self,
        query: StorageComputedFieldHistoryQuery,
    ) -> Result<StorageHistoricalComputedFields, StorageValidationError> {
        if self.class_id != query.class_id || !self.is_effective_at(query.at) {
            return Err(StorageValidationError::invalid(
                "Computed definition history does not match the selected class and instant",
            ));
        }
        Ok(StorageHistoricalComputedFields {
            record: self,
            at: query.at,
        })
    }
}

/// Carries proof that the complete definition set belongs to the requested instant.
#[derive(Clone, PartialEq)]
pub struct StorageHistoricalComputedFields {
    record: StorageComputedFieldHistory,
    at: DateTime<Utc>,
}

impl StorageHistoricalComputedFields {
    pub const fn record(&self) -> &StorageComputedFieldHistory {
        &self.record
    }
    pub const fn at(&self) -> DateTime<Utc> {
        self.at
    }
}

#[cfg(test)]
mod tests;
