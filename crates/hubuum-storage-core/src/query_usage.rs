//! Advisory, class-scoped workload declarations. These never constrain objects.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use hubuum_domain::{ClassId, CollectionId, PrincipalId, ResourceId, ResourceRevision};
use hubuum_events_core::EventContext;
use hubuum_query::JsonFieldPath;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::{StorageError, StorageMutationOutcome, StorageRecordMetadata, StorageValidationError};

pub const MAX_QUERY_USAGE_DECLARATIONS: usize = 32;

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum StorageQueryUsageOperation {
    Equals,
    Gt,
    Gte,
    Lt,
    Lte,
    Between,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum StorageQueryUsageValueType {
    String,
    Numeric,
    Boolean,
}

/// A bounded, validated declaration in Hubuum's existing filter vocabulary.
#[derive(Clone, Deserialize, Serialize, PartialEq, Eq, Hash)]
#[serde(try_from = "PatternWire", into = "PatternWire")]
pub struct StorageQueryUsagePattern {
    path: JsonFieldPath,
    value_type: StorageQueryUsageValueType,
    operations: Vec<StorageQueryUsageOperation>,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct PatternWire {
    path: String,
    value_type: StorageQueryUsageValueType,
    operations: Vec<StorageQueryUsageOperation>,
}

impl StorageQueryUsagePattern {
    pub fn try_new(
        path: &str,
        value_type: StorageQueryUsageValueType,
        mut operations: Vec<StorageQueryUsageOperation>,
    ) -> Result<Self, StorageValidationError> {
        if path.len() > 512 || path.split(',').count() > 32 {
            return Err(StorageValidationError::invalid(
                "Query usage paths accept at most 512 bytes and 32 segments",
            ));
        }
        let path = JsonFieldPath::new(path)
            .map_err(|error| StorageValidationError::invalid(error.to_string()))?;
        if operations.is_empty() || operations.len() > 6 {
            return Err(StorageValidationError::invalid(
                "Specify between one and six query usage operations",
            ));
        }
        operations.sort_unstable();
        operations.dedup();
        if value_type != StorageQueryUsageValueType::Numeric
            && operations != [StorageQueryUsageOperation::Equals]
        {
            return Err(StorageValidationError::invalid(
                "String and boolean query usage currently support only equals",
            ));
        }
        Ok(Self {
            path,
            value_type,
            operations,
        })
    }

    pub fn path(&self) -> &JsonFieldPath {
        &self.path
    }
    pub const fn value_type(&self) -> StorageQueryUsageValueType {
        self.value_type
    }
    pub fn operations(&self) -> &[StorageQueryUsageOperation] {
        &self.operations
    }

    /// Best-effort assessment only; schemas with unresolved alternatives remain unknown.
    pub fn schema_compatibility(
        &self,
        schema: Option<&Value>,
    ) -> StorageQueryUsageSchemaCompatibility {
        use StorageQueryUsageSchemaCompatibility::*;
        let Some(mut schema) = schema else {
            return Unknown;
        };
        for segment in self.path.segments() {
            if ["$ref", "allOf", "anyOf", "oneOf", "if"]
                .iter()
                .any(|key| schema.get(key).is_some())
            {
                return Unknown;
            }
            schema = if let Some(property) = schema
                .get("properties")
                .and_then(|value| value.get(segment))
            {
                property
            } else if segment.parse::<usize>().is_ok() {
                let Some(items) = schema.get("items").filter(|value| value.is_object()) else {
                    return Unknown;
                };
                items
            } else {
                return Unknown;
            };
        }
        if ["$ref", "allOf", "anyOf", "oneOf", "if"]
            .iter()
            .any(|key| schema.get(key).is_some())
        {
            return Unknown;
        }
        match schema.get("type").and_then(Value::as_str) {
            Some("string") if self.value_type == StorageQueryUsageValueType::String => Compatible,
            Some("number" | "integer")
                if self.value_type == StorageQueryUsageValueType::Numeric =>
            {
                Compatible
            }
            Some("boolean") if self.value_type == StorageQueryUsageValueType::Boolean => Compatible,
            Some(_) => Incompatible,
            None => Unknown,
        }
    }
}

impl TryFrom<PatternWire> for StorageQueryUsagePattern {
    type Error = StorageValidationError;
    fn try_from(value: PatternWire) -> Result<Self, Self::Error> {
        Self::try_new(&value.path, value.value_type, value.operations)
    }
}

impl From<StorageQueryUsagePattern> for PatternWire {
    fn from(value: StorageQueryUsagePattern) -> Self {
        Self {
            path: value.path.canonical().to_string(),
            value_type: value.value_type,
            operations: value.operations,
        }
    }
}

impl std::fmt::Debug for StorageQueryUsagePattern {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("StorageQueryUsagePattern")
            .field("value_type", &self.value_type)
            .field("operations", &self.operations)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum StorageQueryUsageSchemaCompatibility {
    Compatible,
    Incompatible,
    Unknown,
}

/// Recorded intent, independent of whether an adapter prepares any resource.
#[derive(Clone, Debug, PartialEq)]
pub struct StorageQueryUsageDeclaration {
    metadata: StorageRecordMetadata,
    class_id: ClassId,
    pattern: StorageQueryUsagePattern,
    created_by: Option<PrincipalId>,
    updated_by: Option<PrincipalId>,
}

impl StorageQueryUsageDeclaration {
    pub const fn new(
        metadata: StorageRecordMetadata,
        class_id: ClassId,
        pattern: StorageQueryUsagePattern,
        created_by: Option<PrincipalId>,
        updated_by: Option<PrincipalId>,
    ) -> Self {
        Self {
            metadata,
            class_id,
            pattern,
            created_by,
            updated_by,
        }
    }
    pub const fn metadata(&self) -> StorageRecordMetadata {
        self.metadata
    }
    pub const fn class_id(&self) -> ClassId {
        self.class_id
    }
    pub fn pattern(&self) -> &StorageQueryUsagePattern {
        &self.pattern
    }
    pub const fn created_by(&self) -> Option<PrincipalId> {
        self.created_by
    }
    pub const fn updated_by(&self) -> Option<PrincipalId> {
        self.updated_by
    }
    pub fn snapshot(&self) -> Value {
        json!({"id": self.metadata.id().id(), "class_id": self.class_id.id(), "pattern": self.pattern,
            "revision": self.metadata.revision().get(), "created_at": self.metadata.created_at(), "updated_at": self.metadata.updated_at(),
            "created_by": self.created_by, "updated_by": self.updated_by})
    }
    pub fn from_snapshot(value: Value) -> Result<Self, StorageValidationError> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Record {
            id: ResourceId,
            class_id: ClassId,
            pattern: StorageQueryUsagePattern,
            revision: ResourceRevision,
            created_at: DateTime<Utc>,
            updated_at: DateTime<Utc>,
            created_by: Option<PrincipalId>,
            updated_by: Option<PrincipalId>,
        }
        let record: Record = serde_json::from_value(value)
            .map_err(|error| StorageValidationError::invalid(error.to_string()))?;
        Ok(Self::new(
            StorageRecordMetadata::try_new(
                record.id,
                record.created_at,
                record.updated_at,
                record.revision,
            )?,
            record.class_id,
            record.pattern,
            record.created_by,
            record.updated_by,
        ))
    }
}

/// Adapters recheck this collection while holding the class mutation boundary.
#[derive(Clone, Copy, Debug)]
pub struct StorageQueryUsageScope {
    class_id: ClassId,
    authorized_collection: CollectionId,
}
impl StorageQueryUsageScope {
    pub const fn new(class_id: ClassId, authorized_collection: CollectionId) -> Self {
        Self {
            class_id,
            authorized_collection,
        }
    }
    pub const fn class_id(self) -> ClassId {
        self.class_id
    }
    pub const fn authorized_collection(self) -> CollectionId {
        self.authorized_collection
    }
}

#[derive(Clone)]
pub struct StorageQueryUsageCreate {
    scope: StorageQueryUsageScope,
    pattern: StorageQueryUsagePattern,
    context: EventContext,
}
impl StorageQueryUsageCreate {
    pub const fn new(
        scope: StorageQueryUsageScope,
        pattern: StorageQueryUsagePattern,
        context: EventContext,
    ) -> Self {
        Self {
            scope,
            pattern,
            context,
        }
    }
    pub const fn scope(&self) -> StorageQueryUsageScope {
        self.scope
    }
    pub fn pattern(&self) -> &StorageQueryUsagePattern {
        &self.pattern
    }
    pub const fn context(&self) -> &EventContext {
        &self.context
    }
}

#[derive(Clone)]
pub struct StorageQueryUsageReplace {
    create: StorageQueryUsageCreate,
    id: ResourceId,
    expected_revision: ResourceRevision,
}
impl StorageQueryUsageReplace {
    pub const fn new(
        create: StorageQueryUsageCreate,
        id: ResourceId,
        expected_revision: ResourceRevision,
    ) -> Self {
        Self {
            create,
            id,
            expected_revision,
        }
    }
    pub const fn scope(&self) -> StorageQueryUsageScope {
        self.create.scope()
    }
    pub const fn id(&self) -> ResourceId {
        self.id
    }
    pub const fn expected_revision(&self) -> ResourceRevision {
        self.expected_revision
    }
    pub fn pattern(&self) -> &StorageQueryUsagePattern {
        self.create.pattern()
    }
    pub const fn context(&self) -> &EventContext {
        self.create.context()
    }
}

#[derive(Clone)]
pub struct StorageQueryUsageDelete {
    scope: StorageQueryUsageScope,
    id: ResourceId,
    expected_revision: ResourceRevision,
    context: EventContext,
}
impl StorageQueryUsageDelete {
    pub const fn new(
        scope: StorageQueryUsageScope,
        id: ResourceId,
        expected_revision: ResourceRevision,
        context: EventContext,
    ) -> Self {
        Self {
            scope,
            id,
            expected_revision,
            context,
        }
    }
    pub const fn scope(&self) -> StorageQueryUsageScope {
        self.scope
    }
    pub const fn id(&self) -> ResourceId {
        self.id
    }
    pub const fn expected_revision(&self) -> ResourceRevision {
        self.expected_revision
    }
    pub const fn context(&self) -> &EventContext {
        &self.context
    }
}

/// Mandatory declaration management; optional native optimization is composed separately.
#[async_trait]
pub trait QueryUsageStorage: Send + Sync {
    /// Complete class-local list, bounded by MAX_QUERY_USAGE_DECLARATIONS.
    async fn list_query_usage(
        &self,
        scope: StorageQueryUsageScope,
    ) -> Result<Vec<StorageQueryUsageDeclaration>, StorageError>;
    async fn create_query_usage(
        &self,
        request: StorageQueryUsageCreate,
    ) -> Result<StorageMutationOutcome<StorageQueryUsageDeclaration>, StorageError>;
    async fn replace_query_usage(
        &self,
        request: StorageQueryUsageReplace,
    ) -> Result<StorageMutationOutcome<StorageQueryUsageDeclaration>, StorageError>;
    async fn delete_query_usage(
        &self,
        request: StorageQueryUsageDelete,
    ) -> Result<StorageMutationOutcome<()>, StorageError>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    #[rstest]
    #[case(json!({"path":"", "value_type":"string", "operations":["equals"]}))]
    #[case(json!({"path":"x.y", "value_type":"string", "operations":["equals"]}))]
    #[case(json!({"path":"x", "value_type":"string", "operations":["gt"]}))]
    #[case(json!({"path":"x", "value_type":"numeric", "operations":[]}))]
    #[case(json!({"path":"x", "value_type":"numeric", "operations":["contains"]}))]
    #[case(json!({"path":"x", "value_type":"string", "operations":["equals"], "sql":"ignored"}))]
    #[case(json!({"path":"x".repeat(513), "value_type":"string", "operations":["equals"]}))]
    #[case(json!({"path":vec!["x"; 33].join(","), "value_type":"string", "operations":["equals"]}))]
    fn boundary_rejects_invalid_patterns(#[case] value: Value) {
        assert!(serde_json::from_value::<StorageQueryUsagePattern>(value).is_err());
    }

    #[test]
    fn operation_sets_have_one_canonical_representation() {
        let pattern = StorageQueryUsagePattern::try_new(
            "hardware,memory_gb",
            StorageQueryUsageValueType::Numeric,
            vec![
                StorageQueryUsageOperation::Gt,
                StorageQueryUsageOperation::Equals,
                StorageQueryUsageOperation::Gt,
            ],
        )
        .unwrap();
        assert_eq!(
            serde_json::to_value(pattern).unwrap()["operations"],
            json!(["equals", "gt"])
        );
    }

    #[rstest]
    #[case(json!({"properties":{"value":{"type":"number"}}}), StorageQueryUsageSchemaCompatibility::Compatible)]
    #[case(json!({"properties":{"value":{"type":"string"}}}), StorageQueryUsageSchemaCompatibility::Incompatible)]
    #[case(json!({"properties":{"value":{"$ref":"#/$defs/value"}}}), StorageQueryUsageSchemaCompatibility::Unknown)]
    #[case(json!({}), StorageQueryUsageSchemaCompatibility::Unknown)]
    fn schema_assessment_is_conservative(
        #[case] schema: Value,
        #[case] expected: StorageQueryUsageSchemaCompatibility,
    ) {
        let pattern = StorageQueryUsagePattern::try_new(
            "value",
            StorageQueryUsageValueType::Numeric,
            vec![StorageQueryUsageOperation::Equals],
        )
        .unwrap();
        assert_eq!(pattern.schema_compatibility(Some(&schema)), expected);
    }
}
