use chrono::{DateTime, Utc};
use hubuum_domain::{ClassId, CollectionId, ObjectId};
use hubuum_query::{CursorValue, FilterField, QueryOptions, SortParam};

use super::{StorageHistoryMetadata, StorageHistoryOperation, StorageHistoryResource};
use crate::{StorageClassRelation, StorageObjectRelation, StorageValidationError};

/// The endpoint whose direct incident relationship changes are requested.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum StorageRelationHistoryResource {
    Class(ClassId),
    Object(ObjectId),
}

/// Changes include deletion markers; point-in-time adjacency includes active edges only.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StorageRelationHistorySelection {
    Changes,
    At(DateTime<Utc>),
}

#[derive(Clone)]
pub struct StorageRelationHistoryQuery {
    resource: StorageRelationHistoryResource,
    selection: StorageRelationHistorySelection,
    options: QueryOptions,
}

impl StorageRelationHistoryQuery {
    pub fn try_new(
        resource: StorageRelationHistoryResource,
        selection: StorageRelationHistorySelection,
        mut options: QueryOptions,
    ) -> Result<Self, StorageValidationError> {
        if options.limit().is_none()
            || !options.filters().is_empty()
            || options.structured_filter().is_some()
            || options
                .sort()
                .iter()
                .any(|sort| sort.field != FilterField::HistoryId)
        {
            return Err(StorageValidationError::invalid(
                "Relationship history requires a page limit, supports only history_id ordering, and does not support filters",
            ));
        }
        if options.sort().is_empty() {
            options
                .sort_mut()
                .append_tie_breaker(SortParam::new(FilterField::HistoryId, true))
                .map_err(|error| StorageValidationError::invalid(error.to_string()))?;
        }
        // Validate the decoded boundary before an adapter constructs a query.
        if let Some(values) = options
            .cursor_values(options.sort())
            .map_err(|error| StorageValidationError::invalid(error.to_string()))?
            && !matches!(values.as_ref(), [CursorValue::Integer(value)] if *value > 0)
        {
            return Err(StorageValidationError::invalid(
                "Relationship history requires a positive history ID cursor",
            ));
        }
        Ok(Self {
            resource,
            selection,
            options,
        })
    }

    pub fn with_options(self, options: QueryOptions) -> Result<Self, StorageValidationError> {
        Self::try_new(self.resource, self.selection, options)
    }

    pub const fn resource(&self) -> StorageRelationHistoryResource {
        self.resource
    }
    pub const fn selection(&self) -> StorageRelationHistorySelection {
        self.selection
    }
    pub const fn options(&self) -> &QueryOptions {
        &self.options
    }
}

/// The stored relationship shape, independent of current endpoint membership.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StorageHistoricalRelation {
    Class(StorageClassRelation),
    Object(StorageObjectRelation),
}

impl StorageHistoricalRelation {
    pub fn endpoints(&self) -> [StorageRelationHistoryResource; 2] {
        match self {
            Self::Class(row) => [
                StorageRelationHistoryResource::Class(row.from_class_id()),
                StorageRelationHistoryResource::Class(row.to_class_id()),
            ],
            Self::Object(row) => [
                StorageRelationHistoryResource::Object(row.from_object_id()),
                StorageRelationHistoryResource::Object(row.to_object_id()),
            ],
        }
    }
}

#[derive(Clone, PartialEq)]
pub struct StorageRelationHistoryRecord {
    relation: StorageHistoricalRelation,
    metadata: StorageHistoryMetadata,
}

impl StorageRelationHistoryRecord {
    pub fn try_new(
        relation: StorageHistoricalRelation,
        metadata: StorageHistoryMetadata,
    ) -> Result<Self, StorageValidationError> {
        let revision = match &relation {
            StorageHistoricalRelation::Class(row) => row.metadata().revision(),
            StorageHistoricalRelation::Object(row) => row.metadata().revision(),
        };
        if revision != metadata.revision() {
            return Err(StorageValidationError::invalid(
                "Relationship and history revisions must match",
            ));
        }
        Ok(Self { relation, metadata })
    }
    pub const fn relation(&self) -> &StorageHistoricalRelation {
        &self.relation
    }
    pub const fn metadata(&self) -> &StorageHistoryMetadata {
        &self.metadata
    }
    pub fn into_parts(self) -> (StorageHistoricalRelation, StorageHistoryMetadata) {
        (self.relation, self.metadata)
    }
}

/// Lightweight historical endpoint facts. Payload data and schemas are not hydrated.
#[derive(Clone, PartialEq)]
pub struct StorageHistoryEndpointSnapshot {
    resource: StorageHistoryResource,
    collection_id: CollectionId,
    name: String,
    metadata: StorageHistoryMetadata,
}

impl StorageHistoryEndpointSnapshot {
    pub fn try_new(
        resource: StorageHistoryResource,
        collection_id: CollectionId,
        name: String,
        metadata: StorageHistoryMetadata,
    ) -> Result<Self, StorageValidationError> {
        if metadata.operation() == StorageHistoryOperation::Delete {
            return Err(StorageValidationError::invalid(
                "An effective endpoint cannot be a deletion marker",
            ));
        }
        Ok(Self {
            resource,
            collection_id,
            name,
            metadata,
        })
    }
    pub const fn resource(&self) -> StorageHistoryResource {
        self.resource
    }
    pub const fn collection_id(&self) -> CollectionId {
        self.collection_id
    }
    pub fn name(&self) -> &str {
        &self.name
    }
    pub const fn metadata(&self) -> &StorageHistoryMetadata {
        &self.metadata
    }
    pub fn identity(&self) -> StorageRelationHistoryResource {
        match self.resource {
            StorageHistoryResource::Class(id) => StorageRelationHistoryResource::Class(id),
            StorageHistoryResource::Object { object_id, .. } => {
                StorageRelationHistoryResource::Object(object_id)
            }
        }
    }
}

/// One relationship with endpoint context proven to belong to the selected instant.
/// Missing retained endpoint context stays absent; adapters must never use live rows.
#[derive(Clone, PartialEq)]
pub struct StorageRelationHistoryEntry {
    record: StorageRelationHistoryRecord,
    context_at: DateTime<Utc>,
    from: Option<StorageHistoryEndpointSnapshot>,
    to: Option<StorageHistoryEndpointSnapshot>,
}

impl StorageRelationHistoryEntry {
    pub fn try_new(
        record: StorageRelationHistoryRecord,
        selection: StorageRelationHistorySelection,
        from: Option<StorageHistoryEndpointSnapshot>,
        to: Option<StorageHistoryEndpointSnapshot>,
    ) -> Result<Self, StorageValidationError> {
        let context_at = match selection {
            StorageRelationHistorySelection::Changes => record.metadata.valid_from(),
            StorageRelationHistorySelection::At(at) => {
                if !effective(&record.metadata, at) {
                    return Err(StorageValidationError::invalid(
                        "Historical adjacency requires an effective non-deleted edge",
                    ));
                }
                at
            }
        };
        for (endpoint, identity) in [from.as_ref(), to.as_ref()]
            .into_iter()
            .zip(record.relation.endpoints())
        {
            if endpoint.is_some_and(|endpoint| {
                endpoint.identity() != identity || !effective(endpoint.metadata(), context_at)
            }) {
                return Err(StorageValidationError::invalid(
                    "Historical relationship endpoint identity or validity does not match",
                ));
            }
        }
        Ok(Self {
            record,
            context_at,
            from,
            to,
        })
    }
    pub const fn record(&self) -> &StorageRelationHistoryRecord {
        &self.record
    }
    pub const fn context_at(&self) -> DateTime<Utc> {
        self.context_at
    }
    pub const fn from(&self) -> Option<&StorageHistoryEndpointSnapshot> {
        self.from.as_ref()
    }
    pub const fn to(&self) -> Option<&StorageHistoryEndpointSnapshot> {
        self.to.as_ref()
    }
    pub fn into_parts(
        self,
    ) -> (
        StorageRelationHistoryRecord,
        DateTime<Utc>,
        Option<StorageHistoryEndpointSnapshot>,
        Option<StorageHistoryEndpointSnapshot>,
    ) {
        (self.record, self.context_at, self.from, self.to)
    }
}

fn effective(metadata: &StorageHistoryMetadata, at: DateTime<Utc>) -> bool {
    metadata.operation() != StorageHistoryOperation::Delete
        && metadata.valid_from() <= at
        && metadata.valid_to().is_none_or(|end| at < end)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::StorageRecordMetadata;
    use hubuum_domain::{HistoryRecordId, ResourceId, ResourceRevision};
    use rstest::rstest;

    fn at() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
            .unwrap()
            .to_utc()
    }

    #[rstest]
    #[case::wrong_identity(3, 0)]
    #[case::future_version(1, 1)]
    #[test]
    fn relationship_context_rejects_unproven_endpoint(#[case] id: i32, #[case] offset: i64) {
        let metadata = || {
            StorageHistoryMetadata::try_new(
                StorageHistoryOperation::Create,
                at(),
                None,
                HistoryRecordId::new(1).unwrap(),
                ResourceRevision::INITIAL,
            )
            .unwrap()
        };
        let relation = StorageClassRelation::try_new(
            StorageRecordMetadata::try_new(
                ResourceId::new(1).unwrap(),
                at(),
                at(),
                ResourceRevision::INITIAL,
            )
            .unwrap(),
            ClassId::new(1).unwrap(),
            ClassId::new(2).unwrap(),
        )
        .unwrap();
        let record = StorageRelationHistoryRecord::try_new(
            StorageHistoricalRelation::Class(relation),
            metadata(),
        )
        .unwrap();
        let endpoint_metadata = StorageHistoryMetadata::try_new(
            StorageHistoryOperation::Create,
            at() + chrono::Duration::nanoseconds(offset),
            None,
            HistoryRecordId::new(2).unwrap(),
            ResourceRevision::INITIAL,
        )
        .unwrap();
        let endpoint = StorageHistoryEndpointSnapshot::try_new(
            StorageHistoryResource::Class(ClassId::new(id).unwrap()),
            CollectionId::new(1).unwrap(),
            "endpoint".into(),
            endpoint_metadata,
        )
        .unwrap();
        assert!(
            StorageRelationHistoryEntry::try_new(
                record,
                StorageRelationHistorySelection::At(at()),
                Some(endpoint),
                None
            )
            .is_err()
        );
    }
}
