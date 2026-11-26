use chrono::{DateTime, Utc};
use hubuum_domain::{ClassId, CollectionId, ObjectId, ResourceId, ResourceRevision};
use serde_json::Value;
use std::fmt;

use crate::StorageValidationError;

/// Backend-neutral identity and revision metadata shared by stored records.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StorageRecordMetadata {
    id: ResourceId,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    revision: ResourceRevision,
}

impl StorageRecordMetadata {
    pub fn try_new(
        id: ResourceId,
        created_at: DateTime<Utc>,
        updated_at: DateTime<Utc>,
        revision: ResourceRevision,
    ) -> Result<Self, StorageValidationError> {
        if updated_at < created_at {
            return Err(StorageValidationError::invalid(
                "record updated_at must not be earlier than created_at",
            ));
        }
        Ok(Self {
            id,
            created_at,
            updated_at,
            revision,
        })
    }

    #[must_use]
    pub const fn into_parts(self) -> (ResourceId, DateTime<Utc>, DateTime<Utc>, ResourceRevision) {
        (self.id, self.created_at, self.updated_at, self.revision)
    }

    #[must_use]
    pub const fn id(self) -> ResourceId {
        self.id
    }

    #[must_use]
    pub const fn created_at(self) -> DateTime<Utc> {
        self.created_at
    }

    #[must_use]
    pub const fn updated_at(self) -> DateTime<Utc> {
        self.updated_at
    }

    #[must_use]
    pub const fn revision(self) -> ResourceRevision {
        self.revision
    }
}

/// Canonical flat collection projection shared by storage capabilities.
#[derive(Clone, PartialEq, Eq)]
pub struct StorageCollection {
    id: CollectionId,
    name: String,
    description: String,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    parent_collection_id: Option<CollectionId>,
    revision: ResourceRevision,
}

impl fmt::Debug for StorageCollection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageCollection")
            .field("id", &self.id)
            .field("name", &"[redacted]")
            .field("description", &"[redacted]")
            .field("created_at", &self.created_at)
            .field("updated_at", &self.updated_at)
            .field("parent_collection_id", &self.parent_collection_id)
            .field("revision", &self.revision)
            .finish()
    }
}

impl StorageCollection {
    pub fn try_new(
        metadata: StorageRecordMetadata,
        name: impl Into<String>,
        description: impl Into<String>,
        parent_collection_id: Option<CollectionId>,
    ) -> Result<Self, StorageValidationError> {
        let id = CollectionId::from(metadata.id());
        if parent_collection_id == Some(id) {
            return Err(StorageValidationError::invalid(
                "collection must not be its own parent",
            ));
        }
        Ok(Self {
            id,
            name: name.into(),
            description: description.into(),
            created_at: metadata.created_at(),
            updated_at: metadata.updated_at(),
            parent_collection_id,
            revision: metadata.revision(),
        })
    }

    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        CollectionId,
        String,
        String,
        DateTime<Utc>,
        DateTime<Utc>,
        Option<CollectionId>,
        ResourceRevision,
    ) {
        (
            self.id,
            self.name,
            self.description,
            self.created_at,
            self.updated_at,
            self.parent_collection_id,
            self.revision,
        )
    }

    #[must_use]
    pub const fn id(&self) -> CollectionId {
        self.id
    }

    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    #[must_use]
    pub fn description(&self) -> &str {
        &self.description
    }

    #[must_use]
    pub const fn created_at(&self) -> DateTime<Utc> {
        self.created_at
    }

    #[must_use]
    pub const fn updated_at(&self) -> DateTime<Utc> {
        self.updated_at
    }

    #[must_use]
    pub const fn revision(&self) -> ResourceRevision {
        self.revision
    }

    #[must_use]
    pub const fn parent_collection_id(&self) -> Option<CollectionId> {
        self.parent_collection_id
    }

    /// Return the canonical collection snapshot stored in audit documents.
    ///
    /// Adapters should construct collection events from this projection rather
    /// than serializing native persistence rows. Timestamps intentionally use
    /// the existing UTC-naive wire representation of durable audit snapshots.
    #[must_use]
    pub fn audit_snapshot(&self) -> Value {
        serde_json::json!({
            "id": self.id.id(),
            "name": self.name,
            "description": self.description,
            "created_at": self.created_at.naive_utc(),
            "updated_at": self.updated_at.naive_utc(),
            "parent_collection_id": self.parent_collection_id.map(CollectionId::id),
            "revision": self.revision.get(),
        })
    }
}

/// Canonical flat object projection shared by storage capabilities.
#[derive(Clone, PartialEq)]
pub struct StorageObject {
    id: ObjectId,
    name: String,
    collection_id: CollectionId,
    class_id: ClassId,
    data: Value,
    description: String,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    revision: ResourceRevision,
}

impl fmt::Debug for StorageObject {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageObject")
            .field("id", &self.id)
            .field("name", &"[redacted]")
            .field("collection_id", &self.collection_id)
            .field("class_id", &self.class_id)
            .field("data", &"[redacted]")
            .field("description", &"[redacted]")
            .field("created_at", &self.created_at)
            .field("updated_at", &self.updated_at)
            .field("revision", &self.revision)
            .finish()
    }
}

impl StorageObject {
    #[must_use]
    pub fn new(
        metadata: StorageRecordMetadata,
        name: impl Into<String>,
        collection_id: CollectionId,
        class_id: ClassId,
        data: Value,
        description: impl Into<String>,
    ) -> Self {
        Self {
            id: ObjectId::from(metadata.id()),
            name: name.into(),
            collection_id,
            class_id,
            data,
            description: description.into(),
            created_at: metadata.created_at(),
            updated_at: metadata.updated_at(),
            revision: metadata.revision(),
        }
    }

    #[must_use]
    #[allow(clippy::type_complexity)]
    pub fn into_parts(
        self,
    ) -> (
        ObjectId,
        String,
        CollectionId,
        ClassId,
        Value,
        String,
        DateTime<Utc>,
        DateTime<Utc>,
        ResourceRevision,
    ) {
        (
            self.id,
            self.name,
            self.collection_id,
            self.class_id,
            self.data,
            self.description,
            self.created_at,
            self.updated_at,
            self.revision,
        )
    }

    #[must_use]
    pub const fn id(&self) -> ObjectId {
        self.id
    }

    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    #[must_use]
    pub const fn collection_id(&self) -> CollectionId {
        self.collection_id
    }

    #[must_use]
    pub const fn class_id(&self) -> ClassId {
        self.class_id
    }

    #[must_use]
    pub const fn data(&self) -> &Value {
        &self.data
    }

    #[must_use]
    pub fn description(&self) -> &str {
        &self.description
    }

    #[must_use]
    pub const fn created_at(&self) -> DateTime<Utc> {
        self.created_at
    }

    #[must_use]
    pub const fn updated_at(&self) -> DateTime<Utc> {
        self.updated_at
    }

    #[must_use]
    pub const fn revision(&self) -> ResourceRevision {
        self.revision
    }

    /// Return the canonical object snapshot stored in audit documents.
    #[must_use]
    pub fn audit_snapshot(&self) -> Value {
        serde_json::json!({
            "id": self.id.id(),
            "name": self.name,
            "collection_id": self.collection_id.id(),
            "hubuum_class_id": self.class_id.id(),
            "data": self.data,
            "description": self.description,
            "created_at": self.created_at.naive_utc(),
            "updated_at": self.updated_at.naive_utc(),
            "revision": self.revision.get(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn collection_audit_snapshot_has_backend_independent_shape() {
        let created_at = "2026-08-30T12:00:00Z".parse().unwrap();
        let updated_at = "2026-08-30T12:01:00Z".parse().unwrap();
        let metadata = StorageRecordMetadata::try_new(
            ResourceId::new(7).unwrap(),
            created_at,
            updated_at,
            ResourceRevision::new(3).unwrap(),
        )
        .unwrap();
        let collection = StorageCollection::try_new(
            metadata,
            "portable",
            "canonical",
            Some(CollectionId::new(2).unwrap()),
        )
        .unwrap();

        assert_eq!(
            collection.audit_snapshot(),
            serde_json::json!({
                "id": 7,
                "name": "portable",
                "description": "canonical",
                "created_at": "2026-08-30T12:00:00",
                "updated_at": "2026-08-30T12:01:00",
                "parent_collection_id": 2,
                "revision": 3,
            })
        );
    }
}
