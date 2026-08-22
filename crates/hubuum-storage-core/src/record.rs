use chrono::{DateTime, Utc};
use hubuum_domain::{ResourceId, ResourceRevision};

use crate::StorageError;

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
    ) -> Result<Self, StorageError> {
        if updated_at < created_at {
            return Err(StorageError::internal(
                "Persisted record updated_at must not be earlier than created_at",
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
