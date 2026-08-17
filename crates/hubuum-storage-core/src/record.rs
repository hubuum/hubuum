use chrono::NaiveDateTime;
use hubuum_domain::{ResourceId, ResourceRevision};

/// Backend-neutral identity and revision metadata shared by stored records.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StorageRecordMetadata {
    id: ResourceId,
    created_at: NaiveDateTime,
    updated_at: NaiveDateTime,
    revision: ResourceRevision,
}

impl StorageRecordMetadata {
    #[must_use]
    pub const fn new(
        id: ResourceId,
        created_at: NaiveDateTime,
        updated_at: NaiveDateTime,
        revision: ResourceRevision,
    ) -> Self {
        Self {
            id,
            created_at,
            updated_at,
            revision,
        }
    }

    #[must_use]
    pub const fn into_parts(self) -> (ResourceId, NaiveDateTime, NaiveDateTime, ResourceRevision) {
        (self.id, self.created_at, self.updated_at, self.revision)
    }

    #[must_use]
    pub const fn id(self) -> ResourceId {
        self.id
    }

    #[must_use]
    pub const fn created_at(self) -> NaiveDateTime {
        self.created_at
    }

    #[must_use]
    pub const fn updated_at(self) -> NaiveDateTime {
        self.updated_at
    }

    #[must_use]
    pub const fn revision(self) -> ResourceRevision {
        self.revision
    }
}
