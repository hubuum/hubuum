use chrono::NaiveDateTime;

/// Backend-neutral identity and revision metadata shared by stored records.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StorageRecordMetadata {
    id: i32,
    created_at: NaiveDateTime,
    updated_at: NaiveDateTime,
    revision: i64,
}

impl StorageRecordMetadata {
    #[must_use]
    pub const fn new(
        id: i32,
        created_at: NaiveDateTime,
        updated_at: NaiveDateTime,
        revision: i64,
    ) -> Self {
        Self {
            id,
            created_at,
            updated_at,
            revision,
        }
    }

    #[must_use]
    pub const fn into_parts(self) -> (i32, NaiveDateTime, NaiveDateTime, i64) {
        (self.id, self.created_at, self.updated_at, self.revision)
    }

    #[must_use]
    pub const fn id(self) -> i32 {
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
    pub const fn revision(self) -> i64 {
        self.revision
    }
}
