use std::fmt;

use hubuum_storage_core::{StorageError, StorageErrorKind};

/// Failure classified by the PostgreSQL adapter before crossing the storage
/// contract.
///
/// Diesel, pool, and PostgreSQL implementation errors are translated into
/// this type inside the adapter. Consumers receive only [`StorageError`].
#[derive(Debug)]
pub struct PostgresStorageError {
    kind: StorageErrorKind,
    message: String,
    current_etag: Option<String>,
}

impl PostgresStorageError {
    #[must_use]
    pub fn new(
        kind: StorageErrorKind,
        message: impl Into<String>,
        current_etag: Option<String>,
    ) -> Self {
        Self {
            kind,
            message: message.into(),
            current_etag,
        }
    }

    #[must_use]
    pub const fn kind(&self) -> StorageErrorKind {
        self.kind
    }
}

impl fmt::Display for PostgresStorageError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for PostgresStorageError {}

impl From<PostgresStorageError> for StorageError {
    fn from(error: PostgresStorageError) -> Self {
        Self::new(error.kind, error.message, error.current_etag)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn postgres_errors_cross_the_boundary_as_storage_errors() {
        let error = StorageError::from(PostgresStorageError::new(
            StorageErrorKind::PreconditionFailed,
            "stale resource",
            Some("etag".to_string()),
        ));

        assert_eq!(error.kind(), StorageErrorKind::PreconditionFailed);
        assert_eq!(error.current_etag(), Some("etag"));
    }
}
