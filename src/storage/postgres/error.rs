use std::fmt;

use crate::errors::ApiError;

use super::super::{StorageError, StorageErrorKind};

/// PostgreSQL adapter failure before it crosses the neutral storage boundary.
///
/// The adapter currently delegates to PostgreSQL operations that return
/// `ApiError`. Wrapping them here prevents that application error type from
/// becoming part of the storage contract and gives the PostgreSQL adapter one
/// deliberate translation point.
#[derive(Debug)]
pub(super) struct PostgresStorageError {
    source: ApiError,
}

impl From<ApiError> for PostgresStorageError {
    fn from(source: ApiError) -> Self {
        Self { source }
    }
}

impl fmt::Display for PostgresStorageError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.source)
    }
}

impl std::error::Error for PostgresStorageError {}

impl From<PostgresStorageError> for StorageError {
    fn from(error: PostgresStorageError) -> Self {
        let source = error.source;
        match source {
            ApiError::PermissionBackendUnavailable(message) => {
                Self::new(StorageErrorKind::AuthorizationUnavailable, message, None)
            }
            ApiError::BadRequest(message)
            | ApiError::InvalidIntegerRange(message)
            | ApiError::OperatorMismatch(message) => {
                Self::new(StorageErrorKind::BadRequest, message, None)
            }
            ApiError::NotAcceptable(message) => {
                Self::new(StorageErrorKind::NotAcceptable, message, None)
            }
            ApiError::ValidationError(message) => {
                Self::new(StorageErrorKind::Validation, message, None)
            }
            ApiError::PayloadTooLarge(message) => {
                Self::new(StorageErrorKind::PayloadTooLarge, message, None)
            }
            ApiError::Conflict(message) => Self::new(StorageErrorKind::Conflict, message, None),
            ApiError::DatabaseError(message) | ApiError::DbConnectionError(message) => {
                Self::new(StorageErrorKind::Database, message, None)
            }
            ApiError::NotFound(message) | ApiError::Gone(message) => {
                Self::new(StorageErrorKind::NotFound, message, None)
            }
            ApiError::PreconditionFailed(message, current_etag) => {
                Self::new(StorageErrorKind::PreconditionFailed, message, current_etag)
            }
            ApiError::ServiceUnavailable(message) => {
                Self::new(StorageErrorKind::Unavailable, message, None)
            }
            error => Self::new(
                StorageErrorKind::Internal,
                format!(
                    "unexpected PostgreSQL storage adapter error ({}): {error}",
                    error.class()
                ),
                None,
            ),
        }
    }
}

pub(super) fn map_postgres_error(error: ApiError) -> StorageError {
    PostgresStorageError::from(error).into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn postgres_errors_are_classified_before_crossing_the_storage_boundary() {
        assert_eq!(
            map_postgres_error(ApiError::DatabaseError("query failed".to_string())).kind(),
            StorageErrorKind::Database
        );
        assert_eq!(
            map_postgres_error(ApiError::NotFound("missing".to_string())).kind(),
            StorageErrorKind::NotFound
        );
        assert_eq!(
            map_postgres_error(ApiError::PermissionBackendUnavailable(
                "policy unavailable".to_string()
            ))
            .kind(),
            StorageErrorKind::AuthorizationUnavailable
        );
    }
}
