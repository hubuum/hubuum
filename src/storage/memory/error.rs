use std::fmt;

use crate::errors::ApiError;

use super::super::{StorageError, StorageErrorKind};

/// Failure produced by the focused in-memory lifecycle contract model.
#[derive(Debug)]
pub(super) struct MemoryStorageModelError {
    source: ApiError,
}

impl From<ApiError> for MemoryStorageModelError {
    fn from(source: ApiError) -> Self {
        Self { source }
    }
}

impl fmt::Display for MemoryStorageModelError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.source)
    }
}

impl std::error::Error for MemoryStorageModelError {}

impl From<MemoryStorageModelError> for StorageError {
    fn from(error: MemoryStorageModelError) -> Self {
        let source = error.source;
        match source {
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
                    "unexpected memory storage model error ({}): {error}",
                    error.class()
                ),
                None,
            ),
        }
    }
}

pub(super) fn map_memory_error(error: ApiError) -> StorageError {
    MemoryStorageModelError::from(error).into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn memory_model_errors_are_classified_before_crossing_the_storage_boundary() {
        assert_eq!(
            map_memory_error(ApiError::ValidationError("invalid value".to_string())).kind(),
            StorageErrorKind::Validation
        );
        assert_eq!(
            map_memory_error(ApiError::Conflict("duplicate".to_string())).kind(),
            StorageErrorKind::Conflict
        );
    }
}
