use std::fmt;

use crate::errors::ApiError;

use super::super::{StorageError, StorageErrorKind};

/// Failure produced by the focused in-memory resource model.
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
            | ApiError::OperatorMismatch(message) => Self::invalid_input(message),
            ApiError::NotAcceptable(message) => Self::invalid_input(message),
            ApiError::ValidationError(message) => Self::validation_failed(message),
            ApiError::PayloadTooLarge(message) => Self::input_too_large(message),
            ApiError::Conflict(message) => Self::conflict(message),
            ApiError::DatabaseError(message) | ApiError::DbConnectionError(message) => {
                Self::backend_failure(message)
            }
            ApiError::NotFound(message) | ApiError::Gone(message) => Self::not_found(message),
            ApiError::PreconditionFailed(message, _) => Self::precondition_failed(message, None),
            ApiError::RevisionConflict(message, current_revision) => {
                Self::revision_conflict(message, current_revision)
            }
            ApiError::TooManyRequests(message) => Self::rate_limited(message),
            ApiError::ServiceUnavailable(message) => Self::unavailable(message),
            error => Self::internal(format!(
                "unexpected memory storage model error ({}): {error}",
                error.class()
            )),
        }
    }
}

pub(super) fn map_memory_error(error: impl Into<ApiError>) -> StorageError {
    MemoryStorageModelError::from(error.into()).into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn memory_model_errors_are_classified_before_crossing_the_storage_boundary() {
        assert_eq!(
            map_memory_error(ApiError::ValidationError("invalid value".to_string())).kind(),
            StorageErrorKind::ValidationFailed
        );
        assert_eq!(
            map_memory_error(ApiError::Conflict("duplicate".to_string())).kind(),
            StorageErrorKind::Conflict
        );
    }
}
