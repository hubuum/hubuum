use std::fmt;

use crate::errors::ApiError;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StorageErrorKind {
    BadRequest,
    Conflict,
    Database,
    Internal,
    NotFound,
    NotAcceptable,
    PayloadTooLarge,
    PreconditionFailed,
    Unavailable,
    Validation,
}

/// Backend-neutral failure returned by storage capabilities.
///
/// The representation deliberately carries no Diesel or Actix types. API
/// adapters translate it into [`ApiError`] only at the application boundary.
#[derive(Debug, PartialEq, Eq)]
pub struct StorageError {
    kind: StorageErrorKind,
    message: String,
    current_etag: Option<String>,
}

impl StorageError {
    pub fn bad_request(message: impl Into<String>) -> Self {
        Self::new(StorageErrorKind::BadRequest, message, None)
    }

    pub fn conflict(message: impl Into<String>) -> Self {
        Self::new(StorageErrorKind::Conflict, message, None)
    }

    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(StorageErrorKind::NotFound, message, None)
    }

    fn new(
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
}

impl fmt::Display for StorageError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for StorageError {}

impl From<ApiError> for StorageError {
    fn from(error: ApiError) -> Self {
        match error {
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
                    "unexpected storage adapter error ({}): {error}",
                    error.class()
                ),
                None,
            ),
        }
    }
}

impl From<StorageError> for ApiError {
    fn from(error: StorageError) -> Self {
        match error.kind {
            StorageErrorKind::BadRequest => Self::BadRequest(error.message),
            StorageErrorKind::Conflict => Self::Conflict(error.message),
            StorageErrorKind::Database => Self::DatabaseError(error.message),
            StorageErrorKind::Internal => Self::InternalServerError(error.message),
            StorageErrorKind::NotFound => Self::NotFound(error.message),
            StorageErrorKind::NotAcceptable => Self::NotAcceptable(error.message),
            StorageErrorKind::PayloadTooLarge => Self::PayloadTooLarge(error.message),
            StorageErrorKind::PreconditionFailed => {
                Self::PreconditionFailed(error.message, error.current_etag)
            }
            StorageErrorKind::Unavailable => Self::ServiceUnavailable(error.message),
            StorageErrorKind::Validation => Self::ValidationError(error.message),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::errors::ApiError;

    use super::StorageError;

    #[test]
    fn storage_errors_preserve_public_failure_categories() {
        for error in [
            ApiError::BadRequest("invalid move".to_string()),
            ApiError::Conflict("collection has children".to_string()),
            ApiError::NotFound("collection missing".to_string()),
            ApiError::ValidationError("object schema mismatch".to_string()),
            ApiError::PayloadTooLarge("object data exceeds its limit".to_string()),
            ApiError::PreconditionFailed(
                "stale collection".to_string(),
                Some("\"collection-1-r2\"".to_string()),
            ),
        ] {
            let expected_class = error.class();
            let expected_message = error.public_message().to_string();
            let round_trip = ApiError::from(StorageError::from(error));
            assert_eq!(round_trip.class(), expected_class);
            assert_eq!(round_trip.public_message(), expected_message);
        }
    }
}
