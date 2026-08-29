use std::fmt;

use chrono::{DateTime, Utc};

use crate::StorageError;

/// The reason a value failed storage-contract validation.
///
/// This is deliberately independent of [`crate::StorageErrorKind`]. The same
/// malformed value is caller input when an application constructs it and a
/// backend failure when an adapter projects it from persisted state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StorageValidationErrorKind {
    InvalidValue,
    ValueTooLarge,
}

/// An unclassified failure to construct a storage-contract value.
///
/// Applications must explicitly map this to a request-facing error, while
/// adapters must map failures for persisted projections to a backend error.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StorageValidationError {
    kind: StorageValidationErrorKind,
    message: String,
}

impl StorageValidationError {
    pub(crate) fn invalid(message: impl Into<String>) -> Self {
        Self::new(StorageValidationErrorKind::InvalidValue, message)
    }

    pub(crate) fn too_large(message: impl Into<String>) -> Self {
        Self::new(StorageValidationErrorKind::ValueTooLarge, message)
    }

    fn new(kind: StorageValidationErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    #[must_use]
    pub const fn kind(&self) -> StorageValidationErrorKind {
        self.kind
    }

    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }

    #[must_use]
    pub fn into_parts(self) -> (StorageValidationErrorKind, String) {
        (self.kind, self.message)
    }

    /// Classify a value supplied by an application or API caller.
    #[must_use]
    pub fn into_request_error(self) -> StorageError {
        match self.kind {
            StorageValidationErrorKind::InvalidValue => StorageError::invalid_input(self.message),
            StorageValidationErrorKind::ValueTooLarge => {
                StorageError::input_too_large(self.message)
            }
        }
    }
}

impl fmt::Display for StorageValidationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for StorageValidationError {}

pub(crate) fn validate_sync_timestamps(
    attempted_at: Option<DateTime<Utc>>,
    succeeded_at: Option<DateTime<Utc>>,
) -> Result<(), StorageValidationError> {
    if succeeded_at.is_some() && attempted_at.is_none() {
        return Err(StorageValidationError::invalid(
            "A successful synchronization requires an attempted timestamp",
        ));
    }
    if attempted_at
        .zip(succeeded_at)
        .is_some_and(|(attempted, succeeded)| succeeded > attempted)
    {
        return Err(StorageValidationError::invalid(
            "A successful synchronization timestamp must not follow the latest attempted timestamp",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::StorageErrorKind;

    #[test]
    fn request_mapping_preserves_the_validation_reason() {
        assert_eq!(
            StorageValidationError::invalid("invalid")
                .into_request_error()
                .kind(),
            StorageErrorKind::InvalidInput
        );
        assert_eq!(
            StorageValidationError::too_large("large")
                .into_request_error()
                .kind(),
            StorageErrorKind::InputTooLarge
        );
    }

    #[test]
    fn synchronization_success_requires_an_attempt() {
        let error = validate_sync_timestamps(None, Some(Utc::now())).unwrap_err();

        assert_eq!(error.kind(), StorageValidationErrorKind::InvalidValue);
    }

    #[test]
    fn synchronization_success_must_not_follow_the_latest_attempt() {
        let attempted_at = Utc::now();
        let error = validate_sync_timestamps(
            Some(attempted_at),
            Some(attempted_at + chrono::Duration::seconds(1)),
        )
        .unwrap_err();

        assert_eq!(error.kind(), StorageValidationErrorKind::InvalidValue);
    }
}
