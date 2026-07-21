//! App-neutral task request primitives.
//!
//! This crate owns invariants that must hold before task requests reach a
//! persistence backend. It intentionally has no dependencies on Actix, Diesel,
//! global application configuration, or task storage.

use std::fmt;
use std::str::FromStr;

/// Maximum UTF-8 byte length accepted for an idempotency key.
///
/// The bound keeps keys comfortably within PostgreSQL B-tree index-entry
/// limits while leaving enough room for structured client-generated values.
pub const MAX_IDEMPOTENCY_KEY_BYTES: usize = 255;

/// A non-empty, storage-safe task idempotency key.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct IdempotencyKey(String);

impl IdempotencyKey {
    pub fn new(value: impl Into<String>) -> Result<Self, IdempotencyKeyError> {
        let value = value.into();
        if value.is_empty() {
            return Err(IdempotencyKeyError::Empty);
        }
        if value.len() > MAX_IDEMPOTENCY_KEY_BYTES {
            return Err(IdempotencyKeyError::TooLong {
                actual: value.len(),
                maximum: MAX_IDEMPOTENCY_KEY_BYTES,
            });
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn into_inner(self) -> String {
        self.0
    }
}

impl fmt::Display for IdempotencyKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl FromStr for IdempotencyKey {
    type Err = IdempotencyKeyError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}

impl TryFrom<String> for IdempotencyKey {
    type Error = IdempotencyKeyError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IdempotencyKeyError {
    Empty,
    TooLong { actual: usize, maximum: usize },
}

impl fmt::Display for IdempotencyKeyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => formatter.write_str("idempotency key must not be empty"),
            Self::TooLong { actual, maximum } => write!(
                formatter,
                "idempotency key is {actual} bytes; the maximum is {maximum} bytes"
            ),
        }
    }
}

impl std::error::Error for IdempotencyKeyError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_a_key_at_the_storage_boundary() {
        let key = IdempotencyKey::new("x".repeat(MAX_IDEMPOTENCY_KEY_BYTES)).unwrap();

        assert_eq!(key.as_str().len(), MAX_IDEMPOTENCY_KEY_BYTES);
    }

    #[test]
    fn rejects_an_empty_key() {
        assert_eq!(
            IdempotencyKey::new("").unwrap_err(),
            IdempotencyKeyError::Empty
        );
    }

    #[test]
    fn rejects_a_key_above_the_storage_boundary() {
        let error = IdempotencyKey::new("x".repeat(MAX_IDEMPOTENCY_KEY_BYTES + 1)).unwrap_err();

        assert_eq!(
            error,
            IdempotencyKeyError::TooLong {
                actual: MAX_IDEMPOTENCY_KEY_BYTES + 1,
                maximum: MAX_IDEMPOTENCY_KEY_BYTES,
            }
        );
    }

    #[test]
    fn measures_the_storage_boundary_in_utf8_bytes() {
        let value = "é".repeat((MAX_IDEMPOTENCY_KEY_BYTES / 2) + 1);

        let error = IdempotencyKey::new(value).unwrap_err();

        assert!(matches!(error, IdempotencyKeyError::TooLong { .. }));
    }
}
