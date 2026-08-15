use std::fmt;

use diesel::result::{DatabaseErrorKind, Error as DieselError};
use diesel_async::pooled_connection::bb8::RunError as PoolError;
use hubuum_domain::{JsonSchemaError, JsonSchemaErrorKind};
use hubuum_storage_core::{StorageError, StorageErrorKind};
use tracing::{debug, error};

const OBJECT_RELATION_CARDINALITY_CONSTRAINT: &str = "hubuumobject_relation_cardinality";

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

    #[must_use]
    pub fn database(message: impl Into<String>) -> Self {
        Self::new(StorageErrorKind::Database, message, None)
    }

    #[must_use]
    pub fn bad_request(message: impl Into<String>) -> Self {
        Self::new(StorageErrorKind::BadRequest, message, None)
    }

    #[must_use]
    pub fn conflict(message: impl Into<String>) -> Self {
        Self::new(StorageErrorKind::Conflict, message, None)
    }

    #[must_use]
    pub fn forbidden(message: impl Into<String>) -> Self {
        Self::new(StorageErrorKind::Forbidden, message, None)
    }

    #[must_use]
    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(StorageErrorKind::Internal, message, None)
    }

    #[must_use]
    pub fn payload_too_large(message: impl Into<String>) -> Self {
        Self::new(StorageErrorKind::PayloadTooLarge, message, None)
    }

    #[must_use]
    pub fn too_many_requests(message: impl Into<String>) -> Self {
        Self::new(StorageErrorKind::TooManyRequests, message, None)
    }

    #[must_use]
    pub fn validation(message: impl Into<String>) -> Self {
        Self::new(StorageErrorKind::Validation, message, None)
    }

    #[must_use]
    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(StorageErrorKind::NotFound, message, None)
    }

    #[must_use]
    pub fn unavailable(message: impl Into<String>) -> Self {
        Self::new(StorageErrorKind::Unavailable, message, None)
    }

    #[must_use]
    pub fn precondition_failed(message: impl Into<String>, current_etag: Option<String>) -> Self {
        Self::new(StorageErrorKind::PreconditionFailed, message, current_etag)
    }
}

impl From<JsonSchemaError> for PostgresStorageError {
    fn from(error: JsonSchemaError) -> Self {
        let (kind, message) = error.into_parts();
        match kind {
            JsonSchemaErrorKind::InvalidSchema => Self::bad_request(message),
            JsonSchemaErrorKind::InvalidValue => Self::validation(message),
        }
    }
}

impl fmt::Display for PostgresStorageError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for PostgresStorageError {}

impl From<StorageError> for PostgresStorageError {
    fn from(error: StorageError) -> Self {
        let (kind, message, current_etag) = error.into_parts();
        Self::new(kind, message, current_etag)
    }
}

impl From<PoolError> for PostgresStorageError {
    fn from(error: PoolError) -> Self {
        error!(
            message = "Unable to get a PostgreSQL connection from the pool",
            backend = "postgresql",
            error = ?error,
        );
        Self::database(error.to_string())
    }
}

impl From<DieselError> for PostgresStorageError {
    fn from(error: DieselError) -> Self {
        match error {
            DieselError::NotFound => {
                debug!(
                    message = "PostgreSQL entity not found",
                    backend = "postgresql"
                );
                Self::not_found("Entity not found")
            }
            DieselError::DatabaseError(DatabaseErrorKind::UniqueViolation, _) => Self::new(
                StorageErrorKind::Conflict,
                "Unique constraint not met",
                None,
            ),
            DieselError::DatabaseError(DatabaseErrorKind::ForeignKeyViolation, _) => {
                Self::not_found("Attempt to associate to a non-existent entity")
            }
            DieselError::DatabaseError(DatabaseErrorKind::CheckViolation, ref info) => {
                if info.constraint_name() == Some(OBJECT_RELATION_CARDINALITY_CONSTRAINT) {
                    return Self::new(StorageErrorKind::Conflict, info.message(), None);
                }
                Self::new(
                    StorageErrorKind::BadRequest,
                    "Check constraint not met",
                    None,
                )
            }
            DieselError::DatabaseError(DatabaseErrorKind::Unknown, ref info) => {
                let message = info.message();
                if message == "hubuum_stale_resource" {
                    return Self::precondition_failed(
                        "The resource changed since the supplied validator was issued",
                        None,
                    );
                }
                if message.starts_with("Invalid object relation:") {
                    return Self::new(StorageErrorKind::BadRequest, message, None);
                }
                error!(
                    message = "PostgreSQL query failed",
                    backend = "postgresql",
                    error = ?error,
                );
                Self::database(error.to_string())
            }
            _ => {
                error!(
                    message = "PostgreSQL query failed",
                    backend = "postgresql",
                    error = ?error,
                );
                Self::database(error.to_string())
            }
        }
    }
}

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

        let (kind, message, current_etag) = error.into_parts();
        assert_eq!(kind, StorageErrorKind::PreconditionFailed);
        assert_eq!(message, "stale resource");
        assert_eq!(current_etag.as_deref(), Some("etag"));
    }

    #[test]
    fn diesel_errors_are_classified_before_crossing_the_boundary() {
        let error = PostgresStorageError::from(DieselError::NotFound);

        assert_eq!(error.kind(), StorageErrorKind::NotFound);
    }
}
