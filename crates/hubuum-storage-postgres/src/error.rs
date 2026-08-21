use std::fmt;

use diesel::result::{DatabaseErrorKind, Error as DieselError};
use diesel_async::pooled_connection::bb8::RunError as PoolError;
use hubuum_domain::{JsonSchemaError, JsonSchemaErrorKind, PositiveIdError, ResourceRevision};
use hubuum_events_core::EventIdentifierError;
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
    current_revision: Option<ResourceRevision>,
}

impl PostgresStorageError {
    pub(crate) fn precondition_failed(
        message: impl Into<String>,
        current_revision: Option<ResourceRevision>,
    ) -> Self {
        Self::new(
            StorageErrorKind::PreconditionFailed,
            message,
            current_revision,
        )
    }

    fn new(
        kind: StorageErrorKind,
        message: impl Into<String>,
        current_revision: Option<ResourceRevision>,
    ) -> Self {
        Self {
            kind,
            message: message.into(),
            current_revision,
        }
    }

    #[must_use]
    pub const fn kind(&self) -> StorageErrorKind {
        self.kind
    }

    #[must_use]
    pub fn database(message: impl Into<String>) -> Self {
        Self::new(StorageErrorKind::Backend, message, None)
    }

    #[must_use]
    pub fn authorization_unavailable(message: impl Into<String>) -> Self {
        Self::new(StorageErrorKind::AuthorizationUnavailable, message, None)
    }

    #[must_use]
    pub fn invalid_input(message: impl Into<String>) -> Self {
        Self::new(StorageErrorKind::InvalidInput, message, None)
    }

    #[must_use]
    pub fn conflict(message: impl Into<String>) -> Self {
        Self::new(StorageErrorKind::Conflict, message, None)
    }

    #[must_use]
    pub fn permission_denied(message: impl Into<String>) -> Self {
        Self::new(StorageErrorKind::PermissionDenied, message, None)
    }

    #[must_use]
    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(StorageErrorKind::Internal, message, None)
    }

    #[must_use]
    pub fn input_too_large(message: impl Into<String>) -> Self {
        Self::new(StorageErrorKind::InputTooLarge, message, None)
    }

    #[must_use]
    pub fn rate_limited(message: impl Into<String>) -> Self {
        Self::new(StorageErrorKind::RateLimited, message, None)
    }

    #[must_use]
    pub fn validation_failed(message: impl Into<String>) -> Self {
        Self::new(StorageErrorKind::ValidationFailed, message, None)
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
    pub fn unsupported_operation(message: impl Into<String>) -> Self {
        Self::new(StorageErrorKind::UnsupportedOperation, message, None)
    }

    #[must_use]
    pub fn authentication_required(message: impl Into<String>) -> Self {
        Self::new(StorageErrorKind::AuthenticationRequired, message, None)
    }

    #[must_use]
    pub fn revision_conflict(
        message: impl Into<String>,
        current_revision: ResourceRevision,
    ) -> Self {
        Self::new(
            StorageErrorKind::RevisionConflict,
            message,
            Some(current_revision),
        )
    }
}

impl From<JsonSchemaError> for PostgresStorageError {
    fn from(error: JsonSchemaError) -> Self {
        let (kind, message) = error.into_parts();
        match kind {
            JsonSchemaErrorKind::InvalidSchema => Self::invalid_input(message),
            JsonSchemaErrorKind::InvalidValue => Self::validation_failed(message),
        }
    }
}

impl From<PositiveIdError> for PostgresStorageError {
    fn from(error: PositiveIdError) -> Self {
        Self::database(format!("Invalid persisted identifier: {error}"))
    }
}

impl From<EventIdentifierError> for PostgresStorageError {
    fn from(error: EventIdentifierError) -> Self {
        Self::database(format!("Invalid persisted event identifier: {error}"))
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
        let (kind, message, current_revision) = error.into_parts();
        match kind {
            StorageErrorKind::AuthorizationUnavailable => Self::authorization_unavailable(message),
            StorageErrorKind::InvalidInput => Self::invalid_input(message),
            StorageErrorKind::Conflict => Self::conflict(message),
            StorageErrorKind::Backend => Self::database(message),
            StorageErrorKind::PermissionDenied => Self::permission_denied(message),
            StorageErrorKind::Internal => Self::internal(message),
            StorageErrorKind::NotFound => Self::not_found(message),
            StorageErrorKind::UnsupportedOperation => Self::unsupported_operation(message),
            StorageErrorKind::InputTooLarge => Self::input_too_large(message),
            StorageErrorKind::RevisionConflict => match current_revision {
                Some(revision) => Self::revision_conflict(message, revision),
                None => Self::internal(
                    "Storage revision conflict crossed the adapter boundary without a revision",
                ),
            },
            StorageErrorKind::PreconditionFailed => {
                Self::precondition_failed(message, current_revision)
            }
            StorageErrorKind::RateLimited => Self::rate_limited(message),
            StorageErrorKind::Unavailable => Self::unavailable(message),
            StorageErrorKind::AuthenticationRequired => Self::authentication_required(message),
            StorageErrorKind::ValidationFailed => Self::validation_failed(message),
        }
    }
}

impl From<PoolError> for PostgresStorageError {
    fn from(error: PoolError) -> Self {
        error!(
            message = "Unable to get a PostgreSQL connection from the pool",
            backend = "postgresql",
            error = ?error,
        );
        Self::database("PostgreSQL connection pool unavailable")
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
            DieselError::DatabaseError(DatabaseErrorKind::UniqueViolation, _) => {
                Self::conflict("Unique constraint not met")
            }
            DieselError::DatabaseError(DatabaseErrorKind::ForeignKeyViolation, _) => {
                Self::not_found("Attempt to associate to a non-existent entity")
            }
            DieselError::DatabaseError(DatabaseErrorKind::CheckViolation, ref info) => {
                if info.constraint_name() == Some(OBJECT_RELATION_CARDINALITY_CONSTRAINT) {
                    return Self::conflict(info.message());
                }
                Self::invalid_input("Check constraint not met")
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
                    return Self::invalid_input(message);
                }
                error!(
                    message = "PostgreSQL query failed",
                    backend = "postgresql",
                    error = ?error,
                );
                Self::database("PostgreSQL query failed")
            }
            _ => {
                error!(
                    message = "PostgreSQL query failed",
                    backend = "postgresql",
                    error = ?error,
                );
                Self::database("PostgreSQL query failed")
            }
        }
    }
}

impl From<PostgresStorageError> for StorageError {
    fn from(error: PostgresStorageError) -> Self {
        match error.kind {
            StorageErrorKind::AuthorizationUnavailable => {
                Self::authorization_unavailable(error.message)
            }
            StorageErrorKind::InvalidInput => Self::invalid_input(error.message),
            StorageErrorKind::Conflict => Self::conflict(error.message),
            StorageErrorKind::Backend => Self::backend_failure(error.message),
            StorageErrorKind::PermissionDenied => Self::permission_denied(error.message),
            StorageErrorKind::Internal => Self::internal(error.message),
            StorageErrorKind::NotFound => Self::not_found(error.message),
            StorageErrorKind::UnsupportedOperation => Self::unsupported_operation(error.message),
            StorageErrorKind::InputTooLarge => Self::input_too_large(error.message),
            StorageErrorKind::RevisionConflict => match error.current_revision {
                Some(revision) => Self::revision_conflict(error.message, revision),
                None => Self::internal(
                    "PostgreSQL revision conflict crossed the storage boundary without a revision",
                ),
            },
            StorageErrorKind::PreconditionFailed => {
                Self::precondition_failed(error.message, error.current_revision)
            }
            StorageErrorKind::RateLimited => Self::rate_limited(error.message),
            StorageErrorKind::Unavailable => Self::unavailable(error.message),
            StorageErrorKind::AuthenticationRequired => {
                Self::authentication_required(error.message)
            }
            StorageErrorKind::ValidationFailed => Self::validation_failed(error.message),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn postgres_errors_cross_the_boundary_as_storage_errors() {
        let error = StorageError::from(PostgresStorageError::revision_conflict(
            "stale resource",
            ResourceRevision::new(2).unwrap(),
        ));

        let (kind, message, current_revision) = error.into_parts();
        assert_eq!(kind, StorageErrorKind::RevisionConflict);
        assert_eq!(message, "stale resource");
        assert_eq!(current_revision, Some(ResourceRevision::new(2).unwrap()));
    }

    #[test]
    fn diesel_errors_are_classified_before_crossing_the_boundary() {
        let error = PostgresStorageError::from(DieselError::NotFound);

        assert_eq!(error.kind(), StorageErrorKind::NotFound);
    }

    #[test]
    fn native_database_details_do_not_cross_the_storage_boundary() {
        let native_detail = "secret native database detail";
        let error = DieselError::DatabaseError(
            DatabaseErrorKind::Unknown,
            Box::new(TestDatabaseErrorInformation(native_detail.to_string())),
        );

        let portable = StorageError::from(PostgresStorageError::from(error));

        assert_eq!(portable.kind(), StorageErrorKind::Backend);
        assert_eq!(portable.message(), "PostgreSQL query failed");
        assert!(!portable.message().contains(native_detail));
    }

    #[derive(Debug)]
    struct TestDatabaseErrorInformation(String);

    impl diesel::result::DatabaseErrorInformation for TestDatabaseErrorInformation {
        fn message(&self) -> &str {
            &self.0
        }

        fn details(&self) -> Option<&str> {
            None
        }

        fn hint(&self) -> Option<&str> {
            None
        }

        fn table_name(&self) -> Option<&str> {
            None
        }

        fn column_name(&self) -> Option<&str> {
            None
        }

        fn constraint_name(&self) -> Option<&str> {
            None
        }

        fn statement_position(&self) -> Option<i32> {
            None
        }
    }
}
