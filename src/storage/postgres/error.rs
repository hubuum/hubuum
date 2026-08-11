use std::fmt;

use diesel::result::{DatabaseErrorKind, Error as DieselError};
use diesel_async::pooled_connection::bb8::RunError as PoolError;
use tracing::{debug, error};

use crate::errors::ApiError;
use crate::observability::metrics;

use super::super::{StorageError, StorageErrorKind};

const OBJECT_RELATION_CARDINALITY_CONSTRAINT: &str = "hubuumobject_relation_cardinality";

impl From<PoolError> for ApiError {
    fn from(error: PoolError) -> Self {
        error!(message = "Unable to get a PostgreSQL connection from the pool", error = ?error);
        Self::DbConnectionError(error.to_string())
    }
}

impl From<DieselError> for ApiError {
    fn from(error: DieselError) -> Self {
        match error {
            DieselError::NotFound => {
                let message = "Entity not found".to_string();
                debug!(message, error = ?error);
                Self::NotFound(message)
            }
            DieselError::DatabaseError(DatabaseErrorKind::UniqueViolation, _) => {
                let message = "Unique constraint not met".to_string();
                debug!(message, error = ?error);
                Self::Conflict(message)
            }
            DieselError::DatabaseError(DatabaseErrorKind::ForeignKeyViolation, _) => {
                let message = "Attempt to associate to a non-existent entity".to_string();
                debug!(message, error = ?error);
                Self::NotFound(message)
            }
            DieselError::DatabaseError(DatabaseErrorKind::CheckViolation, ref info) => {
                if info.constraint_name() == Some(OBJECT_RELATION_CARDINALITY_CONSTRAINT) {
                    let message = info.message().to_string();
                    debug!(message, error = ?error);
                    return Self::Conflict(message);
                }
                let message = "Check constraint not met".to_string();
                debug!(message, error = ?error);
                Self::BadRequest(message)
            }
            DieselError::DatabaseError(DatabaseErrorKind::Unknown, ref info) => {
                let message = info.message();
                if message == "hubuum_stale_resource" {
                    debug!(message = "Conditional mutation rejected as stale");
                    return Self::PreconditionFailed(
                        "The resource changed since the supplied validator was issued".to_string(),
                        None,
                    );
                }
                if message.contains("resource revision")
                    || message.contains("revision advancement")
                    || message.contains("caller-supplied resource revision")
                {
                    metrics::revision_condition("invariant_failure");
                }
                if message.starts_with("Invalid object relation:") {
                    debug!(message, error = ?error);
                    return Self::BadRequest(message.to_string());
                }
                error!(message = "PostgreSQL query failed", error = ?error);
                Self::DatabaseError(error.to_string())
            }
            _ => {
                error!(message = "PostgreSQL query failed", error = ?error);
                Self::DatabaseError(error.to_string())
            }
        }
    }
}

/// PostgreSQL adapter failure before it crosses the neutral storage boundary.
///
/// Legacy PostgreSQL query helpers still return `ApiError` internally. They are
/// classified immediately into this adapter-owned representation so neither
/// the application error nor a Diesel error can cross the storage contract.
#[derive(Debug)]
pub(super) struct PostgresStorageError {
    kind: StorageErrorKind,
    message: String,
    current_etag: Option<String>,
}

impl From<ApiError> for PostgresStorageError {
    fn from(error: ApiError) -> Self {
        let (kind, message, current_etag) = match error {
            ApiError::PermissionBackendUnavailable(message) => {
                (StorageErrorKind::AuthorizationUnavailable, message, None)
            }
            ApiError::BadRequest(message)
            | ApiError::InvalidIntegerRange(message)
            | ApiError::OperatorMismatch(message) => (StorageErrorKind::BadRequest, message, None),
            ApiError::NotAcceptable(message) => (StorageErrorKind::NotAcceptable, message, None),
            ApiError::ValidationError(message) => (StorageErrorKind::Validation, message, None),
            ApiError::PayloadTooLarge(message) => {
                (StorageErrorKind::PayloadTooLarge, message, None)
            }
            ApiError::Conflict(message) => (StorageErrorKind::Conflict, message, None),
            ApiError::Forbidden(message) => (StorageErrorKind::Forbidden, message, None),
            ApiError::DatabaseError(message) | ApiError::DbConnectionError(message) => {
                (StorageErrorKind::Database, message, None)
            }
            ApiError::NotFound(message) | ApiError::Gone(message) => {
                (StorageErrorKind::NotFound, message, None)
            }
            ApiError::PreconditionFailed(message, current_etag) => {
                (StorageErrorKind::PreconditionFailed, message, current_etag)
            }
            ApiError::TooManyRequests(message) => {
                (StorageErrorKind::TooManyRequests, message, None)
            }
            ApiError::ServiceUnavailable(message) => (StorageErrorKind::Unavailable, message, None),
            ApiError::Unauthorized(message) => (StorageErrorKind::Unauthorized, message, None),
            error => (
                StorageErrorKind::Internal,
                format!(
                    "unexpected PostgreSQL storage adapter error ({}): {error}",
                    error.class()
                ),
                None,
            ),
        };
        Self {
            kind,
            message,
            current_etag,
        }
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
        assert_eq!(
            map_postgres_error(ApiError::TooManyRequests("capacity reached".to_string())).kind(),
            StorageErrorKind::TooManyRequests
        );
        assert_eq!(
            map_postgres_error(ApiError::Forbidden("access denied".to_string())).kind(),
            StorageErrorKind::Forbidden
        );
        assert_eq!(
            map_postgres_error(ApiError::Unauthorized("login required".to_string())).kind(),
            StorageErrorKind::Unauthorized
        );
    }

    #[test]
    fn diesel_not_found_is_translated_inside_the_postgres_adapter() {
        assert!(matches!(
            ApiError::from(DieselError::NotFound),
            ApiError::NotFound(_)
        ));
    }

    #[tokio::test]
    async fn pool_failures_are_translated_inside_the_postgres_adapter() {
        let pool = super::super::init_postgres_pool("postgres://invalid:5432/nonexistent", 1);
        let result =
            super::super::with_connection(&pool, async |_conn| Ok::<(), ApiError>(())).await;

        assert!(matches!(result, Err(ApiError::DbConnectionError(_))));
    }
}
