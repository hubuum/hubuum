use diesel::result::{DatabaseErrorKind, Error as DieselError};
use diesel_async::pooled_connection::bb8::RunError as PoolError;
use tracing::{debug, error};

use crate::errors::ApiError;
use crate::observability::metrics;

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

#[cfg(test)]
mod tests {
    use super::*;

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
