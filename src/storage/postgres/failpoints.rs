//! Transitional application error conversion for adapter rollback failpoints.

pub(crate) use hubuum_storage_postgres::PostgresFailpoint;
#[cfg(test)]
pub(crate) use hubuum_storage_postgres::with_failpoint;

pub(super) fn check(point: PostgresFailpoint) -> Result<(), crate::errors::ApiError> {
    hubuum_storage_postgres::check_failpoint(point)
        .map_err(crate::storage::StorageError::from)
        .map_err(crate::errors::ApiError::from)
}
