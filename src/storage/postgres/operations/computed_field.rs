//! Transitional test helper for PostgreSQL computed materialization.

use crate::errors::ApiError;

pub fn source_data_sha256(data: &serde_json::Value) -> Result<String, ApiError> {
    hubuum_storage_postgres::operations::computed_materialization::source_data_sha256(data)
        .map_err(crate::storage::StorageError::from)
        .map_err(ApiError::from)
}
