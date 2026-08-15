use crate::errors::ApiError;
use crate::models::HubuumObject;
use crate::storage::postgres::PostgresConnection;

pub fn source_data_sha256(data: &serde_json::Value) -> Result<String, ApiError> {
    hubuum_storage_postgres::operations::computed_materialization::source_data_sha256(data)
        .map_err(crate::storage::StorageError::from)
        .map_err(ApiError::from)
}

pub(crate) async fn materialize_object_in_transaction(
    connection: &mut PostgresConnection,
    object: &HubuumObject,
) -> Result<(), ApiError> {
    let summary = hubuum_storage_postgres::operations::computed_materialization::materialize_object_on_connection(
        connection,
        object.id,
        object.hubuum_class_id,
        &object.data,
    )
    .await
    .map_err(crate::storage::StorageError::from)
    .map_err(ApiError::from)?;
    if let Some(error_codes) = summary {
        crate::observability::metrics::computed_evaluation_summary("shared", &error_codes);
    }
    Ok(())
}
