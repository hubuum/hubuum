//! Transport adapters for the backend-neutral bounded JSON Patch contract.

use json_patch::PatchOperation;
use serde_json::Value;
use utoipa::openapi::{RefOr, schema::Schema};
use utoipa::{PartialSchema, ToSchema};

use crate::errors::ApiError;

pub(crate) use hubuum_domain::{
    BoundedJsonPatch, MAX_JSON_PATCH_BYTES, MAX_JSON_PATCH_OPERATIONS,
    MAX_JSON_PATCH_POINTER_DEPTH, MAX_JSON_PATCH_RESULT_NESTING_DEPTH, MAX_JSON_PATCH_WORK_BYTES,
};

pub(crate) fn apply_bounded_json_patch(
    patch: &BoundedJsonPatch,
    document: &Value,
) -> Result<Value, ApiError> {
    patch.apply(document).map_err(|error| {
        let (kind, message) = error.into_parts();
        match kind {
            hubuum_domain::JsonPatchErrorKind::BadRequest => ApiError::BadRequest(message),
            hubuum_domain::JsonPatchErrorKind::Conflict => ApiError::Conflict(message),
            hubuum_domain::JsonPatchErrorKind::PayloadTooLarge => {
                ApiError::PayloadTooLarge(message)
            }
        }
    })
}

pub(crate) fn bounded_json_patch_openapi_schema(
    description: &str,
    example: Value,
) -> RefOr<Schema> {
    utoipa::openapi::schema::ArrayBuilder::new()
        .items(PatchOperation::schema())
        .max_items(Some(MAX_JSON_PATCH_OPERATIONS))
        .description(Some(description))
        .examples([example])
        .build()
        .into()
}

pub(crate) fn register_bounded_json_patch_openapi_schemas(
    schemas: &mut Vec<(String, RefOr<Schema>)>,
) {
    schemas.push((
        PatchOperation::name().into_owned(),
        PatchOperation::schema(),
    ));
    PatchOperation::schemas(schemas);
}
