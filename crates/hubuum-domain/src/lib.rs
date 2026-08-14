//! Backend-neutral domain values shared by Hubuum's application and storage
//! adapters.
//!
//! Types in this crate own validation and invariants without depending on
//! Actix, Diesel, application configuration, or transport-facing errors.

mod event_delivery;
mod event_policy;
mod identifier;
mod identity;
mod json_patch;
mod json_schema;
mod json_value;
mod maintenance;
mod revision;
mod token;

pub use event_delivery::{EventDeliveryStatus, EventDeliveryStatusParseError};
pub use event_policy::{
    EventDeliverySettings, EventDeliverySettingsBuilder, EventFanoutSettings, EventPolicyError,
    EventRetentionSettings,
};
pub use identifier::{
    ClassId, ClassRelationId, CollectionId, ComputedFieldDefinitionId, EventDeliveryId,
    EventSinkId, EventSubscriptionId, ExportTemplateId, GroupId, ObjectId, ObjectRelationId,
    PositiveIdError, PrincipalId, RemoteTargetId, RestoreJobId, ServiceAccountId, TaskId, TokenId,
    UserId,
};
pub use identity::{
    EXTERNAL_MEMBERSHIP_SOURCE, LDAP_PROVIDER_KIND, LOCAL_IDENTITY_SCOPE, LOCAL_PROVIDER_KIND,
    MANUAL_MEMBERSHIP_SOURCE,
};
pub use json_patch::{
    BoundedJsonPatch, JsonPatchError, JsonPatchErrorKind, MAX_JSON_PATCH_BYTES,
    MAX_JSON_PATCH_OPERATIONS, MAX_JSON_PATCH_POINTER_DEPTH, MAX_JSON_PATCH_RESULT_NESTING_DEPTH,
    MAX_JSON_PATCH_WORK_BYTES,
};
pub use json_schema::{
    JsonSchemaError, JsonSchemaErrorKind, validate_json_schema, validate_json_schema_for_instances,
    validate_json_value,
};
pub use json_value::{
    MAX_STORAGE_JSON_NESTING_DEPTH, StorageJsonValidationError, validate_storage_json_value,
};
pub use maintenance::{MaintenanceState, MaintenanceStateParseError};
pub use revision::{ResourceRevision, ResourceRevisionError};
pub use token::{
    MIN_TOKEN_RETENTION_PURGE_BATCH_SIZE, TokenIssuancePolicy, TokenLifetime, TokenPolicyError,
    TokenRetentionBatchSize, TokenRetentionCutoffs, TokenRetentionPeriod, TokenRetentionSettings,
    TokenRetentionSettingsBuilder,
};
