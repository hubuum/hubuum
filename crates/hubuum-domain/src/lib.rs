//! Backend-neutral domain values shared by Hubuum's application and storage
//! adapters.
//!
//! Types in this crate own validation and invariants without depending on
//! Actix, Diesel, application configuration, or transport-facing errors.

mod event_policy;
mod identifier;
mod maintenance;
mod revision;
mod token;

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
pub use maintenance::{MaintenanceState, MaintenanceStateParseError};
pub use revision::{ResourceRevision, ResourceRevisionError};
pub use token::{
    MIN_TOKEN_RETENTION_PURGE_BATCH_SIZE, TokenIssuancePolicy, TokenLifetime, TokenPolicyError,
    TokenRetentionBatchSize, TokenRetentionCutoffs, TokenRetentionPeriod, TokenRetentionSettings,
    TokenRetentionSettingsBuilder,
};
