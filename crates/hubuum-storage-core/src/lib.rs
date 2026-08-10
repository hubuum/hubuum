//! Backend-neutral storage metadata and errors.
//!
//! This crate deliberately has no application, transport, database-driver, or
//! asynchronous-runtime dependencies. Application services and adapters share
//! these values without reversing the dependency from storage into the server.

mod events;
mod identity;
mod operational;

pub use events::{
    EventArchive, EventDeliveryBatch, EventDeliveryClaim, EventDeliverySink, EventDeliveryStorage,
    EventDeliverySubscription, EventDeliveryWorkItem, EventFanoutStorage, EventRetentionStorage,
    EventRetentionSummary, RetainedEvent,
};
pub use identity::{
    AuthenticationHuman, AuthenticationIdentity, AuthenticationPrincipal,
    AuthenticationPrincipalKind, AuthenticationResourceScope, AuthenticationStorage,
    AuthenticationTokenScope, AuthenticationTokenScopeQuery,
};
pub use operational::{
    EventDeliveryHealthSnapshot, EventDeliveryStatusSnapshot, EventFanoutSnapshot,
    EventHealthStorage, EventQueueSnapshot, EventSinkHealthSnapshot, EventSinkSnapshot,
    EventSubscriptionHealthSnapshot, OperationalStateStorage, ReadinessSnapshot,
    TokenRetentionStorage,
};

use std::fmt;

/// Version of the complete application storage contract.
///
/// Increment this when a selectable backend must implement a new capability
/// family or when an existing family's externally observable semantics change.
pub const STORAGE_CONTRACT_VERSION: u16 = 1;

/// Stable identity of a selectable storage backend.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StorageBackendKind {
    Postgresql,
}

impl StorageBackendKind {
    /// Every backend kind that can be selected by application composition.
    pub const ALL: [Self; 1] = [Self::Postgresql];

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Postgresql => "postgresql",
        }
    }
}

/// Stable, bounded capability families required of every selectable backend.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StorageCapability {
    DomainLifecycle,
    IdentityAndAuthorizationData,
    QueriesAndHistory,
    Workflows,
    Operations,
}

impl StorageCapability {
    pub const ALL: [Self; 5] = [
        Self::DomainLifecycle,
        Self::IdentityAndAuthorizationData,
        Self::QueriesAndHistory,
        Self::Workflows,
        Self::Operations,
    ];

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::DomainLifecycle => "domain_lifecycle",
            Self::IdentityAndAuthorizationData => "identity_and_authorization_data",
            Self::QueriesAndHistory => "queries_and_history",
            Self::Workflows => "workflows",
            Self::Operations => "operations",
        }
    }
}

/// Non-secret metadata for the backend selected at application composition.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StorageBackendDescriptor {
    kind: StorageBackendKind,
}

impl StorageBackendDescriptor {
    #[must_use]
    pub const fn new(kind: StorageBackendKind) -> Self {
        Self { kind }
    }

    #[must_use]
    pub const fn kind(self) -> StorageBackendKind {
        self.kind
    }

    #[must_use]
    pub const fn contract_version(self) -> u16 {
        STORAGE_CONTRACT_VERSION
    }

    pub fn capabilities(self) -> impl Iterator<Item = StorageCapability> {
        StorageCapability::ALL.into_iter()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StorageErrorKind {
    BadRequest,
    Conflict,
    Database,
    Internal,
    NotFound,
    NotAcceptable,
    PayloadTooLarge,
    PreconditionFailed,
    Unavailable,
    Validation,
}

impl StorageErrorKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::BadRequest => "bad_request",
            Self::Conflict => "conflict",
            Self::Database => "database",
            Self::Internal => "internal",
            Self::NotFound => "not_found",
            Self::NotAcceptable => "not_acceptable",
            Self::PayloadTooLarge => "payload_too_large",
            Self::PreconditionFailed => "precondition_failed",
            Self::Unavailable => "unavailable",
            Self::Validation => "validation",
        }
    }

    #[must_use]
    pub const fn is_backend_failure(self) -> bool {
        matches!(self, Self::Database | Self::Internal | Self::Unavailable)
    }
}

/// Backend-neutral failure returned by storage capabilities.
///
/// The representation deliberately carries no Diesel, Actix, or application
/// error types. The application error layer owns transport-facing translation.
#[derive(Debug, PartialEq, Eq)]
pub struct StorageError {
    kind: StorageErrorKind,
    message: String,
    current_etag: Option<String>,
}

impl StorageError {
    #[must_use]
    pub fn bad_request(message: impl Into<String>) -> Self {
        Self::new(StorageErrorKind::BadRequest, message, None)
    }

    #[must_use]
    pub fn conflict(message: impl Into<String>) -> Self {
        Self::new(StorageErrorKind::Conflict, message, None)
    }

    #[must_use]
    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(StorageErrorKind::Internal, message, None)
    }

    #[must_use]
    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(StorageErrorKind::NotFound, message, None)
    }

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
    pub fn into_parts(self) -> (StorageErrorKind, String, Option<String>) {
        (self.kind, self.message, self.current_etag)
    }

    #[must_use]
    pub const fn kind(&self) -> StorageErrorKind {
        self.kind
    }
}

impl fmt::Display for StorageError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for StorageError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn descriptor_reports_the_complete_contract() {
        let descriptor = StorageBackendDescriptor::new(StorageBackendKind::Postgresql);

        assert_eq!(descriptor.contract_version(), STORAGE_CONTRACT_VERSION);
        assert_eq!(
            descriptor
                .capabilities()
                .map(StorageCapability::as_str)
                .collect::<Vec<_>>(),
            [
                "domain_lifecycle",
                "identity_and_authorization_data",
                "queries_and_history",
                "workflows",
                "operations",
            ]
        );
    }

    #[test]
    fn storage_errors_keep_classification_and_precondition_metadata() {
        let error = StorageError::new(
            StorageErrorKind::PreconditionFailed,
            "stale resource",
            Some("\"revision-2\"".to_string()),
        );

        assert_eq!(error.kind(), StorageErrorKind::PreconditionFailed);
        assert_eq!(
            error.into_parts(),
            (
                StorageErrorKind::PreconditionFailed,
                "stale resource".to_string(),
                Some("\"revision-2\"".to_string()),
            )
        );
    }
}
