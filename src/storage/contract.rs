use std::sync::Arc;

use super::{
    ClassRelationStore, ClassStore, CollectionStore, ObjectRelationStore, ObjectStore,
    PostgresStorage, observed::ObservedLifecycleStorage,
};

/// Version of the complete application storage contract.
///
/// Increment this when a selectable backend must implement a new capability
/// family or when an existing family's externally observable semantics change.
pub(crate) const STORAGE_CONTRACT_VERSION: u16 = 1;

/// Stable identity of a selectable storage backend.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum StorageBackendKind {
    Postgresql,
}

impl StorageBackendKind {
    #[cfg(test)]
    pub(crate) const ALL: [Self; 1] = [Self::Postgresql];

    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Postgresql => "postgresql",
        }
    }
}

/// Stable, bounded capability families required of every selectable backend.
///
/// This is deliberately not a feature bitmap. A backend either satisfies the
/// complete [`StorageBackend`] trait or cannot be selected by `AppContext`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum StorageCapability {
    DomainLifecycle,
    IdentityAndAuthorizationData,
    QueriesAndHistory,
    Workflows,
    Operations,
}

impl StorageCapability {
    pub(crate) const ALL: [Self; 5] = [
        Self::DomainLifecycle,
        Self::IdentityAndAuthorizationData,
        Self::QueriesAndHistory,
        Self::Workflows,
        Self::Operations,
    ];

    pub(crate) const fn as_str(self) -> &'static str {
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
pub(crate) struct StorageBackendDescriptor {
    kind: StorageBackendKind,
}

impl StorageBackendDescriptor {
    pub(crate) const fn new(kind: StorageBackendKind) -> Self {
        Self { kind }
    }

    pub(crate) const fn kind(self) -> StorageBackendKind {
        self.kind
    }

    pub(crate) const fn contract_version(self) -> u16 {
        STORAGE_CONTRACT_VERSION
    }

    pub(crate) fn capabilities(self) -> impl Iterator<Item = StorageCapability> {
        StorageCapability::ALL.into_iter()
    }
}

/// Identifies a lifecycle implementation for diagnostics and contract tests.
///
/// Focused models may implement this and [`LifecycleStorage`] without becoming
/// selectable application backends.
pub(crate) trait StorageIdentity: Send + Sync {
    fn storage_name(&self) -> &'static str;
}

/// Complete lifecycle contract shared by services and focused test models.
pub(crate) trait LifecycleStorage:
    StorageIdentity
    + CollectionStore
    + ClassStore
    + ObjectStore
    + ClassRelationStore
    + ObjectRelationStore
{
}

impl<T> LifecycleStorage for T where
    T: StorageIdentity
        + CollectionStore
        + ClassStore
        + ObjectStore
        + ClassRelationStore
        + ObjectRelationStore
{
}

/// Certification gates for capability families that are still implemented by
/// operation-shaped adapters outside `src/storage`.
///
/// These traits are intentionally sealed and implemented in this central
/// contract module. They make the composition checklist compile-time visible:
/// adding a selectable backend requires an explicit implementation for every
/// family here, followed by the shared compatibility suite.
pub(crate) trait IdentityAndAuthorizationStorage: Send + Sync {}
pub(crate) trait QueryAndHistoryStorage: Send + Sync {}
pub(crate) trait WorkflowStorage: Send + Sync {}
pub(crate) trait OperationalStorage: Send + Sync {}

mod sealed {
    pub trait CertifiedStorageBackend {}
}

/// All-or-nothing storage backend accepted by the application composition root.
///
/// Partial lifecycle models cannot implement the private certification trait
/// and therefore cannot be selected by `AppContext`.
pub(crate) trait StorageBackend:
    LifecycleStorage
    + IdentityAndAuthorizationStorage
    + QueryAndHistoryStorage
    + WorkflowStorage
    + OperationalStorage
    + sealed::CertifiedStorageBackend
{
    fn descriptor(&self) -> StorageBackendDescriptor;
}

impl IdentityAndAuthorizationStorage for PostgresStorage {}
impl QueryAndHistoryStorage for PostgresStorage {}
impl WorkflowStorage for PostgresStorage {}
impl OperationalStorage for PostgresStorage {}
impl sealed::CertifiedStorageBackend for PostgresStorage {}

impl StorageBackend for PostgresStorage {
    fn descriptor(&self) -> StorageBackendDescriptor {
        StorageBackendDescriptor::new(StorageBackendKind::Postgresql)
    }
}

/// Type-erased lifecycle capability used by application services.
#[derive(Clone)]
pub(crate) struct DynLifecycleStorage {
    inner: Arc<dyn LifecycleStorage>,
}

impl DynLifecycleStorage {
    pub(crate) fn from_backend(storage: impl StorageBackend + 'static) -> Self {
        Self::new(storage)
    }

    /// Construct a focused lifecycle contract harness.
    ///
    /// Production composition must use [`Self::from_backend`], whose stronger
    /// bound rejects partial implementations.
    pub(crate) fn new(storage: impl LifecycleStorage + 'static) -> Self {
        let inner: Arc<dyn LifecycleStorage> = Arc::new(storage);
        Self {
            inner: Arc::new(ObservedLifecycleStorage::new(inner)),
        }
    }

    pub(crate) fn inner(&self) -> &dyn LifecycleStorage {
        self.inner.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_complete_backend<T: StorageBackend>() {}

    #[test]
    fn postgres_satisfies_the_complete_storage_backend_contract() {
        assert_complete_backend::<PostgresStorage>();
    }

    #[test]
    fn required_capabilities_are_stable_and_complete() {
        assert_eq!(
            StorageCapability::ALL.map(StorageCapability::as_str),
            [
                "domain_lifecycle",
                "identity_and_authorization_data",
                "queries_and_history",
                "workflows",
                "operations",
            ]
        );
    }
}
