use actix_web::web::Data;
use chrono::{DateTime, Utc};
use hubuum_domain::{
    ClassId, CollectionId, ComputedFieldDefinitionId, EventDeliveryId, EventSinkId,
    EventSubscriptionId, ExportTemplateId, GroupId, IdentityScopeId, ObjectId, PrincipalId,
    RemoteTargetId, RestoreJobId, ServiceAccountId, TaskId, TokenId, UserId,
};
#[cfg(any(test, feature = "integration-test-support", feature = "postgres-bench"))]
use hubuum_storage_postgres::PostgresPool;
use hubuum_storage_postgres::PostgresStorage;
#[cfg(any(test, feature = "integration-test-support"))]
use hubuum_storage_postgres::{PostgresPoolSettings, build_postgres_pool};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tracing::debug;
use uuid::Uuid;

use crate::events::{
    EventContext, EventDeliverySettings, EventFanoutSettings, EventRetentionSettings,
};
use crate::models::search::QueryOptions;
use crate::models::{MaintenanceState, TokenRetentionSettings};
use crate::permissions::AppContext;
use crate::storage::observed::{
    ApplicationStorageObserver, ObservedStorage, observe_storage_call_with,
};
use crate::storage::{
    AuditEventStorage, AuthenticatedToken, AuthenticationAttempt, AuthenticationIdentity,
    AuthenticationStorage, AuthenticationTokenScope, AuthenticationTokenScopeQuery,
    AuthorizationClassResource, AuthorizationCollection, AuthorizationCollectionAccessQuery,
    AuthorizationCollectionGrantListQuery, AuthorizationCollectionGroupsPageQuery,
    AuthorizationCollectionGroupsQuery, AuthorizationCollectionVisibilityQuery,
    AuthorizationCollectionsAccessQuery, AuthorizationCollectionsQuery, AuthorizationDataStorage,
    AuthorizationEffectiveGroupGrant, AuthorizationGrant, AuthorizationGrantDelete,
    AuthorizationGrantKey, AuthorizationGrantMutation, AuthorizationGroup,
    AuthorizationGroupCollectionQuery, AuthorizationGroupGrant, AuthorizationGroupMembershipQuery,
    AuthorizationObjectResource, AuthorizationPermissionSet, AuthorizationPermissionSetQuery,
    AuthorizationPolicySnapshotRow, AuthorizationPrincipal,
    AuthorizationPrincipalCollectionPageQuery, AuthorizationPrincipalCollectionQuery,
    AuthorizationResourceIds, BackupSnapshotStorage, BidirectionalRelatedObjectsQuery,
    CatalogListQuery, CatalogStorage, CertifiedStorageBackend, ClassRelationStorage, ClassStorage,
    CollectionAuthorizationQueryStorage, CollectionStorage, ComputedFieldStorage,
    ComputedObjectEnrichmentQuery, ComputedObjectListQuery, ComputedObjectPage,
    ComputedObjectStorage, EventConfigurationStorage, EventDeliveryAdministrationStorage,
    EventDeliveryBatch, EventDeliveryClaim, EventDeliveryHealthSnapshot,
    EventDeliveryWorkerStorage, EventFanoutStorage, EventHealthStorage, EventMetricsSnapshot,
    EventRetentionBatch, EventRetentionBatchId, EventRetentionStorage, EventRetentionSummary,
    ExecutionStorage, ExportTemplateHistoryRecord, ExportTemplateStorage, ExternalIdentityStorage,
    GroupMembershipStorage, GroupStorage, HistoryAsOfQuery, HistoryListQuery, HistoryPrincipalName,
    HistoryStorage, IdentityScopeStorage, ImportStorage, InventoryGaugeSnapshot, InventoryStorage,
    LocalIdentityCredentialStorage, MetricsStorage, MutationOutcome, ObjectAggregateAuthorization,
    ObjectAggregateStorage, ObjectAggregateStorageQuery, ObjectHistoryAsOfQuery,
    ObjectHistoryListQuery, ObjectHistoryRecord, ObjectRelationStorage,
    ObjectRelationsTouchingIdsQuery, ObjectStorage, OperationalExportTemplateAuditEntry,
    OperationalExportTemplateHealth, OperationalStateStorage, OperationalTaskQueueSnapshot,
    PrincipalStorage, ReadinessSnapshot, RelatedObjectsForRootsQuery, RelationGraphQuery,
    RelationIdsQuery, RelationListQuery, RelationQueryStorage, RelationTouchingQuery,
    RemoteTargetHistoryRecord, RemoteTargetStorage, RestoreStorage, ServiceAccountStorage,
    StorageAuditEvent, StorageAuditEventListQuery, StorageBackendDescriptor, StorageBackendKind,
    StorageBackupOutput, StorageBackupOutputSummary, StorageBackupSnapshot, StorageCapability,
    StorageClass, StorageClassComputationState, StorageClassGraphRow, StorageClassRecord,
    StorageClassRelation, StorageCollection, StorageComputedFieldDefinition,
    StorageComputedFieldMutation, StorageComputedFieldRebuildRequest, StorageComputedObject,
    StorageDefaultAdminBootstrap, StorageError, StorageEventDelivery,
    StorageEventDeliveryListQuery, StorageEventSink, StorageEventSinkCreate,
    StorageEventSinkDelete, StorageEventSinkListQuery, StorageEventSinkUpdate,
    StorageEventSubscription, StorageEventSubscriptionCreate, StorageEventSubscriptionDelete,
    StorageEventSubscriptionListQuery, StorageEventSubscriptionUpdate, StorageExecutionScope,
    StorageExportOutput, StorageExportOutputSummary, StorageExportTemplate,
    StorageExportTemplateCreate, StorageExportTemplateDelete, StorageExportTemplateListQuery,
    StorageExportTemplateReplace, StorageExternalPrincipalState, StorageExternalUserSync,
    StorageGroupCreate, StorageGroupListQuery, StorageGroupMember, StorageGroupUpdate,
    StorageIdentityGroup, StorageIdentityScope, StorageIdentityScopeEnsure, StorageImportApply,
    StorageImportCollectionKey, StorageImportMode, StorageImportPlan, StorageImportPreflight,
    StorageImportResult, StorageImportTaskResult, StorageInventoryCounts,
    StorageLocalPasswordReset, StorageNotification, StorageNotificationListener,
    StorageNotificationShutdown, StorageObject, StorageObjectAggregatePage, StorageObjectGraphRow,
    StorageObjectRelation, StorageObserver, StoragePage, StoragePersonalComputedFieldCreate,
    StoragePersonalComputedFieldDelete, StoragePersonalComputedFieldListQuery,
    StoragePersonalComputedFieldUpdate, StoragePrincipal, StoragePrincipalGroup,
    StoragePrincipalGroupListQuery, StoragePrincipalSettings, StoragePrincipalSettingsMutation,
    StoragePrincipalTokensRevoke, StorageRelatedObjectForRootRow, StorageRelatedObjectIncludeRow,
    StorageRemoteTarget, StorageRemoteTargetCreate, StorageRemoteTargetDelete,
    StorageRemoteTargetInvocation, StorageRemoteTargetListQuery, StorageRemoteTargetUpdate,
    StorageRestoreApply, StorageRestoreCompletion, StorageRestoreCoordinatorSnapshot,
    StorageRestoreDrainState, StorageRestoreFailure, StorageRestoreJob, StorageRestoreStageCreate,
    StorageRestoreStatus, StorageServiceAccount, StorageServiceAccountCreate,
    StorageServiceAccountDetails, StorageServiceAccountDisableOutcome,
    StorageServiceAccountListItem, StorageServiceAccountListQuery, StorageServiceAccountMutation,
    StorageServiceAccountUpdate, StorageSharedComputedFieldCreate,
    StorageSharedComputedFieldDelete, StorageSharedComputedFieldUpdate, StorageSyncedHuman,
    StorageTask, StorageTaskAccess, StorageTaskClaim, StorageTaskCompletion,
    StorageTaskCreateRequest, StorageTaskEvent, StorageTaskEventAppend, StorageTaskFailure,
    StorageTaskLease, StorageTaskLeaseDuration, StorageTaskListQuery, StorageTaskOutputLookup,
    StorageTaskPageQuery, StorageTaskStateUpdate, StorageTokenCreate, StorageTokenHashRevoke,
    StorageTokenListQuery, StorageTokenMetadata, StorageTokenObservation, StorageTokenRenew,
    StorageTokenRevoke, StorageTransaction, StorageTransactionFuture, StorageUser,
    StorageUserAnonymize, StorageUserCreate, StorageUserDelete, StorageUserDetails,
    StorageUserListItem, StorageUserListQuery, StorageUserPasswordUpdate, StorageUserUpdate,
    TaskExecutionStorage, TaskGaugeSnapshot, TaskQueueStorage, TokenRetentionStorage, TokenStorage,
    TransactionStorage, UnifiedSearchQuery, UnifiedSearchStorage, UserStorage,
    WorkerNotificationProvider,
};
use crate::storage::{ClassHistoryRecord, CollectionHistoryRecord};
use crate::storage::{DatabaseDiagnosticsProvider, DatabasePoolState, DatabaseStorageSnapshot};
use async_trait::async_trait;

mod private {
    use super::StorageHandle;

    pub trait BackendAccess {
        fn storage_handle(&self) -> StorageHandle;
    }
}

/// An opaque handle to Hubuum's configured persistence backend.
///
/// Application code passes this handle to domain operations without selecting
/// a database implementation or handling a connection pool directly.
#[derive(Clone)]
pub struct StorageHandle {
    inner: Arc<StorageHandleInner>,
}

struct StorageHandleInner {
    descriptor: StorageBackendDescriptor,
    implementation: BackendImplementation,
    resource_ports: ResourceStoragePorts,
    observer: Arc<dyn StorageObserver>,
    database_diagnostics: Option<Arc<dyn DatabaseDiagnosticsProvider>>,
    worker_notification_provider: Option<Arc<dyn WorkerNotificationProvider>>,
}

pub(crate) enum BackendImplementation {
    Postgres(PostgresStorage),
}

pub(crate) trait RegisteredStorageBackend:
    CertifiedStorageBackend + Clone + 'static
{
    const KIND: StorageBackendKind;

    fn into_implementation(self) -> BackendImplementation;
}

impl RegisteredStorageBackend for PostgresStorage {
    const KIND: StorageBackendKind = StorageBackendKind::Postgres;

    fn into_implementation(self) -> BackendImplementation {
        BackendImplementation::Postgres(self)
    }
}

struct ResourceStoragePorts {
    collections: Arc<dyn CollectionStorage>,
    classes: Arc<dyn ClassStorage>,
    objects: Arc<dyn ObjectStorage>,
    class_relations: Arc<dyn ClassRelationStorage>,
    object_relations: Arc<dyn ObjectRelationStorage>,
}

impl ResourceStoragePorts {
    fn observed<S>(storage: S, backend: &'static str, observer: Arc<dyn StorageObserver>) -> Self
    where
        S: CollectionStorage
            + ClassStorage
            + ObjectStorage
            + ClassRelationStorage
            + ObjectRelationStorage
            + 'static,
    {
        let observed = Arc::new(ObservedStorage::new(storage, backend, observer));
        Self {
            collections: observed.clone(),
            classes: observed.clone(),
            objects: observed.clone(),
            class_relations: observed.clone(),
            object_relations: observed,
        }
    }
}

/// Dispatch one capability call to the selected complete backend.
///
/// Keeping the exhaustive match here means adding a selectable backend has one
/// dispatch change instead of one change per storage operation. The aggregate
/// [`hubuum_storage_core::StorageBackend`] bound still makes missing capability implementations a
/// compile error before a backend can be composed.
macro_rules! dispatch_backend {
    ($handle:expr, |$backend:ident| $call:expr) => {
        match &$handle.inner.implementation {
            BackendImplementation::Postgres($backend) => $call,
        }
    };
}

mod api;
mod computed_fields;
mod events;
mod execution;
mod identity;
mod identity_queries;
mod operational;
mod queries;
mod relations;
mod tasks;
mod transaction;
mod workflows;

pub use api::StorageContext;
pub(crate) use api::storage_handle;

impl StorageHandle {
    #[cfg(any(test, feature = "integration-test-support", feature = "postgres-bench"))]
    pub(crate) fn postgres(pool: PostgresPool) -> Self {
        Self::from_postgres_backend(super::factory::compose_postgres(pool))
    }

    #[cfg(any(test, feature = "integration-test-support"))]
    pub(crate) fn postgres_with_operational_pool_settings(
        pool: PostgresPool,
        settings: PostgresPoolSettings,
    ) -> Self {
        let task_lease_pool = build_postgres_pool(&settings)
            .expect("validated task-lease pool settings must remain constructible");
        let notification_listener_pool_settings =
            super::factory::notification_listener_pool_settings(&settings)
                .expect("validated notification-listener settings must remain constructible");
        let notification_listener_pool = build_postgres_pool(&notification_listener_pool_settings)
            .expect("validated notification-listener pool settings must remain constructible");
        Self::from_postgres_backend(
            super::factory::compose_postgres(pool)
                .with_task_lease_pool(task_lease_pool)
                .with_notification_listener_pool(notification_listener_pool),
        )
    }

    pub(in crate::storage) fn from_postgres_backend(backend: PostgresStorage) -> Self {
        let database_diagnostics = super::factory::postgres_database_diagnostics(backend.clone());
        let worker_notification_provider = Arc::new(backend.clone());
        Self::from_registered_backend(backend)
            .with_database_diagnostics(database_diagnostics)
            .with_worker_notification_provider(worker_notification_provider)
    }

    #[cfg(test)]
    pub(crate) fn from_postgres_backend_with_storage_observer(
        backend: PostgresStorage,
        observer: Arc<dyn StorageObserver>,
    ) -> Self {
        let database_diagnostics = super::factory::postgres_database_diagnostics(backend.clone());
        let worker_notification_provider = Arc::new(backend.clone());
        Self::from_registered_backend_with_observer(backend, observer)
            .with_database_diagnostics(database_diagnostics)
            .with_worker_notification_provider(worker_notification_provider)
    }

    pub(crate) fn from_registered_backend<S>(backend: S) -> Self
    where
        S: RegisteredStorageBackend,
    {
        Self::from_registered_backend_with_observer(backend, Arc::new(ApplicationStorageObserver))
    }

    fn from_registered_backend_with_observer<S>(
        backend: S,
        observer: Arc<dyn StorageObserver>,
    ) -> Self
    where
        S: RegisteredStorageBackend,
    {
        let kind = S::KIND;
        let resource_ports =
            ResourceStoragePorts::observed(backend.clone(), kind.as_str(), observer.clone());
        Self {
            inner: Arc::new(StorageHandleInner {
                descriptor: StorageBackendDescriptor::new(kind),
                implementation: backend.into_implementation(),
                resource_ports,
                observer,
                database_diagnostics: None,
                worker_notification_provider: None,
            }),
        }
    }

    pub(in crate::storage) fn with_database_diagnostics(
        mut self,
        database_diagnostics: Arc<dyn DatabaseDiagnosticsProvider>,
    ) -> Self {
        Arc::get_mut(&mut self.inner)
            .expect("new storage handles have no other owners")
            .database_diagnostics = Some(database_diagnostics);
        self
    }

    fn with_worker_notification_provider(
        mut self,
        provider: Arc<dyn WorkerNotificationProvider>,
    ) -> Self {
        Arc::get_mut(&mut self.inner)
            .expect("new storage handles have no other owners")
            .worker_notification_provider = Some(provider);
        self
    }

    pub(crate) fn descriptor(&self) -> StorageBackendDescriptor {
        self.inner.descriptor
    }

    fn backend_name(&self) -> &'static str {
        self.descriptor().kind().as_str()
    }

    async fn observe_storage_call<T>(
        &self,
        backend: &'static str,
        capability: StorageCapability,
        operation: &'static str,
        future: impl Future<Output = Result<T, StorageError>>,
    ) -> Result<T, StorageError> {
        observe_storage_call_with(
            self.inner.observer.as_ref(),
            backend,
            capability,
            operation,
            future,
        )
        .await
    }

    /// Return adapter-projected pool diagnostics for the legacy database endpoint.
    pub(crate) fn database_pool_state(&self) -> Option<DatabasePoolState> {
        self.inner
            .database_diagnostics
            .as_ref()
            .map(|diagnostics| diagnostics.pool_state())
    }

    /// Return adapter-projected state for the legacy database endpoint.
    pub(crate) async fn database_storage_snapshot(
        &self,
    ) -> Result<Option<DatabaseStorageSnapshot>, StorageError> {
        let Some(diagnostics) = self.inner.database_diagnostics.as_ref() else {
            return Ok(None);
        };
        diagnostics.storage_snapshot().await.map(Some)
    }

    pub(crate) fn collection_store(&self) -> Arc<dyn CollectionStorage> {
        self.inner.resource_ports.collections.clone()
    }

    pub(crate) fn class_store(&self) -> Arc<dyn ClassStorage> {
        self.inner.resource_ports.classes.clone()
    }

    pub(crate) fn object_store(&self) -> Arc<dyn ObjectStorage> {
        self.inner.resource_ports.objects.clone()
    }

    pub(crate) fn class_relation_store(&self) -> Arc<dyn ClassRelationStorage> {
        self.inner.resource_ports.class_relations.clone()
    }

    pub(crate) fn object_relation_store(&self) -> Arc<dyn ObjectRelationStorage> {
        self.inner.resource_ports.object_relations.clone()
    }
}
