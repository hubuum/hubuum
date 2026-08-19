use actix_web::web::Data;
use chrono::NaiveDateTime;
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
    ApplicationStorageTelemetry, ObservedStorage, observe_infallible_storage_call,
    observe_storage_call,
};
use crate::storage::{
    AuditEventStorage, AuthenticatedToken, AuthenticationAttempt, AuthenticationIdentity,
    AuthenticationStorage, AuthenticationTokenScope, AuthenticationTokenScopeQuery,
    AuthorizationClassResource, AuthorizationCollection, AuthorizationCollectionAccessQuery,
    AuthorizationCollectionGrantListQuery, AuthorizationCollectionGroupsPageQuery,
    AuthorizationCollectionGroupsQuery, AuthorizationCollectionVisibilityQuery,
    AuthorizationCollectionsAccessQuery, AuthorizationCollectionsQuery,
    AuthorizationEffectiveGroupGrant, AuthorizationGrant, AuthorizationGrantDelete,
    AuthorizationGrantKey, AuthorizationGrantMutation, AuthorizationGroup,
    AuthorizationGroupCollectionQuery, AuthorizationGroupGrant, AuthorizationGroupGrantPage,
    AuthorizationGroupMembershipQuery, AuthorizationGroupPage, AuthorizationObjectResource,
    AuthorizationPermissionSet, AuthorizationPermissionSetQuery, AuthorizationPolicySnapshotRow,
    AuthorizationPrincipal, AuthorizationPrincipalCollectionPageQuery,
    AuthorizationPrincipalCollectionQuery, AuthorizationResourceIds, AuthorizationStorage,
    BackupSnapshotStorage, BidirectionalRelatedObjectsQuery, CatalogListQuery, CatalogPage,
    CatalogStorage, CertifiedStorageBackend, ClassRelationStorage, ClassStorage,
    CollectionAuthorizationStorage, CollectionStorage, ComputedFieldLifecycleStorage,
    ComputedObjectEnrichmentQuery, ComputedObjectListQuery, ComputedObjectPage,
    ComputedObjectStorage, EventArchive, EventDeliveryAdministrationStorage, EventDeliveryBatch,
    EventDeliveryClaim, EventDeliveryHealthSnapshot, EventDeliveryStorage, EventFanoutStorage,
    EventHealthStorage, EventMetricsSnapshot, EventRetentionBatch, EventRetentionBatchId,
    EventRetentionStorage, EventRetentionSummary, EventSubscriptionStorage,
    ExportTemplateHistoryRecord, ExportTemplateStorage, GroupStorage, HistoryAsOfQuery,
    HistoryListQuery, HistoryPage, HistoryPrincipalName, HistoryStorage, IdentityStorage,
    ImportStorage, InventoryGaugeSnapshot, InventoryStorage, MetricsStorage, MutationOutcome,
    ObjectAggregateAuthorizer, ObjectAggregateStorage, ObjectAggregateStorageQuery,
    ObjectHistoryAsOfQuery, ObjectHistoryListQuery, ObjectHistoryRecord, ObjectRelationStorage,
    ObjectRelationsTouchingIdsQuery, ObjectStorage, OperationalExportTemplateAuditEntry,
    OperationalExportTemplateHealth, OperationalStateStorage, OperationalStorageSnapshot,
    OperationalTaskQueueSnapshot, PrincipalStorage, ReadinessSnapshot, RelatedObjectsForRootsQuery,
    RelationGraphQuery, RelationIdsQuery, RelationListQuery, RelationPage, RelationQueryStorage,
    RelationTouchingQuery, RemoteTargetHistoryRecord, RemoteTargetStorage, RestoreStorage,
    StorageAuditEvent, StorageAuditEventListQuery, StorageBackendDescriptor,
    StorageBackendIdentity, StorageBackendKind, StorageBackupOutput, StorageBackupOutputSummary,
    StorageBackupSnapshot, StorageClass, StorageClassComputationState, StorageClassGraphRow,
    StorageClassRecord, StorageClassRelation, StorageCollection, StorageComputedFieldDefinition,
    StorageComputedFieldMutation, StorageComputedFieldPage, StorageComputedFieldRebuildRequest,
    StorageComputedObject, StorageDefaultAdminBootstrap, StorageError, StorageEventDelivery,
    StorageEventDeliveryListQuery, StorageEventPage, StorageEventSink, StorageEventSinkCreate,
    StorageEventSinkDelete, StorageEventSinkListQuery, StorageEventSinkUpdate,
    StorageEventSubscription, StorageEventSubscriptionCreate, StorageEventSubscriptionDelete,
    StorageEventSubscriptionListQuery, StorageEventSubscriptionUpdate, StorageExecution,
    StorageExecutionScope, StorageExportOutput, StorageExportOutputSummary, StorageExportTemplate,
    StorageExportTemplateCreate, StorageExportTemplateDelete, StorageExportTemplateListQuery,
    StorageExportTemplatePage, StorageExportTemplateReplace, StorageExternalPrincipalState,
    StorageExternalUserSync, StorageGroupCreate, StorageGroupListQuery, StorageGroupUpdate,
    StorageIdentityGroup, StorageIdentityPage, StorageIdentityScope, StorageIdentityScopeEnsure,
    StorageImportApply, StorageImportCollectionKey, StorageImportMode, StorageImportPlan,
    StorageImportPreflight, StorageImportResult, StorageImportTaskResultPage,
    StorageInventoryCounts, StorageLocalPasswordReset, StorageNotification,
    StorageNotificationListener, StorageNotificationShutdown, StorageObject,
    StorageObjectAggregatePage, StorageObjectGraphRow, StorageObjectRelation,
    StoragePersonalComputedFieldCreate, StoragePersonalComputedFieldDelete,
    StoragePersonalComputedFieldListQuery, StoragePersonalComputedFieldUpdate, StoragePoolState,
    StoragePrincipal, StoragePrincipalGroup, StoragePrincipalGroupListQuery,
    StoragePrincipalSettings, StoragePrincipalSettingsMutation, StoragePrincipalTokensRevoke,
    StorageRelatedObjectForRootRow, StorageRelatedObjectIncludeRow, StorageRemoteTarget,
    StorageRemoteTargetCreate, StorageRemoteTargetDelete, StorageRemoteTargetInvocation,
    StorageRemoteTargetListQuery, StorageRemoteTargetPage, StorageRemoteTargetUpdate,
    StorageRestoreApply, StorageRestoreCompletion, StorageRestoreCoordinatorSnapshot,
    StorageRestoreDrainState, StorageRestoreFailure, StorageRestoreJob, StorageRestoreStageCreate,
    StorageRestoreStatus, StorageServiceAccount, StorageServiceAccountCreate,
    StorageServiceAccountDisableOutcome, StorageServiceAccountListItem,
    StorageServiceAccountListQuery, StorageServiceAccountMutation, StorageServiceAccountPoint,
    StorageServiceAccountUpdate, StorageSharedComputedFieldCreate,
    StorageSharedComputedFieldDelete, StorageSharedComputedFieldUpdate, StorageSyncedHuman,
    StorageTask, StorageTaskAccess, StorageTaskClaim, StorageTaskCompletion,
    StorageTaskCreateRequest, StorageTaskEventAppend, StorageTaskEventPage, StorageTaskFailure,
    StorageTaskLease, StorageTaskLeaseDuration, StorageTaskListQuery, StorageTaskOutputLookup,
    StorageTaskPage, StorageTaskPageQuery, StorageTaskStateUpdate, StorageTelemetry,
    StorageTokenCreate, StorageTokenHashRevoke, StorageTokenListQuery, StorageTokenMetadata,
    StorageTokenObservation, StorageTokenRenew, StorageTokenRevoke, StorageTransaction,
    StorageTransactionFuture, StorageUser, StorageUserAnonymize, StorageUserCreate,
    StorageUserDelete, StorageUserListItem, StorageUserListQuery, StorageUserPasswordUpdate,
    StorageUserPoint, StorageUserUpdate, TaskExecutionStorage, TaskGaugeSnapshot, TaskQueueStorage,
    TokenRetentionStorage, TokenStorage, TransactionalStorage, UnifiedSearchClass,
    UnifiedSearchCollection, UnifiedSearchObject, UnifiedSearchQuery, UnifiedSearchStorage,
    UserStorage, WorkerNotificationStorage,
};
use crate::storage::{ClassHistoryRecord, CollectionHistoryRecord};
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
}

enum BackendImplementation {
    Postgresql(PostgresStorage),
}

struct ResourceStoragePorts {
    collections: Arc<dyn CollectionStorage>,
    classes: Arc<dyn ClassStorage>,
    objects: Arc<dyn ObjectStorage>,
    class_relations: Arc<dyn ClassRelationStorage>,
    object_relations: Arc<dyn ObjectRelationStorage>,
}

impl ResourceStoragePorts {
    fn observed<S>(storage: S, telemetry: Arc<dyn StorageTelemetry>) -> Self
    where
        S: StorageBackendIdentity
            + CollectionStorage
            + ClassStorage
            + ObjectStorage
            + ClassRelationStorage
            + ObjectRelationStorage
            + 'static,
    {
        let observed = Arc::new(ObservedStorage::new(storage, telemetry));
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
            BackendImplementation::Postgresql($backend) => $call,
        }
    };
}

mod api;
mod computed_fields;
mod diagnostics;
mod execution;
mod identity;
mod operations;
mod queries;
mod relations;
mod resources;
mod tasks;
mod transaction;
mod workflows;

pub use api::StorageContext;
use api::assert_complete_storage_backend;
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
        Self::from_postgres_backend_with_storage_telemetry(
            backend,
            Arc::new(ApplicationStorageTelemetry),
        )
    }

    pub(crate) fn from_postgres_backend_with_storage_telemetry(
        backend: PostgresStorage,
        telemetry: Arc<dyn StorageTelemetry>,
    ) -> Self {
        Self::from_certified_backend(
            backend,
            StorageBackendKind::Postgresql,
            BackendImplementation::Postgresql,
            telemetry,
        )
    }

    fn from_certified_backend<S>(
        backend: S,
        kind: StorageBackendKind,
        into_implementation: impl FnOnce(S) -> BackendImplementation,
        telemetry: Arc<dyn StorageTelemetry>,
    ) -> Self
    where
        S: CertifiedStorageBackend + Clone + 'static,
    {
        assert_complete_storage_backend(&backend, kind);
        let resource_ports = ResourceStoragePorts::observed(backend.clone(), telemetry);
        Self {
            inner: Arc::new(StorageHandleInner {
                descriptor: StorageBackendDescriptor::new(kind),
                implementation: into_implementation(backend),
                resource_ports,
            }),
        }
    }

    pub(crate) fn descriptor(&self) -> StorageBackendDescriptor {
        self.inner.descriptor
    }

    fn backend_name(&self) -> &'static str {
        self.descriptor().kind().as_str()
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
