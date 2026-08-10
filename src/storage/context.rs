use actix_web::web::Data;

use crate::events::{EventDeliverySettings, EventFanoutSettings, EventRetentionSettings};
use crate::models::search::QueryOptions;
use crate::models::{MaintenanceState, TokenRetentionSettings};
use crate::permissions::{AppContext, PermissionBackend};
use crate::storage::observed::observe_storage_call;
use crate::storage::postgres::PostgresPool;
use crate::storage::{
    AuthenticationIdentity, AuthenticationStorage, AuthenticationTokenScope,
    AuthenticationTokenScopeQuery, AuthorizationCollection, AuthorizationCollectionAccessQuery,
    AuthorizationCollectionGrantListQuery, AuthorizationCollectionsQuery, AuthorizationGrant,
    AuthorizationGrantKey, AuthorizationGrantMutation, AuthorizationGroup,
    AuthorizationGroupGrantPage, AuthorizationGroupMembershipQuery, AuthorizationPolicySnapshotRow,
    AuthorizationPrincipal, AuthorizationStorage, DynLifecycleStorage, EventArchive,
    EventDeliveryBatch, EventDeliveryClaim, EventDeliveryHealthSnapshot, EventDeliveryStorage,
    EventFanoutStorage, EventHealthStorage, EventMetricsSnapshot, EventRetentionStorage,
    EventRetentionSummary, InventoryGaugeSnapshot, MetricsStorage, OperationalStateStorage,
    PostgresStorage, ReadinessSnapshot, StorageBackend, StorageBackendDescriptor, StorageError,
    StoragePoolState, TaskGaugeSnapshot, TokenRetentionStorage,
};
use async_trait::async_trait;

mod private {
    use crate::storage::postgres::PostgresPool;

    pub trait BackendAccess {
        fn db_pool(&self) -> &PostgresPool;
    }
}

/// An opaque handle to Hubuum's configured persistence backend.
///
/// Application code passes this handle to domain operations without selecting
/// a database implementation or handling a connection pool directly.
#[derive(Clone)]
pub(crate) struct StorageHandle {
    implementation: BackendImplementation,
}

#[derive(Clone)]
enum BackendImplementation {
    Postgresql(PostgresStorage),
}

impl StorageHandle {
    pub(crate) fn postgres(pool: PostgresPool) -> Self {
        let backend = PostgresStorage::new(pool);
        assert_complete_storage_backend(&backend);
        Self {
            implementation: BackendImplementation::Postgresql(backend),
        }
    }

    pub(crate) fn descriptor(&self) -> StorageBackendDescriptor {
        match &self.implementation {
            BackendImplementation::Postgresql(backend) => backend.descriptor(),
        }
    }

    fn backend_name(&self) -> &'static str {
        self.descriptor().kind().as_str()
    }

    pub(crate) fn lifecycle_storage(&self) -> DynLifecycleStorage {
        match &self.implementation {
            BackendImplementation::Postgresql(backend) => {
                DynLifecycleStorage::from_backend(backend.clone())
            }
        }
    }

    fn postgres_pool(&self) -> &PostgresPool {
        match &self.implementation {
            BackendImplementation::Postgresql(backend) => backend.pool(),
        }
    }
}

#[async_trait]
impl AuthenticationStorage for StorageHandle {
    async fn load_authentication_identity(
        &self,
        principal_id: i32,
    ) -> Result<AuthenticationIdentity, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "authentication",
            "load_identity",
            async {
                match &self.implementation {
                    BackendImplementation::Postgresql(backend) => {
                        backend.load_authentication_identity(principal_id).await
                    }
                }
            },
        )
        .await
    }

    async fn load_authentication_token_scope(
        &self,
        query: AuthenticationTokenScopeQuery,
    ) -> Result<Option<AuthenticationTokenScope>, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "authentication",
            "load_token_scope",
            async {
                match &self.implementation {
                    BackendImplementation::Postgresql(backend) => {
                        backend.load_authentication_token_scope(query).await
                    }
                }
            },
        )
        .await
    }
}

#[async_trait]
impl AuthorizationStorage for StorageHandle {
    async fn load_authorization_principal(
        &self,
        principal_id: i32,
    ) -> Result<AuthorizationPrincipal, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "authorization",
            "load_principal",
            async {
                match &self.implementation {
                    BackendImplementation::Postgresql(backend) => {
                        backend.load_authorization_principal(principal_id).await
                    }
                }
            },
        )
        .await
    }

    async fn authorization_principal_is_group_member(
        &self,
        query: AuthorizationGroupMembershipQuery,
    ) -> Result<bool, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "authorization",
            "principal_is_group_member",
            async {
                match &self.implementation {
                    BackendImplementation::Postgresql(backend) => {
                        backend.authorization_principal_is_group_member(query).await
                    }
                }
            },
        )
        .await
    }

    async fn authorize_local_collection(
        &self,
        query: AuthorizationCollectionAccessQuery,
    ) -> Result<bool, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "authorization",
            "authorize_local_collection",
            async {
                match &self.implementation {
                    BackendImplementation::Postgresql(backend) => {
                        backend.authorize_local_collection(query).await
                    }
                }
            },
        )
        .await
    }

    async fn local_authorized_collections(
        &self,
        query: AuthorizationCollectionsQuery,
    ) -> Result<Vec<AuthorizationCollection>, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "authorization",
            "local_authorized_collections",
            async {
                match &self.implementation {
                    BackendImplementation::Postgresql(backend) => {
                        backend.local_authorized_collections(query).await
                    }
                }
            },
        )
        .await
    }

    async fn list_authorization_collection_candidates(
        &self,
    ) -> Result<Vec<AuthorizationCollection>, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "authorization",
            "list_collection_candidates",
            async {
                match &self.implementation {
                    BackendImplementation::Postgresql(backend) => {
                        backend.list_authorization_collection_candidates().await
                    }
                }
            },
        )
        .await
    }

    async fn list_authorization_group_candidates(
        &self,
        query_options: QueryOptions,
    ) -> Result<Vec<AuthorizationGroup>, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "authorization",
            "list_group_candidates",
            async {
                match &self.implementation {
                    BackendImplementation::Postgresql(backend) => {
                        backend
                            .list_authorization_group_candidates(query_options)
                            .await
                    }
                }
            },
        )
        .await
    }

    async fn authorization_policy_snapshot(
        &self,
    ) -> Result<Vec<AuthorizationPolicySnapshotRow>, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "authorization",
            "policy_snapshot",
            async {
                match &self.implementation {
                    BackendImplementation::Postgresql(backend) => {
                        backend.authorization_policy_snapshot().await
                    }
                }
            },
        )
        .await
    }

    async fn list_local_collection_grants(
        &self,
        query: AuthorizationCollectionGrantListQuery,
    ) -> Result<AuthorizationGroupGrantPage, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "authorization",
            "list_local_collection_grants",
            async {
                match &self.implementation {
                    BackendImplementation::Postgresql(backend) => {
                        backend.list_local_collection_grants(query).await
                    }
                }
            },
        )
        .await
    }

    async fn get_local_collection_grant(
        &self,
        key: AuthorizationGrantKey,
    ) -> Result<Option<AuthorizationGrant>, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "authorization",
            "get_local_collection_grant",
            async {
                match &self.implementation {
                    BackendImplementation::Postgresql(backend) => {
                        backend.get_local_collection_grant(key).await
                    }
                }
            },
        )
        .await
    }

    async fn apply_local_collection_grant(
        &self,
        mutation: AuthorizationGrantMutation,
    ) -> Result<AuthorizationGrant, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "authorization",
            "apply_local_collection_grant",
            async {
                match &self.implementation {
                    BackendImplementation::Postgresql(backend) => {
                        backend.apply_local_collection_grant(mutation).await
                    }
                }
            },
        )
        .await
    }

    async fn revoke_local_collection_grant(
        &self,
        mutation: AuthorizationGrantMutation,
    ) -> Result<AuthorizationGrant, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "authorization",
            "revoke_local_collection_grant",
            async {
                match &self.implementation {
                    BackendImplementation::Postgresql(backend) => {
                        backend.revoke_local_collection_grant(mutation).await
                    }
                }
            },
        )
        .await
    }

    async fn revoke_all_local_collection_grants(
        &self,
        key: AuthorizationGrantKey,
    ) -> Result<(), StorageError> {
        observe_storage_call(
            self.backend_name(),
            "authorization",
            "revoke_all_local_collection_grants",
            async {
                match &self.implementation {
                    BackendImplementation::Postgresql(backend) => {
                        backend.revoke_all_local_collection_grants(key).await
                    }
                }
            },
        )
        .await
    }
}

#[async_trait]
impl MetricsStorage for StorageHandle {
    fn metrics_pool_state(&self) -> StoragePoolState {
        match &self.implementation {
            BackendImplementation::Postgresql(backend) => backend.metrics_pool_state(),
        }
    }

    async fn metrics_inventory_snapshot(&self) -> Result<InventoryGaugeSnapshot, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "metrics",
            "inventory_snapshot",
            async {
                match &self.implementation {
                    BackendImplementation::Postgresql(backend) => {
                        backend.metrics_inventory_snapshot().await
                    }
                }
            },
        )
        .await
    }

    async fn metrics_task_snapshot(&self) -> Result<TaskGaugeSnapshot, StorageError> {
        observe_storage_call(self.backend_name(), "metrics", "task_snapshot", async {
            match &self.implementation {
                BackendImplementation::Postgresql(backend) => backend.metrics_task_snapshot().await,
            }
        })
        .await
    }

    async fn metrics_event_snapshot(&self) -> Result<EventMetricsSnapshot, StorageError> {
        observe_storage_call(self.backend_name(), "metrics", "event_snapshot", async {
            match &self.implementation {
                BackendImplementation::Postgresql(backend) => {
                    backend.metrics_event_snapshot().await
                }
            }
        })
        .await
    }
}

#[async_trait]
impl OperationalStateStorage for StorageHandle {
    async fn readiness_snapshot(&self) -> Result<ReadinessSnapshot, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "operational_state",
            "readiness_snapshot",
            async {
                match &self.implementation {
                    BackendImplementation::Postgresql(backend) => {
                        backend.readiness_snapshot().await
                    }
                }
            },
        )
        .await
    }

    async fn maintenance_state(&self) -> Result<MaintenanceState, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "operational_state",
            "maintenance_state",
            async {
                match &self.implementation {
                    BackendImplementation::Postgresql(backend) => backend.maintenance_state().await,
                }
            },
        )
        .await
    }
}

#[async_trait]
impl EventHealthStorage for StorageHandle {
    async fn event_delivery_health(&self) -> Result<EventDeliveryHealthSnapshot, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "event_health",
            "delivery_health",
            async {
                match &self.implementation {
                    BackendImplementation::Postgresql(backend) => {
                        backend.event_delivery_health().await
                    }
                }
            },
        )
        .await
    }
}

#[async_trait]
impl EventDeliveryStorage for StorageHandle {
    async fn claim_event_delivery_batch(
        &self,
        settings: EventDeliverySettings,
    ) -> Result<EventDeliveryBatch, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "event_delivery",
            "claim_batch",
            async {
                match &self.implementation {
                    BackendImplementation::Postgresql(backend) => {
                        backend.claim_event_delivery_batch(settings).await
                    }
                }
            },
        )
        .await
    }

    async fn mark_event_delivery_succeeded(
        &self,
        claim: &EventDeliveryClaim,
    ) -> Result<(), StorageError> {
        observe_storage_call(
            self.backend_name(),
            "event_delivery",
            "mark_succeeded",
            async {
                match &self.implementation {
                    BackendImplementation::Postgresql(backend) => {
                        backend.mark_event_delivery_succeeded(claim).await
                    }
                }
            },
        )
        .await
    }

    async fn mark_event_delivery_failed(
        &self,
        claim: &EventDeliveryClaim,
        settings: EventDeliverySettings,
        error: &str,
    ) -> Result<(), StorageError> {
        observe_storage_call(
            self.backend_name(),
            "event_delivery",
            "mark_failed",
            async {
                match &self.implementation {
                    BackendImplementation::Postgresql(backend) => {
                        backend
                            .mark_event_delivery_failed(claim, settings, error)
                            .await
                    }
                }
            },
        )
        .await
    }
}

#[async_trait]
impl EventFanoutStorage for StorageHandle {
    async fn process_event_fanout_batch(
        &self,
        settings: EventFanoutSettings,
    ) -> Result<usize, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "event_fanout",
            "process_batch",
            async {
                match &self.implementation {
                    BackendImplementation::Postgresql(backend) => {
                        backend.process_event_fanout_batch(settings).await
                    }
                }
            },
        )
        .await
    }
}

#[async_trait]
impl EventRetentionStorage for StorageHandle {
    async fn process_event_retention_batch(
        &self,
        settings: EventRetentionSettings,
        archive: &dyn EventArchive,
    ) -> Result<EventRetentionSummary, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "event_retention",
            "process_batch",
            async {
                match &self.implementation {
                    BackendImplementation::Postgresql(backend) => {
                        backend
                            .process_event_retention_batch(settings, archive)
                            .await
                    }
                }
            },
        )
        .await
    }
}

#[async_trait]
impl TokenRetentionStorage for StorageHandle {
    async fn purge_expired_tokens(
        &self,
        settings: TokenRetentionSettings,
    ) -> Result<usize, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "token_retention",
            "purge_expired",
            async {
                match &self.implementation {
                    BackendImplementation::Postgresql(backend) => {
                        backend.purge_expired_tokens(settings).await
                    }
                }
            },
        )
        .await
    }
}

fn assert_complete_storage_backend(backend: &impl StorageBackend) {
    let _ = backend.descriptor();
}

/// A persistence capability accepted by Hubuum's domain and workflow APIs.
///
/// The trait is sealed so consumers cannot depend on backend implementation
/// details. The current PostgreSQL adapter is selected at application
/// composition time and remains hidden behind this capability.
pub trait StorageContext: private::BackendAccess + Send + Sync {
    fn permission_backend(&self) -> Option<&dyn PermissionBackend> {
        None
    }
}

pub(in crate::storage) fn postgres_pool<C>(backend: &C) -> &PostgresPool
where
    C: StorageContext + ?Sized,
{
    private::BackendAccess::db_pool(backend)
}

/// Normalize any accepted storage context into the opaque application handle.
pub(crate) fn storage_handle<C>(backend: &C) -> StorageHandle
where
    C: StorageContext + ?Sized,
{
    StorageHandle::postgres(postgres_pool(backend).clone())
}

impl private::BackendAccess for StorageHandle {
    fn db_pool(&self) -> &PostgresPool {
        self.postgres_pool()
    }
}

impl StorageContext for StorageHandle {}

impl private::BackendAccess for PostgresPool {
    fn db_pool(&self) -> &PostgresPool {
        self
    }
}

impl StorageContext for PostgresPool {}

impl private::BackendAccess for AppContext {
    fn db_pool(&self) -> &PostgresPool {
        private::BackendAccess::db_pool(self.backend())
    }
}

impl<T> private::BackendAccess for &T
where
    T: StorageContext + ?Sized,
{
    fn db_pool(&self) -> &PostgresPool {
        private::BackendAccess::db_pool(*self)
    }
}

impl<T> StorageContext for &T
where
    T: StorageContext + ?Sized,
{
    fn permission_backend(&self) -> Option<&dyn PermissionBackend> {
        (*self).permission_backend()
    }
}

impl<T> private::BackendAccess for Data<T>
where
    T: StorageContext + ?Sized + 'static,
{
    fn db_pool(&self) -> &PostgresPool {
        private::BackendAccess::db_pool(self.as_ref())
    }
}

impl<T> StorageContext for Data<T>
where
    T: StorageContext + ?Sized + 'static,
{
    fn permission_backend(&self) -> Option<&dyn PermissionBackend> {
        self.as_ref().permission_backend()
    }
}
