use actix_web::web::Data;
use chrono::NaiveDateTime;
use std::future::Future;
use std::pin::Pin;
use uuid::Uuid;

use crate::events::{
    EventContext, EventDeliverySettings, EventFanoutSettings, EventRetentionSettings,
    MutationProvenance,
};
use crate::models::search::QueryOptions;
use crate::models::{
    Collection, CollectionKey, HubuumClass, HubuumObject, ImportMode, MaintenanceState,
    NewHubuumObject, TokenRetentionSettings, UpdateHubuumObject,
};
use crate::permissions::{AppContext, PermissionBackend};
use crate::storage::observed::observe_storage_call;
use crate::storage::postgres::PostgresPool;
use crate::storage::{
    AuditEventStorage, AuthenticatedToken, AuthenticationCredential, AuthenticationIdentity,
    AuthenticationStorage, AuthenticationTokenScope, AuthenticationTokenScopeQuery,
    AuthorizationClassResource, AuthorizationCollection, AuthorizationCollectionAccessQuery,
    AuthorizationCollectionGrantListQuery, AuthorizationCollectionsAccessQuery,
    AuthorizationCollectionsQuery, AuthorizationGrant, AuthorizationGrantDelete,
    AuthorizationGrantKey, AuthorizationGrantMutation, AuthorizationGroup,
    AuthorizationGroupGrantPage, AuthorizationGroupMembershipQuery, AuthorizationObjectResource,
    AuthorizationPermissionSet, AuthorizationPermissionSetQuery, AuthorizationPolicySnapshotRow,
    AuthorizationPrincipal, AuthorizationResourceIds, AuthorizationStorage, BackupSnapshotStorage,
    BidirectionalRelatedObjectsQuery, CatalogListQuery, CatalogPage, CatalogStorage,
    ComputedFieldLifecycleStorage, ComputedObjectEnrichmentQuery, ComputedObjectListQuery,
    ComputedObjectPage, ComputedObjectStorage, DynLifecycleStorage, EventArchive,
    EventDeliveryAdministrationStorage, EventDeliveryBatch, EventDeliveryClaim,
    EventDeliveryHealthSnapshot, EventDeliveryStorage, EventFanoutStorage, EventHealthStorage,
    EventMetricsSnapshot, EventRetentionStorage, EventRetentionSummary, EventSubscriptionStorage,
    ExportQueryStorage, ExportTemplateHistoryRecord, ExportTemplateStorage, HistoryAsOfQuery,
    HistoryListQuery, HistoryPage, HistoryPrincipalName, HistoryStorage, IdentityStorage,
    ImportStorage, InventoryGaugeSnapshot, InventoryStorage, MetricsStorage,
    ObjectAggregateAuthorizer, ObjectAggregateStorage, ObjectAggregateStorageQuery,
    ObjectHistoryAsOfQuery, ObjectHistoryListQuery, ObjectHistoryRecord, ObjectRecordStorage,
    ObjectRelationsTouchingIdsQuery, OperationalExportTemplateAuditEntry,
    OperationalExportTemplateHealth, OperationalStateStorage, OperationalStorageSnapshot,
    OperationalTaskQueueSnapshot, PostgresStorage, ReadinessSnapshot, RelatedObjectsForRootsQuery,
    RelationGraphQuery, RelationIdsQuery, RelationListQuery, RelationPage, RelationQueryStorage,
    RelationTouchingQuery, RemoteTargetHistoryRecord, RemoteTargetStorage, RestoreStorage,
    StorageAuditEvent, StorageAuditEventListQuery, StorageBackend, StorageBackendDescriptor,
    StorageBackupOutput, StorageBackupOutputSummary, StorageBackupSnapshot, StorageCallSite,
    StorageClass, StorageClassComputationState, StorageClassGraphRow, StorageClassRelation,
    StorageCollection, StorageComputedFieldDefinition, StorageComputedFieldMutation,
    StorageComputedFieldPage, StorageComputedFieldRebuildRequest, StorageComputedObject,
    StorageDefaultAdminBootstrap, StorageError, StorageEventDelivery,
    StorageEventDeliveryListQuery, StorageEventPage, StorageEventSink, StorageEventSinkCreate,
    StorageEventSinkDelete, StorageEventSinkListQuery, StorageEventSinkUpdate,
    StorageEventSubscription, StorageEventSubscriptionCreate, StorageEventSubscriptionDelete,
    StorageEventSubscriptionListQuery, StorageEventSubscriptionUpdate, StorageExecution,
    StorageExportOutput, StorageExportOutputSummary, StorageExportTemplate,
    StorageExportTemplateCreate, StorageExportTemplateDelete, StorageExportTemplateListQuery,
    StorageExportTemplatePage, StorageExportTemplateReplace, StorageExternalPrincipalState,
    StorageExternalUserSync, StorageIdentityPage, StorageIdentityScope, StorageIdentityScopeEnsure,
    StorageImportApply, StorageImportPlanItem, StorageImportPreflight, StorageImportResult,
    StorageImportTaskResultPage, StorageInventoryCounts, StorageLocalPasswordReset, StorageObject,
    StorageObjectAggregatePage, StorageObjectGraphRow, StorageObjectRelation,
    StoragePersonalComputedFieldCreate, StoragePersonalComputedFieldDelete,
    StoragePersonalComputedFieldListQuery, StoragePersonalComputedFieldUpdate, StoragePoolState,
    StoragePrincipalGroup, StorageQueryBudget, StorageRelatedObjectForRootRow,
    StorageRelatedObjectIncludeRow, StorageRemoteTarget, StorageRemoteTargetCreate,
    StorageRemoteTargetDelete, StorageRemoteTargetInvocation, StorageRemoteTargetListQuery,
    StorageRemoteTargetPage, StorageRemoteTargetUpdate, StorageRestoreApply,
    StorageRestoreCompletion, StorageRestoreCoordinatorSnapshot, StorageRestoreDrainState,
    StorageRestoreFailure, StorageRestoreJob, StorageRestoreStageCreate, StorageRestoreStatus,
    StorageRevisionPrecondition, StorageServiceAccount, StorageServiceAccountCreate,
    StorageServiceAccountListItem, StorageServiceAccountListQuery, StorageServiceAccountMutation,
    StorageServiceAccountPoint, StorageServiceAccountUpdate, StorageSharedComputedFieldCreate,
    StorageSharedComputedFieldDelete, StorageSharedComputedFieldUpdate, StorageSyncedHuman,
    StorageTask, StorageTaskAccess, StorageTaskClaim, StorageTaskCompletion,
    StorageTaskCreateRequest, StorageTaskEventAppend, StorageTaskEventPage, StorageTaskFailure,
    StorageTaskLease, StorageTaskLeaseDuration, StorageTaskListQuery, StorageTaskOutputLookup,
    StorageTaskPage, StorageTaskPageQuery, StorageTaskStateUpdate, StorageTokenListQuery,
    StorageTokenMetadata, TaskExecutionStorage, TaskGaugeSnapshot, TaskQueueStorage,
    TokenRetentionStorage, UnifiedSearchClass, UnifiedSearchCollection, UnifiedSearchObject,
    UnifiedSearchQuery, UnifiedSearchStorage,
};
use crate::storage::{ClassHistoryRecord, CollectionHistoryRecord};
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

impl ExportQueryStorage for StorageHandle {
    fn run_export_queries<'a, F, R>(
        &'a self,
        budget: Option<StorageQueryBudget>,
        future: F,
    ) -> Pin<Box<dyn Future<Output = R> + 'a>>
    where
        F: Future<Output = R> + 'a,
        R: 'a,
    {
        match &self.implementation {
            BackendImplementation::Postgresql(backend) => {
                backend.run_export_queries(budget, future)
            }
        }
    }
}

impl StorageExecution for StorageHandle {
    fn run_with_call_site<'a, F, R>(
        &'a self,
        call_site: StorageCallSite,
        future: F,
    ) -> Pin<Box<dyn Future<Output = R> + 'a>>
    where
        F: Future<Output = R> + 'a,
        R: 'a,
    {
        match &self.implementation {
            BackendImplementation::Postgresql(backend) => {
                backend.run_with_call_site(call_site, future)
            }
        }
    }

    fn run_with_call_site_send<'a, F, R>(
        &'a self,
        call_site: StorageCallSite,
        future: F,
    ) -> Pin<Box<dyn Future<Output = R> + Send + 'a>>
    where
        F: Future<Output = R> + Send + 'a,
        R: Send + 'a,
    {
        match &self.implementation {
            BackendImplementation::Postgresql(backend) => {
                backend.run_with_call_site_send(call_site, future)
            }
        }
    }

    fn run_with_mutation_provenance<'a, F, R>(
        &'a self,
        provenance: Option<MutationProvenance>,
        future: F,
    ) -> Pin<Box<dyn Future<Output = R> + 'a>>
    where
        F: Future<Output = R> + 'a,
        R: 'a,
    {
        match &self.implementation {
            BackendImplementation::Postgresql(backend) => {
                backend.run_with_mutation_provenance(provenance, future)
            }
        }
    }

    fn run_with_revision_precondition<'a, F, R>(
        &'a self,
        precondition: Option<StorageRevisionPrecondition>,
        future: F,
    ) -> Pin<Box<dyn Future<Output = R> + 'a>>
    where
        F: Future<Output = R> + 'a,
        R: 'a,
    {
        match &self.implementation {
            BackendImplementation::Postgresql(backend) => {
                backend.run_with_revision_precondition(precondition, future)
            }
        }
    }
}

#[async_trait]
impl AuthenticationStorage for StorageHandle {
    async fn authenticate_bearer_token(
        &self,
        credential: AuthenticationCredential,
    ) -> Result<AuthenticatedToken, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "authentication",
            "authenticate_bearer_token",
            async {
                match &self.implementation {
                    BackendImplementation::Postgresql(backend) => {
                        backend.authenticate_bearer_token(credential).await
                    }
                }
            },
        )
        .await
    }

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
impl IdentityStorage for StorageHandle {
    async fn default_admin_bootstrap_required(&self) -> Result<bool, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "identity",
            "default_admin_bootstrap_required",
            async {
                match &self.implementation {
                    BackendImplementation::Postgresql(backend) => {
                        backend.default_admin_bootstrap_required().await
                    }
                }
            },
        )
        .await
    }

    async fn bootstrap_default_admin(
        &self,
        request: StorageDefaultAdminBootstrap,
    ) -> Result<bool, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "identity",
            "bootstrap_default_admin",
            async {
                match &self.implementation {
                    BackendImplementation::Postgresql(backend) => {
                        backend.bootstrap_default_admin(request).await
                    }
                }
            },
        )
        .await
    }

    async fn reset_local_password(
        &self,
        request: StorageLocalPasswordReset,
    ) -> Result<usize, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "identity",
            "reset_local_password",
            async {
                match &self.implementation {
                    BackendImplementation::Postgresql(backend) => {
                        backend.reset_local_password(request).await
                    }
                }
            },
        )
        .await
    }

    async fn ensure_identity_scope(
        &self,
        request: StorageIdentityScopeEnsure,
    ) -> Result<StorageIdentityScope, StorageError> {
        observe_storage_call(self.backend_name(), "identity", "ensure_scope", async {
            match &self.implementation {
                BackendImplementation::Postgresql(backend) => {
                    backend.ensure_identity_scope(request).await
                }
            }
        })
        .await
    }

    async fn identity_scope_name(&self, scope_id: i32) -> Result<String, StorageError> {
        observe_storage_call(self.backend_name(), "identity", "load_scope_name", async {
            match &self.implementation {
                BackendImplementation::Postgresql(backend) => {
                    backend.identity_scope_name(scope_id).await
                }
            }
        })
        .await
    }

    async fn identity_scope_names(
        &self,
        scope_ids: Vec<i32>,
    ) -> Result<Vec<(i32, String)>, StorageError> {
        observe_storage_call(self.backend_name(), "identity", "load_scope_names", async {
            match &self.implementation {
                BackendImplementation::Postgresql(backend) => {
                    backend.identity_scope_names(scope_ids).await
                }
            }
        })
        .await
    }

    async fn load_principal_group(
        &self,
        principal_id: i32,
        group_id: i32,
    ) -> Result<StoragePrincipalGroup, StorageError> {
        observe_storage_call(self.backend_name(), "identity", "load_membership", async {
            match &self.implementation {
                BackendImplementation::Postgresql(backend) => {
                    backend.load_principal_group(principal_id, group_id).await
                }
            }
        })
        .await
    }

    async fn list_retained_tokens(
        &self,
        query: StorageTokenListQuery,
    ) -> Result<StorageIdentityPage<StorageTokenMetadata>, StorageError> {
        observe_storage_call(self.backend_name(), "identity", "list_tokens", async {
            match &self.implementation {
                BackendImplementation::Postgresql(backend) => {
                    backend.list_retained_tokens(query).await
                }
            }
        })
        .await
    }

    async fn is_human_owner_group_member(
        &self,
        principal_id: i32,
        owner_group_id: i32,
    ) -> Result<bool, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "identity",
            "human_owner_group_member",
            async {
                match &self.implementation {
                    BackendImplementation::Postgresql(backend) => {
                        backend
                            .is_human_owner_group_member(principal_id, owner_group_id)
                            .await
                    }
                }
            },
        )
        .await
    }

    async fn principal_is_disabled(&self, principal_id: i32) -> Result<bool, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "identity",
            "principal_is_disabled",
            async {
                match &self.implementation {
                    BackendImplementation::Postgresql(backend) => {
                        backend.principal_is_disabled(principal_id).await
                    }
                }
            },
        )
        .await
    }

    async fn load_service_account(
        &self,
        service_account_id: i32,
    ) -> Result<StorageServiceAccount, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "identity",
            "load_service_account",
            async {
                match &self.implementation {
                    BackendImplementation::Postgresql(backend) => {
                        backend.load_service_account(service_account_id).await
                    }
                }
            },
        )
        .await
    }

    async fn load_service_account_point(
        &self,
        service_account_id: i32,
    ) -> Result<StorageServiceAccountPoint, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "identity",
            "load_service_account_point",
            async {
                match &self.implementation {
                    BackendImplementation::Postgresql(backend) => {
                        backend.load_service_account_point(service_account_id).await
                    }
                }
            },
        )
        .await
    }

    async fn list_manageable_service_accounts(
        &self,
        query: StorageServiceAccountListQuery,
    ) -> Result<StorageIdentityPage<StorageServiceAccountListItem>, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "identity",
            "list_service_accounts",
            async {
                match &self.implementation {
                    BackendImplementation::Postgresql(backend) => {
                        backend.list_manageable_service_accounts(query).await
                    }
                }
            },
        )
        .await
    }

    async fn create_service_account(
        &self,
        request: StorageServiceAccountCreate,
    ) -> Result<StorageServiceAccount, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "identity",
            "create_service_account",
            async {
                match &self.implementation {
                    BackendImplementation::Postgresql(backend) => {
                        backend.create_service_account(request).await
                    }
                }
            },
        )
        .await
    }

    async fn update_service_account(
        &self,
        request: StorageServiceAccountUpdate,
    ) -> Result<StorageServiceAccount, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "identity",
            "update_service_account",
            async {
                match &self.implementation {
                    BackendImplementation::Postgresql(backend) => {
                        backend.update_service_account(request).await
                    }
                }
            },
        )
        .await
    }

    async fn disable_service_account(
        &self,
        request: StorageServiceAccountMutation,
    ) -> Result<StorageServiceAccount, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "identity",
            "disable_service_account",
            async {
                match &self.implementation {
                    BackendImplementation::Postgresql(backend) => {
                        backend.disable_service_account(request).await
                    }
                }
            },
        )
        .await
    }

    async fn delete_service_account(
        &self,
        request: StorageServiceAccountMutation,
    ) -> Result<(), StorageError> {
        observe_storage_call(
            self.backend_name(),
            "identity",
            "delete_service_account",
            async {
                match &self.implementation {
                    BackendImplementation::Postgresql(backend) => {
                        backend.delete_service_account(request).await
                    }
                }
            },
        )
        .await
    }

    async fn external_principal_state(
        &self,
        principal_id: i32,
    ) -> Result<Option<StorageExternalPrincipalState>, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "identity",
            "load_external_state",
            async {
                match &self.implementation {
                    BackendImplementation::Postgresql(backend) => {
                        backend.external_principal_state(principal_id).await
                    }
                }
            },
        )
        .await
    }

    async fn mark_external_sync_attempted(&self, principal_id: i32) -> Result<(), StorageError> {
        observe_storage_call(
            self.backend_name(),
            "identity",
            "mark_external_sync_attempted",
            async {
                match &self.implementation {
                    BackendImplementation::Postgresql(backend) => {
                        backend.mark_external_sync_attempted(principal_id).await
                    }
                }
            },
        )
        .await
    }

    async fn sync_external_user(
        &self,
        request: StorageExternalUserSync,
    ) -> Result<StorageSyncedHuman, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "identity",
            "sync_external_user",
            async {
                match &self.implementation {
                    BackendImplementation::Postgresql(backend) => {
                        backend.sync_external_user(request).await
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

    async fn load_authorization_classes(
        &self,
        query: AuthorizationResourceIds,
    ) -> Result<Vec<AuthorizationClassResource>, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "authorization",
            "load_classes",
            async {
                match &self.implementation {
                    BackendImplementation::Postgresql(backend) => {
                        backend.load_authorization_classes(query).await
                    }
                }
            },
        )
        .await
    }

    async fn load_authorization_objects(
        &self,
        query: AuthorizationResourceIds,
    ) -> Result<Vec<AuthorizationObjectResource>, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "authorization",
            "load_objects",
            async {
                match &self.implementation {
                    BackendImplementation::Postgresql(backend) => {
                        backend.load_authorization_objects(query).await
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

    async fn authorize_local_collections(
        &self,
        query: AuthorizationCollectionsAccessQuery,
    ) -> Result<bool, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "authorization",
            "authorize_local_collections",
            async {
                match &self.implementation {
                    BackendImplementation::Postgresql(backend) => {
                        backend.authorize_local_collections(query).await
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

    async fn load_local_collection_permission_set(
        &self,
        query: AuthorizationPermissionSetQuery,
    ) -> Result<AuthorizationPermissionSet, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "authorization",
            "load_local_collection_permission_set",
            async {
                match &self.implementation {
                    BackendImplementation::Postgresql(backend) => {
                        backend.load_local_collection_permission_set(query).await
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
        request: AuthorizationGrantDelete,
    ) -> Result<(), StorageError> {
        observe_storage_call(
            self.backend_name(),
            "authorization",
            "revoke_all_local_collection_grants",
            async {
                match &self.implementation {
                    BackendImplementation::Postgresql(backend) => {
                        backend.revoke_all_local_collection_grants(request).await
                    }
                }
            },
        )
        .await
    }
}

#[async_trait]
impl HistoryStorage for StorageHandle {
    async fn resolve_history_principal_names(
        &self,
        principal_ids: Vec<i32>,
    ) -> Result<Vec<HistoryPrincipalName>, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "history",
            "resolve_principal_names",
            async {
                match &self.implementation {
                    BackendImplementation::Postgresql(backend) => {
                        backend.resolve_history_principal_names(principal_ids).await
                    }
                }
            },
        )
        .await
    }

    async fn list_collection_history(
        &self,
        query: HistoryListQuery,
    ) -> Result<HistoryPage<CollectionHistoryRecord>, StorageError> {
        observe_storage_call(self.backend_name(), "history", "list_collections", async {
            match &self.implementation {
                BackendImplementation::Postgresql(backend) => {
                    backend.list_collection_history(query).await
                }
            }
        })
        .await
    }

    async fn collection_history_as_of(
        &self,
        query: HistoryAsOfQuery,
    ) -> Result<Option<CollectionHistoryRecord>, StorageError> {
        observe_storage_call(self.backend_name(), "history", "collection_as_of", async {
            match &self.implementation {
                BackendImplementation::Postgresql(backend) => {
                    backend.collection_history_as_of(query).await
                }
            }
        })
        .await
    }

    async fn list_class_history(
        &self,
        query: HistoryListQuery,
    ) -> Result<HistoryPage<ClassHistoryRecord>, StorageError> {
        observe_storage_call(self.backend_name(), "history", "list_classes", async {
            match &self.implementation {
                BackendImplementation::Postgresql(backend) => {
                    backend.list_class_history(query).await
                }
            }
        })
        .await
    }

    async fn class_history_as_of(
        &self,
        query: HistoryAsOfQuery,
    ) -> Result<Option<ClassHistoryRecord>, StorageError> {
        observe_storage_call(self.backend_name(), "history", "class_as_of", async {
            match &self.implementation {
                BackendImplementation::Postgresql(backend) => {
                    backend.class_history_as_of(query).await
                }
            }
        })
        .await
    }

    async fn list_object_history(
        &self,
        query: ObjectHistoryListQuery,
    ) -> Result<HistoryPage<ObjectHistoryRecord>, StorageError> {
        observe_storage_call(self.backend_name(), "history", "list_objects", async {
            match &self.implementation {
                BackendImplementation::Postgresql(backend) => {
                    backend.list_object_history(query).await
                }
            }
        })
        .await
    }

    async fn object_history_as_of(
        &self,
        query: ObjectHistoryAsOfQuery,
    ) -> Result<Option<ObjectHistoryRecord>, StorageError> {
        observe_storage_call(self.backend_name(), "history", "object_as_of", async {
            match &self.implementation {
                BackendImplementation::Postgresql(backend) => {
                    backend.object_history_as_of(query).await
                }
            }
        })
        .await
    }

    async fn list_export_template_history(
        &self,
        query: HistoryListQuery,
    ) -> Result<HistoryPage<ExportTemplateHistoryRecord>, StorageError> {
        observe_storage_call(self.backend_name(), "history", "list_templates", async {
            match &self.implementation {
                BackendImplementation::Postgresql(backend) => {
                    backend.list_export_template_history(query).await
                }
            }
        })
        .await
    }

    async fn export_template_history_as_of(
        &self,
        query: HistoryAsOfQuery,
    ) -> Result<Option<ExportTemplateHistoryRecord>, StorageError> {
        observe_storage_call(self.backend_name(), "history", "template_as_of", async {
            match &self.implementation {
                BackendImplementation::Postgresql(backend) => {
                    backend.export_template_history_as_of(query).await
                }
            }
        })
        .await
    }

    async fn list_remote_target_history(
        &self,
        query: HistoryListQuery,
    ) -> Result<HistoryPage<RemoteTargetHistoryRecord>, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "history",
            "list_remote_targets",
            async {
                match &self.implementation {
                    BackendImplementation::Postgresql(backend) => {
                        backend.list_remote_target_history(query).await
                    }
                }
            },
        )
        .await
    }

    async fn remote_target_history_as_of(
        &self,
        query: HistoryAsOfQuery,
    ) -> Result<Option<RemoteTargetHistoryRecord>, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "history",
            "remote_target_as_of",
            async {
                match &self.implementation {
                    BackendImplementation::Postgresql(backend) => {
                        backend.remote_target_history_as_of(query).await
                    }
                }
            },
        )
        .await
    }
}

#[async_trait]
impl CatalogStorage for StorageHandle {
    async fn list_collections(
        &self,
        query: CatalogListQuery,
    ) -> Result<CatalogPage<StorageCollection>, StorageError> {
        observe_storage_call(self.backend_name(), "catalog", "collections", async {
            match &self.implementation {
                BackendImplementation::Postgresql(backend) => backend.list_collections(query).await,
            }
        })
        .await
    }

    async fn list_classes(
        &self,
        query: CatalogListQuery,
    ) -> Result<CatalogPage<StorageClass>, StorageError> {
        observe_storage_call(self.backend_name(), "catalog", "classes", async {
            match &self.implementation {
                BackendImplementation::Postgresql(backend) => backend.list_classes(query).await,
            }
        })
        .await
    }

    async fn list_objects(
        &self,
        query: CatalogListQuery,
    ) -> Result<CatalogPage<StorageObject>, StorageError> {
        observe_storage_call(self.backend_name(), "catalog", "objects", async {
            match &self.implementation {
                BackendImplementation::Postgresql(backend) => backend.list_objects(query).await,
            }
        })
        .await
    }
}

#[async_trait]
impl ComputedObjectStorage for StorageHandle {
    async fn list_computed_objects(
        &self,
        query: ComputedObjectListQuery,
    ) -> Result<ComputedObjectPage, StorageError> {
        observe_storage_call(self.backend_name(), "computed_objects", "list", async {
            match &self.implementation {
                BackendImplementation::Postgresql(backend) => {
                    backend.list_computed_objects(query).await
                }
            }
        })
        .await
    }

    async fn enrich_objects_with_computed(
        &self,
        query: ComputedObjectEnrichmentQuery,
    ) -> Result<Vec<StorageComputedObject>, StorageError> {
        observe_storage_call(self.backend_name(), "computed_objects", "enrich", async {
            match &self.implementation {
                BackendImplementation::Postgresql(backend) => {
                    backend.enrich_objects_with_computed(query).await
                }
            }
        })
        .await
    }
}

#[async_trait]
impl TaskQueueStorage for StorageHandle {
    async fn create_task(
        &self,
        request: StorageTaskCreateRequest,
    ) -> Result<StorageTask, StorageError> {
        observe_storage_call(self.backend_name(), "tasks", "create", async {
            match &self.implementation {
                BackendImplementation::Postgresql(backend) => backend.create_task(request).await,
            }
        })
        .await
    }

    async fn get_task_access(&self, task_id: i32) -> Result<StorageTaskAccess, StorageError> {
        observe_storage_call(self.backend_name(), "tasks", "get_access", async {
            match &self.implementation {
                BackendImplementation::Postgresql(backend) => {
                    backend.get_task_access(task_id).await
                }
            }
        })
        .await
    }

    async fn list_tasks(
        &self,
        query: StorageTaskListQuery,
    ) -> Result<StorageTaskPage, StorageError> {
        observe_storage_call(self.backend_name(), "tasks", "list", async {
            match &self.implementation {
                BackendImplementation::Postgresql(backend) => backend.list_tasks(query).await,
            }
        })
        .await
    }

    async fn list_task_events(
        &self,
        query: StorageTaskPageQuery,
    ) -> Result<StorageTaskEventPage, StorageError> {
        observe_storage_call(self.backend_name(), "tasks", "list_events", async {
            match &self.implementation {
                BackendImplementation::Postgresql(backend) => backend.list_task_events(query).await,
            }
        })
        .await
    }

    async fn list_import_task_results(
        &self,
        query: StorageTaskPageQuery,
    ) -> Result<StorageImportTaskResultPage, StorageError> {
        observe_storage_call(self.backend_name(), "tasks", "list_import_results", async {
            match &self.implementation {
                BackendImplementation::Postgresql(backend) => {
                    backend.list_import_task_results(query).await
                }
            }
        })
        .await
    }

    async fn list_export_output_summaries(
        &self,
        task_ids: Vec<i32>,
    ) -> Result<Vec<StorageExportOutputSummary>, StorageError> {
        observe_storage_call(self.backend_name(), "tasks", "list_export_outputs", async {
            match &self.implementation {
                BackendImplementation::Postgresql(backend) => {
                    backend.list_export_output_summaries(task_ids).await
                }
            }
        })
        .await
    }

    async fn list_backup_output_summaries(
        &self,
        task_ids: Vec<i32>,
    ) -> Result<Vec<StorageBackupOutputSummary>, StorageError> {
        observe_storage_call(self.backend_name(), "tasks", "list_backup_outputs", async {
            match &self.implementation {
                BackendImplementation::Postgresql(backend) => {
                    backend.list_backup_output_summaries(task_ids).await
                }
            }
        })
        .await
    }

    async fn get_export_output_summary(
        &self,
        task_id: i32,
    ) -> Result<StorageTaskOutputLookup<StorageExportOutputSummary>, StorageError> {
        observe_storage_call(self.backend_name(), "tasks", "get_export_summary", async {
            match &self.implementation {
                BackendImplementation::Postgresql(backend) => {
                    backend.get_export_output_summary(task_id).await
                }
            }
        })
        .await
    }

    async fn get_backup_output_summary(
        &self,
        task_id: i32,
    ) -> Result<StorageTaskOutputLookup<StorageBackupOutputSummary>, StorageError> {
        observe_storage_call(self.backend_name(), "tasks", "get_backup_summary", async {
            match &self.implementation {
                BackendImplementation::Postgresql(backend) => {
                    backend.get_backup_output_summary(task_id).await
                }
            }
        })
        .await
    }

    async fn get_export_output(
        &self,
        task_id: i32,
    ) -> Result<StorageTaskOutputLookup<StorageExportOutput>, StorageError> {
        observe_storage_call(self.backend_name(), "tasks", "get_export_output", async {
            match &self.implementation {
                BackendImplementation::Postgresql(backend) => {
                    backend.get_export_output(task_id).await
                }
            }
        })
        .await
    }

    async fn get_backup_output(
        &self,
        task_id: i32,
    ) -> Result<StorageTaskOutputLookup<StorageBackupOutput>, StorageError> {
        observe_storage_call(self.backend_name(), "tasks", "get_backup_output", async {
            match &self.implementation {
                BackendImplementation::Postgresql(backend) => {
                    backend.get_backup_output(task_id).await
                }
            }
        })
        .await
    }
}

#[async_trait]
impl TaskExecutionStorage for StorageHandle {
    async fn claim_next_task(
        &self,
        lease_duration: StorageTaskLeaseDuration,
    ) -> Result<Option<StorageTaskClaim>, StorageError> {
        observe_storage_call(self.backend_name(), "task_execution", "claim", async {
            match &self.implementation {
                BackendImplementation::Postgresql(backend) => {
                    backend.claim_next_task(lease_duration).await
                }
            }
        })
        .await
    }

    async fn renew_task_lease(
        &self,
        lease: StorageTaskLease,
        lease_duration: StorageTaskLeaseDuration,
    ) -> Result<bool, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "task_execution",
            "renew_lease",
            async {
                match &self.implementation {
                    BackendImplementation::Postgresql(backend) => {
                        backend.renew_task_lease(lease, lease_duration).await
                    }
                }
            },
        )
        .await
    }

    async fn recover_expired_task_leases(
        &self,
        batch_size: usize,
    ) -> Result<Vec<StorageTask>, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "task_execution",
            "recover_leases",
            async {
                match &self.implementation {
                    BackendImplementation::Postgresql(backend) => {
                        backend.recover_expired_task_leases(batch_size).await
                    }
                }
            },
        )
        .await
    }

    async fn append_task_event(&self, event: StorageTaskEventAppend) -> Result<(), StorageError> {
        observe_storage_call(
            self.backend_name(),
            "task_execution",
            "append_event",
            async {
                match &self.implementation {
                    BackendImplementation::Postgresql(backend) => {
                        backend.append_task_event(event).await
                    }
                }
            },
        )
        .await
    }

    async fn update_task_state(
        &self,
        update: StorageTaskStateUpdate,
    ) -> Result<StorageTask, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "task_execution",
            "update_state",
            async {
                match &self.implementation {
                    BackendImplementation::Postgresql(backend) => {
                        backend.update_task_state(update).await
                    }
                }
            },
        )
        .await
    }

    async fn complete_task(
        &self,
        completion: StorageTaskCompletion,
    ) -> Result<StorageTask, StorageError> {
        observe_storage_call(self.backend_name(), "task_execution", "complete", async {
            match &self.implementation {
                BackendImplementation::Postgresql(backend) => {
                    backend.complete_task(completion).await
                }
            }
        })
        .await
    }

    async fn fail_task(&self, failure: StorageTaskFailure) -> Result<StorageTask, StorageError> {
        observe_storage_call(self.backend_name(), "task_execution", "fail", async {
            match &self.implementation {
                BackendImplementation::Postgresql(backend) => backend.fail_task(failure).await,
            }
        })
        .await
    }

    async fn purge_expired_export_outputs(&self) -> Result<usize, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "task_execution",
            "purge_export_outputs",
            async {
                match &self.implementation {
                    BackendImplementation::Postgresql(backend) => {
                        backend.purge_expired_export_outputs().await
                    }
                }
            },
        )
        .await
    }

    async fn purge_expired_backup_outputs(&self) -> Result<usize, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "task_execution",
            "purge_backup_outputs",
            async {
                match &self.implementation {
                    BackendImplementation::Postgresql(backend) => {
                        backend.purge_expired_backup_outputs().await
                    }
                }
            },
        )
        .await
    }
}

#[async_trait]
impl BackupSnapshotStorage for StorageHandle {
    async fn snapshot_backup(
        &self,
        include_history: bool,
    ) -> Result<StorageBackupSnapshot, StorageError> {
        observe_storage_call(self.backend_name(), "backup_snapshots", "snapshot", async {
            match &self.implementation {
                BackendImplementation::Postgresql(backend) => {
                    backend.snapshot_backup(include_history).await
                }
            }
        })
        .await
    }
}

#[async_trait]
impl ComputedFieldLifecycleStorage for StorageHandle {
    async fn computed_field_state(
        &self,
        class_id: i32,
    ) -> Result<StorageClassComputationState, StorageError> {
        observe_storage_call(self.backend_name(), "computed_fields", "state", async {
            match &self.implementation {
                BackendImplementation::Postgresql(backend) => {
                    backend.computed_field_state(class_id).await
                }
            }
        })
        .await
    }

    async fn list_shared_computed_fields(
        &self,
        class_id: i32,
    ) -> Result<Vec<StorageComputedFieldDefinition>, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "computed_fields",
            "list_shared",
            async {
                match &self.implementation {
                    BackendImplementation::Postgresql(backend) => {
                        backend.list_shared_computed_fields(class_id).await
                    }
                }
            },
        )
        .await
    }

    async fn list_personal_computed_fields(
        &self,
        query: StoragePersonalComputedFieldListQuery,
    ) -> Result<StorageComputedFieldPage, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "computed_fields",
            "list_personal",
            async {
                match &self.implementation {
                    BackendImplementation::Postgresql(backend) => {
                        backend.list_personal_computed_fields(query).await
                    }
                }
            },
        )
        .await
    }

    async fn get_computed_field(
        &self,
        definition_id: i32,
    ) -> Result<StorageComputedFieldDefinition, StorageError> {
        observe_storage_call(self.backend_name(), "computed_fields", "get", async {
            match &self.implementation {
                BackendImplementation::Postgresql(backend) => {
                    backend.get_computed_field(definition_id).await
                }
            }
        })
        .await
    }

    async fn create_shared_computed_field(
        &self,
        request: StorageSharedComputedFieldCreate,
    ) -> Result<StorageComputedFieldMutation, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "computed_fields",
            "create_shared",
            async {
                match &self.implementation {
                    BackendImplementation::Postgresql(backend) => {
                        backend.create_shared_computed_field(request).await
                    }
                }
            },
        )
        .await
    }

    async fn update_shared_computed_field(
        &self,
        request: StorageSharedComputedFieldUpdate,
    ) -> Result<StorageComputedFieldMutation, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "computed_fields",
            "update_shared",
            async {
                match &self.implementation {
                    BackendImplementation::Postgresql(backend) => {
                        backend.update_shared_computed_field(request).await
                    }
                }
            },
        )
        .await
    }

    async fn delete_shared_computed_field(
        &self,
        request: StorageSharedComputedFieldDelete,
    ) -> Result<StorageClassComputationState, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "computed_fields",
            "delete_shared",
            async {
                match &self.implementation {
                    BackendImplementation::Postgresql(backend) => {
                        backend.delete_shared_computed_field(request).await
                    }
                }
            },
        )
        .await
    }

    async fn create_personal_computed_field(
        &self,
        request: StoragePersonalComputedFieldCreate,
    ) -> Result<StorageComputedFieldDefinition, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "computed_fields",
            "create_personal",
            async {
                match &self.implementation {
                    BackendImplementation::Postgresql(backend) => {
                        backend.create_personal_computed_field(request).await
                    }
                }
            },
        )
        .await
    }

    async fn update_personal_computed_field(
        &self,
        request: StoragePersonalComputedFieldUpdate,
    ) -> Result<StorageComputedFieldDefinition, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "computed_fields",
            "update_personal",
            async {
                match &self.implementation {
                    BackendImplementation::Postgresql(backend) => {
                        backend.update_personal_computed_field(request).await
                    }
                }
            },
        )
        .await
    }

    async fn delete_personal_computed_field(
        &self,
        request: StoragePersonalComputedFieldDelete,
    ) -> Result<(), StorageError> {
        observe_storage_call(
            self.backend_name(),
            "computed_fields",
            "delete_personal",
            async {
                match &self.implementation {
                    BackendImplementation::Postgresql(backend) => {
                        backend.delete_personal_computed_field(request).await
                    }
                }
            },
        )
        .await
    }

    async fn request_computed_field_rebuild(
        &self,
        request: StorageComputedFieldRebuildRequest,
    ) -> Result<StorageClassComputationState, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "computed_fields",
            "request_rebuild",
            async {
                match &self.implementation {
                    BackendImplementation::Postgresql(backend) => {
                        backend.request_computed_field_rebuild(request).await
                    }
                }
            },
        )
        .await
    }

    async fn execute_computed_field_rebuild(
        &self,
        lease: StorageTaskLease,
    ) -> Result<StorageTask, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "computed_fields",
            "execute_rebuild",
            async {
                match &self.implementation {
                    BackendImplementation::Postgresql(backend) => {
                        backend.execute_computed_field_rebuild(lease).await
                    }
                }
            },
        )
        .await
    }
}

#[async_trait]
impl ObjectAggregateStorage for StorageHandle {
    async fn aggregate_objects(
        &self,
        query: ObjectAggregateStorageQuery,
        authorizer: Option<&dyn ObjectAggregateAuthorizer>,
    ) -> Result<StorageObjectAggregatePage, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "object_aggregates",
            "aggregate",
            async {
                match &self.implementation {
                    BackendImplementation::Postgresql(backend) => {
                        backend.aggregate_objects(query, authorizer).await
                    }
                }
            },
        )
        .await
    }
}

#[async_trait]
impl RelationQueryStorage for StorageHandle {
    async fn list_class_relations(
        &self,
        query: RelationListQuery,
    ) -> Result<RelationPage<StorageClassRelation>, StorageError> {
        observe_storage_call(self.backend_name(), "relations", "list_classes", async {
            match &self.implementation {
                BackendImplementation::Postgresql(backend) => {
                    backend.list_class_relations(query).await
                }
            }
        })
        .await
    }

    async fn list_object_relations(
        &self,
        query: RelationListQuery,
    ) -> Result<RelationPage<StorageObjectRelation>, StorageError> {
        observe_storage_call(self.backend_name(), "relations", "list_objects", async {
            match &self.implementation {
                BackendImplementation::Postgresql(backend) => {
                    backend.list_object_relations(query).await
                }
            }
        })
        .await
    }

    async fn list_class_relations_touching(
        &self,
        query: RelationTouchingQuery,
    ) -> Result<RelationPage<StorageClassRelation>, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "relations",
            "classes_touching",
            async {
                match &self.implementation {
                    BackendImplementation::Postgresql(backend) => {
                        backend.list_class_relations_touching(query).await
                    }
                }
            },
        )
        .await
    }

    async fn list_object_relations_touching(
        &self,
        query: RelationTouchingQuery,
    ) -> Result<RelationPage<StorageObjectRelation>, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "relations",
            "objects_touching",
            async {
                match &self.implementation {
                    BackendImplementation::Postgresql(backend) => {
                        backend.list_object_relations_touching(query).await
                    }
                }
            },
        )
        .await
    }

    async fn class_relations_touching_ids(
        &self,
        query: RelationIdsQuery,
    ) -> Result<Vec<StorageClassRelation>, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "relations",
            "classes_touching_ids",
            async {
                match &self.implementation {
                    BackendImplementation::Postgresql(backend) => {
                        backend.class_relations_touching_ids(query).await
                    }
                }
            },
        )
        .await
    }

    async fn class_relations_between_ids(
        &self,
        query: RelationIdsQuery,
    ) -> Result<Vec<StorageClassRelation>, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "relations",
            "classes_between_ids",
            async {
                match &self.implementation {
                    BackendImplementation::Postgresql(backend) => {
                        backend.class_relations_between_ids(query).await
                    }
                }
            },
        )
        .await
    }

    async fn object_relations_between_ids(
        &self,
        query: RelationIdsQuery,
    ) -> Result<Vec<StorageObjectRelation>, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "relations",
            "objects_between_ids",
            async {
                match &self.implementation {
                    BackendImplementation::Postgresql(backend) => {
                        backend.object_relations_between_ids(query).await
                    }
                }
            },
        )
        .await
    }

    async fn object_relations_touching_ids(
        &self,
        query: ObjectRelationsTouchingIdsQuery,
    ) -> Result<Vec<StorageObjectRelation>, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "relations",
            "objects_touching_ids",
            async {
                match &self.implementation {
                    BackendImplementation::Postgresql(backend) => {
                        backend.object_relations_touching_ids(query).await
                    }
                }
            },
        )
        .await
    }

    async fn related_classes(
        &self,
        query: RelationGraphQuery,
    ) -> Result<RelationPage<StorageClassGraphRow>, StorageError> {
        observe_storage_call(self.backend_name(), "relations", "related_classes", async {
            match &self.implementation {
                BackendImplementation::Postgresql(backend) => backend.related_classes(query).await,
            }
        })
        .await
    }

    async fn related_objects(
        &self,
        query: RelationGraphQuery,
    ) -> Result<RelationPage<StorageObjectGraphRow>, StorageError> {
        observe_storage_call(self.backend_name(), "relations", "related_objects", async {
            match &self.implementation {
                BackendImplementation::Postgresql(backend) => backend.related_objects(query).await,
            }
        })
        .await
    }

    async fn related_objects_for_roots(
        &self,
        query: RelatedObjectsForRootsQuery,
    ) -> Result<Vec<StorageRelatedObjectIncludeRow>, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "relations",
            "related_objects_for_roots",
            async {
                match &self.implementation {
                    BackendImplementation::Postgresql(backend) => {
                        backend.related_objects_for_roots(query).await
                    }
                }
            },
        )
        .await
    }

    async fn bidirectionally_related_objects_for_roots(
        &self,
        query: BidirectionalRelatedObjectsQuery,
    ) -> Result<Vec<StorageRelatedObjectForRootRow>, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "relations",
            "bidirectional_objects_for_roots",
            async {
                match &self.implementation {
                    BackendImplementation::Postgresql(backend) => {
                        backend
                            .bidirectionally_related_objects_for_roots(query)
                            .await
                    }
                }
            },
        )
        .await
    }
}

#[async_trait]
impl UnifiedSearchStorage for StorageHandle {
    async fn search_unified_collections(
        &self,
        query: UnifiedSearchQuery,
    ) -> Result<Vec<UnifiedSearchCollection>, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "unified_search",
            "collections",
            async {
                match &self.implementation {
                    BackendImplementation::Postgresql(backend) => {
                        backend.search_unified_collections(query).await
                    }
                }
            },
        )
        .await
    }

    async fn search_unified_classes(
        &self,
        query: UnifiedSearchQuery,
    ) -> Result<Vec<UnifiedSearchClass>, StorageError> {
        observe_storage_call(self.backend_name(), "unified_search", "classes", async {
            match &self.implementation {
                BackendImplementation::Postgresql(backend) => {
                    backend.search_unified_classes(query).await
                }
            }
        })
        .await
    }

    async fn search_unified_objects(
        &self,
        query: UnifiedSearchQuery,
    ) -> Result<Vec<UnifiedSearchObject>, StorageError> {
        observe_storage_call(self.backend_name(), "unified_search", "objects", async {
            match &self.implementation {
                BackendImplementation::Postgresql(backend) => {
                    backend.search_unified_objects(query).await
                }
            }
        })
        .await
    }
}

#[async_trait]
impl ExportTemplateStorage for StorageHandle {
    async fn get_export_template(
        &self,
        template_id: i32,
    ) -> Result<StorageExportTemplate, StorageError> {
        observe_storage_call(self.backend_name(), "export_templates", "get", async {
            match &self.implementation {
                BackendImplementation::Postgresql(backend) => {
                    backend.get_export_template(template_id).await
                }
            }
        })
        .await
    }

    async fn list_export_templates(
        &self,
        query: StorageExportTemplateListQuery,
    ) -> Result<StorageExportTemplatePage, StorageError> {
        observe_storage_call(self.backend_name(), "export_templates", "list", async {
            match &self.implementation {
                BackendImplementation::Postgresql(backend) => {
                    backend.list_export_templates(query).await
                }
            }
        })
        .await
    }

    async fn list_export_templates_in_collection(
        &self,
        collection_id: i32,
        exclude_template_id: Option<i32>,
    ) -> Result<Vec<StorageExportTemplate>, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "export_templates",
            "list_in_collection",
            async {
                match &self.implementation {
                    BackendImplementation::Postgresql(backend) => {
                        backend
                            .list_export_templates_in_collection(collection_id, exclude_template_id)
                            .await
                    }
                }
            },
        )
        .await
    }

    async fn export_template_class_collection_id(
        &self,
        class_id: i32,
    ) -> Result<Option<i32>, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "export_templates",
            "class_collection",
            async {
                match &self.implementation {
                    BackendImplementation::Postgresql(backend) => {
                        backend.export_template_class_collection_id(class_id).await
                    }
                }
            },
        )
        .await
    }

    async fn create_export_template(
        &self,
        request: StorageExportTemplateCreate,
    ) -> Result<StorageExportTemplate, StorageError> {
        observe_storage_call(self.backend_name(), "export_templates", "create", async {
            match &self.implementation {
                BackendImplementation::Postgresql(backend) => {
                    backend.create_export_template(request).await
                }
            }
        })
        .await
    }

    async fn replace_export_template(
        &self,
        request: StorageExportTemplateReplace,
    ) -> Result<StorageExportTemplate, StorageError> {
        observe_storage_call(self.backend_name(), "export_templates", "replace", async {
            match &self.implementation {
                BackendImplementation::Postgresql(backend) => {
                    backend.replace_export_template(request).await
                }
            }
        })
        .await
    }

    async fn delete_export_template(
        &self,
        request: StorageExportTemplateDelete,
    ) -> Result<(), StorageError> {
        observe_storage_call(self.backend_name(), "export_templates", "delete", async {
            match &self.implementation {
                BackendImplementation::Postgresql(backend) => {
                    backend.delete_export_template(request).await
                }
            }
        })
        .await
    }
}

#[async_trait]
impl RemoteTargetStorage for StorageHandle {
    async fn get_remote_target(&self, target_id: i32) -> Result<StorageRemoteTarget, StorageError> {
        observe_storage_call(self.backend_name(), "remote_targets", "get", async {
            match &self.implementation {
                BackendImplementation::Postgresql(backend) => {
                    backend.get_remote_target(target_id).await
                }
            }
        })
        .await
    }

    async fn list_remote_targets(
        &self,
        query: StorageRemoteTargetListQuery,
    ) -> Result<StorageRemoteTargetPage, StorageError> {
        observe_storage_call(self.backend_name(), "remote_targets", "list", async {
            match &self.implementation {
                BackendImplementation::Postgresql(backend) => {
                    backend.list_remote_targets(query).await
                }
            }
        })
        .await
    }

    async fn create_remote_target(
        &self,
        request: StorageRemoteTargetCreate,
    ) -> Result<StorageRemoteTarget, StorageError> {
        observe_storage_call(self.backend_name(), "remote_targets", "create", async {
            match &self.implementation {
                BackendImplementation::Postgresql(backend) => {
                    backend.create_remote_target(request).await
                }
            }
        })
        .await
    }

    async fn update_remote_target(
        &self,
        request: StorageRemoteTargetUpdate,
    ) -> Result<StorageRemoteTarget, StorageError> {
        observe_storage_call(self.backend_name(), "remote_targets", "update", async {
            match &self.implementation {
                BackendImplementation::Postgresql(backend) => {
                    backend.update_remote_target(request).await
                }
            }
        })
        .await
    }

    async fn delete_remote_target(
        &self,
        request: StorageRemoteTargetDelete,
    ) -> Result<(), StorageError> {
        observe_storage_call(self.backend_name(), "remote_targets", "delete", async {
            match &self.implementation {
                BackendImplementation::Postgresql(backend) => {
                    backend.delete_remote_target(request).await
                }
            }
        })
        .await
    }

    async fn record_remote_target_invocation(
        &self,
        request: StorageRemoteTargetInvocation,
    ) -> Result<(), StorageError> {
        observe_storage_call(
            self.backend_name(),
            "remote_targets",
            "record_invocation",
            async {
                match &self.implementation {
                    BackendImplementation::Postgresql(backend) => {
                        backend.record_remote_target_invocation(request).await
                    }
                }
            },
        )
        .await
    }
}

#[async_trait]
impl ImportStorage for StorageHandle {
    async fn import_root_collection(&self) -> Result<Collection, StorageError> {
        observe_storage_call(self.backend_name(), "imports", "root_collection", async {
            match &self.implementation {
                BackendImplementation::Postgresql(backend) => {
                    backend.import_root_collection().await
                }
            }
        })
        .await
    }

    async fn import_collection_by_id(
        &self,
        collection_id: i32,
    ) -> Result<Option<Collection>, StorageError> {
        observe_storage_call(self.backend_name(), "imports", "collection_by_id", async {
            match &self.implementation {
                BackendImplementation::Postgresql(backend) => {
                    backend.import_collection_by_id(collection_id).await
                }
            }
        })
        .await
    }

    async fn import_collection_by_key(
        &self,
        key: &CollectionKey,
    ) -> Result<Option<Collection>, StorageError> {
        observe_storage_call(self.backend_name(), "imports", "collection_by_key", async {
            match &self.implementation {
                BackendImplementation::Postgresql(backend) => {
                    backend.import_collection_by_key(key).await
                }
            }
        })
        .await
    }

    async fn import_collections_by_name(
        &self,
        name: &str,
    ) -> Result<Vec<Collection>, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "imports",
            "collections_by_name",
            async {
                match &self.implementation {
                    BackendImplementation::Postgresql(backend) => {
                        backend.import_collections_by_name(name).await
                    }
                }
            },
        )
        .await
    }

    async fn import_collection_child_by_name(
        &self,
        parent_collection_id: i32,
        name: &str,
    ) -> Result<Option<Collection>, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "imports",
            "collection_child_by_name",
            async {
                match &self.implementation {
                    BackendImplementation::Postgresql(backend) => {
                        backend
                            .import_collection_child_by_name(parent_collection_id, name)
                            .await
                    }
                }
            },
        )
        .await
    }

    async fn import_class_by_name(
        &self,
        collection_id: i32,
        name: &str,
    ) -> Result<Option<HubuumClass>, StorageError> {
        observe_storage_call(self.backend_name(), "imports", "class_by_name", async {
            match &self.implementation {
                BackendImplementation::Postgresql(backend) => {
                    backend.import_class_by_name(collection_id, name).await
                }
            }
        })
        .await
    }

    async fn import_classes_by_names(
        &self,
        collection_id: i32,
        names: &[String],
    ) -> Result<Vec<HubuumClass>, StorageError> {
        observe_storage_call(self.backend_name(), "imports", "classes_by_names", async {
            match &self.implementation {
                BackendImplementation::Postgresql(backend) => {
                    backend.import_classes_by_names(collection_id, names).await
                }
            }
        })
        .await
    }

    async fn import_object_by_name(
        &self,
        class_id: i32,
        name: &str,
    ) -> Result<Option<HubuumObject>, StorageError> {
        observe_storage_call(self.backend_name(), "imports", "object_by_name", async {
            match &self.implementation {
                BackendImplementation::Postgresql(backend) => {
                    backend.import_object_by_name(class_id, name).await
                }
            }
        })
        .await
    }

    async fn import_objects_by_names(
        &self,
        class_id: i32,
        names: &[String],
    ) -> Result<Vec<HubuumObject>, StorageError> {
        observe_storage_call(self.backend_name(), "imports", "objects_by_names", async {
            match &self.implementation {
                BackendImplementation::Postgresql(backend) => {
                    backend.import_objects_by_names(class_id, names).await
                }
            }
        })
        .await
    }

    async fn import_class_relation_exists(
        &self,
        left_class_id: i32,
        right_class_id: i32,
    ) -> Result<bool, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "imports",
            "class_relation_exists",
            async {
                match &self.implementation {
                    BackendImplementation::Postgresql(backend) => {
                        backend
                            .import_class_relation_exists(left_class_id, right_class_id)
                            .await
                    }
                }
            },
        )
        .await
    }

    async fn import_object_relation_exists(
        &self,
        left_object_id: i32,
        right_object_id: i32,
    ) -> Result<bool, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "imports",
            "object_relation_exists",
            async {
                match &self.implementation {
                    BackendImplementation::Postgresql(backend) => {
                        backend
                            .import_object_relation_exists(left_object_id, right_object_id)
                            .await
                    }
                }
            },
        )
        .await
    }

    async fn import_group_exists(
        &self,
        identity_scope: &str,
        group_name: &str,
    ) -> Result<bool, StorageError> {
        observe_storage_call(self.backend_name(), "imports", "group_exists", async {
            match &self.implementation {
                BackendImplementation::Postgresql(backend) => {
                    backend
                        .import_group_exists(identity_scope, group_name)
                        .await
                }
            }
        })
        .await
    }

    async fn preflight_import(
        &self,
        items: Vec<StorageImportPlanItem>,
        mode: ImportMode,
    ) -> Result<StorageImportPreflight, StorageError> {
        observe_storage_call(self.backend_name(), "imports", "preflight", async {
            match &self.implementation {
                BackendImplementation::Postgresql(backend) => {
                    backend.preflight_import(items, mode).await
                }
            }
        })
        .await
    }

    async fn apply_import_strict(
        &self,
        items: Vec<StorageImportPlanItem>,
    ) -> Result<(), StorageError> {
        observe_storage_call(self.backend_name(), "imports", "apply_strict", async {
            match &self.implementation {
                BackendImplementation::Postgresql(backend) => {
                    backend.apply_import_strict(items).await
                }
            }
        })
        .await
    }

    async fn apply_import_best_effort(
        &self,
        items: Vec<StorageImportPlanItem>,
        mode: ImportMode,
    ) -> Result<StorageImportApply, StorageError> {
        observe_storage_call(self.backend_name(), "imports", "apply_best_effort", async {
            match &self.implementation {
                BackendImplementation::Postgresql(backend) => {
                    backend.apply_import_best_effort(items, mode).await
                }
            }
        })
        .await
    }

    async fn record_import_results(
        &self,
        results: Vec<StorageImportResult>,
    ) -> Result<(), StorageError> {
        observe_storage_call(self.backend_name(), "imports", "record_results", async {
            match &self.implementation {
                BackendImplementation::Postgresql(backend) => {
                    backend.record_import_results(results).await
                }
            }
        })
        .await
    }
}

#[async_trait]
impl RestoreStorage for StorageHandle {
    async fn stage_restore(
        &self,
        request: StorageRestoreStageCreate,
    ) -> Result<StorageRestoreJob, StorageError> {
        observe_storage_call(self.backend_name(), "restores", "stage", async {
            match &self.implementation {
                BackendImplementation::Postgresql(backend) => backend.stage_restore(request).await,
            }
        })
        .await
    }

    async fn get_restore_job(&self, job_id: i64) -> Result<StorageRestoreJob, StorageError> {
        observe_storage_call(self.backend_name(), "restores", "get_job", async {
            match &self.implementation {
                BackendImplementation::Postgresql(backend) => backend.get_restore_job(job_id).await,
            }
        })
        .await
    }

    async fn get_restore_status(&self, job_id: i64) -> Result<StorageRestoreStatus, StorageError> {
        observe_storage_call(self.backend_name(), "restores", "get_status", async {
            match &self.implementation {
                BackendImplementation::Postgresql(backend) => {
                    backend.get_restore_status(job_id).await
                }
            }
        })
        .await
    }

    async fn expire_restore_stage(&self, job_id: i64) -> Result<bool, StorageError> {
        observe_storage_call(self.backend_name(), "restores", "expire", async {
            match &self.implementation {
                BackendImplementation::Postgresql(backend) => {
                    backend.expire_restore_stage(job_id).await
                }
            }
        })
        .await
    }

    async fn start_restore_draining(&self, job_id: i64) -> Result<NaiveDateTime, StorageError> {
        observe_storage_call(self.backend_name(), "restores", "start_draining", async {
            match &self.implementation {
                BackendImplementation::Postgresql(backend) => {
                    backend.start_restore_draining(job_id).await
                }
            }
        })
        .await
    }

    async fn apply_restore(
        &self,
        request: StorageRestoreApply,
    ) -> Result<StorageRestoreCompletion, StorageError> {
        observe_storage_call(self.backend_name(), "restores", "apply", async {
            match &self.implementation {
                BackendImplementation::Postgresql(backend) => backend.apply_restore(request).await,
            }
        })
        .await
    }

    async fn fail_restore_and_resume(
        &self,
        request: StorageRestoreFailure,
    ) -> Result<(), StorageError> {
        observe_storage_call(self.backend_name(), "restores", "fail_and_resume", async {
            match &self.implementation {
                BackendImplementation::Postgresql(backend) => {
                    backend.fail_restore_and_resume(request).await
                }
            }
        })
        .await
    }

    async fn restore_coordinator_snapshot(
        &self,
    ) -> Result<StorageRestoreCoordinatorSnapshot, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "restores",
            "coordinator_snapshot",
            async {
                match &self.implementation {
                    BackendImplementation::Postgresql(backend) => {
                        backend.restore_coordinator_snapshot().await
                    }
                }
            },
        )
        .await
    }

    async fn resume_maintenance_without_restore(&self) -> Result<(), StorageError> {
        observe_storage_call(
            self.backend_name(),
            "restores",
            "resume_without_job",
            async {
                match &self.implementation {
                    BackendImplementation::Postgresql(backend) => {
                        backend.resume_maintenance_without_restore().await
                    }
                }
            },
        )
        .await
    }

    async fn resume_terminal_restore(&self, job_id: i64) -> Result<(), StorageError> {
        observe_storage_call(self.backend_name(), "restores", "resume_terminal", async {
            match &self.implementation {
                BackendImplementation::Postgresql(backend) => {
                    backend.resume_terminal_restore(job_id).await
                }
            }
        })
        .await
    }

    async fn tick_restore_coordinator(
        &self,
        instance_id: Uuid,
        local_work_is_idle: &(dyn Fn() -> bool + Send + Sync),
        expire_validated_jobs: bool,
    ) -> Result<StorageRestoreCoordinatorSnapshot, StorageError> {
        observe_storage_call(self.backend_name(), "restores", "tick", async {
            match &self.implementation {
                BackendImplementation::Postgresql(backend) => {
                    backend
                        .tick_restore_coordinator(
                            instance_id,
                            local_work_is_idle,
                            expire_validated_jobs,
                        )
                        .await
                }
            }
        })
        .await
    }

    async fn restore_drain_state(
        &self,
        heartbeat_cutoff: NaiveDateTime,
    ) -> Result<StorageRestoreDrainState, StorageError> {
        observe_storage_call(self.backend_name(), "restores", "drain_state", async {
            match &self.implementation {
                BackendImplementation::Postgresql(backend) => {
                    backend.restore_drain_state(heartbeat_cutoff).await
                }
            }
        })
        .await
    }

    async fn remove_restore_instance(&self, instance_id: Uuid) -> Result<(), StorageError> {
        observe_storage_call(self.backend_name(), "restores", "remove_instance", async {
            match &self.implementation {
                BackendImplementation::Postgresql(backend) => {
                    backend.remove_restore_instance(instance_id).await
                }
            }
        })
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
impl InventoryStorage for StorageHandle {
    async fn inventory_counts(&self) -> Result<StorageInventoryCounts, StorageError> {
        observe_storage_call(self.backend_name(), "inventory", "counts", async {
            match &self.implementation {
                BackendImplementation::Postgresql(backend) => backend.inventory_counts().await,
            }
        })
        .await
    }
}

#[async_trait]
impl ObjectRecordStorage for StorageHandle {
    async fn validate_object(&self, object: &HubuumObject) -> Result<(), StorageError> {
        observe_storage_call(self.backend_name(), "object_records", "validate", async {
            match &self.implementation {
                BackendImplementation::Postgresql(backend) => backend.validate_object(object).await,
            }
        })
        .await
    }

    async fn validate_new_object(&self, object: &NewHubuumObject) -> Result<(), StorageError> {
        observe_storage_call(
            self.backend_name(),
            "object_records",
            "validate_new",
            async {
                match &self.implementation {
                    BackendImplementation::Postgresql(backend) => {
                        backend.validate_new_object(object).await
                    }
                }
            },
        )
        .await
    }

    async fn validate_object_update(
        &self,
        update: &UpdateHubuumObject,
        object_id: i32,
    ) -> Result<(), StorageError> {
        observe_storage_call(
            self.backend_name(),
            "object_records",
            "validate_update",
            async {
                match &self.implementation {
                    BackendImplementation::Postgresql(backend) => {
                        backend.validate_object_update(update, object_id).await
                    }
                }
            },
        )
        .await
    }

    async fn save_object_record(
        &self,
        object: &HubuumObject,
        context: Option<&EventContext>,
    ) -> Result<HubuumObject, StorageError> {
        observe_storage_call(self.backend_name(), "object_records", "save", async {
            match &self.implementation {
                BackendImplementation::Postgresql(backend) => {
                    backend.save_object_record(object, context).await
                }
            }
        })
        .await
    }

    async fn create_object_record(
        &self,
        object: &NewHubuumObject,
        context: Option<&EventContext>,
    ) -> Result<HubuumObject, StorageError> {
        observe_storage_call(self.backend_name(), "object_records", "create", async {
            match &self.implementation {
                BackendImplementation::Postgresql(backend) => {
                    backend.create_object_record(object, context).await
                }
            }
        })
        .await
    }

    async fn update_object_record(
        &self,
        update: &UpdateHubuumObject,
        object_id: i32,
        context: Option<&EventContext>,
    ) -> Result<HubuumObject, StorageError> {
        observe_storage_call(self.backend_name(), "object_records", "update", async {
            match &self.implementation {
                BackendImplementation::Postgresql(backend) => {
                    backend
                        .update_object_record(update, object_id, context)
                        .await
                }
            }
        })
        .await
    }

    async fn delete_object_record(
        &self,
        object: &HubuumObject,
        context: Option<&EventContext>,
    ) -> Result<(), StorageError> {
        observe_storage_call(self.backend_name(), "object_records", "delete", async {
            match &self.implementation {
                BackendImplementation::Postgresql(backend) => {
                    backend.delete_object_record(object, context).await
                }
            }
        })
        .await
    }

    async fn load_object_record(&self, object_id: i32) -> Result<HubuumObject, StorageError> {
        observe_storage_call(self.backend_name(), "object_records", "load", async {
            match &self.implementation {
                BackendImplementation::Postgresql(backend) => {
                    backend.load_object_record(object_id).await
                }
            }
        })
        .await
    }

    async fn object_collection(&self, object_id: i32) -> Result<Collection, StorageError> {
        observe_storage_call(self.backend_name(), "object_records", "collection", async {
            match &self.implementation {
                BackendImplementation::Postgresql(backend) => {
                    backend.object_collection(object_id).await
                }
            }
        })
        .await
    }

    async fn object_class(&self, object_id: i32) -> Result<HubuumClass, StorageError> {
        observe_storage_call(self.backend_name(), "object_records", "class", async {
            match &self.implementation {
                BackendImplementation::Postgresql(backend) => backend.object_class(object_id).await,
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

    async fn storage_snapshot(&self) -> Result<OperationalStorageSnapshot, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "operational_state",
            "storage_snapshot",
            async {
                match &self.implementation {
                    BackendImplementation::Postgresql(backend) => backend.storage_snapshot().await,
                }
            },
        )
        .await
    }

    async fn task_queue_snapshot(&self) -> Result<OperationalTaskQueueSnapshot, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "operational_state",
            "task_queue_snapshot",
            async {
                match &self.implementation {
                    BackendImplementation::Postgresql(backend) => {
                        backend.task_queue_snapshot().await
                    }
                }
            },
        )
        .await
    }

    async fn export_template_health(
        &self,
    ) -> Result<Vec<OperationalExportTemplateHealth>, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "operational_state",
            "export_template_health",
            async {
                match &self.implementation {
                    BackendImplementation::Postgresql(backend) => {
                        backend.export_template_health().await
                    }
                }
            },
        )
        .await
    }

    async fn export_templates_for_audit(
        &self,
    ) -> Result<Vec<OperationalExportTemplateAuditEntry>, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "operational_state",
            "export_templates_for_audit",
            async {
                match &self.implementation {
                    BackendImplementation::Postgresql(backend) => {
                        backend.export_templates_for_audit().await
                    }
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
impl AuditEventStorage for StorageHandle {
    async fn list_audit_events(
        &self,
        query: StorageAuditEventListQuery,
    ) -> Result<StorageEventPage<StorageAuditEvent>, StorageError> {
        observe_storage_call(self.backend_name(), "audit_events", "list", async {
            match &self.implementation {
                BackendImplementation::Postgresql(backend) => {
                    backend.list_audit_events(query).await
                }
            }
        })
        .await
    }
}

#[async_trait]
impl EventSubscriptionStorage for StorageHandle {
    async fn enabled_event_sink_count(&self) -> Result<i64, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "event_subscriptions",
            "count_enabled_sinks",
            async {
                match &self.implementation {
                    BackendImplementation::Postgresql(backend) => {
                        backend.enabled_event_sink_count().await
                    }
                }
            },
        )
        .await
    }

    async fn list_event_sinks(
        &self,
        query: StorageEventSinkListQuery,
    ) -> Result<StorageEventPage<StorageEventSink>, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "event_subscriptions",
            "list_sinks",
            async {
                match &self.implementation {
                    BackendImplementation::Postgresql(backend) => {
                        backend.list_event_sinks(query).await
                    }
                }
            },
        )
        .await
    }

    async fn load_event_sink(&self, sink_id: i32) -> Result<StorageEventSink, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "event_subscriptions",
            "load_sink",
            async {
                match &self.implementation {
                    BackendImplementation::Postgresql(backend) => {
                        backend.load_event_sink(sink_id).await
                    }
                }
            },
        )
        .await
    }

    async fn create_event_sink(
        &self,
        request: StorageEventSinkCreate,
    ) -> Result<StorageEventSink, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "event_subscriptions",
            "create_sink",
            async {
                match &self.implementation {
                    BackendImplementation::Postgresql(backend) => {
                        backend.create_event_sink(request).await
                    }
                }
            },
        )
        .await
    }

    async fn update_event_sink(
        &self,
        request: StorageEventSinkUpdate,
    ) -> Result<StorageEventSink, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "event_subscriptions",
            "update_sink",
            async {
                match &self.implementation {
                    BackendImplementation::Postgresql(backend) => {
                        backend.update_event_sink(request).await
                    }
                }
            },
        )
        .await
    }

    async fn delete_event_sink(&self, request: StorageEventSinkDelete) -> Result<(), StorageError> {
        observe_storage_call(
            self.backend_name(),
            "event_subscriptions",
            "delete_sink",
            async {
                match &self.implementation {
                    BackendImplementation::Postgresql(backend) => {
                        backend.delete_event_sink(request).await
                    }
                }
            },
        )
        .await
    }

    async fn list_event_subscriptions(
        &self,
        query: StorageEventSubscriptionListQuery,
    ) -> Result<StorageEventPage<StorageEventSubscription>, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "event_subscriptions",
            "list_subscriptions",
            async {
                match &self.implementation {
                    BackendImplementation::Postgresql(backend) => {
                        backend.list_event_subscriptions(query).await
                    }
                }
            },
        )
        .await
    }

    async fn load_event_subscription(
        &self,
        collection_id: i32,
        subscription_id: i32,
    ) -> Result<StorageEventSubscription, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "event_subscriptions",
            "load_subscription",
            async {
                match &self.implementation {
                    BackendImplementation::Postgresql(backend) => {
                        backend
                            .load_event_subscription(collection_id, subscription_id)
                            .await
                    }
                }
            },
        )
        .await
    }

    async fn create_event_subscription(
        &self,
        request: StorageEventSubscriptionCreate,
    ) -> Result<StorageEventSubscription, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "event_subscriptions",
            "create_subscription",
            async {
                match &self.implementation {
                    BackendImplementation::Postgresql(backend) => {
                        backend.create_event_subscription(request).await
                    }
                }
            },
        )
        .await
    }

    async fn update_event_subscription(
        &self,
        request: StorageEventSubscriptionUpdate,
    ) -> Result<StorageEventSubscription, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "event_subscriptions",
            "update_subscription",
            async {
                match &self.implementation {
                    BackendImplementation::Postgresql(backend) => {
                        backend.update_event_subscription(request).await
                    }
                }
            },
        )
        .await
    }

    async fn delete_event_subscription(
        &self,
        request: StorageEventSubscriptionDelete,
    ) -> Result<(), StorageError> {
        observe_storage_call(
            self.backend_name(),
            "event_subscriptions",
            "delete_subscription",
            async {
                match &self.implementation {
                    BackendImplementation::Postgresql(backend) => {
                        backend.delete_event_subscription(request).await
                    }
                }
            },
        )
        .await
    }
}

#[async_trait]
impl EventDeliveryAdministrationStorage for StorageHandle {
    async fn list_event_deliveries(
        &self,
        query: StorageEventDeliveryListQuery,
    ) -> Result<StorageEventPage<StorageEventDelivery>, StorageError> {
        observe_storage_call(self.backend_name(), "event_delivery", "list", async {
            match &self.implementation {
                BackendImplementation::Postgresql(backend) => {
                    backend.list_event_deliveries(query).await
                }
            }
        })
        .await
    }

    async fn load_event_delivery(
        &self,
        delivery_id: i64,
    ) -> Result<StorageEventDelivery, StorageError> {
        observe_storage_call(self.backend_name(), "event_delivery", "load", async {
            match &self.implementation {
                BackendImplementation::Postgresql(backend) => {
                    backend.load_event_delivery(delivery_id).await
                }
            }
        })
        .await
    }

    async fn release_event_delivery_for_retry(
        &self,
        delivery_id: i64,
    ) -> Result<StorageEventDelivery, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "event_delivery",
            "release_for_retry",
            async {
                match &self.implementation {
                    BackendImplementation::Postgresql(backend) => {
                        backend.release_event_delivery_for_retry(delivery_id).await
                    }
                }
            },
        )
        .await
    }

    async fn mark_event_delivery_dead(
        &self,
        delivery_id: i64,
    ) -> Result<StorageEventDelivery, StorageError> {
        observe_storage_call(self.backend_name(), "event_delivery", "mark_dead", async {
            match &self.implementation {
                BackendImplementation::Postgresql(backend) => {
                    backend.mark_event_delivery_dead(delivery_id).await
                }
            }
        })
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
