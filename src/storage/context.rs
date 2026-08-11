use actix_web::web::Data;
use chrono::NaiveDateTime;
use uuid::Uuid;

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
    AuthorizationPrincipal, AuthorizationStorage, BackupSnapshotStorage,
    BidirectionalRelatedObjectsQuery, CatalogListQuery, CatalogPage, CatalogStorage,
    ComputedFieldLifecycleStorage, ComputedObjectEnrichmentQuery, ComputedObjectListQuery,
    ComputedObjectPage, ComputedObjectStorage, DynLifecycleStorage, EventArchive,
    EventDeliveryBatch, EventDeliveryClaim, EventDeliveryHealthSnapshot, EventDeliveryStorage,
    EventFanoutStorage, EventHealthStorage, EventMetricsSnapshot, EventRetentionStorage,
    EventRetentionSummary, ExportTemplateHistoryRecord, HistoryAsOfQuery, HistoryListQuery,
    HistoryPage, HistoryPrincipalName, HistoryStorage, InventoryGaugeSnapshot, MetricsStorage,
    ObjectAggregateAuthorizer, ObjectAggregateStorage, ObjectAggregateStorageQuery,
    ObjectHistoryAsOfQuery, ObjectHistoryListQuery, ObjectHistoryRecord,
    ObjectRelationsTouchingIdsQuery, OperationalStateStorage, PostgresStorage, ReadinessSnapshot,
    RelatedObjectsForRootsQuery, RelationGraphQuery, RelationIdsQuery, RelationListQuery,
    RelationPage, RelationQueryStorage, RelationTouchingQuery, RemoteTargetHistoryRecord,
    RemoteTargetStorage, RestoreStorage, StorageBackend, StorageBackendDescriptor,
    StorageBackupOutput, StorageBackupOutputSummary, StorageBackupSnapshot, StorageClass,
    StorageClassComputationState, StorageClassGraphRow, StorageClassRelation, StorageCollection,
    StorageComputedFieldDefinition, StorageComputedFieldMutation, StorageComputedFieldPage,
    StorageComputedFieldRebuildRequest, StorageComputedObject, StorageError, StorageExportOutput,
    StorageExportOutputSummary, StorageImportTaskResultPage, StorageObject,
    StorageObjectAggregatePage, StorageObjectGraphRow, StorageObjectRelation,
    StoragePersonalComputedFieldCreate, StoragePersonalComputedFieldDelete,
    StoragePersonalComputedFieldListQuery, StoragePersonalComputedFieldUpdate, StoragePoolState,
    StorageRelatedObjectForRootRow, StorageRelatedObjectIncludeRow, StorageRemoteTarget,
    StorageRemoteTargetCreate, StorageRemoteTargetDelete, StorageRemoteTargetInvocation,
    StorageRemoteTargetListQuery, StorageRemoteTargetPage, StorageRemoteTargetUpdate,
    StorageRestoreApply, StorageRestoreCompletion, StorageRestoreCoordinatorSnapshot,
    StorageRestoreDrainState, StorageRestoreFailure, StorageRestoreJob, StorageRestoreStageCreate,
    StorageRestoreStatus, StorageSharedComputedFieldCreate, StorageSharedComputedFieldDelete,
    StorageSharedComputedFieldUpdate, StorageTask, StorageTaskAccess, StorageTaskClaim,
    StorageTaskCompletion, StorageTaskCreateRequest, StorageTaskEventAppend, StorageTaskEventPage,
    StorageTaskFailure, StorageTaskLease, StorageTaskLeaseDuration, StorageTaskListQuery,
    StorageTaskOutputLookup, StorageTaskPage, StorageTaskPageQuery, StorageTaskStateUpdate,
    TaskExecutionStorage, TaskGaugeSnapshot, TaskQueueStorage, TokenRetentionStorage,
    UnifiedSearchClass, UnifiedSearchCollection, UnifiedSearchObject, UnifiedSearchQuery,
    UnifiedSearchStorage,
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
