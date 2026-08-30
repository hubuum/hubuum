use std::collections::HashSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use actix_web::{App, http, test, web::Data};
use async_trait::async_trait;
use hubuum_domain::{
    ClassId, ClassRelationId, CollectionId, ComputedFieldDefinitionId, EventDeliveryStatus,
    GroupId, ObjectId, ObjectRelationId, PrincipalId, ResourceId, ResourceRevision,
};
use hubuum_task_core::IdempotencyKey;
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};

use futures::FutureExt;
use futures::future::BoxFuture;
use hubuum_storage_conformance::{
    ApplicationCompatibilityExpectations, ApplicationCompatibilityFixture,
    ApplicationCompatibilityProbe, BackendAuditFixture, CommittedMutationProbe, ContractReport,
    DeliveryFaultFixture, DeliveryFaultProbe, DeliveryRecoveryProbe, FanoutProbe, FixtureError,
    LeaseLossFaultFixture, LeaseLossFaultProbe, ObservationProbe, RecordingStorageObserver,
    RestoreCoordinationFaultFixture, RestoreCoordinationFaultProbe, RevisionConflictProbe,
    RollbackProbe, TransactionFaultProbe, UnchangedMutationProbe, verify_application_compatibility,
    verify_backend_audit_contract, verify_delivery_fault_contract,
    verify_lease_loss_fault_contract, verify_restore_coordination_fault_contract,
};
use hubuum_storage_core::StorageTaskClaimToken;
use hubuum_storage_postgres::{
    PostgresFaultController, PostgresFaultPoint, PostgresObserver,
    PostgresStorage as AdapterPostgresStorage,
};

fn principal_id(id: i32) -> PrincipalId {
    PrincipalId::new(id).expect("test principal id must be positive")
}

fn collection_id(id: i32) -> CollectionId {
    CollectionId::new(id).expect("test collection id must be positive")
}

fn group_id(id: i32) -> GroupId {
    GroupId::new(id).expect("test group id must be positive")
}

use crate::events::{
    Action, EntityType, EventContext, EventEntityId, EventFanoutSettings, EventRetentionSettings,
    MutationProvenance,
};
use crate::events::{EventDeliverySettings, EventEnvelope, Sink, SinkError, SinkResolver};
use crate::models::TokenRetentionSettings;
use crate::models::identity::LOCAL_IDENTITY_SCOPE;
use crate::models::search::{
    FilterField, ParsedQueryParam, QueryOptions, SearchOperator,
    parse_query_parameter_with_computed_filters_and_passthrough,
};
use crate::models::{
    CollectionHistory, CollectionID, CollectionKey, ExportTemplateHistory, HubuumClassHistory,
    HubuumObjectHistory, ImportAtomicity, ImportClassInput, ImportCollectionInput, ImportMode,
    NewHubuumClass, NewHubuumClassRelation, NewHubuumObject, NewHubuumObjectRelation,
    RemoteTargetHistory,
};
use crate::pagination::prepare_db_pagination;
use crate::permissions::{AppContext, LocalPermissionBackend};
use crate::services::Services;
use crate::storage::StorageHandle;
use crate::storage::{
    ApplicationImportOperation, AuditEventStorage, AuthenticationStorage, AuthorizationDataStorage,
    BackupSnapshotStorage, CatalogStorage, CollectionAuthorizationQueryStorage,
    ComputedFieldStorage, ComputedObjectStorage, EventArchiveSink, EventConfigurationStorage,
    EventDeliveryAdministrationStorage, EventDeliveryWorkerStorage, EventFanoutStorage,
    EventHealthStorage, ExecutionStorage, ExportTemplateStorage, ExternalIdentityStorage,
    GroupMembershipStorage, GroupStorage, HistoryStorage, IdentityScopeStorage, ImportStorage,
    InventoryStorage, LocalIdentityCredentialStorage, MetricsStorage, ObjectAggregateAuthorizer,
    ObjectAggregateStorage, OperationalStateStorage, PrincipalStorage, RelationQueryStorage,
    RemoteTargetStorage, RestoreStorage, ServiceAccountStorage, StorageAuditEvent,
    StorageAuditEventFilters, StorageAuditEventListQuery, StorageAuthenticationAttempt,
    StorageAuthenticationCredential, StorageAuthenticationTokenScopeQuery,
    StorageAuthorizationCollectionAccessQuery, StorageAuthorizationCollectionCandidateQuery,
    StorageAuthorizationCollectionGrantListQuery, StorageAuthorizationCollectionGroupsPageQuery,
    StorageAuthorizationCollectionGroupsQuery, StorageAuthorizationCollectionVisibilityQuery,
    StorageAuthorizationCollectionsAccessQuery, StorageAuthorizationCollectionsQuery,
    StorageAuthorizationGrantDelete, StorageAuthorizationGrantKey,
    StorageAuthorizationGrantMutation, StorageAuthorizationGroupCandidateQuery,
    StorageAuthorizationGroupCollectionQuery, StorageAuthorizationGroupMembershipQuery,
    StorageAuthorizationPermission, StorageAuthorizationPermissionSetQuery,
    StorageAuthorizationPrincipalCollectionPageQuery, StorageAuthorizationPrincipalCollectionQuery,
    StorageAuthorizationResourceIds, StorageBackendKind, StorageBackupTaskArtifact,
    StorageBidirectionalRelatedObjectsQuery, StorageCallSite, StorageCandidatePageLimit,
    StorageCatalogListQuery, StorageClassCreate, StorageClassRelationCreate, StorageClassSelector,
    StorageClassUpdate, StorageCollectionCreate, StorageCollectionUpdate,
    StorageComputedFieldDefinitionInput, StorageComputedFieldDefinitionPatch,
    StorageComputedFieldRebuildRequest, StorageComputedFieldVisibility,
    StorageComputedObjectEnrichmentQuery, StorageComputedObjectListQuery,
    StorageComputedObjectProjection, StorageComputedObjectQueryOptions,
    StorageComputedObjectVisibility, StorageDefaultAdminBootstrap, StorageError, StorageErrorKind,
    StorageEventDeliveryListQuery, StorageEventRetentionBatch, StorageEventSinkCreate,
    StorageEventSinkDelete, StorageEventSinkListQuery, StorageEventSinkUpdate,
    StorageEventSubscriptionCreate, StorageEventSubscriptionDelete,
    StorageEventSubscriptionListQuery, StorageEventSubscriptionUpdate, StorageExecutionScope,
    StorageExportTaskArtifact, StorageExportTemplateCreate, StorageExportTemplateDefinition,
    StorageExportTemplateDelete, StorageExportTemplateListQuery, StorageExportTemplateReplace,
    StorageGroupCreate, StorageGroupListQuery, StorageGroupUpdate, StorageHistoryAsOfQuery,
    StorageHistoryCollectionScope, StorageHistoryListQuery, StorageImportPlan,
    StorageImportPlanItem, StorageImportResult, StorageLocalPasswordReset, StorageObject,
    StorageObjectAggregateAuthorization, StorageObjectAggregateAuthorizationCandidate,
    StorageObjectAggregateAuthorizationTarget, StorageObjectAggregateQuery,
    StorageObjectAggregateSort, StorageObjectAggregateSpec, StorageObjectAggregateTarget,
    StorageObjectCreate, StorageObjectDataPatch, StorageObjectHistoryAsOfQuery,
    StorageObjectHistoryListQuery, StorageObjectRelationCreate,
    StorageObjectRelationCreateSelector, StorageObjectRelationSelector,
    StorageObjectRelationsTouchingIdsQuery, StorageObjectSelector, StorageObjectUpdate,
    StoragePersonalComputedFieldCreate, StoragePersonalComputedFieldDelete,
    StoragePersonalComputedFieldListQuery, StoragePersonalComputedFieldUpdate,
    StoragePrincipalGroupListQuery, StoragePrincipalSettingsMutation, StoragePrincipalTokensRevoke,
    StorageQueryBudget, StorageRecordMetadata, StorageRelatedDirection,
    StorageRelatedObjectsForRootsQuery, StorageRelatedSort, StorageRelationGraphQuery,
    StorageRelationIdsQuery, StorageRelationListQuery, StorageRelationTouchingQuery,
    StorageRemoteCallArtifactOutcome, StorageRemoteCallArtifactResponse,
    StorageRemoteCallArtifactTarget, StorageRemoteCallTaskArtifact, StorageRemoteTargetCreate,
    StorageRemoteTargetDefinition, StorageRemoteTargetDelete, StorageRemoteTargetInvocation,
    StorageRemoteTargetListQuery, StorageRemoteTargetPatch, StorageRemoteTargetPolicy,
    StorageRemoteTargetTransport, StorageRemoteTargetUpdate, StorageRestoreArtifactSummary,
    StorageRestoreFailure, StorageRestoreInitiator, StorageRestoreJobStatus,
    StorageRestoreStageCreate, StorageRevisionPrecondition, StorageRevisionTarget,
    StorageServiceAccountCreate, StorageServiceAccountListQuery, StorageServiceAccountMutation,
    StorageServiceAccountUpdate, StorageSharedComputedFieldCreate,
    StorageSharedComputedFieldDelete, StorageSharedComputedFieldUpdate, StorageTaskActiveUpdate,
    StorageTaskChildListQuery, StorageTaskCompletion, StorageTaskCompletionArtifact,
    StorageTaskCreateRequest, StorageTaskEventAppend, StorageTaskEventInput, StorageTaskFailure,
    StorageTaskKind, StorageTaskLease, StorageTaskLeaseDuration, StorageTaskListQuery,
    StorageTaskOutputLookup, StorageTaskResultCounts, StorageTaskScopeSnapshot, StorageTaskStatus,
    StorageTaskTerminalUpdate, StorageTokenCreate, StorageTokenHashRevoke,
    StorageTokenIssuancePolicy, StorageTokenListQuery, StorageTokenListState,
    StorageTokenObservation, StorageTokenRenew, StorageTokenRevoke, StorageUnifiedSearchQuery,
    StorageUserAnonymize, StorageUserCreate, StorageUserDelete, StorageUserListQuery,
    StorageUserPasswordUpdate, StorageUserUpdate, StorageVisibility, TaskExecutionStorage,
    TaskQueueStorage, TokenRetentionStorage, TokenStorage, UnifiedSearchStorage, UserStorage,
};
use crate::traits::{CanDelete, CanSave};
use hubuum_storage_postgres::PostgresPool;

#[derive(Clone, Copy, Debug)]
pub(crate) enum LifecycleContractImplementation {
    MemoryModel,
    PostgresAdapter,
}

struct AllowAllObjectAggregateAuthorizer;

#[async_trait]
impl ObjectAggregateAuthorizer for AllowAllObjectAggregateAuthorizer {
    async fn authorize_target(
        &self,
        _target: StorageObjectAggregateAuthorizationTarget,
        _required_permissions: Vec<StorageAuthorizationPermission>,
    ) -> Result<bool, StorageError> {
        Ok(true)
    }

    async fn authorize_objects(
        &self,
        candidates: Vec<StorageObjectAggregateAuthorizationCandidate>,
        _required_permissions: Vec<StorageAuthorizationPermission>,
    ) -> Result<Vec<bool>, StorageError> {
        Ok(vec![true; candidates.len()])
    }
}

struct PausingObjectAggregateAuthorizer {
    authorization_calls: AtomicUsize,
    first_batch_seen: Notify,
    resume: Notify,
}

impl PausingObjectAggregateAuthorizer {
    fn new() -> Self {
        Self {
            authorization_calls: AtomicUsize::new(0),
            first_batch_seen: Notify::new(),
            resume: Notify::new(),
        }
    }
}

#[async_trait]
impl ObjectAggregateAuthorizer for PausingObjectAggregateAuthorizer {
    async fn authorize_target(
        &self,
        _target: StorageObjectAggregateAuthorizationTarget,
        _required_permissions: Vec<StorageAuthorizationPermission>,
    ) -> Result<bool, StorageError> {
        Ok(true)
    }

    async fn authorize_objects(
        &self,
        candidates: Vec<StorageObjectAggregateAuthorizationCandidate>,
        _required_permissions: Vec<StorageAuthorizationPermission>,
    ) -> Result<Vec<bool>, StorageError> {
        if self.authorization_calls.fetch_add(1, Ordering::SeqCst) == 0 {
            self.first_batch_seen.notify_one();
            self.resume.notified().await;
        }
        Ok(vec![true; candidates.len()])
    }
}

/// Backend-owned resources needed by the reusable application compatibility
/// harness. Concrete resources stay inside the matching adapter branch.
#[derive(Clone)]
enum BackendTestEnvironment {
    Postgres { pool: PostgresPool },
}

impl BackendTestEnvironment {
    fn kind(&self) -> StorageBackendKind {
        match self {
            Self::Postgres { .. } => StorageBackendKind::Postgres,
        }
    }

    fn storage(&self) -> StorageHandle {
        match self {
            Self::Postgres { pool } => StorageHandle::postgres(pool.clone()),
        }
    }
}

/// Construct the test environment for every adapter advertised by application
/// composition. Registering an adapter here enrolls it in all generic storage,
/// service, and HTTP compatibility tests below.
fn available_backend_environments() -> impl Iterator<Item = BackendTestEnvironment> {
    let postgres_pool = pool();
    StorageBackendKind::ALL.into_iter().map(move |kind| {
        let environment = match kind {
            StorageBackendKind::Postgres => BackendTestEnvironment::Postgres {
                pool: postgres_pool.get_ref().clone(),
            },
        };
        assert_eq!(environment.kind(), kind);
        environment
    })
}

fn available_backends() -> impl Iterator<Item = StorageHandle> {
    available_backend_environments().map(|environment| {
        let expected_kind = environment.kind();
        let backend = environment.storage();
        assert_eq!(backend.descriptor().kind(), expected_kind);
        backend
    })
}

#[derive(Debug, Default)]
struct RecordingPostgresObserver {
    operations: AtomicUsize,
    failures: AtomicUsize,
}

impl RecordingPostgresObserver {
    fn operation_count(&self) -> usize {
        AtomicUsize::load(&self.operations, Ordering::Relaxed)
    }

    fn failure_count(&self) -> usize {
        AtomicUsize::load(&self.failures, Ordering::Relaxed)
    }
}

impl PostgresObserver for RecordingPostgresObserver {
    fn operation_finished(
        &self,
        _call_site: StorageCallSite,
        _operation: &'static str,
        _duration: Duration,
        error: Option<StorageErrorKind>,
    ) {
        self.operations.fetch_add(1, Ordering::Relaxed);
        if error.is_some() {
            self.failures.fetch_add(1, Ordering::Relaxed);
        }
    }
}

struct ContractRecordingSink {
    deliveries: Arc<AtomicUsize>,
}

impl Sink for ContractRecordingSink {
    fn deliver<'a>(
        &'a self,
        _envelope: &'a EventEnvelope,
        _subscription: &'a crate::storage::StorageEventDeliverySubscription,
        _sink: &'a crate::storage::StorageEventDeliverySink,
    ) -> BoxFuture<'a, Result<(), SinkError>> {
        async move {
            self.deliveries.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
        .boxed()
    }
}

struct ContractDiscardSink;

impl Sink for ContractDiscardSink {
    fn deliver<'a>(
        &'a self,
        _envelope: &'a EventEnvelope,
        _subscription: &'a crate::storage::StorageEventDeliverySubscription,
        _sink: &'a crate::storage::StorageEventDeliverySink,
    ) -> BoxFuture<'a, Result<(), SinkError>> {
        async { Ok(()) }.boxed()
    }
}

struct ContractSinkResolver {
    recording: ContractRecordingSink,
    discard: ContractDiscardSink,
}

impl SinkResolver for ContractSinkResolver {
    fn resolve(&self, kind: &str) -> Option<&dyn Sink> {
        if kind == "webhook" {
            Some(&self.recording)
        } else {
            Some(&self.discard)
        }
    }
}

struct PostgresAuditContractFixture {
    backend: StorageHandle,
    pool: PostgresPool,
    group_id: GroupId,
    sink_id: hubuum_domain::EventSinkId,
    subscription_id: hubuum_domain::EventSubscriptionId,
    collection_id: i32,
    logical_observer: Arc<RecordingStorageObserver>,
    postgres_observer: Arc<RecordingPostgresObserver>,
    sink_deliveries: Arc<AtomicUsize>,
}

impl PostgresAuditContractFixture {
    async fn new(pool: PostgresPool) -> Result<Self, FixtureError> {
        let postgres_observer = Arc::new(RecordingPostgresObserver::default());
        let logical_observer = Arc::new(RecordingStorageObserver::default());
        let adapter = AdapterPostgresStorage::new(pool.clone(), postgres_observer.clone());
        let backend = StorageHandle::from_postgres_backend_with_storage_observer(
            adapter,
            logical_observer.clone(),
        );
        let context = EventContext::system();
        let group = backend
            .create_group(
                StorageGroupCreate::new(
                    None,
                    prefix("audit_contract_group"),
                    Some("audit contract owner".to_string()),
                ),
                &context,
            )
            .await?
            .into_value();
        let sink = backend
            .create_event_sink(
                StorageEventSinkCreate::builder(
                    prefix("audit_contract_sink"),
                    "webhook",
                    context.clone(),
                )
                .configuration(serde_json::json!({}))
                .enabled(true)
                .try_build()
                .unwrap(),
            )
            .await?
            .into_value();
        let collection = backend
            .collection_store()
            .create_collection(
                StorageCollectionCreate::new(
                    prefix("audit_contract_collection"),
                    "audit contract collection before committed probe",
                    group.id(),
                    None,
                ),
                &context,
            )
            .await?
            .into_value();
        let subscription = backend
            .create_event_subscription(
                StorageEventSubscriptionCreate::builder(
                    collection.id(),
                    sink.id(),
                    prefix("audit_contract_subscription"),
                    context,
                )
                .entity_types(vec![EntityType::Collection])
                .actions(vec![Action::Updated])
                .routing(serde_json::json!({}))
                .enabled(true)
                .try_build()
                .unwrap(),
            )
            .await?
            .into_value();
        Ok(Self {
            backend,
            pool,
            group_id: group.id(),
            sink_id: sink.id(),
            subscription_id: subscription.id(),
            collection_id: collection.id().id(),
            logical_observer,
            postgres_observer,
            sink_deliveries: Arc::new(AtomicUsize::new(0)),
        })
    }

    fn query_options() -> QueryOptions {
        QueryOptions::new(Vec::new(), Vec::new(), Some(100), None, true)
            .expect("audit contract query options must be valid")
    }

    async fn collection_events(
        &self,
        collection_id: i32,
    ) -> Result<Vec<StorageAuditEvent>, FixtureError> {
        let (events, _) = self
            .backend
            .list_audit_events(StorageAuditEventListQuery::new(
                vec![self::collection_id(collection_id)],
                false,
                StorageAuditEventFilters::new()
                    .entity_type(Some(EntityType::Collection))
                    .entity_id(Some(
                        hubuum_events_core::EventEntityId::new(collection_id).unwrap(),
                    )),
                Self::query_options(),
            ))
            .await?
            .into_parts();
        Ok(events)
    }

    async fn collection_event_count_by_name(
        &self,
        collection_name: &str,
    ) -> Result<i64, FixtureError> {
        Ok(
            hubuum_storage_postgres::test_support::count_collection_events_by_name(
                &self.pool,
                collection_name,
            )
            .await?,
        )
    }

    async fn cleanup_resources(&self) -> Result<(), FixtureError> {
        let context = EventContext::system();
        self.backend
            .delete_event_subscription(StorageEventSubscriptionDelete::new(
                collection_id(self.collection_id),
                self.subscription_id,
                context.clone(),
            ))
            .await
            .map_err(|error| std::io::Error::other(error.to_string()))?
            .into_value();
        self.backend
            .collection_store()
            .delete_collection(collection_id(self.collection_id), &context)
            .await?
            .into_value();
        self.backend
            .delete_event_sink(StorageEventSinkDelete::new(self.sink_id, context.clone()))
            .await?
            .into_value();
        let _ = self
            .backend
            .delete_group(self.group_id, &context)
            .await?
            .into_value();
        Ok(())
    }
}

#[async_trait]
impl BackendAuditFixture for PostgresAuditContractFixture {
    async fn committed_mutation(&self) -> Result<CommittedMutationProbe, FixtureError> {
        let before = self
            .backend
            .collection_store()
            .get_collection(collection_id(self.collection_id))
            .await?;
        let outcome = self
            .backend
            .collection_store()
            .update_collection(
                collection_id(self.collection_id),
                StorageCollectionUpdate::new(
                    None,
                    Some("audited committed probe update".to_string()),
                ),
                &EventContext::system(),
            )
            .await?;
        let expected_document = hubuum_events_core::AuditDocument::try_new(
            format!("Collection '{}' updated", outcome.value().name()),
            Some(before.audit_snapshot()),
            Some(outcome.value().audit_snapshot()),
            serde_json::json!({}),
        )?;
        let receipt = outcome
            .audits()
            .map(hubuum_storage_core::StorageAuditReceipts::first)
            .ok_or_else(|| std::io::Error::other("committed update had no receipt"))?;
        let event = self
            .collection_events(self.collection_id)
            .await?
            .into_iter()
            .find(|event| event.clone().into_parts().0.id() == receipt.sequence())
            .ok_or_else(|| std::io::Error::other("receipt event was not queryable"))?;
        Ok(CommittedMutationProbe::new(
            outcome.map(drop),
            event,
            expected_document,
        ))
    }

    async fn unchanged_mutation(&self) -> Result<UnchangedMutationProbe, FixtureError> {
        let id = self.collection_id;
        let before = self.collection_events(id).await?.len();
        let outcome = self
            .backend
            .collection_store()
            .update_collection(
                collection_id(id),
                StorageCollectionUpdate::new(None, None),
                &EventContext::system(),
            )
            .await?;
        let after = self.collection_events(id).await?.len();
        Ok(UnchangedMutationProbe::new(
            outcome.map(drop),
            after.saturating_sub(before),
        ))
    }

    async fn rolled_back_mutation(&self) -> Result<RollbackProbe, FixtureError> {
        let name = prefix("audit_contract_rollback");
        let event_count_before = self.collection_event_count_by_name(&name).await?;
        let result =
            PostgresFaultController::failing(PostgresFaultPoint::CollectionCreateAfterRecords)
                .run(self.backend.collection_store().create_collection(
                    StorageCollectionCreate::new(
                        name.clone(),
                        "must roll back",
                        self.group_id,
                        None,
                    ),
                    &EventContext::system(),
                ))
                .await;
        if result.is_ok() {
            return Err(std::io::Error::other("rollback failpoint did not fail").into());
        }
        let state_count =
            hubuum_storage_postgres::test_support::count_collections_by_name(&self.pool, &name)
                .await?;
        let event_count_after = self.collection_event_count_by_name(&name).await?;
        Ok(RollbackProbe::new(
            state_count != 0,
            event_count_after != event_count_before,
        ))
    }

    async fn fanout_to_recording_sink(&self) -> Result<FanoutProbe, FixtureError> {
        let subscription_id = self.subscription_id;
        let fanout = EventFanoutSettings::new(1_000, 30_000)
            .map_err(|error| std::io::Error::other(error.to_string()))?;
        for _ in 0..10 {
            self.backend.process_event_fanout_batch(fanout).await?;
            let (deliveries, _) = self
                .backend
                .list_event_deliveries(
                    StorageEventDeliveryListQuery::new(Self::query_options())
                        .subscription_id(Some(subscription_id)),
                )
                .await?
                .into_parts();
            if !deliveries.is_empty() {
                let durable_delivery_count = deliveries.len();
                let settings = EventDeliverySettings::builder()
                    .batch_size(1_000)
                    .lock_timeout_ms(30_000)
                    .transport_timeout_ms(25_000)
                    .retry_backoff_base_ms(1_000)
                    .retry_backoff_max_ms(10_000)
                    .max_attempts(3)
                    .build()
                    .map_err(|error| std::io::Error::other(error.to_string()))?;
                let resolver = ContractSinkResolver {
                    recording: ContractRecordingSink {
                        deliveries: self.sink_deliveries.clone(),
                    },
                    discard: ContractDiscardSink,
                };
                for delivery in deliveries {
                    let item = hubuum_storage_postgres::test_support::claim_event_delivery_by_id(
                        &self.pool,
                        delivery.id(),
                        settings,
                    )
                    .await?;
                    crate::events::process_event_delivery_work_item(
                        &self.backend,
                        settings,
                        &resolver,
                        item,
                    )
                    .await
                    .map_err(|error| std::io::Error::other(error.to_string()))?;
                }
                return Ok(FanoutProbe::new(
                    durable_delivery_count,
                    AtomicUsize::load(self.sink_deliveries.as_ref(), Ordering::Relaxed),
                ));
            }
            tokio::task::yield_now().await;
        }
        Ok(FanoutProbe::new(0, 0))
    }

    async fn observations(&self) -> Result<ObservationProbe, FixtureError> {
        Ok(ObservationProbe::new(
            self.logical_observer
                .operation_count("collection", "create_collection"),
            self.logical_observer
                .operation_count("event_configuration", "create_event_subscription"),
            self.postgres_observer.operation_count(),
            self.postgres_observer.failure_count(),
        ))
    }

    async fn revision_conflict(&self) -> Result<RevisionConflictProbe, FixtureError> {
        let id = self.collection_id;
        let collection = self
            .backend
            .collection_store()
            .get_collection(collection_id(id))
            .await?;
        let current_revision = collection.revision();
        let stale_revision = current_revision
            .checked_advance()
            .map_err(|error| std::io::Error::other(error.to_string()))?;
        let error = self
            .backend
            .run_in_scope_send(
                StorageExecutionScope::default().with_revision_precondition(Some(
                    StorageRevisionPrecondition::new(
                        StorageRevisionTarget::Collection(collection_id(id)),
                        vec![stale_revision],
                    ),
                )),
                self.backend.collection_store().update_collection(
                    collection_id(id),
                    StorageCollectionUpdate::new(
                        None,
                        Some("stale conformance update must not persist".to_string()),
                    ),
                    &EventContext::system(),
                ),
            )
            .await
            .expect_err("stale storage mutation must fail");
        Ok(RevisionConflictProbe::new(error, current_revision))
    }

    async fn cleanup(&self) -> Result<(), FixtureError> {
        self.cleanup_resources().await
    }
}

#[async_trait]
impl DeliveryFaultFixture for PostgresAuditContractFixture {
    async fn delivery_fault_probe(&self) -> Result<DeliveryFaultProbe, FixtureError> {
        self.committed_mutation().await?;
        let subscription_id = self.subscription_id;
        let fanout = EventFanoutSettings::new(1_000, 30_000)
            .map_err(|error| std::io::Error::other(error.to_string()))?;
        let mut delivery_id = None;
        for _ in 0..10 {
            self.backend.process_event_fanout_batch(fanout).await?;
            let (deliveries, _) = self
                .backend
                .list_event_deliveries(
                    StorageEventDeliveryListQuery::new(Self::query_options())
                        .subscription_id(Some(subscription_id)),
                )
                .await?
                .into_parts();
            if let Some(delivery) = deliveries.into_iter().next() {
                delivery_id = Some(delivery.id());
                break;
            }
            tokio::task::yield_now().await;
        }
        let delivery_id = delivery_id
            .ok_or_else(|| std::io::Error::other("delivery fault event produced no delivery"))?;
        let settings = EventDeliverySettings::builder()
            .batch_size(1)
            .lock_timeout_ms(30_000)
            .transport_timeout_ms(25_000)
            .retry_backoff_base_ms(1_000)
            .retry_backoff_max_ms(10_000)
            .max_attempts(3)
            .build()
            .map_err(|error| std::io::Error::other(error.to_string()))?;
        let claim_error =
            PostgresFaultController::failing(PostgresFaultPoint::EventDeliveryAfterClaim)
                .run(
                    hubuum_storage_postgres::test_support::claim_event_delivery_by_id(
                        &self.pool,
                        delivery_id,
                        settings,
                    ),
                )
                .await
                .err()
                .ok_or_else(|| std::io::Error::other("delivery claim failpoint did not fail"))?;

        let work_item = hubuum_storage_postgres::test_support::claim_event_delivery_by_id(
            &self.pool,
            delivery_id,
            settings,
        )
        .await?;
        let (claim, _, _, _) = work_item.into_parts();
        let acknowledgement_error =
            PostgresFaultController::failing(PostgresFaultPoint::EventDeliveryBeforeAcknowledge)
                .run(self.backend.mark_event_delivery_succeeded(&claim))
                .await
                .err()
                .ok_or_else(|| {
                    std::io::Error::other("delivery acknowledgement failpoint did not fail")
                })?;

        self.backend
            .mark_event_delivery_failed(&claim, settings, "deterministic transport failure")
            .await?;
        let failed = self.backend.get_event_delivery(delivery_id).await?;
        let failure_persisted = failed.status() == EventDeliveryStatus::Failed
            && failed.attempts() == 1
            && failed.last_error() == Some("deterministic transport failure");

        hubuum_storage_postgres::test_support::make_event_delivery_due(&self.pool, delivery_id)
            .await?;
        let retry = hubuum_storage_postgres::test_support::claim_event_delivery_by_id(
            &self.pool,
            delivery_id,
            settings,
        )
        .await?;
        let (retry_claim, _, _, _) = retry.into_parts();
        let attempt_preserved = retry_claim.attempts() == 1;
        let claim_token_rotated = retry_claim.token() != claim.token();
        self.backend
            .mark_event_delivery_succeeded(&retry_claim)
            .await?;
        let retry_completed = self.backend.get_event_delivery(delivery_id).await?.status()
            == EventDeliveryStatus::Succeeded;

        Ok(DeliveryFaultProbe::new(
            TransactionFaultProbe::new(claim_error.kind(), true),
            TransactionFaultProbe::new(acknowledgement_error.kind(), true),
            DeliveryRecoveryProbe::new(
                failure_persisted,
                attempt_preserved,
                claim_token_rotated,
                retry_completed,
            ),
        ))
    }
}

struct PostgresRestoreCoordinationFaultFixture {
    pool: PostgresPool,
}

#[async_trait]
impl RestoreCoordinationFaultFixture for PostgresRestoreCoordinationFaultFixture {
    async fn restore_coordination_fault_probe(
        &self,
    ) -> Result<RestoreCoordinationFaultProbe, FixtureError> {
        let backend = StorageHandle::postgres(self.pool.clone());
        let now = chrono::Utc::now();
        let instance_id = uuid::Uuid::new_v4();
        let local_idle = || true;
        let heartbeat_error =
            PostgresFaultController::failing(PostgresFaultPoint::RestoreCoordinatorAfterHeartbeat)
                .run(backend.tick_restore_coordinator(instance_id, &local_idle, false))
                .await
                .err()
                .ok_or_else(|| std::io::Error::other("restore heartbeat failpoint did not fail"))?;
        let (_, instances) = backend
            .get_restore_drain_state(
                now - chrono::Duration::try_minutes(1).expect("valid test duration"),
            )
            .await?
            .into_parts();
        let heartbeat_rolled_back = instances
            .iter()
            .all(|instance| instance.instance_id() != instance_id);

        let job = backend
            .stage_restore(
                StorageRestoreStageCreate::try_new(
                    StorageRestoreInitiator::try_new(None, "fault-test", prefix("restore_fault"))
                        .expect("valid restore initiator"),
                    b"{}".to_vec(),
                    StorageRestoreArtifactSummary::try_new(
                        2,
                        "44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a",
                    )
                    .expect("valid restore artifact"),
                    "f".repeat(64),
                    serde_json::json!({"compatible": true}),
                    now + chrono::Duration::try_hours(1).expect("valid test duration"),
                )
                .expect("valid restore staging request"),
            )
            .await?;
        let job_id = job.summary().id();
        let transition_error =
            PostgresFaultController::failing(PostgresFaultPoint::RestoreAfterDrainTransition)
                .run(backend.start_restore_draining(job_id))
                .await
                .err()
                .ok_or_else(|| {
                    std::io::Error::other("restore transition failpoint did not fail")
                })?;
        let transition_rolled_back = backend
            .get_restore_status(job_id)
            .await?
            .into_parts()
            .0
            .status()
            == StorageRestoreJobStatus::Validated;
        let snapshot = backend.get_restore_coordinator_snapshot().await?;
        let coordinator_remained_normal =
            snapshot.maintenance_state().is_normal() && snapshot.restore_job_id().is_none();
        hubuum_storage_postgres::test_support::delete_restore_job(&self.pool, job_id).await?;

        Ok(RestoreCoordinationFaultProbe::new(
            TransactionFaultProbe::new(heartbeat_error.kind(), heartbeat_rolled_back),
            TransactionFaultProbe::new(transition_error.kind(), transition_rolled_back),
            coordinator_remained_normal,
        ))
    }
}

struct PostgresLeaseLossFaultFixture {
    pool: PostgresPool,
}

#[async_trait]
impl LeaseLossFaultFixture for PostgresLeaseLossFaultFixture {
    async fn lease_loss_fault_probe(&self) -> Result<LeaseLossFaultProbe, FixtureError> {
        let backend = StorageHandle::postgres(self.pool.clone());
        let user = crate::tests::create_user_with_params(
            &self.pool,
            &prefix("lease_loss_user"),
            "testpassword",
        )
        .await;
        let task = backend
            .create_task(
                StorageTaskCreateRequest::builder(
                    StorageTaskKind::Import,
                    principal_id(user.id),
                    serde_json::json!({"lease_loss": true}),
                    1,
                )
                .idempotency_key(Some(
                    IdempotencyKey::new(prefix("lease_loss"))
                        .expect("lease-loss idempotency key should be valid"),
                ))
                .request_hash(Some(prefix("lease_loss_hash")))
                .scope_snapshot(StorageTaskScopeSnapshot::unscoped())
                .try_build(1)
                .expect("lease-loss task request should be valid"),
            )
            .await?;
        let lease_duration = StorageTaskLeaseDuration::from_milliseconds(50)
            .ok_or_else(|| std::io::Error::other("lease duration must be positive"))?;
        let first_claim = hubuum_storage_postgres::test_support::claim_task_by_id_with_lease(
            &self.pool,
            task.id(),
            lease_duration,
        )
        .await?;
        let renewal_error =
            PostgresFaultController::failing(PostgresFaultPoint::TaskLeaseBeforeRenewal)
                .run(backend.renew_task_lease(first_claim.lease().clone(), lease_duration))
                .await
                .err()
                .ok_or_else(|| std::io::Error::other("task renewal failpoint did not fail"))?;
        tokio::time::sleep(Duration::from_millis(100)).await;
        let recovered = backend.recover_expired_task_leases(100).await?;
        let recovered_task = recovered
            .iter()
            .find(|recovered| recovered.id() == task.id());
        let recovered_as_failed =
            recovered_task.is_some_and(|task| task.status() == StorageTaskStatus::Failed);
        let lease_cleared = recovered_task.is_some_and(|task| !task.has_lease());
        let request_payload_cleared =
            recovered_task.is_some_and(|task| task.request_payload().is_none());
        let stale_renewal_rejected = !backend
            .renew_task_lease(first_claim.lease().clone(), lease_duration)
            .await?;
        hubuum_storage_postgres::test_support::delete_task(&self.pool, task.id()).await?;
        user.delete_without_events(&self.pool)
            .await
            .map_err(|error| std::io::Error::other(error.to_string()))?;

        Ok(LeaseLossFaultProbe::new(
            renewal_error.kind(),
            recovered_as_failed,
            lease_cleared,
            request_payload_cleared,
            stale_renewal_rejected,
        ))
    }
}

impl BackendTestEnvironment {
    async fn verify_audit_contract(&self) -> Result<ContractReport, FixtureError> {
        match self {
            Self::Postgres { pool } => {
                let fixture = PostgresAuditContractFixture::new(pool.clone()).await?;
                verify_backend_audit_contract(&fixture)
                    .await
                    .map_err(Into::into)
            }
        }
    }
}

#[actix_web::test]
async fn every_registered_backend_satisfies_the_audited_mutation_contract() {
    let _permit = postgres_permit().await;

    for environment in available_backend_environments() {
        let report = environment
            .verify_audit_contract()
            .await
            .expect("certified backend must satisfy the audit contract");
        assert_eq!(report.checks(), 6);
    }
}

#[actix_web::test]
async fn backend_registration_does_not_require_optional_application_providers() {
    let _permit = postgres_permit().await;
    let pool = pool();
    let backend = StorageHandle::from_registered_backend(AdapterPostgresStorage::unobserved(
        pool.get_ref().clone(),
    ));

    assert!(backend.database_pool_state().is_none());
    assert!(!backend.has_worker_notification_provider());
    assert!(
        backend
            .database_storage_snapshot()
            .await
            .expect("missing optional diagnostics must not be a backend failure")
            .is_none()
    );
    assert!(backend.get_readiness_snapshot().await.is_ok());
}

struct BackendApplicationFixture {
    backend: StorageHandle,
    administrator: crate::models::User,
    bearer_token: String,
}

async fn backend_application_fixture(
    environment: &BackendTestEnvironment,
) -> BackendApplicationFixture {
    match environment {
        BackendTestEnvironment::Postgres { pool } => {
            let administrator = crate::tests::create_test_admin(pool).await;
            let bearer_token = administrator
                .create_token(pool)
                .await
                .expect("backend compatibility administrator token should be created")
                .get_token();
            BackendApplicationFixture {
                backend: environment.storage(),
                administrator,
                bearer_token,
            }
        }
    }
}

impl BackendApplicationFixture {
    async fn cleanup(self, environment: &BackendTestEnvironment) {
        match environment {
            BackendTestEnvironment::Postgres { pool } => {
                self.administrator
                    .delete_without_events(pool)
                    .await
                    .expect("backend compatibility administrator should be removed");
            }
        }
    }
}

struct RegisteredApplicationCompatibilityFixture {
    environment: BackendTestEnvironment,
}

#[async_trait(?Send)]
impl ApplicationCompatibilityFixture for RegisteredApplicationCompatibilityFixture {
    async fn application_compatibility_probe(
        &self,
    ) -> Result<ApplicationCompatibilityProbe, FixtureError> {
        let fixture_error = |error: crate::errors::ApiError| -> FixtureError {
            std::io::Error::other(error.to_string()).into()
        };
        let config = crate::tests::integration_test_config().map_err(fixture_error)?;
        let fixture = backend_application_fixture(&self.environment).await;
        let backend = fixture.backend.clone();
        let descriptor = backend.descriptor();

        let services = Services::from_storage(backend.clone());
        let root = services
            .collections()
            .get(CollectionID::new(1).expect("valid root collection id"))
            .await
            .map_err(fixture_error)?;

        let permissions = Arc::new(LocalPermissionBackend::new(
            backend.clone(),
            config.admin_groupname.clone(),
        ));
        let app = test::init_service(
            App::new()
                .wrap(actix_web::middleware::from_fn(
                    crate::middlewares::actor_context,
                ))
                .app_data(Data::new(AppContext::new(backend, permissions)))
                .configure(crate::api::config),
        )
        .await;

        let ready =
            test::call_service(&app, test::TestRequest::get().uri("/readyz").to_request()).await;
        let readiness_status = ready.status().as_u16();

        let authorization = (
            http::header::AUTHORIZATION,
            format!("Bearer {}", fixture.bearer_token),
        );
        let point = test::call_service(
            &app,
            test::TestRequest::get()
                .uri("/api/v1/collections/1")
                .insert_header(authorization.clone())
                .to_request(),
        )
        .await;
        let point_status = point.status().as_u16();
        let point_id = if point.status() == http::StatusCode::OK {
            test::read_body_json::<crate::models::Collection, _>(point)
                .await
                .id
        } else {
            -1
        };

        let list = test::call_service(
            &app,
            test::TestRequest::get()
                .uri("/api/v1/collections?limit=10")
                .insert_header(authorization)
                .to_request(),
        )
        .await;
        let list_status = list.status().as_u16();
        let listed = if list.status() == http::StatusCode::OK {
            test::read_body_json::<Vec<crate::models::Collection>, _>(list).await
        } else {
            Vec::new()
        };

        drop(app);
        fixture.cleanup(&self.environment).await;

        ApplicationCompatibilityProbe::builder(descriptor.kind().as_str())
            .service_resource_id(root.id)
            .readiness_status(readiness_status)
            .point(point_status, point_id)
            .list(
                list_status,
                listed.into_iter().map(|collection| collection.id).collect(),
            )
            .build()
    }
}

#[actix_web::test]
async fn every_available_storage_backend_supplies_metrics_snapshots() {
    let _permit = postgres_permit().await;

    for backend in available_backends() {
        let inventory = backend
            .get_inventory_metrics_snapshot()
            .await
            .expect("certified backend should supply inventory metrics");
        assert!(inventory.counts().collections() >= 1);

        let tasks = backend
            .get_task_metrics_snapshot()
            .await
            .expect("certified backend should supply task metrics");
        assert_eq!(tasks.ages().len(), StorageTaskKind::ALL.len());

        let events = backend
            .get_event_metrics_snapshot()
            .await
            .expect("certified backend should supply event metrics");
        assert!(events.fanout().pending_events() >= 0);
        assert!(events.delivery().counts().total() >= 0);
    }
}

#[actix_web::test]
async fn postgres_rolls_back_a_compound_collection_create_at_an_injected_failure() {
    let _permit = postgres_permit().await;
    let pool = pool();
    let backend = StorageHandle::postgres(pool.get_ref().clone());
    let group = backend
        .create_group(
            StorageGroupCreate::new(
                None,
                prefix("collection_failpoint_group"),
                Some("collection rollback owner".to_string()),
            ),
            &EventContext::system(),
        )
        .await
        .expect("collection rollback owner group should be created")
        .into_value();
    let collection_name = prefix("collection_failpoint");
    let command = StorageCollectionCreate::new(
        collection_name.clone(),
        "must be rolled back",
        group.id(),
        Some(collection_id(
            CollectionID::new(1).expect("root id should be valid").id(),
        )),
    );

    let result = PostgresFaultController::failing(PostgresFaultPoint::CollectionCreateAfterRecords)
        .run(
            backend
                .collection_store()
                .create_collection(command, &EventContext::system()),
        )
        .await;
    let error = match result {
        Err(error) => error,
        Ok(_) => panic!("injected failure should abort collection creation"),
    };
    assert_eq!(error.kind(), StorageErrorKind::Backend);

    let persisted = hubuum_storage_postgres::test_support::count_collections_by_name(
        pool.get_ref(),
        &collection_name,
    )
    .await
    .expect("collection rollback should remain queryable");
    assert_eq!(persisted, 0, "all collection records must roll back");

    let _ = backend
        .delete_group(group.id(), &EventContext::system())
        .await
        .expect("collection rollback owner group should be removed")
        .into_value();
}

#[actix_web::test]
async fn postgres_rolls_back_task_finalization_at_an_injected_failure() {
    let _permit = postgres_permit().await;
    let pool = pool();
    let backend = StorageHandle::postgres(pool.get_ref().clone());
    let user = crate::tests::create_user_with_params(
        pool.get_ref(),
        &prefix("task_failpoint_user"),
        "testpassword",
    )
    .await;
    let task = backend
        .create_task(
            StorageTaskCreateRequest::builder(
                StorageTaskKind::Import,
                principal_id(user.id),
                serde_json::json!({"failpoint": true}),
                1,
            )
            .idempotency_key(Some(
                IdempotencyKey::new(prefix("task_failpoint_key"))
                    .expect("failpoint idempotency key should be valid"),
            ))
            .request_hash(Some(prefix("task_failpoint_hash")))
            .scope_snapshot(StorageTaskScopeSnapshot::unscoped())
            .try_build(10)
            .expect("failpoint task request should be valid"),
        )
        .await
        .expect("task rollback fixture should be created");
    let claim_token = uuid::Uuid::new_v4();
    hubuum_storage_postgres::test_support::assign_task_lease(
        pool.get_ref(),
        task.id(),
        StorageTaskStatus::Validating,
        claim_token,
        chrono::Utc::now().naive_utc()
            + chrono::Duration::try_minutes(1).expect("valid failpoint lease"),
    )
    .await
    .expect("task rollback fixture should receive a live claim");
    let lease = StorageTaskLease::new(
        task.id(),
        StorageTaskClaimToken::new(claim_token.to_string()),
    );

    let error = PostgresFaultController::failing(PostgresFaultPoint::TaskFinalizeAfterEvent)
        .run(
            backend.complete_task(
                StorageTaskCompletion::try_new(
                    StorageTaskKind::Import,
                    StorageTaskTerminalUpdate::try_new(
                        lease,
                        StorageTaskStatus::Succeeded,
                        StorageTaskResultCounts::try_new(1, 1, 0)
                            .expect("non-negative task counts should be valid"),
                    )
                    .expect("succeeded is a terminal task status"),
                    StorageTaskEventInput::new("succeeded", "Must be rolled back"),
                    StorageTaskCompletionArtifact::None,
                )
                .expect("an import task accepts no completion artifact"),
            ),
        )
        .await
        .expect_err("injected failure should abort task finalization");
    assert_eq!(error.kind(), StorageErrorKind::Backend);

    let (persisted, _) = backend
        .get_task_access(task.id())
        .await
        .expect("rolled-back task should remain readable")
        .into_parts();
    assert_eq!(persisted.status(), StorageTaskStatus::Validating);
    let events = backend
        .list_task_events(StorageTaskChildListQuery::new(
            task.id(),
            QueryOptions::new(Vec::new(), Vec::new(), Some(10), None, true)
                .expect("contract query must be valid"),
        ))
        .await
        .expect("rolled-back task events should remain readable");
    assert_eq!(
        events.into_parts().0.len(),
        1,
        "terminal event must roll back"
    );

    hubuum_storage_postgres::test_support::delete_task(pool.get_ref(), task.id())
        .await
        .expect("task rollback fixture should be removed");
    user.delete_without_events(pool.get_ref())
        .await
        .expect("task rollback user should be removed");
}

#[actix_web::test]
async fn postgres_task_page_count_and_rows_share_one_snapshot() {
    let _permit = postgres_permit().await;
    let pool = pool();
    let backend = StorageHandle::postgres(pool.get_ref().clone());
    let user = crate::tests::create_user_with_params(
        pool.get_ref(),
        &prefix("task_snapshot_user"),
        "testpassword",
    )
    .await;
    let first = backend
        .create_task(
            StorageTaskCreateRequest::builder(
                StorageTaskKind::Import,
                principal_id(user.id),
                serde_json::json!({"snapshot": "first"}),
                0,
            )
            .idempotency_key(Some(
                IdempotencyKey::new(prefix("task_snapshot_first"))
                    .expect("snapshot idempotency key should be valid"),
            ))
            .scope_snapshot(StorageTaskScopeSnapshot::unscoped())
            .try_build(10)
            .expect("initial snapshot task request should be valid"),
        )
        .await
        .expect("initial snapshot task should be created");

    let controller = PostgresFaultController::pausing(PostgresFaultPoint::PageAfterCount);
    let listing_controller = controller.clone();
    let listing_backend = backend.clone();
    let query = StorageTaskListQuery::new(
        Some(principal_id(user.id)),
        Some(StorageTaskKind::Import),
        Some(StorageTaskStatus::Queued),
        QueryOptions::new(Vec::new(), Vec::new(), Some(10), None, true)
            .expect("snapshot query should be valid"),
    );
    let listing = tokio::spawn(async move {
        listing_controller
            .run(listing_backend.list_tasks(query))
            .await
    });
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        controller.wait_until_reached(),
    )
    .await
    .expect("task page should pause after its count");

    let second = backend
        .create_task(
            StorageTaskCreateRequest::builder(
                StorageTaskKind::Import,
                principal_id(user.id),
                serde_json::json!({"snapshot": "second"}),
                0,
            )
            .idempotency_key(Some(
                IdempotencyKey::new(prefix("task_snapshot_second"))
                    .expect("snapshot idempotency key should be valid"),
            ))
            .scope_snapshot(StorageTaskScopeSnapshot::unscoped())
            .try_build(10)
            .expect("concurrent snapshot task request should be valid"),
        )
        .await
        .expect("concurrent snapshot task should be created");
    controller.resume();

    let page = tokio::time::timeout(std::time::Duration::from_secs(5), listing)
        .await
        .expect("task page should finish after resuming")
        .expect("task page future should not panic")
        .expect("task page should succeed");
    let (tasks, total) = page.into_parts();
    assert_eq!(total, Some(1));
    assert_eq!(tasks.len(), 1);
    assert_eq!(tasks[0].id(), first.id());

    for task in [first, second] {
        hubuum_storage_postgres::test_support::delete_task(pool.get_ref(), task.id())
            .await
            .expect("snapshot task should be removed");
    }
    user.delete_without_events(pool.get_ref())
        .await
        .expect("snapshot user should be removed");
}

#[actix_web::test]
async fn postgres_delivery_faults_preserve_claim_acknowledgement_and_retry_recovery() {
    let _permit = postgres_permit().await;
    let pool = pool();
    let fixture = PostgresAuditContractFixture::new(pool.get_ref().clone())
        .await
        .expect("delivery fault fixture should initialize");
    let report = verify_delivery_fault_contract(&fixture)
        .await
        .expect("PostgreSQL must satisfy the portable delivery fault contract");
    assert_eq!(report.checks(), 8);

    fixture
        .cleanup()
        .await
        .expect("delivery fault fixture should clean up");
}

#[actix_web::test]
async fn postgres_restore_faults_roll_back_coordination_transitions() {
    let _permit = postgres_permit().await;
    let pool = pool();
    let fixture = PostgresRestoreCoordinationFaultFixture {
        pool: pool.get_ref().clone(),
    };
    let report = verify_restore_coordination_fault_contract(&fixture)
        .await
        .expect("PostgreSQL must satisfy the portable restore coordination fault contract");
    assert_eq!(report.checks(), 5);
}

#[actix_web::test]
async fn postgres_lease_renewal_loss_expires_and_finalizes_without_replay() {
    let _permit = postgres_permit().await;
    let pool = pool();
    let fixture = PostgresLeaseLossFaultFixture {
        pool: pool.get_ref().clone(),
    };
    let report = verify_lease_loss_fault_contract(&fixture)
        .await
        .expect("PostgreSQL must satisfy the portable lease-loss fault contract");
    assert_eq!(report.checks(), 5);
}

#[actix_web::test]
async fn every_available_storage_backend_supplies_consistent_inventory_counts() {
    let _permit = postgres_permit().await;

    for backend in available_backends() {
        let counts = backend
            .get_inventory_counts()
            .await
            .expect("certified backend should supply inventory counts");
        let grouped_objects = counts
            .objects_by_class()
            .iter()
            .map(|row| row.count())
            .sum::<i64>();

        assert_eq!(grouped_objects, counts.total_objects());
        assert!(counts.total_classes() >= counts.objects_by_class().len() as i64);
        assert!(counts.total_collections() >= 1);
        assert!(
            counts
                .objects_by_class()
                .windows(2)
                .all(|rows| rows[0].class_id() < rows[1].class_id()),
            "per-class counts must use stable class-id ordering"
        );
    }
}

#[actix_web::test]
async fn every_available_storage_backend_supplies_complete_group_behavior() {
    let _permit = postgres_permit().await;
    let pool = pool();

    for backend in available_backends() {
        let initial_name = prefix("group_contract");
        let renamed = prefix("group_contract_renamed");
        let created = backend
            .create_group(
                StorageGroupCreate::new(
                    None,
                    initial_name,
                    Some("storage compatibility group".to_string()),
                ),
                &EventContext::system(),
            )
            .await
            .expect("certified backend should create groups")
            .into_value();

        let loaded = backend
            .get_group(created.id())
            .await
            .expect("certified backend should load groups");
        assert_eq!(loaded.id(), created.id());
        assert_eq!(
            backend
                .resolve_group_identity_scope_name(created.id())
                .await
                .expect("certified backend should resolve group identity scopes"),
            LOCAL_IDENTITY_SCOPE
        );

        let updated = backend
            .update_group(
                created.id(),
                StorageGroupUpdate::new(Some(renamed.clone())),
                &EventContext::system(),
            )
            .await
            .expect("certified backend should update groups")
            .into_value();
        assert_eq!(updated.name(), renamed);

        let list_options = QueryOptions::new(Vec::new(), Vec::new(), None, None, true)
            .expect("contract query must be valid");
        let (listed, total_count) = backend
            .list_groups(StorageGroupListQuery::new(list_options))
            .await
            .expect("certified backend should list and count groups")
            .into_parts();
        assert!(listed.iter().any(|group| group.id() == created.id()));
        assert!(total_count.is_some_and(|count| count >= listed.len() as i64));

        let user = crate::tests::create_user_with_params(
            pool.get_ref(),
            &prefix("group_contract_user"),
            "testpassword",
        )
        .await;
        backend
            .add_group_member(principal_id(user.id), created.id(), &EventContext::system())
            .await
            .expect("certified backend should add group members")
            .into_value();

        let members = backend
            .load_group_member_principals(created.id())
            .await
            .expect("certified backend should list group members");
        assert!(
            members
                .iter()
                .any(|member| member.id() == principal_id(user.id))
        );
        let query_options = QueryOptions::new(Vec::new(), Vec::new(), Some(10), None, true)
            .expect("contract query must be valid");
        let page = backend
            .list_group_members(created.id(), query_options.clone())
            .await
            .expect("certified backend should page group members");
        assert!(
            page.rows()
                .iter()
                .any(|member| member.principal().id() == principal_id(user.id))
        );
        assert_eq!(page.total(), Some(1));

        backend
            .remove_group_member(principal_id(user.id), created.id(), &EventContext::system())
            .await
            .expect("certified backend should remove group members")
            .into_value();
        let empty_page = backend
            .list_group_members(created.id(), query_options)
            .await
            .expect("certified backend should recount group members");
        assert_eq!(empty_page.total(), Some(0));
        assert_eq!(
            backend
                .delete_group(created.id(), &EventContext::system())
                .await
                .expect("certified backend should delete groups")
                .into_value(),
            1
        );
    }
}

#[actix_web::test]
async fn every_available_storage_backend_supplies_complete_principal_behavior() {
    let _permit = postgres_permit().await;
    let pool = pool();
    let user = crate::tests::create_user_with_params(
        pool.get_ref(),
        &prefix("principal_contract_user"),
        "testpassword",
    )
    .await;
    let event_context = EventContext::user(principal_id(user.id), None, None);

    for backend in available_backends() {
        let loaded = backend
            .get_principal(principal_id(user.id))
            .await
            .expect("certified backend should load principals");
        assert_eq!(loaded.id(), principal_id(user.id));

        let initial = backend
            .get_principal_settings(principal_id(user.id))
            .await
            .expect("certified backend should load principal settings");
        assert_eq!(initial.document(), &serde_json::json!({}));

        let replaced = backend
            .update_principal_settings(
                principal_id(user.id),
                StoragePrincipalSettingsMutation::Replace(serde_json::json!({
                    "theme": "light",
                    "notifications": {"email": true}
                })),
                &event_context,
            )
            .await
            .expect("certified backend should replace principal settings")
            .into_value();
        assert_eq!(replaced.document()["theme"], "light");

        let merged = backend
            .update_principal_settings(
                principal_id(user.id),
                StoragePrincipalSettingsMutation::MergePatch(serde_json::json!({
                    "notifications": {"push": true}
                })),
                &event_context,
            )
            .await
            .expect("certified backend should merge principal settings")
            .into_value();
        assert_eq!(merged.document()["notifications"]["email"], true);
        assert_eq!(merged.document()["notifications"]["push"], true);

        let patched = backend
            .update_principal_settings(
                principal_id(user.id),
                StoragePrincipalSettingsMutation::JsonPatch(serde_json::json!([
                    {"op": "replace", "path": "/theme", "value": "dark"}
                ])),
                &event_context,
            )
            .await
            .expect("certified backend should apply principal settings JSON Patch")
            .into_value();
        assert_eq!(patched.document()["theme"], "dark");

        let reset = backend
            .update_principal_settings(
                principal_id(user.id),
                StoragePrincipalSettingsMutation::Reset,
                &event_context,
            )
            .await
            .expect("certified backend should reset principal settings")
            .into_value();
        assert_eq!(reset.document(), &serde_json::json!({}));
    }
}

#[actix_web::test]
async fn every_available_storage_backend_supplies_authentication_projections() {
    let _permit = postgres_permit().await;
    let pool = pool();
    let user = crate::tests::create_user_with_params(
        pool.get_ref(),
        &prefix("authentication_user"),
        "testpassword",
    )
    .await;
    let token = user
        .create_token(pool.get_ref())
        .await
        .expect("authentication compatibility token should be created");

    for backend in available_backends() {
        let observed_at = chrono::Utc::now();
        let attempt = StorageAuthenticationAttempt::try_new(
            StorageAuthenticationCredential::new(token.storage_hash()),
            observed_at,
            observed_at - chrono::Duration::days(1),
        )
        .expect("compatibility authentication window should be valid");
        let authenticated = backend
            .authenticate_bearer_token(attempt)
            .await
            .expect("certified backend should validate active bearer credentials");
        assert_eq!(authenticated.principal_id(), principal_id(user.id));
        assert!(!authenticated.is_scoped());

        let identity = backend
            .get_authentication_identity(principal_id(user.id))
            .await
            .expect("certified backend should supply authentication identity data");
        let (principal, human) = identity.into_parts();

        assert_eq!(principal.id(), principal_id(user.id));
        assert!(principal.is_human());
        assert!(human.is_some());

        let scope = backend
            .get_authentication_token_scope(StorageAuthenticationTokenScopeQuery::new(
                hubuum_domain::TokenId::new(i32::MAX).unwrap(),
                true,
                false,
            ))
            .await
            .expect("certified backend should preserve empty scope dimensions")
            .expect("an enabled scope dimension should produce a scope snapshot");
        let (permissions, resources) = scope.into_parts();
        assert_eq!(permissions, Some(Vec::new()));
        assert_eq!(resources, None);
    }

    user.delete_without_events(pool.get_ref())
        .await
        .expect("authentication compatibility fixture should be removed");
}

#[actix_web::test]
async fn every_available_storage_backend_supplies_complete_identity_operations() {
    let _permit = postgres_permit().await;
    let pool = pool();
    let username = prefix("identity_contract_user");
    let user =
        crate::tests::create_user_with_params(pool.get_ref(), &username, "testpassword").await;
    let _token = user
        .create_token(pool.get_ref())
        .await
        .expect("identity compatibility token should be created");
    let owner_group = crate::tests::create_test_group(pool.get_ref()).await;
    owner_group
        .add_member_without_events(pool.get_ref(), &user)
        .await
        .expect("identity compatibility membership should be created");

    for backend in available_backends() {
        assert!(
            !backend
                .is_default_admin_bootstrap_required()
                .await
                .expect("seeded certified backend should report bootstrap state")
        );
        assert!(
            !backend
                .bootstrap_default_admin(StorageDefaultAdminBootstrap::new(
                    "unused-contract-admin-group",
                    "unused-contract-password-hash",
                ))
                .await
                .expect("certified backend should coordinate administrator bootstrap")
        );
        let local_scope = backend
            .ensure_identity_scope(crate::storage::StorageIdentityScopeEnsure::new(
                LOCAL_IDENTITY_SCOPE,
                crate::models::LOCAL_PROVIDER_KIND,
            ))
            .await
            .expect("certified backend should reconcile identity scopes");
        assert_eq!(
            backend
                .resolve_identity_scope_name(local_scope.id())
                .await
                .expect("certified backend should resolve one identity scope"),
            LOCAL_IDENTITY_SCOPE
        );
        assert_eq!(
            backend
                .resolve_identity_scope_names(vec![local_scope.id()])
                .await
                .expect("certified backend should resolve identity scopes"),
            vec![(local_scope.id(), LOCAL_IDENTITY_SCOPE.to_string())]
        );

        let membership = backend
            .get_principal_group(principal_id(user.id), group_id(owner_group.id))
            .await
            .expect("certified backend should load effective memberships");
        assert_eq!(membership.principal_id(), principal_id(user.id));
        let group_options = prepare_db_pagination::<crate::models::Group>(
            &QueryOptions::new(Vec::new(), Vec::new(), Some(20), None, true)
                .expect("contract query must be valid"),
        )
        .expect("identity compatibility group query should be valid");
        let (groups, group_total) = backend
            .list_principal_groups(StoragePrincipalGroupListQuery::new(
                principal_id(user.id),
                group_options,
            ))
            .await
            .expect("certified backend should list principal groups")
            .into_parts();
        assert!(group_total.is_some_and(|total| total >= 1));
        assert!(
            groups
                .into_iter()
                .any(|group| group.id() == group_id(owner_group.id))
        );
        assert!(
            backend
                .is_human_owner_group_member(principal_id(user.id), group_id(owner_group.id))
                .await
                .expect("certified backend should evaluate human ownership")
        );

        let event_context = EventContext::user(principal_id(user.id), None, None);
        let contract_username = prefix("complete_user_contract");
        let contract_user = backend
            .create_user(StorageUserCreate::new(
                None,
                &contract_username,
                "complete-user-contract-password-hash",
                Some("Complete Contract".to_string()),
                Some("complete-contract@example.invalid".to_string()),
                event_context.clone(),
            ))
            .await
            .expect("certified backend should create users")
            .into_value();
        let contract_user_id = contract_user.into_parts().id();
        let contract_principal_id = principal_id(contract_user_id.id());
        assert_eq!(
            backend
                .get_user(contract_user_id)
                .await
                .expect("certified backend should load users")
                .into_parts()
                .id(),
            contract_user_id
        );
        assert_eq!(
            backend
                .get_user_by_name(LOCAL_IDENTITY_SCOPE.to_string(), contract_username.clone(),)
                .await
                .expect("certified backend should resolve scoped user names")
                .into_parts()
                .id(),
            contract_user_id
        );
        assert_eq!(
            backend
                .get_user_details(contract_user_id)
                .await
                .expect("certified backend should load user points")
                .into_parts()
                .id(),
            contract_user_id
        );
        let user_options = prepare_db_pagination::<crate::models::UserWithName>(
            &QueryOptions::new(
                vec![ParsedQueryParam {
                    field: FilterField::Id,
                    operator: SearchOperator::Equals { is_negated: false },
                    value: contract_user_id.to_string(),
                }],
                Vec::new(),
                Some(100),
                None,
                true,
            )
            .expect("contract query must be valid"),
        )
        .expect("identity compatibility user query should be valid");
        let (users, user_total) = backend
            .list_users(StorageUserListQuery::new(user_options))
            .await
            .expect("certified backend should list users")
            .into_parts();
        assert!(user_total.is_some_and(|total| total >= 1));
        assert!(users.into_iter().any(|item| {
            item.into_parts().user().clone().into_parts().id() == contract_user_id
        }));
        backend
            .update_user(StorageUserUpdate::new(
                contract_user_id,
                None,
                Some("Updated Contract".to_string()),
                None,
                event_context.clone(),
            ))
            .await
            .expect("certified backend should update users")
            .into_value();
        backend
            .set_user_password(StorageUserPasswordUpdate::new(
                contract_user_id,
                "updated-complete-user-contract-password-hash",
                event_context.clone(),
            ))
            .await
            .expect("certified backend should replace local passwords")
            .into_value();

        let token_policy = StorageTokenIssuancePolicy::try_new(24, 24)
            .expect("contract token policy should be valid");
        let token_observed_at = chrono::Utc::now() + chrono::Duration::seconds(1);
        let token_observation = StorageTokenObservation::try_new(
            token_observed_at,
            token_observed_at - chrono::Duration::hours(24),
        )
        .expect("identity compatibility token observation should be valid");
        let first_hash = prefix("complete_token_hash");
        let first_token = backend
            .create_token(StorageTokenCreate::new(
                contract_principal_id,
                &first_hash,
                token_policy,
                event_context.clone(),
            ))
            .await
            .expect("certified backend should create tokens")
            .into_value();
        let first_token_id = first_token.id();
        assert_eq!(
            backend
                .get_token_metadata(contract_principal_id, first_token_id, token_observation)
                .await
                .expect("certified backend should load token metadata")
                .id(),
            first_token_id
        );
        let batch = backend
            .load_token_metadata_by_ids(vec![first_token_id, first_token_id], token_observation)
            .await
            .expect("certified backend should preserve token batch order");
        assert_eq!(batch.len(), 2);
        assert_eq!(batch[0].id(), batch[1].id());

        let second_hash = prefix("complete_renewed_token_hash");
        let renewed = backend
            .renew_token(StorageTokenRenew::new(
                first_token_id,
                contract_principal_id,
                &second_hash,
                None,
                token_policy,
                event_context.clone(),
            ))
            .await
            .expect("certified backend should renew tokens")
            .into_value();
        assert_ne!(renewed.id(), first_token_id);
        assert_eq!(
            backend
                .revoke_token(StorageTokenRevoke::new(
                    first_token_id,
                    contract_principal_id,
                    event_context.clone(),
                ))
                .await
                .expect("certified backend should revoke principal-scoped tokens")
                .into_value(),
            1
        );
        assert_eq!(
            backend
                .revoke_token_by_hash(StorageTokenHashRevoke::new(
                    Some(contract_principal_id),
                    second_hash,
                    event_context.clone(),
                ))
                .await
                .expect("certified backend should revoke HMAC-keyed tokens")
                .into_value(),
            1
        );
        let third_hash = prefix("complete_revoke_all_token_hash");
        backend
            .create_token(StorageTokenCreate::new(
                contract_principal_id,
                third_hash,
                token_policy,
                event_context.clone(),
            ))
            .await
            .expect("certified backend should create a token for bulk revocation")
            .into_value();
        assert_eq!(
            backend
                .revoke_all_principal_tokens(StoragePrincipalTokensRevoke::new(
                    contract_principal_id,
                    event_context.clone(),
                ))
                .await
                .expect("certified backend should revoke all principal tokens")
                .into_value(),
            1
        );
        backend
            .anonymize_user(StorageUserAnonymize::new(
                contract_user_id,
                event_context.clone(),
            ))
            .await
            .expect("certified backend should anonymize users")
            .into_value();
        assert_eq!(
            backend
                .delete_user(StorageUserDelete::new(
                    contract_user_id,
                    event_context.clone(),
                ))
                .await
                .expect("certified backend should delete users")
                .into_value(),
            1
        );

        let token_options = prepare_db_pagination::<crate::models::PrincipalToken>(
            &QueryOptions::new(Vec::new(), Vec::new(), Some(20), None, true)
                .expect("contract query must be valid"),
        )
        .expect("identity compatibility token query should be valid");
        let (tokens, token_total) = backend
            .list_retained_tokens(StorageTokenListQuery::new(
                principal_id(user.id),
                token_options,
                StorageTokenListState::Active,
                token_observation,
            ))
            .await
            .expect("certified backend should list retained tokens")
            .into_parts();
        assert_eq!(token_total, Some(1));
        assert_eq!(tokens[0].principal_id(), principal_id(user.id));
        let password_reset = backend
            .reset_local_password(StorageLocalPasswordReset::new(
                &username,
                "identity-contract-password-hash",
                event_context.clone(),
            ))
            .await
            .expect("certified backend should reset local credentials");
        assert!(password_reset.is_committed());
        assert_eq!(password_reset.audits().map(|audits| audits.len()), Some(1));
        assert_eq!(password_reset.into_value(), 1);

        let service_account_name = prefix("identity_contract_sa");
        let created = backend
            .create_service_account(StorageServiceAccountCreate::new(
                &service_account_name,
                "identity contract",
                group_id(owner_group.id),
                Some(principal_id(user.id)),
                event_context.clone(),
            ))
            .await
            .expect("certified backend should create service accounts")
            .into_value();
        let loaded = backend
            .get_service_account(created.id())
            .await
            .expect("certified backend should load service accounts");
        assert_eq!(loaded.owner_group_id(), group_id(owner_group.id));
        let point = backend
            .get_service_account_details(created.id())
            .await
            .expect("certified backend should load service-account points");
        assert_eq!(point.into_parts().2, service_account_name);

        let service_account_options =
            prepare_db_pagination::<crate::models::ServiceAccountWithName>(
                &QueryOptions::new(Vec::new(), Vec::new(), Some(100), None, true)
                    .expect("contract query must be valid"),
            )
            .expect("identity compatibility service-account query should be valid");
        let (accounts, account_total) = backend
            .list_manageable_service_accounts(StorageServiceAccountListQuery::new(
                principal_id(user.id),
                true,
                service_account_options,
            ))
            .await
            .expect("certified backend should list manageable service accounts")
            .into_parts();
        assert!(account_total.is_some_and(|total| total >= 1));
        assert!(accounts.into_iter().any(|account| {
            let (account, _, _, _) = account.into_parts();
            account.id() == created.id()
        }));

        let updated = backend
            .update_service_account(StorageServiceAccountUpdate::new(
                created.id(),
                Some("updated identity contract".to_string()),
                None,
                event_context.clone(),
            ))
            .await
            .expect("certified backend should update service accounts")
            .into_value();
        assert_eq!(updated.description(), "updated identity contract");
        assert!(
            !backend
                .is_service_account_disabled(principal_id(created.id().id()))
                .await
                .expect("certified backend should read principal lifecycle")
        );
        let queued_task = backend
            .create_task(
                StorageTaskCreateRequest::builder(
                    StorageTaskKind::Import,
                    principal_id(created.id().id()),
                    serde_json::json!({"items": []}),
                    0,
                )
                .idempotency_key(Some(
                    IdempotencyKey::new(prefix("identity_contract_sa_task"))
                        .expect("identity contract task key should be valid"),
                ))
                .request_hash(Some(prefix("identity_contract_sa_task_hash")))
                .scope_snapshot(StorageTaskScopeSnapshot::unscoped())
                .try_build(10)
                .expect("identity contract task request should be valid"),
            )
            .await
            .expect("certified backend should queue service-account work");
        let (disabled, cancelled_task_kinds) = backend
            .disable_service_account(StorageServiceAccountMutation::new(
                created.id(),
                event_context.clone(),
            ))
            .await
            .expect("certified backend should disable service accounts")
            .into_value()
            .into_parts();
        assert!(disabled.is_disabled());
        assert_eq!(cancelled_task_kinds, vec![StorageTaskKind::Import]);
        let (cancelled_task, _) = backend
            .get_task_access(queued_task.id())
            .await
            .expect("certified backend should expose the cancelled task")
            .into_parts();
        assert_eq!(cancelled_task.status(), StorageTaskStatus::Cancelled);
        assert!(cancelled_task.finished_at().is_some());
        assert!(cancelled_task.request_payload().is_none());
        assert!(cancelled_task.request_redacted_at().is_some());
        assert!(
            backend
                .is_service_account_disabled(principal_id(created.id().id()))
                .await
                .expect("certified backend should observe disabled principals")
        );

        let external_scope = prefix("identity_contract_scope");
        let external_name = prefix("identity_contract_external");
        let external = backend
            .sync_external_user(
                crate::storage::StorageExternalUserSync::builder(
                    &external_scope,
                    "compatibility_provider",
                    prefix("identity_contract_subject"),
                    &external_name,
                )
                .groups(vec![crate::storage::StorageExternalGroup::new(
                    prefix("identity_contract_group_key"),
                    prefix("identity_contract_group"),
                    None,
                )])
                .build(),
            )
            .await
            .expect("certified backend should synchronize external identities")
            .into_value();
        let external_id = external.id();
        let external_state = backend
            .get_external_principal_state(principal_id(external_id.id()))
            .await
            .expect("certified backend should load external identity state")
            .expect("synchronized external identity should have refresh state");
        assert_eq!(external_state.identity_scope(), external_scope);
        backend
            .mark_external_sync_attempted(principal_id(external_id.id()))
            .await
            .expect("certified backend should record external sync attempts");

        backend
            .delete_service_account(StorageServiceAccountMutation::new(
                created.id(),
                event_context,
            ))
            .await
            .expect("certified backend should delete service accounts")
            .into_value();
    }

    user.delete_without_events(pool.get_ref())
        .await
        .expect("identity compatibility user should be removed");
}

#[actix_web::test]
async fn postgres_external_sync_preserves_identity_and_reconciles_membership_sources() {
    use hubuum_storage_core::{StorageExternalGroup, StorageExternalUserSync};

    let _permit = postgres_permit().await;
    let pool = pool();
    let backend = StorageHandle::postgres(pool.get_ref().clone());
    let identity_scope = prefix("external_contract_scope");
    let initial_subject = prefix("external_contract_subject");
    let replacement_subject = prefix("external_contract_subject_reformatted");
    let initial_name = prefix("external_contract_user");
    let renamed = prefix("external_contract_user_renamed");
    let first_group_key = prefix("external_contract_group_first_key");
    let first_group_name = prefix("external_contract_group_first");
    let second_group_key = prefix("external_contract_group_second_key");
    let second_group_name = prefix("external_contract_group_second");
    let request = |subject: &str, name: &str, key: &str, group_name: &str| {
        StorageExternalUserSync::builder(&identity_scope, "compatibility_provider", subject, name)
            .proper_name(Some(format!("{name} Example")))
            .email(Some(format!("{name}@example.org")))
            .groups(vec![StorageExternalGroup::new(
                key,
                group_name,
                Some("directory-owned group".to_string()),
            )])
            .build()
    };

    let first_id = backend
        .sync_external_user(request(
            &initial_subject,
            &initial_name,
            &first_group_key,
            &first_group_name,
        ))
        .await
        .expect("initial external identity sync should succeed")
        .into_value()
        .id();
    let manual_group = backend
        .create_group(
            StorageGroupCreate::new(
                None,
                prefix("external_contract_manual_group"),
                Some("manual membership must survive provider sync".to_string()),
            ),
            &EventContext::system(),
        )
        .await
        .expect("manual group should be created")
        .into_value();
    let _ = backend
        .add_group_member(
            principal_id(first_id.id()),
            manual_group.id(),
            &EventContext::system(),
        )
        .await
        .expect("manual membership should be created")
        .into_value();

    let renamed_id = backend
        .sync_external_user(request(
            &initial_subject,
            &renamed,
            &second_group_key,
            &second_group_name,
        ))
        .await
        .expect("renamed external identity sync should succeed")
        .into_value()
        .id();
    let replacement_id = backend
        .sync_external_user(request(
            &replacement_subject,
            &renamed,
            &second_group_key,
            &second_group_name,
        ))
        .await
        .expect("external subject replacement should succeed")
        .into_value()
        .id();
    assert_eq!(renamed_id, first_id);
    assert_eq!(replacement_id, first_id);

    let state = backend
        .get_external_principal_state(principal_id(first_id.id()))
        .await
        .expect("external principal state should load")
        .expect("external principal should remain provider-managed");
    assert_eq!(state.identity_scope(), identity_scope);
    assert_eq!(state.username(), renamed);
    assert_eq!(state.external_subject(), replacement_subject);

    let persistence = hubuum_storage_postgres::test_support::external_identity_persistence(
        pool.get_ref(),
        &identity_scope,
        principal_id(first_id.id()),
    )
    .await
    .expect("external sync state should be queryable");
    assert_eq!(persistence.principal_count(), 1);
    assert!(
        persistence
            .memberships()
            .iter()
            .any(|membership| membership.group_id() == manual_group.id())
    );
    assert!(
        persistence
            .memberships()
            .iter()
            .any(|membership| membership.external_key() == Some(second_group_key.as_str()))
    );
    assert!(
        !persistence
            .memberships()
            .iter()
            .any(|membership| membership.external_key() == Some(first_group_key.as_str()))
    );
}

#[actix_web::test]
async fn every_available_storage_backend_supplies_execution_context() {
    for backend in available_backends() {
        let evaluations = Arc::new(AtomicUsize::new(0));

        let evaluated = Arc::clone(&evaluations);
        backend
            .run_in_scope(
                StorageExecutionScope::default().with_call_site(StorageCallSite::Readiness),
                async move {
                    evaluated.fetch_add(1, Ordering::SeqCst);
                },
            )
            .await;

        let evaluated = Arc::clone(&evaluations);
        backend
            .run_in_scope_send(
                StorageExecutionScope::default().with_call_site(StorageCallSite::TaskLease),
                async move {
                    evaluated.fetch_add(1, Ordering::SeqCst);
                },
            )
            .await;

        let evaluated = Arc::clone(&evaluations);
        backend
            .run_in_scope(
                StorageExecutionScope::default()
                    .with_mutation_provenance(Some(MutationProvenance::system())),
                async move {
                    evaluated.fetch_add(1, Ordering::SeqCst);
                },
            )
            .await;

        let evaluated = Arc::clone(&evaluations);
        backend
            .run_in_scope(
                StorageExecutionScope::default().with_revision_precondition(Some(
                    StorageRevisionPrecondition::new(
                        StorageRevisionTarget::Collection(CollectionID::new(1).unwrap()),
                        vec![crate::models::ResourceRevision::INITIAL],
                    ),
                )),
                async move {
                    evaluated.fetch_add(1, Ordering::SeqCst);
                },
            )
            .await;

        assert_eq!(AtomicUsize::load(evaluations.as_ref(), Ordering::SeqCst), 4);
    }
}

#[actix_web::test]
async fn every_available_storage_backend_supplies_the_export_query_scope() {
    for backend in available_backends() {
        let evaluations = Arc::new(AtomicUsize::new(0));
        let evaluated = Arc::clone(&evaluations);
        let output = backend
            .run_in_scope(
                StorageExecutionScope::default()
                    .with_query_budget(StorageQueryBudget::from_millis(250)),
                async move {
                    evaluated.fetch_add(1, Ordering::SeqCst);
                    "complete"
                },
            )
            .await;

        assert_eq!(output, "complete");
        assert_eq!(AtomicUsize::load(evaluations.as_ref(), Ordering::SeqCst), 1);
    }
}

#[actix_web::test]
async fn every_available_storage_backend_supplies_the_complete_import_contract() {
    let _permit = postgres_permit().await;
    let pool = pool();
    let preflight_name = prefix("import_preflight_collection");
    let best_effort_name = prefix("import_best_effort_collection");
    let rollback_name = prefix("import_rollback_collection");
    let collection_input = |name: &str, reference: &str| ImportCollectionInput {
        ref_: Some(reference.to_string()),
        name: name.to_string(),
        description: "storage compatibility import".to_string(),
        parent_collection_ref: None,
        parent_collection_key: None,
        condition: None,
        timestamps: None,
    };

    for backend in available_backends() {
        let root = backend
            .get_import_root_collection()
            .await
            .expect("certified backend should resolve the import root");
        let root_id = root.id();
        let root_name = root.name().to_string();
        assert_eq!(
            backend
                .get_import_collection_by_id(root_id)
                .await
                .expect("certified backend should look up import collections by id")
                .map(|collection| collection.id()),
            Some(root_id)
        );
        let root_key = crate::services::import_boundary::collection_key_to_storage(CollectionKey {
            name: root_name.clone(),
            path: Some(Vec::new()),
        });
        assert!(
            backend
                .get_import_collection_by_key(&root_key)
                .await
                .expect("certified backend should look up import collections by path")
                .is_some()
        );
        assert!(
            backend
                .list_import_collections_by_name(&root_name)
                .await
                .expect("certified backend should look up import collections by name")
                .iter()
                .any(|collection| collection.id() == root_id)
        );
        assert!(
            backend
                .get_import_collection_child_by_name(root_id, &preflight_name)
                .await
                .expect("certified backend should look up import children")
                .is_none()
        );
        assert!(
            backend
                .get_import_class_by_name(root_id, &prefix("missing_import_class"))
                .await
                .expect("certified backend should look up import classes")
                .is_none()
        );
        assert!(
            backend
                .list_import_classes_by_names(root_id, &[])
                .await
                .expect("certified backend should batch import class lookups")
                .is_empty()
        );
        assert!(
            backend
                .get_import_object_by_name(
                    ClassId::new(i32::MAX).unwrap(),
                    &prefix("missing_import_object"),
                )
                .await
                .expect("certified backend should look up import objects")
                .is_none()
        );
        assert!(
            backend
                .list_import_objects_by_names(ClassId::new(i32::MAX).unwrap(), &[])
                .await
                .expect("certified backend should batch import object lookups")
                .is_empty()
        );
        assert!(
            !backend
                .has_import_class_relation(
                    ClassId::new(i32::MAX - 1).unwrap(),
                    ClassId::new(i32::MAX).unwrap(),
                )
                .await
                .expect("certified backend should look up import class relations")
        );
        assert!(
            !backend
                .has_import_object_relation(
                    ObjectId::new(i32::MAX - 1).unwrap(),
                    ObjectId::new(i32::MAX).unwrap(),
                )
                .await
                .expect("certified backend should look up import object relations")
        );
        assert!(
            !backend
                .has_import_group(LOCAL_IDENTITY_SCOPE, &prefix("missing_import_group"),)
                .await
                .expect("certified backend should look up import groups")
        );

        let preflight_plan = StorageImportPlan::try_new(vec![StorageImportPlanItem::new(
            0,
            crate::services::import_boundary::import_operation_to_storage(
                ApplicationImportOperation::CreateCollection(collection_input(
                    &preflight_name,
                    "collection:preflight",
                )),
            )
            .expect("valid collection input should cross the storage boundary"),
        )])
        .expect("valid operations should form an import plan");
        let (preflight, aborted) = backend
            .preflight_import(
                preflight_plan.clone(),
                crate::services::import_boundary::import_mode_to_storage(ImportMode::default()),
            )
            .await
            .expect("certified backend should preflight an import")
            .into_parts();
        assert!(!aborted);
        assert_eq!(preflight.len(), 1);
        assert!(
            preflight
                .into_iter()
                .next()
                .unwrap()
                .into_parts()
                .2
                .is_none()
        );
        assert!(
            backend
                .get_import_collection_child_by_name(root_id, &preflight_name)
                .await
                .expect("preflight rollback should remain queryable")
                .is_none(),
            "import preflight must roll back every mutation"
        );

        backend
            .apply_import_strict(preflight_plan)
            .await
            .expect("certified backend should atomically apply a strict import");

        let rollback_plan = StorageImportPlan::try_new(vec![
            StorageImportPlanItem::new(
                0,
                crate::services::import_boundary::import_operation_to_storage(
                    ApplicationImportOperation::CreateCollection(collection_input(
                        &rollback_name,
                        "collection:rollback",
                    )),
                )
                .expect("valid collection input should cross the storage boundary"),
            ),
            StorageImportPlanItem::new(
                1,
                crate::services::import_boundary::import_operation_to_storage(
                    ApplicationImportOperation::CreateClass(ImportClassInput {
                        ref_: Some("class:rollback_failure".to_string()),
                        name: prefix("import_rollback_class"),
                        description: "must fail".to_string(),
                        json_schema: None,
                        validate_schema: Some(false),
                        collection_ref: Some("collection:missing".to_string()),
                        collection_key: None,
                        condition: None,
                        timestamps: None,
                    }),
                )
                .expect("valid class input should cross the storage boundary"),
            ),
        ])
        .expect("valid operations should form a rollback import plan");
        assert!(backend.apply_import_strict(rollback_plan).await.is_err());
        assert!(
            backend
                .get_import_collection_child_by_name(root_id, &rollback_name)
                .await
                .expect("strict rollback should remain queryable")
                .is_none(),
            "strict import must roll back earlier successful items"
        );

        let best_effort = backend
            .apply_import_best_effort(
                StorageImportPlan::try_new(vec![
                    StorageImportPlanItem::new(
                        0,
                        crate::services::import_boundary::import_operation_to_storage(
                            ApplicationImportOperation::CreateCollection(collection_input(
                                &best_effort_name,
                                "collection:best_effort",
                            )),
                        )
                        .expect("valid collection input should cross the storage boundary"),
                    ),
                    StorageImportPlanItem::new(
                        1,
                        crate::services::import_boundary::import_operation_to_storage(
                            ApplicationImportOperation::CreateClass(ImportClassInput {
                                ref_: Some("class:best_effort_failure".to_string()),
                                name: prefix("import_best_effort_class"),
                                description: "must fail".to_string(),
                                json_schema: None,
                                validate_schema: Some(false),
                                collection_ref: Some("collection:missing".to_string()),
                                collection_key: None,
                                condition: None,
                                timestamps: None,
                            }),
                        )
                        .expect("valid class input should cross the storage boundary"),
                    ),
                ])
                .expect("valid operations should form a best-effort import plan"),
                crate::services::import_boundary::import_mode_to_storage(ImportMode {
                    atomicity: Some(ImportAtomicity::BestEffort),
                    ..ImportMode::default()
                }),
            )
            .await
            .expect("certified backend should apply a best-effort import");
        let (best_effort, aborted) = best_effort.into_parts();
        assert!(!aborted);
        assert_eq!(best_effort.len(), 2);
        assert!(best_effort[0].error().is_none());
        assert!(best_effort[1].error().is_some());

        for name in [&preflight_name, &best_effort_name] {
            let collection = backend
                .get_import_collection_child_by_name(root_id, name)
                .await
                .expect("committed import collection should remain queryable")
                .expect("committed import collection should exist");
            crate::services::storage_boundary::collection_from_storage(collection)
                .expect("backend collection should satisfy application invariants")
                .delete_without_events(pool.get_ref())
                .await
                .expect("import compatibility fixture should be removed");
        }
    }
}

#[actix_web::test]
async fn every_available_storage_backend_supplies_the_complete_task_queue() {
    let _permit = postgres_permit().await;
    let pool = pool();
    let user = crate::tests::create_user_with_params(
        pool.get_ref(),
        &prefix("task_queue_user"),
        "testpassword",
    )
    .await;
    let options = || {
        QueryOptions::new(Vec::new(), Vec::new(), Some(10), None, true)
            .expect("contract query must be valid")
    };

    for backend in available_backends() {
        let task = backend
            .create_task(
                StorageTaskCreateRequest::builder(
                    StorageTaskKind::Import,
                    principal_id(user.id),
                    serde_json::json!({"items": []}),
                    0,
                )
                .idempotency_key(Some(
                    IdempotencyKey::new(prefix("task_queue_key"))
                        .expect("compatibility idempotency key should be valid"),
                ))
                .request_hash(Some(prefix("task_queue_hash")))
                .scope_snapshot(StorageTaskScopeSnapshot::unscoped())
                .try_build(10)
                .expect("task queue request should be valid"),
            )
            .await
            .expect("certified backend should create a task");
        let task_id = task.id();
        assert_eq!(task.kind(), StorageTaskKind::Import);
        assert_eq!(task.status(), StorageTaskStatus::Queued);

        let access = backend
            .get_task_access(task_id)
            .await
            .expect("certified backend should return task access facts");
        assert_eq!(access.into_parts().0.id(), task_id);

        let (tasks, total) = backend
            .list_tasks(StorageTaskListQuery::new(
                Some(principal_id(user.id)),
                Some(StorageTaskKind::Import),
                Some(StorageTaskStatus::Queued),
                options(),
            ))
            .await
            .expect("certified backend should list tasks")
            .into_parts();
        assert_eq!(total, Some(1));
        assert_eq!(tasks.len(), 1);

        let (events, event_total) = backend
            .list_task_events(StorageTaskChildListQuery::new(task_id, options()))
            .await
            .expect("certified backend should list task events")
            .into_parts();
        assert_eq!(event_total, Some(1));
        assert_eq!(events.len(), 1);

        let (results, result_total) = backend
            .list_import_task_results(StorageTaskChildListQuery::new(task_id, options()))
            .await
            .expect("certified backend should list import results")
            .into_parts();
        assert_eq!(result_total, Some(0));
        assert!(results.is_empty());

        backend
            .record_import_results(vec![
                StorageImportResult::builder(task_id, "compatibility", "verify", "succeeded")
                    .item_ref(Some("compatibility:item".to_string()))
                    .build(),
            ])
            .await
            .expect("certified backend should persist import results");
        let (results, result_total) = backend
            .list_import_task_results(StorageTaskChildListQuery::new(task_id, options()))
            .await
            .expect("certified backend should return persisted import results")
            .into_parts();
        assert_eq!(result_total, Some(1));
        assert_eq!(results.len(), 1);

        assert!(
            backend
                .list_export_output_summaries(vec![task_id])
                .await
                .expect("certified backend should list export output summaries")
                .is_empty()
        );
        assert!(
            backend
                .list_backup_output_summaries(vec![task_id])
                .await
                .expect("certified backend should list backup output summaries")
                .is_empty()
        );
        assert!(matches!(
            backend.get_export_output_summary(task_id).await,
            Ok(StorageTaskOutputLookup::Missing)
        ));
        assert!(matches!(
            backend.get_backup_output_summary(task_id).await,
            Ok(StorageTaskOutputLookup::Missing)
        ));
        assert!(matches!(
            backend.get_export_output(task_id).await,
            Ok(StorageTaskOutputLookup::Missing)
        ));
        assert!(matches!(
            backend.get_backup_output(task_id).await,
            Ok(StorageTaskOutputLookup::Missing)
        ));

        hubuum_storage_postgres::test_support::delete_task(pool.get_ref(), task_id)
            .await
            .expect("task queue compatibility fixture should be removed");
    }

    user.delete_without_events(pool.get_ref())
        .await
        .expect("task queue compatibility user should be removed");
}

#[actix_web::test]
async fn every_available_storage_backend_supplies_the_complete_task_state_machine() {
    let _permit = postgres_permit().await;
    let pool = pool();
    let user = crate::tests::create_user_with_params(
        pool.get_ref(),
        &prefix("task_execution_user"),
        "testpassword",
    )
    .await;

    for backend in available_backends() {
        let mut fixture_ids = Vec::new();
        for task_kind in StorageTaskKind::ALL {
            let task = backend
                .create_task(
                    StorageTaskCreateRequest::builder(
                        task_kind,
                        principal_id(user.id),
                        serde_json::json!({"compatibility": true}),
                        1,
                    )
                    .idempotency_key(Some(
                        IdempotencyKey::new(prefix(&format!(
                            "task_execution_{}",
                            task_kind.as_str()
                        )))
                        .expect("compatibility idempotency key should be valid"),
                    ))
                    .request_hash(Some(prefix(&format!(
                        "task_execution_hash_{}",
                        task_kind.as_str()
                    ))))
                    .scope_snapshot(StorageTaskScopeSnapshot::unscoped())
                    .try_build(10)
                    .expect("task execution request should be valid"),
                )
                .await
                .expect("certified backend should create an executable task");
            fixture_ids.push(task.id());
        }
        for task_id in &fixture_ids {
            hubuum_storage_postgres::test_support::prioritize_task(pool.get_ref(), *task_id)
                .await
                .expect("compatibility tasks should be made claim-first");
        }

        let lease_duration = StorageTaskLeaseDuration::from_milliseconds(60_000)
            .expect("compatibility lease duration should be valid");
        assert!(
            backend
                .recover_expired_task_leases(0)
                .await
                .expect("certified backend should recover expired claims")
                .is_empty()
        );

        let mut completed_ids = HashSet::new();
        let mut completed_kinds = HashSet::new();
        for completed_index in 0..StorageTaskKind::ALL.len() {
            let claimed = backend
                .claim_next_task(lease_duration)
                .await
                .expect("certified backend should claim the next task")
                .expect("a compatibility task should be claimable");
            assert!(fixture_ids.contains(&claimed.task().id()));
            assert!(completed_ids.insert(claimed.task().id()));
            assert!(completed_kinds.insert(claimed.task().kind()));
            if completed_index == 0 {
                assert!(
                    backend
                        .renew_task_lease(claimed.lease().clone(), lease_duration)
                        .await
                        .expect("certified backend should renew a live claim")
                );
                backend
                    .append_task_event(StorageTaskEventAppend::new(
                        claimed.lease().clone(),
                        StorageTaskEventInput::new("running", "Compatibility event"),
                    ))
                    .await
                    .expect("certified backend should append a claim-owned event");
            }
            backend
                .update_task_state(
                    StorageTaskActiveUpdate::try_new(
                        claimed.lease().clone(),
                        StorageTaskStatus::Running,
                        StorageTaskResultCounts::try_new(0, 0, 0)
                            .expect("non-negative task counts should be valid"),
                    )
                    .expect("running is an active task status"),
                )
                .await
                .expect("certified backend should update claimed task state");
            let artifact = compatibility_completion_artifact(claimed.task().kind());
            backend
                .complete_task(
                    StorageTaskCompletion::try_new(
                        claimed.task().kind(),
                        StorageTaskTerminalUpdate::try_new(
                            claimed.lease().clone(),
                            StorageTaskStatus::Succeeded,
                            StorageTaskResultCounts::try_new(1, 1, 0)
                                .expect("non-negative task counts should be valid"),
                        )
                        .expect("succeeded is a terminal task status"),
                        StorageTaskEventInput::new("succeeded", "Compatibility completed"),
                        artifact,
                    )
                    .expect("task kind and completion artifact should match"),
                )
                .await
                .expect("certified backend should complete a claimed task");
            match claimed.task().kind() {
                StorageTaskKind::Export => assert!(matches!(
                    backend.get_export_output(claimed.task().id()).await,
                    Ok(StorageTaskOutputLookup::Available(_))
                )),
                StorageTaskKind::Backup => assert!(matches!(
                    backend.get_backup_output(claimed.task().id()).await,
                    Ok(StorageTaskOutputLookup::Available(_))
                )),
                StorageTaskKind::Import
                | StorageTaskKind::Reindex
                | StorageTaskKind::RemoteCall => {}
            }
        }
        assert_eq!(completed_kinds.len(), StorageTaskKind::ALL.len());

        let mismatched_kind_task = backend
            .create_task(
                StorageTaskCreateRequest::builder(
                    StorageTaskKind::Export,
                    principal_id(user.id),
                    serde_json::json!({"compatibility_kind_mismatch": true}),
                    1,
                )
                .idempotency_key(Some(
                    IdempotencyKey::new(prefix("task_execution_kind_mismatch"))
                        .expect("compatibility idempotency key should be valid"),
                ))
                .request_hash(Some(prefix("task_execution_kind_mismatch_hash")))
                .scope_snapshot(StorageTaskScopeSnapshot::unscoped())
                .try_build(10)
                .expect("task kind mismatch request should be valid"),
            )
            .await
            .expect("certified backend should create a kind mismatch fixture");
        fixture_ids.push(mismatched_kind_task.id());
        // This subcase verifies completion validation, not queue scheduling. Claim the
        // exact fixture so a concurrent task from another contract cannot be consumed.
        let mismatched_kind_claim =
            hubuum_storage_postgres::test_support::claim_task_by_id_with_lease(
                pool.get_ref(),
                mismatched_kind_task.id(),
                lease_duration,
            )
            .await
            .expect("certified backend should claim the kind mismatch fixture");
        let mismatched_kind_error = backend
            .complete_task(
                StorageTaskCompletion::try_new(
                    StorageTaskKind::Import,
                    StorageTaskTerminalUpdate::try_new(
                        mismatched_kind_claim.lease().clone(),
                        StorageTaskStatus::Succeeded,
                        StorageTaskResultCounts::try_new(1, 1, 0)
                            .expect("non-negative task counts should be valid"),
                    )
                    .expect("succeeded is a terminal task status"),
                    StorageTaskEventInput::new("succeeded", "Mismatched kind must fail"),
                    StorageTaskCompletionArtifact::None,
                )
                .expect("an import task accepts no completion artifact"),
            )
            .await
            .expect_err("a completion kind that differs from storage must be rejected");
        assert_eq!(mismatched_kind_error.kind(), StorageErrorKind::InvalidInput);
        backend
            .fail_task(StorageTaskFailure::new(
                mismatched_kind_claim.lease().clone(),
                "Compatibility cleanup",
                StorageTaskEventInput::new("failed", "Compatibility cleanup"),
            ))
            .await
            .expect("the rejected completion claim should remain valid");

        let mut failure_fixture_ids = Vec::new();
        for task_kind in StorageTaskKind::ALL {
            let task = backend
                .create_task(
                    StorageTaskCreateRequest::builder(
                        task_kind,
                        principal_id(user.id),
                        serde_json::json!({"compatibility_failure": true}),
                        1,
                    )
                    .idempotency_key(Some(
                        IdempotencyKey::new(prefix(&format!(
                            "task_execution_failure_{}",
                            task_kind.as_str()
                        )))
                        .expect("compatibility idempotency key should be valid"),
                    ))
                    .request_hash(Some(prefix(&format!(
                        "task_execution_failure_hash_{}",
                        task_kind.as_str()
                    ))))
                    .scope_snapshot(StorageTaskScopeSnapshot::unscoped())
                    .try_build(10)
                    .expect("task execution failure request should be valid"),
                )
                .await
                .expect("certified backend should create a failure fixture");
            failure_fixture_ids.push(task.id());
            fixture_ids.push(task.id());
        }
        for task_id in &failure_fixture_ids {
            hubuum_storage_postgres::test_support::prioritize_task(pool.get_ref(), *task_id)
                .await
                .expect("failure fixtures should be made claim-first");
        }
        let failed = backend
            .claim_next_task(lease_duration)
            .await
            .expect("certified backend should claim a failure fixture")
            .expect("a compatibility failure fixture should be claimable");
        assert!(failure_fixture_ids.contains(&failed.task().id()));
        backend
            .fail_task(StorageTaskFailure::new(
                failed.lease().clone(),
                "Compatibility failure",
                StorageTaskEventInput::new("failed", "Compatibility failure"),
            ))
            .await
            .expect("certified backend should fail a claimed task");

        backend
            .purge_expired_export_outputs()
            .await
            .expect("certified backend should purge expired export outputs");
        backend
            .purge_expired_backup_outputs()
            .await
            .expect("certified backend should purge expired backup outputs");

        hubuum_storage_postgres::test_support::delete_tasks(pool.get_ref(), &fixture_ids)
            .await
            .expect("task execution compatibility fixtures should be removed");
    }

    user.delete_without_events(pool.get_ref())
        .await
        .expect("task execution compatibility user should be removed");
}

fn compatibility_completion_artifact(kind: StorageTaskKind) -> StorageTaskCompletionArtifact {
    let output_expires_at =
        chrono::Utc::now() + chrono::Duration::try_hours(1).expect("valid duration");
    match kind {
        StorageTaskKind::Import | StorageTaskKind::Reindex => StorageTaskCompletionArtifact::None,
        StorageTaskKind::Export => StorageTaskCompletionArtifact::Export(
            StorageExportTaskArtifact::builder(
                "application/json",
                crate::storage::StorageExportTaskArtifactContent::Json(serde_json::json!({
                    "compatible": true
                })),
                serde_json::json!({"compatibility": true}),
                serde_json::json!([]),
                output_expires_at,
            )
            .try_build()
            .expect("compatibility export artifact should be valid"),
        ),
        StorageTaskKind::Backup => StorageTaskCompletionArtifact::Backup(
            StorageBackupTaskArtifact::try_new(b"{}".to_vec(), output_expires_at)
                .expect("compatibility backup artifact should be valid"),
        ),
        StorageTaskKind::RemoteCall => {
            StorageTaskCompletionArtifact::RemoteCall(StorageRemoteCallTaskArtifact::new(
                StorageRemoteCallArtifactTarget::new(
                    None,
                    crate::storage::StorageRemoteTargetSubjectType::Collection,
                    ResourceId::new(1).unwrap(),
                    Some(crate::storage::StorageRemoteTargetHttpMethod::Get),
                    "https://compatibility.invalid",
                ),
                StorageRemoteCallArtifactResponse::new(
                    Some(200),
                    Some(serde_json::json!({})),
                    Some("compatible".to_string()),
                ),
                StorageRemoteCallArtifactOutcome::new(1, true, None),
            ))
        }
    }
}

#[actix_web::test]
async fn every_available_storage_backend_supplies_backup_snapshots() {
    let _permit = postgres_permit().await;

    for backend in available_backends() {
        let (state, history) = backend
            .capture_backup_snapshot(false)
            .await
            .expect("certified backend should supply a state-only backup snapshot")
            .into_parts();
        assert_eq!(
            state.len(),
            crate::storage::StorageBackupStateSection::ALL.len()
        );
        for section in crate::storage::StorageBackupStateSection::ALL {
            assert!(state.contains_key(section));
        }
        assert!(history.is_none());

        let (state, history) = backend
            .capture_backup_snapshot(true)
            .await
            .expect("certified backend should supply a history-inclusive backup snapshot")
            .into_parts();
        assert_eq!(
            state.len(),
            crate::storage::StorageBackupStateSection::ALL.len()
        );
        for section in crate::storage::StorageBackupStateSection::ALL {
            assert!(state.contains_key(section));
        }
        let history = history.expect("history was requested");
        assert_eq!(
            history.len(),
            crate::storage::StorageBackupHistorySection::ALL.len()
        );
        for section in crate::storage::StorageBackupHistorySection::ALL {
            assert!(history.contains_key(section));
        }
    }
}

#[actix_web::test]
async fn every_available_storage_backend_supplies_export_template_lifecycle() {
    let _permit = postgres_permit().await;
    let pool = pool();
    let owner = crate::tests::create_test_user(pool.get_ref()).await;
    let fixture = crate::tests::create_collection_fixture(
        pool.get_ref(),
        &prefix("export_template_collection"),
    )
    .await;
    let collection_id = fixture.collection.id;
    let storage_collection_id = self::collection_id(collection_id);
    let class = NewHubuumClass {
        name: prefix("export_template_class"),
        collection_id,
        json_schema: None,
        validate_schema: Some(false),
        description: "export-template compatibility class".to_string(),
    }
    .save_without_events(pool.get_ref())
    .await
    .expect("export-template compatibility class should be created");
    let event_context = EventContext::user(principal_id(owner.id), None, None);

    for backend in available_backends() {
        let name = prefix("export_template");
        let definition = StorageExportTemplateDefinition::new(
            "compatibility fragment",
            "text/plain",
            "Hello {{ object.name }}",
            "fragment",
        );
        let created = backend
            .create_export_template(StorageExportTemplateCreate::new(
                storage_collection_id,
                name.clone(),
                definition,
                event_context.clone(),
            ))
            .await
            .expect("certified backend should create an export template")
            .into_value();
        let (metadata, created_collection_id, created_name, _) = created.into_parts();
        let template_id = hubuum_domain::ExportTemplateId::from(metadata.id());
        assert_eq!(created_collection_id, storage_collection_id);
        assert_eq!(created_name, name);

        let loaded = backend
            .get_export_template(template_id)
            .await
            .expect("certified backend should load an export template");
        assert_eq!(
            loaded.into_parts().0.id(),
            ResourceId::new(template_id.id()).unwrap()
        );

        let (templates, total) = backend
            .list_export_templates(StorageExportTemplateListQuery::within_collections(
                vec![storage_collection_id],
                QueryOptions::new(Vec::new(), Vec::new(), Some(10), None, true)
                    .expect("contract query must be valid"),
            ))
            .await
            .expect("certified backend should list export templates")
            .into_parts();
        assert_eq!(total, Some(1));
        assert_eq!(templates.len(), 1);

        let siblings = backend
            .list_export_templates_in_collection(storage_collection_id, Some(template_id))
            .await
            .expect("certified backend should list collection template siblings");
        assert!(siblings.is_empty());

        let replacement_name = format!("{name}_updated");
        let replaced = backend
            .replace_export_template(StorageExportTemplateReplace::new(
                template_id,
                storage_collection_id,
                replacement_name.clone(),
                StorageExportTemplateDefinition::new(
                    "updated compatibility fragment",
                    "text/plain",
                    "Updated {{ object.name }}",
                    "fragment",
                ),
                event_context.clone(),
            ))
            .await
            .expect("certified backend should replace an export template")
            .into_value();
        assert_eq!(replaced.into_parts().2, replacement_name);

        backend
            .delete_export_template(StorageExportTemplateDelete::new(
                template_id,
                event_context.clone(),
            ))
            .await
            .expect("certified backend should delete an export template")
            .into_value();
        assert!(backend.get_export_template(template_id).await.is_err());
    }

    class
        .delete_without_events(pool.get_ref())
        .await
        .expect("export-template compatibility class should be removed");
    fixture
        .cleanup()
        .await
        .expect("export-template compatibility collection should be removed");
    owner
        .delete_without_events(pool.get_ref())
        .await
        .expect("export-template compatibility owner should be removed");
}

#[actix_web::test]
async fn every_available_storage_backend_supplies_remote_target_lifecycle() {
    let _permit = postgres_permit().await;
    let pool = pool();
    let owner = crate::tests::create_test_user(pool.get_ref()).await;
    let fixture = crate::tests::create_collection_fixture(
        pool.get_ref(),
        &prefix("remote_target_collection"),
    )
    .await;
    let collection_id = fixture.collection.id;
    let storage_collection_id = self::collection_id(collection_id);
    let event_context = EventContext::user(principal_id(owner.id), None, None);

    for backend in available_backends() {
        let name = prefix("remote_target");
        let created = backend
            .create_remote_target(StorageRemoteTargetCreate::new(
                storage_collection_id,
                name.clone(),
                StorageRemoteTargetDefinition::new(
                    "Compatibility remote target",
                    StorageRemoteTargetTransport::try_new(
                        crate::storage::StorageRemoteTargetHttpMethod::Get,
                        "https://compatibility.invalid/collections/{{ collection.id }}",
                        serde_json::json!({}),
                        None,
                        serde_json::json!({"type": "none"}),
                        1_000,
                    )
                    .expect("valid compatibility remote-target transport"),
                    StorageRemoteTargetPolicy::try_new(
                        None,
                        vec![crate::storage::StorageRemoteTargetSubjectType::Collection],
                        true,
                    )
                    .expect("valid compatibility remote-target policy"),
                ),
                event_context.clone(),
            ))
            .await
            .expect("certified backend should create a remote target")
            .into_value();
        let target_id = hubuum_domain::RemoteTargetId::from(created.metadata().id());
        assert_eq!(created.collection_id(), storage_collection_id);

        let loaded = backend
            .get_remote_target(target_id)
            .await
            .expect("certified backend should load a remote target");
        assert_eq!(
            loaded.metadata().id(),
            ResourceId::new(target_id.id()).unwrap()
        );

        let (targets, total) = backend
            .list_remote_targets(StorageRemoteTargetListQuery::new(
                vec![storage_collection_id],
                QueryOptions::new(Vec::new(), Vec::new(), Some(10), None, true)
                    .expect("contract query must be valid"),
            ))
            .await
            .expect("certified backend should list remote targets")
            .into_parts();
        assert_eq!(total, Some(1));
        assert!(
            targets
                .iter()
                .any(|target| target.metadata().id().id() == target_id.id())
        );

        let updated = backend
            .update_remote_target(StorageRemoteTargetUpdate::new(
                target_id,
                StorageRemoteTargetPatch::new()
                    .with_name(Some(format!("{name}_updated")))
                    .with_enabled(Some(false)),
                event_context.clone(),
            ))
            .await
            .expect("certified backend should update a remote target")
            .into_value();
        let (metadata, _, updated_name, definition) = updated.into_parts();
        let (_, _, policy) = definition.into_parts();
        let (_, allowed_subject_types, enabled) = policy.into_parts();
        assert_eq!(metadata.id().id(), target_id.id());
        assert_eq!(updated_name, format!("{name}_updated"));
        assert_eq!(
            allowed_subject_types,
            [crate::storage::StorageRemoteTargetSubjectType::Collection]
        );
        assert!(!enabled);

        backend
            .record_remote_target_invocation(StorageRemoteTargetInvocation::new(
                target_id,
                hubuum_domain::TaskId::new(12345).unwrap(),
                crate::storage::StorageRemoteTargetSubjectType::Collection,
                ResourceId::new(collection_id).unwrap(),
                event_context.clone(),
            ))
            .await
            .expect("certified backend should record remote-target invocation provenance")
            .into_value();
        backend
            .delete_remote_target(StorageRemoteTargetDelete::new(
                target_id,
                event_context.clone(),
            ))
            .await
            .expect("certified backend should delete a remote target")
            .into_value();
        assert!(backend.get_remote_target(target_id).await.is_err());
    }

    fixture
        .cleanup()
        .await
        .expect("remote-target compatibility fixture should be removed");
    owner
        .delete_without_events(pool.get_ref())
        .await
        .expect("remote-target compatibility user should be removed");
}

#[actix_web::test]
async fn every_available_storage_backend_supplies_restore_lifecycle_and_coordination() {
    let _permit = postgres_permit().await;
    let pool = pool();
    let now = chrono::Utc::now();
    let instance_id = uuid::Uuid::new_v4();
    let mut staged_ids = Vec::new();

    for backend in available_backends() {
        let label = prefix("restore");
        let job = backend
            .stage_restore(
                StorageRestoreStageCreate::try_new(
                    StorageRestoreInitiator::try_new(None, "compatibility", label.clone())
                        .expect("valid restore initiator"),
                    b"{}".to_vec(),
                    StorageRestoreArtifactSummary::try_new(
                        2,
                        "44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a",
                    )
                    .expect("valid restore artifact"),
                    "b".repeat(64),
                    serde_json::json!({"compatible": true}),
                    now + chrono::Duration::try_hours(1).expect("valid duration"),
                )
                .expect("valid restore staging request"),
            )
            .await
            .expect("certified backend should stage a restore artifact");
        let job_id = job.summary().id();
        staged_ids.push(job_id);
        assert_eq!(job.summary().status(), StorageRestoreJobStatus::Validated);

        let loaded = backend
            .get_restore_job(job_id)
            .await
            .expect("certified backend should load staged restore bytes");
        let (loaded_summary, document, capability_hash) = loaded.into_parts();
        assert_eq!(loaded_summary.id(), job_id);
        assert_eq!(document, b"{}".to_vec());
        assert_eq!(capability_hash, "b".repeat(64));

        let status = backend
            .get_restore_status(job_id)
            .await
            .expect("certified backend should load document-free restore status");
        let (status_summary, status_capability_hash, validation) = status.into_parts();
        assert_eq!(status_summary.status(), StorageRestoreJobStatus::Validated);
        assert_eq!(status_capability_hash, "b".repeat(64));
        assert_eq!(validation, serde_json::json!({"compatible": true}));

        let snapshot = backend
            .get_restore_coordinator_snapshot()
            .await
            .expect("certified backend should read restore coordination state");
        assert!(snapshot.maintenance_state().is_normal());
        assert_eq!(snapshot.restore_job_id(), None);

        let local_idle = || true;
        let tick = backend
            .tick_restore_coordinator(instance_id, &local_idle, false)
            .await
            .expect("certified backend should publish a coordinator heartbeat");
        assert!(tick.maintenance_state().is_normal());
        let (generation, instances) = backend
            .get_restore_drain_state(
                tick.backend_now() - chrono::Duration::try_minutes(1).expect("valid duration"),
            )
            .await
            .expect("certified backend should report live restore coordinators")
            .into_parts();
        let instance = instances
            .into_iter()
            .find(|instance| instance.instance_id() == instance_id)
            .expect("compatibility coordinator should be visible");
        assert_eq!(instance.maintenance_generation(), generation);
        assert!(!instance.is_drained());
        backend
            .remove_restore_instance(instance_id)
            .await
            .expect("certified backend should remove coordinator membership");

        backend
            .fail_restore_and_resume(StorageRestoreFailure::new(job_id, "compatibility failure"))
            .await
            .expect("certified backend should atomically fail a restore");
        let failed = backend
            .get_restore_job(job_id)
            .await
            .expect("failed restore should remain queryable");
        let (failed_summary, failed_document, _) = failed.into_parts();
        assert_eq!(failed_summary.status(), StorageRestoreJobStatus::Failed);
        assert!(failed_document.is_empty());

        backend
            .resume_maintenance_without_restore()
            .await
            .expect("orphaned-maintenance recovery should be idempotent");
        backend
            .resume_terminal_restore(job_id)
            .await
            .expect("terminal-restore recovery should be idempotent");

        let expired_label = prefix("expired_restore");
        let expired = backend
            .stage_restore(
                StorageRestoreStageCreate::try_new(
                    StorageRestoreInitiator::try_new(None, "compatibility", expired_label.clone())
                        .expect("valid restore initiator"),
                    b"{}".to_vec(),
                    StorageRestoreArtifactSummary::try_new(
                        2,
                        "44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a",
                    )
                    .expect("valid restore artifact"),
                    "d".repeat(64),
                    serde_json::json!({"compatible": true}),
                    now - chrono::Duration::try_minutes(1).expect("valid duration"),
                )
                .expect("valid restore staging request"),
            )
            .await
            .expect("certified backend should stage an expiring restore artifact");
        let expired_id = expired.summary().id();
        staged_ids.push(expired_id);
        assert!(
            backend
                .expire_restore_stage(expired_id)
                .await
                .expect("certified backend should expire a validated restore")
        );
        let expired_status = backend
            .get_restore_status(expired_id)
            .await
            .expect("expired restore should remain queryable");
        let (expired_summary, _, _) = expired_status.into_parts();
        assert_eq!(expired_summary.status(), StorageRestoreJobStatus::Expired);
    }

    hubuum_storage_postgres::test_support::delete_restore_jobs(pool.get_ref(), &staged_ids)
        .await
        .expect("restore compatibility fixtures should be removed");
}

#[actix_web::test]
async fn every_available_storage_backend_supplies_collection_lifecycle() {
    let _permit = postgres_permit().await;
    let pool = pool();
    let group = crate::tests::create_test_group(pool.get_ref()).await;

    for backend in available_backends() {
        let command = StorageCollectionCreate::new(
            prefix("collection_lifecycle"),
            "collection lifecycle",
            group_id(group.id),
            None,
        );
        let collections = backend.collection_store();
        let created = backend
            .collection_store()
            .create_collection(command, &EventContext::system())
            .await
            .expect("certified backend should create collections")
            .into_value();
        let updated = collections
            .update_collection(
                created.id(),
                StorageCollectionUpdate::new(
                    None,
                    Some("updated collection lifecycle".to_string()),
                ),
                &EventContext::system(),
            )
            .await
            .expect("certified backend should update collections")
            .into_value();
        assert_eq!(updated.description(), "updated collection lifecycle");
        let moved = collections
            .move_collection(created.id(), collection_id(1), &EventContext::system())
            .await
            .expect("certified backend should move collections")
            .into_value();
        assert_eq!(moved.parent_collection_id(), Some(collection_id(1)));
        let loaded = collections
            .get_collection(created.id())
            .await
            .expect("certified backend should load collections by id");
        assert_eq!(loaded, moved);
        let children = collections
            .list_collection_children(collection_id(1))
            .await
            .expect("certified backend should list direct collection children");
        assert!(children.iter().any(|child| child.id() == created.id()));
        let ancestors = collections
            .list_collection_ancestors(created.id())
            .await
            .expect("certified backend should list collection ancestors");
        assert_eq!(
            ancestors.first().map(|ancestor| ancestor.id()),
            Some(collection_id(1))
        );
        collections
            .delete_collection(created.id(), &EventContext::system())
            .await
            .expect("certified backend should delete collections")
            .into_value();
    }

    group
        .delete_without_events(pool.get_ref())
        .await
        .expect("collection record compatibility group should be removed");
}

#[actix_web::test]
async fn every_available_storage_backend_supplies_class_lifecycle() {
    let _permit = postgres_permit().await;
    let pool = pool();
    let group = crate::tests::create_test_group(pool.get_ref()).await;

    for backend in available_backends() {
        let collections = backend.collection_store();
        let collection = collections
            .create_collection(
                StorageCollectionCreate::new(
                    prefix("class_lifecycle_collection"),
                    "class lifecycle collection",
                    group_id(group.id),
                    None,
                ),
                &EventContext::system(),
            )
            .await
            .expect("certified backend should create the class collection")
            .into_value();
        let classes = backend.class_store();
        let class_name = prefix("class_lifecycle");
        let created = classes
            .create_class(
                StorageClassCreate::builder(&class_name, collection.id(), "class lifecycle")
                    .json_schema(Some(serde_json::json!({
                        "type": "object",
                        "properties": {"name": {"type": "string"}}
                    })))
                    .validate_schema(true)
                    .build(),
                &EventContext::system(),
            )
            .await
            .expect("certified backend should create classes")
            .into_value();
        assert_eq!(created.collection_id(), collection.id());
        assert!(created.validates_schema());

        let resolved_by_id = classes
            .resolve_class(StorageClassSelector::Id(created.id()))
            .await
            .expect("certified backend should resolve classes by id");
        assert_eq!(resolved_by_id.class().name(), class_name);
        let resolved_by_name = classes
            .resolve_class(StorageClassSelector::Name(class_name))
            .await
            .expect("certified backend should resolve classes by name");
        assert_eq!(resolved_by_name.class().id(), created.id());

        let updated = classes
            .update_class(
                &resolved_by_id,
                StorageClassUpdate::builder()
                    .description(Some("updated class lifecycle".to_string()))
                    .build(),
                &EventContext::system(),
            )
            .await
            .expect("certified backend should update classes")
            .into_value();
        assert_eq!(updated.description(), "updated class lifecycle");
        assert_eq!(
            classes
                .resolve_class_names(vec![created.id(), created.id()])
                .await
                .expect("certified backend should resolve a complete class-name set"),
            vec![(created.id(), updated.name().to_string())]
        );
        let missing = classes
            .resolve_class_names(vec![
                created.id(),
                ClassId::new(i32::MAX).expect("maximum class id is positive"),
            ])
            .await
            .expect_err("certified backend must reject a partial class-name mapping");
        assert_eq!(missing.kind(), StorageErrorKind::NotFound);

        let updated_target = classes
            .resolve_class(StorageClassSelector::Id(updated.id()))
            .await
            .expect("certified backend should resolve the updated class");
        classes
            .delete_class(&updated_target, &EventContext::system())
            .await
            .expect("certified backend should delete classes")
            .into_value();
        assert_eq!(
            classes
                .resolve_class(StorageClassSelector::Id(updated.id()))
                .await
                .err()
                .expect("deleted classes must not resolve")
                .kind(),
            StorageErrorKind::NotFound
        );
        collections
            .delete_collection(collection.id(), &EventContext::system())
            .await
            .expect("class lifecycle collection should be removable")
            .into_value();
    }

    group
        .delete_without_events(pool.get_ref())
        .await
        .expect("class lifecycle group should be removed");
}

#[actix_web::test]
async fn every_available_storage_backend_supplies_object_lifecycle() {
    let _permit = postgres_permit().await;
    let pool = pool();
    let group = crate::tests::create_test_group(pool.get_ref()).await;

    for backend in available_backends() {
        let collections = backend.collection_store();
        let collection = collections
            .create_collection(
                StorageCollectionCreate::new(
                    prefix("object_lifecycle_collection"),
                    "object lifecycle collection",
                    group_id(group.id),
                    None,
                ),
                &EventContext::system(),
            )
            .await
            .expect("certified backend should create the object collection")
            .into_value();
        let classes = backend.class_store();
        let class = classes
            .create_class(
                StorageClassCreate::builder(
                    prefix("object_lifecycle_class"),
                    collection.id(),
                    "object lifecycle class",
                )
                .json_schema(Some(serde_json::json!({
                    "type": "object",
                    "properties": {"value": {"type": "integer"}}
                })))
                .validate_schema(true)
                .build(),
                &EventContext::system(),
            )
            .await
            .expect("certified backend should create the object class")
            .into_value();
        let resolved_class = classes
            .resolve_class(StorageClassSelector::Id(class.id()))
            .await
            .expect("certified backend should resolve the object class");
        let objects = backend.object_store();
        let object_name = prefix("object_lifecycle");
        let create = StorageObjectCreate::new(
            &object_name,
            collection.id(),
            class.id(),
            serde_json::json!({"value": 1}),
            "object lifecycle",
        );
        objects
            .validate_object_create(create.clone())
            .await
            .expect("certified backend should validate object creation");
        let created = objects
            .create_object(&resolved_class, create, &EventContext::system())
            .await
            .expect("certified backend should create objects")
            .into_value();
        objects
            .validate_object(created.clone())
            .await
            .expect("certified backend should validate stored objects");

        let loaded = objects
            .get_object(created.id())
            .await
            .expect("certified backend should load objects by id");
        assert_eq!(loaded.object(), &created);
        let resolved = objects
            .resolve_object(StorageObjectSelector::Names {
                class_name: class.name().to_string(),
                object_name: object_name.clone(),
            })
            .await
            .expect("certified backend should resolve objects by name");
        assert_eq!(resolved.object().id(), created.id());

        let update = StorageObjectUpdate::builder()
            .description(Some("updated object lifecycle".to_string()))
            .build();
        objects
            .validate_object_update(created.id(), update.clone())
            .await
            .expect("certified backend should validate object updates");
        let updated = objects
            .update_object(&resolved, update, &EventContext::system())
            .await
            .expect("certified backend should update objects")
            .into_value();
        assert_eq!(updated.description(), "updated object lifecycle");

        let patch_document = serde_json::from_value(serde_json::json!([
            {"op": "replace", "path": "/value", "value": 2}
        ]))
        .expect("object lifecycle patch should be valid");
        let patch: StorageObjectDataPatch =
            crate::services::storage_boundary::object_patch_to_storage(patch_document)
                .expect("object lifecycle patch should cross the storage boundary");
        let updated_target = objects
            .get_object(created.id())
            .await
            .expect("updated object should remain resolvable");
        let patched = objects
            .patch_object_data(&updated_target, patch, &EventContext::system())
            .await
            .expect("certified backend should patch object data")
            .into_value();
        assert_eq!(patched.data(), &serde_json::json!({"value": 2}));

        let delete_target = objects
            .get_object(created.id())
            .await
            .expect("patched object should remain resolvable");
        objects
            .delete_object(&delete_target, &EventContext::system())
            .await
            .expect("certified backend should delete objects")
            .into_value();
        assert_eq!(
            objects
                .get_object(created.id())
                .await
                .err()
                .expect("deleted objects must not resolve")
                .kind(),
            StorageErrorKind::NotFound
        );
        classes
            .delete_class(&resolved_class, &EventContext::system())
            .await
            .expect("object lifecycle class should be removable")
            .into_value();
        collections
            .delete_collection(collection.id(), &EventContext::system())
            .await
            .expect("object lifecycle collection should be removable")
            .into_value();
    }

    group
        .delete_without_events(pool.get_ref())
        .await
        .expect("object lifecycle group should be removed");
}

#[actix_web::test]
async fn every_available_storage_backend_supplies_relation_lifecycle() {
    let _permit = postgres_permit().await;
    let pool = pool();
    let group = crate::tests::create_test_group(pool.get_ref()).await;

    for backend in available_backends() {
        let collections = backend.collection_store();
        let collection = collections
            .create_collection(
                StorageCollectionCreate::new(
                    prefix("relation_lifecycle_collection"),
                    "relation lifecycle collection",
                    group_id(group.id),
                    None,
                ),
                &EventContext::system(),
            )
            .await
            .expect("certified backend should create the relation collection")
            .into_value();
        let classes = backend.class_store();
        let from_class = classes
            .create_class(
                StorageClassCreate::builder(
                    prefix("relation_lifecycle_from_class"),
                    collection.id(),
                    "relation lifecycle source class",
                )
                .build(),
                &EventContext::system(),
            )
            .await
            .expect("certified backend should create the source class")
            .into_value();
        let to_class = classes
            .create_class(
                StorageClassCreate::builder(
                    prefix("relation_lifecycle_to_class"),
                    collection.id(),
                    "relation lifecycle target class",
                )
                .build(),
                &EventContext::system(),
            )
            .await
            .expect("certified backend should create the target class")
            .into_value();

        let class_relations = backend.class_relation_store();
        let prepared_class_relation = class_relations
            .prepare_class_relation(
                StorageClassRelationCreate::builder(from_class.id(), to_class.id()).build(),
            )
            .await
            .expect("certified backend should prepare class relations");
        assert_eq!(prepared_class_relation.from_class().id(), from_class.id());
        assert_eq!(prepared_class_relation.to_class().id(), to_class.id());
        let class_relation = class_relations
            .create_class_relation(&prepared_class_relation, &EventContext::system())
            .await
            .expect("certified backend should create class relations")
            .into_value();
        let class_relation_id = ClassRelationId::from(class_relation.relation().metadata().id());
        let resolved_class_relation = class_relations
            .resolve_class_relation(class_relation_id)
            .await
            .expect("certified backend should resolve class relations");
        assert_eq!(
            resolved_class_relation.relation(),
            class_relation.relation()
        );

        let objects = backend.object_store();
        let from_object = objects
            .create_object(
                &classes
                    .resolve_class(StorageClassSelector::Id(from_class.id()))
                    .await
                    .expect("source class should resolve"),
                StorageObjectCreate::new(
                    prefix("relation_lifecycle_from_object"),
                    collection.id(),
                    from_class.id(),
                    serde_json::json!({}),
                    "relation lifecycle source object",
                ),
                &EventContext::system(),
            )
            .await
            .expect("certified backend should create the source object")
            .into_value();
        let to_object = objects
            .create_object(
                &classes
                    .resolve_class(StorageClassSelector::Id(to_class.id()))
                    .await
                    .expect("target class should resolve"),
                StorageObjectCreate::new(
                    prefix("relation_lifecycle_to_object"),
                    collection.id(),
                    to_class.id(),
                    serde_json::json!({}),
                    "relation lifecycle target object",
                ),
                &EventContext::system(),
            )
            .await
            .expect("certified backend should create the target object")
            .into_value();

        let object_relations = backend.object_relation_store();
        let prepared_object_relation = object_relations
            .prepare_object_relation(StorageObjectRelationCreateSelector::Explicit(
                StorageObjectRelationCreate::new(
                    from_object.id(),
                    to_object.id(),
                    class_relation_id,
                ),
            ))
            .await
            .expect("certified backend should prepare object relations");
        assert_eq!(prepared_object_relation.from_object(), &from_object);
        assert_eq!(prepared_object_relation.to_object(), &to_object);
        let object_relation = object_relations
            .create_object_relation(&prepared_object_relation, &EventContext::system())
            .await
            .expect("certified backend should create object relations")
            .into_value();
        let object_relation_id = ObjectRelationId::from(object_relation.relation().metadata().id());
        let resolved_object_relation = object_relations
            .resolve_object_relation(StorageObjectRelationSelector::Id(object_relation_id))
            .await
            .expect("certified backend should resolve object relations");
        assert_eq!(
            resolved_object_relation.relation(),
            object_relation.relation()
        );

        object_relations
            .delete_object_relation(&resolved_object_relation, &EventContext::system())
            .await
            .expect("certified backend should delete object relations")
            .into_value();
        assert_eq!(
            object_relations
                .resolve_object_relation(StorageObjectRelationSelector::Id(object_relation_id))
                .await
                .err()
                .expect("deleted object relations must not resolve")
                .kind(),
            StorageErrorKind::NotFound
        );
        for object in [from_object, to_object] {
            let target = objects
                .get_object(object.id())
                .await
                .expect("relation lifecycle object should resolve for cleanup");
            objects
                .delete_object(&target, &EventContext::system())
                .await
                .expect("relation lifecycle object should be removable")
                .into_value();
        }
        class_relations
            .delete_class_relation(&resolved_class_relation, &EventContext::system())
            .await
            .expect("certified backend should delete class relations")
            .into_value();
        assert_eq!(
            class_relations
                .resolve_class_relation(class_relation_id)
                .await
                .err()
                .expect("deleted class relations must not resolve")
                .kind(),
            StorageErrorKind::NotFound
        );
        for class in [from_class, to_class] {
            let target = classes
                .resolve_class(StorageClassSelector::Id(class.id()))
                .await
                .expect("relation lifecycle class should resolve for cleanup");
            classes
                .delete_class(&target, &EventContext::system())
                .await
                .expect("relation lifecycle class should be removable")
                .into_value();
        }
        collections
            .delete_collection(collection.id(), &EventContext::system())
            .await
            .expect("relation lifecycle collection should be removable")
            .into_value();
    }

    group
        .delete_without_events(pool.get_ref())
        .await
        .expect("relation lifecycle group should be removed");
}

#[actix_web::test]
async fn every_available_storage_backend_supplies_local_authorization_data() {
    let _permit = postgres_permit().await;
    let pool = pool();
    let user = crate::tests::create_user_with_params(
        pool.get_ref(),
        &prefix("authorization_user"),
        "testpassword",
    )
    .await;
    let group = crate::tests::create_test_group(pool.get_ref()).await;
    group
        .add_member_without_events(pool.get_ref(), &user)
        .await
        .expect("authorization compatibility membership should be created");
    let collection = crate::tests::create_collection_fixture(
        pool.get_ref(),
        &prefix("authorization_collection"),
    )
    .await;
    let needle = prefix("authorization_resource");
    let fixture = crate::tests::create_object_fixture(
        pool.get_ref(),
        collection,
        NewHubuumClass {
            name: format!("{needle}_class"),
            collection_id: 0,
            json_schema: None,
            validate_schema: Some(false),
            description: "authorization compatibility class".to_string(),
        },
        vec![NewHubuumObject {
            name: format!("{needle}_object"),
            collection_id: 0,
            hubuum_class_id: 0,
            data: serde_json::json!({}),
            description: "authorization compatibility object".to_string(),
        }],
    )
    .await
    .expect("authorization compatibility resource fixture should be created");
    let collection_id = collection_id(fixture.collection_id());
    let principal_id = principal_id(user.id);
    let group_id = group_id(group.id);

    for backend in available_backends() {
        let principal = backend
            .get_authorization_principal(principal_id)
            .await
            .expect("certified backend should supply authorization principal facts");
        assert!(principal.group_ids().contains(&group_id));

        let membership = StorageAuthorizationGroupMembershipQuery::new(
            principal_id,
            &group.groupname,
            LOCAL_IDENTITY_SCOPE,
        );
        assert!(
            backend
                .is_authorization_principal_group_member(membership)
                .await
                .expect("certified backend should query group membership")
        );

        let classes = backend
            .list_authorization_classes(StorageAuthorizationResourceIds::new([
                ResourceId::new(fixture.class.id).unwrap(),
                ResourceId::new(fixture.class.id).unwrap(),
            ]))
            .await
            .expect("certified backend should project authorization class facts");
        assert_eq!(classes.len(), 1);
        assert_eq!(classes[0].id(), ClassId::new(fixture.class.id).unwrap());
        assert_eq!(classes[0].collection_id(), collection_id);

        let objects = backend
            .list_authorization_objects(StorageAuthorizationResourceIds::new([
                ResourceId::new(fixture.objects[0].id).unwrap(),
                ResourceId::new(fixture.objects[0].id).unwrap(),
            ]))
            .await
            .expect("certified backend should project authorization object facts");
        assert_eq!(objects.len(), 1);
        assert_eq!(
            objects[0].id(),
            ObjectId::new(fixture.objects[0].id).unwrap()
        );
        assert_eq!(objects[0].collection_id(), collection_id);
        assert_eq!(
            objects[0].class_id(),
            ClassId::new(fixture.class.id).unwrap()
        );
        assert_eq!(objects[0].name(), fixture.objects[0].name);

        let access_query = || {
            StorageAuthorizationCollectionAccessQuery::new(
                principal_id,
                collection_id,
                [StorageAuthorizationPermission::ReadCollection],
            )
        };
        let batch_access_query = || {
            StorageAuthorizationCollectionsAccessQuery::new(
                principal_id,
                [collection_id, collection_id],
                [StorageAuthorizationPermission::ReadCollection],
            )
        };
        assert!(
            !backend
                .authorize_local_collection(access_query())
                .await
                .expect("missing local grant should deny")
        );
        assert!(
            !backend
                .authorize_local_collections(batch_access_query())
                .await
                .expect("missing local batch grant should deny")
        );

        let key = StorageAuthorizationGrantKey::new(collection_id, group_id);
        backend
            .apply_local_collection_grant(StorageAuthorizationGrantMutation::new(
                key,
                [StorageAuthorizationPermission::ReadCollection],
                false,
                EventContext::system(),
            ))
            .await
            .expect("certified backend should apply a local grant")
            .into_value();
        let grant = backend
            .get_local_collection_grant(key)
            .await
            .expect("certified backend should load a local grant")
            .expect("applied local grant should exist");
        assert!(
            grant
                .permissions()
                .contains(&StorageAuthorizationPermission::ReadCollection)
        );
        let (permission_collection_id, permission_revision, permission_grants) = backend
            .get_local_collection_permission_set(StorageAuthorizationPermissionSetQuery::new(
                collection_id,
                Some(group_id),
            ))
            .await
            .expect("certified backend should load revisioned permission sets")
            .into_parts();
        assert_eq!(permission_collection_id, collection_id);
        assert!(permission_revision.get() > 0);
        assert_eq!(permission_grants.len(), 1);
        assert_eq!(permission_grants[0].group_id(), group_id);
        assert!(
            backend
                .authorize_local_collection(access_query())
                .await
                .expect("applied local grant should authorize")
        );
        assert!(
            backend
                .authorize_local_collections(batch_access_query())
                .await
                .expect("applied local grant should authorize the batch")
        );

        let page_options = || {
            QueryOptions::new(Vec::new(), Vec::new(), None, None, true)
                .expect("contract query must be valid")
        };
        let principal_query =
            || StorageAuthorizationPrincipalCollectionQuery::new(principal_id, collection_id);

        let principal_permissions = backend
            .load_principal_collection_permissions(principal_query())
            .await
            .expect("certified backend should project principal collection grants");
        assert!(
            principal_permissions
                .iter()
                .cloned()
                .any(|row| row.into_parts().0.id() == group_id)
        );

        let all_permissions = backend
            .list_all_principal_collection_permissions(principal_id)
            .await
            .expect("certified backend should project all principal collection grants");
        assert!(all_permissions.iter().cloned().any(|row| {
            let (_, row_group, collection) = row.into_parts();
            collection.id() == collection_id && row_group.id() == group_id
        }));

        let (principal_page, principal_total) = backend
            .list_principal_collection_permissions(
                StorageAuthorizationPrincipalCollectionPageQuery::new(
                    principal_query(),
                    page_options(),
                ),
            )
            .await
            .expect("certified backend should page principal collection grants")
            .into_parts();
        assert!(principal_total.is_some_and(|total| total >= 1));
        assert!(!principal_page.is_empty());

        let effective_principal = backend
            .list_effective_principal_collection_permissions(principal_query())
            .await
            .expect("certified backend should project effective principal grants");
        assert!(
            effective_principal
                .iter()
                .cloned()
                .any(|row| row.into_parts().4.id() == group_id)
        );

        let visible = backend
            .list_visible_collections(StorageAuthorizationCollectionVisibilityQuery::new(
                principal_id,
                false,
                StorageAuthorizationPermission::ReadCollection,
                None,
            ))
            .await
            .expect("certified backend should project visible collections");
        assert!(
            visible
                .iter()
                .any(|collection| collection.id() == collection_id)
        );

        let group_query = StorageAuthorizationGroupCollectionQuery::new(
            collection_id,
            group_id,
            StorageAuthorizationPermission::ReadCollection,
        );
        assert!(
            backend
                .has_group_collection_permission(group_query)
                .await
                .expect("certified backend should test group collection grants")
        );

        let effective_group = backend
            .list_effective_group_collection_permissions(collection_id, group_id)
            .await
            .expect("certified backend should project effective group grants");
        assert!(!effective_group.is_empty());

        let groups_query = || {
            StorageAuthorizationCollectionGroupsQuery::new(
                collection_id,
                StorageAuthorizationPermission::ReadCollection,
            )
        };
        let groups = backend
            .load_groups_with_collection_permission(groups_query())
            .await
            .expect("certified backend should list groups with collection grants");
        assert!(groups.iter().any(|candidate| candidate.id() == group_id));

        let (groups_page, groups_total) = backend
            .list_groups_with_collection_permission(
                StorageAuthorizationCollectionGroupsPageQuery::new(groups_query(), page_options()),
            )
            .await
            .expect("certified backend should page groups with collection grants")
            .into_parts();
        assert!(groups_total.is_some_and(|total| total >= 1));
        assert!(!groups_page.is_empty());

        let collections = backend
            .list_local_authorized_collections(StorageAuthorizationCollectionsQuery::new(
                principal_id,
                [StorageAuthorizationPermission::ReadCollection],
            ))
            .await
            .expect("certified backend should run reverse authorization queries");
        assert!(
            collections
                .iter()
                .any(|collection| collection.id() == collection_id)
        );

        let page = backend
            .list_local_collection_grants(StorageAuthorizationCollectionGrantListQuery::new(
                collection_id,
                [StorageAuthorizationPermission::ReadCollection],
                QueryOptions::new(Vec::new(), Vec::new(), None, None, true)
                    .expect("contract query must be valid"),
            ))
            .await
            .expect("certified backend should list local grants");
        let (items, total_count) = page.into_parts();
        assert!(total_count.is_some_and(|total| total >= 1));
        assert!(!items.is_empty());

        let collection_candidates = backend
            .load_authorization_collection_candidates(
                StorageAuthorizationCollectionCandidateQuery::new(
                    None,
                    StorageCandidatePageLimit::try_new(512)
                        .expect("contract candidate page limit should be valid"),
                ),
            )
            .await
            .expect("certified backend should list authorization collection candidates")
            .into_parts()
            .0;
        assert!(
            collection_candidates
                .iter()
                .any(|collection| collection.id() == collection_id)
        );

        let group_candidates = backend
            .load_authorization_group_candidates(StorageAuthorizationGroupCandidateQuery::new(
                QueryOptions::new(
                    vec![ParsedQueryParam {
                        field: FilterField::Description,
                        operator: SearchOperator::Equals { is_negated: false },
                        value: group.description.clone(),
                    }],
                    Vec::new(),
                    None,
                    None,
                    false,
                )
                .expect("authorization candidate query should be valid"),
                StorageCandidatePageLimit::try_new(512)
                    .expect("contract candidate page limit should be valid"),
            ))
            .await
            .expect("certified backend should list authorization group candidates")
            .into_parts()
            .0;
        assert!(
            group_candidates
                .iter()
                .any(|candidate| candidate.id() == group_id)
        );

        let policy_snapshot = backend
            .get_authorization_policy_snapshot()
            .await
            .expect("certified backend should supply the local policy snapshot");
        assert!(policy_snapshot.into_iter().any(|row| {
            let (grant, snapshot_group, collection) = row.into_parts();
            grant.group_id() == group_id
                && snapshot_group.id() == group_id
                && collection.id() == collection_id
        }));

        backend
            .revoke_local_collection_grant(StorageAuthorizationGrantMutation::new(
                key,
                [StorageAuthorizationPermission::ReadCollection],
                false,
                EventContext::system(),
            ))
            .await
            .expect("certified backend should revoke selected local permissions")
            .into_value();
        assert!(
            !backend
                .authorize_local_collection(access_query())
                .await
                .expect("revoked local grant should deny")
        );
        backend
            .revoke_all_local_collection_grants(StorageAuthorizationGrantDelete::new(
                key,
                EventContext::system(),
            ))
            .await
            .expect("certified backend should remove the local grant row")
            .into_value();
    }

    group
        .remove_member_without_events(&user, pool.get_ref())
        .await
        .expect("authorization compatibility membership should be removed");
    group
        .delete_without_events(pool.get_ref())
        .await
        .expect("authorization compatibility group should be removed");
    fixture
        .cleanup()
        .await
        .expect("authorization compatibility collection should be removed");
    user.delete_without_events(pool.get_ref())
        .await
        .expect("authorization compatibility user should be removed");
}

#[actix_web::test]
async fn every_available_storage_backend_supplies_complete_temporal_history() {
    let _permit = postgres_permit().await;
    let pool = pool();
    let fixture =
        crate::tests::create_collection_fixture(pool.get_ref(), &prefix("history_collection"))
            .await;
    let actor_name = prefix("history_actor");
    let actor =
        crate::tests::create_user_with_params(pool.get_ref(), &actor_name, "testpassword").await;
    let at = chrono::Utc::now();

    for backend in available_backends() {
        let collection_options = prepare_db_pagination::<CollectionHistory>(
            &crate::models::search::parse_query_parameter("limit=10")
                .expect("history compatibility query should parse"),
        )
        .expect("collection history pagination should prepare");
        let collection_page = backend
            .list_collection_history(StorageHistoryListQuery::new(
                ResourceId::new(fixture.collection.id).unwrap(),
                collection_options,
                StorageHistoryCollectionScope::All,
            ))
            .await
            .expect("certified backend should list collection history");
        let (collection_rows, total_count) = collection_page.into_parts();
        assert!(!collection_rows.is_empty());
        assert!(total_count.is_some_and(|total| total >= 1));
        assert!(
            backend
                .get_collection_history_as_of(StorageHistoryAsOfQuery::new(
                    ResourceId::new(fixture.collection.id).unwrap(),
                    at,
                ))
                .await
                .expect("certified backend should load collection history as of a point")
                .is_some()
        );

        let class_options = prepare_db_pagination::<HubuumClassHistory>(
            &crate::models::search::parse_query_parameter("limit=10")
                .expect("history compatibility query should parse"),
        )
        .expect("class history pagination should prepare");
        backend
            .list_class_history(StorageHistoryListQuery::new(
                ResourceId::new(i32::MAX).unwrap(),
                class_options,
                StorageHistoryCollectionScope::All,
            ))
            .await
            .expect("certified backend should list class history");
        assert!(
            backend
                .get_class_history_as_of(StorageHistoryAsOfQuery::new(
                    ResourceId::new(i32::MAX).unwrap(),
                    at,
                ))
                .await
                .expect("certified backend should query class history as of a point")
                .is_none()
        );

        let object_options = prepare_db_pagination::<HubuumObjectHistory>(
            &crate::models::search::parse_query_parameter("limit=10")
                .expect("history compatibility query should parse"),
        )
        .expect("object history pagination should prepare");
        backend
            .list_object_history(StorageObjectHistoryListQuery::new(
                ObjectId::new(i32::MAX).unwrap(),
                ClassId::new(i32::MAX).unwrap(),
                object_options,
                StorageHistoryCollectionScope::All,
            ))
            .await
            .expect("certified backend should list object history");
        assert!(
            backend
                .get_object_history_as_of(StorageObjectHistoryAsOfQuery::new(
                    ObjectId::new(i32::MAX).unwrap(),
                    ClassId::new(i32::MAX).unwrap(),
                    at,
                ))
                .await
                .expect("certified backend should query object history as of a point")
                .is_none()
        );

        let template_options = prepare_db_pagination::<ExportTemplateHistory>(
            &crate::models::search::parse_query_parameter("limit=10")
                .expect("history compatibility query should parse"),
        )
        .expect("template history pagination should prepare");
        backend
            .list_export_template_history(StorageHistoryListQuery::new(
                ResourceId::new(i32::MAX).unwrap(),
                template_options,
                StorageHistoryCollectionScope::All,
            ))
            .await
            .expect("certified backend should list template history");
        assert!(
            backend
                .get_export_template_history_as_of(StorageHistoryAsOfQuery::new(
                    ResourceId::new(i32::MAX).unwrap(),
                    at,
                ))
                .await
                .expect("certified backend should query template history as of a point")
                .is_none()
        );

        let remote_target_options = prepare_db_pagination::<RemoteTargetHistory>(
            &crate::models::search::parse_query_parameter("limit=10")
                .expect("history compatibility query should parse"),
        )
        .expect("remote-target history pagination should prepare");
        backend
            .list_remote_target_history(StorageHistoryListQuery::new(
                ResourceId::new(i32::MAX).unwrap(),
                remote_target_options,
                StorageHistoryCollectionScope::All,
            ))
            .await
            .expect("certified backend should list remote-target history");
        assert!(
            backend
                .get_remote_target_history_as_of(StorageHistoryAsOfQuery::new(
                    ResourceId::new(i32::MAX).unwrap(),
                    at,
                ))
                .await
                .expect("certified backend should query remote-target history as of a point")
                .is_none()
        );

        let names = backend
            .resolve_history_principal_names(vec![principal_id(actor.id)])
            .await
            .expect("certified backend should resolve history principal names");
        assert!(names.into_iter().any(|row| {
            let (principal_id, name) = row.into_parts();
            principal_id.id() == actor.id && name == actor_name
        }));
    }

    fixture
        .cleanup()
        .await
        .expect("history compatibility collection should be removed");
    actor
        .delete_without_events(pool.get_ref())
        .await
        .expect("history compatibility actor should be removed");
}

#[actix_web::test]
async fn every_available_storage_backend_supplies_catalog_queries() {
    let _permit = postgres_permit().await;
    let pool = pool();
    let needle = prefix("catalog_query");
    let collection = crate::tests::create_collection_fixture(pool.get_ref(), &needle).await;
    let fixture = crate::tests::create_object_fixture(
        pool.get_ref(),
        collection,
        NewHubuumClass {
            name: format!("{needle}_class"),
            collection_id: 0,
            json_schema: None,
            validate_schema: Some(false),
            description: "catalog compatibility class".to_string(),
        },
        vec![NewHubuumObject {
            name: format!("{needle}_object"),
            collection_id: 0,
            hubuum_class_id: 0,
            data: serde_json::json!({"needle": needle}),
            description: "catalog compatibility object".to_string(),
        }],
    )
    .await
    .expect("catalog compatibility fixture should be created");

    for backend in available_backends() {
        let request = || {
            StorageCatalogListQuery::new(
                QueryOptions::new(
                    vec![ParsedQueryParam {
                        field: FilterField::Name,
                        operator: SearchOperator::Contains { is_negated: false },
                        value: needle.clone(),
                    }],
                    Vec::new(),
                    Some(10),
                    None,
                    true,
                )
                .expect("contract query must be valid"),
                StorageVisibility::new(
                    principal_id(i32::MAX),
                    true,
                    None::<Vec<StorageAuthorizationPermission>>,
                    None,
                ),
            )
        };

        let (collections, collection_total) = backend
            .list_collections(request())
            .await
            .expect("certified backend should list collections")
            .into_parts();
        assert_eq!(collection_total, Some(1));
        assert!(collections.into_iter().any(|row| {
            let (id, ..) = row.into_parts();
            id.id() == fixture.collection.collection.id
        }));

        let (classes, class_total) = backend
            .list_classes(request())
            .await
            .expect("certified backend should list classes")
            .into_parts();
        assert_eq!(class_total, Some(1));
        assert!(classes.into_iter().any(|row| {
            let (id, ..) = row.into_parts();
            id.id() == fixture.class.id
        }));

        let (objects, object_total) = backend
            .list_objects(request())
            .await
            .expect("certified backend should list objects")
            .into_parts();
        assert_eq!(object_total, Some(1));
        assert!(objects.into_iter().any(|row| {
            let (id, ..) = row.into_parts();
            id.id() == fixture.objects[0].id
        }));
    }

    fixture
        .cleanup()
        .await
        .expect("catalog compatibility fixture should be removed");
}

#[actix_web::test]
async fn every_available_storage_backend_supplies_computed_object_queries() {
    let _permit = postgres_permit().await;
    let pool = pool();
    let needle = prefix("computed_object_query");
    let collection = crate::tests::create_collection_fixture(pool.get_ref(), &needle).await;
    let fixture = crate::tests::create_object_fixture(
        pool.get_ref(),
        collection,
        NewHubuumClass {
            name: format!("{needle}_class"),
            collection_id: 0,
            json_schema: None,
            validate_schema: Some(false),
            description: "computed-object compatibility class".to_string(),
        },
        vec![NewHubuumObject {
            name: format!("{needle}_object"),
            collection_id: 0,
            hubuum_class_id: 0,
            data: serde_json::json!({"compatibility": needle}),
            description: "computed-object compatibility object".to_string(),
        }],
    )
    .await
    .expect("computed-object compatibility fixture should be created");
    let owner = crate::tests::create_test_user(pool.get_ref()).await;
    let class_id = ClassId::new(fixture.class.id).expect("persisted class id must be positive");
    let fixture_collection_id = self::collection_id(fixture.collection.collection.id);
    let _ = StorageHandle::postgres(pool.get_ref().clone())
        .create_shared_computed_field(StorageSharedComputedFieldCreate::new(
            class_id,
            fixture_collection_id,
            principal_id(owner.id),
            StorageComputedFieldDefinitionInput::new(
                "compatibility".to_string(),
                "Compatibility".to_string(),
                serde_json::json!({
                    "type": "first_non_null",
                    "paths": ["/compatibility"]
                }),
                "string".to_string(),
            ),
            EventContext::user(principal_id(owner.id), None, None),
        ))
        .await
        .expect("computed-object compatibility definition should be inserted");

    for backend in available_backends() {
        let (options, passthrough) = parse_query_parameter_with_computed_filters_and_passthrough(
            &format!("computed.shared.compatibility__equals={needle}&sort=id"),
            &[],
        )
        .expect("computed compatibility query should parse");
        assert!(passthrough.is_empty());
        let visibility = StorageVisibility::new(
            principal_id(i32::MAX),
            true,
            None::<Vec<StorageAuthorizationPermission>>,
            None,
        );
        let page_limit = crate::pagination::effective_page_limit(&options)
            .expect("computed-object page limit should be valid");
        let execution_options = prepare_db_pagination::<crate::models::HubuumObject>(&options)
            .expect("computed-object execution query should be valid");
        let (rows, total, computed, _) = backend
            .list_computed_objects(StorageComputedObjectListQuery::new(
                ClassId::new(fixture.class.id).expect("persisted class id must be positive"),
                None,
                StorageComputedObjectQueryOptions::try_new(options, execution_options, page_limit)
                    .expect("computed-object queries should be coherent"),
                StorageComputedObjectVisibility::storage(visibility),
                StorageComputedObjectProjection::All,
            ))
            .await
            .expect("certified backend should query computed objects")
            .into_parts();
        assert_eq!(total, Some(1));
        assert_eq!(rows.len(), 1);
        assert_eq!(computed.len(), 1);

        let object = &fixture.objects[0];
        let enriched = backend
            .enrich_objects_with_computed(StorageComputedObjectEnrichmentQuery::new(
                vec![StorageObject::new(
                    StorageRecordMetadata::try_new(
                        ResourceId::new(object.id).expect("persisted resource id must be positive"),
                        object.created_at.and_utc(),
                        object.updated_at.and_utc(),
                        ResourceRevision::new(object.revision.get())
                            .expect("persisted revision must be positive"),
                    )
                    .expect("persisted object timestamps must be ordered"),
                    object.name.clone(),
                    collection_id(object.collection_id),
                    ClassId::new(object.hubuum_class_id)
                        .expect("persisted class id must be positive"),
                    object.data.clone(),
                    object.description.clone(),
                )],
                None,
            ))
            .await
            .expect("certified backend should enrich objects with computed values");
        assert_eq!(enriched.len(), 1);
    }

    fixture
        .cleanup()
        .await
        .expect("computed-object compatibility fixture should be removed");
    owner
        .delete_without_events(pool.get_ref())
        .await
        .expect("computed-object compatibility owner should be removed");
}

#[actix_web::test]
async fn every_available_storage_backend_supplies_computed_fields() {
    let _permit = postgres_permit().await;
    let pool = pool();
    let needle = prefix("computed_fields");
    let owner = crate::tests::create_test_user(pool.get_ref()).await;
    let fixture = crate::tests::create_class_fixture(
        pool.get_ref(),
        crate::tests::create_collection_fixture(pool.get_ref(), &needle).await,
        vec![NewHubuumClass {
            name: format!("{needle}_class"),
            collection_id: 0,
            json_schema: None,
            validate_schema: Some(false),
            description: "computed-field lifecycle compatibility class".to_string(),
        }],
    )
    .await
    .expect("computed-field lifecycle compatibility fixture should be created");
    let class_id =
        ClassId::new(fixture.classes[0].id).expect("persisted class id must be positive");
    let collection_id = self::collection_id(fixture.collection.collection.id);
    let owner_id = principal_id(owner.id);
    let event_context = EventContext::user(owner_id, None, None);
    let definition = |key: &str| {
        StorageComputedFieldDefinitionInput::new(
            key.to_string(),
            "Compatibility".to_string(),
            serde_json::json!({
                "type": "first_non_null",
                "paths": ["/compatibility"]
            }),
            "string".to_string(),
        )
        .with_description("Backend compatibility definition".to_string())
    };

    for backend in available_backends() {
        let initial_state = backend
            .get_computed_field_state(class_id)
            .await
            .expect("certified backend should supply computed-field state");
        assert_eq!(initial_state.class_id(), class_id);

        let (shared, created_state) = backend
            .create_shared_computed_field(StorageSharedComputedFieldCreate::new(
                class_id,
                collection_id,
                owner_id,
                definition("compatibility_shared"),
                event_context.clone(),
            ))
            .await
            .expect("certified backend should create a shared computed field")
            .into_value()
            .into_parts();
        assert_eq!(shared.visibility(), StorageComputedFieldVisibility::Shared);
        assert!(created_state.evaluation_revision() > initial_state.evaluation_revision());

        let shared_rows = backend
            .list_shared_computed_fields(class_id)
            .await
            .expect("certified backend should list shared computed fields");
        assert!(
            shared_rows
                .iter()
                .any(|row| row.metadata().id() == shared.metadata().id())
        );

        let shared_id = ComputedFieldDefinitionId::new(shared.metadata().id().id())
            .expect("persisted computed-field definition id must be positive");
        let loaded = backend
            .get_computed_field(shared_id)
            .await
            .expect("certified backend should load a computed field");
        assert_eq!(loaded.key(), "compatibility_shared");

        let (updated_shared, _) = backend
            .update_shared_computed_field(StorageSharedComputedFieldUpdate::new(
                class_id,
                collection_id,
                shared_id,
                owner_id,
                StorageComputedFieldDefinitionPatch::new()
                    .with_label(Some("Updated compatibility".to_string())),
                event_context.clone(),
            ))
            .await
            .expect("certified backend should update a shared computed field")
            .into_value()
            .into_parts();
        assert_eq!(updated_shared.label(), "Updated compatibility");

        let rebuild_state = backend
            .request_computed_field_rebuild(StorageComputedFieldRebuildRequest::new(
                class_id,
                collection_id,
                Some(owner_id),
            ))
            .await
            .expect("certified backend should request a computed-field rebuild");
        assert_eq!(rebuild_state.class_id(), class_id);
        let rebuild_task_id = rebuild_state
            .active_task_id()
            .expect("rebuild request should identify its task");
        let claim_token = uuid::Uuid::new_v4();
        hubuum_storage_postgres::test_support::assign_task_lease(
            pool.get_ref(),
            rebuild_task_id,
            StorageTaskStatus::Validating,
            claim_token,
            chrono::Utc::now().naive_utc()
                + chrono::Duration::try_minutes(1).expect("valid compatibility lease"),
        )
        .await
        .expect("compatibility rebuild should receive a live backend claim");
        let rebuilt = backend
            .execute_computed_field_rebuild(StorageTaskLease::new(
                rebuild_task_id,
                StorageTaskClaimToken::new(claim_token.to_string()),
            ))
            .await
            .expect("certified backend should execute a claimed computed-field rebuild");
        assert_eq!(rebuilt.status(), StorageTaskStatus::Succeeded);
        let ready_state = backend
            .get_computed_field_state(class_id)
            .await
            .expect("certified backend should expose the completed rebuild state");
        assert_eq!(ready_state.rebuild_status().as_str(), "ready");
        assert_eq!(ready_state.active_task_id(), None);

        let personal_outcome = backend
            .create_personal_computed_field(StoragePersonalComputedFieldCreate::new(
                class_id,
                owner_id,
                definition("compatibility_personal"),
                event_context.clone(),
            ))
            .await
            .expect("certified backend should create a personal computed field");
        assert!(personal_outcome.is_committed());
        let personal = personal_outcome.into_value();
        assert_eq!(
            personal.visibility(),
            StorageComputedFieldVisibility::Personal { owner_id }
        );

        let (personal_rows, total) = backend
            .list_personal_computed_fields(StoragePersonalComputedFieldListQuery::new(
                owner_id,
                Some(class_id),
                QueryOptions::new(Vec::new(), Vec::new(), Some(10), None, true)
                    .expect("contract query must be valid"),
            ))
            .await
            .expect("certified backend should list personal computed fields")
            .into_parts();
        assert_eq!(total, Some(1));
        assert_eq!(personal_rows.len(), 1);

        let personal_id = ComputedFieldDefinitionId::new(personal.metadata().id().id())
            .expect("persisted computed-field definition id must be positive");
        let updated_personal_outcome = backend
            .update_personal_computed_field(StoragePersonalComputedFieldUpdate::new(
                owner_id,
                personal_id,
                StorageComputedFieldDefinitionPatch::new()
                    .with_label(Some("Updated personal compatibility".to_string())),
                event_context.clone(),
            ))
            .await
            .expect("certified backend should update a personal computed field");
        assert!(updated_personal_outcome.is_committed());
        let updated_personal = updated_personal_outcome.into_value();
        assert_eq!(updated_personal.label(), "Updated personal compatibility");

        let deleted_personal = backend
            .delete_personal_computed_field(StoragePersonalComputedFieldDelete::new(
                owner_id,
                personal_id,
                event_context.clone(),
            ))
            .await
            .expect("certified backend should delete a personal computed field");
        assert!(deleted_personal.is_committed());

        let deleted_state = backend
            .delete_shared_computed_field(StorageSharedComputedFieldDelete::new(
                class_id,
                collection_id,
                shared_id,
                owner_id,
                event_context.clone(),
            ))
            .await
            .expect("certified backend should delete a shared computed field")
            .into_value();
        assert_eq!(deleted_state.class_id(), class_id);
    }

    fixture
        .cleanup()
        .await
        .expect("computed-field lifecycle fixture should be removed");
    owner
        .delete_without_events(pool.get_ref())
        .await
        .expect("computed-field lifecycle owner should be removed");
}

#[actix_web::test]
async fn every_available_storage_backend_supplies_object_aggregates() {
    use hubuum_storage_core::{StorageObjectAggregateDimension, StorageObjectAggregateScalarField};

    let _permit = postgres_permit().await;
    let pool = pool();
    let needle = prefix("object_aggregate");
    let collection = crate::tests::create_collection_fixture(pool.get_ref(), &needle).await;
    let fixture = crate::tests::create_object_fixture(
        pool.get_ref(),
        collection,
        NewHubuumClass {
            name: format!("{needle}_class"),
            collection_id: 0,
            json_schema: None,
            validate_schema: Some(false),
            description: "object-aggregate compatibility class".to_string(),
        },
        vec![NewHubuumObject {
            name: format!("{needle}_object"),
            collection_id: 0,
            hubuum_class_id: 0,
            data: serde_json::json!({"compatibility": true}),
            description: "object-aggregate compatibility object".to_string(),
        }],
    )
    .await
    .expect("object-aggregate compatibility fixture should be created");
    let visibility = || {
        StorageVisibility::new(
            principal_id(i32::MAX),
            true,
            None::<Vec<StorageAuthorizationPermission>>,
            None,
        )
    };
    let query = || {
        StorageObjectAggregateQuery::builder(
            StorageObjectAggregateTarget::new(
                ClassId::new(fixture.class.id).expect("persisted class id must be positive"),
                fixture.class.name.clone(),
                collection_id(fixture.class.collection_id),
            ),
            QueryOptions::new(
                vec![
                    ParsedQueryParam {
                        field: FilterField::ClassId,
                        operator: SearchOperator::Equals { is_negated: false },
                        value: fixture.class.id.to_string(),
                    },
                    ParsedQueryParam {
                        field: FilterField::CollectionId,
                        operator: SearchOperator::Equals { is_negated: false },
                        value: fixture.class.collection_id.to_string(),
                    },
                ],
                Vec::new(),
                Some(50),
                None,
                true,
            )
            .expect("contract query must be valid"),
            StorageObjectAggregateSpec::try_new(
                [StorageObjectAggregateDimension::Scalar(
                    StorageObjectAggregateScalarField::Name,
                )],
                [],
                StorageObjectAggregateSort::DimensionsAscending,
            )
            .expect("compatibility aggregate spec should be valid"),
            visibility(),
        )
        .required_permissions([
            StorageAuthorizationPermission::ReadObject,
            StorageAuthorizationPermission::ReadCollection,
        ])
        .page_limit(50)
        .cursor_max_encoded_bytes(4_096)
        .try_build()
        .expect("compatibility aggregate query should be valid")
    };

    for backend in available_backends() {
        let storage_page = backend
            .aggregate_objects(query(), StorageObjectAggregateAuthorization::Storage)
            .await
            .expect("certified backend should aggregate with storage authorization");
        let (rows, total, next_cursor) = storage_page.into_parts();
        assert_eq!(rows.len(), 1);
        assert_eq!(total, Some(1));
        assert!(next_cursor.is_none());

        let delegated_page = backend
            .aggregate_objects(
                query(),
                StorageObjectAggregateAuthorization::Delegated(&AllowAllObjectAggregateAuthorizer),
            )
            .await
            .expect("certified backend should aggregate with delegated authorization");
        let (rows, total, next_cursor) = delegated_page.into_parts();
        assert_eq!(rows.len(), 1);
        assert_eq!(total, Some(1));
        assert!(next_cursor.is_none());
    }

    fixture
        .cleanup()
        .await
        .expect("object-aggregate compatibility fixture should be removed");
}

#[actix_web::test]
async fn delegated_object_aggregation_keeps_one_snapshot_across_authorization_batches() {
    use hubuum_storage_core::{StorageObjectAggregateDimension, StorageObjectAggregateScalarField};

    let _permit = postgres_permit().await;
    let pool = pool();
    let needle = prefix("delegated_object_aggregate_snapshot");
    let collection = crate::tests::create_collection_fixture(pool.get_ref(), &needle).await;
    let initial_objects = (0..3)
        .map(|index| NewHubuumObject {
            name: format!("{needle}_object_{index}"),
            collection_id: 0,
            hubuum_class_id: 0,
            data: serde_json::json!({"snapshot": true}),
            description: "delegated object aggregate snapshot object".to_string(),
        })
        .collect();
    let fixture = crate::tests::create_object_fixture(
        pool.get_ref(),
        collection,
        NewHubuumClass {
            name: format!("{needle}_class"),
            collection_id: 0,
            json_schema: None,
            validate_schema: Some(false),
            description: "delegated object aggregate snapshot class".to_string(),
        },
        initial_objects,
    )
    .await
    .expect("delegated aggregate snapshot fixture should be created");
    let query = StorageObjectAggregateQuery::builder(
        StorageObjectAggregateTarget::new(
            ClassId::new(fixture.class.id).expect("persisted class id must be positive"),
            fixture.class.name.clone(),
            collection_id(fixture.class.collection_id),
        ),
        QueryOptions::new(Vec::new(), Vec::new(), Some(50), None, true)
            .expect("snapshot aggregate query must be valid"),
        StorageObjectAggregateSpec::try_new(
            [StorageObjectAggregateDimension::Scalar(
                StorageObjectAggregateScalarField::Name,
            )],
            [],
            StorageObjectAggregateSort::DimensionsAscending,
        )
        .expect("snapshot aggregate spec must be valid"),
        StorageVisibility::new(
            principal_id(i32::MAX),
            true,
            None::<Vec<StorageAuthorizationPermission>>,
            None,
        ),
    )
    .required_permissions([
        StorageAuthorizationPermission::ReadObject,
        StorageAuthorizationPermission::ReadCollection,
    ])
    .page_limit(50)
    .cursor_max_encoded_bytes(4_096)
    .try_build()
    .expect("snapshot aggregate query must be valid");
    let authorizer = Arc::new(PausingObjectAggregateAuthorizer::new());
    let storage = StorageHandle::postgres(pool.get_ref().clone());
    let aggregate_task = tokio::spawn({
        let authorizer = Arc::clone(&authorizer);
        async move {
            storage
                .aggregate_objects(
                    query,
                    StorageObjectAggregateAuthorization::Delegated(authorizer.as_ref()),
                )
                .await
        }
    });

    tokio::time::timeout(
        Duration::from_secs(10),
        authorizer.first_batch_seen.notified(),
    )
    .await
    .expect("delegated authorization should receive its first candidate batch");
    NewHubuumObject {
        name: format!("{needle}_concurrent_object"),
        collection_id: fixture.class.collection_id,
        hubuum_class_id: fixture.class.id,
        data: serde_json::json!({"snapshot": false}),
        description: "concurrently inserted object".to_string(),
    }
    .save_without_events(pool.get_ref())
    .await
    .expect("concurrent object should be committed while authorization is paused");
    authorizer.resume.notify_one();

    let page = aggregate_task
        .await
        .expect("delegated aggregate task should join")
        .expect("delegated aggregate should succeed");
    let (rows, total, next_cursor) = page.into_parts();
    assert_eq!(rows.len(), 3);
    assert_eq!(total, Some(3));
    assert!(next_cursor.is_none());

    fixture
        .cleanup()
        .await
        .expect("delegated aggregate snapshot fixture should be removed");
}

#[actix_web::test]
async fn every_available_storage_backend_supplies_relation_queries() {
    let _permit = postgres_permit().await;
    let pool = pool();
    let needle = prefix("relation_query");
    let collection = crate::tests::create_collection_fixture(pool.get_ref(), &needle).await;
    let class_one = NewHubuumClass {
        name: format!("{needle}_class_one"),
        collection_id: collection.collection.id,
        json_schema: None,
        validate_schema: Some(false),
        description: "relation compatibility source class".to_string(),
    }
    .save_without_events(pool.get_ref())
    .await
    .expect("source class should be created");
    let class_two = NewHubuumClass {
        name: format!("{needle}_class_two"),
        collection_id: collection.collection.id,
        json_schema: None,
        validate_schema: Some(false),
        description: "relation compatibility target class".to_string(),
    }
    .save_without_events(pool.get_ref())
    .await
    .expect("target class should be created");
    let class_relation = NewHubuumClassRelation {
        from_hubuum_class_id: class_one.id,
        to_hubuum_class_id: class_two.id,
        forward_template_alias: None,
        reverse_template_alias: None,
        from_max_relations: None,
        to_max_relations: None,
    }
    .save_without_events(pool.get_ref())
    .await
    .expect("class relation should be created");
    let object_one = NewHubuumObject {
        name: format!("{needle}_object_one"),
        collection_id: collection.collection.id,
        hubuum_class_id: class_one.id,
        data: serde_json::json!({}),
        description: "relation compatibility source object".to_string(),
    }
    .save_without_events(pool.get_ref())
    .await
    .expect("source object should be created");
    let object_two = NewHubuumObject {
        name: format!("{needle}_object_two"),
        collection_id: collection.collection.id,
        hubuum_class_id: class_two.id,
        data: serde_json::json!({}),
        description: "relation compatibility target object".to_string(),
    }
    .save_without_events(pool.get_ref())
    .await
    .expect("target object should be created");
    let object_relation = NewHubuumObjectRelation {
        from_hubuum_object_id: object_one.id,
        to_hubuum_object_id: object_two.id,
        class_relation_id: class_relation.id,
    }
    .save_without_events(pool.get_ref())
    .await
    .expect("object relation should be created");
    let class_one_id = ResourceId::new(class_one.id).expect("persisted class id must be positive");
    let class_two_id = ResourceId::new(class_two.id).expect("persisted class id must be positive");
    let class_two_typed_id =
        ClassId::new(class_two.id).expect("persisted class id must be positive");
    let class_relation_id = ClassRelationId::new(class_relation.id)
        .expect("persisted class-relation id must be positive");
    let object_one_id = ObjectId::new(object_one.id).expect("persisted object id must be positive");
    let object_one_resource_id =
        ResourceId::new(object_one.id).expect("persisted object id must be positive");
    let object_two_resource_id =
        ResourceId::new(object_two.id).expect("persisted object id must be positive");
    let object_relation_id = ObjectRelationId::new(object_relation.id)
        .expect("persisted object-relation id must be positive");

    for backend in available_backends() {
        let visibility = || {
            StorageVisibility::new(
                principal_id(i32::MAX),
                true,
                None::<Vec<StorageAuthorizationPermission>>,
                None,
            )
        };
        let options = || {
            QueryOptions::new(Vec::new(), Vec::new(), Some(50), None, true)
                .expect("contract query must be valid")
        };

        let (class_relations, class_total) = backend
            .list_class_relations(StorageRelationListQuery::new(options(), visibility()))
            .await
            .expect("certified backend should list class relations")
            .into_parts();
        assert!(class_total.is_some_and(|total| total >= 1));
        assert!(class_relations.into_iter().any(|row| {
            let (id, ..) = row.into_parts();
            id == class_relation_id
        }));

        let (object_relations, object_total) = backend
            .list_object_relations(StorageRelationListQuery::new(options(), visibility()))
            .await
            .expect("certified backend should list object relations")
            .into_parts();
        assert!(object_total.is_some_and(|total| total >= 1));
        assert!(object_relations.into_iter().any(|row| {
            let (id, ..) = row.into_parts();
            id == object_relation_id
        }));

        let (touching_classes, _) = backend
            .list_class_relations_touching(StorageRelationTouchingQuery::new(
                class_one_id,
                options(),
                visibility(),
            ))
            .await
            .expect("certified backend should list class relations touching an id")
            .into_parts();
        assert_eq!(touching_classes.len(), 1);

        let (touching_objects, _) = backend
            .list_object_relations_touching(StorageRelationTouchingQuery::new(
                object_one_resource_id,
                options(),
                visibility(),
            ))
            .await
            .expect("certified backend should list object relations touching an id")
            .into_parts();
        assert_eq!(touching_objects.len(), 1);

        let class_ids = [class_one_id, class_two_id];
        assert_eq!(
            backend
                .list_class_relations_touching_ids(StorageRelationIdsQuery::new(
                    class_ids,
                    visibility(),
                ))
                .await
                .expect("certified backend should query class relations touching ids")
                .len(),
            1
        );
        assert_eq!(
            backend
                .list_class_relations_between_ids(StorageRelationIdsQuery::new(
                    class_ids,
                    visibility(),
                ))
                .await
                .expect("certified backend should query class relations between ids")
                .len(),
            1
        );

        let object_ids = [object_one_resource_id, object_two_resource_id];
        assert_eq!(
            backend
                .list_object_relations_touching_ids(StorageObjectRelationsTouchingIdsQuery::new(
                    [object_one_id],
                    10,
                    visibility(),
                ))
                .await
                .expect("certified backend should query object relations touching ids")
                .len(),
            1
        );
        assert!(
            backend
                .list_object_relations_touching_ids(
                    StorageObjectRelationsTouchingIdsQuery::new([object_one_id], 10, visibility(),)
                        .excluding_relation_ids([object_relation_id]),
                )
                .await
                .expect("certified backend should exclude previously visited relations")
                .is_empty()
        );
        assert_eq!(
            backend
                .list_object_relations_between_ids(StorageRelationIdsQuery::new(
                    object_ids,
                    visibility(),
                ))
                .await
                .expect("certified backend should query object relations between ids")
                .len(),
            1
        );

        let (list_related_classes, _) = backend
            .list_related_classes(StorageRelationGraphQuery::new(
                class_one_id,
                options(),
                visibility(),
            ))
            .await
            .expect("certified backend should traverse related classes")
            .into_parts();
        assert!(!list_related_classes.is_empty());

        let (list_related_objects, _) = backend
            .list_related_objects(StorageRelationGraphQuery::new(
                object_one_resource_id,
                options(),
                visibility(),
            ))
            .await
            .expect("certified backend should traverse related objects")
            .into_parts();
        assert!(!list_related_objects.is_empty());

        let included = backend
            .list_related_objects_for_roots(
                StorageRelatedObjectsForRootsQuery::new(
                    [object_one_id],
                    class_two_typed_id,
                    visibility(),
                )
                .class_relation_id(Some(class_relation_id))
                .direction(StorageRelatedDirection::Any)
                .sort(StorageRelatedSort::Path)
                .max_depth(1)
                .limit(10),
            )
            .await
            .expect("certified backend should traverse directional root graphs");
        assert_eq!(included.len(), 1);

        let bidirectional = backend
            .list_bidirectionally_related_objects_for_roots(
                StorageBidirectionalRelatedObjectsQuery::new(
                    [object_one_id],
                    1,
                    10,
                    false,
                    visibility(),
                ),
            )
            .await
            .expect("certified backend should traverse bidirectional root graphs");
        assert_eq!(bidirectional.len(), 1);
    }

    collection
        .cleanup()
        .await
        .expect("relation compatibility collection should be removed");
}

#[actix_web::test]
async fn every_available_storage_backend_supplies_ranked_unified_search() {
    let _permit = postgres_permit().await;
    let pool = pool();
    let needle = prefix("unified_search");
    let collection = crate::tests::create_collection_fixture(pool.get_ref(), &needle).await;
    let fixture = crate::tests::create_object_fixture(
        pool.get_ref(),
        collection,
        NewHubuumClass {
            name: format!("{needle}_class"),
            collection_id: 0,
            json_schema: Some(serde_json::json!({"title": needle})),
            validate_schema: Some(false),
            description: "unified search compatibility class".to_string(),
        },
        vec![NewHubuumObject {
            name: format!("{needle}_object"),
            collection_id: 0,
            hubuum_class_id: 0,
            data: serde_json::json!({"needle": needle}),
            description: "unified search compatibility object".to_string(),
        }],
    )
    .await
    .expect("unified search compatibility fixture should be created");

    for backend in available_backends() {
        let request = || {
            StorageUnifiedSearchQuery::new(
                needle.clone(),
                StorageCandidatePageLimit::try_new(10)
                    .expect("contract candidate page limit should be valid"),
                StorageVisibility::new(
                    principal_id(i32::MAX),
                    true,
                    None::<Vec<StorageAuthorizationPermission>>,
                    None,
                ),
            )
            .search_extended_document(true)
        };

        let collections = backend
            .search_collections(request())
            .await
            .expect("certified backend should search collections")
            .into_parts()
            .0;
        assert!(collections.into_iter().any(|row| {
            let (row, _) = row.into_parts();
            let (id, ..) = row.into_parts();
            id.id() == fixture.collection.collection.id
        }));

        let classes = backend
            .search_classes(request())
            .await
            .expect("certified backend should search classes")
            .into_parts()
            .0;
        assert!(classes.into_iter().any(|row| {
            let (row, _) = row.into_parts();
            let (id, ..) = row.into_parts();
            id.id() == fixture.class.id
        }));

        let objects = backend
            .search_objects(request())
            .await
            .expect("certified backend should search objects")
            .into_parts()
            .0;
        assert!(objects.into_iter().any(|row| {
            let (row, _) = row.into_parts();
            let (id, ..) = row.into_parts();
            id.id() == fixture.objects[0].id
        }));
    }

    fixture
        .cleanup()
        .await
        .expect("unified search compatibility fixture should be removed");
}

#[actix_web::test]
async fn every_available_storage_backend_supplies_operational_state() {
    let _permit = postgres_permit().await;

    for backend in available_backends() {
        let state = backend
            .get_maintenance_state()
            .await
            .expect("certified backend should expose maintenance state");
        let readiness = backend
            .get_readiness_snapshot()
            .await
            .expect("certified backend should expose readiness state");

        assert_eq!(readiness.maintenance_state(), state);
        assert!(readiness.storage_is_ready());
        let task_queue = backend
            .get_task_queue_snapshot()
            .await
            .expect("certified backend should expose task queue diagnostics");
        assert!(task_queue.statuses().total() >= 0);
        assert!(task_queue.total_task_events() >= 0);
        let export_health = backend
            .load_export_template_health()
            .await
            .expect("certified backend should aggregate export-template health");
        assert!(export_health.iter().all(|row| row.runs() > 0));
        let audit_entries = backend
            .load_export_templates_for_audit()
            .await
            .expect("certified backend should supply the template audit set");
        assert!(audit_entries.windows(2).all(|entries| {
            (entries[0].collection_id(), entries[0].id())
                <= (entries[1].collection_id(), entries[1].id())
        }));
    }
}

#[actix_web::test]
async fn every_available_storage_backend_supplies_event_health() {
    let _permit = postgres_permit().await;

    for backend in available_backends() {
        let health = backend
            .get_event_delivery_health()
            .await
            .expect("certified backend should expose event delivery health");
        assert!(health.fanout().pending_events() >= 0);
        assert!(health.delivery().counts().total() >= 0);
    }
}

#[actix_web::test]
async fn every_available_storage_backend_supplies_complete_event_administration() {
    let _permit = postgres_permit().await;
    let options = || {
        QueryOptions::new(Vec::new(), Vec::new(), Some(50), None, true)
            .expect("contract query must be valid")
    };
    let fanout_settings = EventFanoutSettings::new(1_000, 30_000)
        .expect("compatibility fan-out settings should be valid");
    let event_admin_collection_id = collection_id(1);

    for backend in available_backends() {
        let event_context = EventContext::system();
        let sink_name = prefix("event_admin_sink");
        let sink = backend
            .create_event_sink(
                StorageEventSinkCreate::builder(sink_name, "webhook", event_context.clone())
                    .configuration(serde_json::json!({}))
                    .enabled(true)
                    .try_build()
                    .unwrap(),
            )
            .await
            .expect("certified backend should create event sinks")
            .into_value();
        let sink_id = sink.id();

        assert!(
            backend
                .count_enabled_event_sinks()
                .await
                .expect("certified backend should count enabled event sinks")
                >= 1
        );
        assert_eq!(
            backend
                .get_event_sink(sink_id)
                .await
                .expect("certified backend should load event sinks")
                .id(),
            sink_id
        );
        let (sinks, sink_total) = backend
            .list_event_sinks(StorageEventSinkListQuery::new(options()))
            .await
            .expect("certified backend should list event sinks")
            .into_parts();
        assert!(!sinks.is_empty());
        assert!(sink_total.is_some_and(|total| total >= 1));
        let updated_sink = backend
            .update_event_sink(
                StorageEventSinkUpdate::builder(sink_id, event_context.clone())
                    .name(Some(prefix("event_admin_sink_updated")))
                    .try_build()
                    .unwrap(),
            )
            .await
            .expect("certified backend should update event sinks")
            .into_value();
        assert!(updated_sink.revision() > sink.revision());

        let subscription = backend
            .create_event_subscription(
                StorageEventSubscriptionCreate::builder(
                    event_admin_collection_id,
                    sink_id,
                    prefix("event_admin_subscription"),
                    event_context.clone(),
                )
                .description("storage compatibility event subscription")
                .entity_types(vec![EntityType::EventSubscription])
                .actions(vec![Action::Created])
                .routing(serde_json::json!({}))
                .enabled(true)
                .try_build()
                .unwrap(),
            )
            .await
            .expect("certified backend should create event subscriptions")
            .into_value();
        let subscription_id = subscription.id();
        assert_eq!(
            backend
                .get_event_subscription(event_admin_collection_id, subscription_id)
                .await
                .expect("certified backend should load scoped subscriptions")
                .collection_id(),
            event_admin_collection_id
        );
        let (subscriptions, subscription_total) = backend
            .list_event_subscriptions(StorageEventSubscriptionListQuery::new(
                event_admin_collection_id,
                options(),
            ))
            .await
            .expect("certified backend should list scoped subscriptions")
            .into_parts();
        assert!(!subscriptions.is_empty());
        assert!(subscription_total.is_some_and(|total| total >= 1));
        let updated_subscription = backend
            .update_event_subscription(
                StorageEventSubscriptionUpdate::builder(
                    event_admin_collection_id,
                    subscription_id,
                    event_context.clone(),
                )
                .description(Some(
                    "updated storage compatibility subscription".to_string(),
                ))
                .try_build()
                .unwrap(),
            )
            .await
            .expect("certified backend should update event subscriptions")
            .into_value();
        assert!(updated_subscription.revision() > subscription.revision());

        let (audit_events, audit_total) = backend
            .list_audit_events(StorageAuditEventListQuery::new(
                vec![event_admin_collection_id],
                false,
                StorageAuditEventFilters::new()
                    .entity_type(Some(EntityType::EventSubscription))
                    .entity_id(Some(
                        EventEntityId::new(subscription_id.id())
                            .expect("persisted event-subscription id must be positive"),
                    )),
                options(),
            ))
            .await
            .expect("certified backend should list event audit records")
            .into_parts();
        assert!(audit_events.len() >= 2);
        assert!(audit_total.is_some_and(|total| total >= 2));

        let mut delivery = None;
        for _ in 0..20 {
            let (deliveries, total) = backend
                .list_event_deliveries(
                    StorageEventDeliveryListQuery::new(options())
                        .subscription_id(Some(subscription_id)),
                )
                .await
                .expect("certified backend should list event deliveries")
                .into_parts();
            assert!(total.is_some());
            if let Some(row) = deliveries.into_iter().next() {
                delivery = Some(row);
                break;
            }
            backend
                .process_event_fanout_batch(fanout_settings)
                .await
                .expect("certified backend should fan out lifecycle events");
        }
        let delivery =
            delivery.expect("event-administration compatibility event should produce a delivery");
        let delivery_id = delivery.id();
        let dead = backend
            .mark_event_delivery_dead(delivery_id)
            .await
            .expect("certified backend should dead-letter event deliveries");
        assert_eq!(dead.status(), EventDeliveryStatus::Dead);
        let pending = backend
            .release_event_delivery_for_retry(delivery_id)
            .await
            .expect("certified backend should release event deliveries for retry");
        assert_eq!(pending.status(), EventDeliveryStatus::Pending);
        assert_eq!(
            backend
                .get_event_delivery(delivery_id)
                .await
                .expect("certified backend should load event deliveries")
                .id(),
            delivery_id
        );

        backend
            .delete_event_subscription(StorageEventSubscriptionDelete::new(
                event_admin_collection_id,
                subscription_id,
                event_context.clone(),
            ))
            .await
            .expect("event-subscription compatibility fixture should be removed")
            .into_value();
        backend
            .delete_event_sink(StorageEventSinkDelete::new(sink_id, event_context))
            .await
            .expect("event-sink compatibility fixture should be removed")
            .into_value();
    }
}

#[actix_web::test]
async fn every_available_storage_backend_processes_event_fanout() {
    let _permit = postgres_permit().await;
    let settings = EventFanoutSettings::new(10, 30_000)
        .expect("compatibility fan-out settings should be valid");

    for backend in available_backends() {
        let processed = backend
            .process_event_fanout_batch(settings)
            .await
            .expect("certified backend should process event fan-out");
        assert!(processed <= settings.batch_size());
    }
}

#[actix_web::test]
async fn every_available_storage_backend_processes_event_retention() {
    struct DiscardArchive;

    #[async_trait]
    impl EventArchiveSink for DiscardArchive {
        async fn archive(&self, _batch: &StorageEventRetentionBatch) -> Result<(), StorageError> {
            Ok(())
        }
    }

    let _permit = postgres_permit().await;
    let settings = EventRetentionSettings::new(10_000, 10_000, 10)
        .expect("compatibility event-retention settings should be valid");

    for backend in available_backends() {
        let summary =
            crate::storage::execute_event_retention_batch(&backend, settings, &DiscardArchive)
                .await
                .expect("certified backend should process event retention");

        assert!(!summary.did_work());
    }
}

#[actix_web::test]
async fn every_available_storage_backend_supplies_token_retention() {
    let _permit = postgres_permit().await;
    let settings = TokenRetentionSettings::builder()
        .retention_days(1_000_000)
        .token_lifetime_hours(24)
        .batch_size(10)
        .build()
        .expect("compatibility retention settings should be valid");

    for backend in available_backends() {
        let purged = backend
            .purge_expired_tokens(settings)
            .await
            .expect("certified backend should execute token retention");

        assert_eq!(purged, 0);
    }
}

#[actix_web::test]
async fn every_available_storage_backend_composes_through_services_and_http() {
    let _permit = postgres_permit().await;

    for environment in available_backend_environments() {
        let expectations = ApplicationCompatibilityExpectations::new(
            environment.kind().as_str(),
            1,
            http::StatusCode::OK.as_u16(),
        );
        let fixture = RegisteredApplicationCompatibilityFixture { environment };
        let report = verify_application_compatibility(&fixture, &expectations)
            .await
            .expect("registered backend must satisfy application compatibility");
        assert_eq!(report.checks(), 7);
    }
}

pub(crate) async fn postgres_permit() -> OwnedSemaphorePermit {
    static LIMITER: LazyLock<Arc<Semaphore>> = LazyLock::new(|| Arc::new(Semaphore::new(4)));
    LIMITER
        .clone()
        .acquire_owned()
        .await
        .expect("storage contract semaphore should remain open")
}

pub(crate) fn pool() -> Data<PostgresPool> {
    let config = crate::tests::integration_test_config()
        .expect("integration test config should be initialized");
    Data::new(crate::tests::postgres_test_pool(&config.database_url, 2))
}

pub(crate) fn prefix(label: &str) -> String {
    let suffix = crate::utilities::auth::generate_random_password(12).to_ascii_lowercase();
    format!("storage_contract_{label}_{suffix}")
}
