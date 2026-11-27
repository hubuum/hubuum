use crate::tests::asserts::expect_legacy_task;
use hubuum_domain::ResourceIdentity;
use hubuum_storage_core::{HistoryPolicyScope, StorageRestoreStageArtifact};
use hubuum_storage_core::{StorageTaskClaimRequest, TaskWorkerId};
mod auth_sessions;
use hubuum_storage_core::{EventSinkGrantAction, StorageAuditSubject, StorageEventSinkGrantChange};
mod api_request_admission;
mod authentication;
mod authorization_resources;
mod catalog_filters;
mod credential_approvals;
mod event_delivery;
mod history_access;
mod history_archive;
mod history_authentication;
mod history_computed;
mod history_fixtures;
mod history_http_publication;
mod history_name;
mod history_navigation;
mod history_population;
mod history_relations;
mod logical_identity;
mod login_events;
mod regex;
mod remote_targets;
mod schema_evolution;
mod task_capacity;
mod task_control;
mod task_waiting;
mod webhook_notifications;

use std::collections::HashSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use actix_web::{App, ResponseError, http, test, web::Data};
use async_trait::async_trait;
use hubuum_computed_fields::{Definition, FieldKey, JsonPointer, Operation, ResultType};
use hubuum_domain::{
    ClassId, ClassRelationId, CollectionId, ComputedFieldDefinitionId, EventDeliveryStatus,
    GroupId, ObjectId, ObjectRelationId, PrincipalId, ResourceId, UserId,
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
use hubuum_storage_core::{StorageBackupBudget, StorageExternalSyncFailure, StorageTaskClaimToken};
use hubuum_storage_memory::MemoryStorage;
use hubuum_storage_postgres::test_support::fanout_event;
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

use crate::errors::ApiError;
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
    ClassKey, CollectionHistory, CollectionID, CollectionKey, ExportTemplateHistory,
    HubuumClassHistory, HubuumObjectHistory, ImportAtomicity, ImportClassInput,
    ImportCollectionInput, ImportMode, ImportObjectInput, ImportWriteCondition, NewHubuumClass,
    NewHubuumObject, RemoteTargetHistory, ResourceRevision,
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
    StorageCatalogListQuery, StorageClassCreate, StorageClassRelationCreate,
    StorageClassSchemaPolicy, StorageClassSelector, StorageClassUpdate, StorageCollectionCreate,
    StorageCollectionUpdate, StorageComputedFieldDefinitionInput,
    StorageComputedFieldDefinitionPatch, StorageComputedFieldRebuildRequest,
    StorageComputedFieldVisibility, StorageComputedObjectEnrichmentQuery,
    StorageComputedObjectListQuery, StorageComputedObjectProjection,
    StorageComputedObjectQueryOptions, StorageComputedObjectVisibility,
    StorageDefaultAdminBootstrap, StorageError, StorageErrorKind, StorageEventDelivery,
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
    StorageQueryBudget, StorageRelatedDirection, StorageRelatedObjectsForRootsQuery,
    StorageRelatedSort, StorageRelationGraphQuery, StorageRelationIdsQuery,
    StorageRelationListQuery, StorageRelationTouchingQuery, StorageRemoteCallArtifactOutcome,
    StorageRemoteCallArtifactResponse, StorageRemoteCallArtifactTarget,
    StorageRemoteCallTaskArtifact, StorageRemoteTargetCreate, StorageRemoteTargetDefinition,
    StorageRemoteTargetDelete, StorageRemoteTargetInvocation, StorageRemoteTargetListQuery,
    StorageRemoteTargetPatch, StorageRemoteTargetPolicy, StorageRemoteTargetTransport,
    StorageRemoteTargetUpdate, StorageRestoreFailure, StorageRestoreInitiator,
    StorageRestoreJobStatus, StorageRevisionPrecondition, StorageRevisionTarget,
    StorageServiceAccountCreate, StorageServiceAccountListQuery, StorageServiceAccountMutation,
    StorageServiceAccountUpdate, StorageSharedComputedFieldCreate,
    StorageSharedComputedFieldDelete, StorageSharedComputedFieldUpdate, StorageTaskActiveStatus,
    StorageTaskActiveUpdate, StorageTaskChildListQuery, StorageTaskCompletion,
    StorageTaskCompletionPayload, StorageTaskCreateRequest, StorageTaskEventAppend,
    StorageTaskEventInput, StorageTaskFailure, StorageTaskKind, StorageTaskLease,
    StorageTaskLeaseDuration, StorageTaskListQuery, StorageTaskOutputLookup,
    StorageTaskResultCounts, StorageTaskScopeSnapshot, StorageTaskStatus,
    StorageTaskTerminalStatus, StorageTaskTerminalUpdate, StorageTokenCreate, StorageTokenDigest,
    StorageTokenFormat, StorageTokenHashAlgorithm, StorageTokenHashKeyId, StorageTokenHashRevoke,
    StorageTokenIssuancePolicy, StorageTokenListQuery, StorageTokenListState,
    StorageTokenMigrationOutcome, StorageTokenObservation, StorageTokenRenew, StorageTokenRevoke,
    StorageUnifiedSearchQuery, StorageUserAnonymize, StorageUserCreate, StorageUserDelete,
    StorageUserListQuery, StorageUserPasswordUpdate, StorageUserUpdate, StorageVisibility,
    TaskExecutionStorage, TaskQueueStorage, TokenRetentionStorage, TokenStorage,
    UnifiedSearchStorage, UserStorage,
};
use crate::traits::CanSave;
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
    Memory { storage: MemoryStorage },
    Postgres { pool: PostgresPool },
}

impl BackendTestEnvironment {
    fn kind(&self) -> StorageBackendKind {
        match self {
            Self::Memory { .. } => StorageBackendKind::Memory,
            Self::Postgres { .. } => StorageBackendKind::Postgres,
        }
    }

    fn storage(&self) -> StorageHandle {
        match self {
            Self::Memory { storage } => StorageHandle::from_registered_backend(storage.clone()),
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
            StorageBackendKind::Memory => BackendTestEnvironment::Memory {
                storage: MemoryStorage::new(),
            },
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

struct BackendUserFixture {
    id: UserId,
    principal_id: PrincipalId,
    raw_token: String,
    token_hash: String,
}

async fn create_backend_user(backend: &StorageHandle, name: &str) -> BackendUserFixture {
    let user = backend
        .create_user(StorageUserCreate::new(
            None,
            name,
            "storage-contract-password-hash",
            Some("Storage Contract User".to_string()),
            Some(format!("{name}@example.invalid")),
            EventContext::system(),
        ))
        .await
        .expect("backend-local compatibility user should be created")
        .into_value();
    let id = user.into_parts().id();
    let principal_id = principal_id(id.id());
    let raw_token = prefix("backend_user_token");
    let token_hash = crate::models::Token::storage_hash_from_raw(&raw_token);
    backend
        .create_token(StorageTokenCreate::new(
            principal_id,
            StorageTokenDigest::legacy_unidentified(&token_hash),
            StorageTokenIssuancePolicy::try_new(24, 24)
                .expect("backend-local token policy should be valid"),
            EventContext::system(),
        ))
        .await
        .expect("backend-local compatibility token should be created")
        .into_value();
    BackendUserFixture {
        id,
        principal_id,
        raw_token,
        token_hash,
    }
}

async fn delete_backend_user(backend: &StorageHandle, user: BackendUserFixture) {
    backend
        .delete_user(StorageUserDelete::new(user.id, EventContext::system()))
        .await
        .expect("backend-local compatibility user should be removed")
        .into_value();
}

struct BackendObjectFixture {
    collection: crate::storage::StorageCollection,
    class: crate::storage::StorageClass,
    objects: Vec<StorageObject>,
    owned_group: Option<crate::storage::StorageIdentityGroup>,
}

async fn create_backend_object_fixture(
    backend: &StorageHandle,
    needle: &str,
    object_documents: Vec<serde_json::Value>,
) -> BackendObjectFixture {
    let owner_group = backend
        .create_group(
            StorageGroupCreate::new(
                None,
                format!("{needle}_owner"),
                Some("backend-local compatibility owner".to_string()),
            ),
            &EventContext::system(),
        )
        .await
        .expect("backend-local compatibility owner group should be created")
        .into_value();
    let mut fixture = create_backend_object_fixture_for_owner(
        backend,
        needle,
        object_documents,
        owner_group.id(),
    )
    .await;
    fixture.owned_group = Some(owner_group);
    fixture
}

async fn create_backend_object_fixture_for_owner(
    backend: &StorageHandle,
    needle: &str,
    object_documents: Vec<serde_json::Value>,
    owner_group_id: GroupId,
) -> BackendObjectFixture {
    let collection = backend
        .collection_store()
        .create_collection(
            StorageCollectionCreate::new(
                format!("{needle}_collection"),
                "backend-local compatibility collection",
                owner_group_id,
                None,
            ),
            &EventContext::system(),
        )
        .await
        .expect("backend-local compatibility collection should be created")
        .into_value();
    let class = backend
        .class_store()
        .create_class(
            StorageClassCreate::builder(
                format!("{needle}_class"),
                collection.id(),
                "backend-local compatibility class",
            )
            .build(),
            &EventContext::system(),
        )
        .await
        .expect("backend-local compatibility class should be created")
        .into_value();
    let resolved_class = backend
        .class_store()
        .resolve_class(StorageClassSelector::Id(class.id()))
        .await
        .expect("backend-local compatibility class should resolve");
    let mut objects = Vec::with_capacity(object_documents.len());
    for (index, data) in object_documents.into_iter().enumerate() {
        objects.push(
            backend
                .object_store()
                .create_object(
                    &resolved_class,
                    StorageObjectCreate::new(
                        format!("{needle}_object_{index}"),
                        collection.id(),
                        class.id(),
                        data,
                        "backend-local compatibility object",
                    ),
                    &EventContext::system(),
                )
                .await
                .expect("backend-local compatibility object should be created")
                .into_value(),
        );
    }
    BackendObjectFixture {
        collection,
        class,
        objects,
        owned_group: None,
    }
}

async fn delete_backend_object_fixture(backend: &StorageHandle, fixture: BackendObjectFixture) {
    for object in fixture.objects {
        let resolved = backend
            .object_store()
            .get_object(object.id())
            .await
            .expect("backend-local compatibility object should resolve for cleanup");
        backend
            .object_store()
            .delete_object(&resolved, &EventContext::system())
            .await
            .expect("backend-local compatibility object should be removed")
            .into_value();
    }
    let class = backend
        .class_store()
        .resolve_class(StorageClassSelector::Id(fixture.class.id()))
        .await
        .expect("backend-local compatibility class should resolve for cleanup");
    backend
        .class_store()
        .delete_class(&class, &EventContext::system())
        .await
        .expect("backend-local compatibility class should be removed")
        .into_value();
    backend
        .collection_store()
        .delete_collection(fixture.collection.id(), &EventContext::system())
        .await
        .expect("backend-local compatibility collection should be removed")
        .into_value();
    if let Some(group) = fixture.owned_group {
        backend
            .delete_group(group.id(), &EventContext::system())
            .await
            .expect("backend-local compatibility owner group should be removed")
            .into_value();
    }
}

struct BackendRelationFixture {
    collection: crate::storage::StorageCollection,
    owner_group: crate::storage::StorageIdentityGroup,
    classes: Vec<crate::storage::StorageClass>,
    objects: Vec<StorageObject>,
    class_relation_id: ClassRelationId,
    object_relation_id: ObjectRelationId,
}

async fn create_backend_relation_fixture(
    backend: &StorageHandle,
    needle: &str,
) -> BackendRelationFixture {
    let owner_group = backend
        .create_group(
            StorageGroupCreate::new(
                None,
                format!("{needle}_owner"),
                Some("backend-local relation owner".to_string()),
            ),
            &EventContext::system(),
        )
        .await
        .expect("backend-local relation owner group should be created")
        .into_value();
    let collection = backend
        .collection_store()
        .create_collection(
            StorageCollectionCreate::new(
                format!("{needle}_collection"),
                "backend-local relation collection",
                owner_group.id(),
                None,
            ),
            &EventContext::system(),
        )
        .await
        .expect("backend-local relation collection should be created")
        .into_value();
    let mut classes = Vec::new();
    for suffix in ["one", "two"] {
        classes.push(
            backend
                .class_store()
                .create_class(
                    StorageClassCreate::builder(
                        format!("{needle}_class_{suffix}"),
                        collection.id(),
                        format!("backend-local relation class {suffix}"),
                    )
                    .build(),
                    &EventContext::system(),
                )
                .await
                .expect("backend-local relation class should be created")
                .into_value(),
        );
    }
    let prepared_class_relation = backend
        .class_relation_store()
        .prepare_class_relation(
            StorageClassRelationCreate::builder(classes[0].id(), classes[1].id()).build(),
        )
        .await
        .expect("backend-local class relation should prepare");
    let class_relation = backend
        .class_relation_store()
        .create_class_relation(&prepared_class_relation, &EventContext::system())
        .await
        .expect("backend-local class relation should be created")
        .into_value();
    let class_relation_id = ClassRelationId::from(class_relation.relation().metadata().id());
    let mut objects = Vec::new();
    for (index, class) in classes.iter().enumerate() {
        let resolved_class = backend
            .class_store()
            .resolve_class(StorageClassSelector::Id(class.id()))
            .await
            .expect("backend-local relation class should resolve");
        objects.push(
            backend
                .object_store()
                .create_object(
                    &resolved_class,
                    StorageObjectCreate::new(
                        format!("{needle}_object_{index}"),
                        collection.id(),
                        class.id(),
                        serde_json::json!({}),
                        "backend-local relation object",
                    ),
                    &EventContext::system(),
                )
                .await
                .expect("backend-local relation object should be created")
                .into_value(),
        );
    }
    let prepared_object_relation = backend
        .object_relation_store()
        .prepare_object_relation(StorageObjectRelationCreateSelector::Explicit(
            StorageObjectRelationCreate::new(objects[0].id(), objects[1].id(), class_relation_id),
        ))
        .await
        .expect("backend-local object relation should prepare");
    let object_relation = backend
        .object_relation_store()
        .create_object_relation(&prepared_object_relation, &EventContext::system())
        .await
        .expect("backend-local object relation should be created")
        .into_value();
    let object_relation_id = ObjectRelationId::from(object_relation.relation().metadata().id());
    BackendRelationFixture {
        collection,
        owner_group,
        classes,
        objects,
        class_relation_id,
        object_relation_id,
    }
}

async fn delete_backend_relation_fixture(backend: &StorageHandle, fixture: BackendRelationFixture) {
    let object_relation = backend
        .object_relation_store()
        .resolve_object_relation(StorageObjectRelationSelector::Id(
            fixture.object_relation_id,
        ))
        .await
        .expect("backend-local object relation should resolve for cleanup");
    backend
        .object_relation_store()
        .delete_object_relation(&object_relation, &EventContext::system())
        .await
        .expect("backend-local object relation should be removed")
        .into_value();
    for object in fixture.objects {
        let object = backend
            .object_store()
            .get_object(object.id())
            .await
            .expect("backend-local relation object should resolve for cleanup");
        backend
            .object_store()
            .delete_object(&object, &EventContext::system())
            .await
            .expect("backend-local relation object should be removed")
            .into_value();
    }
    let class_relation = backend
        .class_relation_store()
        .resolve_class_relation(fixture.class_relation_id)
        .await
        .expect("backend-local class relation should resolve for cleanup");
    backend
        .class_relation_store()
        .delete_class_relation(&class_relation, &EventContext::system())
        .await
        .expect("backend-local class relation should be removed")
        .into_value();
    for class in fixture.classes {
        let class = backend
            .class_store()
            .resolve_class(StorageClassSelector::Id(class.id()))
            .await
            .expect("backend-local relation class should resolve for cleanup");
        backend
            .class_store()
            .delete_class(&class, &EventContext::system())
            .await
            .expect("backend-local relation class should be removed")
            .into_value();
    }
    backend
        .collection_store()
        .delete_collection(fixture.collection.id(), &EventContext::system())
        .await
        .expect("backend-local relation collection should be removed")
        .into_value();
    backend
        .delete_group(fixture.owner_group.id(), &EventContext::system())
        .await
        .expect("backend-local relation owner group should be removed")
        .into_value();
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
        backend
            .change_event_sink_grant(StorageEventSinkGrantChange::new(
                sink.id(),
                collection.id(),
                EventSinkGrantAction::Grant,
                EventContext::system(),
            ))
            .await
            .expect("fixture grant")
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

    async fn fanout_deliveries(&self) -> Result<Vec<StorageEventDelivery>, FixtureError> {
        // Parallel fixtures share the queue and may claim our event before their
        // fan-out transaction finishes. Drive only this fixture's events through
        // the adapter's production fan-out operation instead of polling global
        // batches for an arbitrary number of scheduler yields.
        for event in self.collection_events(self.collection_id).await? {
            let (envelope, _, _) = event.into_parts();
            fanout_event(&self.pool, envelope.id()).await?;
        }
        let (deliveries, _) = self
            .backend
            .list_event_deliveries(
                StorageEventDeliveryListQuery::new(Self::query_options())
                    .subscription_id(Some(self.subscription_id)),
            )
            .await?
            .into_parts();
        Ok(deliveries)
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
        let deliveries = self.fanout_deliveries().await?;
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
        Ok(FanoutProbe::new(
            durable_delivery_count,
            self.sink_deliveries.load(Ordering::Relaxed),
        ))
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
        let delivery_id = self
            .fanout_deliveries()
            .await?
            .first()
            .ok_or_else(|| std::io::Error::other("delivery fault event produced no delivery"))?
            .id();
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
            .stage_restore_artifact(
                StorageRestoreStageArtifact::try_new(
                    StorageRestoreInitiator::try_new(None, "fault-test", prefix("restore_fault"))
                        .expect("valid restore initiator"),
                    hubuum_storage_core::StorageRestoreArtifact::from_reader(b"{}".as_slice(), 2)
                        .unwrap(),
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
                .run(backend.start_restore_draining(job_id.into()))
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
                .try_build()
                .expect("lease-loss task request should be valid"),
            )
            .await
            .map(expect_legacy_task)?;
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

struct MemoryAuditContractFixture {
    backend: StorageHandle,
    group_id: GroupId,
    sink_id: hubuum_domain::EventSinkId,
    subscription_id: hubuum_domain::EventSubscriptionId,
    collection_id: CollectionId,
    logical_observer: Arc<RecordingStorageObserver>,
    sink_deliveries: Arc<AtomicUsize>,
}

impl MemoryAuditContractFixture {
    async fn new(storage: MemoryStorage) -> Result<Self, FixtureError> {
        let logical_observer = Arc::new(RecordingStorageObserver::default());
        let backend =
            StorageHandle::from_registered_backend_with_observer(storage, logical_observer.clone());
        let context = EventContext::system();
        let group = backend
            .create_group(
                StorageGroupCreate::new(
                    None,
                    prefix("memory_audit_contract_group"),
                    Some("memory audit contract owner".to_string()),
                ),
                &context,
            )
            .await?
            .into_value();
        let sink = backend
            .create_event_sink(
                StorageEventSinkCreate::builder(
                    prefix("memory_audit_contract_sink"),
                    "webhook",
                    context.clone(),
                )
                .configuration(serde_json::json!({}))
                .enabled(true)
                .try_build()?,
            )
            .await?
            .into_value();
        let collection = backend
            .collection_store()
            .create_collection(
                StorageCollectionCreate::new(
                    prefix("memory_audit_contract_collection"),
                    "memory audit contract collection before committed probe",
                    group.id(),
                    None,
                ),
                &context,
            )
            .await?
            .into_value();
        backend
            .change_event_sink_grant(StorageEventSinkGrantChange::new(
                sink.id(),
                collection.id(),
                EventSinkGrantAction::Grant,
                context.clone(),
            ))
            .await
            .expect("fixture grant")
            .into_value();
        let subscription = backend
            .create_event_subscription(
                StorageEventSubscriptionCreate::builder(
                    collection.id(),
                    sink.id(),
                    prefix("memory_audit_contract_subscription"),
                    context,
                )
                .entity_types(vec![EntityType::Collection])
                .actions(vec![Action::Updated])
                .routing(serde_json::json!({}))
                .enabled(true)
                .try_build()?,
            )
            .await?
            .into_value();
        Ok(Self {
            backend,
            group_id: group.id(),
            sink_id: sink.id(),
            subscription_id: subscription.id(),
            collection_id: collection.id(),
            logical_observer,
            sink_deliveries: Arc::new(AtomicUsize::new(0)),
        })
    }

    fn query_options() -> QueryOptions {
        QueryOptions::new(Vec::new(), Vec::new(), Some(100), None, true)
            .expect("memory audit contract query options must be valid")
    }

    async fn collection_events(&self) -> Result<Vec<StorageAuditEvent>, FixtureError> {
        let (events, _) = self
            .backend
            .list_audit_events(StorageAuditEventListQuery::new(
                vec![self.collection_id],
                false,
                StorageAuditEventFilters::new()
                    .entity_type(Some(EntityType::Collection))
                    .entity_id(Some(EventEntityId::new(self.collection_id.id())?)),
                Self::query_options(),
            ))
            .await?
            .into_parts();
        Ok(events)
    }

    async fn all_event_count(&self) -> Result<usize, FixtureError> {
        let (events, _) = self
            .backend
            .list_audit_events(StorageAuditEventListQuery::new(
                Vec::new(),
                true,
                StorageAuditEventFilters::new(),
                Self::query_options(),
            ))
            .await?
            .into_parts();
        Ok(events.len())
    }

    async fn cleanup_resources(&self) -> Result<(), FixtureError> {
        let context = EventContext::system();
        self.backend
            .delete_event_subscription(StorageEventSubscriptionDelete::new(
                self.collection_id,
                self.subscription_id,
                context.clone(),
            ))
            .await?
            .into_value();
        self.backend
            .collection_store()
            .delete_collection(self.collection_id, &context)
            .await?
            .into_value();
        self.backend
            .delete_event_sink(StorageEventSinkDelete::new(self.sink_id, context.clone()))
            .await?
            .into_value();
        self.backend
            .delete_group(self.group_id, &context)
            .await?
            .into_value();
        Ok(())
    }
}

#[async_trait]
impl BackendAuditFixture for MemoryAuditContractFixture {
    async fn committed_mutation(&self) -> Result<CommittedMutationProbe, FixtureError> {
        let before = self
            .backend
            .collection_store()
            .get_collection(self.collection_id)
            .await?;
        let outcome = self
            .backend
            .collection_store()
            .update_collection(
                self.collection_id,
                StorageCollectionUpdate::new(
                    None,
                    Some("audited committed memory probe update".to_string()),
                ),
                &EventContext::system(),
            )
            .await?;
        let expected_document = hubuum_events_core::AuditDocument::builder(format!(
            "Updated collection '{}'",
            outcome.value().name()
        ))
        .before(before.audit_snapshot())
        .after(outcome.value().audit_snapshot())
        .try_build()?;
        let receipt = outcome
            .audits()
            .map(hubuum_storage_core::StorageAuditReceipts::first)
            .ok_or_else(|| std::io::Error::other("committed memory update had no receipt"))?;
        let event = self
            .collection_events()
            .await?
            .into_iter()
            .find(|event| event.clone().into_parts().0.id() == receipt.sequence())
            .ok_or_else(|| std::io::Error::other("memory receipt event was not queryable"))?;
        Ok(CommittedMutationProbe::new(
            outcome.map(drop),
            event,
            expected_document,
        ))
    }

    async fn unchanged_mutation(&self) -> Result<UnchangedMutationProbe, FixtureError> {
        let before = self.collection_events().await?.len();
        let outcome = self
            .backend
            .collection_store()
            .update_collection(
                self.collection_id,
                StorageCollectionUpdate::new(None, None),
                &EventContext::system(),
            )
            .await?;
        let after = self.collection_events().await?.len();
        Ok(UnchangedMutationProbe::new(
            outcome.map(drop),
            after.saturating_sub(before),
        ))
    }

    async fn rolled_back_mutation(&self) -> Result<RollbackProbe, FixtureError> {
        let name = prefix("memory_audit_contract_rollback");
        let before = self.all_event_count().await?;
        let collection = ImportCollectionInput {
            ref_: Some("collection:memory_audit_rollback".to_string()),
            name: name.clone(),
            description: "must roll back".to_string(),
            parent_collection_ref: None,
            parent_collection_key: None,
            condition: None,
            timestamps: None,
        };
        let class = ImportClassInput {
            schema_activation: None,
            ref_: None,
            name: prefix("memory_audit_contract_rollback_class"),
            description: "must fail".to_string(),
            json_schema: None,
            validate_schema: Some(false),
            collection_ref: Some("collection:missing".to_string()),
            collection_key: None,
            condition: None,
            timestamps: None,
        };
        let plan = StorageImportPlan::try_new(vec![
            StorageImportPlanItem::new(
                0,
                crate::services::import_boundary::import_operation_to_storage(
                    ApplicationImportOperation::CreateCollection(collection),
                )
                .map_err(|error| std::io::Error::other(error.to_string()))?,
            ),
            StorageImportPlanItem::new(
                1,
                crate::services::import_boundary::import_operation_to_storage(
                    ApplicationImportOperation::CreateClass(class),
                )
                .map_err(|error| std::io::Error::other(error.to_string()))?,
            ),
        ])?;
        if self.backend.apply_import_strict(plan).await.is_ok() {
            return Err(std::io::Error::other("memory rollback probe did not fail").into());
        }
        let root = self.backend.get_import_root_collection().await?;
        let state_change_persisted = self
            .backend
            .get_import_collection_child_by_name(root.id(), &name)
            .await?
            .is_some();
        let audit_event_persisted = self.all_event_count().await? != before;
        Ok(RollbackProbe::new(
            state_change_persisted,
            audit_event_persisted,
        ))
    }

    async fn fanout_to_recording_sink(&self) -> Result<FanoutProbe, FixtureError> {
        let settings = EventFanoutSettings::new(1_000, 30_000)?;
        self.backend.process_event_fanout_batch(settings).await?;
        let (deliveries, _) = self
            .backend
            .list_event_deliveries(
                StorageEventDeliveryListQuery::new(Self::query_options())
                    .subscription_id(Some(self.subscription_id)),
            )
            .await?
            .into_parts();
        let delivery_count = deliveries.len();
        let settings = EventDeliverySettings::builder()
            .batch_size(1_000)
            .lock_timeout_ms(30_000)
            .transport_timeout_ms(25_000)
            .retry_backoff_base_ms(1_000)
            .retry_backoff_max_ms(10_000)
            .max_attempts(3)
            .build()?;
        let resolver = ContractSinkResolver {
            recording: ContractRecordingSink {
                deliveries: self.sink_deliveries.clone(),
            },
            discard: ContractDiscardSink,
        };
        let (work, _) = self
            .backend
            .claim_event_delivery_batch(settings)
            .await?
            .into_parts();
        for item in work {
            crate::events::process_event_delivery_work_item(
                &self.backend,
                settings,
                &resolver,
                item,
            )
            .await
            .map_err(|error| std::io::Error::other(error.to_string()))?;
        }
        Ok(FanoutProbe::new(
            delivery_count,
            self.sink_deliveries.load(Ordering::Relaxed),
        ))
    }

    async fn observations(&self) -> Result<ObservationProbe, FixtureError> {
        let observations = self.logical_observer.observations();
        Ok(ObservationProbe::new(
            self.logical_observer
                .operation_count("collection", "create_collection"),
            self.logical_observer
                .operation_count("event_configuration", "create_event_subscription"),
            self.all_event_count().await?,
            observations
                .iter()
                .filter(|observation| observation.result() != "success")
                .count(),
        ))
    }

    async fn revision_conflict(&self) -> Result<RevisionConflictProbe, FixtureError> {
        let collection = self
            .backend
            .collection_store()
            .get_collection(self.collection_id)
            .await?;
        let current_revision = collection.revision();
        let stale_revision = current_revision.checked_advance()?;
        let error = self
            .backend
            .run_in_scope_send(
                StorageExecutionScope::default().with_revision_precondition(Some(
                    StorageRevisionPrecondition::new(
                        StorageRevisionTarget::Collection(self.collection_id),
                        vec![stale_revision],
                    ),
                )),
                self.backend.collection_store().update_collection(
                    self.collection_id,
                    StorageCollectionUpdate::new(
                        None,
                        Some("stale memory conformance update must not persist".to_string()),
                    ),
                    &EventContext::system(),
                ),
            )
            .await
            .expect_err("stale memory storage mutation must fail");
        Ok(RevisionConflictProbe::new(error, current_revision))
    }

    async fn cleanup(&self) -> Result<(), FixtureError> {
        self.cleanup_resources().await
    }
}

impl BackendTestEnvironment {
    async fn verify_audit_contract(&self) -> Result<ContractReport, FixtureError> {
        match self {
            Self::Memory { storage } => {
                let fixture = MemoryAuditContractFixture::new(storage.clone()).await?;
                verify_backend_audit_contract(&fixture)
                    .await
                    .map_err(Into::into)
            }
            Self::Postgres { pool } => {
                let fixture = PostgresAuditContractFixture::new(pool.clone()).await?;
                verify_backend_audit_contract(&fixture)
                    .await
                    .map_err(Into::into)
            }
        }
    }
}

struct BackendApplicationFixture {
    backend: StorageHandle,
    administrator: Option<crate::models::User>,
    memory_user: Option<BackendUserFixture>,
    bearer_token: String,
}

async fn backend_application_fixture(
    environment: &BackendTestEnvironment,
) -> BackendApplicationFixture {
    match environment {
        BackendTestEnvironment::Memory { .. } => {
            let backend = environment.storage();
            let memory_user = create_backend_user(&backend, &prefix("memory_app_admin")).await;
            backend
                .add_group_member(
                    memory_user.principal_id,
                    group_id(1),
                    &EventContext::system(),
                )
                .await
                .expect("memory compatibility administrator should join the admin group")
                .into_value();
            let bearer_token = memory_user.raw_token.clone();
            BackendApplicationFixture {
                backend,
                administrator: None,
                memory_user: Some(memory_user),
                bearer_token,
            }
        }
        BackendTestEnvironment::Postgres { pool } => {
            let administrator = crate::tests::create_test_admin(pool).await;
            let bearer_token = administrator
                .create_token(pool)
                .await
                .expect("backend compatibility administrator token should be created")
                .get_token();
            BackendApplicationFixture {
                backend: environment.storage(),
                administrator: Some(administrator),
                memory_user: None,
                bearer_token,
            }
        }
    }
}

impl BackendApplicationFixture {
    async fn cleanup(self, environment: &BackendTestEnvironment) {
        match environment {
            BackendTestEnvironment::Memory { .. } => {
                if let Some(user) = self.memory_user {
                    delete_backend_user(&self.backend, user).await;
                }
            }
            BackendTestEnvironment::Postgres { pool } => {
                self.administrator
                    .expect("PostgreSQL fixture has an application administrator")
                    .delete_without_events(pool)
                    .await
                    .expect("backend compatibility administrator should be removed");
            }
        }
    }
}

fn compatibility_completion_payload(kind: StorageTaskKind) -> StorageTaskCompletionPayload {
    let output_expires_at =
        chrono::Utc::now() + chrono::Duration::try_hours(1).expect("valid duration");
    match kind {
        StorageTaskKind::HistoryExpiry => StorageTaskCompletionPayload::HistoryExpiry,
        StorageTaskKind::Import => StorageTaskCompletionPayload::Import,
        StorageTaskKind::Reindex => StorageTaskCompletionPayload::Reindex,
        StorageTaskKind::SchemaValidation => StorageTaskCompletionPayload::SchemaValidation,
        StorageTaskKind::SchemaProfiling => StorageTaskCompletionPayload::SchemaProfiling(
            hubuum_storage_core::StorageSchemaProfileArtifact::new(
                serde_json::json!({"report":{}, "object_ids":[]}),
                output_expires_at,
            )
            .unwrap(),
        ),
        StorageTaskKind::Export => StorageTaskCompletionPayload::Export(
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
        StorageTaskKind::Backup => StorageTaskCompletionPayload::Backup(
            StorageBackupTaskArtifact::try_new(b"{}".to_vec(), output_expires_at)
                .expect("compatibility backup artifact should be valid"),
        ),
        StorageTaskKind::RemoteCall => {
            StorageTaskCompletionPayload::RemoteCall(StorageRemoteCallTaskArtifact::new(
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

mod query_usage;
mod query_usage_analysis;
mod runtime_settings;

mod artifacts;
mod authorization;
mod certification;
mod execution;
mod identity_operations;
mod imports;
mod operations;
mod postgres_faults;
mod relation_search;
mod resource_lifecycle;
mod resource_queries;
mod task_queue;

#[rstest::rstest]
#[case::class(HistoryPolicyScope::Class { identity: ResourceIdentity::generate() })]
#[case::collection(HistoryPolicyScope::Collection { identity: ResourceIdentity::generate() })]
#[actix_web::test]
async fn every_backend_rejects_an_unknown_history_inspection_scope(
    #[case] scope: HistoryPolicyScope,
) {
    use hubuum_storage_core::{
        HistoryLifecycleScan, HistoryLifecycleStorage, HistoryPolicyKey, StorageIdentityHistoryKind,
    };
    let _permit = postgres_permit().await;
    let scan = HistoryLifecycleScan::new(
        HistoryPolicyKey::new(StorageIdentityHistoryKind::Object, scope).unwrap(),
        1,
    )
    .unwrap();
    for backend in available_backends() {
        let error = backend
            .scan_history_lifecycle(scan.clone())
            .await
            .unwrap_err();
        assert_eq!(error.kind(), StorageErrorKind::NotFound);
    }
}

#[actix_web::test]
async fn every_available_storage_backend_restores_scope_after_synchronous_polling() {
    for backend in available_backends() {
        backend
            .run_in_scope(
                StorageExecutionScope::default().with_call_site(StorageCallSite::HttpRequest),
                async {
                    let polled = backend.run_in_scope_sync(
                        StorageExecutionScope::default()
                            .with_call_site(StorageCallSite::TaskWorker),
                        || backend.current_execution_scope().call_site_override(),
                    );
                    assert_eq!(polled, Some(StorageCallSite::TaskWorker));
                    assert_eq!(
                        backend.current_execution_scope().call_site_override(),
                        Some(StorageCallSite::HttpRequest)
                    );
                },
            )
            .await;
    }
}
