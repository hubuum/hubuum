//! Independent, process-local storage adapter used to validate Hubuum's
//! backend-neutral storage contract.

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use hubuum_domain::*;
use hubuum_events_core::*;
use hubuum_query::*;
use hubuum_storage_core::StorageValidationError;
use hubuum_storage_core::capabilities::{
    backend::*, common::*, events::*, identity::*, operational::*, queries::*, resources::*,
    workflows::*,
};
use tokio::sync::RwLock;
use uuid::Uuid;

const ROOT_COLLECTION_ID: i32 = 1;

tokio::task_local! {
    static MEMORY_EXECUTION_SCOPE: StorageExecutionScope;
}

#[derive(Clone)]
struct MemoryUserRecord {
    user: StorageUser,
    identity_scope_id: IdentityScopeId,
    name: String,
    provider_managed: bool,
    external_subject: Option<String>,
    last_sync_attempted_at: Option<DateTime<Utc>>,
    last_sync_success_at: Option<DateTime<Utc>>,
}

#[derive(Clone)]
struct MemoryTokenRecord {
    id: TokenId,
    principal_id: PrincipalId,
    token_hash: String,
    name: Option<String>,
    description: Option<String>,
    issued: DateTime<Utc>,
    expires_at: Option<DateTime<Utc>>,
    last_used_at: Option<DateTime<Utc>>,
    revoked_at: Option<DateTime<Utc>>,
    scope: Option<StorageAuthenticationTokenScope>,
    revision: ResourceRevision,
}

#[derive(Clone)]
struct MemoryTaskRecord {
    id: TaskId,
    kind: StorageTaskKind,
    status: StorageTaskStatus,
    submitted_by: Option<PrincipalId>,
    idempotency_key: Option<String>,
    request_hash: Option<String>,
    request_payload: Option<serde_json::Value>,
    summary: Option<String>,
    progress: StorageTaskProgress,
    scope_snapshot: StorageTaskScopeSnapshot,
    request_redacted_at: Option<DateTime<Utc>>,
    started_at: Option<DateTime<Utc>>,
    finished_at: Option<DateTime<Utc>>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    lease_expires_at: Option<DateTime<Utc>>,
    attempt_count: i32,
    initiator_principal_id: Option<PrincipalId>,
    claim_token: Option<String>,
}

#[derive(Clone, Copy)]
enum MemoryImportReference {
    IdentityScope(IdentityScopeId),
    Group(GroupId),
    Principal(PrincipalId),
    Collection(CollectionId),
    Class(ClassId),
    Object(ObjectId),
    EventSink(EventSinkId),
}

#[derive(Clone)]
enum MemoryHistoryValue {
    Collection(StorageCollection),
    Class(StorageClass),
    Object(StorageObject),
    ExportTemplate(StorageExportTemplate),
    RemoteTarget(StorageRemoteTarget),
}

impl MemoryHistoryValue {
    fn entity_id(&self) -> i32 {
        match self {
            Self::Collection(value) => value.id().id(),
            Self::Class(value) => value.id().id(),
            Self::Object(value) => value.id().id(),
            Self::ExportTemplate(value) => value.metadata().id().id(),
            Self::RemoteTarget(value) => value.metadata().id().id(),
        }
    }

    fn collection_id(&self) -> CollectionId {
        match self {
            Self::Collection(value) => value.id(),
            Self::Class(value) => value.collection_id(),
            Self::Object(value) => value.collection_id(),
            Self::ExportTemplate(value) => value.clone().into_parts().1,
            Self::RemoteTarget(value) => value.collection_id(),
        }
    }

    fn revision(&self) -> ResourceRevision {
        match self {
            Self::Collection(value) => value.revision(),
            Self::Class(value) => value.revision(),
            Self::Object(value) => value.revision(),
            Self::ExportTemplate(value) => value.metadata().revision(),
            Self::RemoteTarget(value) => value.metadata().revision(),
        }
    }
}

#[derive(Clone)]
struct MemoryHistoryEntry {
    id: HistoryRecordId,
    value: MemoryHistoryValue,
    operation: StorageHistoryOperation,
    valid_from: DateTime<Utc>,
    actor_id: Option<PrincipalId>,
    actor_kind: String,
    initiator_principal_id: Option<PrincipalId>,
    task_id: Option<TaskId>,
}

#[derive(Clone)]
struct MemoryRestoreRecord {
    job: StorageRestoreJob,
    validation_summary: serde_json::Value,
}

#[derive(Clone, Copy)]
struct MemoryRestoreInstance {
    generation: i64,
    drained: bool,
    heartbeat_at: DateTime<Utc>,
}

struct MemoryEventAppend<'a> {
    entity_type: EntityType,
    entity_id: i32,
    entity_name: Option<&'a str>,
    collection_id: Option<CollectionId>,
    action: Action,
    context: &'a EventContext,
    document: AuditDocument,
    before_revision: Option<ResourceRevision>,
    after_revision: Option<ResourceRevision>,
}

struct MemoryScopedSimpleEventAppend<'a> {
    entity_type: EntityType,
    entity_id: i32,
    entity_name: Option<&'a str>,
    collection_id: CollectionId,
    action: Action,
    context: &'a EventContext,
    summary: String,
}

macro_rules! append_memory_event {
    ($state:expr, $entity_type:expr, $entity_id:expr, $entity_name:expr,
     $collection_id:expr, $action:expr, $context:expr, $document:expr,
     $before_revision:expr, $after_revision:expr $(,)?) => {
        $state.append_event_record(MemoryEventAppend {
            entity_type: $entity_type,
            entity_id: $entity_id,
            entity_name: $entity_name,
            collection_id: $collection_id,
            action: $action,
            context: $context,
            document: $document,
            before_revision: $before_revision,
            after_revision: $after_revision,
        })
    };
}

macro_rules! append_memory_scoped_simple_event {
    ($state:expr, $entity_type:expr, $entity_id:expr, $entity_name:expr,
     $collection_id:expr, $action:expr, $context:expr, $summary:expr $(,)?) => {
        $state.append_scoped_simple_event_record(MemoryScopedSimpleEventAppend {
            entity_type: $entity_type,
            entity_id: $entity_id,
            entity_name: $entity_name,
            collection_id: $collection_id,
            action: $action,
            context: $context,
            summary: $summary.into(),
        })
    };
}

impl MemoryHistoryEntry {
    fn metadata(
        &self,
        valid_to: Option<DateTime<Utc>>,
    ) -> Result<StorageHistoryMetadata, StorageError> {
        StorageHistoryMetadata::try_new(
            self.operation,
            self.valid_from,
            valid_to,
            self.id,
            self.value.revision(),
        )
        .map(|metadata| {
            metadata
                .actor(self.actor_id, Some(self.actor_kind.clone()))
                .initiator_principal_id(self.initiator_principal_id)
                .task_id(self.task_id)
        })
        .map_err(invalid_contract_value)
    }
}

impl MemoryTaskRecord {
    fn projection(&self) -> Result<StorageTask, StorageError> {
        StorageTask::builder(
            self.id,
            self.kind,
            self.status,
            self.created_at,
            self.updated_at,
        )
        .submitted_by(self.submitted_by)
        .idempotency_key(self.idempotency_key.clone())
        .request_hash(self.request_hash.clone())
        .request_payload(self.request_payload.clone())
        .summary(self.summary.clone())
        .progress(self.progress)
        .scope_snapshot(self.scope_snapshot.clone())
        .request_redacted_at(self.request_redacted_at)
        .started_at(self.started_at)
        .finished_at(self.finished_at)
        .lease_expires_at(self.lease_expires_at)
        .attempt_count(self.attempt_count)
        .initiator_principal_id(self.initiator_principal_id)
        .try_build()
        .map_err(invalid_contract_value)
    }

    fn lease_matches(&self, lease: &StorageTaskLease) -> bool {
        self.id == lease.task_id()
            && self.claim_token.as_deref() == Some(lease.token().adapter_value())
            && self
                .lease_expires_at
                .is_some_and(|expires_at| expires_at > Utc::now())
    }
}

impl MemoryTokenRecord {
    fn metadata(
        &self,
        observation: StorageTokenObservation,
    ) -> Result<StorageTokenMetadata, StorageError> {
        let (observed_at, legacy_valid_after) = observation.into_parts();
        let expired = self
            .expires_at
            .is_some_and(|expires_at| expires_at <= observed_at)
            || (self.expires_at.is_none() && self.issued < legacy_valid_after);
        StorageTokenMetadata::builder(self.id, self.principal_id, self.issued, self.revision)
            .name(self.name.clone())
            .description(self.description.clone())
            .expires_at(self.expires_at)
            .last_used_at(self.last_used_at)
            .revoked_at(self.revoked_at)
            .active(!expired && self.revoked_at.is_none())
            .expired(expired)
            .scope(self.scope.clone())
            .try_build()
            .map_err(invalid_contract_value)
    }
}

#[derive(Clone)]
struct MemoryState {
    next_collection_id: i32,
    next_class_id: i32,
    next_object_id: i32,
    next_class_relation_id: i32,
    next_object_relation_id: i32,
    next_event_sequence: i64,
    next_identity_scope_id: i32,
    next_principal_id: i32,
    next_group_id: i32,
    next_token_id: i32,
    next_task_id: i32,
    next_task_event_sequence: i64,
    next_import_result_id: i32,
    next_computed_field_id: i32,
    next_export_template_id: i32,
    next_remote_target_id: i32,
    next_authorization_grant_id: i32,
    next_event_sink_id: i32,
    next_event_subscription_id: i32,
    next_event_delivery_id: i64,
    next_history_id: i64,
    next_restore_job_id: i64,
    fanout_event_cursor: i64,
    collections: BTreeMap<i32, StorageCollection>,
    classes: BTreeMap<i32, StorageClass>,
    objects: BTreeMap<i32, StorageObject>,
    class_relations: BTreeMap<i32, StorageClassRelation>,
    object_relations: BTreeMap<i32, StorageObjectRelation>,
    identity_scopes: BTreeMap<i32, StorageIdentityScope>,
    principals: BTreeMap<i32, StoragePrincipal>,
    users: BTreeMap<i32, MemoryUserRecord>,
    groups: BTreeMap<i32, StorageIdentityGroup>,
    memberships: BTreeMap<(i32, i32), StoragePrincipalGroup>,
    external_memberships: BTreeSet<(i32, i32)>,
    tokens: BTreeMap<i32, MemoryTokenRecord>,
    service_accounts: BTreeMap<i32, StorageServiceAccount>,
    tasks: BTreeMap<i32, MemoryTaskRecord>,
    task_events: BTreeMap<i32, Vec<StorageTaskEvent>>,
    import_task_results: BTreeMap<i32, Vec<StorageImportTaskResult>>,
    export_outputs: BTreeMap<i32, StorageExportOutput>,
    backup_outputs: BTreeMap<i32, StorageBackupOutput>,
    export_templates: BTreeMap<i32, StorageExportTemplate>,
    remote_targets: BTreeMap<i32, StorageRemoteTarget>,
    computed_fields: BTreeMap<i32, StorageComputedFieldDefinition>,
    computation_states: BTreeMap<i32, StorageClassComputationState>,
    computed_rebuild_tasks: BTreeMap<i32, ClassId>,
    authorization_grants: BTreeMap<(i32, i32), StorageAuthorizationGrant>,
    event_sinks: BTreeMap<i32, StorageEventSink>,
    event_subscriptions: BTreeMap<i32, StorageEventSubscription>,
    event_deliveries: BTreeMap<i64, StorageEventDelivery>,
    event_delivery_claims: BTreeMap<i64, Uuid>,
    event_retention_batches: BTreeMap<Uuid, Vec<i64>>,
    history: Vec<MemoryHistoryEntry>,
    restore_jobs: BTreeMap<i64, MemoryRestoreRecord>,
    maintenance_state: MaintenanceState,
    maintenance_restore_job_id: Option<RestoreJobId>,
    maintenance_generation: i64,
    restore_instances: BTreeMap<Uuid, MemoryRestoreInstance>,
    events: Vec<StorageRecordedEvent>,
}

impl MemoryState {
    fn new() -> Self {
        let now = Utc::now();
        let metadata = StorageRecordMetadata::try_new(
            ResourceId::new(ROOT_COLLECTION_ID).expect("root resource id is valid"),
            now,
            now,
            ResourceRevision::INITIAL,
        )
        .expect("root collection metadata is valid");
        let root = StorageCollection::try_new(metadata, "root", "Root collection", None)
            .expect("root collection is valid");
        let local_scope_id = IdentityScopeId::new(1).expect("local identity scope id is valid");
        let local_scope = StorageIdentityScope::try_new(
            local_scope_id,
            LOCAL_IDENTITY_SCOPE,
            LOCAL_PROVIDER_KIND,
            now,
            now,
            ResourceRevision::INITIAL,
        )
        .expect("local identity scope is valid");
        let admin_principal_id = PrincipalId::new(1).expect("admin principal id is valid");
        let admin_metadata = StorageRecordMetadata::try_new(
            ResourceId::new(admin_principal_id.id()).expect("admin resource id is valid"),
            now,
            now,
            ResourceRevision::INITIAL,
        )
        .expect("admin principal metadata is valid");
        let admin_principal = StoragePrincipal::builder(
            admin_metadata,
            PrincipalKind::Human,
            "admin",
            local_scope_id,
        )
        .try_build()
        .expect("admin principal is valid");
        let admin_user = StorageUser::try_new(
            UserId::new(admin_principal_id.id()).expect("admin user id is valid"),
            Some("memory-adapter-placeholder-password-hash".to_string()),
            Some("Administrator".to_string()),
            None,
            now,
            now,
            None,
        )
        .expect("admin user is valid");
        let admin_group_id = GroupId::new(1).expect("admin group id is valid");
        let admin_group_metadata = StorageRecordMetadata::try_new(
            ResourceId::new(admin_group_id.id()).expect("admin group resource id is valid"),
            now,
            now,
            ResourceRevision::INITIAL,
        )
        .expect("admin group metadata is valid");
        let admin_group = StorageIdentityGroup::builder(
            admin_group_metadata,
            "admin",
            "Administrators",
            local_scope_id,
            LOCAL_PROVIDER_KIND,
        )
        .try_build()
        .expect("admin group is valid");
        let admin_membership = StoragePrincipalGroup::try_new(
            admin_principal_id,
            admin_group_id,
            now,
            now,
            ResourceRevision::INITIAL,
        )
        .expect("admin group membership is valid");
        Self {
            next_collection_id: ROOT_COLLECTION_ID + 1,
            next_class_id: 1,
            next_object_id: 1,
            next_class_relation_id: 1,
            next_object_relation_id: 1,
            next_event_sequence: 1,
            next_identity_scope_id: 2,
            next_principal_id: 2,
            next_group_id: 2,
            next_token_id: 1,
            next_task_id: 1,
            next_task_event_sequence: 1,
            next_import_result_id: 1,
            next_computed_field_id: 1,
            next_export_template_id: 1,
            next_remote_target_id: 1,
            next_authorization_grant_id: 1,
            next_event_sink_id: 1,
            next_event_subscription_id: 1,
            next_event_delivery_id: 1,
            next_history_id: 1,
            next_restore_job_id: 1,
            fanout_event_cursor: 0,
            collections: BTreeMap::from([(ROOT_COLLECTION_ID, root)]),
            classes: BTreeMap::new(),
            objects: BTreeMap::new(),
            class_relations: BTreeMap::new(),
            object_relations: BTreeMap::new(),
            identity_scopes: BTreeMap::from([(local_scope_id.id(), local_scope)]),
            principals: BTreeMap::from([(admin_principal_id.id(), admin_principal)]),
            users: BTreeMap::from([(
                admin_principal_id.id(),
                MemoryUserRecord {
                    user: admin_user,
                    identity_scope_id: local_scope_id,
                    name: "admin".to_string(),
                    provider_managed: false,
                    external_subject: None,
                    last_sync_attempted_at: None,
                    last_sync_success_at: None,
                },
            )]),
            groups: BTreeMap::from([(admin_group_id.id(), admin_group)]),
            memberships: BTreeMap::from([(
                (admin_principal_id.id(), admin_group_id.id()),
                admin_membership,
            )]),
            external_memberships: BTreeSet::new(),
            tokens: BTreeMap::new(),
            service_accounts: BTreeMap::new(),
            tasks: BTreeMap::new(),
            task_events: BTreeMap::new(),
            import_task_results: BTreeMap::new(),
            export_outputs: BTreeMap::new(),
            backup_outputs: BTreeMap::new(),
            export_templates: BTreeMap::new(),
            remote_targets: BTreeMap::new(),
            computed_fields: BTreeMap::new(),
            computation_states: BTreeMap::new(),
            computed_rebuild_tasks: BTreeMap::new(),
            authorization_grants: BTreeMap::new(),
            event_sinks: BTreeMap::new(),
            event_subscriptions: BTreeMap::new(),
            event_deliveries: BTreeMap::new(),
            event_delivery_claims: BTreeMap::new(),
            event_retention_batches: BTreeMap::new(),
            history: Vec::new(),
            restore_jobs: BTreeMap::new(),
            maintenance_state: MaintenanceState::Normal,
            maintenance_restore_job_id: None,
            maintenance_generation: 0,
            restore_instances: BTreeMap::new(),
            events: Vec::new(),
        }
    }

    fn append_event_record(
        &mut self,
        request: MemoryEventAppend<'_>,
    ) -> Result<StorageAuditReceipt, StorageError> {
        let MemoryEventAppend {
            entity_type,
            entity_id,
            entity_name,
            collection_id,
            action,
            context,
            document,
            before_revision,
            after_revision,
        } = request;
        let sequence = EventSequence::new(self.next_event_sequence)
            .map_err(|error| StorageError::backend_failure(error.to_string()))?;
        self.next_event_sequence += 1;
        let event_id = Uuid::new_v4();
        let actor_user_id = context.actor_user_id();
        let principal = |principal_id| ProvenancePrincipal {
            principal_id,
            name: None,
        };
        let provenance = Provenance {
            actor: ProvenanceActor {
                kind: Some(context.actor_kind().as_str().to_string()),
                principal: actor_user_id.map(principal),
            },
            initiator: context.initiator_user_id().map(principal),
            task_id: context.task_id(),
        };
        let envelope = EventEnvelope::builder()
            .id(sequence)
            .event_id(event_id)
            .occurred_at(Utc::now())
            .entity_type(entity_type)
            .entity_id(Some(EventEntityId::new(entity_id).map_err(|error| {
                StorageError::backend_failure(error.to_string())
            })?))
            .entity_name(entity_name.map(ToOwned::to_owned))
            .collection_id(collection_id)
            .action(action)
            .actor_user_id(actor_user_id)
            .actor_kind(context.actor_kind())
            .provenance(provenance)
            .request_id(context.request_id())
            .correlation_id(context.correlation_id().map(ToOwned::to_owned))
            .summary(document.summary_text().to_string())
            .before(document.before().cloned())
            .after(document.after().cloned())
            .metadata(document.metadata().clone())
            .schema_version(document.schema_version())
            .try_build()
            .map_err(|error| StorageError::backend_failure(error.to_string()))?;
        let recorded = StorageRecordedEvent::new(envelope, before_revision, after_revision);
        let receipt = recorded.clone().into_audit_receipt();
        self.events.push(recorded);
        Ok(receipt)
    }

    fn append_simple_event(
        &mut self,
        entity_type: EntityType,
        entity_id: i32,
        entity_name: Option<&str>,
        action: Action,
        context: &EventContext,
        summary: impl Into<String>,
    ) -> Result<StorageAuditReceipt, StorageError> {
        let document = AuditDocument::try_new(summary, None, None, serde_json::json!({}))
            .map_err(|error| StorageError::internal(error.to_string()))?;
        append_memory_event!(
            self,
            entity_type,
            entity_id,
            entity_name,
            None,
            action,
            context,
            document,
            None,
            None,
        )
    }

    fn append_scoped_simple_event_record(
        &mut self,
        request: MemoryScopedSimpleEventAppend<'_>,
    ) -> Result<StorageAuditReceipt, StorageError> {
        let document = AuditDocument::try_new(request.summary, None, None, serde_json::json!({}))
            .map_err(|error| StorageError::internal(error.to_string()))?;
        append_memory_event!(
            self,
            request.entity_type,
            request.entity_id,
            request.entity_name,
            Some(request.collection_id),
            request.action,
            request.context,
            document,
            None,
            None,
        )
    }

    fn append_history(
        &mut self,
        value: MemoryHistoryValue,
        operation: StorageHistoryOperation,
        context: &EventContext,
    ) -> Result<(), StorageError> {
        let id = HistoryRecordId::new(self.next_history_id)
            .map_err(|error| StorageError::internal(error.to_string()))?;
        self.next_history_id += 1;
        self.history.push(MemoryHistoryEntry {
            id,
            value,
            operation,
            valid_from: Utc::now(),
            actor_id: context.actor_user_id(),
            actor_kind: context.actor_kind().as_str().to_string(),
            initiator_principal_id: context.initiator_user_id(),
            task_id: context.task_id(),
        });
        Ok(())
    }

    fn identity_scope_by_name(&self, name: &str) -> Option<&StorageIdentityScope> {
        self.identity_scopes
            .values()
            .find(|scope| scope.name() == name)
    }

    fn user_list_item(
        &self,
        record: &MemoryUserRecord,
    ) -> Result<StorageUserListItem, StorageError> {
        let principal = self
            .principals
            .get(&record.user.clone().into_parts().id().id())
            .ok_or_else(|| StorageError::internal("user principal is missing"))?;
        let scope = self
            .identity_scopes
            .get(&record.identity_scope_id.id())
            .ok_or_else(|| StorageError::internal("user identity scope is missing"))?;
        StorageUserListItem::builder(
            record.user.clone(),
            scope.name(),
            scope.provider_kind(),
            record.name.clone(),
            principal.revision(),
        )
        .provider_managed(record.provider_managed)
        .last_sync_attempted_at(record.last_sync_attempted_at)
        .last_sync_success_at(record.last_sync_success_at)
        .try_build()
        .map_err(invalid_contract_value)
    }

    fn user_details(&self, record: &MemoryUserRecord) -> Result<StorageUserDetails, StorageError> {
        let parts = record.user.clone().into_parts();
        let principal = self
            .principals
            .get(&parts.id().id())
            .ok_or_else(|| StorageError::internal("user principal is missing"))?;
        StorageUserDetails::builder(
            parts.id(),
            parts.created_at(),
            parts.updated_at(),
            record.identity_scope_id,
            record.name.clone(),
            principal.revision(),
        )
        .proper_name(parts.proper_name().map(ToOwned::to_owned))
        .email(parts.email().map(ToOwned::to_owned))
        .provider_managed(record.provider_managed)
        .try_build()
        .map_err(invalid_contract_value)
    }

    fn append_task_event_record(
        &mut self,
        task_id: TaskId,
        input: StorageTaskEventInput,
    ) -> Result<(), StorageError> {
        let (event_type, message, data) = input.into_parts();
        let id = EventSequence::new(self.next_task_event_sequence)
            .map_err(|error| StorageError::internal(error.to_string()))?;
        self.next_task_event_sequence += 1;
        let event =
            StorageTaskEvent::builder(id, task_id, event_type, message, Utc::now(), "system")
                .data(data)
                .build();
        self.task_events
            .entry(task_id.id())
            .or_default()
            .push(event);
        Ok(())
    }

    fn cancel_queued_tasks_for_principal(
        &mut self,
        principal_id: PrincipalId,
    ) -> Result<Vec<StorageTaskKind>, StorageError> {
        let now = Utc::now();
        let mut kinds = Vec::new();
        for task in self.tasks.values_mut() {
            if task.submitted_by == Some(principal_id)
                && matches!(task.status, StorageTaskStatus::Queued)
            {
                task.status = StorageTaskStatus::Cancelled;
                task.summary = Some("Service account disabled".to_string());
                task.finished_at = Some(now);
                task.updated_at = now;
                task.request_payload = None;
                task.request_redacted_at = Some(now);
                task.lease_expires_at = None;
                task.claim_token = None;
                kinds.push(task.kind);
            }
        }
        kinds.sort_unstable_by_key(|kind| kind.as_str());
        kinds.dedup();
        Ok(kinds)
    }
}

/// Independent process-local implementation of the complete storage contract.
#[derive(Clone)]
pub struct MemoryStorage {
    state: Arc<RwLock<MemoryState>>,
}

fn ordered_ids<T: Ord>(first: T, second: T) -> (T, T) {
    if first < second {
        (first, second)
    } else {
        (second, first)
    }
}

fn assert_import_revision(
    condition: Option<StorageImportWriteCondition>,
    current_revision: ResourceRevision,
) -> Result<(), StorageError> {
    let Some(expected) = condition.and_then(StorageImportWriteCondition::expected_revision) else {
        return Ok(());
    };
    if expected == current_revision.get() {
        return Ok(());
    }
    Err(StorageError::precondition_failed(
        format!(
            "stale_revision: expected revision {expected}, observed {}",
            current_revision.get()
        ),
        Some(current_revision),
    ))
}

fn assert_import_create_condition(
    condition: Option<StorageImportWriteCondition>,
) -> Result<(), StorageError> {
    if condition.is_some_and(StorageImportWriteCondition::requires_existing) {
        return Err(StorageError::precondition_failed(
            "conditional_import_target_missing",
            None,
        ));
    }
    Ok(())
}

impl Default for MemoryStorage {
    fn default() -> Self {
        Self::new()
    }
}

fn invalid_contract_value(error: StorageValidationError) -> StorageError {
    StorageError::internal(error.to_string())
}

fn page<T>(mut rows: Vec<T>, options: &QueryOptions) -> Result<StoragePage<T>, StorageError> {
    let total = options
        .include_total()
        .then(|| i64::try_from(rows.len()).unwrap_or(i64::MAX));
    if let Some(limit) = options.limit() {
        rows.truncate(limit);
    }
    StoragePage::try_new(rows, total).map_err(invalid_contract_value)
}

fn string_filter_matches(actual: &str, operator: &SearchOperator, expected: &str) -> bool {
    let (operator, negated) = operator.op_and_neg();
    let actual_lower = actual.to_lowercase();
    let expected_lower = expected.to_lowercase();
    let matched = match operator {
        Operator::Equals => actual == expected,
        Operator::IEquals => actual_lower == expected_lower,
        Operator::Contains => actual.contains(expected),
        Operator::IContains => actual_lower.contains(&expected_lower),
        Operator::StartsWith => actual.starts_with(expected),
        Operator::IStartsWith => actual_lower.starts_with(&expected_lower),
        Operator::EndsWith => actual.ends_with(expected),
        Operator::IEndsWith => actual_lower.ends_with(&expected_lower),
        Operator::In => expected.split(',').any(|value| actual == value.trim()),
        _ => true,
    };
    matched != negated
}

fn resource_filters_match(options: &QueryOptions, id: i32, name: &str, description: &str) -> bool {
    options.filters().as_slice().iter().all(|filter| {
        let actual = match filter.field {
            FilterField::Id => id.to_string(),
            FilterField::Name => name.to_string(),
            FilterField::Description => description.to_string(),
            _ => return true,
        };
        string_filter_matches(&actual, &filter.operator, &filter.value)
    })
}

fn authorization_collection(
    collection: &StorageCollection,
) -> Result<StorageAuthorizationCollection, StorageError> {
    StorageAuthorizationCollection::try_new(
        collection.id(),
        collection.name(),
        collection.description(),
        collection.created_at(),
        collection.updated_at(),
        collection.parent_collection_id(),
        collection.revision(),
    )
    .map_err(invalid_contract_value)
}

fn authorization_group(
    group: &StorageIdentityGroup,
) -> Result<StorageAuthorizationGroup, StorageError> {
    let identity = StorageAuthorizationGroupIdentity::new(
        group.id(),
        group.name(),
        group.identity_scope_id(),
        group.managed_by(),
        group.external_key().map(ToOwned::to_owned),
    );
    let profile = StorageAuthorizationGroupProfile::try_new(
        group.description(),
        group.created_at(),
        group.updated_at(),
        group.revision(),
    )
    .map_err(invalid_contract_value)?;
    let sync = StorageAuthorizationGroupSyncState::try_new(
        group.last_sync_attempted_at(),
        group.last_sync_success_at(),
    )
    .map_err(invalid_contract_value)?;
    Ok(StorageAuthorizationGroup::new(identity, profile, sync))
}

fn principal_group_ids(state: &MemoryState, principal_id: PrincipalId) -> Vec<GroupId> {
    state
        .memberships
        .keys()
        .filter(|(candidate_principal_id, _)| *candidate_principal_id == principal_id.id())
        .map(|(_, group_id)| GroupId::new(*group_id).expect("stored group ids are positive"))
        .collect()
}

fn permissions_include(
    available: &[StorageAuthorizationPermission],
    required: &[StorageAuthorizationPermission],
) -> bool {
    required
        .iter()
        .all(|permission| available.contains(permission))
}

fn principal_has_collection_permissions(
    state: &MemoryState,
    principal_id: PrincipalId,
    collection_id: CollectionId,
    permissions: &[StorageAuthorizationPermission],
) -> bool {
    let group_ids = principal_group_ids(state, principal_id);
    group_ids.iter().any(|group_id| {
        state
            .authorization_grants
            .get(&(collection_id.id(), group_id.id()))
            .is_some_and(|grant| permissions_include(grant.permissions(), permissions))
    })
}

fn authorization_group_grant(
    state: &MemoryState,
    grant: &StorageAuthorizationGrant,
) -> Result<StorageAuthorizationGroupGrant, StorageError> {
    let group = state
        .groups
        .get(&grant.group_id().id())
        .ok_or_else(|| StorageError::internal("authorization grant group is missing"))?;
    StorageAuthorizationGroupGrant::try_new(authorization_group(group)?, grant.clone())
        .map_err(invalid_contract_value)
}

fn authorization_policy_row(
    state: &MemoryState,
    grant: &StorageAuthorizationGrant,
) -> Result<StorageAuthorizationPolicySnapshotRow, StorageError> {
    let group = state
        .groups
        .get(&grant.group_id().id())
        .ok_or_else(|| StorageError::internal("authorization grant group is missing"))?;
    let collection = state
        .collections
        .get(&grant.collection_id().id())
        .ok_or_else(|| StorageError::internal("authorization grant collection is missing"))?;
    StorageAuthorizationPolicySnapshotRow::try_new(
        grant.clone(),
        authorization_group(group)?,
        authorization_collection(collection)?,
    )
    .map_err(invalid_contract_value)
}

fn authorization_effective_group_grant(
    state: &MemoryState,
    grant: &StorageAuthorizationGrant,
) -> Result<StorageAuthorizationEffectiveGroupGrant, StorageError> {
    let collection = state
        .collections
        .get(&grant.collection_id().id())
        .ok_or_else(|| StorageError::internal("authorization grant collection is missing"))?;
    let group = state
        .groups
        .get(&grant.group_id().id())
        .ok_or_else(|| StorageError::internal("authorization grant group is missing"))?;
    let collection = authorization_collection(collection)?;
    Ok(StorageAuthorizationEffectiveGroupGrant::new(
        collection.clone(),
        collection,
        0,
        false,
        authorization_group(group)?,
        grant.clone(),
    ))
}

fn rebuild_event_delivery(
    delivery: &StorageEventDelivery,
    status: EventDeliveryStatus,
    attempts: i32,
    next_attempt_at: DateTime<Utc>,
    last_error: Option<String>,
    locked_until: Option<DateTime<Utc>>,
) -> Result<StorageEventDelivery, StorageError> {
    StorageEventDelivery::builder(
        delivery.id(),
        delivery.event_id(),
        delivery.subscription_id(),
        status,
        next_attempt_at,
        delivery.created_at(),
        Utc::now(),
    )
    .attempts(attempts)
    .last_error(last_error)
    .locked_until(locked_until)
    .try_build()
    .map_err(invalid_contract_value)
}

fn event_status_counts(
    deliveries: impl IntoIterator<Item = EventDeliveryStatus>,
) -> Result<StorageEventDeliveryStatusSnapshot, StorageError> {
    let mut total = 0_i64;
    let mut pending = 0_i64;
    let mut in_flight = 0_i64;
    let mut succeeded = 0_i64;
    let mut failed = 0_i64;
    let mut dead = 0_i64;
    for status in deliveries {
        total += 1;
        match status {
            EventDeliveryStatus::Pending => pending += 1,
            EventDeliveryStatus::InFlight => in_flight += 1,
            EventDeliveryStatus::Succeeded => succeeded += 1,
            EventDeliveryStatus::Failed => failed += 1,
            EventDeliveryStatus::Dead => dead += 1,
        }
    }
    StorageEventDeliveryStatusSnapshot::try_new(
        total, pending, in_flight, succeeded, failed, dead, failed,
    )
    .map_err(invalid_contract_value)
}

fn history_scope_allows(
    scope: &StorageHistoryCollectionScope,
    collection_id: CollectionId,
) -> bool {
    match scope {
        StorageHistoryCollectionScope::All => true,
        StorageHistoryCollectionScope::Visible(ids) => ids.contains(&collection_id),
    }
}

fn history_valid_to(state: &MemoryState, entry: &MemoryHistoryEntry) -> Option<DateTime<Utc>> {
    let variant = std::mem::discriminant(&entry.value);
    state
        .history
        .iter()
        .filter(|candidate| {
            candidate.id != entry.id
                && candidate.value.entity_id() == entry.value.entity_id()
                && std::mem::discriminant(&candidate.value) == variant
                && (candidate.valid_from > entry.valid_from
                    || (candidate.valid_from == entry.valid_from
                        && candidate.id.id() > entry.id.id()))
        })
        .min_by_key(|candidate| (candidate.valid_from, candidate.id.id()))
        .map(|candidate| candidate.valid_from)
}

fn transition_restore_record(
    record: &MemoryRestoreRecord,
    status: StorageRestoreJobStatus,
    error: Option<String>,
    confirmed_at: Option<DateTime<Utc>>,
    finished_at: Option<DateTime<Utc>>,
    erase_document: bool,
) -> Result<MemoryRestoreRecord, StorageError> {
    let (summary, mut document, capability_hash) = record.job.clone().into_parts();
    let (id, _, initiator, artifact, _, timestamps) = summary.into_parts();
    let timestamp_parts = timestamps.into_parts();
    let timestamps = StorageRestoreTimestamps::try_new(
        timestamp_parts.expires_at(),
        confirmed_at,
        finished_at,
        timestamp_parts.created_at(),
        Utc::now(),
    )
    .map_err(invalid_contract_value)?;
    let summary =
        StorageRestoreJobSummary::try_new(id, status, initiator, artifact, error, timestamps)
            .map_err(invalid_contract_value)?;
    if erase_document {
        document.clear();
    }
    let job = StorageRestoreJob::try_new(summary, document, capability_hash)
        .map_err(invalid_contract_value)?;
    Ok(MemoryRestoreRecord {
        job,
        validation_summary: record.validation_summary.clone(),
    })
}

fn class_with_collection(
    state: &MemoryState,
    class: &StorageClass,
) -> Result<StorageClassWithCollection, StorageError> {
    let collection = state
        .collections
        .get(&class.collection_id().id())
        .cloned()
        .ok_or_else(|| StorageError::internal("class collection is missing"))?;
    let metadata = StorageRecordMetadata::try_new(
        ResourceId::new(class.id().id()).expect("class id is positive"),
        class.created_at(),
        class.updated_at(),
        class.revision(),
    )
    .map_err(invalid_contract_value)?;
    Ok(
        StorageClassWithCollection::builder(
            metadata,
            class.name(),
            collection,
            class.description(),
        )
        .json_schema(class.json_schema().cloned())
        .validate_schema(class.validates_schema())
        .build(),
    )
}

fn search_rank(name: &str, description: &str, extended: Option<&str>, term: &str) -> Option<i32> {
    let name = name.to_lowercase();
    let description = description.to_lowercase();
    let extended = extended.map(str::to_lowercase);
    let term = term.to_lowercase();
    if name == term {
        Some(3)
    } else if name.starts_with(&term) {
        Some(2)
    } else if name.contains(&term)
        || description.contains(&term)
        || extended.as_ref().is_some_and(|value| value.contains(&term))
    {
        Some(1)
    } else {
        None
    }
}

fn graph_class(class: &StorageClass) -> Result<StorageGraphClass, StorageError> {
    let metadata = StorageRecordMetadata::try_new(
        ResourceId::new(class.id().id()).expect("class id is positive"),
        class.created_at(),
        class.updated_at(),
        class.revision(),
    )
    .map_err(invalid_contract_value)?;
    Ok(StorageGraphClass::new(
        StorageGraphResource::new(
            metadata,
            class.name().to_string(),
            class.collection_id(),
            class.description().to_string(),
        ),
        class.json_schema().cloned(),
        class.validates_schema(),
    ))
}

fn graph_object(object: &StorageObject) -> Result<StorageGraphObject, StorageError> {
    let metadata = StorageRecordMetadata::try_new(
        ResourceId::new(object.id().id()).expect("object id is positive"),
        object.created_at(),
        object.updated_at(),
        object.revision(),
    )
    .map_err(invalid_contract_value)?;
    Ok(StorageGraphObject::new(
        StorageGraphResource::new(
            metadata,
            object.name().to_string(),
            object.collection_id(),
            object.description().to_string(),
        ),
        object.class_id(),
        object.data().clone(),
    ))
}

fn ready_computation_state(
    class_id: ClassId,
    revision: i64,
    created_at: DateTime<Utc>,
) -> Result<StorageClassComputationState, StorageError> {
    StorageClassComputationState::builder(
        class_id,
        StorageComputationRevision::try_new(revision).map_err(invalid_contract_value)?,
        StorageComputationRebuildStatus::Ready,
        created_at,
        Utc::now(),
    )
    .try_build()
    .map_err(invalid_contract_value)
}

fn updated_computed_field(
    current: &StorageComputedFieldDefinition,
    patch: &StorageComputedFieldDefinitionPatch,
    actor_id: PrincipalId,
) -> Result<StorageComputedFieldDefinition, StorageError> {
    let metadata = current.metadata();
    let metadata = StorageRecordMetadata::try_new(
        metadata.id(),
        metadata.created_at(),
        Utc::now(),
        metadata
            .revision()
            .checked_advance()
            .map_err(|error| StorageError::internal(error.to_string()))?,
    )
    .map_err(invalid_contract_value)?;
    let input = StorageComputedFieldDefinitionInput::new(
        patch.key().unwrap_or(current.key()).to_string(),
        patch.label().unwrap_or(current.label()).to_string(),
        patch
            .operation()
            .cloned()
            .unwrap_or_else(|| current.operation().clone()),
        patch
            .result_type()
            .unwrap_or(current.result_type())
            .to_string(),
    )
    .with_description(
        patch
            .description()
            .unwrap_or(current.description())
            .to_string(),
    )
    .with_enabled(patch.enabled().unwrap_or(current.enabled()));
    Ok(StorageComputedFieldDefinition::new(
        metadata,
        current.class_id(),
        current.visibility(),
        StorageComputedFieldDefinitionContent::new(input, current.semantics_version()),
        StorageComputedFieldProvenance::new(current.created_by(), Some(actor_id)),
    ))
}

fn evaluate_computed_definition(
    definition: &StorageComputedFieldDefinition,
    object: &StorageObject,
) -> serde_json::Value {
    definition
        .operation()
        .get("paths")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(serde_json::Value::as_str)
        .find_map(|path| object.data().pointer(path).cloned())
        .unwrap_or(serde_json::Value::Null)
}

fn computed_scope(
    state: &MemoryState,
    object: &StorageObject,
    visibility: StorageComputedFieldVisibility,
) -> StorageComputedScope {
    let values = state
        .computed_fields
        .values()
        .filter(|definition| {
            definition.class_id() == object.class_id()
                && definition.visibility() == visibility
                && definition.enabled()
        })
        .map(|definition| {
            (
                definition.key().to_string(),
                evaluate_computed_definition(definition, object),
            )
        })
        .collect();
    StorageComputedScope::new(values, BTreeMap::new())
}

fn computed_object(
    state: &MemoryState,
    object: StorageObject,
    personal_owner_id: Option<PrincipalId>,
) -> Result<StorageComputedObject, StorageError> {
    let revision = state
        .computation_states
        .get(&object.class_id().id())
        .map_or(0, |value| value.evaluation_revision().get());
    let shared = StorageSharedComputedScope::new(
        StorageComputationRevision::try_new(revision).map_err(invalid_contract_value)?,
        false,
        computed_scope(state, &object, StorageComputedFieldVisibility::Shared),
    );
    let personal = personal_owner_id.map(|owner_id| {
        computed_scope(
            state,
            &object,
            StorageComputedFieldVisibility::Personal { owner_id },
        )
    });
    Ok(StorageComputedObject::new(object, shared, personal))
}

fn export_output_summary(
    output: &StorageExportOutput,
) -> Result<StorageExportOutputSummary, StorageError> {
    StorageExportOutputSummary::try_new(
        output.task_id(),
        output.template_name().map(ToOwned::to_owned),
        output.content_type(),
        output.warning_count(),
        output.truncated(),
        output.output_expires_at(),
        output.durations(),
    )
    .map_err(invalid_contract_value)
}

fn backup_output_summary(
    output: &StorageBackupOutput,
) -> Result<StorageBackupOutputSummary, StorageError> {
    StorageBackupOutputSummary::try_new(
        output.task_id(),
        output.byte_size(),
        output.sha256(),
        output.output_expires_at(),
    )
    .map_err(invalid_contract_value)
}

fn invalid_task_lease() -> StorageError {
    StorageError::conflict("Task lease is no longer valid")
}

fn advanced_principal(
    current: &StoragePrincipal,
    name: impl Into<String>,
    settings: serde_json::Value,
    updated_at: DateTime<Utc>,
) -> Result<StoragePrincipal, StorageError> {
    let revision = current
        .revision()
        .checked_advance()
        .map_err(|error| StorageError::internal(error.to_string()))?;
    let metadata = StorageRecordMetadata::try_new(
        ResourceId::new(current.id().id()).expect("principal id is positive"),
        current.created_at(),
        updated_at,
        revision,
    )
    .map_err(invalid_contract_value)?;
    StoragePrincipal::builder(metadata, current.kind(), name, current.identity_scope_id())
        .provider_managed(current.provider_managed())
        .settings(settings)
        .external_subject(current.external_subject().map(ToOwned::to_owned))
        .last_sync_attempted_at(current.last_sync_attempted_at())
        .last_sync_success_at(current.last_sync_success_at())
        .try_build()
        .map_err(invalid_contract_value)
}

fn token_state_matches(metadata: &StorageTokenMetadata, state: StorageTokenListState) -> bool {
    match state {
        StorageTokenListState::Active => metadata.is_active(),
        StorageTokenListState::Expired => metadata.is_expired(),
        StorageTokenListState::Revoked => metadata.revoked_at().is_some(),
        StorageTokenListState::All => true,
    }
}

fn empty_event_fanout_snapshot() -> Result<StorageEventFanoutSnapshot, StorageError> {
    StorageEventFanoutSnapshot::try_new(0, 0, 0, None).map_err(invalid_contract_value)
}

fn empty_event_queue_snapshot() -> Result<StorageEventQueueSnapshot, StorageError> {
    StorageEventQueueSnapshot::try_new(
        StorageEventDeliveryStatusSnapshot::try_new(0, 0, 0, 0, 0, 0, 0)
            .map_err(invalid_contract_value)?,
        0,
        None,
    )
    .map_err(invalid_contract_value)
}

mod adapter;
mod events;
mod execution;
mod identity;
mod operational;
mod queries;
mod resources;
mod workflows;
