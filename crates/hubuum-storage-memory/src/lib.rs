//! Independent, process-local storage adapter used to validate Hubuum's
//! backend-neutral storage contract.

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use hubuum_computed_fields::{
    Definition, EvaluationLimits, FieldKey, Operation, ResultType, evaluate,
};
use hubuum_domain::*;
use hubuum_events_core::*;
use hubuum_query::*;
use hubuum_storage_core::capabilities::{
    backend::*, common::*, events::*, identity::*, operational::*, queries::*, resources::*,
    workflows::*,
};
use hubuum_storage_core::{
    StorageAuthenticationCredential, StorageTokenDigest, StorageTokenFormat,
    StorageTokenHashAlgorithm, StorageTokenHashKeyId, StorageTokenMigrationOutcome,
    StorageValidationError,
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
    token_format: StorageTokenFormat,
    token_hash_algorithm: StorageTokenHashAlgorithm,
    token_hash_key_id: Option<StorageTokenHashKeyId>,
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
    trace_link: Option<TraceLink>,
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
        .trace_link(self.trace_link.clone())
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
    fn matches_credential(&self, credential: &StorageAuthenticationCredential) -> bool {
        let digest = credential.digest();
        if !digest.matches_lookup_value(&self.token_hash)
            || self.token_format != digest.format()
            || self.token_hash_algorithm != digest.algorithm()
        {
            return false;
        }
        match self.token_format {
            StorageTokenFormat::Version1 => self.token_hash_key_id.as_ref() == digest.key_id(),
            StorageTokenFormat::Legacy => {
                self.token_hash_key_id.is_none()
                    || digest.key_id().is_none()
                    || self.token_hash_key_id.as_ref() == digest.key_id()
            }
        }
    }

    fn migrate_legacy_digest(&mut self, target: StorageTokenDigest) {
        let (token_hash, token_format, token_hash_algorithm, token_hash_key_id) =
            target.into_parts();
        self.token_hash = token_hash;
        self.token_format = token_format;
        self.token_hash_algorithm = token_hash_algorithm;
        self.token_hash_key_id = token_hash_key_id;
    }

    fn metadata(
        &self,
        observation: StorageTokenObservation,
    ) -> Result<StorageTokenMetadata, StorageError> {
        let (observed_at, legacy_valid_after) = observation.into_parts();
        let expired = self
            .expires_at
            .is_some_and(|expires_at| expires_at <= observed_at)
            || (self.expires_at.is_none() && self.issued <= legacy_valid_after);
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
    import_execution_receipts: BTreeSet<(i32, usize)>,
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
            .correlation_id(context.correlation_id().cloned())
            .trace_link(context.trace_link().cloned())
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

impl MemoryStorage {
    /// Creates an empty in-memory adapter with Hubuum's required bootstrap records.
    #[must_use]
    pub fn new() -> Self {
        Self {
            state: Arc::new(RwLock::new(MemoryState::new())),
        }
    }
}

impl Default for MemoryStorage {
    fn default() -> Self {
        Self::new()
    }
}

mod support;
use support::*;

mod events;
mod execution;
mod identity;
mod imports;
mod operational;
mod queries;
mod resources;
mod state;
mod workflows;
