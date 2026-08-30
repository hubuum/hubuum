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

impl MemoryStorage {
    #[must_use]
    pub fn new() -> Self {
        Self {
            state: Arc::new(RwLock::new(MemoryState::new())),
        }
    }

    async fn import_identity_scope_id(
        &self,
        reference: Option<&str>,
        key: Option<&StorageImportIdentityScopeKey>,
        references: &BTreeMap<String, MemoryImportReference>,
    ) -> Result<IdentityScopeId, StorageError> {
        if let Some(reference) = reference {
            return match references.get(reference) {
                Some(MemoryImportReference::IdentityScope(id)) => Ok(*id),
                _ => Err(StorageError::not_found(format!(
                    "Import identity-scope reference '{reference}' was not found"
                ))),
            };
        }
        let name = key
            .ok_or_else(|| {
                StorageError::invalid_input("Import identity scope selector is missing")
            })?
            .clone()
            .into_parts()
            .name;
        self.state
            .read()
            .await
            .identity_scope_by_name(&name)
            .map(StorageIdentityScope::id)
            .ok_or_else(|| StorageError::not_found("Import identity scope was not found"))
    }

    async fn import_group_id(
        &self,
        reference: Option<&str>,
        key: Option<&StorageImportGroupKey>,
        references: &BTreeMap<String, MemoryImportReference>,
    ) -> Result<GroupId, StorageError> {
        if let Some(reference) = reference {
            return match references.get(reference) {
                Some(MemoryImportReference::Group(id)) => Ok(*id),
                _ => Err(StorageError::not_found(format!(
                    "Import group reference '{reference}' was not found"
                ))),
            };
        }
        let parts = key
            .ok_or_else(|| StorageError::invalid_input("Import group selector is missing"))?
            .clone()
            .into_parts();
        let state = self.state.read().await;
        state
            .groups
            .values()
            .find(|group| {
                group.name() == parts.name
                    && state
                        .identity_scopes
                        .get(&group.identity_scope_id().id())
                        .is_some_and(|scope| scope.name() == parts.identity_scope)
            })
            .map(StorageIdentityGroup::id)
            .ok_or_else(|| StorageError::not_found("Import group was not found"))
    }

    async fn import_principal_id(
        &self,
        reference: Option<&str>,
        key: Option<&StorageImportPrincipalKey>,
        references: &BTreeMap<String, MemoryImportReference>,
    ) -> Result<PrincipalId, StorageError> {
        if let Some(reference) = reference {
            return match references.get(reference) {
                Some(MemoryImportReference::Principal(id)) => Ok(*id),
                _ => Err(StorageError::not_found(format!(
                    "Import principal reference '{reference}' was not found"
                ))),
            };
        }
        let parts = key
            .ok_or_else(|| StorageError::invalid_input("Import principal selector is missing"))?
            .clone()
            .into_parts();
        let state = self.state.read().await;
        state
            .principals
            .values()
            .find(|principal| {
                principal.name() == parts.name
                    && state
                        .identity_scopes
                        .get(&principal.identity_scope_id().id())
                        .is_some_and(|scope| scope.name() == parts.identity_scope)
            })
            .map(StoragePrincipal::id)
            .ok_or_else(|| StorageError::not_found("Import principal was not found"))
    }

    async fn import_collection_id(
        &self,
        reference: Option<&str>,
        key: Option<&StorageImportCollectionKey>,
        references: &BTreeMap<String, MemoryImportReference>,
    ) -> Result<CollectionId, StorageError> {
        if let Some(reference) = reference {
            return match references.get(reference) {
                Some(MemoryImportReference::Collection(id)) => Ok(*id),
                _ => Err(StorageError::not_found(format!(
                    "Import collection reference '{reference}' was not found"
                ))),
            };
        }
        let key = key
            .ok_or_else(|| StorageError::invalid_input("Import collection selector is missing"))?;
        self.get_import_collection_by_key(key)
            .await?
            .map(|collection| collection.id())
            .ok_or_else(|| StorageError::not_found("Import collection was not found"))
    }

    async fn import_class_id(
        &self,
        reference: Option<&str>,
        key: Option<&StorageImportClassKey>,
        references: &BTreeMap<String, MemoryImportReference>,
    ) -> Result<ClassId, StorageError> {
        if let Some(reference) = reference {
            return match references.get(reference) {
                Some(MemoryImportReference::Class(id)) => Ok(*id),
                _ => Err(StorageError::not_found(format!(
                    "Import class reference '{reference}' was not found"
                ))),
            };
        }
        let parts = key
            .ok_or_else(|| StorageError::invalid_input("Import class selector is missing"))?
            .clone()
            .into_parts();
        let collection_id = self
            .import_collection_id(
                parts.collection_ref.as_deref(),
                parts.collection_key.as_ref(),
                references,
            )
            .await?;
        self.get_import_class_by_name(collection_id, &parts.name)
            .await?
            .map(|class| class.id())
            .ok_or_else(|| StorageError::not_found("Import class was not found"))
    }

    async fn import_object_id(
        &self,
        reference: Option<&str>,
        key: Option<&StorageImportObjectKey>,
        references: &BTreeMap<String, MemoryImportReference>,
    ) -> Result<ObjectId, StorageError> {
        if let Some(reference) = reference {
            return match references.get(reference) {
                Some(MemoryImportReference::Object(id)) => Ok(*id),
                _ => Err(StorageError::not_found(format!(
                    "Import object reference '{reference}' was not found"
                ))),
            };
        }
        let parts = key
            .ok_or_else(|| StorageError::invalid_input("Import object selector is missing"))?
            .clone()
            .into_parts();
        let class_id = self
            .import_class_id(
                parts.class_ref.as_deref(),
                parts.class_key.as_ref(),
                references,
            )
            .await?;
        self.get_import_object_by_name(class_id, &parts.name)
            .await?
            .map(|object| object.id())
            .ok_or_else(|| StorageError::not_found("Import object was not found"))
    }

    async fn import_event_sink_id(
        &self,
        reference: Option<&str>,
        key: Option<&StorageImportEventSinkKey>,
        references: &BTreeMap<String, MemoryImportReference>,
    ) -> Result<EventSinkId, StorageError> {
        if let Some(reference) = reference {
            return match references.get(reference) {
                Some(MemoryImportReference::EventSink(id)) => Ok(*id),
                _ => Err(StorageError::not_found(format!(
                    "Import event-sink reference '{reference}' was not found"
                ))),
            };
        }
        let name = key
            .ok_or_else(|| StorageError::invalid_input("Import event-sink selector is missing"))?
            .clone()
            .into_parts()
            .name;
        self.state
            .read()
            .await
            .event_sinks
            .values()
            .find(|sink| sink.name() == name)
            .map(StorageEventSink::id)
            .ok_or_else(|| StorageError::not_found("Import event sink was not found"))
    }

    async fn apply_import_operation(
        &self,
        operation: StorageImportOperation,
        references: &mut BTreeMap<String, MemoryImportReference>,
    ) -> Result<Option<ResourceRevision>, StorageError> {
        match operation {
            StorageImportOperation::UpsertIdentityScope { input, overwrite } => {
                let parts = input.into_parts();
                let mut state = self.state.write().await;
                let existing = state.identity_scope_by_name(&parts.name).cloned();
                if let Some(current) = &existing {
                    assert_import_revision(parts.condition, current.revision())?;
                    if !overwrite {
                        return Err(StorageError::conflict(
                            "Import identity scope already exists",
                        ));
                    }
                } else {
                    assert_import_create_condition(parts.condition)?;
                }
                let id = existing.as_ref().map_or_else(
                    || {
                        let id = IdentityScopeId::new(state.next_identity_scope_id)
                            .expect("memory import identity scope id is positive");
                        state.next_identity_scope_id += 1;
                        id
                    },
                    StorageIdentityScope::id,
                );
                let now = Utc::now();
                let (created_at, updated_at) = parts.timestamps.map_or_else(
                    || {
                        existing
                            .as_ref()
                            .map_or((now, now), |current| (current.created_at(), now))
                    },
                    StorageImportTimestamps::into_parts,
                );
                let revision = existing
                    .as_ref()
                    .map_or(Ok(ResourceRevision::INITIAL), |current| {
                        current.revision().checked_advance()
                    })
                    .map_err(|error| StorageError::internal(error.to_string()))?;
                let scope = StorageIdentityScope::try_new(
                    id,
                    parts.name,
                    parts.provider_kind,
                    created_at,
                    updated_at,
                    revision,
                )
                .map_err(invalid_contract_value)?;
                state.identity_scopes.insert(id.id(), scope);
                if let Some(reference) = parts.reference {
                    references.insert(reference, MemoryImportReference::IdentityScope(id));
                }
                Ok(Some(revision))
            }
            StorageImportOperation::UpsertGroup { input, overwrite } => {
                let parts = input.into_parts();
                let scope_id = self
                    .import_identity_scope_id(
                        parts.identity_scope_ref.as_deref(),
                        parts.identity_scope_key.as_ref(),
                        references,
                    )
                    .await?;
                let mut state = self.state.write().await;
                let existing = state
                    .groups
                    .values()
                    .find(|group| {
                        group.identity_scope_id() == scope_id && group.name() == parts.name
                    })
                    .cloned();
                if let Some(current) = &existing {
                    assert_import_revision(parts.condition, current.revision())?;
                    if !overwrite {
                        return Err(StorageError::conflict("Import group already exists"));
                    }
                } else {
                    assert_import_create_condition(parts.condition)?;
                }
                let id = existing.as_ref().map_or_else(
                    || {
                        let id = GroupId::new(state.next_group_id)
                            .expect("memory import group id is positive");
                        state.next_group_id += 1;
                        id
                    },
                    StorageIdentityGroup::id,
                );
                let now = Utc::now();
                let (created_at, updated_at) = parts.timestamps.map_or_else(
                    || {
                        existing
                            .as_ref()
                            .map_or((now, now), |value| (value.created_at(), now))
                    },
                    StorageImportTimestamps::into_parts,
                );
                let revision = existing
                    .as_ref()
                    .map_or(Ok(ResourceRevision::INITIAL), |value| {
                        value.revision().checked_advance()
                    })
                    .map_err(|error| StorageError::internal(error.to_string()))?;
                let metadata = StorageRecordMetadata::try_new(
                    ResourceId::new(id.id()).expect("group id is a valid resource id"),
                    created_at,
                    updated_at,
                    revision,
                )
                .map_err(invalid_contract_value)?;
                let group = StorageIdentityGroup::builder(
                    metadata,
                    parts.name,
                    parts.description,
                    scope_id,
                    parts.managed_by,
                )
                .external_key(parts.external_key)
                .last_sync_attempted_at(parts.last_sync_attempted_at)
                .last_sync_success_at(parts.last_sync_success_at)
                .try_build()
                .map_err(invalid_contract_value)?;
                state.groups.insert(id.id(), group);
                if let Some(reference) = parts.reference {
                    references.insert(reference, MemoryImportReference::Group(id));
                }
                Ok(Some(revision))
            }
            StorageImportOperation::UpsertPrincipal { input, overwrite } => {
                self.apply_import_principal(input, overwrite, references)
                    .await
            }
            StorageImportOperation::UpsertGroupMembership { input, overwrite } => {
                let parts = input.into_parts();
                let principal_id = self
                    .import_principal_id(
                        parts.principal_ref.as_deref(),
                        parts.principal_key.as_ref(),
                        references,
                    )
                    .await?;
                let group_id = self
                    .import_group_id(
                        parts.group_ref.as_deref(),
                        parts.group_key.as_ref(),
                        references,
                    )
                    .await?;
                let mut state = self.state.write().await;
                let existing = state
                    .memberships
                    .get(&(principal_id.id(), group_id.id()))
                    .cloned();
                if let Some(current) = &existing {
                    assert_import_revision(parts.condition, current.revision())?;
                    if !overwrite {
                        return Err(StorageError::conflict("Import membership already exists"));
                    }
                } else {
                    assert_import_create_condition(parts.condition)?;
                }
                let now = Utc::now();
                let (created_at, updated_at) = parts.timestamps.map_or_else(
                    || {
                        existing
                            .as_ref()
                            .map_or((now, now), |value| (value.created_at(), now))
                    },
                    StorageImportTimestamps::into_parts,
                );
                let revision = existing
                    .as_ref()
                    .map_or(Ok(ResourceRevision::INITIAL), |value| {
                        value.revision().checked_advance()
                    })
                    .map_err(|error| StorageError::internal(error.to_string()))?;
                let membership = StoragePrincipalGroup::try_new(
                    principal_id,
                    group_id,
                    created_at,
                    updated_at,
                    revision,
                )
                .map_err(invalid_contract_value)?;
                state
                    .memberships
                    .insert((principal_id.id(), group_id.id()), membership);
                if parts.sources.is_empty() {
                    state
                        .external_memberships
                        .remove(&(principal_id.id(), group_id.id()));
                } else {
                    state
                        .external_memberships
                        .insert((principal_id.id(), group_id.id()));
                }
                Ok(Some(revision))
            }
            StorageImportOperation::CreateCollection(input) => {
                let parts = input.into_parts();
                assert_import_create_condition(parts.condition)?;
                let parent_collection_id = if let Some(reference) = parts.parent_collection_ref {
                    match references.get(&reference) {
                        Some(MemoryImportReference::Collection(id)) => Some(*id),
                        _ => {
                            return Err(StorageError::not_found(format!(
                                "Import collection reference '{reference}' was not found"
                            )));
                        }
                    }
                } else if let Some(key) = parts.parent_collection_key {
                    self.get_import_collection_by_key(&key)
                        .await?
                        .map(|collection| collection.id())
                        .ok_or_else(|| {
                            StorageError::not_found("Import parent collection was not found")
                        })?
                        .into()
                } else {
                    Some(CollectionId::new(ROOT_COLLECTION_ID).expect("root id is valid"))
                };
                let owner_group_id = GroupId::new(1).expect("seeded admin group id is valid");
                let created = self
                    .create_collection(
                        StorageCollectionCreate::new(
                            parts.name,
                            parts.description,
                            owner_group_id,
                            parent_collection_id,
                        ),
                        &EventContext::system(),
                    )
                    .await?
                    .into_value();
                if let Some(reference) = parts.reference {
                    references.insert(reference, MemoryImportReference::Collection(created.id()));
                }
                Ok(Some(created.revision()))
            }
            StorageImportOperation::CreateClass(input) => {
                let parts = input.into_parts();
                assert_import_create_condition(parts.condition)?;
                let collection_id = if let Some(reference) = parts.collection_ref {
                    match references.get(&reference) {
                        Some(MemoryImportReference::Collection(id)) => *id,
                        _ => {
                            return Err(StorageError::not_found(format!(
                                "Import collection reference '{reference}' was not found"
                            )));
                        }
                    }
                } else if let Some(key) = parts.collection_key {
                    self.get_import_collection_by_key(&key)
                        .await?
                        .map(|collection| collection.id())
                        .ok_or_else(|| StorageError::not_found("Import collection was not found"))?
                } else {
                    return Err(StorageError::invalid_input(
                        "Import class requires a collection selector",
                    ));
                };
                let created = self
                    .create_class(
                        StorageClassCreate::builder(parts.name, collection_id, parts.description)
                            .json_schema(parts.json_schema)
                            .validate_schema(parts.validate_schema)
                            .build(),
                        &EventContext::system(),
                    )
                    .await?
                    .into_value();
                if let Some(reference) = parts.reference {
                    references.insert(reference, MemoryImportReference::Class(created.id()));
                }
                Ok(Some(created.revision()))
            }
            StorageImportOperation::UpdateCollection {
                collection_id,
                input,
            } => {
                let parts = input.into_parts();
                let current = self.get_collection(collection_id).await?;
                assert_import_revision(parts.condition, current.revision())?;
                let updated = self
                    .update_collection(
                        collection_id,
                        StorageCollectionUpdate::new(Some(parts.name), Some(parts.description)),
                        &EventContext::system(),
                    )
                    .await?
                    .into_value();
                if let Some(reference) = parts.reference {
                    references.insert(reference, MemoryImportReference::Collection(updated.id()));
                }
                Ok(Some(updated.revision()))
            }
            StorageImportOperation::UpdateClass { class_id, input } => {
                let parts = input.into_parts();
                let target = self
                    .resolve_class(StorageClassSelector::Id(class_id))
                    .await?;
                assert_import_revision(parts.condition, target.class().revision())?;
                let updated = self
                    .update_class(
                        &target,
                        StorageClassUpdate::builder()
                            .name(Some(parts.name))
                            .json_schema(parts.json_schema)
                            .validate_schema(Some(parts.validate_schema))
                            .description(Some(parts.description))
                            .build(),
                        &EventContext::system(),
                    )
                    .await?
                    .into_value();
                if let Some(reference) = parts.reference {
                    references.insert(reference, MemoryImportReference::Class(updated.id()));
                }
                Ok(Some(updated.revision()))
            }
            StorageImportOperation::CreateObject(input) => {
                let parts = input.into_parts();
                assert_import_create_condition(parts.condition)?;
                let class_id = self
                    .import_class_id(
                        parts.class_ref.as_deref(),
                        parts.class_key.as_ref(),
                        references,
                    )
                    .await?;
                let class = self
                    .resolve_class(StorageClassSelector::Id(class_id))
                    .await?;
                let created = self
                    .create_object(
                        &class,
                        StorageObjectCreate::new(
                            parts.name,
                            class.class().collection_id(),
                            class_id,
                            parts.data,
                            parts.description,
                        ),
                        &EventContext::system(),
                    )
                    .await?
                    .into_value();
                if let Some(reference) = parts.reference {
                    references.insert(reference, MemoryImportReference::Object(created.id()));
                }
                Ok(Some(created.revision()))
            }
            StorageImportOperation::UpdateObject { object_id, input } => {
                let parts = input.into_parts();
                let current = self
                    .state
                    .read()
                    .await
                    .objects
                    .get(&object_id.id())
                    .cloned()
                    .ok_or_else(|| StorageError::not_found("Import object was not found"))?;
                assert_import_revision(parts.condition, current.revision())?;
                let target = self
                    .resolve_object(StorageObjectSelector::Ids {
                        class_id: current.class_id(),
                        object_id,
                    })
                    .await?;
                let updated = self
                    .update_object(
                        &target,
                        StorageObjectUpdate::builder()
                            .name(Some(parts.name))
                            .data(Some(parts.data))
                            .description(Some(parts.description))
                            .build(),
                        &EventContext::system(),
                    )
                    .await?
                    .into_value();
                if let Some(reference) = parts.reference {
                    references.insert(reference, MemoryImportReference::Object(updated.id()));
                }
                Ok(Some(updated.revision()))
            }
            StorageImportOperation::CreateClassRelation(input) => {
                let parts = input.into_parts();
                assert_import_create_condition(parts.condition)?;
                let from_class_id = self
                    .import_class_id(
                        parts.from_class_ref.as_deref(),
                        parts.from_class_key.as_ref(),
                        references,
                    )
                    .await?;
                let to_class_id = self
                    .import_class_id(
                        parts.to_class_ref.as_deref(),
                        parts.to_class_key.as_ref(),
                        references,
                    )
                    .await?;
                let (from_class_id, to_class_id) = ordered_ids(from_class_id, to_class_id);
                let prepared = self
                    .prepare_class_relation(
                        StorageClassRelationCreate::builder(from_class_id, to_class_id)
                            .template_aliases(
                                parts.forward_template_alias,
                                parts.reverse_template_alias,
                            )
                            .relation_limits(parts.from_max_relations, parts.to_max_relations)
                            .build(),
                    )
                    .await?;
                let created = self
                    .create_class_relation(&prepared, &EventContext::system())
                    .await?
                    .into_value();
                Ok(Some(created.relation().metadata().revision()))
            }
            StorageImportOperation::CheckClassRelationCondition(input) => {
                let parts = input.into_parts();
                let from_class_id = self
                    .import_class_id(
                        parts.from_class_ref.as_deref(),
                        parts.from_class_key.as_ref(),
                        references,
                    )
                    .await?;
                let to_class_id = self
                    .import_class_id(
                        parts.to_class_ref.as_deref(),
                        parts.to_class_key.as_ref(),
                        references,
                    )
                    .await?;
                let relation = self
                    .import_class_relation(from_class_id, to_class_id)
                    .await?;
                assert_import_revision(parts.condition, relation.metadata().revision())?;
                Ok(Some(relation.metadata().revision()))
            }
            StorageImportOperation::UpdateClassRelationTimestamps { input, timestamps } => {
                let parts = input.into_parts();
                let from_class_id = self
                    .import_class_id(
                        parts.from_class_ref.as_deref(),
                        parts.from_class_key.as_ref(),
                        references,
                    )
                    .await?;
                let to_class_id = self
                    .import_class_id(
                        parts.to_class_ref.as_deref(),
                        parts.to_class_key.as_ref(),
                        references,
                    )
                    .await?;
                let current = self
                    .import_class_relation(from_class_id, to_class_id)
                    .await?;
                assert_import_revision(parts.condition, current.metadata().revision())?;
                let revision = current
                    .metadata()
                    .revision()
                    .checked_advance()
                    .map_err(|error| StorageError::internal(error.to_string()))?;
                let (created_at, updated_at) = timestamps.into_parts();
                let metadata = StorageRecordMetadata::try_new(
                    current.metadata().id(),
                    created_at,
                    updated_at,
                    revision,
                )
                .map_err(invalid_contract_value)?;
                let updated = StorageClassRelation::try_new(
                    metadata,
                    current.from_class_id(),
                    current.to_class_id(),
                )
                .and_then(|relation| {
                    relation.try_with_template_aliases(
                        current.forward_template_alias().map(ToOwned::to_owned),
                        current.reverse_template_alias().map(ToOwned::to_owned),
                    )
                })
                .and_then(|relation| {
                    relation.try_with_relation_limits(
                        current.from_max_relations(),
                        current.to_max_relations(),
                    )
                })
                .map_err(invalid_contract_value)?;
                self.state
                    .write()
                    .await
                    .class_relations
                    .insert(ClassRelationId::from(updated.metadata().id()).id(), updated);
                Ok(Some(revision))
            }
            StorageImportOperation::CreateObjectRelation(input) => {
                let parts = input.into_parts();
                assert_import_create_condition(parts.condition)?;
                let from_object_id = self
                    .import_object_id(
                        parts.from_object_ref.as_deref(),
                        parts.from_object_key.as_ref(),
                        references,
                    )
                    .await?;
                let to_object_id = self
                    .import_object_id(
                        parts.to_object_ref.as_deref(),
                        parts.to_object_key.as_ref(),
                        references,
                    )
                    .await?;
                let (from_object_id, to_object_id) = ordered_ids(from_object_id, to_object_id);
                let state = self.state.read().await;
                let from = state
                    .objects
                    .get(&from_object_id.id())
                    .ok_or_else(|| StorageError::not_found("Import object was not found"))?;
                let to = state
                    .objects
                    .get(&to_object_id.id())
                    .ok_or_else(|| StorageError::not_found("Import object was not found"))?;
                let from_class_id = from.class_id();
                let to_class_id = to.class_id();
                drop(state);
                let class_relation = self
                    .import_class_relation(from_class_id, to_class_id)
                    .await?;
                let prepared = self
                    .prepare_object_relation(StorageObjectRelationCreateSelector::Explicit(
                        StorageObjectRelationCreate::new(
                            from_object_id,
                            to_object_id,
                            ClassRelationId::from(class_relation.metadata().id()),
                        ),
                    ))
                    .await?;
                let created = self
                    .create_object_relation(&prepared, &EventContext::system())
                    .await?
                    .into_value();
                Ok(Some(created.relation().metadata().revision()))
            }
            StorageImportOperation::CheckObjectRelationCondition(input) => {
                let parts = input.into_parts();
                let from_object_id = self
                    .import_object_id(
                        parts.from_object_ref.as_deref(),
                        parts.from_object_key.as_ref(),
                        references,
                    )
                    .await?;
                let to_object_id = self
                    .import_object_id(
                        parts.to_object_ref.as_deref(),
                        parts.to_object_key.as_ref(),
                        references,
                    )
                    .await?;
                let relation = self
                    .import_object_relation(from_object_id, to_object_id)
                    .await?;
                assert_import_revision(parts.condition, relation.metadata().revision())?;
                Ok(Some(relation.metadata().revision()))
            }
            StorageImportOperation::UpdateObjectRelationTimestamps { input, timestamps } => {
                let parts = input.into_parts();
                let from_object_id = self
                    .import_object_id(
                        parts.from_object_ref.as_deref(),
                        parts.from_object_key.as_ref(),
                        references,
                    )
                    .await?;
                let to_object_id = self
                    .import_object_id(
                        parts.to_object_ref.as_deref(),
                        parts.to_object_key.as_ref(),
                        references,
                    )
                    .await?;
                let current = self
                    .import_object_relation(from_object_id, to_object_id)
                    .await?;
                assert_import_revision(parts.condition, current.metadata().revision())?;
                let revision = current
                    .metadata()
                    .revision()
                    .checked_advance()
                    .map_err(|error| StorageError::internal(error.to_string()))?;
                let (created_at, updated_at) = timestamps.into_parts();
                let metadata = StorageRecordMetadata::try_new(
                    current.metadata().id(),
                    created_at,
                    updated_at,
                    revision,
                )
                .map_err(invalid_contract_value)?;
                let updated = StorageObjectRelation::try_new(
                    metadata,
                    current.from_object_id(),
                    current.to_object_id(),
                    current.class_relation_id(),
                )
                .map_err(invalid_contract_value)?;
                self.state.write().await.object_relations.insert(
                    ObjectRelationId::from(updated.metadata().id()).id(),
                    updated,
                );
                Ok(Some(revision))
            }
            StorageImportOperation::UpsertComputedField { input, overwrite } => {
                let parts = input.into_parts();
                let class_id = self
                    .import_class_id(
                        parts.class_ref.as_deref(),
                        parts.class_key.as_ref(),
                        references,
                    )
                    .await?;
                let visibility = match parts.visibility {
                    StorageImportComputedFieldVisibility::Shared => {
                        StorageComputedFieldVisibility::Shared
                    }
                    StorageImportComputedFieldVisibility::Personal => {
                        StorageComputedFieldVisibility::Personal {
                            owner_id: self
                                .import_principal_id(
                                    parts.owner_ref.as_deref(),
                                    parts.owner_key.as_ref(),
                                    references,
                                )
                                .await?,
                        }
                    }
                };
                let mut state = self.state.write().await;
                let existing = state
                    .computed_fields
                    .values()
                    .find(|definition| {
                        definition.class_id() == class_id
                            && definition.visibility() == visibility
                            && definition.key() == parts.key
                    })
                    .cloned();
                if let Some(current) = &existing {
                    assert_import_revision(parts.condition, current.metadata().revision())?;
                    if !overwrite {
                        return Err(StorageError::conflict(
                            "Import computed field already exists",
                        ));
                    }
                } else {
                    assert_import_create_condition(parts.condition)?;
                }
                let id = existing.as_ref().map_or_else(
                    || {
                        let id = ComputedFieldDefinitionId::new(state.next_computed_field_id)
                            .expect("memory import computed-field id is positive");
                        state.next_computed_field_id += 1;
                        id
                    },
                    |value| ComputedFieldDefinitionId::from(value.metadata().id()),
                );
                let now = Utc::now();
                let (created_at, updated_at) = parts.timestamps.map_or_else(
                    || {
                        existing
                            .as_ref()
                            .map_or((now, now), |value| (value.metadata().created_at(), now))
                    },
                    StorageImportTimestamps::into_parts,
                );
                let revision = existing
                    .as_ref()
                    .map_or(Ok(ResourceRevision::INITIAL), |value| {
                        value.metadata().revision().checked_advance()
                    })
                    .map_err(|error| StorageError::internal(error.to_string()))?;
                let metadata = StorageRecordMetadata::try_new(
                    ResourceId::new(id.id()).expect("computed-field id is a valid resource id"),
                    created_at,
                    updated_at,
                    revision,
                )
                .map_err(invalid_contract_value)?;
                let actor_id = match visibility {
                    StorageComputedFieldVisibility::Shared => {
                        PrincipalId::new(1).expect("seeded administrator principal id is valid")
                    }
                    StorageComputedFieldVisibility::Personal { owner_id } => owner_id,
                };
                let definition = StorageComputedFieldDefinition::new(
                    metadata,
                    class_id,
                    visibility,
                    StorageComputedFieldDefinitionContent::new(
                        StorageComputedFieldDefinitionInput::new(
                            parts.key,
                            parts.label,
                            parts.operation,
                            parts.result_type,
                        )
                        .with_description(parts.description)
                        .with_enabled(parts.enabled),
                        1,
                    ),
                    StorageComputedFieldProvenance::new(Some(actor_id), Some(actor_id)),
                );
                state.computed_fields.insert(id.id(), definition);
                Ok(Some(revision))
            }
            StorageImportOperation::ApplyCollectionPermissions { input, overwrite } => {
                let parts = input.into_parts();
                let collection_id = self
                    .import_collection_id(
                        parts.collection_ref.as_deref(),
                        parts.collection_key.as_ref(),
                        references,
                    )
                    .await?;
                let group_id = self
                    .import_group_id(None, Some(&parts.group_key), references)
                    .await?;
                let collection = self.get_collection(collection_id).await?;
                assert_import_revision(parts.condition, collection.revision())?;
                self.apply_local_collection_grant(StorageAuthorizationGrantMutation::new(
                    StorageAuthorizationGrantKey::new(collection_id, group_id),
                    parts.permissions,
                    overwrite || parts.replace_existing,
                    EventContext::system(),
                ))
                .await?
                .into_value();
                Ok(Some(collection.revision()))
            }
            StorageImportOperation::UpsertExportTemplate { input, overwrite } => {
                let parts = input.into_parts();
                let collection_id = self
                    .import_collection_id(
                        parts.collection_ref.as_deref(),
                        parts.collection_key.as_ref(),
                        references,
                    )
                    .await?;
                let class_id = match (parts.class_ref.as_deref(), parts.class_key.as_ref()) {
                    (None, None) => None,
                    (reference, key) => {
                        Some(self.import_class_id(reference, key, references).await?)
                    }
                };
                let mut state = self.state.write().await;
                let existing = state
                    .export_templates
                    .values()
                    .find(|template| {
                        let (_, current_collection_id, name, _) = (*template).clone().into_parts();
                        current_collection_id == collection_id && name == parts.name
                    })
                    .cloned();
                if let Some(current) = &existing {
                    assert_import_revision(parts.condition, current.metadata().revision())?;
                    if !overwrite {
                        return Err(StorageError::conflict(
                            "Import export template already exists",
                        ));
                    }
                } else {
                    assert_import_create_condition(parts.condition)?;
                }
                let id = existing.as_ref().map_or_else(
                    || {
                        let id = ExportTemplateId::new(state.next_export_template_id)
                            .expect("memory import export-template id is positive");
                        state.next_export_template_id += 1;
                        id
                    },
                    |value| ExportTemplateId::from(value.metadata().id()),
                );
                let now = Utc::now();
                let (created_at, updated_at) = parts.timestamps.map_or_else(
                    || {
                        existing
                            .as_ref()
                            .map_or((now, now), |value| (value.metadata().created_at(), now))
                    },
                    StorageImportTimestamps::into_parts,
                );
                let revision = existing
                    .as_ref()
                    .map_or(Ok(ResourceRevision::INITIAL), |value| {
                        value.metadata().revision().checked_advance()
                    })
                    .map_err(|error| StorageError::internal(error.to_string()))?;
                let definition = StorageExportTemplateDefinition::new(
                    parts.description,
                    parts.content_type,
                    parts.template,
                    parts.kind,
                )
                .with_scope(parts.scope_kind, class_id)
                .with_default_query(parts.default_query)
                .with_include(parts.include)
                .with_relation_context(parts.relation_context)
                .with_default_missing_data_policy(parts.default_missing_data_policy)
                .with_default_limits(parts.default_limits);
                let template = StorageExportTemplate::new(
                    StorageRecordMetadata::try_new(
                        ResourceId::new(id.id())
                            .expect("export-template id is a valid resource id"),
                        created_at,
                        updated_at,
                        revision,
                    )
                    .map_err(invalid_contract_value)?,
                    collection_id,
                    parts.name,
                    definition,
                );
                state.export_templates.insert(id.id(), template);
                Ok(Some(revision))
            }
            StorageImportOperation::UpsertRemoteTarget { input, overwrite } => {
                let parts = input.into_parts();
                let collection_id = self
                    .import_collection_id(
                        parts.collection_ref.as_deref(),
                        parts.collection_key.as_ref(),
                        references,
                    )
                    .await?;
                let class_id = match (parts.class_ref.as_deref(), parts.class_key.as_ref()) {
                    (None, None) => None,
                    (reference, key) => {
                        Some(self.import_class_id(reference, key, references).await?)
                    }
                };
                let transport = StorageRemoteTargetTransport::try_new(
                    parts.method,
                    parts.url_template,
                    parts.headers_template,
                    parts.body_template,
                    parts.auth_config,
                    parts.timeout_ms,
                )
                .map_err(invalid_contract_value)?;
                let policy = StorageRemoteTargetPolicy::try_new(
                    class_id,
                    parts.allowed_subject_types,
                    parts.enabled,
                )
                .map_err(invalid_contract_value)?;
                let mut state = self.state.write().await;
                let existing = state
                    .remote_targets
                    .values()
                    .find(|target| {
                        let (_, current_collection_id, name, _) = (*target).clone().into_parts();
                        current_collection_id == collection_id && name == parts.name
                    })
                    .cloned();
                if let Some(current) = &existing {
                    assert_import_revision(parts.condition, current.metadata().revision())?;
                    if !overwrite {
                        return Err(StorageError::conflict(
                            "Import remote target already exists",
                        ));
                    }
                } else {
                    assert_import_create_condition(parts.condition)?;
                }
                let id = existing.as_ref().map_or_else(
                    || {
                        let id = RemoteTargetId::new(state.next_remote_target_id)
                            .expect("memory import remote-target id is positive");
                        state.next_remote_target_id += 1;
                        id
                    },
                    |value| RemoteTargetId::from(value.metadata().id()),
                );
                let now = Utc::now();
                let (created_at, updated_at) = parts.timestamps.map_or_else(
                    || {
                        existing
                            .as_ref()
                            .map_or((now, now), |value| (value.metadata().created_at(), now))
                    },
                    StorageImportTimestamps::into_parts,
                );
                let revision = existing
                    .as_ref()
                    .map_or(Ok(ResourceRevision::INITIAL), |value| {
                        value.metadata().revision().checked_advance()
                    })
                    .map_err(|error| StorageError::internal(error.to_string()))?;
                let target = StorageRemoteTarget::new(
                    StorageRecordMetadata::try_new(
                        ResourceId::new(id.id()).expect("remote-target id is a valid resource id"),
                        created_at,
                        updated_at,
                        revision,
                    )
                    .map_err(invalid_contract_value)?,
                    collection_id,
                    parts.name,
                    StorageRemoteTargetDefinition::new(parts.description, transport, policy),
                );
                state.remote_targets.insert(id.id(), target);
                Ok(Some(revision))
            }
            StorageImportOperation::UpsertEventSink { input, overwrite } => {
                let parts = input.into_parts();
                let mut state = self.state.write().await;
                let existing = state
                    .event_sinks
                    .values()
                    .find(|sink| sink.name() == parts.name)
                    .cloned();
                if let Some(current) = &existing {
                    assert_import_revision(parts.condition, current.revision())?;
                    if !overwrite {
                        return Err(StorageError::conflict("Import event sink already exists"));
                    }
                } else {
                    assert_import_create_condition(parts.condition)?;
                }
                let id = existing.as_ref().map_or_else(
                    || {
                        let id = EventSinkId::new(state.next_event_sink_id)
                            .expect("memory import event-sink id is positive");
                        state.next_event_sink_id += 1;
                        id
                    },
                    StorageEventSink::id,
                );
                let now = Utc::now();
                let (created_at, updated_at) = parts.timestamps.map_or_else(
                    || {
                        existing
                            .as_ref()
                            .map_or((now, now), |value| (value.created_at(), now))
                    },
                    StorageImportTimestamps::into_parts,
                );
                let revision = existing
                    .as_ref()
                    .map_or(Ok(ResourceRevision::INITIAL), |value| {
                        value.revision().checked_advance()
                    })
                    .map_err(|error| StorageError::internal(error.to_string()))?;
                let sink = StorageEventSink::builder(
                    id, parts.name, parts.kind, created_at, updated_at, revision,
                )
                .configuration(parts.config)
                .secret_ref(parts.secret_ref)
                .enabled(parts.enabled)
                .try_build()
                .map_err(invalid_contract_value)?;
                state.event_sinks.insert(id.id(), sink);
                if let Some(reference) = parts.reference {
                    references.insert(reference, MemoryImportReference::EventSink(id));
                }
                Ok(Some(revision))
            }
            StorageImportOperation::UpsertEventSubscription { input, overwrite } => {
                let parts = input.into_parts();
                let collection_id = self
                    .import_collection_id(
                        parts.collection_ref.as_deref(),
                        parts.collection_key.as_ref(),
                        references,
                    )
                    .await?;
                let sink_id = self
                    .import_event_sink_id(
                        parts.sink_ref.as_deref(),
                        parts.sink_key.as_ref(),
                        references,
                    )
                    .await?;
                let entity_types = parts
                    .entity_types
                    .iter()
                    .map(|value| {
                        EntityType::parse(value)
                            .map_err(|error| StorageError::invalid_input(error.to_string()))
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let actions = parts
                    .actions
                    .iter()
                    .map(|value| {
                        Action::parse(value)
                            .map_err(|error| StorageError::invalid_input(error.to_string()))
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let filter = serde_json::from_value(parts.filter)
                    .map_err(|error| StorageError::invalid_input(error.to_string()))?;
                let mut state = self.state.write().await;
                let existing = state
                    .event_subscriptions
                    .values()
                    .find(|subscription| {
                        subscription.collection_id() == collection_id
                            && subscription.name() == parts.name
                    })
                    .cloned();
                if let Some(current) = &existing {
                    assert_import_revision(parts.condition, current.revision())?;
                    if !overwrite {
                        return Err(StorageError::conflict(
                            "Import event subscription already exists",
                        ));
                    }
                } else {
                    assert_import_create_condition(parts.condition)?;
                }
                let id = existing.as_ref().map_or_else(
                    || {
                        let id = EventSubscriptionId::new(state.next_event_subscription_id)
                            .expect("memory import event-subscription id is positive");
                        state.next_event_subscription_id += 1;
                        id
                    },
                    StorageEventSubscription::id,
                );
                let now = Utc::now();
                let (created_at, updated_at) = parts.timestamps.map_or_else(
                    || {
                        existing
                            .as_ref()
                            .map_or((now, now), |value| (value.created_at(), now))
                    },
                    StorageImportTimestamps::into_parts,
                );
                let revision = existing
                    .as_ref()
                    .map_or(Ok(ResourceRevision::INITIAL), |value| {
                        value.revision().checked_advance()
                    })
                    .map_err(|error| StorageError::internal(error.to_string()))?;
                let subscription = StorageEventSubscription::builder(
                    id,
                    collection_id,
                    sink_id,
                    parts.name,
                    created_at,
                    updated_at,
                    revision,
                )
                .description(parts.description)
                .entity_types(entity_types)
                .actions(actions)
                .filter(filter)
                .routing(parts.routing)
                .enabled(parts.enabled)
                .try_build()
                .map_err(invalid_contract_value)?;
                state.event_subscriptions.insert(id.id(), subscription);
                Ok(Some(revision))
            }
        }
    }

    async fn import_class_relation(
        &self,
        first: ClassId,
        second: ClassId,
    ) -> Result<StorageClassRelation, StorageError> {
        let (from, to) = ordered_ids(first, second);
        self.state
            .read()
            .await
            .class_relations
            .values()
            .find(|relation| relation.from_class_id() == from && relation.to_class_id() == to)
            .cloned()
            .ok_or_else(|| StorageError::not_found("Import class relation was not found"))
    }

    async fn import_object_relation(
        &self,
        first: ObjectId,
        second: ObjectId,
    ) -> Result<StorageObjectRelation, StorageError> {
        let (from, to) = ordered_ids(first, second);
        self.state
            .read()
            .await
            .object_relations
            .values()
            .find(|relation| relation.from_object_id() == from && relation.to_object_id() == to)
            .cloned()
            .ok_or_else(|| StorageError::not_found("Import object relation was not found"))
    }

    async fn apply_import_principal(
        &self,
        input: StorageImportPrincipal,
        overwrite: bool,
        references: &mut BTreeMap<String, MemoryImportReference>,
    ) -> Result<Option<ResourceRevision>, StorageError> {
        enum ResolvedSubtype {
            Human {
                password: Option<String>,
                proper_name: Option<String>,
                email: Option<String>,
                anonymized_at: Option<DateTime<Utc>>,
            },
            ServiceAccount {
                description: String,
                owner_group_id: GroupId,
                created_by: Option<PrincipalId>,
                disabled_at: Option<DateTime<Utc>>,
            },
        }

        let parts = input.into_parts();
        let scope_id = self
            .import_identity_scope_id(
                parts.identity_scope_ref.as_deref(),
                parts.identity_scope_key.as_ref(),
                references,
            )
            .await?;
        let subtype = match parts.subtype {
            StorageImportPrincipalSubtype::Human {
                password,
                password_hash,
                proper_name,
                email,
                anonymized_at,
            } => ResolvedSubtype::Human {
                password: password_hash.or(password),
                proper_name,
                email,
                anonymized_at,
            },
            StorageImportPrincipalSubtype::ServiceAccount {
                description,
                owner_group_ref,
                owner_group_key,
                created_by_ref,
                created_by_key,
                disabled_at,
            } => ResolvedSubtype::ServiceAccount {
                description,
                owner_group_id: self
                    .import_group_id(
                        owner_group_ref.as_deref(),
                        owner_group_key.as_ref(),
                        references,
                    )
                    .await?,
                created_by: match (created_by_ref.as_deref(), created_by_key.as_ref()) {
                    (None, None) => None,
                    (reference, key) => {
                        Some(self.import_principal_id(reference, key, references).await?)
                    }
                },
                disabled_at,
            },
        };
        let mut state = self.state.write().await;
        let existing = state
            .principals
            .values()
            .find(|principal| {
                principal.identity_scope_id() == scope_id && principal.name() == parts.name
            })
            .cloned();
        if let Some(current) = &existing {
            assert_import_revision(parts.condition, current.revision())?;
            if !overwrite {
                return Err(StorageError::conflict("Import principal already exists"));
            }
        } else {
            assert_import_create_condition(parts.condition)?;
        }
        let id = existing.as_ref().map_or_else(
            || {
                let id = PrincipalId::new(state.next_principal_id)
                    .expect("memory import principal id is positive");
                state.next_principal_id += 1;
                id
            },
            StoragePrincipal::id,
        );
        let now = Utc::now();
        let (created_at, updated_at) = parts.timestamps.map_or_else(
            || {
                existing
                    .as_ref()
                    .map_or((now, now), |value| (value.created_at(), now))
            },
            StorageImportTimestamps::into_parts,
        );
        let revision = existing
            .as_ref()
            .map_or(Ok(ResourceRevision::INITIAL), |value| {
                value.revision().checked_advance()
            })
            .map_err(|error| StorageError::internal(error.to_string()))?;
        let kind = match subtype {
            ResolvedSubtype::Human { .. } => PrincipalKind::Human,
            ResolvedSubtype::ServiceAccount { .. } => PrincipalKind::ServiceAccount,
        };
        let metadata = StorageRecordMetadata::try_new(
            ResourceId::new(id.id()).expect("principal id is a valid resource id"),
            created_at,
            updated_at,
            revision,
        )
        .map_err(invalid_contract_value)?;
        let principal = StoragePrincipal::builder(metadata, kind, &parts.name, scope_id)
            .provider_managed(parts.provider_managed)
            .settings(parts.settings)
            .external_subject(parts.external_subject.clone())
            .last_sync_attempted_at(parts.last_sync_attempted_at)
            .last_sync_success_at(parts.last_sync_success_at)
            .try_build()
            .map_err(invalid_contract_value)?;
        match subtype {
            ResolvedSubtype::Human {
                password,
                proper_name,
                email,
                anonymized_at,
            } => {
                let user_id = UserId::new(id.id()).expect("principal id is a valid user id");
                let user = StorageUser::try_new(
                    user_id,
                    password,
                    proper_name,
                    email,
                    created_at,
                    updated_at,
                    anonymized_at,
                )
                .map_err(invalid_contract_value)?;
                state.users.insert(
                    id.id(),
                    MemoryUserRecord {
                        user,
                        identity_scope_id: scope_id,
                        name: parts.name.clone(),
                        provider_managed: parts.provider_managed,
                        external_subject: parts.external_subject,
                        last_sync_attempted_at: parts.last_sync_attempted_at,
                        last_sync_success_at: parts.last_sync_success_at,
                    },
                );
                state.service_accounts.remove(&id.id());
            }
            ResolvedSubtype::ServiceAccount {
                description,
                owner_group_id,
                created_by,
                disabled_at,
            } => {
                let account = StorageServiceAccount::try_new(
                    ServiceAccountId::new(id.id())
                        .expect("principal id is a valid service-account id"),
                    description,
                    owner_group_id,
                    created_by,
                    disabled_at,
                    created_at,
                    updated_at,
                )
                .map_err(invalid_contract_value)?;
                state.service_accounts.insert(id.id(), account);
                state.users.remove(&id.id());
            }
        }
        state.principals.insert(id.id(), principal);
        if let Some(reference) = parts.reference {
            references.insert(reference, MemoryImportReference::Principal(id));
        }
        Ok(Some(revision))
    }
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

#[async_trait]
impl CollectionStorage for MemoryStorage {
    async fn get_collection(&self, id: CollectionId) -> Result<StorageCollection, StorageError> {
        self.state
            .read()
            .await
            .collections
            .get(&id.id())
            .cloned()
            .ok_or_else(|| StorageError::not_found(format!("Collection {} was not found", id.id())))
    }

    async fn create_collection(
        &self,
        command: StorageCollectionCreate,
        context: &EventContext,
    ) -> Result<StorageMutationOutcome<StorageCollection>, StorageError> {
        let mut state = self.state.write().await;
        let parent_id = command
            .parent_collection_id()
            .unwrap_or(CollectionId::new(ROOT_COLLECTION_ID).expect("root id is valid"));
        if !state.collections.contains_key(&parent_id.id()) {
            return Err(StorageError::not_found(format!(
                "Parent collection {} was not found",
                parent_id.id()
            )));
        }
        if state.collections.values().any(|collection| {
            collection.parent_collection_id() == Some(parent_id)
                && collection.name() == command.name()
        }) {
            return Err(StorageError::conflict(format!(
                "A collection named '{}' already exists under parent {}",
                command.name(),
                parent_id.id()
            )));
        }
        let id = state.next_collection_id;
        state.next_collection_id += 1;
        let now = Utc::now();
        let metadata = StorageRecordMetadata::try_new(
            ResourceId::new(id)
                .map_err(|error| StorageError::backend_failure(error.to_string()))?,
            now,
            now,
            ResourceRevision::INITIAL,
        )
        .map_err(|error| StorageError::backend_failure(error.to_string()))?;
        let collection = StorageCollection::try_new(
            metadata,
            command.name(),
            command.description(),
            Some(parent_id),
        )
        .map_err(|error| StorageError::backend_failure(error.to_string()))?;
        let document =
            AuditDocument::builder(format!("Created collection '{}'", collection.name()))
                .after(collection.audit_snapshot())
                .metadata(serde_json::json!({"owner_group_id": command.owner_group_id().id()}))
                .try_build()
                .map_err(|error| StorageError::backend_failure(error.to_string()))?;
        let receipt = append_memory_event!(
            state,
            EntityType::Collection,
            id,
            Some(collection.name()),
            Some(collection.id()),
            Action::Created,
            context,
            document,
            None,
            Some(collection.revision()),
        )?;
        state.collections.insert(id, collection.clone());
        state.append_history(
            MemoryHistoryValue::Collection(collection.clone()),
            StorageHistoryOperation::Create,
            context,
        )?;
        Ok(StorageMutationOutcome::committed(collection, receipt))
    }

    async fn update_collection(
        &self,
        id: CollectionId,
        changes: StorageCollectionUpdate,
        context: &EventContext,
    ) -> Result<StorageMutationOutcome<StorageCollection>, StorageError> {
        let mut state = self.state.write().await;
        let current = state.collections.get(&id.id()).cloned().ok_or_else(|| {
            StorageError::not_found(format!("Collection {} was not found", id.id()))
        })?;
        enforce_memory_revision_precondition(
            StorageRevisionTarget::Collection(id),
            current.revision(),
        )?;
        let name = changes.name().unwrap_or(current.name());
        let description = changes.description().unwrap_or(current.description());
        if name == current.name() && description == current.description() {
            return Ok(StorageMutationOutcome::unchanged(current));
        }
        if state.collections.values().any(|collection| {
            collection.id() != id
                && collection.parent_collection_id() == current.parent_collection_id()
                && collection.name() == name
        }) {
            return Err(StorageError::conflict(format!(
                "A collection named '{name}' already exists under the same parent"
            )));
        }
        let revision = current
            .revision()
            .checked_advance()
            .map_err(|error| StorageError::backend_failure(error.to_string()))?;
        let metadata = StorageRecordMetadata::try_new(
            ResourceId::new(id.id())
                .map_err(|error| StorageError::backend_failure(error.to_string()))?,
            current.created_at(),
            Utc::now(),
            revision,
        )
        .map_err(|error| StorageError::backend_failure(error.to_string()))?;
        let updated =
            StorageCollection::try_new(metadata, name, description, current.parent_collection_id())
                .map_err(|error| StorageError::backend_failure(error.to_string()))?;
        let document = AuditDocument::builder(format!("Updated collection '{}'", updated.name()))
            .before(current.audit_snapshot())
            .after(updated.audit_snapshot())
            .try_build()
            .map_err(|error| StorageError::backend_failure(error.to_string()))?;
        let receipt = append_memory_event!(
            state,
            EntityType::Collection,
            id.id(),
            Some(updated.name()),
            Some(updated.id()),
            Action::Updated,
            context,
            document,
            Some(current.revision()),
            Some(updated.revision()),
        )?;
        state.collections.insert(id.id(), updated.clone());
        state.append_history(
            MemoryHistoryValue::Collection(updated.clone()),
            StorageHistoryOperation::Update,
            context,
        )?;
        Ok(StorageMutationOutcome::committed(updated, receipt))
    }

    async fn delete_collection(
        &self,
        id: CollectionId,
        context: &EventContext,
    ) -> Result<StorageMutationOutcome<()>, StorageError> {
        if id.id() == ROOT_COLLECTION_ID {
            return Err(StorageError::conflict(
                "The root collection cannot be deleted",
            ));
        }
        let mut state = self.state.write().await;
        let current = state.collections.get(&id.id()).cloned().ok_or_else(|| {
            StorageError::not_found(format!("Collection {} was not found", id.id()))
        })?;
        if state
            .collections
            .values()
            .any(|collection| collection.parent_collection_id() == Some(id))
        {
            return Err(StorageError::conflict(
                "Collections with child collections cannot be deleted",
            ));
        }
        let document = AuditDocument::builder(format!("Deleted collection '{}'", current.name()))
            .before(current.audit_snapshot())
            .try_build()
            .map_err(|error| StorageError::backend_failure(error.to_string()))?;
        let receipt = append_memory_event!(
            state,
            EntityType::Collection,
            id.id(),
            Some(current.name()),
            Some(id),
            Action::Deleted,
            context,
            document,
            Some(current.revision()),
            None,
        )?;
        state.append_history(
            MemoryHistoryValue::Collection(current.clone()),
            StorageHistoryOperation::Delete,
            context,
        )?;
        state.collections.remove(&id.id());
        Ok(StorageMutationOutcome::committed((), receipt))
    }

    async fn list_collection_children(
        &self,
        id: CollectionId,
    ) -> Result<Vec<StorageCollection>, StorageError> {
        let state = self.state.read().await;
        if !state.collections.contains_key(&id.id()) {
            return Err(StorageError::not_found(format!(
                "Collection {} was not found",
                id.id()
            )));
        }
        let mut children = state
            .collections
            .values()
            .filter(|collection| collection.parent_collection_id() == Some(id))
            .cloned()
            .collect::<Vec<_>>();
        children.sort_by(|left, right| left.name().cmp(right.name()));
        Ok(children)
    }

    async fn list_collection_ancestors(
        &self,
        id: CollectionId,
    ) -> Result<Vec<StorageCollection>, StorageError> {
        let state = self.state.read().await;
        let mut current = state
            .collections
            .get(&id.id())
            .ok_or_else(|| {
                StorageError::not_found(format!("Collection {} was not found", id.id()))
            })?
            .parent_collection_id();
        let mut ancestors = Vec::new();
        while let Some(parent_id) = current {
            let parent = state.collections.get(&parent_id.id()).ok_or_else(|| {
                StorageError::backend_failure(format!(
                    "Collection {} references a missing parent {}",
                    id.id(),
                    parent_id.id()
                ))
            })?;
            ancestors.push(parent.clone());
            current = parent.parent_collection_id();
        }
        Ok(ancestors)
    }

    async fn move_collection(
        &self,
        id: CollectionId,
        new_parent_id: CollectionId,
        context: &EventContext,
    ) -> Result<StorageMutationOutcome<StorageCollection>, StorageError> {
        let mut state = self.state.write().await;
        let current = state.collections.get(&id.id()).cloned().ok_or_else(|| {
            StorageError::not_found(format!("Collection {} was not found", id.id()))
        })?;
        if id.id() == ROOT_COLLECTION_ID {
            return Err(StorageError::conflict(
                "The root collection cannot be moved",
            ));
        }
        if !state.collections.contains_key(&new_parent_id.id()) {
            return Err(StorageError::not_found(format!(
                "Parent collection {} was not found",
                new_parent_id.id()
            )));
        }
        if current.parent_collection_id() == Some(new_parent_id) {
            return Ok(StorageMutationOutcome::unchanged(current));
        }
        let mut ancestor_id = Some(new_parent_id);
        while let Some(candidate) = ancestor_id {
            if candidate == id {
                return Err(StorageError::invalid_input(
                    "A collection cannot be moved under itself or a descendant",
                ));
            }
            ancestor_id = state
                .collections
                .get(&candidate.id())
                .and_then(StorageCollection::parent_collection_id);
        }
        if state.collections.values().any(|collection| {
            collection.id() != id
                && collection.parent_collection_id() == Some(new_parent_id)
                && collection.name() == current.name()
        }) {
            return Err(StorageError::conflict(format!(
                "A collection named '{}' already exists under parent {}",
                current.name(),
                new_parent_id.id()
            )));
        }
        let revision = current
            .revision()
            .checked_advance()
            .map_err(|error| StorageError::backend_failure(error.to_string()))?;
        let metadata = StorageRecordMetadata::try_new(
            ResourceId::new(id.id())
                .map_err(|error| StorageError::backend_failure(error.to_string()))?,
            current.created_at(),
            Utc::now(),
            revision,
        )
        .map_err(|error| StorageError::backend_failure(error.to_string()))?;
        let moved = StorageCollection::try_new(
            metadata,
            current.name(),
            current.description(),
            Some(new_parent_id),
        )
        .map_err(|error| StorageError::backend_failure(error.to_string()))?;
        let document = AuditDocument::builder(format!("Moved collection '{}'", moved.name()))
            .before(current.audit_snapshot())
            .after(moved.audit_snapshot())
            .try_build()
            .map_err(|error| StorageError::backend_failure(error.to_string()))?;
        let receipt = append_memory_event!(
            state,
            EntityType::Collection,
            id.id(),
            Some(moved.name()),
            Some(id),
            Action::Updated,
            context,
            document,
            Some(current.revision()),
            Some(moved.revision()),
        )?;
        state.collections.insert(id.id(), moved.clone());
        state.append_history(
            MemoryHistoryValue::Collection(moved.clone()),
            StorageHistoryOperation::Update,
            context,
        )?;
        Ok(StorageMutationOutcome::committed(moved, receipt))
    }
}

#[async_trait]
impl ClassStorage for MemoryStorage {
    async fn resolve_class(
        &self,
        selector: StorageClassSelector,
    ) -> Result<StorageResolvedClass, StorageError> {
        let state = self.state.read().await;
        let class = match &selector {
            StorageClassSelector::Id(id) => state.classes.get(&id.id()),
            StorageClassSelector::Name(name) => {
                state.classes.values().find(|class| class.name() == name)
            }
        }
        .cloned()
        .ok_or_else(|| StorageError::not_found("Class was not found"))?;
        StorageResolvedClass::try_new(selector, class)
            .map_err(|error| StorageError::backend_failure(error.to_string()))
    }

    async fn create_class(
        &self,
        command: StorageClassCreate,
        context: &EventContext,
    ) -> Result<StorageMutationOutcome<StorageClass>, StorageError> {
        let mut state = self.state.write().await;
        if !state
            .collections
            .contains_key(&command.collection_id().id())
        {
            return Err(StorageError::not_found(format!(
                "Collection {} was not found",
                command.collection_id().id()
            )));
        }
        if state
            .classes
            .values()
            .any(|class| class.name() == command.name())
        {
            return Err(StorageError::conflict(format!(
                "A class named '{}' already exists",
                command.name()
            )));
        }
        let id = state.next_class_id;
        state.next_class_id += 1;
        let now = Utc::now();
        let metadata = StorageRecordMetadata::try_new(
            ResourceId::new(id)
                .map_err(|error| StorageError::backend_failure(error.to_string()))?,
            now,
            now,
            ResourceRevision::INITIAL,
        )
        .map_err(|error| StorageError::backend_failure(error.to_string()))?;
        let class = StorageClass::builder(
            metadata,
            command.name(),
            command.collection_id(),
            command.description(),
        )
        .json_schema(command.json_schema().cloned())
        .validate_schema(command.validates_schema())
        .build();
        let document = AuditDocument::builder(format!("Class '{}' created", class.name()))
            .after(class.audit_snapshot())
            .try_build()
            .map_err(|error| StorageError::backend_failure(error.to_string()))?;
        let receipt = append_memory_event!(
            state,
            EntityType::Class,
            id,
            Some(class.name()),
            Some(class.collection_id()),
            Action::Created,
            context,
            document,
            None,
            Some(class.revision()),
        )?;
        state.classes.insert(id, class.clone());
        state.append_history(
            MemoryHistoryValue::Class(class.clone()),
            StorageHistoryOperation::Create,
            context,
        )?;
        Ok(StorageMutationOutcome::committed(class, receipt))
    }

    async fn update_class(
        &self,
        target: &StorageResolvedClass,
        changes: StorageClassUpdate,
        context: &EventContext,
    ) -> Result<StorageMutationOutcome<StorageClass>, StorageError> {
        let mut state = self.state.write().await;
        let id = target.class().id();
        let current = state
            .classes
            .get(&id.id())
            .cloned()
            .ok_or_else(|| StorageError::not_found("Class was not found"))?;
        if &current != target.class() {
            return Err(StorageError::not_found(
                "Class no longer matches the resolved route target",
            ));
        }
        let name = changes.name().unwrap_or(current.name());
        let collection_id = changes.collection_id().unwrap_or(current.collection_id());
        let json_schema = changes
            .json_schema()
            .cloned()
            .or_else(|| current.json_schema().cloned());
        let validate_schema = changes
            .validate_schema()
            .unwrap_or(current.validates_schema());
        let description = changes.description().unwrap_or(current.description());
        if name == current.name()
            && collection_id == current.collection_id()
            && json_schema.as_ref() == current.json_schema()
            && validate_schema == current.validates_schema()
            && description == current.description()
        {
            return Ok(StorageMutationOutcome::unchanged(current));
        }
        if !state.collections.contains_key(&collection_id.id()) {
            return Err(StorageError::not_found(format!(
                "Collection {} was not found",
                collection_id.id()
            )));
        }
        if state
            .classes
            .values()
            .any(|class| class.id() != id && class.name() == name)
        {
            return Err(StorageError::conflict(format!(
                "A class named '{name}' already exists"
            )));
        }
        let revision = current
            .revision()
            .checked_advance()
            .map_err(|error| StorageError::backend_failure(error.to_string()))?;
        let metadata = StorageRecordMetadata::try_new(
            ResourceId::new(id.id())
                .map_err(|error| StorageError::backend_failure(error.to_string()))?,
            current.created_at(),
            Utc::now(),
            revision,
        )
        .map_err(|error| StorageError::backend_failure(error.to_string()))?;
        let updated = StorageClass::builder(metadata, name, collection_id, description)
            .json_schema(json_schema)
            .validate_schema(validate_schema)
            .build();
        let document = AuditDocument::builder(format!("Class '{}' updated", updated.name()))
            .before(current.audit_snapshot())
            .after(updated.audit_snapshot())
            .try_build()
            .map_err(|error| StorageError::backend_failure(error.to_string()))?;
        let receipt = append_memory_event!(
            state,
            EntityType::Class,
            id.id(),
            Some(updated.name()),
            Some(updated.collection_id()),
            Action::Updated,
            context,
            document,
            Some(current.revision()),
            Some(updated.revision()),
        )?;
        state.classes.insert(id.id(), updated.clone());
        state.append_history(
            MemoryHistoryValue::Class(updated.clone()),
            StorageHistoryOperation::Update,
            context,
        )?;
        Ok(StorageMutationOutcome::committed(updated, receipt))
    }

    async fn delete_class(
        &self,
        target: &StorageResolvedClass,
        context: &EventContext,
    ) -> Result<StorageMutationOutcome<()>, StorageError> {
        let mut state = self.state.write().await;
        let id = target.class().id();
        let current = state
            .classes
            .get(&id.id())
            .cloned()
            .ok_or_else(|| StorageError::not_found("Class was not found"))?;
        if &current != target.class() {
            return Err(StorageError::not_found(
                "Class no longer matches the resolved route target",
            ));
        }
        let document = AuditDocument::builder(format!("Class '{}' deleted", current.name()))
            .before(current.audit_snapshot())
            .try_build()
            .map_err(|error| StorageError::backend_failure(error.to_string()))?;
        let receipt = append_memory_event!(
            state,
            EntityType::Class,
            id.id(),
            Some(current.name()),
            Some(current.collection_id()),
            Action::Deleted,
            context,
            document,
            Some(current.revision()),
            None,
        )?;
        state.append_history(
            MemoryHistoryValue::Class(current.clone()),
            StorageHistoryOperation::Delete,
            context,
        )?;
        state.classes.remove(&id.id());
        state.objects.retain(|_, object| object.class_id() != id);
        state
            .class_relations
            .retain(|_, relation| relation.from_class_id() != id && relation.to_class_id() != id);
        Ok(StorageMutationOutcome::committed((), receipt))
    }

    async fn resolve_class_names(
        &self,
        class_ids: Vec<ClassId>,
    ) -> Result<Vec<(ClassId, String)>, StorageError> {
        let state = self.state.read().await;
        let class_ids = class_ids.into_iter().collect::<BTreeSet<_>>();
        let rows = class_ids
            .iter()
            .filter_map(|id| {
                state
                    .classes
                    .get(&id.id())
                    .map(|class| (*id, class.name().to_string()))
            })
            .collect::<Vec<_>>();
        if rows.len() != class_ids.len() {
            return Err(StorageError::not_found(
                "One or more requested classes were not found",
            ));
        }
        Ok(rows)
    }
}

#[async_trait]
impl ObjectStorage for MemoryStorage {
    async fn get_object(&self, object_id: ObjectId) -> Result<StorageResolvedObject, StorageError> {
        let state = self.state.read().await;
        let object = state
            .objects
            .get(&object_id.id())
            .cloned()
            .ok_or_else(|| StorageError::not_found("Object was not found"))?;
        let class = state
            .classes
            .get(&object.class_id().id())
            .cloned()
            .ok_or_else(|| StorageError::backend_failure("Object references a missing class"))?;
        StorageResolvedObject::try_new(
            StorageObjectSelector::Ids {
                class_id: class.id(),
                object_id,
            },
            class,
            object,
        )
        .map_err(|error| StorageError::backend_failure(error.to_string()))
    }

    async fn resolve_object(
        &self,
        selector: StorageObjectSelector,
    ) -> Result<StorageResolvedObject, StorageError> {
        let state = self.state.read().await;
        let (class, object) = match &selector {
            StorageObjectSelector::Ids {
                class_id,
                object_id,
            } => {
                let class = state.classes.get(&class_id.id());
                let object = state
                    .objects
                    .get(&object_id.id())
                    .filter(|object| object.class_id() == *class_id);
                (class, object)
            }
            StorageObjectSelector::Names {
                class_name,
                object_name,
            } => {
                let class = state
                    .classes
                    .values()
                    .find(|class| class.name() == class_name);
                let object = class.and_then(|class| {
                    state.objects.values().find(|object| {
                        object.class_id() == class.id() && object.name() == object_name
                    })
                });
                (class, object)
            }
        };
        StorageResolvedObject::try_new(
            selector,
            class
                .cloned()
                .ok_or_else(|| StorageError::not_found("Object class was not found"))?,
            object
                .cloned()
                .ok_or_else(|| StorageError::not_found("Object was not found"))?,
        )
        .map_err(|error| StorageError::backend_failure(error.to_string()))
    }

    async fn create_object(
        &self,
        class: &StorageResolvedClass,
        command: StorageObjectCreate,
        context: &EventContext,
    ) -> Result<StorageMutationOutcome<StorageObject>, StorageError> {
        if class.class().id() != command.class_id()
            || class.class().collection_id() != command.collection_id()
        {
            return Err(StorageError::invalid_input(
                "Object class and collection must match the resolved class",
            ));
        }
        if class.class().validates_schema()
            && let Some(schema) = class.class().json_schema()
        {
            validate_json_value(schema, command.data())
                .map_err(|error| StorageError::invalid_input(error.to_string()))?;
        }
        let mut state = self.state.write().await;
        if state.classes.get(&command.class_id().id()) != Some(class.class()) {
            return Err(StorageError::not_found(
                "Class no longer matches the resolved route target",
            ));
        }
        if state.objects.values().any(|object| {
            object.class_id() == command.class_id() && object.name() == command.name()
        }) {
            return Err(StorageError::conflict(format!(
                "An object named '{}' already exists in class {}",
                command.name(),
                command.class_id().id()
            )));
        }
        let id = state.next_object_id;
        state.next_object_id += 1;
        let now = Utc::now();
        let metadata = StorageRecordMetadata::try_new(
            ResourceId::new(id)
                .map_err(|error| StorageError::backend_failure(error.to_string()))?,
            now,
            now,
            ResourceRevision::INITIAL,
        )
        .map_err(|error| StorageError::backend_failure(error.to_string()))?;
        let object = StorageObject::new(
            metadata,
            command.name(),
            command.collection_id(),
            command.class_id(),
            command.data().clone(),
            command.description(),
        );
        let document = AuditDocument::builder(format!("Object '{}' created", object.name()))
            .after(object.audit_snapshot())
            .metadata(serde_json::json!({"class_id": object.class_id().id()}))
            .try_build()
            .map_err(|error| StorageError::backend_failure(error.to_string()))?;
        let receipt = append_memory_event!(
            state,
            EntityType::Object,
            id,
            Some(object.name()),
            Some(object.collection_id()),
            Action::Created,
            context,
            document,
            None,
            Some(object.revision()),
        )?;
        state.objects.insert(id, object.clone());
        state.append_history(
            MemoryHistoryValue::Object(object.clone()),
            StorageHistoryOperation::Create,
            context,
        )?;
        Ok(StorageMutationOutcome::committed(object, receipt))
    }

    async fn update_object(
        &self,
        target: &StorageResolvedObject,
        changes: StorageObjectUpdate,
        context: &EventContext,
    ) -> Result<StorageMutationOutcome<StorageObject>, StorageError> {
        let mut state = self.state.write().await;
        let id = target.object().id();
        let current = state
            .objects
            .get(&id.id())
            .cloned()
            .ok_or_else(|| StorageError::not_found("Object was not found"))?;
        if &current != target.object() {
            return Err(StorageError::not_found(
                "Object no longer matches the resolved route target",
            ));
        }
        let name = changes.name().unwrap_or(current.name());
        let collection_id = changes.collection_id().unwrap_or(current.collection_id());
        let class_id = changes.class_id().unwrap_or(current.class_id());
        let data = changes
            .data()
            .cloned()
            .unwrap_or_else(|| current.data().clone());
        let description = changes.description().unwrap_or(current.description());
        if name == current.name()
            && collection_id == current.collection_id()
            && class_id == current.class_id()
            && data == *current.data()
            && description == current.description()
        {
            return Ok(StorageMutationOutcome::unchanged(current));
        }
        let class = state
            .classes
            .get(&class_id.id())
            .ok_or_else(|| StorageError::not_found("Object class was not found"))?;
        if class.collection_id() != collection_id {
            return Err(StorageError::invalid_input(
                "Object collection must match its class collection",
            ));
        }
        if class.validates_schema()
            && let Some(schema) = class.json_schema()
        {
            validate_json_value(schema, &data)
                .map_err(|error| StorageError::invalid_input(error.to_string()))?;
        }
        if state.objects.values().any(|object| {
            object.id() != id && object.class_id() == class_id && object.name() == name
        }) {
            return Err(StorageError::conflict(format!(
                "An object named '{name}' already exists in class {}",
                class_id.id()
            )));
        }
        let revision = current
            .revision()
            .checked_advance()
            .map_err(|error| StorageError::backend_failure(error.to_string()))?;
        let metadata = StorageRecordMetadata::try_new(
            ResourceId::new(id.id())
                .map_err(|error| StorageError::backend_failure(error.to_string()))?,
            current.created_at(),
            Utc::now(),
            revision,
        )
        .map_err(|error| StorageError::backend_failure(error.to_string()))?;
        let updated =
            StorageObject::new(metadata, name, collection_id, class_id, data, description);
        let document = AuditDocument::builder(format!("Object '{}' updated", updated.name()))
            .before(current.audit_snapshot())
            .after(updated.audit_snapshot())
            .metadata(serde_json::json!({"class_id": updated.class_id().id()}))
            .try_build()
            .map_err(|error| StorageError::backend_failure(error.to_string()))?;
        let receipt = append_memory_event!(
            state,
            EntityType::Object,
            id.id(),
            Some(updated.name()),
            Some(updated.collection_id()),
            Action::Updated,
            context,
            document,
            Some(current.revision()),
            Some(updated.revision()),
        )?;
        state.objects.insert(id.id(), updated.clone());
        state.append_history(
            MemoryHistoryValue::Object(updated.clone()),
            StorageHistoryOperation::Update,
            context,
        )?;
        Ok(StorageMutationOutcome::committed(updated, receipt))
    }

    async fn patch_object_data(
        &self,
        target: &StorageResolvedObject,
        patch: StorageObjectDataPatch,
        context: &EventContext,
    ) -> Result<StorageMutationOutcome<StorageObject>, StorageError> {
        let patched = patch.apply(target.object().data())?;
        self.update_object(
            target,
            StorageObjectUpdate::builder().data(Some(patched)).build(),
            context,
        )
        .await
    }

    async fn delete_object(
        &self,
        target: &StorageResolvedObject,
        context: &EventContext,
    ) -> Result<StorageMutationOutcome<()>, StorageError> {
        let mut state = self.state.write().await;
        let id = target.object().id();
        let current = state
            .objects
            .get(&id.id())
            .cloned()
            .ok_or_else(|| StorageError::not_found("Object was not found"))?;
        if &current != target.object() {
            return Err(StorageError::not_found(
                "Object no longer matches the resolved route target",
            ));
        }
        let document = AuditDocument::builder(format!("Object '{}' deleted", current.name()))
            .before(current.audit_snapshot())
            .metadata(serde_json::json!({"class_id": current.class_id().id()}))
            .try_build()
            .map_err(|error| StorageError::backend_failure(error.to_string()))?;
        let receipt = append_memory_event!(
            state,
            EntityType::Object,
            id.id(),
            Some(current.name()),
            Some(current.collection_id()),
            Action::Deleted,
            context,
            document,
            Some(current.revision()),
            None,
        )?;
        state.append_history(
            MemoryHistoryValue::Object(current.clone()),
            StorageHistoryOperation::Delete,
            context,
        )?;
        state.objects.remove(&id.id());
        state
            .object_relations
            .retain(|_, relation| relation.from_object_id() != id && relation.to_object_id() != id);
        Ok(StorageMutationOutcome::committed((), receipt))
    }

    async fn validate_object(&self, object: StorageObject) -> Result<(), StorageError> {
        let state = self.state.read().await;
        let class = state
            .classes
            .get(&object.class_id().id())
            .ok_or_else(|| StorageError::not_found("Object class was not found"))?;
        if class.collection_id() != object.collection_id() {
            return Err(StorageError::invalid_input(
                "Object collection must match its class collection",
            ));
        }
        if class.validates_schema()
            && let Some(schema) = class.json_schema()
        {
            validate_json_value(schema, object.data())
                .map_err(|error| StorageError::invalid_input(error.to_string()))?;
        }
        Ok(())
    }

    async fn validate_object_create(
        &self,
        command: StorageObjectCreate,
    ) -> Result<(), StorageError> {
        let state = self.state.read().await;
        let class = state
            .classes
            .get(&command.class_id().id())
            .ok_or_else(|| StorageError::not_found("Object class was not found"))?;
        if class.collection_id() != command.collection_id() {
            return Err(StorageError::invalid_input(
                "Object collection must match its class collection",
            ));
        }
        if class.validates_schema()
            && let Some(schema) = class.json_schema()
        {
            validate_json_value(schema, command.data())
                .map_err(|error| StorageError::invalid_input(error.to_string()))?;
        }
        Ok(())
    }

    async fn validate_object_update(
        &self,
        object_id: ObjectId,
        changes: StorageObjectUpdate,
    ) -> Result<(), StorageError> {
        let state = self.state.read().await;
        let current = state
            .objects
            .get(&object_id.id())
            .ok_or_else(|| StorageError::not_found("Object was not found"))?;
        let collection_id = changes.collection_id().unwrap_or(current.collection_id());
        let class_id = changes.class_id().unwrap_or(current.class_id());
        let data = changes.data().unwrap_or(current.data());
        let class = state
            .classes
            .get(&class_id.id())
            .ok_or_else(|| StorageError::not_found("Object class was not found"))?;
        if class.collection_id() != collection_id {
            return Err(StorageError::invalid_input(
                "Object collection must match its class collection",
            ));
        }
        if class.validates_schema()
            && let Some(schema) = class.json_schema()
        {
            validate_json_value(schema, data)
                .map_err(|error| StorageError::invalid_input(error.to_string()))?;
        }
        Ok(())
    }
}

#[async_trait]
impl ClassRelationStorage for MemoryStorage {
    async fn prepare_class_relation(
        &self,
        command: StorageClassRelationCreate,
    ) -> Result<StoragePreparedClassRelation, StorageError> {
        let state = self.state.read().await;
        let from = state
            .classes
            .get(&command.from_class_id().id())
            .cloned()
            .ok_or_else(|| StorageError::not_found("From class was not found"))?;
        let to = state
            .classes
            .get(&command.to_class_id().id())
            .cloned()
            .ok_or_else(|| StorageError::not_found("To class was not found"))?;
        StoragePreparedClassRelation::try_new(command, from, to)
            .map_err(|error| StorageError::invalid_input(error.to_string()))
    }

    async fn resolve_class_relation(
        &self,
        id: ClassRelationId,
    ) -> Result<StorageResolvedClassRelation, StorageError> {
        let state = self.state.read().await;
        let relation = state
            .class_relations
            .get(&id.id())
            .cloned()
            .ok_or_else(|| StorageError::not_found("Class relation was not found"))?;
        let from = state
            .classes
            .get(&relation.from_class_id().id())
            .cloned()
            .ok_or_else(|| StorageError::backend_failure("Class relation endpoint is missing"))?;
        let to = state
            .classes
            .get(&relation.to_class_id().id())
            .cloned()
            .ok_or_else(|| StorageError::backend_failure("Class relation endpoint is missing"))?;
        StorageResolvedClassRelation::try_new(relation, from, to)
            .map_err(|error| StorageError::backend_failure(error.to_string()))
    }

    async fn create_class_relation(
        &self,
        prepared: &StoragePreparedClassRelation,
        context: &EventContext,
    ) -> Result<StorageMutationOutcome<StorageResolvedClassRelation>, StorageError> {
        let mut state = self.state.write().await;
        let command = prepared.command();
        if state.classes.get(&prepared.from_class().id().id()) != Some(prepared.from_class())
            || state.classes.get(&prepared.to_class().id().id()) != Some(prepared.to_class())
        {
            return Err(StorageError::not_found(
                "Class relation endpoint no longer matches its prepared value",
            ));
        }
        if state.class_relations.values().any(|relation| {
            relation.from_class_id() == command.from_class_id()
                && relation.to_class_id() == command.to_class_id()
        }) {
            return Err(StorageError::conflict(
                "A class relation already exists between these classes",
            ));
        }
        let id = state.next_class_relation_id;
        state.next_class_relation_id += 1;
        let now = Utc::now();
        let metadata = StorageRecordMetadata::try_new(
            ResourceId::new(id)
                .map_err(|error| StorageError::backend_failure(error.to_string()))?,
            now,
            now,
            ResourceRevision::INITIAL,
        )
        .map_err(|error| StorageError::backend_failure(error.to_string()))?;
        let relation =
            StorageClassRelation::try_new(metadata, command.from_class_id(), command.to_class_id())
                .and_then(|relation| {
                    relation.try_with_template_aliases(
                        command.forward_template_alias().map(ToOwned::to_owned),
                        command.reverse_template_alias().map(ToOwned::to_owned),
                    )
                })
                .and_then(|relation| {
                    relation.try_with_relation_limits(
                        command.from_max_relations(),
                        command.to_max_relations(),
                    )
                })
                .map_err(|error| StorageError::invalid_input(error.to_string()))?;
        let resolved = StorageResolvedClassRelation::try_new(
            relation.clone(),
            prepared.from_class().clone(),
            prepared.to_class().clone(),
        )
        .map_err(|error| StorageError::backend_failure(error.to_string()))?;
        let document = AuditDocument::builder(format!(
            "Class relation {} -> {} created",
            relation.from_class_id().id(),
            relation.to_class_id().id()
        ))
        .after(relation.audit_snapshot())
        .metadata(serde_json::json!({
            "from_class_id": relation.from_class_id().id(),
            "to_class_id": relation.to_class_id().id(),
            "related_collection_ids": [
                prepared.from_class().collection_id().id(),
                prepared.to_class().collection_id().id()
            ],
        }))
        .try_build()
        .map_err(|error| StorageError::backend_failure(error.to_string()))?;
        let receipt = append_memory_event!(
            state,
            EntityType::ClassRelation,
            id,
            None,
            None,
            Action::Created,
            context,
            document,
            None,
            Some(relation.metadata().revision()),
        )?;
        state.class_relations.insert(id, relation);
        Ok(StorageMutationOutcome::committed(resolved, receipt))
    }

    async fn delete_class_relation(
        &self,
        target: &StorageResolvedClassRelation,
        context: &EventContext,
    ) -> Result<StorageMutationOutcome<()>, StorageError> {
        let mut state = self.state.write().await;
        let relation_id = ClassRelationId::from(target.relation().metadata().id());
        let current = state
            .class_relations
            .get(&relation_id.id())
            .cloned()
            .ok_or_else(|| StorageError::not_found("Class relation was not found"))?;
        if &current != target.relation() {
            return Err(StorageError::not_found(
                "Class relation no longer matches the resolved target",
            ));
        }
        let document = AuditDocument::builder(format!(
            "Class relation {} -> {} deleted",
            current.from_class_id().id(),
            current.to_class_id().id()
        ))
        .before(current.audit_snapshot())
        .metadata(serde_json::json!({
            "from_class_id": current.from_class_id().id(),
            "to_class_id": current.to_class_id().id(),
            "related_collection_ids": [
                target.from_class().collection_id().id(),
                target.to_class().collection_id().id()
            ],
        }))
        .try_build()
        .map_err(|error| StorageError::backend_failure(error.to_string()))?;
        let receipt = append_memory_event!(
            state,
            EntityType::ClassRelation,
            relation_id.id(),
            None,
            None,
            Action::Deleted,
            context,
            document,
            Some(current.metadata().revision()),
            None,
        )?;
        state.class_relations.remove(&relation_id.id());
        state
            .object_relations
            .retain(|_, relation| relation.class_relation_id() != relation_id);
        Ok(StorageMutationOutcome::committed((), receipt))
    }
}

#[async_trait]
impl ObjectRelationStorage for MemoryStorage {
    async fn prepare_object_relation(
        &self,
        selector: StorageObjectRelationCreateSelector,
    ) -> Result<StoragePreparedObjectRelation, StorageError> {
        let state = self.state.read().await;
        let command = match selector {
            StorageObjectRelationCreateSelector::Explicit(command) => command,
            StorageObjectRelationCreateSelector::Between { from, to } => {
                let (from_object_id, to_object_id) = if from.object_id() < to.object_id() {
                    (from.object_id(), to.object_id())
                } else {
                    (to.object_id(), from.object_id())
                };
                let class_relation = state
                    .class_relations
                    .values()
                    .find(|relation| {
                        (relation.from_class_id() == from.class_id()
                            && relation.to_class_id() == to.class_id())
                            || (relation.from_class_id() == to.class_id()
                                && relation.to_class_id() == from.class_id())
                    })
                    .ok_or_else(|| StorageError::not_found("Class relation was not found"))?;
                StorageObjectRelationCreate::new(
                    from_object_id,
                    to_object_id,
                    ClassRelationId::from(class_relation.metadata().id()),
                )
            }
        };
        let from_object = state
            .objects
            .get(&command.from_object_id().id())
            .cloned()
            .ok_or_else(|| StorageError::not_found("From object was not found"))?;
        let to_object = state
            .objects
            .get(&command.to_object_id().id())
            .cloned()
            .ok_or_else(|| StorageError::not_found("To object was not found"))?;
        let relation = state
            .class_relations
            .get(&command.class_relation_id().id())
            .cloned()
            .ok_or_else(|| StorageError::not_found("Class relation was not found"))?;
        let from_class = state
            .classes
            .get(&relation.from_class_id().id())
            .cloned()
            .ok_or_else(|| StorageError::backend_failure("Class relation endpoint is missing"))?;
        let to_class = state
            .classes
            .get(&relation.to_class_id().id())
            .cloned()
            .ok_or_else(|| StorageError::backend_failure("Class relation endpoint is missing"))?;
        let class_relation = StorageResolvedClassRelation::try_new(relation, from_class, to_class)
            .map_err(|error| StorageError::backend_failure(error.to_string()))?;
        StoragePreparedObjectRelation::try_new(command, from_object, to_object, class_relation)
            .map_err(|error| StorageError::invalid_input(error.to_string()))
    }

    async fn resolve_object_relation(
        &self,
        selector: StorageObjectRelationSelector,
    ) -> Result<StorageResolvedObjectRelation, StorageError> {
        let state = self.state.read().await;
        let relation = match selector {
            StorageObjectRelationSelector::Id(id) => state.object_relations.get(&id.id()),
            StorageObjectRelationSelector::Between { from, to } => {
                let (from_id, to_id) = if from.object_id() < to.object_id() {
                    (from.object_id(), to.object_id())
                } else {
                    (to.object_id(), from.object_id())
                };
                state.object_relations.values().find(|relation| {
                    relation.from_object_id() == from_id && relation.to_object_id() == to_id
                })
            }
        }
        .cloned()
        .ok_or_else(|| StorageError::not_found("Object relation was not found"))?;
        let from_object = state
            .objects
            .get(&relation.from_object_id().id())
            .cloned()
            .ok_or_else(|| StorageError::backend_failure("Object relation endpoint is missing"))?;
        let to_object = state
            .objects
            .get(&relation.to_object_id().id())
            .cloned()
            .ok_or_else(|| StorageError::backend_failure("Object relation endpoint is missing"))?;
        let class_relation = state
            .class_relations
            .get(&relation.class_relation_id().id())
            .cloned()
            .ok_or_else(|| StorageError::backend_failure("Class relation is missing"))?;
        let from_class = state
            .classes
            .get(&class_relation.from_class_id().id())
            .cloned()
            .ok_or_else(|| StorageError::backend_failure("Class relation endpoint is missing"))?;
        let to_class = state
            .classes
            .get(&class_relation.to_class_id().id())
            .cloned()
            .ok_or_else(|| StorageError::backend_failure("Class relation endpoint is missing"))?;
        let class_relation =
            StorageResolvedClassRelation::try_new(class_relation, from_class, to_class)
                .map_err(|error| StorageError::backend_failure(error.to_string()))?;
        StorageResolvedObjectRelation::try_new(relation, from_object, to_object, class_relation)
            .map_err(|error| StorageError::backend_failure(error.to_string()))
    }

    async fn create_object_relation(
        &self,
        prepared: &StoragePreparedObjectRelation,
        context: &EventContext,
    ) -> Result<StorageMutationOutcome<StorageResolvedObjectRelation>, StorageError> {
        let mut state = self.state.write().await;
        let command = *prepared.command();
        if state.objects.get(&prepared.from_object().id().id()) != Some(prepared.from_object())
            || state.objects.get(&prepared.to_object().id().id()) != Some(prepared.to_object())
            || state.class_relations.get(&command.class_relation_id().id())
                != Some(prepared.class_relation().relation())
        {
            return Err(StorageError::not_found(
                "Object relation aggregate no longer matches its prepared value",
            ));
        }
        if state.object_relations.values().any(|relation| {
            relation.from_object_id() == command.from_object_id()
                && relation.to_object_id() == command.to_object_id()
        }) {
            return Err(StorageError::conflict(
                "An object relation already exists between these objects",
            ));
        }
        let id = state.next_object_relation_id;
        state.next_object_relation_id += 1;
        let now = Utc::now();
        let metadata = StorageRecordMetadata::try_new(
            ResourceId::new(id)
                .map_err(|error| StorageError::backend_failure(error.to_string()))?,
            now,
            now,
            ResourceRevision::INITIAL,
        )
        .map_err(|error| StorageError::backend_failure(error.to_string()))?;
        let relation = StorageObjectRelation::try_new(
            metadata,
            command.from_object_id(),
            command.to_object_id(),
            command.class_relation_id(),
        )
        .map_err(|error| StorageError::invalid_input(error.to_string()))?;
        let resolved = StorageResolvedObjectRelation::try_new(
            relation.clone(),
            prepared.from_object().clone(),
            prepared.to_object().clone(),
            prepared.class_relation().clone(),
        )
        .map_err(|error| StorageError::backend_failure(error.to_string()))?;
        let document = AuditDocument::builder(format!(
            "Object relation {} -> {} created",
            relation.from_object_id().id(),
            relation.to_object_id().id()
        ))
        .after(relation.audit_snapshot())
        .metadata(serde_json::json!({
            "class_relation_id": relation.class_relation_id().id(),
            "from_object_id": prepared.from_object().id().id(),
            "to_object_id": prepared.to_object().id().id(),
            "from_class_id": prepared.from_object().class_id().id(),
            "to_class_id": prepared.to_object().class_id().id(),
            "related_collection_ids": [
                prepared.from_object().collection_id().id(),
                prepared.to_object().collection_id().id()
            ],
        }))
        .try_build()
        .map_err(|error| StorageError::backend_failure(error.to_string()))?;
        let receipt = append_memory_event!(
            state,
            EntityType::ObjectRelation,
            id,
            None,
            None,
            Action::Created,
            context,
            document,
            None,
            Some(relation.metadata().revision()),
        )?;
        state.object_relations.insert(id, relation);
        Ok(StorageMutationOutcome::committed(resolved, receipt))
    }

    async fn delete_object_relation(
        &self,
        target: &StorageResolvedObjectRelation,
        context: &EventContext,
    ) -> Result<StorageMutationOutcome<()>, StorageError> {
        let mut state = self.state.write().await;
        let relation_id = ObjectRelationId::from(target.relation().metadata().id());
        let current = state
            .object_relations
            .get(&relation_id.id())
            .cloned()
            .ok_or_else(|| StorageError::not_found("Object relation was not found"))?;
        if &current != target.relation() {
            return Err(StorageError::not_found(
                "Object relation no longer matches the resolved target",
            ));
        }
        let document = AuditDocument::builder(format!(
            "Object relation {} -> {} deleted",
            current.from_object_id().id(),
            current.to_object_id().id()
        ))
        .before(current.audit_snapshot())
        .metadata(serde_json::json!({
            "class_relation_id": current.class_relation_id().id(),
            "from_object_id": target.from_object().id().id(),
            "to_object_id": target.to_object().id().id(),
            "from_class_id": target.from_object().class_id().id(),
            "to_class_id": target.to_object().class_id().id(),
            "related_collection_ids": [
                target.from_object().collection_id().id(),
                target.to_object().collection_id().id()
            ],
        }))
        .try_build()
        .map_err(|error| StorageError::backend_failure(error.to_string()))?;
        let receipt = append_memory_event!(
            state,
            EntityType::ObjectRelation,
            relation_id.id(),
            None,
            None,
            Action::Deleted,
            context,
            document,
            Some(current.metadata().revision()),
            None,
        )?;
        state.object_relations.remove(&relation_id.id());
        Ok(StorageMutationOutcome::committed((), receipt))
    }
}

#[async_trait]
impl AuthenticationStorage for MemoryStorage {
    async fn authenticate_bearer_token(
        &self,
        attempt: StorageAuthenticationAttempt,
    ) -> Result<StorageAuthenticatedToken, StorageError> {
        let (credential, observed_at, legacy_valid_after) = attempt.into_parts();
        let observation = StorageTokenObservation::try_new(observed_at, legacy_valid_after)
            .map_err(invalid_contract_value)?;
        let mut state = self.state.write().await;
        let token_id = state
            .tokens
            .values()
            .find(|record| record.token_hash == credential.lookup_value())
            .map(|record| record.id)
            .ok_or_else(|| StorageError::authentication_required("Invalid bearer token"))?;
        let token = state
            .tokens
            .get(&token_id.id())
            .cloned()
            .ok_or_else(|| StorageError::internal("authenticated token disappeared"))?;
        let metadata = token.metadata(observation)?;
        if !metadata.is_active() {
            return Err(StorageError::authentication_required(
                "Bearer token is expired or revoked",
            ));
        }
        let principal = state
            .principals
            .get(&token.principal_id.id())
            .ok_or_else(|| {
                StorageError::authentication_required("Token principal was not found")
            })?;
        if principal.kind().is_service_account()
            && state
                .service_accounts
                .get(&principal.id().id())
                .is_some_and(|account| account.is_disabled())
        {
            return Err(StorageError::authentication_required(
                "Token principal is disabled",
            ));
        }
        let permission_scoped = token
            .scope
            .clone()
            .map(StorageAuthenticationTokenScope::into_parts)
            .is_some_and(|parts| parts.0.is_some());
        let resource_scoped = token
            .scope
            .clone()
            .map(StorageAuthenticationTokenScope::into_parts)
            .is_some_and(|parts| parts.1.is_some());
        let record = state
            .tokens
            .get_mut(&token_id.id())
            .ok_or_else(|| StorageError::internal("authenticated token disappeared"))?;
        record.last_used_at = Some(observed_at);
        StorageAuthenticatedToken::builder(
            token.id,
            token.principal_id,
            token.issued,
            token.revision,
        )
        .name(token.name)
        .description(token.description)
        .expires_at(token.expires_at)
        .last_used_at(Some(observed_at))
        .permission_scoped(permission_scoped)
        .resource_scoped(resource_scoped)
        .try_build()
        .map_err(invalid_contract_value)
    }

    async fn get_authentication_identity(
        &self,
        principal_id: PrincipalId,
    ) -> Result<StorageAuthenticationIdentity, StorageError> {
        let state = self.state.read().await;
        let principal = state.principals.get(&principal_id.id()).ok_or_else(|| {
            StorageError::not_found(format!("Principal {} was not found", principal_id.id()))
        })?;
        let projection = StorageAuthenticationPrincipal::new(
            principal.id(),
            principal.kind(),
            principal.name(),
            principal.identity_scope_id(),
        );
        let human = if principal.kind().is_human() {
            let parts = state
                .users
                .get(&principal_id.id())
                .ok_or_else(|| StorageError::internal("human principal has no user record"))?
                .user
                .clone()
                .into_parts();
            Some(
                StorageAuthenticationHuman::try_new(
                    parts.id(),
                    parts.proper_name().map(ToOwned::to_owned),
                    parts.email().map(ToOwned::to_owned),
                    parts.created_at(),
                    parts.updated_at(),
                    parts.anonymized_at(),
                )
                .map_err(invalid_contract_value)?,
            )
        } else {
            None
        };
        StorageAuthenticationIdentity::try_new(projection, human).map_err(invalid_contract_value)
    }

    async fn get_authentication_token_scope(
        &self,
        query: StorageAuthenticationTokenScopeQuery,
    ) -> Result<Option<StorageAuthenticationTokenScope>, StorageError> {
        if !query.is_scoped() {
            return Ok(None);
        }
        let persisted = self
            .state
            .read()
            .await
            .tokens
            .get(&query.token_id().id())
            .and_then(|record| record.scope.clone());
        let (persisted_permissions, persisted_resources) = persisted
            .map(StorageAuthenticationTokenScope::into_parts)
            .unwrap_or_default();
        Ok(Some(StorageAuthenticationTokenScope::new(
            query
                .is_permission_scoped()
                .then(|| persisted_permissions.unwrap_or_default()),
            query.is_resource_scoped().then(|| {
                persisted_resources.unwrap_or_else(StorageAuthenticationResourceScope::default)
            }),
        )))
    }
}

#[async_trait]
impl LocalIdentityCredentialStorage for MemoryStorage {
    async fn is_default_admin_bootstrap_required(&self) -> Result<bool, StorageError> {
        Ok(self.state.read().await.users.is_empty())
    }

    async fn bootstrap_default_admin(
        &self,
        _request: StorageDefaultAdminBootstrap,
    ) -> Result<bool, StorageError> {
        Ok(false)
    }

    async fn reset_local_password(
        &self,
        request: StorageLocalPasswordReset,
    ) -> Result<StorageMutationOutcome<usize>, StorageError> {
        let (name, password_hash, context) = request.into_parts();
        let mut state = self.state.write().await;
        let user_id = state
            .users
            .iter()
            .find(|(_, record)| {
                record.name == name
                    && state
                        .identity_scopes
                        .get(&record.identity_scope_id.id())
                        .is_some_and(|scope| scope.name() == LOCAL_IDENTITY_SCOPE)
            })
            .map(|(id, _)| *id)
            .ok_or_else(|| StorageError::not_found(format!("User '{name}' was not found")))?;
        let record = state
            .users
            .get(&user_id)
            .cloned()
            .ok_or_else(|| StorageError::internal("password reset user disappeared"))?;
        let parts = record.user.into_parts();
        let now = Utc::now();
        let user = StorageUser::try_new(
            parts.id(),
            Some(password_hash),
            parts.proper_name().map(ToOwned::to_owned),
            parts.email().map(ToOwned::to_owned),
            parts.created_at(),
            now,
            parts.anonymized_at(),
        )
        .map_err(invalid_contract_value)?;
        state
            .users
            .get_mut(&user_id)
            .expect("password reset user exists")
            .user = user;
        let principal = state
            .principals
            .get(&user_id)
            .cloned()
            .ok_or_else(|| StorageError::internal("password reset principal is missing"))?;
        state.principals.insert(
            user_id,
            advanced_principal(
                &principal,
                principal.name(),
                principal.settings().clone(),
                now,
            )?,
        );
        for token in state.tokens.values_mut() {
            if token.principal_id.id() == user_id && token.revoked_at.is_none() {
                token.revoked_at = Some(now);
            }
        }
        let receipt = state.append_simple_event(
            EntityType::User,
            user_id,
            Some(&name),
            Action::Updated,
            &context,
            format!("User '{name}' password reset"),
        )?;
        Ok(StorageMutationOutcome::committed(1, receipt))
    }
}

#[async_trait]
impl IdentityScopeStorage for MemoryStorage {
    async fn ensure_identity_scope(
        &self,
        request: StorageIdentityScopeEnsure,
    ) -> Result<StorageIdentityScope, StorageError> {
        let mut state = self.state.write().await;
        if let Some(scope) = state.identity_scope_by_name(request.name()) {
            if scope.provider_kind() != request.provider_kind() {
                return Err(StorageError::conflict(format!(
                    "Identity scope '{}' uses provider kind '{}'",
                    request.name(),
                    scope.provider_kind()
                )));
            }
            return Ok(scope.clone());
        }
        let id = IdentityScopeId::new(state.next_identity_scope_id)
            .expect("memory identity scope id is positive");
        state.next_identity_scope_id += 1;
        let now = Utc::now();
        let scope = StorageIdentityScope::try_new(
            id,
            request.name(),
            request.provider_kind(),
            now,
            now,
            ResourceRevision::INITIAL,
        )
        .map_err(invalid_contract_value)?;
        state.identity_scopes.insert(id.id(), scope.clone());
        Ok(scope)
    }

    async fn resolve_identity_scope_name(
        &self,
        scope_id: IdentityScopeId,
    ) -> Result<String, StorageError> {
        self.state
            .read()
            .await
            .identity_scopes
            .get(&scope_id.id())
            .map(|scope| scope.name().to_string())
            .ok_or_else(|| {
                StorageError::not_found(format!("Identity scope {} was not found", scope_id.id()))
            })
    }

    async fn resolve_identity_scope_names(
        &self,
        scope_ids: Vec<IdentityScopeId>,
    ) -> Result<Vec<(IdentityScopeId, String)>, StorageError> {
        let state = self.state.read().await;
        scope_ids
            .into_iter()
            .map(|id| {
                state
                    .identity_scopes
                    .get(&id.id())
                    .map(|scope| (id, scope.name().to_string()))
                    .ok_or_else(|| {
                        StorageError::not_found(format!("Identity scope {} was not found", id.id()))
                    })
            })
            .collect()
    }
}

#[async_trait]
impl GroupMembershipStorage for MemoryStorage {
    async fn get_principal_group(
        &self,
        principal_id: PrincipalId,
        group_id: GroupId,
    ) -> Result<StoragePrincipalGroup, StorageError> {
        self.state
            .read()
            .await
            .memberships
            .get(&(principal_id.id(), group_id.id()))
            .cloned()
            .ok_or_else(|| {
                StorageError::not_found(format!(
                    "Principal {} is not a member of group {}",
                    principal_id.id(),
                    group_id.id()
                ))
            })
    }

    async fn list_principal_groups(
        &self,
        query: StoragePrincipalGroupListQuery,
    ) -> Result<StoragePage<StorageIdentityGroup>, StorageError> {
        let (principal_id, options) = query.into_parts();
        let state = self.state.read().await;
        let rows = state
            .memberships
            .values()
            .filter(|membership| membership.principal_id() == principal_id)
            .filter_map(|membership| state.groups.get(&membership.group_id().id()).cloned())
            .collect();
        page(rows, &options)
    }

    async fn is_human_owner_group_member(
        &self,
        principal_id: PrincipalId,
        owner_group_id: GroupId,
    ) -> Result<bool, StorageError> {
        let state = self.state.read().await;
        Ok(state
            .principals
            .get(&principal_id.id())
            .is_some_and(|principal| principal.kind().is_human())
            && state
                .memberships
                .contains_key(&(principal_id.id(), owner_group_id.id())))
    }

    async fn load_group_member_principals(
        &self,
        group_id: GroupId,
    ) -> Result<Vec<StoragePrincipal>, StorageError> {
        let state = self.state.read().await;
        Ok(state
            .memberships
            .values()
            .filter(|membership| membership.group_id() == group_id)
            .filter_map(|membership| {
                state
                    .principals
                    .get(&membership.principal_id().id())
                    .cloned()
            })
            .collect())
    }

    async fn list_group_members(
        &self,
        group_id: GroupId,
        query_options: QueryOptions,
    ) -> Result<StoragePage<StorageGroupMember>, StorageError> {
        let state = self.state.read().await;
        let rows = state
            .memberships
            .values()
            .filter(|membership| membership.group_id() == group_id)
            .map(|membership| {
                let principal = state
                    .principals
                    .get(&membership.principal_id().id())
                    .cloned()
                    .ok_or_else(|| StorageError::internal("membership principal is missing"))?;
                StorageGroupMember::try_new(membership.clone(), principal)
                    .map_err(invalid_contract_value)
            })
            .collect::<Result<Vec<_>, _>>()?;
        page(rows, &query_options)
    }

    async fn add_group_member(
        &self,
        principal_id: PrincipalId,
        group_id: GroupId,
        context: &EventContext,
    ) -> Result<StorageMutationOutcome<StoragePrincipalGroup>, StorageError> {
        let mut state = self.state.write().await;
        if !state.principals.contains_key(&principal_id.id()) {
            return Err(StorageError::not_found(format!(
                "Principal {} was not found",
                principal_id.id()
            )));
        }
        if !state.groups.contains_key(&group_id.id()) {
            return Err(StorageError::not_found(format!(
                "Group {} was not found",
                group_id.id()
            )));
        }
        if let Some(existing) = state.memberships.get(&(principal_id.id(), group_id.id())) {
            return Ok(StorageMutationOutcome::unchanged(existing.clone()));
        }
        let now = Utc::now();
        let membership = StoragePrincipalGroup::try_new(
            principal_id,
            group_id,
            now,
            now,
            ResourceRevision::INITIAL,
        )
        .map_err(invalid_contract_value)?;
        state
            .memberships
            .insert((principal_id.id(), group_id.id()), membership.clone());
        let receipt = state.append_simple_event(
            EntityType::UserGroup,
            principal_id.id(),
            None,
            Action::Added,
            context,
            format!(
                "Principal {} added to group {}",
                principal_id.id(),
                group_id.id()
            ),
        )?;
        Ok(StorageMutationOutcome::committed(membership, receipt))
    }

    async fn remove_group_member(
        &self,
        principal_id: PrincipalId,
        group_id: GroupId,
        context: &EventContext,
    ) -> Result<StorageMutationOutcome<()>, StorageError> {
        let mut state = self.state.write().await;
        if state
            .memberships
            .remove(&(principal_id.id(), group_id.id()))
            .is_none()
        {
            return Ok(StorageMutationOutcome::unchanged(()));
        }
        let receipt = state.append_simple_event(
            EntityType::UserGroup,
            principal_id.id(),
            None,
            Action::Removed,
            context,
            format!(
                "Principal {} removed from group {}",
                principal_id.id(),
                group_id.id()
            ),
        )?;
        Ok(StorageMutationOutcome::committed((), receipt))
    }
}

#[async_trait]
impl ServiceAccountStorage for MemoryStorage {
    async fn is_service_account_disabled(
        &self,
        principal_id: PrincipalId,
    ) -> Result<bool, StorageError> {
        self.state
            .read()
            .await
            .service_accounts
            .get(&principal_id.id())
            .map(StorageServiceAccount::is_disabled)
            .ok_or_else(|| {
                StorageError::not_found(format!(
                    "Service account {} was not found",
                    principal_id.id()
                ))
            })
    }

    async fn get_service_account(
        &self,
        service_account_id: ServiceAccountId,
    ) -> Result<StorageServiceAccount, StorageError> {
        self.state
            .read()
            .await
            .service_accounts
            .get(&service_account_id.id())
            .cloned()
            .ok_or_else(|| {
                StorageError::not_found(format!(
                    "Service account {} was not found",
                    service_account_id.id()
                ))
            })
    }

    async fn get_service_account_details(
        &self,
        service_account_id: ServiceAccountId,
    ) -> Result<StorageServiceAccountDetails, StorageError> {
        let state = self.state.read().await;
        let account = state
            .service_accounts
            .get(&service_account_id.id())
            .cloned()
            .ok_or_else(|| {
                StorageError::not_found(format!(
                    "Service account {} was not found",
                    service_account_id.id()
                ))
            })?;
        let principal = state
            .principals
            .get(&service_account_id.id())
            .ok_or_else(|| StorageError::internal("service-account principal is missing"))?;
        Ok(StorageServiceAccountDetails::new(
            account,
            principal.identity_scope_id(),
            principal.name(),
            principal.revision(),
        ))
    }

    async fn list_manageable_service_accounts(
        &self,
        query: StorageServiceAccountListQuery,
    ) -> Result<StoragePage<StorageServiceAccountListItem>, StorageError> {
        let (requestor_id, administrator, options) = query.into_parts();
        let state = self.state.read().await;
        let rows = state
            .service_accounts
            .values()
            .filter(|account| {
                administrator
                    || account.created_by() == Some(requestor_id)
                    || state
                        .memberships
                        .contains_key(&(requestor_id.id(), account.owner_group_id().id()))
            })
            .map(|account| {
                let principal = state.principals.get(&account.id().id()).ok_or_else(|| {
                    StorageError::internal("service-account principal is missing")
                })?;
                let scope = state
                    .identity_scopes
                    .get(&principal.identity_scope_id().id())
                    .ok_or_else(|| {
                        StorageError::internal("service-account identity scope is missing")
                    })?;
                Ok(StorageServiceAccountListItem::new(
                    account.clone(),
                    scope.name(),
                    principal.name(),
                    principal.revision(),
                ))
            })
            .collect::<Result<Vec<_>, StorageError>>()?;
        page(rows, &options)
    }

    async fn create_service_account(
        &self,
        request: StorageServiceAccountCreate,
    ) -> Result<StorageMutationOutcome<StorageServiceAccount>, StorageError> {
        let (name, description, owner_group_id, created_by, context) = request.into_parts();
        let mut state = self.state.write().await;
        if !state.groups.contains_key(&owner_group_id.id()) {
            return Err(StorageError::not_found(format!(
                "Owner group {} was not found",
                owner_group_id.id()
            )));
        }
        let scope = state
            .identity_scope_by_name(LOCAL_IDENTITY_SCOPE)
            .cloned()
            .ok_or_else(|| StorageError::internal("local identity scope is missing"))?;
        if state.principals.values().any(|principal| {
            principal.identity_scope_id() == scope.id() && principal.name() == name
        }) {
            return Err(StorageError::conflict(format!(
                "Principal '{name}' already exists"
            )));
        }
        let principal_id = PrincipalId::new(state.next_principal_id)
            .expect("memory service-account principal id is positive");
        state.next_principal_id += 1;
        let id = ServiceAccountId::new(principal_id.id())
            .expect("memory service-account id is positive");
        let now = Utc::now();
        let metadata = StorageRecordMetadata::try_new(
            ResourceId::new(id.id()).expect("service-account resource id is positive"),
            now,
            now,
            ResourceRevision::INITIAL,
        )
        .map_err(invalid_contract_value)?;
        let principal = StoragePrincipal::builder(
            metadata,
            PrincipalKind::ServiceAccount,
            name.clone(),
            scope.id(),
        )
        .try_build()
        .map_err(invalid_contract_value)?;
        let account = StorageServiceAccount::try_new(
            id,
            description,
            owner_group_id,
            created_by,
            None,
            now,
            now,
        )
        .map_err(invalid_contract_value)?;
        state.principals.insert(principal_id.id(), principal);
        state.service_accounts.insert(id.id(), account.clone());
        let receipt = state.append_simple_event(
            EntityType::ServiceAccount,
            id.id(),
            Some(&name),
            Action::Created,
            &context,
            format!("Service account '{name}' created"),
        )?;
        Ok(StorageMutationOutcome::committed(account, receipt))
    }

    async fn update_service_account(
        &self,
        request: StorageServiceAccountUpdate,
    ) -> Result<StorageMutationOutcome<StorageServiceAccount>, StorageError> {
        let (id, description, owner_group_id, context) = request.into_parts();
        let mut state = self.state.write().await;
        let current = state
            .service_accounts
            .get(&id.id())
            .cloned()
            .ok_or_else(|| {
                StorageError::not_found(format!("Service account {} was not found", id.id()))
            })?;
        if description.is_none() && owner_group_id.is_none() {
            return Ok(StorageMutationOutcome::unchanged(current));
        }
        let owner_group_id = owner_group_id.unwrap_or(current.owner_group_id());
        if !state.groups.contains_key(&owner_group_id.id()) {
            return Err(StorageError::not_found(format!(
                "Owner group {} was not found",
                owner_group_id.id()
            )));
        }
        let now = Utc::now();
        let updated = StorageServiceAccount::try_new(
            id,
            description.unwrap_or_else(|| current.description().to_string()),
            owner_group_id,
            current.created_by(),
            current.disabled_at(),
            current.created_at(),
            now,
        )
        .map_err(invalid_contract_value)?;
        state.service_accounts.insert(id.id(), updated.clone());
        let principal = state
            .principals
            .get(&id.id())
            .cloned()
            .ok_or_else(|| StorageError::internal("service-account principal is missing"))?;
        state.principals.insert(
            id.id(),
            advanced_principal(
                &principal,
                principal.name(),
                principal.settings().clone(),
                now,
            )?,
        );
        let receipt = state.append_simple_event(
            EntityType::ServiceAccount,
            id.id(),
            Some(principal.name()),
            Action::Updated,
            &context,
            format!("Service account '{}' updated", principal.name()),
        )?;
        Ok(StorageMutationOutcome::committed(updated, receipt))
    }

    async fn disable_service_account(
        &self,
        request: StorageServiceAccountMutation,
    ) -> Result<StorageMutationOutcome<StorageServiceAccountDisableOutcome>, StorageError> {
        let (id, context) = request.into_parts();
        let mut state = self.state.write().await;
        let current = state
            .service_accounts
            .get(&id.id())
            .cloned()
            .ok_or_else(|| {
                StorageError::not_found(format!("Service account {} was not found", id.id()))
            })?;
        if current.is_disabled() {
            return Ok(StorageMutationOutcome::unchanged(
                StorageServiceAccountDisableOutcome::new(current, Vec::new()),
            ));
        }
        let now = Utc::now();
        let disabled = StorageServiceAccount::try_new(
            id,
            current.description(),
            current.owner_group_id(),
            current.created_by(),
            Some(now),
            current.created_at(),
            now,
        )
        .map_err(invalid_contract_value)?;
        state.service_accounts.insert(id.id(), disabled.clone());
        for token in state.tokens.values_mut() {
            if token.principal_id.id() == id.id() && token.revoked_at.is_none() {
                token.revoked_at = Some(now);
            }
        }
        let cancelled = state.cancel_queued_tasks_for_principal(
            PrincipalId::new(id.id()).expect("service account principal id is positive"),
        )?;
        let principal = state
            .principals
            .get(&id.id())
            .cloned()
            .ok_or_else(|| StorageError::internal("service-account principal is missing"))?;
        state.principals.insert(
            id.id(),
            advanced_principal(
                &principal,
                principal.name(),
                principal.settings().clone(),
                now,
            )?,
        );
        let receipt = state.append_simple_event(
            EntityType::ServiceAccount,
            id.id(),
            Some(principal.name()),
            Action::Disabled,
            &context,
            format!("Service account '{}' disabled", principal.name()),
        )?;
        Ok(StorageMutationOutcome::committed(
            StorageServiceAccountDisableOutcome::new(disabled, cancelled),
            receipt,
        ))
    }

    async fn delete_service_account(
        &self,
        request: StorageServiceAccountMutation,
    ) -> Result<StorageMutationOutcome<()>, StorageError> {
        let (id, context) = request.into_parts();
        let mut state = self.state.write().await;
        let Some(account) = state.service_accounts.remove(&id.id()) else {
            return Ok(StorageMutationOutcome::unchanged(()));
        };
        let name = state
            .principals
            .remove(&id.id())
            .map(|principal| principal.name().to_string())
            .unwrap_or_else(|| id.to_string());
        state
            .memberships
            .retain(|(principal_id, _), _| *principal_id != id.id());
        state
            .tokens
            .retain(|_, token| token.principal_id.id() != id.id());
        for task in state.tasks.values_mut() {
            if task
                .submitted_by
                .is_some_and(|principal| principal.id() == id.id())
            {
                task.submitted_by = None;
            }
        }
        let receipt = state.append_simple_event(
            EntityType::ServiceAccount,
            account.id().id(),
            Some(&name),
            Action::Deleted,
            &context,
            format!("Service account '{name}' deleted"),
        )?;
        Ok(StorageMutationOutcome::committed((), receipt))
    }
}

#[async_trait]
impl ExternalIdentityStorage for MemoryStorage {
    async fn get_external_principal_state(
        &self,
        principal_id: PrincipalId,
    ) -> Result<Option<StorageExternalPrincipalState>, StorageError> {
        let state = self.state.read().await;
        let Some(record) = state.users.get(&principal_id.id()) else {
            return Ok(None);
        };
        if !record.provider_managed {
            return Ok(None);
        }
        let scope = state
            .identity_scopes
            .get(&record.identity_scope_id.id())
            .ok_or_else(|| StorageError::internal("external user identity scope is missing"))?;
        StorageExternalPrincipalState::try_new(
            scope.name(),
            record.name.clone(),
            record
                .external_subject
                .clone()
                .ok_or_else(|| StorageError::internal("external user subject is missing"))?,
            record.last_sync_attempted_at,
            record.last_sync_success_at,
        )
        .map(Some)
        .map_err(invalid_contract_value)
    }

    async fn mark_external_sync_attempted(
        &self,
        principal_id: PrincipalId,
    ) -> Result<(), StorageError> {
        let mut state = self.state.write().await;
        let current = state
            .users
            .get(&principal_id.id())
            .cloned()
            .ok_or_else(|| {
                StorageError::not_found(format!("Principal {} was not found", principal_id.id()))
            })?;
        if !current.provider_managed {
            return Ok(());
        }
        let now = Utc::now();
        state
            .users
            .get_mut(&principal_id.id())
            .expect("external user exists")
            .last_sync_attempted_at = Some(now);
        let principal = state
            .principals
            .get(&principal_id.id())
            .cloned()
            .ok_or_else(|| StorageError::internal("external principal is missing"))?;
        let revision = principal
            .revision()
            .checked_advance()
            .map_err(|error| StorageError::internal(error.to_string()))?;
        let metadata = StorageRecordMetadata::try_new(
            ResourceId::new(principal_id.id()).expect("principal resource id is positive"),
            principal.created_at(),
            now,
            revision,
        )
        .map_err(invalid_contract_value)?;
        let updated = StoragePrincipal::builder(
            metadata,
            principal.kind(),
            principal.name(),
            principal.identity_scope_id(),
        )
        .provider_managed(true)
        .settings(principal.settings().clone())
        .external_subject(principal.external_subject().map(ToOwned::to_owned))
        .last_sync_attempted_at(Some(now))
        .last_sync_success_at(principal.last_sync_success_at())
        .try_build()
        .map_err(invalid_contract_value)?;
        state.principals.insert(principal_id.id(), updated);
        Ok(())
    }

    async fn sync_external_user(
        &self,
        request: StorageExternalUserSync,
    ) -> Result<StorageMutationOutcome<StorageSyncedHuman>, StorageError> {
        let (scope_name, provider_kind, subject, name, proper_name, email, groups) =
            request.into_parts();
        let mut state = self.state.write().await;
        let scope = if let Some(scope) = state.identity_scope_by_name(&scope_name).cloned() {
            if scope.provider_kind() != provider_kind {
                return Err(StorageError::conflict(format!(
                    "Identity scope '{scope_name}' uses provider kind '{}'",
                    scope.provider_kind()
                )));
            }
            scope
        } else {
            let id = IdentityScopeId::new(state.next_identity_scope_id)
                .expect("memory identity scope id is positive");
            state.next_identity_scope_id += 1;
            let now = Utc::now();
            let scope = StorageIdentityScope::try_new(
                id,
                scope_name.clone(),
                provider_kind.clone(),
                now,
                now,
                ResourceRevision::INITIAL,
            )
            .map_err(invalid_contract_value)?;
            state.identity_scopes.insert(id.id(), scope.clone());
            scope
        };
        let existing_id = state
            .users
            .iter()
            .find(|(_, record)| {
                record.identity_scope_id == scope.id()
                    && (record.external_subject.as_deref() == Some(subject.as_str())
                        || record.name == name)
            })
            .map(|(id, _)| *id);
        let now = Utc::now();
        let principal_id = if let Some(id) = existing_id {
            let record = state
                .users
                .get(&id)
                .cloned()
                .expect("selected external user exists");
            let parts = record.user.into_parts();
            let user = StorageUser::try_new(
                parts.id(),
                None,
                proper_name.clone(),
                email.clone(),
                parts.created_at(),
                now,
                parts.anonymized_at(),
            )
            .map_err(invalid_contract_value)?;
            let record = state.users.get_mut(&id).expect("external user exists");
            record.user = user;
            record.name = name.clone();
            record.provider_managed = true;
            record.external_subject = Some(subject.clone());
            record.last_sync_attempted_at = Some(now);
            record.last_sync_success_at = Some(now);
            let current = state
                .principals
                .get(&id)
                .cloned()
                .ok_or_else(|| StorageError::internal("external principal is missing"))?;
            let revision = current
                .revision()
                .checked_advance()
                .map_err(|error| StorageError::internal(error.to_string()))?;
            let metadata = StorageRecordMetadata::try_new(
                ResourceId::new(id).expect("principal resource id is positive"),
                current.created_at(),
                now,
                revision,
            )
            .map_err(invalid_contract_value)?;
            let principal =
                StoragePrincipal::builder(metadata, PrincipalKind::Human, name.clone(), scope.id())
                    .provider_managed(true)
                    .settings(current.settings().clone())
                    .external_subject(Some(subject.clone()))
                    .last_sync_attempted_at(Some(now))
                    .last_sync_success_at(Some(now))
                    .try_build()
                    .map_err(invalid_contract_value)?;
            state.principals.insert(id, principal);
            PrincipalId::new(id).expect("external principal id is positive")
        } else {
            let principal_id =
                PrincipalId::new(state.next_principal_id).expect("memory principal id is positive");
            state.next_principal_id += 1;
            let metadata = StorageRecordMetadata::try_new(
                ResourceId::new(principal_id.id()).expect("principal resource id is positive"),
                now,
                now,
                ResourceRevision::INITIAL,
            )
            .map_err(invalid_contract_value)?;
            let principal =
                StoragePrincipal::builder(metadata, PrincipalKind::Human, name.clone(), scope.id())
                    .provider_managed(true)
                    .external_subject(Some(subject.clone()))
                    .last_sync_attempted_at(Some(now))
                    .last_sync_success_at(Some(now))
                    .try_build()
                    .map_err(invalid_contract_value)?;
            let user_id = UserId::new(principal_id.id()).expect("external user id is positive");
            let user = StorageUser::try_new(
                user_id,
                None,
                proper_name.clone(),
                email.clone(),
                now,
                now,
                None,
            )
            .map_err(invalid_contract_value)?;
            state.principals.insert(principal_id.id(), principal);
            state.users.insert(
                user_id.id(),
                MemoryUserRecord {
                    user,
                    identity_scope_id: scope.id(),
                    name: name.clone(),
                    provider_managed: true,
                    external_subject: Some(subject.clone()),
                    last_sync_attempted_at: Some(now),
                    last_sync_success_at: Some(now),
                },
            );
            principal_id
        };

        let mut current_external_groups = BTreeSet::new();
        for external_group in groups {
            let group = if let Some(group) = state.groups.values().find(|group| {
                group.identity_scope_id() == scope.id()
                    && (group.external_key() == Some(external_group.key())
                        || group.name() == external_group.name())
            }) {
                group.clone()
            } else {
                let group_id = GroupId::new(state.next_group_id)
                    .expect("memory external group id is positive");
                state.next_group_id += 1;
                let metadata = StorageRecordMetadata::try_new(
                    ResourceId::new(group_id.id()).expect("group resource id is positive"),
                    now,
                    now,
                    ResourceRevision::INITIAL,
                )
                .map_err(invalid_contract_value)?;
                let group = StorageIdentityGroup::builder(
                    metadata,
                    external_group.name(),
                    external_group.description().unwrap_or_default(),
                    scope.id(),
                    provider_kind.clone(),
                )
                .external_key(Some(external_group.key().to_string()))
                .last_sync_attempted_at(Some(now))
                .last_sync_success_at(Some(now))
                .try_build()
                .map_err(invalid_contract_value)?;
                state.groups.insert(group_id.id(), group.clone());
                group
            };
            let key = (principal_id.id(), group.id().id());
            current_external_groups.insert(key);
            if let std::collections::btree_map::Entry::Vacant(entry) = state.memberships.entry(key)
            {
                let membership = StoragePrincipalGroup::try_new(
                    principal_id,
                    group.id(),
                    now,
                    now,
                    ResourceRevision::INITIAL,
                )
                .map_err(invalid_contract_value)?;
                entry.insert(membership);
            }
        }
        let stale_groups = state
            .external_memberships
            .iter()
            .filter(|(id, _)| *id == principal_id.id())
            .filter(|key| !current_external_groups.contains(key))
            .copied()
            .collect::<Vec<_>>();
        for key in stale_groups {
            state.external_memberships.remove(&key);
            state.memberships.remove(&key);
        }
        state.external_memberships.extend(current_external_groups);
        let user = state
            .users
            .get(&principal_id.id())
            .ok_or_else(|| StorageError::internal("synchronized user is missing"))?
            .user
            .clone()
            .into_parts();
        let synced = StorageSyncedHuman::try_new(
            user.id(),
            user.proper_name().map(ToOwned::to_owned),
            user.email().map(ToOwned::to_owned),
            user.created_at(),
            user.updated_at(),
            user.anonymized_at(),
        )
        .map_err(invalid_contract_value)?;
        let receipt = state.append_simple_event(
            EntityType::ExternalIdentitySync,
            principal_id.id(),
            Some(&name),
            Action::Succeeded,
            &EventContext::system(),
            format!("External identity '{name}' synchronized"),
        )?;
        Ok(StorageMutationOutcome::committed(synced, receipt))
    }
}

#[async_trait]
impl UserStorage for MemoryStorage {
    async fn get_user(&self, id: UserId) -> Result<StorageUser, StorageError> {
        self.state
            .read()
            .await
            .users
            .get(&id.id())
            .map(|record| record.user.clone())
            .ok_or_else(|| StorageError::not_found(format!("User {} was not found", id.id())))
    }

    async fn get_user_by_name(
        &self,
        identity_scope: String,
        name: String,
    ) -> Result<StorageUser, StorageError> {
        let state = self.state.read().await;
        state
            .users
            .values()
            .find(|record| {
                record.name == name
                    && state
                        .identity_scopes
                        .get(&record.identity_scope_id.id())
                        .is_some_and(|scope| scope.name() == identity_scope)
            })
            .map(|record| record.user.clone())
            .ok_or_else(|| {
                StorageError::not_found(format!(
                    "User '{name}' was not found in identity scope '{identity_scope}'"
                ))
            })
    }

    async fn get_user_details(&self, id: UserId) -> Result<StorageUserDetails, StorageError> {
        let state = self.state.read().await;
        let record = state
            .users
            .get(&id.id())
            .ok_or_else(|| StorageError::not_found(format!("User {} was not found", id.id())))?;
        state.user_details(record)
    }

    async fn list_users(
        &self,
        query: StorageUserListQuery,
    ) -> Result<StoragePage<StorageUserListItem>, StorageError> {
        let options = query.into_options();
        let state = self.state.read().await;
        let rows = state
            .users
            .values()
            .filter(|record| {
                options.filters().as_slice().iter().all(|filter| {
                    let equal = match filter.field {
                        FilterField::Id => {
                            record.user.clone().into_parts().id().to_string() == filter.value
                        }
                        FilterField::Name => record.name == filter.value,
                        _ => true,
                    };
                    match filter.operator {
                        SearchOperator::Equals { is_negated } => equal != is_negated,
                        _ => true,
                    }
                })
            })
            .map(|record| state.user_list_item(record))
            .collect::<Result<Vec<_>, _>>()?;
        page(rows, &options)
    }

    async fn create_user(
        &self,
        request: StorageUserCreate,
    ) -> Result<StorageMutationOutcome<StorageUser>, StorageError> {
        let (identity_scope, name, password_hash, proper_name, email, context) =
            request.into_parts();
        let identity_scope = identity_scope.unwrap_or_else(|| LOCAL_IDENTITY_SCOPE.to_string());
        let mut state = self.state.write().await;
        let scope = state
            .identity_scope_by_name(&identity_scope)
            .cloned()
            .ok_or_else(|| {
                StorageError::not_found(format!("Identity scope '{identity_scope}' was not found"))
            })?;
        if state
            .users
            .values()
            .any(|record| record.identity_scope_id == scope.id() && record.name == name.as_str())
        {
            return Err(StorageError::conflict(format!(
                "User '{name}' already exists in identity scope '{identity_scope}'"
            )));
        }
        let principal_id =
            PrincipalId::new(state.next_principal_id).expect("memory principal id is positive");
        state.next_principal_id += 1;
        let user_id = UserId::new(principal_id.id()).expect("memory user id is positive");
        let now = Utc::now();
        let metadata = StorageRecordMetadata::try_new(
            ResourceId::new(principal_id.id()).expect("user resource id is positive"),
            now,
            now,
            ResourceRevision::INITIAL,
        )
        .map_err(invalid_contract_value)?;
        let principal =
            StoragePrincipal::builder(metadata, PrincipalKind::Human, name.clone(), scope.id())
                .try_build()
                .map_err(invalid_contract_value)?;
        let user = StorageUser::try_new(
            user_id,
            Some(password_hash),
            proper_name,
            email,
            now,
            now,
            None,
        )
        .map_err(invalid_contract_value)?;
        state.principals.insert(principal_id.id(), principal);
        state.users.insert(
            user_id.id(),
            MemoryUserRecord {
                user: user.clone(),
                identity_scope_id: scope.id(),
                name: name.clone(),
                provider_managed: false,
                external_subject: None,
                last_sync_attempted_at: None,
                last_sync_success_at: None,
            },
        );
        let receipt = state.append_simple_event(
            EntityType::User,
            user_id.id(),
            Some(&name),
            Action::Created,
            &context,
            format!("User '{name}' created"),
        )?;
        Ok(StorageMutationOutcome::committed(user, receipt))
    }

    async fn update_user(
        &self,
        request: StorageUserUpdate,
    ) -> Result<StorageMutationOutcome<StorageUser>, StorageError> {
        let (id, password_hash, proper_name, email, context) = request.into_parts();
        let mut state = self.state.write().await;
        let current =
            state.users.get(&id.id()).cloned().ok_or_else(|| {
                StorageError::not_found(format!("User {} was not found", id.id()))
            })?;
        if password_hash.is_none() && proper_name.is_none() && email.is_none() {
            return Ok(StorageMutationOutcome::unchanged(current.user));
        }
        let parts = current.user.into_parts();
        let now = Utc::now();
        let user = StorageUser::try_new(
            id,
            password_hash.or_else(|| parts.password_hash().map(ToOwned::to_owned)),
            proper_name.or_else(|| parts.proper_name().map(ToOwned::to_owned)),
            email.or_else(|| parts.email().map(ToOwned::to_owned)),
            parts.created_at(),
            now,
            parts.anonymized_at(),
        )
        .map_err(invalid_contract_value)?;
        let record = state.users.get_mut(&id.id()).expect("updated user exists");
        record.user = user.clone();
        let principal = state
            .principals
            .get(&id.id())
            .cloned()
            .ok_or_else(|| StorageError::internal("updated user principal is missing"))?;
        state.principals.insert(
            id.id(),
            advanced_principal(
                &principal,
                principal.name(),
                principal.settings().clone(),
                now,
            )?,
        );
        let receipt = state.append_simple_event(
            EntityType::User,
            id.id(),
            Some(principal.name()),
            Action::Updated,
            &context,
            format!("User '{}' updated", principal.name()),
        )?;
        Ok(StorageMutationOutcome::committed(user, receipt))
    }

    async fn set_user_password(
        &self,
        request: StorageUserPasswordUpdate,
    ) -> Result<StorageMutationOutcome<usize>, StorageError> {
        let (id, password_hash, context) = request.into_parts();
        let outcome = self
            .update_user(StorageUserUpdate::new(
                id,
                Some(password_hash),
                None,
                None,
                context,
            ))
            .await?;
        let now = Utc::now();
        let mut state = self.state.write().await;
        let mut revoked = 0_usize;
        for token in state.tokens.values_mut() {
            if token.principal_id.id() == id.id() && token.revoked_at.is_none() {
                token.revoked_at = Some(now);
                revoked += 1;
            }
        }
        Ok(outcome.map(|_| revoked))
    }

    async fn delete_user(
        &self,
        request: StorageUserDelete,
    ) -> Result<StorageMutationOutcome<usize>, StorageError> {
        let (id, context) = request.into_parts();
        let mut state = self.state.write().await;
        let Some(record) = state.users.remove(&id.id()) else {
            return Ok(StorageMutationOutcome::unchanged(0));
        };
        state.principals.remove(&id.id());
        state
            .memberships
            .retain(|(principal_id, _), _| *principal_id != id.id());
        state
            .tokens
            .retain(|_, token| token.principal_id.id() != id.id());
        let receipt = state.append_simple_event(
            EntityType::User,
            id.id(),
            Some(&record.name),
            Action::Deleted,
            &context,
            format!("User '{}' deleted", record.name),
        )?;
        Ok(StorageMutationOutcome::committed(1, receipt))
    }

    async fn anonymize_user(
        &self,
        request: StorageUserAnonymize,
    ) -> Result<StorageMutationOutcome<()>, StorageError> {
        let (id, context) = request.into_parts();
        let mut state = self.state.write().await;
        let current =
            state.users.get(&id.id()).cloned().ok_or_else(|| {
                StorageError::not_found(format!("User {} was not found", id.id()))
            })?;
        let parts = current.user.into_parts();
        if parts.anonymized_at().is_some() {
            return Ok(StorageMutationOutcome::unchanged(()));
        }
        let now = Utc::now();
        let anonymous_name = format!("anonymized-{}", id.id());
        let user = StorageUser::try_new(id, None, None, None, parts.created_at(), now, Some(now))
            .map_err(invalid_contract_value)?;
        let record = state
            .users
            .get_mut(&id.id())
            .expect("anonymized user exists");
        record.user = user;
        record.name = anonymous_name.clone();
        let principal = state
            .principals
            .get(&id.id())
            .cloned()
            .ok_or_else(|| StorageError::internal("anonymized user principal is missing"))?;
        state.principals.insert(
            id.id(),
            advanced_principal(
                &principal,
                anonymous_name,
                principal.settings().clone(),
                now,
            )?,
        );
        let receipt = state.append_simple_event(
            EntityType::User,
            id.id(),
            None,
            Action::Updated,
            &context,
            format!("User {} anonymized", id.id()),
        )?;
        Ok(StorageMutationOutcome::committed((), receipt))
    }
}

#[async_trait]
impl TokenStorage for MemoryStorage {
    async fn list_retained_tokens(
        &self,
        query: StorageTokenListQuery,
    ) -> Result<StoragePage<StorageTokenMetadata>, StorageError> {
        let (principal_id, options, list_state, observation) = query.into_parts();
        let state = self.state.read().await;
        let rows = state
            .tokens
            .values()
            .filter(|record| record.principal_id == principal_id)
            .map(|record| record.metadata(observation))
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .filter(|metadata| token_state_matches(metadata, list_state))
            .collect();
        page(rows, &options)
    }

    async fn create_token(
        &self,
        request: StorageTokenCreate,
    ) -> Result<StorageMutationOutcome<StorageTokenMetadata>, StorageError> {
        let request = request.into_parts();
        let mut state = self.state.write().await;
        if !state.principals.contains_key(&request.principal_id().id()) {
            return Err(StorageError::not_found(format!(
                "Principal {} was not found",
                request.principal_id().id()
            )));
        }
        if state
            .tokens
            .values()
            .any(|token| token.token_hash == request.token_hash())
        {
            return Err(StorageError::conflict("Token credential already exists"));
        }
        let id = TokenId::new(state.next_token_id).expect("memory token id is positive");
        state.next_token_id += 1;
        let issued = Utc::now();
        let (default_lifetime_hours, maximum_lifetime_hours) = request.policy().into_parts();
        let maximum_expiry = issued
            + chrono::Duration::try_hours(maximum_lifetime_hours)
                .ok_or_else(|| StorageError::invalid_input("Token lifetime is too large"))?;
        let expires_at = request.expires_at().unwrap_or_else(|| {
            issued
                + chrono::Duration::try_hours(default_lifetime_hours)
                    .expect("validated token lifetime fits chrono duration")
        });
        if expires_at > maximum_expiry || expires_at <= issued {
            return Err(StorageError::invalid_input(
                "Token expiry is outside the issuance policy",
            ));
        }
        let record = MemoryTokenRecord {
            id,
            principal_id: request.principal_id(),
            token_hash: request.token_hash().to_string(),
            name: request.name().map(ToOwned::to_owned),
            description: request.description().map(ToOwned::to_owned),
            issued,
            expires_at: Some(expires_at),
            last_used_at: None,
            revoked_at: None,
            scope: request.scope().cloned(),
            revision: ResourceRevision::INITIAL,
        };
        let observation =
            StorageTokenObservation::try_new(issued, issued).map_err(invalid_contract_value)?;
        let metadata = record.metadata(observation)?;
        state.tokens.insert(id.id(), record);
        let receipt = state.append_simple_event(
            EntityType::Token,
            id.id(),
            None,
            Action::Created,
            request.event_context(),
            format!("Token {} created", id.id()),
        )?;
        Ok(StorageMutationOutcome::committed(metadata, receipt))
    }

    async fn renew_token(
        &self,
        request: StorageTokenRenew,
    ) -> Result<StorageMutationOutcome<StorageTokenMetadata>, StorageError> {
        let (source_id, principal_id, token_hash, expires_at, policy, context) =
            request.into_parts();
        let source = self
            .state
            .read()
            .await
            .tokens
            .get(&source_id.id())
            .cloned()
            .ok_or_else(|| {
                StorageError::not_found(format!("Token {} was not found", source_id.id()))
            })?;
        if source.principal_id != principal_id {
            return Err(StorageError::not_found(format!(
                "Token {} was not found for principal {}",
                source_id.id(),
                principal_id.id()
            )));
        }
        self.create_token(
            StorageTokenCreate::new(principal_id, token_hash, policy, context)
                .name(source.name)
                .description(source.description)
                .expires_at(expires_at)
                .scope(source.scope),
        )
        .await
    }

    async fn get_token_metadata(
        &self,
        principal_id: PrincipalId,
        token_id: TokenId,
        observation: StorageTokenObservation,
    ) -> Result<StorageTokenMetadata, StorageError> {
        let state = self.state.read().await;
        let token = state.tokens.get(&token_id.id()).ok_or_else(|| {
            StorageError::not_found(format!("Token {} was not found", token_id.id()))
        })?;
        if token.principal_id != principal_id {
            return Err(StorageError::not_found(format!(
                "Token {} was not found for principal {}",
                token_id.id(),
                principal_id.id()
            )));
        }
        token.metadata(observation)
    }

    async fn load_token_metadata_by_ids(
        &self,
        token_ids: Vec<TokenId>,
        observation: StorageTokenObservation,
    ) -> Result<Vec<StorageTokenMetadata>, StorageError> {
        let state = self.state.read().await;
        token_ids
            .into_iter()
            .map(|id| {
                state
                    .tokens
                    .get(&id.id())
                    .ok_or_else(|| {
                        StorageError::not_found(format!("Token {} was not found", id.id()))
                    })?
                    .metadata(observation)
            })
            .collect()
    }

    async fn revoke_token(
        &self,
        request: StorageTokenRevoke,
    ) -> Result<StorageMutationOutcome<usize>, StorageError> {
        let (token_id, principal_id, context) = request.into_parts();
        let mut state = self.state.write().await;
        let token = state.tokens.get_mut(&token_id.id()).ok_or_else(|| {
            StorageError::not_found(format!("Token {} was not found", token_id.id()))
        })?;
        if token.principal_id != principal_id {
            return Err(StorageError::not_found(format!(
                "Token {} was not found for principal {}",
                token_id.id(),
                principal_id.id()
            )));
        }
        if token.revoked_at.is_some() {
            return Ok(StorageMutationOutcome::unchanged(0));
        }
        token.revoked_at = Some(Utc::now());
        token.revision = token
            .revision
            .checked_advance()
            .map_err(|error| StorageError::internal(error.to_string()))?;
        let receipt = state.append_simple_event(
            EntityType::Token,
            token_id.id(),
            None,
            Action::Revoked,
            &context,
            format!("Token {} revoked", token_id.id()),
        )?;
        Ok(StorageMutationOutcome::committed(1, receipt))
    }

    async fn revoke_token_by_hash(
        &self,
        request: StorageTokenHashRevoke,
    ) -> Result<StorageMutationOutcome<usize>, StorageError> {
        let (principal_id, token_hash, context) = request.into_parts();
        let token_id = self
            .state
            .read()
            .await
            .tokens
            .values()
            .find(|token| {
                token.token_hash == token_hash
                    && principal_id.is_none_or(|id| token.principal_id == id)
            })
            .map(|token| token.id);
        let Some(token_id) = token_id else {
            return Ok(StorageMutationOutcome::unchanged(0));
        };
        let owner = self
            .state
            .read()
            .await
            .tokens
            .get(&token_id.id())
            .expect("token selected by id exists")
            .principal_id;
        self.revoke_token(StorageTokenRevoke::new(token_id, owner, context))
            .await
    }

    async fn revoke_all_principal_tokens(
        &self,
        request: StoragePrincipalTokensRevoke,
    ) -> Result<StorageMutationOutcome<usize>, StorageError> {
        let (principal_id, context) = request.into_parts();
        let mut state = self.state.write().await;
        let now = Utc::now();
        let mut revoked = Vec::new();
        for token in state.tokens.values_mut() {
            if token.principal_id == principal_id && token.revoked_at.is_none() {
                token.revoked_at = Some(now);
                token.revision = token
                    .revision
                    .checked_advance()
                    .map_err(|error| StorageError::internal(error.to_string()))?;
                revoked.push(token.id);
            }
        }
        if revoked.is_empty() {
            return Ok(StorageMutationOutcome::unchanged(0));
        }
        let receipts = revoked
            .into_iter()
            .map(|token_id| {
                state.append_simple_event(
                    EntityType::Token,
                    token_id.id(),
                    None,
                    Action::Revoked,
                    &context,
                    format!("Token {} revoked", token_id.id()),
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        let count = receipts.len();
        let audits = StorageAuditReceipts::try_from_vec(receipts)
            .map_err(|error| StorageError::internal(error.to_string()))?;
        Ok(StorageMutationOutcome::committed_with_audits(count, audits))
    }
}

#[async_trait]
impl AuthorizationDataStorage for MemoryStorage {
    async fn get_authorization_principal(
        &self,
        principal_id: PrincipalId,
    ) -> Result<StorageAuthorizationPrincipal, StorageError> {
        let state = self.state.read().await;
        if !state.principals.contains_key(&principal_id.id()) {
            return Err(StorageError::not_found(format!(
                "Principal {} was not found",
                principal_id.id()
            )));
        }
        Ok(StorageAuthorizationPrincipal::new(
            principal_id,
            principal_group_ids(&state, principal_id),
        ))
    }

    async fn is_authorization_principal_group_member(
        &self,
        query: StorageAuthorizationGroupMembershipQuery,
    ) -> Result<bool, StorageError> {
        let state = self.state.read().await;
        Ok(state.groups.values().any(|group| {
            group.name() == query.group_name()
                && state
                    .identity_scopes
                    .get(&group.identity_scope_id().id())
                    .is_some_and(|scope| scope.name() == query.identity_scope())
                && state
                    .memberships
                    .contains_key(&(query.principal_id().id(), group.id().id()))
        }))
    }

    async fn list_authorization_classes(
        &self,
        query: StorageAuthorizationResourceIds,
    ) -> Result<Vec<StorageAuthorizationClassResource>, StorageError> {
        let state = self.state.read().await;
        Ok(query
            .ids()
            .iter()
            .filter_map(|id| state.classes.get(&id.id()))
            .map(|class| StorageAuthorizationClassResource::new(class.id(), class.collection_id()))
            .collect())
    }

    async fn list_authorization_objects(
        &self,
        query: StorageAuthorizationResourceIds,
    ) -> Result<Vec<StorageAuthorizationObjectResource>, StorageError> {
        let state = self.state.read().await;
        Ok(query
            .ids()
            .iter()
            .filter_map(|id| state.objects.get(&id.id()))
            .map(|object| {
                StorageAuthorizationObjectResource::new(
                    object.id(),
                    object.collection_id(),
                    object.class_id(),
                    object.name(),
                )
            })
            .collect())
    }

    async fn authorize_local_collection(
        &self,
        query: StorageAuthorizationCollectionAccessQuery,
    ) -> Result<bool, StorageError> {
        let state = self.state.read().await;
        Ok(principal_has_collection_permissions(
            &state,
            query.principal_id(),
            query.collection_id(),
            query.permissions(),
        ))
    }

    async fn authorize_local_collections(
        &self,
        query: StorageAuthorizationCollectionsAccessQuery,
    ) -> Result<bool, StorageError> {
        let state = self.state.read().await;
        Ok(query.collection_ids().iter().all(|collection_id| {
            principal_has_collection_permissions(
                &state,
                query.principal_id(),
                *collection_id,
                query.permissions(),
            )
        }))
    }

    async fn list_local_authorized_collections(
        &self,
        query: StorageAuthorizationCollectionsQuery,
    ) -> Result<Vec<StorageAuthorizationCollection>, StorageError> {
        let state = self.state.read().await;
        state
            .collections
            .values()
            .filter(|collection| {
                principal_has_collection_permissions(
                    &state,
                    query.principal_id(),
                    collection.id(),
                    query.permissions(),
                )
            })
            .map(authorization_collection)
            .collect()
    }

    async fn load_authorization_collection_candidates(
        &self,
        query: StorageAuthorizationCollectionCandidateQuery,
    ) -> Result<StorageCandidatePage<StorageAuthorizationCollection>, StorageError> {
        let state = self.state.read().await;
        let limit = query.page_limit();
        let mut rows = state
            .collections
            .values()
            .filter(|collection| query.after_id().is_none_or(|id| collection.id() > id))
            .take(limit.get().saturating_add(1))
            .map(authorization_collection)
            .collect::<Result<Vec<_>, _>>()?;
        let has_more = rows.len() > limit.get();
        rows.truncate(limit.get());
        StorageCandidatePage::try_new(rows, has_more, limit).map_err(invalid_contract_value)
    }

    async fn load_authorization_group_candidates(
        &self,
        query: StorageAuthorizationGroupCandidateQuery,
    ) -> Result<StorageCandidatePage<StorageAuthorizationGroup>, StorageError> {
        let state = self.state.read().await;
        let limit = query.page_limit();
        let mut rows = state
            .groups
            .values()
            .filter(|group| {
                resource_filters_match(
                    query.options(),
                    group.id().id(),
                    group.name(),
                    group.description(),
                )
            })
            .take(limit.get().saturating_add(1))
            .map(authorization_group)
            .collect::<Result<Vec<_>, _>>()?;
        let has_more = rows.len() > limit.get();
        rows.truncate(limit.get());
        StorageCandidatePage::try_new(rows, has_more, limit).map_err(invalid_contract_value)
    }

    async fn get_authorization_policy_snapshot(
        &self,
    ) -> Result<Vec<StorageAuthorizationPolicySnapshotRow>, StorageError> {
        let state = self.state.read().await;
        state
            .authorization_grants
            .values()
            .map(|grant| authorization_policy_row(&state, grant))
            .collect()
    }

    async fn list_local_collection_grants(
        &self,
        query: StorageAuthorizationCollectionGrantListQuery,
    ) -> Result<StoragePage<StorageAuthorizationGroupGrant>, StorageError> {
        let state = self.state.read().await;
        let rows = state
            .authorization_grants
            .values()
            .filter(|grant| {
                grant.collection_id() == query.collection_id()
                    && permissions_include(grant.permissions(), query.required_permissions())
            })
            .map(|grant| authorization_group_grant(&state, grant))
            .collect::<Result<Vec<_>, _>>()?;
        page(rows, query.query_options())
    }

    async fn get_local_collection_grant(
        &self,
        key: StorageAuthorizationGrantKey,
    ) -> Result<Option<StorageAuthorizationGrant>, StorageError> {
        Ok(self
            .state
            .read()
            .await
            .authorization_grants
            .get(&(key.collection_id().id(), key.group_id().id()))
            .cloned())
    }

    async fn get_local_collection_permission_set(
        &self,
        query: StorageAuthorizationPermissionSetQuery,
    ) -> Result<StorageAuthorizationPermissionSet, StorageError> {
        let state = self.state.read().await;
        let collection = state
            .collections
            .get(&query.collection_id().id())
            .ok_or_else(|| {
                StorageError::not_found(format!(
                    "Collection {} was not found",
                    query.collection_id().id()
                ))
            })?;
        let grants = state
            .authorization_grants
            .values()
            .filter(|grant| {
                grant.collection_id() == query.collection_id()
                    && query.group_id().is_none_or(|id| grant.group_id() == id)
            })
            .cloned()
            .collect();
        StorageAuthorizationPermissionSet::try_new(collection.id(), collection.revision(), grants)
            .map_err(invalid_contract_value)
    }

    async fn apply_local_collection_grant(
        &self,
        mutation: StorageAuthorizationGrantMutation,
    ) -> Result<StorageMutationOutcome<StorageAuthorizationGrant>, StorageError> {
        let key = mutation.key();
        let mut state = self.state.write().await;
        if !state.collections.contains_key(&key.collection_id().id()) {
            return Err(StorageError::not_found(format!(
                "Collection {} was not found",
                key.collection_id().id()
            )));
        }
        if !state.groups.contains_key(&key.group_id().id()) {
            return Err(StorageError::not_found(format!(
                "Group {} was not found",
                key.group_id().id()
            )));
        }
        let map_key = (key.collection_id().id(), key.group_id().id());
        let current = state.authorization_grants.get(&map_key).cloned();
        let mut permissions = if mutation.replace_existing() {
            Vec::new()
        } else {
            current
                .as_ref()
                .map(|grant| grant.permissions().to_vec())
                .unwrap_or_default()
        };
        permissions.extend_from_slice(mutation.permissions());
        permissions.sort_unstable();
        permissions.dedup();
        if current
            .as_ref()
            .is_some_and(|grant| grant.permissions() == permissions)
        {
            return Ok(StorageMutationOutcome::unchanged(
                current.expect("grant exists"),
            ));
        }
        let now = Utc::now();
        let (id, created_at) = current.as_ref().map_or_else(
            || {
                let id = AuthorizationGrantId::new(state.next_authorization_grant_id)
                    .expect("memory authorization grant ids are positive");
                state.next_authorization_grant_id += 1;
                (id, now)
            },
            |grant| (grant.id(), grant.created_at()),
        );
        let grant = StorageAuthorizationGrant::try_new(
            id,
            key.collection_id(),
            key.group_id(),
            permissions,
            created_at,
            now,
        )
        .map_err(invalid_contract_value)?;
        state.authorization_grants.insert(map_key, grant.clone());
        let receipt = state.append_simple_event(
            EntityType::Permission,
            id.id(),
            None,
            Action::Granted,
            mutation.event_context(),
            format!(
                "Collection {} permissions granted to group {}",
                key.collection_id().id(),
                key.group_id().id()
            ),
        )?;
        Ok(StorageMutationOutcome::committed(grant, receipt))
    }

    async fn revoke_local_collection_grant(
        &self,
        mutation: StorageAuthorizationGrantMutation,
    ) -> Result<StorageMutationOutcome<StorageAuthorizationGrant>, StorageError> {
        let key = mutation.key();
        let mut state = self.state.write().await;
        let map_key = (key.collection_id().id(), key.group_id().id());
        let Some(current) = state.authorization_grants.get(&map_key).cloned() else {
            return Err(StorageError::not_found(format!(
                "Collection {} has no grant for group {}",
                key.collection_id().id(),
                key.group_id().id()
            )));
        };
        let permissions = current
            .permissions()
            .iter()
            .copied()
            .filter(|permission| !mutation.permissions().contains(permission))
            .collect::<Vec<_>>();
        if permissions == current.permissions() {
            return Ok(StorageMutationOutcome::unchanged(current));
        }
        let grant = StorageAuthorizationGrant::try_new(
            current.id(),
            current.collection_id(),
            current.group_id(),
            permissions,
            current.created_at(),
            Utc::now(),
        )
        .map_err(invalid_contract_value)?;
        state.authorization_grants.insert(map_key, grant.clone());
        let receipt = state.append_simple_event(
            EntityType::Permission,
            grant.id().id(),
            None,
            Action::Revoked,
            mutation.event_context(),
            format!(
                "Collection {} permissions revoked from group {}",
                key.collection_id().id(),
                key.group_id().id()
            ),
        )?;
        Ok(StorageMutationOutcome::committed(grant, receipt))
    }

    async fn revoke_all_local_collection_grants(
        &self,
        request: StorageAuthorizationGrantDelete,
    ) -> Result<StorageMutationOutcome<()>, StorageError> {
        let key = request.key();
        let mut state = self.state.write().await;
        let Some(grant) = state
            .authorization_grants
            .remove(&(key.collection_id().id(), key.group_id().id()))
        else {
            return Ok(StorageMutationOutcome::unchanged(()));
        };
        let receipt = state.append_simple_event(
            EntityType::Permission,
            grant.id().id(),
            None,
            Action::Revoked,
            request.event_context(),
            format!(
                "All collection {} permissions revoked from group {}",
                key.collection_id().id(),
                key.group_id().id()
            ),
        )?;
        Ok(StorageMutationOutcome::committed((), receipt))
    }
}

#[async_trait]
impl CatalogStorage for MemoryStorage {
    async fn list_collections(
        &self,
        query: StorageCatalogListQuery,
    ) -> Result<StoragePage<StorageCollection>, StorageError> {
        let (options, visibility) = query.into_parts();
        let state = self.state.read().await;
        let rows = state
            .collections
            .values()
            .filter(|collection| {
                visibility
                    .resources()
                    .is_none_or(|scope| scope.collection_ids().contains(&collection.id()))
                    && resource_filters_match(
                        &options,
                        collection.id().id(),
                        collection.name(),
                        collection.description(),
                    )
            })
            .cloned()
            .collect();
        page(rows, &options)
    }

    async fn list_classes(
        &self,
        query: StorageCatalogListQuery,
    ) -> Result<StoragePage<StorageClassWithCollection>, StorageError> {
        let (options, visibility) = query.into_parts();
        let state = self.state.read().await;
        let rows = state
            .classes
            .values()
            .filter(|class| {
                visibility.resources().is_none_or(|scope| {
                    scope.class_ids().contains(&class.id())
                        || scope.collection_ids().contains(&class.collection_id())
                }) && resource_filters_match(
                    &options,
                    class.id().id(),
                    class.name(),
                    class.description(),
                )
            })
            .map(|class| class_with_collection(&state, class))
            .collect::<Result<Vec<_>, StorageError>>()?;
        page(rows, &options)
    }

    async fn list_objects(
        &self,
        query: StorageCatalogListQuery,
    ) -> Result<StoragePage<StorageObject>, StorageError> {
        let (options, visibility) = query.into_parts();
        let state = self.state.read().await;
        let rows = state
            .objects
            .values()
            .filter(|object| {
                visibility.resources().is_none_or(|scope| {
                    scope.object_ids().contains(&object.id())
                        || scope.class_ids().contains(&object.class_id())
                        || scope.collection_ids().contains(&object.collection_id())
                }) && resource_filters_match(
                    &options,
                    object.id().id(),
                    object.name(),
                    object.description(),
                )
            })
            .cloned()
            .collect();
        page(rows, &options)
    }
}

#[async_trait]
impl ComputedFieldStorage for MemoryStorage {
    async fn get_computed_field_state(
        &self,
        class_id: ClassId,
    ) -> Result<StorageClassComputationState, StorageError> {
        let state = self.state.read().await;
        if let Some(computation_state) = state.computation_states.get(&class_id.id()) {
            return Ok(computation_state.clone());
        }
        let class = state.classes.get(&class_id.id()).ok_or_else(|| {
            StorageError::not_found(format!("Class {} was not found", class_id.id()))
        })?;
        ready_computation_state(class_id, 0, class.created_at())
    }

    async fn list_shared_computed_fields(
        &self,
        class_id: ClassId,
    ) -> Result<Vec<StorageComputedFieldDefinition>, StorageError> {
        Ok(self
            .state
            .read()
            .await
            .computed_fields
            .values()
            .filter(|definition| {
                definition.class_id() == class_id
                    && definition.visibility() == StorageComputedFieldVisibility::Shared
            })
            .cloned()
            .collect())
    }

    async fn list_personal_computed_fields(
        &self,
        query: StoragePersonalComputedFieldListQuery,
    ) -> Result<StoragePage<StorageComputedFieldDefinition>, StorageError> {
        let (owner_id, class_id, options) = query.into_parts();
        let rows = self
            .state
            .read()
            .await
            .computed_fields
            .values()
            .filter(|definition| {
                definition.visibility() == StorageComputedFieldVisibility::Personal { owner_id }
                    && class_id.is_none_or(|id| definition.class_id() == id)
            })
            .cloned()
            .collect();
        page(rows, &options)
    }

    async fn get_computed_field(
        &self,
        definition_id: ComputedFieldDefinitionId,
    ) -> Result<StorageComputedFieldDefinition, StorageError> {
        self.state
            .read()
            .await
            .computed_fields
            .get(&definition_id.id())
            .cloned()
            .ok_or_else(|| {
                StorageError::not_found(format!(
                    "Computed field definition {} was not found",
                    definition_id.id()
                ))
            })
    }

    async fn create_shared_computed_field(
        &self,
        request: StorageSharedComputedFieldCreate,
    ) -> Result<StorageMutationOutcome<StorageComputedFieldMutation>, StorageError> {
        let (class_id, collection_id, actor_id, input, context) = request.into_parts();
        let mut state = self.state.write().await;
        let class = state.classes.get(&class_id.id()).cloned().ok_or_else(|| {
            StorageError::not_found(format!("Class {} was not found", class_id.id()))
        })?;
        if class.collection_id() != collection_id {
            return Err(StorageError::not_found(
                "Class was not found in the authorized collection",
            ));
        }
        if state.computed_fields.values().any(|definition| {
            definition.class_id() == class_id
                && definition.visibility() == StorageComputedFieldVisibility::Shared
                && definition.key() == input.key()
        }) {
            return Err(StorageError::conflict(format!(
                "Shared computed field '{}' already exists",
                input.key()
            )));
        }
        let id = state.next_computed_field_id;
        state.next_computed_field_id += 1;
        let now = Utc::now();
        let metadata = StorageRecordMetadata::try_new(
            ResourceId::new(id).expect("memory computed field id is positive"),
            now,
            now,
            ResourceRevision::INITIAL,
        )
        .map_err(invalid_contract_value)?;
        let definition = StorageComputedFieldDefinition::new(
            metadata,
            class_id,
            StorageComputedFieldVisibility::Shared,
            StorageComputedFieldDefinitionContent::new(input, 1),
            StorageComputedFieldProvenance::new(Some(actor_id), Some(actor_id)),
        );
        let previous_revision = state
            .computation_states
            .get(&class_id.id())
            .map_or(0, |value| value.evaluation_revision().get());
        let computation_state = ready_computation_state(class_id, previous_revision + 1, now)?;
        state.computed_fields.insert(id, definition.clone());
        state
            .computation_states
            .insert(class_id.id(), computation_state.clone());
        let receipt = state.append_simple_event(
            EntityType::ComputedFieldDefinition,
            id,
            Some(definition.key()),
            Action::Created,
            &context,
            format!("Computed field '{}' created", definition.key()),
        )?;
        Ok(StorageMutationOutcome::committed(
            StorageComputedFieldMutation::new(definition, computation_state),
            receipt,
        ))
    }

    async fn update_shared_computed_field(
        &self,
        request: StorageSharedComputedFieldUpdate,
    ) -> Result<StorageMutationOutcome<StorageComputedFieldMutation>, StorageError> {
        let (class_id, collection_id, definition_id, actor_id, patch, context) =
            request.into_parts();
        let mut state = self.state.write().await;
        let class = state.classes.get(&class_id.id()).ok_or_else(|| {
            StorageError::not_found(format!("Class {} was not found", class_id.id()))
        })?;
        if class.collection_id() != collection_id {
            return Err(StorageError::not_found(
                "Class was not found in the authorized collection",
            ));
        }
        let current = state
            .computed_fields
            .get(&definition_id.id())
            .cloned()
            .ok_or_else(|| StorageError::not_found("Computed field definition was not found"))?;
        if current.class_id() != class_id
            || current.visibility() != StorageComputedFieldVisibility::Shared
        {
            return Err(StorageError::not_found(
                "Shared computed field definition was not found",
            ));
        }
        let definition = updated_computed_field(&current, &patch, actor_id)?;
        let previous = state
            .computation_states
            .get(&class_id.id())
            .cloned()
            .unwrap_or(ready_computation_state(class_id, 0, class.created_at())?);
        let computation_state = ready_computation_state(
            class_id,
            previous.evaluation_revision().get() + 1,
            previous.created_at(),
        )?;
        state
            .computed_fields
            .insert(definition_id.id(), definition.clone());
        state
            .computation_states
            .insert(class_id.id(), computation_state.clone());
        let receipt = state.append_simple_event(
            EntityType::ComputedFieldDefinition,
            definition_id.id(),
            Some(definition.key()),
            Action::Updated,
            &context,
            format!("Computed field '{}' updated", definition.key()),
        )?;
        Ok(StorageMutationOutcome::committed(
            StorageComputedFieldMutation::new(definition, computation_state),
            receipt,
        ))
    }

    async fn delete_shared_computed_field(
        &self,
        request: StorageSharedComputedFieldDelete,
    ) -> Result<StorageMutationOutcome<StorageClassComputationState>, StorageError> {
        let (class_id, collection_id, definition_id, _, context) = request.into_parts();
        let mut state = self.state.write().await;
        let class = state.classes.get(&class_id.id()).cloned().ok_or_else(|| {
            StorageError::not_found(format!("Class {} was not found", class_id.id()))
        })?;
        if class.collection_id() != collection_id {
            return Err(StorageError::not_found(
                "Class was not found in the authorized collection",
            ));
        }
        let definition = state
            .computed_fields
            .remove(&definition_id.id())
            .ok_or_else(|| StorageError::not_found("Computed field definition was not found"))?;
        if definition.class_id() != class_id
            || definition.visibility() != StorageComputedFieldVisibility::Shared
        {
            state.computed_fields.insert(definition_id.id(), definition);
            return Err(StorageError::not_found(
                "Shared computed field definition was not found",
            ));
        }
        let previous = state
            .computation_states
            .get(&class_id.id())
            .cloned()
            .unwrap_or(ready_computation_state(class_id, 0, class.created_at())?);
        let computation_state = ready_computation_state(
            class_id,
            previous.evaluation_revision().get() + 1,
            previous.created_at(),
        )?;
        state
            .computation_states
            .insert(class_id.id(), computation_state.clone());
        let receipt = state.append_simple_event(
            EntityType::ComputedFieldDefinition,
            definition_id.id(),
            Some(definition.key()),
            Action::Deleted,
            &context,
            format!("Computed field '{}' deleted", definition.key()),
        )?;
        Ok(StorageMutationOutcome::committed(
            computation_state,
            receipt,
        ))
    }

    async fn create_personal_computed_field(
        &self,
        request: StoragePersonalComputedFieldCreate,
    ) -> Result<StorageMutationOutcome<StorageComputedFieldDefinition>, StorageError> {
        let (class_id, owner_id, input, context) = request.into_parts();
        let mut state = self.state.write().await;
        if !state.classes.contains_key(&class_id.id()) {
            return Err(StorageError::not_found(format!(
                "Class {} was not found",
                class_id.id()
            )));
        }
        if !state.principals.contains_key(&owner_id.id()) {
            return Err(StorageError::not_found(format!(
                "Principal {} was not found",
                owner_id.id()
            )));
        }
        if state.computed_fields.values().any(|definition| {
            definition.class_id() == class_id
                && definition.visibility() == StorageComputedFieldVisibility::Personal { owner_id }
                && definition.key() == input.key()
        }) {
            return Err(StorageError::conflict(format!(
                "Personal computed field '{}' already exists",
                input.key()
            )));
        }
        let id = state.next_computed_field_id;
        state.next_computed_field_id += 1;
        let now = Utc::now();
        let metadata = StorageRecordMetadata::try_new(
            ResourceId::new(id).expect("memory computed field id is positive"),
            now,
            now,
            ResourceRevision::INITIAL,
        )
        .map_err(invalid_contract_value)?;
        let definition = StorageComputedFieldDefinition::new(
            metadata,
            class_id,
            StorageComputedFieldVisibility::Personal { owner_id },
            StorageComputedFieldDefinitionContent::new(input, 1),
            StorageComputedFieldProvenance::new(Some(owner_id), Some(owner_id)),
        );
        state.computed_fields.insert(id, definition.clone());
        let receipt = state.append_simple_event(
            EntityType::ComputedFieldDefinition,
            id,
            Some(definition.key()),
            Action::Created,
            &context,
            format!("Computed field '{}' created", definition.key()),
        )?;
        Ok(StorageMutationOutcome::committed(definition, receipt))
    }

    async fn update_personal_computed_field(
        &self,
        request: StoragePersonalComputedFieldUpdate,
    ) -> Result<StorageMutationOutcome<StorageComputedFieldDefinition>, StorageError> {
        let (owner_id, definition_id, patch, context) = request.into_parts();
        let mut state = self.state.write().await;
        let current = state
            .computed_fields
            .get(&definition_id.id())
            .cloned()
            .ok_or_else(|| StorageError::not_found("Computed field definition was not found"))?;
        if current.visibility() != (StorageComputedFieldVisibility::Personal { owner_id }) {
            return Err(StorageError::not_found(
                "Personal computed field definition was not found",
            ));
        }
        let definition = updated_computed_field(&current, &patch, owner_id)?;
        state
            .computed_fields
            .insert(definition_id.id(), definition.clone());
        let receipt = state.append_simple_event(
            EntityType::ComputedFieldDefinition,
            definition_id.id(),
            Some(definition.key()),
            Action::Updated,
            &context,
            format!("Computed field '{}' updated", definition.key()),
        )?;
        Ok(StorageMutationOutcome::committed(definition, receipt))
    }

    async fn delete_personal_computed_field(
        &self,
        request: StoragePersonalComputedFieldDelete,
    ) -> Result<StorageMutationOutcome<()>, StorageError> {
        let (owner_id, definition_id, context) = request.into_parts();
        let mut state = self.state.write().await;
        let Some(definition) = state.computed_fields.get(&definition_id.id()).cloned() else {
            return Ok(StorageMutationOutcome::unchanged(()));
        };
        if definition.visibility() != (StorageComputedFieldVisibility::Personal { owner_id }) {
            return Err(StorageError::not_found(
                "Personal computed field definition was not found",
            ));
        }
        state.computed_fields.remove(&definition_id.id());
        let receipt = state.append_simple_event(
            EntityType::ComputedFieldDefinition,
            definition_id.id(),
            Some(definition.key()),
            Action::Deleted,
            &context,
            format!("Computed field '{}' deleted", definition.key()),
        )?;
        Ok(StorageMutationOutcome::committed((), receipt))
    }

    async fn request_computed_field_rebuild(
        &self,
        request: StorageComputedFieldRebuildRequest,
    ) -> Result<StorageClassComputationState, StorageError> {
        let (class_id, collection_id, actor_id) = request.into_parts();
        let actor_id = actor_id.unwrap_or_else(|| PrincipalId::new(1).expect("admin id is valid"));
        {
            let state = self.state.read().await;
            let class = state.classes.get(&class_id.id()).ok_or_else(|| {
                StorageError::not_found(format!("Class {} was not found", class_id.id()))
            })?;
            if class.collection_id() != collection_id {
                return Err(StorageError::not_found(
                    "Class was not found in the authorized collection",
                ));
            }
        }
        let task = self
            .create_task(
                StorageTaskCreateRequest::builder(
                    StorageTaskKind::Reindex,
                    actor_id,
                    serde_json::json!({"class_id": class_id.id()}),
                    0,
                )
                .scope_snapshot(StorageTaskScopeSnapshot::unscoped())
                .try_build(100)?,
            )
            .await?;
        let mut state = self.state.write().await;
        let class = state.classes.get(&class_id.id()).cloned().ok_or_else(|| {
            StorageError::not_found(format!("Class {} was not found", class_id.id()))
        })?;
        let previous = state
            .computation_states
            .get(&class_id.id())
            .cloned()
            .unwrap_or(ready_computation_state(class_id, 0, class.created_at())?);
        let computation_state = StorageClassComputationState::builder(
            class_id,
            previous.evaluation_revision(),
            StorageComputationRebuildStatus::Rebuilding,
            previous.created_at(),
            Utc::now(),
        )
        .active_task(Some(task.id()))
        .try_build()
        .map_err(invalid_contract_value)?;
        state
            .computed_rebuild_tasks
            .insert(task.id().id(), class_id);
        state
            .computation_states
            .insert(class_id.id(), computation_state.clone());
        Ok(computation_state)
    }

    async fn execute_computed_field_rebuild(
        &self,
        lease: StorageTaskLease,
    ) -> Result<StorageTask, StorageError> {
        let mut state = self.state.write().await;
        let class_id = state
            .computed_rebuild_tasks
            .get(&lease.task_id().id())
            .copied()
            .ok_or_else(|| StorageError::not_found("Computed-field rebuild task was not found"))?;
        let task = state
            .tasks
            .get_mut(&lease.task_id().id())
            .ok_or_else(|| StorageError::not_found("Computed-field rebuild task was not found"))?;
        if task.kind != StorageTaskKind::Reindex || !task.lease_matches(&lease) {
            return Err(invalid_task_lease());
        }
        let now = Utc::now();
        task.status = StorageTaskStatus::Succeeded;
        task.summary = Some("Computed-field rebuild completed".to_string());
        task.updated_at = now;
        task.finished_at = Some(now);
        task.lease_expires_at = None;
        task.claim_token = None;
        task.request_payload = None;
        task.request_redacted_at = Some(now);
        let completed = task.projection()?;
        let previous = state
            .computation_states
            .get(&class_id.id())
            .cloned()
            .ok_or_else(|| StorageError::internal("computed-field rebuild state is missing"))?;
        let computation_state = ready_computation_state(
            class_id,
            previous.evaluation_revision().get(),
            previous.created_at(),
        )?;
        state
            .computation_states
            .insert(class_id.id(), computation_state);
        state.computed_rebuild_tasks.remove(&lease.task_id().id());
        state.append_task_event_record(
            lease.task_id(),
            StorageTaskEventInput::new("succeeded", "Computed-field rebuild completed"),
        )?;
        Ok(completed)
    }
}

#[async_trait]
impl ComputedObjectStorage for MemoryStorage {
    async fn list_computed_objects(
        &self,
        query: StorageComputedObjectListQuery,
    ) -> Result<StorageComputedObjectPage, StorageError> {
        let (class_id, personal_owner_id, options, visibility, projection) = query.into_parts();
        let (requested, _, effective_page_limit) = options.into_parts();
        let state = self.state.read().await;
        let mut rows = state
            .objects
            .values()
            .filter(|object| object.class_id() == class_id)
            .filter(|object| match &visibility {
                StorageComputedObjectVisibility::Storage(visibility) => visibility
                    .resources()
                    .is_none_or(|scope| scope.object_ids().contains(&object.id())),
                StorageComputedObjectVisibility::AuthorizedObjectIds { object_ids, .. } => {
                    object_ids.contains(&object.id())
                }
            })
            .filter(|object| {
                requested.filters().as_slice().iter().all(|filter| {
                    if let Some(computed) = filter.field.computed_query() {
                        let visibility = match computed.scope() {
                            ComputedFieldScope::Shared => StorageComputedFieldVisibility::Shared,
                            ComputedFieldScope::Personal => {
                                let Some(owner_id) = personal_owner_id else {
                                    return false;
                                };
                                StorageComputedFieldVisibility::Personal { owner_id }
                            }
                        };
                        let value = state
                            .computed_fields
                            .values()
                            .find(|definition| {
                                definition.class_id() == object.class_id()
                                    && definition.visibility() == visibility
                                    && definition.key() == computed.key()
                            })
                            .map(|definition| evaluate_computed_definition(definition, object));
                        let actual = value.as_ref().map_or_else(
                            || "null".to_string(),
                            |value| {
                                value
                                    .as_str()
                                    .map(ToOwned::to_owned)
                                    .unwrap_or_else(|| value.to_string())
                            },
                        );
                        return string_filter_matches(&actual, &filter.operator, &filter.value);
                    }
                    let actual = match filter.field {
                        FilterField::Id => object.id().id().to_string(),
                        FilterField::Name => object.name().to_string(),
                        FilterField::ClassId => object.class_id().id().to_string(),
                        FilterField::CollectionId => object.collection_id().id().to_string(),
                        _ => return true,
                    };
                    string_filter_matches(&actual, &filter.operator, &filter.value)
                })
            })
            .cloned()
            .collect::<Vec<_>>();
        rows.sort_by_key(StorageObject::id);
        let total = requested
            .include_total()
            .then(|| i64::try_from(rows.len()).unwrap_or(i64::MAX));
        rows.truncate(effective_page_limit);
        let computed = if projection == StorageComputedObjectProjection::None {
            Vec::new()
        } else {
            rows.iter()
                .cloned()
                .map(|object| computed_object(&state, object, personal_owner_id))
                .collect::<Result<Vec<_>, _>>()?
        };
        StorageComputedObjectPage::try_new(rows, total, computed, requested)
            .map_err(invalid_contract_value)
    }

    async fn enrich_objects_with_computed(
        &self,
        query: StorageComputedObjectEnrichmentQuery,
    ) -> Result<Vec<StorageComputedObject>, StorageError> {
        let (objects, personal_owner_id) = query.into_parts();
        let state = self.state.read().await;
        objects
            .into_iter()
            .map(|object| computed_object(&state, object, personal_owner_id))
            .collect()
    }
}

#[async_trait]
impl ObjectAggregateStorage for MemoryStorage {
    async fn aggregate_objects(
        &self,
        query: StorageObjectAggregateQuery,
        authorization: StorageObjectAggregateAuthorization<'_>,
    ) -> Result<StorageObjectAggregatePage, StorageError> {
        let (collection_name, mut objects) = {
            let state = self.state.read().await;
            let collection = state
                .collections
                .get(&query.target().collection_id().id())
                .ok_or_else(|| StorageError::not_found("Aggregate collection was not found"))?;
            let objects = state
                .objects
                .values()
                .filter(|object| object.class_id() == query.target().class_id())
                .filter(|object| {
                    query.options().filters().as_slice().iter().all(|filter| {
                        let actual = match filter.field {
                            FilterField::Id => object.id().id().to_string(),
                            FilterField::Name => object.name().to_string(),
                            FilterField::ClassId => object.class_id().id().to_string(),
                            FilterField::CollectionId => object.collection_id().id().to_string(),
                            _ => return true,
                        };
                        string_filter_matches(&actual, &filter.operator, &filter.value)
                    })
                })
                .cloned()
                .collect::<Vec<_>>();
            (collection.name().to_string(), objects)
        };
        if let StorageObjectAggregateAuthorization::Delegated(authorizer) = authorization {
            let target_allowed = authorizer
                .authorize_target(
                    StorageObjectAggregateAuthorizationTarget::new(
                        query.target().class_id(),
                        query.target().class_name().to_string(),
                        query.target().collection_id(),
                        collection_name,
                    ),
                    query.required_permissions().to_vec(),
                )
                .await?;
            if !target_allowed {
                return StorageObjectAggregatePage::try_new(
                    Vec::new(),
                    query.options().include_total().then_some(0),
                    None,
                )
                .map_err(invalid_contract_value);
            }
            let decisions = authorizer
                .authorize_objects(
                    objects
                        .iter()
                        .map(|object| {
                            StorageObjectAggregateAuthorizationCandidate::new(
                                object.id(),
                                object.name().to_string(),
                                object.collection_id(),
                                object.class_id(),
                            )
                        })
                        .collect(),
                    query.required_permissions().to_vec(),
                )
                .await?;
            if decisions.len() != objects.len() {
                return Err(StorageError::backend_failure(
                    "Aggregate authorizer returned the wrong decision count",
                ));
            }
            objects = objects
                .into_iter()
                .zip(decisions)
                .filter_map(|(object, allowed)| allowed.then_some(object))
                .collect();
        }

        struct AggregateGroup {
            sort_key: serde_json::Value,
            object_count: i64,
            measures: Vec<Vec<f64>>,
        }

        let state = self.state.read().await;
        let dimension_value = |object: &StorageObject,
                               dimension: &StorageObjectAggregateDimension|
         -> serde_json::Value {
            let value = match dimension {
                StorageObjectAggregateDimension::Scalar(field) => match field {
                    StorageObjectAggregateScalarField::Name => {
                        serde_json::Value::String(object.name().to_string())
                    }
                    StorageObjectAggregateScalarField::Description => {
                        serde_json::Value::String(object.description().to_string())
                    }
                    StorageObjectAggregateScalarField::CollectionId => {
                        serde_json::Value::from(object.collection_id().id())
                    }
                    StorageObjectAggregateScalarField::CreatedAt => {
                        serde_json::Value::String(object.created_at().naive_utc().to_string())
                    }
                    StorageObjectAggregateScalarField::UpdatedAt => {
                        serde_json::Value::String(object.updated_at().naive_utc().to_string())
                    }
                },
                StorageObjectAggregateDimension::JsonData(path) => path
                    .segments()
                    .try_fold(object.data(), |value, segment| value.get(segment))
                    .cloned()
                    .unwrap_or(serde_json::Value::Null),
                StorageObjectAggregateDimension::Computed(selector) => {
                    let visibility = match selector.scope() {
                        ComputedFieldScope::Shared => StorageComputedFieldVisibility::Shared,
                        ComputedFieldScope::Personal => {
                            let Some(owner_id) = query.personal_owner_id() else {
                                return serde_json::json!([3, null]);
                            };
                            StorageComputedFieldVisibility::Personal { owner_id }
                        }
                    };
                    state
                        .computed_fields
                        .values()
                        .find(|definition| {
                            definition.class_id() == object.class_id()
                                && definition.visibility() == visibility
                                && definition.key() == selector.key()
                        })
                        .map(|definition| evaluate_computed_definition(definition, object))
                        .unwrap_or(serde_json::Value::Null)
                }
            };
            if value.is_null() {
                let state = match dimension {
                    StorageObjectAggregateDimension::JsonData(_) => 2,
                    StorageObjectAggregateDimension::Computed(_) => 3,
                    StorageObjectAggregateDimension::Scalar(_) => 1,
                };
                serde_json::json!([state, null])
            } else {
                serde_json::json!([0, value])
            }
        };
        let measure_value = |object: &StorageObject,
                             measure: &StorageObjectAggregateMeasure|
         -> Option<f64> {
            let value = match measure.field() {
                StorageObjectAggregateMeasureField::JsonData(path) => path
                    .segments()
                    .try_fold(object.data(), |value, segment| value.get(segment))
                    .cloned(),
                StorageObjectAggregateMeasureField::Computed(selector) => {
                    let visibility = match selector.scope() {
                        ComputedFieldScope::Shared => StorageComputedFieldVisibility::Shared,
                        ComputedFieldScope::Personal => StorageComputedFieldVisibility::Personal {
                            owner_id: query.personal_owner_id()?,
                        },
                    };
                    state
                        .computed_fields
                        .values()
                        .find(|definition| {
                            definition.class_id() == object.class_id()
                                && definition.visibility() == visibility
                                && definition.key() == selector.key()
                        })
                        .map(|definition| evaluate_computed_definition(definition, object))
                }
            }?;
            value.as_f64()
        };
        let mut groups = BTreeMap::<String, AggregateGroup>::new();
        for object in &objects {
            let sort_key = serde_json::Value::Array(
                query
                    .spec()
                    .dimensions()
                    .iter()
                    .map(|dimension| dimension_value(object, dimension))
                    .collect(),
            );
            let key = sort_key.to_string();
            let group = groups.entry(key).or_insert_with(|| AggregateGroup {
                sort_key,
                object_count: 0,
                measures: vec![Vec::new(); query.spec().measures().len()],
            });
            group.object_count += 1;
            for (values, measure) in group.measures.iter_mut().zip(query.spec().measures()) {
                if let Some(value) = measure_value(object, measure) {
                    values.push(value);
                }
            }
        }
        drop(state);
        let mut rows = groups
            .into_values()
            .map(|group| {
                let measures = group
                    .measures
                    .into_iter()
                    .zip(query.spec().measures())
                    .map(|(values, measure)| {
                        if values.is_empty() {
                            return StorageObjectAggregateMeasureValue::try_new(
                                StorageObjectAggregateMeasureState::Empty,
                                0,
                                group.object_count,
                                None,
                            )
                            .map_err(invalid_contract_value);
                        }
                        let value = match measure.operation() {
                            StorageObjectAggregateMeasureOperation::Sum => values.iter().sum(),
                            StorageObjectAggregateMeasureOperation::Average => {
                                values.iter().sum::<f64>() / values.len() as f64
                            }
                            StorageObjectAggregateMeasureOperation::Min => values
                                .iter()
                                .copied()
                                .reduce(f64::min)
                                .expect("non-empty measure values"),
                            StorageObjectAggregateMeasureOperation::Max => values
                                .iter()
                                .copied()
                                .reduce(f64::max)
                                .expect("non-empty measure values"),
                        };
                        StorageObjectAggregateMeasureValue::try_new(
                            StorageObjectAggregateMeasureState::Value,
                            i64::try_from(values.len()).unwrap_or(i64::MAX),
                            group.object_count - i64::try_from(values.len()).unwrap_or(i64::MAX),
                            serde_json::Number::from_f64(value).map(serde_json::Value::Number),
                        )
                        .map_err(invalid_contract_value)
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                StorageObjectAggregateRow::try_new(measures, group.object_count, group.sort_key)
                    .map_err(invalid_contract_value)
            })
            .collect::<Result<Vec<_>, StorageError>>()?;
        match query.spec().sort() {
            StorageObjectAggregateSort::DimensionsAscending => rows.sort_by_key(|row| {
                let (_, _, key) = row.clone().into_parts();
                key.to_string()
            }),
            StorageObjectAggregateSort::DimensionsDescending => rows.sort_by_key(|row| {
                let (_, _, key) = row.clone().into_parts();
                std::cmp::Reverse(key.to_string())
            }),
            StorageObjectAggregateSort::ObjectCountAscending => rows.sort_by_key(|row| {
                let (_, count, _) = row.clone().into_parts();
                count
            }),
            StorageObjectAggregateSort::ObjectCountDescending => rows.sort_by_key(|row| {
                let (_, count, _) = row.clone().into_parts();
                std::cmp::Reverse(count)
            }),
        }
        let total = query
            .options()
            .include_total()
            .then(|| i64::try_from(rows.len()).unwrap_or(i64::MAX));
        let has_more = rows.len() > query.page_limit();
        rows.truncate(query.page_limit());
        let next_cursor = has_more
            .then(|| rows.last())
            .flatten()
            .map(|row| {
                query
                    .spec()
                    .encode_cursor(row, query.cursor_max_encoded_bytes())
            })
            .transpose()?;
        StorageObjectAggregatePage::try_new(rows, total, next_cursor)
            .map_err(invalid_contract_value)
    }
}

#[async_trait]
impl RelationQueryStorage for MemoryStorage {
    async fn list_class_relations(
        &self,
        query: StorageRelationListQuery,
    ) -> Result<StoragePage<StorageClassRelation>, StorageError> {
        let (options, _) = query.into_parts();
        let rows = self
            .state
            .read()
            .await
            .class_relations
            .values()
            .cloned()
            .collect();
        page(rows, &options)
    }

    async fn list_object_relations(
        &self,
        query: StorageRelationListQuery,
    ) -> Result<StoragePage<StorageObjectRelation>, StorageError> {
        let (options, _) = query.into_parts();
        let rows = self
            .state
            .read()
            .await
            .object_relations
            .values()
            .cloned()
            .collect();
        page(rows, &options)
    }

    async fn list_class_relations_touching(
        &self,
        query: StorageRelationTouchingQuery,
    ) -> Result<StoragePage<StorageClassRelation>, StorageError> {
        let (anchor_id, options, _) = query.into_parts();
        let anchor = ClassId::from(anchor_id);
        let rows = self
            .state
            .read()
            .await
            .class_relations
            .values()
            .filter(|relation| {
                relation.from_class_id() == anchor || relation.to_class_id() == anchor
            })
            .cloned()
            .collect();
        page(rows, &options)
    }

    async fn list_object_relations_touching(
        &self,
        query: StorageRelationTouchingQuery,
    ) -> Result<StoragePage<StorageObjectRelation>, StorageError> {
        let (anchor_id, options, _) = query.into_parts();
        let anchor = ObjectId::from(anchor_id);
        let rows = self
            .state
            .read()
            .await
            .object_relations
            .values()
            .filter(|relation| {
                relation.from_object_id() == anchor || relation.to_object_id() == anchor
            })
            .cloned()
            .collect();
        page(rows, &options)
    }

    async fn list_class_relations_touching_ids(
        &self,
        query: StorageRelationIdsQuery,
    ) -> Result<Vec<StorageClassRelation>, StorageError> {
        let (ids, _) = query.into_parts();
        let ids = ids.into_iter().map(ClassId::from).collect::<BTreeSet<_>>();
        Ok(self
            .state
            .read()
            .await
            .class_relations
            .values()
            .filter(|relation| {
                ids.contains(&relation.from_class_id()) || ids.contains(&relation.to_class_id())
            })
            .cloned()
            .collect())
    }

    async fn list_class_relations_between_ids(
        &self,
        query: StorageRelationIdsQuery,
    ) -> Result<Vec<StorageClassRelation>, StorageError> {
        let (ids, _) = query.into_parts();
        let ids = ids.into_iter().map(ClassId::from).collect::<BTreeSet<_>>();
        Ok(self
            .state
            .read()
            .await
            .class_relations
            .values()
            .filter(|relation| {
                ids.contains(&relation.from_class_id()) && ids.contains(&relation.to_class_id())
            })
            .cloned()
            .collect())
    }

    async fn list_object_relations_touching_ids(
        &self,
        query: StorageObjectRelationsTouchingIdsQuery,
    ) -> Result<Vec<StorageObjectRelation>, StorageError> {
        let (object_ids, excluded_relation_ids, max_results, _) = query.into_parts();
        let object_ids = object_ids.into_iter().collect::<BTreeSet<_>>();
        let excluded_relation_ids = excluded_relation_ids.into_iter().collect::<BTreeSet<_>>();
        Ok(self
            .state
            .read()
            .await
            .object_relations
            .values()
            .filter(|relation| {
                let relation_id = ObjectRelationId::from(relation.metadata().id());
                !excluded_relation_ids.contains(&relation_id)
                    && (object_ids.contains(&relation.from_object_id())
                        || object_ids.contains(&relation.to_object_id()))
            })
            .take(max_results)
            .cloned()
            .collect())
    }

    async fn list_object_relations_between_ids(
        &self,
        query: StorageRelationIdsQuery,
    ) -> Result<Vec<StorageObjectRelation>, StorageError> {
        let (ids, _) = query.into_parts();
        let ids = ids.into_iter().map(ObjectId::from).collect::<BTreeSet<_>>();
        Ok(self
            .state
            .read()
            .await
            .object_relations
            .values()
            .filter(|relation| {
                ids.contains(&relation.from_object_id()) && ids.contains(&relation.to_object_id())
            })
            .cloned()
            .collect())
    }

    async fn list_related_classes(
        &self,
        query: StorageRelationGraphQuery,
    ) -> Result<StoragePage<StorageClassGraphRow>, StorageError> {
        let (root_id, options, _) = query.into_parts();
        let root_id = ClassId::from(root_id);
        let state = self.state.read().await;
        let root = state
            .classes
            .get(&root_id.id())
            .ok_or_else(|| StorageError::not_found("Relation graph root class was not found"))?;
        let mut rows = Vec::new();
        for relation in state.class_relations.values().filter(|relation| {
            relation.from_class_id() == root_id || relation.to_class_id() == root_id
        }) {
            let descendant_id = if relation.from_class_id() == root_id {
                relation.to_class_id()
            } else {
                relation.from_class_id()
            };
            let descendant = state
                .classes
                .get(&descendant_id.id())
                .ok_or_else(|| StorageError::internal("related class is missing"))?;
            rows.push(
                StorageClassGraphRow::try_new(
                    graph_class(root)?,
                    graph_class(descendant)?,
                    1,
                    vec![root_id, descendant_id],
                )
                .map_err(invalid_contract_value)?,
            );
        }
        page(rows, &options)
    }

    async fn list_related_objects(
        &self,
        query: StorageRelationGraphQuery,
    ) -> Result<StoragePage<StorageObjectGraphRow>, StorageError> {
        let (root_id, options, _) = query.into_parts();
        let root_id = ObjectId::from(root_id);
        let state = self.state.read().await;
        let root = state
            .objects
            .get(&root_id.id())
            .ok_or_else(|| StorageError::not_found("Relation graph root object was not found"))?;
        let mut rows = Vec::new();
        for relation in state.object_relations.values().filter(|relation| {
            relation.from_object_id() == root_id || relation.to_object_id() == root_id
        }) {
            let descendant_id = if relation.from_object_id() == root_id {
                relation.to_object_id()
            } else {
                relation.from_object_id()
            };
            let descendant = state
                .objects
                .get(&descendant_id.id())
                .ok_or_else(|| StorageError::internal("related object is missing"))?;
            rows.push(
                StorageObjectGraphRow::try_new(
                    graph_object(root)?,
                    graph_object(descendant)?,
                    1,
                    vec![root_id, descendant_id],
                )
                .map_err(invalid_contract_value)?,
            );
        }
        page(rows, &options)
    }

    async fn list_related_objects_for_roots(
        &self,
        query: StorageRelatedObjectsForRootsQuery,
    ) -> Result<Vec<StorageRelatedObjectIncludeRow>, StorageError> {
        let (root_ids, class_id, class_relation_id, direction, _, max_depth, limit, _, _) =
            query.into_parts();
        if max_depth < 1 || limit <= 0 {
            return Ok(Vec::new());
        }
        let state = self.state.read().await;
        let mut rows = Vec::new();
        for root_id in root_ids {
            let Some(root) = state.objects.get(&root_id.id()) else {
                continue;
            };
            for relation in state.object_relations.values().filter(|relation| {
                class_relation_id.is_none_or(|id| relation.class_relation_id() == id)
                    && match direction {
                        StorageRelatedDirection::Any => {
                            relation.from_object_id() == root_id
                                || relation.to_object_id() == root_id
                        }
                        StorageRelatedDirection::Outgoing => relation.from_object_id() == root_id,
                        StorageRelatedDirection::Incoming => relation.to_object_id() == root_id,
                    }
            }) {
                let descendant_id = if relation.from_object_id() == root_id {
                    relation.to_object_id()
                } else {
                    relation.from_object_id()
                };
                let Some(descendant) = state.objects.get(&descendant_id.id()) else {
                    continue;
                };
                if descendant.class_id() != class_id {
                    continue;
                }
                let graph_row = StorageObjectGraphRow::try_new(
                    graph_object(root)?,
                    graph_object(descendant)?,
                    1,
                    vec![root_id, descendant_id],
                )
                .map_err(invalid_contract_value)?;
                rows.push(
                    StorageRelatedObjectIncludeRow::try_new(root_id, graph_row)
                        .map_err(invalid_contract_value)?,
                );
                if rows.len() >= usize::try_from(limit).unwrap_or(usize::MAX) {
                    return Ok(rows);
                }
            }
        }
        Ok(rows)
    }

    async fn list_bidirectionally_related_objects_for_roots(
        &self,
        query: StorageBidirectionalRelatedObjectsQuery,
    ) -> Result<Vec<StorageRelatedObjectForRootRow>, StorageError> {
        let (root_ids, max_depth, per_root_cap, _, _) = query.into_parts();
        if max_depth < 1 || per_root_cap <= 0 {
            return Ok(Vec::new());
        }
        let state = self.state.read().await;
        let mut rows = Vec::new();
        for root_id in root_ids {
            let mut root_count = 0_i32;
            for relation in state.object_relations.values().filter(|relation| {
                relation.from_object_id() == root_id || relation.to_object_id() == root_id
            }) {
                if root_count >= per_root_cap {
                    break;
                }
                let descendant_id = if relation.from_object_id() == root_id {
                    relation.to_object_id()
                } else {
                    relation.from_object_id()
                };
                let Some(descendant) = state.objects.get(&descendant_id.id()) else {
                    continue;
                };
                rows.push(
                    StorageRelatedObjectForRootRow::try_new(
                        root_id,
                        graph_object(descendant)?,
                        1,
                        vec![root_id, descendant_id],
                    )
                    .map_err(invalid_contract_value)?,
                );
                root_count += 1;
            }
        }
        Ok(rows)
    }
}

#[async_trait]
impl AuditEventStorage for MemoryStorage {
    async fn list_audit_events(
        &self,
        query: StorageAuditEventListQuery,
    ) -> Result<StoragePage<StorageAuditEvent>, StorageError> {
        let state = self.state.read().await;
        let filters = query.filters();
        let mut events = state
            .events
            .iter()
            .filter(|recorded| {
                let (event, _, _) = (*recorded).clone().into_parts();
                let visible = event
                    .collection_id()
                    .map_or(query.include_collection_less(), |id| {
                        query.accessible_collection_ids().contains(&id)
                    });
                visible
                    && filters
                        .entity_type_value()
                        .is_none_or(|value| event.entity_type() == value)
                    && filters
                        .entity_id_value()
                        .is_none_or(|value| event.entity_id() == Some(value))
                    && filters
                        .action_value()
                        .is_none_or(|value| event.action() == value)
                    && filters
                        .actor_kind_value()
                        .is_none_or(|value| event.actor_kind() == value)
                    && filters
                        .actor_user_id_value()
                        .is_none_or(|value| event.actor_user_id() == Some(value))
                    && filters.initiator_user_id_value().is_none_or(|value| {
                        event
                            .provenance()
                            .initiator
                            .as_ref()
                            .map(|principal| principal.principal_id)
                            == Some(value)
                    })
                    && filters
                        .collection_id_value()
                        .is_none_or(|value| event.collection_id() == Some(value))
                    && filters
                        .occurred_after_value()
                        .is_none_or(|value| event.occurred_at() > value)
                    && filters
                        .occurred_before_value()
                        .is_none_or(|value| event.occurred_at() < value)
            })
            .cloned()
            .collect::<Vec<_>>();
        events.sort_by_key(|recorded| {
            let (event, _, _) = recorded.clone().into_parts();
            std::cmp::Reverse(event.id().get())
        });
        let total = query
            .options()
            .include_total()
            .then(|| i64::try_from(events.len()).unwrap_or(i64::MAX));
        if let Some(limit) = query.options().limit() {
            events.truncate(limit);
        }
        StoragePage::try_new(events, total)
            .map_err(|error| StorageError::backend_failure(error.to_string()))
    }
}

#[async_trait]
impl EventConfigurationStorage for MemoryStorage {
    async fn count_enabled_event_sinks(&self) -> Result<i64, StorageError> {
        i64::try_from(
            self.state
                .read()
                .await
                .event_sinks
                .values()
                .filter(|sink| sink.enabled())
                .count(),
        )
        .map_err(|_| StorageError::internal("event sink count does not fit i64"))
    }

    async fn list_event_sinks(
        &self,
        query: StorageEventSinkListQuery,
    ) -> Result<StoragePage<StorageEventSink>, StorageError> {
        page(
            self.state
                .read()
                .await
                .event_sinks
                .values()
                .cloned()
                .collect(),
            query.options(),
        )
    }

    async fn get_event_sink(&self, sink_id: EventSinkId) -> Result<StorageEventSink, StorageError> {
        self.state
            .read()
            .await
            .event_sinks
            .get(&sink_id.id())
            .cloned()
            .ok_or_else(|| {
                StorageError::not_found(format!("Event sink {} was not found", sink_id.id()))
            })
    }

    async fn create_event_sink(
        &self,
        request: StorageEventSinkCreate,
    ) -> Result<StorageMutationOutcome<StorageEventSink>, StorageError> {
        let mut state = self.state.write().await;
        if state
            .event_sinks
            .values()
            .any(|sink| sink.name() == request.name())
        {
            return Err(StorageError::conflict(format!(
                "Event sink '{}' already exists",
                request.name()
            )));
        }
        let id = EventSinkId::new(state.next_event_sink_id)
            .map_err(|error| StorageError::internal(error.to_string()))?;
        state.next_event_sink_id += 1;
        let now = Utc::now();
        let sink = StorageEventSink::builder(
            id,
            request.name(),
            request.kind(),
            now,
            now,
            ResourceRevision::INITIAL,
        )
        .configuration(request.configuration().clone())
        .secret_ref(request.secret_ref().map(ToOwned::to_owned))
        .enabled(request.enabled())
        .try_build()
        .map_err(invalid_contract_value)?;
        state.event_sinks.insert(id.id(), sink.clone());
        let receipt = state.append_simple_event(
            EntityType::EventSink,
            id.id(),
            Some(sink.name()),
            Action::Created,
            request.event_context(),
            format!("Event sink '{}' created", sink.name()),
        )?;
        Ok(StorageMutationOutcome::committed(sink, receipt))
    }

    async fn update_event_sink(
        &self,
        request: StorageEventSinkUpdate,
    ) -> Result<StorageMutationOutcome<StorageEventSink>, StorageError> {
        let mut state = self.state.write().await;
        let current = state
            .event_sinks
            .get(&request.id().id())
            .cloned()
            .ok_or_else(|| {
                StorageError::not_found(format!("Event sink {} was not found", request.id().id()))
            })?;
        let name = request.name_value().unwrap_or(current.name());
        let kind = request.kind_value().unwrap_or(current.kind());
        let configuration = request
            .configuration_value()
            .unwrap_or(current.configuration())
            .clone();
        let secret_ref = request.secret_ref_value().map_or_else(
            || current.secret_ref().map(ToOwned::to_owned),
            |value| value.map(ToOwned::to_owned),
        );
        let enabled = request.enabled_value().unwrap_or(current.enabled());
        if name == current.name()
            && kind == current.kind()
            && configuration == *current.configuration()
            && secret_ref.as_deref() == current.secret_ref()
            && enabled == current.enabled()
        {
            return Ok(StorageMutationOutcome::unchanged(current));
        }
        if state
            .event_sinks
            .values()
            .any(|sink| sink.id() != request.id() && sink.name() == name)
        {
            return Err(StorageError::conflict(format!(
                "Event sink '{name}' already exists"
            )));
        }
        let sink = StorageEventSink::builder(
            current.id(),
            name,
            kind,
            current.created_at(),
            Utc::now(),
            current
                .revision()
                .checked_advance()
                .map_err(|error| StorageError::internal(error.to_string()))?,
        )
        .configuration(configuration)
        .secret_ref(secret_ref)
        .enabled(enabled)
        .try_build()
        .map_err(invalid_contract_value)?;
        state.event_sinks.insert(sink.id().id(), sink.clone());
        let receipt = state.append_simple_event(
            EntityType::EventSink,
            sink.id().id(),
            Some(sink.name()),
            Action::Updated,
            request.event_context(),
            format!("Event sink '{}' updated", sink.name()),
        )?;
        Ok(StorageMutationOutcome::committed(sink, receipt))
    }

    async fn delete_event_sink(
        &self,
        request: StorageEventSinkDelete,
    ) -> Result<StorageMutationOutcome<()>, StorageError> {
        let mut state = self.state.write().await;
        if state
            .event_subscriptions
            .values()
            .any(|subscription| subscription.sink_id() == request.id())
        {
            return Err(StorageError::conflict("Event sink still has subscriptions"));
        }
        let sink = state
            .event_sinks
            .remove(&request.id().id())
            .ok_or_else(|| {
                StorageError::not_found(format!("Event sink {} was not found", request.id().id()))
            })?;
        let receipt = state.append_simple_event(
            EntityType::EventSink,
            sink.id().id(),
            Some(sink.name()),
            Action::Deleted,
            request.event_context(),
            format!("Event sink '{}' deleted", sink.name()),
        )?;
        Ok(StorageMutationOutcome::committed((), receipt))
    }

    async fn list_event_subscriptions(
        &self,
        query: StorageEventSubscriptionListQuery,
    ) -> Result<StoragePage<StorageEventSubscription>, StorageError> {
        let rows = self
            .state
            .read()
            .await
            .event_subscriptions
            .values()
            .filter(|subscription| subscription.collection_id() == query.collection_id())
            .cloned()
            .collect();
        page(rows, query.options())
    }

    async fn get_event_subscription(
        &self,
        collection_id: CollectionId,
        subscription_id: EventSubscriptionId,
    ) -> Result<StorageEventSubscription, StorageError> {
        self.state
            .read()
            .await
            .event_subscriptions
            .get(&subscription_id.id())
            .filter(|subscription| subscription.collection_id() == collection_id)
            .cloned()
            .ok_or_else(|| {
                StorageError::not_found(format!(
                    "Event subscription {} was not found in collection {}",
                    subscription_id.id(),
                    collection_id.id()
                ))
            })
    }

    async fn create_event_subscription(
        &self,
        request: StorageEventSubscriptionCreate,
    ) -> Result<StorageMutationOutcome<StorageEventSubscription>, StorageError> {
        let mut state = self.state.write().await;
        if !state
            .collections
            .contains_key(&request.collection_id().id())
        {
            return Err(StorageError::not_found(format!(
                "Collection {} was not found",
                request.collection_id().id()
            )));
        }
        if !state.event_sinks.contains_key(&request.sink_id().id()) {
            return Err(StorageError::not_found(format!(
                "Event sink {} was not found",
                request.sink_id().id()
            )));
        }
        if state.event_subscriptions.values().any(|subscription| {
            subscription.collection_id() == request.collection_id()
                && subscription.name() == request.name()
        }) {
            return Err(StorageError::conflict(format!(
                "Event subscription '{}' already exists",
                request.name()
            )));
        }
        let id = EventSubscriptionId::new(state.next_event_subscription_id)
            .map_err(|error| StorageError::internal(error.to_string()))?;
        state.next_event_subscription_id += 1;
        let now = Utc::now();
        let subscription = StorageEventSubscription::builder(
            id,
            request.collection_id(),
            request.sink_id(),
            request.name(),
            now,
            now,
            ResourceRevision::INITIAL,
        )
        .description(request.description())
        .entity_types(request.entity_types().to_vec())
        .actions(request.actions().to_vec())
        .filter(request.filter().clone())
        .routing(request.routing().clone())
        .enabled(request.enabled())
        .try_build()
        .map_err(invalid_contract_value)?;
        state
            .event_subscriptions
            .insert(id.id(), subscription.clone());
        let receipt = append_memory_scoped_simple_event!(
            state,
            EntityType::EventSubscription,
            id.id(),
            Some(subscription.name()),
            subscription.collection_id(),
            Action::Created,
            request.event_context(),
            format!("Event subscription '{}' created", subscription.name()),
        )?;
        Ok(StorageMutationOutcome::committed(subscription, receipt))
    }

    async fn update_event_subscription(
        &self,
        request: StorageEventSubscriptionUpdate,
    ) -> Result<StorageMutationOutcome<StorageEventSubscription>, StorageError> {
        let mut state = self.state.write().await;
        let current = state
            .event_subscriptions
            .get(&request.id().id())
            .filter(|subscription| subscription.collection_id() == request.collection_id())
            .cloned()
            .ok_or_else(|| {
                StorageError::not_found(format!(
                    "Event subscription {} was not found in collection {}",
                    request.id().id(),
                    request.collection_id().id()
                ))
            })?;
        let sink_id = request.sink_id_value().unwrap_or(current.sink_id());
        if !state.event_sinks.contains_key(&sink_id.id()) {
            return Err(StorageError::not_found(format!(
                "Event sink {} was not found",
                sink_id.id()
            )));
        }
        let name = request.name_value().unwrap_or(current.name());
        let description = request.description_value().unwrap_or(current.description());
        let entity_types = request
            .entity_types_value()
            .unwrap_or(current.entity_types());
        let actions = request.actions_value().unwrap_or(current.actions());
        let filter = request.filter_value().unwrap_or(current.filter());
        let routing = request.routing_value().unwrap_or(current.routing());
        let enabled = request.enabled_value().unwrap_or(current.enabled());
        let subscription = StorageEventSubscription::builder(
            current.id(),
            current.collection_id(),
            sink_id,
            name,
            current.created_at(),
            Utc::now(),
            current
                .revision()
                .checked_advance()
                .map_err(|error| StorageError::internal(error.to_string()))?,
        )
        .description(description)
        .entity_types(entity_types.to_vec())
        .actions(actions.to_vec())
        .filter(filter.clone())
        .routing(routing.clone())
        .enabled(enabled)
        .try_build()
        .map_err(invalid_contract_value)?;
        state
            .event_subscriptions
            .insert(subscription.id().id(), subscription.clone());
        let receipt = append_memory_scoped_simple_event!(
            state,
            EntityType::EventSubscription,
            subscription.id().id(),
            Some(subscription.name()),
            subscription.collection_id(),
            Action::Updated,
            request.event_context(),
            format!("Event subscription '{}' updated", subscription.name()),
        )?;
        Ok(StorageMutationOutcome::committed(subscription, receipt))
    }

    async fn delete_event_subscription(
        &self,
        request: StorageEventSubscriptionDelete,
    ) -> Result<StorageMutationOutcome<()>, StorageError> {
        let mut state = self.state.write().await;
        let subscription = state
            .event_subscriptions
            .remove(&request.id().id())
            .filter(|subscription| subscription.collection_id() == request.collection_id())
            .ok_or_else(|| {
                StorageError::not_found(format!(
                    "Event subscription {} was not found in collection {}",
                    request.id().id(),
                    request.collection_id().id()
                ))
            })?;
        let delivery_ids = state
            .event_deliveries
            .values()
            .filter(|delivery| delivery.subscription_id() == subscription.id())
            .map(|delivery| delivery.id().id())
            .collect::<Vec<_>>();
        for delivery_id in delivery_ids {
            state.event_deliveries.remove(&delivery_id);
            state.event_delivery_claims.remove(&delivery_id);
        }
        let receipt = append_memory_scoped_simple_event!(
            state,
            EntityType::EventSubscription,
            subscription.id().id(),
            Some(subscription.name()),
            subscription.collection_id(),
            Action::Deleted,
            request.event_context(),
            format!("Event subscription '{}' deleted", subscription.name()),
        )?;
        Ok(StorageMutationOutcome::committed((), receipt))
    }
}

#[async_trait]
impl EventDeliveryAdministrationStorage for MemoryStorage {
    async fn list_event_deliveries(
        &self,
        query: StorageEventDeliveryListQuery,
    ) -> Result<StoragePage<StorageEventDelivery>, StorageError> {
        let state = self.state.read().await;
        let rows = state
            .event_deliveries
            .values()
            .filter(|delivery| {
                query
                    .subscription_id_value()
                    .is_none_or(|id| delivery.subscription_id() == id)
            })
            .cloned()
            .collect();
        page(rows, query.options())
    }

    async fn get_event_delivery(
        &self,
        delivery_id: EventDeliveryId,
    ) -> Result<StorageEventDelivery, StorageError> {
        self.state
            .read()
            .await
            .event_deliveries
            .get(&delivery_id.id())
            .cloned()
            .ok_or_else(|| {
                StorageError::not_found(format!(
                    "Event delivery {} was not found",
                    delivery_id.id()
                ))
            })
    }

    async fn release_event_delivery_for_retry(
        &self,
        delivery_id: EventDeliveryId,
    ) -> Result<StorageEventDelivery, StorageError> {
        let mut state = self.state.write().await;
        let current = state
            .event_deliveries
            .get(&delivery_id.id())
            .cloned()
            .ok_or_else(|| {
                StorageError::not_found(format!(
                    "Event delivery {} was not found",
                    delivery_id.id()
                ))
            })?;
        let delivery = rebuild_event_delivery(
            &current,
            EventDeliveryStatus::Pending,
            current.attempts(),
            Utc::now(),
            None,
            None,
        )?;
        state.event_delivery_claims.remove(&delivery_id.id());
        state
            .event_deliveries
            .insert(delivery_id.id(), delivery.clone());
        Ok(delivery)
    }

    async fn mark_event_delivery_dead(
        &self,
        delivery_id: EventDeliveryId,
    ) -> Result<StorageEventDelivery, StorageError> {
        let mut state = self.state.write().await;
        let current = state
            .event_deliveries
            .get(&delivery_id.id())
            .cloned()
            .ok_or_else(|| {
                StorageError::not_found(format!(
                    "Event delivery {} was not found",
                    delivery_id.id()
                ))
            })?;
        if current.status() == EventDeliveryStatus::Succeeded {
            return Err(StorageError::conflict(
                "A succeeded event delivery cannot be marked dead",
            ));
        }
        let delivery = rebuild_event_delivery(
            &current,
            EventDeliveryStatus::Dead,
            current.attempts(),
            current.next_attempt_at(),
            Some("Marked dead by an administrator".to_string()),
            None,
        )?;
        state.event_delivery_claims.remove(&delivery_id.id());
        state
            .event_deliveries
            .insert(delivery_id.id(), delivery.clone());
        Ok(delivery)
    }
}

#[async_trait]
impl EventDeliveryWorkerStorage for MemoryStorage {
    async fn claim_event_delivery_batch(
        &self,
        settings: hubuum_domain::EventDeliverySettings,
    ) -> Result<StorageEventDeliveryBatch, StorageError> {
        let mut state = self.state.write().await;
        let now = Utc::now();
        let locked_until = settings
            .lock_deadline(now.naive_utc())
            .map(|value| DateTime::from_naive_utc_and_offset(value, Utc))
            .ok_or_else(|| StorageError::internal("event delivery lock deadline overflowed"))?;
        let candidates = state
            .event_deliveries
            .values()
            .filter(|delivery| {
                matches!(
                    delivery.status(),
                    EventDeliveryStatus::Pending | EventDeliveryStatus::Failed
                ) && delivery.next_attempt_at() <= now
                    && delivery.attempts() < settings.max_attempts()
            })
            .take(settings.batch_size())
            .cloned()
            .collect::<Vec<_>>();
        let mut work = Vec::with_capacity(candidates.len());
        for current in candidates {
            let attempts = current.attempts().saturating_add(1);
            let token = Uuid::new_v4();
            let delivery = rebuild_event_delivery(
                &current,
                EventDeliveryStatus::InFlight,
                attempts,
                current.next_attempt_at(),
                None,
                Some(locked_until),
            )?;
            let envelope = state
                .events
                .iter()
                .find_map(|recorded| {
                    let (event, _, _) = recorded.clone().into_parts();
                    (event.id() == delivery.event_id()).then_some(event)
                })
                .ok_or_else(|| StorageError::internal("event delivery event is missing"))?;
            let subscription = state
                .event_subscriptions
                .get(&delivery.subscription_id().id())
                .ok_or_else(|| StorageError::internal("event delivery subscription is missing"))?;
            let sink = state
                .event_sinks
                .get(&subscription.sink_id().id())
                .ok_or_else(|| StorageError::internal("event delivery sink is missing"))?;
            let claim = StorageEventDeliveryClaim::try_new(delivery.id(), attempts, token)
                .map_err(invalid_contract_value)?;
            let delivery_subscription = StorageEventDeliverySubscription::try_new(
                subscription.id(),
                subscription.name(),
                subscription.routing().clone(),
            )
            .map_err(invalid_contract_value)?;
            let delivery_sink = StorageEventDeliverySink::try_new(
                sink.id(),
                sink.name(),
                sink.kind(),
                sink.configuration().clone(),
                sink.secret_ref().map(ToOwned::to_owned),
            )
            .map_err(invalid_contract_value)?;
            state
                .event_delivery_claims
                .insert(delivery.id().id(), token);
            state.event_deliveries.insert(delivery.id().id(), delivery);
            work.push(StorageEventDeliveryWorkItem::new(
                claim,
                envelope,
                delivery_subscription,
                delivery_sink,
            ));
        }
        Ok(StorageEventDeliveryBatch::new(work, None))
    }

    async fn mark_event_delivery_succeeded(
        &self,
        claim: &StorageEventDeliveryClaim,
    ) -> Result<(), StorageError> {
        let mut state = self.state.write().await;
        if state.event_delivery_claims.get(&claim.delivery_id().id()) != Some(&claim.token()) {
            return Err(StorageError::conflict("Event delivery claim is stale"));
        }
        let current = state
            .event_deliveries
            .get(&claim.delivery_id().id())
            .cloned()
            .ok_or_else(|| StorageError::not_found("Event delivery was not found"))?;
        if current.status() != EventDeliveryStatus::InFlight
            || current.attempts() != claim.attempts()
        {
            return Err(StorageError::conflict("Event delivery claim is stale"));
        }
        let delivery = rebuild_event_delivery(
            &current,
            EventDeliveryStatus::Succeeded,
            current.attempts(),
            current.next_attempt_at(),
            None,
            None,
        )?;
        state
            .event_delivery_claims
            .remove(&claim.delivery_id().id());
        state
            .event_deliveries
            .insert(claim.delivery_id().id(), delivery);
        Ok(())
    }

    async fn mark_event_delivery_failed(
        &self,
        claim: &StorageEventDeliveryClaim,
        settings: hubuum_domain::EventDeliverySettings,
        error: &str,
    ) -> Result<(), StorageError> {
        let mut state = self.state.write().await;
        if state.event_delivery_claims.get(&claim.delivery_id().id()) != Some(&claim.token()) {
            return Err(StorageError::conflict("Event delivery claim is stale"));
        }
        let current = state
            .event_deliveries
            .get(&claim.delivery_id().id())
            .cloned()
            .ok_or_else(|| StorageError::not_found("Event delivery was not found"))?;
        if current.status() != EventDeliveryStatus::InFlight
            || current.attempts() != claim.attempts()
        {
            return Err(StorageError::conflict("Event delivery claim is stale"));
        }
        let exhausted = current.attempts() >= settings.max_attempts();
        let status = if exhausted {
            EventDeliveryStatus::Dead
        } else {
            EventDeliveryStatus::Failed
        };
        let next_attempt_at = settings
            .retry_deadline(Utc::now().naive_utc(), current.attempts())
            .map(|value| DateTime::from_naive_utc_and_offset(value, Utc))
            .ok_or_else(|| StorageError::internal("event retry deadline overflowed"))?;
        let delivery = rebuild_event_delivery(
            &current,
            status,
            current.attempts(),
            next_attempt_at,
            Some(error.to_string()),
            None,
        )?;
        state
            .event_delivery_claims
            .remove(&claim.delivery_id().id());
        state
            .event_deliveries
            .insert(claim.delivery_id().id(), delivery);
        Ok(())
    }
}

#[async_trait]
impl EventFanoutStorage for MemoryStorage {
    async fn process_event_fanout_batch(
        &self,
        settings: EventFanoutSettings,
    ) -> Result<usize, StorageError> {
        let mut state = self.state.write().await;
        let events = state
            .events
            .iter()
            .filter_map(|recorded| {
                let (event, _, _) = recorded.clone().into_parts();
                (event.id().get() > state.fanout_event_cursor).then_some(event)
            })
            .take(settings.batch_size())
            .collect::<Vec<_>>();
        if events.is_empty() {
            return Ok(0);
        }
        let subscriptions = state
            .event_subscriptions
            .values()
            .filter(|subscription| subscription.enabled())
            .cloned()
            .collect::<Vec<_>>();
        for event in &events {
            for subscription in &subscriptions {
                let sink_enabled = state
                    .event_sinks
                    .get(&subscription.sink_id().id())
                    .is_some_and(|sink| sink.enabled());
                let matches = sink_enabled
                    && event.collection_id() == Some(subscription.collection_id())
                    && subscription.entity_types().contains(&event.entity_type())
                    && subscription.actions().contains(&event.action());
                let exists = state.event_deliveries.values().any(|delivery| {
                    delivery.event_id() == event.id()
                        && delivery.subscription_id() == subscription.id()
                });
                if matches && !exists {
                    let id = EventDeliveryId::new(state.next_event_delivery_id)
                        .map_err(|error| StorageError::internal(error.to_string()))?;
                    state.next_event_delivery_id += 1;
                    let now = Utc::now();
                    let delivery = StorageEventDelivery::builder(
                        id,
                        event.id(),
                        subscription.id(),
                        EventDeliveryStatus::Pending,
                        now,
                        now,
                        now,
                    )
                    .try_build()
                    .map_err(invalid_contract_value)?;
                    state.event_deliveries.insert(id.id(), delivery);
                }
            }
        }
        state.fanout_event_cursor = events
            .last()
            .map(|event| event.id().get())
            .unwrap_or(state.fanout_event_cursor);
        Ok(events.len())
    }
}

#[async_trait]
impl EventHealthStorage for MemoryStorage {
    async fn get_event_delivery_health(
        &self,
    ) -> Result<StorageEventDeliveryHealthSnapshot, StorageError> {
        let state = self.state.read().await;
        let pending_events = state
            .events
            .iter()
            .filter(|recorded| {
                let (event, _, _) = (*recorded).clone().into_parts();
                event.id().get() > state.fanout_event_cursor
            })
            .count();
        let fanout = StorageEventFanoutSnapshot::try_new(
            i64::try_from(pending_events).unwrap_or(i64::MAX),
            0,
            0,
            (pending_events > 0).then_some(0),
        )
        .map_err(invalid_contract_value)?;
        let counts = event_status_counts(
            state
                .event_deliveries
                .values()
                .map(StorageEventDelivery::status),
        )?;
        let due = counts.pending() + counts.retryable();
        let delivery = StorageEventQueueSnapshot::try_new(counts, 0, (due > 0).then_some(0))
            .map_err(invalid_contract_value)?;
        Ok(StorageEventDeliveryHealthSnapshot::new(
            fanout,
            delivery,
            Vec::new(),
            Vec::new(),
        ))
    }
}

#[async_trait]
impl EventRetentionStorage for MemoryStorage {
    async fn claim_event_retention_batch(
        &self,
        settings: EventRetentionSettings,
    ) -> Result<Option<StorageEventRetentionBatch>, StorageError> {
        let cutoff: DateTime<Utc> = settings
            .event_cutoff(Utc::now().naive_utc())
            .map(|value| DateTime::from_naive_utc_and_offset(value, Utc))
            .ok_or_else(|| StorageError::internal("event retention cutoff overflowed"))?;
        let mut state = self.state.write().await;
        let retained = state
            .events
            .iter()
            .filter_map(|recorded| {
                let (event, _, _) = recorded.clone().into_parts();
                (event.occurred_at() < cutoff).then_some(event)
            })
            .take(settings.batch_size())
            .map(|event| {
                let id = event.id();
                serde_json::to_string(&event)
                    .map_err(|error| StorageError::internal(error.to_string()))
                    .and_then(|json| {
                        StorageRetainedEvent::try_new(id, json).map_err(invalid_contract_value)
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        if retained.is_empty() {
            return Ok(None);
        }
        let id = StorageEventRetentionBatchId::new(Uuid::new_v4());
        state.event_retention_batches.insert(
            id.as_uuid(),
            retained.iter().map(|event| event.id().get()).collect(),
        );
        Ok(Some(StorageEventRetentionBatch::new(id, retained)))
    }

    async fn complete_event_retention_batch(
        &self,
        batch_id: StorageEventRetentionBatchId,
    ) -> Result<StorageEventRetentionSummary, StorageError> {
        let mut state = self.state.write().await;
        let Some(event_ids) = state.event_retention_batches.remove(&batch_id.as_uuid()) else {
            return Ok(StorageEventRetentionSummary::default());
        };
        let event_ids = event_ids.into_iter().collect::<BTreeSet<_>>();
        let before_events = state.events.len();
        state.events.retain(|recorded| {
            let (event, _, _) = recorded.clone().into_parts();
            !event_ids.contains(&event.id().get())
        });
        let terminal_delivery_ids = state
            .event_deliveries
            .values()
            .filter(|delivery| {
                event_ids.contains(&delivery.event_id().get())
                    && matches!(
                        delivery.status(),
                        EventDeliveryStatus::Succeeded | EventDeliveryStatus::Dead
                    )
            })
            .map(|delivery| delivery.id().id())
            .collect::<Vec<_>>();
        for delivery_id in &terminal_delivery_ids {
            state.event_deliveries.remove(delivery_id);
            state.event_delivery_claims.remove(delivery_id);
        }
        Ok(StorageEventRetentionSummary::new(
            before_events - state.events.len(),
            terminal_delivery_ids.len(),
        ))
    }
}

#[async_trait]
impl HistoryStorage for MemoryStorage {
    async fn resolve_history_principal_names(
        &self,
        principal_ids: Vec<PrincipalId>,
    ) -> Result<Vec<StorageHistoryPrincipalName>, StorageError> {
        let state = self.state.read().await;
        Ok(principal_ids
            .into_iter()
            .filter_map(|id| {
                state
                    .principals
                    .get(&id.id())
                    .map(|principal| StorageHistoryPrincipalName::new(id, principal.name()))
            })
            .collect())
    }

    async fn list_collection_history(
        &self,
        query: StorageHistoryListQuery,
    ) -> Result<StoragePage<StorageCollectionHistoryRecord>, StorageError> {
        let (entity_id, options, scope) = query.into_parts();
        let state = self.state.read().await;
        let mut entries = state
            .history
            .iter()
            .filter(|entry| {
                entry.value.entity_id() == entity_id.id()
                    && matches!(entry.value, MemoryHistoryValue::Collection(_))
                    && history_scope_allows(&scope, entry.value.collection_id())
            })
            .collect::<Vec<_>>();
        entries.reverse();
        let rows = entries
            .into_iter()
            .map(|entry| match &entry.value {
                MemoryHistoryValue::Collection(record) => StorageCollectionHistoryRecord::try_new(
                    record.clone(),
                    entry.metadata(history_valid_to(&state, entry))?,
                )
                .map_err(invalid_contract_value),
                _ => unreachable!("collection history filter guarantees the variant"),
            })
            .collect::<Result<Vec<_>, _>>()?;
        page(rows, &options)
    }

    async fn get_collection_history_as_of(
        &self,
        query: StorageHistoryAsOfQuery,
    ) -> Result<Option<StorageCollectionHistoryRecord>, StorageError> {
        let (entity_id, at) = query.into_parts();
        let state = self.state.read().await;
        let entry = state.history.iter().rev().find(|entry| {
            entry.value.entity_id() == entity_id.id()
                && matches!(entry.value, MemoryHistoryValue::Collection(_))
                && entry.valid_from <= at
        });
        let Some(entry) = entry else {
            return Ok(None);
        };
        if entry.operation == StorageHistoryOperation::Delete {
            return Ok(None);
        }
        match &entry.value {
            MemoryHistoryValue::Collection(record) => StorageCollectionHistoryRecord::try_new(
                record.clone(),
                entry.metadata(history_valid_to(&state, entry))?,
            )
            .map(Some)
            .map_err(invalid_contract_value),
            _ => unreachable!("collection history filter guarantees the variant"),
        }
    }

    async fn list_class_history(
        &self,
        query: StorageHistoryListQuery,
    ) -> Result<StoragePage<StorageClassHistoryRecord>, StorageError> {
        let (entity_id, options, scope) = query.into_parts();
        let state = self.state.read().await;
        let mut entries = state
            .history
            .iter()
            .filter(|entry| {
                entry.value.entity_id() == entity_id.id()
                    && matches!(entry.value, MemoryHistoryValue::Class(_))
                    && history_scope_allows(&scope, entry.value.collection_id())
            })
            .collect::<Vec<_>>();
        entries.reverse();
        let rows = entries
            .into_iter()
            .map(|entry| match &entry.value {
                MemoryHistoryValue::Class(record) => StorageClassHistoryRecord::try_new(
                    record.clone(),
                    entry.metadata(history_valid_to(&state, entry))?,
                )
                .map_err(invalid_contract_value),
                _ => unreachable!("class history filter guarantees the variant"),
            })
            .collect::<Result<Vec<_>, _>>()?;
        page(rows, &options)
    }

    async fn get_class_history_as_of(
        &self,
        query: StorageHistoryAsOfQuery,
    ) -> Result<Option<StorageClassHistoryRecord>, StorageError> {
        let (entity_id, at) = query.into_parts();
        let state = self.state.read().await;
        let entry = state.history.iter().rev().find(|entry| {
            entry.value.entity_id() == entity_id.id()
                && matches!(entry.value, MemoryHistoryValue::Class(_))
                && entry.valid_from <= at
        });
        let Some(entry) = entry else {
            return Ok(None);
        };
        if entry.operation == StorageHistoryOperation::Delete {
            return Ok(None);
        }
        match &entry.value {
            MemoryHistoryValue::Class(record) => StorageClassHistoryRecord::try_new(
                record.clone(),
                entry.metadata(history_valid_to(&state, entry))?,
            )
            .map(Some)
            .map_err(invalid_contract_value),
            _ => unreachable!("class history filter guarantees the variant"),
        }
    }

    async fn list_object_history(
        &self,
        query: StorageObjectHistoryListQuery,
    ) -> Result<StoragePage<StorageObjectHistoryRecord>, StorageError> {
        let (object_id, class_id, options, scope) = query.into_parts();
        let state = self.state.read().await;
        let mut entries = state
            .history
            .iter()
            .filter(|entry| match &entry.value {
                MemoryHistoryValue::Object(record) => {
                    record.id() == object_id
                        && record.class_id() == class_id
                        && history_scope_allows(&scope, record.collection_id())
                }
                _ => false,
            })
            .collect::<Vec<_>>();
        entries.reverse();
        let rows = entries
            .into_iter()
            .map(|entry| match &entry.value {
                MemoryHistoryValue::Object(record) => StorageObjectHistoryRecord::try_new(
                    record.clone(),
                    entry.metadata(history_valid_to(&state, entry))?,
                )
                .map_err(invalid_contract_value),
                _ => unreachable!("object history filter guarantees the variant"),
            })
            .collect::<Result<Vec<_>, _>>()?;
        page(rows, &options)
    }

    async fn get_object_history_as_of(
        &self,
        query: StorageObjectHistoryAsOfQuery,
    ) -> Result<Option<StorageObjectHistoryRecord>, StorageError> {
        let (object_id, class_id, at) = query.into_parts();
        let state = self.state.read().await;
        let entry = state.history.iter().rev().find(|entry| match &entry.value {
            MemoryHistoryValue::Object(record) => {
                record.id() == object_id && record.class_id() == class_id && entry.valid_from <= at
            }
            _ => false,
        });
        let Some(entry) = entry else {
            return Ok(None);
        };
        if entry.operation == StorageHistoryOperation::Delete {
            return Ok(None);
        }
        match &entry.value {
            MemoryHistoryValue::Object(record) => StorageObjectHistoryRecord::try_new(
                record.clone(),
                entry.metadata(history_valid_to(&state, entry))?,
            )
            .map(Some)
            .map_err(invalid_contract_value),
            _ => unreachable!("object history filter guarantees the variant"),
        }
    }

    async fn list_export_template_history(
        &self,
        query: StorageHistoryListQuery,
    ) -> Result<StoragePage<StorageExportTemplateHistoryRecord>, StorageError> {
        let (entity_id, options, scope) = query.into_parts();
        let state = self.state.read().await;
        let mut entries = state
            .history
            .iter()
            .filter(|entry| {
                entry.value.entity_id() == entity_id.id()
                    && matches!(entry.value, MemoryHistoryValue::ExportTemplate(_))
                    && history_scope_allows(&scope, entry.value.collection_id())
            })
            .collect::<Vec<_>>();
        entries.reverse();
        let rows = entries
            .into_iter()
            .map(|entry| match &entry.value {
                MemoryHistoryValue::ExportTemplate(record) => {
                    StorageExportTemplateHistoryRecord::try_new(
                        record.clone(),
                        entry.metadata(history_valid_to(&state, entry))?,
                    )
                    .map_err(invalid_contract_value)
                }
                _ => unreachable!("export-template history filter guarantees the variant"),
            })
            .collect::<Result<Vec<_>, _>>()?;
        page(rows, &options)
    }

    async fn get_export_template_history_as_of(
        &self,
        query: StorageHistoryAsOfQuery,
    ) -> Result<Option<StorageExportTemplateHistoryRecord>, StorageError> {
        let (entity_id, at) = query.into_parts();
        let state = self.state.read().await;
        let entry = state.history.iter().rev().find(|entry| {
            entry.value.entity_id() == entity_id.id()
                && matches!(entry.value, MemoryHistoryValue::ExportTemplate(_))
                && entry.valid_from <= at
        });
        let Some(entry) = entry else {
            return Ok(None);
        };
        if entry.operation == StorageHistoryOperation::Delete {
            return Ok(None);
        }
        match &entry.value {
            MemoryHistoryValue::ExportTemplate(record) => {
                StorageExportTemplateHistoryRecord::try_new(
                    record.clone(),
                    entry.metadata(history_valid_to(&state, entry))?,
                )
                .map(Some)
                .map_err(invalid_contract_value)
            }
            _ => unreachable!("export-template history filter guarantees the variant"),
        }
    }

    async fn list_remote_target_history(
        &self,
        query: StorageHistoryListQuery,
    ) -> Result<StoragePage<StorageRemoteTargetHistoryRecord>, StorageError> {
        let (entity_id, options, scope) = query.into_parts();
        let state = self.state.read().await;
        let mut entries = state
            .history
            .iter()
            .filter(|entry| {
                entry.value.entity_id() == entity_id.id()
                    && matches!(entry.value, MemoryHistoryValue::RemoteTarget(_))
                    && history_scope_allows(&scope, entry.value.collection_id())
            })
            .collect::<Vec<_>>();
        entries.reverse();
        let rows = entries
            .into_iter()
            .map(|entry| match &entry.value {
                MemoryHistoryValue::RemoteTarget(record) => {
                    StorageRemoteTargetHistoryRecord::try_new(
                        record.clone(),
                        entry.metadata(history_valid_to(&state, entry))?,
                    )
                    .map_err(invalid_contract_value)
                }
                _ => unreachable!("remote-target history filter guarantees the variant"),
            })
            .collect::<Result<Vec<_>, _>>()?;
        page(rows, &options)
    }

    async fn get_remote_target_history_as_of(
        &self,
        query: StorageHistoryAsOfQuery,
    ) -> Result<Option<StorageRemoteTargetHistoryRecord>, StorageError> {
        let (entity_id, at) = query.into_parts();
        let state = self.state.read().await;
        let entry = state.history.iter().rev().find(|entry| {
            entry.value.entity_id() == entity_id.id()
                && matches!(entry.value, MemoryHistoryValue::RemoteTarget(_))
                && entry.valid_from <= at
        });
        let Some(entry) = entry else {
            return Ok(None);
        };
        if entry.operation == StorageHistoryOperation::Delete {
            return Ok(None);
        }
        match &entry.value {
            MemoryHistoryValue::RemoteTarget(record) => StorageRemoteTargetHistoryRecord::try_new(
                record.clone(),
                entry.metadata(history_valid_to(&state, entry))?,
            )
            .map(Some)
            .map_err(invalid_contract_value),
            _ => unreachable!("remote-target history filter guarantees the variant"),
        }
    }
}

#[async_trait]
impl InventoryStorage for MemoryStorage {
    async fn get_inventory_counts(&self) -> Result<StorageInventoryCounts, StorageError> {
        let state = self.state.read().await;
        let mut objects_by_class = BTreeMap::<ClassId, i64>::new();
        for object in state.objects.values() {
            *objects_by_class.entry(object.class_id()).or_default() += 1;
        }
        let objects_by_class = objects_by_class
            .into_iter()
            .map(|(class_id, count)| StorageObjectCountByClass::try_new(class_id, count))
            .collect::<Result<Vec<_>, _>>()
            .map_err(invalid_contract_value)?;
        StorageInventoryCounts::try_new(
            i64::try_from(state.objects.len())
                .map_err(|_| StorageError::internal("object count does not fit i64"))?,
            i64::try_from(state.classes.len())
                .map_err(|_| StorageError::internal("class count does not fit i64"))?,
            i64::try_from(state.collections.len())
                .map_err(|_| StorageError::internal("collection count does not fit i64"))?,
            objects_by_class,
        )
        .map_err(invalid_contract_value)
    }
}

#[async_trait]
impl MetricsStorage for MemoryStorage {
    async fn get_inventory_metrics_snapshot(
        &self,
    ) -> Result<StorageInventoryGaugeSnapshot, StorageError> {
        let counts = self.get_inventory_counts().await?;
        let state = self.state.read().await;
        StorageInventoryGaugeSnapshot::try_new(
            StorageInventoryMetricsSnapshot::try_new(
                counts.total_collections(),
                counts.total_classes(),
                counts.total_objects(),
                i64::try_from(state.users.len())
                    .map_err(|_| StorageError::internal("user count does not fit i64"))?,
                i64::try_from(state.groups.len())
                    .map_err(|_| StorageError::internal("group count does not fit i64"))?,
                i64::try_from(state.service_accounts.len()).map_err(|_| {
                    StorageError::internal("service-account count does not fit i64")
                })?,
                0,
            )
            .map_err(invalid_contract_value)?,
            Vec::new(),
        )
        .map_err(invalid_contract_value)
    }

    async fn get_task_metrics_snapshot(&self) -> Result<StorageTaskGaugeSnapshot, StorageError> {
        let ages = StorageTaskKind::ALL
            .into_iter()
            .map(|kind| StorageTaskGaugeAge::new(kind, None, None))
            .collect();
        StorageTaskGaugeSnapshot::try_new(Vec::new(), ages, Vec::new())
            .map_err(invalid_contract_value)
    }

    async fn get_event_metrics_snapshot(
        &self,
    ) -> Result<StorageEventMetricsSnapshot, StorageError> {
        Ok(StorageEventMetricsSnapshot::new(
            empty_event_fanout_snapshot()?,
            empty_event_queue_snapshot()?,
        ))
    }
}

#[async_trait]
impl OperationalStateStorage for MemoryStorage {
    async fn get_readiness_snapshot(&self) -> Result<StorageReadinessSnapshot, StorageError> {
        Ok(StorageReadinessSnapshot::new(
            true,
            self.state.read().await.maintenance_state,
        ))
    }

    async fn get_maintenance_state(&self) -> Result<MaintenanceState, StorageError> {
        Ok(self.state.read().await.maintenance_state)
    }

    async fn get_task_queue_snapshot(
        &self,
    ) -> Result<StorageOperationalTaskQueueSnapshot, StorageError> {
        StorageOperationalTaskQueueSnapshot::try_new(
            StorageOperationalTaskStatusCounts::try_new(
                0,
                StorageOperationalTaskActiveCounts::try_new(0, 0, 0)
                    .map_err(invalid_contract_value)?,
                StorageOperationalTaskTerminalCounts::try_new(0, 0, 0, 0)
                    .map_err(invalid_contract_value)?,
            )
            .map_err(invalid_contract_value)?,
            StorageOperationalTaskKindCounts::try_new(0, 0, 0).map_err(invalid_contract_value)?,
            0,
            0,
            None,
            None,
        )
        .map_err(invalid_contract_value)
    }

    async fn load_export_template_health(
        &self,
    ) -> Result<Vec<StorageOperationalExportTemplateHealth>, StorageError> {
        Ok(Vec::new())
    }

    async fn load_export_templates_for_audit(
        &self,
    ) -> Result<Vec<StorageOperationalExportTemplateAuditEntry>, StorageError> {
        Ok(Vec::new())
    }
}

#[async_trait]
impl TokenRetentionStorage for MemoryStorage {
    async fn purge_expired_tokens(
        &self,
        _settings: TokenRetentionSettings,
    ) -> Result<usize, StorageError> {
        Ok(0)
    }
}

#[async_trait]
impl UnifiedSearchStorage for MemoryStorage {
    async fn search_collections(
        &self,
        query: StorageUnifiedSearchQuery,
    ) -> Result<StorageCandidatePage<StorageUnifiedSearchCandidate<StorageCollection>>, StorageError>
    {
        let state = self.state.read().await;
        let mut ranked = state
            .collections
            .values()
            .filter(|collection| {
                query
                    .visibility()
                    .resources()
                    .is_none_or(|scope| scope.collection_ids().contains(&collection.id()))
            })
            .filter_map(|collection| {
                search_rank(
                    collection.name(),
                    collection.description(),
                    None,
                    query.search_term(),
                )
                .map(|rank| (rank, collection.clone()))
            })
            .collect::<Vec<_>>();
        ranked.sort_by(|(left_rank, left), (right_rank, right)| {
            right_rank
                .cmp(left_rank)
                .then_with(|| left.name().to_lowercase().cmp(&right.name().to_lowercase()))
                .then_with(|| left.id().cmp(&right.id()))
        });
        let limit = query.page_limit();
        let has_more = ranked.len() > limit.get();
        ranked.truncate(limit.get());
        let rows = ranked
            .into_iter()
            .map(|(rank, collection)| {
                let cursor = StorageUnifiedSearchCursor::new(
                    rank,
                    collection.name().to_lowercase(),
                    ResourceId::new(collection.id().id()).expect("collection id is positive"),
                );
                StorageUnifiedSearchCandidate::new(collection, cursor)
            })
            .collect();
        StorageCandidatePage::try_new(rows, has_more, limit).map_err(invalid_contract_value)
    }

    async fn search_classes(
        &self,
        query: StorageUnifiedSearchQuery,
    ) -> Result<
        StorageCandidatePage<StorageUnifiedSearchCandidate<StorageClassWithCollection>>,
        StorageError,
    > {
        let state = self.state.read().await;
        let mut ranked = state
            .classes
            .values()
            .filter(|class| {
                query.visibility().resources().is_none_or(|scope| {
                    scope.class_ids().contains(&class.id())
                        || scope.collection_ids().contains(&class.collection_id())
                })
            })
            .filter_map(|class| {
                let extended = query.searches_extended_document().then(|| {
                    class
                        .json_schema()
                        .map(ToString::to_string)
                        .unwrap_or_default()
                });
                search_rank(
                    class.name(),
                    class.description(),
                    extended.as_deref(),
                    query.search_term(),
                )
                .map(|rank| (rank, class))
            })
            .map(|(rank, class)| Ok((rank, class_with_collection(&state, class)?)))
            .collect::<Result<Vec<_>, StorageError>>()?;
        ranked.sort_by(|(left_rank, left), (right_rank, right)| {
            right_rank
                .cmp(left_rank)
                .then_with(|| left.name().to_lowercase().cmp(&right.name().to_lowercase()))
                .then_with(|| left.id().cmp(&right.id()))
        });
        let limit = query.page_limit();
        let has_more = ranked.len() > limit.get();
        ranked.truncate(limit.get());
        let rows = ranked
            .into_iter()
            .map(|(rank, class)| {
                let cursor = StorageUnifiedSearchCursor::new(
                    rank,
                    class.name().to_lowercase(),
                    ResourceId::new(class.id().id()).expect("class id is positive"),
                );
                StorageUnifiedSearchCandidate::new(class, cursor)
            })
            .collect();
        StorageCandidatePage::try_new(rows, has_more, limit).map_err(invalid_contract_value)
    }

    async fn search_objects(
        &self,
        query: StorageUnifiedSearchQuery,
    ) -> Result<StorageCandidatePage<StorageUnifiedSearchCandidate<StorageObject>>, StorageError>
    {
        let state = self.state.read().await;
        let mut ranked = state
            .objects
            .values()
            .filter(|object| {
                query.visibility().resources().is_none_or(|scope| {
                    scope.object_ids().contains(&object.id())
                        || scope.class_ids().contains(&object.class_id())
                        || scope.collection_ids().contains(&object.collection_id())
                })
            })
            .filter_map(|object| {
                let extended = query
                    .searches_extended_document()
                    .then(|| object.data().to_string());
                search_rank(
                    object.name(),
                    object.description(),
                    extended.as_deref(),
                    query.search_term(),
                )
                .map(|rank| (rank, object.clone()))
            })
            .collect::<Vec<_>>();
        ranked.sort_by(|(left_rank, left), (right_rank, right)| {
            right_rank
                .cmp(left_rank)
                .then_with(|| left.name().to_lowercase().cmp(&right.name().to_lowercase()))
                .then_with(|| left.id().cmp(&right.id()))
        });
        let limit = query.page_limit();
        let has_more = ranked.len() > limit.get();
        ranked.truncate(limit.get());
        let rows = ranked
            .into_iter()
            .map(|(rank, object)| {
                let cursor = StorageUnifiedSearchCursor::new(
                    rank,
                    object.name().to_lowercase(),
                    ResourceId::new(object.id().id()).expect("object id is positive"),
                );
                StorageUnifiedSearchCandidate::new(object, cursor)
            })
            .collect();
        StorageCandidatePage::try_new(rows, has_more, limit).map_err(invalid_contract_value)
    }
}

#[async_trait]
impl GroupStorage for MemoryStorage {
    async fn list_groups(
        &self,
        query: StorageGroupListQuery,
    ) -> Result<StoragePage<StorageIdentityGroup>, StorageError> {
        let options = query.into_options();
        let rows = self.state.read().await.groups.values().cloned().collect();
        page(rows, &options)
    }

    async fn get_group(&self, group_id: GroupId) -> Result<StorageIdentityGroup, StorageError> {
        self.state
            .read()
            .await
            .groups
            .get(&group_id.id())
            .cloned()
            .ok_or_else(|| {
                StorageError::not_found(format!("Group {} was not found", group_id.id()))
            })
    }

    async fn resolve_group_identity_scope_name(
        &self,
        group_id: GroupId,
    ) -> Result<String, StorageError> {
        let state = self.state.read().await;
        let group = state.groups.get(&group_id.id()).ok_or_else(|| {
            StorageError::not_found(format!("Group {} was not found", group_id.id()))
        })?;
        state
            .identity_scopes
            .get(&group.identity_scope_id().id())
            .map(|scope| scope.name().to_string())
            .ok_or_else(|| StorageError::internal("group identity scope is missing"))
    }

    async fn create_group(
        &self,
        command: StorageGroupCreate,
        context: &EventContext,
    ) -> Result<crate::StorageMutationOutcome<StorageIdentityGroup>, StorageError> {
        let (identity_scope, name, description) = command.into_parts();
        let identity_scope = identity_scope.unwrap_or_else(|| LOCAL_IDENTITY_SCOPE.to_string());
        let mut state = self.state.write().await;
        let scope = state
            .identity_scope_by_name(&identity_scope)
            .cloned()
            .ok_or_else(|| {
                StorageError::not_found(format!("Identity scope '{identity_scope}' was not found"))
            })?;
        if state
            .groups
            .values()
            .any(|group| group.identity_scope_id() == scope.id() && group.name() == name.as_str())
        {
            return Err(StorageError::conflict(format!(
                "Group '{name}' already exists in identity scope '{identity_scope}'"
            )));
        }
        let id = GroupId::new(state.next_group_id).expect("memory group id is positive");
        state.next_group_id += 1;
        let now = Utc::now();
        let metadata = StorageRecordMetadata::try_new(
            ResourceId::new(id.id()).expect("group resource id is positive"),
            now,
            now,
            ResourceRevision::INITIAL,
        )
        .map_err(invalid_contract_value)?;
        let group = StorageIdentityGroup::builder(
            metadata,
            name.clone(),
            description.unwrap_or_default(),
            scope.id(),
            LOCAL_PROVIDER_KIND,
        )
        .try_build()
        .map_err(invalid_contract_value)?;
        state.groups.insert(id.id(), group.clone());
        let receipt = state.append_simple_event(
            EntityType::Group,
            id.id(),
            Some(&name),
            Action::Created,
            context,
            format!("Group '{name}' created"),
        )?;
        Ok(StorageMutationOutcome::committed(group, receipt))
    }

    async fn update_group(
        &self,
        group_id: GroupId,
        update: StorageGroupUpdate,
        context: &EventContext,
    ) -> Result<crate::StorageMutationOutcome<StorageIdentityGroup>, StorageError> {
        let mut state = self.state.write().await;
        let current = state.groups.get(&group_id.id()).cloned().ok_or_else(|| {
            StorageError::not_found(format!("Group {} was not found", group_id.id()))
        })?;
        let Some(name) = update.into_name() else {
            return Ok(StorageMutationOutcome::unchanged(current));
        };
        if name == current.name() {
            return Ok(StorageMutationOutcome::unchanged(current));
        }
        if state.groups.values().any(|group| {
            group.id() != group_id
                && group.identity_scope_id() == current.identity_scope_id()
                && group.name() == name.as_str()
        }) {
            return Err(StorageError::conflict(format!(
                "Group '{name}' already exists"
            )));
        }
        let now = Utc::now();
        let revision = current
            .revision()
            .checked_advance()
            .map_err(|error| StorageError::internal(error.to_string()))?;
        let metadata = StorageRecordMetadata::try_new(
            ResourceId::new(group_id.id()).expect("group resource id is positive"),
            current.created_at(),
            now,
            revision,
        )
        .map_err(invalid_contract_value)?;
        let group = StorageIdentityGroup::builder(
            metadata,
            name.clone(),
            current.description(),
            current.identity_scope_id(),
            current.managed_by(),
        )
        .external_key(current.external_key().map(ToOwned::to_owned))
        .last_sync_attempted_at(current.last_sync_attempted_at())
        .last_sync_success_at(current.last_sync_success_at())
        .try_build()
        .map_err(invalid_contract_value)?;
        state.groups.insert(group_id.id(), group.clone());
        let receipt = state.append_simple_event(
            EntityType::Group,
            group_id.id(),
            Some(&name),
            Action::Updated,
            context,
            format!("Group '{name}' updated"),
        )?;
        Ok(StorageMutationOutcome::committed(group, receipt))
    }

    async fn delete_group(
        &self,
        group_id: GroupId,
        context: &EventContext,
    ) -> Result<crate::StorageMutationOutcome<usize>, StorageError> {
        let mut state = self.state.write().await;
        let Some(group) = state.groups.remove(&group_id.id()) else {
            return Ok(StorageMutationOutcome::unchanged(0));
        };
        state
            .memberships
            .retain(|(_, member_group_id), _| *member_group_id != group_id.id());
        let receipt = state.append_simple_event(
            EntityType::Group,
            group_id.id(),
            Some(group.name()),
            Action::Deleted,
            context,
            format!("Group '{}' deleted", group.name()),
        )?;
        Ok(StorageMutationOutcome::committed(1, receipt))
    }
}

#[async_trait]
impl PrincipalStorage for MemoryStorage {
    async fn get_principal(
        &self,
        principal_id: PrincipalId,
    ) -> Result<StoragePrincipal, StorageError> {
        self.state
            .read()
            .await
            .principals
            .get(&principal_id.id())
            .cloned()
            .ok_or_else(|| {
                StorageError::not_found(format!("Principal {} was not found", principal_id.id()))
            })
    }

    async fn get_principal_settings(
        &self,
        principal_id: PrincipalId,
    ) -> Result<StoragePrincipalSettings, StorageError> {
        let principal = self.get_principal(principal_id).await?;
        StoragePrincipalSettings::try_new(
            principal_id,
            principal.revision(),
            principal.settings().clone(),
        )
        .map_err(invalid_contract_value)
    }

    async fn update_principal_settings(
        &self,
        principal_id: PrincipalId,
        mutation: StoragePrincipalSettingsMutation,
        context: &EventContext,
    ) -> Result<crate::StorageMutationOutcome<StoragePrincipalSettings>, StorageError> {
        let mut state = self.state.write().await;
        let current = state
            .principals
            .get(&principal_id.id())
            .cloned()
            .ok_or_else(|| {
                StorageError::not_found(format!("Principal {} was not found", principal_id.id()))
            })?;
        let mut document = current.settings().clone();
        match mutation {
            StoragePrincipalSettingsMutation::Replace(replacement) => document = replacement,
            StoragePrincipalSettingsMutation::MergePatch(patch) => {
                json_patch::merge(&mut document, &patch);
            }
            StoragePrincipalSettingsMutation::JsonPatch(patch) => {
                let patch = serde_json::from_value::<json_patch::Patch>(patch)
                    .map_err(|error| StorageError::invalid_input(error.to_string()))?;
                json_patch::patch(&mut document, &patch)
                    .map_err(|error| StorageError::invalid_input(error.to_string()))?;
            }
            StoragePrincipalSettingsMutation::Reset => document = serde_json::json!({}),
        }
        if !document.is_object() {
            return Err(StorageError::invalid_input(
                "Principal settings must be a JSON object",
            ));
        }
        if document == *current.settings() {
            let settings =
                StoragePrincipalSettings::try_new(principal_id, current.revision(), document)
                    .map_err(invalid_contract_value)?;
            return Ok(StorageMutationOutcome::unchanged(settings));
        }
        let updated = advanced_principal(&current, current.name(), document.clone(), Utc::now())?;
        let settings =
            StoragePrincipalSettings::try_new(principal_id, updated.revision(), document)
                .map_err(invalid_contract_value)?;
        state.principals.insert(principal_id.id(), updated);
        let entity_type = if current.kind().is_human() {
            EntityType::User
        } else {
            EntityType::ServiceAccount
        };
        let receipt = state.append_simple_event(
            entity_type,
            principal_id.id(),
            Some(current.name()),
            Action::Updated,
            context,
            format!("Principal '{}' settings updated", current.name()),
        )?;
        Ok(StorageMutationOutcome::committed(settings, receipt))
    }
}

#[async_trait]
impl CollectionAuthorizationQueryStorage for MemoryStorage {
    async fn load_principal_collection_permissions(
        &self,
        query: StorageAuthorizationPrincipalCollectionQuery,
    ) -> Result<Vec<StorageAuthorizationGroupGrant>, StorageError> {
        let state = self.state.read().await;
        let group_ids = principal_group_ids(&state, query.principal_id());
        state
            .authorization_grants
            .values()
            .filter(|grant| {
                grant.collection_id() == query.collection_id()
                    && group_ids.contains(&grant.group_id())
            })
            .map(|grant| authorization_group_grant(&state, grant))
            .collect()
    }

    async fn list_all_principal_collection_permissions(
        &self,
        principal_id: PrincipalId,
    ) -> Result<Vec<StorageAuthorizationPolicySnapshotRow>, StorageError> {
        let state = self.state.read().await;
        let group_ids = principal_group_ids(&state, principal_id);
        state
            .authorization_grants
            .values()
            .filter(|grant| group_ids.contains(&grant.group_id()))
            .map(|grant| authorization_policy_row(&state, grant))
            .collect()
    }

    async fn list_principal_collection_permissions(
        &self,
        query: StorageAuthorizationPrincipalCollectionPageQuery,
    ) -> Result<StoragePage<StorageAuthorizationGroupGrant>, StorageError> {
        let state = self.state.read().await;
        let group_ids = principal_group_ids(&state, query.principal().principal_id());
        let rows = state
            .authorization_grants
            .values()
            .filter(|grant| {
                grant.collection_id() == query.principal().collection_id()
                    && group_ids.contains(&grant.group_id())
            })
            .map(|grant| authorization_group_grant(&state, grant))
            .collect::<Result<Vec<_>, _>>()?;
        page(rows, query.query_options())
    }

    async fn list_effective_principal_collection_permissions(
        &self,
        query: StorageAuthorizationPrincipalCollectionQuery,
    ) -> Result<Vec<StorageAuthorizationEffectiveGroupGrant>, StorageError> {
        let state = self.state.read().await;
        let group_ids = principal_group_ids(&state, query.principal_id());
        state
            .authorization_grants
            .values()
            .filter(|grant| {
                grant.collection_id() == query.collection_id()
                    && group_ids.contains(&grant.group_id())
            })
            .map(|grant| authorization_effective_group_grant(&state, grant))
            .collect()
    }

    async fn list_visible_collections(
        &self,
        query: StorageAuthorizationCollectionVisibilityQuery,
    ) -> Result<Vec<StorageAuthorizationCollection>, StorageError> {
        let (principal_id, is_admin, permission, scope) = query.into_parts();
        let (scope_permissions, scope_resources) = scope
            .map(StorageAuthenticationTokenScope::into_parts)
            .unwrap_or((None, None));
        if scope_permissions
            .as_ref()
            .is_some_and(|permissions| !permissions.contains(&permission))
        {
            return Ok(Vec::new());
        }
        let resources = scope_resources.map(StorageAuthenticationResourceScope::into_parts);
        let state = self.state.read().await;
        state
            .collections
            .values()
            .filter(|collection| {
                let resource_allowed =
                    resources
                        .as_ref()
                        .is_none_or(|(collection_ids, class_ids, object_ids)| {
                            collection_ids.contains(&collection.id())
                                || class_ids.iter().any(|class_id| {
                                    state.classes.get(&class_id.id()).is_some_and(|class| {
                                        class.collection_id() == collection.id()
                                    })
                                })
                                || object_ids.iter().any(|object_id| {
                                    state.objects.get(&object_id.id()).is_some_and(|object| {
                                        object.collection_id() == collection.id()
                                    })
                                })
                        });
                resource_allowed
                    && (is_admin
                        || principal_has_collection_permissions(
                            &state,
                            principal_id,
                            collection.id(),
                            &[permission],
                        ))
            })
            .map(authorization_collection)
            .collect()
    }

    async fn has_group_collection_permission(
        &self,
        query: StorageAuthorizationGroupCollectionQuery,
    ) -> Result<bool, StorageError> {
        Ok(self
            .state
            .read()
            .await
            .authorization_grants
            .get(&(query.collection_id().id(), query.group_id().id()))
            .is_some_and(|grant| grant.permissions().contains(&query.permission())))
    }

    async fn list_effective_group_collection_permissions(
        &self,
        collection_id: CollectionId,
        group_id: GroupId,
    ) -> Result<Vec<StorageAuthorizationEffectiveGroupGrant>, StorageError> {
        let state = self.state.read().await;
        state
            .authorization_grants
            .get(&(collection_id.id(), group_id.id()))
            .into_iter()
            .map(|grant| authorization_effective_group_grant(&state, grant))
            .collect()
    }

    async fn load_groups_with_collection_permission(
        &self,
        query: StorageAuthorizationCollectionGroupsQuery,
    ) -> Result<Vec<StorageAuthorizationGroup>, StorageError> {
        let state = self.state.read().await;
        state
            .authorization_grants
            .values()
            .filter(|grant| {
                grant.collection_id() == query.collection_id()
                    && grant.permissions().contains(&query.permission())
            })
            .map(|grant| {
                state
                    .groups
                    .get(&grant.group_id().id())
                    .ok_or_else(|| StorageError::internal("authorization grant group is missing"))
                    .and_then(authorization_group)
            })
            .collect()
    }

    async fn list_groups_with_collection_permission(
        &self,
        query: StorageAuthorizationCollectionGroupsPageQuery,
    ) -> Result<StoragePage<StorageAuthorizationGroup>, StorageError> {
        let state = self.state.read().await;
        let groups = query.groups();
        let rows = state
            .authorization_grants
            .values()
            .filter(|grant| {
                grant.collection_id() == groups.collection_id()
                    && grant.permissions().contains(&groups.permission())
            })
            .map(|grant| {
                state
                    .groups
                    .get(&grant.group_id().id())
                    .ok_or_else(|| StorageError::internal("authorization grant group is missing"))
                    .and_then(authorization_group)
            })
            .collect::<Result<Vec<_>, _>>()?;
        page(rows, query.query_options())
    }
}

#[async_trait]
impl RemoteTargetStorage for MemoryStorage {
    async fn get_remote_target(
        &self,
        target_id: RemoteTargetId,
    ) -> Result<StorageRemoteTarget, StorageError> {
        self.state
            .read()
            .await
            .remote_targets
            .get(&target_id.id())
            .cloned()
            .ok_or_else(|| {
                StorageError::not_found(format!("Remote target {} was not found", target_id.id()))
            })
    }

    async fn list_remote_targets(
        &self,
        query: StorageRemoteTargetListQuery,
    ) -> Result<StoragePage<StorageRemoteTarget>, StorageError> {
        let (collection_ids, options) = query.into_parts();
        let rows = self
            .state
            .read()
            .await
            .remote_targets
            .values()
            .filter(|target| collection_ids.contains(&target.collection_id()))
            .cloned()
            .collect();
        page(rows, &options)
    }

    async fn create_remote_target(
        &self,
        request: StorageRemoteTargetCreate,
    ) -> Result<StorageMutationOutcome<StorageRemoteTarget>, StorageError> {
        let (collection_id, name, definition, context) = request.into_parts();
        let mut state = self.state.write().await;
        if !state.collections.contains_key(&collection_id.id()) {
            return Err(StorageError::not_found(format!(
                "Collection {} was not found",
                collection_id.id()
            )));
        }
        if state.remote_targets.values().any(|target| {
            let (_, candidate_collection_id, candidate_name, _) = target.clone().into_parts();
            candidate_collection_id == collection_id && candidate_name == name
        }) {
            return Err(StorageError::conflict(format!(
                "A remote target named '{name}' already exists in collection {}",
                collection_id.id()
            )));
        }
        let id = state.next_remote_target_id;
        state.next_remote_target_id += 1;
        let now = Utc::now();
        let metadata = StorageRecordMetadata::try_new(
            ResourceId::new(id).expect("memory remote target id is positive"),
            now,
            now,
            ResourceRevision::INITIAL,
        )
        .map_err(invalid_contract_value)?;
        let target = StorageRemoteTarget::new(metadata, collection_id, &name, definition);
        state.remote_targets.insert(id, target.clone());
        let receipt = state.append_simple_event(
            EntityType::RemoteTarget,
            id,
            Some(&name),
            Action::Created,
            &context,
            format!("Remote target '{name}' created"),
        )?;
        state.append_history(
            MemoryHistoryValue::RemoteTarget(target.clone()),
            StorageHistoryOperation::Create,
            &context,
        )?;
        Ok(StorageMutationOutcome::committed(target, receipt))
    }

    async fn update_remote_target(
        &self,
        request: StorageRemoteTargetUpdate,
    ) -> Result<StorageMutationOutcome<StorageRemoteTarget>, StorageError> {
        let (target_id, patch, context) = request.into_parts();
        let mut state = self.state.write().await;
        let current = state
            .remote_targets
            .get(&target_id.id())
            .cloned()
            .ok_or_else(|| {
                StorageError::not_found(format!("Remote target {} was not found", target_id.id()))
            })?;
        let (current_metadata, current_collection_id, current_name, current_definition) =
            current.into_parts();
        let (current_description, current_transport, current_policy) =
            current_definition.into_parts();
        let transport = current_transport.into_parts();
        let (current_class_id, current_subject_types, current_enabled) =
            current_policy.into_parts();
        let patch = patch.into_parts();
        let collection_id = patch.collection_id().unwrap_or(current_collection_id);
        if !state.collections.contains_key(&collection_id.id()) {
            return Err(StorageError::not_found(format!(
                "Collection {} was not found",
                collection_id.id()
            )));
        }
        let name = patch.name().unwrap_or(&current_name).to_string();
        let description = patch
            .description()
            .unwrap_or(&current_description)
            .to_string();
        let class_id = patch.class_id().unwrap_or(current_class_id);
        let subject_types = patch
            .allowed_subject_types()
            .map(<[StorageRemoteTargetSubjectType]>::to_vec)
            .unwrap_or(current_subject_types);
        let enabled = patch.enabled().unwrap_or(current_enabled);
        let body_template = match patch.body_template() {
            Some(value) => value.map(ToOwned::to_owned),
            None => transport.body_template().map(ToOwned::to_owned),
        };
        let updated_transport = StorageRemoteTargetTransport::try_new(
            patch.method().unwrap_or(transport.method()),
            patch.url_template().unwrap_or(transport.url_template()),
            patch
                .headers_template()
                .cloned()
                .unwrap_or_else(|| transport.headers_template().clone()),
            body_template,
            patch
                .auth_config()
                .cloned()
                .unwrap_or_else(|| transport.auth_config().clone()),
            patch.timeout_ms().unwrap_or(transport.timeout_ms()),
        )
        .map_err(invalid_contract_value)?;
        let updated_policy = StorageRemoteTargetPolicy::try_new(class_id, subject_types, enabled)
            .map_err(invalid_contract_value)?;
        let metadata = StorageRecordMetadata::try_new(
            current_metadata.id(),
            current_metadata.created_at(),
            Utc::now(),
            current_metadata
                .revision()
                .checked_advance()
                .map_err(|error| StorageError::internal(error.to_string()))?,
        )
        .map_err(invalid_contract_value)?;
        let target = StorageRemoteTarget::new(
            metadata,
            collection_id,
            &name,
            StorageRemoteTargetDefinition::new(description, updated_transport, updated_policy),
        );
        state.remote_targets.insert(target_id.id(), target.clone());
        let receipt = state.append_simple_event(
            EntityType::RemoteTarget,
            target_id.id(),
            Some(&name),
            Action::Updated,
            &context,
            format!("Remote target '{name}' updated"),
        )?;
        state.append_history(
            MemoryHistoryValue::RemoteTarget(target.clone()),
            StorageHistoryOperation::Update,
            &context,
        )?;
        Ok(StorageMutationOutcome::committed(target, receipt))
    }

    async fn delete_remote_target(
        &self,
        request: StorageRemoteTargetDelete,
    ) -> Result<StorageMutationOutcome<()>, StorageError> {
        let (target_id, context) = request.into_parts();
        let mut state = self.state.write().await;
        let Some(target) = state.remote_targets.remove(&target_id.id()) else {
            return Ok(StorageMutationOutcome::unchanged(()));
        };
        let (_, _, name, _) = target.clone().into_parts();
        let receipt = state.append_simple_event(
            EntityType::RemoteTarget,
            target_id.id(),
            Some(&name),
            Action::Deleted,
            &context,
            format!("Remote target '{name}' deleted"),
        )?;
        state.append_history(
            MemoryHistoryValue::RemoteTarget(target),
            StorageHistoryOperation::Delete,
            &context,
        )?;
        Ok(StorageMutationOutcome::committed((), receipt))
    }

    async fn record_remote_target_invocation(
        &self,
        request: StorageRemoteTargetInvocation,
    ) -> Result<StorageMutationOutcome<()>, StorageError> {
        let (target_id, task_id, subject_type, subject_id, context) = request.into_parts();
        let mut state = self.state.write().await;
        let target = state
            .remote_targets
            .get(&target_id.id())
            .cloned()
            .ok_or_else(|| {
                StorageError::not_found(format!("Remote target {} was not found", target_id.id()))
            })?;
        let (_, _, name, _) = target.into_parts();
        let receipt = state.append_simple_event(
            EntityType::RemoteTarget,
            target_id.id(),
            Some(&name),
            Action::Invoked,
            &context,
            format!(
                "Remote target '{name}' invoked for {} {} by task {}",
                subject_type.as_str(),
                subject_id.id(),
                task_id.id()
            ),
        )?;
        Ok(StorageMutationOutcome::committed((), receipt))
    }
}

#[async_trait]
impl TaskQueueStorage for MemoryStorage {
    async fn create_task(
        &self,
        request: StorageTaskCreateRequest,
    ) -> Result<StorageTask, StorageError> {
        let mut state = self.state.write().await;
        if !state.principals.contains_key(&request.submitted_by().id()) {
            return Err(StorageError::not_found(format!(
                "Principal {} was not found",
                request.submitted_by().id()
            )));
        }
        let idempotency_key = request.idempotency_key().map(|key| key.as_str().to_owned());
        if let Some(key) = idempotency_key.as_deref()
            && let Some(existing) = state.tasks.values().find(|task| {
                task.submitted_by == Some(request.submitted_by())
                    && task.idempotency_key.as_deref() == Some(key)
            })
        {
            if existing.kind == request.kind()
                && existing.request_hash.as_deref() == request.request_hash()
            {
                return existing.projection();
            }
            return Err(StorageError::conflict(
                "Idempotency-Key is already in use for a different task submission",
            ));
        }
        let active_count = state
            .tasks
            .values()
            .filter(|task| {
                task.submitted_by == Some(request.submitted_by())
                    && task.kind == request.kind()
                    && !task.status.is_terminal()
            })
            .count();
        if active_count >= request.maximum_active_tasks() {
            return Err(StorageError::rate_limited(format!(
                "Too many active {} tasks for user ({active_count} >= {}); wait for queued or running tasks to finish",
                request.kind().as_str(),
                request.maximum_active_tasks()
            )));
        }
        let id = TaskId::new(state.next_task_id)
            .map_err(|error| StorageError::internal(error.to_string()))?;
        state.next_task_id += 1;
        let now = Utc::now();
        let record = MemoryTaskRecord {
            id,
            kind: request.kind(),
            status: StorageTaskStatus::Queued,
            submitted_by: Some(request.submitted_by()),
            idempotency_key,
            request_hash: request.request_hash().map(ToOwned::to_owned),
            request_payload: Some(request.request_payload().clone()),
            summary: None,
            progress: StorageTaskProgress::try_new(request.total_items(), 0, 0, 0)
                .map_err(invalid_contract_value)?,
            scope_snapshot: request.scope_snapshot().clone(),
            request_redacted_at: None,
            started_at: None,
            finished_at: None,
            created_at: now,
            updated_at: now,
            lease_expires_at: None,
            attempt_count: 0,
            initiator_principal_id: Some(request.submitted_by()),
            claim_token: None,
        };
        let task = record.projection()?;
        state.tasks.insert(id.id(), record);
        state.append_task_event_record(
            id,
            StorageTaskEventInput::new(StorageTaskStatus::Queued.as_str(), "Task queued"),
        )?;
        Ok(task)
    }

    async fn get_task_access(&self, task_id: TaskId) -> Result<StorageTaskAccess, StorageError> {
        let state = self.state.read().await;
        let task = state.tasks.get(&task_id.id()).ok_or_else(|| {
            StorageError::not_found(format!("Task {} was not found", task_id.id()))
        })?;
        let owner_group_id = task.submitted_by.and_then(|principal_id| {
            state
                .service_accounts
                .get(&principal_id.id())
                .map(StorageServiceAccount::owner_group_id)
        });
        Ok(StorageTaskAccess::new(task.projection()?, owner_group_id))
    }

    async fn list_tasks(
        &self,
        query: StorageTaskListQuery,
    ) -> Result<StoragePage<StorageTask>, StorageError> {
        let (submitted_by, kind, status, options) = query.into_parts();
        let state = self.state.read().await;
        let rows = state
            .tasks
            .values()
            .filter(|task| submitted_by.is_none_or(|value| task.submitted_by == Some(value)))
            .filter(|task| kind.is_none_or(|value| task.kind == value))
            .filter(|task| status.is_none_or(|value| task.status == value))
            .map(MemoryTaskRecord::projection)
            .collect::<Result<Vec<_>, _>>()?;
        page(rows, &options)
    }

    async fn list_task_events(
        &self,
        query: StorageTaskChildListQuery,
    ) -> Result<StoragePage<StorageTaskEvent>, StorageError> {
        let (task_id, options) = query.into_parts();
        let state = self.state.read().await;
        if !state.tasks.contains_key(&task_id.id()) {
            return Err(StorageError::not_found(format!(
                "Task {} was not found",
                task_id.id()
            )));
        }
        page(
            state
                .task_events
                .get(&task_id.id())
                .cloned()
                .unwrap_or_default(),
            &options,
        )
    }

    async fn list_import_task_results(
        &self,
        query: StorageTaskChildListQuery,
    ) -> Result<StoragePage<StorageImportTaskResult>, StorageError> {
        let (task_id, options) = query.into_parts();
        let state = self.state.read().await;
        if !state.tasks.contains_key(&task_id.id()) {
            return Err(StorageError::not_found(format!(
                "Task {} was not found",
                task_id.id()
            )));
        }
        page(
            state
                .import_task_results
                .get(&task_id.id())
                .cloned()
                .unwrap_or_default(),
            &options,
        )
    }

    async fn list_export_output_summaries(
        &self,
        task_ids: Vec<TaskId>,
    ) -> Result<Vec<StorageExportOutputSummary>, StorageError> {
        let state = self.state.read().await;
        let now = Utc::now();
        task_ids
            .into_iter()
            .filter_map(|task_id| state.export_outputs.get(&task_id.id()))
            .filter(|output| output.output_expires_at() > now)
            .map(export_output_summary)
            .collect()
    }

    async fn list_backup_output_summaries(
        &self,
        task_ids: Vec<TaskId>,
    ) -> Result<Vec<StorageBackupOutputSummary>, StorageError> {
        let state = self.state.read().await;
        let now = Utc::now();
        task_ids
            .into_iter()
            .filter_map(|task_id| state.backup_outputs.get(&task_id.id()))
            .filter(|output| output.output_expires_at() > now)
            .map(backup_output_summary)
            .collect()
    }

    async fn get_export_output_summary(
        &self,
        task_id: TaskId,
    ) -> Result<StorageTaskOutputLookup<StorageExportOutputSummary>, StorageError> {
        let state = self.state.read().await;
        let Some(output) = state.export_outputs.get(&task_id.id()) else {
            return Ok(StorageTaskOutputLookup::Missing);
        };
        if output.output_expires_at() <= Utc::now() {
            return Ok(StorageTaskOutputLookup::Expired {
                expires_at: output.output_expires_at(),
            });
        }
        Ok(StorageTaskOutputLookup::Available(export_output_summary(
            output,
        )?))
    }

    async fn get_backup_output_summary(
        &self,
        task_id: TaskId,
    ) -> Result<StorageTaskOutputLookup<StorageBackupOutputSummary>, StorageError> {
        let state = self.state.read().await;
        let Some(output) = state.backup_outputs.get(&task_id.id()) else {
            return Ok(StorageTaskOutputLookup::Missing);
        };
        if output.output_expires_at() <= Utc::now() {
            return Ok(StorageTaskOutputLookup::Expired {
                expires_at: output.output_expires_at(),
            });
        }
        Ok(StorageTaskOutputLookup::Available(backup_output_summary(
            output,
        )?))
    }

    async fn get_export_output(
        &self,
        task_id: TaskId,
    ) -> Result<StorageTaskOutputLookup<StorageExportOutput>, StorageError> {
        let state = self.state.read().await;
        let Some(output) = state.export_outputs.get(&task_id.id()) else {
            return Ok(StorageTaskOutputLookup::Missing);
        };
        if output.output_expires_at() <= Utc::now() {
            return Ok(StorageTaskOutputLookup::Expired {
                expires_at: output.output_expires_at(),
            });
        }
        Ok(StorageTaskOutputLookup::Available(output.clone()))
    }

    async fn get_backup_output(
        &self,
        task_id: TaskId,
    ) -> Result<StorageTaskOutputLookup<StorageBackupOutput>, StorageError> {
        let state = self.state.read().await;
        let Some(output) = state.backup_outputs.get(&task_id.id()) else {
            return Ok(StorageTaskOutputLookup::Missing);
        };
        if output.output_expires_at() <= Utc::now() {
            return Ok(StorageTaskOutputLookup::Expired {
                expires_at: output.output_expires_at(),
            });
        }
        Ok(StorageTaskOutputLookup::Available(output.clone()))
    }
}

#[async_trait]
impl TaskExecutionStorage for MemoryStorage {
    async fn claim_next_task(
        &self,
        lease_duration: StorageTaskLeaseDuration,
    ) -> Result<Option<StorageTaskClaim>, StorageError> {
        let mut state = self.state.write().await;
        let Some(task_id) = state
            .tasks
            .values()
            .find(|task| task.status == StorageTaskStatus::Queued)
            .map(|task| task.id)
        else {
            return Ok(None);
        };
        let now = Utc::now();
        let expires_at = now
            .checked_add_signed(Duration::milliseconds(lease_duration.milliseconds()))
            .ok_or_else(|| StorageError::invalid_input("Task lease duration is too large"))?;
        let claim_token = Uuid::new_v4().to_string();
        let task = state
            .tasks
            .get_mut(&task_id.id())
            .expect("selected task remains present");
        task.status = StorageTaskStatus::Validating;
        task.started_at = Some(now);
        task.updated_at = now;
        task.lease_expires_at = Some(expires_at);
        task.attempt_count += 1;
        task.claim_token = Some(claim_token.clone());
        let projection = task.projection()?;
        let lease = StorageTaskLease::new(task_id, StorageTaskClaimToken::new(claim_token));
        state.append_task_event_record(
            task_id,
            StorageTaskEventInput::new(
                StorageTaskStatus::Validating.as_str(),
                "Task claimed for validation",
            ),
        )?;
        StorageTaskClaim::try_new(projection, lease)
            .map(Some)
            .map_err(invalid_contract_value)
    }

    async fn renew_task_lease(
        &self,
        lease: StorageTaskLease,
        lease_duration: StorageTaskLeaseDuration,
    ) -> Result<bool, StorageError> {
        let mut state = self.state.write().await;
        let now = Utc::now();
        let Some(task) = state.tasks.get_mut(&lease.task_id().id()) else {
            return Ok(false);
        };
        if !task.status.is_active() || !task.lease_matches(&lease) {
            return Ok(false);
        }
        task.lease_expires_at = Some(
            now.checked_add_signed(Duration::milliseconds(lease_duration.milliseconds()))
                .ok_or_else(|| StorageError::invalid_input("Task lease duration is too large"))?,
        );
        task.updated_at = now;
        Ok(true)
    }

    async fn recover_expired_task_leases(
        &self,
        batch_size: usize,
    ) -> Result<Vec<StorageTask>, StorageError> {
        if batch_size == 0 {
            return Ok(Vec::new());
        }
        let mut state = self.state.write().await;
        let now = Utc::now();
        let task_ids = state
            .tasks
            .values()
            .filter(|task| {
                task.status.is_active()
                    && task
                        .lease_expires_at
                        .is_none_or(|expires_at| expires_at <= now)
            })
            .take(batch_size)
            .map(|task| task.id)
            .collect::<Vec<_>>();
        let mut recovered = Vec::with_capacity(task_ids.len());
        for task_id in task_ids {
            let task = state
                .tasks
                .get_mut(&task_id.id())
                .expect("selected task remains present");
            task.status = StorageTaskStatus::Failed;
            task.summary = Some("Task worker lease expired".to_string());
            task.finished_at = Some(now);
            task.request_payload = None;
            task.request_redacted_at = Some(now);
            task.lease_expires_at = None;
            task.claim_token = None;
            task.updated_at = now;
            recovered.push(task.projection()?);
            state.append_task_event_record(
                task_id,
                StorageTaskEventInput::new(
                    StorageTaskStatus::Failed.as_str(),
                    "Task worker lease expired",
                ),
            )?;
        }
        Ok(recovered)
    }

    async fn append_task_event(&self, event: StorageTaskEventAppend) -> Result<(), StorageError> {
        let (lease, event) = event.into_parts();
        let mut state = self.state.write().await;
        let task = state
            .tasks
            .get(&lease.task_id().id())
            .ok_or_else(invalid_task_lease)?;
        if !task.status.is_active() || !task.lease_matches(&lease) {
            return Err(invalid_task_lease());
        }
        state.append_task_event_record(lease.task_id(), event)
    }

    async fn update_task_state(
        &self,
        update: StorageTaskActiveUpdate,
    ) -> Result<StorageTask, StorageError> {
        let (lease, status, summary, counts, started_at) = update.into_parts();
        let mut state = self.state.write().await;
        let now = Utc::now();
        let task = state
            .tasks
            .get_mut(&lease.task_id().id())
            .ok_or_else(invalid_task_lease)?;
        if !task.status.is_active() || !task.lease_matches(&lease) {
            return Err(invalid_task_lease());
        }
        task.status = status;
        task.summary = summary;
        task.progress = StorageTaskProgress::try_new(
            task.progress.total(),
            counts.processed(),
            counts.succeeded(),
            counts.failed(),
        )
        .map_err(invalid_contract_value)?;
        task.started_at = started_at.or(task.started_at).or(Some(now));
        task.updated_at = now;
        task.projection()
    }

    async fn complete_task(
        &self,
        completion: StorageTaskCompletion,
    ) -> Result<StorageTask, StorageError> {
        let (expected_kind, update, event, artifact) = completion.into_parts();
        let (lease, status, summary, counts, started_at) = update.into_parts();
        let mut state = self.state.write().await;
        let stored = state
            .tasks
            .get(&lease.task_id().id())
            .ok_or_else(invalid_task_lease)?;
        if stored.kind != expected_kind {
            return Err(StorageError::invalid_input(format!(
                "Task completion kind '{}' does not match stored task kind '{}'",
                expected_kind.as_str(),
                stored.kind.as_str()
            )));
        }
        if !stored.status.is_active() || !stored.lease_matches(&lease) {
            return Err(invalid_task_lease());
        }
        let now = Utc::now();
        match artifact {
            StorageTaskCompletionArtifact::None | StorageTaskCompletionArtifact::RemoteCall(_) => {}
            StorageTaskCompletionArtifact::Export(artifact) => {
                let (identity, content, report, output_expires_at, durations) =
                    artifact.into_parts();
                let (template_name, content_type) = identity.into_parts();
                let (json_output, text_output) = content.into_parts();
                let (metadata, warnings, warning_count, truncated) = report.into_parts();
                let output = StorageExportOutput::builder(
                    lease.task_id(),
                    content_type,
                    metadata,
                    warnings,
                    output_expires_at,
                    now,
                )
                .template_name(template_name)
                .output(json_output, text_output)
                .warning_state(warning_count, truncated)
                .durations(durations)
                .try_build()
                .map_err(invalid_contract_value)?;
                state.export_outputs.insert(lease.task_id().id(), output);
            }
            StorageTaskCompletionArtifact::Backup(artifact) => {
                let (document, byte_size, sha256, output_expires_at) = artifact.into_parts();
                let output = StorageBackupOutput::try_new(
                    lease.task_id(),
                    document,
                    byte_size,
                    sha256,
                    output_expires_at,
                    now,
                )
                .map_err(invalid_contract_value)?;
                state.backup_outputs.insert(lease.task_id().id(), output);
            }
        }
        let task = state
            .tasks
            .get_mut(&lease.task_id().id())
            .expect("validated task remains present");
        task.status = status;
        task.summary = summary;
        task.progress = StorageTaskProgress::try_new(
            task.progress.total(),
            counts.processed(),
            counts.succeeded(),
            counts.failed(),
        )
        .map_err(invalid_contract_value)?;
        task.started_at = started_at.or(task.started_at).or(Some(now));
        task.finished_at = Some(now);
        task.request_payload = None;
        task.request_redacted_at = Some(now);
        task.lease_expires_at = None;
        task.claim_token = None;
        task.updated_at = now;
        let projection = task.projection()?;
        state.append_task_event_record(lease.task_id(), event)?;
        Ok(projection)
    }

    async fn fail_task(&self, failure: StorageTaskFailure) -> Result<StorageTask, StorageError> {
        let (lease, summary, event) = failure.into_parts();
        let mut state = self.state.write().await;
        let now = Utc::now();
        let task = state
            .tasks
            .get_mut(&lease.task_id().id())
            .ok_or_else(invalid_task_lease)?;
        if !task.status.is_active() || !task.lease_matches(&lease) {
            return Err(invalid_task_lease());
        }
        let succeeded = task.progress.succeeded();
        let processed = task.progress.processed().max(1);
        task.status = StorageTaskStatus::Failed;
        task.summary = Some(summary);
        task.progress =
            StorageTaskProgress::try_new(task.progress.total(), processed, succeeded, 1)
                .map_err(invalid_contract_value)?;
        task.started_at = task.started_at.or(Some(now));
        task.finished_at = Some(now);
        task.request_payload = None;
        task.request_redacted_at = Some(now);
        task.lease_expires_at = None;
        task.claim_token = None;
        task.updated_at = now;
        let projection = task.projection()?;
        state.append_task_event_record(lease.task_id(), event)?;
        Ok(projection)
    }

    async fn purge_expired_export_outputs(&self) -> Result<usize, StorageError> {
        let mut state = self.state.write().await;
        let before = state.export_outputs.len();
        let now = Utc::now();
        state
            .export_outputs
            .retain(|_, output| output.output_expires_at() > now);
        Ok(before - state.export_outputs.len())
    }

    async fn purge_expired_backup_outputs(&self) -> Result<usize, StorageError> {
        let mut state = self.state.write().await;
        let before = state.backup_outputs.len();
        let now = Utc::now();
        state
            .backup_outputs
            .retain(|_, output| output.output_expires_at() > now);
        Ok(before - state.backup_outputs.len())
    }
}

#[async_trait]
impl BackupSnapshotStorage for MemoryStorage {
    async fn capture_backup_snapshot(
        &self,
        include_history: bool,
    ) -> Result<StorageBackupSnapshot, StorageError> {
        let state = self.state.read().await;
        let mut state_sections = StorageBackupStateSection::ALL
            .iter()
            .copied()
            .map(|section| (section, Vec::new()))
            .collect::<StorageBackupStateSections>();
        state_sections.insert(
            StorageBackupStateSection::Collections,
            state
                .collections
                .values()
                .map(|collection| {
                    memory_backup_row(serde_json::json!({
                        "id": collection.id().id(),
                        "name": collection.name(),
                        "description": collection.description(),
                        "created_at": collection.created_at(),
                        "updated_at": collection.updated_at(),
                        "parent_collection_id": collection.parent_collection_id().map(CollectionId::id),
                        "revision": collection.revision().get(),
                    }))
                })
                .collect::<Result<Vec<_>, _>>()?,
        );
        state_sections.insert(
            StorageBackupStateSection::Classes,
            state
                .classes
                .values()
                .map(|class| {
                    memory_backup_row(serde_json::json!({
                        "id": class.id().id(),
                        "name": class.name(),
                        "collection_id": class.collection_id().id(),
                        "json_schema": class.json_schema(),
                        "validate_schema": class.validates_schema(),
                        "description": class.description(),
                        "created_at": class.created_at(),
                        "updated_at": class.updated_at(),
                        "revision": class.revision().get(),
                    }))
                })
                .collect::<Result<Vec<_>, _>>()?,
        );
        state_sections.insert(
            StorageBackupStateSection::Objects,
            state
                .objects
                .values()
                .map(|object| {
                    memory_backup_row(serde_json::json!({
                        "id": object.id().id(),
                        "name": object.name(),
                        "collection_id": object.collection_id().id(),
                        "class_id": object.class_id().id(),
                        "data": object.data(),
                        "description": object.description(),
                        "created_at": object.created_at(),
                        "updated_at": object.updated_at(),
                        "revision": object.revision().get(),
                    }))
                })
                .collect::<Result<Vec<_>, _>>()?,
        );
        state_sections.insert(
            StorageBackupStateSection::ClassRelations,
            state
                .class_relations
                .values()
                .map(|relation| {
                    memory_backup_row(serde_json::json!({
                        "id": relation.metadata().id().id(),
                        "from_class_id": relation.from_class_id().id(),
                        "to_class_id": relation.to_class_id().id(),
                        "forward_template_alias": relation.forward_template_alias(),
                        "reverse_template_alias": relation.reverse_template_alias(),
                        "from_max_relations": relation.from_max_relations(),
                        "to_max_relations": relation.to_max_relations(),
                        "created_at": relation.metadata().created_at(),
                        "updated_at": relation.metadata().updated_at(),
                        "revision": relation.metadata().revision().get(),
                    }))
                })
                .collect::<Result<Vec<_>, _>>()?,
        );
        state_sections.insert(
            StorageBackupStateSection::ObjectRelations,
            state
                .object_relations
                .values()
                .map(|relation| {
                    memory_backup_row(serde_json::json!({
                        "id": relation.metadata().id().id(),
                        "from_object_id": relation.from_object_id().id(),
                        "to_object_id": relation.to_object_id().id(),
                        "class_relation_id": relation.class_relation_id().id(),
                        "created_at": relation.metadata().created_at(),
                        "updated_at": relation.metadata().updated_at(),
                        "revision": relation.metadata().revision().get(),
                    }))
                })
                .collect::<Result<Vec<_>, _>>()?,
        );
        let history_sections = include_history.then(|| {
            StorageBackupHistorySection::ALL
                .iter()
                .copied()
                .map(|section| (section, Vec::new()))
                .collect()
        });
        StorageBackupSnapshot::try_new(state_sections, history_sections)
            .map_err(invalid_contract_value)
    }
}

fn memory_backup_row(value: serde_json::Value) -> Result<StorageBackupRow, StorageError> {
    StorageBackupRow::try_from_value(value).map_err(invalid_contract_value)
}

#[async_trait]
impl RestoreStorage for MemoryStorage {
    async fn stage_restore(
        &self,
        request: StorageRestoreStageCreate,
    ) -> Result<StorageRestoreJob, StorageError> {
        let (initiator, document, artifact, capability_hash, validation_summary, expires_at) =
            request.into_parts();
        let mut state = self.state.write().await;
        let id = RestoreJobId::new(state.next_restore_job_id)
            .map_err(|error| StorageError::internal(error.to_string()))?;
        state.next_restore_job_id += 1;
        let now = Utc::now();
        let timestamps = StorageRestoreTimestamps::try_new(expires_at, None, None, now, now)
            .map_err(invalid_contract_value)?;
        let summary = StorageRestoreJobSummary::try_new(
            id,
            StorageRestoreJobStatus::Validated,
            initiator,
            artifact,
            None,
            timestamps,
        )
        .map_err(invalid_contract_value)?;
        let job = StorageRestoreJob::try_new(summary, document, capability_hash)
            .map_err(invalid_contract_value)?;
        state.restore_jobs.insert(
            id.id(),
            MemoryRestoreRecord {
                job: job.clone(),
                validation_summary,
            },
        );
        Ok(job)
    }

    async fn get_restore_job(
        &self,
        job_id: RestoreJobId,
    ) -> Result<StorageRestoreJob, StorageError> {
        self.state
            .read()
            .await
            .restore_jobs
            .get(&job_id.id())
            .map(|record| record.job.clone())
            .ok_or_else(|| {
                StorageError::not_found(format!("Restore job {} was not found", job_id.id()))
            })
    }

    async fn get_restore_status(
        &self,
        job_id: RestoreJobId,
    ) -> Result<StorageRestoreStatus, StorageError> {
        let state = self.state.read().await;
        let record = state.restore_jobs.get(&job_id.id()).ok_or_else(|| {
            StorageError::not_found(format!("Restore job {} was not found", job_id.id()))
        })?;
        let (summary, _, capability_hash) = record.job.clone().into_parts();
        StorageRestoreStatus::try_new(summary, capability_hash, record.validation_summary.clone())
            .map_err(invalid_contract_value)
    }

    async fn expire_restore_stage(&self, job_id: RestoreJobId) -> Result<bool, StorageError> {
        let mut state = self.state.write().await;
        let Some(current) = state.restore_jobs.get(&job_id.id()).cloned() else {
            return Err(StorageError::not_found(format!(
                "Restore job {} was not found",
                job_id.id()
            )));
        };
        if current.job.summary().status() != StorageRestoreJobStatus::Validated
            || current.job.summary().timestamps().expires_at() > Utc::now()
        {
            return Ok(false);
        }
        let expired = transition_restore_record(
            &current,
            StorageRestoreJobStatus::Expired,
            None,
            None,
            None,
            true,
        )?;
        state.restore_jobs.insert(job_id.id(), expired);
        Ok(true)
    }

    async fn start_restore_draining(
        &self,
        job_id: RestoreJobId,
    ) -> Result<DateTime<Utc>, StorageError> {
        let mut state = self.state.write().await;
        if !state.maintenance_state.is_normal() {
            return Err(StorageError::conflict(
                "Another maintenance operation is already active",
            ));
        }
        let current = state
            .restore_jobs
            .get(&job_id.id())
            .cloned()
            .ok_or_else(|| {
                StorageError::not_found(format!("Restore job {} was not found", job_id.id()))
            })?;
        if current.job.summary().status() != StorageRestoreJobStatus::Validated {
            return Err(StorageError::conflict(
                "Only a validated restore can be confirmed",
            ));
        }
        if current.job.summary().timestamps().expires_at() <= Utc::now() {
            return Err(StorageError::conflict("The staged restore has expired"));
        }
        let confirmed_at = Utc::now();
        let confirmed = transition_restore_record(
            &current,
            StorageRestoreJobStatus::Confirmed,
            None,
            Some(confirmed_at),
            None,
            false,
        )?;
        state.restore_jobs.insert(job_id.id(), confirmed);
        state.maintenance_state = MaintenanceState::Draining;
        state.maintenance_restore_job_id = Some(job_id);
        state.maintenance_generation = state.maintenance_generation.saturating_add(1);
        state.restore_instances.clear();
        Ok(confirmed_at)
    }

    async fn apply_restore(
        &self,
        request: StorageRestoreApply,
    ) -> Result<StorageRestoreCompletion, StorageError> {
        let (job_id, _document) = request.into_parts();
        let mut state = self.state.write().await;
        if state.maintenance_state != MaintenanceState::Draining
            || state.maintenance_restore_job_id != Some(job_id)
        {
            return Err(StorageError::conflict(
                "The restore job does not own draining maintenance",
            ));
        }
        let current = state
            .restore_jobs
            .get(&job_id.id())
            .cloned()
            .ok_or_else(|| {
                StorageError::not_found(format!("Restore job {} was not found", job_id.id()))
            })?;
        if current.job.summary().status() != StorageRestoreJobStatus::Confirmed {
            return Err(StorageError::conflict("The restore job is not confirmed"));
        }
        let timestamp_parts = current.job.summary().timestamps().into_parts();
        let started_at = timestamp_parts
            .confirmed_at()
            .ok_or_else(|| StorageError::internal("confirmed restore timestamp is missing"))?;
        let finished_at = Utc::now();
        let succeeded = transition_restore_record(
            &current,
            StorageRestoreJobStatus::Succeeded,
            None,
            Some(started_at),
            Some(finished_at),
            false,
        )?;
        state.restore_jobs.insert(job_id.id(), succeeded);
        state.maintenance_state = MaintenanceState::Normal;
        state.maintenance_restore_job_id = None;
        state.restore_instances.clear();
        StorageRestoreCompletion::try_new(started_at, finished_at).map_err(invalid_contract_value)
    }

    async fn fail_restore_and_resume(
        &self,
        request: StorageRestoreFailure,
    ) -> Result<(), StorageError> {
        let (job_id, error) = request.into_parts();
        let mut state = self.state.write().await;
        let current = state
            .restore_jobs
            .get(&job_id.id())
            .cloned()
            .ok_or_else(|| {
                StorageError::not_found(format!("Restore job {} was not found", job_id.id()))
            })?;
        if matches!(
            current.job.summary().status(),
            StorageRestoreJobStatus::Succeeded
                | StorageRestoreJobStatus::Failed
                | StorageRestoreJobStatus::Expired
        ) {
            return Err(StorageError::conflict(
                "The restore job is already terminal",
            ));
        }
        let timestamp_parts = current.job.summary().timestamps().into_parts();
        let failed = transition_restore_record(
            &current,
            StorageRestoreJobStatus::Failed,
            Some(error),
            timestamp_parts.confirmed_at(),
            Some(Utc::now()),
            true,
        )?;
        state.restore_jobs.insert(job_id.id(), failed);
        if state.maintenance_restore_job_id == Some(job_id) {
            state.maintenance_state = MaintenanceState::Normal;
            state.maintenance_restore_job_id = None;
            state.restore_instances.clear();
        }
        Ok(())
    }

    async fn get_restore_coordinator_snapshot(
        &self,
    ) -> Result<StorageRestoreCoordinatorSnapshot, StorageError> {
        let state = self.state.read().await;
        Ok(StorageRestoreCoordinatorSnapshot::new(
            state.maintenance_state,
            state.maintenance_restore_job_id,
            Utc::now(),
        ))
    }

    async fn resume_maintenance_without_restore(&self) -> Result<(), StorageError> {
        let mut state = self.state.write().await;
        if state.maintenance_restore_job_id.is_none() {
            state.maintenance_state = MaintenanceState::Normal;
            state.restore_instances.clear();
        }
        Ok(())
    }

    async fn resume_terminal_restore(&self, job_id: RestoreJobId) -> Result<(), StorageError> {
        let mut state = self.state.write().await;
        let status = state
            .restore_jobs
            .get(&job_id.id())
            .map(|record| record.job.summary().status())
            .ok_or_else(|| {
                StorageError::not_found(format!("Restore job {} was not found", job_id.id()))
            })?;
        if matches!(
            status,
            StorageRestoreJobStatus::Succeeded
                | StorageRestoreJobStatus::Failed
                | StorageRestoreJobStatus::Expired
        ) && state.maintenance_restore_job_id == Some(job_id)
        {
            state.maintenance_state = MaintenanceState::Normal;
            state.maintenance_restore_job_id = None;
            state.restore_instances.clear();
        }
        Ok(())
    }

    async fn tick_restore_coordinator(
        &self,
        instance_id: Uuid,
        local_work_is_idle: &(dyn Fn() -> bool + Send + Sync),
        expire_validated_jobs: bool,
    ) -> Result<StorageRestoreCoordinatorSnapshot, StorageError> {
        let mut state = self.state.write().await;
        let now = Utc::now();
        if expire_validated_jobs {
            let expired_ids = state
                .restore_jobs
                .iter()
                .filter_map(|(id, record)| {
                    (record.job.summary().status() == StorageRestoreJobStatus::Validated
                        && record.job.summary().timestamps().expires_at() <= now)
                        .then_some(*id)
                })
                .collect::<Vec<_>>();
            for id in expired_ids {
                let current = state
                    .restore_jobs
                    .get(&id)
                    .cloned()
                    .expect("restore selected for expiry exists");
                state.restore_jobs.insert(
                    id,
                    transition_restore_record(
                        &current,
                        StorageRestoreJobStatus::Expired,
                        None,
                        None,
                        None,
                        true,
                    )?,
                );
            }
        }
        let drained = if state.maintenance_state.is_normal() {
            false
        } else {
            local_work_is_idle()
        };
        let generation = state.maintenance_generation;
        state.restore_instances.insert(
            instance_id,
            MemoryRestoreInstance {
                generation,
                drained,
                heartbeat_at: now,
            },
        );
        Ok(StorageRestoreCoordinatorSnapshot::new(
            state.maintenance_state,
            state.maintenance_restore_job_id,
            now,
        ))
    }

    async fn get_restore_drain_state(
        &self,
        heartbeat_cutoff: DateTime<Utc>,
    ) -> Result<StorageRestoreDrainState, StorageError> {
        let state = self.state.read().await;
        let instances = state
            .restore_instances
            .iter()
            .filter(|(_, instance)| {
                instance.heartbeat_at >= heartbeat_cutoff
                    && instance.generation == state.maintenance_generation
            })
            .map(|(id, instance)| {
                StorageRestoreInstance::try_new(*id, instance.generation, instance.drained)
                    .map_err(invalid_contract_value)
            })
            .collect::<Result<Vec<_>, _>>()?;
        StorageRestoreDrainState::try_new(state.maintenance_generation, instances)
            .map_err(invalid_contract_value)
    }

    async fn remove_restore_instance(&self, instance_id: Uuid) -> Result<(), StorageError> {
        self.state
            .write()
            .await
            .restore_instances
            .remove(&instance_id);
        Ok(())
    }
}

#[async_trait]
impl ImportStorage for MemoryStorage {
    async fn get_import_root_collection(&self) -> Result<StorageCollection, StorageError> {
        self.get_collection(CollectionId::new(ROOT_COLLECTION_ID).expect("root id is valid"))
            .await
    }

    async fn get_import_collection_by_id(
        &self,
        collection_id: CollectionId,
    ) -> Result<Option<StorageCollection>, StorageError> {
        Ok(self
            .state
            .read()
            .await
            .collections
            .get(&collection_id.id())
            .cloned())
    }

    async fn get_import_collection_by_key(
        &self,
        key: &StorageImportCollectionKey,
    ) -> Result<Option<StorageCollection>, StorageError> {
        let parts = key.clone().into_parts();
        let state = self.state.read().await;
        let mut candidates = state
            .collections
            .values()
            .filter(|collection| collection.name() == parts.name)
            .cloned()
            .collect::<Vec<_>>();
        if let Some(path) = parts.path {
            candidates.retain(|collection| {
                let mut names = Vec::new();
                let mut parent = collection.parent_collection_id();
                while let Some(parent_id) = parent {
                    let Some(ancestor) = state.collections.get(&parent_id.id()) else {
                        return false;
                    };
                    names.push(ancestor.name().to_string());
                    parent = ancestor.parent_collection_id();
                }
                names.reverse();
                names == path || (collection.id().id() == ROOT_COLLECTION_ID && path.is_empty())
            });
        }
        match candidates.as_slice() {
            [] => Ok(None),
            [collection] => Ok(Some(collection.clone())),
            _ => Err(StorageError::conflict(format!(
                "Import collection key '{}' is ambiguous",
                parts.name
            ))),
        }
    }

    async fn list_import_collections_by_name(
        &self,
        name: &str,
    ) -> Result<Vec<StorageCollection>, StorageError> {
        Ok(self
            .state
            .read()
            .await
            .collections
            .values()
            .filter(|collection| collection.name() == name)
            .cloned()
            .collect())
    }

    async fn get_import_collection_child_by_name(
        &self,
        parent_collection_id: CollectionId,
        name: &str,
    ) -> Result<Option<StorageCollection>, StorageError> {
        Ok(self
            .state
            .read()
            .await
            .collections
            .values()
            .find(|collection| {
                collection.parent_collection_id() == Some(parent_collection_id)
                    && collection.name() == name
            })
            .cloned())
    }

    async fn get_import_class_by_name(
        &self,
        collection_id: CollectionId,
        name: &str,
    ) -> Result<Option<StorageClass>, StorageError> {
        Ok(self
            .state
            .read()
            .await
            .classes
            .values()
            .find(|class| class.collection_id() == collection_id && class.name() == name)
            .cloned())
    }

    async fn list_import_classes_by_names(
        &self,
        collection_id: CollectionId,
        names: &[String],
    ) -> Result<Vec<StorageClass>, StorageError> {
        Ok(self
            .state
            .read()
            .await
            .classes
            .values()
            .filter(|class| {
                class.collection_id() == collection_id
                    && names.iter().any(|name| name == class.name())
            })
            .cloned()
            .collect())
    }

    async fn get_import_object_by_name(
        &self,
        class_id: ClassId,
        name: &str,
    ) -> Result<Option<StorageObject>, StorageError> {
        Ok(self
            .state
            .read()
            .await
            .objects
            .values()
            .find(|object| object.class_id() == class_id && object.name() == name)
            .cloned())
    }

    async fn list_import_objects_by_names(
        &self,
        class_id: ClassId,
        names: &[String],
    ) -> Result<Vec<StorageObject>, StorageError> {
        Ok(self
            .state
            .read()
            .await
            .objects
            .values()
            .filter(|object| {
                object.class_id() == class_id && names.iter().any(|name| name == object.name())
            })
            .cloned()
            .collect())
    }

    async fn has_import_class_relation(
        &self,
        left_class_id: ClassId,
        right_class_id: ClassId,
    ) -> Result<bool, StorageError> {
        Ok(self
            .state
            .read()
            .await
            .class_relations
            .values()
            .any(|relation| {
                relation.from_class_id() == left_class_id
                    && relation.to_class_id() == right_class_id
            }))
    }

    async fn has_import_object_relation(
        &self,
        left_object_id: ObjectId,
        right_object_id: ObjectId,
    ) -> Result<bool, StorageError> {
        Ok(self
            .state
            .read()
            .await
            .object_relations
            .values()
            .any(|relation| {
                relation.from_object_id() == left_object_id
                    && relation.to_object_id() == right_object_id
            }))
    }

    async fn has_import_group(
        &self,
        identity_scope: &str,
        group_name: &str,
    ) -> Result<bool, StorageError> {
        let state = self.state.read().await;
        let scope_ids = state
            .identity_scopes
            .values()
            .filter(|scope| scope.name() == identity_scope)
            .map(|scope| scope.id())
            .collect::<BTreeSet<_>>();
        Ok(state.groups.values().any(|group| {
            group.name() == group_name && scope_ids.contains(&group.identity_scope_id())
        }))
    }

    async fn preflight_import(
        &self,
        plan: StorageImportPlan,
        mode: StorageImportMode,
    ) -> Result<StorageImportPreflight, StorageError> {
        let scratch = Self {
            state: Arc::new(RwLock::new(self.state.read().await.clone())),
        };
        let mut references = BTreeMap::new();
        let mut items = Vec::new();
        let mut aborted = false;
        for item in plan.into_items() {
            let (index, operation) = item.into_parts();
            match scratch
                .apply_import_operation(operation, &mut references)
                .await
            {
                Ok(revision) => items.push(StorageImportPreflightItem::success(index, revision)),
                Err(error) => {
                    items.push(StorageImportPreflightItem::failure(index, None, error));
                    if mode.atomicity() == StorageImportAtomicity::Strict {
                        aborted = true;
                        break;
                    }
                }
            }
        }
        Ok(StorageImportPreflight::new(items, aborted))
    }

    async fn apply_import_strict(&self, plan: StorageImportPlan) -> Result<(), StorageError> {
        let scratch = Self {
            state: Arc::new(RwLock::new(self.state.read().await.clone())),
        };
        let mut references = BTreeMap::new();
        for item in plan.into_items() {
            scratch
                .apply_import_operation(item.into_parts().1, &mut references)
                .await?;
        }
        *self.state.write().await = scratch.state.read().await.clone();
        Ok(())
    }

    async fn apply_import_best_effort(
        &self,
        plan: StorageImportPlan,
        _mode: StorageImportMode,
    ) -> Result<StorageImportApply, StorageError> {
        let mut references = BTreeMap::new();
        let mut items = Vec::new();
        for item in plan.into_items() {
            let (index, operation) = item.into_parts();
            match self
                .apply_import_operation(operation, &mut references)
                .await
            {
                Ok(_) => items.push(StorageImportApplyItem::success(index)),
                Err(error) => items.push(StorageImportApplyItem::failure(index, error)),
            }
        }
        Ok(StorageImportApply::new(items, false))
    }

    async fn record_import_results(
        &self,
        results: Vec<StorageImportResult>,
    ) -> Result<(), StorageError> {
        let mut state = self.state.write().await;
        for result in &results {
            let task_id = result.clone().into_parts().0;
            let task = state.tasks.get(&task_id.id()).ok_or_else(|| {
                StorageError::not_found(format!("Task {} was not found", task_id.id()))
            })?;
            if task.kind != StorageTaskKind::Import {
                return Err(StorageError::invalid_input(format!(
                    "Task {} is not an import task",
                    task_id.id()
                )));
            }
        }
        for result in results {
            let (task_id, item_ref, entity_kind, action, identifier, outcome, error, details) =
                result.into_parts();
            let id = ImportTaskResultId::new(state.next_import_result_id)
                .map_err(|value| StorageError::internal(value.to_string()))?;
            state.next_import_result_id += 1;
            let stored = StorageImportTaskResult::builder(
                id,
                task_id,
                entity_kind,
                action,
                outcome,
                Utc::now(),
            )
            .item_ref(item_ref)
            .identifier(identifier)
            .error(error)
            .details(details)
            .build();
            state
                .import_task_results
                .entry(task_id.id())
                .or_default()
                .push(stored);
        }
        Ok(())
    }
}

#[async_trait]
impl ExportTemplateStorage for MemoryStorage {
    async fn get_export_template(
        &self,
        template_id: ExportTemplateId,
    ) -> Result<StorageExportTemplate, StorageError> {
        self.state
            .read()
            .await
            .export_templates
            .get(&template_id.id())
            .cloned()
            .ok_or_else(|| {
                StorageError::not_found(format!(
                    "Export template {} was not found",
                    template_id.id()
                ))
            })
    }

    async fn list_export_templates(
        &self,
        query: StorageExportTemplateListQuery,
    ) -> Result<StoragePage<StorageExportTemplate>, StorageError> {
        let (collection_ids, options) = query.into_parts();
        let rows = self
            .state
            .read()
            .await
            .export_templates
            .values()
            .filter(|template| {
                let (_, collection_id, _, _) = (*template).clone().into_parts();
                collection_ids
                    .as_ref()
                    .is_none_or(|ids| ids.contains(&collection_id))
            })
            .cloned()
            .collect();
        page(rows, &options)
    }

    async fn list_export_templates_in_collection(
        &self,
        collection_id: CollectionId,
        exclude_template_id: Option<ExportTemplateId>,
    ) -> Result<Vec<StorageExportTemplate>, StorageError> {
        Ok(self
            .state
            .read()
            .await
            .export_templates
            .values()
            .filter(|template| {
                let (metadata, candidate_collection_id, _, _) = (*template).clone().into_parts();
                candidate_collection_id == collection_id
                    && exclude_template_id
                        .is_none_or(|excluded| metadata.id().id() != excluded.id())
            })
            .cloned()
            .collect())
    }

    async fn create_export_template(
        &self,
        request: StorageExportTemplateCreate,
    ) -> Result<StorageMutationOutcome<StorageExportTemplate>, StorageError> {
        let (collection_id, name, definition, context) = request.into_parts();
        let mut state = self.state.write().await;
        if !state.collections.contains_key(&collection_id.id()) {
            return Err(StorageError::not_found(format!(
                "Collection {} was not found",
                collection_id.id()
            )));
        }
        if state.export_templates.values().any(|template| {
            let (_, candidate_collection_id, candidate_name, _) = template.clone().into_parts();
            candidate_collection_id == collection_id && candidate_name == name
        }) {
            return Err(StorageError::conflict(format!(
                "An export template named '{name}' already exists in collection {}",
                collection_id.id()
            )));
        }
        let id = state.next_export_template_id;
        state.next_export_template_id += 1;
        let now = Utc::now();
        let metadata = StorageRecordMetadata::try_new(
            ResourceId::new(id).expect("memory export template id is positive"),
            now,
            now,
            ResourceRevision::INITIAL,
        )
        .map_err(invalid_contract_value)?;
        let template = StorageExportTemplate::new(metadata, collection_id, &name, definition);
        state.export_templates.insert(id, template.clone());
        let receipt = state.append_simple_event(
            EntityType::ExportTemplate,
            id,
            Some(&name),
            Action::Created,
            &context,
            format!("Export template '{name}' created"),
        )?;
        state.append_history(
            MemoryHistoryValue::ExportTemplate(template.clone()),
            StorageHistoryOperation::Create,
            &context,
        )?;
        Ok(StorageMutationOutcome::committed(template, receipt))
    }

    async fn replace_export_template(
        &self,
        request: StorageExportTemplateReplace,
    ) -> Result<StorageMutationOutcome<StorageExportTemplate>, StorageError> {
        let (template_id, collection_id, name, definition, context) = request.into_parts();
        let mut state = self.state.write().await;
        let current = state
            .export_templates
            .get(&template_id.id())
            .cloned()
            .ok_or_else(|| {
                StorageError::not_found(format!(
                    "Export template {} was not found",
                    template_id.id()
                ))
            })?;
        if !state.collections.contains_key(&collection_id.id()) {
            return Err(StorageError::not_found(format!(
                "Collection {} was not found",
                collection_id.id()
            )));
        }
        let (current_metadata, ..) = current.into_parts();
        let metadata = StorageRecordMetadata::try_new(
            current_metadata.id(),
            current_metadata.created_at(),
            Utc::now(),
            current_metadata
                .revision()
                .checked_advance()
                .map_err(|error| StorageError::internal(error.to_string()))?,
        )
        .map_err(invalid_contract_value)?;
        let template = StorageExportTemplate::new(metadata, collection_id, &name, definition);
        state
            .export_templates
            .insert(template_id.id(), template.clone());
        let receipt = state.append_simple_event(
            EntityType::ExportTemplate,
            template_id.id(),
            Some(&name),
            Action::Updated,
            &context,
            format!("Export template '{name}' updated"),
        )?;
        state.append_history(
            MemoryHistoryValue::ExportTemplate(template.clone()),
            StorageHistoryOperation::Update,
            &context,
        )?;
        Ok(StorageMutationOutcome::committed(template, receipt))
    }

    async fn delete_export_template(
        &self,
        request: StorageExportTemplateDelete,
    ) -> Result<StorageMutationOutcome<()>, StorageError> {
        let (template_id, context) = request.into_parts();
        let mut state = self.state.write().await;
        let Some(template) = state.export_templates.remove(&template_id.id()) else {
            return Ok(StorageMutationOutcome::unchanged(()));
        };
        let (_, _, name, _) = template.clone().into_parts();
        let receipt = state.append_simple_event(
            EntityType::ExportTemplate,
            template_id.id(),
            Some(&name),
            Action::Deleted,
            &context,
            format!("Export template '{name}' deleted"),
        )?;
        state.append_history(
            MemoryHistoryValue::ExportTemplate(template),
            StorageHistoryOperation::Delete,
            &context,
        )?;
        Ok(StorageMutationOutcome::committed((), receipt))
    }
}

impl ExecutionStorage for MemoryStorage {
    fn run_in_scope<'a, F, R>(
        &'a self,
        scope: StorageExecutionScope,
        future: F,
    ) -> Pin<Box<dyn Future<Output = R> + 'a>>
    where
        F: Future<Output = R> + 'a,
        R: 'a,
    {
        let scope = MEMORY_EXECUTION_SCOPE
            .try_with(|current| merge_memory_execution_scope(current, &scope))
            .unwrap_or(scope);
        Box::pin(MEMORY_EXECUTION_SCOPE.scope(scope, future))
    }

    fn run_in_scope_send<'a, F, R>(
        &'a self,
        scope: StorageExecutionScope,
        future: F,
    ) -> Pin<Box<dyn Future<Output = R> + Send + 'a>>
    where
        F: Future<Output = R> + Send + 'a,
        R: Send + 'a,
    {
        let scope = MEMORY_EXECUTION_SCOPE
            .try_with(|current| merge_memory_execution_scope(current, &scope))
            .unwrap_or(scope);
        Box::pin(MEMORY_EXECUTION_SCOPE.scope(scope, future))
    }
}

fn merge_memory_execution_scope(
    parent: &StorageExecutionScope,
    child: &StorageExecutionScope,
) -> StorageExecutionScope {
    let mut merged = StorageExecutionScope::default();
    if let Some(call_site) = child
        .call_site_override()
        .or_else(|| parent.call_site_override())
    {
        merged = merged.with_call_site(call_site);
    }
    if let Some(provenance) = child
        .mutation_provenance_override()
        .or_else(|| parent.mutation_provenance_override())
    {
        merged = merged.with_mutation_provenance(provenance.clone());
    }
    if let Some(precondition) = child
        .revision_precondition_override()
        .or_else(|| parent.revision_precondition_override())
    {
        merged = merged.with_revision_precondition(precondition.clone());
    }
    if let Some(budget) = child
        .query_budget_override()
        .or_else(|| parent.query_budget_override())
    {
        merged = merged.with_query_budget(budget);
    }
    merged
}

fn enforce_memory_revision_precondition(
    target: StorageRevisionTarget,
    current_revision: ResourceRevision,
) -> Result<(), StorageError> {
    let precondition = MEMORY_EXECUTION_SCOPE
        .try_with(|scope| scope.revision_precondition_override().cloned().flatten())
        .ok()
        .flatten();
    let Some(precondition) = precondition.filter(|condition| condition.target() == target) else {
        return Ok(());
    };
    if precondition.revisions().is_empty() || precondition.revisions().contains(&current_revision) {
        return Ok(());
    }
    Err(StorageError::revision_conflict(
        "The resource revision does not match the requested precondition",
        current_revision,
    ))
}

struct MemoryTransaction {
    storage: MemoryStorage,
    event_context: EventContext,
}

impl StorageTransaction for MemoryTransaction {
    fn collections(&self) -> TransactionalCollections<'_> {
        TransactionalCollections::new(&self.storage, &self.event_context)
    }

    fn classes(&self) -> TransactionalClasses<'_> {
        TransactionalClasses::new(&self.storage, &self.event_context)
    }

    fn class_relations(&self) -> TransactionalClassRelations<'_> {
        TransactionalClassRelations::new(&self.storage, &self.event_context)
    }

    fn objects(&self) -> TransactionalObjects<'_> {
        TransactionalObjects::new(&self.storage, &self.event_context)
    }

    fn object_relations(&self) -> TransactionalObjectRelations<'_> {
        TransactionalObjectRelations::new(&self.storage, &self.event_context)
    }
}

#[async_trait]
impl TransactionStorage for MemoryStorage {
    async fn with_transaction<F, R>(
        &self,
        event_context: EventContext,
        operation: F,
    ) -> Result<R, StorageError>
    where
        F: for<'transaction> FnOnce(
                &'transaction dyn StorageTransaction,
            ) -> StorageTransactionFuture<'transaction, R>
            + Send,
        R: Send,
    {
        let mut committed = self.state.write().await;
        let transaction = MemoryTransaction {
            storage: Self {
                state: Arc::new(RwLock::new(committed.clone())),
            },
            event_context,
        };
        let result = operation(&transaction).await;
        if result.is_ok() {
            *committed = transaction.storage.state.read().await.clone();
        }
        result
    }
}

impl StorageBackend for MemoryStorage {}

#[test]
fn an_external_crate_can_implement_the_complete_backend_contract() {
    fn assert_complete<T: StorageBackend + Clone + 'static>() {}
    assert_complete::<MemoryStorage>();
}
