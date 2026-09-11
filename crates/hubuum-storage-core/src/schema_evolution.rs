//! Versioned schema lifecycle, compatibility analysis and fenced validation evidence.

use async_trait::async_trait;
use chrono::{DateTime, SubsecRound, Utc};
use hubuum_domain::{
    ClassId, CollectionId, CompiledSchema, JsonSchemaLimits, ObjectId, PrincipalId,
    ResourceRevision, SchemaReference, SchemaRevision, TaskId,
};
use hubuum_events_core::EventContext;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    StorageClassSchemaPolicy, StorageError, StorageMutationOutcome, StorageTaskLease,
    StorageValidationError,
};

/// Schema policy validated once before it crosses into lifecycle persistence.
#[derive(Clone)]
pub struct StorageValidatedSchemaPolicy {
    policy: StorageClassSchemaPolicy,
    compiled: Option<CompiledSchema>,
    limits: JsonSchemaLimits,
}

impl std::fmt::Debug for StorageValidatedSchemaPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StorageValidatedSchemaPolicy")
            .field("enforced", &self.policy.validates_schema())
            .finish_non_exhaustive()
    }
}

impl StorageValidatedSchemaPolicy {
    pub fn try_new(policy: StorageClassSchemaPolicy) -> Result<Self, StorageValidationError> {
        Self::try_new_with_limits(policy, JsonSchemaLimits::default())
    }

    pub fn try_new_with_limits(
        policy: StorageClassSchemaPolicy,
        limits: JsonSchemaLimits,
    ) -> Result<Self, StorageValidationError> {
        let compiled = match policy.json_schema() {
            Some(document) if policy.validates_schema() => Some(
                CompiledSchema::try_new_with_limits(document.clone(), limits)
                    .map_err(|error| StorageValidationError::invalid(error.to_string()))?,
            ),
            Some(document) => {
                limits
                    .validate_schema(document)
                    .map_err(|error| StorageValidationError::invalid(error.to_string()))?;
                None
            }
            None => None,
        };
        Ok(Self {
            policy,
            compiled,
            limits,
        })
    }

    #[must_use]
    pub const fn limits(&self) -> JsonSchemaLimits {
        self.limits
    }

    /// A proof from a different deployment must not bypass the adapter's policy.
    pub fn ensure_limits(&self, limits: JsonSchemaLimits) -> Result<(), StorageValidationError> {
        if self.limits != limits {
            return Err(StorageValidationError::invalid(
                "Schema policy was validated with different deployment budgets",
            ));
        }
        Ok(())
    }

    #[must_use]
    pub const fn policy(&self) -> &StorageClassSchemaPolicy {
        &self.policy
    }
    #[must_use]
    pub fn inspect(&self, value: &Value) -> StorageComplianceStatus {
        match &self.compiled {
            None => StorageComplianceStatus::NotRequired,
            Some(schema) if schema.inspect(value).is_ok() => StorageComplianceStatus::Valid,
            Some(_) => StorageComplianceStatus::Invalid,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StorageSchemaRevisionStatus {
    Staged,
    Active,
    Retired,
    Abandoned,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StorageSchemaActivationPolicy {
    RejectIncompatible,
    AllowPending,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StorageComplianceStatus {
    Valid,
    Invalid,
    Pending,
    NotRequired,
}

impl StorageComplianceStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Valid => "valid",
            Self::Invalid => "invalid",
            Self::Pending => "pending",
            Self::NotRequired => "not_required",
        }
    }
}

/// Immutable document and separately mutable, audited revision lifecycle metadata.
/// Provenance uses microseconds so native columns and embedded history snapshots
/// retain identical instants across every selectable storage backend.
#[derive(Clone, Debug)]
pub struct StorageSchemaRevision {
    reference: SchemaReference,
    policy: StorageValidatedSchemaPolicy,
    status: StorageSchemaRevisionStatus,
    created_at: DateTime<Utc>,
    created_by: Option<PrincipalId>,
    activated_at: Option<DateTime<Utc>>,
    activation_policy: Option<StorageSchemaActivationPolicy>,
}

impl StorageSchemaRevision {
    pub fn from_snapshot(value: Value) -> Result<Self, StorageValidationError> {
        Self::from_snapshot_with_limits(value, JsonSchemaLimits::default())
    }

    pub fn from_snapshot_with_limits(
        value: Value,
        limits: JsonSchemaLimits,
    ) -> Result<Self, StorageValidationError> {
        #[derive(Deserialize)]
        struct Snapshot {
            class_id: ClassId,
            revision: SchemaRevision,
            json_schema: Option<Value>,
            validate_schema: bool,
            status: StorageSchemaRevisionStatus,
            created_at: DateTime<Utc>,
            created_by: Option<PrincipalId>,
            activated_at: Option<DateTime<Utc>>,
            activation_policy: Option<StorageSchemaActivationPolicy>,
        }
        let raw: Snapshot = serde_json::from_value(value)
            .map_err(|_| StorageValidationError::invalid("Invalid schema revision snapshot"))?;
        let policy = StorageValidatedSchemaPolicy::try_new_with_limits(
            StorageClassSchemaPolicy::try_from_parts(raw.json_schema, raw.validate_schema)?,
            limits,
        )?;
        Self {
            reference: SchemaReference::new(raw.class_id, raw.revision),
            policy,
            status: StorageSchemaRevisionStatus::Staged,
            created_at: raw.created_at.trunc_subsecs(6),
            created_by: raw.created_by,
            activated_at: None,
            activation_policy: None,
        }
        .restore_lifecycle(raw.status, raw.activated_at, raw.activation_policy)
    }
    #[must_use]
    pub fn snapshot(&self) -> Value {
        serde_json::json!({"class_id":self.reference.class_id(),"revision":self.reference.revision(),"json_schema":self.policy.policy().json_schema(),"validate_schema":self.policy.policy().validates_schema(),"status":self.status,"created_at":self.created_at,"created_by":self.created_by,"activated_at":self.activated_at,"activation_policy":self.activation_policy})
    }
    #[must_use]
    pub fn staged(
        reference: SchemaReference,
        policy: StorageValidatedSchemaPolicy,
        context: &EventContext,
        created_at: DateTime<Utc>,
    ) -> Self {
        Self {
            reference,
            policy,
            status: StorageSchemaRevisionStatus::Staged,
            created_at: created_at.trunc_subsecs(6),
            created_by: context.actor_user_id(),
            activated_at: None,
            activation_policy: None,
        }
    }
    pub fn restore_lifecycle(
        mut self,
        status: StorageSchemaRevisionStatus,
        activated_at: Option<DateTime<Utc>>,
        activation_policy: Option<StorageSchemaActivationPolicy>,
    ) -> Result<Self, StorageValidationError> {
        if matches!(
            status,
            StorageSchemaRevisionStatus::Active | StorageSchemaRevisionStatus::Retired
        ) != activated_at.is_some()
            || activated_at.is_some() != activation_policy.is_some()
        {
            return Err(StorageValidationError::invalid(
                "Schema revision lifecycle metadata is inconsistent",
            ));
        }
        self.status = status;
        self.activated_at = activated_at.map(|timestamp| timestamp.trunc_subsecs(6));
        self.activation_policy = activation_policy;
        Ok(self)
    }
    #[must_use]
    pub const fn reference(&self) -> SchemaReference {
        self.reference
    }
    #[must_use]
    pub const fn policy(&self) -> &StorageValidatedSchemaPolicy {
        &self.policy
    }
    #[must_use]
    pub const fn status(&self) -> StorageSchemaRevisionStatus {
        self.status
    }
    #[must_use]
    pub const fn created_at(&self) -> DateTime<Utc> {
        self.created_at
    }
    #[must_use]
    pub const fn created_by(&self) -> Option<PrincipalId> {
        self.created_by
    }
    #[must_use]
    pub const fn activated_at(&self) -> Option<DateTime<Utc>> {
        self.activated_at
    }
    #[must_use]
    pub const fn activation_policy(&self) -> Option<StorageSchemaActivationPolicy> {
        self.activation_policy
    }
}

impl Serialize for StorageSchemaRevision {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.snapshot().serialize(serializer)
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct StorageComplianceCounts {
    valid: u64,
    invalid: u64,
    pending: u64,
    not_required: u64,
}

impl StorageComplianceCounts {
    #[must_use]
    pub const fn new(valid: u64, invalid: u64, pending: u64, not_required: u64) -> Self {
        Self {
            valid,
            invalid,
            pending,
            not_required,
        }
    }
    #[must_use]
    pub const fn valid(&self) -> u64 {
        self.valid
    }
    #[must_use]
    pub const fn invalid(&self) -> u64 {
        self.invalid
    }
    #[must_use]
    pub const fn pending(&self) -> u64 {
        self.pending
    }
    #[must_use]
    pub const fn not_required(&self) -> u64 {
        self.not_required
    }
    pub fn observe(&mut self, status: StorageComplianceStatus) {
        let count = match status {
            StorageComplianceStatus::Valid => &mut self.valid,
            StorageComplianceStatus::Invalid => &mut self.invalid,
            StorageComplianceStatus::Pending => &mut self.pending,
            StorageComplianceStatus::NotRequired => &mut self.not_required,
        };
        *count = count.saturating_add(1);
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct StorageClassSchemaState {
    active: StorageSchemaRevision,
    counts: StorageComplianceCounts,
    object_epoch: u64,
}

impl StorageClassSchemaState {
    #[must_use]
    pub const fn new(
        active: StorageSchemaRevision,
        counts: StorageComplianceCounts,
        object_epoch: u64,
    ) -> Self {
        Self {
            active,
            counts,
            object_epoch,
        }
    }
    #[must_use]
    pub const fn active(&self) -> &StorageSchemaRevision {
        &self.active
    }
    #[must_use]
    pub const fn counts(&self) -> &StorageComplianceCounts {
        &self.counts
    }
    #[must_use]
    pub const fn object_epoch(&self) -> u64 {
        self.object_epoch
    }
}

/// The committed activation and its optional asynchronous validation task.
#[derive(Clone, Debug, Serialize)]
pub struct StorageSchemaActivationResult {
    active: StorageSchemaRevision,
    task_id: Option<TaskId>,
    dependent_rebuild_task_id: Option<TaskId>,
}
impl StorageSchemaActivationResult {
    #[must_use]
    pub const fn new(active: StorageSchemaRevision, task_id: Option<TaskId>) -> Self {
        Self {
            active,
            task_id,
            dependent_rebuild_task_id: None,
        }
    }
    #[must_use]
    pub const fn with_dependent_rebuild(mut self, task_id: Option<TaskId>) -> Self {
        self.dependent_rebuild_task_id = task_id;
        self
    }
    #[must_use]
    pub const fn dependent_rebuild_task_id(&self) -> Option<TaskId> {
        self.dependent_rebuild_task_id
    }
    #[must_use]
    pub const fn active(&self) -> &StorageSchemaRevision {
        &self.active
    }
    #[must_use]
    pub const fn task_id(&self) -> Option<TaskId> {
        self.task_id
    }
}

/// Evidence can only describe an inspected object revision; pending is a read projection.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StorageSchemaEvidence {
    schema: SchemaReference,
    object_revision: ResourceRevision,
    valid: bool,
    validated_at: DateTime<Utc>,
}

impl StorageSchemaEvidence {
    #[must_use]
    pub fn new(
        schema: SchemaReference,
        object_revision: ResourceRevision,
        valid: bool,
        validated_at: DateTime<Utc>,
    ) -> Self {
        Self {
            schema,
            object_revision,
            valid,
            validated_at: validated_at.trunc_subsecs(6),
        }
    }
    #[must_use]
    pub const fn schema(&self) -> SchemaReference {
        self.schema
    }
    #[must_use]
    pub const fn object_revision(&self) -> ResourceRevision {
        self.object_revision
    }
    #[must_use]
    pub const fn valid(&self) -> bool {
        self.valid
    }
    #[must_use]
    pub const fn validated_at(&self) -> DateTime<Utc> {
        self.validated_at
    }
    #[must_use]
    pub fn effective_status(
        evidence: Option<&Self>,
        active: &StorageSchemaRevision,
        object_revision: ResourceRevision,
    ) -> StorageComplianceStatus {
        if !active.policy().policy().validates_schema() {
            return StorageComplianceStatus::NotRequired;
        }
        match evidence.filter(|value| {
            value.schema == active.reference() && value.object_revision == object_revision
        }) {
            Some(value) if value.valid => StorageComplianceStatus::Valid,
            Some(_) => StorageComplianceStatus::Invalid,
            None => StorageComplianceStatus::Pending,
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct StorageObjectCompliance {
    object_id: ObjectId,
    object_revision: ResourceRevision,
    active_schema: SchemaReference,
    status: StorageComplianceStatus,
    evidence: Option<StorageSchemaEvidence>,
}

impl StorageObjectCompliance {
    #[must_use]
    pub fn new(
        object_id: ObjectId,
        object_revision: ResourceRevision,
        active: &StorageSchemaRevision,
        evidence: Option<StorageSchemaEvidence>,
    ) -> Self {
        Self {
            object_id,
            object_revision,
            active_schema: active.reference(),
            status: StorageSchemaEvidence::effective_status(
                evidence.as_ref(),
                active,
                object_revision,
            ),
            evidence,
        }
    }
    #[must_use]
    pub const fn object_id(&self) -> ObjectId {
        self.object_id
    }
    #[must_use]
    pub const fn status(&self) -> StorageComplianceStatus {
        self.status
    }
}

#[derive(Clone, Copy, Debug)]
pub struct StorageSchemaPage {
    class_id: ClassId,
    after: i64,
    limit: usize,
}

impl StorageSchemaPage {
    pub fn try_new(
        class_id: ClassId,
        after: i64,
        limit: usize,
    ) -> Result<Self, StorageValidationError> {
        if after < 0 || !(1..=100).contains(&limit) {
            return Err(StorageValidationError::invalid(
                "Schema pages require after >= 0 and limit between 1 and 100",
            ));
        }
        Ok(Self {
            class_id,
            after,
            limit,
        })
    }
    #[must_use]
    pub const fn class_id(self) -> ClassId {
        self.class_id
    }
    #[must_use]
    pub const fn after(self) -> i64 {
        self.after
    }
    #[must_use]
    pub const fn limit(self) -> usize {
        self.limit
    }
}

#[derive(Clone, Debug)]
pub struct StorageSchemaStage {
    authorized_collection: CollectionId,
    class_id: ClassId,
    policy: StorageValidatedSchemaPolicy,
    context: EventContext,
}
impl StorageSchemaStage {
    #[must_use]
    pub const fn new(
        authorized_collection: CollectionId,
        class_id: ClassId,
        policy: StorageValidatedSchemaPolicy,
        context: EventContext,
    ) -> Self {
        Self {
            authorized_collection,
            class_id,
            policy,
            context,
        }
    }
    #[must_use]
    pub const fn class_id(&self) -> ClassId {
        self.class_id
    }
    #[must_use]
    pub const fn policy(&self) -> &StorageValidatedSchemaPolicy {
        &self.policy
    }
    #[must_use]
    pub const fn authorized_collection(&self) -> CollectionId {
        self.authorized_collection
    }
    #[must_use]
    pub const fn context(&self) -> &EventContext {
        &self.context
    }
}

#[derive(Clone, Debug)]
pub struct StorageSchemaActivation {
    authorized_collection: CollectionId,
    target: SchemaReference,
    expected_active: SchemaRevision,
    policy: StorageSchemaActivationPolicy,
    proof_task: Option<TaskId>,
    context: EventContext,
}
impl StorageSchemaActivation {
    #[must_use]
    pub const fn new(
        authorized_collection: CollectionId,
        target: SchemaReference,
        expected_active: SchemaRevision,
        policy: StorageSchemaActivationPolicy,
        context: EventContext,
    ) -> Self {
        Self {
            authorized_collection,
            target,
            expected_active,
            policy,
            proof_task: None,
            context,
        }
    }
    #[must_use]
    pub const fn with_proof_task(mut self, task: Option<TaskId>) -> Self {
        self.proof_task = task;
        self
    }
    #[must_use]
    pub const fn target(&self) -> SchemaReference {
        self.target
    }
    #[must_use]
    pub const fn expected_active(&self) -> SchemaRevision {
        self.expected_active
    }
    #[must_use]
    pub const fn policy(&self) -> StorageSchemaActivationPolicy {
        self.policy
    }
    #[must_use]
    pub const fn proof_task(&self) -> Option<TaskId> {
        self.proof_task
    }
    #[must_use]
    pub const fn authorized_collection(&self) -> CollectionId {
        self.authorized_collection
    }
    #[must_use]
    pub const fn context(&self) -> &EventContext {
        &self.context
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StorageSchemaWorkKind {
    Impact,
    Revalidation,
}

#[derive(Clone, Debug)]
pub struct StorageSchemaWorkRequest {
    authorized_collection: CollectionId,
    target: SchemaReference,
    kind: StorageSchemaWorkKind,
    context: EventContext,
}
impl StorageSchemaWorkRequest {
    #[must_use]
    pub const fn new(
        authorized_collection: CollectionId,
        target: SchemaReference,
        kind: StorageSchemaWorkKind,
        context: EventContext,
    ) -> Self {
        Self {
            authorized_collection,
            target,
            kind,
            context,
        }
    }
    #[must_use]
    pub const fn target(&self) -> SchemaReference {
        self.target
    }
    #[must_use]
    pub const fn kind(&self) -> StorageSchemaWorkKind {
        self.kind
    }
    #[must_use]
    pub const fn authorized_collection(&self) -> CollectionId {
        self.authorized_collection
    }
    #[must_use]
    pub const fn context(&self) -> &EventContext {
        &self.context
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StorageSchemaWorkStatus {
    Running,
    Failed,
    Complete,
    Cancelled,
    Superseded,
}

/// Bounded resumable scan state. Serialized only as workflow/backup metadata.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(try_from = "SchemaWorkSnapshot")]
pub struct StorageSchemaWork {
    task_id: TaskId,
    target: SchemaReference,
    kind: StorageSchemaWorkKind,
    status: StorageSchemaWorkStatus,
    start_epoch: u64,
    end_epoch: Option<u64>,
    upper_bound: i32,
    cursor: i32,
    examined: u64,
    valid: u64,
    not_required: u64,
    invalid: u64,
    uninspectable: u64,
    stale: u64,
    invalid_samples: Vec<ObjectId>,
    elapsed_millis: u64,
    batches: u64,
    created_at: DateTime<Utc>,
}
#[derive(Deserialize)]
struct SchemaWorkSnapshot {
    task_id: TaskId,
    target: SchemaReference,
    kind: StorageSchemaWorkKind,
    status: StorageSchemaWorkStatus,
    start_epoch: u64,
    end_epoch: Option<u64>,
    upper_bound: i32,
    cursor: i32,
    examined: u64,
    valid: u64,
    not_required: u64,
    invalid: u64,
    uninspectable: u64,
    stale: u64,
    invalid_samples: Vec<ObjectId>,
    elapsed_millis: u64,
    batches: u64,
    created_at: DateTime<Utc>,
}
impl TryFrom<SchemaWorkSnapshot> for StorageSchemaWork {
    type Error = StorageValidationError;
    fn try_from(raw: SchemaWorkSnapshot) -> Result<Self, Self::Error> {
        let work = Self {
            task_id: raw.task_id,
            target: raw.target,
            kind: raw.kind,
            status: raw.status,
            start_epoch: raw.start_epoch,
            end_epoch: raw.end_epoch,
            upper_bound: raw.upper_bound,
            cursor: raw.cursor,
            examined: raw.examined,
            valid: raw.valid,
            not_required: raw.not_required,
            invalid: raw.invalid,
            uninspectable: raw.uninspectable,
            stale: raw.stale,
            invalid_samples: raw.invalid_samples,
            elapsed_millis: raw.elapsed_millis,
            batches: raw.batches,
            created_at: raw.created_at.trunc_subsecs(6),
        };
        if work.upper_bound < 0
            || work.cursor < 0
            || work.cursor > work.upper_bound
            || work.start_epoch > i64::MAX as u64
            || work.end_epoch.is_some_and(|epoch| epoch > i64::MAX as u64)
            || (work.status == StorageSchemaWorkStatus::Running) != work.end_epoch.is_none()
            || work
                .valid
                .checked_add(work.invalid)
                .and_then(|count| count.checked_add(work.not_required))
                .and_then(|count| count.checked_add(work.uninspectable))
                != Some(work.examined)
            || work.stale > work.examined
            || work.invalid_samples.len() > 20
            || work.invalid_samples.len() as u64 > work.invalid
            || work.invalid_samples.iter().any(|id| id.id() > work.cursor)
            || work
                .invalid_samples
                .windows(2)
                .any(|pair| pair[0].id() >= pair[1].id())
        {
            return Err(StorageValidationError::invalid(
                "Schema work checkpoint is inconsistent",
            ));
        }
        Ok(work)
    }
}

impl StorageSchemaWork {
    #[must_use]
    pub fn start(
        task_id: TaskId,
        request: &StorageSchemaWorkRequest,
        epoch: u64,
        upper_bound: i32,
    ) -> Self {
        Self {
            task_id,
            target: request.target,
            kind: request.kind,
            status: StorageSchemaWorkStatus::Running,
            start_epoch: epoch,
            end_epoch: None,
            upper_bound,
            cursor: 0,
            examined: 0,
            valid: 0,
            not_required: 0,
            invalid: 0,
            uninspectable: 0,
            stale: 0,
            invalid_samples: Vec::new(),
            elapsed_millis: 0,
            batches: 0,
            created_at: Utc::now().trunc_subsecs(6),
        }
    }
    #[must_use]
    pub const fn created_at(&self) -> DateTime<Utc> {
        self.created_at
    }
    #[must_use]
    pub const fn batches(&self) -> u64 {
        self.batches
    }
    #[must_use]
    pub const fn task_id(&self) -> TaskId {
        self.task_id
    }
    #[must_use]
    pub const fn target(&self) -> SchemaReference {
        self.target
    }
    #[must_use]
    pub const fn kind(&self) -> StorageSchemaWorkKind {
        self.kind
    }
    #[must_use]
    pub const fn status(&self) -> StorageSchemaWorkStatus {
        self.status
    }
    #[must_use]
    pub const fn cursor(&self) -> i32 {
        self.cursor
    }
    #[must_use]
    pub const fn upper_bound(&self) -> i32 {
        self.upper_bound
    }
    #[must_use]
    pub const fn examined(&self) -> u64 {
        self.examined
    }
    #[must_use]
    pub const fn not_required(&self) -> u64 {
        self.not_required
    }
    #[must_use]
    pub const fn valid(&self) -> u64 {
        self.valid
    }
    #[must_use]
    pub const fn invalid(&self) -> u64 {
        self.invalid
    }
    #[must_use]
    pub const fn uninspectable(&self) -> u64 {
        self.uninspectable
    }
    #[must_use]
    pub const fn stale(&self) -> u64 {
        self.stale
    }
    #[must_use]
    pub fn proves_compatible(&self, target: SchemaReference, epoch: u64) -> bool {
        self.target == target
            && self.kind == StorageSchemaWorkKind::Impact
            && self.status == StorageSchemaWorkStatus::Complete
            && self.start_epoch == epoch
            && self.end_epoch == Some(epoch)
            && self.invalid == 0
            && self.uninspectable == 0
            && self.stale == 0
    }
    pub fn record(
        &mut self,
        object_id: ObjectId,
        status: Option<StorageComplianceStatus>,
        stale: bool,
    ) {
        self.cursor = object_id.id();
        self.examined += 1;
        if stale {
            self.stale += 1;
        }
        match status {
            Some(StorageComplianceStatus::NotRequired) => self.not_required += 1,
            Some(StorageComplianceStatus::Valid) => self.valid += 1,
            Some(StorageComplianceStatus::Invalid) => {
                self.invalid += 1;
                if self.invalid_samples.len() < 20 {
                    self.invalid_samples.push(object_id);
                }
            }
            _ => self.uninspectable += 1,
        }
    }
    pub fn batch_committed(&mut self, elapsed_millis: u64) {
        self.batches += 1;
        self.elapsed_millis = self.elapsed_millis.saturating_add(elapsed_millis);
    }
    pub fn finish(&mut self, status: StorageSchemaWorkStatus, epoch: u64) {
        self.status = status;
        self.end_epoch = Some(epoch);
    }
}

/// Each adapter bounds snapshot rows and bytes before materializing JSON.
#[derive(Clone, Copy, Debug)]
pub struct StorageSchemaBatchLimits {
    rows: usize,
    bytes: usize,
    object_bytes: usize,
}
impl StorageSchemaBatchLimits {
    /// Keep worker materialization bounds aligned with validated deployment budgets.
    #[must_use]
    pub fn for_schema_limits(limits: JsonSchemaLimits) -> Self {
        Self {
            rows: 64,
            bytes: (8 * 1024 * 1024).max(limits.instance_bytes()),
            object_bytes: limits.instance_bytes(),
        }
    }

    pub fn try_new(
        rows: usize,
        bytes: usize,
        object_bytes: usize,
    ) -> Result<Self, StorageValidationError> {
        if !(1..=100).contains(&rows)
            || !(1024..=16 * 1024 * 1024).contains(&bytes)
            || !(1024..=bytes).contains(&object_bytes)
        {
            return Err(StorageValidationError::invalid(
                "Schema batches require 1..100 rows, 1 KiB..16 MiB total bytes, and an object limit within the byte budget",
            ));
        }
        Ok(Self {
            rows,
            bytes,
            object_bytes,
        })
    }
    #[must_use]
    pub const fn rows(self) -> usize {
        self.rows
    }
    #[must_use]
    pub const fn bytes(self) -> usize {
        self.bytes
    }
    #[must_use]
    pub const fn object_bytes(self) -> usize {
        self.object_bytes
    }
}
impl Default for StorageSchemaBatchLimits {
    fn default() -> Self {
        Self::for_schema_limits(JsonSchemaLimits::default())
    }
}

#[async_trait]
pub trait SchemaEvolutionStorage: Send + Sync {
    async fn schema_compliance_counts(&self) -> Result<StorageComplianceCounts, StorageError>;
    async fn list_schema_revisions(
        &self,
        query: StorageSchemaPage,
    ) -> Result<Vec<StorageSchemaRevision>, StorageError>;
    async fn get_schema_state(
        &self,
        class_id: ClassId,
    ) -> Result<StorageClassSchemaState, StorageError>;
    async fn stage_schema_revision(
        &self,
        request: StorageSchemaStage,
    ) -> Result<StorageMutationOutcome<StorageSchemaRevision>, StorageError>;
    async fn abandon_schema_revision(
        &self,
        target: SchemaReference,
        authorized_collection: CollectionId,
        context: &EventContext,
    ) -> Result<StorageMutationOutcome<StorageSchemaRevision>, StorageError>;
    async fn activate_schema_revision(
        &self,
        request: StorageSchemaActivation,
    ) -> Result<StorageMutationOutcome<StorageSchemaActivationResult>, StorageError>;
    async fn request_schema_work(
        &self,
        request: StorageSchemaWorkRequest,
    ) -> Result<StorageMutationOutcome<StorageSchemaWork>, StorageError>;
    async fn get_schema_work(&self, task_id: TaskId) -> Result<StorageSchemaWork, StorageError>;
    async fn process_schema_work(
        &self,
        lease: StorageTaskLease,
        limits: StorageSchemaBatchLimits,
    ) -> Result<StorageSchemaWork, StorageError>;
    async fn cancel_schema_work(
        &self,
        task_id: TaskId,
        authorized_collection: CollectionId,
        context: &EventContext,
    ) -> Result<StorageMutationOutcome<StorageSchemaWork>, StorageError>;
    async fn list_schema_compliance(
        &self,
        query: StorageSchemaPage,
        status: Option<StorageComplianceStatus>,
    ) -> Result<Vec<StorageObjectCompliance>, StorageError>;
}

/// Revision-aware activation intent carried by an import's resolved class item.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StorageImportSchemaActivation {
    revision: SchemaRevision,
    expected_active: SchemaRevision,
    policy: StorageSchemaActivationPolicy,
    proof_task: Option<TaskId>,
}
impl StorageImportSchemaActivation {
    #[must_use]
    pub const fn new(
        revision: SchemaRevision,
        expected_active: SchemaRevision,
        policy: StorageSchemaActivationPolicy,
        proof_task: Option<TaskId>,
    ) -> Self {
        Self {
            revision,
            expected_active,
            policy,
            proof_task,
        }
    }
    #[must_use]
    pub const fn revision(&self) -> SchemaRevision {
        self.revision
    }
    #[must_use]
    pub const fn for_class(
        &self,
        class_id: ClassId,
        collection_id: CollectionId,
        context: EventContext,
    ) -> StorageSchemaActivation {
        StorageSchemaActivation::new(
            collection_id,
            SchemaReference::new(class_id, self.revision),
            self.expected_active,
            self.policy,
            context,
        )
        .with_proof_task(self.proof_task)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;
    use serde_json::json;

    #[rstest]
    #[case("2026-09-11T12:34:56.123456789Z")]
    #[case("2026-09-11T14:34:56.123456789+02:00")]
    fn schema_provenance_uses_shared_microsecond_precision(#[case] timestamp: &str) {
        let timestamp = DateTime::parse_from_rfc3339(timestamp)
            .unwrap()
            .with_timezone(&Utc);
        let reference = SchemaReference::new(ClassId::new(1).unwrap(), SchemaRevision::INITIAL);
        let revision = StorageSchemaRevision::staged(
            reference,
            StorageValidatedSchemaPolicy::try_new(
                StorageClassSchemaPolicy::try_from_parts(None, false).unwrap(),
            )
            .unwrap(),
            &EventContext::system(),
            timestamp,
        )
        .restore_lifecycle(
            StorageSchemaRevisionStatus::Active,
            Some(timestamp),
            Some(StorageSchemaActivationPolicy::RejectIncompatible),
        )
        .unwrap();
        let snapshot = revision.snapshot();
        assert_eq!(snapshot["created_at"], json!("2026-09-11T12:34:56.123456Z"));
        assert_eq!(snapshot["activated_at"], snapshot["created_at"]);
        assert_eq!(
            StorageSchemaRevision::from_snapshot(snapshot)
                .unwrap()
                .created_at(),
            timestamp.trunc_subsecs(6)
        );
    }

    #[test]
    fn schema_evidence_uses_shared_microsecond_precision() {
        let timestamp = DateTime::parse_from_rfc3339("2026-09-11T12:34:56.123456789Z")
            .unwrap()
            .with_timezone(&Utc);
        let evidence = StorageSchemaEvidence::new(
            SchemaReference::new(ClassId::new(1).unwrap(), SchemaRevision::INITIAL),
            ResourceRevision::INITIAL,
            true,
            timestamp,
        );
        assert_eq!(evidence.validated_at(), timestamp.trunc_subsecs(6));
    }
}
