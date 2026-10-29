use super::*;
use crate::logical_identity::IdentityRecord;
use hubuum_domain::{IdentityReference, PrincipalIdentity, ResourceIdentity, TaskIdentity};
use hubuum_storage_core::{
    StorageIdentityResourceKind, StorageIdentitySubject, StorageIdentityTarget,
    StorageTaskCapacitySnapshot, StorageTaskCapacityUsage, StorageTaskPolicy,
    StorageTaskPolicyUpdate, TaskAdmissionPolicy, TaskCapacityRejection, TaskCapacityStorage,
    TaskPolicyRevision, TaskSchedulingPolicy, TaskScopeCapacity,
};
use std::collections::btree_map::Entry;
use std::io::{self, Write};

/// Control state is carried across application replay explicitly. It has no
/// membership in backup state and no ownership through an integer task ID.
#[derive(Clone)]
pub(super) struct MemoryTaskCapacity {
    pub(super) scheduler: super::task_scheduling::MemoryTaskScheduler,
    initialized: bool,
    policy: StorageTaskPolicy,
    generation: u64,
    partial: bool,
    reconciling: bool,
    owners: BTreeMap<TaskIdentity, TaskAccounting>,
}

impl Default for MemoryTaskCapacity {
    fn default() -> Self {
        Self {
            scheduler: super::task_scheduling::MemoryTaskScheduler::default(),
            initialized: false,
            generation: 0,
            partial: false,
            reconciling: false,
            owners: BTreeMap::new(),
            policy: StorageTaskPolicy::new(
                TaskPolicyRevision::new(1).expect("initial policy revision is valid"),
                TaskAdmissionPolicy::default(),
            ),
        }
    }
}

#[derive(Clone)]
struct TaskOwner {
    principal: Option<PrincipalIdentity>,
    collections: BTreeSet<ResourceIdentity>,
    kind: StorageTaskKind,
}

impl TaskOwner {
    fn internal(&self) -> bool {
        matches!(
            self.kind,
            StorageTaskKind::Reindex | StorageTaskKind::SchemaValidation
        )
    }
}

#[derive(Clone)]
struct TaskAccounting {
    owner: TaskOwner,
    task_id: Option<TaskId>,
    outstanding: bool,
    executing: bool,
    resident_input: u64,
    resident_output: u64,
    cumulative_input: u64,
    cumulative_output: u64,
    cumulative_items: u64,
    processed_items: u64,
}

impl TaskAccounting {
    fn new(owner: TaskOwner, task: &MemoryTaskRecord) -> Result<Self, StorageError> {
        Ok(Self {
            owner,
            task_id: Some(task.id),
            outstanding: !task.status.is_terminal(),
            executing: task.status.is_active() && task.claim_token.is_some(),
            resident_input: task
                .request_payload
                .as_ref()
                .map(json_bytes)
                .transpose()?
                .unwrap_or(0),
            resident_output: 0,
            cumulative_input: 0,
            cumulative_output: 0,
            cumulative_items: 0,
            processed_items: task.progress.processed() as u64,
        })
    }
}

#[derive(Clone, Copy)]
enum CapacityChange {
    Admission,
    Claim,
    Output,
}

#[derive(Default)]
struct JsonSize(u64);

impl Write for JsonSize {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0 = self
            .0
            .checked_add(bytes.len() as u64)
            .ok_or_else(|| io::Error::other("Task byte accounting overflow"))?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn json_bytes(value: &impl serde::Serialize) -> Result<u64, StorageError> {
    let mut size = JsonSize::default();
    serde_json::to_writer(&mut size, value)
        .map_err(|error| StorageError::internal(error.to_string()))?;
    Ok(size.0)
}

fn unknown_identity() -> StorageError {
    StorageError::task_capacity(TaskCapacityRejection::IdentityUnavailable)
}

impl MemoryTaskCapacity {
    pub(super) fn for_delta(mut self) -> Self {
        self.partial = true;
        self
    }

    pub(super) fn restore_control(mut self) -> Result<Self, StorageError> {
        self.scheduler.clear_workers();
        self.generation = self
            .generation
            .checked_add(1)
            .ok_or_else(|| StorageError::internal("Task control generation exhausted"))?;
        self.partial = false;
        self.reconciling = true;
        for owner in self.owners.values_mut() {
            owner.task_id = None;
            owner.outstanding = false;
            owner.executing = false;
            owner.resident_input = 0;
            owner.resident_output = 0;
        }
        Ok(self)
    }

    pub(super) fn for_commit(mut self) -> Self {
        self.partial = false;
        self
    }

    pub(super) fn finish_restore(&mut self) {
        self.reconciling = false;
    }

    fn check(
        &self,
        candidate: &TaskAccounting,
        change: CapacityChange,
    ) -> Result<(), StorageError> {
        if self.reconciling {
            return Ok(());
        }
        let policy = self.policy.policy();
        if matches!(change, CapacityChange::Admission)
            && candidate.outstanding
            && u128::from(candidate.cumulative_input) + u128::from(candidate.resident_input)
                > u128::from(policy.cumulative_input_bytes_per_task().bytes())
        {
            return Err(StorageError::task_capacity(
                TaskCapacityRejection::CumulativeWork,
            ));
        }
        let others = || {
            self.owners.values().filter(|entry| {
                (entry.task_id.is_some() || entry.resident_output > 0)
                    && entry.task_id != candidate.task_id
                    && entry.owner.internal() == candidate.owner.internal()
            })
        };
        let check = |limits: TaskScopeCapacity, scope: Vec<&TaskAccounting>, reason| {
            let outstanding = scope.iter().filter(|entry| entry.outstanding).count() as u128
                + u128::from(candidate.outstanding);
            let executing = scope.iter().filter(|entry| entry.executing).count() as u128
                + u128::from(candidate.executing);
            let bytes = scope
                .iter()
                .map(|entry| u128::from(entry.resident_input) + u128::from(entry.resident_output))
                .sum::<u128>()
                + u128::from(candidate.resident_input)
                + u128::from(candidate.resident_output);
            if (matches!(change, CapacityChange::Admission)
                && outstanding > u128::from(limits.outstanding().get()))
                || (matches!(change, CapacityChange::Claim)
                    && executing > u128::from(limits.executing().get()))
            {
                return Err(StorageError::task_capacity(reason));
            }
            if !matches!(change, CapacityChange::Claim)
                && bytes > u128::from(limits.resident_bytes().bytes())
            {
                return Err(StorageError::task_capacity(
                    TaskCapacityRejection::ResidentBytes,
                ));
            }
            Ok(())
        };
        if candidate.owner.internal() {
            check(
                policy.internal(),
                others().collect(),
                TaskCapacityRejection::Internal,
            )?;
            return if matches!(change, CapacityChange::Claim) {
                check(
                    policy.for_kind(candidate.owner.kind).capacity(),
                    others()
                        .filter(|entry| entry.owner.kind == candidate.owner.kind)
                        .collect(),
                    TaskCapacityRejection::Kind,
                )
            } else {
                Ok(())
            };
        }
        check(
            policy.deployment(),
            others().collect(),
            TaskCapacityRejection::Deployment,
        )?;
        if candidate.owner.principal.is_some() {
            check(
                policy.principal(),
                others()
                    .filter(|entry| entry.owner.principal == candidate.owner.principal)
                    .collect(),
                TaskCapacityRejection::Principal,
            )?;
        }
        let kind = policy.for_kind(candidate.owner.kind);
        check(
            kind.capacity(),
            others()
                .filter(|entry| entry.owner.kind == candidate.owner.kind)
                .collect(),
            TaskCapacityRejection::Kind,
        )?;
        if matches!(change, CapacityChange::Admission) {
            let principal_kind = others()
                .filter(|entry| {
                    entry.outstanding
                        && entry.owner.kind == candidate.owner.kind
                        && entry.owner.principal == candidate.owner.principal
                })
                .count() as u64;
            if principal_kind >= kind.principal_outstanding().get() {
                return Err(StorageError::task_capacity(
                    TaskCapacityRejection::PrincipalKind,
                ));
            }
        }
        for collection in &candidate.owner.collections {
            check(
                policy.collection(),
                others()
                    .filter(|entry| entry.owner.collections.contains(collection))
                    .collect(),
                TaskCapacityRejection::Collection,
            )?;
        }
        Ok(())
    }
}

impl MemoryState {
    pub(super) fn scheduling_policy(&self) -> &TaskSchedulingPolicy {
        self.task_capacity.policy.policy().scheduling()
    }

    pub(super) fn scheduling_principal(
        &self,
        id: TaskId,
    ) -> Result<Option<PrincipalIdentity>, StorageError> {
        let identity = self.task_identity(id)?;
        self.task_capacity
            .owners
            .get(&identity)
            .map(|entry| entry.owner.principal)
            .ok_or_else(unknown_identity)
    }
    pub(super) fn task_identity(&self, id: TaskId) -> Result<TaskIdentity, StorageError> {
        match self
            .logical_identities
            .record(StorageIdentityTarget::Task(id))?
            .subject()
        {
            Some(StorageIdentitySubject::Task {
                identity: IdentityReference::Known { identity },
            }) => Ok(identity),
            _ => Err(unknown_identity()),
        }
    }

    fn task_owner(&self, task: &MemoryTaskRecord) -> Result<TaskOwner, StorageError> {
        let principal = task
            .submitted_by
            .map(|id| {
                match self
                    .logical_identities
                    .record(StorageIdentityTarget::Principal(id))?
                    .subject()
                {
                    Some(StorageIdentitySubject::Principal {
                        identity: IdentityReference::Known { identity },
                    }) => Ok(identity),
                    _ => Err(unknown_identity()),
                }
            })
            .transpose()?;
        let mut ids = BTreeSet::new();
        for target in task.references().into_values() {
            let StorageIdentityTarget::Resource { kind, id } = target else {
                continue;
            };
            match kind {
                StorageIdentityResourceKind::Collection => {
                    ids.insert(
                        CollectionId::new(id.id())
                            .map_err(|error| StorageError::invalid_input(error.to_string()))?,
                    );
                }
                StorageIdentityResourceKind::Class => {
                    ids.insert(
                        self.classes
                            .get(&id.id())
                            .ok_or_else(unknown_identity)?
                            .collection_id(),
                    );
                }
                StorageIdentityResourceKind::Object => {
                    ids.insert(
                        self.objects
                            .get(&id.id())
                            .ok_or_else(unknown_identity)?
                            .collection_id(),
                    );
                }
                StorageIdentityResourceKind::ClassRelation => {
                    let relation = self
                        .class_relations
                        .get(&id.id())
                        .ok_or_else(unknown_identity)?;
                    for endpoint in [relation.from_class_id(), relation.to_class_id()] {
                        ids.insert(
                            self.classes
                                .get(&endpoint.id())
                                .ok_or_else(unknown_identity)?
                                .collection_id(),
                        );
                    }
                }
                StorageIdentityResourceKind::ObjectRelation => {
                    let relation = self
                        .object_relations
                        .get(&id.id())
                        .ok_or_else(unknown_identity)?;
                    for endpoint in [relation.from_object_id(), relation.to_object_id()] {
                        ids.insert(
                            self.objects
                                .get(&endpoint.id())
                                .ok_or_else(unknown_identity)?
                                .collection_id(),
                        );
                    }
                }
                _ => {}
            }
        }
        let collections = ids
            .into_iter()
            .map(|id| {
                self.logical_identities
                    .record(StorageIdentityTarget::Resource {
                        kind: StorageIdentityResourceKind::Collection,
                        id: id.into(),
                    })?
                    .subject()
                    .and_then(StorageIdentitySubject::record_identity)
                    .map(|identity| identity.resource())
                    .ok_or_else(unknown_identity)
            })
            .collect::<Result<_, _>>()?;
        Ok(TaskOwner {
            principal,
            collections,
            kind: task.kind,
        })
    }

    pub(super) fn task_accounting_snapshot(&self) -> Result<MemoryTaskCapacity, StorageError> {
        let mut capacity = self.task_capacity.clone();
        if !capacity.partial {
            for (identity, entry) in &mut capacity.owners {
                entry.resident_output = 0;
                if entry.task_id.is_some_and(|id| {
                    !self.tasks.contains_key(&id.id())
                        || self.task_identity(id).ok().as_ref() != Some(identity)
                }) {
                    entry.task_id = None;
                    entry.outstanding = false;
                    entry.executing = false;
                    entry.resident_input = 0;
                    entry.resident_output = 0;
                }
            }
        }
        for task in self.tasks.values() {
            let identity = self.task_identity(task.id)?;
            if let Entry::Vacant(entry) = capacity.owners.entry(identity) {
                let owner = if task.status.is_terminal() {
                    let evidence = self
                        .logical_identities
                        .record(StorageIdentityTarget::Task(task.id))?;
                    TaskOwner {
                        principal: evidence.principal_reference("/submitted_by"),
                        collections: BTreeSet::new(),
                        kind: task.kind,
                    }
                } else {
                    self.task_owner(task)?
                };
                entry.insert(TaskAccounting::new(owner, task)?);
            }
            let entry = capacity
                .owners
                .get_mut(&identity)
                .expect("accounting owner was installed");
            entry.task_id = Some(task.id);
            entry.outstanding = !task.status.is_terminal();
            entry.executing = task.status.is_active() && task.claim_token.is_some();
            entry.resident_input = task
                .request_payload
                .as_ref()
                .map(json_bytes)
                .transpose()?
                .unwrap_or(0);
            let output_bytes = self.task_output_bytes(task.id)?;
            if !capacity.partial || output_bytes > 0 {
                entry.resident_output = output_bytes;
            }
            let processed = task.progress.processed() as u64;
            entry.cumulative_items = entry
                .cumulative_items
                .checked_add(if capacity.reconciling {
                    0
                } else {
                    processed.saturating_sub(entry.processed_items)
                })
                .ok_or_else(|| StorageError::internal("Task work accounting overflow"))?;
            entry.processed_items = processed;
        }
        for (identity, bytes) in self.artifacts.reservations() {
            let entry = capacity
                .owners
                .get_mut(&identity)
                .ok_or_else(unknown_identity)?;
            entry.resident_output = entry
                .resident_output
                .checked_add(bytes)
                .ok_or_else(|| StorageError::internal("Artifact accounting overflow"))?;
        }
        Ok(capacity)
    }

    pub(super) fn check_restored_artifact_space(
        &self,
        additional: u64,
    ) -> Result<(), StorageError> {
        let capacity = self.task_accounting_snapshot()?;
        let occupied: u128 = capacity
            .owners
            .values()
            .filter(|entry| !entry.owner.internal())
            .map(|entry| u128::from(entry.resident_input) + u128::from(entry.resident_output))
            .sum();
        if occupied + u128::from(additional)
            > u128::from(
                capacity
                    .policy
                    .policy()
                    .deployment()
                    .resident_bytes()
                    .bytes(),
            )
        {
            return Err(StorageError::task_capacity(
                TaskCapacityRejection::ResidentBytes,
            ));
        }
        Ok(())
    }

    pub(super) fn check_artifact_capacity(&self, id: TaskId) -> Result<(), StorageError> {
        let capacity = self.task_accounting_snapshot()?;
        let identity = self.task_identity(id)?;
        capacity.check(
            capacity
                .owners
                .get(&identity)
                .ok_or_else(unknown_identity)?,
            CapacityChange::Output,
        )
    }

    pub(super) fn store_task(
        &mut self,
        id: i32,
        mut task: MemoryTaskRecord,
    ) -> Result<(), StorageError> {
        if !self.tasks.contains_key(&id) && self.task_control.activation().is_protected() {
            task.retention_policy = Some(self.task_control.policy().revision());
        }
        let mut capacity = self.task_accounting_snapshot()?;
        let owner = self.task_owner(&task)?;
        let candidate = TaskAccounting::new(owner, &task)?;
        capacity.check(&candidate, CapacityChange::Admission)?;
        let task_id = task.id;
        self.store_task_record(id, task);
        let identity = self.task_identity(task_id)?;
        capacity.owners.insert(identity, candidate);
        self.task_capacity = capacity;
        Ok(())
    }

    pub(super) fn task_claim_has_capacity(&self, id: TaskId) -> Result<bool, StorageError> {
        let identity = self.task_identity(id)?;
        let Some(entry) = self.task_capacity.owners.get(&identity) else {
            return Err(unknown_identity());
        };
        let mut candidate = entry.clone();
        candidate.executing = true;
        if u128::from(candidate.cumulative_input) + u128::from(candidate.resident_input)
            > u128::from(
                self.task_capacity
                    .policy
                    .policy()
                    .cumulative_input_bytes_per_task()
                    .bytes(),
            )
        {
            return Ok(false);
        }
        match self.task_capacity.check(&candidate, CapacityChange::Claim) {
            Ok(()) => Ok(true),
            Err(error) if error.task_capacity_rejection().is_some() => Ok(false),
            Err(error) => Err(error),
        }
    }

    pub(super) fn charge_task_claim(&mut self, id: TaskId) -> Result<(), StorageError> {
        let identity = self.task_identity(id)?;
        let entry = self
            .task_capacity
            .owners
            .get_mut(&identity)
            .ok_or_else(unknown_identity)?;
        entry.cumulative_input = entry
            .cumulative_input
            .checked_add(entry.resident_input)
            .ok_or_else(|| StorageError::internal("Task work accounting overflow"))?;
        entry.executing = true;
        Ok(())
    }

    pub(super) fn account_task_output(&mut self, id: TaskId) -> Result<(), StorageError> {
        let identity = self.task_identity(id)?;
        let mut capacity = self.task_accounting_snapshot()?;
        let entry = capacity
            .owners
            .get(&identity)
            .ok_or_else(unknown_identity)?;
        let previous_bytes = self
            .task_capacity
            .owners
            .get(&identity)
            .map_or(0, |entry| entry.resident_output);
        if entry.resident_output > previous_bytes {
            capacity.check(entry, CapacityChange::Output)?;
        }
        let entry = capacity.owners.get_mut(&identity).expect("known owner");
        entry.cumulative_output = entry
            .cumulative_output
            .checked_add(entry.resident_output)
            .ok_or_else(|| StorageError::internal("Task work accounting overflow"))?;
        self.task_capacity = capacity;
        Ok(())
    }

    fn task_output_bytes(&self, id: TaskId) -> Result<u64, StorageError> {
        let mut bytes = 0;
        if let Some(output) = self.backup_outputs.get(&id.id()) {
            bytes += output.document().map_or(0, |bytes| bytes.len()) as u64;
        }
        if let Some(output) = self.export_outputs.get(&id.id()) {
            bytes += output
                .json_output()
                .map(json_bytes)
                .transpose()?
                .unwrap_or(0)
                + output.text_output().map_or(0, |text| text.len() as u64)
                + json_bytes(output.metadata())?
                + json_bytes(output.warnings())?;
        }
        if let Some(output) = self.schema_profile_outputs.get(&id.id()) {
            bytes += json_bytes(output.report())?;
        }
        if let Some(output) = self.schema_repair_reports.get(&id.id()) {
            bytes += json_bytes(output)?;
        }
        for output in &self.remote_call_results {
            if output.get("task_id").and_then(serde_json::Value::as_i64) == Some(i64::from(id.id()))
            {
                bytes += output
                    .get("response_headers")
                    .filter(|value| !value.is_null())
                    .map(json_bytes)
                    .transpose()?
                    .unwrap_or(0)
                    + output
                        .get("response_body_preview")
                        .and_then(serde_json::Value::as_str)
                        .map_or(0, |text| text.len() as u64);
            }
        }
        Ok(bytes)
    }
}

#[async_trait]
impl TaskCapacityStorage for MemoryStorage {
    async fn initialize_task_policy(
        &self,
        policy: TaskAdmissionPolicy,
    ) -> Result<StorageTaskPolicy, StorageError> {
        let mut state = self.state.write().await?;
        let capacity = &mut state.task_capacity;
        if !capacity.initialized {
            capacity.policy = StorageTaskPolicy::new(capacity.policy.revision(), policy);
            capacity.initialized = true;
        }
        Ok(capacity.policy.clone())
    }

    async fn task_policy(&self) -> Result<StorageTaskPolicy, StorageError> {
        Ok(self.state.read().await?.task_capacity.policy.clone())
    }

    async fn task_capacity_snapshot(&self) -> Result<StorageTaskCapacitySnapshot, StorageError> {
        let mut state = self.state.write().await?;
        state.task_capacity = state.task_accounting_snapshot()?;
        let capacity = &state.task_capacity;
        let usage = |internal| -> Result<StorageTaskCapacityUsage, StorageError> {
            let mut values = [0_u64; 6];
            for entry in capacity
                .owners
                .values()
                .filter(|entry| entry.owner.internal() == internal)
            {
                let counts = [
                    u64::from(entry.outstanding),
                    u64::from(entry.executing),
                    entry
                        .resident_input
                        .checked_add(entry.resident_output)
                        .ok_or_else(|| StorageError::internal("Task byte accounting overflow"))?,
                    entry.cumulative_input,
                    entry.cumulative_output,
                    entry.cumulative_items,
                ];
                for (total, count) in values.iter_mut().zip(counts) {
                    *total = total
                        .checked_add(count)
                        .ok_or_else(|| StorageError::internal("Task accounting overflow"))?;
                }
            }
            Ok(StorageTaskCapacityUsage::default()
                .with_current(values[0], values[1], values[2])
                .with_cumulative(values[3], values[4], values[5]))
        };
        let budget = u128::from(
            capacity
                .policy
                .policy()
                .cumulative_input_bytes_per_task()
                .bytes(),
        );
        let blocked = capacity
            .owners
            .values()
            .filter(|entry| {
                entry.outstanding
                    && !entry.executing
                    && u128::from(entry.cumulative_input) + u128::from(entry.resident_input)
                        > budget
            })
            .count() as u64;
        Ok(StorageTaskCapacitySnapshot::new(
            capacity.policy.clone(),
            capacity.generation,
            state.scheduling_snapshot(Utc::now()),
        )
        .with_usage(usage(false)?, usage(true)?)
        .with_work_budget_blocked(blocked))
    }

    async fn replace_task_policy(
        &self,
        update: StorageTaskPolicyUpdate,
    ) -> Result<StorageTaskPolicy, StorageError> {
        let mut state = self.state.write().await?;
        let capacity = &mut state.task_capacity;
        if capacity.policy.revision() != update.expected_revision() {
            return Err(StorageError::conflict(
                "Task policy revision changed; refetch before updating",
            ));
        }
        let revision = capacity
            .policy
            .revision()
            .next()
            .map_err(invalid_contract_value)?;
        capacity.policy = StorageTaskPolicy::new(revision, update.policy().clone());
        capacity.initialized = true;
        Ok(capacity.policy.clone())
    }
}
