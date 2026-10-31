//! Snapshot commit fencing for long-running memory transactions. Lease renewal
//! remains independent; all other writes invalidate a staged snapshot.

use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use hubuum_storage_core::{RuntimeOwner, StorageExecutionPermit};
use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use super::{
    MEMORY_EXECUTION_SCOPE, MemoryState, StorageError, StorageTaskKind, StorageTaskLease, Utc,
    invalid_task_lease,
};

pub(super) struct MemoryStateLock {
    state: Arc<RwLock<MemoryState>>,
    runtime_owner: Option<RuntimeOwner>,
}

impl MemoryStateLock {
    pub(super) fn new(state: MemoryState) -> Self {
        Self {
            state: Arc::new(RwLock::new(state)),
            runtime_owner: None,
        }
    }

    pub(super) fn with_runtime_owner(&self, owner: RuntimeOwner) -> Self {
        Self {
            state: Arc::clone(&self.state),
            runtime_owner: Some(owner),
        }
    }

    pub(super) fn runtime_owner(&self) -> Option<&RuntimeOwner> {
        self.runtime_owner.as_ref()
    }

    pub(super) fn fork(&self, state: MemoryState) -> Self {
        Self {
            state: Arc::new(RwLock::new(state)),
            runtime_owner: self.runtime_owner.clone(),
        }
    }

    pub(super) async fn read(&self) -> Result<RwLockReadGuard<'_, MemoryState>, StorageError> {
        let state = self.state.read().await;
        self.require_application_access(&state)?;
        Ok(state)
    }

    pub(super) async fn write(&self) -> Result<MemoryStateWriteGuard<'_>, StorageError> {
        let guard = self.state.write().await;
        self.require_application_access(&guard)?;
        Ok(MemoryStateWriteGuard {
            guard,
            mutated: false,
        })
    }

    /// Reserved for protected authority and operational state that must remain
    /// available while application exposure is quarantined.
    pub(super) async fn read_control(&self) -> RwLockReadGuard<'_, MemoryState> {
        self.state.read().await
    }

    /// Reserved for control transitions and nonproductive failure/cancellation
    /// settlement. Ordinary capability methods use the checked write guard.
    pub(super) async fn write_control(&self) -> MemoryStateWriteGuard<'_> {
        MemoryStateWriteGuard {
            guard: self.state.write().await,
            mutated: false,
        }
    }

    fn require_application_access(&self, state: &MemoryState) -> Result<(), StorageError> {
        let permit = MEMORY_EXECUTION_SCOPE
            .try_with(|scope| scope.application_access_override().cloned())
            .ok()
            .flatten();
        if let Some(owner) = &self.runtime_owner {
            let matching_owner = permit
                .as_ref()
                .and_then(StorageExecutionPermit::owner)
                .is_some_and(|captured| {
                    captured.id() == owner.id()
                        && captured.ownership_token() == owner.ownership_token()
                });
            if !matching_owner {
                return Err(StorageError::unavailable(
                    "Registered runtime requires its captured application execution permit",
                ));
            }
        }
        if let Some(permit) = &permit {
            state.ensure_application_permit(permit)?;
        }
        Ok(())
    }

    /// Only lease expiry and its activity timestamp may change through this
    /// guard. Snapshot commit explicitly merges these fields from live tasks.
    pub(super) async fn write_lease(&self) -> RwLockWriteGuard<'_, MemoryState> {
        self.state.write().await
    }

    pub(super) async fn commit_import(
        &self,
        generation: &Arc<()>,
        mut staged: MemoryState,
        lease: &StorageTaskLease,
    ) -> Result<(), StorageError> {
        let mut live = self.state.write().await;
        self.require_application_access(&live)?;
        live.ensure_task_lease_current(lease)?;
        let task = live
            .tasks
            .get(&lease.task_id().id())
            .filter(|task| {
                task.kind == StorageTaskKind::Import
                    && task.status.is_active()
                    && task.lease_matches(lease)
            })
            .ok_or_else(invalid_task_lease)?;
        if let Some(reason) = task.control.stop_reason(Utc::now()) {
            return Err(StorageError::task_stopped(reason));
        }
        if !Arc::ptr_eq(generation, &live.generation) {
            return Err(StorageError::conflict(
                "Concurrent memory mutation invalidated the import snapshot; no staged changes committed",
            ));
        }
        // Lease renewal does not invalidate domain snapshots. Preserve every
        // renewed task, including workers unrelated to this import.
        for (id, task) in &mut staged.tasks {
            if let Some(current) = live.tasks.get(id) {
                task.lease_expires_at = current.lease_expires_at;
                task.updated_at = task.updated_at.max(current.updated_at);
            }
        }
        staged.generation = Arc::new(());
        staged.task_capacity.scheduler = live.task_capacity.scheduler.clone();
        *live = staged;
        Ok(())
    }
}

pub(super) struct MemoryStateWriteGuard<'a> {
    guard: RwLockWriteGuard<'a, MemoryState>,
    mutated: bool,
}

impl Deref for MemoryStateWriteGuard<'_> {
    type Target = MemoryState;
    fn deref(&self) -> &Self::Target {
        &self.guard
    }
}

impl DerefMut for MemoryStateWriteGuard<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.mutated = true;
        &mut self.guard
    }
}

impl Drop for MemoryStateWriteGuard<'_> {
    fn drop(&mut self) {
        // Refresh after mutation, including assignments of a cloned snapshot.
        // An identity token has no integer wraparound or ABA reuse while a
        // staged transaction retains the previous token.
        if self.mutated {
            self.guard.generation = Arc::new(());
        }
    }
}
