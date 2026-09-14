//! Snapshot commit fencing for long-running memory transactions. Lease renewal
//! remains independent; all other writes invalidate a staged snapshot.

use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use super::{
    MemoryState, StorageError, StorageTaskKind, StorageTaskLease, Utc, invalid_task_lease,
};

pub(super) struct MemoryStateLock(RwLock<MemoryState>);

impl MemoryStateLock {
    pub(super) fn new(state: MemoryState) -> Self {
        Self(RwLock::new(state))
    }

    pub(super) async fn read(&self) -> RwLockReadGuard<'_, MemoryState> {
        self.0.read().await
    }

    pub(super) async fn write(&self) -> MemoryStateWriteGuard<'_> {
        MemoryStateWriteGuard {
            guard: self.0.write().await,
            mutated: false,
        }
    }

    /// Only lease expiry and its activity timestamp may change through this
    /// guard. Snapshot commit explicitly merges these fields from live tasks.
    pub(super) async fn write_lease(&self) -> RwLockWriteGuard<'_, MemoryState> {
        self.0.write().await
    }

    pub(super) async fn commit_import(
        &self,
        generation: &Arc<()>,
        mut staged: MemoryState,
        lease: &StorageTaskLease,
    ) -> Result<(), StorageError> {
        let mut live = self.0.write().await;
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
