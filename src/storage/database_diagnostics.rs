//! Application-owned compatibility projection for the legacy database endpoint.
//!
//! These values deliberately do not belong to `hubuum-storage-core`: pool
//! counters and database-size metadata are operational details, not portable
//! persistence capabilities. Each registered adapter maps its native
//! diagnostics into this root-owned projection during application composition.

use async_trait::async_trait;
use chrono::NaiveDateTime;

use super::StorageError;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct DatabasePoolCapacity {
    pub(in crate::storage) max_connections: u32,
    pub(in crate::storage) total_connections: u32,
    pub(in crate::storage) available_connections: u32,
    pub(in crate::storage) idle_connections: u32,
    pub(in crate::storage) in_use_connections: u32,
}

impl DatabasePoolCapacity {
    pub(crate) const fn max_connections(self) -> u32 {
        self.max_connections
    }

    pub(crate) const fn total_connections(self) -> u32 {
        self.total_connections
    }

    pub(crate) const fn available_connections(self) -> u32 {
        self.available_connections
    }

    pub(crate) const fn idle_connections(self) -> u32 {
        self.idle_connections
    }

    pub(crate) const fn in_use_connections(self) -> u32 {
        self.in_use_connections
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct DatabasePoolAcquisitions {
    pub(in crate::storage) pending: u64,
    pub(in crate::storage) started: u64,
    pub(in crate::storage) direct: u64,
    pub(in crate::storage) waited: u64,
    pub(in crate::storage) timed_out: u64,
    pub(in crate::storage) wait_time_ms: u64,
}

impl DatabasePoolAcquisitions {
    pub(crate) const fn pending(self) -> u64 {
        self.pending
    }

    pub(crate) const fn started(self) -> u64 {
        self.started
    }

    pub(crate) const fn direct(self) -> u64 {
        self.direct
    }

    pub(crate) const fn waited(self) -> u64 {
        self.waited
    }

    pub(crate) const fn timed_out(self) -> u64 {
        self.timed_out
    }

    pub(crate) const fn wait_time_ms(self) -> u64 {
        self.wait_time_ms
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct DatabasePoolConnections {
    pub(in crate::storage) created: u64,
    pub(in crate::storage) closed_broken: u64,
    pub(in crate::storage) closed_invalid: u64,
    pub(in crate::storage) closed_max_lifetime: u64,
    pub(in crate::storage) closed_idle_timeout: u64,
}

impl DatabasePoolConnections {
    pub(crate) const fn created(self) -> u64 {
        self.created
    }

    pub(crate) const fn closed_broken(self) -> u64 {
        self.closed_broken
    }

    pub(crate) const fn closed_invalid(self) -> u64 {
        self.closed_invalid
    }

    pub(crate) const fn closed_max_lifetime(self) -> u64 {
        self.closed_max_lifetime
    }

    pub(crate) const fn closed_idle_timeout(self) -> u64 {
        self.closed_idle_timeout
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct DatabasePoolState {
    pub(in crate::storage) capacity: DatabasePoolCapacity,
    pub(in crate::storage) acquisitions: DatabasePoolAcquisitions,
    pub(in crate::storage) connections: DatabasePoolConnections,
}

impl DatabasePoolState {
    pub(crate) const fn capacity(self) -> DatabasePoolCapacity {
        self.capacity
    }

    pub(crate) const fn acquisitions(self) -> DatabasePoolAcquisitions {
        self.acquisitions
    }

    pub(crate) const fn connections(self) -> DatabasePoolConnections {
        self.connections
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct DatabaseStorageSnapshot {
    pub(in crate::storage) active_sessions: i64,
    pub(in crate::storage) storage_bytes: i64,
    pub(in crate::storage) last_maintenance_at: Option<NaiveDateTime>,
}

impl DatabaseStorageSnapshot {
    pub(crate) const fn active_sessions(self) -> i64 {
        self.active_sessions
    }

    pub(crate) const fn storage_bytes(self) -> i64 {
        self.storage_bytes
    }

    pub(crate) const fn last_maintenance_at(self) -> Option<NaiveDateTime> {
        self.last_maintenance_at
    }
}

#[async_trait]
pub(crate) trait DatabaseDiagnosticsProvider: Send + Sync {
    fn pool_state(&self) -> DatabasePoolState;

    async fn storage_snapshot(&self) -> Result<DatabaseStorageSnapshot, StorageError>;
}
