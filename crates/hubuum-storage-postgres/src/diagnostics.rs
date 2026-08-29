use chrono::NaiveDateTime;

/// Capacity and current occupancy of the PostgreSQL connection pool.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PostgresPoolCapacity {
    max_connections: u32,
    total_connections: u32,
    available_connections: u32,
    idle_connections: u32,
    in_use_connections: u32,
}

impl PostgresPoolCapacity {
    pub(crate) const fn new(
        max_connections: u32,
        total_connections: u32,
        available_connections: u32,
        idle_connections: u32,
        in_use_connections: u32,
    ) -> Self {
        Self {
            max_connections,
            total_connections,
            available_connections,
            idle_connections,
            in_use_connections,
        }
    }

    #[must_use]
    pub(crate) const fn max_connections(self) -> u32 {
        self.max_connections
    }

    #[must_use]
    pub(crate) const fn total_connections(self) -> u32 {
        self.total_connections
    }

    #[must_use]
    pub(crate) const fn available_connections(self) -> u32 {
        self.available_connections
    }

    #[must_use]
    pub(crate) const fn idle_connections(self) -> u32 {
        self.idle_connections
    }

    #[must_use]
    pub(crate) const fn in_use_connections(self) -> u32 {
        self.in_use_connections
    }
}

/// Acquisition counters reported by the PostgreSQL connection pool.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PostgresPoolAcquisitionState {
    pending: u64,
    started: u64,
    direct: u64,
    waited: u64,
    timed_out: u64,
    wait_time_ms: u64,
}

impl PostgresPoolAcquisitionState {
    pub(crate) const fn new(
        pending: u64,
        started: u64,
        direct: u64,
        waited: u64,
        timed_out: u64,
        wait_time_ms: u64,
    ) -> Self {
        Self {
            pending,
            started,
            direct,
            waited,
            timed_out,
            wait_time_ms,
        }
    }

    #[must_use]
    pub(crate) const fn pending(self) -> u64 {
        self.pending
    }

    #[must_use]
    pub(crate) const fn started(self) -> u64 {
        self.started
    }

    #[must_use]
    pub(crate) const fn direct(self) -> u64 {
        self.direct
    }

    #[must_use]
    pub(crate) const fn waited(self) -> u64 {
        self.waited
    }

    #[must_use]
    pub(crate) const fn timed_out(self) -> u64 {
        self.timed_out
    }

    #[must_use]
    pub(crate) const fn wait_time_ms(self) -> u64 {
        self.wait_time_ms
    }
}

/// Connection lifecycle counters reported by the PostgreSQL pool.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PostgresPoolConnectionState {
    created: u64,
    closed_broken: u64,
    closed_invalid: u64,
    closed_max_lifetime: u64,
    closed_idle_timeout: u64,
}

impl PostgresPoolConnectionState {
    pub(crate) const fn new(
        created: u64,
        closed_broken: u64,
        closed_invalid: u64,
        closed_max_lifetime: u64,
        closed_idle_timeout: u64,
    ) -> Self {
        Self {
            created,
            closed_broken,
            closed_invalid,
            closed_max_lifetime,
            closed_idle_timeout,
        }
    }

    #[must_use]
    pub(crate) const fn created(self) -> u64 {
        self.created
    }

    #[must_use]
    pub(crate) const fn closed_broken(self) -> u64 {
        self.closed_broken
    }

    #[must_use]
    pub(crate) const fn closed_invalid(self) -> u64 {
        self.closed_invalid
    }

    #[must_use]
    pub(crate) const fn closed_max_lifetime(self) -> u64 {
        self.closed_max_lifetime
    }

    #[must_use]
    pub(crate) const fn closed_idle_timeout(self) -> u64 {
        self.closed_idle_timeout
    }
}

/// Current state of the PostgreSQL connection pool.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PostgresPoolState {
    capacity: PostgresPoolCapacity,
    acquisitions: PostgresPoolAcquisitionState,
    connections: PostgresPoolConnectionState,
}

impl PostgresPoolState {
    pub(crate) const fn new(
        capacity: PostgresPoolCapacity,
        acquisitions: PostgresPoolAcquisitionState,
        connections: PostgresPoolConnectionState,
    ) -> Self {
        Self {
            capacity,
            acquisitions,
            connections,
        }
    }

    #[must_use]
    pub const fn max_connections(self) -> u32 {
        self.capacity.max_connections()
    }

    #[must_use]
    pub const fn total_connections(self) -> u32 {
        self.capacity.total_connections()
    }

    #[must_use]
    pub const fn available_connections(self) -> u32 {
        self.capacity.available_connections()
    }

    #[must_use]
    pub const fn idle_connections(self) -> u32 {
        self.capacity.idle_connections()
    }

    #[must_use]
    pub const fn in_use_connections(self) -> u32 {
        self.capacity.in_use_connections()
    }

    #[must_use]
    pub const fn pending_acquisitions(self) -> u64 {
        self.acquisitions.pending()
    }

    #[must_use]
    pub const fn acquisitions_started(self) -> u64 {
        self.acquisitions.started()
    }

    #[must_use]
    pub const fn acquisitions_direct(self) -> u64 {
        self.acquisitions.direct()
    }

    #[must_use]
    pub const fn acquisitions_waited(self) -> u64 {
        self.acquisitions.waited()
    }

    #[must_use]
    pub const fn acquisitions_timed_out(self) -> u64 {
        self.acquisitions.timed_out()
    }

    #[must_use]
    pub const fn acquisition_wait_time_ms(self) -> u64 {
        self.acquisitions.wait_time_ms()
    }

    #[must_use]
    pub const fn connections_created(self) -> u64 {
        self.connections.created()
    }

    #[must_use]
    pub const fn connections_closed_broken(self) -> u64 {
        self.connections.closed_broken()
    }

    #[must_use]
    pub const fn connections_closed_invalid(self) -> u64 {
        self.connections.closed_invalid()
    }

    #[must_use]
    pub const fn connections_closed_max_lifetime(self) -> u64 {
        self.connections.closed_max_lifetime()
    }

    #[must_use]
    pub const fn connections_closed_idle_timeout(self) -> u64 {
        self.connections.closed_idle_timeout()
    }
}

/// PostgreSQL-specific database state used by administrator diagnostics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PostgresStorageSnapshot {
    active_sessions: i64,
    storage_bytes: i64,
    last_maintenance_at: Option<NaiveDateTime>,
}

impl PostgresStorageSnapshot {
    pub(crate) const fn new(
        active_sessions: i64,
        storage_bytes: i64,
        last_maintenance_at: Option<NaiveDateTime>,
    ) -> Self {
        Self {
            active_sessions,
            storage_bytes,
            last_maintenance_at,
        }
    }

    #[must_use]
    pub const fn active_sessions(self) -> i64 {
        self.active_sessions
    }

    #[must_use]
    pub const fn storage_bytes(self) -> i64 {
        self.storage_bytes
    }

    #[must_use]
    pub const fn last_maintenance_at(self) -> Option<NaiveDateTime> {
        self.last_maintenance_at
    }
}
