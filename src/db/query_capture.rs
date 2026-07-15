//! Opt-in PostgreSQL query capture for deterministic performance tests.
//!
//! Capture is task-scoped and installed on every pool checkout. This lets one
//! snapshot cover an operation that acquires several connections while also
//! clearing stale instrumentation before a pooled connection is reused.

use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};

use diesel::connection::{Instrumentation, InstrumentationEvent};
use diesel_async::AsyncConnection;

use super::{DbConnection, PooledConnection};

tokio::task_local! {
    static ACTIVE_QUERY_CAPTURE_ID: u64;
}

static NEXT_QUERY_CAPTURE_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct QueryCaptureSnapshot {
    total_queries: usize,
    control_queries: usize,
    connection_checkouts: usize,
    query_counts: BTreeMap<String, usize>,
}

impl QueryCaptureSnapshot {
    pub fn total_queries(&self) -> usize {
        self.total_queries
    }

    pub fn control_queries(&self) -> usize {
        self.control_queries
    }

    pub fn domain_queries(&self) -> usize {
        self.total_queries.saturating_sub(self.control_queries)
    }

    pub fn connection_checkouts(&self) -> usize {
        self.connection_checkouts
    }

    pub fn query_counts(&self) -> &BTreeMap<String, usize> {
        &self.query_counts
    }

    pub fn queries_matching(&self, needle: &str) -> usize {
        self.query_counts
            .iter()
            .filter(|(query, _)| query.contains(needle))
            .map(|(_, count)| *count)
            .sum()
    }
}

#[derive(Default)]
struct QueryCaptureRegistry {
    captures: HashMap<u64, QueryCaptureSnapshot>,
}

fn registry() -> &'static Mutex<QueryCaptureRegistry> {
    static REGISTRY: OnceLock<Mutex<QueryCaptureRegistry>> = OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(QueryCaptureRegistry::default()))
}

fn normalize_query(query: &str) -> String {
    query
        .split_once(" -- binds:")
        .map_or(query, |(statement, _)| statement)
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn is_control_query(query: &str) -> bool {
    let query = query.trim_start().to_ascii_uppercase();
    [
        "BEGIN",
        "COMMIT",
        "ROLLBACK",
        "SAVEPOINT",
        "RELEASE SAVEPOINT",
        "SET TRANSACTION",
        "SELECT SET_CONFIG(",
    ]
    .iter()
    .any(|prefix| query.starts_with(prefix))
}

fn is_pool_validation_query(query: &str) -> bool {
    query == "SELECT $1"
}

fn active_capture_id() -> Option<u64> {
    ACTIVE_QUERY_CAPTURE_ID.try_with(|id| *id).ok()
}

fn record_query(capture_id: u64, query: &str) {
    let query = normalize_query(query);
    // bb8's test-on-checkout probe runs before `acquire_connection` can replace
    // instrumentation on a reused connection. Pool use is captured separately
    // through `connection_checkouts`, so do not attribute this internal probe
    // to the application operation that happens to receive the connection.
    if is_pool_validation_query(&query) {
        return;
    }
    let mut registry = registry()
        .lock()
        .expect("query capture registry should not be poisoned");
    let Some(snapshot) = registry.captures.get_mut(&capture_id) else {
        // A pooled connection may retain instrumentation after its capture has
        // ended. Ignore that stale id until the next checkout reconfigures it.
        return;
    };
    snapshot.total_queries += 1;
    if is_control_query(&query) {
        snapshot.control_queries += 1;
    }
    *snapshot.query_counts.entry(query).or_insert(0) += 1;
}

fn record_connection_checkout(capture_id: u64) {
    let mut registry = registry()
        .lock()
        .expect("query capture registry should not be poisoned");
    registry
        .captures
        .entry(capture_id)
        .or_default()
        .connection_checkouts += 1;
}

struct QueryCaptureInstrumentation {
    capture_id: Option<u64>,
}

impl Instrumentation for QueryCaptureInstrumentation {
    fn on_connection_event(&mut self, event: InstrumentationEvent<'_>) {
        let Some(capture_id) = self.capture_id else {
            return;
        };
        if let InstrumentationEvent::StartQuery { query, .. } = event {
            record_query(capture_id, &query.to_string());
        }
    }
}

pub(super) fn configure_connection(conn: &mut PooledConnection<'_, DbConnection>) {
    let capture_id = active_capture_id();
    if let Some(capture_id) = capture_id {
        record_connection_checkout(capture_id);
    }
    conn.set_instrumentation(QueryCaptureInstrumentation { capture_id });
}

/// Capture every Diesel query started by `future` on the current task.
///
/// Fixture setup should happen before entering this scope. Futures polled on
/// the same task, including `join!` branches, inherit the capture; independently
/// spawned tasks intentionally do not.
pub async fn capture_queries<T>(future: impl Future<Output = T>) -> (T, QueryCaptureSnapshot) {
    let capture_id = NEXT_QUERY_CAPTURE_ID.fetch_add(1, Ordering::Relaxed);
    registry()
        .lock()
        .expect("query capture registry should not be poisoned")
        .captures
        .insert(capture_id, QueryCaptureSnapshot::default());

    let output = ACTIVE_QUERY_CAPTURE_ID.scope(capture_id, future).await;
    let snapshot = registry()
        .lock()
        .expect("query capture registry should not be poisoned")
        .captures
        .remove(&capture_id)
        .unwrap_or_default();

    (output, snapshot)
}

#[cfg(test)]
mod tests {
    use super::{is_control_query, is_pool_validation_query, normalize_query};

    #[test]
    fn query_normalization_collapses_whitespace() {
        assert_eq!(
            normalize_query("SELECT  *\n  FROM widgets\tWHERE id = $1 -- binds: [42]"),
            "SELECT * FROM widgets WHERE id = $1"
        );
    }

    #[test]
    fn transaction_and_session_statements_are_control_queries() {
        for query in [
            "BEGIN",
            "COMMIT",
            "ROLLBACK",
            "SAVEPOINT diesel_savepoint_1",
            "RELEASE SAVEPOINT diesel_savepoint_1",
            "SELECT set_config('statement_timeout', $1, true)",
        ] {
            assert!(is_control_query(query), "expected control query: {query}");
        }
        assert!(!is_control_query("SELECT * FROM collections"));
    }

    #[test]
    fn pool_validation_probe_is_not_an_application_query() {
        assert!(is_pool_validation_query("SELECT $1"));
        assert!(!is_pool_validation_query("SELECT * FROM collections"));
    }
}
