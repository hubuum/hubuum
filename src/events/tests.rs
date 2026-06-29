//! Tests for the events table + emit_event helper (#71).
//!
//! The load-bearing property is "recorded iff committed": an event emitted
//! inside a transaction that commits is persisted, and one emitted inside a
//! transaction that rolls back is not. These tests exercise that directly
//! against a real Postgres pool.

#![cfg(test)]

use diesel::prelude::*;
use rstest::rstest;
use uuid::Uuid;

use crate::db::{with_connection, with_transaction};
use crate::errors::ApiError;
use crate::events::{
    Action, ActorKind, EntityType, Event, NewEvent, RequestProvenance, emit_event,
};
use crate::schema::events::dsl::events;
use crate::tests::test_scope;

/// Count event rows for a given `event_id` (0 or 1, since `event_id` is UNIQUE).
fn count_events_for(conn: &mut PgConnection, target: Uuid) -> i64 {
    use crate::schema::events::dsl::event_id;
    events
        .filter(event_id.eq(target))
        .count()
        .get_result(conn)
        .expect("count query")
}

#[rstest]
#[case::commit_persists(false)]
#[case::rollback_discards(true)]
fn emit_event_respects_transaction_outcome(#[case] rollback: bool) {
    let scope = test_scope();
    let pool = scope.pool.clone();

    let new_event = NewEvent::new(
        EntityType::Namespace,
        Action::Created,
        ActorKind::System,
        "test event",
    )
    .unwrap()
    .with_namespace_id(1)
    .with_entity_id(1)
    .with_entity_name("ns-test")
    .with_request_id(Uuid::new_v4())
    .with_correlation_id("client-provided-correlation-id")
    .with_metadata(serde_json::json!({"k": "v"}));
    let event_uuid = new_event.event_id();

    let result: Result<Event, ApiError> = with_transaction(&pool, |conn| {
        let event = emit_event(conn, &new_event)?;
        // The row is visible inside the same transaction.
        assert_eq!(count_events_for(conn, event_uuid), 1);
        if rollback {
            // Simulate a later mutation step failing, aborting the whole tx.
            return Err(ApiError::InternalServerError("simulated failure".into()));
        }
        Ok(event)
    });

    if rollback {
        assert!(result.is_err(), "expected rollback error");
    } else {
        assert!(result.is_ok(), "expected commit, got {result:?}");
    }

    // After the transaction settles, the row persists iff it committed.
    let persisted = with_connection(&pool, |conn| {
        Ok::<_, diesel::result::Error>(count_events_for(conn, event_uuid))
    })
    .unwrap();
    if rollback {
        assert_eq!(
            persisted, 0,
            "event must not survive a rolled-back transaction"
        );
    } else {
        assert_eq!(persisted, 1, "event must survive a committed transaction");
    }
}

#[test]
fn new_event_rejects_invalid_action_for_type() {
    // object_relation has no Updated per the catalog.
    let err = NewEvent::new(
        EntityType::ObjectRelation,
        Action::Updated,
        ActorKind::System,
        "bad pair",
    )
    .unwrap_err();
    assert!(
        matches!(err, ApiError::ValidationError(ref m) if m.contains("not valid for entity_type")),
        "expected ValidationError, got {err:?}"
    );
}

#[test]
fn new_event_accepts_arbitrary_correlation_id() {
    let ev = NewEvent::new(EntityType::Namespace, Action::Created, ActorKind::User, "n")
        .unwrap()
        .with_correlation_id("any-arbitrary-client-value-!@#$%");
    // correlation_id accepts arbitrary caller-provided header values (#71).
    assert_eq!(
        ev.correlation_id(),
        Some("any-arbitrary-client-value-!@#$%")
    );
}

#[test]
fn new_event_applies_event_context() {
    let request_id = Uuid::new_v4();
    let provenance = RequestProvenance::new(request_id, Some("client-correlation".to_string()));
    let context = provenance.user_event_context(42);

    let ev = NewEvent::new(
        EntityType::Namespace,
        Action::Created,
        ActorKind::System,
        "created namespace",
    )
    .unwrap()
    .with_context(&context);

    assert_eq!(ev.actor_kind(), ActorKind::User);
    assert_eq!(ev.actor_user_id(), Some(42));
    assert_eq!(ev.request_id(), Some(request_id));
    assert_eq!(ev.correlation_id(), Some("client-correlation"));
}

#[test]
fn fanout_backlog_index_exists() {
    // The partial fan-out backlog index must be present before #76 (#71 done-when).
    let scope = test_scope();
    with_connection(&scope.pool, |conn| {
        let exists: bool = diesel::sql_query(
            "SELECT EXISTS (
                SELECT 1 FROM pg_indexes
                WHERE schemaname = 'public'
                  AND tablename = 'events'
                  AND indexname = 'events_fanout_backlog_idx'
                  AND indexdef LIKE '%WHERE (dispatched_at IS NULL)%'
            )",
        )
        .get_result::<IndexExistsRow>(conn)
        .map(|r| r.exists)?;
        assert!(exists, "events_fanout_backlog_idx partial index is missing");
        Ok::<_, diesel::result::Error>(())
    })
    .unwrap();
}

#[derive(diesel::QueryableByName)]
struct IndexExistsRow {
    #[diesel(sql_type = diesel::sql_types::Bool)]
    exists: bool,
}
