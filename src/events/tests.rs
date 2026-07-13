//! Tests for the events table + emit_event helper (#71).
//!
//! The load-bearing property is "recorded iff committed": an event emitted
//! inside a transaction that commits is persisted, and one emitted inside a
//! transaction that rolls back is not. These tests exercise that directly
//! against a real Postgres pool.

#![cfg(test)]

use crate::db::prelude::*;
use rstest::rstest;
use uuid::Uuid;

use super::delivery::process_claimed_event_delivery;
use crate::db::traits::event_delivery::{
    EventDeliverySettings, claim_event_deliveries, claim_event_delivery_by_id,
    mark_event_delivery_dead, mark_event_delivery_failed,
};
use crate::db::traits::event_fanout::{
    EventFanoutSettings, claim_events_for_fanout, count_event_deliveries_for_event, fanout_event,
};
use crate::db::traits::event_retention::{
    EventRetentionSettings, purge_event_retention_without_archive,
};
use crate::db::traits::event_subscription::{SaveEventSinkRecord, SaveEventSubscriptionRecord};
use crate::db::traits::remote_target::{
    DeleteRemoteTargetRecord, SaveRemoteTargetRecord, UpdateRemoteTargetRecord,
    emit_remote_target_invoked_event,
};
use crate::db::{with_connection, with_transaction};
use crate::errors::ApiError;
use crate::events::{
    Action, ActorKind, EntityType, Event, EventContext, NewEvent, RequestProvenance, emit_event,
};
use crate::models::class::{NewHubuumClass, UpdateHubuumClass};
use crate::models::collection::{NewCollectionWithAssignee, UpdateCollection, move_collection};
use crate::models::group::{NewGroup, UpdateGroup};
use crate::models::object::{NewHubuumObject, UpdateHubuumObject};
use crate::models::token::{create_principal_token, revoke_token_by_id_for_principal};
use crate::models::{
    CollectionID, EventDelivery, EventDeliveryID, EventDeliveryStatus, EventSink as EventSinkModel,
    EventSinkID, EventSinkKind, EventSubscription, ExportContentType, ExportTemplateID,
    ExportTemplateKind, GroupID, HubuumClassRelationID, NewEventSink, NewEventSubscription,
    NewExportTemplate, NewHubuumClassRelation, NewHubuumObjectRelation, NewRemoteTargetRow,
    NewUser, Permissions, PermissionsList, PrincipalToken, RemoteTargetID, Token,
    UpdateExportTemplate, UpdateRemoteTargetRow, UpdateUser,
};
use crate::schema::events::dsl::events;
use crate::tests::{
    TestMutex, TestScope, create_test_user, lock_test_mutex, test_mutex, test_scope,
};
use crate::traits::{CanDelete, CanSave, CanUpdate, PermissionController};

static EVENT_DELIVERY_TEST_LOCK: TestMutex = test_mutex();
static EVENT_RETENTION_TEST_LOCK: TestMutex = test_mutex();

/// Count event rows for a given `event_id` (0 or 1, since `event_id` is UNIQUE).
async fn count_events_for(conn: &mut crate::db::DbConnection, target: Uuid) -> i64 {
    use crate::schema::events::dsl::event_id;
    events
        .filter(event_id.eq(target))
        .count()
        .get_result(conn)
        .await
        .expect("count query")
}

#[rstest]
#[tokio::test]
#[case::commit_persists(false)]
#[case::rollback_discards(true)]
async fn emit_event_respects_transaction_outcome(#[case] rollback: bool) {
    let scope = test_scope();
    let pool = scope.pool.clone();

    let new_event = NewEvent::new(
        EntityType::Collection,
        Action::Created,
        ActorKind::System,
        "test event",
    )
    .unwrap()
    .with_collection_id(1)
    .with_entity_id(1)
    .with_entity_name("collection_fixture-test")
    .with_request_id(Uuid::new_v4())
    .with_correlation_id("client-provided-correlation-id")
    .with_metadata(serde_json::json!({"k": "v"}));
    let event_uuid = new_event.event_id();

    let result: Result<Event, ApiError> = with_transaction(&pool, async |conn| {
        let event = emit_event(conn, &new_event).await?;
        // The row is visible inside the same transaction.
        assert_eq!(count_events_for(conn, event_uuid).await, 1);
        if rollback {
            // Simulate a later mutation step failing, aborting the whole tx.
            return Err(ApiError::InternalServerError("simulated failure".into()));
        }
        Ok(event)
    })
    .await;

    if rollback {
        assert!(result.is_err(), "expected rollback error");
    } else {
        assert!(result.is_ok(), "expected commit, got {result:?}");
    }

    // After the transaction settles, the row persists iff it committed.
    let persisted = with_connection(&pool, async |conn| {
        Ok::<_, diesel::result::Error>(count_events_for(conn, event_uuid).await)
    })
    .await
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
    let ev = NewEvent::new(
        EntityType::Collection,
        Action::Created,
        ActorKind::User,
        "n",
    )
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
        EntityType::Collection,
        Action::Created,
        ActorKind::System,
        "created collection",
    )
    .unwrap()
    .with_context(&context);

    assert_eq!(ev.actor_kind(), ActorKind::User);
    assert_eq!(ev.actor_user_id(), Some(42));
    assert_eq!(ev.request_id(), Some(request_id));
    assert_eq!(ev.correlation_id(), Some("client-correlation"));
}

#[tokio::test]
async fn fanout_backlog_index_exists() {
    // The partial fan-out backlog index must be present before #76 (#71 done-when).
    let scope = test_scope();
    with_connection(&scope.pool, async |conn| {
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
        .await
        .map(|r| r.exists)?;
        assert!(exists, "events_fanout_backlog_idx partial index is missing");
        Ok::<_, diesel::result::Error>(())
    })
    .await
    .unwrap();
}

async fn create_collection_event_subscription(
    scope: &TestScope,
    collection_id: i32,
    label: &str,
    enabled: bool,
) -> i32 {
    create_collection_event_subscription_with_filter(
        scope,
        collection_id,
        label,
        enabled,
        hubuum_events_core::EventSubscriptionFilter::default(),
    )
    .await
}

async fn create_collection_event_subscription_with_filter(
    scope: &TestScope,
    collection_id: i32,
    label: &str,
    enabled: bool,
    filter: hubuum_events_core::EventSubscriptionFilter,
) -> i32 {
    let sink = NewEventSink {
        name: scope.scoped_name(&format!("{label}_sink")),
        kind: EventSinkKind::Webhook,
        config: serde_json::json!({}),
        secret_ref: None,
        enabled: true,
    }
    .into_row()
    .unwrap()
    .save_event_sink_record_without_events(&scope.pool)
    .await
    .unwrap();

    NewEventSubscription {
        sink_id: EventSinkID::new(sink.id).unwrap(),
        name: scope.scoped_name(&format!("{label}_subscription")),
        description: String::new(),
        entity_types: vec![EntityType::Collection.as_str().to_string()],
        actions: vec![Action::Created.as_str().to_string()],
        filter,
        routing: serde_json::json!({}),
        enabled,
    }
    .into_row(CollectionID::new(collection_id).unwrap())
    .unwrap()
    .save_event_subscription_record_without_events(&scope.pool)
    .await
    .unwrap()
    .id
}

async fn emit_collection_created_event(scope: &TestScope, collection_id: i32) -> Event {
    let event = NewEvent::new(
        EntityType::Collection,
        Action::Created,
        ActorKind::System,
        "collection fanout test",
    )
    .unwrap()
    .with_collection_id(collection_id)
    .with_entity_id(collection_id)
    .with_entity_name(scope.scoped_name("fanout_collection"));

    with_connection(&scope.pool, async |conn| emit_event(conn, &event).await)
        .await
        .unwrap()
}

async fn insert_collection_created_event_at(
    scope: &TestScope,
    collection_id: i32,
    occurred_at_value: chrono::NaiveDateTime,
) -> Event {
    use crate::schema::events::dsl::{
        action, actor_kind, collection_id as event_collection_id, entity_id, entity_name,
        entity_type, event_id, events, metadata, occurred_at, summary,
    };

    with_connection(&scope.pool, async |conn| {
        diesel::insert_into(events)
            .values((
                event_id.eq(Uuid::new_v4()),
                occurred_at.eq(occurred_at_value),
                entity_type.eq(EntityType::Collection.as_str()),
                entity_id.eq(Some(collection_id)),
                entity_name.eq(Some(scope.scoped_name("retention_collection"))),
                event_collection_id.eq(Some(collection_id)),
                action.eq(Action::Created.as_str()),
                actor_kind.eq(ActorKind::System.as_str()),
                summary.eq("collection retention test"),
                metadata.eq(serde_json::json!({})),
            ))
            .get_result::<Event>(conn)
            .await
    })
    .await
    .unwrap()
}

async fn mark_event_dispatched(scope: &TestScope, event_id_value: i64) {
    use crate::schema::events::dsl::{dispatched_at, events, id};

    with_connection(&scope.pool, async |conn| {
        diesel::update(events.filter(id.eq(event_id_value)))
            .set(dispatched_at.eq(Some(chrono::Utc::now().naive_utc())))
            .execute(conn)
            .await
    })
    .await
    .unwrap();
}

async fn delivery_for_event(scope: &TestScope, event_id_value: i64) -> EventDelivery {
    use crate::schema::event_deliveries::dsl::{event_deliveries, event_id};

    with_connection(&scope.pool, async |conn| {
        event_deliveries
            .filter(event_id.eq(event_id_value))
            .first::<EventDelivery>(conn)
            .await
    })
    .await
    .unwrap()
}

async fn expire_delivery_claim(scope: &TestScope, delivery_id: i64) {
    use crate::schema::event_deliveries::dsl::{event_deliveries, id, locked_until};

    with_connection(&scope.pool, async |conn| {
        diesel::update(event_deliveries.filter(id.eq(delivery_id)))
            .set(locked_until.eq(Some(
                chrono::Utc::now().naive_utc() - chrono::Duration::seconds(1),
            )))
            .execute(conn)
            .await
    })
    .await
    .unwrap();
}

fn make_delivery_settings(max_attempts: i32) -> EventDeliverySettings {
    EventDeliverySettings {
        batch_size: 100_000,
        lock_timeout_ms: 30_000,
        transport_timeout_ms: 25_000,
        retry_backoff_base_ms: 1_000,
        retry_backoff_max_ms: 10_000,
        max_attempts,
    }
}

fn make_retention_settings() -> EventRetentionSettings {
    EventRetentionSettings {
        event_retention_days: 365,
        delivery_retention_days: 30,
        batch_size: 100,
    }
}

struct StaticSinkResolver<'a> {
    sink: &'a dyn crate::events::Sink,
}

impl crate::events::SinkResolver for StaticSinkResolver<'_> {
    fn resolve(&self, kind: EventSinkKind) -> Option<&dyn crate::events::Sink> {
        (kind == EventSinkKind::Webhook).then_some(self.sink)
    }
}

struct SuccessfulSink;

impl crate::events::Sink for SuccessfulSink {
    fn deliver<'a>(
        &'a self,
        envelope: &'a crate::events::EventEnvelope,
        subscription: &'a EventSubscription,
        sink: &'a EventSinkModel,
    ) -> futures::future::BoxFuture<'a, Result<(), crate::events::SinkError>> {
        use futures::FutureExt;

        async move {
            assert_eq!(envelope.entity_type, EntityType::Collection.as_str());
            assert_eq!(
                subscription.entity_types,
                vec![EntityType::Collection.as_str().to_string()]
            );
            assert_eq!(sink.kind, EventSinkKind::Webhook);
            Ok(())
        }
        .boxed()
    }
}

struct FailingSink;

impl crate::events::Sink for FailingSink {
    fn deliver<'a>(
        &'a self,
        _envelope: &'a crate::events::EventEnvelope,
        _subscription: &'a EventSubscription,
        _sink: &'a EventSinkModel,
    ) -> futures::future::BoxFuture<'a, Result<(), crate::events::SinkError>> {
        use futures::FutureExt;

        async { Err(crate::events::SinkError::new("delivery failed")) }.boxed()
    }
}

#[actix_web::test]
async fn event_fanout_creates_delivery_for_each_matching_subscription_once() {
    let scope = test_scope();
    let fixture = scope.with_collection().await;
    create_collection_event_subscription(&scope, fixture.collection.id, "fanout_one", true).await;
    create_collection_event_subscription(&scope, fixture.collection.id, "fanout_two", true).await;
    let event = emit_collection_created_event(&scope, fixture.collection.id).await;

    let inserted = fanout_event(&scope.pool, event.id).await.unwrap();
    assert_eq!(inserted, 2);
    assert_eq!(
        count_event_deliveries_for_event(&scope.pool, event.id)
            .await
            .unwrap(),
        2
    );

    let inserted_again = fanout_event(&scope.pool, event.id).await.unwrap();
    assert_eq!(inserted_again, 0);
    assert_eq!(
        count_event_deliveries_for_event(&scope.pool, event.id)
            .await
            .unwrap(),
        2
    );
}

#[actix_web::test]
async fn event_fanout_skips_disabled_subscriptions() {
    let scope = test_scope();
    let fixture = scope.with_collection().await;
    create_collection_event_subscription(&scope, fixture.collection.id, "fanout_disabled", false)
        .await;
    let event = emit_collection_created_event(&scope, fixture.collection.id).await;

    let inserted = fanout_event(&scope.pool, event.id).await.unwrap();

    assert_eq!(inserted, 0);
    assert_eq!(
        count_event_deliveries_for_event(&scope.pool, event.id)
            .await
            .unwrap(),
        0
    );
}

#[actix_web::test]
async fn event_fanout_applies_subscription_filter_before_creating_delivery() {
    let scope = test_scope();
    let fixture = scope.with_collection().await;
    create_collection_event_subscription_with_filter(
        &scope,
        fixture.collection.id,
        "fanout_filter_match",
        true,
        hubuum_events_core::EventSubscriptionFilter {
            entity_ids: vec![fixture.collection.id],
            ..hubuum_events_core::EventSubscriptionFilter::default()
        },
    )
    .await;
    create_collection_event_subscription_with_filter(
        &scope,
        fixture.collection.id,
        "fanout_filter_miss",
        true,
        hubuum_events_core::EventSubscriptionFilter {
            entity_ids: vec![fixture.collection.id + 10_000],
            ..hubuum_events_core::EventSubscriptionFilter::default()
        },
    )
    .await;
    let event = emit_collection_created_event(&scope, fixture.collection.id).await;

    let inserted = fanout_event(&scope.pool, event.id).await.unwrap();

    assert_eq!(inserted, 1);
    assert_eq!(
        count_event_deliveries_for_event(&scope.pool, event.id)
            .await
            .unwrap(),
        1
    );
}

#[actix_web::test]
async fn event_fanout_reclaims_expired_claims() {
    let scope = test_scope();
    let fixture = scope.with_collection().await;
    let event = emit_collection_created_event(&scope, fixture.collection.id).await;
    let settings = EventFanoutSettings {
        batch_size: 100_000,
        lock_timeout_ms: 30_000,
    };

    let claimed = claim_events_for_fanout(&scope.pool, settings)
        .await
        .unwrap();
    assert!(claimed.iter().any(|claimed| claimed.id == event.id));

    let blocked = claim_events_for_fanout(&scope.pool, settings)
        .await
        .unwrap();
    assert!(!blocked.iter().any(|claimed| claimed.id == event.id));

    with_connection(&scope.pool, async |conn| {
        use crate::schema::events::dsl::{events, fanout_locked_until, id};

        diesel::update(events.filter(id.eq(event.id)))
            .set(fanout_locked_until.eq(Some(
                chrono::Utc::now().naive_utc() - chrono::Duration::seconds(1),
            )))
            .execute(conn)
            .await
    })
    .await
    .unwrap();

    let reclaimed = claim_events_for_fanout(&scope.pool, settings)
        .await
        .unwrap();
    assert!(reclaimed.iter().any(|claimed| claimed.id == event.id));
}

#[actix_web::test]
async fn ordinary_event_delete_is_rejected_by_append_only_trigger() {
    let scope = test_scope();
    let event = emit_collection_created_event(&scope, 1).await;

    let error = with_connection(&scope.pool, async |conn| {
        diesel::delete(events.filter(crate::schema::events::dsl::id.eq(event.id)))
            .execute(conn)
            .await
    })
    .await
    .unwrap_err();

    assert!(
        error
            .to_string()
            .contains("events table is append-only: DELETE is not permitted"),
        "unexpected delete error: {error}"
    );
}

#[actix_web::test]
async fn event_retention_purge_deletes_old_events_through_guarded_path() {
    let _lock = lock_test_mutex(&EVENT_RETENTION_TEST_LOCK).await;
    let scope = test_scope();
    let old_event = insert_collection_created_event_at(
        &scope,
        1,
        chrono::Utc::now().naive_utc() - chrono::Duration::days(400),
    )
    .await;
    mark_event_dispatched(&scope, old_event.id).await;

    purge_event_retention_without_archive(&scope.pool, make_retention_settings())
        .await
        .unwrap();

    let remaining = with_connection(&scope.pool, async |conn| {
        events
            .filter(crate::schema::events::dsl::id.eq(old_event.id))
            .count()
            .get_result::<i64>(conn)
            .await
    })
    .await
    .unwrap();
    assert_eq!(remaining, 0);
}

#[actix_web::test]
async fn event_retention_purge_keeps_events_pending_fanout() {
    let _lock = lock_test_mutex(&EVENT_RETENTION_TEST_LOCK).await;
    let scope = test_scope();
    let old_event = insert_collection_created_event_at(
        &scope,
        1,
        chrono::Utc::now().naive_utc() - chrono::Duration::days(400),
    )
    .await;

    purge_event_retention_without_archive(&scope.pool, make_retention_settings())
        .await
        .unwrap();

    let remaining = with_connection(&scope.pool, async |conn| {
        events
            .filter(crate::schema::events::dsl::id.eq(old_event.id))
            .count()
            .get_result::<i64>(conn)
            .await
    })
    .await
    .unwrap();
    assert_eq!(remaining, 1);
}

#[actix_web::test]
async fn event_retention_purge_keeps_events_with_active_deliveries() {
    let _lock = lock_test_mutex(&EVENT_RETENTION_TEST_LOCK).await;
    let scope = test_scope();
    let fixture = scope.with_collection().await;
    create_collection_event_subscription(&scope, fixture.collection.id, "retention_active", true)
        .await;
    let old_event = insert_collection_created_event_at(
        &scope,
        fixture.collection.id,
        chrono::Utc::now().naive_utc() - chrono::Duration::days(400),
    )
    .await;
    fanout_event(&scope.pool, old_event.id).await.unwrap();

    purge_event_retention_without_archive(&scope.pool, make_retention_settings())
        .await
        .unwrap();

    let remaining = with_connection(&scope.pool, async |conn| {
        events
            .filter(crate::schema::events::dsl::id.eq(old_event.id))
            .count()
            .get_result::<i64>(conn)
            .await
    })
    .await
    .unwrap();
    assert_eq!(remaining, 1);
}

#[actix_web::test]
async fn event_retention_purge_deletes_old_terminal_deliveries_without_deleting_event() {
    let _lock = lock_test_mutex(&EVENT_RETENTION_TEST_LOCK).await;
    let scope = test_scope();
    let fixture = scope.with_collection().await;
    let subscription_id = create_collection_event_subscription(
        &scope,
        fixture.collection.id,
        "retention_terminal",
        true,
    )
    .await;
    let event = emit_collection_created_event(&scope, fixture.collection.id).await;
    let old_timestamp = chrono::Utc::now().naive_utc() - chrono::Duration::days(40);
    use crate::schema::event_deliveries::dsl::{
        created_at, event_deliveries, event_id, status,
        subscription_id as delivery_subscription_id, updated_at,
    };
    with_connection(&scope.pool, async |conn| {
        diesel::insert_into(event_deliveries)
            .values((
                event_id.eq(event.id),
                delivery_subscription_id.eq(subscription_id),
                status.eq(EventDeliveryStatus::Succeeded.as_str()),
                created_at.eq(old_timestamp),
                updated_at.eq(old_timestamp),
            ))
            .execute(conn)
            .await
    })
    .await
    .unwrap();

    let summary = purge_event_retention_without_archive(&scope.pool, make_retention_settings())
        .await
        .unwrap();

    assert_eq!(summary.purged_events, 0);
    assert_eq!(summary.purged_terminal_deliveries, 1);
    assert_eq!(
        count_event_deliveries_for_event(&scope.pool, event.id)
            .await
            .unwrap(),
        0
    );
    let remaining_events = with_connection(&scope.pool, async |conn| {
        events
            .filter(crate::schema::events::dsl::id.eq(event.id))
            .count()
            .get_result::<i64>(conn)
            .await
    })
    .await
    .unwrap();
    assert_eq!(remaining_events, 1);
}

#[actix_web::test]
async fn event_delivery_worker_marks_successful_delivery_succeeded() {
    let _lock = lock_test_mutex(&EVENT_DELIVERY_TEST_LOCK).await;
    let scope = test_scope();
    let fixture = scope.with_collection().await;
    create_collection_event_subscription(&scope, fixture.collection.id, "delivery_success", true)
        .await;
    let event = emit_collection_created_event(&scope, fixture.collection.id).await;
    fanout_event(&scope.pool, event.id).await.unwrap();
    let sink = SuccessfulSink;
    let resolver = StaticSinkResolver { sink: &sink };

    let delivery = delivery_for_event(&scope, event.id).await;
    let settings = make_delivery_settings(3);
    let claimed = claim_event_delivery_by_id(&scope.pool, delivery.id, settings)
        .await
        .unwrap();
    process_claimed_event_delivery(&scope.pool, settings, &resolver, claimed)
        .await
        .unwrap();

    let delivery = delivery_for_event(&scope, event.id).await;
    assert_eq!(delivery.status, EventDeliveryStatus::Succeeded.as_str());
    assert_eq!(delivery.attempts, 0);
    assert!(delivery.claim_token.is_none());
    assert!(delivery.locked_until.is_none());
    assert!(delivery.last_error.is_none());
}

#[actix_web::test]
async fn event_delivery_worker_retries_with_backoff_then_marks_dead() {
    let _lock = lock_test_mutex(&EVENT_DELIVERY_TEST_LOCK).await;
    let scope = test_scope();
    let fixture = scope.with_collection().await;
    create_collection_event_subscription(&scope, fixture.collection.id, "delivery_retry", true)
        .await;
    let event = emit_collection_created_event(&scope, fixture.collection.id).await;
    fanout_event(&scope.pool, event.id).await.unwrap();
    let sink = FailingSink;
    let resolver = StaticSinkResolver { sink: &sink };
    let settings = make_delivery_settings(2);

    let delivery = delivery_for_event(&scope, event.id).await;
    let claimed = claim_event_delivery_by_id(&scope.pool, delivery.id, settings)
        .await
        .unwrap();
    process_claimed_event_delivery(&scope.pool, settings, &resolver, claimed)
        .await
        .unwrap();
    let first_failure = delivery_for_event(&scope, event.id).await;
    assert_eq!(first_failure.status, EventDeliveryStatus::Failed.as_str());
    assert_eq!(first_failure.attempts, 1);
    assert_eq!(first_failure.last_error.as_deref(), Some("delivery failed"));
    assert!(first_failure.next_attempt_at > chrono::Utc::now().naive_utc());

    with_connection(&scope.pool, async |conn| {
        use crate::schema::event_deliveries::dsl::{event_deliveries, id, next_attempt_at};

        diesel::update(event_deliveries.filter(id.eq(first_failure.id)))
            .set(next_attempt_at.eq(chrono::Utc::now().naive_utc() - chrono::Duration::seconds(1)))
            .execute(conn)
            .await
    })
    .await
    .unwrap();

    let claimed = claim_event_delivery_by_id(&scope.pool, first_failure.id, settings)
        .await
        .unwrap();
    process_claimed_event_delivery(&scope.pool, settings, &resolver, claimed)
        .await
        .unwrap();
    let dead = delivery_for_event(&scope, event.id).await;
    assert_eq!(dead.status, EventDeliveryStatus::Dead.as_str());
    assert_eq!(dead.attempts, 2);
    assert!(dead.claim_token.is_none());
    assert!(dead.locked_until.is_none());
}

#[actix_web::test]
async fn event_delivery_claims_expired_in_flight_rows_again() {
    let _lock = lock_test_mutex(&EVENT_DELIVERY_TEST_LOCK).await;
    let scope = test_scope();
    let fixture = scope.with_collection().await;
    create_collection_event_subscription(&scope, fixture.collection.id, "delivery_reclaim", true)
        .await;
    let event = emit_collection_created_event(&scope, fixture.collection.id).await;
    fanout_event(&scope.pool, event.id).await.unwrap();
    let settings = make_delivery_settings(3);

    let claimed = claim_event_deliveries(&scope.pool, settings).await.unwrap();
    let delivery_id = claimed
        .iter()
        .find(|claimed| claimed.delivery.event_id == event.id)
        .map(|claimed| claimed.delivery.id)
        .expect("test delivery should be claimed");

    let blocked = claim_event_deliveries(&scope.pool, settings).await.unwrap();
    assert!(
        !blocked
            .iter()
            .any(|claimed| claimed.delivery.id == delivery_id)
    );

    expire_delivery_claim(&scope, delivery_id).await;

    let reclaimed = claim_event_deliveries(&scope.pool, settings).await.unwrap();
    assert!(
        reclaimed
            .iter()
            .any(|claimed| claimed.delivery.id == delivery_id)
    );
}

#[actix_web::test]
async fn event_delivery_failed_mark_respects_claim_token() {
    let _lock = lock_test_mutex(&EVENT_DELIVERY_TEST_LOCK).await;
    let scope = test_scope();
    let fixture = scope.with_collection().await;
    create_collection_event_subscription(
        &scope,
        fixture.collection.id,
        "delivery_claim_token",
        true,
    )
    .await;
    let event = emit_collection_created_event(&scope, fixture.collection.id).await;
    fanout_event(&scope.pool, event.id).await.unwrap();
    let settings = make_delivery_settings(3);
    let mut claimed = claim_event_deliveries(&scope.pool, settings).await.unwrap();
    let mut delivery = claimed.remove(0).delivery;
    delivery.claim_token = Some(Uuid::new_v4());

    let error = mark_event_delivery_failed(&scope.pool, &delivery, settings, "wrong claim").await;

    assert!(matches!(error, Err(ApiError::NotFound(_))));
    mark_event_delivery_dead(&scope.pool, EventDeliveryID::new(delivery.id).unwrap())
        .await
        .unwrap();
}

#[actix_web::test]
async fn collection_writes_emit_lifecycle_events_in_transaction() {
    let scope = test_scope();
    let fixture = scope.with_collection().await;
    let context = EventContext::user(7, Some(Uuid::new_v4()), Some("audit-correlation".into()));
    let collection_name = scope.scoped_name("audited_collection");

    let collection = NewCollectionWithAssignee {
        name: collection_name.clone(),
        description: "before".to_string(),
        group_id: fixture.owner_group.id,
        parent_collection_id: None,
    }
    .save(&scope.pool, &context)
    .await
    .unwrap();

    let updated = UpdateCollection {
        name: Some(collection_name.clone()),
        description: Some("after".to_string()),
    }
    .update(&scope.pool, collection.id, &context)
    .await
    .unwrap();

    let unchanged = UpdateCollection {
        name: Some(collection_name.clone()),
        description: Some("after".to_string()),
    }
    .update(&scope.pool, collection.id, &context)
    .await
    .unwrap();
    assert_eq!(unchanged.updated_at, updated.updated_at);

    unchanged.delete(&scope.pool, &context).await.unwrap();

    let rows = events_for(&scope, "collection", collection.id).await;
    assert_eq!(rows.len(), 3);

    assert_eq!(rows[0].action, "created");
    assert_eq!(
        rows[0].entity_name.as_deref(),
        Some(collection_name.as_str())
    );
    assert_eq!(rows[0].collection_id, Some(collection.id));
    assert_eq!(rows[0].actor_user_id, Some(7));
    assert_eq!(rows[0].correlation_id.as_deref(), Some("audit-correlation"));
    assert_eq!(rows[0].after.as_ref().unwrap()["description"], "before");
    assert_eq!(
        rows[0].metadata["assignee_group_id"],
        serde_json::json!(fixture.owner_group.id)
    );

    assert_eq!(rows[1].action, "updated");
    assert_eq!(rows[1].before.as_ref().unwrap()["description"], "before");
    assert_eq!(rows[1].after.as_ref().unwrap()["description"], "after");

    assert_eq!(rows[2].action, "deleted");
    assert_eq!(rows[2].before.as_ref().unwrap()["description"], "after");
    assert!(rows[2].after.is_none());

    fixture.cleanup().await.unwrap();
}

#[actix_web::test]
async fn moving_a_collection_to_its_current_parent_is_a_noop() {
    let scope = test_scope();
    let fixture = scope.with_collection().await;
    let context = EventContext::user(8, Some(Uuid::new_v4()), None);
    let collection = fixture.collection.clone();
    let parent_id = collection.parent_collection_id.unwrap();
    let event_count = events_for(&scope, "collection", collection.id).await.len();

    let unchanged = move_collection(&scope.pool, collection.id, parent_id, Some(&context))
        .await
        .unwrap();

    assert_eq!(unchanged.updated_at, collection.updated_at);
    assert_eq!(
        events_for(&scope, "collection", collection.id).await.len(),
        event_count
    );
    fixture.cleanup().await.unwrap();
}

#[actix_web::test]
async fn class_writes_emit_lifecycle_events_in_transaction() {
    let scope = test_scope();
    let fixture = scope.with_collection().await;
    let context = EventContext::user(9, Some(Uuid::new_v4()), Some("class-correlation".into()));
    let class_name = scope.scoped_name("audited_class");

    let class = NewHubuumClass {
        name: class_name.clone(),
        collection_id: fixture.collection.id,
        json_schema: Some(serde_json::json!({"type": "object"})),
        validate_schema: Some(true),
        description: "before".to_string(),
    }
    .save(&scope.pool, &context)
    .await
    .unwrap();

    let updated = UpdateHubuumClass {
        name: Some(class_name.clone()),
        collection_id: None,
        json_schema: Some(serde_json::json!({"type": "object", "additionalProperties": true})),
        validate_schema: Some(false),
        description: Some("after".to_string()),
    }
    .update(&scope.pool, class.id, &context)
    .await
    .unwrap();

    let unchanged = UpdateHubuumClass {
        name: Some(class_name.clone()),
        collection_id: Some(fixture.collection.id),
        json_schema: Some(serde_json::json!({
            "type": "object",
            "additionalProperties": true
        })),
        validate_schema: Some(false),
        description: Some("after".to_string()),
    }
    .update(&scope.pool, class.id, &context)
    .await
    .unwrap();
    assert_eq!(unchanged.updated_at, updated.updated_at);

    unchanged.delete(&scope.pool, &context).await.unwrap();

    let rows = events_for(&scope, "class", class.id).await;
    assert_eq!(rows.len(), 3);

    assert_eq!(rows[0].action, "created");
    assert_eq!(rows[0].entity_name.as_deref(), Some(class_name.as_str()));
    assert_eq!(rows[0].collection_id, Some(fixture.collection.id));
    assert_eq!(rows[0].actor_user_id, Some(9));
    assert_eq!(rows[0].correlation_id.as_deref(), Some("class-correlation"));
    assert_eq!(rows[0].after.as_ref().unwrap()["description"], "before");
    assert_eq!(rows[0].after.as_ref().unwrap()["validate_schema"], true);

    assert_eq!(rows[1].action, "updated");
    assert_eq!(rows[1].before.as_ref().unwrap()["description"], "before");
    assert_eq!(rows[1].after.as_ref().unwrap()["description"], "after");
    assert_eq!(rows[1].after.as_ref().unwrap()["validate_schema"], false);

    assert_eq!(rows[2].action, "deleted");
    assert_eq!(rows[2].before.as_ref().unwrap()["description"], "after");
    assert!(rows[2].after.is_none());

    fixture.cleanup().await.unwrap();
}

#[actix_web::test]
async fn object_writes_emit_lifecycle_events_in_transaction() {
    let scope = test_scope();
    let fixture = scope.with_collection().await;
    let context = EventContext::user(11, Some(Uuid::new_v4()), Some("object-correlation".into()));
    let class_name = scope.scoped_name("object_event_class");
    let object_name = scope.scoped_name("audited_object");

    let class = NewHubuumClass {
        name: class_name,
        collection_id: fixture.collection.id,
        json_schema: None,
        validate_schema: Some(false),
        description: "class".to_string(),
    }
    .save_without_events(&scope.pool)
    .await
    .unwrap();

    let object = NewHubuumObject {
        name: object_name.clone(),
        collection_id: fixture.collection.id,
        hubuum_class_id: class.id,
        data: serde_json::json!({"state": "before"}),
        description: "before".to_string(),
    }
    .save(&scope.pool, &context)
    .await
    .unwrap();

    let updated = UpdateHubuumObject {
        name: Some(object_name.clone()),
        collection_id: None,
        hubuum_class_id: None,
        data: Some(serde_json::json!({"state": "after"})),
        description: Some("after".to_string()),
    }
    .update(&scope.pool, object.id, &context)
    .await
    .unwrap();

    let unchanged = UpdateHubuumObject {
        name: Some(object_name.clone()),
        collection_id: Some(fixture.collection.id),
        hubuum_class_id: Some(class.id),
        data: Some(serde_json::json!({"state": "after"})),
        description: Some("after".to_string()),
    }
    .update(&scope.pool, object.id, &context)
    .await
    .unwrap();
    assert_eq!(unchanged.updated_at, updated.updated_at);

    unchanged.delete(&scope.pool, &context).await.unwrap();

    let rows = events_for(&scope, "object", object.id).await;
    assert_eq!(rows.len(), 3);

    assert_eq!(rows[0].action, "created");
    assert_eq!(rows[0].entity_name.as_deref(), Some(object_name.as_str()));
    assert_eq!(rows[0].collection_id, Some(fixture.collection.id));
    assert_eq!(rows[0].actor_user_id, Some(11));
    assert_eq!(
        rows[0].correlation_id.as_deref(),
        Some("object-correlation")
    );
    assert_eq!(rows[0].metadata["class_id"], serde_json::json!(class.id));
    assert_eq!(rows[0].after.as_ref().unwrap()["data"]["state"], "before");

    assert_eq!(rows[1].action, "updated");
    assert_eq!(rows[1].before.as_ref().unwrap()["data"]["state"], "before");
    assert_eq!(rows[1].after.as_ref().unwrap()["data"]["state"], "after");

    assert_eq!(rows[2].action, "deleted");
    assert_eq!(rows[2].before.as_ref().unwrap()["description"], "after");
    assert!(rows[2].after.is_none());

    class.delete_without_events(&scope.pool).await.unwrap();
    fixture.cleanup().await.unwrap();
}

#[actix_web::test]
async fn class_relation_writes_emit_lifecycle_events_in_transaction() {
    let scope = test_scope();
    let fixture = scope.with_collection().await;
    let context = EventContext::user(
        13,
        Some(Uuid::new_v4()),
        Some("class-relation-correlation".into()),
    );

    let class_a = NewHubuumClass {
        name: scope.scoped_name("relation_class_a"),
        collection_id: fixture.collection.id,
        json_schema: None,
        validate_schema: Some(false),
        description: "a".to_string(),
    }
    .save_without_events(&scope.pool)
    .await
    .unwrap();
    let class_b = NewHubuumClass {
        name: scope.scoped_name("relation_class_b"),
        collection_id: fixture.collection.id,
        json_schema: None,
        validate_schema: Some(false),
        description: "b".to_string(),
    }
    .save_without_events(&scope.pool)
    .await
    .unwrap();

    let relation = NewHubuumClassRelation {
        from_hubuum_class_id: class_a.id,
        to_hubuum_class_id: class_b.id,
        forward_template_alias: Some("children".to_string()),
        reverse_template_alias: Some("parents".to_string()),
    }
    .save(&scope.pool, &context)
    .await
    .unwrap();

    HubuumClassRelationID::new(relation.id)
        .unwrap()
        .delete(&scope.pool, &context)
        .await
        .unwrap();

    let rows = events_for(&scope, "class_relation", relation.id).await;
    assert_eq!(rows.len(), 2);

    assert_eq!(rows[0].action, "created");
    assert_eq!(rows[0].actor_user_id, Some(13));
    assert_eq!(
        rows[0].correlation_id.as_deref(),
        Some("class-relation-correlation")
    );
    assert_eq!(
        rows[0].metadata["from_class_id"],
        serde_json::json!(class_a.id)
    );
    assert_eq!(
        rows[0].metadata["to_class_id"],
        serde_json::json!(class_b.id)
    );
    assert_eq!(
        rows[0].metadata["related_collection_ids"],
        serde_json::json!([fixture.collection.id, fixture.collection.id])
    );
    assert_eq!(
        rows[0].after.as_ref().unwrap()["forward_template_alias"],
        "children"
    );

    assert_eq!(rows[1].action, "deleted");
    assert_eq!(
        rows[1].metadata["related_collection_ids"],
        serde_json::json!([fixture.collection.id, fixture.collection.id])
    );
    assert_eq!(
        rows[1].before.as_ref().unwrap()["reverse_template_alias"],
        "parents"
    );
    assert!(rows[1].after.is_none());

    class_a.delete_without_events(&scope.pool).await.unwrap();
    class_b.delete_without_events(&scope.pool).await.unwrap();
    fixture.cleanup().await.unwrap();
}

#[actix_web::test]
async fn object_relation_writes_emit_lifecycle_events_in_transaction() {
    let scope = test_scope();
    let fixture = scope.with_collection().await;
    let context = EventContext::user(
        15,
        Some(Uuid::new_v4()),
        Some("object-relation-correlation".into()),
    );

    let class_a = NewHubuumClass {
        name: scope.scoped_name("object_relation_class_a"),
        collection_id: fixture.collection.id,
        json_schema: None,
        validate_schema: Some(false),
        description: "a".to_string(),
    }
    .save_without_events(&scope.pool)
    .await
    .unwrap();
    let class_b = NewHubuumClass {
        name: scope.scoped_name("object_relation_class_b"),
        collection_id: fixture.collection.id,
        json_schema: None,
        validate_schema: Some(false),
        description: "b".to_string(),
    }
    .save_without_events(&scope.pool)
    .await
    .unwrap();
    let class_relation = NewHubuumClassRelation {
        from_hubuum_class_id: class_a.id,
        to_hubuum_class_id: class_b.id,
        forward_template_alias: None,
        reverse_template_alias: None,
    }
    .save_without_events(&scope.pool)
    .await
    .unwrap();

    let object_a = NewHubuumObject {
        name: scope.scoped_name("object_relation_object_a"),
        collection_id: fixture.collection.id,
        hubuum_class_id: class_a.id,
        data: serde_json::json!({}),
        description: "a".to_string(),
    }
    .save_without_events(&scope.pool)
    .await
    .unwrap();
    let object_b = NewHubuumObject {
        name: scope.scoped_name("object_relation_object_b"),
        collection_id: fixture.collection.id,
        hubuum_class_id: class_b.id,
        data: serde_json::json!({}),
        description: "b".to_string(),
    }
    .save_without_events(&scope.pool)
    .await
    .unwrap();

    let relation = NewHubuumObjectRelation {
        from_hubuum_object_id: object_a.id,
        to_hubuum_object_id: object_b.id,
        class_relation_id: class_relation.id,
    }
    .save(&scope.pool, &context)
    .await
    .unwrap();

    relation.delete(&scope.pool, &context).await.unwrap();

    let rows = events_for(&scope, "object_relation", relation.id).await;
    assert_eq!(rows.len(), 2);

    assert_eq!(rows[0].action, "created");
    assert_eq!(rows[0].actor_user_id, Some(15));
    assert_eq!(
        rows[0].correlation_id.as_deref(),
        Some("object-relation-correlation")
    );
    assert_eq!(
        rows[0].metadata["class_relation_id"],
        serde_json::json!(class_relation.id)
    );
    assert_eq!(
        rows[0].metadata["from_object_id"],
        serde_json::json!(object_a.id)
    );
    assert_eq!(
        rows[0].metadata["to_object_id"],
        serde_json::json!(object_b.id)
    );
    assert_eq!(
        rows[0].after.as_ref().unwrap()["class_relation_id"],
        class_relation.id
    );

    assert_eq!(rows[1].action, "deleted");
    assert_eq!(
        rows[1].before.as_ref().unwrap()["from_hubuum_object_id"],
        object_a.id
    );
    assert!(rows[1].after.is_none());

    object_a.delete_without_events(&scope.pool).await.unwrap();
    object_b.delete_without_events(&scope.pool).await.unwrap();
    class_relation
        .delete_without_events(&scope.pool)
        .await
        .unwrap();
    class_a.delete_without_events(&scope.pool).await.unwrap();
    class_b.delete_without_events(&scope.pool).await.unwrap();
    fixture.cleanup().await.unwrap();
}

#[actix_web::test]
async fn group_writes_emit_lifecycle_events_in_transaction() {
    let scope = test_scope();
    let context = EventContext::user(21, Some(Uuid::new_v4()), Some("group-correlation".into()));

    let group = NewGroup {
        identity_scope: None,
        groupname: scope.scoped_name("event_group"),
        description: Some("before".to_string()),
    }
    .save(&scope.pool, Some(&context))
    .await
    .unwrap();

    let updated = UpdateGroup {
        groupname: Some(scope.scoped_name("event_group_after")),
    }
    .save(group.id, &scope.pool, Some(&context))
    .await
    .unwrap();

    let unchanged = UpdateGroup {
        groupname: Some(updated.groupname.clone()),
    }
    .save(group.id, &scope.pool, Some(&context))
    .await
    .unwrap();
    assert_eq!(unchanged.updated_at, updated.updated_at);

    GroupID::new(unchanged.id)
        .unwrap()
        .delete(&scope.pool, Some(&context))
        .await
        .unwrap();

    let rows = events_for(&scope, "group", group.id).await;
    assert_eq!(rows.len(), 3);

    assert_eq!(rows[0].action, "created");
    assert_eq!(rows[0].actor_user_id, Some(21));
    assert_eq!(rows[0].correlation_id.as_deref(), Some("group-correlation"));
    assert_eq!(
        rows[0].entity_name.as_deref(),
        Some(group.groupname.as_str())
    );
    assert_eq!(
        rows[0].after.as_ref().unwrap()["description"],
        serde_json::json!("before")
    );

    assert_eq!(rows[1].action, "updated");
    assert_eq!(
        rows[1].before.as_ref().unwrap()["groupname"],
        serde_json::json!(group.groupname)
    );
    assert_eq!(
        rows[1].after.as_ref().unwrap()["groupname"],
        serde_json::json!(updated.groupname)
    );

    assert_eq!(rows[2].action, "deleted");
    assert_eq!(
        rows[2].before.as_ref().unwrap()["groupname"],
        serde_json::json!(updated.groupname)
    );
    assert!(rows[2].after.is_none());
}

#[actix_web::test]
async fn group_membership_writes_emit_added_removed_events_when_changed() {
    let scope = test_scope();
    let context = EventContext::user(
        22,
        Some(Uuid::new_v4()),
        Some("membership-correlation".into()),
    );

    let group = NewGroup {
        identity_scope: None,
        groupname: scope.scoped_name("event_membership_group"),
        description: Some("membership group".to_string()),
    }
    .save_without_events(&scope.pool)
    .await
    .unwrap();
    let user = create_test_user(&scope.pool).await;

    group
        .add_member(&scope.pool, &user, Some(&context))
        .await
        .unwrap();
    group
        .add_member(&scope.pool, &user, Some(&context))
        .await
        .unwrap();
    group
        .remove_member(&user, &scope.pool, Some(&context))
        .await
        .unwrap();
    group
        .remove_member(&user, &scope.pool, Some(&context))
        .await
        .unwrap();

    let rows = events_for_type(&scope, "user_group")
        .await
        .into_iter()
        .filter(|row| {
            row.metadata["principal_id"] == serde_json::json!(user.id)
                && row.metadata["group_id"] == serde_json::json!(group.id)
        })
        .collect::<Vec<_>>();
    assert_eq!(rows.len(), 2);

    assert_eq!(rows[0].action, "added");
    assert_eq!(rows[0].actor_user_id, Some(22));
    assert_eq!(
        rows[0].correlation_id.as_deref(),
        Some("membership-correlation")
    );
    assert_eq!(rows[0].metadata["principal_id"], serde_json::json!(user.id));
    assert_eq!(rows[0].metadata["group_id"], serde_json::json!(group.id));

    assert_eq!(rows[1].action, "removed");
    assert_eq!(rows[1].metadata["principal_id"], serde_json::json!(user.id));
    assert_eq!(rows[1].metadata["group_id"], serde_json::json!(group.id));

    group.delete_without_events(&scope.pool).await.unwrap();
    user.delete_without_events(&scope.pool).await.unwrap();
}

#[actix_web::test]
async fn user_writes_emit_lifecycle_events_without_password_material() {
    let scope = test_scope();
    let context = EventContext::user(23, Some(Uuid::new_v4()), Some("user-correlation".into()));
    let username = scope.scoped_name("event_user");

    let user = NewUser {
        identity_scope: None,
        name: username.clone(),
        password: "initial-password".to_string(),
        proper_name: Some("Before User".to_string()),
        email: Some("before@example.invalid".to_string()),
    }
    .save(&scope.pool, Some(&context))
    .await
    .unwrap();

    let updated = UpdateUser {
        password: Some("updated-password".to_string()),
        proper_name: Some("After User".to_string()),
        email: Some("after@example.invalid".to_string()),
    }
    .save(user.id, &scope.pool, Some(&context))
    .await
    .unwrap();

    let unchanged = UpdateUser {
        password: None,
        proper_name: Some("After User".to_string()),
        email: Some("after@example.invalid".to_string()),
    }
    .save(user.id, &scope.pool, Some(&context))
    .await
    .unwrap();
    assert_eq!(unchanged.updated_at, updated.updated_at);

    unchanged.delete(&scope.pool, Some(&context)).await.unwrap();

    let rows = events_for(&scope, "user", user.id).await;
    assert_eq!(rows.len(), 3);

    assert_eq!(rows[0].action, "created");
    assert_eq!(rows[0].actor_user_id, Some(23));
    assert_eq!(rows[0].correlation_id.as_deref(), Some("user-correlation"));
    assert_eq!(rows[0].entity_name.as_deref(), Some(username.as_str()));
    assert_eq!(
        rows[0].after.as_ref().unwrap()["proper_name"],
        serde_json::json!("Before User")
    );
    assert!(rows[0].after.as_ref().unwrap().get("password").is_none());

    assert_eq!(rows[1].action, "updated");
    assert_eq!(
        rows[1].metadata["password_changed"],
        serde_json::json!(true)
    );
    assert_eq!(
        rows[1].before.as_ref().unwrap()["email"],
        serde_json::json!("before@example.invalid")
    );
    assert_eq!(
        rows[1].after.as_ref().unwrap()["email"],
        serde_json::json!("after@example.invalid")
    );
    assert!(rows[1].before.as_ref().unwrap().get("password").is_none());
    assert!(rows[1].after.as_ref().unwrap().get("password").is_none());

    assert_eq!(rows[2].action, "deleted");
    assert_eq!(
        rows[2].before.as_ref().unwrap()["proper_name"],
        serde_json::json!("After User")
    );
    assert!(rows[2].before.as_ref().unwrap().get("password").is_none());
    assert!(rows[2].after.is_none());
}

#[actix_web::test]
async fn token_writes_emit_created_revoked_events_without_token_material() {
    let scope = test_scope();
    let context = EventContext::user(24, Some(Uuid::new_v4()), Some("token-correlation".into()));

    let user = NewUser {
        identity_scope: None,
        name: scope.scoped_name("event_token_user"),
        password: "token-user-password".to_string(),
        proper_name: None,
        email: None,
    }
    .save_without_events(&scope.pool)
    .await
    .unwrap();

    let raw = create_principal_token(
        &scope.pool,
        user.id,
        Some("automation"),
        Some("for event tests"),
        None,
        None,
        Some(&context),
    )
    .await
    .unwrap();
    let token = token_by_raw_value(&scope, &raw).await;

    let revoked = revoke_token_by_id_for_principal(&scope.pool, token.id, user.id, Some(&context))
        .await
        .unwrap();
    assert_eq!(revoked, 1);

    let rows = events_for(&scope, "token", token.id).await;
    assert_eq!(rows.len(), 2);

    assert_eq!(rows[0].action, "created");
    assert_eq!(rows[0].actor_user_id, Some(24));
    assert_eq!(rows[0].correlation_id.as_deref(), Some("token-correlation"));
    assert_eq!(rows[0].metadata["principal_id"], serde_json::json!(user.id));
    assert_eq!(rows[0].after.as_ref().unwrap()["name"], "automation");
    assert!(rows[0].after.as_ref().unwrap().get("token").is_none());

    assert_eq!(rows[1].action, "revoked");
    assert_eq!(rows[1].metadata["principal_id"], serde_json::json!(user.id));
    assert!(rows[1].before.as_ref().unwrap()["revoked_at"].is_null());
    assert!(!rows[1].after.as_ref().unwrap()["revoked_at"].is_null());
    assert!(rows[1].before.as_ref().unwrap().get("token").is_none());
    assert!(rows[1].after.as_ref().unwrap().get("token").is_none());

    user.delete_without_events(&scope.pool).await.unwrap();
}

#[actix_web::test]
async fn permission_writes_emit_granted_revoked_events() {
    let scope = test_scope();
    let fixture = scope.with_collection().await;
    let context = EventContext::user(
        25,
        Some(Uuid::new_v4()),
        Some("permission-correlation".into()),
    );
    let group = NewGroup {
        identity_scope: None,
        groupname: scope.scoped_name("event_permission_group"),
        description: Some("permission group".to_string()),
    }
    .save_without_events(&scope.pool)
    .await
    .unwrap();

    let permission = fixture
        .collection
        .grant(
            &scope.pool,
            group.id,
            PermissionsList::new([Permissions::ReadCollection, Permissions::CreateClass]),
            Some(&context),
        )
        .await
        .unwrap();

    fixture
        .collection
        .grant(
            &scope.pool,
            group.id,
            PermissionsList::new([Permissions::ReadCollection, Permissions::CreateClass]),
            Some(&context),
        )
        .await
        .unwrap();

    fixture
        .collection
        .apply_permissions(
            &scope.pool,
            group.id,
            PermissionsList::new([Permissions::ReadCollection, Permissions::CreateClass]),
            true,
            Some(&context),
        )
        .await
        .unwrap();

    fixture
        .collection
        .revoke(
            &scope.pool,
            group.id,
            PermissionsList::new([Permissions::CreateClass]),
            Some(&context),
        )
        .await
        .unwrap();

    fixture
        .collection
        .revoke(
            &scope.pool,
            group.id,
            PermissionsList::new([Permissions::CreateClass]),
            Some(&context),
        )
        .await
        .unwrap();

    fixture
        .collection
        .revoke_all(&scope.pool, group.id, Some(&context))
        .await
        .unwrap();
    fixture
        .collection
        .revoke_all(&scope.pool, group.id, Some(&context))
        .await
        .unwrap();

    let rows = events_for(&scope, "permission", permission.id).await;
    assert_eq!(rows.len(), 3);

    assert_eq!(rows[0].action, "granted");
    assert_eq!(rows[0].actor_user_id, Some(25));
    assert_eq!(
        rows[0].correlation_id.as_deref(),
        Some("permission-correlation")
    );
    assert_eq!(
        rows[0].metadata["collection_id"],
        serde_json::json!(fixture.collection.id)
    );
    assert_eq!(rows[0].metadata["group_id"], serde_json::json!(group.id));
    assert_eq!(
        rows[0].metadata["requested_permissions"],
        serde_json::json!(["ReadCollection", "CreateClass"])
    );
    assert_eq!(
        rows[0].after.as_ref().unwrap()["granted_permissions"],
        serde_json::json!(["ReadCollection", "CreateClass"])
    );

    assert_eq!(rows[1].action, "revoked");
    assert_eq!(
        rows[1].metadata["requested_permissions"],
        serde_json::json!(["CreateClass"])
    );
    assert_eq!(
        rows[1].before.as_ref().unwrap()["granted_permissions"],
        serde_json::json!(["ReadCollection", "CreateClass"])
    );
    assert_eq!(
        rows[1].after.as_ref().unwrap()["granted_permissions"],
        serde_json::json!(["ReadCollection"])
    );

    assert_eq!(rows[2].action, "revoked");
    assert_eq!(
        rows[2].metadata["requested_permissions"],
        serde_json::json!(["ReadCollection"])
    );
    assert!(rows[2].after.is_none());

    group.delete_without_events(&scope.pool).await.unwrap();
    fixture.cleanup().await.unwrap();
}

#[actix_web::test]
async fn export_template_writes_emit_lifecycle_events() {
    let scope = test_scope();
    let fixture = scope.with_collection().await;
    let context = EventContext::user(
        26,
        Some(Uuid::new_v4()),
        Some("export-template-correlation".into()),
    );

    let template = NewExportTemplate {
        collection_id: fixture.collection.id,
        name: scope.scoped_name("event_template"),
        description: "before".to_string(),
        content_type: ExportContentType::TextPlain,
        template: "Hello {{ name }}".to_string(),
        kind: ExportTemplateKind::Fragment,
        scope_kind: None,
        class_id: None,
        default_query: None,
        include: None,
        relation_context: None,
        default_missing_data_policy: None,
        default_limits: None,
    }
    .save(&scope.pool, &context)
    .await
    .unwrap();

    let updated = UpdateExportTemplate {
        collection_id: None,
        name: None,
        description: Some("after".to_string()),
        template: Some("Goodbye {{ name }}".to_string()),
        kind: None,
        scope_kind: None,
        class_id: None,
        default_query: None,
        include: None,
        relation_context: None,
        default_missing_data_policy: None,
        default_limits: None,
    }
    .update(&scope.pool, template.id, &context)
    .await
    .unwrap();

    UpdateExportTemplate {
        collection_id: Some(fixture.collection.id),
        name: Some(updated.name.clone()),
        description: Some("after".to_string()),
        template: Some("Goodbye {{ name }}".to_string()),
        kind: Some(ExportTemplateKind::Fragment),
        scope_kind: None,
        class_id: None,
        default_query: None,
        include: None,
        relation_context: None,
        default_missing_data_policy: None,
        default_limits: None,
    }
    .update(&scope.pool, template.id, &context)
    .await
    .unwrap();

    ExportTemplateID::new(updated.id)
        .unwrap()
        .delete(&scope.pool, &context)
        .await
        .unwrap();

    let rows = events_for(&scope, "export_template", template.id).await;
    assert_eq!(rows.len(), 3);

    assert_eq!(rows[0].action, "created");
    assert_eq!(rows[0].actor_user_id, Some(26));
    assert_eq!(
        rows[0].correlation_id.as_deref(),
        Some("export-template-correlation")
    );
    assert_eq!(rows[0].collection_id, Some(fixture.collection.id));
    assert_eq!(rows[0].entity_name.as_deref(), Some(template.name.as_str()));
    assert_eq!(rows[0].after.as_ref().unwrap()["description"], "before");

    assert_eq!(rows[1].action, "updated");
    assert_eq!(rows[1].before.as_ref().unwrap()["description"], "before");
    assert_eq!(rows[1].after.as_ref().unwrap()["description"], "after");

    assert_eq!(rows[2].action, "deleted");
    assert_eq!(rows[2].before.as_ref().unwrap()["description"], "after");
    assert!(rows[2].after.is_none());

    fixture.cleanup().await.unwrap();
}

#[actix_web::test]
async fn remote_target_writes_emit_lifecycle_and_invoked_events_with_redacted_auth() {
    let scope = test_scope();
    let fixture = scope.with_collection().await;
    let context = EventContext::user(
        27,
        Some(Uuid::new_v4()),
        Some("remote-target-correlation".into()),
    );

    let row = NewRemoteTargetRow {
        collection_id: fixture.collection.id,
        class_id: None,
        name: scope.scoped_name("event_remote_target"),
        description: "before".to_string(),
        method: "get".to_string(),
        url_template: "https://example.invalid/{{ subject.id }}".to_string(),
        headers_template: serde_json::json!({}),
        body_template: None,
        auth_config: serde_json::json!({
            "type": "api_key_secret",
            "header": "X-Api-Key",
            "secret": "super-secret"
        }),
        allowed_subject_types: serde_json::json!(["collection"]),
        timeout_ms: 1000,
        enabled: true,
    }
    .save_remote_target_record(&scope.pool, Some(&context))
    .await
    .unwrap();

    let updated = UpdateRemoteTargetRow {
        collection_id: None,
        class_id: None,
        name: None,
        description: Some("after".to_string()),
        method: None,
        url_template: None,
        headers_template: None,
        body_template: None,
        auth_config: None,
        allowed_subject_types: None,
        timeout_ms: None,
        enabled: None,
    }
    .update_remote_target_record(&scope.pool, row.id, Some(&context))
    .await
    .unwrap();
    let unchanged = UpdateRemoteTargetRow {
        collection_id: Some(updated.collection_id),
        class_id: Some(updated.class_id),
        name: Some(updated.name.clone()),
        description: Some(updated.description.clone()),
        method: Some(updated.method.clone()),
        url_template: Some(updated.url_template.clone()),
        headers_template: Some(updated.headers_template.clone()),
        body_template: Some(updated.body_template.clone()),
        auth_config: Some(updated.auth_config.clone()),
        allowed_subject_types: Some(updated.allowed_subject_types.clone()),
        timeout_ms: Some(updated.timeout_ms),
        enabled: Some(updated.enabled),
    }
    .update_remote_target_record(&scope.pool, row.id, Some(&context))
    .await
    .unwrap();
    assert_eq!(unchanged.updated_at, updated.updated_at);
    let target = updated.clone().try_into().unwrap();

    emit_remote_target_invoked_event(
        &scope.pool,
        &target,
        &context,
        12345,
        "collection",
        fixture.collection.id,
    )
    .await
    .unwrap();

    RemoteTargetID::new(row.id)
        .unwrap()
        .delete_remote_target_record(&scope.pool, Some(&context))
        .await
        .unwrap();

    let rows = events_for(&scope, "remote_target", row.id).await;
    assert_eq!(rows.len(), 4);

    assert_eq!(rows[0].action, "created");
    assert_eq!(rows[0].actor_user_id, Some(27));
    assert_eq!(
        rows[0].correlation_id.as_deref(),
        Some("remote-target-correlation")
    );
    assert_eq!(rows[0].collection_id, Some(fixture.collection.id));
    assert_eq!(rows[0].after.as_ref().unwrap()["description"], "before");
    assert_eq!(
        rows[0].after.as_ref().unwrap()["auth_config"],
        serde_json::json!("<redacted>")
    );

    assert_eq!(rows[1].action, "updated");
    assert_eq!(rows[1].before.as_ref().unwrap()["description"], "before");
    assert_eq!(rows[1].after.as_ref().unwrap()["description"], "after");
    assert_eq!(
        rows[1].before.as_ref().unwrap()["auth_config"],
        serde_json::json!("<redacted>")
    );

    assert_eq!(rows[2].action, "invoked");
    assert_eq!(rows[2].metadata["task_id"], serde_json::json!(12345));
    assert_eq!(rows[2].metadata["subject_type"], "collection");
    assert_eq!(
        rows[2].metadata["subject_id"],
        serde_json::json!(fixture.collection.id)
    );
    assert!(rows[2].before.is_none());
    assert!(rows[2].after.is_none());

    assert_eq!(rows[3].action, "deleted");
    assert_eq!(rows[3].before.as_ref().unwrap()["description"], "after");
    assert_eq!(
        rows[3].before.as_ref().unwrap()["auth_config"],
        serde_json::json!("<redacted>")
    );

    fixture.cleanup().await.unwrap();
}

async fn events_for(
    scope: &TestScope,
    event_entity_type: &str,
    event_entity_id: i32,
) -> Vec<Event> {
    use crate::schema::events::dsl::{entity_id, entity_type, id};

    with_connection(&scope.pool, async |conn| {
        events
            .filter(entity_type.eq(event_entity_type))
            .filter(entity_id.eq(event_entity_id))
            .order(id.asc())
            .load::<Event>(conn)
            .await
    })
    .await
    .unwrap()
}

async fn token_by_raw_value(scope: &TestScope, raw: &Token) -> PrincipalToken {
    use crate::schema::tokens::dsl::{token, tokens};

    with_connection(&scope.pool, async |conn| {
        tokens
            .filter(token.eq(raw.storage_hash()))
            .first::<PrincipalToken>(conn)
            .await
    })
    .await
    .unwrap()
}

async fn events_for_type(scope: &TestScope, event_entity_type: &str) -> Vec<Event> {
    use crate::schema::events::dsl::{entity_type, id};

    with_connection(&scope.pool, async |conn| {
        events
            .filter(entity_type.eq(event_entity_type))
            .order(id.asc())
            .load::<Event>(conn)
            .await
    })
    .await
    .unwrap()
}

#[derive(diesel::QueryableByName)]
struct IndexExistsRow {
    #[diesel(sql_type = diesel::sql_types::Bool)]
    exists: bool,
}
