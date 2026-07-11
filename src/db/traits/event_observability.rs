use crate::db::prelude::*;
use diesel::sql_types::{BigInt, Bool, Integer, Nullable, Text};

use crate::config::{
    DEFAULT_EVENT_DELIVERY_BATCH_SIZE, DEFAULT_EVENT_DELIVERY_LOCK_TIMEOUT_MS,
    DEFAULT_EVENT_DELIVERY_POLL_INTERVAL_MS, DEFAULT_EVENT_DELIVERY_WORKERS,
    DEFAULT_EVENT_FANOUT_BATCH_SIZE, DEFAULT_EVENT_FANOUT_LOCK_TIMEOUT_MS,
    DEFAULT_EVENT_FANOUT_POLL_INTERVAL_MS, DEFAULT_EVENT_FANOUT_WORKERS, get_config,
};
use crate::db::{DbPool, with_connection};
use crate::errors::ApiError;
use crate::events::{event_delivery_wakeup_stats, event_fanout_wakeup_stats};
use crate::models::{
    EventDeliveryHealthResponse, EventDeliveryQueueHealth, EventDeliveryStatusCounts,
    EventFanoutHealth, EventSinkDeliveryHealth, EventSubscriptionDeliveryHealth, EventWorkerHealth,
};

#[derive(Debug, QueryableByName)]
struct FanoutHealthRow {
    #[diesel(sql_type = BigInt)]
    pending_events: i64,
    #[diesel(sql_type = BigInt)]
    in_flight_events: i64,
    #[diesel(sql_type = BigInt)]
    stale_claims: i64,
    #[diesel(sql_type = Nullable<BigInt>)]
    oldest_pending_age_seconds: Option<i64>,
}

#[derive(Debug, QueryableByName)]
struct DeliveryQueueHealthRow {
    #[diesel(sql_type = BigInt)]
    total: i64,
    #[diesel(sql_type = BigInt)]
    pending: i64,
    #[diesel(sql_type = BigInt)]
    in_flight: i64,
    #[diesel(sql_type = BigInt)]
    succeeded: i64,
    #[diesel(sql_type = BigInt)]
    failed: i64,
    #[diesel(sql_type = BigInt)]
    dead: i64,
    #[diesel(sql_type = BigInt)]
    retryable: i64,
    #[diesel(sql_type = BigInt)]
    stale_claims: i64,
    #[diesel(sql_type = Nullable<BigInt>)]
    oldest_due_age_seconds: Option<i64>,
}

#[derive(Debug, QueryableByName)]
struct SinkHealthRow {
    #[diesel(sql_type = Integer)]
    sink_id: i32,
    #[diesel(sql_type = Text)]
    sink_name: String,
    #[diesel(sql_type = Text)]
    sink_kind: String,
    #[diesel(sql_type = Bool)]
    sink_enabled: bool,
    #[diesel(sql_type = BigInt)]
    subscription_count: i64,
    #[diesel(sql_type = BigInt)]
    total: i64,
    #[diesel(sql_type = BigInt)]
    pending: i64,
    #[diesel(sql_type = BigInt)]
    in_flight: i64,
    #[diesel(sql_type = BigInt)]
    succeeded: i64,
    #[diesel(sql_type = BigInt)]
    failed: i64,
    #[diesel(sql_type = BigInt)]
    dead: i64,
    #[diesel(sql_type = BigInt)]
    retryable: i64,
    #[diesel(sql_type = BigInt)]
    stale_claims: i64,
    #[diesel(sql_type = Nullable<BigInt>)]
    oldest_due_age_seconds: Option<i64>,
}

#[derive(Debug, QueryableByName)]
struct SubscriptionHealthRow {
    #[diesel(sql_type = Integer)]
    subscription_id: i32,
    #[diesel(sql_type = Text)]
    subscription_name: String,
    #[diesel(sql_type = Integer)]
    collection_id: i32,
    #[diesel(sql_type = Integer)]
    sink_id: i32,
    #[diesel(sql_type = Text)]
    sink_name: String,
    #[diesel(sql_type = Text)]
    sink_kind: String,
    #[diesel(sql_type = Bool)]
    subscription_enabled: bool,
    #[diesel(sql_type = Bool)]
    sink_enabled: bool,
    #[diesel(sql_type = BigInt)]
    total: i64,
    #[diesel(sql_type = BigInt)]
    pending: i64,
    #[diesel(sql_type = BigInt)]
    in_flight: i64,
    #[diesel(sql_type = BigInt)]
    succeeded: i64,
    #[diesel(sql_type = BigInt)]
    failed: i64,
    #[diesel(sql_type = BigInt)]
    dead: i64,
    #[diesel(sql_type = BigInt)]
    retryable: i64,
    #[diesel(sql_type = BigInt)]
    stale_claims: i64,
    #[diesel(sql_type = Nullable<BigInt>)]
    oldest_due_age_seconds: Option<i64>,
}

pub async fn load_event_delivery_health(
    pool: &DbPool,
) -> Result<EventDeliveryHealthResponse, ApiError> {
    with_connection(pool, async |conn| {
        let fanout = load_fanout_health(conn).await?;
        let delivery = load_delivery_queue_health(conn).await?;
        let sinks = load_sink_health(conn).await?;
        let subscriptions = load_subscription_health(conn).await?;

        Ok::<EventDeliveryHealthResponse, ApiError>(EventDeliveryHealthResponse {
            fanout,
            delivery,
            sinks,
            subscriptions,
        })
    })
    .await
}

async fn load_fanout_health(
    conn: &mut crate::db::DbConnection,
) -> Result<EventFanoutHealth, ApiError> {
    let row = diesel::sql_query(
        r#"
        SELECT
            COUNT(*) AS pending_events,
            COUNT(*) FILTER (
                WHERE fanout_locked_until IS NOT NULL
                  AND fanout_locked_until > NOW()
            ) AS in_flight_events,
            COUNT(*) FILTER (
                WHERE fanout_locked_until IS NOT NULL
                  AND fanout_locked_until <= NOW()
            ) AS stale_claims,
            CASE
                WHEN MIN(occurred_at) IS NULL THEN NULL
                ELSE GREATEST(
                    0,
                    EXTRACT(EPOCH FROM (NOW() - MIN(occurred_at)))::bigint
                )
            END AS oldest_pending_age_seconds
        FROM events
        WHERE dispatched_at IS NULL
        "#,
    )
    .get_result::<FanoutHealthRow>(conn)
    .await?;

    Ok(EventFanoutHealth {
        pending_events: row.pending_events,
        in_flight_events: row.in_flight_events,
        stale_claims: row.stale_claims,
        oldest_pending_age_seconds: row.oldest_pending_age_seconds,
        worker: fanout_worker_health(),
    })
}

async fn load_delivery_queue_health(
    conn: &mut crate::db::DbConnection,
) -> Result<EventDeliveryQueueHealth, ApiError> {
    let row = diesel::sql_query(
        r#"
        SELECT
            COUNT(*) AS total,
            COUNT(*) FILTER (WHERE status = 'pending') AS pending,
            COUNT(*) FILTER (WHERE status = 'in_flight') AS in_flight,
            COUNT(*) FILTER (WHERE status = 'succeeded') AS succeeded,
            COUNT(*) FILTER (WHERE status = 'failed') AS failed,
            COUNT(*) FILTER (WHERE status = 'dead') AS dead,
            COUNT(*) FILTER (
                WHERE status = 'failed'
                  AND next_attempt_at <= NOW()
            ) AS retryable,
            COUNT(*) FILTER (
                WHERE status = 'in_flight'
                  AND locked_until <= NOW()
            ) AS stale_claims,
            CASE
                WHEN MIN(created_at) FILTER (
                    WHERE status = 'pending'
                       OR (status = 'failed' AND next_attempt_at <= NOW())
                       OR (status = 'in_flight' AND locked_until <= NOW())
                ) IS NULL THEN NULL
                ELSE GREATEST(
                    0,
                    EXTRACT(EPOCH FROM (NOW() - MIN(created_at) FILTER (
                        WHERE status = 'pending'
                           OR (status = 'failed' AND next_attempt_at <= NOW())
                           OR (status = 'in_flight' AND locked_until <= NOW())
                    )))::bigint
                )
            END AS oldest_due_age_seconds
        FROM event_deliveries
        "#,
    )
    .get_result::<DeliveryQueueHealthRow>(conn)
    .await?;

    Ok(EventDeliveryQueueHealth {
        counts: status_counts(&row),
        stale_claims: row.stale_claims,
        oldest_due_age_seconds: row.oldest_due_age_seconds,
        worker: delivery_worker_health(),
    })
}

async fn load_sink_health(
    conn: &mut crate::db::DbConnection,
) -> Result<Vec<EventSinkDeliveryHealth>, ApiError> {
    let rows = diesel::sql_query(
        r#"
        SELECT
            s.id AS sink_id,
            s.name AS sink_name,
            s.kind AS sink_kind,
            s.enabled AS sink_enabled,
            COUNT(DISTINCT sub.id) AS subscription_count,
            COUNT(d.id) AS total,
            COUNT(d.id) FILTER (WHERE d.status = 'pending') AS pending,
            COUNT(d.id) FILTER (WHERE d.status = 'in_flight') AS in_flight,
            COUNT(d.id) FILTER (WHERE d.status = 'succeeded') AS succeeded,
            COUNT(d.id) FILTER (WHERE d.status = 'failed') AS failed,
            COUNT(d.id) FILTER (WHERE d.status = 'dead') AS dead,
            COUNT(d.id) FILTER (
                WHERE d.status = 'failed'
                  AND d.next_attempt_at <= NOW()
            ) AS retryable,
            COUNT(d.id) FILTER (
                WHERE d.status = 'in_flight'
                  AND d.locked_until <= NOW()
            ) AS stale_claims,
            CASE
                WHEN MIN(d.created_at) FILTER (
                    WHERE d.status = 'pending'
                       OR (d.status = 'failed' AND d.next_attempt_at <= NOW())
                       OR (d.status = 'in_flight' AND d.locked_until <= NOW())
                ) IS NULL THEN NULL
                ELSE GREATEST(
                    0,
                    EXTRACT(EPOCH FROM (NOW() - MIN(d.created_at) FILTER (
                        WHERE d.status = 'pending'
                           OR (d.status = 'failed' AND d.next_attempt_at <= NOW())
                           OR (d.status = 'in_flight' AND d.locked_until <= NOW())
                    )))::bigint
                )
            END AS oldest_due_age_seconds
        FROM event_sinks s
        LEFT JOIN event_subscriptions sub ON sub.sink_id = s.id
        LEFT JOIN event_deliveries d ON d.subscription_id = sub.id
        GROUP BY s.id, s.name, s.kind, s.enabled
        ORDER BY s.id
        "#,
    )
    .load::<SinkHealthRow>(conn)
    .await?;

    Ok(rows
        .into_iter()
        .map(|row| {
            let counts = status_counts(&row);
            EventSinkDeliveryHealth {
                sink_id: row.sink_id,
                sink_name: row.sink_name,
                sink_kind: row.sink_kind,
                sink_enabled: row.sink_enabled,
                subscription_count: row.subscription_count,
                counts,
                stale_claims: row.stale_claims,
                oldest_due_age_seconds: row.oldest_due_age_seconds,
            }
        })
        .collect())
}

async fn load_subscription_health(
    conn: &mut crate::db::DbConnection,
) -> Result<Vec<EventSubscriptionDeliveryHealth>, ApiError> {
    let rows = diesel::sql_query(
        r#"
        SELECT
            sub.id AS subscription_id,
            sub.name AS subscription_name,
            sub.collection_id AS collection_id,
            s.id AS sink_id,
            s.name AS sink_name,
            s.kind AS sink_kind,
            sub.enabled AS subscription_enabled,
            s.enabled AS sink_enabled,
            COUNT(d.id) AS total,
            COUNT(d.id) FILTER (WHERE d.status = 'pending') AS pending,
            COUNT(d.id) FILTER (WHERE d.status = 'in_flight') AS in_flight,
            COUNT(d.id) FILTER (WHERE d.status = 'succeeded') AS succeeded,
            COUNT(d.id) FILTER (WHERE d.status = 'failed') AS failed,
            COUNT(d.id) FILTER (WHERE d.status = 'dead') AS dead,
            COUNT(d.id) FILTER (
                WHERE d.status = 'failed'
                  AND d.next_attempt_at <= NOW()
            ) AS retryable,
            COUNT(d.id) FILTER (
                WHERE d.status = 'in_flight'
                  AND d.locked_until <= NOW()
            ) AS stale_claims,
            CASE
                WHEN MIN(d.created_at) FILTER (
                    WHERE d.status = 'pending'
                       OR (d.status = 'failed' AND d.next_attempt_at <= NOW())
                       OR (d.status = 'in_flight' AND d.locked_until <= NOW())
                ) IS NULL THEN NULL
                ELSE GREATEST(
                    0,
                    EXTRACT(EPOCH FROM (NOW() - MIN(d.created_at) FILTER (
                        WHERE d.status = 'pending'
                           OR (d.status = 'failed' AND d.next_attempt_at <= NOW())
                           OR (d.status = 'in_flight' AND d.locked_until <= NOW())
                    )))::bigint
                )
            END AS oldest_due_age_seconds
        FROM event_subscriptions sub
        INNER JOIN event_sinks s ON s.id = sub.sink_id
        LEFT JOIN event_deliveries d ON d.subscription_id = sub.id
        GROUP BY sub.id, sub.name, sub.collection_id, s.id, s.name, s.kind, sub.enabled, s.enabled
        ORDER BY sub.id
        "#,
    )
    .load::<SubscriptionHealthRow>(conn)
    .await?;

    Ok(rows
        .into_iter()
        .map(|row| {
            let counts = status_counts(&row);
            EventSubscriptionDeliveryHealth {
                subscription_id: row.subscription_id,
                subscription_name: row.subscription_name,
                collection_id: row.collection_id,
                sink_id: row.sink_id,
                sink_name: row.sink_name,
                sink_kind: row.sink_kind,
                subscription_enabled: row.subscription_enabled,
                sink_enabled: row.sink_enabled,
                counts,
                stale_claims: row.stale_claims,
                oldest_due_age_seconds: row.oldest_due_age_seconds,
            }
        })
        .collect())
}

fn fanout_worker_health() -> EventWorkerHealth {
    let config = get_config().ok();
    EventWorkerHealth {
        workers_configured: config
            .as_ref()
            .map(|config| config.event_fanout_workers)
            .unwrap_or(DEFAULT_EVENT_FANOUT_WORKERS),
        batch_size: config
            .as_ref()
            .map(|config| config.event_fanout_batch_size)
            .unwrap_or(DEFAULT_EVENT_FANOUT_BATCH_SIZE),
        poll_interval_ms: config
            .as_ref()
            .map(|config| config.event_fanout_poll_interval_ms)
            .unwrap_or(DEFAULT_EVENT_FANOUT_POLL_INTERVAL_MS),
        lock_timeout_ms: config
            .as_ref()
            .map(|config| config.event_fanout_lock_timeout_ms)
            .unwrap_or(DEFAULT_EVENT_FANOUT_LOCK_TIMEOUT_MS),
        wakeups: event_fanout_wakeup_stats(),
    }
}

fn delivery_worker_health() -> EventWorkerHealth {
    let config = get_config().ok();
    EventWorkerHealth {
        workers_configured: config
            .as_ref()
            .map(|config| config.event_delivery_workers)
            .unwrap_or(DEFAULT_EVENT_DELIVERY_WORKERS),
        batch_size: config
            .as_ref()
            .map(|config| config.event_delivery_batch_size)
            .unwrap_or(DEFAULT_EVENT_DELIVERY_BATCH_SIZE),
        poll_interval_ms: config
            .as_ref()
            .map(|config| config.event_delivery_poll_interval_ms)
            .unwrap_or(DEFAULT_EVENT_DELIVERY_POLL_INTERVAL_MS),
        lock_timeout_ms: config
            .as_ref()
            .map(|config| config.event_delivery_lock_timeout_ms)
            .unwrap_or(DEFAULT_EVENT_DELIVERY_LOCK_TIMEOUT_MS),
        wakeups: event_delivery_wakeup_stats(),
    }
}

fn status_counts(row: &impl HasDeliveryCounts) -> EventDeliveryStatusCounts {
    EventDeliveryStatusCounts {
        total: row.total(),
        pending: row.pending(),
        in_flight: row.in_flight(),
        succeeded: row.succeeded(),
        failed: row.failed(),
        dead: row.dead(),
        retryable: row.retryable(),
    }
}

trait HasDeliveryCounts {
    fn total(&self) -> i64;
    fn pending(&self) -> i64;
    fn in_flight(&self) -> i64;
    fn succeeded(&self) -> i64;
    fn failed(&self) -> i64;
    fn dead(&self) -> i64;
    fn retryable(&self) -> i64;
}

macro_rules! impl_delivery_counts {
    ($type:ty) => {
        impl HasDeliveryCounts for $type {
            fn total(&self) -> i64 {
                self.total
            }

            fn pending(&self) -> i64 {
                self.pending
            }

            fn in_flight(&self) -> i64 {
                self.in_flight
            }

            fn succeeded(&self) -> i64 {
                self.succeeded
            }

            fn failed(&self) -> i64 {
                self.failed
            }

            fn dead(&self) -> i64 {
                self.dead
            }

            fn retryable(&self) -> i64 {
                self.retryable
            }
        }
    };
}

impl_delivery_counts!(DeliveryQueueHealthRow);
impl_delivery_counts!(SinkHealthRow);
impl_delivery_counts!(SubscriptionHealthRow);
