use crate::storage::postgres::prelude::*;
use diesel::sql_types::{BigInt, Bool, Integer, Nullable, Text};

use crate::errors::ApiError;
use crate::storage::postgres::with_connection;
use crate::storage::{
    EventDeliveryHealthSnapshot, EventDeliveryStatusSnapshot, EventFanoutSnapshot,
    EventMetricsSnapshot, EventQueueSnapshot, EventSinkHealthSnapshot, EventSinkSnapshot,
    EventSubscriptionHealthSnapshot,
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

pub(crate) async fn load_event_delivery_health(
    pool: &crate::storage::postgres::PostgresPool,
) -> Result<EventDeliveryHealthSnapshot, ApiError> {
    with_connection(pool, async |conn| {
        let fanout = load_fanout_health(conn).await?;
        let delivery = load_delivery_queue_health(conn).await?;
        let sinks = load_sink_health(conn).await?;
        let subscriptions = load_subscription_health(conn).await?;

        Ok::<EventDeliveryHealthSnapshot, ApiError>(EventDeliveryHealthSnapshot::new(
            fanout,
            delivery,
            sinks,
            subscriptions,
        ))
    })
    .await
}

pub(crate) async fn load_event_metrics_snapshot(
    pool: &crate::storage::postgres::PostgresPool,
) -> Result<EventMetricsSnapshot, ApiError> {
    with_connection(pool, async |conn| {
        Ok::<EventMetricsSnapshot, ApiError>(EventMetricsSnapshot {
            fanout: load_fanout_health(conn).await?,
            delivery: load_delivery_queue_health(conn).await?,
        })
    })
    .await
}

async fn load_fanout_health(
    conn: &mut crate::storage::postgres::PostgresConnection,
) -> Result<EventFanoutSnapshot, ApiError> {
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

    Ok(EventFanoutSnapshot::new(
        row.pending_events,
        row.in_flight_events,
        row.stale_claims,
        row.oldest_pending_age_seconds,
    ))
}

async fn load_delivery_queue_health(
    conn: &mut crate::storage::postgres::PostgresConnection,
) -> Result<EventQueueSnapshot, ApiError> {
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

    Ok(EventQueueSnapshot::new(
        status_counts(&row),
        row.stale_claims,
        row.oldest_due_age_seconds,
    ))
}

async fn load_sink_health(
    conn: &mut crate::storage::postgres::PostgresConnection,
) -> Result<Vec<EventSinkHealthSnapshot>, ApiError> {
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
            EventSinkHealthSnapshot::new(
                EventSinkSnapshot::new(row.sink_id, row.sink_name, row.sink_kind, row.sink_enabled),
                row.subscription_count,
                EventQueueSnapshot::new(counts, row.stale_claims, row.oldest_due_age_seconds),
            )
        })
        .collect())
}

async fn load_subscription_health(
    conn: &mut crate::storage::postgres::PostgresConnection,
) -> Result<Vec<EventSubscriptionHealthSnapshot>, ApiError> {
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
            EventSubscriptionHealthSnapshot::new(
                row.subscription_id,
                row.subscription_name,
                row.collection_id,
                row.subscription_enabled,
                EventSinkSnapshot::new(row.sink_id, row.sink_name, row.sink_kind, row.sink_enabled),
                EventQueueSnapshot::new(counts, row.stale_claims, row.oldest_due_age_seconds),
            )
        })
        .collect())
}

fn status_counts(row: &impl HasDeliveryCounts) -> EventDeliveryStatusSnapshot {
    EventDeliveryStatusSnapshot::new(
        row.total(),
        row.pending(),
        row.in_flight(),
        row.succeeded(),
        row.failed(),
        row.dead(),
        row.retryable(),
    )
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
