//! PostgreSQL LISTEN/NOTIFY support for durable-worker wake-ups.
//!
//! The application owns process lifecycle and supplies the shutdown future.
//! This module owns the PostgreSQL channels, listener connection, retry loop,
//! and notification filtering.

use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use diesel::sql_types::Text;
use diesel_async::RunQueryDsl as _;
use futures_util::StreamExt as _;
use hubuum_storage_core::StorageNotification;
use tracing::{debug, error, info};

use crate::{PostgresConnection, PostgresPool, PostgresStorageError};

const RETRY_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Clone, Copy)]
struct NotificationChannel(&'static str);

impl NotificationChannel {
    const EVENT_FANOUT: Self = Self("hubuum_events_fanout");
    const EVENT_DELIVERY: Self = Self("hubuum_event_delivery");
    const TASK_QUEUE: Self = Self("hubuum_task_queue");

    const fn for_topic(topic: StorageNotification) -> Self {
        match topic {
            StorageNotification::EventFanout => Self::EVENT_FANOUT,
            StorageNotification::EventDelivery => Self::EVENT_DELIVERY,
            StorageNotification::TaskQueue => Self::TASK_QUEUE,
        }
    }

    const fn as_str(self) -> &'static str {
        self.0
    }
}

/// Return the native channel used for a backend-neutral wake-up topic.
///
/// Exposed for adapter-native diagnostics and tests. Application behavior must
/// continue to select topics through [`StorageNotification`].
#[doc(hidden)]
#[must_use]
#[cfg(any(test, feature = "integration-test-support"))]
pub const fn channel_name(topic: StorageNotification) -> &'static str {
    NotificationChannel::for_topic(topic).as_str()
}

/// Emit a task-queue wake-up inside the caller's transaction.
#[doc(hidden)]
pub async fn notify_task_queue(
    conn: &mut PostgresConnection,
    task_id: i32,
) -> Result<usize, PostgresStorageError> {
    notify_channel(conn, NotificationChannel::TASK_QUEUE, &task_id.to_string()).await
}

async fn notify_channel(
    conn: &mut PostgresConnection,
    channel: NotificationChannel,
    payload: &str,
) -> Result<usize, PostgresStorageError> {
    diesel::sql_query("SELECT pg_notify($1, $2)")
        .bind::<Text, _>(channel.as_str())
        .bind::<Text, _>(payload)
        .execute(conn)
        .await
        .map_err(PostgresStorageError::from)
}

/// Run a resilient native notification listener until `shutdown` resolves.
///
/// Correctness must not depend on this listener: durable workers retain their
/// polling path. `on_listening` exists so composition and native tests can
/// observe successful registration without exposing the connection.
pub async fn listen<F>(
    pool: PostgresPool,
    topic: StorageNotification,
    on_notification: fn(),
    on_listening: fn(),
    shutdown: F,
) where
    F: Future<Output = ()>,
{
    let channel = NotificationChannel::for_topic(topic);
    futures_util::pin_mut!(shutdown);

    loop {
        let connection = tokio::select! {
            biased;
            () = shutdown.as_mut() => break,
            connection = pool.get() => connection,
        };

        match connection {
            Ok(mut conn) => {
                let listen_result = tokio::select! {
                    biased;
                    () = shutdown.as_mut() => break,
                    result = diesel::sql_query(format!("LISTEN {}", channel.as_str()))
                        .execute(&mut conn) => result,
                };
                if let Err(error) = listen_result {
                    error!(
                        message = "Failed to register Postgres notification listener",
                        channel = channel.as_str(),
                        error = %error
                    );
                    if !wait_for_retry_or_shutdown(shutdown.as_mut()).await {
                        break;
                    }
                    continue;
                }

                info!(
                    message = "Listening for Postgres worker notifications",
                    channel = channel.as_str()
                );
                on_listening();
                if poll_notifications(&mut conn, channel, on_notification, shutdown.as_mut()).await
                {
                    if let Err(error) = diesel::sql_query(format!("UNLISTEN {}", channel.as_str()))
                        .execute(&mut conn)
                        .await
                    {
                        info!(
                            message = "Postgres notification connection closed during shutdown",
                            channel = channel.as_str(),
                            error = %error
                        );
                    }
                    break;
                }
            }
            Err(error) => {
                error!(
                    message = "Failed to acquire Postgres notification listener connection",
                    channel = channel.as_str(),
                    error = %error
                );
                if !wait_for_retry_or_shutdown(shutdown.as_mut()).await {
                    break;
                }
            }
        }
    }
}

async fn wait_for_retry_or_shutdown<F>(mut shutdown: Pin<&mut F>) -> bool
where
    F: Future<Output = ()>,
{
    tokio::select! {
        biased;
        () = shutdown.as_mut() => false,
        () = tokio::time::sleep(RETRY_INTERVAL) => true,
    }
}

async fn poll_notifications<F>(
    conn: &mut PostgresConnection,
    channel: NotificationChannel,
    on_notification: fn(),
    mut shutdown: Pin<&mut F>,
) -> bool
where
    F: Future<Output = ()>,
{
    let notifications = conn.notifications_stream();
    futures_util::pin_mut!(notifications);
    loop {
        let notification = tokio::select! {
            biased;
            () = shutdown.as_mut() => return true,
            notification = notifications.next() => notification,
        };
        let Some(notification) = notification else {
            return false;
        };
        match notification {
            Ok(notification) if notification.channel == channel.as_str() => {
                debug!(
                    message = "Received Postgres worker notification",
                    channel = channel.as_str(),
                    process_id = notification.process_id
                );
                on_notification();
            }
            Ok(_) => {}
            Err(error) => {
                error!(
                    message = "Postgres notification listener failed",
                    channel = channel.as_str(),
                    error = %error
                );
                return false;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn topics_map_to_stable_postgres_channels() {
        assert_eq!(
            channel_name(StorageNotification::EventFanout),
            "hubuum_events_fanout"
        );
        assert_eq!(
            channel_name(StorageNotification::EventDelivery),
            "hubuum_event_delivery"
        );
        assert_eq!(
            channel_name(StorageNotification::TaskQueue),
            "hubuum_task_queue"
        );
    }
}
