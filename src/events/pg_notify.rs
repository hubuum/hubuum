use std::time::Duration;

use crate::db::prelude::*;
use futures_util::StreamExt;
use tracing::{debug, error, info};

use crate::config::get_config;
use crate::db::{DatabasePoolSettings, DbPool, init_pool_with_settings};
use crate::lifecycle::{ShutdownSignal, spawn_background_worker};

const NOTIFICATION_LISTENER_POOL_SIZE: u32 = 1;

#[derive(Clone, Copy)]
pub(crate) struct NotificationChannel(&'static str);

impl NotificationChannel {
    const fn new(channel: &'static str) -> Self {
        Self(channel)
    }

    pub(crate) const fn as_str(self) -> &'static str {
        self.0
    }
}

pub(crate) const EVENT_FANOUT_CHANNEL: NotificationChannel =
    NotificationChannel::new("hubuum_events_fanout");
pub(crate) const EVENT_DELIVERY_CHANNEL: NotificationChannel =
    NotificationChannel::new("hubuum_event_delivery");
pub(crate) const TASK_QUEUE_CHANNEL: NotificationChannel =
    NotificationChannel::new("hubuum_task_queue");

pub(crate) async fn notify_event_delivery(
    conn: &mut crate::db::DbConnection,
) -> QueryResult<usize> {
    notify_channel(conn, EVENT_DELIVERY_CHANNEL, "").await
}

pub(crate) async fn notify_task_queue(
    conn: &mut crate::db::DbConnection,
    task_id: i32,
) -> QueryResult<usize> {
    let payload = task_id.to_string();
    notify_channel(conn, TASK_QUEUE_CHANNEL, &payload).await
}

async fn notify_channel(
    conn: &mut crate::db::DbConnection,
    channel: NotificationChannel,
    payload: &str,
) -> QueryResult<usize> {
    diesel::sql_query("SELECT pg_notify($1, $2)")
        .bind::<diesel::sql_types::Text, _>(channel.as_str())
        .bind::<diesel::sql_types::Text, _>(payload)
        .execute(conn)
        .await
}

pub(crate) fn spawn_postgres_notification_listener(
    channel: NotificationChannel,
    thread_name: &'static str,
    on_notification: fn(),
) {
    let pool = new_postgres_notification_listener_pool();
    spawn_background_worker(thread_name, move |shutdown| {
        let system = actix_rt::System::new();
        system.block_on(listen_loop(pool, channel, on_notification, || {}, shutdown));
    });
}

fn new_postgres_notification_listener_pool() -> DbPool {
    let config =
        get_config().expect("Postgres notification listeners require database configuration");
    let settings = DatabasePoolSettings::builder(config.database_url.clone())
        .max_size(NOTIFICATION_LISTENER_POOL_SIZE)
        .statement_timeout_ms(config.db_statement_timeout_ms)
        .acquire_timeout_ms(config.db_pool_acquire_timeout_ms)
        .build()
        .expect("Postgres notification listener pool settings must be valid");
    init_pool_with_settings(&settings)
}

async fn listen_loop(
    pool: DbPool,
    channel: NotificationChannel,
    on_notification: fn(),
    on_listening: fn(),
    shutdown: ShutdownSignal,
) {
    loop {
        let connection = tokio::select! {
            biased;
            _ = shutdown.requested() => break,
            connection = pool.get() => connection,
        };
        match connection {
            Ok(mut conn) => {
                let listen_result = tokio::select! {
                    biased;
                    _ = shutdown.requested() => break,
                    result = diesel::sql_query(format!("LISTEN {}", channel.as_str()))
                        .execute(&mut conn) => result,
                };
                if let Err(error) = listen_result {
                    error!(
                        message = "Failed to register Postgres notification listener",
                        channel = channel.as_str(),
                        error = %error
                    );
                    if !wait_for_retry_or_shutdown(&shutdown).await {
                        break;
                    }
                    continue;
                }

                info!(
                    message = "Listening for Postgres worker notifications",
                    channel = channel.as_str()
                );
                on_listening();
                if poll_notifications(&mut conn, channel, on_notification, &shutdown).await {
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
                if !wait_for_retry_or_shutdown(&shutdown).await {
                    break;
                }
            }
        }
    }
}

async fn wait_for_retry_or_shutdown(shutdown: &ShutdownSignal) -> bool {
    tokio::select! {
        biased;
        _ = shutdown.requested() => false,
        _ = actix_rt::time::sleep(Duration::from_secs(1)) => true,
    }
}

async fn poll_notifications(
    conn: &mut crate::db::DbConnection,
    channel: NotificationChannel,
    on_notification: fn(),
    shutdown: &ShutdownSignal,
) -> bool {
    let notifications = conn.notifications_stream();
    futures_util::pin_mut!(notifications);
    loop {
        let notification = tokio::select! {
            biased;
            _ = shutdown.requested() => return true,
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
    use std::sync::Arc;
    use std::sync::atomic::{AtomicI32, AtomicI64, AtomicUsize, Ordering};

    use diesel::sql_types::Text;
    use futures_util::StreamExt;
    use rstest::rstest;

    use crate::config::get_config;
    use crate::db::{init_pool, with_transaction};
    use crate::errors::ApiError;
    use crate::events::{Action, ActorKind, EntityType, NewEvent, emit_event};
    use crate::tests::test_scope;

    use super::*;

    const TEST_CHANNEL: NotificationChannel =
        NotificationChannel::new("hubuum_shutdown_listener_test");
    static LISTENER_READY: AtomicUsize = AtomicUsize::new(0);
    static NEXT_TASK_NOTIFICATION_ID: AtomicI32 = AtomicI32::new(1);

    #[derive(QueryableByName)]
    struct ListeningChannel {
        #[diesel(sql_type = Text)]
        channel: String,
    }

    fn mark_listener_ready() {
        LISTENER_READY.fetch_add(1, Ordering::Release);
    }

    #[rstest]
    #[case::commit(true)]
    #[case::rollback(false)]
    #[tokio::test]
    async fn fanout_trigger_notifies_only_after_commit(#[case] commit: bool) {
        let scope = test_scope();
        let mut listener = scope.pool.get().await.expect("listener connection");
        diesel::sql_query(format!("LISTEN {}", EVENT_FANOUT_CHANNEL.as_str()))
            .execute(&mut listener)
            .await
            .expect("listen on fanout channel");

        let inserted_id = Arc::new(AtomicI64::new(0));
        let captured_id = inserted_id.clone();
        let event = NewEvent::new(
            EntityType::Collection,
            Action::Created,
            ActorKind::System,
            "notification transaction test",
        )
        .unwrap();
        let result: Result<(), ApiError> = with_transaction(&scope.pool, async |conn| {
            let event = emit_event(conn, &event).await?;
            captured_id.store(event.id, Ordering::Release);
            if commit {
                Ok(())
            } else {
                Err(ApiError::InternalServerError(
                    "notification rollback test".to_string(),
                ))
            }
        })
        .await;
        assert_eq!(result.is_ok(), commit);

        let target_payload = inserted_id.as_ref().load(Ordering::Acquire).to_string();
        let notifications = listener.notifications_stream();
        futures_util::pin_mut!(notifications);
        let received = tokio::time::timeout(Duration::from_millis(500), async {
            while let Some(notification) = notifications.next().await {
                let notification = notification.expect("fanout notification");
                if notification.channel == EVENT_FANOUT_CHANNEL.as_str()
                    && notification.payload == target_payload
                {
                    return true;
                }
            }
            false
        })
        .await
        .unwrap_or(false);
        assert_eq!(received, commit);
    }

    #[rstest]
    #[case::commit(true)]
    #[case::rollback(false)]
    #[tokio::test]
    async fn task_queue_notification_is_delivered_only_after_commit(#[case] commit: bool) {
        let scope = test_scope();
        let mut listener = scope.pool.get().await.expect("listener connection");
        diesel::sql_query(format!("LISTEN {}", TASK_QUEUE_CHANNEL.as_str()))
            .execute(&mut listener)
            .await
            .expect("listen on task queue channel");

        let task_id = NEXT_TASK_NOTIFICATION_ID.fetch_add(1, Ordering::Relaxed);
        let result: Result<(), ApiError> = with_transaction(&scope.pool, async |conn| {
            notify_task_queue(conn, task_id).await?;
            if commit {
                Ok(())
            } else {
                Err(ApiError::InternalServerError(
                    "notification rollback test".to_string(),
                ))
            }
        })
        .await;
        assert_eq!(result.is_ok(), commit);

        let notifications = listener.notifications_stream();
        futures_util::pin_mut!(notifications);
        let received = tokio::time::timeout(Duration::from_millis(500), async {
            while let Some(notification) = notifications.next().await {
                let notification = notification.expect("task queue notification");
                if notification.channel == TASK_QUEUE_CHANNEL.as_str()
                    && notification.payload == task_id.to_string()
                {
                    return true;
                }
            }
            false
        })
        .await
        .unwrap_or(false);
        assert_eq!(received, commit);
    }

    #[actix_rt::test]
    async fn shutdown_releases_postgres_notification_listener() {
        LISTENER_READY.store(0, Ordering::Relaxed);
        let config = get_config().expect("test requires database configuration");
        let listener_pool = init_pool(&config.database_url, 1);
        let shutdown = ShutdownSignal::new();
        let listener = actix_rt::spawn(listen_loop(
            listener_pool.clone(),
            TEST_CHANNEL,
            || {},
            mark_listener_ready,
            shutdown.clone(),
        ));

        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if AtomicUsize::load(&LISTENER_READY, Ordering::Acquire) > 0 {
                    break;
                }
                actix_rt::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("listener should register promptly");

        shutdown.request();
        tokio::time::timeout(Duration::from_secs(1), listener)
            .await
            .expect("listener should stop promptly")
            .expect("listener task should not panic");

        let mut listener_connection = listener_pool
            .get()
            .await
            .expect("listener connection should return to its pool");
        let channels = diesel::sql_query("SELECT pg_listening_channels()::text AS channel")
            .load::<ListeningChannel>(&mut listener_connection)
            .await
            .expect("listening channels should be queryable");
        assert!(
            channels.is_empty(),
            "listener connection should UNLISTEN before returning to the pool: {:?}",
            channels
                .into_iter()
                .map(|row| row.channel)
                .collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn notification_listener_pool_is_isolated_from_the_execution_pool() {
        let listener_pool = new_postgres_notification_listener_pool();
        let _listener_connection = listener_pool.get().await.unwrap();

        let config = get_config().expect("test requires database configuration");
        let execution_pool = init_pool(&config.database_url, 1);

        tokio::time::timeout(Duration::from_secs(5), execution_pool.get())
            .await
            .expect("execution checkout must not wait for the notification listener")
            .expect("the one-connection execution pool should remain available");
    }
}
