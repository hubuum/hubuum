use std::thread;
use std::time::Duration;

use crate::db::prelude::*;
use futures_util::StreamExt;
use tracing::{debug, error, info};

use crate::db::DbPool;

pub const EVENT_FANOUT_CHANNEL: &str = "hubuum_event_fanout";
pub const EVENT_DELIVERY_CHANNEL: &str = "hubuum_event_delivery";

pub async fn notify_event_fanout(conn: &mut crate::db::DbConnection) -> QueryResult<usize> {
    notify_channel(conn, EVENT_FANOUT_CHANNEL).await
}

pub async fn notify_event_delivery(conn: &mut crate::db::DbConnection) -> QueryResult<usize> {
    notify_channel(conn, EVENT_DELIVERY_CHANNEL).await
}

async fn notify_channel(conn: &mut crate::db::DbConnection, channel: &str) -> QueryResult<usize> {
    diesel::sql_query(format!("NOTIFY {channel}"))
        .execute(conn)
        .await
}

pub fn spawn_postgres_notification_listener(
    pool: DbPool,
    channel: &'static str,
    thread_name: &'static str,
    on_notification: fn(),
) {
    thread::Builder::new()
        .name(thread_name.to_string())
        .spawn(move || {
            let system = actix_rt::System::new();
            system.block_on(listen_loop(pool, channel, on_notification));
        })
        .expect("failed to spawn Postgres notification listener thread");
}

async fn listen_loop(pool: DbPool, channel: &'static str, on_notification: fn()) {
    loop {
        match pool.get().await {
            Ok(mut conn) => {
                if let Err(error) = diesel::sql_query(format!("LISTEN {channel}"))
                    .execute(&mut conn)
                    .await
                {
                    error!(
                        message = "Failed to register Postgres notification listener",
                        channel = channel,
                        error = %error
                    );
                    actix_rt::time::sleep(Duration::from_secs(1)).await;
                    continue;
                }

                info!(
                    message = "Listening for Postgres event worker notifications",
                    channel = channel
                );
                poll_notifications(&mut conn, channel, on_notification).await;
            }
            Err(error) => {
                error!(
                    message = "Failed to acquire Postgres notification listener connection",
                    channel = channel,
                    error = %error
                );
                actix_rt::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
}

async fn poll_notifications(
    conn: &mut crate::db::DbConnection,
    channel: &'static str,
    on_notification: fn(),
) {
    let notifications = conn.notifications_stream();
    futures_util::pin_mut!(notifications);
    while let Some(notification) = notifications.next().await {
        match notification {
            Ok(notification) if notification.channel == channel => {
                debug!(
                    message = "Received Postgres event worker notification",
                    channel = channel,
                    process_id = notification.process_id
                );
                on_notification();
            }
            Ok(_) => {}
            Err(error) => {
                error!(
                    message = "Postgres notification listener failed",
                    channel = channel,
                    error = %error
                );
                return;
            }
        }
    }
}
