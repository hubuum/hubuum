use std::thread;
use std::time::Duration;

use diesel::prelude::*;
use tracing::{debug, error, info};

use crate::db::DbPool;

pub const EVENT_FANOUT_CHANNEL: &str = "hubuum_event_fanout";
pub const EVENT_DELIVERY_CHANNEL: &str = "hubuum_event_delivery";

pub fn notify_event_fanout(conn: &mut PgConnection) -> QueryResult<usize> {
    notify_channel(conn, EVENT_FANOUT_CHANNEL)
}

pub fn notify_event_delivery(conn: &mut PgConnection) -> QueryResult<usize> {
    notify_channel(conn, EVENT_DELIVERY_CHANNEL)
}

fn notify_channel(conn: &mut PgConnection, channel: &str) -> QueryResult<usize> {
    diesel::sql_query(format!("NOTIFY {channel}")).execute(conn)
}

pub fn spawn_postgres_notification_listener(
    pool: DbPool,
    channel: &'static str,
    thread_name: &'static str,
    on_notification: fn(),
) {
    thread::Builder::new()
        .name(thread_name.to_string())
        .spawn(move || listen_loop(pool, channel, on_notification))
        .expect("failed to spawn Postgres notification listener thread");
}

fn listen_loop(pool: DbPool, channel: &'static str, on_notification: fn()) {
    loop {
        match pool.get() {
            Ok(mut conn) => {
                if let Err(error) =
                    diesel::sql_query(format!("LISTEN {channel}")).execute(&mut conn)
                {
                    error!(
                        message = "Failed to register Postgres notification listener",
                        channel = channel,
                        error = %error
                    );
                    thread::sleep(Duration::from_secs(1));
                    continue;
                }

                info!(
                    message = "Listening for Postgres event worker notifications",
                    channel = channel
                );
                poll_notifications(&mut conn, channel, on_notification);
            }
            Err(error) => {
                error!(
                    message = "Failed to acquire Postgres notification listener connection",
                    channel = channel,
                    error = %error
                );
                thread::sleep(Duration::from_secs(1));
            }
        }
    }
}

fn poll_notifications(conn: &mut PgConnection, channel: &'static str, on_notification: fn()) {
    loop {
        let mut should_reconnect = false;
        for notification in conn.notifications_iter() {
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
                    should_reconnect = true;
                    break;
                }
            }
        }
        if should_reconnect {
            return;
        }
        thread::sleep(Duration::from_millis(250));
    }
}
