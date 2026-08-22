#![cfg(feature = "integration-test-support")]

use std::sync::Arc;
use std::sync::atomic::{AtomicI32, AtomicI64, AtomicUsize, Ordering};
use std::time::Duration;

use diesel::QueryableByName;
use diesel::sql_types::Text;
use diesel_async::RunQueryDsl;
use futures_util::StreamExt;
use hubuum_events_core::{Action, ActorKind, EntityType, NewEvent};
use hubuum_storage_core::StorageNotification;
use hubuum_storage_postgres::test_support::{append_event_on_connection, integration_test_pool};
use hubuum_storage_postgres::worker_notifications::{channel_name, listen, notify_task_queue};
use hubuum_storage_postgres::{PostgresConnection, PostgresStorageError, with_transaction};

static LISTENER_READY: AtomicUsize = AtomicUsize::new(0);
static NEXT_TASK_NOTIFICATION_ID: AtomicI32 = AtomicI32::new(-1);

#[derive(QueryableByName)]
struct ListeningChannel {
    #[diesel(sql_type = Text)]
    channel: String,
}

async fn matching_notification(
    listener: &mut PostgresConnection,
    channel: &str,
    payload: &str,
) -> bool {
    let notifications = listener.notifications_stream();
    futures_util::pin_mut!(notifications);
    tokio::time::timeout(Duration::from_millis(500), async {
        while let Some(notification) = notifications.next().await {
            let notification = notification.expect("notification must decode");
            if notification.channel == channel && notification.payload == payload {
                return true;
            }
        }
        false
    })
    .await
    .unwrap_or(false)
}

#[tokio::test]
async fn fanout_trigger_notifies_only_after_commit() {
    for commit in [true, false] {
        let pool = integration_test_pool(2);
        let mut listener = pool.get().await.expect("listener connection");
        let channel = channel_name(StorageNotification::EventFanout);
        diesel::sql_query(format!("LISTEN {channel}"))
            .execute(&mut listener)
            .await
            .expect("fanout channel must be listenable");

        let inserted_id = Arc::new(AtomicI64::new(0));
        let captured_id = Arc::clone(&inserted_id);
        let event = NewEvent::new(
            EntityType::Collection,
            Action::Created,
            ActorKind::System,
            "notification transaction test",
        )
        .expect("test event must be valid");
        let result = with_transaction(&pool, async |connection| {
            let event = append_event_on_connection(connection, &event).await?;
            captured_id.store(event.into_parts().0.id.get(), Ordering::Release);
            if commit {
                Ok(())
            } else {
                Err(PostgresStorageError::database("notification rollback test"))
            }
        })
        .await;
        assert_eq!(result.is_ok(), commit);

        let payload = AtomicI64::load(inserted_id.as_ref(), Ordering::Acquire).to_string();
        assert_eq!(
            matching_notification(&mut listener, channel, &payload).await,
            commit
        );
    }
}

#[tokio::test]
async fn task_queue_notification_is_delivered_only_after_commit() {
    for commit in [true, false] {
        let pool = integration_test_pool(2);
        let mut listener = pool.get().await.expect("listener connection");
        let channel = channel_name(StorageNotification::TaskQueue);
        diesel::sql_query(format!("LISTEN {channel}"))
            .execute(&mut listener)
            .await
            .expect("task channel must be listenable");

        let task_id = NEXT_TASK_NOTIFICATION_ID.fetch_sub(1, Ordering::Relaxed);
        let result = with_transaction(&pool, async |connection| {
            notify_task_queue(connection, task_id).await?;
            if commit {
                Ok(())
            } else {
                Err(PostgresStorageError::database("notification rollback test"))
            }
        })
        .await;
        assert_eq!(result.is_ok(), commit);
        assert_eq!(
            matching_notification(&mut listener, channel, &task_id.to_string()).await,
            commit
        );
    }
}

fn mark_listener_ready() {
    LISTENER_READY.fetch_add(1, Ordering::Release);
}

#[tokio::test]
async fn shutdown_releases_postgres_notification_listener() {
    LISTENER_READY.store(0, Ordering::Relaxed);
    let pool = integration_test_pool(1);
    let (shutdown_sender, shutdown_receiver) = tokio::sync::oneshot::channel::<()>();
    let listener_pool = pool.clone();
    let listener = tokio::spawn(listen(
        listener_pool,
        StorageNotification::TaskQueue,
        || {},
        mark_listener_ready,
        async move {
            let _ = shutdown_receiver.await;
        },
    ));

    tokio::time::timeout(Duration::from_secs(2), async {
        while AtomicUsize::load(&LISTENER_READY, Ordering::Acquire) == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("listener must register promptly");
    shutdown_sender
        .send(())
        .expect("listener must still await shutdown");
    tokio::time::timeout(Duration::from_secs(2), listener)
        .await
        .expect("listener must stop promptly")
        .expect("listener task must not panic");

    let mut connection = pool
        .get()
        .await
        .expect("listener connection must return to its pool");
    let channels = diesel::sql_query("SELECT pg_listening_channels()::text AS channel")
        .load::<ListeningChannel>(&mut connection)
        .await
        .expect("listening channels must be queryable");
    assert!(
        channels.is_empty(),
        "listener connection must UNLISTEN before reuse: {:?}",
        channels
            .into_iter()
            .map(|row| row.channel)
            .collect::<Vec<_>>()
    );
}
