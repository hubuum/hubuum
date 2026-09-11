use std::time::Duration as StdDuration;

use chrono::{DateTime, Duration, NaiveDateTime, Utc};
use diesel::prelude::*;
use diesel::sql_types::BigInt;
use diesel_async::RunQueryDsl;
use hubuum_domain::UserId;
use hubuum_events_core::EventContext;
use hubuum_storage_core::{
    StorageAuthenticatedToken, StorageAuthenticationAttempt, StorageAuthenticationCredential,
    StorageUserCreate, StorageUserDelete,
};
use rstest::rstest;
use tokio::task::JoinHandle;
use tokio::time::timeout;
use uuid::Uuid;

use super::authenticate_bearer_token;
use crate::operations::user::{create_user, delete_user};
use crate::schema::tokens;
use crate::test_support::integration_test_pool;
use crate::{PostgresFaultController, PostgresFaultPoint, PostgresRuntime, PostgresStorageError};

const WAIT: StdDuration = StdDuration::from_secs(10);
const BURST_SIZE: usize = 8;

/// Own a uniquely named principal and token in the runner's migrated database.
struct TestContext {
    runtime: PostgresRuntime,
    user_id: UserId,
    token_id: i32,
    digest: String,
    observed_at: DateTime<Utc>,
}

impl TestContext {
    async fn new(previous_age: Option<Duration>) -> Self {
        let runtime = PostgresRuntime::unobserved(integration_test_pool(1));
        let digest = format!("activity-{}", Uuid::new_v4());
        let user_id = create_user(
            &runtime,
            StorageUserCreate::new(
                None,
                &digest,
                "unused-test-password-hash",
                None,
                None,
                EventContext::system(),
            ),
        )
        .await
        .unwrap()
        .into_value()
        .into_parts()
        .id();
        // Fixed microsecond-aligned observations exercise the exact boundary
        // without sleeping or depending on the database/server clock offset.
        let observed_at = DateTime::from_timestamp(1_800_000_000, 0).unwrap();
        let token_id = runtime
            .with_connection(async |connection| {
                diesel::insert_into(tokens::table)
                    .values((
                        tokens::token.eq(&digest),
                        tokens::principal_id.eq(user_id.id()),
                        tokens::issued.eq((observed_at - Duration::hours(1)).naive_utc()),
                        tokens::expires_at.eq((observed_at + Duration::hours(1)).naive_utc()),
                        tokens::last_used_at
                            .eq(previous_age.map(|age| (observed_at - age).naive_utc())),
                    ))
                    .returning(tokens::id)
                    .get_result::<i32>(connection)
                    .await
            })
            .await
            .unwrap();
        Self {
            runtime,
            user_id,
            token_id,
            digest,
            observed_at,
        }
    }

    fn attempt_at(&self, observed_at: DateTime<Utc>) -> StorageAuthenticationAttempt {
        StorageAuthenticationAttempt::try_new(
            StorageAuthenticationCredential::new(&self.digest),
            observed_at,
            observed_at - Duration::hours(2),
        )
        .unwrap()
    }

    async fn last_used_at(&self) -> Option<DateTime<Utc>> {
        self.runtime
            .with_read_connection(async |connection| {
                tokens::table
                    .find(self.token_id)
                    .select(tokens::last_used_at)
                    .first::<Option<NaiveDateTime>>(connection)
                    .await
            })
            .await
            .unwrap()
            .map(|value| value.and_utc())
    }

    async fn cleanup(self) {
        delete_user(
            &self.runtime,
            StorageUserDelete::new(self.user_id, EventContext::system()),
        )
        .await
        .unwrap()
        .into_value();
    }
}

#[derive(QueryableByName)]
struct UpdateCount {
    #[diesel(sql_type = BigInt)]
    updates: i64,
}

struct MeasuredAuthentication {
    result: Result<StorageAuthenticatedToken, PostgresStorageError>,
    updates: i64,
}

/// A dedicated one-connection pool keeps the production authentication call
/// and PostgreSQL's transaction-local tuple counter on the same session. The
/// explicit READ COMMITTED transaction permits concurrent updates to commit
/// and predicates to be rechecked after a row-lock wait, as in production.
/// Counting physical token updates distinguishes writes from skipped updates;
/// unrelated parallel tests cannot contribute to this transaction's count.
async fn measured_authentication(attempt: StorageAuthenticationAttempt) -> MeasuredAuthentication {
    let runtime = PostgresRuntime::unobserved(integration_test_pool(1));
    runtime
        .with_connection(async |connection| {
            diesel::sql_query("BEGIN ISOLATION LEVEL READ COMMITTED")
                .execute(connection)
                .await
        })
        .await
        .unwrap();
    let result = authenticate_bearer_token(&runtime, attempt).await;
    let updates = runtime
        .with_read_connection(async |connection| {
            diesel::sql_query(
                "SELECT pg_stat_get_xact_tuples_updated('tokens'::regclass) AS updates",
            )
            .get_result::<UpdateCount>(connection)
            .await
        })
        .await
        .unwrap()
        .updates;
    runtime
        .with_connection(async |connection| diesel::sql_query("COMMIT").execute(connection).await)
        .await
        .unwrap();
    MeasuredAuthentication { result, updates }
}

fn paused_authentication(
    attempt: StorageAuthenticationAttempt,
) -> (PostgresFaultController, JoinHandle<MeasuredAuthentication>) {
    let controller =
        PostgresFaultController::pausing(PostgresFaultPoint::AuthenticationBeforeActivityUpdate);
    let task_controller = controller.clone();
    let task =
        tokio::spawn(async move { task_controller.run(measured_authentication(attempt)).await });
    (controller, task)
}

#[rstest]
#[case::first_use(None)]
#[case::exact_boundary(Some(Duration::seconds(60)))]
#[tokio::test]
async fn concurrent_activity_refresh_writes_once(#[case] previous_age: Option<Duration>) {
    let context = TestContext::new(previous_age).await;
    let mut contenders = Vec::new();
    for index in 0..BURST_SIZE {
        // Real requests observe slightly different times. Identical values can
        // be suppressed by the revision trigger even with an unguarded UPDATE.
        let observed_at = context.observed_at + Duration::microseconds(index as i64);
        contenders.push(paused_authentication(context.attempt_at(observed_at)));
    }
    for (controller, _) in &contenders {
        timeout(WAIT, controller.wait_until_reached())
            .await
            .expect("every contender must read the stale timestamp before any update");
    }
    for (controller, _) in &contenders {
        controller.resume();
    }
    let mut updates = 0;
    let mut written_at = None;
    for (_, task) in contenders {
        let measured = timeout(WAIT, task).await.unwrap().unwrap();
        let authenticated = measured.result.expect("every contender must authenticate");
        assert_eq!(authenticated.id().id(), context.token_id);
        updates += measured.updates;
        if measured.updates == 1 {
            written_at = authenticated.last_used_at();
        }
    }
    let persisted = context.last_used_at().await;
    context.cleanup().await;

    eprintln!("{BURST_SIZE} concurrent authentications performed {updates} token row updates");
    assert_eq!(
        updates, 1,
        "one burst must persist exactly one activity refresh"
    );
    assert_eq!(persisted, written_at);
}

#[rstest]
#[case::first_use(None)]
#[case::exact_boundary(Some(Duration::seconds(60)))]
#[tokio::test]
async fn delayed_activity_refresh_preserves_newer_timestamp(
    #[case] previous_age: Option<Duration>,
) {
    let context = TestContext::new(previous_age).await;
    let previous = context.last_used_at().await;
    let (controller, older) = paused_authentication(context.attempt_at(context.observed_at));
    timeout(WAIT, controller.wait_until_reached())
        .await
        .expect("older observation must pause after reading stale activity");

    let newer_at = context.observed_at + Duration::seconds(1);
    let newer = measured_authentication(context.attempt_at(newer_at)).await;
    newer.result.expect("newer observation must authenticate");
    let before_delayed_update = context.last_used_at().await;
    controller.resume();
    let older = timeout(WAIT, older).await.unwrap().unwrap();
    let older_result = older.result.expect("delayed observation must authenticate");
    let after_delayed_update = context.last_used_at().await;
    context.cleanup().await;

    assert_eq!(newer.updates, 1);
    assert_eq!(before_delayed_update, Some(newer_at));
    assert_eq!(after_delayed_update, before_delayed_update);
    assert_eq!(
        older.updates, 0,
        "an older observation must not rewrite activity"
    );
    assert_eq!(
        older_result.last_used_at(),
        previous,
        "a skipped update must not claim its observation was persisted"
    );
}

#[rstest]
#[case::first_use(None, true)]
#[case::before_boundary(Some(Duration::microseconds(59_999_999)), false)]
#[case::at_boundary(Some(Duration::seconds(60)), true)]
#[case::after_boundary(Some(Duration::microseconds(60_000_001)), true)]
#[case::future_timestamp(Some(Duration::seconds(-1)), false)]
#[tokio::test]
async fn activity_refresh_respects_throttle(
    #[case] previous_age: Option<Duration>,
    #[case] refresh: bool,
) {
    let context = TestContext::new(previous_age).await;
    let expected = if refresh {
        Some(context.observed_at)
    } else {
        context.last_used_at().await
    };
    let measured = measured_authentication(context.attempt_at(context.observed_at)).await;
    let authenticated = measured.result.unwrap();
    let persisted = context.last_used_at().await;
    context.cleanup().await;

    assert_eq!(measured.updates, i64::from(refresh));
    assert_eq!(persisted, expected);
    assert_eq!(authenticated.last_used_at(), persisted);
}

#[tokio::test]
async fn activity_update_database_failure_does_not_reject_authentication() {
    let context = TestContext::new(Some(Duration::seconds(60))).await;
    let previous = context.last_used_at().await;
    // PostgreSQL rejects the actual UPDATE while the preceding token lookup
    // succeeds. The single-connection pool keeps both in this transaction.
    context
        .runtime
        .with_connection(async |connection| {
            diesel::sql_query("BEGIN READ ONLY")
                .execute(connection)
                .await
        })
        .await
        .unwrap();
    let result =
        authenticate_bearer_token(&context.runtime, context.attempt_at(context.observed_at)).await;
    context
        .runtime
        .with_connection(async |connection| diesel::sql_query("ROLLBACK").execute(connection).await)
        .await
        .unwrap();
    let persisted = context.last_used_at().await;
    context.cleanup().await;

    assert_eq!(result.unwrap().last_used_at(), previous);
    assert_eq!(persisted, previous);
}
