use super::*;
use crate::test_support::{
    database_role_tests_enabled, integration_test_database_roles, integration_test_migration_pool,
    integration_test_pool,
};
use hubuum_domain::PrincipalIdentity;
use std::time::{Duration, Instant};

#[tokio::test]
async fn native_age_page_timeout_is_unavailable_without_partial_admission() {
    let backend = PostgresStorage::unobserved(integration_test_pool(2));
    let before = backend.capture_history_lifecycle().await.unwrap();
    let permit = StorageExecutionPermit::new(
        None,
        ApplicationFence::new(
            before.fence().incarnation(),
            before.fence().deletion_revision().value() as i64,
        )
        .unwrap(),
    );
    let owner_pool = integration_test_migration_pool(1);
    backend
        .runtime()
        .with_history_control_transaction(permit, async |connection| {
            connection
                .batch_execute("SAVEPOINT age_timeout_fixture")
                .await?;
            let snapshot = capture_on(connection).await?;
            let key = HistoryPolicyKey::new(
                StorageIdentityHistoryKind::Collection,
                HistoryPolicyScope::Deployment,
            )?;
            let change = HistoryPolicyChange::new(
                key,
                snapshot
                    .control()
                    .authority()
                    .policies()
                    .current_revision(key),
                HistoryPolicyBinding::Expire {
                    trigger: HistoryExpiryTrigger::Age {
                        age_seconds: HistoryRetentionSeconds::new(0)?,
                        deleted_age_seconds: None,
                    },
                    minimum_age_seconds: HistoryRetentionSeconds::new(0)?,
                },
                HistoryPolicyApplication::Prospective,
            )?;
            let applied = commit_on(
                connection,
                snapshot
                    .control()
                    .apply(
                        HistoryPolicyApplyRequest::new(change, None),
                        PrincipalIdentity::generate(),
                        snapshot.fence(),
                        snapshot.observed_at(),
                    )
                    .map_err(|_| {
                        PostgresStorageError::invalid_input("Invalid age timeout fixture policy")
                    })?,
            )
            .await?;
            let request = HistoryExpiryAdmissionRequest::new(
                Uuid::new_v4(),
                Uuid::new_v4(),
                HistoryExpiryOrigin::Manual {
                    principal: PrincipalIdentity::generate(),
                },
                HistoryLifecycleScan::new(key, 1000)?,
                applied.fence(),
            )?;
            let mut blocker = owner_pool.get().await.unwrap();
            blocker.batch_execute("BEGIN").await.unwrap();
            if database_role_tests_enabled() {
                sql_query("SELECT set_config('role',$1,true)")
                    .bind::<Text, _>(integration_test_database_roles().owner().as_str())
                    .execute(&mut blocker)
                    .await
                    .unwrap();
            }
            // The history boundary remains readable. Only dependency evaluation
            // against current collections blocks inside the bounded source page.
            blocker
                .batch_execute("LOCK TABLE public.collections IN ACCESS EXCLUSIVE MODE")
                .await
                .unwrap();
            connection
                .batch_execute("SAVEPOINT age_page_attempt; SET LOCAL statement_timeout='50ms'")
                .await?;
            let started = Instant::now();
            let result = admit_on(connection, request).await;
            let elapsed = started.elapsed();
            blocker.batch_execute("ROLLBACK").await?;
            connection
                .batch_execute(
                    "ROLLBACK TO SAVEPOINT age_page_attempt; RELEASE SAVEPOINT age_page_attempt",
                )
                .await?;
            assert_eq!(result.unwrap_err().kind(), StorageErrorKind::Unavailable);
            assert!(
                elapsed < Duration::from_secs(1),
                "A shorter inherited timeout must remain binding: {elapsed:?}"
            );
            assert_eq!(capture_on(connection).await?.control(), applied.control());
            connection
                .batch_execute("ROLLBACK TO SAVEPOINT age_timeout_fixture")
                .await?;
            Ok::<_, PostgresStorageError>(())
        })
        .await
        .unwrap();
}
