use super::*;

async fn store(label: &str) -> valkey::ValkeyLoginRateLimitStore {
    valkey::ValkeyLoginRateLimitStore::connect(
        valkey_test_url(),
        format!("hubuum:test-{label}:{}", Uuid::new_v4()),
        Duration::from_secs(1),
    )
    .await
    .unwrap()
}

#[rstest]
#[case(false, LoginAttemptOutcome::Succeeded)]
#[case(false, LoginAttemptOutcome::Failed)]
#[case(false, LoginAttemptOutcome::Aborted)]
#[case(true, LoginAttemptOutcome::Succeeded)]
#[case(true, LoginAttemptOutcome::Failed)]
#[case(true, LoginAttemptOutcome::Aborted)]
#[tokio::test]
#[ignore = "requires Valkey or Redis at redis://127.0.0.1:6379/"]
async fn reset_fences_old_shared_completion(
    #[case] clear_all: bool,
    #[case] outcome: LoginAttemptOutcome,
) {
    let config = LoginRateLimitConfig {
        max_attempts: 1,
        ..cfg()
    };
    let store = store("reset-cancellation").await;
    let old = permit_for("reset", &config);
    assert!(store.begin(&old, &config).await.unwrap());
    if clear_all {
        store.clear_all().await.unwrap();
    } else {
        store.release_entry(&old.user_ip_key).await.unwrap();
    }
    let current = permit_for("reset", &config);
    assert!(store.begin(&current, &config).await.unwrap());
    assert!(
        store
            .finish(&old, outcome, &config)
            .await
            .unwrap()
            .is_empty()
    );
    let blocked = permit_for("reset", &config);
    assert!(!store.begin(&blocked, &config).await.unwrap());
    store
        .finish(&current, LoginAttemptOutcome::Aborted, &config)
        .await
        .unwrap();
    assert!(store.snapshot(&config).await.unwrap().is_empty());
    store.clear_all().await.unwrap();
}

#[rstest]
#[case(LoginAttemptOutcome::Succeeded)]
#[case(LoginAttemptOutcome::Failed)]
#[case(LoginAttemptOutcome::Aborted)]
#[tokio::test]
#[ignore = "requires Valkey or Redis at redis://127.0.0.1:6379/"]
async fn repeated_shared_completion_preserves_new_lockout(#[case] outcome: LoginAttemptOutcome) {
    let config = LoginRateLimitConfig {
        max_attempts: 1,
        ..cfg()
    };
    let store = store("repeated-completion").await;
    let old = permit_for("repeat", &config);
    assert!(store.begin(&old, &config).await.unwrap());
    store
        .finish(&old, LoginAttemptOutcome::Succeeded, &config)
        .await
        .unwrap();
    let current = permit_for("repeat", &config);
    assert!(store.begin(&current, &config).await.unwrap());
    store
        .finish(&current, LoginAttemptOutcome::Failed, &config)
        .await
        .unwrap();

    assert!(
        store
            .finish(&old, outcome, &config)
            .await
            .unwrap()
            .is_empty()
    );
    let snapshot = store.snapshot(&config).await.unwrap();
    assert!(snapshot[0].locked);
    assert_eq!(snapshot[0].lockout_level, 1);
    store.clear_all().await.unwrap();
}

#[rstest]
#[case(false)]
#[case(true)]
#[tokio::test]
#[ignore = "requires Valkey or Redis at redis://127.0.0.1:6379/"]
async fn expired_shared_reservation_cannot_apply_a_late_outcome(#[case] prune_on_begin: bool) {
    let config = LoginRateLimitConfig {
        max_attempts: 1,
        ..cfg()
    };
    let prefix = format!("hubuum:test-expired-cancellation:{}", Uuid::new_v4());
    let store = valkey::ValkeyLoginRateLimitStore::connect(
        valkey_test_url(),
        prefix.clone(),
        Duration::from_secs(1),
    )
    .await
    .unwrap();
    let old = permit_for("expire", &config);
    assert!(store.begin(&old, &config).await.unwrap());
    // Age only this isolated reservation beyond its lease and configured window;
    // the actual admission/completion Lua scripts must enforce expiration.
    let mut connection = redis::Client::open(valkey_test_url())
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    redis::cmd("ZADD")
        .arg(format!(
            "{prefix}:{{login-rate-limit}}:scope:{}:inflight",
            old.user_ip_key
        ))
        .arg(0)
        .arg(old.reservation.id().to_string())
        .query_async::<i64>(&mut connection)
        .await
        .unwrap();
    let current = permit_for("expire", &config);
    if prune_on_begin {
        assert!(store.begin(&current, &config).await.unwrap());
    }
    assert!(
        store
            .finish(&old, LoginAttemptOutcome::Failed, &config)
            .await
            .unwrap()
            .is_empty()
    );
    if !prune_on_begin {
        assert!(store.begin(&current, &config).await.unwrap());
    }
    store
        .finish(&current, LoginAttemptOutcome::Aborted, &config)
        .await
        .unwrap();
    assert!(store.snapshot(&config).await.unwrap().is_empty());
    store.clear_all().await.unwrap();
}
