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

enum PruneOperation {
    Begin,
    Snapshot,
    Finish,
    Eviction,
}

enum ExpiryUpdate {
    Abort,
    Fail,
    BeginOther,
    FinishOther,
}

#[rstest]
#[case::abort(ExpiryUpdate::Abort)]
#[case::fail(ExpiryUpdate::Fail)]
#[case::begin_other(ExpiryUpdate::BeginOther)]
#[case::finish_other(ExpiryUpdate::FinishOther)]
#[tokio::test]
#[ignore = "requires Valkey or Redis at redis://127.0.0.1:6379/"]
async fn shorter_policy_preserves_shared_lockout_expiry(#[case] update: ExpiryUpdate) {
    let original = LoginRateLimitConfig {
        max_attempts: 1,
        window_seconds: 1,
        backoff_base_seconds: 3600,
        backoff_max_seconds: 3600,
        ..cfg()
    };
    let updated = LoginRateLimitConfig {
        max_attempts: 3,
        backoff_base_seconds: 1,
        backoff_max_seconds: 1,
        ..original
    };
    let prefix = format!("hubuum:test-lockout-expiry:{}", Uuid::new_v4());
    let store = valkey::ValkeyLoginRateLimitStore::connect(
        valkey_test_url(),
        prefix.clone(),
        Duration::from_secs(1),
    )
    .await
    .unwrap();
    let old = permit_for("lockout-expiry", &original);
    let current = permit_for("lockout-expiry", &updated);
    assert!(store.begin(&old, &original).await.unwrap());
    assert!(store.begin(&current, &updated).await.unwrap());
    store
        .finish(&old, LoginAttemptOutcome::Failed, &original)
        .await
        .unwrap();
    let outcome = if matches!(update, ExpiryUpdate::Fail) {
        LoginAttemptOutcome::Failed
    } else {
        LoginAttemptOutcome::Aborted
    };
    store.finish(&current, outcome, &updated).await.unwrap();
    if matches!(update, ExpiryUpdate::BeginOther | ExpiryUpdate::FinishOther) {
        let other = permit_for("other-scope", &updated);
        assert!(store.begin(&other, &updated).await.unwrap());
        if matches!(update, ExpiryUpdate::FinishOther) {
            store
                .finish(&other, LoginAttemptOutcome::Aborted, &updated)
                .await
                .unwrap();
        }
    }

    // Inspect absolute deadlines instead of waiting an hour for the lockout.
    let mut connection = redis::Client::open(valkey_test_url())
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let state = format!(
        "{prefix}:{{login-rate-limit}}:scope:{}:state",
        old.user_ip_key
    );
    let locked_until: i64 = redis::cmd("HGET")
        .arg(&state)
        .arg("locked_until")
        .query_async(&mut connection)
        .await
        .unwrap();
    for key in [state, format!("{prefix}:{{login-rate-limit}}:index")] {
        let expires_at: i64 = redis::cmd("PEXPIRETIME")
            .arg(&key)
            .query_async(&mut connection)
            .await
            .unwrap();
        assert!(
            expires_at >= locked_until + 1000,
            "{key} expires before lockout cooldown ends"
        );
    }
    store.clear_all().await.unwrap();
}

#[rstest]
#[case::begin(PruneOperation::Begin)]
#[case::snapshot(PruneOperation::Snapshot)]
#[case::finish(PruneOperation::Finish)]
#[case::eviction(PruneOperation::Eviction)]
#[tokio::test]
#[ignore = "requires Valkey or Redis at redis://127.0.0.1:6379/"]
async fn shrinking_window_preserves_shared_reservation(#[case] operation: PruneOperation) {
    let original = LoginRateLimitConfig {
        max_attempts: 2,
        window_seconds: 60,
        ..cfg()
    };
    let updated = LoginRateLimitConfig {
        window_seconds: 5,
        ..original
    };
    let prefix = format!("hubuum:test-window-shrink:{}", Uuid::new_v4());
    let store = valkey::ValkeyLoginRateLimitStore::connect(
        valkey_test_url(),
        prefix.clone(),
        Duration::from_secs(1),
    )
    .await
    .unwrap();
    let old = permit_for("window-shrink", &original);
    assert!(store.begin(&old, &original).await.unwrap());

    // Simulate six seconds since admission without sleeping. Moving the stored
    // score preserves whether it represents admission time or a fixed deadline,
    // so this catches pruning that reinterprets it using the new window.
    let mut connection = redis::Client::open(valkey_test_url())
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    redis::cmd("ZINCRBY")
        .arg(format!(
            "{prefix}:{{login-rate-limit}}:scope:{}:inflight",
            old.user_ip_key
        ))
        .arg(-6_000)
        .arg(old.reservation.id().to_string())
        .query_async::<f64>(&mut connection)
        .await
        .unwrap();
    redis::cmd("ZINCRBY")
        .arg(format!("{prefix}:{{login-rate-limit}}:index"))
        .arg(-6_000)
        .arg(&old.user_ip_key)
        .query_async::<f64>(&mut connection)
        .await
        .unwrap();

    let current = permit_for("window-shrink", &updated);
    match operation {
        PruneOperation::Begin => {
            assert!(store.begin(&current, &updated).await.unwrap());
        }
        PruneOperation::Snapshot => {
            store.snapshot(&updated).await.unwrap();
        }
        PruneOperation::Finish => {
            // Even a completion without ownership prunes other reservations.
            store
                .finish(&current, LoginAttemptOutcome::Aborted, &updated)
                .await
                .unwrap();
        }
        PruneOperation::Eviction => {
            let other = permit_for("other-scope", &updated);
            assert!(
                !store
                    .begin_with_max_keys(&other, &updated, 1)
                    .await
                    .unwrap()
            );
        }
    }

    store
        .finish(&old, LoginAttemptOutcome::Failed, &original)
        .await
        .unwrap();
    let snapshot = store.snapshot(&updated).await.unwrap();
    assert_eq!(snapshot.len(), 1);
    assert_eq!(
        snapshot[0].attempts, 1,
        "the original permit must still own its reservation"
    );
    store.clear_all().await.unwrap();
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
