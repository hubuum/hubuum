use std::future::pending;

use futures::poll;

use super::*;

#[tokio::test]
async fn canceled_authentication_releases_every_scope_without_failures() {
    let _guard = LOGIN_RATE_LIMIT_TEST_LOCK.lock().await;
    reset_login_rate_limit_for_tests().await;
    let config = cfg();
    let ip = Some(IpAddr::V4(Ipv4Addr::new(192, 0, 2, 1)));
    for _ in 0..config.max_attempts {
        let mut request = Box::pin(async {
            let permit = begin_login_attempt("local", "canceled-auth", ip)
                .await
                .unwrap()
                .unwrap();
            pending::<()>().await;
            finish_login_attempt(permit, LoginAttemptOutcome::Failed)
                .await
                .unwrap();
        });
        assert!(poll!(&mut request).is_pending());
        assert_eq!(LOGIN_ATTEMPTS.lock().await.len(), 3);
        drop(request);
    }

    assert!(
        MemoryLoginRateLimitStore
            .snapshot(&config)
            .await
            .unwrap()
            .is_empty()
    );
}

#[rstest]
#[case(LoginAttemptOutcome::Succeeded)]
#[case(LoginAttemptOutcome::Failed)]
#[case(LoginAttemptOutcome::Aborted)]
#[tokio::test]
async fn cancellation_while_completion_waits_for_memory_releases_capacity(
    #[case] outcome: LoginAttemptOutcome,
) {
    let _guard = LOGIN_RATE_LIMIT_TEST_LOCK.lock().await;
    reset_login_rate_limit_for_tests().await;
    let permit = begin_login_attempt("local", "cancel-finish", None)
        .await
        .unwrap()
        .unwrap();
    let map = LOGIN_ATTEMPTS.lock().await;
    let mut completion = Box::pin(finish_login_attempt(permit, outcome));
    assert!(poll!(&mut completion).is_pending());
    drop(completion);
    drop(map);

    assert!(snapshot().await.unwrap().is_empty());
}

#[rstest]
#[case(5)]
#[case(30)]
#[case(300)]
#[tokio::test]
async fn reservation_capacity_expires_by_the_configured_window(#[case] window_seconds: u64) {
    let _guard = LOGIN_RATE_LIMIT_TEST_LOCK.lock().await;
    reset_login_rate_limit_for_tests().await;
    let config = LoginRateLimitConfig {
        max_attempts: 1,
        window_seconds,
        ..cfg()
    };
    let store = MemoryLoginRateLimitStore;
    let old = permit_for("expired-reservation", &config);
    assert!(store.begin(&old, &config).await.unwrap());
    let window = Duration::from_secs(config.window_seconds);
    prune_login_attempts_map(
        &mut *LOGIN_ATTEMPTS.lock().await,
        Instant::now() + window,
        window,
    );
    let current = permit_for("expired-reservation", &config);

    assert!(store.begin(&current, &config).await.unwrap());
    store
        .finish(&old, LoginAttemptOutcome::Failed, &config)
        .await
        .unwrap();
    assert_eq!(
        LOGIN_ATTEMPTS.lock().await[&current.user_ip_key]
            .in_flight
            .len(),
        1
    );
    store
        .finish(&current, LoginAttemptOutcome::Aborted, &config)
        .await
        .unwrap();
}

#[rstest]
#[case(false, LoginAttemptOutcome::Succeeded)]
#[case(false, LoginAttemptOutcome::Failed)]
#[case(false, LoginAttemptOutcome::Aborted)]
#[case(true, LoginAttemptOutcome::Succeeded)]
#[case(true, LoginAttemptOutcome::Failed)]
#[case(true, LoginAttemptOutcome::Aborted)]
#[tokio::test]
async fn reset_fences_old_completion_from_new_reservations(
    #[case] clear_all: bool,
    #[case] outcome: LoginAttemptOutcome,
) {
    let _guard = LOGIN_RATE_LIMIT_TEST_LOCK.lock().await;
    reset_login_rate_limit_for_tests().await;
    let config = LoginRateLimitConfig {
        max_attempts: 1,
        ..cfg()
    };
    let store = MemoryLoginRateLimitStore;
    let old = permit_for("reset-reservation", &config);
    assert!(store.begin(&old, &config).await.unwrap());
    if clear_all {
        store.clear_all().await.unwrap();
    } else {
        store.release_entry(&old.user_ip_key).await.unwrap();
    }
    let current = permit_for("reset-reservation", &config);
    assert!(store.begin(&current, &config).await.unwrap());
    store.finish(&old, outcome, &config).await.unwrap();

    let blocked = permit_for("reset-reservation", &config);
    assert!(!store.begin(&blocked, &config).await.unwrap());
    let map = LOGIN_ATTEMPTS.lock().await;
    let state = &map[&current.user_ip_key];
    assert_eq!(state.in_flight.len(), 1);
    assert!(state.attempts.is_empty());
    assert!(state.locked_until.is_none());
}

#[rstest]
#[case(LoginAttemptOutcome::Succeeded)]
#[case(LoginAttemptOutcome::Failed)]
#[case(LoginAttemptOutcome::Aborted)]
#[tokio::test]
async fn repeated_completion_does_not_change_new_failures(#[case] repeated: LoginAttemptOutcome) {
    let _guard = LOGIN_RATE_LIMIT_TEST_LOCK.lock().await;
    reset_login_rate_limit_for_tests().await;
    let config = LoginRateLimitConfig {
        max_attempts: 1,
        ..cfg()
    };
    let store = MemoryLoginRateLimitStore;
    let old = permit_for("repeated-completion", &config);
    assert!(store.begin(&old, &config).await.unwrap());
    store
        .finish(&old, LoginAttemptOutcome::Succeeded, &config)
        .await
        .unwrap();
    let current = permit_for("repeated-completion", &config);
    assert!(store.begin(&current, &config).await.unwrap());
    store
        .finish(&current, LoginAttemptOutcome::Failed, &config)
        .await
        .unwrap();

    assert!(
        store
            .finish(&old, repeated, &config)
            .await
            .unwrap()
            .is_empty()
    );
    let snapshots = store.snapshot(&config).await.unwrap();
    assert!(snapshots[0].locked);
    assert_eq!(snapshots[0].lockout_level, 1);
}

#[tokio::test]
async fn canceled_reservation_does_not_pin_the_key_limit() {
    let _guard = LOGIN_RATE_LIMIT_TEST_LOCK.lock().await;
    reset_login_rate_limit_for_tests().await;
    let config = cfg();
    let store = MemoryLoginRateLimitStore;
    let old = permit_for("canceled-key", &config);
    assert!(store.begin_with_max_keys(&old, &config, 1).await.unwrap());
    drop(old);
    let current = permit_for("new-key", &config);

    assert!(
        store
            .begin_with_max_keys(&current, &config, 1)
            .await
            .unwrap()
    );
}

#[cfg(feature = "login-rate-limit-valkey")]
mod shared {
    use super::*;

    #[derive(Clone, Copy)]
    enum Reply {
        Accept,
        Reject,
        Fail,
        Pending,
    }

    impl Reply {
        async fn result(self) -> Result<bool, ApiError> {
            match self {
                Self::Accept => Ok(true),
                Self::Reject => Ok(false),
                Self::Fail => Err(ApiError::ServiceUnavailable("test outage".to_string())),
                Self::Pending => pending().await,
            }
        }
    }

    struct ControlledStore {
        begin: Reply,
        finish: Reply,
    }

    impl LoginRateLimitStore for ControlledStore {
        async fn begin(
            &self,
            _: &LoginAttemptPermit,
            _: &LoginRateLimitConfig,
        ) -> Result<bool, ApiError> {
            self.begin.result().await
        }
        async fn finish(
            &self,
            _: &LoginAttemptPermit,
            _: LoginAttemptOutcome,
            _: &LoginRateLimitConfig,
        ) -> Result<Vec<String>, ApiError> {
            self.finish.result().await.map(|_| Vec::new())
        }
        async fn snapshot(&self, _: &LoginRateLimitConfig) -> Result<Vec<ScopeSnapshot>, ApiError> {
            unreachable!("the controlled store only exercises request operations")
        }
        async fn release_entry(&self, _: &str) -> Result<bool, ApiError> {
            unreachable!("the controlled store only exercises request operations")
        }
        async fn clear_all(&self) -> Result<usize, ApiError> {
            unreachable!("the controlled store only exercises request operations")
        }
    }

    #[rstest]
    #[case(Reply::Pending)]
    #[case(Reply::Fail)]
    #[tokio::test]
    async fn canceled_shared_admission_or_degraded_authentication_releases_local_capacity(
        #[case] begin: Reply,
    ) {
        let _guard = LOGIN_RATE_LIMIT_TEST_LOCK.lock().await;
        reset_login_rate_limit_for_tests().await;
        let config = LoginRateLimitConfig {
            max_attempts: 1,
            ..cfg()
        };
        let shared = SharedLoginRateLimitStore::new(ControlledStore {
            begin,
            finish: Reply::Accept,
        });
        let mut request = Box::pin(async {
            let permit = permit_for("shared-cancellation", &config);
            assert!(shared.begin(&permit, &config).await.unwrap());
            pending::<()>().await;
            shared
                .finish(&permit, LoginAttemptOutcome::Failed, &config)
                .await
                .unwrap();
        });
        assert!(poll!(&mut request).is_pending());
        assert_eq!(LOGIN_ATTEMPTS.lock().await.len(), 1);
        drop(request);

        let current = permit_for("shared-cancellation", &config);
        assert!(shared.local.begin(&current, &config).await.unwrap());
    }

    #[rstest]
    #[case(Reply::Pending)]
    #[case(Reply::Fail)]
    #[tokio::test]
    async fn interrupted_shared_completion_preserves_the_local_failure(#[case] finish: Reply) {
        let _guard = LOGIN_RATE_LIMIT_TEST_LOCK.lock().await;
        reset_login_rate_limit_for_tests().await;
        let config = LoginRateLimitConfig {
            max_attempts: 2,
            ..cfg()
        };
        let shared = SharedLoginRateLimitStore::new(ControlledStore {
            begin: Reply::Accept,
            finish,
        });
        let permit = permit_for("shared-finish", &config);
        assert!(shared.begin(&permit, &config).await.unwrap());
        let mut completion = Box::pin(async {
            shared
                .finish(&permit, LoginAttemptOutcome::Failed, &config)
                .await
                .unwrap();
        });
        let result = poll!(&mut completion);
        assert_eq!(result.is_pending(), matches!(finish, Reply::Pending));
        drop(completion);
        drop(permit);

        let map = LOGIN_ATTEMPTS.lock().await;
        let state = &map[&user_ip_key("local", "shared-finish", "unknown")];
        assert!(state.in_flight.is_empty());
        assert_eq!(state.attempts.len(), 1);
    }

    #[tokio::test]
    async fn shared_override_does_not_release_another_local_reservation() {
        let _guard = LOGIN_RATE_LIMIT_TEST_LOCK.lock().await;
        reset_login_rate_limit_for_tests().await;
        let config = LoginRateLimitConfig {
            max_attempts: 1,
            ..cfg()
        };
        let shared = SharedLoginRateLimitStore::new(ControlledStore {
            begin: Reply::Accept,
            finish: Reply::Accept,
        });
        let local = permit_for("shared-override", &config);
        assert!(shared.local.begin(&local, &config).await.unwrap());
        let accepted = permit_for("shared-override", &config);
        assert!(shared.begin(&accepted, &config).await.unwrap());
        shared
            .finish(&accepted, LoginAttemptOutcome::Aborted, &config)
            .await
            .unwrap();
        drop(accepted);

        assert_eq!(
            LOGIN_ATTEMPTS.lock().await[&local.user_ip_key]
                .in_flight
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn healthy_shared_rejection_releases_local_admission() {
        let _guard = LOGIN_RATE_LIMIT_TEST_LOCK.lock().await;
        reset_login_rate_limit_for_tests().await;
        let config = cfg();
        let shared = SharedLoginRateLimitStore::new(ControlledStore {
            begin: Reply::Reject,
            finish: Reply::Accept,
        });
        let permit = permit_for("shared-rejection", &config);

        assert!(!shared.begin(&permit, &config).await.unwrap());
        assert!(shared.local.snapshot(&config).await.unwrap().is_empty());
    }
}
