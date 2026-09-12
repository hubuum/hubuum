use super::*;
use rstest::rstest;
use std::net::Ipv4Addr;

#[cfg(feature = "login-rate-limit-valkey")]
fn valkey_test_url() -> String {
    std::env::var("HUBUUM_TEST_VALKEY_URL")
        .unwrap_or_else(|_| "redis://127.0.0.1:6379/".to_string())
}

#[cfg(feature = "login-rate-limit-valkey")]
#[test]
fn valkey_store_settings_debug_redacts_connection_url() {
    let settings = LoginRateLimitStoreSettings::valkey(
        "redis://limiter-user:limiter-secret@valkey.example/7",
        "hubuum-login",
        Duration::from_secs(2),
    )
    .unwrap();

    let debug = format!("{settings:?}");

    assert!(debug.contains(REDACTED_DEBUG_VALUE));
    assert!(debug.contains("hubuum-login"));
    assert!(debug.contains("2s"));
    assert!(!debug.contains("limiter-user"));
    assert!(!debug.contains("limiter-secret"));
    assert!(!debug.contains("valkey.example"));
}

#[rstest]
#[case("u:local/alice|192.0.2.1", "principal_ip")]
#[case("i:192.0.2.1", "ip")]
#[case("s:192.0.2.0/24", "subnet")]
#[case("x:unexpected", "unknown")]
fn limiter_keys_have_bounded_metric_scope_kinds(#[case] key: &str, #[case] expected: &'static str) {
    assert_eq!(scope_kind(key), expected);
}

fn cfg() -> LoginRateLimitConfig {
    LoginRateLimitConfig {
        enabled: true,
        max_attempts: 5,
        max_attempts_per_ip: 20,
        max_attempts_per_subnet: 100,
        window_seconds: 300,
        backoff_base_seconds: 300,
        backoff_max_seconds: 86_400,
        subnet_prefix_v4: 24,
        subnet_prefix_v6: 64,
    }
}

fn permit_for(username: &str, config: &LoginRateLimitConfig) -> LoginAttemptPermit {
    LoginAttemptPermit {
        scopes: scopes_for("local", username, None, config),
        user_ip_key: user_ip_key("local", username, "unknown"),
        reservation: Reservation::new(Duration::from_secs(config.window_seconds)),
        enabled: true,
    }
}

#[cfg(feature = "login-rate-limit-valkey")]
async fn shared_local_snapshot(
    store: &ActiveLoginRateLimitStore,
    config: &LoginRateLimitConfig,
) -> Vec<ScopeSnapshot> {
    match store {
        ActiveLoginRateLimitStore::Shared(store) => store.local.snapshot(config).await.unwrap(),
        ActiveLoginRateLimitStore::Memory(_) => unreachable!(),
    }
}

async fn assert_store_contract(store: &impl LoginRateLimitStore, username: &str) {
    let mut config = cfg();
    config.max_attempts = 2;
    config.max_attempts_per_ip = 0;
    config.max_attempts_per_subnet = 0;
    store.clear_all().await.unwrap();

    for _ in 0..config.max_attempts {
        let permit = permit_for(username, &config);
        assert!(store.begin(&permit, &config).await.unwrap());
        store
            .finish(&permit, LoginAttemptOutcome::Failed, &config)
            .await
            .unwrap();
    }

    let blocked = permit_for(username, &config);
    assert!(!store.begin(&blocked, &config).await.unwrap());
    let snapshots = store.snapshot(&config).await.unwrap();
    assert_eq!(snapshots.len(), 1);
    assert!(snapshots[0].locked);
    assert!(store.release_entry(&blocked.user_ip_key).await.unwrap());

    let released = permit_for(username, &config);
    assert!(store.begin(&released, &config).await.unwrap());
    store
        .finish(&released, LoginAttemptOutcome::Aborted, &config)
        .await
        .unwrap();
    assert_eq!(store.clear_all().await.unwrap(), 0);
}

#[tokio::test]
async fn memory_store_satisfies_limiter_contract() {
    let _guard = LOGIN_RATE_LIMIT_TEST_LOCK.lock().await;
    assert_store_contract(&MemoryLoginRateLimitStore, "memory-contract").await;
}

#[cfg(feature = "login-rate-limit-valkey")]
#[tokio::test]
#[ignore = "requires Valkey or Redis at redis://127.0.0.1:6379/"]
async fn valkey_store_satisfies_limiter_contract() {
    let store = valkey::ValkeyLoginRateLimitStore::connect(
        valkey_test_url(),
        format!("hubuum:test-contract:{}", Uuid::new_v4()),
        Duration::from_secs(1),
    )
    .await
    .unwrap();
    assert_store_contract(&store, "valkey-contract").await;
}

#[cfg(feature = "login-rate-limit-valkey")]
#[tokio::test]
#[ignore = "requires Valkey or Redis at redis://127.0.0.1:6379/"]
async fn valkey_capacity_does_not_evict_in_flight_reservations() {
    let mut config = cfg();
    config.max_attempts_per_ip = 0;
    config.max_attempts_per_subnet = 0;
    let store = valkey::ValkeyLoginRateLimitStore::connect(
        valkey_test_url(),
        format!("hubuum:test-capacity-inflight:{}", Uuid::new_v4()),
        Duration::from_secs(1),
    )
    .await
    .unwrap();
    let protected = permit_for("protected-inflight", &config);
    let newcomer = permit_for("newcomer", &config);

    assert!(
        store
            .begin_with_max_keys(&protected, &config, 1)
            .await
            .unwrap()
    );
    assert!(
        !store
            .begin_with_max_keys(&newcomer, &config, 1)
            .await
            .unwrap()
    );
    let snapshots = store.snapshot(&config).await.unwrap();
    assert_eq!(snapshots.len(), 1);
    assert_eq!(snapshots[0].key, protected.user_ip_key);

    store.clear_all().await.unwrap();
}

#[cfg(feature = "login-rate-limit-valkey")]
#[tokio::test]
#[ignore = "requires Valkey or Redis at redis://127.0.0.1:6379/"]
async fn valkey_capacity_does_not_evict_active_lockouts() {
    let mut config = cfg();
    config.max_attempts = 1;
    config.max_attempts_per_ip = 0;
    config.max_attempts_per_subnet = 0;
    let store = valkey::ValkeyLoginRateLimitStore::connect(
        valkey_test_url(),
        format!("hubuum:test-capacity-lock:{}", Uuid::new_v4()),
        Duration::from_secs(1),
    )
    .await
    .unwrap();
    let protected = permit_for("protected-lock", &config);
    let newcomer = permit_for("newcomer", &config);

    assert!(
        store
            .begin_with_max_keys(&protected, &config, 1)
            .await
            .unwrap()
    );
    store
        .finish(&protected, LoginAttemptOutcome::Failed, &config)
        .await
        .unwrap();
    assert!(
        !store
            .begin_with_max_keys(&newcomer, &config, 1)
            .await
            .unwrap()
    );
    let snapshots = store.snapshot(&config).await.unwrap();
    assert_eq!(snapshots.len(), 1);
    assert_eq!(snapshots[0].key, protected.user_ip_key);
    assert!(snapshots[0].locked);

    store.clear_all().await.unwrap();
}

#[cfg(feature = "login-rate-limit-valkey")]
#[tokio::test]
#[ignore = "requires Valkey or Redis at redis://127.0.0.1:6379/"]
async fn shared_store_accepts_remote_admin_release_despite_stale_local_lockout() {
    let _guard = LOGIN_RATE_LIMIT_TEST_LOCK.lock().await;
    reset_login_rate_limit_for_tests().await;
    let mut config = cfg();
    config.max_attempts = 2;
    config.max_attempts_per_ip = 0;
    config.max_attempts_per_subnet = 0;
    let prefix = format!("hubuum:test-remote-release:{}", Uuid::new_v4());
    let shared = ActiveLoginRateLimitStore::Shared(Box::new(SharedLoginRateLimitStore::new(
        valkey::ValkeyLoginRateLimitStore::connect(
            valkey_test_url(),
            prefix.clone(),
            Duration::from_secs(1),
        )
        .await
        .unwrap(),
    )));
    let remote_admin = valkey::ValkeyLoginRateLimitStore::connect(
        valkey_test_url(),
        prefix,
        Duration::from_secs(1),
    )
    .await
    .unwrap();

    for _ in 0..config.max_attempts {
        let permit = permit_for("remote-release", &config);
        assert!(shared.begin(&permit, &config).await.unwrap());
        shared
            .finish(&permit, LoginAttemptOutcome::Failed, &config)
            .await
            .unwrap();
    }

    let blocked = permit_for("remote-release", &config);
    assert!(!shared.begin(&blocked, &config).await.unwrap());
    assert!(
        remote_admin
            .release_entry(&blocked.user_ip_key)
            .await
            .unwrap()
    );
    assert!(shared_local_snapshot(&shared, &config).await[0].locked);

    let released = permit_for("remote-release", &config);
    assert!(shared.begin(&released, &config).await.unwrap());
    shared
        .finish(&released, LoginAttemptOutcome::Aborted, &config)
        .await
        .unwrap();

    remote_admin.clear_all().await.unwrap();
    reset_login_rate_limit_for_tests().await;
}

#[test]
fn lockout_duration_doubles_and_caps() {
    assert_eq!(lockout_duration(1, 300, 86_400), Duration::from_secs(300));
    assert_eq!(lockout_duration(2, 300, 86_400), Duration::from_secs(600));
    assert_eq!(lockout_duration(3, 300, 86_400), Duration::from_secs(1200));
    // Caps at the maximum rather than overflowing.
    assert_eq!(
        lockout_duration(64, 300, 86_400),
        Duration::from_secs(86_400)
    );
    assert_eq!(
        lockout_duration(1_000_000, 300, 86_400),
        Duration::from_secs(86_400)
    );
}

#[test]
fn scope_locks_after_reaching_threshold() {
    let now = Instant::now();
    let window = Duration::from_secs(300);
    let mut state = ScopeState::default();
    let config = cfg();

    for _ in 0..config.max_attempts {
        state.prune(now, window);
        state.attempts.push_back(now);
        if state.attempts.len() >= config.max_attempts {
            state.trigger_lockout(now, &config);
        }
    }

    assert!(state.is_locked(now));
    // Lockout clears the window so the level (not the count) drives further backoff.
    assert!(state.attempts.is_empty());
    assert_eq!(state.lockout_level, 1);
}

#[test]
fn repeated_lockouts_increase_backoff_level() {
    let now = Instant::now();
    let config = cfg();
    let mut state = ScopeState::default();

    state.trigger_lockout(now, &config);
    let first = state.locked_until.unwrap();
    state.trigger_lockout(now, &config);
    let second = state.locked_until.unwrap();

    assert_eq!(state.lockout_level, 2);
    assert!(second > first);
}

#[test]
fn backoff_escalates_when_sustained_but_resets_after_cooloff() {
    let config = cfg();
    let window = Duration::from_secs(config.window_seconds);
    let t0 = Instant::now();
    let mut state = ScopeState::default();

    // First episode: reach the threshold -> lockout level 1.
    for _ in 0..config.max_attempts {
        state.register_failure(t0, config.max_attempts, &config);
    }
    assert_eq!(state.lockout_level, 1);

    // Sustained: resume just after the lockout expires (within the cool-off) -> the
    // escalation persists and the next lockout is level 2.
    let resume = state.locked_until.unwrap() + Duration::from_secs(1);
    for _ in 0..config.max_attempts {
        state.register_failure(resume, config.max_attempts, &config);
    }
    assert_eq!(
        state.lockout_level, 2,
        "sustained abuse must keep escalating"
    );

    // Cool-off: stay quiet past expiry + a full window -> escalation resets.
    let cooled = state.locked_until.unwrap() + window + Duration::from_secs(1);
    state.register_failure(cooled, config.max_attempts, &config);
    assert_eq!(state.lockout_level, 0, "a genuine cool-off resets backoff");
    assert!(state.locked_until.is_none());
    assert_eq!(state.attempts.len(), 1);
}

#[test]
fn subnet_label_aggregates_by_prefix() {
    let config = cfg();
    let a = subnet_label(IpAddr::V4(Ipv4Addr::new(198, 51, 100, 10)), &config);
    let b = subnet_label(IpAddr::V4(Ipv4Addr::new(198, 51, 100, 250)), &config);
    assert_eq!(a, b);
    assert_eq!(a, "198.51.100.0/24");
}

#[test]
fn scopes_skip_disabled_and_unknown_ip() {
    let mut config = cfg();
    config.max_attempts_per_ip = 0;
    config.max_attempts_per_subnet = 0;
    let only_user = scopes_for(
        "local",
        "alice",
        Some(IpAddr::V4(Ipv4Addr::LOCALHOST)),
        &config,
    );
    assert_eq!(only_user.len(), 1);

    let no_ip = scopes_for("local", "alice", None, &cfg());
    assert_eq!(no_ip.len(), 1, "no IP means only the user+IP scope applies");
}

#[test]
fn scopes_cover_user_ip_and_subnet_when_enabled() {
    let scopes = scopes_for(
        "Directory",
        "Alice",
        Some(IpAddr::V4(Ipv4Addr::new(198, 51, 100, 7))),
        &cfg(),
    );
    let keys: Vec<&str> = scopes.iter().map(|(k, _)| k.as_str()).collect();
    assert!(keys.iter().any(|k| k.starts_with("u:directory/alice|")));
    assert!(keys.contains(&"i:198.51.100.7"));
    assert!(keys.contains(&"s:198.51.100.0/24"));
}

#[tokio::test]
async fn memory_capacity_rejects_new_scope_without_evicting_active_lockout() {
    let _guard = LOGIN_RATE_LIMIT_TEST_LOCK.lock().await;
    reset_login_rate_limit_for_tests().await;
    let mut config = cfg();
    config.max_attempts = 1;
    config.max_attempts_per_ip = 0;
    config.max_attempts_per_subnet = 0;
    let store = MemoryLoginRateLimitStore;
    let protected = permit_for("protected-memory-lock", &config);
    let newcomer = permit_for("newcomer-memory-lock", &config);

    assert!(
        store
            .begin_with_max_keys(&protected, &config, 1)
            .await
            .unwrap()
    );
    store
        .finish(&protected, LoginAttemptOutcome::Failed, &config)
        .await
        .unwrap();
    assert!(
        !store
            .begin_with_max_keys(&newcomer, &config, 1)
            .await
            .unwrap()
    );

    let snapshots = store.snapshot(&config).await.unwrap();
    assert_eq!(snapshots.len(), 1);
    assert_eq!(snapshots[0].key, protected.user_ip_key);
    assert!(snapshots[0].locked);
    reset_login_rate_limit_for_tests().await;
}

#[test]
fn prune_map_keeps_locked_but_drops_expired() {
    let now = Instant::now();
    let window = Duration::from_secs(300);
    let config = cfg();
    let mut map: HashMap<String, ScopeState> = HashMap::new();

    let mut expired = ScopeState::default();
    expired.attempts.push_back(now - Duration::from_secs(600));
    map.insert("expired".to_string(), expired);

    let mut locked = ScopeState::default();
    locked.trigger_lockout(now, &config);
    map.insert("locked".to_string(), locked);

    prune_login_attempts_map(&mut map, now, window);

    assert!(!map.contains_key("expired"));
    assert!(map.contains_key("locked"));
}

#[test]
fn prune_map_retains_expired_lockouts_within_cooloff() {
    let now = Instant::now();
    let window = Duration::from_secs(300);
    let mut map: HashMap<String, ScopeState> = HashMap::new();

    // Lockout expired 10s ago: still inside the cool-off window, so its escalation
    // level must survive pruning.
    let mut cooling = ScopeState {
        locked_until: Some(now - Duration::from_secs(10)),
        lockout_level: 2,
        ..ScopeState::default()
    };
    cooling.attempts.clear();
    map.insert("cooling".to_string(), cooling);

    // Lockout expired 400s ago: past the cool-off, eligible for pruning.
    let cooled = ScopeState {
        locked_until: Some(now - Duration::from_secs(400)),
        lockout_level: 3,
        ..ScopeState::default()
    };
    map.insert("cooled".to_string(), cooled);

    prune_login_attempts_map(&mut map, now, window);

    assert!(map.contains_key("cooling"));
    assert_eq!(map["cooling"].lockout_level, 2);
    assert!(!map.contains_key("cooled"));
}

#[tokio::test]
async fn concurrent_attempts_reserve_capacity_atomically() {
    let _guard = LOGIN_RATE_LIMIT_TEST_LOCK.lock().await;
    reset_login_rate_limit_for_tests().await;
    let config = login_rate_limit_config();
    assert!(config.enabled);

    let mut permits = Vec::new();
    for _ in 0..config.max_attempts {
        permits.push(
            begin_login_attempt("local", "atomic-limit-user", None)
                .await
                .expect("limiter store should be available")
                .expect("capacity below the threshold should be reserved"),
        );
    }
    assert!(
        begin_login_attempt("local", "atomic-limit-user", None)
            .await
            .expect("limiter store should be available")
            .is_none()
    );

    for permit in permits {
        finish_login_attempt(permit, LoginAttemptOutcome::Aborted)
            .await
            .unwrap();
    }
    assert!(
        begin_login_attempt("local", "atomic-limit-user", None)
            .await
            .expect("limiter store should be available")
            .is_some()
    );
    reset_login_rate_limit_for_tests().await;
}

#[cfg(feature = "login-rate-limit-valkey")]
#[tokio::test]
async fn unavailable_valkey_falls_back_to_local_enforcement() {
    let _guard = LOGIN_RATE_LIMIT_TEST_LOCK.lock().await;
    reset_login_rate_limit_for_tests().await;
    let mut config = cfg();
    config.max_attempts = 2;
    config.max_attempts_per_ip = 0;
    config.max_attempts_per_subnet = 0;
    let shared = ActiveLoginRateLimitStore::Shared(Box::new(SharedLoginRateLimitStore::new(
        valkey::ValkeyLoginRateLimitStore::connect(
            "redis://127.0.0.1:1/".to_string(),
            "hubuum:test-unavailable".to_string(),
            Duration::from_millis(25),
        )
        .await
        .unwrap(),
    )));

    for _ in 0..config.max_attempts {
        let permit = LoginAttemptPermit {
            scopes: scopes_for("local", "fallback-user", None, &config),
            user_ip_key: user_ip_key("local", "fallback-user", "unknown"),
            reservation: Reservation::new(Duration::from_secs(config.window_seconds)),
            enabled: true,
        };
        assert!(shared.begin(&permit, &config).await.unwrap());
        shared
            .finish(&permit, LoginAttemptOutcome::Failed, &config)
            .await
            .unwrap();
    }

    let blocked = LoginAttemptPermit {
        scopes: scopes_for("local", "fallback-user", None, &config),
        user_ip_key: user_ip_key("local", "fallback-user", "unknown"),
        reservation: Reservation::new(Duration::from_secs(config.window_seconds)),
        enabled: true,
    };
    assert!(!shared.begin(&blocked, &config).await.unwrap());

    assert_eq!(shared_local_snapshot(&shared, &config).await.len(), 1);
    assert!(shared.release_entry(&blocked.user_ip_key).await.is_err());
    assert_eq!(shared_local_snapshot(&shared, &config).await.len(), 1);
    assert!(shared.clear_all().await.is_err());
    assert_eq!(shared_local_snapshot(&shared, &config).await.len(), 1);
    reset_login_rate_limit_for_tests().await;
}

mod cancellation;

#[cfg(feature = "login-rate-limit-valkey")]
mod valkey_cancellation;
