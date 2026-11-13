//! Per-replica IP budgets complement the adapter's shared active-row ceilings.
use std::{
    collections::HashMap,
    net::IpAddr,
    num::NonZeroU32,
    sync::{LazyLock, Mutex},
    time::Duration,
};

use governor::{
    Quota, RateLimiter,
    clock::{Clock, MonotonicClock, Reference},
    middleware::NoOpMiddleware,
    state::{InMemoryState, NotKeyed},
};
use ipnet::Ipv6Net;

use crate::errors::{ApiError, InteractionError};

// A minute of inactivity fully replenishes either quota, so only then may we
// discard a bucket. Never evict an active bucket to admit an untracked address.
const IDLE_RETENTION: Duration = Duration::from_secs(60);
const MAX_KEYS: usize = 10_000;
const MAX_BEGINS: u32 = 30;
const MAX_REQUESTS: u32 = 600;
static BUDGETS: LazyLock<Mutex<Budgets>> =
    LazyLock::new(|| Mutex::new(Budgets::new(MonotonicClock)));

type Limiter<C> = RateLimiter<NotKeyed, InMemoryState, C, NoOpMiddleware<<C as Clock>::Instant>>;
struct Budget<C: Clock> {
    requests: Limiter<C>,
    begins: Limiter<C>,
    last_seen: C::Instant,
}
struct Budgets<C: Clock = MonotonicClock> {
    entries: HashMap<Option<IpAddr>, Budget<C>>,
    clock: C,
    last_pruned: C::Instant,
}
impl<C: Clock + Clone> Budgets<C> {
    fn new(clock: C) -> Self {
        Self {
            entries: HashMap::new(),
            last_pruned: clock.now(),
            clock,
        }
    }
    fn admit(&mut self, ip: Option<IpAddr>, creation: bool) -> bool {
        // IPv6 privacy addresses from one subnet share admission authority.
        let ip = ip.map(|ip| match ip.to_canonical() {
            IpAddr::V6(ip) => Ipv6Net::new(ip, 64)
                .expect("valid IPv6 prefix")
                .network()
                .into(),
            ip => ip,
        });
        let now = self.clock.now();
        if now.duration_since(self.last_pruned) >= IDLE_RETENTION.into() {
            self.entries
                .retain(|_, budget| now.duration_since(budget.last_seen) < IDLE_RETENTION.into());
            self.last_pruned = now;
        }
        // Reserve one shared overflow/unknown bucket. Filling the registry must
        // not itself deny every new client, and must never reset active quotas.
        let ip = if self.entries.len() >= MAX_KEYS - 1 && !self.entries.contains_key(&ip) {
            None
        } else {
            ip
        };
        let budget = self.entries.entry(ip).or_insert_with(|| Budget {
            requests: RateLimiter::direct_with_clock(
                Quota::per_minute(NonZeroU32::new(MAX_REQUESTS).unwrap()),
                self.clock.clone(),
            ),
            begins: RateLimiter::direct_with_clock(
                Quota::per_minute(NonZeroU32::new(MAX_BEGINS).unwrap()),
                self.clock.clone(),
            ),
            last_seen: now,
        });
        // Rejected creations still consume the total request budget.
        let admitted =
            budget.requests.check().is_ok() && (!creation || budget.begins.check().is_ok());
        // Start idle retention after the limiter checks, including any scheduling
        // delay, so reclamation cannot reset quota debt before it has replenished.
        budget.last_seen = self.clock.now();
        admitted
    }
}
pub(super) fn admit(ip: Option<IpAddr>, creation: bool) -> Result<(), ApiError> {
    if BUDGETS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .admit(ip, creation)
    {
        Ok(())
    } else {
        Err(InteractionError::SlowDown {
            retry_after_seconds: 60,
        }
        .into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use governor::clock::FakeRelativeClock;
    use rstest::rstest;
    use std::net::Ipv4Addr;

    #[rstest]
    #[case::creation(true, MAX_BEGINS)]
    #[case::requests(false, MAX_REQUESTS)]
    fn budgets_limit_bursts_and_refill_gradually(#[case] creation: bool, #[case] maximum: u32) {
        let clock = FakeRelativeClock::default();
        let mut budgets = Budgets::new(clock.clone());
        let ip = Some("192.0.2.1".parse().unwrap());
        for _ in 0..maximum {
            assert!(budgets.admit(ip, creation));
        }
        assert!(!budgets.admit(ip, creation));
        clock.advance(IDLE_RETENTION / maximum);
        assert!(budgets.admit(ip, creation));
        assert!(!budgets.admit(ip, creation));
    }

    #[test]
    fn unknown_addresses_share_one_bounded_bucket() {
        let mut budgets = Budgets::new(FakeRelativeClock::default());
        for _ in 0..MAX_BEGINS {
            assert!(budgets.admit(None, true));
        }
        assert!(!budgets.admit(None, true));
        assert_eq!(budgets.entries.len(), 1);
    }

    #[test]
    fn creations_share_the_total_request_budget() {
        let mut budgets = Budgets::new(FakeRelativeClock::default());
        for _ in 0..MAX_REQUESTS {
            assert!(budgets.admit(None, false));
        }
        assert!(!budgets.admit(None, true));
    }

    #[test]
    fn rejected_creations_still_count_as_requests() {
        let mut budgets = Budgets::new(FakeRelativeClock::default());
        for _ in 0..MAX_REQUESTS {
            budgets.admit(None, true);
        }
        assert!(!budgets.admit(None, false));
    }

    #[test]
    fn full_registry_preserves_active_limits_and_reclaims_only_idle_buckets() {
        let clock = FakeRelativeClock::default();
        let mut budgets = Budgets::new(clock.clone());
        for ip in 0..(MAX_KEYS - 1) as u32 {
            assert!(budgets.admit(Some(Ipv4Addr::from(ip).into()), true));
        }
        let active = Some(Ipv4Addr::from(0).into());
        let new = Some(Ipv4Addr::from(MAX_KEYS as u32).into());
        assert!(budgets.admit(new, true));
        clock.advance(IDLE_RETENTION / 2);
        for _ in 0..MAX_BEGINS {
            assert!(budgets.admit(active, true));
        }
        // The old fixed-window boundary must not replenish this active bucket.
        clock.advance(IDLE_RETENTION / 2);
        for _ in 0..MAX_BEGINS / 2 {
            assert!(budgets.admit(active, true));
        }
        assert!(!budgets.admit(active, true));
        assert!(budgets.admit(new, true));
        assert_eq!(budgets.entries.len(), 2);
    }

    #[test]
    fn overflow_clients_share_bounded_quota_without_eviction() {
        let mut budgets = Budgets::new(FakeRelativeClock::default());
        for ip in 0..(MAX_KEYS - 1) as u32 {
            assert!(budgets.admit(Some(Ipv4Addr::from(ip).into()), true));
        }
        for ip in MAX_KEYS as u32..MAX_KEYS as u32 + MAX_BEGINS {
            assert!(budgets.admit(Some(Ipv4Addr::from(ip).into()), true));
        }
        assert!(!budgets.admit(Some("192.0.2.1".parse().unwrap()), true));
        assert_eq!(budgets.entries.len(), MAX_KEYS);
    }

    #[test]
    fn ipv6_privacy_addresses_share_the_subnet_budget() {
        let mut budgets = Budgets::new(FakeRelativeClock::default());
        for suffix in 1..=MAX_BEGINS {
            assert!(budgets.admit(
                Some(format!("2001:db8:1:2::{suffix:x}").parse().unwrap()),
                true
            ));
        }
        assert!(!budgets.admit(Some("2001:db8:1:2::ffff".parse().unwrap()), true));
        assert!(budgets.admit(Some("2001:db8:1:3::1".parse().unwrap()), true));
        assert_eq!(budgets.entries.len(), 2);
    }
}
