//! Per-replica IP budgets complement the adapter's shared active-row ceilings.
use crate::errors::{ApiError, InteractionError};
use std::{
    collections::HashMap,
    net::IpAddr,
    sync::{LazyLock, Mutex},
    time::{Duration, Instant},
};

const WINDOW: Duration = Duration::from_secs(60);
const MAX_KEYS: usize = 10_000;
const MAX_BEGINS: u32 = 30;
const MAX_REQUESTS: u32 = 600;
static BUDGETS: LazyLock<Mutex<Budgets>> = LazyLock::new(|| Mutex::new(Budgets::default()));

#[derive(Default)]
struct Budgets {
    entries: HashMap<Option<IpAddr>, Window>,
}
struct Window {
    start: Instant,
    requests: u32,
    begins: u32,
}
impl Budgets {
    fn admit(&mut self, ip: Option<IpAddr>, creation: bool, now: Instant) -> bool {
        self.entries
            .retain(|_, window| now.duration_since(window.start) < WINDOW);
        if self.entries.len() >= MAX_KEYS && !self.entries.contains_key(&ip) {
            return false;
        }
        let window = self.entries.entry(ip).or_insert(Window {
            start: now,
            requests: 0,
            begins: 0,
        });
        if window.requests >= MAX_REQUESTS || creation && window.begins >= MAX_BEGINS {
            return false;
        }
        window.requests += 1;
        window.begins += u32::from(creation);
        true
    }
}
pub(super) fn admit(ip: Option<IpAddr>, creation: bool) -> Result<(), ApiError> {
    if BUDGETS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .admit(ip, creation, Instant::now())
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
    use rstest::rstest;

    #[rstest]
    #[case::creation(true, MAX_BEGINS)]
    #[case::requests(false, MAX_REQUESTS)]
    fn budgets_limit_one_ip_and_expire(#[case] creation: bool, #[case] maximum: u32) {
        let mut budgets = Budgets::default();
        let now = Instant::now();
        let ip = Some("192.0.2.1".parse().unwrap());
        for _ in 0..maximum {
            assert!(budgets.admit(ip, creation, now));
        }
        assert!(!budgets.admit(ip, creation, now));
        assert!(budgets.admit(ip, creation, now + WINDOW));
    }

    #[test]
    fn unknown_addresses_share_one_bounded_bucket() {
        let mut budgets = Budgets::default();
        let now = Instant::now();
        for _ in 0..MAX_BEGINS {
            assert!(budgets.admit(None, true, now));
        }
        assert!(!budgets.admit(None, true, now));
        assert_eq!(budgets.entries.len(), 1);
    }
}
