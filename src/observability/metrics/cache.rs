use std::time::{Duration, Instant};

use crate::storage::{
    StorageEventMetricsSnapshot, StorageInventoryGaugeSnapshot, StorageTaskGaugeSnapshot,
};

const DB_SCRAPE_CACHE_TTL: Duration = Duration::from_secs(30);

#[derive(Default)]
pub(super) struct ScrapeCache {
    pub(super) inventory: CachedSnapshot<StorageInventoryGaugeSnapshot>,
    pub(super) tasks: CachedSnapshot<StorageTaskGaugeSnapshot>,
    pub(super) events: CachedSnapshot<StorageEventMetricsSnapshot>,
}

pub(super) struct CachedSnapshot<T> {
    value: Option<T>,
    refreshed_at: Option<Instant>,
}

impl<T> Default for CachedSnapshot<T> {
    fn default() -> Self {
        Self {
            value: None,
            refreshed_at: None,
        }
    }
}

impl<T: Clone> CachedSnapshot<T> {
    pub(super) fn fresh_value(&self, now: Instant) -> Option<T> {
        match (self.value.as_ref(), self.refreshed_at) {
            (Some(value), Some(refreshed_at))
                if now.duration_since(refreshed_at) < DB_SCRAPE_CACHE_TTL =>
            {
                Some(value.clone())
            }
            _ => None,
        }
    }

    pub(super) fn cached_value(&self) -> Option<T> {
        self.value.clone()
    }

    pub(super) fn store(&mut self, value: T, now: Instant) {
        self.value = Some(value);
        self.refreshed_at = Some(now);
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use super::{CachedSnapshot, DB_SCRAPE_CACHE_TTL};

    #[test]
    fn cached_snapshot_is_fresh_before_the_ttl() {
        let stored_at = Instant::now();
        let mut snapshot = CachedSnapshot::default();
        snapshot.store("value", stored_at);

        assert_eq!(
            snapshot.fresh_value(stored_at + Duration::from_secs(29)),
            Some("value")
        );
    }

    #[test]
    fn expired_snapshot_remains_available_as_a_stale_fallback() {
        let stored_at = Instant::now();
        let mut snapshot = CachedSnapshot::default();
        snapshot.store("value", stored_at);

        assert_eq!(snapshot.fresh_value(stored_at + DB_SCRAPE_CACHE_TTL), None);
        assert_eq!(snapshot.cached_value(), Some("value"));
    }
}
