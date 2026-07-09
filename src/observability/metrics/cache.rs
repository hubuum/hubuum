use std::time::{Duration, Instant};

use crate::db::traits::metrics::{InventoryMetricsSnapshot, TaskMetricsSnapshot};

const DB_SCRAPE_CACHE_TTL: Duration = Duration::from_secs(30);

#[derive(Default)]
pub(super) struct ScrapeCache {
    pub(super) inventory: CachedSnapshot<InventoryMetricsSnapshot>,
    pub(super) tasks: CachedSnapshot<TaskMetricsSnapshot>,
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
