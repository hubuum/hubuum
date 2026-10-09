//! Bounded, value-free observations shared by all application storage adapters.
use chrono::{DateTime, Utc};
use hubuum_domain::ClassId;
use hubuum_query::{FilterField, Operator, QueryFilters, QueryScalarType, infer_query_scalar_type};
use hubuum_storage_core::{
    StorageQueryObservationSettings, StorageQueryObservationSnapshot,
    StorageQueryPatternObservation, StorageQueryUsageOperation as UsageOperation,
    StorageQueryUsagePattern, StorageQueryUsageValueType as UsageType,
};
use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

type PatternKey = (ClassId, StorageQueryUsagePattern);
struct Entry {
    observation: StorageQueryPatternObservation,
    expires_at: Instant,
}
struct State {
    entries: HashMap<PatternKey, Entry>,
    class_counts: HashMap<ClassId, usize>,
    drops: u64,
}
impl State {
    fn expire(&mut self, now: Instant) {
        let counts = &mut self.class_counts;
        self.entries.retain(|(class_id, _), entry| {
            if entry.expires_at > now {
                return true;
            }
            if let Some(count) = counts.get_mut(class_id) {
                *count -= 1;
                if *count == 0 {
                    counts.remove(class_id);
                }
            }
            false
        });
    }
}

pub(crate) struct QueryObservations {
    settings: StorageQueryObservationSettings,
    started_at: DateTime<Utc>,
    requests: AtomicU64,
    state: Mutex<State>,
}
impl QueryObservations {
    pub(crate) fn new(settings: StorageQueryObservationSettings) -> Self {
        Self {
            settings,
            started_at: Utc::now(),
            requests: AtomicU64::new(0),
            state: Mutex::new(State {
                entries: HashMap::new(),
                class_counts: HashMap::new(),
                drops: 0,
            }),
        }
    }

    pub(crate) fn begin(
        self: &Arc<Self>,
        filters: &QueryFilters,
        explicit_class: Option<ClassId>,
    ) -> Option<QueryObservationTicket> {
        if !self.settings.enabled()
            || !self
                .requests
                .fetch_add(1, Ordering::Relaxed)
                .is_multiple_of(u64::from(self.settings.sample_every()))
        {
            return None;
        }
        if filters.len() > self.settings.max_predicates_per_query() {
            return None;
        }
        let class_id = match explicit_class {
            Some(id) => id,
            None => attributed_class(filters)?,
        };
        let mut patterns = HashSet::new();
        for filter in filters
            .iter()
            .filter(|filter| filter.field == FilterField::JsonData)
        {
            let (operator, negated) = filter.operator.op_and_neg();
            if negated {
                continue;
            }
            let usage_operation = match operator {
                Operator::Equals => UsageOperation::Equals,
                Operator::Gt => UsageOperation::Gt,
                Operator::Gte => UsageOperation::Gte,
                Operator::Lt => UsageOperation::Lt,
                Operator::Lte => UsageOperation::Lte,
                Operator::Between => UsageOperation::Between,
                _ => continue,
            };
            let Some((path, value)) = filter.value.split_once('=') else {
                continue;
            };
            if path.len() > 512 || value.len() > 4096 {
                continue;
            }
            let value_type = match infer_query_scalar_type(value, operator) {
                Some(QueryScalarType::String) => UsageType::String,
                Some(QueryScalarType::Numeric) => UsageType::Numeric,
                Some(QueryScalarType::Boolean) => UsageType::Boolean,
                _ => continue,
            };
            if let Ok(pattern) =
                StorageQueryUsagePattern::try_new(path, value_type, vec![usage_operation])
            {
                patterns.insert(pattern);
            }
        }
        if patterns.is_empty() {
            return None;
        }
        Some(QueryObservationTicket {
            collector: self.clone(),
            class_id,
            patterns,
            started: Instant::now(),
        })
    }

    pub(crate) fn snapshot(&self, class_id: ClassId) -> StorageQueryObservationSnapshot {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.expire(Instant::now());
        let mut patterns = state
            .entries
            .iter()
            .filter(|((id, _), _)| *id == class_id)
            .map(|(_, entry)| entry.observation.clone())
            .collect::<Vec<_>>();
        patterns.sort_by_cached_key(|value| {
            (
                value.pattern().path().canonical().to_owned(),
                serde_json::to_string(&value.pattern().value_type()).expect("scalar enum"),
            )
        });
        StorageQueryObservationSnapshot::new(self.settings, self.started_at, patterns, state.drops)
    }
}

pub(crate) struct QueryObservationTicket {
    collector: Arc<QueryObservations>,
    class_id: ClassId,
    patterns: HashSet<StorageQueryUsagePattern>,
    started: Instant,
}
impl QueryObservationTicket {
    pub(crate) fn complete(self) {
        let elapsed = self.started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64;
        let instant = Instant::now();
        let now = Utc::now();
        let settings = self.collector.settings;
        let mut state = self
            .collector
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.expire(instant);
        for pattern in self.patterns {
            let key = (self.class_id, pattern);
            if let Some(entry) = state.entries.get_mut(&key) {
                entry.observation.record(now, elapsed);
                continue;
            }
            if state.entries.len() >= settings.max_patterns()
                || state.class_counts.get(&self.class_id).copied().unwrap_or(0)
                    >= settings.max_patterns_per_class()
            {
                state.drops = state.drops.saturating_add(1);
                continue;
            }
            *state.class_counts.entry(self.class_id).or_default() += 1;
            let observation = StorageQueryPatternObservation::new(key.1.clone(), now, elapsed);
            state.entries.insert(
                key,
                Entry {
                    observation,
                    expires_at: instant
                        + Duration::from_secs(u64::from(settings.retention_seconds())),
                },
            );
        }
    }
}

fn attributed_class(filters: &QueryFilters) -> Option<ClassId> {
    let mut class = None;
    for filter in filters
        .iter()
        .filter(|filter| matches!(filter.field, FilterField::ClassId | FilterField::Classes))
    {
        let (operator, negated) = filter.operator.op_and_neg();
        if negated || !matches!(operator, Operator::Equals | Operator::In) {
            return None;
        }
        let id = ClassId::new(filter.value.parse::<i32>().ok()?).ok()?;
        if class.is_some_and(|previous| previous != id) {
            return None;
        }
        class = Some(id);
    }
    class
}

#[cfg(test)]
mod tests {
    use super::*;
    use hubuum_query::parse_query_parameter;
    use rstest::rstest;

    fn collector(max: usize, per_class: usize, sample: u32) -> Arc<QueryObservations> {
        Arc::new(QueryObservations::new(
            StorageQueryObservationSettings::try_new(true, sample, max, per_class, 60, 16).unwrap(),
        ))
    }
    fn record(collector: &Arc<QueryObservations>, query: &str) {
        let options = parse_query_parameter(query).unwrap();
        if let Some(ticket) = collector.begin(options.filters(), None) {
            ticket.complete();
        }
    }
    #[test]
    fn observed_structure_excludes_values_and_failed_queries() {
        let collector = collector(8, 8, 1);
        record(&collector, "class_id=1&json_data=serial=secret-alpha");
        record(&collector, "class_id=1&json_data=serial=secret-beta");
        let failed = parse_query_parameter("class_id=1&json_data=failed=secret-gamma").unwrap();
        drop(collector.begin(failed.filters(), None));
        let snapshot = collector.snapshot(ClassId::new(1).unwrap());
        assert_eq!(snapshot.patterns().len(), 1);
        assert_eq!(snapshot.patterns()[0].sampled_queries(), 2);
        let serialized = serde_json::to_string(&snapshot).unwrap();
        assert!(!serialized.contains("secret"));
        assert!(!serialized.contains("failed"));
    }
    #[rstest]
    #[case("json_data=serial=secret")]
    #[case("class_id=1,2&json_data=serial=secret")]
    #[case("class_id__not_equals=1&json_data=serial=secret")]
    #[case("class_id=1&json_data__not_equals=serial=secret")]
    #[case("class_id=1&json_data__icontains=serial=secret")]
    fn unsupported_or_unattributable_queries_are_not_observed(#[case] query: &str) {
        let collector = collector(8, 8, 1);
        record(&collector, query);
        assert!(collector.state.lock().unwrap().entries.is_empty());
    }
    #[test]
    fn sampling_counts_successful_logical_requests_once() {
        let collector = collector(8, 8, 2);
        for _ in 0..10 {
            record(
                &collector,
                "class_id=1&include_total=true&json_data=serial=sample",
            );
        }
        assert_eq!(
            collector.snapshot(ClassId::new(1).unwrap()).patterns()[0].sampled_queries(),
            5
        );
    }
    #[test]
    fn per_class_capacity_preserves_room_for_other_classes() {
        let collector = collector(3, 1, 1);
        record(&collector, "class_id=1&json_data=first=sample");
        record(&collector, "class_id=1&json_data=second=sample");
        record(&collector, "class_id=2&json_data=first=sample");
        let state = collector.state.lock().unwrap();
        assert_eq!(state.entries.len(), 2);
        assert_eq!(state.drops, 1);
    }
    #[test]
    fn global_capacity_is_bounded() {
        let collector = collector(2, 2, 1);
        for id in 1..=3 {
            record(&collector, &format!("class_id={id}&json_data=first=sample"));
        }
        let state = collector.state.lock().unwrap();
        assert_eq!(state.entries.len(), 2);
        assert_eq!(state.drops, 1);
    }
    #[test]
    fn expired_patterns_release_capacity_and_counts() {
        let collector = collector(1, 1, 1);
        record(&collector, "class_id=1&json_data=old=sample");
        collector
            .state
            .lock()
            .unwrap()
            .expire(Instant::now() + Duration::from_secs(61));
        record(&collector, "class_id=2&json_data=new=sample");
        assert!(
            collector
                .snapshot(ClassId::new(1).unwrap())
                .patterns()
                .is_empty()
        );
        assert_eq!(
            collector
                .snapshot(ClassId::new(2).unwrap())
                .patterns()
                .len(),
            1
        );
    }
}
