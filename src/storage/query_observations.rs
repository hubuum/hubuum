//! Bounded, value-free observations shared by all application storage adapters.
use chrono::{DateTime, Utc};
use hubuum_domain::ClassId;
use hubuum_query::{
    FilterField, Operator, ParsedQueryParam, QueryOptions, QueryScalarType,
    StructuredQueryExpression, infer_query_scalar_type,
};
use hubuum_storage_core::{
    StorageQueryObservationSettings, StorageQueryObservationSnapshot,
    StorageQueryPatternObservation, StorageQueryUsageOperation as UsageOperation,
    StorageQueryUsagePattern, StorageQueryUsageValueType as UsageType,
};
use std::{
    collections::{HashMap, HashSet},
    future::Future,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use super::runtime_settings::AppliedRuntimeSettings;

type PatternKey = (ClassId, StorageQueryUsagePattern);
struct Entry {
    observation: StorageQueryPatternObservation,
    observed_at: Instant,
}
struct State {
    entries: HashMap<PatternKey, Entry>,
    class_counts: HashMap<ClassId, usize>,
    drops: u64,
}
impl State {
    fn expire(&mut self, now: Instant, retention_seconds: u32) {
        let retention = Duration::from_secs(u64::from(retention_seconds));
        let expired = self
            .entries
            .iter()
            .filter(|(_, entry)| now.saturating_duration_since(entry.observed_at) >= retention)
            .map(|(key, _)| key.clone())
            .collect::<Vec<_>>();
        for key in expired {
            self.remove(&key, false);
        }
    }

    fn enforce_limits(&mut self, settings: StorageQueryObservationSettings) {
        while self.entries.len() > settings.max_patterns() {
            if !self.remove_oldest(None) {
                break;
            }
        }
        loop {
            let over_limit = self
                .class_counts
                .iter()
                .find(|(_, count)| **count > settings.max_patterns_per_class())
                .map(|(class_id, _)| *class_id);
            let Some(class_id) = over_limit else {
                break;
            };
            if !self.remove_oldest(Some(class_id)) {
                break;
            }
        }
    }

    fn remove_oldest(&mut self, class_id: Option<ClassId>) -> bool {
        let key = self
            .entries
            .iter()
            .filter(|((id, _), _)| class_id.is_none_or(|class_id| class_id == *id))
            .min_by_key(|(_, entry)| entry.observed_at)
            .map(|(key, _)| key.clone());
        key.is_some_and(|key| self.remove(&key, true))
    }

    fn remove(&mut self, key: &PatternKey, count_drop: bool) -> bool {
        if self.entries.remove(key).is_none() {
            return false;
        }
        if let Some(count) = self.class_counts.get_mut(&key.0) {
            *count -= 1;
            if *count == 0 {
                self.class_counts.remove(&key.0);
            }
        }
        if count_drop {
            self.drops = self.drops.saturating_add(1);
        }
        true
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

    /// Deployment settings from this process's startup configuration.
    pub(crate) const fn deployment_settings(&self) -> StorageQueryObservationSettings {
        self.settings
    }

    /// Effective settings after the shared policy and local ceilings apply.
    pub(crate) fn settings(
        &self,
        applied: &AppliedRuntimeSettings,
    ) -> StorageQueryObservationSettings {
        match applied
            .snapshot()
            .and_then(|snapshot| snapshot.query_observations())
        {
            Some(policy) => self.settings.with_runtime_policy(policy),
            None => self.settings,
        }
    }

    #[cfg(test)]
    pub(crate) fn collection_enabled(&self, applied: &AppliedRuntimeSettings) -> bool {
        !applied.refresh_failed() && self.settings(applied).enabled()
    }

    pub(crate) async fn observe<T, E>(
        self: &Arc<Self>,
        query: &QueryOptions,
        class_id: Option<ClassId>,
        settings: StorageQueryObservationSettings,
        operation: impl Future<Output = Result<T, E>>,
    ) -> Result<T, E> {
        let ticket = self.begin(query, class_id, settings);
        let result = operation.await;
        if result.is_ok()
            && let Some(ticket) = ticket
        {
            ticket.complete();
        }
        result
    }

    fn begin(
        self: &Arc<Self>,
        query: &QueryOptions,
        explicit_class: Option<ClassId>,
        settings: StorageQueryObservationSettings,
    ) -> Option<QueryObservationTicket> {
        if !self
            .requests
            .fetch_add(1, Ordering::Relaxed)
            .is_multiple_of(u64::from(settings.sample_every()))
        {
            return None;
        }
        let filters = direct_filters(query, settings.max_predicates_per_query())?;
        let class_id = match explicit_class {
            Some(id) => id,
            None => attributed_class(&filters)?,
        };
        let mut patterns = HashSet::new();
        for filter in filters
            .iter()
            .filter(|filter| *filter.field() == FilterField::JsonData)
        {
            let (operator, negated) = filter.operator().op_and_neg();
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
            let Some((path, value)) = filter.value().split_once('=') else {
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
            settings,
        })
    }

    pub(crate) fn snapshot_with_settings(
        &self,
        class_id: ClassId,
        settings: StorageQueryObservationSettings,
    ) -> StorageQueryObservationSnapshot {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.expire(Instant::now(), settings.retention_seconds());
        state.enforce_limits(settings);
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
        StorageQueryObservationSnapshot::new(settings, self.started_at, patterns, state.drops)
    }

    #[cfg(test)]
    fn snapshot(&self, class_id: ClassId) -> StorageQueryObservationSnapshot {
        self.snapshot_with_settings(class_id, self.settings)
    }
}

struct QueryObservationTicket {
    collector: Arc<QueryObservations>,
    class_id: ClassId,
    patterns: HashSet<StorageQueryUsagePattern>,
    started: Instant,
    settings: StorageQueryObservationSettings,
}
impl QueryObservationTicket {
    fn complete(self) {
        let elapsed = self.started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64;
        let instant = Instant::now();
        let now = Utc::now();
        let settings = self.settings;
        let mut state = self
            .collector
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.expire(instant, settings.retention_seconds());
        state.enforce_limits(settings);
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
                    observed_at: instant,
                },
            );
        }
    }
}

/// Only positive conjunctive fields describe direct predicates on this query's
/// objects. Never attribute related targets or fields beneath OR/NOT to them.
fn direct_filters(query: &QueryOptions, maximum: usize) -> Option<Vec<&ParsedQueryParam>> {
    if query.filters().len() > maximum {
        return None;
    }
    let mut filters = query.filters().iter().collect::<Vec<_>>();
    let mut pending = query.structured_filter().into_iter().collect::<Vec<_>>();
    let mut visited = 0;
    while let Some(expression) = pending.pop() {
        visited += 1;
        // Bound traversal as well as retained predicates, including deeply
        // nested conjunctions with few leaves. Exhaustion omits the observation.
        if visited > maximum * 2 {
            return None;
        }
        match expression {
            StructuredQueryExpression::Field { parameter, .. } => {
                if filters.len() == maximum {
                    return None;
                }
                filters.push(parameter);
            }
            StructuredQueryExpression::And(children) => {
                if pending.len() + children.len() > maximum * 2 {
                    return None;
                }
                pending.extend(children);
            }
            StructuredQueryExpression::Or(_)
            | StructuredQueryExpression::Not(_)
            | StructuredQueryExpression::Related(_) => {}
        }
    }
    Some(filters)
}

fn attributed_class(filters: &[&ParsedQueryParam]) -> Option<ClassId> {
    let mut class = None;
    for filter in filters
        .iter()
        .filter(|filter| matches!(*filter.field(), FilterField::ClassId | FilterField::Classes))
    {
        let (operator, negated) = filter.operator().op_and_neg();
        if negated || !matches!(operator, Operator::Equals | Operator::In) {
            return None;
        }
        let id = ClassId::new(filter.value().parse::<i32>().ok()?).ok()?;
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
    use hubuum_domain::ResourceRevision;
    use hubuum_query::{StructuredQueryField, parse_query_parameter};
    use hubuum_storage_core::{
        StorageError, StorageQueryObservationCeilings, StorageQueryObservationPolicy,
        StorageRuntimeSettingsAssignment, StorageRuntimeSettingsGroup,
        StorageRuntimeSettingsRecord, StorageRuntimeSettingsSnapshot, StorageRuntimeSettingsValue,
    };
    use rstest::rstest;

    async fn applied(
        result: Result<Option<bool>, StorageError>,
    ) -> super::super::runtime_settings::AppliedRuntimeSettings {
        let replica = super::super::runtime_settings::RuntimeSettingsReplica::new();
        let _ = replica
            .refresh(async {
                result.map(|enabled| {
                    let assignment = match enabled {
                        Some(enabled) => StorageRuntimeSettingsAssignment::Managed(
                            StorageRuntimeSettingsValue::QueryObservations(
                                StorageQueryObservationPolicy::new(enabled),
                            ),
                        ),
                        None => StorageRuntimeSettingsAssignment::Deployment(
                            StorageRuntimeSettingsGroup::QueryObservations,
                        ),
                    };
                    let now = Utc::now();
                    let mut records = StorageRuntimeSettingsGroup::ALL
                        .into_iter()
                        .map(|group| StorageRuntimeSettingsRecord::initial(group, now))
                        .collect::<Vec<_>>();
                    records.retain(|record| {
                        record.group() != StorageRuntimeSettingsGroup::QueryObservations
                    });
                    records.push(StorageRuntimeSettingsRecord::new(
                        assignment,
                        ResourceRevision::INITIAL,
                        now,
                        None,
                    ));
                    StorageRuntimeSettingsSnapshot::try_new(records).unwrap()
                })
            })
            .await;
        replica.current()
    }

    #[rstest::rstest]
    #[case::disabled(false, None, false)]
    #[case::startup(true, None, true)]
    #[case::enable(false, Some(true), true)]
    #[case::disable(true, Some(false), false)]
    #[tokio::test]
    async fn runtime_settings_override_startup(
        #[case] startup: bool,
        #[case] managed: Option<bool>,
        #[case] expected: bool,
    ) {
        let collector = QueryObservations::new(
            StorageQueryObservationSettings::try_new(startup, 1, 8, 8, 60, 16).unwrap(),
        );
        assert_eq!(
            collector.collection_enabled(&applied(Ok(managed)).await),
            expected
        );
    }

    #[tokio::test]
    async fn runtime_resource_limits_are_clamped_to_replica_ceilings() {
        let ceilings = StorageQueryObservationCeilings::try_new(4, 4, 2, 60, 8).unwrap();
        let deployment =
            StorageQueryObservationSettings::try_new_with_ceilings(true, 8, 4, 2, 60, 8, ceilings)
                .unwrap();
        let replica = super::super::runtime_settings::RuntimeSettingsReplica::new();
        let assignment = StorageRuntimeSettingsAssignment::Managed(
            StorageRuntimeSettingsValue::QueryObservations(
                StorageQueryObservationPolicy::try_with_resource_limits(true, 1, 16, 8, 3600, 32)
                    .unwrap(),
            ),
        );
        replica
            .refresh(async {
                let now = Utc::now();
                let mut records = StorageRuntimeSettingsGroup::ALL
                    .into_iter()
                    .map(|group| StorageRuntimeSettingsRecord::initial(group, now))
                    .collect::<Vec<_>>();
                records.retain(|record| {
                    record.group() != StorageRuntimeSettingsGroup::QueryObservations
                });
                records.push(StorageRuntimeSettingsRecord::new(
                    assignment,
                    ResourceRevision::new(2).unwrap(),
                    now,
                    None,
                ));
                Ok(StorageRuntimeSettingsSnapshot::try_new(records).unwrap())
            })
            .await
            .unwrap();
        let collector = QueryObservations::new(deployment);
        let settings = collector.settings(&replica.current());
        assert_eq!(
            (
                settings.sample_every(),
                settings.max_patterns(),
                settings.max_patterns_per_class(),
                settings.retention_seconds(),
                settings.max_predicates_per_query(),
            ),
            (4, 4, 2, 60, 8)
        );
    }

    #[test]
    fn lowering_capacity_trims_retained_patterns() {
        let collector = collector(8, 8, 1);
        for id in 1..=4 {
            record(&collector, &format!("class_id={id}&json_data=serial=value"));
        }
        let restricted = StorageQueryObservationSettings::try_new(true, 1, 2, 2, 60, 16).unwrap();
        let snapshot = collector.snapshot_with_settings(ClassId::new(1).unwrap(), restricted);
        assert!(snapshot.patterns().len() <= 2);
        assert!(collector.state.lock().unwrap().entries.len() <= 2);
    }

    #[test]
    fn unloaded_runtime_settings_apply_the_startup_setting() {
        let collector = collector(8, 8, 1);
        assert!(collector.collection_enabled(&Default::default()));
    }

    #[rstest]
    #[case::startup_enabled(true)]
    #[case::startup_disabled(false)]
    #[tokio::test]
    async fn failed_refresh_pauses_collection(#[case] startup: bool) {
        let collector = QueryObservations::new(
            StorageQueryObservationSettings::try_new(startup, 1, 8, 8, 60, 16).unwrap(),
        );
        let failed = applied(Err(StorageError::unavailable("database unavailable"))).await;
        assert!(!collector.collection_enabled(&failed));
    }

    fn collector(max: usize, per_class: usize, sample: u32) -> Arc<QueryObservations> {
        Arc::new(QueryObservations::new(
            StorageQueryObservationSettings::try_new(true, sample, max, per_class, 60, 16).unwrap(),
        ))
    }
    fn record(collector: &Arc<QueryObservations>, query: &str) {
        let options = parse_query_parameter(query).unwrap();
        if let Some(ticket) = collector.begin(&options, None, collector.settings) {
            ticket.complete();
        }
    }
    #[test]
    fn observed_structure_excludes_values_and_failed_queries() {
        let collector = collector(8, 8, 1);
        record(&collector, "class_id=1&json_data=serial=secret-alpha");
        record(&collector, "class_id=1&json_data=serial=secret-beta");
        let failed = parse_query_parameter("class_id=1&json_data=failed=secret-gamma").unwrap();
        drop(collector.begin(&failed, None, collector.settings));
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

    #[rstest]
    #[case::direct(std::convert::identity, 1)]
    #[case::conjunction(|field: StructuredQueryExpression| StructuredQueryExpression::And(vec![field.clone(), field]), 1)]
    #[case::disjunction(|field: StructuredQueryExpression| StructuredQueryExpression::Or(vec![field.clone(), field]), 0)]
    #[case::negated(|field| StructuredQueryExpression::Not(Box::new(field)), 0)]
    #[case::related(|field| match field { StructuredQueryExpression::Field { parameter, .. } => StructuredQueryExpression::Related(vec![parameter]), _ => unreachable!() }, 0)]
    #[case::predicate_budget(|field| StructuredQueryExpression::And(vec![field; 16]), 0)]
    #[case::traversal_budget(|field| (0..40).fold(field, |child, _| StructuredQueryExpression::And(vec![child])), 0)]
    #[case::pending_budget(|field| StructuredQueryExpression::And(vec![field; 40]), 0)]
    fn structured_observations_only_include_bounded_direct_predicates(
        #[case] expression: fn(StructuredQueryExpression) -> StructuredQueryExpression,
        #[case] expected_patterns: usize,
    ) {
        let collector = collector(8, 8, 1);
        let mut query = parse_query_parameter("class_id=1").unwrap();
        let predicate = parse_query_parameter("json_data=serial=secret").unwrap();
        query.set_structured_filter(Some(expression(StructuredQueryExpression::Field {
            field: StructuredQueryField::JsonData,
            parameter: predicate.filters()[0].clone(),
        })));
        if let Some(ticket) = collector.begin(&query, None, collector.settings) {
            ticket.complete();
        }
        assert_eq!(
            collector
                .snapshot(ClassId::new(1).unwrap())
                .patterns()
                .len(),
            expected_patterns
        );
    }
    #[tokio::test]
    async fn operation_duration_includes_work_after_candidate_fetching() {
        let collector = collector(8, 8, 1);
        let query = parse_query_parameter("class_id=1&json_data=serial=sample").unwrap();
        let authorization_delay = Duration::from_millis(20);
        collector
            .observe(&query, None, collector.settings, async {
                tokio::time::sleep(authorization_delay).await;
                Ok::<_, ()>(())
            })
            .await
            .unwrap();
        let snapshot = serde_json::to_value(collector.snapshot(ClassId::new(1).unwrap())).unwrap();
        assert!(
            snapshot["patterns"][0]["whole_query_duration_micros"]
                .as_u64()
                .unwrap()
                >= authorization_delay.as_micros() as u64
        );
    }

    #[tokio::test]
    async fn cancelled_operation_does_not_record_a_sample() {
        use futures::FutureExt;
        let collector = collector(8, 8, 1);
        let query = parse_query_parameter("class_id=1&json_data=serial=sample").unwrap();
        let operation = collector.observe(
            &query,
            None,
            collector.settings,
            std::future::pending::<Result<(), ()>>(),
        );
        assert!(operation.now_or_never().is_none());
        assert!(
            collector
                .snapshot(ClassId::new(1).unwrap())
                .patterns()
                .is_empty()
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
            .expire(Instant::now() + Duration::from_secs(61), 60);
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
