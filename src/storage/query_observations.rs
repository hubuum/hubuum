//! Bounded, value-free observations shared by all application storage adapters.
use chrono::{DateTime, Utc};
use hubuum_domain::ClassId;
use hubuum_query::{
    FilterField, Operator, ParsedQueryParam, QueryOptions, QueryScalarType,
    StructuredQueryExpression, infer_query_scalar_type,
};
use hubuum_storage_core::{
    StorageError, StorageQueryObservationControl, StorageQueryObservationSettings,
    StorageQueryObservationSnapshot, StorageQueryPatternObservation,
    StorageQueryUsageOperation as UsageOperation, StorageQueryUsagePattern,
    StorageQueryUsageValueType as UsageType,
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

type PatternKey = (ClassId, StorageQueryUsagePattern);
const CONTROL_REFRESH_INTERVAL: Duration = Duration::from_secs(1);
const CONTROL_READ_TIMEOUT: Duration = Duration::from_millis(250);
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
    control: tokio::sync::Mutex<Option<(Instant, StorageQueryObservationControl)>>,
}
impl QueryObservations {
    pub(crate) fn new(settings: StorageQueryObservationSettings) -> Self {
        Self {
            settings,
            started_at: Utc::now(),
            requests: AtomicU64::new(0),
            control: tokio::sync::Mutex::new(None),
            state: Mutex::new(State {
                entries: HashMap::new(),
                class_counts: HashMap::new(),
                drops: 0,
            }),
        }
    }

    pub(crate) fn settings(
        &self,
        control: StorageQueryObservationControl,
    ) -> StorageQueryObservationSettings {
        control.resolve(self.settings)
    }

    pub(crate) async fn invalidate_control(&self) {
        *self.control.lock().await = None;
    }

    /// Refresh even while disabled. A failed or slow read pauses collection;
    /// it must neither keep a stale enablement nor fail the application query.
    pub(crate) async fn collection_enabled(
        &self,
        load: impl Future<Output = Result<StorageQueryObservationControl, StorageError>>,
    ) -> bool {
        let control = self
            .load_control(load, false)
            .await
            .unwrap_or(StorageQueryObservationControl::Disabled);
        self.settings(control).enabled()
    }

    pub(crate) async fn refresh_control(
        &self,
        load: impl Future<Output = Result<StorageQueryObservationControl, StorageError>>,
    ) -> Result<StorageQueryObservationControl, StorageError> {
        self.load_control(load, true).await
    }

    async fn load_control(
        &self,
        load: impl Future<Output = Result<StorageQueryObservationControl, StorageError>>,
        force: bool,
    ) -> Result<StorageQueryObservationControl, StorageError> {
        // Serialize loads so an older refresh cannot replace a newer result.
        // Include queuing in the read budget so refreshes cannot stall queries.
        let deadline = tokio::time::Instant::now() + CONTROL_READ_TIMEOUT;
        let timeout_error =
            || StorageError::internal("Query observation control refresh timed out");
        let mut cache = tokio::time::timeout_at(deadline, self.control.lock())
            .await
            .map_err(|_| timeout_error())?;
        let now = Instant::now();
        if !force
            && let Some((expires_at, control)) = *cache
            && now < expires_at
        {
            return Ok(control);
        }
        let result = tokio::time::timeout_at(deadline, load)
            .await
            .unwrap_or_else(|_| Err(timeout_error()));
        let control = match &result {
            Ok(control) => *control,
            Err(error) => {
                tracing::warn!(%error, "Query observation control unavailable; collection paused");
                StorageQueryObservationControl::Disabled
            }
        };
        *cache = Some((now + CONTROL_REFRESH_INTERVAL, control));
        result
    }

    pub(crate) async fn observe<T, E>(
        self: &Arc<Self>,
        query: &QueryOptions,
        class_id: Option<ClassId>,
        operation: impl Future<Output = Result<T, E>>,
    ) -> Result<T, E> {
        let ticket = self.begin(query, class_id);
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
    ) -> Option<QueryObservationTicket> {
        if !self
            .requests
            .fetch_add(1, Ordering::Relaxed)
            .is_multiple_of(u64::from(self.settings.sample_every()))
        {
            return None;
        }
        let filters = direct_filters(query, self.settings.max_predicates_per_query())?;
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
        })
    }

    pub(crate) fn snapshot_with_control(
        &self,
        class_id: ClassId,
        control: StorageQueryObservationControl,
    ) -> StorageQueryObservationSnapshot {
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
        StorageQueryObservationSnapshot::new(
            self.settings(control),
            self.started_at,
            patterns,
            state.drops,
        )
    }

    #[cfg(test)]
    fn snapshot(&self, class_id: ClassId) -> StorageQueryObservationSnapshot {
        self.snapshot_with_control(class_id, StorageQueryObservationControl::Startup)
    }
}

struct QueryObservationTicket {
    collector: Arc<QueryObservations>,
    class_id: ClassId,
    patterns: HashSet<StorageQueryUsagePattern>,
    started: Instant,
}
impl QueryObservationTicket {
    fn complete(self) {
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
    use hubuum_query::{StructuredQueryField, parse_query_parameter};
    use rstest::rstest;

    #[rstest::rstest]
    #[case::disabled(false, StorageQueryObservationControl::Startup, false)]
    #[case::startup(true, StorageQueryObservationControl::Startup, true)]
    #[case::enable(false, StorageQueryObservationControl::Enabled, true)]
    #[case::disable(true, StorageQueryObservationControl::Disabled, false)]
    #[tokio::test]
    async fn runtime_control_overrides_startup(
        #[case] startup: bool,
        #[case] control: StorageQueryObservationControl,
        #[case] expected: bool,
    ) {
        let collector = QueryObservations::new(
            StorageQueryObservationSettings::try_new(startup, 1, 8, 8, 60, 16).unwrap(),
        );
        assert_eq!(
            collector.collection_enabled(async { Ok(control) }).await,
            expected
        );
    }

    #[rstest]
    #[case::enable(StorageQueryObservationControl::Enabled, true)]
    #[case::disable(StorageQueryObservationControl::Disabled, false)]
    #[tokio::test]
    async fn administrative_refresh_applies_to_the_live_collector(
        #[case] control: StorageQueryObservationControl,
        #[case] enabled: bool,
    ) {
        let collector = collector(8, 8, 1);
        collector
            .collection_enabled(async {
                Ok(StorageQueryObservationControl::from_override(Some(
                    !enabled,
                )))
            })
            .await;
        assert_eq!(
            collector
                .refresh_control(async { Ok(control) })
                .await
                .unwrap(),
            control
        );
        assert_eq!(
            collector
                .collection_enabled(async {
                    panic!("administrative refresh must replace the cached recording state")
                })
                .await,
            enabled
        );
    }

    #[tokio::test]
    async fn runtime_control_is_cached_between_queries() {
        let collector = collector(8, 8, 1);
        assert!(
            collector
                .collection_enabled(async { Ok(StorageQueryObservationControl::Enabled) })
                .await
        );
        assert!(
            collector
                .collection_enabled(async { panic!("cache should avoid another storage read") })
                .await
        );
    }

    #[rstest::rstest]
    #[case::enabled(StorageQueryObservationControl::Enabled, true)]
    #[case::disabled(StorageQueryObservationControl::Disabled, false)]
    #[tokio::test]
    async fn expired_control_refreshes_even_when_disabled(
        #[case] next: StorageQueryObservationControl,
        #[case] expected: bool,
    ) {
        let collector = collector(8, 8, 1);
        *collector.control.lock().await = Some((
            Instant::now() - Duration::from_secs(1),
            StorageQueryObservationControl::from_override(Some(!expected)),
        ));
        assert_eq!(
            collector.collection_enabled(async { Ok(next) }).await,
            expected
        );
    }

    #[tokio::test]
    async fn failed_control_read_does_not_reuse_stale_enablement() {
        let collector = collector(8, 8, 1);
        *collector.control.lock().await = Some((
            Instant::now() - Duration::from_secs(1),
            StorageQueryObservationControl::Enabled,
        ));
        assert!(
            !collector
                .collection_enabled(async { Err(StorageError::internal("unavailable")) })
                .await
        );
    }

    #[tokio::test]
    async fn stalled_control_read_pauses_collection() {
        let collector = collector(8, 8, 1);
        assert!(!collector.collection_enabled(std::future::pending()).await);
    }

    #[rstest]
    #[case::enabled(StorageQueryObservationControl::Enabled)]
    #[case::disabled(StorageQueryObservationControl::Disabled)]
    #[tokio::test]
    async fn queued_refreshes_cannot_extend_the_query_control_timeout(
        #[case] control: StorageQueryObservationControl,
    ) {
        let collector = collector(8, 8, 1);
        collector
            .refresh_control(async { Ok(control) })
            .await
            .unwrap();

        let mut refreshes = Box::pin(futures::future::join_all(
            (0..8).map(|_| collector.refresh_control(std::future::pending())),
        ));
        // Poll once to hold the lock and queue every refresh before the query.
        assert!(futures::poll!(&mut refreshes).is_pending());

        let enabled = tokio::time::timeout(CONTROL_READ_TIMEOUT * 2, async {
            let (_, enabled) = tokio::join!(
                refreshes,
                collector.collection_enabled(async {
                    panic!("a timed-out query must skip collection without another control read")
                })
            );
            enabled
        })
        .await
        .expect("queued refreshes must share the query's bounded wait budget");
        assert!(!enabled);
    }
    fn collector(max: usize, per_class: usize, sample: u32) -> Arc<QueryObservations> {
        Arc::new(QueryObservations::new(
            StorageQueryObservationSettings::try_new(true, sample, max, per_class, 60, 16).unwrap(),
        ))
    }
    fn record(collector: &Arc<QueryObservations>, query: &str) {
        let options = parse_query_parameter(query).unwrap();
        if let Some(ticket) = collector.begin(&options, None) {
            ticket.complete();
        }
    }
    #[test]
    fn observed_structure_excludes_values_and_failed_queries() {
        let collector = collector(8, 8, 1);
        record(&collector, "class_id=1&json_data=serial=secret-alpha");
        record(&collector, "class_id=1&json_data=serial=secret-beta");
        let failed = parse_query_parameter("class_id=1&json_data=failed=secret-gamma").unwrap();
        drop(collector.begin(&failed, None));
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
        if let Some(ticket) = collector.begin(&query, None) {
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
            .observe(&query, None, async {
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
        let operation = collector.observe(&query, None, std::future::pending::<Result<(), ()>>());
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
