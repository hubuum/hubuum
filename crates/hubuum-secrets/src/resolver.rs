use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime};

use tokio::sync::{Mutex, OnceCell};

use crate::provider::ProviderSecret;
use crate::{
    ProviderHealth, ProviderHealthState, SecretError, SecretErrorKind, SecretProvider,
    SecretProviderKind, SecretRef, SecretValue, SecretVersion,
};

const DEFAULT_CACHE_CAPACITY: usize = 128;
const DEFAULT_CACHE_TTL: Duration = Duration::from_secs(300);
const DEFAULT_MAX_GROUP_SIZE: usize = 32;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StalePolicy {
    FailClosed,
    AllowFor(Duration),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CachePolicy {
    capacity: NonZeroUsize,
    max_total_bytes: NonZeroUsize,
    ttl: Duration,
    stale: StalePolicy,
    max_group_size: NonZeroUsize,
}

impl CachePolicy {
    pub fn new(capacity: NonZeroUsize, ttl: Duration) -> Self {
        let max_total_bytes = NonZeroUsize::new(
            capacity
                .get()
                .saturating_mul(crate::DEFAULT_MAX_SECRET_BYTES),
        )
        .expect("a positive cache capacity produces a positive byte limit");
        Self {
            capacity,
            max_total_bytes,
            ttl,
            stale: StalePolicy::FailClosed,
            max_group_size: NonZeroUsize::new(DEFAULT_MAX_GROUP_SIZE)
                .expect("the default maximum group size is non-zero"),
        }
    }

    pub fn stale_policy(mut self, stale: StalePolicy) -> Self {
        self.stale = stale;
        self
    }

    pub fn max_total_bytes(mut self, max_total_bytes: NonZeroUsize) -> Self {
        self.max_total_bytes = max_total_bytes;
        self
    }

    pub fn max_group_size(mut self, max_group_size: NonZeroUsize) -> Self {
        self.max_group_size = max_group_size;
        self
    }

    pub fn capacity(self) -> NonZeroUsize {
        self.capacity
    }

    pub fn ttl(self) -> Duration {
        self.ttl
    }

    pub fn total_byte_limit(self) -> NonZeroUsize {
        self.max_total_bytes
    }

    pub fn stale(self) -> StalePolicy {
        self.stale
    }

    pub fn group_size_limit(self) -> NonZeroUsize {
        self.max_group_size
    }
}

impl Default for CachePolicy {
    fn default() -> Self {
        Self::new(
            NonZeroUsize::new(DEFAULT_CACHE_CAPACITY)
                .expect("the default cache capacity is non-zero"),
            DEFAULT_CACHE_TTL,
        )
    }
}

#[derive(Clone)]
pub struct ResolvedSecret {
    value: Arc<SecretValue>,
    version: SecretVersion,
    generation: u64,
    loaded_at: Instant,
}

impl ResolvedSecret {
    pub fn value(&self) -> &SecretValue {
        &self.value
    }

    pub fn version(&self) -> &SecretVersion {
        &self.version
    }

    pub fn generation(&self) -> u64 {
        self.generation
    }

    pub fn age(&self) -> Duration {
        self.loaded_at.elapsed()
    }
}

impl fmt::Debug for ResolvedSecret {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ResolvedSecret")
            .field("value", &"<redacted>")
            .field("version", &self.version)
            .field("generation", &self.generation)
            .finish()
    }
}

pub struct ResolvedSecretGroup {
    values: Vec<ResolvedSecret>,
    generation: u64,
}

impl ResolvedSecretGroup {
    pub fn values(&self) -> &[ResolvedSecret] {
        &self.values
    }

    pub fn generation(&self) -> u64 {
        self.generation
    }
}

impl fmt::Debug for ResolvedSecretGroup {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ResolvedSecretGroup")
            .field("value_count", &self.values.len())
            .field("generation", &self.generation)
            .finish()
    }
}

#[derive(Debug, Clone)]
pub struct SecretResolverDiagnostics {
    providers: Vec<ProviderHealth>,
    cache_entries: usize,
    cache_capacity: usize,
    cached_bytes: usize,
    cache_byte_capacity: usize,
}

impl SecretResolverDiagnostics {
    pub fn providers(&self) -> &[ProviderHealth] {
        &self.providers
    }

    pub fn cache_entries(&self) -> usize {
        self.cache_entries
    }

    pub fn cache_capacity(&self) -> usize {
        self.cache_capacity
    }

    pub fn cached_bytes(&self) -> usize {
        self.cached_bytes
    }

    pub fn cache_byte_capacity(&self) -> usize {
        self.cache_byte_capacity
    }
}

#[derive(Default)]
pub struct SecretResolverBuilder {
    providers: HashMap<SecretProviderKind, Arc<dyn SecretProvider>>,
    cache_policy: CachePolicy,
}

impl SecretResolverBuilder {
    pub fn provider(
        mut self,
        provider: impl SecretProvider + 'static,
    ) -> Result<Self, SecretError> {
        let kind = *provider.kind();
        if self.providers.insert(kind, Arc::new(provider)).is_some() {
            return Err(SecretError::new(
                SecretErrorKind::InvalidProviderConfiguration,
                "a secret provider kind may only be configured once",
            ));
        }
        Ok(self)
    }

    pub fn shared_provider(
        mut self,
        provider: Arc<dyn SecretProvider>,
    ) -> Result<Self, SecretError> {
        let kind = *provider.kind();
        if self.providers.insert(kind, provider).is_some() {
            return Err(SecretError::new(
                SecretErrorKind::InvalidProviderConfiguration,
                "a secret provider kind may only be configured once",
            ));
        }
        Ok(self)
    }

    pub fn cache_policy(mut self, cache_policy: CachePolicy) -> Self {
        self.cache_policy = cache_policy;
        self
    }

    pub fn build(self) -> SecretResolver {
        let health = self
            .providers
            .keys()
            .cloned()
            .map(|kind| {
                (
                    kind,
                    ProviderHealth {
                        kind,
                        state: ProviderHealthState::Unknown,
                        last_success: None,
                        last_error_kind: None,
                    },
                )
            })
            .collect();
        SecretResolver {
            providers: self.providers,
            cache_policy: self.cache_policy,
            state: Mutex::new(ResolverState {
                cache: HashMap::new(),
                recency: VecDeque::new(),
                health,
            }),
            next_generation: AtomicU64::new(1),
        }
    }
}

pub struct SecretResolver {
    providers: HashMap<SecretProviderKind, Arc<dyn SecretProvider>>,
    cache_policy: CachePolicy,
    state: Mutex<ResolverState>,
    next_generation: AtomicU64,
}

struct ResolverState {
    cache: HashMap<SecretRef, CacheEntry>,
    recency: VecDeque<SecretRef>,
    health: HashMap<SecretProviderKind, ProviderHealth>,
}

struct CacheEntry {
    cell: Arc<OnceCell<ResolvedSecret>>,
    stale: Option<ResolvedSecret>,
    expires_at: Option<Instant>,
}

enum ResolutionPlan {
    Ready(ResolvedSecret),
    Load {
        cell: Arc<OnceCell<ResolvedSecret>>,
        stale: Option<ResolvedSecret>,
    },
}

impl SecretResolver {
    pub fn builder() -> SecretResolverBuilder {
        SecretResolverBuilder::default()
    }

    pub async fn resolve(&self, reference: &SecretRef) -> Result<ResolvedSecret, SecretError> {
        let provider = self.providers.get(reference.provider()).ok_or_else(|| {
            SecretError::new(
                SecretErrorKind::ProviderNotConfigured,
                "secret provider is not configured",
            )
        })?;
        let plan = self.resolution_plan(reference).await?;
        let ResolutionPlan::Load { cell, stale } = plan else {
            let ResolutionPlan::Ready(value) = plan else {
                unreachable!()
            };
            return Ok(value);
        };

        let result = cell
            .get_or_try_init(|| async {
                provider
                    .resolve(reference)
                    .await
                    .and_then(|secret| self.resolved_for_cache(secret, None))
            })
            .await
            .cloned();

        match result {
            Ok(value) => {
                self.finish_success(reference, &cell).await;
                Ok(value)
            }
            Err(error) => {
                self.finish_failure(reference, &cell, error.kind()).await;
                if self.stale_allowed(stale.as_ref()) {
                    return Ok(stale.expect("the stale policy checked this value"));
                }
                Err(error)
            }
        }
    }

    pub async fn resolve_group(
        &self,
        references: &[SecretRef],
    ) -> Result<ResolvedSecretGroup, SecretError> {
        if references.is_empty() {
            return Err(SecretError::new(
                SecretErrorKind::InvalidReference,
                "secret groups must contain at least one reference",
            ));
        }
        if references.len() > self.cache_policy.group_size_limit().get() {
            return Err(SecretError::new(
                SecretErrorKind::InvalidReference,
                "secret group exceeds the configured size limit",
            ));
        }
        let provider_kind = references[0].provider();
        if references
            .iter()
            .any(|reference| reference.provider() != provider_kind)
        {
            return Err(SecretError::new(
                SecretErrorKind::InvalidReference,
                "an atomic secret group must use one provider",
            ));
        }
        let provider = self.providers.get(provider_kind).ok_or_else(|| {
            SecretError::new(
                SecretErrorKind::ProviderNotConfigured,
                "secret provider is not configured",
            )
        })?;
        match provider.resolve_group(references).await {
            Ok(values) if values.len() == references.len() => {
                let total_bytes = values
                    .iter()
                    .try_fold(0usize, |total, value| {
                        total.checked_add(value.value().len()).ok_or(())
                    })
                    .unwrap_or(usize::MAX);
                if total_bytes > self.cache_policy.total_byte_limit().get() {
                    let error = SecretError::new(
                        SecretErrorKind::TooLarge,
                        "resolved secret group exceeds the configured byte limit",
                    );
                    self.record_failure(provider_kind, error.kind()).await;
                    return Err(error);
                }
                self.record_success(provider_kind).await;
                let generation = self.next_generation.fetch_add(1, Ordering::Relaxed);
                let loaded_at = Instant::now();
                Ok(ResolvedSecretGroup {
                    values: values
                        .into_iter()
                        .map(|value| self.resolved(value, Some((generation, loaded_at))))
                        .collect(),
                    generation,
                })
            }
            Ok(_) => {
                let error = SecretError::new(
                    SecretErrorKind::Internal,
                    "secret provider returned an incomplete group",
                );
                self.record_failure(provider_kind, error.kind()).await;
                Err(error)
            }
            Err(error) => {
                self.record_failure(provider_kind, error.kind()).await;
                Err(error)
            }
        }
    }

    pub async fn refresh(&self, reference: &SecretRef) -> Result<ResolvedSecret, SecretError> {
        self.invalidate(reference).await;
        self.resolve(reference).await
    }

    pub async fn invalidate(&self, reference: &SecretRef) {
        let mut state = self.state.lock().await;
        state.cache.remove(reference);
        state.remove_recency(reference);
    }

    pub async fn invalidate_provider(&self, provider: &SecretProviderKind) {
        let mut state = self.state.lock().await;
        state
            .cache
            .retain(|reference, _| reference.provider() != provider);
        state
            .recency
            .retain(|reference| reference.provider() != provider);
    }

    pub async fn diagnostics(&self) -> SecretResolverDiagnostics {
        let state = self.state.lock().await;
        let mut providers = state.health.values().cloned().collect::<Vec<_>>();
        providers.sort_by(|left, right| left.kind().cmp(right.kind()));
        SecretResolverDiagnostics {
            providers,
            cache_entries: state.cache.len(),
            cache_capacity: self.cache_policy.capacity().get(),
            cached_bytes: state.cached_bytes(),
            cache_byte_capacity: self.cache_policy.total_byte_limit().get(),
        }
    }

    async fn resolution_plan(&self, reference: &SecretRef) -> Result<ResolutionPlan, SecretError> {
        let mut state = self.state.lock().await;
        state.touch(reference);
        if let Some(entry) = state.cache.get_mut(reference) {
            if let Some(value) = entry.cell.get().cloned() {
                if entry
                    .expires_at
                    .is_some_and(|expires_at| expires_at > Instant::now())
                {
                    return Ok(ResolutionPlan::Ready(value));
                }
                entry.stale = Some(value);
                entry.cell = Arc::new(OnceCell::new());
                entry.expires_at = None;
            }
            return Ok(ResolutionPlan::Load {
                cell: Arc::clone(&entry.cell),
                stale: entry.stale.clone(),
            });
        }

        let capacity = self.cache_policy.capacity().get();
        state.evict_to(
            capacity.saturating_sub(1),
            self.cache_policy.total_byte_limit().get(),
        );
        if state.cache.len() >= capacity {
            state.remove_recency(reference);
            return Err(SecretError::new(
                SecretErrorKind::Unavailable,
                "secret resolver capacity is temporarily exhausted",
            ));
        }
        let cell = Arc::new(OnceCell::new());
        state.cache.insert(
            reference.clone(),
            CacheEntry {
                cell: Arc::clone(&cell),
                stale: None,
                expires_at: None,
            },
        );
        Ok(ResolutionPlan::Load { cell, stale: None })
    }

    fn resolved(
        &self,
        secret: ProviderSecret,
        generation_and_time: Option<(u64, Instant)>,
    ) -> ResolvedSecret {
        let (generation, loaded_at) = generation_and_time.unwrap_or_else(|| {
            (
                self.next_generation.fetch_add(1, Ordering::Relaxed),
                Instant::now(),
            )
        });
        ResolvedSecret {
            value: secret.value,
            version: secret.version,
            generation,
            loaded_at,
        }
    }

    fn resolved_for_cache(
        &self,
        secret: ProviderSecret,
        generation_and_time: Option<(u64, Instant)>,
    ) -> Result<ResolvedSecret, SecretError> {
        if secret.value().len() > self.cache_policy.total_byte_limit().get() {
            return Err(SecretError::new(
                SecretErrorKind::TooLarge,
                "resolved secret exceeds the configured cache byte limit",
            ));
        }
        Ok(self.resolved(secret, generation_and_time))
    }

    async fn finish_success(&self, reference: &SecretRef, cell: &Arc<OnceCell<ResolvedSecret>>) {
        let mut state = self.state.lock().await;
        if let Some(entry) = state.cache.get_mut(reference)
            && Arc::ptr_eq(&entry.cell, cell)
        {
            entry.expires_at = Some(Instant::now() + self.cache_policy.ttl());
            entry.stale = None;
        }
        state.record_success(reference.provider());
        state.evict_to(
            self.cache_policy.capacity().get(),
            self.cache_policy.total_byte_limit().get(),
        );
    }

    async fn finish_failure(
        &self,
        reference: &SecretRef,
        cell: &Arc<OnceCell<ResolvedSecret>>,
        error_kind: SecretErrorKind,
    ) {
        let mut state = self.state.lock().await;
        let remove = state.cache.get(reference).is_some_and(|entry| {
            Arc::ptr_eq(&entry.cell, cell) && entry.cell.get().is_none() && entry.stale.is_none()
        });
        if remove {
            state.cache.remove(reference);
            state.remove_recency(reference);
        }
        state.record_failure(reference.provider(), error_kind);
    }

    fn stale_allowed(&self, stale: Option<&ResolvedSecret>) -> bool {
        match (self.cache_policy.stale(), stale) {
            (StalePolicy::AllowFor(max_stale), Some(stale)) => {
                stale.age() <= self.cache_policy.ttl().saturating_add(max_stale)
            }
            _ => false,
        }
    }

    async fn record_success(&self, provider: &SecretProviderKind) {
        self.state.lock().await.record_success(provider);
    }

    async fn record_failure(&self, provider: &SecretProviderKind, error_kind: SecretErrorKind) {
        self.state.lock().await.record_failure(provider, error_kind);
    }
}

impl fmt::Debug for SecretResolver {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SecretResolver")
            .field("provider_count", &self.providers.len())
            .field("cache_policy", &self.cache_policy)
            .finish()
    }
}

impl ResolverState {
    fn touch(&mut self, reference: &SecretRef) {
        self.remove_recency(reference);
        self.recency.push_back(reference.clone());
    }

    fn remove_recency(&mut self, reference: &SecretRef) {
        if let Some(index) = self
            .recency
            .iter()
            .position(|candidate| candidate == reference)
        {
            self.recency.remove(index);
        }
    }

    fn cached_bytes(&self) -> usize {
        self.cache
            .values()
            .filter_map(|entry| entry.cell.get())
            .fold(0usize, |total, value| {
                total.saturating_add(value.value.len())
            })
    }

    fn evict_to(&mut self, capacity: usize, byte_capacity: usize) {
        let mut candidates = self.recency.len();
        while (self.cache.len() > capacity || self.cached_bytes() > byte_capacity) && candidates > 0
        {
            candidates -= 1;
            let Some(reference) = self.recency.pop_front() else {
                return;
            };
            let idle = self
                .cache
                .get(&reference)
                .is_some_and(|entry| Arc::strong_count(&entry.cell) == 1);
            if idle {
                self.cache.remove(&reference);
            } else {
                self.recency.push_back(reference);
            }
        }
    }

    fn record_success(&mut self, provider: &SecretProviderKind) {
        if let Some(health) = self.health.get_mut(provider) {
            health.state = ProviderHealthState::Healthy;
            health.last_success = Some(SystemTime::now());
            health.last_error_kind = None;
        }
    }

    fn record_failure(&mut self, provider: &SecretProviderKind, error_kind: SecretErrorKind) {
        if let Some(health) = self.health.get_mut(provider) {
            health.state = if health.last_success.is_some() {
                ProviderHealthState::Degraded
            } else {
                ProviderHealthState::Unavailable
            };
            health.last_error_kind = Some(error_kind);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicBool;

    use async_trait::async_trait;
    use tokio::sync::Notify;

    use super::*;
    use crate::{ProviderSecret, SecretName, SecretVersion};

    struct TestProvider {
        kind: SecretProviderKind,
        calls: AtomicU64,
        fail: AtomicBool,
        gate: Option<Arc<Notify>>,
    }

    impl fmt::Debug for TestProvider {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("TestProvider")
        }
    }

    #[async_trait]
    impl SecretProvider for TestProvider {
        fn kind(&self) -> &SecretProviderKind {
            &self.kind
        }

        async fn resolve(&self, reference: &SecretRef) -> Result<ProviderSecret, SecretError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if let Some(gate) = &self.gate {
                gate.notified().await;
            }
            if self.fail.load(Ordering::SeqCst) {
                return Err(SecretError::new(
                    SecretErrorKind::Unavailable,
                    "test provider unavailable",
                ));
            }
            ProviderSecret::new(
                SecretValue::new(reference.name().as_str().as_bytes().to_vec())?,
                SecretVersion::new("version-1")?,
            )
            .pipe(Ok)
        }

        async fn resolve_group(
            &self,
            references: &[SecretRef],
        ) -> Result<Vec<ProviderSecret>, SecretError> {
            let mut values = Vec::with_capacity(references.len());
            for reference in references {
                values.push(self.resolve(reference).await?);
            }
            Ok(values)
        }
    }

    trait Pipe: Sized {
        fn pipe<T>(self, apply: impl FnOnce(Self) -> T) -> T {
            apply(self)
        }
    }

    impl<T> Pipe for T {}

    fn reference(name: &str) -> SecretRef {
        SecretRef::new(
            SecretProviderKind::environment(),
            SecretName::new(name).unwrap(),
        )
    }

    #[tokio::test]
    async fn concurrent_resolves_use_one_provider_call() {
        let provider = Arc::new(TestProvider {
            kind: SecretProviderKind::environment(),
            calls: AtomicU64::new(0),
            fail: AtomicBool::new(false),
            gate: None,
        });
        let resolver = Arc::new(
            SecretResolver::builder()
                .shared_provider(provider.clone())
                .unwrap()
                .build(),
        );
        let reference = reference("shared");

        let mut tasks = Vec::new();
        for _ in 0..16 {
            let resolver = Arc::clone(&resolver);
            let reference = reference.clone();
            tasks.push(tokio::spawn(
                async move { resolver.resolve(&reference).await },
            ));
        }
        let mut values = Vec::new();
        for task in tasks {
            values.push(task.await.unwrap());
        }

        assert!(values.into_iter().all(|value| value.is_ok()));
        assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn cache_is_bounded_after_resolutions_complete() {
        let provider = Arc::new(TestProvider {
            kind: SecretProviderKind::environment(),
            calls: AtomicU64::new(0),
            fail: AtomicBool::new(false),
            gate: None,
        });
        let resolver = SecretResolver::builder()
            .shared_provider(provider)
            .unwrap()
            .cache_policy(CachePolicy::new(
                NonZeroUsize::new(2).unwrap(),
                Duration::from_secs(60),
            ))
            .build();

        for name in ["one", "two", "three", "four"] {
            resolver.resolve(&reference(name)).await.unwrap();
        }

        assert_eq!(resolver.diagnostics().await.cache_entries(), 2);
    }

    #[tokio::test]
    async fn cache_evicts_entries_to_stay_within_its_total_byte_limit() {
        let provider = Arc::new(TestProvider {
            kind: SecretProviderKind::environment(),
            calls: AtomicU64::new(0),
            fail: AtomicBool::new(false),
            gate: None,
        });
        let resolver = SecretResolver::builder()
            .shared_provider(provider)
            .unwrap()
            .cache_policy(
                CachePolicy::new(NonZeroUsize::new(10).unwrap(), Duration::from_secs(60))
                    .max_total_bytes(NonZeroUsize::new(6).unwrap()),
            )
            .build();

        for name in ["one", "two", "three"] {
            resolver.resolve(&reference(name)).await.unwrap();
        }

        let diagnostics = resolver.diagnostics().await;
        assert_eq!(diagnostics.cache_entries(), 1);
        assert_eq!(diagnostics.cached_bytes(), 5);
        assert_eq!(diagnostics.cache_byte_capacity(), 6);
    }

    #[tokio::test]
    async fn cache_rejects_one_value_larger_than_its_total_byte_limit() {
        let provider = Arc::new(TestProvider {
            kind: SecretProviderKind::environment(),
            calls: AtomicU64::new(0),
            fail: AtomicBool::new(false),
            gate: None,
        });
        let resolver = SecretResolver::builder()
            .shared_provider(provider)
            .unwrap()
            .cache_policy(
                CachePolicy::new(NonZeroUsize::new(2).unwrap(), Duration::from_secs(60))
                    .max_total_bytes(NonZeroUsize::new(4).unwrap()),
            )
            .build();

        assert_eq!(
            resolver
                .resolve(&reference("oversized"))
                .await
                .unwrap_err()
                .kind(),
            SecretErrorKind::TooLarge
        );
        assert_eq!(resolver.diagnostics().await.cache_entries(), 0);
    }

    #[tokio::test]
    async fn in_flight_resolutions_cannot_exceed_cache_capacity() {
        let gate = Arc::new(Notify::new());
        let provider = Arc::new(TestProvider {
            kind: SecretProviderKind::environment(),
            calls: AtomicU64::new(0),
            fail: AtomicBool::new(false),
            gate: Some(Arc::clone(&gate)),
        });
        let resolver = Arc::new(
            SecretResolver::builder()
                .shared_provider(provider.clone())
                .unwrap()
                .cache_policy(CachePolicy::new(
                    NonZeroUsize::new(2).unwrap(),
                    Duration::from_secs(60),
                ))
                .build(),
        );
        let mut tasks = Vec::new();
        for name in ["one", "two"] {
            let resolver = Arc::clone(&resolver);
            let reference = reference(name);
            tasks.push(tokio::spawn(
                async move { resolver.resolve(&reference).await },
            ));
        }
        while provider.calls.load(Ordering::SeqCst) < 2 {
            tokio::task::yield_now().await;
        }

        assert_eq!(
            resolver
                .resolve(&reference("three"))
                .await
                .unwrap_err()
                .kind(),
            SecretErrorKind::Unavailable
        );
        assert_eq!(resolver.diagnostics().await.cache_entries(), 2);
        assert_eq!(provider.calls.load(Ordering::SeqCst), 2);

        gate.notify_waiters();
        for task in tasks {
            task.await.unwrap().unwrap();
        }
    }

    #[tokio::test]
    async fn cancelled_initialization_can_be_retried() {
        let gate = Arc::new(Notify::new());
        let provider = Arc::new(TestProvider {
            kind: SecretProviderKind::environment(),
            calls: AtomicU64::new(0),
            fail: AtomicBool::new(false),
            gate: Some(Arc::clone(&gate)),
        });
        let resolver = Arc::new(
            SecretResolver::builder()
                .shared_provider(provider.clone())
                .unwrap()
                .build(),
        );
        let reference = reference("cancelled");
        let task = tokio::spawn({
            let resolver = Arc::clone(&resolver);
            let reference = reference.clone();
            async move { resolver.resolve(&reference).await }
        });
        while provider.calls.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
        task.abort();
        let _ = task.await;

        let retry = tokio::spawn({
            let resolver = Arc::clone(&resolver);
            async move { resolver.resolve(&reference).await }
        });
        while provider.calls.load(Ordering::SeqCst) < 2 {
            tokio::task::yield_now().await;
        }
        gate.notify_waiters();

        assert_eq!(retry.await.unwrap().unwrap().value().expose(), b"cancelled");
    }

    #[tokio::test]
    async fn fail_closed_does_not_return_expired_values() {
        let provider = Arc::new(TestProvider {
            kind: SecretProviderKind::environment(),
            calls: AtomicU64::new(0),
            fail: AtomicBool::new(false),
            gate: None,
        });
        let resolver = SecretResolver::builder()
            .shared_provider(provider.clone())
            .unwrap()
            .cache_policy(CachePolicy::new(
                NonZeroUsize::new(2).unwrap(),
                Duration::ZERO,
            ))
            .build();
        let reference = reference("outage");
        resolver.resolve(&reference).await.unwrap();
        provider.fail.store(true, Ordering::SeqCst);

        assert_eq!(
            resolver.resolve(&reference).await.unwrap_err().kind(),
            SecretErrorKind::Unavailable
        );
    }

    #[tokio::test]
    async fn failed_resolutions_are_not_cached_and_provider_recovery_is_observed() {
        let provider = Arc::new(TestProvider {
            kind: SecretProviderKind::environment(),
            calls: AtomicU64::new(0),
            fail: AtomicBool::new(true),
            gate: None,
        });
        let resolver = SecretResolver::builder()
            .shared_provider(provider.clone())
            .unwrap()
            .build();
        let reference = reference("recovery");

        assert_eq!(
            resolver.resolve(&reference).await.unwrap_err().kind(),
            SecretErrorKind::Unavailable
        );
        provider.fail.store(false, Ordering::SeqCst);
        assert_eq!(
            resolver.resolve(&reference).await.unwrap().value().expose(),
            b"recovery"
        );
        assert_eq!(provider.calls.load(Ordering::SeqCst), 2);
        assert_eq!(
            resolver.diagnostics().await.providers()[0].state(),
            ProviderHealthState::Healthy
        );
    }

    #[tokio::test]
    async fn explicit_stale_policy_survives_repeated_provider_failures() {
        let provider = Arc::new(TestProvider {
            kind: SecretProviderKind::environment(),
            calls: AtomicU64::new(0),
            fail: AtomicBool::new(false),
            gate: None,
        });
        let resolver = SecretResolver::builder()
            .shared_provider(provider.clone())
            .unwrap()
            .cache_policy(
                CachePolicy::new(NonZeroUsize::new(2).unwrap(), Duration::ZERO)
                    .stale_policy(StalePolicy::AllowFor(Duration::from_secs(60))),
            )
            .build();
        let reference = reference("outage");
        resolver.resolve(&reference).await.unwrap();
        provider.fail.store(true, Ordering::SeqCst);

        for _ in 0..2 {
            assert_eq!(
                resolver.resolve(&reference).await.unwrap().value().expose(),
                b"outage"
            );
        }
        assert_eq!(
            resolver.diagnostics().await.providers()[0].state(),
            ProviderHealthState::Degraded
        );
    }

    #[tokio::test]
    async fn groups_share_one_generation_and_reject_mixed_providers() {
        let provider = TestProvider {
            kind: SecretProviderKind::environment(),
            calls: AtomicU64::new(0),
            fail: AtomicBool::new(false),
            gate: None,
        };
        let resolver = SecretResolver::builder()
            .provider(provider)
            .unwrap()
            .build();
        let references = [reference("first"), reference("second")];

        let group = resolver.resolve_group(&references).await.unwrap();

        assert_eq!(group.values().len(), 2);
        assert!(
            group
                .values()
                .iter()
                .all(|value| value.generation() == group.generation())
        );
        let mixed = [
            reference("first"),
            SecretRef::new(
                SecretProviderKind::file(),
                SecretName::new("second").unwrap(),
            ),
        ];
        assert_eq!(
            resolver.resolve_group(&mixed).await.unwrap_err().kind(),
            SecretErrorKind::InvalidReference
        );
    }

    #[tokio::test]
    async fn diagnostics_never_include_secret_names_or_values() {
        let provider = TestProvider {
            kind: SecretProviderKind::environment(),
            calls: AtomicU64::new(0),
            fail: AtomicBool::new(false),
            gate: None,
        };
        let resolver = SecretResolver::builder()
            .provider(provider)
            .unwrap()
            .build();
        resolver.resolve(&reference("canary-secret")).await.unwrap();

        let debug = format!("{:?}", resolver.diagnostics().await);
        assert!(!debug.contains("canary-secret"));
        assert!(!debug.contains("test provider unavailable"));
    }
}
