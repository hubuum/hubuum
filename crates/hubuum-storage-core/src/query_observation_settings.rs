//! Shared runtime policy and deployment bounds for query observations.
use crate::StorageValidationError;
use serde::Serialize;

/// Sampling and capacity values that have been validated together.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct QueryObservationLimits {
    sample_every: u32,
    max_patterns: usize,
    max_patterns_per_class: usize,
    retention_seconds: u32,
    max_predicates_per_query: usize,
}

impl QueryObservationLimits {
    fn try_new(
        sample_every: u32,
        max_patterns: usize,
        max_patterns_per_class: usize,
        retention_seconds: u32,
        max_predicates_per_query: usize,
    ) -> Result<Self, StorageValidationError> {
        if !(1..=1_000_000).contains(&sample_every)
            || !(1..=16_384).contains(&max_patterns)
            || !(1..=128).contains(&max_patterns_per_class)
            || max_patterns_per_class > max_patterns
            || !(1..=604_800).contains(&retention_seconds)
            || !(1..=32).contains(&max_predicates_per_query)
        {
            return Err(StorageValidationError::invalid(
                "Query observation limits exceed supported bounds",
            ));
        }
        Ok(Self {
            sample_every,
            max_patterns,
            max_patterns_per_class,
            retention_seconds,
            max_predicates_per_query,
        })
    }

    const fn within(self, ceilings: StorageQueryObservationCeilings) -> bool {
        self.sample_every >= ceilings.min_sample_every
            && self.max_patterns <= ceilings.max_patterns
            && self.max_patterns_per_class <= ceilings.max_patterns_per_class
            && self.retention_seconds <= ceilings.max_retention_seconds
            && self.max_predicates_per_query <= ceilings.max_predicates_per_query
    }

    const fn clamp(mut self, ceilings: StorageQueryObservationCeilings) -> Self {
        if self.sample_every < ceilings.min_sample_every {
            self.sample_every = ceilings.min_sample_every;
        }
        if self.max_patterns > ceilings.max_patterns {
            self.max_patterns = ceilings.max_patterns;
        }
        if self.max_patterns_per_class > ceilings.max_patterns_per_class {
            self.max_patterns_per_class = ceilings.max_patterns_per_class;
        }
        if self.retention_seconds > ceilings.max_retention_seconds {
            self.retention_seconds = ceilings.max_retention_seconds;
        }
        if self.max_predicates_per_query > ceilings.max_predicates_per_query {
            self.max_predicates_per_query = ceilings.max_predicates_per_query;
        }
        self
    }

    const fn settings(
        self,
        enabled: bool,
        ceilings: StorageQueryObservationCeilings,
    ) -> StorageQueryObservationSettings {
        StorageQueryObservationSettings {
            enabled,
            sample_every: self.sample_every,
            max_patterns: self.max_patterns,
            max_patterns_per_class: self.max_patterns_per_class,
            retention_seconds: self.retention_seconds,
            max_predicates_per_query: self.max_predicates_per_query,
            ceilings,
        }
    }
}

/// Shared enablement and optional sampling/capacity overrides for
/// process-local query observation collection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StorageQueryObservationPolicy {
    enabled: bool,
    limits: Option<QueryObservationLimits>,
}

impl StorageQueryObservationPolicy {
    #[must_use]
    pub const fn new(enabled: bool) -> Self {
        Self {
            enabled,
            limits: None,
        }
    }

    pub fn try_with_resource_limits(
        enabled: bool,
        sample_every: u32,
        max_patterns: usize,
        max_patterns_per_class: usize,
        retention_seconds: u32,
        max_predicates_per_query: usize,
    ) -> Result<Self, StorageValidationError> {
        let limits = QueryObservationLimits::try_new(
            sample_every,
            max_patterns,
            max_patterns_per_class,
            retention_seconds,
            max_predicates_per_query,
        )?;
        Ok(Self {
            enabled,
            limits: Some(limits),
        })
    }

    #[must_use]
    pub const fn enabled(self) -> bool {
        self.enabled
    }

    pub const fn sample_every(self) -> Option<u32> {
        match self.limits {
            Some(limits) => Some(limits.sample_every),
            None => None,
        }
    }

    pub const fn max_patterns(self) -> Option<usize> {
        match self.limits {
            Some(limits) => Some(limits.max_patterns),
            None => None,
        }
    }

    pub const fn max_patterns_per_class(self) -> Option<usize> {
        match self.limits {
            Some(limits) => Some(limits.max_patterns_per_class),
            None => None,
        }
    }

    pub const fn retention_seconds(self) -> Option<u32> {
        match self.limits {
            Some(limits) => Some(limits.retention_seconds),
            None => None,
        }
    }

    pub const fn max_predicates_per_query(self) -> Option<usize> {
        match self.limits {
            Some(limits) => Some(limits.max_predicates_per_query),
            None => None,
        }
    }
}

/// Deployment resource ceilings for runtime query-observation settings.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub struct StorageQueryObservationCeilings {
    min_sample_every: u32,
    max_patterns: usize,
    max_patterns_per_class: usize,
    max_retention_seconds: u32,
    max_predicates_per_query: usize,
}

impl StorageQueryObservationCeilings {
    pub fn try_new(
        min_sample_every: u32,
        max_patterns: usize,
        max_patterns_per_class: usize,
        max_retention_seconds: u32,
        max_predicates_per_query: usize,
    ) -> Result<Self, StorageValidationError> {
        let limits = QueryObservationLimits::try_new(
            min_sample_every,
            max_patterns,
            max_patterns_per_class,
            max_retention_seconds,
            max_predicates_per_query,
        )
        .map_err(|_| {
            StorageValidationError::invalid("Query observation ceilings exceed supported bounds")
        })?;
        Ok(Self {
            min_sample_every: limits.sample_every,
            max_patterns: limits.max_patterns,
            max_patterns_per_class: limits.max_patterns_per_class,
            max_retention_seconds: limits.retention_seconds,
            max_predicates_per_query: limits.max_predicates_per_query,
        })
    }

    pub const fn min_sample_every(self) -> u32 {
        self.min_sample_every
    }

    pub const fn max_patterns(self) -> usize {
        self.max_patterns
    }

    pub const fn max_patterns_per_class(self) -> usize {
        self.max_patterns_per_class
    }

    pub const fn max_retention_seconds(self) -> u32 {
        self.max_retention_seconds
    }

    pub const fn max_predicates_per_query(self) -> usize {
        self.max_predicates_per_query
    }
}

impl Default for StorageQueryObservationCeilings {
    fn default() -> Self {
        Self {
            min_sample_every: 1,
            max_patterns: 16_384,
            max_patterns_per_class: 128,
            max_retention_seconds: 604_800,
            max_predicates_per_query: 32,
        }
    }
}

/// Validated deployment defaults; class identifiers and paths are never metric labels.
#[derive(Clone, Copy, Debug, Serialize)]
pub struct StorageQueryObservationSettings {
    enabled: bool,
    sample_every: u32,
    max_patterns: usize,
    max_patterns_per_class: usize,
    retention_seconds: u32,
    max_predicates_per_query: usize,
    #[serde(skip_serializing)]
    ceilings: StorageQueryObservationCeilings,
}
impl Default for StorageQueryObservationSettings {
    fn default() -> Self {
        Self {
            enabled: false,
            sample_every: 16,
            max_patterns: 2048,
            max_patterns_per_class: 64,
            retention_seconds: 86400,
            max_predicates_per_query: 16,
            ceilings: StorageQueryObservationCeilings::default(),
        }
    }
}
impl StorageQueryObservationSettings {
    pub fn try_new(
        enabled: bool,
        sample_every: u32,
        max_patterns: usize,
        max_patterns_per_class: usize,
        retention_seconds: u32,
        max_predicates_per_query: usize,
    ) -> Result<Self, StorageValidationError> {
        Self::try_new_with_ceilings(
            enabled,
            sample_every,
            max_patterns,
            max_patterns_per_class,
            retention_seconds,
            max_predicates_per_query,
            StorageQueryObservationCeilings::default(),
        )
    }

    pub fn try_new_with_ceilings(
        enabled: bool,
        sample_every: u32,
        max_patterns: usize,
        max_patterns_per_class: usize,
        retention_seconds: u32,
        max_predicates_per_query: usize,
        ceilings: StorageQueryObservationCeilings,
    ) -> Result<Self, StorageValidationError> {
        let limits = QueryObservationLimits::try_new(
            sample_every,
            max_patterns,
            max_patterns_per_class,
            retention_seconds,
            max_predicates_per_query,
        )?;
        if !limits.within(ceilings) {
            return Err(StorageValidationError::invalid(
                "Query observation deployment settings exceed their configured ceilings",
            ));
        }
        Ok(limits.settings(enabled, ceilings))
    }

    pub const fn enabled(self) -> bool {
        self.enabled
    }
    /// Apply a shared runtime enablement while keeping deployment resource bounds.
    #[must_use]
    pub const fn with_enabled(self, enabled: bool) -> Self {
        Self { enabled, ..self }
    }
    pub const fn sample_every(self) -> u32 {
        self.sample_every
    }
    pub const fn max_patterns(self) -> usize {
        self.max_patterns
    }
    pub const fn max_patterns_per_class(self) -> usize {
        self.max_patterns_per_class
    }
    pub const fn retention_seconds(self) -> u32 {
        self.retention_seconds
    }
    pub const fn max_predicates_per_query(self) -> usize {
        self.max_predicates_per_query
    }

    pub const fn ceilings(self) -> StorageQueryObservationCeilings {
        self.ceilings
    }

    /// Apply a shared runtime policy, clamping resource values to this
    /// replica's deployment ceilings.
    #[must_use]
    pub const fn with_runtime_policy(self, policy: StorageQueryObservationPolicy) -> Self {
        match policy.limits {
            Some(limits) => limits
                .clamp(self.ceilings)
                .settings(policy.enabled, self.ceilings),
            None => self.with_enabled(policy.enabled),
        }
    }

    /// Whether a proposed runtime policy fits this replica's resource ceilings.
    pub const fn policy_within_ceilings(self, policy: StorageQueryObservationPolicy) -> bool {
        match policy.limits {
            Some(limits) => limits.within(self.ceilings),
            None => true,
        }
    }
}
