//! Optional, read-only analysis of class query usage.
use crate::{
    StorageError, StorageQueryUsageDeclaration, StorageQueryUsagePattern,
    StorageQueryUsageSchemaCompatibility, StorageQueryUsageScope, StorageValidationError,
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use hubuum_domain::ResourceId;
use serde::Serialize;

/// Validated deployment bounds; class identifiers and paths are never metric labels.
#[derive(Clone, Copy, Debug, Serialize)]
pub struct StorageQueryObservationSettings {
    enabled: bool,
    sample_every: u32,
    max_patterns: usize,
    max_patterns_per_class: usize,
    retention_seconds: u32,
    max_predicates_per_query: usize,
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
            enabled,
            sample_every,
            max_patterns,
            max_patterns_per_class,
            retention_seconds,
            max_predicates_per_query,
        })
    }
    pub const fn enabled(self) -> bool {
        self.enabled
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
}

/// Successful sampled logical requests containing this pattern. Duration describes
/// the whole query, including other predicates and authorization, never this predicate's cost.
#[derive(Clone, Debug, Serialize)]
pub struct StorageQueryPatternObservation {
    pattern: StorageQueryUsagePattern,
    sampled_queries: u64,
    first_observed_at: DateTime<Utc>,
    last_observed_at: DateTime<Utc>,
    whole_query_duration_micros: u64,
}
impl StorageQueryPatternObservation {
    pub fn new(pattern: StorageQueryUsagePattern, now: DateTime<Utc>, elapsed_micros: u64) -> Self {
        Self {
            pattern,
            sampled_queries: 1,
            first_observed_at: now,
            last_observed_at: now,
            whole_query_duration_micros: elapsed_micros,
        }
    }
    pub fn record(&mut self, now: DateTime<Utc>, elapsed_micros: u64) {
        self.sampled_queries = self.sampled_queries.saturating_add(1);
        self.last_observed_at = now.max(self.last_observed_at);
        self.whole_query_duration_micros = self
            .whole_query_duration_micros
            .saturating_add(elapsed_micros);
    }
    pub fn pattern(&self) -> &StorageQueryUsagePattern {
        &self.pattern
    }
    pub const fn sampled_queries(&self) -> u64 {
        self.sampled_queries
    }
    pub const fn last_observed_at(&self) -> DateTime<Utc> {
        self.last_observed_at
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct StorageQueryObservationSnapshot {
    source: &'static str,
    settings: StorageQueryObservationSettings,
    process_started_at: DateTime<Utc>,
    captured_at: DateTime<Utc>,
    patterns: Vec<StorageQueryPatternObservation>,
    capacity_drops: u64,
}
impl StorageQueryObservationSnapshot {
    pub fn new(
        settings: StorageQueryObservationSettings,
        process_started_at: DateTime<Utc>,
        patterns: Vec<StorageQueryPatternObservation>,
        capacity_drops: u64,
    ) -> Self {
        Self {
            source: "successful structured queries in this server process",
            settings,
            process_started_at,
            captured_at: Utc::now(),
            patterns,
            capacity_drops,
        }
    }
    pub const fn settings(&self) -> StorageQueryObservationSettings {
        self.settings
    }
    pub fn patterns(&self) -> &[StorageQueryPatternObservation] {
        &self.patterns
    }
}

pub struct StorageQueryUsageAnalysisRequest {
    scope: StorageQueryUsageScope,
    proposed: Vec<StorageQueryUsagePattern>,
    observations: StorageQueryObservationSnapshot,
}
impl StorageQueryUsageAnalysisRequest {
    pub fn try_new(
        scope: StorageQueryUsageScope,
        proposed: Vec<StorageQueryUsagePattern>,
        observations: StorageQueryObservationSnapshot,
    ) -> Result<Self, StorageValidationError> {
        if proposed.len() > 32 || observations.patterns.len() > 128 {
            return Err(StorageValidationError::invalid(
                "Query usage analysis exceeds its pattern budget",
            ));
        }
        Ok(Self {
            scope,
            proposed,
            observations,
        })
    }
    pub const fn scope(&self) -> StorageQueryUsageScope {
        self.scope
    }
    pub fn proposed(&self) -> &[StorageQueryUsagePattern] {
        &self.proposed
    }
    pub fn observations(&self) -> &StorageQueryObservationSnapshot {
        &self.observations
    }
    pub fn into_observations(self) -> StorageQueryObservationSnapshot {
        self.observations
    }
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum StorageQueryUsageAnalysisStatus {
    Unavailable,
    InsufficientEvidence,
    Complete,
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum StorageQueryUsageResourceOwnership {
    IndependentlyManaged,
    DeclarationManaged,
}

/// Adapter-owned opaque identity and observed facts, without SQL or query values.
#[derive(Clone, Debug, Serialize)]
pub struct StorageQueryUsageResource {
    reference: String,
    ownership: StorageQueryUsageResourceOwnership,
    bytes: u64,
    native_scan_count: Option<u64>,
    declaration_owners: Option<u64>,
    cleanup_eligible_if_removed: bool,
}
impl StorageQueryUsageResource {
    pub fn reference(&self) -> &str {
        &self.reference
    }
    pub fn external(reference: String, bytes: u64, native_scan_count: Option<u64>) -> Self {
        Self {
            reference,
            ownership: StorageQueryUsageResourceOwnership::IndependentlyManaged,
            bytes,
            native_scan_count,
            declaration_owners: None,
            cleanup_eligible_if_removed: false,
        }
    }
    pub fn with_declaration_ownership(
        mut self,
        owners: u64,
        cleanup_eligible_if_removed: bool,
    ) -> Self {
        self.ownership = StorageQueryUsageResourceOwnership::DeclarationManaged;
        self.declaration_owners = Some(owners);
        self.cleanup_eligible_if_removed = cleanup_eligible_if_removed;
        self
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct StorageQueryUsageAssessment {
    declaration_id: Option<ResourceId>,
    pattern: StorageQueryUsagePattern,
    schema_compatibility: StorageQueryUsageSchemaCompatibility,
    can_prepare: bool,
    resources: Vec<StorageQueryUsageResource>,
    adapter_progress: Option<StorageQueryUsageAdapterProgress>,
    rationale: String,
}
impl StorageQueryUsageAssessment {
    pub fn new(
        pattern: StorageQueryUsagePattern,
        compatibility: StorageQueryUsageSchemaCompatibility,
        can_prepare: bool,
        rationale: impl Into<String>,
    ) -> Self {
        Self {
            declaration_id: None,
            pattern,
            schema_compatibility: compatibility,
            can_prepare,
            resources: Vec::new(),
            adapter_progress: None,
            rationale: rationale.into(),
        }
    }
    pub fn adapter_progress(mut self, progress: StorageQueryUsageAdapterProgress) -> Self {
        self.adapter_progress = Some(progress);
        self
    }
    pub fn declaration(mut self, value: &StorageQueryUsageDeclaration) -> Self {
        self.declaration_id = Some(value.metadata().id());
        self
    }
    pub fn resources(mut self, values: Vec<StorageQueryUsageResource>) -> Self {
        self.resources = values;
        self
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct StorageQueryUsageSuggestion {
    proposed: StorageQueryUsagePattern,
    observations: Vec<StorageQueryPatternObservation>,
    rationale: String,
}
impl StorageQueryUsageSuggestion {
    pub fn new(
        proposed: StorageQueryUsagePattern,
        observations: Vec<StorageQueryPatternObservation>,
        rationale: impl Into<String>,
    ) -> Self {
        Self {
            proposed,
            observations,
            rationale: rationale.into(),
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct StorageQueryUsageAnalysis {
    status: StorageQueryUsageAnalysisStatus,
    assessed_at: DateTime<Utc>,
    observations: StorageQueryObservationSnapshot,
    assessments: Vec<StorageQueryUsageAssessment>,
    suggestions: Vec<StorageQueryUsageSuggestion>,
    limitations: Vec<String>,
}
impl StorageQueryUsageAnalysis {
    pub fn new(
        status: StorageQueryUsageAnalysisStatus,
        observations: StorageQueryObservationSnapshot,
    ) -> Self {
        Self { status, assessed_at: Utc::now(), observations, assessments: Vec::new(), suggestions: Vec::new(), limitations: vec!["Observations cover sampled successful requests in this process, not the whole deployment; restarts reset them and limits can omit patterns.".into(), "Whole-query timings include other predicates and authorization; they are not costs attributable to a predicate.".into(), "Absence of observed use does not establish that a resource is unnecessary. Recommendations and resource coverage provide no performance guarantee.".into()] }
    }
    pub fn assessment(&mut self, value: StorageQueryUsageAssessment) {
        self.assessments.push(value);
    }
    pub fn suggestion(&mut self, value: StorageQueryUsageSuggestion) {
        self.suggestions.push(value);
    }
    pub fn limitation(&mut self, value: impl Into<String>) {
        self.limitations.push(value.into());
    }
}

/// Optional adapter-owned capability. Analysis must not change declarations or
/// optimization resources. Application callers authorize administrative scope.
#[async_trait]
pub trait QueryUsageAnalysisProvider: Send + Sync {
    async fn analyze_query_usage(
        &self,
        request: StorageQueryUsageAnalysisRequest,
    ) -> Result<StorageQueryUsageAnalysis, StorageError>;
}

/// Informational adapter-owned progress, not a universal declaration lifecycle.
#[derive(Clone, Debug, Serialize)]
pub struct StorageQueryUsageAdapterProgress {
    reference: String,
    state: String,
    declaration_owners: u64,
    last_error: Option<String>,
    updated_at: DateTime<Utc>,
}
impl StorageQueryUsageAdapterProgress {
    pub fn new(
        reference: String,
        state: String,
        declaration_owners: u64,
        last_error: Option<String>,
        updated_at: DateTime<Utc>,
    ) -> Self {
        Self {
            reference,
            state,
            declaration_owners,
            last_error,
            updated_at,
        }
    }
}
