//! Bounded synthetic workloads and aggregate lifecycle storage measurements.
//!
//! These are benchmark contracts, independent of an application runtime or a
//! database driver. They are not lifecycle policy or archive storage interfaces.

use std::error::Error;
use std::fmt;
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::ScaleProfile;

const WORKLOAD: &str = include_str!("../../../scale-benchmarks/lifecycle/workload-v1.toml");

#[derive(Debug)]
pub enum LifecycleError {
    InvalidInput(String),
    Measurement(String),
}

impl fmt::Display for LifecycleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidInput(message) | Self::Measurement(message) => f.write_str(message),
        }
    }
}

impl Error for LifecycleError {}

/// An observed byte count, distinct from row counts and configured limits.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub struct MeasuredBytes(u64);

impl MeasuredBytes {
    pub const fn new(bytes: u64) -> Self {
        Self(bytes)
    }

    pub const fn bytes(self) -> u64 {
        self.0
    }
}

/// Complete logical measurement of one relation, never a truncated sample.
#[derive(Debug, Serialize)]
pub struct LogicalMeasurement {
    rows: u64,
    encoded_bytes: MeasuredBytes,
    largest_row_bytes: MeasuredBytes,
}

impl LogicalMeasurement {
    pub fn new(
        rows: u64,
        encoded_bytes: MeasuredBytes,
        largest_row_bytes: MeasuredBytes,
    ) -> Result<Self, LifecycleError> {
        if (rows == 0 && encoded_bytes.bytes() != 0)
            || (rows != 0 && largest_row_bytes.bytes() == 0)
            || largest_row_bytes.bytes() > encoded_bytes.bytes()
        {
            return Err(LifecycleError::InvalidInput(
                "inconsistent logical row/byte measurements".into(),
            ));
        }
        Ok(Self {
            rows,
            encoded_bytes,
            largest_row_bytes,
        })
    }

    pub const fn encoded_bytes(&self) -> MeasuredBytes {
        self.encoded_bytes
    }
}

/// Adapter-defined allocation including auxiliary data, with indexes separate.
#[derive(Debug, Serialize)]
pub struct PhysicalMeasurement {
    table_bytes: MeasuredBytes,
    index_bytes: MeasuredBytes,
}

impl PhysicalMeasurement {
    pub const fn new(table_bytes: MeasuredBytes, index_bytes: MeasuredBytes) -> Self {
        Self {
            table_bytes,
            index_bytes,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct RelationMeasurement {
    relation: String,
    logical: LogicalMeasurement,
    physical: PhysicalMeasurement,
}

impl RelationMeasurement {
    pub fn new(
        relation: String,
        logical: LogicalMeasurement,
        physical: PhysicalMeasurement,
    ) -> Self {
        Self {
            relation,
            logical,
            physical,
        }
    }
}

/// Raw configuration is private; only checked workloads cross the adapter boundary.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct WorkloadInput {
    version: u32,
    tasks: u32,
    principals: u32,
    dominant_principal_percent: u32,
    long_task_every: u32,
    short_task_ms: u32,
    long_task_ms: u32,
    large_row_every: u32,
    small_row_bytes: u32,
    large_row_bytes: u32,
    output_every: u32,
    output_bytes: u32,
}

#[derive(Clone, Debug, Serialize)]
#[serde(transparent)]
pub struct LifecycleWorkload(WorkloadInput);

/// A compatible, bounded pairing of a scale corpus and its lifecycle overlay.
/// Validate before giving an adapter permission to load the fixture.
#[derive(Debug, Serialize)]
pub struct LifecycleFixture {
    profile: ScaleProfile,
    workload: LifecycleWorkload,
}

impl LifecycleFixture {
    pub fn new(profile: ScaleProfile, workload: LifecycleWorkload) -> Result<Self, LifecycleError> {
        profile
            .validate()
            .map_err(|error| LifecycleError::InvalidInput(error.to_string()))?;
        if profile.totals.principals < u64::from(workload.principal_count())
            || profile.overlays.terminal_tasks != u64::from(workload.task_count())
            || profile.totals.objects > 100_000
        {
            return Err(LifecycleError::InvalidInput(
                "lifecycle fixture requires matching task counts, sufficient principals and at most 100,000 objects".into(),
            ));
        }
        let large_rows = profile.totals.objects / u64::from(workload.0.large_row_every);
        let payload_bytes = large_rows * u64::from(workload.0.large_row_bytes)
            + (profile.totals.objects - large_rows) * u64::from(workload.0.small_row_bytes);
        if payload_bytes > 1_073_741_824 {
            return Err(LifecycleError::InvalidInput(
                "synthetic lifecycle object payloads must total at most 1 GiB".into(),
            ));
        }
        Ok(Self { profile, workload })
    }

    pub const fn profile(&self) -> &ScaleProfile {
        &self.profile
    }

    pub const fn workload(&self) -> &LifecycleWorkload {
        &self.workload
    }
}

impl LifecycleWorkload {
    pub fn bundled() -> Result<Self, LifecycleError> {
        Self::parse(WORKLOAD)
    }

    pub fn parse(text: &str) -> Result<Self, LifecycleError> {
        let raw: WorkloadInput = toml::from_str(text)
            .map_err(|error| LifecycleError::InvalidInput(error.to_string()))?;
        if raw.version != 1
            || !(1..=100_000).contains(&raw.tasks)
            || !(2..=1_000).contains(&raw.principals)
            || !(1..=99).contains(&raw.dominant_principal_percent)
            || raw.long_task_every == 0
            || raw.large_row_every == 0
            || raw.output_every == 0
            || raw.short_task_ms == 0
            || raw.long_task_ms < raw.short_task_ms
            || raw.long_task_ms > 3_600_000
            || raw.small_row_bytes == 0
            || raw.large_row_bytes < raw.small_row_bytes
            || raw.large_row_bytes > 4_194_304
            || raw.output_bytes == 0
            || raw.output_bytes > 4_194_304
        {
            return Err(LifecycleError::InvalidInput(
                "invalid lifecycle workload: bounded counts, durations and row sizes are required"
                    .into(),
            ));
        }
        let output_count = u64::from(raw.tasks).div_ceil(u64::from(raw.output_every));
        if output_count * u64::from(raw.output_bytes) > 1_073_741_824 {
            return Err(LifecycleError::InvalidInput(
                "synthetic lifecycle outputs must total at most 1 GiB".into(),
            ));
        }
        Ok(Self(raw))
    }

    pub const fn task_count(&self) -> u32 {
        self.0.tasks
    }

    pub const fn principal_count(&self) -> u32 {
        self.0.principals
    }

    /// A deterministic workload description, not a persisted task identity.
    pub fn tasks(&self) -> impl Iterator<Item = FixtureTask> + '_ {
        (0..self.0.tasks).map(|index| {
            let principal = if index % 100 < self.0.dominant_principal_percent {
                1
            } else {
                // Advance across minority arrivals, independently of the 100-task cycle.
                let minority = index / 100 * (100 - self.0.dominant_principal_percent)
                    + index % 100
                    - self.0.dominant_principal_percent;
                2 + minority % (self.0.principals - 1)
            };
            FixtureTask {
                ordinal: index + 1,
                principal,
                duration: Duration::from_millis(u64::from(
                    if index % self.0.long_task_every == 0 {
                        self.0.long_task_ms
                    } else {
                        self.0.short_task_ms
                    },
                )),
                output: (index % self.0.output_every == 0)
                    .then_some(MeasuredBytes::new(u64::from(self.0.output_bytes))),
            }
        })
    }

    pub fn row_bytes(&self, ordinal: u64) -> MeasuredBytes {
        MeasuredBytes::new(u64::from(
            if ordinal.is_multiple_of(u64::from(self.0.large_row_every)) {
                self.0.large_row_bytes
            } else {
                self.0.small_row_bytes
            },
        ))
    }
}

#[derive(Debug)]
pub struct FixtureTask {
    ordinal: u32,
    principal: u32,
    duration: Duration,
    output: Option<MeasuredBytes>,
}

impl FixtureTask {
    pub const fn ordinal(&self) -> u32 {
        self.ordinal
    }

    pub const fn principal_ordinal(&self) -> u32 {
        self.principal
    }

    pub const fn duration(&self) -> Duration {
        self.duration
    }

    pub const fn output_bytes(&self) -> Option<MeasuredBytes> {
        self.output
    }
}

#[derive(Debug, Serialize)]
pub struct LifecycleInventory {
    relations: Vec<RelationMeasurement>,
}

impl LifecycleInventory {
    pub fn new(relations: Vec<RelationMeasurement>) -> Self {
        Self { relations }
    }
}

/// Benchmark-only operations. No connection, SQL, app config or task engine leaks
/// through this interface. Other adapters can implement their own physical probe.
#[async_trait]
pub trait LifecycleBenchmarkBackend: Send + Sync {
    /// Requires a freshly migrated disposable database, like the scale loader.
    async fn load_lifecycle_fixture(
        &self,
        fixture: &LifecycleFixture,
    ) -> Result<LifecycleInventory, LifecycleError>;

    /// Return aggregate sizes only; never return payloads or resource identifiers.
    async fn lifecycle_inventory(&self) -> Result<LifecycleInventory, LifecycleError>;
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    fn dataset() -> ScaleProfile {
        toml::from_str(include_str!(
            "../../../scale-benchmarks/lifecycle/dataset-v1.toml"
        ))
        .unwrap()
    }

    #[test]
    fn bundled_fixture_is_compatible() {
        LifecycleFixture::new(dataset(), LifecycleWorkload::bundled().unwrap()).unwrap();
    }

    #[test]
    fn fixture_rejects_task_count_mismatch_before_loading() {
        let mut profile = dataset();
        profile.overlays.terminal_tasks += 1;
        assert!(LifecycleFixture::new(profile, LifecycleWorkload::bundled().unwrap()).is_err());
    }

    #[test]
    fn fixture_rejects_excessive_total_object_payload() {
        let workload = LifecycleWorkload::parse(
            &WORKLOAD
                .replace("large_row_every = 100", "large_row_every = 1")
                .replace("large_row_bytes = 65536", "large_row_bytes = 4194304"),
        )
        .unwrap();
        assert!(LifecycleFixture::new(dataset(), workload).is_err());
    }

    #[test]
    fn workload_preserves_principal_skew() {
        let workload = LifecycleWorkload::bundled().unwrap();
        let dominant = workload.tasks().filter(|task| task.principal == 1).count();
        assert_eq!(dominant, workload.task_count() as usize * 80 / 100);
    }

    #[test]
    fn workload_has_unequal_durations() {
        let workload = LifecycleWorkload::bundled().unwrap();
        let duration_classes: BTreeSet<_> = workload.tasks().map(|task| task.duration).collect();
        assert_eq!(duration_classes.len(), 2);
    }

    #[test]
    fn invalid_workload_cannot_cross_boundary() {
        assert!(
            LifecycleWorkload::parse(
                &WORKLOAD.replace("long_task_every = 10", "long_task_every = 0")
            )
            .is_err()
        );
    }

    #[test]
    fn fixture_rejects_excessive_aggregate_output() {
        assert!(
            LifecycleWorkload::parse(&WORKLOAD.replace("output_every = 100", "output_every = 1"))
                .is_err()
        );
    }

    #[test]
    fn every_minority_principal_receives_work() {
        let workload = LifecycleWorkload::bundled().unwrap();
        let principals: BTreeSet<_> = workload.tasks().map(|task| task.principal).collect();
        assert_eq!(principals.len(), workload.principal_count() as usize);
    }

    #[test]
    fn logical_measurement_rejects_incomplete_counts() {
        assert!(
            LogicalMeasurement::new(0, MeasuredBytes::new(10), MeasuredBytes::new(10)).is_err()
        );
    }
}
