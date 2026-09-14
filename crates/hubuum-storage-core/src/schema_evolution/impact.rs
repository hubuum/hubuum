use hubuum_domain::{ObjectId, SchemaFailure, SchemaImpactInspection, SchemaReference};
use serde::{Deserialize, Serialize};
use serde_json::Value;

mod report;
pub use report::{
    StorageSchemaImpactFinding, StorageSchemaImpactReport, StorageSchemaWorkReport,
    StorageSchemaWorkReportBuilder,
};

use super::{
    StorageComplianceStatus, StorageSchemaRevisionStatus, StorageSchemaWork, StorageSchemaWorkKind,
    StorageSchemaWorkStatus, StorageValidatedSchemaPolicy, StorageValidationError,
};

/// One bounded read of current schema identities, lifecycle, and population epoch.
#[derive(Clone, Debug, Deserialize)]
#[serde(try_from = "ImpactBoundarySnapshot")]
pub struct StorageSchemaImpactBoundary {
    active: SchemaReference,
    target: SchemaReference,
    target_status: StorageSchemaRevisionStatus,
    epoch: u64,
}

#[derive(Deserialize)]
struct ImpactBoundarySnapshot {
    active: SchemaReference,
    target: SchemaReference,
    target_status: StorageSchemaRevisionStatus,
    epoch: u64,
}

impl TryFrom<ImpactBoundarySnapshot> for StorageSchemaImpactBoundary {
    type Error = StorageValidationError;
    fn try_from(raw: ImpactBoundarySnapshot) -> Result<Self, Self::Error> {
        Self::try_new(raw.active, raw.target, raw.target_status, raw.epoch)
    }
}

impl StorageSchemaImpactBoundary {
    pub fn try_new(
        active: SchemaReference,
        target: SchemaReference,
        target_status: StorageSchemaRevisionStatus,
        epoch: u64,
    ) -> Result<Self, StorageValidationError> {
        if active.class_id() != target.class_id() || epoch > i64::MAX as u64 {
            return Err(StorageValidationError::invalid(
                "Invalid schema impact boundary",
            ));
        }
        Ok(Self {
            active,
            target,
            target_status,
            epoch,
        })
    }
    pub const fn active(&self) -> SchemaReference {
        self.active
    }
    pub const fn epoch(&self) -> u64 {
        self.epoch
    }
}

/// A comparison uses the same object snapshot for both immutable policies.
pub struct StorageSchemaInspection {
    before: Option<StorageComplianceStatus>,
    after: Option<StorageComplianceStatus>,
    failure: Option<SchemaFailure>,
}

impl StorageSchemaInspection {
    pub fn new(
        baseline: Option<&StorageValidatedSchemaPolicy>,
        candidate: &StorageValidatedSchemaPolicy,
        value: Option<&Value>,
    ) -> Self {
        let (after, failure) = Self::inspect(candidate, value);
        Self {
            before: baseline.and_then(|policy| Self::inspect(policy, value).0),
            after,
            failure,
        }
    }

    fn inspect(
        policy: &StorageValidatedSchemaPolicy,
        value: Option<&Value>,
    ) -> (Option<StorageComplianceStatus>, Option<SchemaFailure>) {
        let Some(schema) = &policy.compiled else {
            return (Some(StorageComplianceStatus::NotRequired), None);
        };
        match value.map(|value| schema.inspect_impact(value)) {
            Some(SchemaImpactInspection::Valid) => (Some(StorageComplianceStatus::Valid), None),
            Some(SchemaImpactInspection::Invalid(failure)) => {
                (Some(StorageComplianceStatus::Invalid), Some(failure))
            }
            _ => (None, None),
        }
    }

    pub const fn status(&self) -> Option<StorageComplianceStatus> {
        self.after
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StorageSchemaImpactReadiness {
    Compatible,
    Incompatible,
    Inconclusive,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct ImpactCounts {
    newly_invalid: u64,
    newly_valid: u64,
    still_invalid: u64,
    still_valid: u64,
    newly_required_valid: u64,
    no_longer_required: u64,
    unchanged_not_required: u64,
    uninspectable: u64,
}

impl ImpactCounts {
    fn record(
        &mut self,
        before: Option<StorageComplianceStatus>,
        after: Option<StorageComplianceStatus>,
    ) {
        use StorageComplianceStatus::{Invalid, NotRequired, Valid};
        let count = match (before, after) {
            (Some(Valid | NotRequired), Some(Invalid)) => &mut self.newly_invalid,
            (Some(Invalid), Some(Valid)) => &mut self.newly_valid,
            (Some(Invalid), Some(Invalid)) => &mut self.still_invalid,
            (Some(Valid), Some(Valid)) => &mut self.still_valid,
            (Some(NotRequired), Some(Valid)) => &mut self.newly_required_valid,
            (Some(Valid | Invalid), Some(NotRequired)) => &mut self.no_longer_required,
            (Some(NotRequired), Some(NotRequired)) => &mut self.unchanged_not_required,
            _ => &mut self.uninspectable,
        };
        *count += 1;
    }

    fn total(&self) -> Option<u64> {
        [
            self.newly_invalid,
            self.newly_valid,
            self.still_invalid,
            self.still_valid,
            self.newly_required_valid,
            self.no_longer_required,
            self.unchanged_not_required,
            self.uninspectable,
        ]
        .into_iter()
        .try_fold(0_u64, u64::checked_add)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct FailureGroup {
    reason: SchemaFailure,
    objects: u64,
    samples: Vec<ObjectId>,
}

/// Bounded comparison checkpoint. Findings are appended separately from scan progress.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(try_from = "ImpactSnapshot")]
pub struct StorageSchemaImpact {
    baseline: SchemaReference,
    counts: ImpactCounts,
    failures: Vec<FailureGroup>,
    ungrouped_failures: u64,
    #[serde(default)]
    persisted_findings: u64,
}

#[derive(Deserialize)]
struct ImpactSnapshot {
    baseline: SchemaReference,
    counts: ImpactCounts,
    failures: Vec<FailureGroup>,
    ungrouped_failures: u64,
    #[serde(default)]
    persisted_findings: u64,
}

impl TryFrom<ImpactSnapshot> for StorageSchemaImpact {
    type Error = StorageValidationError;

    fn try_from(raw: ImpactSnapshot) -> Result<Self, Self::Error> {
        let impact = Self {
            baseline: raw.baseline,
            counts: raw.counts,
            failures: raw.failures,
            ungrouped_failures: raw.ungrouped_failures,
            persisted_findings: raw.persisted_findings,
        };
        let total = impact.counts.total();
        if total.is_none()
            || impact.failures.len() > 20
            || impact
                .grouped_count()
                .zip(total)
                .is_none_or(|(grouped, total)| grouped > total)
            || impact.failures.iter().enumerate().any(|(index, group)| {
                group.objects == 0
                    || group.samples.is_empty()
                    || group.samples.len() > 5
                    || group.samples.len() as u64 > group.objects
                    || group
                        .samples
                        .windows(2)
                        .any(|pair| pair[0].id() >= pair[1].id())
                    || impact.failures[..index]
                        .iter()
                        .any(|other| other.reason == group.reason)
            })
        {
            return Err(StorageValidationError::invalid(
                "Schema impact checkpoint is inconsistent",
            ));
        }
        Ok(impact)
    }
}

impl StorageSchemaImpact {
    pub(super) fn new(baseline: SchemaReference) -> Self {
        Self {
            baseline,
            counts: ImpactCounts::default(),
            failures: Vec::new(),
            ungrouped_failures: 0,
            persisted_findings: 0,
        }
    }

    pub const fn baseline(&self) -> SchemaReference {
        self.baseline
    }

    pub const fn persisted_findings(&self) -> u64 {
        self.persisted_findings
    }

    pub(super) fn is_inspectable(&self) -> bool {
        self.counts.uninspectable == 0
    }

    pub(super) fn validate(&self, work: &StorageSchemaWork) -> Result<(), StorageValidationError> {
        if work.kind != StorageSchemaWorkKind::Impact
            || self.baseline.class_id() != work.target.class_id()
            || self.counts.total() != Some(work.examined)
            || self.counts.uninspectable < work.stale
            || self
                .grouped_count()
                .is_none_or(|count| count > work.invalid)
            || self
                .failures
                .iter()
                .any(|group| group.samples.iter().any(|id| id.id() > work.cursor))
        {
            return Err(StorageValidationError::invalid(
                "Schema impact checkpoint is inconsistent",
            ));
        }
        Ok(())
    }

    fn grouped_count(&self) -> Option<u64> {
        self.failures.iter().try_fold(
            self.ungrouped_failures
                .checked_add(self.persisted_findings)?,
            |total, group| total.checked_add(group.objects),
        )
    }

    fn record(
        &mut self,
        object: ObjectId,
        inspection: StorageSchemaInspection,
        stale: bool,
    ) -> Option<StorageSchemaImpactFinding> {
        if stale {
            self.counts.record(None, None);
            return None;
        }
        self.counts.record(inspection.before, inspection.after);
        let reason = inspection.failure?;
        self.persisted_findings += 1;
        Some(StorageSchemaImpactFinding::new(object, reason))
    }
}

impl StorageSchemaWork {
    pub const fn impact(&self) -> Option<&StorageSchemaImpact> {
        self.impact.as_ref()
    }

    pub fn record_impact(
        &mut self,
        object: ObjectId,
        inspection: StorageSchemaInspection,
        stale: bool,
    ) -> Option<StorageSchemaImpactFinding> {
        self.record(object, inspection.status(), stale);
        self.impact
            .as_mut()
            .and_then(|impact| impact.record(object, inspection, stale))
    }

    /// Readiness is evaluated against current state, never persisted as a promise.
    pub fn impact_readiness(
        &self,
        state: &StorageSchemaImpactBoundary,
    ) -> StorageSchemaImpactReadiness {
        let Some(impact) = &self.impact else {
            return StorageSchemaImpactReadiness::Inconclusive;
        };
        if self.status != StorageSchemaWorkStatus::Complete
            || self.start_epoch != state.epoch()
            || self.end_epoch != Some(state.epoch())
            || impact.baseline != state.active()
            || self.target != state.target
            || self.target.revision() < state.active().revision()
            || !matches!(
                state.target_status,
                StorageSchemaRevisionStatus::Staged | StorageSchemaRevisionStatus::Active
            )
            || impact.counts.uninspectable > 0
            || self.uninspectable > 0
            || self.stale > 0
        {
            StorageSchemaImpactReadiness::Inconclusive
        } else if self.invalid > 0 {
            StorageSchemaImpactReadiness::Incompatible
        } else {
            StorageSchemaImpactReadiness::Compatible
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{StorageClassSchemaPolicy, schema_evolution::StorageSchemaWorkRequest};
    use hubuum_domain::{ClassId, CollectionId, SchemaRevision, TaskId};
    use hubuum_events_core::EventContext;
    use rstest::rstest;
    use serde_json::json;

    fn work() -> StorageSchemaWork {
        let class = ClassId::new(1).unwrap();
        let baseline = SchemaReference::new(class, SchemaRevision::INITIAL);
        let candidate = SchemaReference::new(class, SchemaRevision::new(2).unwrap());
        let request = StorageSchemaWorkRequest::new(
            CollectionId::new(1).unwrap(),
            candidate,
            StorageSchemaWorkKind::Impact,
            EventContext::system(),
        );
        StorageSchemaWork::start(TaskId::new(1).unwrap(), &request, 0, 1, baseline)
    }

    #[test]
    fn stale_objects_do_not_publish_comparison_counts_or_examples() {
        let mut work = work();
        let baseline =
            StorageValidatedSchemaPolicy::try_new(StorageClassSchemaPolicy::Absent).unwrap();
        let candidate = StorageValidatedSchemaPolicy::try_new(StorageClassSchemaPolicy::Enforced(
            json!({"type":"integer"}),
        ))
        .unwrap();
        assert!(
            work.record_impact(
                ObjectId::new(1).unwrap(),
                StorageSchemaInspection::new(Some(&baseline), &candidate, Some(&json!("secret"))),
                true,
            )
            .is_none()
        );
        let impact = serde_json::to_value(work.impact().unwrap()).unwrap();
        assert_eq!(impact["counts"]["uninspectable"], 1);
        assert_eq!(impact["counts"]["newly_invalid"], 0);
        assert_eq!(impact["failures"], json!([]));
    }

    #[rstest]
    #[case::count("/impact/counts/newly_invalid", json!(1))]
    #[case::overflow("/impact/ungrouped_failures", json!(u64::MAX))]
    #[case::wrong_class("/impact/baseline/class_id", json!(2))]
    fn malformed_comparison_checkpoints_are_rejected(
        #[case] path: &str,
        #[case] replacement: Value,
    ) {
        let mut snapshot = serde_json::to_value(work()).unwrap();
        *snapshot.pointer_mut(path).unwrap() = replacement;
        assert!(serde_json::from_value::<StorageSchemaWork>(snapshot).is_err());
    }

    #[test]
    fn legacy_capped_checkpoints_resume_without_discarding_findings() {
        let mut work = work();
        work.upper_bound = 15;
        let baseline =
            StorageValidatedSchemaPolicy::try_new(StorageClassSchemaPolicy::Absent).unwrap();
        let candidate = StorageValidatedSchemaPolicy::try_new(StorageClassSchemaPolicy::Enforced(
            json!({"type":"integer", "minimum":1}),
        ))
        .unwrap();
        for id in 1..=14 {
            let value = if id <= 7 { json!("private") } else { json!(0) };
            let _ = work.record_impact(
                ObjectId::new(id).unwrap(),
                StorageSchemaInspection::new(Some(&baseline), &candidate, Some(&value)),
                false,
            );
        }
        // Reproduce omitted IDs and groups from an older persisted checkpoint.
        let mut snapshot = serde_json::to_value(work).unwrap();
        snapshot["impact"]["failures"] = json!([{
            "reason": {"keyword":"type", "schema_path":"/type", "missing_property":null},
            "objects":7, "samples":[1,2,3,4,5]
        }]);
        snapshot["impact"]
            .as_object_mut()
            .unwrap()
            .remove("persisted_findings");
        snapshot["impact"]["ungrouped_failures"] = json!(7);
        let mut restored: StorageSchemaWork = serde_json::from_value(snapshot).unwrap();
        let finding = restored
            .record_impact(
                ObjectId::new(15).unwrap(),
                StorageSchemaInspection::new(Some(&baseline), &candidate, Some(&json!("private"))),
                false,
            )
            .unwrap();
        let restored = serde_json::from_value(serde_json::to_value(restored).unwrap()).unwrap();
        let mut builder = StorageSchemaWorkReport::builder(restored);
        builder.push(finding).unwrap();
        let resumed = serde_json::to_value(builder.finish().unwrap().impact().unwrap()).unwrap();
        assert_eq!(
            resumed["failures"],
            json!([{
                "reason": {"keyword":"type", "schema_path":"/type", "missing_property":null},
                "objects":8,
                "samples":[1,2,3,4,5,15]
            }])
        );
        assert_eq!(resumed["ungrouped_failures"], 7);
    }

    #[test]
    fn legacy_checkpoints_cannot_authorize_new_strict_activations() {
        let mut work = work();
        work.finish(StorageSchemaWorkStatus::Complete, 0);
        let baseline = work.impact().unwrap().baseline();
        let mut snapshot = serde_json::to_value(&work).unwrap();
        snapshot.as_object_mut().unwrap().remove("impact");
        let restored: StorageSchemaWork = serde_json::from_value(snapshot).unwrap();
        assert!(!restored.proves_compatible(work.target(), baseline, 0));
    }

    #[rstest]
    #[case::same_reason(1)]
    #[case::many_reasons(256)]
    fn checkpoint_size_is_independent_of_accumulated_findings(#[case] reasons: usize) {
        let mut work = work();
        work.upper_bound = 8192;
        let reasons = (0..reasons).map(|index| serde_json::from_value::<SchemaFailure>(json!({
            "keyword":"type", "schema_path":format!("/properties/field{index}/type"), "missing_property":null
        })).unwrap()).collect::<Vec<_>>();
        let mut findings = Vec::new();
        for id in 1..=work.upper_bound() {
            findings.extend(work.record_impact(
                ObjectId::new(id).unwrap(),
                StorageSchemaInspection {
                    before: Some(StorageComplianceStatus::NotRequired),
                    after: Some(StorageComplianceStatus::Invalid),
                    failure: Some(reasons[(id as usize - 1) % reasons.len()].clone()),
                },
                false,
            ));
            if id % 64 == 0 {
                let checkpoint = serde_json::to_vec(&work).unwrap();
                assert!(
                    checkpoint.len() < 2048,
                    "checkpoint grew to {} bytes at object {id}",
                    checkpoint.len()
                );
                work = serde_json::from_slice(&checkpoint).unwrap();
            }
        }
        let mut report = StorageSchemaWorkReport::builder(work);
        for finding in findings {
            report.push(finding).unwrap();
        }
        let report = report.finish().unwrap();
        let impact = serde_json::to_value(report.impact().unwrap()).unwrap();
        assert_eq!(impact["failures"].as_array().unwrap().len(), reasons.len());
        assert_eq!(
            impact["failures"]
                .as_array()
                .unwrap()
                .iter()
                .map(|group| group["samples"].as_array().unwrap().len())
                .sum::<usize>(),
            8192
        );
    }

    #[rstest]
    #[case::missing("missing")]
    #[case::duplicate("duplicate")]
    #[case::unordered("unordered")]
    #[case::past_cursor("past_cursor")]
    fn inconsistent_persisted_findings_cannot_produce_a_report(#[case] corruption: &str) {
        let mut work = work();
        work.upper_bound = 3;
        let candidate =
            StorageValidatedSchemaPolicy::try_new(StorageClassSchemaPolicy::Enforced(json!(false)))
                .unwrap();
        let mut findings = (1..=2)
            .map(|id| {
                work.record_impact(
                    ObjectId::new(id).unwrap(),
                    StorageSchemaInspection::new(Some(&candidate), &candidate, Some(&json!({}))),
                    false,
                )
                .unwrap()
            })
            .collect::<Vec<_>>();
        match corruption {
            "missing" => {
                findings.pop();
            }
            "duplicate" => {
                findings[1] = findings[0].clone();
            }
            "unordered" => findings.swap(0, 1),
            "past_cursor" => {
                findings[1] = serde_json::from_value(json!({"object_id":3,"reason":{"keyword":"falseSchema","schema_path":"","missing_property":null}})).unwrap();
            }
            _ => unreachable!(),
        }
        let mut report = StorageSchemaWorkReport::builder(work);
        let outcome = findings
            .into_iter()
            .try_for_each(|finding| report.push(finding))
            .and_then(|()| report.finish().map(|_| ()));
        assert!(outcome.is_err());
    }
}
