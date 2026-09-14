use std::collections::{HashMap, HashSet};

use hubuum_domain::{ObjectId, SchemaFailure, SchemaReference};
use serde::{Deserialize, Serialize};

use super::{FailureGroup, ImpactCounts, StorageSchemaWork, StorageValidationError};

/// One validated mismatch, persisted atomically with the batch that inspected it.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StorageSchemaImpactFinding {
    object_id: ObjectId,
    reason: SchemaFailure,
}

impl StorageSchemaImpactFinding {
    pub fn new(object_id: ObjectId, reason: SchemaFailure) -> Self {
        Self { object_id, reason }
    }

    pub const fn object_id(&self) -> ObjectId {
        self.object_id
    }
}

/// Complete report projection; never used as a worker checkpoint.
#[derive(Debug, Serialize)]
pub struct StorageSchemaImpactReport {
    baseline: SchemaReference,
    counts: ImpactCounts,
    failures: Vec<FailureGroup>,
    ungrouped_failures: u64,
}

#[derive(Debug)]
pub struct StorageSchemaWorkReport {
    work: StorageSchemaWork,
    impact: Option<StorageSchemaImpactReport>,
}

impl StorageSchemaWorkReport {
    pub fn builder(work: StorageSchemaWork) -> StorageSchemaWorkReportBuilder {
        let impact = work.impact().map(|impact| StorageSchemaImpactReport {
            baseline: impact.baseline,
            counts: impact.counts.clone(),
            failures: impact.failures.clone(),
            ungrouped_failures: impact.ungrouped_failures,
        });
        let groups = impact
            .iter()
            .flat_map(|impact| impact.failures.iter().enumerate())
            .map(|(index, group)| (group.reason.clone(), index))
            .collect();
        let legacy_ids = impact
            .iter()
            .flat_map(|impact| &impact.failures)
            .flat_map(|group| group.samples.iter().copied())
            .collect();
        StorageSchemaWorkReportBuilder {
            report: Self { work, impact },
            groups,
            legacy_ids,
            recorded: 0,
            last_object: 0,
        }
    }

    pub const fn work(&self) -> &StorageSchemaWork {
        &self.work
    }

    pub const fn impact(&self) -> Option<&StorageSchemaImpactReport> {
        self.impact.as_ref()
    }
}

/// Consume findings in object-ID order without retaining a second complete list.
pub struct StorageSchemaWorkReportBuilder {
    report: StorageSchemaWorkReport,
    groups: HashMap<SchemaFailure, usize>,
    legacy_ids: HashSet<ObjectId>,
    recorded: u64,
    last_object: i32,
}

impl StorageSchemaWorkReportBuilder {
    pub fn push(
        &mut self,
        finding: StorageSchemaImpactFinding,
    ) -> Result<(), StorageValidationError> {
        let expected = self
            .report
            .work
            .impact()
            .map_or(0, |impact| impact.persisted_findings());
        if finding.object_id.id() <= self.last_object
            || finding.object_id.id() > self.report.work.cursor()
            || self.legacy_ids.contains(&finding.object_id)
            || self.recorded >= expected
        {
            return Err(StorageValidationError::invalid(
                "Schema impact findings are inconsistent with the checkpoint",
            ));
        }
        let impact = self.report.impact.as_mut().ok_or_else(|| {
            StorageValidationError::invalid("Revalidation cannot have impact findings")
        })?;
        self.last_object = finding.object_id.id();
        self.recorded += 1;
        if let Some(index) = self.groups.get(&finding.reason) {
            let group = &mut impact.failures[*index];
            if group
                .samples
                .last()
                .is_some_and(|id| id.id() >= finding.object_id.id())
            {
                return Err(StorageValidationError::invalid(
                    "Schema impact findings precede legacy samples",
                ));
            }
            group.objects += 1;
            group.samples.push(finding.object_id);
        } else {
            self.groups
                .insert(finding.reason.clone(), impact.failures.len());
            impact.failures.push(FailureGroup {
                reason: finding.reason,
                objects: 1,
                samples: vec![finding.object_id],
            });
        }
        Ok(())
    }

    pub fn finish(self) -> Result<StorageSchemaWorkReport, StorageValidationError> {
        let expected = self
            .report
            .work
            .impact()
            .map_or(0, |impact| impact.persisted_findings());
        if self.recorded != expected {
            return Err(StorageValidationError::invalid(
                "Schema impact findings are missing from the report",
            ));
        }
        Ok(self.report)
    }
}
