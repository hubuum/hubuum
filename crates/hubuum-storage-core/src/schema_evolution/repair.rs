use chrono::{DateTime, Utc};
use hubuum_domain::{CollectionId, SchemaReference, TaskId};
use serde::{Deserialize, Serialize};

use super::{StorageSchemaWork, StorageSchemaWorkKind, StorageValidationError};

/// A complete HTML artifact of an impact snapshot. Rendering limits fail closed.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(try_from = "RepairReportSnapshot")]
pub struct StorageSchemaRepairReport {
    task_id: TaskId,
    target: SchemaReference,
    generated_at: DateTime<Utc>,
    html: String,
}

#[derive(Deserialize)]
struct RepairReportSnapshot {
    task_id: TaskId,
    target: SchemaReference,
    generated_at: DateTime<Utc>,
    html: String,
}

impl TryFrom<RepairReportSnapshot> for StorageSchemaRepairReport {
    type Error = StorageValidationError;

    fn try_from(raw: RepairReportSnapshot) -> Result<Self, Self::Error> {
        if raw.html.is_empty() || raw.html.len() > Self::MAX_BYTES {
            return Err(StorageValidationError::invalid(
                "Repair report must contain between 1 byte and 16 MiB of HTML",
            ));
        }
        Ok(Self {
            task_id: raw.task_id,
            target: raw.target,
            generated_at: raw.generated_at,
            html: raw.html,
        })
    }
}

impl StorageSchemaRepairReport {
    pub const MAX_BYTES: usize = 16 * 1024 * 1024;

    pub fn try_new(
        work: &StorageSchemaWork,
        generated_at: DateTime<Utc>,
        html: String,
    ) -> Result<Self, StorageValidationError> {
        if work.kind() != StorageSchemaWorkKind::Impact {
            return Err(StorageValidationError::invalid(
                "Only schema impact analyses have repair reports",
            ));
        }
        RepairReportSnapshot {
            task_id: work.task_id(),
            target: work.target(),
            generated_at,
            html,
        }
        .try_into()
    }

    pub const fn task_id(&self) -> TaskId {
        self.task_id
    }
    pub const fn target(&self) -> SchemaReference {
        self.target
    }
    pub const fn generated_at(&self) -> DateTime<Utc> {
        self.generated_at
    }
    pub fn html(&self) -> &str {
        &self.html
    }
    pub fn into_html(self) -> String {
        self.html
    }
}

/// The collection authorized by the caller must still contain the source class.
pub struct StorageSchemaRepairReportWrite {
    report: StorageSchemaRepairReport,
    authorized_collection: CollectionId,
}

impl StorageSchemaRepairReportWrite {
    pub fn new(report: StorageSchemaRepairReport, authorized_collection: CollectionId) -> Self {
        Self {
            report,
            authorized_collection,
        }
    }
    pub const fn report(&self) -> &StorageSchemaRepairReport {
        &self.report
    }
    pub const fn authorized_collection(&self) -> CollectionId {
        self.authorized_collection
    }
    pub fn into_report(self) -> StorageSchemaRepairReport {
        self.report
    }
}
