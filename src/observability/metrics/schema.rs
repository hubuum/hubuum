use super::{Metrics, current};
use hubuum_storage_core::{
    SchemaEvolutionStorage, StorageError, StorageErrorKind, StorageMutationOutcome,
    StorageSchemaWork, StorageSchemaWorkKind, StorageSchemaWorkStatus,
};
use opentelemetry::KeyValue;

pub(crate) fn schema_mutation<T>(
    policy: &'static str,
    result: &Result<StorageMutationOutcome<T>, StorageError>,
) {
    if let Some(metrics) = current() {
        let result = match result {
            Ok(value) if value.is_committed() => "committed",
            Ok(_) => "unchanged",
            Err(error) if error.kind() == StorageErrorKind::Conflict => "conflict",
            Err(_) => "error",
        };
        metrics.schema_mutations.add(
            1,
            &[
                KeyValue::new("policy", policy),
                KeyValue::new("result", result),
            ],
        );
    }
}
pub(crate) fn schema_work_progress(before: &StorageSchemaWork, after: &StorageSchemaWork) {
    if let Some(metrics) = current() {
        let kind = match after.kind() {
            StorageSchemaWorkKind::Impact => "impact",
            StorageSchemaWorkKind::Revalidation => "revalidation",
        };
        for (result, count) in [
            ("valid", after.valid().saturating_sub(before.valid())),
            (
                "not_required",
                after.not_required().saturating_sub(before.not_required()),
            ),
            ("invalid", after.invalid().saturating_sub(before.invalid())),
            (
                "uninspectable",
                after.uninspectable().saturating_sub(before.uninspectable()),
            ),
            ("stale", after.stale().saturating_sub(before.stale())),
        ] {
            metrics.schema_objects.add(
                count,
                &[KeyValue::new("kind", kind), KeyValue::new("result", result)],
            );
        }
        if before.status() == StorageSchemaWorkStatus::Running
            && after.status() != StorageSchemaWorkStatus::Running
        {
            let status = match after.status() {
                StorageSchemaWorkStatus::Complete => "complete",
                StorageSchemaWorkStatus::Superseded => "superseded",
                _ => "cancelled",
            };
            let elapsed = chrono::Utc::now()
                .signed_duration_since(after.created_at())
                .num_milliseconds()
                .max(0) as f64
                / 1000.0;
            metrics.schema_duration.record(
                elapsed,
                &[KeyValue::new("kind", kind), KeyValue::new("status", status)],
            );
        }
    }
}
pub(super) async fn refresh_compliance(metrics: &Metrics, backend: &crate::storage::StorageHandle) {
    match backend.schema_compliance_counts().await {
        Ok(counts) => {
            for (status, count) in [
                ("valid", counts.valid()),
                ("invalid", counts.invalid()),
                ("pending", counts.pending()),
                ("not_required", counts.not_required()),
            ] {
                metrics
                    .schema_compliance
                    .record(count, &[KeyValue::new("status", status)]);
            }
        }
        Err(error) => {
            tracing::warn!(error_kind=?error.kind(),"Schema compliance metrics refresh failed")
        }
    }
}

pub(crate) fn schema_dependency_rebuild() {
    if let Some(metrics) = current() {
        metrics.schema_dependency_rebuilds.add(1, &[]);
    }
}
