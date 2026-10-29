use clap::Args;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::errors::ApiError;

/// Operational admission limits. Retained manifests use a stable format ceiling,
/// so lowering these limits never changes which existing outputs can be read.
#[derive(Args, Clone, Debug, Deserialize, Serialize, ToSchema)]
#[serde(default)]
pub struct ArtifactOptions {
    #[arg(long, env = "HUBUUM_ARTIFACT_MAX_SPOOL_BYTES", default_value_t = 256 * 1024 * 1024)]
    pub max_spool_bytes: u64,
    #[arg(long, env = "HUBUUM_ARTIFACT_WORKER_RESIDENT_BYTES", default_value_t = 4 * 1024 * 1024 * 1024)]
    pub worker_resident_bytes: u64,
}
impl Default for ArtifactOptions {
    fn default() -> Self {
        Self {
            max_spool_bytes: 256 * 1024 * 1024,
            worker_resident_bytes: 4 * 1024 * 1024 * 1024,
        }
    }
}
impl ArtifactOptions {
    pub(super) fn validate(
        &self,
        export_bytes: usize,
        backup_bytes: usize,
    ) -> Result<(), ApiError> {
        let ceiling =
            hubuum_storage_core::artifacts::ArtifactLimits::format_ceiling().artifact_bytes();
        if self.max_spool_bytes == 0
            || self.max_spool_bytes > ceiling
            || self.worker_resident_bytes == 0
            || self.worker_resident_bytes > i64::MAX as u64
            || export_bytes as u64 > ceiling
            || backup_bytes as u64 > ceiling
        {
            return Err(ApiError::BadRequest(
                "Artifact limits exceed the supported format/admission bounds".into(),
            ));
        }
        let export_reservation =
            export_bytes as u64 + self.max_spool_bytes.max(export_bytes as u64);
        let backup_reservation = 2 * backup_bytes as u64;
        if export_reservation.max(backup_reservation) > self.worker_resident_bytes {
            return Err(ApiError::BadRequest(
                "Artifact worker resident bytes must cover one configured output and its spool"
                    .into(),
            ));
        }
        Ok(())
    }
    #[cfg(any(test, feature = "integration-test-support"))]
    pub(super) fn from_environment() -> Result<Self, ApiError> {
        use clap::Parser;
        #[derive(Parser)]
        struct Environment {
            #[command(flatten)]
            options: ArtifactOptions,
        }
        Environment::try_parse_from(["hubuum-artifacts"])
            .map(|args| args.options)
            .map_err(|error| ApiError::BadRequest(error.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;
    #[rstest]
    #[case(0, 4 << 30)]
    #[case(65 << 30, 128 << 30)]
    #[case(1 << 30, 1 << 30)]
    fn invalid_or_unfulfillable_admission_limits_fail(#[case] spool: u64, #[case] worker: u64) {
        assert!(
            ArtifactOptions {
                max_spool_bytes: spool,
                worker_resident_bytes: worker
            }
            .validate(1 << 30, 1 << 30)
            .is_err()
        );
    }
}
