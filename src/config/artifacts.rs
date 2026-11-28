use std::{path::PathBuf, str::FromStr, sync::Arc};

use clap::Args;
use hubuum_artifacts::{ArtifactStore, DestinationId};
use hubuum_storage_core::artifacts::ArtifactLimits;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::errors::ApiError;

/// One immutable destination served from an existing private Unix artifact root.
#[derive(Clone, Deserialize)]
#[serde(try_from = "String")]
pub(crate) struct HistoryArchiveFilesystem {
    destination: DestinationId,
    directory: PathBuf,
}

impl FromStr for HistoryArchiveFilesystem {
    type Err = &'static str;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let (id, directory) = value
            .split_once('=')
            .ok_or("expected UUID=ABSOLUTE_DIRECTORY")?;
        let destination = DestinationId::try_from(id.to_owned())
            .map_err(|_| "invalid archive destination UUID")?;
        let directory = PathBuf::from(directory);
        if !directory.is_absolute() {
            return Err("archive directory must be absolute");
        }
        Ok(Self {
            destination,
            directory,
        })
    }
}

impl TryFrom<String> for HistoryArchiveFilesystem {
    type Error = &'static str;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse()
    }
}

impl HistoryArchiveFilesystem {
    pub(crate) async fn open(&self) -> Result<(DestinationId, Arc<dyn ArtifactStore>), ApiError> {
        #[cfg(unix)]
        {
            let store = hubuum_artifacts::FilesystemArtifactStore::open_registered_destination(
                &self.directory,
                self.destination,
            )
            .await
            .map_err(|error| {
                ApiError::BadRequest(format!(
                    "Cannot open registered history archive destination: {error}"
                ))
            })?;
            Ok((self.destination, Arc::new(store)))
        }
        #[cfg(not(unix))]
        {
            let _ = (&self.directory, self.destination);
            Err(ApiError::BadRequest(
                "Filesystem archive reads require Unix".into(),
            ))
        }
    }
}

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
        let ceiling = ArtifactLimits::format_ceiling().artifact_bytes();
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
        let manifest_bytes = |bytes: usize| {
            if bytes as u64 <= ArtifactLimits::default().artifact_bytes() {
                ArtifactLimits::default().manifest_bytes() as u64
            } else {
                ArtifactLimits::format_ceiling().manifest_bytes() as u64
            }
        };
        let export_reservation = export_bytes as u64
            + self.max_spool_bytes.max(export_bytes as u64)
            + manifest_bytes(export_bytes);
        let backup_reservation = 2 * backup_bytes as u64 + manifest_bytes(backup_bytes);
        if export_reservation.max(backup_reservation) > self.worker_resident_bytes {
            return Err(ApiError::BadRequest(
                "Artifact worker resident bytes must cover one configured output, manifest and spool"
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
    #[case("invalid=/archive")]
    #[case("00000000-0000-0000-0000-000000000000=/archive")]
    #[case("24cdb492-ab19-49b2-8455-397cc61151f0")]
    #[case("24cdb492-ab19-49b2-8455-397cc61151f0=relative/path")]
    #[case("24cdb492-ab19-49b2-8455-397cc61151f0=")]
    fn filesystem_archive_config_requires_a_destination_and_absolute_path(#[case] input: &str) {
        assert!(input.parse::<HistoryArchiveFilesystem>().is_err());
    }

    #[cfg(unix)]
    #[test]
    fn filesystem_archive_config_preserves_equals_in_directory() {
        use crate::config::{AppConfig, running::RunningConfig};
        use clap::Parser;
        let _lock = crate::config::TEST_ENV_LOCK.lock().unwrap();
        let directory = "/private/archive=warm";
        let id = DestinationId::generate();
        let option = format!("{}={directory}", String::from(id));
        let config =
            AppConfig::try_parse_from(["hubuum-server", "--history-archive-filesystem", &option])
                .unwrap();
        let selected = config.history_archive_filesystem.as_ref().unwrap();
        assert_eq!(selected.destination, id);
        assert_eq!(selected.directory, PathBuf::from(directory));
        let running = serde_json::to_value(RunningConfig::from(&config)).unwrap();
        assert_eq!(running["history_archive_filesystem_configured"], true);
        assert!(!running.to_string().contains(directory));
        assert!(!running.to_string().contains(&String::from(id)));
    }

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
