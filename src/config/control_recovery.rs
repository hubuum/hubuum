use std::path::{Path, PathBuf};

use clap::Args;
use serde::Deserialize;

use crate::errors::ApiError;

/// Independent recovery evidence must be configured outside database backups.
#[derive(Clone, Default, Args, Deserialize)]
pub struct ControlRecoveryOptions {
    /// Require the latest independent deletion checkpoint before runtime startup
    #[arg(
        long = "control-recovery-directory",
        id = "control_recovery_directory",
        env = "HUBUUM_CONTROL_RECOVERY_DIRECTORY",
        requires = "control_recovery_key"
    )]
    directory: Option<PathBuf>,

    /// Owner-only 32-byte checkpoint authentication key (same trusted host account)
    #[arg(
        long = "control-recovery-key",
        id = "control_recovery_key",
        env = "HUBUUM_CONTROL_RECOVERY_KEY",
        requires = "control_recovery_directory"
    )]
    key: Option<PathBuf>,
}

impl ControlRecoveryOptions {
    pub(crate) fn paths(&self) -> Result<Option<(&Path, &Path)>, ApiError> {
        match (&self.directory, &self.key) {
            (Some(directory), Some(key)) => Ok(Some((directory, key))),
            (None, None) => Ok(None),
            _ => Err(ApiError::BadRequest(
                "Control recovery requires both a repository directory and authentication key"
                    .into(),
            )),
        }
    }

    pub(crate) fn configured(&self) -> bool {
        self.directory.is_some()
    }

    #[cfg(any(test, feature = "integration-test-support"))]
    pub(crate) fn from_environment() -> Self {
        Self {
            directory: std::env::var_os("HUBUUM_CONTROL_RECOVERY_DIRECTORY").map(PathBuf::from),
            key: std::env::var_os("HUBUUM_CONTROL_RECOVERY_KEY").map(PathBuf::from),
        }
    }
}
