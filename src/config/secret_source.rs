use std::path::PathBuf;

use clap::{ArgMatches, Args, Parser, ValueEnum, parser::ValueSource};
use hubuum_secrets::{SecretError, SecretErrorKind};
use serde::Deserialize;

use super::environment::constraints;

#[derive(Clone, Copy, Default, Deserialize, ValueEnum)]
#[serde(rename_all = "lowercase")]
enum SecretSource {
    #[default]
    Environment,
    File,
}

/// Shared server and administrator options for resolving credential material.
#[derive(Args, Clone, Default, Deserialize)]
pub struct SecretSourceOptions {
    /// Credential source; file mode uses the layout documented in docs/secret_sources.md
    #[arg(
        long,
        env = "HUBUUM_SECRET_SOURCE",
        value_enum,
        ignore_case = true,
        default_value = "environment"
    )]
    secret_source: SecretSource,

    /// Mounted secret directory, required when --secret-source=file
    #[arg(
        long,
        env = "HUBUUM_SECRET_FILE_ROOT",
        value_name = "DIRECTORY",
        hide_env_values = true
    )]
    secret_file_root: Option<PathBuf>,
}

/// Preserves the required directory when file resolution is selected.
#[derive(Clone, PartialEq, Eq)]
pub(crate) enum SecretSourceSettings {
    Environment,
    File(PathBuf),
}

impl SecretSourceOptions {
    pub(crate) fn settings(&self) -> Result<SecretSourceSettings, SecretError> {
        match self.secret_source {
            SecretSource::Environment => Ok(SecretSourceSettings::Environment),
            SecretSource::File => constraints::SECRET_FILE_ROOT
                .require(self.secret_file_root.as_ref().filter(|root| !root.as_os_str().is_empty()).cloned())
                .map(SecretSourceSettings::File)
                .map_err(|_| SecretError::new(
                    SecretErrorKind::InvalidProviderConfiguration,
                    "--secret-file-root or HUBUUM_SECRET_FILE_ROOT is required when the secret source is file",
                )),
        }
    }

    pub(crate) fn provider_label(&self) -> &'static str {
        match self.secret_source {
            SecretSource::Environment => "environment",
            SecretSource::File => "file",
        }
    }

    pub(crate) fn file_root_configured(&self) -> bool {
        self.secret_file_root
            .as_ref()
            .is_some_and(|root| !root.as_os_str().is_empty())
    }

    pub(crate) fn from_environment() -> Result<Self, SecretError> {
        #[derive(Parser)]
        struct EnvironmentOptions {
            #[command(flatten)]
            secrets: SecretSourceOptions,
        }

        EnvironmentOptions::try_parse_from(["hubuum-secrets"])
            .map(|options| options.secrets)
            .map_err(|_| SecretError::new(
                SecretErrorKind::InvalidProviderConfiguration,
                "Invalid secret source configuration; HUBUUM_SECRET_SOURCE must be environment or file",
            ))
    }
}

/// Proof that a URL was supplied explicitly, rather than inherited from the environment.
#[derive(Clone)]
pub(crate) struct CommandLineDatabaseUrl(String);

impl CommandLineDatabaseUrl {
    pub(crate) fn from_matches(matches: &ArgMatches, id: &str) -> Option<Self> {
        (matches.value_source(id) == Some(ValueSource::CommandLine))
            .then(|| matches.get_one::<String>(id).cloned().map(Self))
            .flatten()
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}
