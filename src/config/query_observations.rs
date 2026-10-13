use crate::errors::ApiError;
use clap::Args;
use hubuum_storage_core::StorageQueryObservationSettings;
use serde::Deserialize;

/// Opt-in, bounded collection of query structure without values.
#[derive(Args, Clone, Deserialize)]
#[serde(default)]
pub struct QueryObservationOptions {
    #[arg(long, env = "HUBUUM_QUERY_OBSERVATIONS_ENABLED", default_value_t = false, action = clap::ArgAction::Set)]
    pub(super) query_observations_enabled: bool,
    #[arg(
        long,
        env = "HUBUUM_QUERY_OBSERVATIONS_SAMPLE_EVERY",
        default_value_t = 16
    )]
    pub(super) query_observations_sample_every: u32,
    #[arg(
        long,
        env = "HUBUUM_QUERY_OBSERVATIONS_MAX_PATTERNS",
        default_value_t = 2048
    )]
    pub(super) query_observations_max_patterns: usize,
    #[arg(
        long,
        env = "HUBUUM_QUERY_OBSERVATIONS_MAX_PATTERNS_PER_CLASS",
        default_value_t = 64
    )]
    pub(super) query_observations_max_patterns_per_class: usize,
    #[arg(
        long,
        env = "HUBUUM_QUERY_OBSERVATIONS_RETENTION_SECONDS",
        default_value_t = 86400
    )]
    pub(super) query_observations_retention_seconds: u32,
    #[arg(
        long,
        env = "HUBUUM_QUERY_OBSERVATIONS_MAX_PREDICATES_PER_QUERY",
        default_value_t = 16
    )]
    pub(super) query_observations_max_predicates_per_query: usize,
}
impl Default for QueryObservationOptions {
    fn default() -> Self {
        Self {
            query_observations_enabled: false,
            query_observations_sample_every: 16,
            query_observations_max_patterns: 2048,
            query_observations_max_patterns_per_class: 64,
            query_observations_retention_seconds: 86400,
            query_observations_max_predicates_per_query: 16,
        }
    }
}
impl QueryObservationOptions {
    pub(crate) fn validate(&self) -> Result<StorageQueryObservationSettings, ApiError> {
        StorageQueryObservationSettings::try_new(
            self.query_observations_enabled,
            self.query_observations_sample_every,
            self.query_observations_max_patterns,
            self.query_observations_max_patterns_per_class,
            self.query_observations_retention_seconds,
            self.query_observations_max_predicates_per_query,
        )
        .map_err(|error| ApiError::BadRequest(error.to_string()))
    }
    #[cfg(any(test, feature = "integration-test-support"))]
    pub(super) fn from_environment() -> Result<Self, ApiError> {
        use clap::Parser;
        #[derive(Parser)]
        struct Environment {
            #[command(flatten)]
            options: QueryObservationOptions,
        }
        Environment::try_parse_from(["hubuum-query-observations"])
            .map(|args| args.options)
            .map_err(|error| ApiError::BadRequest(error.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;
    use rstest::rstest;
    #[derive(Parser)]
    struct Command {
        #[command(flatten)]
        options: QueryObservationOptions,
    }
    #[rstest]
    #[case("--query-observations-sample-every", "0")]
    #[case("--query-observations-max-patterns", "16385")]
    #[case("--query-observations-max-patterns-per-class", "129")]
    #[case("--query-observations-retention-seconds", "604801")]
    #[case("--query-observations-max-predicates-per-query", "33")]
    fn invalid_budgets_fail_at_configuration(#[case] flag: &str, #[case] value: &str) {
        assert!(
            Command::try_parse_from(["test", flag, value])
                .unwrap()
                .options
                .validate()
                .is_err()
        );
    }
    #[test]
    fn collection_requires_explicit_enablement() {
        assert!(
            !QueryObservationOptions::default()
                .validate()
                .unwrap()
                .enabled()
        );
        assert!(
            Command::try_parse_from(["test", "--query-observations-enabled", "true"])
                .unwrap()
                .options
                .validate()
                .unwrap()
                .enabled()
        );
    }
}
