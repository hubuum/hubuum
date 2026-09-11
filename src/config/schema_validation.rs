use clap::Args;
use hubuum_domain::JsonSchemaLimits;
use serde::Deserialize;

use crate::errors::ApiError;

/// Deployment options shared by the server and administrative restore tools.
#[derive(Args, Clone, Deserialize)]
#[serde(default)]
pub struct SchemaValidationOptions {
    /// Maximum estimated encoded bytes in a JSON Schema document.
    #[arg(long, env = "HUBUUM_SCHEMA_MAX_BYTES", default_value_t = JsonSchemaLimits::DEFAULT.schema_bytes())]
    pub(super) schema_max_bytes: usize,
    /// Maximum work after expanding local schema references and applicators.
    #[arg(long, env = "HUBUUM_SCHEMA_MAX_EXPANDED_WORK", default_value_t = JsonSchemaLimits::DEFAULT.expanded_work())]
    pub(super) schema_max_expanded_work: usize,
    /// Maximum estimated encoded bytes in an enforced object document.
    #[arg(long, env = "HUBUUM_SCHEMA_MAX_INSTANCE_BYTES", default_value_t = JsonSchemaLimits::DEFAULT.instance_bytes())]
    pub(super) schema_max_instance_bytes: usize,
    /// Maximum estimated work for validating one object against its schema.
    #[arg(long, env = "HUBUUM_SCHEMA_MAX_INSTANCE_WORK", default_value_t = JsonSchemaLimits::DEFAULT.instance_work())]
    pub(super) schema_max_instance_work: usize,
}

impl Default for SchemaValidationOptions {
    fn default() -> Self {
        let limits = JsonSchemaLimits::default();
        Self {
            schema_max_bytes: limits.schema_bytes(),
            schema_max_expanded_work: limits.expanded_work(),
            schema_max_instance_bytes: limits.instance_bytes(),
            schema_max_instance_work: limits.instance_work(),
        }
    }
}

impl SchemaValidationOptions {
    pub(crate) fn validate(&self) -> Result<JsonSchemaLimits, ApiError> {
        JsonSchemaLimits::builder()
            .schema_bytes(self.schema_max_bytes)
            .expanded_work(self.schema_max_expanded_work)
            .instance_bytes(self.schema_max_instance_bytes)
            .instance_work(self.schema_max_instance_work)
            .build()
            .map_err(|error| ApiError::BadRequest(error.to_string()))
    }

    #[cfg(any(test, feature = "integration-test-support"))]
    pub(super) fn from_environment() -> Result<Self, ApiError> {
        use clap::Parser;
        #[derive(Parser)]
        struct Environment {
            #[command(flatten)]
            options: SchemaValidationOptions,
        }
        Environment::try_parse_from(["hubuum-schema"])
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
        options: SchemaValidationOptions,
    }

    #[test]
    fn cli_options_produce_explicit_deployment_budgets() {
        let parsed = Command::try_parse_from([
            "test",
            "--schema-max-bytes",
            "98304",
            "--schema-max-expanded-work",
            "32768",
            "--schema-max-instance-bytes",
            "4194304",
            "--schema-max-instance-work",
            "536870912",
        ])
        .unwrap();
        let limits = parsed.options.validate().unwrap();
        assert_eq!(
            limits,
            JsonSchemaLimits::builder()
                .schema_bytes(98304)
                .expanded_work(32768)
                .instance_bytes(4194304)
                .instance_work(536870912)
                .build()
                .unwrap()
        );
    }

    #[rstest]
    #[case("--schema-max-bytes", "1023")]
    #[case("--schema-max-expanded-work", "65537")]
    #[case("--schema-max-instance-bytes", "16777217")]
    #[case("--schema-max-instance-work", "0")]
    fn invalid_cli_budgets_fail_configuration(#[case] flag: &str, #[case] value: &str) {
        let parsed = Command::try_parse_from(["test", flag, value]).unwrap();
        assert!(parsed.options.validate().is_err());
    }
}
