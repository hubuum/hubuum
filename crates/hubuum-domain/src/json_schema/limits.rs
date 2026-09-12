use std::fmt;

/// Validated deployment budgets carried with each compiled schema.
/// Structural, reference-policy, numeric and regex guards remain independent.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct JsonSchemaLimits {
    schema_bytes: usize,
    expanded_work: usize,
    instance_bytes: usize,
    instance_work: usize,
}

impl JsonSchemaLimits {
    pub const MAX_SCHEMA_BYTES: usize = 1024 * 1024;
    pub const MAX_EXPANDED_WORK: usize = 65_536;
    pub const MAX_INSTANCE_BYTES: usize = 16 * 1024 * 1024;
    pub const MAX_INSTANCE_WORK: usize = 1024 * 1024 * 1024;
    pub const DEFAULT: Self = Self {
        schema_bytes: 65_536,
        expanded_work: 16_384,
        instance_bytes: 2 * 1024 * 1024,
        instance_work: 256 * 1024 * 1024,
    };

    #[must_use]
    pub const fn builder() -> JsonSchemaLimitsBuilder {
        JsonSchemaLimitsBuilder(Self::DEFAULT)
    }

    #[must_use]
    pub const fn schema_bytes(self) -> usize {
        self.schema_bytes
    }

    #[must_use]
    pub const fn expanded_work(self) -> usize {
        self.expanded_work
    }

    #[must_use]
    pub const fn instance_bytes(self) -> usize {
        self.instance_bytes
    }

    #[must_use]
    pub const fn instance_work(self) -> usize {
        self.instance_work
    }
}

impl Default for JsonSchemaLimits {
    fn default() -> Self {
        Self::DEFAULT
    }
}

/// Builds budgets without exposing an unchecked configuration as a proof.
pub struct JsonSchemaLimitsBuilder(JsonSchemaLimits);

impl JsonSchemaLimitsBuilder {
    #[must_use]
    pub const fn schema_bytes(mut self, value: usize) -> Self {
        self.0.schema_bytes = value;
        self
    }

    #[must_use]
    pub const fn expanded_work(mut self, value: usize) -> Self {
        self.0.expanded_work = value;
        self
    }

    #[must_use]
    pub const fn instance_bytes(mut self, value: usize) -> Self {
        self.0.instance_bytes = value;
        self
    }

    #[must_use]
    pub const fn instance_work(mut self, value: usize) -> Self {
        self.0.instance_work = value;
        self
    }

    pub fn build(self) -> Result<JsonSchemaLimits, JsonSchemaLimitsError> {
        for (name, value, minimum, maximum) in [
            (
                "schema bytes",
                self.0.schema_bytes,
                1024,
                JsonSchemaLimits::MAX_SCHEMA_BYTES,
            ),
            (
                "expanded work",
                self.0.expanded_work,
                1,
                JsonSchemaLimits::MAX_EXPANDED_WORK,
            ),
            (
                "instance bytes",
                self.0.instance_bytes,
                1024,
                JsonSchemaLimits::MAX_INSTANCE_BYTES,
            ),
            (
                "instance work",
                self.0.instance_work,
                1,
                JsonSchemaLimits::MAX_INSTANCE_WORK,
            ),
        ] {
            if !(minimum..=maximum).contains(&value) {
                return Err(JsonSchemaLimitsError(format!(
                    "JSON Schema {name} budget must be between {minimum} and {maximum}"
                )));
            }
        }
        Ok(self.0)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JsonSchemaLimitsError(String);

impl fmt::Display for JsonSchemaLimitsError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for JsonSchemaLimitsError {}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    #[rstest]
    #[case(JsonSchemaLimits::builder().schema_bytes(1023))]
    #[case(JsonSchemaLimits::builder().schema_bytes(JsonSchemaLimits::MAX_SCHEMA_BYTES + 1))]
    #[case(JsonSchemaLimits::builder().expanded_work(0))]
    #[case(JsonSchemaLimits::builder().expanded_work(JsonSchemaLimits::MAX_EXPANDED_WORK + 1))]
    #[case(JsonSchemaLimits::builder().instance_bytes(1023))]
    #[case(JsonSchemaLimits::builder().instance_bytes(JsonSchemaLimits::MAX_INSTANCE_BYTES + 1))]
    #[case(JsonSchemaLimits::builder().instance_work(0))]
    #[case(JsonSchemaLimits::builder().instance_work(JsonSchemaLimits::MAX_INSTANCE_WORK + 1))]
    fn invalid_deployment_budgets_are_rejected(#[case] builder: JsonSchemaLimitsBuilder) {
        assert!(builder.build().is_err());
    }
}
