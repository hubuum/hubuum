use serde::{Deserialize, Serialize};
use std::fmt;
#[cfg(feature = "openapi")]
use utoipa::{
    ToSchema,
    openapi::{
        KnownFormat, ObjectBuilder, RefOr, SchemaFormat,
        schema::{Schema, Type},
    },
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationLimitError(String);
impl fmt::Display for RelationLimitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::error::Error for RelationLimitError {}

/// Maximum number of object relations allowed for one object on one side of a
/// class relation.
#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
pub struct ObjectRelationLimit(i32);

impl ObjectRelationLimit {
    /// Create a positive object-relation limit.
    pub fn new(value: i32) -> Result<Self, RelationLimitError> {
        if value <= 0 {
            return Err(RelationLimitError(format!(
                "Invalid object relation limit '{value}': must be a positive integer"
            )));
        }
        Ok(Self(value))
    }

    /// Return the underlying positive limit.
    pub fn value(self) -> i32 {
        self.0
    }
}

impl<'de> Deserialize<'de> for ObjectRelationLimit {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = i32::deserialize(deserializer)?;
        Self::new(value).map_err(serde::de::Error::custom)
    }
}

#[cfg(feature = "openapi")]
impl utoipa::PartialSchema for ObjectRelationLimit {
    fn schema() -> RefOr<Schema> {
        ObjectBuilder::new()
            .schema_type(Type::Integer)
            .format(Some(SchemaFormat::KnownFormat(KnownFormat::Int32)))
            .minimum(Some(1))
            .description(Some(
                "Maximum number of object relations allowed for one object on one side of a class relation.",
            ))
            .into()
    }
}

#[cfg(feature = "openapi")]
impl ToSchema for ObjectRelationLimit {}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    #[rstest]
    #[case(0)]
    #[case(-1)]
    fn deserialization_rejects_non_positive_limits(#[case] value: i32) {
        assert!(serde_json::from_value::<ObjectRelationLimit>(serde_json::json!(value)).is_err());
    }
}
