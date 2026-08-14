use std::fmt;

use serde::{Deserialize, Serialize};

/// Failure to construct or advance a resource revision.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ResourceRevisionError {
    /// Revisions are positive and the supplied value was zero or negative.
    NonPositive,
    /// The revision cannot advance beyond the maximum signed 64-bit value.
    Overflow,
}

impl fmt::Display for ResourceRevisionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::NonPositive => "resource revision must be greater than zero",
            Self::Overflow => "resource revision cannot advance beyond the maximum 64-bit value",
        })
    }
}

impl std::error::Error for ResourceRevisionError {}

/// Database-owned positive version of an authoritative resource.
///
/// The representation is a JSON integer when serialized. It is suitable for
/// imports, exports, queries, and event identity. HTTP clients should use the
/// opaque ETag returned by canonical point responses for `If-Match`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "i64", into = "i64")]
pub struct ResourceRevision(i64);

impl ResourceRevision {
    /// First persisted revision of a resource.
    pub const INITIAL: Self = Self(1);

    /// Validate a persisted revision value.
    pub const fn new(value: i64) -> Result<Self, ResourceRevisionError> {
        if value <= 0 {
            return Err(ResourceRevisionError::NonPositive);
        }
        Ok(Self(value))
    }

    /// Return the wire and database representation.
    #[must_use]
    pub const fn get(self) -> i64 {
        self.0
    }

    /// Advance by one without permitting signed integer overflow.
    pub const fn checked_advance(self) -> Result<Self, ResourceRevisionError> {
        match self.0.checked_add(1) {
            Some(value) => Self::new(value),
            None => Err(ResourceRevisionError::Overflow),
        }
    }
}

impl Default for ResourceRevision {
    fn default() -> Self {
        Self::INITIAL
    }
}

impl TryFrom<i64> for ResourceRevision {
    type Error = ResourceRevisionError;

    fn try_from(value: i64) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<ResourceRevision> for i64 {
    fn from(value: ResourceRevision) -> Self {
        value.0
    }
}

impl fmt::Display for ResourceRevision {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

#[cfg(feature = "openapi")]
impl utoipa::PartialSchema for ResourceRevision {
    fn schema() -> utoipa::openapi::RefOr<utoipa::openapi::schema::Schema> {
        use utoipa::openapi::schema::{SchemaFormat, Type};
        use utoipa::openapi::{KnownFormat, ObjectBuilder};

        ObjectBuilder::new()
            .schema_type(Type::Integer)
            .format(Some(SchemaFormat::KnownFormat(KnownFormat::Int64)))
            .minimum(Some(1))
            .description(Some("Database-owned positive resource revision."))
            .into()
    }
}

#[cfg(feature = "openapi")]
impl utoipa::ToSchema for ResourceRevision {}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{ResourceRevision, ResourceRevisionError};

    #[test]
    fn revision_is_a_transparent_json_integer() {
        let revision = ResourceRevision::new(17).unwrap();
        assert_eq!(serde_json::to_value(revision).unwrap(), json!(17));
        assert_eq!(
            serde_json::from_value::<ResourceRevision>(json!(17)).unwrap(),
            revision
        );
    }

    #[test]
    fn revision_rejects_non_positive_values() {
        assert_eq!(
            ResourceRevision::new(0),
            Err(ResourceRevisionError::NonPositive)
        );
        assert_eq!(
            ResourceRevision::new(-1),
            Err(ResourceRevisionError::NonPositive)
        );
        assert!(serde_json::from_value::<ResourceRevision>(json!(0)).is_err());
    }

    #[test]
    fn revision_advancement_is_checked() {
        assert_eq!(
            ResourceRevision::INITIAL.checked_advance().unwrap().get(),
            2
        );
        assert_eq!(
            ResourceRevision::new(i64::MAX).unwrap().checked_advance(),
            Err(ResourceRevisionError::Overflow)
        );
    }
}
