use std::fmt;

use diesel::backend::Backend;
use diesel::deserialize::{FromSql, Result as DeserializeResult};
use diesel::pg::Pg;
use diesel::serialize::{Output, Result as SerializeResult, ToSql};
use diesel::sql_types::BigInt;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use utoipa::openapi::schema::{Schema, Type};
use utoipa::openapi::{KnownFormat, ObjectBuilder, RefOr, SchemaFormat};

use crate::errors::ApiError;

/// Database-owned positive version of an authoritative resource.
///
/// The representation is intentionally a JSON integer. It is suitable for
/// imports, exports, queries, and event identity, but HTTP clients should use
/// the opaque ETag returned by canonical point responses for `If-Match`.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    diesel::AsExpression,
    diesel::FromSqlRow,
)]
#[serde(try_from = "i64", into = "i64")]
#[diesel(sql_type = BigInt)]
pub struct ResourceRevision(i64);

impl ResourceRevision {
    pub const INITIAL: Self = Self(1);

    pub fn new(value: i64) -> Result<Self, ApiError> {
        Self::try_from(value)
    }

    pub const fn get(self) -> i64 {
        self.0
    }

    pub fn checked_advance(self) -> Result<Self, ApiError> {
        self.0
            .checked_add(1)
            .ok_or_else(|| {
                ApiError::Conflict(
                    "Resource revision cannot advance beyond the maximum 64-bit value".to_string(),
                )
            })
            .and_then(Self::new)
    }
}

impl Default for ResourceRevision {
    fn default() -> Self {
        Self::INITIAL
    }
}

impl TryFrom<i64> for ResourceRevision {
    type Error = ApiError;

    fn try_from(value: i64) -> Result<Self, Self::Error> {
        if value <= 0 {
            return Err(ApiError::BadRequest(
                "Resource revision must be greater than zero".to_string(),
            ));
        }
        Ok(Self(value))
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

impl utoipa::PartialSchema for ResourceRevision {
    fn schema() -> RefOr<Schema> {
        ObjectBuilder::new()
            .schema_type(Type::Integer)
            .format(Some(SchemaFormat::KnownFormat(KnownFormat::Int64)))
            .minimum(Some(1))
            .description(Some("Database-owned positive resource revision."))
            .into()
    }
}

impl ToSchema for ResourceRevision {}

impl ToSql<BigInt, Pg> for ResourceRevision {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, Pg>) -> SerializeResult {
        <i64 as ToSql<BigInt, Pg>>::to_sql(&self.0, out)
    }
}

impl<DB> FromSql<BigInt, DB> for ResourceRevision
where
    DB: Backend,
    i64: FromSql<BigInt, DB>,
{
    fn from_sql(bytes: DB::RawValue<'_>) -> DeserializeResult<Self> {
        let value = i64::from_sql(bytes)?;
        if value <= 0 {
            return Err("resource revision must be greater than zero".into());
        }
        Ok(Self(value))
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::ResourceRevision;

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
        assert!(ResourceRevision::new(0).is_err());
        assert!(ResourceRevision::new(-1).is_err());
        assert!(serde_json::from_value::<ResourceRevision>(json!(0)).is_err());
    }

    #[test]
    fn revision_advancement_is_checked() {
        assert_eq!(
            ResourceRevision::INITIAL.checked_advance().unwrap().get(),
            2
        );
        assert!(
            ResourceRevision::new(i64::MAX)
                .unwrap()
                .checked_advance()
                .is_err()
        );
    }
}
