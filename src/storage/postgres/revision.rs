use diesel::backend::Backend;
use diesel::deserialize::{FromSql, Result as DeserializeResult};
use diesel::pg::Pg;
use diesel::serialize::{Output, Result as SerializeResult, ToSql};
use diesel::sql_types::{BigInt, SingleValue};

use crate::models::ResourceRevision;

impl<DB, ST> diesel::deserialize::Queryable<ST, DB> for ResourceRevision
where
    DB: Backend,
    ST: SingleValue,
    Self: FromSql<ST, DB>,
{
    type Row = Self;

    fn build(row: Self::Row) -> DeserializeResult<Self> {
        Ok(row)
    }
}

impl ToSql<BigInt, Pg> for ResourceRevision {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, Pg>) -> SerializeResult {
        <i64 as ToSql<BigInt, Pg>>::to_sql(&self.get(), &mut out.reborrow())
    }
}

impl<DB> FromSql<BigInt, DB> for ResourceRevision
where
    DB: Backend,
    i64: FromSql<BigInt, DB>,
{
    fn from_sql(bytes: DB::RawValue<'_>) -> DeserializeResult<Self> {
        let value = i64::from_sql(bytes)?;
        ResourceRevision::new(value)
            .map_err(|_| "resource revision must be greater than zero".into())
    }
}
