use diesel::backend::Backend;
use diesel::deserialize::{FromSql, Result as DeserializeResult};
use diesel::pg::Pg;
use diesel::serialize::{Output, Result as SerializeResult, ToSql};
use diesel::sql_types::BigInt;
use hubuum_domain::{ResourceRevision, ResourceRevisionError};

/// Adapter-private Diesel representation of a domain resource revision.
///
/// Query rows use this local newtype so neither Diesel traits nor PostgreSQL
/// types become part of `hubuum-domain`'s public contract.
#[doc(hidden)]
#[derive(
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    diesel::AsExpression,
    diesel::FromSqlRow,
)]
#[diesel(sql_type = BigInt)]
#[serde(transparent)]
pub struct PostgresRevision(ResourceRevision);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RevisionOwner {
    Class,
    Collection,
    CollectionPermissions,
    EventSink,
    EventSubscription,
    RemoteTarget,
}

impl RevisionOwner {
    const fn table_name(self) -> &'static str {
        match self {
            Self::Class => "hubuumclass",
            Self::Collection => "collections",
            Self::CollectionPermissions => "collection_permissions",
            Self::EventSink => "event_sinks",
            Self::EventSubscription => "event_subscriptions",
            Self::RemoteTarget => "remote_targets",
        }
    }

    pub(crate) fn key(self, resource_id: i32) -> String {
        format!("{}:{resource_id}", self.table_name())
    }
}

impl PostgresRevision {
    pub const INITIAL: Self = Self(ResourceRevision::INITIAL);

    pub const fn new(value: i64) -> Result<Self, ResourceRevisionError> {
        match ResourceRevision::new(value) {
            Ok(revision) => Ok(Self(revision)),
            Err(error) => Err(error),
        }
    }

    #[must_use]
    pub const fn get(self) -> i64 {
        self.0.get()
    }

    #[must_use]
    pub const fn into_domain(self) -> ResourceRevision {
        self.0
    }
}

impl From<ResourceRevision> for PostgresRevision {
    fn from(value: ResourceRevision) -> Self {
        Self(value)
    }
}

impl From<PostgresRevision> for ResourceRevision {
    fn from(value: PostgresRevision) -> Self {
        value.0
    }
}

impl std::fmt::Display for PostgresRevision {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(formatter)
    }
}

impl ToSql<BigInt, Pg> for PostgresRevision {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, Pg>) -> SerializeResult {
        <i64 as ToSql<BigInt, Pg>>::to_sql(&self.get(), &mut out.reborrow())
    }
}

impl<DB> FromSql<BigInt, DB> for PostgresRevision
where
    DB: Backend,
    i64: FromSql<BigInt, DB>,
{
    fn from_sql(bytes: DB::RawValue<'_>) -> DeserializeResult<Self> {
        let value = i64::from_sql(bytes)?;
        Self::new(value).map_err(Into::into)
    }
}
