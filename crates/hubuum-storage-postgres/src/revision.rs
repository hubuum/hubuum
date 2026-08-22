use diesel::backend::Backend;
use diesel::deserialize::{FromSql, Result as DeserializeResult};
use diesel::pg::Pg;
use diesel::serialize::{Output, Result as SerializeResult, ToSql};
use diesel::sql_types::BigInt;
use hubuum_domain::{ResourceId, ResourceRevision, ResourceRevisionError};
use hubuum_storage_core::{StorageRecordMetadata, StorageRevisionTarget};

use crate::PostgresStorageError;

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
    ClassRelation,
    Collection,
    CollectionPermissions,
    ComputedField,
    EventSink,
    EventSubscription,
    ExportTemplate,
    Group,
    IdentityScope,
    Object,
    ObjectRelation,
    Principal,
    RemoteTarget,
    Token,
}

impl RevisionOwner {
    const fn table_name(self) -> &'static str {
        match self {
            Self::Class => "hubuumclass",
            Self::ClassRelation => "hubuumclass_relation",
            Self::Collection => "collections",
            Self::CollectionPermissions => "collection_authorization_state",
            Self::ComputedField => "computed_field_definitions",
            Self::EventSink => "event_sinks",
            Self::EventSubscription => "event_subscriptions",
            Self::ExportTemplate => "export_templates",
            Self::Group => "groups",
            Self::IdentityScope => "identity_scopes",
            Self::Object => "hubuumobject",
            Self::ObjectRelation => "hubuumobject_relation",
            Self::Principal => "principals",
            Self::RemoteTarget => "remote_targets",
            Self::Token => "tokens",
        }
    }

    pub(crate) fn key(self, resource_id: i32) -> String {
        format!("{}:{resource_id}", self.table_name())
    }

    pub(crate) fn membership_key(principal_id: i32, group_id: i32) -> String {
        format!("group_memberships:{principal_id}:{group_id}")
    }
}

pub(crate) fn revision_owner_key(target: StorageRevisionTarget) -> String {
    match target {
        StorageRevisionTarget::IdentityScope(id) => RevisionOwner::IdentityScope.key(id.id()),
        StorageRevisionTarget::Group(id) => RevisionOwner::Group.key(id.id()),
        StorageRevisionTarget::Principal(id) => RevisionOwner::Principal.key(id.id()),
        StorageRevisionTarget::Membership {
            principal_id,
            group_id,
        } => RevisionOwner::membership_key(principal_id.id(), group_id.id()),
        StorageRevisionTarget::Collection(id) => RevisionOwner::Collection.key(id.id()),
        StorageRevisionTarget::CollectionPermissions(id) => {
            RevisionOwner::CollectionPermissions.key(id.id())
        }
        StorageRevisionTarget::Class(id) => RevisionOwner::Class.key(id.id()),
        StorageRevisionTarget::Object(id) => RevisionOwner::Object.key(id.id()),
        StorageRevisionTarget::ClassRelation(id) => RevisionOwner::ClassRelation.key(id.id()),
        StorageRevisionTarget::ObjectRelation(id) => RevisionOwner::ObjectRelation.key(id.id()),
        StorageRevisionTarget::ExportTemplate(id) => RevisionOwner::ExportTemplate.key(id.id()),
        StorageRevisionTarget::RemoteTarget(id) => RevisionOwner::RemoteTarget.key(id.id()),
        StorageRevisionTarget::EventSink(id) => RevisionOwner::EventSink.key(id.id()),
        StorageRevisionTarget::EventSubscription(id) => {
            RevisionOwner::EventSubscription.key(id.id())
        }
        StorageRevisionTarget::ComputedField(id) => RevisionOwner::ComputedField.key(id.id()),
        StorageRevisionTarget::Token(id) => RevisionOwner::Token.key(id.id()),
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

pub(crate) fn record_metadata(
    id: i32,
    created_at: chrono::NaiveDateTime,
    updated_at: chrono::NaiveDateTime,
    revision: PostgresRevision,
) -> Result<StorageRecordMetadata, PostgresStorageError> {
    StorageRecordMetadata::try_new(
        ResourceId::new(id)?,
        created_at.and_utc(),
        updated_at.and_utc(),
        revision.into_domain(),
    )
    .map_err(PostgresStorageError::from)
}

pub(crate) fn record_metadata_from_raw_revision(
    id: i32,
    created_at: chrono::NaiveDateTime,
    updated_at: chrono::NaiveDateTime,
    revision: i64,
) -> Result<StorageRecordMetadata, PostgresStorageError> {
    let revision = PostgresRevision::new(revision).map_err(|error| {
        PostgresStorageError::database(format!("Invalid persisted resource revision: {error}"))
    })?;
    record_metadata(id, created_at, updated_at, revision)
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
