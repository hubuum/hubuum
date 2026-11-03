//! Scoped retention decisions. Original history clocks and captured attribution
//! are inputs; callers must never substitute current class/collection membership.

use chrono::{DateTime, Utc};
use hubuum_domain::{PrincipalIdentity, ResourceIdentity};
use serde::{Deserialize, Serialize};

use crate::{StorageIdentityHistoryKind, StorageValidationError};

mod online_journal;
pub use online_journal::*;
mod online_intent;
pub use online_intent::*;
mod online;
pub use online::*;
mod publication;
pub use publication::*;
mod audit;
mod scan;
pub use scan::*;
mod control;
pub use control::*;
mod authority;
pub use authority::*;
mod policy;
pub use policy::*;
mod resolution;
pub use resolution::*;
#[cfg(test)]
mod tests;

fn invalid(message: &str) -> StorageValidationError {
    StorageValidationError::invalid(message)
}

macro_rules! decimal {
    ($name:ident, $max:expr) => {
        #[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
        #[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
        #[serde(try_from = "String", into = "String")]
        #[cfg_attr(feature = "openapi", schema(value_type = String, pattern = "^(0|[1-9][0-9]*)$"))]
        pub struct $name(u64);
        impl $name {
            pub fn new(value: u64) -> Result<Self, StorageValidationError> {
                if value > $max {
                    return Err(invalid(concat!(
                        stringify!($name),
                        " exceeds its supported range"
                    )));
                }
                Ok(Self(value))
            }
            pub const fn value(self) -> u64 {
                self.0
            }
        }
        impl TryFrom<String> for $name {
            type Error = StorageValidationError;
            fn try_from(value: String) -> Result<Self, Self::Error> {
                if value.is_empty()
                    || value.len() > 20
                    || !value.bytes().all(|byte| byte.is_ascii_digit())
                    || (value.len() > 1 && value.starts_with('0'))
                {
                    return Err(invalid("Expected a canonical unsigned decimal string"));
                }
                Self::new(
                    value
                        .parse()
                        .map_err(|_| invalid("Decimal value is out of range"))?,
                )
            }
        }
        impl From<$name> for String {
            fn from(value: $name) -> Self {
                value.0.to_string()
            }
        }
    };
}
decimal!(HistoryPolicyRevision, i64::MAX as u64);
decimal!(HistoryAuthorityRevision, i64::MAX as u64);
decimal!(HistoryDeletionRevision, i64::MAX as u64);
// Bounds keep addition representable for ordinary supported UTC history clocks.
decimal!(HistoryRetentionSeconds, 315_360_000);
decimal!(HistoryLogicalBytes, i64::MAX as u64);
decimal!(HistoryPublicationGeneration, i64::MAX as u64);

/// Stable scope, independent of replaceable integer compatibility IDs.
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum HistoryPolicyScope {
    Deployment,
    Collection {
        #[cfg_attr(feature = "openapi", schema(value_type = String, format = "uuid"))]
        identity: ResourceIdentity,
    },
    Class {
        #[cfg_attr(feature = "openapi", schema(value_type = String, format = "uuid"))]
        identity: ResourceIdentity,
    },
}

#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(try_from = "KeyInput")]
pub struct HistoryPolicyKey {
    family: StorageIdentityHistoryKind,
    scope: HistoryPolicyScope,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct KeyInput {
    family: StorageIdentityHistoryKind,
    scope: HistoryPolicyScope,
}
impl HistoryPolicyKey {
    pub fn new(
        family: StorageIdentityHistoryKind,
        scope: HistoryPolicyScope,
    ) -> Result<Self, StorageValidationError> {
        if family == StorageIdentityHistoryKind::Collection
            && matches!(scope, HistoryPolicyScope::Class { .. })
        {
            return Err(invalid("Class policies cannot govern collection history"));
        }
        Ok(Self { family, scope })
    }
    pub const fn family(self) -> StorageIdentityHistoryKind {
        self.family
    }
    pub const fn scope(self) -> HistoryPolicyScope {
        self.scope
    }
}
impl TryFrom<KeyInput> for HistoryPolicyKey {
    type Error = StorageValidationError;
    fn try_from(value: KeyInput) -> Result<Self, Self::Error> {
        Self::new(value.family, value.scope)
    }
}

/// Attribution captured with the historical version. Relations have two of
/// these; unresolved attribution is represented separately, never guessed.
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct HistoryScopeAttribution {
    #[cfg_attr(feature = "openapi", schema(value_type = String, format = "uuid"))]
    collection: ResourceIdentity,
    #[cfg_attr(feature = "openapi", schema(value_type = Option<String>, format = "uuid"))]
    class: Option<ResourceIdentity>,
}
impl HistoryScopeAttribution {
    pub const fn new(collection: ResourceIdentity, class: Option<ResourceIdentity>) -> Self {
        Self { collection, class }
    }
    pub const fn collection(self) -> ResourceIdentity {
        self.collection
    }
    pub const fn class(self) -> Option<ResourceIdentity> {
        self.class
    }
    pub fn matches(self, scope: HistoryPolicyScope) -> bool {
        match scope {
            HistoryPolicyScope::Deployment => true,
            HistoryPolicyScope::Collection { identity } => identity == self.collection,
            HistoryPolicyScope::Class { identity } => Some(identity) == self.class,
        }
    }
}

/// A producer's declared reader/publication protocol, not evidence that a
/// stopped or missing producer is quiescent. Registrations never expire by time.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum HistoryRuntimeProtocol {
    Legacy,
    OnlineV1,
}
impl HistoryRuntimeProtocol {
    pub const fn supports_online(self) -> bool {
        matches!(self, Self::OnlineV1)
    }
}

/// Occurrence and tombstone anchors are explicit; no closed interval is invented
/// for schema events or deletion records.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", content = "at", rename_all = "snake_case")]
pub enum HistoryRetentionAnchor {
    Closed(DateTime<Utc>),
    Deleted(DateTime<Utc>),
    SchemaEvent(DateTime<Utc>),
}
impl HistoryRetentionAnchor {
    pub const fn at(self) -> DateTime<Utc> {
        match self {
            Self::Closed(at) | Self::Deleted(at) | Self::SchemaEvent(at) => at,
        }
    }
}

/// Exact immutable policy revision and its governing principal.
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(try_from = "RevisionInput")]
pub struct HistoryPolicyRecord {
    #[cfg_attr(feature = "openapi", schema(value_type = String, format = "uuid"))]
    operation: uuid::Uuid,
    review: Option<HistoryPolicyReviewEvidence>,
    revision: HistoryPolicyRevision,
    proposal: HistoryPolicyChange,
    #[cfg_attr(feature = "openapi", schema(value_type = String, format = DateTime))]
    applied_at: DateTime<Utc>,
    #[cfg_attr(feature = "openapi", schema(value_type = String, format = "uuid"))]
    principal: PrincipalIdentity,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RevisionInput {
    operation: uuid::Uuid,
    review: Option<HistoryPolicyReviewEvidence>,
    revision: HistoryPolicyRevision,
    proposal: HistoryPolicyChange,
    applied_at: DateTime<Utc>,
    principal: PrincipalIdentity,
}
impl HistoryPolicyRecord {
    pub fn new(
        revision: HistoryPolicyRevision,
        proposal: HistoryPolicyChange,
        applied_at: DateTime<Utc>,
        principal: PrincipalIdentity,
    ) -> Result<Self, StorageValidationError> {
        if revision.value() == 0 || revision <= proposal.expected_policy_revision() {
            return Err(invalid(
                "An applied policy requires a new positive revision",
            ));
        }
        proposal.validate_at(applied_at)?;
        Ok(Self {
            operation: uuid::Uuid::new_v4(),
            review: None,
            revision,
            proposal,
            applied_at,
            principal,
        })
    }
    pub const fn operation(&self) -> uuid::Uuid {
        self.operation
    }
    pub const fn review(&self) -> Option<&HistoryPolicyReviewEvidence> {
        self.review.as_ref()
    }
    pub(super) fn bind_review(
        mut self,
        review: HistoryPolicyReviewEvidence,
    ) -> Result<Self, StorageValidationError> {
        review.validate(&self.proposal, self.principal, self.applied_at)?;
        self.review = Some(review);
        Ok(self)
    }
    pub const fn revision(&self) -> HistoryPolicyRevision {
        self.revision
    }
    pub const fn proposal(&self) -> &HistoryPolicyChange {
        &self.proposal
    }
    pub const fn applied_at(&self) -> DateTime<Utc> {
        self.applied_at
    }
    pub const fn principal(&self) -> PrincipalIdentity {
        self.principal
    }
    pub fn governs(&self, anchor: HistoryRetentionAnchor) -> bool {
        anchor.at() > self.applied_at
            || match self.proposal.application() {
                HistoryPolicyApplication::Prospective => false,
                HistoryPolicyApplication::Retrospective { closed_through } => {
                    anchor.at() <= *closed_through
                }
            }
    }
}
impl TryFrom<RevisionInput> for HistoryPolicyRecord {
    type Error = StorageValidationError;
    fn try_from(value: RevisionInput) -> Result<Self, Self::Error> {
        if value.operation.get_variant() != uuid::Variant::RFC4122
            || value.operation.get_version().is_none()
        {
            return Err(invalid("Policy operation must be a non-nil RFC UUID"));
        }
        let mut record = Self::new(
            value.revision,
            value.proposal,
            value.applied_at,
            value.principal,
        )?;
        record.operation = value.operation;
        if let Some(review) = value.review {
            record = record.bind_review(review)?;
        }
        Ok(record)
    }
}

fn deserialize_utc<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<DateTime<Utc>, D::Error> {
    let value = String::deserialize(deserializer)?;
    if !(value.ends_with('Z') || value.ends_with("+00:00")) {
        return Err(serde::de::Error::custom(
            "Timestamp must use RFC 3339 UTC (Z or +00:00)",
        ));
    }
    DateTime::parse_from_rfc3339(&value)
        .map(|at| at.with_timezone(&Utc))
        .map_err(serde::de::Error::custom)
}

#[cfg(feature = "openapi")]
impl utoipa::PartialSchema for HistoryRetentionAnchor {
    fn schema() -> utoipa::openapi::RefOr<utoipa::openapi::schema::Schema> {
        use utoipa::openapi::schema::{KnownFormat, ObjectBuilder, SchemaFormat, SchemaType, Type};
        ObjectBuilder::new()
            .property(
                "kind",
                ObjectBuilder::new()
                    .schema_type(SchemaType::Type(Type::String))
                    .enum_values(Some(["closed", "deleted", "schema_event"])),
            )
            .property(
                "at",
                ObjectBuilder::new()
                    .schema_type(SchemaType::Type(Type::String))
                    .format(Some(SchemaFormat::KnownFormat(KnownFormat::DateTime))),
            )
            .required("kind")
            .required("at")
            .into()
    }
}
#[cfg(feature = "openapi")]
impl utoipa::ToSchema for HistoryRetentionAnchor {}

fn deserialize_present_age<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<HistoryRetentionSeconds>, D::Error> {
    HistoryRetentionSeconds::deserialize(deserializer).map(Some)
}

fn serialize_count<S: serde::Serializer>(value: &u32, serializer: S) -> Result<S::Ok, S::Error> {
    serializer.serialize_str(&value.to_string())
}
fn deserialize_count<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<u32, D::Error> {
    let value = String::deserialize(deserializer)?;
    if value.is_empty()
        || !value.bytes().all(|byte| byte.is_ascii_digit())
        || (value.len() > 1 && value.starts_with('0'))
    {
        return Err(serde::de::Error::custom(
            "Expected a canonical unsigned decimal count",
        ));
    }
    value.parse().map_err(serde::de::Error::custom)
}
