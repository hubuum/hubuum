use std::fmt;

use actix_web::HttpRequest;
use actix_web::http::header;
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use hubuum_domain::{
    ClassId, ClassRelationId, CollectionId, ComputedFieldDefinitionId, EventSinkId,
    EventSubscriptionId, ExportTemplateId, GroupId, IdentityScopeId, ObjectId, ObjectRelationId,
    PositiveIdError, PrincipalId, RemoteTargetId, TokenId,
};

use crate::errors::ApiError;
use crate::models::ResourceRevision;
use crate::storage::{StorageRevisionPrecondition, StorageRevisionTarget};

const ETAG_PREFIX: &str = "hubuum-v1";
const MAX_IF_MATCH_BYTES: usize = 2 * 1024;
const MAX_IF_MATCH_VALIDATORS: usize = 8;
const MAX_RESOURCE_KEY_BYTES: usize = 256;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum EtagResourceKind {
    IdentityScope,
    Group,
    Principal,
    User,
    ServiceAccount,
    PrincipalSettings,
    Membership,
    Collection,
    CollectionPermissions,
    Class,
    Object,
    ClassRelation,
    ObjectRelation,
    ExportTemplate,
    RemoteTarget,
    EventSink,
    EventSubscription,
    ComputedField,
    Token,
}

impl EtagResourceKind {
    const fn as_str(self) -> &'static str {
        match self {
            Self::IdentityScope => "identity_scope",
            Self::Group => "group",
            Self::Principal => "principal",
            Self::User => "user",
            Self::ServiceAccount => "service_account",
            Self::PrincipalSettings => "principal_settings",
            Self::Membership => "membership",
            Self::Collection => "collection",
            Self::CollectionPermissions => "collection_permissions",
            Self::Class => "class",
            Self::Object => "object",
            Self::ClassRelation => "class_relation",
            Self::ObjectRelation => "object_relation",
            Self::ExportTemplate => "export_template",
            Self::RemoteTarget => "remote_target",
            Self::EventSink => "event_sink",
            Self::EventSubscription => "event_subscription",
            Self::ComputedField => "computed_field",
            Self::Token => "token",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        Some(match value {
            "identity_scope" => Self::IdentityScope,
            "group" => Self::Group,
            "principal" => Self::Principal,
            "user" => Self::User,
            "service_account" => Self::ServiceAccount,
            "principal_settings" => Self::PrincipalSettings,
            "membership" => Self::Membership,
            "collection" => Self::Collection,
            "collection_permissions" => Self::CollectionPermissions,
            "class" => Self::Class,
            "object" => Self::Object,
            "class_relation" => Self::ClassRelation,
            "object_relation" => Self::ObjectRelation,
            "export_template" => Self::ExportTemplate,
            "remote_target" => Self::RemoteTarget,
            "event_sink" => Self::EventSink,
            "event_subscription" => Self::EventSubscription,
            "computed_field" => Self::ComputedField,
            "token" => Self::Token,
            _ => return None,
        })
    }
}

fn parse_resource_id<T>(
    key: &str,
    constructor: impl FnOnce(i32) -> Result<T, PositiveIdError>,
) -> Result<T, ApiError> {
    let id = key.parse::<i32>().map_err(|_| {
        ApiError::InternalServerError("ETag resource key is not a valid identifier".to_string())
    })?;
    constructor(id).map_err(|error| ApiError::InternalServerError(error.to_string()))
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct EntityTag {
    kind: EtagResourceKind,
    key: String,
    revision: ResourceRevision,
}

impl EntityTag {
    fn new(
        kind: EtagResourceKind,
        key: impl Into<String>,
        revision: ResourceRevision,
    ) -> Result<Self, ApiError> {
        let key = key.into();
        if key.is_empty() || key.len() > MAX_RESOURCE_KEY_BYTES {
            return Err(ApiError::InternalServerError(
                "ETag resource key is outside its bounded size".to_string(),
            ));
        }
        Ok(Self {
            kind,
            key,
            revision,
        })
    }

    fn for_id(
        kind: EtagResourceKind,
        id: i32,
        revision: ResourceRevision,
    ) -> Result<Self, ApiError> {
        if id <= 0 {
            return Err(ApiError::InternalServerError(
                "ETag resource id must be greater than zero".to_string(),
            ));
        }
        Self::new(kind, id.to_string(), revision)
    }

    fn revision(&self) -> ResourceRevision {
        self.revision
    }

    fn parse(value: &str) -> Result<Self, ApiError> {
        if value.starts_with("W/") || value.starts_with("w/") {
            return Err(invalid_if_match("weak validators are not supported"));
        }
        let inner = value
            .strip_prefix('"')
            .and_then(|value| value.strip_suffix('"'))
            .ok_or_else(|| invalid_if_match("validators must be quoted"))?;
        if inner.contains('"') {
            return Err(invalid_if_match("validator contains an invalid quote"));
        }
        let mut parts = inner.split('.');
        if parts.next() != Some(ETAG_PREFIX) {
            return Err(invalid_if_match("validator has an unsupported format"));
        }
        let kind = parts
            .next()
            .and_then(EtagResourceKind::parse)
            .ok_or_else(|| invalid_if_match("validator has an unknown resource kind"))?;
        let encoded_key = parts
            .next()
            .ok_or_else(|| invalid_if_match("validator is missing its resource key"))?;
        let revision = parts
            .next()
            .ok_or_else(|| invalid_if_match("validator is missing its revision"))?;
        if parts.next().is_some() || encoded_key.is_empty() {
            return Err(invalid_if_match("validator is malformed"));
        }
        let key = URL_SAFE_NO_PAD
            .decode(encoded_key)
            .map_err(|_| invalid_if_match("validator resource key is malformed"))?;
        let key = String::from_utf8(key)
            .map_err(|_| invalid_if_match("validator resource key is malformed"))?;
        if key.is_empty() || key.len() > MAX_RESOURCE_KEY_BYTES {
            return Err(invalid_if_match(
                "validator resource key is outside its bounded size",
            ));
        }
        let revision = revision
            .parse::<i64>()
            .map_err(|_| invalid_if_match("validator revision is malformed"))?;
        let revision = ResourceRevision::new(revision)
            .map_err(|_| invalid_if_match("validator revision must be a positive int64"))?;
        Ok(Self {
            kind,
            key,
            revision,
        })
    }

    fn same_resource(&self, other: &Self) -> bool {
        self.kind == other.kind && self.key == other.key
    }

    fn revision_target(&self) -> Result<StorageRevisionTarget, ApiError> {
        Ok(match self.kind {
            EtagResourceKind::IdentityScope => StorageRevisionTarget::IdentityScope(
                parse_resource_id(&self.key, IdentityScopeId::new)?,
            ),
            EtagResourceKind::Group => {
                StorageRevisionTarget::Group(parse_resource_id(&self.key, GroupId::new)?)
            }
            EtagResourceKind::Principal
            | EtagResourceKind::User
            | EtagResourceKind::ServiceAccount
            | EtagResourceKind::PrincipalSettings => {
                StorageRevisionTarget::Principal(parse_resource_id(&self.key, PrincipalId::new)?)
            }
            EtagResourceKind::Membership => {
                let (principal_id, group_id) = self.key.split_once(':').ok_or_else(|| {
                    ApiError::InternalServerError("ETag membership key is malformed".to_string())
                })?;
                StorageRevisionTarget::Membership {
                    principal_id: parse_resource_id(principal_id, PrincipalId::new)?,
                    group_id: parse_resource_id(group_id, GroupId::new)?,
                }
            }
            EtagResourceKind::Collection => {
                StorageRevisionTarget::Collection(parse_resource_id(&self.key, CollectionId::new)?)
            }
            EtagResourceKind::CollectionPermissions => {
                StorageRevisionTarget::CollectionPermissions(parse_resource_id(
                    &self.key,
                    CollectionId::new,
                )?)
            }
            EtagResourceKind::Class => {
                StorageRevisionTarget::Class(parse_resource_id(&self.key, ClassId::new)?)
            }
            EtagResourceKind::Object => {
                StorageRevisionTarget::Object(parse_resource_id(&self.key, ObjectId::new)?)
            }
            EtagResourceKind::ClassRelation => StorageRevisionTarget::ClassRelation(
                parse_resource_id(&self.key, ClassRelationId::new)?,
            ),
            EtagResourceKind::ObjectRelation => StorageRevisionTarget::ObjectRelation(
                parse_resource_id(&self.key, ObjectRelationId::new)?,
            ),
            EtagResourceKind::ExportTemplate => StorageRevisionTarget::ExportTemplate(
                parse_resource_id(&self.key, ExportTemplateId::new)?,
            ),
            EtagResourceKind::RemoteTarget => StorageRevisionTarget::RemoteTarget(
                parse_resource_id(&self.key, RemoteTargetId::new)?,
            ),
            EtagResourceKind::EventSink => {
                StorageRevisionTarget::EventSink(parse_resource_id(&self.key, EventSinkId::new)?)
            }
            EtagResourceKind::EventSubscription => StorageRevisionTarget::EventSubscription(
                parse_resource_id(&self.key, EventSubscriptionId::new)?,
            ),
            EtagResourceKind::ComputedField => StorageRevisionTarget::ComputedField(
                parse_resource_id(&self.key, ComputedFieldDefinitionId::new)?,
            ),
            EtagResourceKind::Token => {
                StorageRevisionTarget::Token(parse_resource_id(&self.key, TokenId::new)?)
            }
        })
    }
}

impl fmt::Display for EntityTag {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "\"{ETAG_PREFIX}.{}.{}.{}\"",
            self.kind.as_str(),
            URL_SAFE_NO_PAD.encode(self.key.as_bytes()),
            self.revision
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IfMatchCondition {
    Missing,
    Any,
    Tags(Vec<EntityTag>),
}

pub type RevisionPrecondition = StorageRevisionPrecondition;

impl IfMatchCondition {
    pub fn from_request(request: &HttpRequest) -> Result<Self, ApiError> {
        let values = request.headers().get_all(header::IF_MATCH);
        let mut total_bytes = 0usize;
        let mut parts = Vec::new();
        for value in values {
            let value = value
                .to_str()
                .map_err(|_| invalid_if_match("header is not valid ASCII"))?;
            total_bytes = total_bytes.saturating_add(value.len());
            if total_bytes > MAX_IF_MATCH_BYTES {
                return Err(invalid_if_match("header exceeds 2048 bytes"));
            }
            parts.extend(value.split(',').map(str::trim));
        }
        if parts.is_empty() {
            return Ok(Self::Missing);
        }
        if parts.iter().any(|part| part.is_empty()) {
            return Err(invalid_if_match("header contains an empty validator"));
        }
        let has_wildcard = parts.contains(&"*");
        if has_wildcard {
            return if parts.len() == 1 {
                Ok(Self::Any)
            } else {
                Err(invalid_if_match(
                    "wildcard cannot be combined with validators",
                ))
            };
        }
        if parts.len() > MAX_IF_MATCH_VALIDATORS {
            return Err(invalid_if_match(
                "header contains more than eight validators",
            ));
        }
        parts
            .into_iter()
            .map(EntityTag::parse)
            .collect::<Result<Vec<_>, _>>()
            .map(Self::Tags)
    }

    /// Validate resource identity independently of freshness. This prevents a
    /// validator copied from another resource from being treated as a stale
    /// value and gives clients a stable 400 response for that programming error.
    fn ensure_compatible(&self, current: &EntityTag) -> Result<(), ApiError> {
        if let Self::Tags(tags) = self
            && tags.iter().any(|tag| !tag.same_resource(current))
        {
            return Err(invalid_if_match(
                "validator belongs to a different resource",
            ));
        }
        Ok(())
    }

    /// Convert a parsed condition into the database-owned revision assertion
    /// that will be evaluated under the authoritative row lock. Call this only
    /// after the handler has authorized access to `current`.
    pub fn database_precondition(
        &self,
        current: &EntityTag,
    ) -> Result<Option<RevisionPrecondition>, ApiError> {
        self.ensure_compatible(current)?;
        let revisions = match self {
            Self::Missing => {
                crate::observability::metrics::revision_condition("unconditional");
                return Ok(None);
            }
            Self::Any => {
                crate::observability::metrics::revision_condition("wildcard");
                Vec::new()
            }
            Self::Tags(tags) => {
                crate::observability::metrics::revision_condition("matched");
                tags.iter().map(EntityTag::revision).collect()
            }
        };
        Ok(Some(StorageRevisionPrecondition::new(
            current.revision_target()?,
            revisions,
        )))
    }
}

pub trait RevisionedResource {
    fn entity_tag(&self) -> Result<EntityTag, ApiError>;
}

/// Build the database-owned conditional mutation assertion for `resource`.
///
/// Keeping request parsing and resource compatibility checks together avoids
/// subtly different `If-Match` handling across mutation handlers.
pub fn revision_precondition<R>(
    request: &HttpRequest,
    resource: &R,
) -> Result<Option<RevisionPrecondition>, ApiError>
where
    R: RevisionedResource,
{
    revision_precondition_for_tag(request, &resource.entity_tag()?)
}

/// Build a precondition from an already materialized tag. Delete handlers use
/// this form when the same tag is also returned with the successful response.
pub fn revision_precondition_for_tag(
    request: &HttpRequest,
    current: &EntityTag,
) -> Result<Option<RevisionPrecondition>, ApiError> {
    IfMatchCondition::from_request(request)?.database_precondition(current)
}

fn invalid_if_match(detail: &str) -> ApiError {
    crate::observability::metrics::revision_condition("malformed");
    ApiError::BadRequest(format!("Invalid If-Match header: {detail}"))
}

macro_rules! impl_id_etag {
    ($type:ty, $kind:expr) => {
        impl RevisionedResource for $type {
            fn entity_tag(&self) -> Result<EntityTag, ApiError> {
                EntityTag::for_id($kind, self.id, self.revision)
            }
        }
    };
}

impl_id_etag!(
    crate::models::IdentityScope,
    EtagResourceKind::IdentityScope
);
impl_id_etag!(crate::models::Group, EtagResourceKind::Group);
impl_id_etag!(crate::models::GroupPointResponse, EtagResourceKind::Group);
impl_id_etag!(crate::models::Principal, EtagResourceKind::Principal);
impl_id_etag!(crate::models::UserPointResponse, EtagResourceKind::User);
impl_id_etag!(
    crate::models::ServiceAccountPointResponse,
    EtagResourceKind::ServiceAccount
);
impl_id_etag!(crate::models::Collection, EtagResourceKind::Collection);
impl_id_etag!(crate::models::HubuumClass, EtagResourceKind::Class);
impl_id_etag!(crate::models::HubuumObject, EtagResourceKind::Object);
impl_id_etag!(
    crate::models::HubuumClassRelation,
    EtagResourceKind::ClassRelation
);
impl_id_etag!(
    crate::models::HubuumObjectRelation,
    EtagResourceKind::ObjectRelation
);
impl_id_etag!(
    crate::models::ExportTemplate,
    EtagResourceKind::ExportTemplate
);
impl_id_etag!(crate::models::RemoteTarget, EtagResourceKind::RemoteTarget);
impl_id_etag!(crate::models::EventSink, EtagResourceKind::EventSink);
impl_id_etag!(
    crate::models::EventSubscription,
    EtagResourceKind::EventSubscription
);
impl_id_etag!(
    crate::models::ComputedFieldDefinition,
    EtagResourceKind::ComputedField
);
impl_id_etag!(
    crate::models::CollectionHistory,
    EtagResourceKind::Collection
);
impl_id_etag!(crate::models::HubuumClassHistory, EtagResourceKind::Class);
impl_id_etag!(crate::models::HubuumObjectHistory, EtagResourceKind::Object);
impl_id_etag!(
    crate::models::ExportTemplateHistory,
    EtagResourceKind::ExportTemplate
);
impl_id_etag!(
    crate::models::RemoteTargetHistory,
    EtagResourceKind::RemoteTarget
);

impl RevisionedResource for crate::models::PrincipalTokenPointResponse {
    fn entity_tag(&self) -> Result<EntityTag, ApiError> {
        EntityTag::for_id(EtagResourceKind::Token, self.id.id(), self.revision)
    }
}

impl RevisionedResource for crate::models::PrincipalSettingsResponse {
    fn entity_tag(&self) -> Result<EntityTag, ApiError> {
        EntityTag::for_id(
            EtagResourceKind::PrincipalSettings,
            self.principal_id(),
            self.revision,
        )
    }
}

impl RevisionedResource for crate::models::CollectionPermissionSet {
    fn entity_tag(&self) -> Result<EntityTag, ApiError> {
        EntityTag::for_id(
            EtagResourceKind::CollectionPermissions,
            self.collection_id,
            self.revision,
        )
    }
}

impl RevisionedResource for crate::models::PrincipalGroup {
    fn entity_tag(&self) -> Result<EntityTag, ApiError> {
        membership_entity_tag(self.principal_id, self.group_id, self.revision)
    }
}

impl RevisionedResource for crate::models::PrincipalMemberResponse {
    fn entity_tag(&self) -> Result<EntityTag, ApiError> {
        membership_entity_tag(self.principal_id, self.group_id, self.revision)
    }
}

fn membership_entity_tag(
    principal_id: i32,
    group_id: i32,
    revision: ResourceRevision,
) -> Result<EntityTag, ApiError> {
    EntityTag::new(
        EtagResourceKind::Membership,
        format!("{principal_id}:{group_id}"),
        revision,
    )
}

#[cfg(test)]
mod tests {
    use actix_web::test::TestRequest;

    use super::*;

    fn tag(revision: i64) -> EntityTag {
        EntityTag::for_id(
            EtagResourceKind::Collection,
            42,
            ResourceRevision::new(revision).unwrap(),
        )
        .unwrap()
    }

    #[test]
    fn strong_etag_round_trip() {
        let original = tag(17);
        assert_eq!(EntityTag::parse(&original.to_string()).unwrap(), original);
        assert_eq!(original.to_string(), "\"hubuum-v1.collection.NDI.17\"");
    }

    #[test]
    fn lists_and_wildcard_are_supported() {
        let one = tag(16).to_string();
        let two = tag(17).to_string();
        let request = TestRequest::default()
            .insert_header((header::IF_MATCH, format!("{one}, {two}")))
            .to_http_request();
        let parsed = IfMatchCondition::from_request(&request).unwrap();
        let precondition = parsed.database_precondition(&tag(17)).unwrap().unwrap();
        assert_eq!(
            precondition.revisions(),
            [
                ResourceRevision::new(16).unwrap(),
                ResourceRevision::new(17).unwrap(),
            ]
        );

        let request = TestRequest::default()
            .insert_header((header::IF_MATCH, "*"))
            .to_http_request();
        assert_eq!(
            IfMatchCondition::from_request(&request).unwrap(),
            IfMatchCondition::Any
        );
    }

    #[test]
    fn weak_malformed_and_mixed_wildcards_are_rejected() {
        for value in [
            "W/\"hubuum-v1.collection.NDI.1\"",
            "* , \"x\"",
            "plain",
            "\"hubuum-v1.collection._w.1\"",
        ] {
            let request = TestRequest::default()
                .insert_header((header::IF_MATCH, value))
                .to_http_request();
            assert!(IfMatchCondition::from_request(&request).is_err());
        }
    }

    #[test]
    fn parser_enforces_bounds_and_resource_identity() {
        let oversized = "x".repeat(MAX_IF_MATCH_BYTES + 1);
        let request = TestRequest::default()
            .insert_header((header::IF_MATCH, oversized))
            .to_http_request();
        assert!(IfMatchCondition::from_request(&request).is_err());

        let other =
            EntityTag::for_id(EtagResourceKind::Collection, 43, ResourceRevision::INITIAL).unwrap();
        assert!(
            IfMatchCondition::Tags(vec![other])
                .database_precondition(&tag(1))
                .is_err()
        );

        let too_many = (1..=MAX_IF_MATCH_VALIDATORS + 1)
            .map(|revision| tag(revision as i64).to_string())
            .collect::<Vec<_>>()
            .join(",");
        let request = TestRequest::default()
            .insert_header((header::IF_MATCH, too_many))
            .to_http_request();
        assert!(IfMatchCondition::from_request(&request).is_err());
    }
}
