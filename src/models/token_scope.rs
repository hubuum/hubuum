use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::errors::ApiError;
use crate::models::{CollectionID, HubuumClassID, HubuumObjectID, Permissions};
use crate::permissions::{ResourceKind, ResourceRef};

/// Maximum number of collection, class, and object entries in one token boundary.
pub const MAX_TOKEN_RESOURCE_SCOPES: usize = 1_000;

/// One resource explicitly included in a token's resource boundary.
///
/// Resource scopes are additive within the boundary: a collection entry covers
/// that collection, its classes, and their objects; a class entry covers that
/// class and its objects; and an object entry covers only that object. The
/// boundary is still intersected with the principal's live group grants.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(tag = "kind", content = "id", rename_all = "snake_case")]
pub enum TokenResourceScope {
    Collection(CollectionID),
    Class(HubuumClassID),
    Object(HubuumObjectID),
}

impl TokenResourceScope {
    pub fn id(self) -> i32 {
        match self {
            Self::Collection(id) => id.id(),
            Self::Class(id) => id.id(),
            Self::Object(id) => id.id(),
        }
    }

    fn key(self) -> (&'static str, i32) {
        match self {
            Self::Collection(id) => ("collection", id.id()),
            Self::Class(id) => ("class", id.id()),
            Self::Object(id) => ("object", id.id()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TokenResourceScopeSet {
    collections: Vec<i32>,
    classes: Vec<i32>,
    objects: Vec<i32>,
}

impl TokenResourceScopeSet {
    fn new(resources: Vec<TokenResourceScope>) -> Self {
        let mut collections = Vec::new();
        let mut classes = Vec::new();
        let mut objects = Vec::new();
        for resource in resources {
            match resource {
                TokenResourceScope::Collection(id) => collections.push(id.id()),
                TokenResourceScope::Class(id) => classes.push(id.id()),
                TokenResourceScope::Object(id) => objects.push(id.id()),
            }
        }
        collections.sort_unstable();
        collections.dedup();
        classes.sort_unstable();
        classes.dedup();
        objects.sort_unstable();
        objects.dedup();
        Self {
            collections,
            classes,
            objects,
        }
    }

    fn entries(&self) -> Result<Vec<TokenResourceScope>, ApiError> {
        self.collections
            .iter()
            .map(|id| CollectionID::new(*id).map(TokenResourceScope::Collection))
            .chain(
                self.classes
                    .iter()
                    .map(|id| HubuumClassID::new(*id).map(TokenResourceScope::Class)),
            )
            .chain(
                self.objects
                    .iter()
                    .map(|id| HubuumObjectID::new(*id).map(TokenResourceScope::Object)),
            )
            .collect()
    }

    fn allows_collection(&self, collection_id: Option<i32>) -> bool {
        collection_id.is_some_and(|id| self.collections.contains(&id))
    }

    fn allows_class(&self, collection_id: Option<i32>, class_id: Option<i32>) -> bool {
        self.allows_collection(collection_id)
            || class_id.is_some_and(|id| self.classes.contains(&id))
    }

    fn allows_object(
        &self,
        collection_id: Option<i32>,
        class_id: Option<i32>,
        object_id: Option<i32>,
    ) -> bool {
        self.allows_class(collection_id, class_id)
            || object_id.is_some_and(|id| self.objects.contains(&id))
    }
}

/// The complete narrowing boundary attached to one scoped token.
///
/// `None` at the call site means an unscoped token. Within a `TokenScope`, a
/// `None` dimension is unrestricted and a present-but-empty dimension denies
/// everything in that dimension. Keeping those states distinct makes corrupted
/// or partially deleted scope rows fail closed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TokenScope {
    permissions: Option<Vec<Permissions>>,
    resources: Option<TokenResourceScopeSet>,
}

pub(crate) struct TokenScopeParts {
    pub(crate) permissions: Option<Vec<Permissions>>,
    pub(crate) resource_scopes: Option<Vec<TokenResourceScope>>,
}

impl TokenScope {
    /// Build a scope loaded from persistence. Empty dimensions are deliberately
    /// accepted because their meaning is deny-all.
    pub fn from_stored_parts(
        permissions: Option<Vec<Permissions>>,
        resources: Option<Vec<TokenResourceScope>>,
    ) -> Result<Self, ApiError> {
        if permissions.is_none() && resources.is_none() {
            return Err(ApiError::InternalServerError(
                "Scoped token has no scoped dimensions".to_string(),
            ));
        }
        if resources
            .as_ref()
            .is_some_and(|resources| resources.len() > MAX_TOKEN_RESOURCE_SCOPES)
        {
            return Err(ApiError::InternalServerError(format!(
                "Stored token resource scope exceeds the {MAX_TOKEN_RESOURCE_SCOPES}-entry limit"
            )));
        }
        Ok(Self {
            permissions,
            resources: resources.map(TokenResourceScopeSet::new),
        })
    }

    /// Validate request-provided scope dimensions. Omitting both dimensions
    /// creates an unscoped token; explicitly empty or duplicate arrays are
    /// rejected as client mistakes.
    pub fn from_request_parts(
        permissions: Option<Vec<Permissions>>,
        resources: Option<Vec<TokenResourceScope>>,
    ) -> Result<Option<Self>, ApiError> {
        if permissions.as_ref().is_some_and(Vec::is_empty) {
            return Err(ApiError::BadRequest(
                "scopes must be non-empty when provided".to_string(),
            ));
        }
        if resources.as_ref().is_some_and(Vec::is_empty) {
            return Err(ApiError::BadRequest(
                "resource_scopes must be non-empty when provided".to_string(),
            ));
        }
        if resources
            .as_ref()
            .is_some_and(|resources| resources.len() > MAX_TOKEN_RESOURCE_SCOPES)
        {
            return Err(ApiError::BadRequest(format!(
                "resource_scopes must contain at most {MAX_TOKEN_RESOURCE_SCOPES} entries"
            )));
        }
        if let Some(permissions) = &permissions {
            let mut unique = permissions.clone();
            unique.sort_unstable_by_key(|permission| permission.to_string());
            unique.dedup();
            if unique.len() != permissions.len() {
                return Err(ApiError::BadRequest(
                    "scopes must not contain duplicates".to_string(),
                ));
            }
        }
        if let Some(resources) = &resources {
            let mut keys = resources
                .iter()
                .copied()
                .map(TokenResourceScope::key)
                .collect::<Vec<_>>();
            keys.sort_unstable();
            keys.dedup();
            if keys.len() != resources.len() {
                return Err(ApiError::BadRequest(
                    "resource_scopes must not contain duplicates".to_string(),
                ));
            }
        }
        if permissions.is_none() && resources.is_none() {
            return Ok(None);
        }
        Self::from_stored_parts(permissions, resources).map(Some)
    }

    pub fn permissions(&self) -> Option<&[Permissions]> {
        self.permissions.as_deref()
    }

    pub fn is_permission_scoped(&self) -> bool {
        self.permissions.is_some()
    }

    pub fn is_resource_scoped(&self) -> bool {
        self.resources.is_some()
    }

    pub fn resource_scopes(&self) -> Result<Option<Vec<TokenResourceScope>>, ApiError> {
        self.resources
            .as_ref()
            .map(TokenResourceScopeSet::entries)
            .transpose()
    }

    pub fn collection_ids(&self) -> Option<&[i32]> {
        self.resources
            .as_ref()
            .map(|resources| resources.collections.as_slice())
    }

    pub fn class_ids(&self) -> Option<&[i32]> {
        self.resources
            .as_ref()
            .map(|resources| resources.classes.as_slice())
    }

    pub fn object_ids(&self) -> Option<&[i32]> {
        self.resources
            .as_ref()
            .map(|resources| resources.objects.as_slice())
    }

    pub fn allows_permissions(&self, requested: &[Permissions]) -> bool {
        self.permissions.as_ref().is_none_or(|allowed| {
            requested
                .iter()
                .all(|permission| allowed.contains(permission))
        })
    }

    pub fn allows_resource(&self, resource: &ResourceRef) -> bool {
        let Some(resources) = &self.resources else {
            return true;
        };
        match resource.kind {
            ResourceKind::System => false,
            ResourceKind::Collection
            | ResourceKind::Template
            | ResourceKind::RemoteTarget
            | ResourceKind::Audit
            | ResourceKind::EventSubscription => {
                resources.allows_collection(resource.collection_id())
            }
            ResourceKind::Class => resources.allows_class(
                resource.attrs.collection_id,
                Some(resource.id).filter(|id| *id > 0),
            ),
            ResourceKind::Object => resources.allows_object(
                resource.attrs.collection_id,
                resource.attrs.class_id,
                Some(resource.id).filter(|id| *id > 0),
            ),
            ResourceKind::ClassRelation => {
                resources.allows_class(
                    resource.attrs.from_collection_id,
                    resource.attrs.from_class_id,
                ) && resources
                    .allows_class(resource.attrs.to_collection_id, resource.attrs.to_class_id)
            }
            ResourceKind::ObjectRelation => {
                resources.allows_object(
                    resource.attrs.from_collection_id,
                    resource.attrs.from_class_id,
                    resource.attrs.from_object_id,
                ) && resources.allows_object(
                    resource.attrs.to_collection_id,
                    resource.attrs.to_class_id,
                    resource.attrs.to_object_id,
                )
            }
            // Task visibility is principal-owned rather than part of the
            // collection/class/object resource hierarchy.
            ResourceKind::Task => true,
        }
    }

    pub(crate) fn into_parts(self) -> Result<TokenScopeParts, ApiError> {
        let resource_scopes = self
            .resources
            .map(|resources| resources.entries())
            .transpose()?;
        Ok(TokenScopeParts {
            permissions: self.permissions,
            resource_scopes,
        })
    }

    /// Persist this boundary for asynchronous execution. An absent dimension
    /// is encoded as JSON `null`; a present empty dimension remains `[]` and
    /// therefore retains its deny-all meaning.
    pub fn snapshot_json(&self) -> serde_json::Value {
        let resource_scopes = self.resources.as_ref().map(|resources| {
            resources
                .collections
                .iter()
                .map(|id| serde_json::json!({"kind": "collection", "id": id}))
                .chain(
                    resources
                        .classes
                        .iter()
                        .map(|id| serde_json::json!({"kind": "class", "id": id})),
                )
                .chain(
                    resources
                        .objects
                        .iter()
                        .map(|id| serde_json::json!({"kind": "object", "id": id})),
                )
                .collect::<Vec<_>>()
        });
        serde_json::json!({
            "permissions": self.permissions,
            "resource_scopes": resource_scopes,
        })
    }

    /// Reconstruct a task scope snapshot. Permission-only snapshots from older
    /// releases used a bare string array and remain accepted.
    pub fn from_snapshot_json(value: &serde_json::Value) -> Result<Self, ApiError> {
        if let Some(entries) = value.as_array() {
            let permissions = entries
                .iter()
                .map(|entry| {
                    entry
                        .as_str()
                        .ok_or_else(|| {
                            ApiError::InternalServerError(
                                "Task scope snapshot entry is not a string".to_string(),
                            )
                        })
                        .and_then(Permissions::from_string)
                })
                .collect::<Result<Vec<_>, _>>()?;
            return Self::from_stored_parts(Some(permissions), None);
        }

        let object = value.as_object().ok_or_else(|| {
            ApiError::InternalServerError(
                "Task scope snapshot is neither an object nor a legacy array".to_string(),
            )
        })?;
        let permissions = match object.get("permissions") {
            None | Some(serde_json::Value::Null) => None,
            Some(serde_json::Value::Array(entries)) => Some(
                entries
                    .iter()
                    .map(|entry| {
                        entry
                            .as_str()
                            .ok_or_else(|| {
                                ApiError::InternalServerError(
                                    "Task permission scope entry is not a string".to_string(),
                                )
                            })
                            .and_then(Permissions::from_string)
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            ),
            Some(_) => {
                return Err(ApiError::InternalServerError(
                    "Task permission scope snapshot is not an array or null".to_string(),
                ));
            }
        };
        let resources = match object.get("resource_scopes") {
            None | Some(serde_json::Value::Null) => None,
            Some(value @ serde_json::Value::Array(_)) => Some(
                serde_json::from_value::<Vec<TokenResourceScope>>(value.clone()).map_err(
                    |err| {
                        ApiError::InternalServerError(format!(
                            "Invalid task resource scope snapshot: {err}"
                        ))
                    },
                )?,
            ),
            Some(_) => {
                return Err(ApiError::InternalServerError(
                    "Task resource scope snapshot is not an array or null".to_string(),
                ));
            }
        };
        Self::from_stored_parts(permissions, resources)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::permissions::{ResourceAttrs, ResourceKind, ResourceRef};

    fn scope(resources: Vec<TokenResourceScope>) -> TokenScope {
        TokenScope::from_stored_parts(None, Some(resources)).unwrap()
    }

    #[test]
    fn collection_scope_covers_descendant_resources() {
        let scope = scope(vec![TokenResourceScope::Collection(
            CollectionID::new(7).unwrap(),
        )]);
        let object = ResourceRef {
            kind: ResourceKind::Object,
            id: 11,
            attrs: ResourceAttrs {
                collection_id: Some(7),
                class_id: Some(9),
                ..Default::default()
            },
        };

        assert!(scope.allows_resource(&object));
    }

    #[test]
    fn class_scope_does_not_expose_parent_collection() {
        let scope = scope(vec![TokenResourceScope::Class(
            HubuumClassID::new(9).unwrap(),
        )]);

        assert!(!scope.allows_resource(&ResourceRef::collection(7)));
    }

    #[test]
    fn object_scope_covers_only_the_named_object() {
        let scope = scope(vec![TokenResourceScope::Object(
            HubuumObjectID::new(11).unwrap(),
        )]);
        let resource = |id| ResourceRef {
            kind: ResourceKind::Object,
            id,
            attrs: ResourceAttrs {
                collection_id: Some(7),
                class_id: Some(9),
                ..Default::default()
            },
        };

        assert!(scope.allows_resource(&resource(11)));
        assert!(!scope.allows_resource(&resource(12)));
    }

    #[test]
    fn empty_stored_resource_scope_denies_resources() {
        let scope = scope(Vec::new());

        assert!(!scope.allows_resource(&ResourceRef::collection(7)));
    }

    #[test]
    fn relation_requires_both_endpoints_inside_scope() {
        let scope = scope(vec![TokenResourceScope::Class(
            HubuumClassID::new(9).unwrap(),
        )]);
        let relation = ResourceRef {
            kind: ResourceKind::ClassRelation,
            id: 13,
            attrs: ResourceAttrs {
                from_collection_id: Some(7),
                to_collection_id: Some(7),
                from_class_id: Some(9),
                to_class_id: Some(10),
                ..Default::default()
            },
        };

        assert!(!scope.allows_resource(&relation));
    }

    #[test]
    fn task_snapshot_round_trips_both_scope_dimensions() {
        let original = TokenScope::from_stored_parts(
            Some(vec![Permissions::ReadObject]),
            Some(vec![TokenResourceScope::Object(
                HubuumObjectID::new(11).unwrap(),
            )]),
        )
        .unwrap();

        let restored = TokenScope::from_snapshot_json(&original.snapshot_json()).unwrap();

        assert_eq!(restored, original);
    }

    #[test]
    fn resource_scope_uses_tagged_wire_format() {
        let encoded = serde_json::to_value(TokenResourceScope::Collection(
            CollectionID::new(7).unwrap(),
        ))
        .unwrap();

        assert_eq!(encoded, serde_json::json!({"kind": "collection", "id": 7}));
    }

    #[test]
    fn request_resource_scope_count_is_bounded() {
        let resources = (1..=MAX_TOKEN_RESOURCE_SCOPES + 1)
            .map(|id| {
                HubuumObjectID::new(i32::try_from(id).unwrap()).map(TokenResourceScope::Object)
            })
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        let error = TokenScope::from_request_parts(None, Some(resources)).unwrap_err();

        assert!(
            matches!(error, ApiError::BadRequest(message) if message == format!(
                "resource_scopes must contain at most {MAX_TOKEN_RESOURCE_SCOPES} entries"
            ))
        );
    }

    #[test]
    fn stored_resource_scope_count_is_bounded() {
        let resources = (1..=MAX_TOKEN_RESOURCE_SCOPES + 1)
            .map(|id| {
                HubuumObjectID::new(i32::try_from(id).unwrap()).map(TokenResourceScope::Object)
            })
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        let error = TokenScope::from_stored_parts(None, Some(resources)).unwrap_err();

        assert!(
            matches!(error, ApiError::InternalServerError(message) if message == format!(
                "Stored token resource scope exceeds the {MAX_TOKEN_RESOURCE_SCOPES}-entry limit"
            ))
        );
    }
}
