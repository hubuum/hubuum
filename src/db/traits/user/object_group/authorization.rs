use diesel::prelude::*;
use diesel_async::RunQueryDsl;

use super::ObjectGroupRouteTarget;
use super::candidate::ObjectGroupCandidate;
use crate::db::DbConnection;
use crate::errors::ApiError;
use crate::models::Permissions;
use crate::permissions::{
    PermissionBackend, PermissionDecision, PermissionRequest, PrincipalRef, ResourceAttrs,
    ResourceKind, ResourceRef,
};

pub(super) struct ObjectGroupPermissionResources {
    class: ResourceRef,
    collection: ResourceRef,
}

impl ObjectGroupPermissionResources {
    pub(super) async fn load(
        connection: &mut DbConnection,
        target: &ObjectGroupRouteTarget,
    ) -> Result<Self, ApiError> {
        use crate::schema::collections;

        let collection_name = collections::table
            .filter(collections::id.eq(target.collection_id))
            .select(collections::name)
            .first::<String>(connection)
            .await
            .optional()?
            .ok_or_else(|| {
                ApiError::InternalServerError(format!(
                    "Object group target references missing collection {}",
                    target.collection_id
                ))
            })?;
        Ok(Self {
            class: ResourceRef {
                kind: ResourceKind::Class,
                id: target.class_id,
                attrs: ResourceAttrs {
                    collection_id: Some(target.collection_id),
                    name: Some(target.class_name.clone()),
                    ..Default::default()
                },
            },
            collection: ResourceRef {
                kind: ResourceKind::Collection,
                id: target.collection_id,
                attrs: ResourceAttrs {
                    collection_id: Some(target.collection_id),
                    name: Some(collection_name),
                    ..Default::default()
                },
            },
        })
    }

    fn for_object(object: &ObjectGroupCandidate) -> ResourceRef {
        ResourceRef {
            kind: ResourceKind::Object,
            id: object.id,
            attrs: ResourceAttrs {
                collection_id: Some(object.collection_id),
                class_id: Some(object.hubuum_class_id),
                name: Some(object.name.clone()),
                ..Default::default()
            },
        }
    }

    fn for_invariant_permission(&self, permission: Permissions) -> Result<ResourceRef, ApiError> {
        Ok(match permission {
            Permissions::ReadObject | Permissions::UpdateObject | Permissions::DeleteObject => {
                return Err(ApiError::InternalServerError(
                    "Object-specific permission cannot be preauthorized".to_string(),
                ));
            }
            Permissions::CreateObject => {
                let mut resource =
                    ResourceRef::for_permission_on_collection(permission, self.collection.id);
                resource.attrs.class_id = Some(self.class.id);
                resource
            }
            Permissions::ReadClass | Permissions::UpdateClass | Permissions::DeleteClass => {
                self.class.clone()
            }
            Permissions::ReadCollection
            | Permissions::UpdateCollection
            | Permissions::DeleteCollection
            | Permissions::DelegateCollection
            | Permissions::ReadRemoteTarget
            | Permissions::CreateRemoteTarget
            | Permissions::UpdateRemoteTarget
            | Permissions::DeleteRemoteTarget
            | Permissions::ExecuteRemoteTarget
            | Permissions::ReadAudit
            | Permissions::ManageEventSubscription => self.collection.clone(),
            Permissions::CreateClass
            | Permissions::CreateClassRelation
            | Permissions::ReadClassRelation
            | Permissions::UpdateClassRelation
            | Permissions::DeleteClassRelation
            | Permissions::CreateObjectRelation
            | Permissions::ReadObjectRelation
            | Permissions::UpdateObjectRelation
            | Permissions::DeleteObjectRelation
            | Permissions::ReadTemplate
            | Permissions::CreateTemplate
            | Permissions::UpdateTemplate
            | Permissions::DeleteTemplate => {
                ResourceRef::for_permission_on_collection(permission, self.collection.id)
            }
        })
    }
}

pub(super) struct ExternalObjectGroupAuthorizer<'a> {
    backend: &'a dyn PermissionBackend,
    principal: &'a PrincipalRef,
    object_permissions: Vec<Permissions>,
    invariant_requests: Vec<PermissionRequest>,
}

impl<'a> ExternalObjectGroupAuthorizer<'a> {
    pub(super) fn new(
        backend: &'a dyn PermissionBackend,
        principal: &'a PrincipalRef,
        required_permissions: &'a [Permissions],
        resources: &'a ObjectGroupPermissionResources,
    ) -> Result<Self, ApiError> {
        if required_permissions.is_empty() {
            return Err(ApiError::InternalServerError(
                "Object group authorization requires at least one permission".to_string(),
            ));
        }
        let (object_permissions, invariant_permissions): (Vec<_>, Vec<_>) = required_permissions
            .iter()
            .copied()
            .partition(is_object_specific_permission);
        if object_permissions.is_empty() {
            return Err(ApiError::InternalServerError(
                "Object group authorization requires an object permission".to_string(),
            ));
        }
        let invariant_requests = invariant_permissions
            .into_iter()
            .map(|permission| {
                Ok(PermissionRequest {
                    resource: resources.for_invariant_permission(permission)?,
                    permissions: vec![permission],
                })
            })
            .collect::<Result<Vec<_>, ApiError>>()?;
        Ok(Self {
            backend,
            principal,
            object_permissions,
            invariant_requests,
        })
    }

    pub(super) async fn authorize_invariants(&self) -> Result<bool, ApiError> {
        if self.invariant_requests.is_empty() {
            return Ok(true);
        }
        let decisions = self
            .backend
            .authorize_many(self.principal, self.invariant_requests.clone())
            .await?;
        if decisions.len() != self.invariant_requests.len() {
            return Err(ApiError::InternalServerError(
                "Permission backend returned an unexpected number of invariant decisions"
                    .to_string(),
            ));
        }
        Ok(decisions
            .into_iter()
            .all(|decision| decision == PermissionDecision::Allow))
    }

    pub(super) async fn authorize(
        &self,
        candidates: Vec<ObjectGroupCandidate>,
    ) -> Result<Vec<ObjectGroupCandidate>, ApiError> {
        if candidates.is_empty() {
            return Ok(candidates);
        }
        let requests = candidates
            .iter()
            .map(|object| PermissionRequest {
                resource: ObjectGroupPermissionResources::for_object(object),
                permissions: self.object_permissions.clone(),
            })
            .collect::<Vec<_>>();
        let decisions = self
            .backend
            .authorize_many(self.principal, requests)
            .await?;
        if decisions.len() != candidates.len() {
            return Err(ApiError::InternalServerError(
                "Permission backend returned an unexpected number of object decisions".to_string(),
            ));
        }
        Ok(candidates
            .into_iter()
            .zip(decisions)
            .filter_map(|(object, decision)| {
                (decision == PermissionDecision::Allow).then_some(object)
            })
            .collect())
    }
}

fn is_object_specific_permission(permission: &Permissions) -> bool {
    matches!(
        permission,
        Permissions::ReadObject | Permissions::UpdateObject | Permissions::DeleteObject
    )
}
