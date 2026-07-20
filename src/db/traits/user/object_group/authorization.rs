use diesel::prelude::*;
use diesel_async::RunQueryDsl;

use super::ObjectGroupRouteTarget;
use crate::db::DbConnection;
use crate::errors::ApiError;
use crate::models::{HubuumObject, Permissions};
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

    fn for_permission(&self, object: &HubuumObject, permission: Permissions) -> ResourceRef {
        match permission {
            Permissions::ReadObject | Permissions::UpdateObject | Permissions::DeleteObject => {
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
            Permissions::CreateObject => {
                let mut resource =
                    ResourceRef::for_permission_on_collection(permission, object.collection_id);
                resource.attrs.class_id = Some(object.hubuum_class_id);
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
                ResourceRef::for_permission_on_collection(permission, object.collection_id)
            }
        }
    }
}

pub(super) struct ExternalObjectGroupAuthorizer<'a> {
    backend: &'a dyn PermissionBackend,
    principal: &'a PrincipalRef,
    required_permissions: &'a [Permissions],
    resources: &'a ObjectGroupPermissionResources,
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
        Ok(Self {
            backend,
            principal,
            required_permissions,
            resources,
        })
    }

    pub(super) async fn authorize(
        &self,
        candidates: Vec<HubuumObject>,
    ) -> Result<Vec<HubuumObject>, ApiError> {
        let permissions_per_object = self.required_permissions.len();
        let requests = candidates
            .iter()
            .flat_map(|object| {
                self.required_permissions
                    .iter()
                    .copied()
                    .map(|permission| PermissionRequest {
                        resource: self.resources.for_permission(object, permission),
                        permissions: vec![permission],
                    })
            })
            .collect::<Vec<_>>();
        let decisions = self
            .backend
            .authorize_many(self.principal, requests)
            .await?;
        let expected_decisions = candidates
            .len()
            .checked_mul(permissions_per_object)
            .ok_or_else(|| {
                ApiError::InternalServerError(
                    "Object group permission decision count overflowed".to_string(),
                )
            })?;
        if decisions.len() != expected_decisions {
            return Err(ApiError::InternalServerError(
                "Permission backend returned an unexpected number of object decisions".to_string(),
            ));
        }
        let allowed = decisions
            .chunks_exact(permissions_per_object)
            .map(|object_decisions| {
                object_decisions
                    .iter()
                    .all(|decision| *decision == PermissionDecision::Allow)
            });
        Ok(candidates
            .into_iter()
            .zip(allowed)
            .filter_map(|(object, allowed)| allowed.then_some(object))
            .collect())
    }
}
