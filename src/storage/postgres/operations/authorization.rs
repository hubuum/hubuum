//! Transitional application-model conversions for authorization consumers.
//!
//! The `AuthorizationStorage` implementation and PostgreSQL queries live in
//! `hubuum-storage-postgres`. These conversions remain only for older
//! application services whose own contracts have not yet moved to storage DTOs.

use crate::errors::ApiError;
use crate::models::{Collection, Group, GroupPermission, Permission, Permissions};
use crate::storage::postgres::PostgresPool;
use crate::storage::{
    AuthorizationCollection, AuthorizationCollectionsAccessQuery, AuthorizationGrant,
    AuthorizationGroup, AuthorizationGroupGrant, AuthorizationGroupIdentity,
    AuthorizationGroupProfile, AuthorizationGroupSyncState, AuthorizationPermission, StorageError,
};

pub(crate) const fn permission_from_storage(permission: AuthorizationPermission) -> Permissions {
    match permission {
        AuthorizationPermission::ReadCollection => Permissions::ReadCollection,
        AuthorizationPermission::UpdateCollection => Permissions::UpdateCollection,
        AuthorizationPermission::DeleteCollection => Permissions::DeleteCollection,
        AuthorizationPermission::DelegateCollection => Permissions::DelegateCollection,
        AuthorizationPermission::CreateClass => Permissions::CreateClass,
        AuthorizationPermission::ReadClass => Permissions::ReadClass,
        AuthorizationPermission::UpdateClass => Permissions::UpdateClass,
        AuthorizationPermission::DeleteClass => Permissions::DeleteClass,
        AuthorizationPermission::CreateObject => Permissions::CreateObject,
        AuthorizationPermission::ReadObject => Permissions::ReadObject,
        AuthorizationPermission::UpdateObject => Permissions::UpdateObject,
        AuthorizationPermission::DeleteObject => Permissions::DeleteObject,
        AuthorizationPermission::CreateClassRelation => Permissions::CreateClassRelation,
        AuthorizationPermission::ReadClassRelation => Permissions::ReadClassRelation,
        AuthorizationPermission::UpdateClassRelation => Permissions::UpdateClassRelation,
        AuthorizationPermission::DeleteClassRelation => Permissions::DeleteClassRelation,
        AuthorizationPermission::CreateObjectRelation => Permissions::CreateObjectRelation,
        AuthorizationPermission::ReadObjectRelation => Permissions::ReadObjectRelation,
        AuthorizationPermission::UpdateObjectRelation => Permissions::UpdateObjectRelation,
        AuthorizationPermission::DeleteObjectRelation => Permissions::DeleteObjectRelation,
        AuthorizationPermission::ReadTemplate => Permissions::ReadTemplate,
        AuthorizationPermission::CreateTemplate => Permissions::CreateTemplate,
        AuthorizationPermission::UpdateTemplate => Permissions::UpdateTemplate,
        AuthorizationPermission::DeleteTemplate => Permissions::DeleteTemplate,
        AuthorizationPermission::ReadRemoteTarget => Permissions::ReadRemoteTarget,
        AuthorizationPermission::CreateRemoteTarget => Permissions::CreateRemoteTarget,
        AuthorizationPermission::UpdateRemoteTarget => Permissions::UpdateRemoteTarget,
        AuthorizationPermission::DeleteRemoteTarget => Permissions::DeleteRemoteTarget,
        AuthorizationPermission::ExecuteRemoteTarget => Permissions::ExecuteRemoteTarget,
        AuthorizationPermission::ReadAudit => Permissions::ReadAudit,
        AuthorizationPermission::ManageEventSubscription => Permissions::ManageEventSubscription,
    }
}

pub(crate) fn permission_to_storage(permission: Permissions) -> AuthorizationPermission {
    match permission {
        Permissions::ReadCollection => AuthorizationPermission::ReadCollection,
        Permissions::UpdateCollection => AuthorizationPermission::UpdateCollection,
        Permissions::DeleteCollection => AuthorizationPermission::DeleteCollection,
        Permissions::DelegateCollection => AuthorizationPermission::DelegateCollection,
        Permissions::CreateClass => AuthorizationPermission::CreateClass,
        Permissions::ReadClass => AuthorizationPermission::ReadClass,
        Permissions::UpdateClass => AuthorizationPermission::UpdateClass,
        Permissions::DeleteClass => AuthorizationPermission::DeleteClass,
        Permissions::CreateObject => AuthorizationPermission::CreateObject,
        Permissions::ReadObject => AuthorizationPermission::ReadObject,
        Permissions::UpdateObject => AuthorizationPermission::UpdateObject,
        Permissions::DeleteObject => AuthorizationPermission::DeleteObject,
        Permissions::CreateClassRelation => AuthorizationPermission::CreateClassRelation,
        Permissions::ReadClassRelation => AuthorizationPermission::ReadClassRelation,
        Permissions::UpdateClassRelation => AuthorizationPermission::UpdateClassRelation,
        Permissions::DeleteClassRelation => AuthorizationPermission::DeleteClassRelation,
        Permissions::CreateObjectRelation => AuthorizationPermission::CreateObjectRelation,
        Permissions::ReadObjectRelation => AuthorizationPermission::ReadObjectRelation,
        Permissions::UpdateObjectRelation => AuthorizationPermission::UpdateObjectRelation,
        Permissions::DeleteObjectRelation => AuthorizationPermission::DeleteObjectRelation,
        Permissions::ReadTemplate => AuthorizationPermission::ReadTemplate,
        Permissions::CreateTemplate => AuthorizationPermission::CreateTemplate,
        Permissions::UpdateTemplate => AuthorizationPermission::UpdateTemplate,
        Permissions::DeleteTemplate => AuthorizationPermission::DeleteTemplate,
        Permissions::ReadRemoteTarget => AuthorizationPermission::ReadRemoteTarget,
        Permissions::CreateRemoteTarget => AuthorizationPermission::CreateRemoteTarget,
        Permissions::UpdateRemoteTarget => AuthorizationPermission::UpdateRemoteTarget,
        Permissions::DeleteRemoteTarget => AuthorizationPermission::DeleteRemoteTarget,
        Permissions::ExecuteRemoteTarget => AuthorizationPermission::ExecuteRemoteTarget,
        Permissions::ReadAudit => AuthorizationPermission::ReadAudit,
        Permissions::ManageEventSubscription => AuthorizationPermission::ManageEventSubscription,
    }
}

pub(crate) fn collection_to_storage(collection: Collection) -> AuthorizationCollection {
    AuthorizationCollection::new(
        collection.id,
        collection.name,
        collection.description,
        collection.created_at,
        collection.updated_at,
        collection.parent_collection_id,
        collection.revision.get(),
    )
}

pub(crate) fn group_to_storage(group: impl Into<Group>) -> AuthorizationGroup {
    let group = group.into();
    AuthorizationGroup::new(
        AuthorizationGroupIdentity::new(
            group.id,
            group.groupname,
            group.identity_scope_id,
            group.managed_by,
            group.external_key,
        ),
        AuthorizationGroupProfile::new(
            group.description,
            group.created_at,
            group.updated_at,
            group.revision.get(),
        ),
        AuthorizationGroupSyncState::new(group.last_sync_attempted_at, group.last_sync_success_at),
    )
}

pub(crate) fn grant_to_storage(grant: impl Into<Permission>) -> AuthorizationGrant {
    let grant = grant.into();
    AuthorizationGrant::new(
        grant.id,
        grant.collection_id,
        grant.group_id,
        grant.granted().into_iter().map(permission_to_storage),
        grant.created_at,
        grant.updated_at,
    )
}

pub(crate) fn group_grant_to_storage(row: GroupPermission) -> AuthorizationGroupGrant {
    AuthorizationGroupGrant::new(
        group_to_storage(row.group),
        grant_to_storage(row.permission),
    )
}

pub(crate) async fn authorize_local_collections(
    pool: &PostgresPool,
    query: AuthorizationCollectionsAccessQuery,
) -> Result<bool, ApiError> {
    hubuum_storage_postgres::operations::authorization::authorize_local_collections(
        &hubuum_storage_postgres::PostgresRuntime::new(pool.clone()),
        query,
    )
    .await
    .map_err(StorageError::from)
    .map_err(ApiError::from)
}
