use hubuum_storage_core::{
    AuthorizationCollection, AuthorizationEffectiveGroupGrant, AuthorizationGrant,
    AuthorizationGroup, AuthorizationGroupGrant, AuthorizationPermission,
};

use crate::errors::ApiError;
use crate::models::{Collection, Group, GroupPermission, Permission, Permissions};

pub(crate) const fn permission_to_storage(permission: Permissions) -> AuthorizationPermission {
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

pub(crate) fn collection_from_storage(
    collection: AuthorizationCollection,
) -> Result<Collection, ApiError> {
    Ok(Collection {
        id: collection.id().id(),
        name: collection.name().to_string(),
        description: collection.description().to_string(),
        created_at: collection.created_at().naive_utc(),
        updated_at: collection.updated_at().naive_utc(),
        parent_collection_id: collection.parent_collection_id().map(|id| id.id()),
        revision: collection.revision(),
    })
}

pub(crate) fn group_from_storage(group: AuthorizationGroup) -> Result<Group, ApiError> {
    Ok(Group {
        id: group.id().id(),
        groupname: group.group_name().to_string(),
        description: group.description().to_string(),
        created_at: group.created_at().naive_utc(),
        updated_at: group.updated_at().naive_utc(),
        identity_scope_id: group.identity_scope_id().id(),
        managed_by: group.managed_by().to_string(),
        external_key: group.external_key().map(str::to_owned),
        last_sync_attempted_at: group
            .last_sync_attempted_at()
            .map(|timestamp| timestamp.naive_utc()),
        last_sync_success_at: group
            .last_sync_success_at()
            .map(|timestamp| timestamp.naive_utc()),
        revision: group.revision(),
    })
}

pub(crate) fn grant_from_storage(grant: AuthorizationGrant) -> Permission {
    let has = |permission| grant.permissions().contains(&permission);
    Permission {
        id: grant.id().id(),
        collection_id: grant.collection_id().id(),
        group_id: grant.group_id().id(),
        has_read_collection: has(AuthorizationPermission::ReadCollection),
        has_update_collection: has(AuthorizationPermission::UpdateCollection),
        has_delete_collection: has(AuthorizationPermission::DeleteCollection),
        has_delegate_collection: has(AuthorizationPermission::DelegateCollection),
        has_create_class: has(AuthorizationPermission::CreateClass),
        has_read_class: has(AuthorizationPermission::ReadClass),
        has_update_class: has(AuthorizationPermission::UpdateClass),
        has_delete_class: has(AuthorizationPermission::DeleteClass),
        has_create_object: has(AuthorizationPermission::CreateObject),
        has_read_object: has(AuthorizationPermission::ReadObject),
        has_update_object: has(AuthorizationPermission::UpdateObject),
        has_delete_object: has(AuthorizationPermission::DeleteObject),
        has_create_class_relation: has(AuthorizationPermission::CreateClassRelation),
        has_read_class_relation: has(AuthorizationPermission::ReadClassRelation),
        has_update_class_relation: has(AuthorizationPermission::UpdateClassRelation),
        has_delete_class_relation: has(AuthorizationPermission::DeleteClassRelation),
        has_create_object_relation: has(AuthorizationPermission::CreateObjectRelation),
        has_read_object_relation: has(AuthorizationPermission::ReadObjectRelation),
        has_update_object_relation: has(AuthorizationPermission::UpdateObjectRelation),
        has_delete_object_relation: has(AuthorizationPermission::DeleteObjectRelation),
        has_read_template: has(AuthorizationPermission::ReadTemplate),
        has_create_template: has(AuthorizationPermission::CreateTemplate),
        has_update_template: has(AuthorizationPermission::UpdateTemplate),
        has_delete_template: has(AuthorizationPermission::DeleteTemplate),
        has_read_remote_target: has(AuthorizationPermission::ReadRemoteTarget),
        has_create_remote_target: has(AuthorizationPermission::CreateRemoteTarget),
        has_update_remote_target: has(AuthorizationPermission::UpdateRemoteTarget),
        has_delete_remote_target: has(AuthorizationPermission::DeleteRemoteTarget),
        has_execute_remote_target: has(AuthorizationPermission::ExecuteRemoteTarget),
        created_at: grant.created_at().naive_utc(),
        updated_at: grant.updated_at().naive_utc(),
        has_read_audit: has(AuthorizationPermission::ReadAudit),
        has_manage_event_subscription: has(AuthorizationPermission::ManageEventSubscription),
    }
}

pub(crate) fn group_grant_from_storage(
    row: AuthorizationGroupGrant,
) -> Result<GroupPermission, ApiError> {
    let (group, grant) = row.into_parts();
    Ok(GroupPermission {
        group: group_from_storage(group)?,
        permission: grant_from_storage(grant),
    })
}

pub(crate) fn effective_group_grant_from_storage(
    row: AuthorizationEffectiveGroupGrant,
) -> Result<crate::models::EffectiveGroupPermission, ApiError> {
    let (target, source, depth, inherited, group, grant) = row.into_parts();
    Ok(crate::models::EffectiveGroupPermission {
        target_collection: collection_from_storage(target)?,
        source_collection: collection_from_storage(source)?,
        depth,
        inherited,
        group: group_from_storage(group)?,
        permission: grant_from_storage(grant),
    })
}
