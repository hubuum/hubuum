use hubuum_storage_core::{
    StorageAuthorizationCollection, StorageAuthorizationEffectiveGroupGrant,
    StorageAuthorizationGrant, StorageAuthorizationGroup, StorageAuthorizationGroupGrant,
    StorageAuthorizationPermission,
};

use crate::errors::ApiError;
use crate::models::{Collection, Group, GroupPermission, Permission, Permissions};

pub(crate) const fn permission_to_storage(
    permission: Permissions,
) -> StorageAuthorizationPermission {
    match permission {
        Permissions::ReadCollection => StorageAuthorizationPermission::ReadCollection,
        Permissions::UpdateCollection => StorageAuthorizationPermission::UpdateCollection,
        Permissions::DeleteCollection => StorageAuthorizationPermission::DeleteCollection,
        Permissions::DelegateCollection => StorageAuthorizationPermission::DelegateCollection,
        Permissions::CreateClass => StorageAuthorizationPermission::CreateClass,
        Permissions::ReadClass => StorageAuthorizationPermission::ReadClass,
        Permissions::UpdateClass => StorageAuthorizationPermission::UpdateClass,
        Permissions::DeleteClass => StorageAuthorizationPermission::DeleteClass,
        Permissions::CreateObject => StorageAuthorizationPermission::CreateObject,
        Permissions::ReadObject => StorageAuthorizationPermission::ReadObject,
        Permissions::UpdateObject => StorageAuthorizationPermission::UpdateObject,
        Permissions::DeleteObject => StorageAuthorizationPermission::DeleteObject,
        Permissions::CreateClassRelation => StorageAuthorizationPermission::CreateClassRelation,
        Permissions::ReadClassRelation => StorageAuthorizationPermission::ReadClassRelation,
        Permissions::UpdateClassRelation => StorageAuthorizationPermission::UpdateClassRelation,
        Permissions::DeleteClassRelation => StorageAuthorizationPermission::DeleteClassRelation,
        Permissions::CreateObjectRelation => StorageAuthorizationPermission::CreateObjectRelation,
        Permissions::ReadObjectRelation => StorageAuthorizationPermission::ReadObjectRelation,
        Permissions::UpdateObjectRelation => StorageAuthorizationPermission::UpdateObjectRelation,
        Permissions::DeleteObjectRelation => StorageAuthorizationPermission::DeleteObjectRelation,
        Permissions::ReadTemplate => StorageAuthorizationPermission::ReadTemplate,
        Permissions::CreateTemplate => StorageAuthorizationPermission::CreateTemplate,
        Permissions::UpdateTemplate => StorageAuthorizationPermission::UpdateTemplate,
        Permissions::DeleteTemplate => StorageAuthorizationPermission::DeleteTemplate,
        Permissions::ReadRemoteTarget => StorageAuthorizationPermission::ReadRemoteTarget,
        Permissions::CreateRemoteTarget => StorageAuthorizationPermission::CreateRemoteTarget,
        Permissions::UpdateRemoteTarget => StorageAuthorizationPermission::UpdateRemoteTarget,
        Permissions::DeleteRemoteTarget => StorageAuthorizationPermission::DeleteRemoteTarget,
        Permissions::ExecuteRemoteTarget => StorageAuthorizationPermission::ExecuteRemoteTarget,
        Permissions::ReadAudit => StorageAuthorizationPermission::ReadAudit,
        Permissions::ManageEventSubscription => {
            StorageAuthorizationPermission::ManageEventSubscription
        }
    }
}

pub(crate) const fn permission_from_storage(
    permission: StorageAuthorizationPermission,
) -> Permissions {
    match permission {
        StorageAuthorizationPermission::ReadCollection => Permissions::ReadCollection,
        StorageAuthorizationPermission::UpdateCollection => Permissions::UpdateCollection,
        StorageAuthorizationPermission::DeleteCollection => Permissions::DeleteCollection,
        StorageAuthorizationPermission::DelegateCollection => Permissions::DelegateCollection,
        StorageAuthorizationPermission::CreateClass => Permissions::CreateClass,
        StorageAuthorizationPermission::ReadClass => Permissions::ReadClass,
        StorageAuthorizationPermission::UpdateClass => Permissions::UpdateClass,
        StorageAuthorizationPermission::DeleteClass => Permissions::DeleteClass,
        StorageAuthorizationPermission::CreateObject => Permissions::CreateObject,
        StorageAuthorizationPermission::ReadObject => Permissions::ReadObject,
        StorageAuthorizationPermission::UpdateObject => Permissions::UpdateObject,
        StorageAuthorizationPermission::DeleteObject => Permissions::DeleteObject,
        StorageAuthorizationPermission::CreateClassRelation => Permissions::CreateClassRelation,
        StorageAuthorizationPermission::ReadClassRelation => Permissions::ReadClassRelation,
        StorageAuthorizationPermission::UpdateClassRelation => Permissions::UpdateClassRelation,
        StorageAuthorizationPermission::DeleteClassRelation => Permissions::DeleteClassRelation,
        StorageAuthorizationPermission::CreateObjectRelation => Permissions::CreateObjectRelation,
        StorageAuthorizationPermission::ReadObjectRelation => Permissions::ReadObjectRelation,
        StorageAuthorizationPermission::UpdateObjectRelation => Permissions::UpdateObjectRelation,
        StorageAuthorizationPermission::DeleteObjectRelation => Permissions::DeleteObjectRelation,
        StorageAuthorizationPermission::ReadTemplate => Permissions::ReadTemplate,
        StorageAuthorizationPermission::CreateTemplate => Permissions::CreateTemplate,
        StorageAuthorizationPermission::UpdateTemplate => Permissions::UpdateTemplate,
        StorageAuthorizationPermission::DeleteTemplate => Permissions::DeleteTemplate,
        StorageAuthorizationPermission::ReadRemoteTarget => Permissions::ReadRemoteTarget,
        StorageAuthorizationPermission::CreateRemoteTarget => Permissions::CreateRemoteTarget,
        StorageAuthorizationPermission::UpdateRemoteTarget => Permissions::UpdateRemoteTarget,
        StorageAuthorizationPermission::DeleteRemoteTarget => Permissions::DeleteRemoteTarget,
        StorageAuthorizationPermission::ExecuteRemoteTarget => Permissions::ExecuteRemoteTarget,
        StorageAuthorizationPermission::ReadAudit => Permissions::ReadAudit,
        StorageAuthorizationPermission::ManageEventSubscription => {
            Permissions::ManageEventSubscription
        }
    }
}

pub(crate) fn collection_from_storage(
    collection: StorageAuthorizationCollection,
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

pub(crate) fn group_from_storage(group: StorageAuthorizationGroup) -> Result<Group, ApiError> {
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

pub(crate) fn grant_from_storage(grant: StorageAuthorizationGrant) -> Permission {
    let has = |permission| grant.permissions().contains(&permission);
    Permission {
        id: grant.id().id(),
        collection_id: grant.collection_id().id(),
        group_id: grant.group_id().id(),
        has_read_collection: has(StorageAuthorizationPermission::ReadCollection),
        has_update_collection: has(StorageAuthorizationPermission::UpdateCollection),
        has_delete_collection: has(StorageAuthorizationPermission::DeleteCollection),
        has_delegate_collection: has(StorageAuthorizationPermission::DelegateCollection),
        has_create_class: has(StorageAuthorizationPermission::CreateClass),
        has_read_class: has(StorageAuthorizationPermission::ReadClass),
        has_update_class: has(StorageAuthorizationPermission::UpdateClass),
        has_delete_class: has(StorageAuthorizationPermission::DeleteClass),
        has_create_object: has(StorageAuthorizationPermission::CreateObject),
        has_read_object: has(StorageAuthorizationPermission::ReadObject),
        has_update_object: has(StorageAuthorizationPermission::UpdateObject),
        has_delete_object: has(StorageAuthorizationPermission::DeleteObject),
        has_create_class_relation: has(StorageAuthorizationPermission::CreateClassRelation),
        has_read_class_relation: has(StorageAuthorizationPermission::ReadClassRelation),
        has_update_class_relation: has(StorageAuthorizationPermission::UpdateClassRelation),
        has_delete_class_relation: has(StorageAuthorizationPermission::DeleteClassRelation),
        has_create_object_relation: has(StorageAuthorizationPermission::CreateObjectRelation),
        has_read_object_relation: has(StorageAuthorizationPermission::ReadObjectRelation),
        has_update_object_relation: has(StorageAuthorizationPermission::UpdateObjectRelation),
        has_delete_object_relation: has(StorageAuthorizationPermission::DeleteObjectRelation),
        has_read_template: has(StorageAuthorizationPermission::ReadTemplate),
        has_create_template: has(StorageAuthorizationPermission::CreateTemplate),
        has_update_template: has(StorageAuthorizationPermission::UpdateTemplate),
        has_delete_template: has(StorageAuthorizationPermission::DeleteTemplate),
        has_read_remote_target: has(StorageAuthorizationPermission::ReadRemoteTarget),
        has_create_remote_target: has(StorageAuthorizationPermission::CreateRemoteTarget),
        has_update_remote_target: has(StorageAuthorizationPermission::UpdateRemoteTarget),
        has_delete_remote_target: has(StorageAuthorizationPermission::DeleteRemoteTarget),
        has_execute_remote_target: has(StorageAuthorizationPermission::ExecuteRemoteTarget),
        created_at: grant.created_at().naive_utc(),
        updated_at: grant.updated_at().naive_utc(),
        has_read_audit: has(StorageAuthorizationPermission::ReadAudit),
        has_manage_event_subscription: has(StorageAuthorizationPermission::ManageEventSubscription),
    }
}

pub(crate) fn group_grant_from_storage(
    row: StorageAuthorizationGroupGrant,
) -> Result<GroupPermission, ApiError> {
    let (group, grant) = row.into_parts();
    Ok(GroupPermission {
        group: group_from_storage(group)?,
        permission: grant_from_storage(grant),
    })
}

pub(crate) fn effective_group_grant_from_storage(
    row: StorageAuthorizationEffectiveGroupGrant,
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
