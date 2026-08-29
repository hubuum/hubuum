//! PostgreSQL implementation of Hubuum's local authorization data contract.

macro_rules! apply_permission_filter {
    ($query:ident, $permission:expr, $target:expr) => {
        match $permission {
            hubuum_storage_core::StorageAuthorizationPermission::ReadCollection => {
                $query = $query.filter(crate::schema::permissions::has_read_collection.eq($target));
            }
            hubuum_storage_core::StorageAuthorizationPermission::UpdateCollection => {
                $query =
                    $query.filter(crate::schema::permissions::has_update_collection.eq($target));
            }
            hubuum_storage_core::StorageAuthorizationPermission::DeleteCollection => {
                $query =
                    $query.filter(crate::schema::permissions::has_delete_collection.eq($target));
            }
            hubuum_storage_core::StorageAuthorizationPermission::DelegateCollection => {
                $query =
                    $query.filter(crate::schema::permissions::has_delegate_collection.eq($target));
            }
            hubuum_storage_core::StorageAuthorizationPermission::CreateClass => {
                $query = $query.filter(crate::schema::permissions::has_create_class.eq($target));
            }
            hubuum_storage_core::StorageAuthorizationPermission::ReadClass => {
                $query = $query.filter(crate::schema::permissions::has_read_class.eq($target));
            }
            hubuum_storage_core::StorageAuthorizationPermission::UpdateClass => {
                $query = $query.filter(crate::schema::permissions::has_update_class.eq($target));
            }
            hubuum_storage_core::StorageAuthorizationPermission::DeleteClass => {
                $query = $query.filter(crate::schema::permissions::has_delete_class.eq($target));
            }
            hubuum_storage_core::StorageAuthorizationPermission::CreateObject => {
                $query = $query.filter(crate::schema::permissions::has_create_object.eq($target));
            }
            hubuum_storage_core::StorageAuthorizationPermission::ReadObject => {
                $query = $query.filter(crate::schema::permissions::has_read_object.eq($target));
            }
            hubuum_storage_core::StorageAuthorizationPermission::UpdateObject => {
                $query = $query.filter(crate::schema::permissions::has_update_object.eq($target));
            }
            hubuum_storage_core::StorageAuthorizationPermission::DeleteObject => {
                $query = $query.filter(crate::schema::permissions::has_delete_object.eq($target));
            }
            hubuum_storage_core::StorageAuthorizationPermission::CreateClassRelation => {
                $query = $query
                    .filter(crate::schema::permissions::has_create_class_relation.eq($target));
            }
            hubuum_storage_core::StorageAuthorizationPermission::ReadClassRelation => {
                $query =
                    $query.filter(crate::schema::permissions::has_read_class_relation.eq($target));
            }
            hubuum_storage_core::StorageAuthorizationPermission::UpdateClassRelation => {
                $query = $query
                    .filter(crate::schema::permissions::has_update_class_relation.eq($target));
            }
            hubuum_storage_core::StorageAuthorizationPermission::DeleteClassRelation => {
                $query = $query
                    .filter(crate::schema::permissions::has_delete_class_relation.eq($target));
            }
            hubuum_storage_core::StorageAuthorizationPermission::CreateObjectRelation => {
                $query = $query
                    .filter(crate::schema::permissions::has_create_object_relation.eq($target));
            }
            hubuum_storage_core::StorageAuthorizationPermission::ReadObjectRelation => {
                $query =
                    $query.filter(crate::schema::permissions::has_read_object_relation.eq($target));
            }
            hubuum_storage_core::StorageAuthorizationPermission::UpdateObjectRelation => {
                $query = $query
                    .filter(crate::schema::permissions::has_update_object_relation.eq($target));
            }
            hubuum_storage_core::StorageAuthorizationPermission::DeleteObjectRelation => {
                $query = $query
                    .filter(crate::schema::permissions::has_delete_object_relation.eq($target));
            }
            hubuum_storage_core::StorageAuthorizationPermission::ReadTemplate => {
                $query = $query.filter(crate::schema::permissions::has_read_template.eq($target));
            }
            hubuum_storage_core::StorageAuthorizationPermission::CreateTemplate => {
                $query = $query.filter(crate::schema::permissions::has_create_template.eq($target));
            }
            hubuum_storage_core::StorageAuthorizationPermission::UpdateTemplate => {
                $query = $query.filter(crate::schema::permissions::has_update_template.eq($target));
            }
            hubuum_storage_core::StorageAuthorizationPermission::DeleteTemplate => {
                $query = $query.filter(crate::schema::permissions::has_delete_template.eq($target));
            }
            hubuum_storage_core::StorageAuthorizationPermission::ReadRemoteTarget => {
                $query =
                    $query.filter(crate::schema::permissions::has_read_remote_target.eq($target));
            }
            hubuum_storage_core::StorageAuthorizationPermission::CreateRemoteTarget => {
                $query =
                    $query.filter(crate::schema::permissions::has_create_remote_target.eq($target));
            }
            hubuum_storage_core::StorageAuthorizationPermission::UpdateRemoteTarget => {
                $query =
                    $query.filter(crate::schema::permissions::has_update_remote_target.eq($target));
            }
            hubuum_storage_core::StorageAuthorizationPermission::DeleteRemoteTarget => {
                $query =
                    $query.filter(crate::schema::permissions::has_delete_remote_target.eq($target));
            }
            hubuum_storage_core::StorageAuthorizationPermission::ExecuteRemoteTarget => {
                $query = $query
                    .filter(crate::schema::permissions::has_execute_remote_target.eq($target));
            }
            hubuum_storage_core::StorageAuthorizationPermission::ReadAudit => {
                $query = $query.filter(crate::schema::permissions::has_read_audit.eq($target));
            }
            hubuum_storage_core::StorageAuthorizationPermission::ManageEventSubscription => {
                $query = $query
                    .filter(crate::schema::permissions::has_manage_event_subscription.eq($target));
            }
        }
    };
}

pub(crate) use apply_permission_filter;

mod mutations;
mod queries;
mod rows;

pub(crate) use mutations::{NewPermission, UpdatePermission, insert_full_collection_grant};
pub use mutations::{
    apply_local_collection_grant, revoke_all_local_collection_grants, revoke_local_collection_grant,
};
pub use queries::{
    authorize_local_collection, authorize_local_collections, get_authorization_policy_snapshot,
    get_authorization_principal, get_local_collection_grant, get_local_collection_permission_set,
    has_group_collection_permission, is_authorization_principal_group_member,
    list_all_principal_collection_permissions, list_authorization_classes,
    list_authorization_objects, list_effective_group_collection_permissions,
    list_effective_principal_collection_permissions, list_groups_with_collection_permission,
    list_local_authorized_collections, list_local_collection_grants,
    list_principal_collection_permissions, list_visible_collections,
    load_authorization_collection_candidates, load_authorization_group_candidates,
    load_groups_with_collection_permission, load_principal_collection_permissions,
};
