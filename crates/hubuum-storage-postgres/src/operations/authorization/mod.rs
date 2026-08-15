//! PostgreSQL implementation of Hubuum's local authorization data contract.

macro_rules! apply_permission_filter {
    ($query:ident, $permission:expr, $target:expr) => {
        match $permission {
            hubuum_storage_core::AuthorizationPermission::ReadCollection => {
                $query = $query.filter(crate::schema::permissions::has_read_collection.eq($target));
            }
            hubuum_storage_core::AuthorizationPermission::UpdateCollection => {
                $query =
                    $query.filter(crate::schema::permissions::has_update_collection.eq($target));
            }
            hubuum_storage_core::AuthorizationPermission::DeleteCollection => {
                $query =
                    $query.filter(crate::schema::permissions::has_delete_collection.eq($target));
            }
            hubuum_storage_core::AuthorizationPermission::DelegateCollection => {
                $query =
                    $query.filter(crate::schema::permissions::has_delegate_collection.eq($target));
            }
            hubuum_storage_core::AuthorizationPermission::CreateClass => {
                $query = $query.filter(crate::schema::permissions::has_create_class.eq($target));
            }
            hubuum_storage_core::AuthorizationPermission::ReadClass => {
                $query = $query.filter(crate::schema::permissions::has_read_class.eq($target));
            }
            hubuum_storage_core::AuthorizationPermission::UpdateClass => {
                $query = $query.filter(crate::schema::permissions::has_update_class.eq($target));
            }
            hubuum_storage_core::AuthorizationPermission::DeleteClass => {
                $query = $query.filter(crate::schema::permissions::has_delete_class.eq($target));
            }
            hubuum_storage_core::AuthorizationPermission::CreateObject => {
                $query = $query.filter(crate::schema::permissions::has_create_object.eq($target));
            }
            hubuum_storage_core::AuthorizationPermission::ReadObject => {
                $query = $query.filter(crate::schema::permissions::has_read_object.eq($target));
            }
            hubuum_storage_core::AuthorizationPermission::UpdateObject => {
                $query = $query.filter(crate::schema::permissions::has_update_object.eq($target));
            }
            hubuum_storage_core::AuthorizationPermission::DeleteObject => {
                $query = $query.filter(crate::schema::permissions::has_delete_object.eq($target));
            }
            hubuum_storage_core::AuthorizationPermission::CreateClassRelation => {
                $query = $query
                    .filter(crate::schema::permissions::has_create_class_relation.eq($target));
            }
            hubuum_storage_core::AuthorizationPermission::ReadClassRelation => {
                $query =
                    $query.filter(crate::schema::permissions::has_read_class_relation.eq($target));
            }
            hubuum_storage_core::AuthorizationPermission::UpdateClassRelation => {
                $query = $query
                    .filter(crate::schema::permissions::has_update_class_relation.eq($target));
            }
            hubuum_storage_core::AuthorizationPermission::DeleteClassRelation => {
                $query = $query
                    .filter(crate::schema::permissions::has_delete_class_relation.eq($target));
            }
            hubuum_storage_core::AuthorizationPermission::CreateObjectRelation => {
                $query = $query
                    .filter(crate::schema::permissions::has_create_object_relation.eq($target));
            }
            hubuum_storage_core::AuthorizationPermission::ReadObjectRelation => {
                $query =
                    $query.filter(crate::schema::permissions::has_read_object_relation.eq($target));
            }
            hubuum_storage_core::AuthorizationPermission::UpdateObjectRelation => {
                $query = $query
                    .filter(crate::schema::permissions::has_update_object_relation.eq($target));
            }
            hubuum_storage_core::AuthorizationPermission::DeleteObjectRelation => {
                $query = $query
                    .filter(crate::schema::permissions::has_delete_object_relation.eq($target));
            }
            hubuum_storage_core::AuthorizationPermission::ReadTemplate => {
                $query = $query.filter(crate::schema::permissions::has_read_template.eq($target));
            }
            hubuum_storage_core::AuthorizationPermission::CreateTemplate => {
                $query = $query.filter(crate::schema::permissions::has_create_template.eq($target));
            }
            hubuum_storage_core::AuthorizationPermission::UpdateTemplate => {
                $query = $query.filter(crate::schema::permissions::has_update_template.eq($target));
            }
            hubuum_storage_core::AuthorizationPermission::DeleteTemplate => {
                $query = $query.filter(crate::schema::permissions::has_delete_template.eq($target));
            }
            hubuum_storage_core::AuthorizationPermission::ReadRemoteTarget => {
                $query =
                    $query.filter(crate::schema::permissions::has_read_remote_target.eq($target));
            }
            hubuum_storage_core::AuthorizationPermission::CreateRemoteTarget => {
                $query =
                    $query.filter(crate::schema::permissions::has_create_remote_target.eq($target));
            }
            hubuum_storage_core::AuthorizationPermission::UpdateRemoteTarget => {
                $query =
                    $query.filter(crate::schema::permissions::has_update_remote_target.eq($target));
            }
            hubuum_storage_core::AuthorizationPermission::DeleteRemoteTarget => {
                $query =
                    $query.filter(crate::schema::permissions::has_delete_remote_target.eq($target));
            }
            hubuum_storage_core::AuthorizationPermission::ExecuteRemoteTarget => {
                $query = $query
                    .filter(crate::schema::permissions::has_execute_remote_target.eq($target));
            }
            hubuum_storage_core::AuthorizationPermission::ReadAudit => {
                $query = $query.filter(crate::schema::permissions::has_read_audit.eq($target));
            }
            hubuum_storage_core::AuthorizationPermission::ManageEventSubscription => {
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
    authorization_policy_snapshot, authorization_principal_is_group_member,
    authorize_local_collection, authorize_local_collections, collection_group_permission,
    effective_group_collection_permissions, effective_principal_collection_permissions,
    get_local_collection_grant, group_has_collection_permission, groups_with_collection_permission,
    groups_with_collection_permission_page, list_authorization_collection_candidates,
    list_authorization_group_candidates, list_local_collection_grants, load_authorization_classes,
    load_authorization_objects, load_authorization_principal, load_local_collection_permission_set,
    local_authorized_collections, principal_all_collection_permissions,
    principal_collection_permissions, principal_collection_permissions_page, visible_collections,
};
