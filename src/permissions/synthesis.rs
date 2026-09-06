use chrono::DateTime;

use crate::models::{Group, Permission, Permissions};

/// Build a synthetic Permission row from a per-variant decision list.
/// Used by both MockTreetopBackend and TreetopPermissionBackend for
/// synthesizing Permission rows from per-variant authorize results.
pub(crate) fn synthesize_permission(
    collection_id: i32,
    group_id: i32,
    decisions: &[bool],
) -> Permission {
    use Permissions::*;
    let synthetic_timestamp = DateTime::UNIX_EPOCH.naive_utc();

    let perms = Permissions::all();
    debug_assert_eq!(
        perms.len(),
        decisions.len(),
        "synthesize_permission: decisions length must match Permissions::all() length"
    );

    let mut row = Permission {
        // Synthetic rows have no database identity. Reusing the group id gives
        // cursor pagination a stable, unique key across requests.
        id: group_id,
        collection_id,
        group_id,
        has_read_collection: false,
        has_update_collection: false,
        has_delete_collection: false,
        has_delegate_collection: false,
        has_create_class: false,
        has_read_class: false,
        has_update_class: false,
        has_delete_class: false,
        has_create_object: false,
        has_read_object: false,
        has_update_object: false,
        has_delete_object: false,
        has_create_class_relation: false,
        has_read_class_relation: false,
        has_update_class_relation: false,
        has_delete_class_relation: false,
        has_create_object_relation: false,
        has_read_object_relation: false,
        has_update_object_relation: false,
        has_delete_object_relation: false,
        has_read_template: false,
        has_create_template: false,
        has_update_template: false,
        has_delete_template: false,
        has_read_remote_target: false,
        has_create_remote_target: false,
        has_update_remote_target: false,
        has_delete_remote_target: false,
        has_execute_remote_target: false,
        has_read_audit: false,
        has_manage_event_subscription: false,
        created_at: synthetic_timestamp,
        updated_at: synthetic_timestamp,
    };

    for (perm, decision) in perms.iter().zip(decisions) {
        if !decision {
            continue;
        }
        match perm {
            ReadCollection => row.has_read_collection = true,
            UpdateCollection => row.has_update_collection = true,
            DeleteCollection => row.has_delete_collection = true,
            DelegateCollection => row.has_delegate_collection = true,
            CreateClass => row.has_create_class = true,
            ReadClass => row.has_read_class = true,
            UpdateClass => row.has_update_class = true,
            DeleteClass => row.has_delete_class = true,
            CreateObject => row.has_create_object = true,
            ReadObject => row.has_read_object = true,
            UpdateObject => row.has_update_object = true,
            DeleteObject => row.has_delete_object = true,
            CreateClassRelation => row.has_create_class_relation = true,
            ReadClassRelation => row.has_read_class_relation = true,
            UpdateClassRelation => row.has_update_class_relation = true,
            DeleteClassRelation => row.has_delete_class_relation = true,
            CreateObjectRelation => row.has_create_object_relation = true,
            ReadObjectRelation => row.has_read_object_relation = true,
            UpdateObjectRelation => row.has_update_object_relation = true,
            DeleteObjectRelation => row.has_delete_object_relation = true,
            ReadTemplate => row.has_read_template = true,
            CreateTemplate => row.has_create_template = true,
            UpdateTemplate => row.has_update_template = true,
            DeleteTemplate => row.has_delete_template = true,
            ReadRemoteTarget => row.has_read_remote_target = true,
            CreateRemoteTarget => row.has_create_remote_target = true,
            UpdateRemoteTarget => row.has_update_remote_target = true,
            DeleteRemoteTarget => row.has_delete_remote_target = true,
            ExecuteRemoteTarget => row.has_execute_remote_target = true,
            ReadAudit => row.has_read_audit = true,
            ManageEventSubscription => row.has_manage_event_subscription = true,
        }
    }

    row
}

pub(crate) fn synthesize_permission_for_group(
    collection_id: i32,
    group: &Group,
    decisions: &[bool],
) -> Permission {
    let mut permission = synthesize_permission(collection_id, group.id, decisions);
    permission.created_at = group.created_at;
    permission.updated_at = group.updated_at;
    permission
}

/// Whether a synthesized Permission has at least one true field.
pub(crate) fn permission_has_any_grant(p: &Permission) -> bool {
    p.has_read_collection
        || p.has_update_collection
        || p.has_delete_collection
        || p.has_delegate_collection
        || p.has_create_class
        || p.has_read_class
        || p.has_update_class
        || p.has_delete_class
        || p.has_create_object
        || p.has_read_object
        || p.has_update_object
        || p.has_delete_object
        || p.has_create_class_relation
        || p.has_read_class_relation
        || p.has_update_class_relation
        || p.has_delete_class_relation
        || p.has_create_object_relation
        || p.has_read_object_relation
        || p.has_update_object_relation
        || p.has_delete_object_relation
        || p.has_read_template
        || p.has_create_template
        || p.has_update_template
        || p.has_delete_template
        || p.has_read_remote_target
        || p.has_create_remote_target
        || p.has_update_remote_target
        || p.has_delete_remote_target
        || p.has_execute_remote_target
        || p.has_read_audit
        || p.has_manage_event_subscription
}
