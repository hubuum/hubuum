//! Transitional permission-row types used by application-owned import code.
//!
//! Runtime authorization queries and mutations are owned by the PostgreSQL
//! adapter crate. These rows remain until import persistence crosses the same
//! storage DTO boundary.

use crate::models::{Permission, Permissions, PermissionsList};
use crate::schema::permissions;
use crate::storage::postgres::prelude::*;

#[derive(Debug, Queryable, Selectable, Clone, Copy)]
#[diesel(table_name = permissions)]
pub(crate) struct PermissionRow {
    pub(crate) id: i32,
    pub(crate) collection_id: i32,
    pub(crate) group_id: i32,
    pub(crate) has_read_collection: bool,
    pub(crate) has_update_collection: bool,
    pub(crate) has_delete_collection: bool,
    pub(crate) has_delegate_collection: bool,
    pub(crate) has_create_class: bool,
    pub(crate) has_read_class: bool,
    pub(crate) has_update_class: bool,
    pub(crate) has_delete_class: bool,
    pub(crate) has_create_object: bool,
    pub(crate) has_read_object: bool,
    pub(crate) has_update_object: bool,
    pub(crate) has_delete_object: bool,
    pub(crate) has_create_class_relation: bool,
    pub(crate) has_read_class_relation: bool,
    pub(crate) has_update_class_relation: bool,
    pub(crate) has_delete_class_relation: bool,
    pub(crate) has_create_object_relation: bool,
    pub(crate) has_read_object_relation: bool,
    pub(crate) has_update_object_relation: bool,
    pub(crate) has_delete_object_relation: bool,
    pub(crate) has_read_template: bool,
    pub(crate) has_create_template: bool,
    pub(crate) has_update_template: bool,
    pub(crate) has_delete_template: bool,
    pub(crate) has_read_remote_target: bool,
    pub(crate) has_create_remote_target: bool,
    pub(crate) has_update_remote_target: bool,
    pub(crate) has_delete_remote_target: bool,
    pub(crate) has_execute_remote_target: bool,
    pub(crate) created_at: chrono::NaiveDateTime,
    pub(crate) updated_at: chrono::NaiveDateTime,
    pub(crate) has_read_audit: bool,
    pub(crate) has_manage_event_subscription: bool,
}

impl From<PermissionRow> for Permission {
    fn from(row: PermissionRow) -> Self {
        Self {
            id: row.id,
            collection_id: row.collection_id,
            group_id: row.group_id,
            has_read_collection: row.has_read_collection,
            has_update_collection: row.has_update_collection,
            has_delete_collection: row.has_delete_collection,
            has_delegate_collection: row.has_delegate_collection,
            has_create_class: row.has_create_class,
            has_read_class: row.has_read_class,
            has_update_class: row.has_update_class,
            has_delete_class: row.has_delete_class,
            has_create_object: row.has_create_object,
            has_read_object: row.has_read_object,
            has_update_object: row.has_update_object,
            has_delete_object: row.has_delete_object,
            has_create_class_relation: row.has_create_class_relation,
            has_read_class_relation: row.has_read_class_relation,
            has_update_class_relation: row.has_update_class_relation,
            has_delete_class_relation: row.has_delete_class_relation,
            has_create_object_relation: row.has_create_object_relation,
            has_read_object_relation: row.has_read_object_relation,
            has_update_object_relation: row.has_update_object_relation,
            has_delete_object_relation: row.has_delete_object_relation,
            has_read_template: row.has_read_template,
            has_create_template: row.has_create_template,
            has_update_template: row.has_update_template,
            has_delete_template: row.has_delete_template,
            has_read_remote_target: row.has_read_remote_target,
            has_create_remote_target: row.has_create_remote_target,
            has_update_remote_target: row.has_update_remote_target,
            has_delete_remote_target: row.has_delete_remote_target,
            has_execute_remote_target: row.has_execute_remote_target,
            created_at: row.created_at,
            updated_at: row.updated_at,
            has_read_audit: row.has_read_audit,
            has_manage_event_subscription: row.has_manage_event_subscription,
        }
    }
}

#[derive(Debug, Insertable)]
#[diesel(table_name = permissions)]
pub(crate) struct NewPermission {
    pub(crate) collection_id: i32,
    pub(crate) group_id: i32,
    pub(crate) has_read_collection: bool,
    pub(crate) has_update_collection: bool,
    pub(crate) has_delete_collection: bool,
    pub(crate) has_delegate_collection: bool,
    pub(crate) has_create_class: bool,
    pub(crate) has_read_class: bool,
    pub(crate) has_update_class: bool,
    pub(crate) has_delete_class: bool,
    pub(crate) has_create_object: bool,
    pub(crate) has_read_object: bool,
    pub(crate) has_update_object: bool,
    pub(crate) has_delete_object: bool,
    pub(crate) has_create_class_relation: bool,
    pub(crate) has_read_class_relation: bool,
    pub(crate) has_update_class_relation: bool,
    pub(crate) has_delete_class_relation: bool,
    pub(crate) has_create_object_relation: bool,
    pub(crate) has_read_object_relation: bool,
    pub(crate) has_update_object_relation: bool,
    pub(crate) has_delete_object_relation: bool,
    pub(crate) has_read_template: bool,
    pub(crate) has_create_template: bool,
    pub(crate) has_update_template: bool,
    pub(crate) has_delete_template: bool,
    pub(crate) has_read_remote_target: bool,
    pub(crate) has_create_remote_target: bool,
    pub(crate) has_update_remote_target: bool,
    pub(crate) has_delete_remote_target: bool,
    pub(crate) has_execute_remote_target: bool,
    pub(crate) has_read_audit: bool,
    pub(crate) has_manage_event_subscription: bool,
}

#[derive(Debug, AsChangeset, Default)]
#[diesel(table_name = permissions)]
pub(crate) struct UpdatePermission {
    pub(crate) has_read_collection: Option<bool>,
    pub(crate) has_update_collection: Option<bool>,
    pub(crate) has_delete_collection: Option<bool>,
    pub(crate) has_delegate_collection: Option<bool>,
    pub(crate) has_create_class: Option<bool>,
    pub(crate) has_read_class: Option<bool>,
    pub(crate) has_update_class: Option<bool>,
    pub(crate) has_delete_class: Option<bool>,
    pub(crate) has_create_object: Option<bool>,
    pub(crate) has_read_object: Option<bool>,
    pub(crate) has_update_object: Option<bool>,
    pub(crate) has_delete_object: Option<bool>,
    pub(crate) has_create_class_relation: Option<bool>,
    pub(crate) has_read_class_relation: Option<bool>,
    pub(crate) has_update_class_relation: Option<bool>,
    pub(crate) has_delete_class_relation: Option<bool>,
    pub(crate) has_create_object_relation: Option<bool>,
    pub(crate) has_read_object_relation: Option<bool>,
    pub(crate) has_update_object_relation: Option<bool>,
    pub(crate) has_delete_object_relation: Option<bool>,
    pub(crate) has_read_template: Option<bool>,
    pub(crate) has_create_template: Option<bool>,
    pub(crate) has_update_template: Option<bool>,
    pub(crate) has_delete_template: Option<bool>,
    pub(crate) has_read_remote_target: Option<bool>,
    pub(crate) has_create_remote_target: Option<bool>,
    pub(crate) has_update_remote_target: Option<bool>,
    pub(crate) has_delete_remote_target: Option<bool>,
    pub(crate) has_execute_remote_target: Option<bool>,
    pub(crate) has_read_audit: Option<bool>,
    pub(crate) has_manage_event_subscription: Option<bool>,
}

pub(crate) trait PermissionFilter<'a, Q> {
    fn create_boxed_filter(self, query: Q, target: bool) -> Q;
}

impl<'a> PermissionFilter<'a, permissions::BoxedQuery<'a, diesel::pg::Pg>> for Permissions {
    fn create_boxed_filter(
        self,
        mut query: permissions::BoxedQuery<'a, diesel::pg::Pg>,
        target: bool,
    ) -> permissions::BoxedQuery<'a, diesel::pg::Pg> {
        crate::apply_permission_filter!(query, self, target);
        query
    }
}

pub(crate) fn new_permission_from_list(
    target_collection_id: i32,
    group_id: i32,
    permissions: &PermissionsList,
) -> NewPermission {
    let has = |permission| permissions.contains(&permission);
    NewPermission {
        collection_id: target_collection_id,
        group_id,
        has_read_collection: has(Permissions::ReadCollection),
        has_update_collection: has(Permissions::UpdateCollection),
        has_delete_collection: has(Permissions::DeleteCollection),
        has_delegate_collection: has(Permissions::DelegateCollection),
        has_create_class: has(Permissions::CreateClass),
        has_read_class: has(Permissions::ReadClass),
        has_update_class: has(Permissions::UpdateClass),
        has_delete_class: has(Permissions::DeleteClass),
        has_create_object: has(Permissions::CreateObject),
        has_read_object: has(Permissions::ReadObject),
        has_update_object: has(Permissions::UpdateObject),
        has_delete_object: has(Permissions::DeleteObject),
        has_create_class_relation: has(Permissions::CreateClassRelation),
        has_read_class_relation: has(Permissions::ReadClassRelation),
        has_update_class_relation: has(Permissions::UpdateClassRelation),
        has_delete_class_relation: has(Permissions::DeleteClassRelation),
        has_create_object_relation: has(Permissions::CreateObjectRelation),
        has_read_object_relation: has(Permissions::ReadObjectRelation),
        has_update_object_relation: has(Permissions::UpdateObjectRelation),
        has_delete_object_relation: has(Permissions::DeleteObjectRelation),
        has_read_template: has(Permissions::ReadTemplate),
        has_create_template: has(Permissions::CreateTemplate),
        has_update_template: has(Permissions::UpdateTemplate),
        has_delete_template: has(Permissions::DeleteTemplate),
        has_read_remote_target: has(Permissions::ReadRemoteTarget),
        has_create_remote_target: has(Permissions::CreateRemoteTarget),
        has_update_remote_target: has(Permissions::UpdateRemoteTarget),
        has_delete_remote_target: has(Permissions::DeleteRemoteTarget),
        has_execute_remote_target: has(Permissions::ExecuteRemoteTarget),
        has_read_audit: has(Permissions::ReadAudit),
        has_manage_event_subscription: has(Permissions::ManageEventSubscription),
    }
}

#[cfg(test)]
mod tests {
    use super::{PermissionFilter, Permissions};
    use crate::schema::permissions::dsl::permissions;
    use crate::storage::postgres::prelude::*;

    #[test]
    fn template_permissions_filter_map_to_expected_columns() {
        let fixtures = [
            (Permissions::ReadTemplate, "has_read_template"),
            (Permissions::CreateTemplate, "has_create_template"),
            (Permissions::UpdateTemplate, "has_update_template"),
            (Permissions::DeleteTemplate, "has_delete_template"),
        ];

        for (permission, expected_column) in fixtures {
            let base_query = permissions.into_boxed();
            let filtered = permission.create_boxed_filter(base_query, true);
            let sql = diesel::debug_query::<diesel::pg::Pg, _>(&filtered).to_string();
            assert!(
                sql.contains(expected_column),
                "Expected SQL to contain '{expected_column}', got: {sql}"
            );
        }
    }
}
