use crate::storage::postgres::prelude::*;

use crate::api::etag::RevisionOwner;
use crate::errors::ApiError;
use crate::events::{Action, EntityType, EventContext, NewEvent};
use crate::models::{Permission, Permissions, PermissionsList};
use crate::schema::permissions;
use crate::storage::postgres::operations::event_record::emit_event;
use crate::storage::postgres::with_transaction;

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

async fn permission_owner_revision(
    conn: &mut crate::storage::postgres::PostgresConnection,
    target_collection_id: i32,
) -> Result<crate::models::ResourceRevision, ApiError> {
    use crate::schema::collection_authorization_state::dsl::{
        collection_authorization_state, collection_id, revision,
    };

    Ok(collection_authorization_state
        .filter(collection_id.eq(target_collection_id))
        .select(revision)
        .first(conn)
        .await?)
}

async fn lock_permission_owner(
    conn: &mut crate::storage::postgres::PostgresConnection,
    target_collection_id: i32,
) -> Result<crate::models::ResourceRevision, ApiError> {
    use crate::schema::collection_authorization_state::dsl::{
        collection_authorization_state, collection_id, revision,
    };

    let owner_revision = collection_authorization_state
        .filter(collection_id.eq(target_collection_id))
        .select(revision)
        .for_update()
        .first(conn)
        .await?;
    crate::storage::postgres::assert_locked_revision_precondition(
        conn,
        &RevisionOwner::CollectionPermissions.key(target_collection_id),
        owner_revision,
    )
    .await?;
    Ok(owner_revision)
}

fn permission_names(permissions: &[Permissions]) -> Vec<String> {
    permissions.iter().map(ToString::to_string).collect()
}

fn granted_permission_names(permission: &Permission) -> Vec<String> {
    permission_names(&permission.granted())
}

fn permission_snapshot(
    permission: &Permission,
    revision: crate::models::ResourceRevision,
) -> serde_json::Value {
    serde_json::json!({
        "id": permission.id,
        "collection_id": permission.collection_id,
        "group_id": permission.group_id,
        "granted_permissions": granted_permission_names(permission),
        "revision": revision,
        "created_at": permission.created_at,
        "updated_at": permission.updated_at,
    })
}

fn empty_permission_snapshot(
    collection_id: i32,
    group_id: i32,
    revision: crate::models::ResourceRevision,
) -> serde_json::Value {
    serde_json::json!({
        "collection_id": collection_id,
        "group_id": group_id,
        "granted_permissions": Vec::<String>::new(),
        "revision": revision,
    })
}

fn permission_metadata(
    permission: &Permission,
    requested_permissions: &[Permissions],
    replace_existing: Option<bool>,
) -> serde_json::Value {
    let mut metadata = serde_json::json!({
        "collection_id": permission.collection_id,
        "group_id": permission.group_id,
        "requested_permissions": permission_names(requested_permissions),
        "granted_permissions": granted_permission_names(permission),
    });

    if let Some(replace_existing) = replace_existing {
        metadata["replace_existing"] = serde_json::json!(replace_existing);
    }

    metadata
}

fn permission_event(
    permission: &Permission,
    action: Action,
    context: &EventContext,
    summary: impl Into<String>,
    requested_permissions: &[Permissions],
    replace_existing: Option<bool>,
) -> Result<NewEvent, ApiError> {
    Ok(NewEvent::new(
        EntityType::Permission,
        action,
        context.actor_kind(),
        summary,
    )?
    .with_context(context)
    .with_entity_id(permission.id)
    .with_collection_id(permission.collection_id)
    .with_metadata(permission_metadata(
        permission,
        requested_permissions,
        replace_existing,
    )))
}

fn update_permission_for_grant(
    permission_list: &PermissionsList,
    replace_existing: bool,
) -> UpdatePermission {
    let mut update_perm = if replace_existing {
        UpdatePermission {
            has_read_collection: Some(false),
            has_update_collection: Some(false),
            has_delete_collection: Some(false),
            has_delegate_collection: Some(false),
            has_create_class: Some(false),
            has_read_class: Some(false),
            has_update_class: Some(false),
            has_delete_class: Some(false),
            has_create_object: Some(false),
            has_read_object: Some(false),
            has_update_object: Some(false),
            has_delete_object: Some(false),
            has_create_class_relation: Some(false),
            has_read_class_relation: Some(false),
            has_update_class_relation: Some(false),
            has_delete_class_relation: Some(false),
            has_create_object_relation: Some(false),
            has_read_object_relation: Some(false),
            has_update_object_relation: Some(false),
            has_delete_object_relation: Some(false),
            has_read_template: Some(false),
            has_create_template: Some(false),
            has_update_template: Some(false),
            has_delete_template: Some(false),
            has_read_remote_target: Some(false),
            has_create_remote_target: Some(false),
            has_update_remote_target: Some(false),
            has_delete_remote_target: Some(false),
            has_execute_remote_target: Some(false),
            has_read_audit: Some(false),
            has_manage_event_subscription: Some(false),
        }
    } else {
        UpdatePermission::default()
    };

    for permission in permission_list {
        match permission {
            Permissions::ReadCollection => update_perm.has_read_collection = Some(true),
            Permissions::UpdateCollection => update_perm.has_update_collection = Some(true),
            Permissions::DeleteCollection => update_perm.has_delete_collection = Some(true),
            Permissions::DelegateCollection => update_perm.has_delegate_collection = Some(true),
            Permissions::CreateClass => update_perm.has_create_class = Some(true),
            Permissions::ReadClass => update_perm.has_read_class = Some(true),
            Permissions::UpdateClass => update_perm.has_update_class = Some(true),
            Permissions::DeleteClass => update_perm.has_delete_class = Some(true),
            Permissions::CreateObject => update_perm.has_create_object = Some(true),
            Permissions::ReadObject => update_perm.has_read_object = Some(true),
            Permissions::UpdateObject => update_perm.has_update_object = Some(true),
            Permissions::DeleteObject => update_perm.has_delete_object = Some(true),
            Permissions::CreateClassRelation => {
                update_perm.has_create_class_relation = Some(true);
            }
            Permissions::ReadClassRelation => update_perm.has_read_class_relation = Some(true),
            Permissions::UpdateClassRelation => {
                update_perm.has_update_class_relation = Some(true);
            }
            Permissions::DeleteClassRelation => {
                update_perm.has_delete_class_relation = Some(true);
            }
            Permissions::CreateObjectRelation => {
                update_perm.has_create_object_relation = Some(true);
            }
            Permissions::ReadObjectRelation => update_perm.has_read_object_relation = Some(true),
            Permissions::UpdateObjectRelation => {
                update_perm.has_update_object_relation = Some(true);
            }
            Permissions::DeleteObjectRelation => {
                update_perm.has_delete_object_relation = Some(true);
            }
            Permissions::ReadTemplate => update_perm.has_read_template = Some(true),
            Permissions::CreateTemplate => update_perm.has_create_template = Some(true),
            Permissions::UpdateTemplate => update_perm.has_update_template = Some(true),
            Permissions::DeleteTemplate => update_perm.has_delete_template = Some(true),
            Permissions::ReadRemoteTarget => update_perm.has_read_remote_target = Some(true),
            Permissions::CreateRemoteTarget => update_perm.has_create_remote_target = Some(true),
            Permissions::UpdateRemoteTarget => update_perm.has_update_remote_target = Some(true),
            Permissions::DeleteRemoteTarget => update_perm.has_delete_remote_target = Some(true),
            Permissions::ExecuteRemoteTarget => update_perm.has_execute_remote_target = Some(true),
            Permissions::ReadAudit => update_perm.has_read_audit = Some(true),
            Permissions::ManageEventSubscription => {
                update_perm.has_manage_event_subscription = Some(true);
            }
        }
    }

    update_perm
}

fn grant_changes_permission(
    current: &Permission,
    requested: &PermissionsList,
    replace_existing: bool,
) -> bool {
    let granted = current.granted();
    if replace_existing {
        Permissions::ALL
            .iter()
            .any(|permission| granted.contains(permission) != requested.contains(permission))
    } else {
        requested
            .iter()
            .any(|permission| !granted.contains(permission))
    }
}

fn revoke_changes_permission(current: &Permission, requested: &PermissionsList) -> bool {
    let granted = current.granted();
    requested
        .iter()
        .any(|permission| granted.contains(permission))
}

fn update_permission_for_revoke(permission_list: &PermissionsList) -> UpdatePermission {
    let mut update_perm = UpdatePermission::default();
    for permission in permission_list {
        match permission {
            Permissions::ReadCollection => update_perm.has_read_collection = Some(false),
            Permissions::UpdateCollection => update_perm.has_update_collection = Some(false),
            Permissions::DeleteCollection => update_perm.has_delete_collection = Some(false),
            Permissions::DelegateCollection => update_perm.has_delegate_collection = Some(false),
            Permissions::CreateClass => update_perm.has_create_class = Some(false),
            Permissions::ReadClass => update_perm.has_read_class = Some(false),
            Permissions::UpdateClass => update_perm.has_update_class = Some(false),
            Permissions::DeleteClass => update_perm.has_delete_class = Some(false),
            Permissions::CreateObject => update_perm.has_create_object = Some(false),
            Permissions::ReadObject => update_perm.has_read_object = Some(false),
            Permissions::UpdateObject => update_perm.has_update_object = Some(false),
            Permissions::DeleteObject => update_perm.has_delete_object = Some(false),
            Permissions::CreateClassRelation => {
                update_perm.has_create_class_relation = Some(false);
            }
            Permissions::ReadClassRelation => update_perm.has_read_class_relation = Some(false),
            Permissions::UpdateClassRelation => {
                update_perm.has_update_class_relation = Some(false);
            }
            Permissions::DeleteClassRelation => {
                update_perm.has_delete_class_relation = Some(false);
            }
            Permissions::CreateObjectRelation => {
                update_perm.has_create_object_relation = Some(false);
            }
            Permissions::ReadObjectRelation => update_perm.has_read_object_relation = Some(false),
            Permissions::UpdateObjectRelation => {
                update_perm.has_update_object_relation = Some(false);
            }
            Permissions::DeleteObjectRelation => {
                update_perm.has_delete_object_relation = Some(false);
            }
            Permissions::ReadTemplate => update_perm.has_read_template = Some(false),
            Permissions::CreateTemplate => update_perm.has_create_template = Some(false),
            Permissions::UpdateTemplate => update_perm.has_update_template = Some(false),
            Permissions::DeleteTemplate => update_perm.has_delete_template = Some(false),
            Permissions::ReadRemoteTarget => update_perm.has_read_remote_target = Some(false),
            Permissions::CreateRemoteTarget => update_perm.has_create_remote_target = Some(false),
            Permissions::UpdateRemoteTarget => update_perm.has_update_remote_target = Some(false),
            Permissions::DeleteRemoteTarget => update_perm.has_delete_remote_target = Some(false),
            Permissions::ExecuteRemoteTarget => {
                update_perm.has_execute_remote_target = Some(false);
            }
            Permissions::ReadAudit => update_perm.has_read_audit = Some(false),
            Permissions::ManageEventSubscription => {
                update_perm.has_manage_event_subscription = Some(false);
            }
        }
    }
    update_perm
}

pub(crate) fn new_permission_from_list(
    target_collection_id: i32,
    gid: i32,
    permission_list: &PermissionsList,
) -> NewPermission {
    NewPermission {
        collection_id: target_collection_id,
        group_id: gid,
        has_read_collection: permission_list.contains(&Permissions::ReadCollection),
        has_update_collection: permission_list.contains(&Permissions::UpdateCollection),
        has_delete_collection: permission_list.contains(&Permissions::DeleteCollection),
        has_delegate_collection: permission_list.contains(&Permissions::DelegateCollection),
        has_create_class: permission_list.contains(&Permissions::CreateClass),
        has_read_class: permission_list.contains(&Permissions::ReadClass),
        has_update_class: permission_list.contains(&Permissions::UpdateClass),
        has_delete_class: permission_list.contains(&Permissions::DeleteClass),
        has_create_object: permission_list.contains(&Permissions::CreateObject),
        has_read_object: permission_list.contains(&Permissions::ReadObject),
        has_update_object: permission_list.contains(&Permissions::UpdateObject),
        has_delete_object: permission_list.contains(&Permissions::DeleteObject),
        has_create_class_relation: permission_list.contains(&Permissions::CreateClassRelation),
        has_read_class_relation: permission_list.contains(&Permissions::ReadClassRelation),
        has_update_class_relation: permission_list.contains(&Permissions::UpdateClassRelation),
        has_delete_class_relation: permission_list.contains(&Permissions::DeleteClassRelation),
        has_create_object_relation: permission_list.contains(&Permissions::CreateObjectRelation),
        has_read_object_relation: permission_list.contains(&Permissions::ReadObjectRelation),
        has_update_object_relation: permission_list.contains(&Permissions::UpdateObjectRelation),
        has_delete_object_relation: permission_list.contains(&Permissions::DeleteObjectRelation),
        has_read_template: permission_list.contains(&Permissions::ReadTemplate),
        has_create_template: permission_list.contains(&Permissions::CreateTemplate),
        has_update_template: permission_list.contains(&Permissions::UpdateTemplate),
        has_delete_template: permission_list.contains(&Permissions::DeleteTemplate),
        has_read_remote_target: permission_list.contains(&Permissions::ReadRemoteTarget),
        has_create_remote_target: permission_list.contains(&Permissions::CreateRemoteTarget),
        has_update_remote_target: permission_list.contains(&Permissions::UpdateRemoteTarget),
        has_delete_remote_target: permission_list.contains(&Permissions::DeleteRemoteTarget),
        has_execute_remote_target: permission_list.contains(&Permissions::ExecuteRemoteTarget),
        has_read_audit: permission_list.contains(&Permissions::ReadAudit),
        has_manage_event_subscription: permission_list
            .contains(&Permissions::ManageEventSubscription),
    }
}

pub(crate) async fn apply_permission_grant_without_event(
    pool: &impl crate::storage::StorageContext,
    target_collection_id: i32,
    group_id_for_grant: i32,
    permission_list: PermissionsList,
    replace_existing: bool,
) -> Result<Permission, ApiError> {
    use crate::schema::permissions::dsl::*;

    with_transaction(pool, async |conn| -> Result<Permission, ApiError> {
        lock_permission_owner(conn, target_collection_id).await?;
        let existing_entry = permissions
            .filter(collection_id.eq(target_collection_id))
            .filter(group_id.eq(group_id_for_grant))
            .for_update()
            .first::<PermissionRow>(conn)
            .await
            .optional()?
            .map(Into::into);

        match existing_entry {
            Some(existing) => {
                if !grant_changes_permission(&existing, &permission_list, replace_existing) {
                    return Ok(existing);
                }
                let mut update_perm = if replace_existing {
                    UpdatePermission {
                        has_read_collection: Some(false),
                        has_update_collection: Some(false),
                        has_delete_collection: Some(false),
                        has_delegate_collection: Some(false),
                        has_create_class: Some(false),
                        has_read_class: Some(false),
                        has_update_class: Some(false),
                        has_delete_class: Some(false),
                        has_create_object: Some(false),
                        has_read_object: Some(false),
                        has_update_object: Some(false),
                        has_delete_object: Some(false),
                        has_create_class_relation: Some(false),
                        has_read_class_relation: Some(false),
                        has_update_class_relation: Some(false),
                        has_delete_class_relation: Some(false),
                        has_create_object_relation: Some(false),
                        has_read_object_relation: Some(false),
                        has_update_object_relation: Some(false),
                        has_delete_object_relation: Some(false),
                        has_read_template: Some(false),
                        has_create_template: Some(false),
                        has_update_template: Some(false),
                        has_delete_template: Some(false),
                        has_read_remote_target: Some(false),
                        has_create_remote_target: Some(false),
                        has_update_remote_target: Some(false),
                        has_delete_remote_target: Some(false),
                        has_execute_remote_target: Some(false),
                        has_read_audit: Some(false),
                        has_manage_event_subscription: Some(false),
                    }
                } else {
                    UpdatePermission::default()
                };

                for permission in permission_list.into_iter() {
                    match permission {
                        Permissions::ReadCollection => {
                            update_perm.has_read_collection = Some(true);
                        }
                        Permissions::UpdateCollection => {
                            update_perm.has_update_collection = Some(true);
                        }
                        Permissions::DeleteCollection => {
                            update_perm.has_delete_collection = Some(true);
                        }
                        Permissions::DelegateCollection => {
                            update_perm.has_delegate_collection = Some(true);
                        }
                        Permissions::CreateClass => {
                            update_perm.has_create_class = Some(true);
                        }
                        Permissions::ReadClass => {
                            update_perm.has_read_class = Some(true);
                        }
                        Permissions::UpdateClass => {
                            update_perm.has_update_class = Some(true);
                        }
                        Permissions::DeleteClass => {
                            update_perm.has_delete_class = Some(true);
                        }
                        Permissions::CreateObject => {
                            update_perm.has_create_object = Some(true);
                        }
                        Permissions::ReadObject => {
                            update_perm.has_read_object = Some(true);
                        }
                        Permissions::UpdateObject => {
                            update_perm.has_update_object = Some(true);
                        }
                        Permissions::DeleteObject => {
                            update_perm.has_delete_object = Some(true);
                        }
                        Permissions::CreateClassRelation => {
                            update_perm.has_create_class_relation = Some(true);
                        }
                        Permissions::ReadClassRelation => {
                            update_perm.has_read_class_relation = Some(true);
                        }
                        Permissions::UpdateClassRelation => {
                            update_perm.has_update_class_relation = Some(true);
                        }
                        Permissions::DeleteClassRelation => {
                            update_perm.has_delete_class_relation = Some(true);
                        }
                        Permissions::CreateObjectRelation => {
                            update_perm.has_create_object_relation = Some(true);
                        }
                        Permissions::ReadObjectRelation => {
                            update_perm.has_read_object_relation = Some(true);
                        }
                        Permissions::UpdateObjectRelation => {
                            update_perm.has_update_object_relation = Some(true);
                        }
                        Permissions::DeleteObjectRelation => {
                            update_perm.has_delete_object_relation = Some(true);
                        }
                        Permissions::ReadTemplate => {
                            update_perm.has_read_template = Some(true);
                        }
                        Permissions::CreateTemplate => {
                            update_perm.has_create_template = Some(true);
                        }
                        Permissions::UpdateTemplate => {
                            update_perm.has_update_template = Some(true);
                        }
                        Permissions::DeleteTemplate => {
                            update_perm.has_delete_template = Some(true);
                        }
                        Permissions::ReadRemoteTarget => {
                            update_perm.has_read_remote_target = Some(true);
                        }
                        Permissions::CreateRemoteTarget => {
                            update_perm.has_create_remote_target = Some(true);
                        }
                        Permissions::UpdateRemoteTarget => {
                            update_perm.has_update_remote_target = Some(true);
                        }
                        Permissions::DeleteRemoteTarget => {
                            update_perm.has_delete_remote_target = Some(true);
                        }
                        Permissions::ExecuteRemoteTarget => {
                            update_perm.has_execute_remote_target = Some(true);
                        }
                        Permissions::ReadAudit => {
                            update_perm.has_read_audit = Some(true);
                        }
                        Permissions::ManageEventSubscription => {
                            update_perm.has_manage_event_subscription = Some(true);
                        }
                    }
                }

                Ok(diesel::update(permissions)
                    .filter(collection_id.eq(target_collection_id))
                    .filter(group_id.eq(group_id_for_grant))
                    .set(&update_perm)
                    .get_result::<PermissionRow>(conn)
                    .await?
                    .into())
            }
            None => {
                let new_entry = NewPermission {
                    collection_id: target_collection_id,
                    group_id: group_id_for_grant,
                    has_read_collection: permission_list.contains(&Permissions::ReadCollection),
                    has_update_collection: permission_list.contains(&Permissions::UpdateCollection),
                    has_delete_collection: permission_list.contains(&Permissions::DeleteCollection),
                    has_delegate_collection: permission_list
                        .contains(&Permissions::DelegateCollection),
                    has_create_class: permission_list.contains(&Permissions::CreateClass),
                    has_read_class: permission_list.contains(&Permissions::ReadClass),
                    has_update_class: permission_list.contains(&Permissions::UpdateClass),
                    has_delete_class: permission_list.contains(&Permissions::DeleteClass),
                    has_create_object: permission_list.contains(&Permissions::CreateObject),
                    has_read_object: permission_list.contains(&Permissions::ReadObject),
                    has_update_object: permission_list.contains(&Permissions::UpdateObject),
                    has_delete_object: permission_list.contains(&Permissions::DeleteObject),
                    has_create_class_relation: permission_list
                        .contains(&Permissions::CreateClassRelation),
                    has_read_class_relation: permission_list
                        .contains(&Permissions::ReadClassRelation),
                    has_update_class_relation: permission_list
                        .contains(&Permissions::UpdateClassRelation),
                    has_delete_class_relation: permission_list
                        .contains(&Permissions::DeleteClassRelation),
                    has_create_object_relation: permission_list
                        .contains(&Permissions::CreateObjectRelation),
                    has_read_object_relation: permission_list
                        .contains(&Permissions::ReadObjectRelation),
                    has_update_object_relation: permission_list
                        .contains(&Permissions::UpdateObjectRelation),
                    has_delete_object_relation: permission_list
                        .contains(&Permissions::DeleteObjectRelation),
                    has_read_template: permission_list.contains(&Permissions::ReadTemplate),
                    has_create_template: permission_list.contains(&Permissions::CreateTemplate),
                    has_update_template: permission_list.contains(&Permissions::UpdateTemplate),
                    has_delete_template: permission_list.contains(&Permissions::DeleteTemplate),
                    has_read_remote_target: permission_list
                        .contains(&Permissions::ReadRemoteTarget),
                    has_create_remote_target: permission_list
                        .contains(&Permissions::CreateRemoteTarget),
                    has_update_remote_target: permission_list
                        .contains(&Permissions::UpdateRemoteTarget),
                    has_delete_remote_target: permission_list
                        .contains(&Permissions::DeleteRemoteTarget),
                    has_execute_remote_target: permission_list
                        .contains(&Permissions::ExecuteRemoteTarget),
                    has_read_audit: permission_list.contains(&Permissions::ReadAudit),
                    has_manage_event_subscription: permission_list
                        .contains(&Permissions::ManageEventSubscription),
                };

                Ok(diesel::insert_into(permissions)
                    .values(&new_entry)
                    .get_result::<PermissionRow>(conn)
                    .await?
                    .into())
            }
        }
    })
    .await
}

pub(crate) async fn apply_permission_grant(
    pool: &impl crate::storage::StorageContext,
    target_collection_id: i32,
    group_id_for_grant: i32,
    permission_list: PermissionsList,
    replace_existing: bool,
    context: Option<&EventContext>,
) -> Result<Permission, ApiError> {
    let Some(context) = context else {
        return apply_permission_grant_without_event(
            pool,
            target_collection_id,
            group_id_for_grant,
            permission_list,
            replace_existing,
        )
        .await;
    };

    use crate::schema::permissions::dsl::*;

    let requested = permission_list.iter().copied().collect::<Vec<_>>();

    with_transaction(pool, async |conn| -> Result<Permission, ApiError> {
        let before_owner_revision = lock_permission_owner(conn, target_collection_id).await?;
        let before: Option<Permission> = permissions
            .filter(collection_id.eq(target_collection_id))
            .filter(group_id.eq(group_id_for_grant))
            .for_update()
            .first::<PermissionRow>(conn)
            .await
            .optional()?
            .map(Into::into);

        if let Some(current) = before
            && !grant_changes_permission(&current, &permission_list, replace_existing)
        {
            return Ok(current);
        }

        let after = match before {
            Some(_) => {
                let update_perm = update_permission_for_grant(&permission_list, replace_existing);
                diesel::update(permissions)
                    .filter(collection_id.eq(target_collection_id))
                    .filter(group_id.eq(group_id_for_grant))
                    .set(&update_perm)
                    .get_result::<PermissionRow>(conn)
                    .await?
                    .into()
            }
            None => {
                let new_entry = new_permission_from_list(
                    target_collection_id,
                    group_id_for_grant,
                    &permission_list,
                );
                diesel::insert_into(permissions)
                    .values(&new_entry)
                    .get_result::<PermissionRow>(conn)
                    .await?
                    .into()
            }
        };
        let after_owner_revision = permission_owner_revision(conn, target_collection_id).await?;

        let event = permission_event(
            &after,
            Action::Granted,
            context,
            format!(
                "Permissions granted to group {} on collection {}",
                group_id_for_grant, target_collection_id
            ),
            &requested,
            Some(replace_existing),
        )?
        .with_after(permission_snapshot(&after, after_owner_revision));

        let event = event.with_before(match before {
            Some(before) => permission_snapshot(&before, before_owner_revision),
            None => empty_permission_snapshot(
                target_collection_id,
                group_id_for_grant,
                before_owner_revision,
            ),
        });
        emit_event(conn, &event).await?;
        Ok(after)
    })
    .await
}

pub(crate) async fn revoke_permission_grant_without_event(
    pool: &impl crate::storage::StorageContext,
    target_collection_id: i32,
    group_id_for_revoke: i32,
    permission_list: PermissionsList,
) -> Result<Permission, ApiError> {
    use crate::schema::permissions::dsl::*;

    with_transaction(pool, async |conn| -> Result<Permission, ApiError> {
        lock_permission_owner(conn, target_collection_id).await?;
        let before = permissions
            .filter(collection_id.eq(target_collection_id))
            .filter(group_id.eq(group_id_for_revoke))
            .for_update()
            .first::<PermissionRow>(conn)
            .await?
            .into();

        if !revoke_changes_permission(&before, &permission_list) {
            return Ok(before);
        }

        let mut update_perm = UpdatePermission::default();
        for permission in permission_list.into_iter() {
            match permission {
                Permissions::ReadCollection => {
                    update_perm.has_read_collection = Some(false);
                }
                Permissions::UpdateCollection => {
                    update_perm.has_update_collection = Some(false);
                }
                Permissions::DeleteCollection => {
                    update_perm.has_delete_collection = Some(false);
                }
                Permissions::DelegateCollection => {
                    update_perm.has_delegate_collection = Some(false);
                }
                Permissions::CreateClass => {
                    update_perm.has_create_class = Some(false);
                }
                Permissions::ReadClass => {
                    update_perm.has_read_class = Some(false);
                }
                Permissions::UpdateClass => {
                    update_perm.has_update_class = Some(false);
                }
                Permissions::DeleteClass => {
                    update_perm.has_delete_class = Some(false);
                }
                Permissions::CreateObject => {
                    update_perm.has_create_object = Some(false);
                }
                Permissions::ReadObject => {
                    update_perm.has_read_object = Some(false);
                }
                Permissions::UpdateObject => {
                    update_perm.has_update_object = Some(false);
                }
                Permissions::DeleteObject => {
                    update_perm.has_delete_object = Some(false);
                }
                Permissions::CreateClassRelation => {
                    update_perm.has_create_class_relation = Some(false);
                }
                Permissions::ReadClassRelation => {
                    update_perm.has_read_class_relation = Some(false);
                }
                Permissions::UpdateClassRelation => {
                    update_perm.has_update_class_relation = Some(false);
                }
                Permissions::DeleteClassRelation => {
                    update_perm.has_delete_class_relation = Some(false);
                }
                Permissions::CreateObjectRelation => {
                    update_perm.has_create_object_relation = Some(false);
                }
                Permissions::ReadObjectRelation => {
                    update_perm.has_read_object_relation = Some(false);
                }
                Permissions::UpdateObjectRelation => {
                    update_perm.has_update_object_relation = Some(false);
                }
                Permissions::DeleteObjectRelation => {
                    update_perm.has_delete_object_relation = Some(false);
                }
                Permissions::ReadTemplate => {
                    update_perm.has_read_template = Some(false);
                }
                Permissions::CreateTemplate => {
                    update_perm.has_create_template = Some(false);
                }
                Permissions::UpdateTemplate => {
                    update_perm.has_update_template = Some(false);
                }
                Permissions::DeleteTemplate => {
                    update_perm.has_delete_template = Some(false);
                }
                Permissions::ReadRemoteTarget => {
                    update_perm.has_read_remote_target = Some(false);
                }
                Permissions::CreateRemoteTarget => {
                    update_perm.has_create_remote_target = Some(false);
                }
                Permissions::UpdateRemoteTarget => {
                    update_perm.has_update_remote_target = Some(false);
                }
                Permissions::DeleteRemoteTarget => {
                    update_perm.has_delete_remote_target = Some(false);
                }
                Permissions::ExecuteRemoteTarget => {
                    update_perm.has_execute_remote_target = Some(false);
                }
                Permissions::ReadAudit => {
                    update_perm.has_read_audit = Some(false);
                }
                Permissions::ManageEventSubscription => {
                    update_perm.has_manage_event_subscription = Some(false);
                }
            }
        }

        Ok(diesel::update(permissions)
            .filter(collection_id.eq(target_collection_id))
            .filter(group_id.eq(group_id_for_revoke))
            .set(&update_perm)
            .get_result::<PermissionRow>(conn)
            .await?
            .into())
    })
    .await
}

pub(crate) async fn revoke_permission_grant(
    pool: &impl crate::storage::StorageContext,
    target_collection_id: i32,
    group_id_for_revoke: i32,
    permission_list: PermissionsList,
    context: Option<&EventContext>,
) -> Result<Permission, ApiError> {
    let Some(context) = context else {
        return revoke_permission_grant_without_event(
            pool,
            target_collection_id,
            group_id_for_revoke,
            permission_list,
        )
        .await;
    };

    use crate::schema::permissions::dsl::*;

    let requested = permission_list.iter().copied().collect::<Vec<_>>();

    with_transaction(pool, async |conn| -> Result<Permission, ApiError> {
        let before_owner_revision = lock_permission_owner(conn, target_collection_id).await?;
        let before = permissions
            .filter(collection_id.eq(target_collection_id))
            .filter(group_id.eq(group_id_for_revoke))
            .for_update()
            .first::<PermissionRow>(conn)
            .await?
            .into();

        if !revoke_changes_permission(&before, &permission_list) {
            return Ok(before);
        }

        let update_perm = update_permission_for_revoke(&permission_list);
        let after = diesel::update(permissions)
            .filter(collection_id.eq(target_collection_id))
            .filter(group_id.eq(group_id_for_revoke))
            .set(&update_perm)
            .get_result::<PermissionRow>(conn)
            .await?
            .into();
        let after_owner_revision = permission_owner_revision(conn, target_collection_id).await?;

        let event = permission_event(
            &after,
            Action::Revoked,
            context,
            format!(
                "Permissions revoked from group {} on collection {}",
                group_id_for_revoke, target_collection_id
            ),
            &requested,
            None,
        )?
        .with_before(permission_snapshot(&before, before_owner_revision))
        .with_after(permission_snapshot(&after, after_owner_revision));
        emit_event(conn, &event).await?;
        Ok(after)
    })
    .await
}

pub(crate) async fn revoke_all_permission_grants_without_event(
    pool: &impl crate::storage::StorageContext,
    collection_id_for_revoke: i32,
    group_id_for_revoke: i32,
) -> Result<(), ApiError> {
    use crate::schema::permissions::dsl::*;
    with_transaction(pool, async |conn| -> Result<_, ApiError> {
        lock_permission_owner(conn, collection_id_for_revoke).await?;
        diesel::delete(permissions)
            .filter(collection_id.eq(collection_id_for_revoke))
            .filter(group_id.eq(group_id_for_revoke))
            .execute(conn)
            .await
            .map_err(ApiError::from)
    })
    .await?;

    Ok(())
}

pub(crate) async fn revoke_all_permission_grants(
    pool: &impl crate::storage::StorageContext,
    collection_id_for_revoke: i32,
    group_id_for_revoke: i32,
    context: Option<&EventContext>,
) -> Result<(), ApiError> {
    let Some(context) = context else {
        return revoke_all_permission_grants_without_event(
            pool,
            collection_id_for_revoke,
            group_id_for_revoke,
        )
        .await;
    };

    use crate::schema::permissions::dsl::*;
    with_transaction(pool, async |conn| -> Result<(), ApiError> {
        let before_owner_revision = lock_permission_owner(conn, collection_id_for_revoke).await?;
        let before: Option<Permission> = permissions
            .filter(collection_id.eq(collection_id_for_revoke))
            .filter(group_id.eq(group_id_for_revoke))
            .for_update()
            .first::<PermissionRow>(conn)
            .await
            .optional()?
            .map(Into::into);

        diesel::delete(permissions)
            .filter(collection_id.eq(collection_id_for_revoke))
            .filter(group_id.eq(group_id_for_revoke))
            .execute(conn)
            .await?;

        if let Some(before) = before {
            let after_owner_revision =
                permission_owner_revision(conn, collection_id_for_revoke).await?;
            let requested = before.granted();
            let event = permission_event(
                &before,
                Action::Revoked,
                context,
                format!(
                    "All permissions revoked from group {} on collection {}",
                    group_id_for_revoke, collection_id_for_revoke
                ),
                &requested,
                None,
            )?
            .with_before(permission_snapshot(&before, before_owner_revision))
            .with_after(empty_permission_snapshot(
                collection_id_for_revoke,
                group_id_for_revoke,
                after_owner_revision,
            ));
            emit_event(conn, &event).await?;
        }

        Ok(())
    })
    .await
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
