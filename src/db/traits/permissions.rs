use diesel::prelude::*;
use serde::Serialize;

use crate::db::{DbPool, with_connection, with_transaction};
use crate::errors::ApiError;
use crate::events::{Action, EntityType, EventContext, NewEvent, emit_event};
use crate::models::{
    NewPermission, Permission, PermissionFilter, Permissions, PermissionsList, UpdatePermission,
};
use crate::traits::NamespaceAccessors;

use super::authz::{AuthzSubject, scope_allows};

fn permission_names(permissions: &[Permissions]) -> Vec<String> {
    permissions.iter().map(ToString::to_string).collect()
}

fn granted_permission_names(permission: &Permission) -> Vec<String> {
    permission_names(&permission.granted())
}

fn permission_snapshot(permission: &Permission) -> serde_json::Value {
    serde_json::json!({
        "id": permission.id,
        "namespace_id": permission.namespace_id,
        "group_id": permission.group_id,
        "granted_permissions": granted_permission_names(permission),
        "created_at": permission.created_at,
        "updated_at": permission.updated_at,
    })
}

fn permission_metadata(
    permission: &Permission,
    requested_permissions: &[Permissions],
    replace_existing: Option<bool>,
) -> serde_json::Value {
    let mut metadata = serde_json::json!({
        "namespace_id": permission.namespace_id,
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
    .with_namespace_id(permission.namespace_id)
    .with_metadata(permission_metadata(
        permission,
        requested_permissions,
        replace_existing,
    )))
}

fn update_permission_for_grant(
    permission_list: &PermissionsList<Permissions>,
    replace_existing: bool,
) -> UpdatePermission {
    let mut update_perm = if replace_existing {
        UpdatePermission {
            has_read_namespace: Some(false),
            has_update_namespace: Some(false),
            has_delete_namespace: Some(false),
            has_delegate_namespace: Some(false),
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
            Permissions::ReadCollection => update_perm.has_read_namespace = Some(true),
            Permissions::UpdateCollection => update_perm.has_update_namespace = Some(true),
            Permissions::DeleteCollection => update_perm.has_delete_namespace = Some(true),
            Permissions::DelegateCollection => update_perm.has_delegate_namespace = Some(true),
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

fn update_permission_for_revoke(
    permission_list: &PermissionsList<Permissions>,
) -> UpdatePermission {
    let mut update_perm = UpdatePermission::default();
    for permission in permission_list {
        match permission {
            Permissions::ReadCollection => update_perm.has_read_namespace = Some(false),
            Permissions::UpdateCollection => update_perm.has_update_namespace = Some(false),
            Permissions::DeleteCollection => update_perm.has_delete_namespace = Some(false),
            Permissions::DelegateCollection => update_perm.has_delegate_namespace = Some(false),
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

fn new_permission_from_list(
    nid: i32,
    gid: i32,
    permission_list: &PermissionsList<Permissions>,
) -> NewPermission {
    NewPermission {
        namespace_id: nid,
        group_id: gid,
        has_read_namespace: permission_list.contains(&Permissions::ReadCollection),
        has_update_namespace: permission_list.contains(&Permissions::UpdateCollection),
        has_delete_namespace: permission_list.contains(&Permissions::DeleteCollection),
        has_delegate_namespace: permission_list.contains(&Permissions::DelegateCollection),
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

pub trait PermissionControllerBackend: Serialize + NamespaceAccessors {
    async fn user_can_all_from_backend<S: AuthzSubject>(
        &self,
        pool: &DbPool,
        subject: S,
        permissions_requested: Vec<Permissions>,
        scopes: Option<&[Permissions]>,
    ) -> Result<bool, ApiError> {
        let lookup_table = crate::schema::permissions::dsl::permissions;
        let group_id_field = crate::schema::permissions::dsl::group_id;
        let namespace_id_field = crate::schema::permissions::dsl::namespace_id;

        // Fail-closed token-scope pre-filter, applied BEFORE the admin bypass so
        // a scoped admin token can never exceed its scopes.
        if !scope_allows(scopes, &permissions_requested) {
            return Ok(false);
        }

        if subject.is_admin(pool).await? {
            return Ok(true);
        }

        let group_id_subquery = subject.group_ids_subquery();
        let mut base_query = lookup_table
            .into_boxed()
            .filter(namespace_id_field.eq(self.namespace_id(pool).await?.id()))
            .filter(group_id_field.eq_any(group_id_subquery));

        for permission in permissions_requested {
            base_query = permission.create_boxed_filter(base_query, true);
        }

        let result: Option<Permission> =
            with_connection(pool, |conn| base_query.first::<Permission>(conn).optional())?;

        Ok(result.is_some())
    }

    async fn apply_permissions_from_backend(
        &self,
        pool: &DbPool,
        group_id_for_grant: i32,
        permission_list: PermissionsList<Permissions>,
        replace_existing: bool,
    ) -> Result<Permission, ApiError> {
        use crate::schema::permissions::dsl::*;

        let nid = self.namespace_id(pool).await?.id();

        with_transaction(pool, |conn| -> Result<Permission, ApiError> {
            let existing_entry = permissions
                .filter(namespace_id.eq(nid))
                .filter(group_id.eq(group_id_for_grant))
                .first::<Permission>(conn)
                .optional()?;

            match existing_entry {
                Some(_) => {
                    let mut update_perm = if replace_existing {
                        UpdatePermission {
                            has_read_namespace: Some(false),
                            has_update_namespace: Some(false),
                            has_delete_namespace: Some(false),
                            has_delegate_namespace: Some(false),
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
                                update_perm.has_read_namespace = Some(true);
                            }
                            Permissions::UpdateCollection => {
                                update_perm.has_update_namespace = Some(true);
                            }
                            Permissions::DeleteCollection => {
                                update_perm.has_delete_namespace = Some(true);
                            }
                            Permissions::DelegateCollection => {
                                update_perm.has_delegate_namespace = Some(true);
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
                        .filter(namespace_id.eq(nid))
                        .filter(group_id.eq(group_id_for_grant))
                        .set(&update_perm)
                        .get_result(conn)?)
                }
                None => {
                    let new_entry = NewPermission {
                        namespace_id: nid,
                        group_id: group_id_for_grant,
                        has_read_namespace: permission_list.contains(&Permissions::ReadCollection),
                        has_update_namespace: permission_list
                            .contains(&Permissions::UpdateCollection),
                        has_delete_namespace: permission_list
                            .contains(&Permissions::DeleteCollection),
                        has_delegate_namespace: permission_list
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
                        .get_result(conn)?)
                }
            }
        })
    }

    async fn apply_permissions_from_backend_with_context(
        &self,
        pool: &DbPool,
        group_id_for_grant: i32,
        permission_list: PermissionsList<Permissions>,
        replace_existing: bool,
        context: Option<&EventContext>,
    ) -> Result<Permission, ApiError> {
        let Some(context) = context else {
            return self
                .apply_permissions_from_backend(
                    pool,
                    group_id_for_grant,
                    permission_list,
                    replace_existing,
                )
                .await;
        };

        use crate::schema::permissions::dsl::*;

        let nid = self.namespace_id(pool).await?.id();
        let requested = permission_list.iter().copied().collect::<Vec<_>>();

        with_transaction(pool, |conn| -> Result<Permission, ApiError> {
            let before = permissions
                .filter(namespace_id.eq(nid))
                .filter(group_id.eq(group_id_for_grant))
                .first::<Permission>(conn)
                .optional()?;

            let after = match before {
                Some(_) => {
                    let update_perm =
                        update_permission_for_grant(&permission_list, replace_existing);
                    diesel::update(permissions)
                        .filter(namespace_id.eq(nid))
                        .filter(group_id.eq(group_id_for_grant))
                        .set(&update_perm)
                        .get_result::<Permission>(conn)?
                }
                None => {
                    let new_entry =
                        new_permission_from_list(nid, group_id_for_grant, &permission_list);
                    diesel::insert_into(permissions)
                        .values(&new_entry)
                        .get_result::<Permission>(conn)?
                }
            };

            let event = permission_event(
                &after,
                Action::Granted,
                context,
                format!(
                    "Permissions granted to group {} on namespace {}",
                    group_id_for_grant, nid
                ),
                &requested,
                Some(replace_existing),
            )?
            .with_after(permission_snapshot(&after));

            let event = if let Some(before) = before {
                event.with_before(permission_snapshot(&before))
            } else {
                event
            };
            emit_event(conn, &event)?;
            Ok(after)
        })
    }

    async fn revoke_permissions_from_backend(
        &self,
        pool: &DbPool,
        group_id_for_revoke: i32,
        permission_list: PermissionsList<Permissions>,
    ) -> Result<Permission, ApiError> {
        use crate::schema::permissions::dsl::*;

        let nid = self.namespace_id(pool).await?.id();

        with_transaction(pool, |conn| -> Result<Permission, ApiError> {
            permissions
                .filter(namespace_id.eq(nid))
                .filter(group_id.eq(group_id_for_revoke))
                .first::<Permission>(conn)?;

            let mut update_perm = UpdatePermission::default();
            for permission in permission_list.into_iter() {
                match permission {
                    Permissions::ReadCollection => {
                        update_perm.has_read_namespace = Some(false);
                    }
                    Permissions::UpdateCollection => {
                        update_perm.has_update_namespace = Some(false);
                    }
                    Permissions::DeleteCollection => {
                        update_perm.has_delete_namespace = Some(false);
                    }
                    Permissions::DelegateCollection => {
                        update_perm.has_delegate_namespace = Some(false);
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
                .filter(namespace_id.eq(nid))
                .filter(group_id.eq(group_id_for_revoke))
                .set(&update_perm)
                .get_result(conn)?)
        })
    }

    async fn revoke_permissions_from_backend_with_context(
        &self,
        pool: &DbPool,
        group_id_for_revoke: i32,
        permission_list: PermissionsList<Permissions>,
        context: Option<&EventContext>,
    ) -> Result<Permission, ApiError> {
        let Some(context) = context else {
            return self
                .revoke_permissions_from_backend(pool, group_id_for_revoke, permission_list)
                .await;
        };

        use crate::schema::permissions::dsl::*;

        let nid = self.namespace_id(pool).await?.id();
        let requested = permission_list.iter().copied().collect::<Vec<_>>();

        with_transaction(pool, |conn| -> Result<Permission, ApiError> {
            let before = permissions
                .filter(namespace_id.eq(nid))
                .filter(group_id.eq(group_id_for_revoke))
                .first::<Permission>(conn)?;

            let update_perm = update_permission_for_revoke(&permission_list);
            let after = diesel::update(permissions)
                .filter(namespace_id.eq(nid))
                .filter(group_id.eq(group_id_for_revoke))
                .set(&update_perm)
                .get_result::<Permission>(conn)?;

            let event = permission_event(
                &after,
                Action::Revoked,
                context,
                format!(
                    "Permissions revoked from group {} on namespace {}",
                    group_id_for_revoke, nid
                ),
                &requested,
                None,
            )?
            .with_before(permission_snapshot(&before))
            .with_after(permission_snapshot(&after));
            emit_event(conn, &event)?;
            Ok(after)
        })
    }

    async fn revoke_all_from_backend(
        &self,
        pool: &DbPool,
        group_id_for_revoke: i32,
    ) -> Result<(), ApiError> {
        use crate::schema::permissions::dsl::*;

        let namespace_id_for_revoke = self.namespace_id(pool).await?.id();
        with_connection(pool, |conn| {
            diesel::delete(permissions)
                .filter(namespace_id.eq(namespace_id_for_revoke))
                .filter(group_id.eq(group_id_for_revoke))
                .execute(conn)
        })?;

        Ok(())
    }

    async fn revoke_all_from_backend_with_context(
        &self,
        pool: &DbPool,
        group_id_for_revoke: i32,
        context: Option<&EventContext>,
    ) -> Result<(), ApiError> {
        let Some(context) = context else {
            return self
                .revoke_all_from_backend(pool, group_id_for_revoke)
                .await;
        };

        use crate::schema::permissions::dsl::*;

        let namespace_id_for_revoke = self.namespace_id(pool).await?.id();
        with_transaction(pool, |conn| -> Result<(), ApiError> {
            let before = permissions
                .filter(namespace_id.eq(namespace_id_for_revoke))
                .filter(group_id.eq(group_id_for_revoke))
                .first::<Permission>(conn)
                .optional()?;

            diesel::delete(permissions)
                .filter(namespace_id.eq(namespace_id_for_revoke))
                .filter(group_id.eq(group_id_for_revoke))
                .execute(conn)?;

            if let Some(before) = before {
                let requested = before.granted();
                let event = permission_event(
                    &before,
                    Action::Revoked,
                    context,
                    format!(
                        "All permissions revoked from group {} on namespace {}",
                        group_id_for_revoke, namespace_id_for_revoke
                    ),
                    &requested,
                    None,
                )?
                .with_before(permission_snapshot(&before));
                emit_event(conn, &event)?;
            }

            Ok(())
        })
    }
}

impl<T: ?Sized> PermissionControllerBackend for T where T: Serialize + NamespaceAccessors {}
