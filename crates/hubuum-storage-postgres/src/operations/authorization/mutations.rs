use diesel::prelude::{ExpressionMethods, OptionalExtension, QueryDsl};
use diesel::{AsChangeset, Insertable};
use diesel_async::RunQueryDsl;
use hubuum_events_core::{Action, EntityType, EventContext, NewEvent};
use hubuum_storage_core::{
    AuthorizationGrant, AuthorizationGrantDelete, AuthorizationGrantMutation,
    AuthorizationPermission,
};
use serde_json::json;

use crate::operations::event_record::append_event;
use crate::revision::RevisionOwner;
use crate::runtime::assert_locked_revision_precondition;
use crate::{PostgresConnection, PostgresRevision, PostgresRuntime, PostgresStorageError};

use super::rows::PermissionRow;

#[derive(Insertable)]
#[diesel(table_name = crate::schema::permissions)]
pub(crate) struct NewPermission {
    collection_id: i32,
    group_id: i32,
    has_read_collection: bool,
    has_update_collection: bool,
    has_delete_collection: bool,
    has_delegate_collection: bool,
    has_create_class: bool,
    has_read_class: bool,
    has_update_class: bool,
    has_delete_class: bool,
    has_create_object: bool,
    has_read_object: bool,
    has_update_object: bool,
    has_delete_object: bool,
    has_create_class_relation: bool,
    has_read_class_relation: bool,
    has_update_class_relation: bool,
    has_delete_class_relation: bool,
    has_create_object_relation: bool,
    has_read_object_relation: bool,
    has_update_object_relation: bool,
    has_delete_object_relation: bool,
    has_read_template: bool,
    has_create_template: bool,
    has_update_template: bool,
    has_delete_template: bool,
    has_read_remote_target: bool,
    has_create_remote_target: bool,
    has_update_remote_target: bool,
    has_delete_remote_target: bool,
    has_execute_remote_target: bool,
    has_read_audit: bool,
    has_manage_event_subscription: bool,
}

impl NewPermission {
    pub(crate) fn new(
        collection_id: i32,
        group_id: i32,
        permissions: &[AuthorizationPermission],
    ) -> Self {
        let has = |permission| permissions.contains(&permission);
        Self {
            collection_id,
            group_id,
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
            has_read_audit: has(AuthorizationPermission::ReadAudit),
            has_manage_event_subscription: has(AuthorizationPermission::ManageEventSubscription),
        }
    }
}

#[derive(AsChangeset, Default)]
#[diesel(table_name = crate::schema::permissions)]
pub(crate) struct UpdatePermission {
    has_read_collection: Option<bool>,
    has_update_collection: Option<bool>,
    has_delete_collection: Option<bool>,
    has_delegate_collection: Option<bool>,
    has_create_class: Option<bool>,
    has_read_class: Option<bool>,
    has_update_class: Option<bool>,
    has_delete_class: Option<bool>,
    has_create_object: Option<bool>,
    has_read_object: Option<bool>,
    has_update_object: Option<bool>,
    has_delete_object: Option<bool>,
    has_create_class_relation: Option<bool>,
    has_read_class_relation: Option<bool>,
    has_update_class_relation: Option<bool>,
    has_delete_class_relation: Option<bool>,
    has_create_object_relation: Option<bool>,
    has_read_object_relation: Option<bool>,
    has_update_object_relation: Option<bool>,
    has_delete_object_relation: Option<bool>,
    has_read_template: Option<bool>,
    has_create_template: Option<bool>,
    has_update_template: Option<bool>,
    has_delete_template: Option<bool>,
    has_read_remote_target: Option<bool>,
    has_create_remote_target: Option<bool>,
    has_update_remote_target: Option<bool>,
    has_delete_remote_target: Option<bool>,
    has_execute_remote_target: Option<bool>,
    has_read_audit: Option<bool>,
    has_manage_event_subscription: Option<bool>,
}

impl UpdatePermission {
    pub(crate) fn grant(permissions: &[AuthorizationPermission], replace_existing: bool) -> Self {
        let value = |permission| {
            let requested = permissions.contains(&permission);
            if replace_existing {
                Some(requested)
            } else {
                requested.then_some(true)
            }
        };
        Self::from_values(value)
    }

    fn revoke(permissions: &[AuthorizationPermission]) -> Self {
        Self::from_values(|permission| permissions.contains(&permission).then_some(false))
    }

    fn from_values(mut value: impl FnMut(AuthorizationPermission) -> Option<bool>) -> Self {
        Self {
            has_read_collection: value(AuthorizationPermission::ReadCollection),
            has_update_collection: value(AuthorizationPermission::UpdateCollection),
            has_delete_collection: value(AuthorizationPermission::DeleteCollection),
            has_delegate_collection: value(AuthorizationPermission::DelegateCollection),
            has_create_class: value(AuthorizationPermission::CreateClass),
            has_read_class: value(AuthorizationPermission::ReadClass),
            has_update_class: value(AuthorizationPermission::UpdateClass),
            has_delete_class: value(AuthorizationPermission::DeleteClass),
            has_create_object: value(AuthorizationPermission::CreateObject),
            has_read_object: value(AuthorizationPermission::ReadObject),
            has_update_object: value(AuthorizationPermission::UpdateObject),
            has_delete_object: value(AuthorizationPermission::DeleteObject),
            has_create_class_relation: value(AuthorizationPermission::CreateClassRelation),
            has_read_class_relation: value(AuthorizationPermission::ReadClassRelation),
            has_update_class_relation: value(AuthorizationPermission::UpdateClassRelation),
            has_delete_class_relation: value(AuthorizationPermission::DeleteClassRelation),
            has_create_object_relation: value(AuthorizationPermission::CreateObjectRelation),
            has_read_object_relation: value(AuthorizationPermission::ReadObjectRelation),
            has_update_object_relation: value(AuthorizationPermission::UpdateObjectRelation),
            has_delete_object_relation: value(AuthorizationPermission::DeleteObjectRelation),
            has_read_template: value(AuthorizationPermission::ReadTemplate),
            has_create_template: value(AuthorizationPermission::CreateTemplate),
            has_update_template: value(AuthorizationPermission::UpdateTemplate),
            has_delete_template: value(AuthorizationPermission::DeleteTemplate),
            has_read_remote_target: value(AuthorizationPermission::ReadRemoteTarget),
            has_create_remote_target: value(AuthorizationPermission::CreateRemoteTarget),
            has_update_remote_target: value(AuthorizationPermission::UpdateRemoteTarget),
            has_delete_remote_target: value(AuthorizationPermission::DeleteRemoteTarget),
            has_execute_remote_target: value(AuthorizationPermission::ExecuteRemoteTarget),
            has_read_audit: value(AuthorizationPermission::ReadAudit),
            has_manage_event_subscription: value(AuthorizationPermission::ManageEventSubscription),
        }
    }
}

pub(crate) async fn insert_full_collection_grant(
    connection: &mut PostgresConnection,
    collection_id: i32,
    group_id: i32,
) -> Result<(), PostgresStorageError> {
    diesel::insert_into(crate::schema::permissions::table)
        .values(NewPermission::new(
            collection_id,
            group_id,
            &AuthorizationPermission::ALL,
        ))
        .execute(connection)
        .await?;
    Ok(())
}

pub async fn apply_local_collection_grant(
    runtime: &PostgresRuntime,
    mutation: AuthorizationGrantMutation,
) -> Result<AuthorizationGrant, PostgresStorageError> {
    let key = mutation.key();
    let requested = mutation.permissions().to_vec();
    let replace_existing = mutation.replace_existing();
    let event_context = mutation.event_context_value().cloned();
    runtime
        .with_transaction(async move |connection| {
            let before_revision = lock_permission_owner(connection, key.collection_id()).await?;
            let before = lock_grant(connection, key.collection_id(), key.group_id()).await?;
            if let Some(current) = before
                && !grant_changes(&current, &requested, replace_existing)
            {
                return Ok(current.into_storage());
            }

            let after = match before {
                Some(_) => {
                    diesel::update(
                        crate::schema::permissions::table
                            .filter(
                                crate::schema::permissions::collection_id.eq(key.collection_id()),
                            )
                            .filter(crate::schema::permissions::group_id.eq(key.group_id())),
                    )
                    .set(UpdatePermission::grant(&requested, replace_existing))
                    .get_result::<PermissionRow>(connection)
                    .await?
                }
                None => {
                    diesel::insert_into(crate::schema::permissions::table)
                        .values(NewPermission::new(
                            key.collection_id(),
                            key.group_id(),
                            &requested,
                        ))
                        .get_result::<PermissionRow>(connection)
                        .await?
                }
            };
            if let Some(context) = event_context.as_ref() {
                let after_revision =
                    permission_owner_revision(connection, key.collection_id()).await?;
                append_permission_event(
                    connection,
                    PermissionEvent {
                        action: Action::Granted,
                        context,
                        before: before.as_ref(),
                        after: &after,
                        before_revision,
                        after_revision,
                        requested: &requested,
                        replace_existing: Some(replace_existing),
                        removes_grant: false,
                    },
                )
                .await?;
            }
            Ok::<_, PostgresStorageError>(after.into_storage())
        })
        .await
}

pub async fn revoke_local_collection_grant(
    runtime: &PostgresRuntime,
    mutation: AuthorizationGrantMutation,
) -> Result<AuthorizationGrant, PostgresStorageError> {
    let key = mutation.key();
    let requested = mutation.permissions().to_vec();
    let event_context = mutation.event_context_value().cloned();
    runtime
        .with_transaction(async move |connection| {
            let before_revision = lock_permission_owner(connection, key.collection_id()).await?;
            let before = lock_grant(connection, key.collection_id(), key.group_id())
                .await?
                .ok_or_else(|| PostgresStorageError::not_found("Entity not found"))?;
            if !revoke_changes(&before, &requested) {
                return Ok(before.into_storage());
            }
            let after = diesel::update(
                crate::schema::permissions::table
                    .filter(crate::schema::permissions::collection_id.eq(key.collection_id()))
                    .filter(crate::schema::permissions::group_id.eq(key.group_id())),
            )
            .set(UpdatePermission::revoke(&requested))
            .get_result::<PermissionRow>(connection)
            .await?;
            if let Some(context) = event_context.as_ref() {
                let after_revision =
                    permission_owner_revision(connection, key.collection_id()).await?;
                append_permission_event(
                    connection,
                    PermissionEvent {
                        action: Action::Revoked,
                        context,
                        before: Some(&before),
                        after: &after,
                        before_revision,
                        after_revision,
                        requested: &requested,
                        replace_existing: None,
                        removes_grant: false,
                    },
                )
                .await?;
            }
            Ok::<_, PostgresStorageError>(after.into_storage())
        })
        .await
}

pub async fn revoke_all_local_collection_grants(
    runtime: &PostgresRuntime,
    request: AuthorizationGrantDelete,
) -> Result<(), PostgresStorageError> {
    let key = request.key();
    let event_context = request.event_context_value().cloned();
    runtime
        .with_transaction(async move |connection| {
            let before_revision = lock_permission_owner(connection, key.collection_id()).await?;
            let before = lock_grant(connection, key.collection_id(), key.group_id()).await?;
            diesel::delete(
                crate::schema::permissions::table
                    .filter(crate::schema::permissions::collection_id.eq(key.collection_id()))
                    .filter(crate::schema::permissions::group_id.eq(key.group_id())),
            )
            .execute(connection)
            .await?;
            if let (Some(context), Some(before)) = (event_context.as_ref(), before.as_ref()) {
                let after_revision =
                    permission_owner_revision(connection, key.collection_id()).await?;
                let requested = before.permissions();
                append_permission_event(
                    connection,
                    PermissionEvent {
                        action: Action::Revoked,
                        context,
                        before: Some(before),
                        after: before,
                        before_revision,
                        after_revision,
                        requested: &requested,
                        replace_existing: None,
                        removes_grant: true,
                    },
                )
                .await?;
            }
            Ok::<_, PostgresStorageError>(())
        })
        .await
}

async fn lock_permission_owner(
    connection: &mut PostgresConnection,
    collection_id: i32,
) -> Result<PostgresRevision, PostgresStorageError> {
    let revision = crate::schema::collection_authorization_state::table
        .filter(crate::schema::collection_authorization_state::collection_id.eq(collection_id))
        .select(crate::schema::collection_authorization_state::revision)
        .for_update()
        .first::<PostgresRevision>(connection)
        .await?;
    assert_locked_revision_precondition(
        connection,
        &RevisionOwner::CollectionPermissions.key(collection_id),
        revision,
    )
    .await?;
    Ok(revision)
}

async fn permission_owner_revision(
    connection: &mut PostgresConnection,
    collection_id: i32,
) -> Result<PostgresRevision, PostgresStorageError> {
    crate::schema::collection_authorization_state::table
        .filter(crate::schema::collection_authorization_state::collection_id.eq(collection_id))
        .select(crate::schema::collection_authorization_state::revision)
        .first(connection)
        .await
        .map_err(PostgresStorageError::from)
}

async fn lock_grant(
    connection: &mut PostgresConnection,
    collection_id: i32,
    group_id: i32,
) -> Result<Option<PermissionRow>, PostgresStorageError> {
    crate::schema::permissions::table
        .filter(crate::schema::permissions::collection_id.eq(collection_id))
        .filter(crate::schema::permissions::group_id.eq(group_id))
        .for_update()
        .first(connection)
        .await
        .optional()
        .map_err(PostgresStorageError::from)
}

fn grant_changes(
    current: &PermissionRow,
    requested: &[AuthorizationPermission],
    replace_existing: bool,
) -> bool {
    let granted = current.permissions();
    if replace_existing {
        granted != requested
    } else {
        requested
            .iter()
            .any(|permission| !granted.contains(permission))
    }
}

fn revoke_changes(current: &PermissionRow, requested: &[AuthorizationPermission]) -> bool {
    let granted = current.permissions();
    requested
        .iter()
        .any(|permission| granted.contains(permission))
}

struct PermissionEvent<'event> {
    action: Action,
    context: &'event EventContext,
    before: Option<&'event PermissionRow>,
    after: &'event PermissionRow,
    before_revision: PostgresRevision,
    after_revision: PostgresRevision,
    requested: &'event [AuthorizationPermission],
    replace_existing: Option<bool>,
    removes_grant: bool,
}

async fn append_permission_event(
    connection: &mut PostgresConnection,
    details: PermissionEvent<'_>,
) -> Result<(), PostgresStorageError> {
    let summary = match details.action {
        Action::Granted => format!(
            "Permissions granted to group {} on collection {}",
            details.after.group_id, details.after.collection_id
        ),
        Action::Revoked if details.removes_grant => format!(
            "All permissions revoked from group {} on collection {}",
            details.after.group_id, details.after.collection_id
        ),
        Action::Revoked => format!(
            "Permissions revoked from group {} on collection {}",
            details.after.group_id, details.after.collection_id
        ),
        _ => unreachable!("permission events only grant or revoke"),
    };
    let mut metadata = json!({
        "collection_id": details.after.collection_id,
        "group_id": details.after.group_id,
        "requested_permissions": permission_names(details.requested),
        "granted_permissions": permission_names(&details.after.permissions()),
    });
    if let Some(replace_existing) = details.replace_existing {
        metadata["replace_existing"] = json!(replace_existing);
    }
    let event = NewEvent::new(
        EntityType::Permission,
        details.action,
        details.context.actor_kind(),
        summary,
    )
    .map_err(|error| PostgresStorageError::database(error.to_string()))?
    .with_context(details.context)
    .with_entity_id(details.after.id)
    .with_collection_id(details.after.collection_id)
    .with_metadata(metadata)
    .with_before(match details.before {
        Some(before) => permission_snapshot(before, details.before_revision),
        None => empty_permission_snapshot(
            details.after.collection_id,
            details.after.group_id,
            details.before_revision,
        ),
    })
    .with_after(if details.removes_grant {
        empty_permission_snapshot(
            details.after.collection_id,
            details.after.group_id,
            details.after_revision,
        )
    } else {
        permission_snapshot(details.after, details.after_revision)
    });
    append_event(connection, &event).await.map(|_| ())
}

fn permission_snapshot(
    permission: &PermissionRow,
    revision: PostgresRevision,
) -> serde_json::Value {
    json!({
        "id": permission.id,
        "collection_id": permission.collection_id,
        "group_id": permission.group_id,
        "granted_permissions": permission_names(&permission.permissions()),
        "revision": revision,
        "created_at": permission.created_at,
        "updated_at": permission.updated_at,
    })
}

fn empty_permission_snapshot(
    collection_id: i32,
    group_id: i32,
    revision: PostgresRevision,
) -> serde_json::Value {
    json!({
        "collection_id": collection_id,
        "group_id": group_id,
        "granted_permissions": Vec::<String>::new(),
        "revision": revision,
    })
}

fn permission_names(permissions: &[AuthorizationPermission]) -> Vec<&'static str> {
    permissions
        .iter()
        .map(|permission| permission.as_str())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn permission_changeset_visits_the_complete_contract_vocabulary() {
        let mut visited = Vec::new();
        UpdatePermission::from_values(|permission| {
            visited.push(permission);
            None
        });

        assert_eq!(visited, AuthorizationPermission::ALL);
    }

    #[test]
    fn additive_grants_change_only_requested_permissions() {
        let changes = UpdatePermission::grant(&[AuthorizationPermission::ReadCollection], false);

        assert_eq!(changes.has_read_collection, Some(true));
        assert_eq!(changes.has_update_collection, None);
        assert_eq!(changes.has_manage_event_subscription, None);
    }

    #[test]
    fn replacement_grants_clear_unrequested_permissions() {
        let changes = UpdatePermission::grant(&[AuthorizationPermission::ReadCollection], true);

        assert_eq!(changes.has_read_collection, Some(true));
        assert_eq!(changes.has_update_collection, Some(false));
        assert_eq!(changes.has_manage_event_subscription, Some(false));
    }

    #[test]
    fn revocation_changes_only_requested_permissions() {
        let changes = UpdatePermission::revoke(&[
            AuthorizationPermission::UpdateCollection,
            AuthorizationPermission::ManageEventSubscription,
        ]);

        assert_eq!(changes.has_read_collection, None);
        assert_eq!(changes.has_update_collection, Some(false));
        assert_eq!(changes.has_manage_event_subscription, Some(false));
    }
}
