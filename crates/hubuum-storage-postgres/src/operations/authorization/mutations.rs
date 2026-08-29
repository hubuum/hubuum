use diesel::prelude::{ExpressionMethods, OptionalExtension, QueryDsl};
use diesel::{AsChangeset, Insertable};
use diesel_async::RunQueryDsl;
use hubuum_events_core::{Action, EntityType, EventContext, NewEvent};
use hubuum_storage_core::{
    StorageAuditReceipt, StorageAuthorizationGrant, StorageAuthorizationGrantDelete,
    StorageAuthorizationGrantMutation, StorageAuthorizationPermission, StorageMutationOutcome,
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
        permissions: &[StorageAuthorizationPermission],
    ) -> Self {
        let has = |permission| permissions.contains(&permission);
        Self {
            collection_id,
            group_id,
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
            has_read_audit: has(StorageAuthorizationPermission::ReadAudit),
            has_manage_event_subscription: has(
                StorageAuthorizationPermission::ManageEventSubscription,
            ),
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
    pub(crate) fn grant(
        permissions: &[StorageAuthorizationPermission],
        replace_existing: bool,
    ) -> Self {
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

    fn revoke(permissions: &[StorageAuthorizationPermission]) -> Self {
        Self::from_values(|permission| permissions.contains(&permission).then_some(false))
    }

    fn from_values(mut value: impl FnMut(StorageAuthorizationPermission) -> Option<bool>) -> Self {
        Self {
            has_read_collection: value(StorageAuthorizationPermission::ReadCollection),
            has_update_collection: value(StorageAuthorizationPermission::UpdateCollection),
            has_delete_collection: value(StorageAuthorizationPermission::DeleteCollection),
            has_delegate_collection: value(StorageAuthorizationPermission::DelegateCollection),
            has_create_class: value(StorageAuthorizationPermission::CreateClass),
            has_read_class: value(StorageAuthorizationPermission::ReadClass),
            has_update_class: value(StorageAuthorizationPermission::UpdateClass),
            has_delete_class: value(StorageAuthorizationPermission::DeleteClass),
            has_create_object: value(StorageAuthorizationPermission::CreateObject),
            has_read_object: value(StorageAuthorizationPermission::ReadObject),
            has_update_object: value(StorageAuthorizationPermission::UpdateObject),
            has_delete_object: value(StorageAuthorizationPermission::DeleteObject),
            has_create_class_relation: value(StorageAuthorizationPermission::CreateClassRelation),
            has_read_class_relation: value(StorageAuthorizationPermission::ReadClassRelation),
            has_update_class_relation: value(StorageAuthorizationPermission::UpdateClassRelation),
            has_delete_class_relation: value(StorageAuthorizationPermission::DeleteClassRelation),
            has_create_object_relation: value(StorageAuthorizationPermission::CreateObjectRelation),
            has_read_object_relation: value(StorageAuthorizationPermission::ReadObjectRelation),
            has_update_object_relation: value(StorageAuthorizationPermission::UpdateObjectRelation),
            has_delete_object_relation: value(StorageAuthorizationPermission::DeleteObjectRelation),
            has_read_template: value(StorageAuthorizationPermission::ReadTemplate),
            has_create_template: value(StorageAuthorizationPermission::CreateTemplate),
            has_update_template: value(StorageAuthorizationPermission::UpdateTemplate),
            has_delete_template: value(StorageAuthorizationPermission::DeleteTemplate),
            has_read_remote_target: value(StorageAuthorizationPermission::ReadRemoteTarget),
            has_create_remote_target: value(StorageAuthorizationPermission::CreateRemoteTarget),
            has_update_remote_target: value(StorageAuthorizationPermission::UpdateRemoteTarget),
            has_delete_remote_target: value(StorageAuthorizationPermission::DeleteRemoteTarget),
            has_execute_remote_target: value(StorageAuthorizationPermission::ExecuteRemoteTarget),
            has_read_audit: value(StorageAuthorizationPermission::ReadAudit),
            has_manage_event_subscription: value(
                StorageAuthorizationPermission::ManageEventSubscription,
            ),
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
            &StorageAuthorizationPermission::ALL,
        ))
        .execute(connection)
        .await?;
    Ok(())
}

pub async fn apply_local_collection_grant(
    runtime: &PostgresRuntime,
    mutation: StorageAuthorizationGrantMutation,
) -> Result<StorageMutationOutcome<StorageAuthorizationGrant>, PostgresStorageError> {
    let key = mutation.key();
    let requested = mutation.permissions().to_vec();
    let replace_existing = mutation.replace_existing();
    let event_context = mutation.event_context().clone();
    let collection_id = key.collection_id().id();
    let group_id = key.group_id().id();
    runtime
        .with_transaction(async move |connection| {
            let before_revision = lock_permission_owner(connection, collection_id).await?;
            let before = lock_grant(connection, collection_id, group_id).await?;
            if let Some(current) = before
                && !grant_changes(&current, &requested, replace_existing)
            {
                return Ok(StorageMutationOutcome::unchanged(current.into_storage()?));
            }

            let after = match before {
                Some(_) => {
                    diesel::update(
                        crate::schema::permissions::table
                            .filter(crate::schema::permissions::collection_id.eq(collection_id))
                            .filter(crate::schema::permissions::group_id.eq(group_id)),
                    )
                    .set(UpdatePermission::grant(&requested, replace_existing))
                    .get_result::<PermissionRow>(connection)
                    .await?
                }
                None => {
                    diesel::insert_into(crate::schema::permissions::table)
                        .values(NewPermission::new(collection_id, group_id, &requested))
                        .get_result::<PermissionRow>(connection)
                        .await?
                }
            };
            let after_revision = permission_owner_revision(connection, collection_id).await?;
            let audit = append_permission_event(
                connection,
                PermissionEvent {
                    action: Action::Granted,
                    context: &event_context,
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
            Ok::<_, PostgresStorageError>(StorageMutationOutcome::committed(
                after.into_storage()?,
                audit,
            ))
        })
        .await
}

pub async fn revoke_local_collection_grant(
    runtime: &PostgresRuntime,
    mutation: StorageAuthorizationGrantMutation,
) -> Result<StorageMutationOutcome<StorageAuthorizationGrant>, PostgresStorageError> {
    let key = mutation.key();
    let requested = mutation.permissions().to_vec();
    let event_context = mutation.event_context().clone();
    let collection_id = key.collection_id().id();
    let group_id = key.group_id().id();
    runtime
        .with_transaction(async move |connection| {
            let before_revision = lock_permission_owner(connection, collection_id).await?;
            let before = lock_grant(connection, collection_id, group_id)
                .await?
                .ok_or_else(|| PostgresStorageError::not_found("Entity not found"))?;
            if !revoke_changes(&before, &requested) {
                return Ok(StorageMutationOutcome::unchanged(before.into_storage()?));
            }
            let after = diesel::update(
                crate::schema::permissions::table
                    .filter(crate::schema::permissions::collection_id.eq(collection_id))
                    .filter(crate::schema::permissions::group_id.eq(group_id)),
            )
            .set(UpdatePermission::revoke(&requested))
            .get_result::<PermissionRow>(connection)
            .await?;
            let after_revision = permission_owner_revision(connection, collection_id).await?;
            let audit = append_permission_event(
                connection,
                PermissionEvent {
                    action: Action::Revoked,
                    context: &event_context,
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
            Ok::<_, PostgresStorageError>(StorageMutationOutcome::committed(
                after.into_storage()?,
                audit,
            ))
        })
        .await
}

pub async fn revoke_all_local_collection_grants(
    runtime: &PostgresRuntime,
    request: StorageAuthorizationGrantDelete,
) -> Result<StorageMutationOutcome<()>, PostgresStorageError> {
    let key = request.key();
    let event_context = request.event_context().clone();
    let collection_id = key.collection_id().id();
    let group_id = key.group_id().id();
    runtime
        .with_transaction(async move |connection| {
            let before_revision = lock_permission_owner(connection, collection_id).await?;
            let before = lock_grant(connection, collection_id, group_id).await?;
            diesel::delete(
                crate::schema::permissions::table
                    .filter(crate::schema::permissions::collection_id.eq(collection_id))
                    .filter(crate::schema::permissions::group_id.eq(group_id)),
            )
            .execute(connection)
            .await?;
            let Some(before) = before.as_ref() else {
                return Ok(StorageMutationOutcome::unchanged(()));
            };
            let after_revision = permission_owner_revision(connection, collection_id).await?;
            let requested = before.permissions();
            let audit = append_permission_event(
                connection,
                PermissionEvent {
                    action: Action::Revoked,
                    context: &event_context,
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
            Ok::<_, PostgresStorageError>(StorageMutationOutcome::committed((), audit))
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
    requested: &[StorageAuthorizationPermission],
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

fn revoke_changes(current: &PermissionRow, requested: &[StorageAuthorizationPermission]) -> bool {
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
    requested: &'event [StorageAuthorizationPermission],
    replace_existing: Option<bool>,
    removes_grant: bool,
}

async fn append_permission_event(
    connection: &mut PostgresConnection,
    details: PermissionEvent<'_>,
) -> Result<StorageAuditReceipt, PostgresStorageError> {
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
    .with_entity_id(hubuum_events_core::EventEntityId::new(details.after.id)?)
    .with_collection_id(hubuum_domain::CollectionId::new(
        details.after.collection_id,
    )?)
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
    Ok(append_event(connection, &event).await?.into_audit_receipt())
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

fn permission_names(permissions: &[StorageAuthorizationPermission]) -> Vec<&'static str> {
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

        assert_eq!(visited, StorageAuthorizationPermission::ALL);
    }

    #[test]
    fn additive_grants_change_only_requested_permissions() {
        let changes =
            UpdatePermission::grant(&[StorageAuthorizationPermission::ReadCollection], false);

        assert_eq!(changes.has_read_collection, Some(true));
        assert_eq!(changes.has_update_collection, None);
        assert_eq!(changes.has_manage_event_subscription, None);
    }

    #[test]
    fn replacement_grants_clear_unrequested_permissions() {
        let changes =
            UpdatePermission::grant(&[StorageAuthorizationPermission::ReadCollection], true);

        assert_eq!(changes.has_read_collection, Some(true));
        assert_eq!(changes.has_update_collection, Some(false));
        assert_eq!(changes.has_manage_event_subscription, Some(false));
    }

    #[test]
    fn revocation_changes_only_requested_permissions() {
        let changes = UpdatePermission::revoke(&[
            StorageAuthorizationPermission::UpdateCollection,
            StorageAuthorizationPermission::ManageEventSubscription,
        ]);

        assert_eq!(changes.has_read_collection, None);
        assert_eq!(changes.has_update_collection, Some(false));
        assert_eq!(changes.has_manage_event_subscription, Some(false));
    }
}
