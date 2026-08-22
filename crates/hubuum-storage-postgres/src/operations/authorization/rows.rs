use chrono::NaiveDateTime;
use diesel::{Queryable, QueryableByName, Selectable};
use hubuum_domain::{AuthorizationGrantId, CollectionId, GroupId, IdentityScopeId};
use hubuum_storage_core::{
    AuthorizationCollection, AuthorizationGrant, AuthorizationGroup, AuthorizationGroupIdentity,
    AuthorizationGroupProfile, AuthorizationGroupSyncState, AuthorizationPermission,
};

use crate::{PostgresRevision, PostgresStorageError};

#[derive(Clone, Queryable, QueryableByName, Selectable)]
#[diesel(table_name = crate::schema::collections)]
pub(super) struct CollectionRow {
    pub(super) id: i32,
    pub(super) name: String,
    pub(super) description: String,
    pub(super) created_at: NaiveDateTime,
    pub(super) updated_at: NaiveDateTime,
    pub(super) parent_collection_id: Option<i32>,
    pub(super) revision: PostgresRevision,
}

impl CollectionRow {
    pub(super) fn into_storage(self) -> Result<AuthorizationCollection, PostgresStorageError> {
        Ok(AuthorizationCollection::new(
            CollectionId::new(self.id)?,
            self.name,
            self.description,
            self.created_at.and_utc(),
            self.updated_at.and_utc(),
            self.parent_collection_id
                .map(CollectionId::new)
                .transpose()?,
            self.revision.into_domain(),
        ))
    }
}

#[derive(Clone, Queryable, QueryableByName, Selectable)]
#[diesel(table_name = crate::schema::groups)]
pub(super) struct GroupRow {
    pub(super) id: i32,
    pub(super) groupname: String,
    pub(super) description: String,
    pub(super) created_at: NaiveDateTime,
    pub(super) updated_at: NaiveDateTime,
    pub(super) identity_scope_id: i32,
    pub(super) managed_by: String,
    pub(super) external_key: Option<String>,
    pub(super) last_sync_attempted_at: Option<NaiveDateTime>,
    pub(super) last_sync_success_at: Option<NaiveDateTime>,
    pub(super) revision: PostgresRevision,
}

impl GroupRow {
    pub(super) fn into_storage(self) -> Result<AuthorizationGroup, PostgresStorageError> {
        Ok(AuthorizationGroup::new(
            AuthorizationGroupIdentity::new(
                GroupId::new(self.id)?,
                self.groupname,
                IdentityScopeId::new(self.identity_scope_id)?,
                self.managed_by,
                self.external_key,
            ),
            AuthorizationGroupProfile::new(
                self.description,
                self.created_at.and_utc(),
                self.updated_at.and_utc(),
                self.revision.into_domain(),
            ),
            AuthorizationGroupSyncState::new(
                self.last_sync_attempted_at
                    .map(|timestamp| timestamp.and_utc()),
                self.last_sync_success_at
                    .map(|timestamp| timestamp.and_utc()),
            ),
        ))
    }
}

#[derive(Clone, Copy, Queryable, QueryableByName, Selectable)]
#[diesel(table_name = crate::schema::permissions)]
pub(super) struct PermissionRow {
    pub(super) id: i32,
    pub(super) collection_id: i32,
    pub(super) group_id: i32,
    pub(super) has_read_collection: bool,
    pub(super) has_update_collection: bool,
    pub(super) has_delete_collection: bool,
    pub(super) has_delegate_collection: bool,
    pub(super) has_create_class: bool,
    pub(super) has_read_class: bool,
    pub(super) has_update_class: bool,
    pub(super) has_delete_class: bool,
    pub(super) has_create_object: bool,
    pub(super) has_read_object: bool,
    pub(super) has_update_object: bool,
    pub(super) has_delete_object: bool,
    pub(super) has_create_class_relation: bool,
    pub(super) has_read_class_relation: bool,
    pub(super) has_update_class_relation: bool,
    pub(super) has_delete_class_relation: bool,
    pub(super) has_create_object_relation: bool,
    pub(super) has_read_object_relation: bool,
    pub(super) has_update_object_relation: bool,
    pub(super) has_delete_object_relation: bool,
    pub(super) has_read_template: bool,
    pub(super) has_create_template: bool,
    pub(super) has_update_template: bool,
    pub(super) has_delete_template: bool,
    pub(super) has_read_remote_target: bool,
    pub(super) has_create_remote_target: bool,
    pub(super) has_update_remote_target: bool,
    pub(super) has_delete_remote_target: bool,
    pub(super) has_execute_remote_target: bool,
    pub(super) created_at: NaiveDateTime,
    pub(super) updated_at: NaiveDateTime,
    pub(super) has_read_audit: bool,
    pub(super) has_manage_event_subscription: bool,
}

impl PermissionRow {
    pub(super) fn permissions(self) -> Vec<AuthorizationPermission> {
        let values = [
            (
                AuthorizationPermission::ReadCollection,
                self.has_read_collection,
            ),
            (
                AuthorizationPermission::UpdateCollection,
                self.has_update_collection,
            ),
            (
                AuthorizationPermission::DeleteCollection,
                self.has_delete_collection,
            ),
            (
                AuthorizationPermission::DelegateCollection,
                self.has_delegate_collection,
            ),
            (AuthorizationPermission::CreateClass, self.has_create_class),
            (AuthorizationPermission::ReadClass, self.has_read_class),
            (AuthorizationPermission::UpdateClass, self.has_update_class),
            (AuthorizationPermission::DeleteClass, self.has_delete_class),
            (
                AuthorizationPermission::CreateObject,
                self.has_create_object,
            ),
            (AuthorizationPermission::ReadObject, self.has_read_object),
            (
                AuthorizationPermission::UpdateObject,
                self.has_update_object,
            ),
            (
                AuthorizationPermission::DeleteObject,
                self.has_delete_object,
            ),
            (
                AuthorizationPermission::CreateClassRelation,
                self.has_create_class_relation,
            ),
            (
                AuthorizationPermission::ReadClassRelation,
                self.has_read_class_relation,
            ),
            (
                AuthorizationPermission::UpdateClassRelation,
                self.has_update_class_relation,
            ),
            (
                AuthorizationPermission::DeleteClassRelation,
                self.has_delete_class_relation,
            ),
            (
                AuthorizationPermission::CreateObjectRelation,
                self.has_create_object_relation,
            ),
            (
                AuthorizationPermission::ReadObjectRelation,
                self.has_read_object_relation,
            ),
            (
                AuthorizationPermission::UpdateObjectRelation,
                self.has_update_object_relation,
            ),
            (
                AuthorizationPermission::DeleteObjectRelation,
                self.has_delete_object_relation,
            ),
            (
                AuthorizationPermission::ReadTemplate,
                self.has_read_template,
            ),
            (
                AuthorizationPermission::CreateTemplate,
                self.has_create_template,
            ),
            (
                AuthorizationPermission::UpdateTemplate,
                self.has_update_template,
            ),
            (
                AuthorizationPermission::DeleteTemplate,
                self.has_delete_template,
            ),
            (
                AuthorizationPermission::ReadRemoteTarget,
                self.has_read_remote_target,
            ),
            (
                AuthorizationPermission::CreateRemoteTarget,
                self.has_create_remote_target,
            ),
            (
                AuthorizationPermission::UpdateRemoteTarget,
                self.has_update_remote_target,
            ),
            (
                AuthorizationPermission::DeleteRemoteTarget,
                self.has_delete_remote_target,
            ),
            (
                AuthorizationPermission::ExecuteRemoteTarget,
                self.has_execute_remote_target,
            ),
            (AuthorizationPermission::ReadAudit, self.has_read_audit),
            (
                AuthorizationPermission::ManageEventSubscription,
                self.has_manage_event_subscription,
            ),
        ];
        values
            .into_iter()
            .filter_map(|(permission, granted)| granted.then_some(permission))
            .collect()
    }

    pub(super) fn into_storage(self) -> Result<AuthorizationGrant, PostgresStorageError> {
        Ok(AuthorizationGrant::new(
            AuthorizationGrantId::new(self.id)?,
            CollectionId::new(self.collection_id)?,
            GroupId::new(self.group_id)?,
            self.permissions(),
            self.created_at.and_utc(),
            self.updated_at.and_utc(),
        ))
    }
}
