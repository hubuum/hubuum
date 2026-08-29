use chrono::NaiveDateTime;
use diesel::{Queryable, QueryableByName, Selectable};
use hubuum_domain::{AuthorizationGrantId, CollectionId, GroupId, IdentityScopeId};
use hubuum_storage_core::{
    StorageAuthorizationCollection, StorageAuthorizationGrant, StorageAuthorizationGroup,
    StorageAuthorizationGroupIdentity, StorageAuthorizationGroupProfile,
    StorageAuthorizationGroupSyncState, StorageAuthorizationPermission,
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
    pub(super) fn into_storage(
        self,
    ) -> Result<StorageAuthorizationCollection, PostgresStorageError> {
        crate::validate_persisted(
            "authorization collection",
            StorageAuthorizationCollection::try_new(
                CollectionId::new(self.id)?,
                self.name,
                self.description,
                self.created_at.and_utc(),
                self.updated_at.and_utc(),
                self.parent_collection_id
                    .map(CollectionId::new)
                    .transpose()?,
                self.revision.into_domain(),
            ),
        )
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
    pub(super) fn into_storage(self) -> Result<StorageAuthorizationGroup, PostgresStorageError> {
        let profile = crate::validate_persisted(
            "authorization group profile",
            StorageAuthorizationGroupProfile::try_new(
                self.description,
                self.created_at.and_utc(),
                self.updated_at.and_utc(),
                self.revision.into_domain(),
            ),
        )?;
        let sync = crate::validate_persisted(
            "authorization group synchronization state",
            StorageAuthorizationGroupSyncState::try_new(
                self.last_sync_attempted_at
                    .map(|timestamp| timestamp.and_utc()),
                self.last_sync_success_at
                    .map(|timestamp| timestamp.and_utc()),
            ),
        )?;
        Ok(StorageAuthorizationGroup::new(
            StorageAuthorizationGroupIdentity::new(
                GroupId::new(self.id)?,
                self.groupname,
                IdentityScopeId::new(self.identity_scope_id)?,
                self.managed_by,
                self.external_key,
            ),
            profile,
            sync,
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
    pub(super) fn permissions(self) -> Vec<StorageAuthorizationPermission> {
        let values = [
            (
                StorageAuthorizationPermission::ReadCollection,
                self.has_read_collection,
            ),
            (
                StorageAuthorizationPermission::UpdateCollection,
                self.has_update_collection,
            ),
            (
                StorageAuthorizationPermission::DeleteCollection,
                self.has_delete_collection,
            ),
            (
                StorageAuthorizationPermission::DelegateCollection,
                self.has_delegate_collection,
            ),
            (
                StorageAuthorizationPermission::CreateClass,
                self.has_create_class,
            ),
            (
                StorageAuthorizationPermission::ReadClass,
                self.has_read_class,
            ),
            (
                StorageAuthorizationPermission::UpdateClass,
                self.has_update_class,
            ),
            (
                StorageAuthorizationPermission::DeleteClass,
                self.has_delete_class,
            ),
            (
                StorageAuthorizationPermission::CreateObject,
                self.has_create_object,
            ),
            (
                StorageAuthorizationPermission::ReadObject,
                self.has_read_object,
            ),
            (
                StorageAuthorizationPermission::UpdateObject,
                self.has_update_object,
            ),
            (
                StorageAuthorizationPermission::DeleteObject,
                self.has_delete_object,
            ),
            (
                StorageAuthorizationPermission::CreateClassRelation,
                self.has_create_class_relation,
            ),
            (
                StorageAuthorizationPermission::ReadClassRelation,
                self.has_read_class_relation,
            ),
            (
                StorageAuthorizationPermission::UpdateClassRelation,
                self.has_update_class_relation,
            ),
            (
                StorageAuthorizationPermission::DeleteClassRelation,
                self.has_delete_class_relation,
            ),
            (
                StorageAuthorizationPermission::CreateObjectRelation,
                self.has_create_object_relation,
            ),
            (
                StorageAuthorizationPermission::ReadObjectRelation,
                self.has_read_object_relation,
            ),
            (
                StorageAuthorizationPermission::UpdateObjectRelation,
                self.has_update_object_relation,
            ),
            (
                StorageAuthorizationPermission::DeleteObjectRelation,
                self.has_delete_object_relation,
            ),
            (
                StorageAuthorizationPermission::ReadTemplate,
                self.has_read_template,
            ),
            (
                StorageAuthorizationPermission::CreateTemplate,
                self.has_create_template,
            ),
            (
                StorageAuthorizationPermission::UpdateTemplate,
                self.has_update_template,
            ),
            (
                StorageAuthorizationPermission::DeleteTemplate,
                self.has_delete_template,
            ),
            (
                StorageAuthorizationPermission::ReadRemoteTarget,
                self.has_read_remote_target,
            ),
            (
                StorageAuthorizationPermission::CreateRemoteTarget,
                self.has_create_remote_target,
            ),
            (
                StorageAuthorizationPermission::UpdateRemoteTarget,
                self.has_update_remote_target,
            ),
            (
                StorageAuthorizationPermission::DeleteRemoteTarget,
                self.has_delete_remote_target,
            ),
            (
                StorageAuthorizationPermission::ExecuteRemoteTarget,
                self.has_execute_remote_target,
            ),
            (
                StorageAuthorizationPermission::ReadAudit,
                self.has_read_audit,
            ),
            (
                StorageAuthorizationPermission::ManageEventSubscription,
                self.has_manage_event_subscription,
            ),
        ];
        values
            .into_iter()
            .filter_map(|(permission, granted)| granted.then_some(permission))
            .collect()
    }

    pub(super) fn into_storage(self) -> Result<StorageAuthorizationGrant, PostgresStorageError> {
        crate::validate_persisted(
            "authorization grant",
            StorageAuthorizationGrant::try_new(
                AuthorizationGrantId::new(self.id)?,
                CollectionId::new(self.collection_id)?,
                GroupId::new(self.group_id)?,
                self.permissions(),
                self.created_at.and_utc(),
                self.updated_at.and_utc(),
            ),
        )
    }
}
