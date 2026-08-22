// src/models/group.rs

use crate::errors::ApiError;
use crate::events::EventContext;
use crate::models::principal::Principal;
use crate::models::search::{FilterField, QueryOptions, SortParam};
use crate::models::{LOCAL_PROVIDER_KIND, ResourceRevision};
use crate::services::storage_boundary::{
    group_create_to_storage, group_from_storage, group_id_to_storage, group_update_to_storage,
    principal_from_storage, principal_group_from_storage, principal_id_to_storage,
};
use crate::storage::{GroupMembershipStorage, GroupStorage, StorageContext, storage_handle};
use crate::traits::PrincipalIdAccessor;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::traits::accessors::{IdAccessor, InstanceAdapter};
use crate::traits::{CursorPaginated, CursorValue};

pub use hubuum_domain::GroupId as GroupID;

impl IdAccessor for GroupID {
    fn accessor_id(&self) -> i32 {
        (*self).id()
    }
}

impl InstanceAdapter<Group> for GroupID {
    async fn instance_adapter(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<Group, ApiError> {
        self.group(pool).await
    }
}

/// Application behavior for a backend-neutral group identifier.
pub trait GroupIdApplicationExt {
    async fn group<C>(&self, backend: &C) -> Result<Group, ApiError>
    where
        C: StorageContext;

    async fn delete_without_events<C>(&self, backend: &C) -> Result<usize, ApiError>
    where
        C: StorageContext;

    async fn delete<C>(&self, backend: &C, context: &EventContext) -> Result<usize, ApiError>
    where
        C: StorageContext;
}

impl GroupIdApplicationExt for GroupID {
    async fn group<C>(&self, backend: &C) -> Result<Group, ApiError>
    where
        C: StorageContext,
    {
        storage_handle(backend)
            .get_group(*self)
            .await
            .map_err(ApiError::from)
            .and_then(group_from_storage)
    }

    /// Delete this group with system audit attribution.
    ///
    /// The compatibility name is retained for internal callers; the mutation
    /// still emits its audit event.
    async fn delete_without_events<C>(&self, backend: &C) -> Result<usize, ApiError>
    where
        C: StorageContext,
    {
        storage_handle(backend)
            .delete_group(*self, &EventContext::system())
            .await
            .map_err(ApiError::from)
            .map(|outcome| outcome.into_value())
    }

    async fn delete<C>(&self, backend: &C, context: &EventContext) -> Result<usize, ApiError>
    where
        C: StorageContext,
    {
        storage_handle(backend)
            .delete_group(*self, context)
            .await
            .map_err(ApiError::from)
            .map(|outcome| outcome.into_value())
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, ToSchema)]
pub struct Group {
    pub id: i32,
    pub groupname: String,
    pub description: String,
    pub created_at: chrono::NaiveDateTime,
    pub updated_at: chrono::NaiveDateTime,
    pub identity_scope_id: i32,
    pub managed_by: String,
    pub external_key: Option<String>,
    pub last_sync_attempted_at: Option<chrono::NaiveDateTime>,
    pub last_sync_success_at: Option<chrono::NaiveDateTime>,
    pub revision: ResourceRevision,
}

impl Group {
    pub fn ensure_local_writes_allowed(&self) -> Result<(), ApiError> {
        if self.managed_by != LOCAL_PROVIDER_KIND {
            return Err(ApiError::Forbidden(
                "Provider-managed groups are read-only in Hubuum".to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, ToSchema)]
pub struct GroupResponse {
    pub id: i32,
    pub identity_scope: String,
    pub groupname: String,
    pub description: String,
    pub managed_by: String,
    pub external_key: Option<String>,
    pub last_sync_attempted_at: Option<chrono::NaiveDateTime>,
    pub last_sync_success_at: Option<chrono::NaiveDateTime>,
    pub created_at: chrono::NaiveDateTime,
    pub updated_at: chrono::NaiveDateTime,
    pub revision: ResourceRevision,
}

/// Canonical group representation covered completely by the group revision.
/// Directory synchronization timestamps remain available in list responses,
/// but are intentionally excluded here because they are operational state and
/// do not advance the authoritative revision.
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, ToSchema)]
pub struct GroupPointResponse {
    pub id: i32,
    pub identity_scope: String,
    pub groupname: String,
    pub description: String,
    pub managed_by: String,
    pub external_key: Option<String>,
    pub created_at: chrono::NaiveDateTime,
    pub updated_at: chrono::NaiveDateTime,
    pub revision: ResourceRevision,
}

impl From<GroupResponse> for GroupPointResponse {
    fn from(group: GroupResponse) -> Self {
        Self {
            id: group.id,
            identity_scope: group.identity_scope,
            groupname: group.groupname,
            description: group.description,
            managed_by: group.managed_by,
            external_key: group.external_key,
            created_at: group.created_at,
            updated_at: group.updated_at,
            revision: group.revision,
        }
    }
}

impl GroupResponse {
    pub fn from_parts(group: &Group, identity_scope: String) -> Self {
        Self {
            id: group.id,
            identity_scope,
            groupname: group.groupname.clone(),
            description: group.description.clone(),
            managed_by: group.managed_by.clone(),
            external_key: group.external_key.clone(),
            last_sync_attempted_at: group.last_sync_attempted_at,
            last_sync_success_at: group.last_sync_success_at,
            created_at: group.created_at,
            updated_at: group.updated_at,
            revision: group.revision,
        }
    }

    pub async fn from_groups<C>(backend: &C, groups: Vec<Group>) -> Result<Vec<Self>, ApiError>
    where
        C: StorageContext,
    {
        let scope_ids = groups
            .iter()
            .map(|group| group.identity_scope_id)
            .collect::<Vec<_>>();
        let scope_names =
            crate::services::identity::resolve_identity_scope_names(backend, &scope_ids).await?;

        groups
            .into_iter()
            .map(|group| {
                let identity_scope = scope_names
                    .get(&group.identity_scope_id)
                    .cloned()
                    .ok_or_else(|| {
                        ApiError::InternalServerError(format!(
                            "Identity scope '{}' was not resolved",
                            group.identity_scope_id
                        ))
                    })?;
                Ok(Self::from_parts(&group, identity_scope))
            })
            .collect()
    }
}

impl CursorPaginated for GroupResponse {
    fn supports_sort(field: &FilterField) -> bool {
        Group::supports_sort(field)
    }

    fn cursor_value(&self, field: &FilterField) -> Result<CursorValue, ApiError> {
        Ok(match field {
            FilterField::Id => CursorValue::Integer(self.id as i64),
            FilterField::Name | FilterField::Groupname => {
                CursorValue::String(self.groupname.clone())
            }
            FilterField::Description => CursorValue::String(self.description.clone()),
            FilterField::CreatedAt => CursorValue::DateTime(self.created_at),
            FilterField::UpdatedAt => CursorValue::DateTime(self.updated_at),
            FilterField::Revision => CursorValue::Integer(self.revision.get()),
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{}' is not orderable for groups",
                    field
                )));
            }
        })
    }

    fn default_sort() -> Vec<SortParam> {
        Group::default_sort()
    }

    fn tie_breaker_sort() -> Vec<SortParam> {
        Group::tie_breaker_sort()
    }
}

impl IdAccessor for Group {
    fn accessor_id(&self) -> i32 {
        self.id
    }
}

impl InstanceAdapter<Group> for Group {
    async fn instance_adapter(
        &self,
        _pool: &impl crate::storage::StorageContext,
    ) -> Result<Group, ApiError> {
        Ok(self.clone())
    }
}

impl Group {
    pub async fn to_response<C>(&self, backend: &C) -> Result<GroupResponse, ApiError>
    where
        C: StorageContext,
    {
        let identity_scope = storage_handle(backend)
            .resolve_group_identity_scope_name(group_id_to_storage(self.id))
            .await
            .map_err(ApiError::from)?;
        Ok(GroupResponse::from_parts(self, identity_scope))
    }

    pub async fn to_point_response<C>(&self, backend: &C) -> Result<GroupPointResponse, ApiError>
    where
        C: StorageContext,
    {
        Ok(self.to_response(backend).await?.into())
    }

    pub async fn members<C>(&self, backend: &C) -> Result<Vec<Principal>, ApiError>
    where
        C: StorageContext,
    {
        storage_handle(backend)
            .load_group_member_principals(group_id_to_storage(self.id))
            .await
            .map_err(ApiError::from)
            .and_then(|members| members.into_iter().map(principal_from_storage).collect())
    }

    pub async fn members_paginated<C>(
        &self,
        backend: &C,
        query_options: &QueryOptions,
    ) -> Result<(Vec<(crate::models::PrincipalGroup, Principal)>, Option<i64>), ApiError>
    where
        C: StorageContext,
    {
        storage_handle(backend)
            .list_group_members(group_id_to_storage(self.id), query_options.clone())
            .await
            .map_err(ApiError::from)
            .and_then(|page| {
                let (members, total) = page.into_parts();
                let members = members
                    .into_iter()
                    .map(|member| {
                        let (membership, principal) = member.into_parts();
                        Ok((
                            principal_group_from_storage(membership)?,
                            principal_from_storage(principal)?,
                        ))
                    })
                    .collect::<Result<Vec<_>, ApiError>>()?;
                Ok((members, total))
            })
    }

    /// Add a member to a group. If the user is already a member, do nothing.
    ///
    /// ## Arguments
    /// * `backend` - The backend context used to persist the membership
    /// * `user` - The user to add to the group
    ///
    /// ## Returns
    /// * `Ok(())` if the user was added to the group
    /// * `Err(ApiError)` if the user was not added to the group
    ///
    /// This compatibility path emits an event attributed to the system actor.
    ///
    /// If the user is already a member of the group, this function is a safe noop.
    pub async fn add_member_without_events<C, P>(
        &self,
        backend: &C,
        member: &P,
    ) -> Result<(), ApiError>
    where
        C: StorageContext,
        P: PrincipalIdAccessor,
    {
        let _membership = storage_handle(backend)
            .add_group_member(
                principal_id_to_storage(member.principal_id()),
                group_id_to_storage(self.id),
                &EventContext::system(),
            )
            .await
            .map_err(ApiError::from)?
            .into_value();

        Ok(())
    }

    pub async fn add_member<C, P>(
        &self,
        backend: &C,
        member: &P,
        context: &EventContext,
    ) -> Result<crate::models::PrincipalGroup, ApiError>
    where
        C: StorageContext,
        P: PrincipalIdAccessor,
    {
        storage_handle(backend)
            .add_group_member(
                principal_id_to_storage(member.principal_id()),
                group_id_to_storage(self.id),
                context,
            )
            .await
            .map_err(ApiError::from)
            .map(|outcome| outcome.into_value())
            .and_then(principal_group_from_storage)
    }

    /// Remove a member with system audit attribution.
    ///
    /// The compatibility name is retained for internal callers; the mutation
    /// still emits its audit event.
    pub async fn remove_member_without_events<C, P>(
        &self,
        member: &P,
        backend: &C,
    ) -> Result<(), ApiError>
    where
        C: StorageContext,
        P: PrincipalIdAccessor,
    {
        storage_handle(backend)
            .remove_group_member(
                principal_id_to_storage(member.principal_id()),
                group_id_to_storage(self.id),
                &EventContext::system(),
            )
            .await
            .map_err(ApiError::from)
            .map(|outcome| outcome.into_value())
    }

    pub async fn remove_member<C, P>(
        &self,
        member: &P,
        backend: &C,
        context: &EventContext,
    ) -> Result<(), ApiError>
    where
        C: StorageContext,
        P: PrincipalIdAccessor,
    {
        storage_handle(backend)
            .remove_group_member(
                principal_id_to_storage(member.principal_id()),
                group_id_to_storage(self.id),
                context,
            )
            .await
            .map_err(ApiError::from)
            .map(|outcome| outcome.into_value())
    }

    /// Delete this group with system audit attribution.
    ///
    /// The compatibility name is retained for internal callers; the mutation
    /// still emits its audit event.
    pub async fn delete_without_events<C>(&self, backend: &C) -> Result<usize, ApiError>
    where
        C: StorageContext,
    {
        storage_handle(backend)
            .delete_group(group_id_to_storage(self.id), &EventContext::system())
            .await
            .map_err(ApiError::from)
            .map(|outcome| outcome.into_value())
    }
}

#[derive(Deserialize, Serialize, Debug, ToSchema)]
#[schema(example = new_group_example)]
pub struct NewGroup {
    pub identity_scope: Option<String>,
    pub groupname: String,
    pub description: Option<String>,
}

impl NewGroup {
    /// Persist with system audit attribution.
    ///
    /// The compatibility name is retained for internal callers; the mutation
    /// still emits its audit event.
    pub async fn save_without_events<C>(&self, backend: &C) -> Result<Group, ApiError>
    where
        C: StorageContext,
    {
        storage_handle(backend)
            .create_group(group_create_to_storage(self), &EventContext::system())
            .await
            .map_err(ApiError::from)
            .map(|outcome| outcome.into_value())
            .and_then(group_from_storage)
    }

    pub async fn save<C>(&self, backend: &C, context: &EventContext) -> Result<Group, ApiError>
    where
        C: StorageContext,
    {
        storage_handle(backend)
            .create_group(group_create_to_storage(self), context)
            .await
            .map_err(ApiError::from)
            .map(|outcome| outcome.into_value())
            .and_then(group_from_storage)
    }
}

#[derive(Deserialize, Serialize, ToSchema)]
#[schema(example = update_group_example)]
pub struct UpdateGroup {
    pub groupname: Option<String>,
}

impl UpdateGroup {
    /// Persist changes with system audit attribution.
    ///
    /// The compatibility name is retained for internal callers; the mutation
    /// still emits its audit event.
    pub async fn save_without_events<C>(
        &self,
        group_id: GroupID,
        backend: &C,
    ) -> Result<Group, ApiError>
    where
        C: StorageContext,
    {
        storage_handle(backend)
            .update_group(
                group_id,
                group_update_to_storage(self),
                &EventContext::system(),
            )
            .await
            .map_err(ApiError::from)
            .map(|outcome| outcome.into_value())
            .and_then(group_from_storage)
    }

    pub async fn save<C>(
        &self,
        group_id: GroupID,
        backend: &C,
        context: &EventContext,
    ) -> Result<Group, ApiError>
    where
        C: StorageContext,
    {
        storage_handle(backend)
            .update_group(group_id, group_update_to_storage(self), context)
            .await
            .map_err(ApiError::from)
            .map(|outcome| outcome.into_value())
            .and_then(group_from_storage)
    }
}

fn new_group_example() -> NewGroup {
    NewGroup {
        identity_scope: None,
        groupname: "ops".to_string(),
        description: Some("Operations team".to_string()),
    }
}

fn update_group_example() -> UpdateGroup {
    UpdateGroup {
        groupname: Some("platform-ops".to_string()),
    }
}

impl CursorPaginated for Group {
    fn supports_sort(field: &FilterField) -> bool {
        matches!(
            field,
            FilterField::Id
                | FilterField::Name
                | FilterField::Groupname
                | FilterField::Description
                | FilterField::CreatedAt
                | FilterField::UpdatedAt
                | FilterField::Revision
        )
    }

    fn cursor_value(&self, field: &FilterField) -> Result<CursorValue, ApiError> {
        Ok(match field {
            FilterField::Id => CursorValue::Integer(self.id as i64),
            FilterField::Name | FilterField::Groupname => {
                CursorValue::String(self.groupname.clone())
            }
            FilterField::Description => CursorValue::String(self.description.clone()),
            FilterField::CreatedAt => CursorValue::DateTime(self.created_at),
            FilterField::UpdatedAt => CursorValue::DateTime(self.updated_at),
            FilterField::Revision => CursorValue::Integer(self.revision.get()),
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{}' is not orderable for groups",
                    field
                )));
            }
        })
    }

    fn default_sort() -> Vec<SortParam> {
        vec![SortParam {
            field: FilterField::Id,
            descending: false,
        }]
    }

    fn tie_breaker_sort() -> Vec<SortParam> {
        Self::default_sort()
    }
}
