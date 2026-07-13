// src/models/group.rs

use crate::db::traits::group::{
    DeleteGroupRecord, GroupMembersBackend, LoadGroupRecord, SaveGroupRecord,
    SavePrincipalGroupRecord, UpdateGroupRecord, group_identity_scope_name,
};
use crate::errors::ApiError;
use crate::events::EventContext;
use crate::models::principal::Principal;
use crate::models::principal_group::NewPrincipalGroup;
use crate::models::search::{FilterField, QueryOptions, SortParam};
use crate::schema::groups;

use crate::db::prelude::*;
use crate::traits::PrincipalIdAccessor;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::traits::accessors::{IdAccessor, InstanceAdapter};
use crate::traits::{
    BackendContext, CursorPaginated, CursorSqlField, CursorSqlMapping, CursorSqlType, CursorValue,
};

use crate::db::DbPool;

crate::int_id_newtype! {
    /// Identifier wrapper for a [`Group`].
    pub struct GroupID;
    noun = "group id";
}

impl IdAccessor for GroupID {
    fn accessor_id(&self) -> i32 {
        self.0
    }
}

impl InstanceAdapter<Group> for GroupID {
    async fn instance_adapter(&self, pool: &DbPool) -> Result<Group, ApiError> {
        self.group(pool).await
    }
}

impl GroupID {
    pub async fn group<C>(&self, backend: &C) -> Result<Group, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        self.load_group_record(backend.db_pool()).await
    }

    /// Delete this group without emitting domain events.
    ///
    /// Intended only for internal infrastructure paths such as bootstrap/setup,
    /// fixture cleanup, and event-system tests. Normal application code should
    /// use [`GroupID::delete`] so event subscribers observe the change.
    pub async fn delete_without_events<C>(&self, backend: &C) -> Result<usize, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        self.delete_group_record_without_events(backend.db_pool())
            .await
    }

    pub async fn delete<C>(
        &self,
        backend: &C,
        context: Option<&EventContext>,
    ) -> Result<usize, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        self.delete_group_record(backend.db_pool(), context).await
    }
}

#[derive(
    Serialize, Deserialize, Queryable, Selectable, Insertable, PartialEq, Debug, Clone, ToSchema,
)]
#[diesel(table_name = groups)]
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
        }
    }

    pub async fn from_groups<C>(backend: &C, groups: Vec<Group>) -> Result<Vec<Self>, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        let scope_ids = groups
            .iter()
            .map(|group| group.identity_scope_id)
            .collect::<Vec<_>>();
        let scope_names =
            crate::db::traits::identity::identity_scope_names_by_ids(backend.db_pool(), &scope_ids)
                .await?;

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
    async fn instance_adapter(&self, _pool: &DbPool) -> Result<Group, ApiError> {
        Ok(self.clone())
    }
}

impl Group {
    pub async fn to_response<C>(&self, backend: &C) -> Result<GroupResponse, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        let identity_scope = group_identity_scope_name(backend.db_pool(), self.id).await?;
        Ok(GroupResponse::from_parts(self, identity_scope))
    }

    pub async fn members<C>(&self, backend: &C) -> Result<Vec<Principal>, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        self.load_group_members(backend.db_pool()).await
    }

    pub async fn members_paginated<C>(
        &self,
        backend: &C,
        query_options: &QueryOptions,
    ) -> Result<Vec<Principal>, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        self.load_group_members_paginated(backend.db_pool(), query_options)
            .await
    }

    pub async fn count_members_paginated<C>(
        &self,
        backend: &C,
        query_options: &QueryOptions,
    ) -> Result<i64, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        self.count_group_members_paginated(backend.db_pool(), query_options)
            .await
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
    /// This bypasses event emission and is intended only for internal
    /// infrastructure paths such as bootstrap/setup, fixture construction,
    /// cleanup, and event-system tests. Normal application code should use
    /// [`Group::add_member`] so event subscribers observe the change.
    ///
    /// If the user is already a member of the group, this function is a safe noop.
    pub async fn add_member_without_events<C, P>(
        &self,
        backend: &C,
        member: &P,
    ) -> Result<(), ApiError>
    where
        C: BackendContext + ?Sized,
        P: PrincipalIdAccessor,
    {
        NewPrincipalGroup {
            principal_id: member.principal_id(),
            group_id: self.id,
        }
        .save_principal_group_record_without_events(backend.db_pool())
        .await?;

        Ok(())
    }

    pub async fn add_member<C, P>(
        &self,
        backend: &C,
        member: &P,
        context: Option<&EventContext>,
    ) -> Result<(), ApiError>
    where
        C: BackendContext + ?Sized,
        P: PrincipalIdAccessor,
    {
        NewPrincipalGroup {
            principal_id: member.principal_id(),
            group_id: self.id,
        }
        .save_principal_group_record(backend.db_pool(), context)
        .await?;

        Ok(())
    }

    /// Remove a member from this group without emitting domain events.
    ///
    /// Intended only for internal infrastructure paths such as bootstrap/setup,
    /// fixture cleanup, and event-system tests. Normal application code should
    /// use [`Group::remove_member`] so event subscribers observe the change.
    pub async fn remove_member_without_events<C, P>(
        &self,
        member: &P,
        backend: &C,
    ) -> Result<(), ApiError>
    where
        C: BackendContext + ?Sized,
        P: PrincipalIdAccessor,
    {
        self.remove_group_member_from_backend_without_events(
            member.principal_id(),
            backend.db_pool(),
        )
        .await
    }

    pub async fn remove_member<C, P>(
        &self,
        member: &P,
        backend: &C,
        context: Option<&EventContext>,
    ) -> Result<(), ApiError>
    where
        C: BackendContext + ?Sized,
        P: PrincipalIdAccessor,
    {
        self.remove_group_member_from_backend(member.principal_id(), backend.db_pool(), context)
            .await
    }

    /// Delete this group without emitting domain events.
    ///
    /// Intended only for internal infrastructure paths such as bootstrap/setup,
    /// fixture cleanup, and event-system tests. Normal application code should
    /// use the event-aware delete path so event subscribers observe the change.
    pub async fn delete_without_events<C>(&self, backend: &C) -> Result<usize, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        self.delete_group_record_without_events(backend.db_pool())
            .await
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
    /// Persist without emitting domain events.
    ///
    /// Intended only for internal infrastructure paths such as bootstrap/setup,
    /// fixture construction, cleanup, and event-system tests. Normal application
    /// code should use [`NewGroup::save`] so event subscribers observe the change.
    pub async fn save_without_events<C>(&self, backend: &C) -> Result<Group, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        self.save_group_record_without_events(backend.db_pool())
            .await
    }

    pub async fn save<C>(
        &self,
        backend: &C,
        context: Option<&EventContext>,
    ) -> Result<Group, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        self.save_group_record(backend.db_pool(), context).await
    }
}

#[derive(Deserialize, Serialize, AsChangeset, ToSchema)]
#[schema(example = update_group_example)]
#[diesel(table_name = groups)]
pub struct UpdateGroup {
    pub groupname: Option<String>,
}

impl UpdateGroup {
    pub(crate) fn has_changes(&self, current: &Group) -> bool {
        self.groupname
            .as_ref()
            .is_some_and(|value| value != &current.groupname)
    }
}

impl UpdateGroup {
    /// Persist changes without emitting domain events.
    ///
    /// Intended only for internal infrastructure paths such as bootstrap/setup,
    /// fixture construction, cleanup, and event-system tests. Normal application
    /// code should use [`UpdateGroup::save`] so event subscribers observe the
    /// change.
    pub async fn save_without_events<C>(
        &self,
        group_id: i32,
        backend: &C,
    ) -> Result<Group, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        self.update_group_record_without_events(group_id, backend.db_pool())
            .await
    }

    pub async fn save<C>(
        &self,
        group_id: i32,
        backend: &C,
        context: Option<&EventContext>,
    ) -> Result<Group, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        self.update_group_record(group_id, backend.db_pool(), context)
            .await
    }
}

#[allow(dead_code)]
fn new_group_example() -> NewGroup {
    NewGroup {
        identity_scope: None,
        groupname: "ops".to_string(),
        description: Some("Operations team".to_string()),
    }
}

#[allow(dead_code)]
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

impl CursorSqlMapping for Group {
    fn sql_field(field: &FilterField) -> Result<CursorSqlField, ApiError> {
        Ok(match field {
            FilterField::Id => CursorSqlField {
                column: "groups.id",
                sql_type: CursorSqlType::Integer,
                nullable: false,
            },
            FilterField::Name | FilterField::Groupname => CursorSqlField {
                column: "groups.groupname",
                sql_type: CursorSqlType::String,
                nullable: false,
            },
            FilterField::Description => CursorSqlField {
                column: "groups.description",
                sql_type: CursorSqlType::String,
                nullable: false,
            },
            FilterField::CreatedAt => CursorSqlField {
                column: "groups.created_at",
                sql_type: CursorSqlType::DateTime,
                nullable: false,
            },
            FilterField::UpdatedAt => CursorSqlField {
                column: "groups.updated_at",
                sql_type: CursorSqlType::DateTime,
                nullable: false,
            },
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{}' is not orderable for groups",
                    field
                )));
            }
        })
    }
}
