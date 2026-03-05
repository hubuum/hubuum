// src/models/group.rs

use crate::db::traits::group::{
    DeleteGroupRecord, GroupMembersBackend, LoadGroupRecord, SaveGroupRecord, UpdateGroupRecord,
};
use crate::errors::ApiError;
use crate::models::search::{FilterField, QueryOptions, SortParam};
use crate::models::user_group::NewUserGroup;
use crate::schema::groups;

use crate::models::user::User;
use diesel::prelude::*;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::traits::accessors::{IdAccessor, InstanceAdapter};
use crate::traits::{
    BackendContext, CanSave, CursorPaginated, CursorSqlField, CursorSqlMapping, CursorSqlType,
    CursorValue,
};

use crate::db::DbPool;

#[derive(Serialize, Deserialize, ToSchema)]
pub struct GroupID(pub i32);

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

    pub async fn delete<C>(&self, backend: &C) -> Result<usize, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        self.delete_group_record(backend.db_pool()).await
    }
}

#[derive(Serialize, Deserialize, Queryable, Insertable, PartialEq, Debug, Clone, ToSchema)]
#[diesel(table_name = groups)]
pub struct Group {
    pub id: i32,
    pub groupname: String,
    pub description: String,
    pub created_at: chrono::NaiveDateTime,
    pub updated_at: chrono::NaiveDateTime,
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
    pub async fn members<C>(&self, backend: &C) -> Result<Vec<User>, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        self.load_group_members(backend.db_pool()).await
    }

    pub async fn members_paginated<C>(
        &self,
        backend: &C,
        query_options: &QueryOptions,
    ) -> Result<Vec<User>, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        self.load_group_members_paginated(backend.db_pool(), query_options)
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
    /// If the user is already a member of the group, this function is a safe noop.
    pub async fn add_member<C>(&self, backend: &C, user: &User) -> Result<(), ApiError>
    where
        C: BackendContext + ?Sized,
    {
        NewUserGroup {
            user_id: user.id,
            group_id: self.id,
        }
        .save(backend)
        .await?;

        Ok(())
    }

    pub async fn remove_member<C>(&self, user: &User, backend: &C) -> Result<(), ApiError>
    where
        C: BackendContext + ?Sized,
    {
        self.remove_group_member_from_backend(user, backend.db_pool())
            .await
    }

    pub async fn delete<C>(&self, backend: &C) -> Result<usize, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        self.delete_group_record(backend.db_pool()).await
    }
}

#[derive(Deserialize, Serialize, Insertable, Debug, ToSchema)]
#[schema(example = new_group_example)]
#[diesel(table_name = groups)]
pub struct NewGroup {
    pub groupname: String,
    pub description: Option<String>,
}

impl NewGroup {
    pub async fn new(groupname: &str, description: Option<&str>) -> Self {
        NewGroup {
            groupname: groupname.to_string(),
            description: description.map(|s| s.to_string()),
        }
    }

    pub async fn save<C>(&self, backend: &C) -> Result<Group, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        self.save_group_record(backend.db_pool()).await
    }
}

#[derive(Deserialize, Serialize, AsChangeset, ToSchema)]
#[schema(example = update_group_example)]
#[diesel(table_name = groups)]
pub struct UpdateGroup {
    pub groupname: Option<String>,
}

impl UpdateGroup {
    pub async fn save<C>(&self, group_id: i32, backend: &C) -> Result<Group, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        self.update_group_record(group_id, backend.db_pool()).await
    }
}

#[allow(dead_code)]
fn new_group_example() -> NewGroup {
    NewGroup {
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
