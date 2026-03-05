use diesel::prelude::*;

use crate::db::{DbPool, with_connection};
use crate::errors::ApiError;
use crate::models::search::{FilterField, QueryOptions};
use crate::models::{Group, GroupID, NewGroup, NewUserGroup, UpdateGroup, User, UserGroup};
use crate::{date_search, numeric_search, string_search};

pub trait LoadGroupRecord {
    async fn load_group_record(&self, pool: &DbPool) -> Result<Group, ApiError>;
}

impl LoadGroupRecord for GroupID {
    async fn load_group_record(&self, pool: &DbPool) -> Result<Group, ApiError> {
        use crate::schema::groups::dsl::{groups, id};

        with_connection(pool, |conn| {
            groups.filter(id.eq(self.0)).first::<Group>(conn)
        })
    }
}

pub trait DeleteGroupRecord {
    async fn delete_group_record(&self, pool: &DbPool) -> Result<usize, ApiError>;
}

impl DeleteGroupRecord for GroupID {
    async fn delete_group_record(&self, pool: &DbPool) -> Result<usize, ApiError> {
        use crate::schema::groups::dsl::{groups, id};

        with_connection(pool, |conn| {
            diesel::delete(groups.filter(id.eq(self.0))).execute(conn)
        })
    }
}

impl DeleteGroupRecord for Group {
    async fn delete_group_record(&self, pool: &DbPool) -> Result<usize, ApiError> {
        use crate::schema::groups::dsl::{groups, id};

        with_connection(pool, |conn| {
            diesel::delete(groups.filter(id.eq(self.id))).execute(conn)
        })
    }
}

pub trait SaveGroupRecord {
    async fn save_group_record(&self, pool: &DbPool) -> Result<Group, ApiError>;
}

impl SaveGroupRecord for NewGroup {
    async fn save_group_record(&self, pool: &DbPool) -> Result<Group, ApiError> {
        use crate::schema::groups::dsl::groups;

        with_connection(pool, |conn| {
            diesel::insert_into(groups)
                .values(self)
                .get_result::<Group>(conn)
        })
    }
}

pub trait UpdateGroupRecord {
    async fn update_group_record(&self, group_id: i32, pool: &DbPool) -> Result<Group, ApiError>;
}

impl UpdateGroupRecord for UpdateGroup {
    async fn update_group_record(&self, group_id: i32, pool: &DbPool) -> Result<Group, ApiError> {
        use crate::schema::groups::dsl::{groups, id};

        with_connection(pool, |conn| {
            diesel::update(groups.filter(id.eq(group_id)))
                .set(self)
                .get_result::<Group>(conn)
        })
    }
}

pub trait GroupMembersBackend {
    async fn load_group_members(&self, pool: &DbPool) -> Result<Vec<User>, ApiError>;

    async fn load_group_members_paginated(
        &self,
        pool: &DbPool,
        query_options: &QueryOptions,
    ) -> Result<Vec<User>, ApiError>;

    async fn remove_group_member_from_backend(
        &self,
        user: &User,
        pool: &DbPool,
    ) -> Result<(), ApiError>;
}

impl GroupMembersBackend for Group {
    async fn load_group_members(&self, pool: &DbPool) -> Result<Vec<User>, ApiError> {
        use crate::schema::user_groups::dsl::{group_id, user_groups, user_id};
        use crate::schema::users::dsl::*;

        with_connection(pool, |conn| {
            user_groups
                .filter(group_id.eq(self.id))
                .inner_join(users.on(id.eq(user_id)))
                .select((id, username, password, email, created_at, updated_at))
                .load::<User>(conn)
        })
    }

    async fn load_group_members_paginated(
        &self,
        pool: &DbPool,
        query_options: &QueryOptions,
    ) -> Result<Vec<User>, ApiError> {
        use crate::schema::user_groups::dsl::{group_id, user_groups, user_id};
        use crate::schema::users::dsl::{
            created_at, email, id, password, updated_at, username, users,
        };

        let mut base_query = user_groups
            .filter(group_id.eq(self.id))
            .inner_join(users.on(id.eq(user_id)))
            .select((id, username, password, email, created_at, updated_at))
            .into_boxed();

        for param in &query_options.filters {
            let operator = param.operator.clone();
            match param.field {
                FilterField::Id => numeric_search!(base_query, param, operator, id),
                FilterField::Name | FilterField::Username => {
                    string_search!(base_query, param, operator, username)
                }
                FilterField::Email => string_search!(base_query, param, operator, email),
                FilterField::CreatedAt => date_search!(base_query, param, operator, created_at),
                FilterField::UpdatedAt => date_search!(base_query, param, operator, updated_at),
                _ => {
                    return Err(ApiError::BadRequest(format!(
                        "Field '{}' isn't searchable (or does not exist) for users",
                        param.field
                    )));
                }
            }
        }

        crate::apply_query_options!(base_query, query_options, User);

        with_connection(pool, |conn| base_query.load::<User>(conn))
    }

    async fn remove_group_member_from_backend(
        &self,
        user: &User,
        pool: &DbPool,
    ) -> Result<(), ApiError> {
        use crate::schema::user_groups::dsl::*;

        with_connection(pool, |conn| {
            diesel::delete(user_groups.filter(user_id.eq(user.id))).execute(conn)
        })?;
        Ok(())
    }
}

pub trait SaveUserGroupRecord {
    async fn save_user_group_record(&self, pool: &DbPool) -> Result<UserGroup, ApiError>;
}

impl SaveUserGroupRecord for NewUserGroup {
    async fn save_user_group_record(&self, pool: &DbPool) -> Result<UserGroup, ApiError> {
        use crate::schema::user_groups::dsl::user_groups;

        with_connection(pool, |conn| {
            diesel::insert_into(user_groups)
                .values(self)
                .get_result(conn)
        })
    }
}

impl SaveUserGroupRecord for UserGroup {
    async fn save_user_group_record(&self, pool: &DbPool) -> Result<UserGroup, ApiError> {
        use crate::schema::user_groups::dsl::user_groups;

        with_connection(pool, |conn| {
            diesel::insert_into(user_groups)
                .values(self)
                .get_result(conn)
        })
    }
}

pub trait DeleteUserGroupRecord {
    async fn delete_user_group_record(&self, pool: &DbPool) -> Result<(), ApiError>;
}

impl DeleteUserGroupRecord for UserGroup {
    async fn delete_user_group_record(&self, pool: &DbPool) -> Result<(), ApiError> {
        use crate::schema::user_groups::dsl::*;

        with_connection(pool, |conn| {
            diesel::delete(
                user_groups
                    .filter(user_id.eq(self.user_id))
                    .filter(group_id.eq(self.group_id)),
            )
            .execute(conn)
        })?;
        Ok(())
    }
}

pub trait UserGroupUserLookup {
    async fn load_user_group_user(&self, pool: &DbPool) -> Result<User, ApiError>;
}

impl UserGroupUserLookup for UserGroup {
    async fn load_user_group_user(&self, pool: &DbPool) -> Result<User, ApiError> {
        use crate::schema::users::dsl::{id, users};

        with_connection(pool, |conn| {
            users.filter(id.eq(self.user_id)).first::<User>(conn)
        })
    }
}

pub trait UserGroupGroupLookup {
    async fn load_user_group_group(&self, pool: &DbPool) -> Result<Group, ApiError>;
}

impl UserGroupGroupLookup for UserGroup {
    async fn load_user_group_group(&self, pool: &DbPool) -> Result<Group, ApiError> {
        use crate::schema::groups::dsl::{groups, id};

        with_connection(pool, |conn| {
            groups.filter(id.eq(self.group_id)).first::<Group>(conn)
        })
    }
}
