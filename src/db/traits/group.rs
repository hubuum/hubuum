use diesel::prelude::*;

use crate::db::traits::identity::{identity_scope_by_name, identity_scope_id_by_name_conn};
use crate::db::{DbPool, with_connection, with_transaction};
use crate::errors::ApiError;
use crate::events::{Action, EntityType, EventContext, NewEvent, emit_event};
use crate::models::identity::{
    LOCAL_IDENTITY_SCOPE, LOCAL_PROVIDER_KIND, MANUAL_MEMBERSHIP_SOURCE,
};
use crate::models::search::{FilterField, QueryOptions};
use crate::models::{
    Group, GroupID, NewGroup, NewPrincipalGroup, Principal, PrincipalGroup, UpdateGroup,
};
use crate::{date_search, numeric_search, string_search};

fn group_snapshot(group: &Group) -> serde_json::Value {
    serde_json::json!({
        "id": group.id,
        "identity_scope_id": group.identity_scope_id,
        "groupname": group.groupname,
        "description": group.description,
        "managed_by": group.managed_by,
        "external_key": group.external_key,
        "created_at": group.created_at,
        "updated_at": group.updated_at,
    })
}

fn group_event(
    group: &Group,
    action: Action,
    context: &EventContext,
    summary: impl Into<String>,
) -> Result<NewEvent, ApiError> {
    Ok(
        NewEvent::new(EntityType::Group, action, context.actor_kind(), summary)?
            .with_context(context)
            .with_entity_id(group.id)
            .with_entity_name(group.groupname.clone()),
    )
}

fn user_group_metadata(principal_id: i32, group_id: i32) -> serde_json::Value {
    serde_json::json!({
        "principal_id": principal_id,
        "group_id": group_id,
    })
}

fn insert_effective_membership(
    conn: &mut PgConnection,
    principal: i32,
    group: i32,
) -> Result<PrincipalGroup, diesel::result::Error> {
    use crate::schema::group_memberships::dsl::group_memberships;

    diesel::insert_into(group_memberships)
        .values((
            crate::schema::group_memberships::principal_id.eq(principal),
            crate::schema::group_memberships::group_id.eq(group),
        ))
        .on_conflict_do_nothing()
        .get_result(conn)
        .optional()?
        .map_or_else(|| load_principal_group(conn, principal, group), Ok)
}

fn insert_manual_membership_source(
    conn: &mut PgConnection,
    principal: i32,
    group: i32,
) -> Result<(), ApiError> {
    use crate::schema::group_membership_sources;

    ensure_group_allows_local_write(conn, group)?;
    let local_scope_id = identity_scope_id_by_name_conn(conn, LOCAL_IDENTITY_SCOPE)?;
    diesel::insert_into(group_membership_sources::table)
        .values((
            group_membership_sources::principal_id.eq(principal),
            group_membership_sources::group_id.eq(group),
            group_membership_sources::source.eq(MANUAL_MEMBERSHIP_SOURCE),
            group_membership_sources::source_scope_id.eq(local_scope_id),
            group_membership_sources::source_key.eq(""),
        ))
        .on_conflict_do_nothing()
        .execute(conn)?;
    Ok(())
}

fn remove_manual_membership_source(
    conn: &mut PgConnection,
    principal: i32,
    group: i32,
) -> Result<(), ApiError> {
    use crate::schema::{group_membership_sources, group_memberships};

    ensure_group_allows_local_write(conn, group)?;
    let local_scope_id = identity_scope_id_by_name_conn(conn, LOCAL_IDENTITY_SCOPE)?;
    diesel::delete(
        group_membership_sources::table
            .filter(group_membership_sources::principal_id.eq(principal))
            .filter(group_membership_sources::group_id.eq(group))
            .filter(group_membership_sources::source.eq(MANUAL_MEMBERSHIP_SOURCE))
            .filter(group_membership_sources::source_scope_id.eq(local_scope_id))
            .filter(group_membership_sources::source_key.eq("")),
    )
    .execute(conn)?;

    let remaining = group_membership_sources::table
        .filter(group_membership_sources::principal_id.eq(principal))
        .filter(group_membership_sources::group_id.eq(group))
        .count()
        .get_result::<i64>(conn)?;
    if remaining == 0 {
        diesel::delete(
            group_memberships::table
                .filter(group_memberships::principal_id.eq(principal))
                .filter(group_memberships::group_id.eq(group)),
        )
        .execute(conn)?;
    }
    Ok(())
}

fn ensure_group_allows_local_write(conn: &mut PgConnection, group: i32) -> Result<(), ApiError> {
    use crate::schema::groups::dsl::{groups, id, managed_by};

    let manager = groups
        .filter(id.eq(group))
        .select(managed_by)
        .first::<String>(conn)?;
    if manager != LOCAL_PROVIDER_KIND {
        return Err(ApiError::Forbidden(
            "Provider-managed groups are read-only in Hubuum".to_string(),
        ));
    }
    Ok(())
}

pub trait LoadGroupRecord {
    async fn load_group_record(&self, pool: &DbPool) -> Result<Group, ApiError>;
}

pub fn count_group_records(pool: &DbPool) -> Result<i64, ApiError> {
    use crate::schema::groups::dsl::groups;
    with_connection(pool, |conn| groups.count().get_result::<i64>(conn))
}

pub async fn group_identity_scope_name(
    pool: &DbPool,
    group_id_value: i32,
) -> Result<String, ApiError> {
    use crate::schema::{groups, identity_scopes};
    with_connection(pool, |conn| {
        groups::table
            .inner_join(identity_scopes::table)
            .filter(groups::id.eq(group_id_value))
            .select(identity_scopes::name)
            .first::<String>(conn)
    })
}

impl LoadGroupRecord for GroupID {
    async fn load_group_record(&self, pool: &DbPool) -> Result<Group, ApiError> {
        use crate::schema::groups::dsl::{groups, id};

        with_connection(pool, |conn| {
            groups.filter(id.eq(self.id())).first::<Group>(conn)
        })
    }
}

pub trait DeleteGroupRecord {
    async fn delete_group_record_without_events(&self, pool: &DbPool) -> Result<usize, ApiError>;

    async fn delete_group_record(
        &self,
        pool: &DbPool,
        context: Option<&EventContext>,
    ) -> Result<usize, ApiError> {
        let _ = context;
        self.delete_group_record_without_events(pool).await
    }
}

impl DeleteGroupRecord for GroupID {
    async fn delete_group_record_without_events(&self, pool: &DbPool) -> Result<usize, ApiError> {
        use crate::schema::groups::dsl::{groups, id};

        with_connection(pool, |conn| -> Result<usize, ApiError> {
            ensure_group_allows_local_write(conn, self.id())?;
            Ok(diesel::delete(groups.filter(id.eq(self.id()))).execute(conn)?)
        })
    }

    async fn delete_group_record(
        &self,
        pool: &DbPool,
        context: Option<&EventContext>,
    ) -> Result<usize, ApiError> {
        let Some(context) = context else {
            return self.delete_group_record_without_events(pool).await;
        };

        use crate::schema::groups::dsl::{groups, id};

        with_transaction(pool, |conn| -> Result<usize, ApiError> {
            let group = groups.filter(id.eq(self.id())).first::<Group>(conn)?;
            ensure_group_allows_local_write(conn, group.id)?;
            let deleted = diesel::delete(groups.filter(id.eq(self.id()))).execute(conn)?;
            let event = group_event(
                &group,
                Action::Deleted,
                context,
                format!("Group '{}' deleted", group.groupname),
            )?
            .with_before(group_snapshot(&group));
            emit_event(conn, &event)?;
            Ok(deleted)
        })
    }
}

impl DeleteGroupRecord for Group {
    async fn delete_group_record_without_events(&self, pool: &DbPool) -> Result<usize, ApiError> {
        use crate::schema::groups::dsl::{groups, id};

        with_connection(pool, |conn| -> Result<usize, ApiError> {
            ensure_group_allows_local_write(conn, self.id)?;
            Ok(diesel::delete(groups.filter(id.eq(self.id))).execute(conn)?)
        })
    }

    async fn delete_group_record(
        &self,
        pool: &DbPool,
        context: Option<&EventContext>,
    ) -> Result<usize, ApiError> {
        let Some(context) = context else {
            return self.delete_group_record_without_events(pool).await;
        };

        use crate::schema::groups::dsl::{groups, id};

        with_transaction(pool, |conn| -> Result<usize, ApiError> {
            let before = groups.filter(id.eq(self.id)).first::<Group>(conn)?;
            ensure_group_allows_local_write(conn, before.id)?;
            let deleted = diesel::delete(groups.filter(id.eq(self.id))).execute(conn)?;
            let event = group_event(
                &before,
                Action::Deleted,
                context,
                format!("Group '{}' deleted", before.groupname),
            )?
            .with_before(group_snapshot(&before));
            emit_event(conn, &event)?;
            Ok(deleted)
        })
    }
}

pub trait SaveGroupRecord {
    async fn save_group_record_without_events(&self, pool: &DbPool) -> Result<Group, ApiError>;

    async fn save_group_record(
        &self,
        pool: &DbPool,
        context: Option<&EventContext>,
    ) -> Result<Group, ApiError> {
        let _ = context;
        self.save_group_record_without_events(pool).await
    }
}

impl SaveGroupRecord for NewGroup {
    async fn save_group_record_without_events(&self, pool: &DbPool) -> Result<Group, ApiError> {
        use crate::schema::groups;

        let scope_name = self
            .identity_scope
            .as_deref()
            .unwrap_or(LOCAL_IDENTITY_SCOPE);
        if scope_name != LOCAL_IDENTITY_SCOPE {
            return Err(ApiError::BadRequest(
                "groups in non-local identity scopes are managed by their identity provider"
                    .to_string(),
            ));
        }
        let scope = identity_scope_by_name(pool, scope_name).await?;
        let description = self.description.clone().unwrap_or_default();

        with_connection(pool, |conn| {
            diesel::insert_into(groups::table)
                .values((
                    groups::identity_scope_id.eq(scope.id),
                    groups::groupname.eq(&self.groupname),
                    groups::description.eq(&description),
                    groups::managed_by.eq(LOCAL_PROVIDER_KIND),
                ))
                .get_result::<Group>(conn)
        })
    }

    async fn save_group_record(
        &self,
        pool: &DbPool,
        context: Option<&EventContext>,
    ) -> Result<Group, ApiError> {
        let Some(context) = context else {
            return self.save_group_record_without_events(pool).await;
        };

        use crate::schema::groups;
        let scope_name = self
            .identity_scope
            .as_deref()
            .unwrap_or(LOCAL_IDENTITY_SCOPE);
        if scope_name != LOCAL_IDENTITY_SCOPE {
            return Err(ApiError::BadRequest(
                "groups in non-local identity scopes are managed by their identity provider"
                    .to_string(),
            ));
        }
        let scope = identity_scope_by_name(pool, scope_name).await?;
        let description = self.description.clone().unwrap_or_default();

        with_transaction(pool, |conn| -> Result<Group, ApiError> {
            let group = diesel::insert_into(groups::table)
                .values((
                    groups::identity_scope_id.eq(scope.id),
                    groups::groupname.eq(&self.groupname),
                    groups::description.eq(&description),
                    groups::managed_by.eq(LOCAL_PROVIDER_KIND),
                ))
                .get_result::<Group>(conn)?;
            let event = group_event(
                &group,
                Action::Created,
                context,
                format!("Group '{}' created", group.groupname),
            )?
            .with_after(group_snapshot(&group));
            emit_event(conn, &event)?;
            Ok(group)
        })
    }
}

pub trait UpdateGroupRecord {
    async fn update_group_record_without_events(
        &self,
        group_id: i32,
        pool: &DbPool,
    ) -> Result<Group, ApiError>;

    async fn update_group_record(
        &self,
        group_id: i32,
        pool: &DbPool,
        context: Option<&EventContext>,
    ) -> Result<Group, ApiError> {
        let _ = context;
        self.update_group_record_without_events(group_id, pool)
            .await
    }
}

impl UpdateGroupRecord for UpdateGroup {
    async fn update_group_record_without_events(
        &self,
        group_id: i32,
        pool: &DbPool,
    ) -> Result<Group, ApiError> {
        use crate::schema::groups::dsl::{groups, id};

        with_connection(pool, |conn| -> Result<Group, ApiError> {
            ensure_group_allows_local_write(conn, group_id)?;
            Ok(diesel::update(groups.filter(id.eq(group_id)))
                .set(self)
                .get_result::<Group>(conn)?)
        })
    }

    async fn update_group_record(
        &self,
        group_id: i32,
        pool: &DbPool,
        context: Option<&EventContext>,
    ) -> Result<Group, ApiError> {
        let Some(context) = context else {
            return self
                .update_group_record_without_events(group_id, pool)
                .await;
        };

        use crate::schema::groups::dsl::{groups, id};

        with_transaction(pool, |conn| -> Result<Group, ApiError> {
            let before = groups.filter(id.eq(group_id)).first::<Group>(conn)?;
            ensure_group_allows_local_write(conn, before.id)?;
            let after = diesel::update(groups.filter(id.eq(group_id)))
                .set(self)
                .get_result::<Group>(conn)?;
            let event = group_event(
                &after,
                Action::Updated,
                context,
                format!("Group '{}' updated", after.groupname),
            )?
            .with_before(group_snapshot(&before))
            .with_after(group_snapshot(&after));
            emit_event(conn, &event)?;
            Ok(after)
        })
    }
}

/// Group membership is principal-centric: members are `Principal`s, which may be
/// human users or service accounts. Member listings expose the principal name
/// and kind via the principals table.
pub trait GroupMembersBackend {
    async fn load_group_members(&self, pool: &DbPool) -> Result<Vec<Principal>, ApiError>;

    async fn load_group_members_paginated(
        &self,
        pool: &DbPool,
        query_options: &QueryOptions,
    ) -> Result<Vec<Principal>, ApiError>;

    async fn count_group_members_paginated(
        &self,
        pool: &DbPool,
        query_options: &QueryOptions,
    ) -> Result<i64, ApiError>;

    async fn remove_group_member_from_backend_without_events(
        &self,
        member_principal_id: i32,
        pool: &DbPool,
    ) -> Result<(), ApiError>;

    async fn remove_group_member_from_backend(
        &self,
        member_principal_id: i32,
        pool: &DbPool,
        context: Option<&EventContext>,
    ) -> Result<(), ApiError> {
        let _ = context;
        self.remove_group_member_from_backend_without_events(member_principal_id, pool)
            .await
    }
}

impl GroupMembersBackend for Group {
    async fn load_group_members(&self, pool: &DbPool) -> Result<Vec<Principal>, ApiError> {
        use crate::schema::group_memberships::dsl::{group_id, group_memberships};
        use crate::schema::principals::dsl::principals;

        with_connection(pool, |conn| {
            group_memberships
                .filter(group_id.eq(self.id))
                .inner_join(principals)
                .select(crate::schema::principals::all_columns)
                .load::<Principal>(conn)
        })
    }

    async fn load_group_members_paginated(
        &self,
        pool: &DbPool,
        query_options: &QueryOptions,
    ) -> Result<Vec<Principal>, ApiError> {
        use crate::schema::group_memberships::dsl::{group_id, group_memberships};
        use crate::schema::principals::dsl::{created_at, id, name, principals, updated_at};

        let mut base_query = group_memberships
            .filter(group_id.eq(self.id))
            .inner_join(principals)
            .select(crate::schema::principals::all_columns)
            .into_boxed();

        for param in &query_options.filters {
            let operator = param.operator.clone();
            match param.field {
                FilterField::Id => numeric_search!(base_query, param, operator, id),
                FilterField::Name | FilterField::Username => {
                    string_search!(base_query, param, operator, name)
                }
                FilterField::CreatedAt => date_search!(base_query, param, operator, created_at),
                FilterField::UpdatedAt => date_search!(base_query, param, operator, updated_at),
                _ => {
                    return Err(ApiError::BadRequest(format!(
                        "Field '{}' isn't searchable (or does not exist) for principals",
                        param.field
                    )));
                }
            }
        }

        crate::apply_query_options!(base_query, query_options, Principal);

        with_connection(pool, |conn| base_query.load::<Principal>(conn))
    }

    async fn count_group_members_paginated(
        &self,
        pool: &DbPool,
        query_options: &QueryOptions,
    ) -> Result<i64, ApiError> {
        use crate::schema::group_memberships::dsl::{group_id, group_memberships};
        use crate::schema::principals::dsl::{created_at, id, name, principals, updated_at};

        let mut base_query = group_memberships
            .filter(group_id.eq(self.id))
            .inner_join(principals)
            .into_boxed();

        for param in &query_options.filters {
            let operator = param.operator.clone();
            match param.field {
                FilterField::Id => numeric_search!(base_query, param, operator, id),
                FilterField::Name | FilterField::Username => {
                    string_search!(base_query, param, operator, name)
                }
                FilterField::CreatedAt => date_search!(base_query, param, operator, created_at),
                FilterField::UpdatedAt => date_search!(base_query, param, operator, updated_at),
                _ => {
                    return Err(ApiError::BadRequest(format!(
                        "Field '{}' isn't searchable (or does not exist) for principals",
                        param.field
                    )));
                }
            }
        }

        with_connection(pool, |conn| base_query.count().get_result::<i64>(conn))
    }

    async fn remove_group_member_from_backend_without_events(
        &self,
        member_principal_id: i32,
        pool: &DbPool,
    ) -> Result<(), ApiError> {
        with_transaction(pool, |conn| -> Result<(), ApiError> {
            remove_manual_membership_source(conn, member_principal_id, self.id)?;
            Ok(())
        })?;
        Ok(())
    }

    async fn remove_group_member_from_backend(
        &self,
        member_principal_id: i32,
        pool: &DbPool,
        context: Option<&EventContext>,
    ) -> Result<(), ApiError> {
        let Some(context) = context else {
            return self
                .remove_group_member_from_backend_without_events(member_principal_id, pool)
                .await;
        };

        with_transaction(pool, |conn| -> Result<(), ApiError> {
            let membership = load_principal_group(conn, member_principal_id, self.id).optional()?;
            remove_manual_membership_source(conn, member_principal_id, self.id)?;

            if let Some(membership) = membership {
                let event = NewEvent::new(
                    EntityType::UserGroup,
                    Action::Removed,
                    context.actor_kind(),
                    format!(
                        "Principal {} removed from group {}",
                        membership.principal_id, membership.group_id
                    ),
                )?
                .with_context(context)
                .with_metadata(user_group_metadata(
                    membership.principal_id,
                    membership.group_id,
                ));
                emit_event(conn, &event)?;
            }

            Ok(())
        })
    }
}

pub trait SavePrincipalGroupRecord {
    async fn save_principal_group_record_without_events(
        &self,
        pool: &DbPool,
    ) -> Result<PrincipalGroup, ApiError>;

    async fn save_principal_group_record(
        &self,
        pool: &DbPool,
        context: Option<&EventContext>,
    ) -> Result<PrincipalGroup, ApiError> {
        let _ = context;
        self.save_principal_group_record_without_events(pool).await
    }
}

impl SavePrincipalGroupRecord for NewPrincipalGroup {
    async fn save_principal_group_record_without_events(
        &self,
        pool: &DbPool,
    ) -> Result<PrincipalGroup, ApiError> {
        with_transaction(pool, |conn| -> Result<PrincipalGroup, ApiError> {
            let membership = insert_effective_membership(conn, self.principal_id, self.group_id)?;
            insert_manual_membership_source(conn, self.principal_id, self.group_id)?;
            Ok(membership)
        })
    }

    async fn save_principal_group_record(
        &self,
        pool: &DbPool,
        context: Option<&EventContext>,
    ) -> Result<PrincipalGroup, ApiError> {
        let Some(context) = context else {
            return self.save_principal_group_record_without_events(pool).await;
        };

        with_transaction(pool, |conn| -> Result<PrincipalGroup, ApiError> {
            let already_present =
                load_principal_group(conn, self.principal_id, self.group_id).optional()?;
            let membership = insert_effective_membership(conn, self.principal_id, self.group_id)?;
            insert_manual_membership_source(conn, self.principal_id, self.group_id)?;
            match already_present {
                None => {
                    let event = NewEvent::new(
                        EntityType::UserGroup,
                        Action::Added,
                        context.actor_kind(),
                        format!(
                            "Principal {} added to group {}",
                            membership.principal_id, membership.group_id
                        ),
                    )?
                    .with_context(context)
                    .with_metadata(user_group_metadata(
                        membership.principal_id,
                        membership.group_id,
                    ));
                    emit_event(conn, &event)?;
                    Ok(membership)
                }
                Some(_) => Ok(membership),
            }
        })
    }
}

impl SavePrincipalGroupRecord for PrincipalGroup {
    async fn save_principal_group_record_without_events(
        &self,
        pool: &DbPool,
    ) -> Result<PrincipalGroup, ApiError> {
        with_transaction(pool, |conn| -> Result<PrincipalGroup, ApiError> {
            let membership = insert_effective_membership(conn, self.principal_id, self.group_id)?;
            insert_manual_membership_source(conn, self.principal_id, self.group_id)?;
            Ok(membership)
        })
    }

    async fn save_principal_group_record(
        &self,
        pool: &DbPool,
        context: Option<&EventContext>,
    ) -> Result<PrincipalGroup, ApiError> {
        let Some(context) = context else {
            return self.save_principal_group_record_without_events(pool).await;
        };

        with_transaction(pool, |conn| -> Result<PrincipalGroup, ApiError> {
            let membership = insert_effective_membership(conn, self.principal_id, self.group_id)?;
            insert_manual_membership_source(conn, self.principal_id, self.group_id)?;
            let event = NewEvent::new(
                EntityType::UserGroup,
                Action::Added,
                context.actor_kind(),
                format!(
                    "Principal {} added to group {}",
                    membership.principal_id, membership.group_id
                ),
            )?
            .with_context(context)
            .with_metadata(user_group_metadata(
                membership.principal_id,
                membership.group_id,
            ));
            emit_event(conn, &event)?;
            Ok(membership)
        })
    }
}

fn load_principal_group(
    conn: &mut PgConnection,
    principal: i32,
    group: i32,
) -> Result<PrincipalGroup, diesel::result::Error> {
    use crate::schema::group_memberships::dsl::{group_id, group_memberships, principal_id};
    group_memberships
        .filter(principal_id.eq(principal))
        .filter(group_id.eq(group))
        .first::<PrincipalGroup>(conn)
}

pub trait DeletePrincipalGroupRecord {
    async fn delete_principal_group_record(&self, pool: &DbPool) -> Result<(), ApiError>;
}

impl DeletePrincipalGroupRecord for PrincipalGroup {
    async fn delete_principal_group_record(&self, pool: &DbPool) -> Result<(), ApiError> {
        with_transaction(pool, |conn| -> Result<(), ApiError> {
            remove_manual_membership_source(conn, self.principal_id, self.group_id)
        })?;
        Ok(())
    }
}

pub trait PrincipalGroupPrincipalLookup {
    async fn load_principal_group_principal(&self, pool: &DbPool) -> Result<Principal, ApiError>;
}

impl PrincipalGroupPrincipalLookup for PrincipalGroup {
    async fn load_principal_group_principal(&self, pool: &DbPool) -> Result<Principal, ApiError> {
        use crate::schema::principals::dsl::{id, principals};

        with_connection(pool, |conn| {
            principals
                .filter(id.eq(self.principal_id))
                .first::<Principal>(conn)
        })
    }
}

pub trait PrincipalGroupGroupLookup {
    async fn load_principal_group_group(&self, pool: &DbPool) -> Result<Group, ApiError>;
}

impl PrincipalGroupGroupLookup for PrincipalGroup {
    async fn load_principal_group_group(&self, pool: &DbPool) -> Result<Group, ApiError> {
        use crate::schema::groups::dsl::{groups, id};

        with_connection(pool, |conn| {
            groups.filter(id.eq(self.group_id)).first::<Group>(conn)
        })
    }
}
