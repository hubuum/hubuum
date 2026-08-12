use crate::pagination::{CursorSqlField, CursorSqlMapping, CursorSqlType};
use crate::storage::postgres::prelude::*;

use crate::api::etag::RevisionOwner;
use crate::errors::ApiError;
use crate::events::{Action, EntityType, EventContext, NewEvent};
use crate::models::identity::{
    LOCAL_IDENTITY_SCOPE, LOCAL_PROVIDER_KIND, MANUAL_MEMBERSHIP_SOURCE,
};
use crate::models::search::{FilterField, QueryOptions, SortParam};
use crate::models::{Group, GroupID, NewGroup, Principal, PrincipalGroup, UpdateGroup};
use crate::storage::postgres::operations::event_record::emit_event;
use crate::storage::postgres::operations::identity::{
    identity_scope_by_name, identity_scope_id_by_name_conn,
};
use crate::storage::postgres::operations::principal::{PrincipalMemberQueryRow, PrincipalRow};
use crate::storage::postgres::{PostgresConnection, with_connection, with_transaction};
use crate::traits::{CursorPaginated, CursorValue};
use crate::{date_search, numeric_search, string_search};

const OWNED_SERVICE_ACCOUNT_PREVIEW_LIMIT: i64 = 10;

#[derive(Debug, Queryable, Selectable, Clone)]
#[diesel(table_name = crate::schema::groups)]
pub(crate) struct GroupRow {
    pub(crate) id: i32,
    pub(crate) groupname: String,
    pub(crate) description: String,
    pub(crate) created_at: chrono::NaiveDateTime,
    pub(crate) updated_at: chrono::NaiveDateTime,
    pub(crate) identity_scope_id: i32,
    pub(crate) managed_by: String,
    pub(crate) external_key: Option<String>,
    pub(crate) last_sync_attempted_at: Option<chrono::NaiveDateTime>,
    pub(crate) last_sync_success_at: Option<chrono::NaiveDateTime>,
    pub(crate) revision: crate::models::ResourceRevision,
}

impl From<GroupRow> for Group {
    fn from(row: GroupRow) -> Self {
        Self {
            id: row.id,
            groupname: row.groupname,
            description: row.description,
            created_at: row.created_at,
            updated_at: row.updated_at,
            identity_scope_id: row.identity_scope_id,
            managed_by: row.managed_by,
            external_key: row.external_key,
            last_sync_attempted_at: row.last_sync_attempted_at,
            last_sync_success_at: row.last_sync_success_at,
            revision: row.revision,
        }
    }
}

#[derive(Debug, Queryable, Selectable, Clone)]
#[diesel(table_name = crate::schema::group_memberships)]
pub(crate) struct PrincipalGroupRow {
    pub(crate) principal_id: i32,
    pub(crate) group_id: i32,
    pub(crate) created_at: chrono::NaiveDateTime,
    pub(crate) updated_at: chrono::NaiveDateTime,
    pub(crate) revision: crate::models::ResourceRevision,
}

impl From<PrincipalGroupRow> for PrincipalGroup {
    fn from(row: PrincipalGroupRow) -> Self {
        Self {
            principal_id: row.principal_id,
            group_id: row.group_id,
            created_at: row.created_at,
            updated_at: row.updated_at,
            revision: row.revision,
        }
    }
}

impl CursorPaginated for GroupRow {
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

impl CursorSqlMapping for GroupRow {
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
            FilterField::Revision => CursorSqlField {
                column: "groups.revision",
                sql_type: CursorSqlType::BigInt,
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

#[derive(AsChangeset)]
#[diesel(table_name = crate::schema::groups)]
struct UpdateGroupRow<'a> {
    groupname: Option<&'a str>,
}

impl<'a> From<&'a UpdateGroup> for UpdateGroupRow<'a> {
    fn from(update: &'a UpdateGroup) -> Self {
        Self {
            groupname: update.groupname.as_deref(),
        }
    }
}

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
        "revision": group.revision,
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

fn membership_snapshot(membership: &PrincipalGroup) -> serde_json::Value {
    serde_json::json!({
        "principal_id": membership.principal_id,
        "group_id": membership.group_id,
        "created_at": membership.created_at,
        "updated_at": membership.updated_at,
        "revision": membership.revision,
    })
}

async fn insert_effective_membership(
    conn: &mut PostgresConnection,
    principal: i32,
    group: i32,
) -> Result<bool, ApiError> {
    use crate::schema::group_memberships::dsl::{group_id, group_memberships, principal_id};

    let owner_key = RevisionOwner::membership_key(principal, group);
    let current = group_memberships
        .filter(principal_id.eq(principal))
        .filter(group_id.eq(group))
        .for_update()
        .first::<PrincipalGroupRow>(conn)
        .await
        .optional()?;
    if let Some(membership) = current.map(PrincipalGroup::from) {
        crate::storage::postgres::assert_locked_revision_precondition(
            conn,
            &owner_key,
            membership.revision,
        )
        .await?;
        return Ok(false);
    }

    crate::storage::postgres::assert_revision_precondition_allows_missing_target(&owner_key)?;

    let inserted = diesel::insert_into(group_memberships)
        .values((
            crate::schema::group_memberships::principal_id.eq(principal),
            crate::schema::group_memberships::group_id.eq(group),
        ))
        .on_conflict_do_nothing()
        .get_result::<PrincipalGroupRow>(conn)
        .await
        .optional()?;
    match inserted {
        Some(_) => Ok(true),
        None => {
            load_principal_group(conn, principal, group).await?;
            Ok(false)
        }
    }
}

struct ManualMembershipInsert {
    membership: PrincipalGroup,
    effective_membership_created: bool,
}

async fn insert_manual_membership_source(
    conn: &mut PostgresConnection,
    principal: i32,
    group: i32,
) -> Result<(), ApiError> {
    use crate::schema::group_membership_sources;

    ensure_group_allows_local_write(conn, group).await?;
    let local_scope_id = identity_scope_id_by_name_conn(conn, LOCAL_IDENTITY_SCOPE).await?;
    diesel::insert_into(group_membership_sources::table)
        .values((
            group_membership_sources::principal_id.eq(principal),
            group_membership_sources::group_id.eq(group),
            group_membership_sources::source.eq(MANUAL_MEMBERSHIP_SOURCE),
            group_membership_sources::source_scope_id.eq(local_scope_id),
            group_membership_sources::source_key.eq(""),
        ))
        .on_conflict_do_nothing()
        .execute(conn)
        .await?;
    Ok(())
}

async fn insert_manual_membership(
    conn: &mut PostgresConnection,
    principal: i32,
    group: i32,
) -> Result<ManualMembershipInsert, ApiError> {
    let effective_membership_created = insert_effective_membership(conn, principal, group).await?;
    insert_manual_membership_source(conn, principal, group).await?;
    // The source row owns the effective membership revision and may have
    // advanced it. Always return the final committed representation.
    Ok(ManualMembershipInsert {
        membership: load_principal_group(conn, principal, group).await?,
        effective_membership_created,
    })
}

async fn remove_manual_membership_source(
    conn: &mut PostgresConnection,
    principal: i32,
    group: i32,
) -> Result<Option<PrincipalGroup>, ApiError> {
    use crate::schema::{group_membership_sources, group_memberships};

    ensure_group_allows_local_write(conn, group).await?;
    let owner_key = RevisionOwner::membership_key(principal, group);
    let membership = group_memberships::table
        .filter(group_memberships::principal_id.eq(principal))
        .filter(group_memberships::group_id.eq(group))
        .for_update()
        .first::<PrincipalGroupRow>(conn)
        .await
        .optional()?
        .map(PrincipalGroup::from);
    let Some(membership) = membership else {
        crate::storage::postgres::assert_revision_precondition_allows_missing_target(&owner_key)?;
        return Ok(None);
    };
    crate::storage::postgres::assert_locked_revision_precondition(
        conn,
        &owner_key,
        membership.revision,
    )
    .await?;

    let local_scope_id = identity_scope_id_by_name_conn(conn, LOCAL_IDENTITY_SCOPE).await?;
    diesel::delete(
        group_membership_sources::table
            .filter(group_membership_sources::principal_id.eq(principal))
            .filter(group_membership_sources::group_id.eq(group))
            .filter(group_membership_sources::source.eq(MANUAL_MEMBERSHIP_SOURCE))
            .filter(group_membership_sources::source_scope_id.eq(local_scope_id))
            .filter(group_membership_sources::source_key.eq("")),
    )
    .execute(conn)
    .await?;

    let remaining = group_membership_sources::table
        .filter(group_membership_sources::principal_id.eq(principal))
        .filter(group_membership_sources::group_id.eq(group))
        .count()
        .get_result::<i64>(conn)
        .await?;
    if remaining == 0 {
        let deleted = diesel::delete(
            group_memberships::table
                .filter(group_memberships::principal_id.eq(principal))
                .filter(group_memberships::group_id.eq(group)),
        )
        .execute(conn)
        .await?;
        return Ok((deleted > 0).then_some(membership));
    }
    Ok(None)
}

async fn ensure_group_allows_local_write(
    conn: &mut PostgresConnection,
    group: i32,
) -> Result<(), ApiError> {
    use crate::schema::groups::dsl::{groups, id, managed_by};

    let manager = groups
        .filter(id.eq(group))
        .select(managed_by)
        .first::<String>(conn)
        .await?;
    if manager != LOCAL_PROVIDER_KIND {
        return Err(ApiError::Forbidden(
            "Provider-managed groups are read-only in Hubuum".to_string(),
        ));
    }
    Ok(())
}

async fn ensure_group_has_no_owned_service_accounts(
    conn: &mut PostgresConnection,
    group_id: i32,
) -> Result<(), ApiError> {
    use crate::schema::{principals, service_accounts};

    let mut owned = service_accounts::table
        .inner_join(principals::table.on(principals::id.eq(service_accounts::id)))
        .filter(service_accounts::owner_group_id.eq(group_id))
        .select((service_accounts::id, principals::name))
        .order(service_accounts::id.asc())
        .limit(OWNED_SERVICE_ACCOUNT_PREVIEW_LIMIT + 1)
        .load::<(i32, String)>(conn)
        .await?;
    if owned.is_empty() {
        return Ok(());
    }

    let additional_accounts_omitted = owned.len() > OWNED_SERVICE_ACCOUNT_PREVIEW_LIMIT as usize;
    owned.truncate(OWNED_SERVICE_ACCOUNT_PREVIEW_LIMIT as usize);
    let list = owned
        .iter()
        .map(|(id, name)| format!("{name} (id {id})"))
        .collect::<Vec<_>>()
        .join(", ");
    let suffix = if additional_accounts_omitted {
        "; additional service accounts omitted"
    } else {
        ""
    };
    Err(ApiError::Conflict(format!(
        "Group owns service accounts; reassign or delete them first: {list}{suffix}"
    )))
}

async fn lock_group_for_delete(
    conn: &mut PostgresConnection,
    group_id: i32,
) -> Result<Group, ApiError> {
    use crate::schema::groups::dsl::{groups, id};

    let group: Group = groups
        .filter(id.eq(group_id))
        .for_update()
        .first::<GroupRow>(conn)
        .await?
        .into();
    crate::storage::postgres::assert_locked_revision_precondition(
        conn,
        &RevisionOwner::Group.key(group.id),
        group.revision,
    )
    .await?;
    ensure_group_has_no_owned_service_accounts(conn, group.id).await?;
    ensure_group_allows_local_write(conn, group.id).await?;
    Ok(group)
}

async fn delete_group_by_id(
    pool: &crate::storage::postgres::PostgresPool,
    group_id: i32,
    context: Option<&EventContext>,
) -> Result<usize, ApiError> {
    use crate::schema::groups::dsl::{groups, id};

    with_transaction(pool, async |conn| -> Result<usize, ApiError> {
        let group = lock_group_for_delete(conn, group_id).await?;
        let deleted = diesel::delete(groups.filter(id.eq(group.id)))
            .execute(conn)
            .await?;

        if let Some(context) = context {
            let event = group_event(
                &group,
                Action::Deleted,
                context,
                format!("Group '{}' deleted", group.groupname),
            )?
            .with_before(group_snapshot(&group));
            emit_event(conn, &event).await?;
        }

        Ok(deleted)
    })
    .await
}

pub trait LoadGroupRecord {
    async fn load_group_record(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
    ) -> Result<Group, ApiError>;
}

pub async fn count_group_records(
    pool: &crate::storage::postgres::PostgresPool,
) -> Result<i64, ApiError> {
    use crate::schema::groups::dsl::groups;
    with_connection(pool, async |conn| {
        groups.count().get_result::<i64>(conn).await
    })
    .await
}

pub async fn group_identity_scope_name(
    pool: &crate::storage::postgres::PostgresPool,
    group_id_value: i32,
) -> Result<String, ApiError> {
    use crate::schema::{groups, identity_scopes};
    with_connection(pool, async |conn| {
        groups::table
            .inner_join(identity_scopes::table)
            .filter(groups::id.eq(group_id_value))
            .select(identity_scopes::name)
            .first::<String>(conn)
            .await
    })
    .await
}

impl LoadGroupRecord for GroupID {
    async fn load_group_record(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
    ) -> Result<Group, ApiError> {
        use crate::schema::groups::dsl::{groups, id};

        with_connection(pool, async |conn| {
            groups
                .filter(id.eq(self.id()))
                .first::<GroupRow>(conn)
                .await
                .map(Into::into)
        })
        .await
    }
}

pub trait DeleteGroupRecord {
    async fn delete_group_record_without_events(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
    ) -> Result<usize, ApiError>;

    async fn delete_group_record(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
        context: Option<&EventContext>,
    ) -> Result<usize, ApiError> {
        let _ = context;
        self.delete_group_record_without_events(pool).await
    }
}

impl DeleteGroupRecord for GroupID {
    async fn delete_group_record_without_events(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
    ) -> Result<usize, ApiError> {
        delete_group_by_id(pool, self.id(), None).await
    }

    async fn delete_group_record(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
        context: Option<&EventContext>,
    ) -> Result<usize, ApiError> {
        delete_group_by_id(pool, self.id(), context).await
    }
}

impl DeleteGroupRecord for Group {
    async fn delete_group_record_without_events(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
    ) -> Result<usize, ApiError> {
        delete_group_by_id(pool, self.id, None).await
    }

    async fn delete_group_record(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
        context: Option<&EventContext>,
    ) -> Result<usize, ApiError> {
        delete_group_by_id(pool, self.id, context).await
    }
}

pub trait SaveGroupRecord {
    async fn save_group_record_without_events(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
    ) -> Result<Group, ApiError>;

    async fn save_group_record(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
        context: Option<&EventContext>,
    ) -> Result<Group, ApiError> {
        let _ = context;
        self.save_group_record_without_events(pool).await
    }
}

impl SaveGroupRecord for NewGroup {
    async fn save_group_record_without_events(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
    ) -> Result<Group, ApiError> {
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

        with_connection(pool, async |conn| {
            diesel::insert_into(groups::table)
                .values((
                    groups::identity_scope_id.eq(scope.id),
                    groups::groupname.eq(&self.groupname),
                    groups::description.eq(&description),
                    groups::managed_by.eq(LOCAL_PROVIDER_KIND),
                ))
                .get_result::<GroupRow>(conn)
                .await
                .map(Into::into)
        })
        .await
    }

    async fn save_group_record(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
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

        with_transaction(pool, async |conn| -> Result<Group, ApiError> {
            let group = diesel::insert_into(groups::table)
                .values((
                    groups::identity_scope_id.eq(scope.id),
                    groups::groupname.eq(&self.groupname),
                    groups::description.eq(&description),
                    groups::managed_by.eq(LOCAL_PROVIDER_KIND),
                ))
                .get_result::<GroupRow>(conn)
                .await?
                .into();
            let event = group_event(
                &group,
                Action::Created,
                context,
                format!("Group '{}' created", group.groupname),
            )?
            .with_after(group_snapshot(&group));
            emit_event(conn, &event).await?;
            Ok(group)
        })
        .await
    }
}

pub trait UpdateGroupRecord {
    async fn update_group_record_without_events(
        &self,
        group_id: i32,
        pool: &crate::storage::postgres::PostgresPool,
    ) -> Result<Group, ApiError>;

    async fn update_group_record(
        &self,
        group_id: i32,
        pool: &crate::storage::postgres::PostgresPool,
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
        pool: &crate::storage::postgres::PostgresPool,
    ) -> Result<Group, ApiError> {
        use crate::schema::groups::dsl::{groups, id};

        with_connection(pool, async |conn| -> Result<Group, ApiError> {
            ensure_group_allows_local_write(conn, group_id).await?;
            Ok(diesel::update(groups.filter(id.eq(group_id)))
                .set(&UpdateGroupRow::from(self))
                .get_result::<GroupRow>(conn)
                .await?
                .into())
        })
        .await
    }

    async fn update_group_record(
        &self,
        group_id: i32,
        pool: &crate::storage::postgres::PostgresPool,
        context: Option<&EventContext>,
    ) -> Result<Group, ApiError> {
        let Some(context) = context else {
            return self
                .update_group_record_without_events(group_id, pool)
                .await;
        };

        use crate::schema::groups::dsl::{groups, id};

        with_transaction(pool, async |conn| -> Result<Group, ApiError> {
            let before: Group = groups
                .filter(id.eq(group_id))
                .for_update()
                .first::<GroupRow>(conn)
                .await?
                .into();
            crate::storage::postgres::assert_locked_revision_precondition(
                conn,
                &RevisionOwner::Group.key(before.id),
                before.revision,
            )
            .await?;
            ensure_group_allows_local_write(conn, before.id).await?;
            if !self.has_changes(&before) {
                return Ok(before);
            }
            let after = diesel::update(groups.filter(id.eq(group_id)))
                .set(&UpdateGroupRow::from(self))
                .get_result::<GroupRow>(conn)
                .await?
                .into();
            let event = group_event(
                &after,
                Action::Updated,
                context,
                format!("Group '{}' updated", after.groupname),
            )?
            .with_before(group_snapshot(&before))
            .with_after(group_snapshot(&after));
            emit_event(conn, &event).await?;
            Ok(after)
        })
        .await
    }
}

/// Group membership is principal-centric. Paginated public listings retain the
/// membership row because it is the authoritative revision owner and join the
/// principal only as nested display data.
pub trait GroupMembersBackend {
    async fn load_group_members(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
    ) -> Result<Vec<Principal>, ApiError>;

    async fn load_group_members_paginated(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
        query_options: &QueryOptions,
    ) -> Result<Vec<(PrincipalGroup, Principal)>, ApiError>;

    async fn count_group_members_paginated(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
        query_options: &QueryOptions,
    ) -> Result<i64, ApiError>;

    async fn remove_group_member_from_backend_without_events(
        &self,
        member_principal_id: i32,
        pool: &crate::storage::postgres::PostgresPool,
    ) -> Result<(), ApiError>;

    async fn remove_group_member_from_backend(
        &self,
        member_principal_id: i32,
        pool: &crate::storage::postgres::PostgresPool,
        context: Option<&EventContext>,
    ) -> Result<(), ApiError> {
        let _ = context;
        self.remove_group_member_from_backend_without_events(member_principal_id, pool)
            .await
    }
}

impl GroupMembersBackend for Group {
    async fn load_group_members(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
    ) -> Result<Vec<Principal>, ApiError> {
        use crate::schema::group_memberships::dsl::{group_id, group_memberships};
        use crate::schema::principals::dsl::principals;

        with_connection(pool, async |conn| {
            group_memberships
                .filter(group_id.eq(self.id))
                .inner_join(principals)
                .select(crate::schema::principals::all_columns)
                .load::<PrincipalRow>(conn)
                .await
        })
        .await
        .map(|rows| rows.into_iter().map(Into::into).collect())
    }

    async fn load_group_members_paginated(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
        query_options: &QueryOptions,
    ) -> Result<Vec<(PrincipalGroup, Principal)>, ApiError> {
        use crate::schema::group_memberships::dsl::{
            created_at as membership_created_at, group_id, group_memberships,
            revision as membership_revision, updated_at as membership_updated_at,
        };
        use crate::schema::principals::dsl::{id, name, principals};

        let mut base_query = group_memberships
            .filter(group_id.eq(self.id))
            .inner_join(principals)
            .select((
                crate::schema::group_memberships::all_columns,
                crate::schema::principals::all_columns,
            ))
            .into_boxed();

        for param in &query_options.filters {
            let operator = param.operator.clone();
            match param.field {
                FilterField::Id => numeric_search!(base_query, param, operator, id),
                FilterField::Name | FilterField::Username => {
                    string_search!(base_query, param, operator, name)
                }
                FilterField::CreatedAt => {
                    date_search!(base_query, param, operator, membership_created_at)
                }
                FilterField::UpdatedAt => {
                    date_search!(base_query, param, operator, membership_updated_at)
                }
                FilterField::Revision => {
                    crate::revision_search!(base_query, param, operator, membership_revision)
                }
                _ => {
                    return Err(ApiError::BadRequest(format!(
                        "Field '{}' isn't searchable (or does not exist) for principals",
                        param.field
                    )));
                }
            }
        }

        crate::apply_query_options!(base_query, query_options, PrincipalMemberQueryRow);

        with_connection(pool, async |conn| {
            base_query
                .load::<(PrincipalGroupRow, PrincipalRow)>(conn)
                .await
        })
        .await
        .map(|rows| {
            rows.into_iter()
                .map(|(membership, principal)| (membership.into(), principal.into()))
                .collect()
        })
    }

    async fn count_group_members_paginated(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
        query_options: &QueryOptions,
    ) -> Result<i64, ApiError> {
        use crate::schema::group_memberships::dsl::{
            created_at as membership_created_at, group_id, group_memberships,
            revision as membership_revision, updated_at as membership_updated_at,
        };
        use crate::schema::principals::dsl::{id, name, principals};

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
                FilterField::CreatedAt => {
                    date_search!(base_query, param, operator, membership_created_at)
                }
                FilterField::UpdatedAt => {
                    date_search!(base_query, param, operator, membership_updated_at)
                }
                FilterField::Revision => {
                    crate::revision_search!(base_query, param, operator, membership_revision)
                }
                _ => {
                    return Err(ApiError::BadRequest(format!(
                        "Field '{}' isn't searchable (or does not exist) for principals",
                        param.field
                    )));
                }
            }
        }

        with_connection(pool, async |conn| {
            base_query.count().get_result::<i64>(conn).await
        })
        .await
    }

    async fn remove_group_member_from_backend_without_events(
        &self,
        member_principal_id: i32,
        pool: &crate::storage::postgres::PostgresPool,
    ) -> Result<(), ApiError> {
        with_transaction(pool, async |conn| -> Result<(), ApiError> {
            remove_manual_membership_source(conn, member_principal_id, self.id).await?;
            Ok(())
        })
        .await?;
        Ok(())
    }

    async fn remove_group_member_from_backend(
        &self,
        member_principal_id: i32,
        pool: &crate::storage::postgres::PostgresPool,
        context: Option<&EventContext>,
    ) -> Result<(), ApiError> {
        let Some(context) = context else {
            return self
                .remove_group_member_from_backend_without_events(member_principal_id, pool)
                .await;
        };

        with_transaction(pool, async |conn| -> Result<(), ApiError> {
            let removed_membership =
                remove_manual_membership_source(conn, member_principal_id, self.id).await?;

            if let Some(membership) = removed_membership {
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
                .with_before(membership_snapshot(&membership))
                .with_metadata(user_group_metadata(
                    membership.principal_id,
                    membership.group_id,
                ));
                emit_event(conn, &event).await?;
            }

            Ok(())
        })
        .await
    }
}

pub(crate) async fn save_manual_membership(
    pool: &crate::storage::postgres::PostgresPool,
    principal_id: i32,
    group_id: i32,
    context: Option<&EventContext>,
) -> Result<PrincipalGroup, ApiError> {
    with_transaction(pool, async |conn| -> Result<PrincipalGroup, ApiError> {
        let ManualMembershipInsert {
            membership,
            effective_membership_created,
        } = insert_manual_membership(conn, principal_id, group_id).await?;

        if let Some(context) = context
            && effective_membership_created
        {
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
            .with_after(membership_snapshot(&membership))
            .with_metadata(user_group_metadata(
                membership.principal_id,
                membership.group_id,
            ));
            emit_event(conn, &event).await?;
        }

        Ok(membership)
    })
    .await
}

async fn load_principal_group(
    conn: &mut crate::storage::postgres::PostgresConnection,
    principal: i32,
    group: i32,
) -> Result<PrincipalGroup, diesel::result::Error> {
    use crate::schema::group_memberships::dsl::{group_id, group_memberships, principal_id};
    group_memberships
        .filter(principal_id.eq(principal))
        .filter(group_id.eq(group))
        .first::<PrincipalGroupRow>(conn)
        .await
        .map(Into::into)
}

pub async fn principal_group_by_ids(
    pool: &crate::storage::postgres::PostgresPool,
    principal: i32,
    group: i32,
) -> Result<PrincipalGroup, ApiError> {
    with_connection(pool, async |conn| {
        load_principal_group(conn, principal, group).await
    })
    .await
}

pub(crate) async fn group_member_principal(
    pool: &crate::storage::postgres::PostgresPool,
    principal_id: i32,
) -> Result<Principal, ApiError> {
    use crate::schema::principals::dsl::{id, principals};

    with_connection(pool, async |conn| {
        principals
            .filter(id.eq(principal_id))
            .first::<PrincipalRow>(conn)
            .await
            .map(Into::into)
    })
    .await
}
