//! PostgreSQL implementation of group lifecycle and membership contracts.

use chrono::NaiveDateTime;
use diesel::prelude::{ExpressionMethods, OptionalExtension, QueryDsl};
use diesel::{AsChangeset, JoinOnDsl, Queryable, Selectable, SelectableHelper};
use diesel_async::RunQueryDsl;
use hubuum_domain::{GroupId, IdentityScopeId, PrincipalId};
use hubuum_events_core::{Action, EntityType, EventContext, NewEvent};
use hubuum_query::{FilterField, QueryOptions};
use hubuum_storage_core::{
    MutationOutcome, StorageGroupCreate, StorageGroupListQuery, StorageGroupMember,
    StorageGroupUpdate, StorageIdentityGroup, StoragePage, StoragePrincipal, StoragePrincipalGroup,
    StoragePrincipalGroupListQuery,
};
use serde_json::{Value, json};

use crate::cursor::{CursorSqlField, CursorSqlType};
use crate::operations::event_record::append_event;
use crate::operations::principal::PrincipalRow;
use crate::revision::{RevisionOwner, record_metadata};
use crate::runtime::{
    assert_locked_revision_precondition, assert_revision_precondition_allows_missing_target,
};
use crate::{PostgresConnection, PostgresRevision, PostgresRuntime, PostgresStorageError};

const LOCAL_IDENTITY_SCOPE: &str = "local";
const LOCAL_PROVIDER_KIND: &str = "local";
const MANUAL_MEMBERSHIP_SOURCE: &str = "manual";
const OWNED_SERVICE_ACCOUNT_PREVIEW_LIMIT: i64 = 10;

macro_rules! apply_group_filters {
    ($query:ident, $options:expr, $allow_revision:expr) => {
        for parameter in $options.filters() {
            match parameter.field {
                FilterField::Id => {
                    crate::postgres_integer_filter!($query, parameter, crate::schema::groups::id)
                }
                FilterField::Name | FilterField::Groupname => {
                    crate::postgres_string_filter!(
                        $query,
                        parameter,
                        crate::schema::groups::groupname
                    )
                }
                FilterField::IdentityScope => crate::postgres_string_filter!(
                    $query,
                    parameter,
                    crate::schema::identity_scopes::name
                ),
                FilterField::Description => crate::postgres_string_filter!(
                    $query,
                    parameter,
                    crate::schema::groups::description
                ),
                FilterField::CreatedAt => crate::postgres_datetime_filter!(
                    $query,
                    parameter,
                    crate::schema::groups::created_at
                ),
                FilterField::UpdatedAt => crate::postgres_datetime_filter!(
                    $query,
                    parameter,
                    crate::schema::groups::updated_at
                ),
                FilterField::Revision if $allow_revision => crate::postgres_revision_filter!(
                    $query,
                    parameter,
                    crate::schema::groups::revision
                ),
                _ => {
                    return Err(PostgresStorageError::invalid_input(format!(
                        "Field '{}' isn't searchable (or does not exist) for groups",
                        parameter.field
                    )));
                }
            }
        }
    };
}

type GroupMemberQuery<'query> = diesel::dsl::IntoBoxed<
    'query,
    diesel::dsl::InnerJoin<
        crate::schema::group_memberships::table,
        crate::schema::principals::table,
    >,
    diesel::pg::Pg,
>;

#[derive(Clone, Queryable, Selectable)]
#[diesel(table_name = crate::schema::groups)]
pub(crate) struct GroupRow {
    pub(crate) id: i32,
    pub(crate) groupname: String,
    pub(crate) description: String,
    pub(crate) created_at: NaiveDateTime,
    pub(crate) updated_at: NaiveDateTime,
    pub(crate) identity_scope_id: i32,
    pub(crate) managed_by: String,
    pub(crate) external_key: Option<String>,
    pub(crate) last_sync_attempted_at: Option<NaiveDateTime>,
    pub(crate) last_sync_success_at: Option<NaiveDateTime>,
    pub(crate) revision: PostgresRevision,
}

impl GroupRow {
    fn into_storage(self) -> Result<StorageIdentityGroup, PostgresStorageError> {
        Ok(StorageIdentityGroup::builder(
            record_metadata(self.id, self.created_at, self.updated_at, self.revision)?,
            self.groupname,
            self.description,
            IdentityScopeId::new(self.identity_scope_id)?,
            self.managed_by,
        )
        .external_key(self.external_key)
        .last_sync_attempted_at(
            self.last_sync_attempted_at
                .map(|timestamp| timestamp.and_utc()),
        )
        .last_sync_success_at(
            self.last_sync_success_at
                .map(|timestamp| timestamp.and_utc()),
        )
        .build())
    }

    fn snapshot(&self) -> Value {
        json!({
            "id": self.id,
            "identity_scope_id": self.identity_scope_id,
            "groupname": self.groupname,
            "description": self.description,
            "managed_by": self.managed_by,
            "external_key": self.external_key,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "revision": self.revision,
        })
    }
}

#[derive(Clone, Queryable, Selectable)]
#[diesel(table_name = crate::schema::group_memberships)]
struct PrincipalGroupRow {
    principal_id: i32,
    group_id: i32,
    created_at: NaiveDateTime,
    updated_at: NaiveDateTime,
    revision: PostgresRevision,
}

impl PrincipalGroupRow {
    fn into_storage(self) -> Result<StoragePrincipalGroup, PostgresStorageError> {
        Ok(StoragePrincipalGroup::new(
            PrincipalId::new(self.principal_id)?,
            GroupId::new(self.group_id)?,
            self.created_at.and_utc(),
            self.updated_at.and_utc(),
            self.revision.into_domain(),
        ))
    }

    fn snapshot(&self) -> Value {
        json!({
            "principal_id": self.principal_id,
            "group_id": self.group_id,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "revision": self.revision,
        })
    }
}

#[derive(AsChangeset)]
#[diesel(table_name = crate::schema::groups)]
struct UpdateGroupRow<'value> {
    groupname: Option<&'value str>,
}

impl<'value> From<&'value StorageGroupUpdate> for UpdateGroupRow<'value> {
    fn from(update: &'value StorageGroupUpdate) -> Self {
        Self {
            groupname: update.name(),
        }
    }
}

pub async fn get_group(
    runtime: &PostgresRuntime,
    group_id: i32,
) -> Result<StorageIdentityGroup, PostgresStorageError> {
    validate_positive_id(group_id, "group id")?;
    runtime
        .with_connection(async |connection| {
            load_group_row(connection, group_id).await?.into_storage()
        })
        .await
}

#[cfg(feature = "integration-test-support")]
pub(crate) async fn load_group_by_name_for_test(
    runtime: &PostgresRuntime,
    group_name: String,
) -> Result<StorageIdentityGroup, PostgresStorageError> {
    runtime
        .with_connection(async move |connection| {
            crate::schema::groups::table
                .filter(crate::schema::groups::groupname.eq(group_name))
                .select(GroupRow::as_select())
                .first::<GroupRow>(connection)
                .await?
                .into_storage()
        })
        .await
}

pub async fn resolve_group_identity_scope_name(
    runtime: &PostgresRuntime,
    group_id: i32,
) -> Result<String, PostgresStorageError> {
    validate_positive_id(group_id, "group id")?;
    runtime
        .with_connection(async |connection| {
            crate::schema::groups::table
                .inner_join(crate::schema::identity_scopes::table)
                .filter(crate::schema::groups::id.eq(group_id))
                .select(crate::schema::identity_scopes::name)
                .first::<String>(connection)
                .await
        })
        .await
}

pub async fn list_principal_groups(
    runtime: &PostgresRuntime,
    query: StoragePrincipalGroupListQuery,
) -> Result<StoragePage<StorageIdentityGroup>, PostgresStorageError> {
    let (principal_id, options) = query.into_parts();
    let principal_id = principal_id.id();
    runtime
        .with_read_only_snapshot(async move |connection| {
            let build_query = || -> Result<_, PostgresStorageError> {
                let mut records = crate::schema::group_memberships::table
                    .inner_join(crate::schema::groups::table.on(
                        crate::schema::groups::id.eq(crate::schema::group_memberships::group_id),
                    ))
                    .inner_join(
                        crate::schema::identity_scopes::table
                            .on(crate::schema::groups::identity_scope_id
                                .eq(crate::schema::identity_scopes::id)),
                    )
                    .filter(crate::schema::group_memberships::principal_id.eq(principal_id))
                    .into_boxed();
                apply_group_filters!(records, options, false);
                Ok(records)
            };

            let total = if options.include_total() {
                Some(build_query()?.count().get_result::<i64>(connection).await?)
            } else {
                None
            };
            let mut records = build_query()?.select(GroupRow::as_select());
            let fields = options
                .sort()
                .iter()
                .map(|sort| group_cursor_field(&sort.field))
                .collect::<Result<Vec<_>, _>>()?;
            crate::apply_query_options_with_fields!(records, options, fields);
            let groups = records
                .load::<GroupRow>(connection)
                .await?
                .into_iter()
                .map(GroupRow::into_storage)
                .collect::<Result<Vec<_>, _>>()?;
            StoragePage::try_new(groups, total).map_err(PostgresStorageError::from)
        })
        .await
}

pub async fn list_groups(
    runtime: &PostgresRuntime,
    query: StorageGroupListQuery,
) -> Result<StoragePage<StorageIdentityGroup>, PostgresStorageError> {
    let (options, count_options) = query.into_parts();
    runtime
        .with_read_only_snapshot(async move |connection| {
            let build_query = |query_options: &QueryOptions| -> Result<_, PostgresStorageError> {
                let mut records = crate::schema::groups::table
                    .inner_join(crate::schema::identity_scopes::table)
                    .into_boxed();
                apply_group_filters!(records, query_options, true);
                Ok(records)
            };
            let total = match count_options.as_ref() {
                Some(count_options) => Some(
                    build_query(count_options)?
                        .count()
                        .get_result::<i64>(connection)
                        .await?,
                ),
                None => None,
            };
            let mut records = build_query(&options)?.select(GroupRow::as_select());
            let fields = options
                .sort()
                .iter()
                .map(|sort| group_cursor_field(&sort.field))
                .collect::<Result<Vec<_>, _>>()?;
            crate::apply_query_options_with_fields!(records, options, fields);
            let groups = records
                .distinct()
                .load::<GroupRow>(connection)
                .await?
                .into_iter()
                .map(GroupRow::into_storage)
                .collect::<Result<Vec<_>, _>>()?;
            StoragePage::try_new(groups, total).map_err(PostgresStorageError::from)
        })
        .await
}

pub async fn create_group(
    runtime: &PostgresRuntime,
    command: StorageGroupCreate,
    context: &EventContext,
) -> Result<MutationOutcome<StorageIdentityGroup>, PostgresStorageError> {
    let (identity_scope, name, description) = command.into_parts();
    let identity_scope = identity_scope.unwrap_or_else(|| LOCAL_IDENTITY_SCOPE.to_string());
    if identity_scope != LOCAL_IDENTITY_SCOPE {
        return Err(PostgresStorageError::invalid_input(
            "groups in non-local identity scopes are managed by their identity provider",
        ));
    }
    let description = description.unwrap_or_default();
    let context = context.clone();

    runtime
        .with_transaction(async move |connection| {
            let group = insert_local_group(connection, &name, &description).await?;
            let event = group_event(
                &group,
                Action::Created,
                &context,
                format!("Group '{}' created", group.groupname),
            )?
            .with_after(group.snapshot());
            let audit = append_event(connection, &event)
                .await?
                .into_audit_receipt()?;
            Ok::<_, PostgresStorageError>(MutationOutcome::committed(group.into_storage()?, audit))
        })
        .await
}

pub async fn update_group(
    runtime: &PostgresRuntime,
    group_id: i32,
    update: StorageGroupUpdate,
    context: &EventContext,
) -> Result<MutationOutcome<StorageIdentityGroup>, PostgresStorageError> {
    validate_positive_id(group_id, "group id")?;
    let context = context.clone();

    runtime
        .with_transaction(async move |connection| {
            let before = lock_group(connection, group_id).await?;
            ensure_group_allows_local_write(connection, group_id).await?;
            if !update.name().is_some_and(|name| name != before.groupname) {
                return Ok(MutationOutcome::unchanged(before.into_storage()?));
            }
            let after = diesel::update(
                crate::schema::groups::table.filter(crate::schema::groups::id.eq(group_id)),
            )
            .set(UpdateGroupRow::from(&update))
            .get_result::<GroupRow>(connection)
            .await?;
            let event = group_event(
                &after,
                Action::Updated,
                &context,
                format!("Group '{}' updated", after.groupname),
            )?
            .with_before(before.snapshot())
            .with_after(after.snapshot());
            let audit = append_event(connection, &event)
                .await?
                .into_audit_receipt()?;
            Ok::<_, PostgresStorageError>(MutationOutcome::committed(after.into_storage()?, audit))
        })
        .await
}

pub async fn delete_group(
    runtime: &PostgresRuntime,
    group_id: i32,
    context: &EventContext,
) -> Result<MutationOutcome<usize>, PostgresStorageError> {
    validate_positive_id(group_id, "group id")?;
    let context = context.clone();
    runtime
        .with_transaction(async move |connection| {
            let group = lock_group(connection, group_id).await?;
            ensure_group_has_no_owned_service_accounts(connection, group_id).await?;
            ensure_group_allows_local_write(connection, group_id).await?;
            let deleted = diesel::delete(
                crate::schema::groups::table.filter(crate::schema::groups::id.eq(group_id)),
            )
            .execute(connection)
            .await?;
            let event = group_event(
                &group,
                Action::Deleted,
                &context,
                format!("Group '{}' deleted", group.groupname),
            )?
            .with_before(group.snapshot());
            let audit = append_event(connection, &event)
                .await?
                .into_audit_receipt()?;
            Ok::<_, PostgresStorageError>(MutationOutcome::committed(deleted, audit))
        })
        .await
}

pub async fn load_group_member_principals(
    runtime: &PostgresRuntime,
    group_id: i32,
) -> Result<Vec<StoragePrincipal>, PostgresStorageError> {
    validate_positive_id(group_id, "group id")?;
    runtime
        .with_connection(async |connection| {
            let rows = crate::schema::group_memberships::table
                .filter(crate::schema::group_memberships::group_id.eq(group_id))
                .inner_join(crate::schema::principals::table)
                .select(PrincipalRow::as_select())
                .load::<PrincipalRow>(connection)
                .await?;
            rows.into_iter().map(PrincipalRow::into_storage).collect()
        })
        .await
}

pub async fn list_group_members(
    runtime: &PostgresRuntime,
    group_id: i32,
    options: QueryOptions,
) -> Result<StoragePage<StorageGroupMember>, PostgresStorageError> {
    validate_positive_id(group_id, "group id")?;
    runtime
        .with_read_only_snapshot(async |connection| {
            let build_query = || -> Result<_, PostgresStorageError> {
                let query = crate::schema::group_memberships::table
                    .filter(crate::schema::group_memberships::group_id.eq(group_id))
                    .inner_join(crate::schema::principals::table)
                    .into_boxed();
                apply_member_filters(query, &options)
            };
            let total = if options.include_total() {
                Some(build_query()?.count().get_result::<i64>(connection).await?)
            } else {
                None
            };
            let mut query = build_query()?;
            let fields = options
                .sort()
                .iter()
                .map(|sort| member_cursor_field(&sort.field))
                .collect::<Result<Vec<_>, _>>()?;
            crate::apply_query_options_with_fields!(query, options, fields);
            let rows = query
                .select((PrincipalGroupRow::as_select(), PrincipalRow::as_select()))
                .load::<(PrincipalGroupRow, PrincipalRow)>(connection)
                .await?;
            let members = rows
                .into_iter()
                .map(|(membership, principal)| {
                    Ok::<_, PostgresStorageError>(StorageGroupMember::new(
                        membership.into_storage()?,
                        principal.into_storage()?,
                    ))
                })
                .collect::<Result<Vec<_>, _>>()?;
            StoragePage::try_new(members, total).map_err(PostgresStorageError::from)
        })
        .await
}

pub async fn add_group_member(
    runtime: &PostgresRuntime,
    principal_id: i32,
    group_id: i32,
    context: &EventContext,
) -> Result<MutationOutcome<StoragePrincipalGroup>, PostgresStorageError> {
    validate_positive_id(principal_id, "principal id")?;
    validate_positive_id(group_id, "group id")?;
    let context = context.clone();
    runtime
        .with_transaction(async move |connection| {
            let (membership, effective_membership_created) =
                insert_manual_membership(connection, principal_id, group_id).await?;
            if effective_membership_created {
                let event = membership_event(
                    &membership,
                    Action::Added,
                    &context,
                    format!(
                        "Principal {} added to group {}",
                        membership.principal_id, membership.group_id
                    ),
                )?
                .with_after(membership.snapshot());
                let audit = append_event(connection, &event)
                    .await?
                    .into_audit_receipt()?;
                return Ok::<_, PostgresStorageError>(MutationOutcome::committed(
                    membership.into_storage()?,
                    audit,
                ));
            }
            Ok::<_, PostgresStorageError>(MutationOutcome::unchanged(membership.into_storage()?))
        })
        .await
}

pub async fn remove_group_member(
    runtime: &PostgresRuntime,
    principal_id: i32,
    group_id: i32,
    context: &EventContext,
) -> Result<MutationOutcome<()>, PostgresStorageError> {
    validate_positive_id(principal_id, "principal id")?;
    validate_positive_id(group_id, "group id")?;
    let context = context.clone();
    runtime
        .with_transaction(async move |connection| {
            let removed =
                remove_manual_membership_source(connection, principal_id, group_id).await?;
            if let Some(membership) = removed.as_ref() {
                let event = membership_event(
                    membership,
                    Action::Removed,
                    &context,
                    format!(
                        "Principal {} removed from group {}",
                        membership.principal_id, membership.group_id
                    ),
                )?
                .with_before(membership.snapshot());
                let audit = append_event(connection, &event)
                    .await?
                    .into_audit_receipt()?;
                return Ok::<_, PostgresStorageError>(MutationOutcome::committed((), audit));
            }
            Ok::<_, PostgresStorageError>(MutationOutcome::unchanged(()))
        })
        .await
}

async fn load_group_row(
    connection: &mut PostgresConnection,
    group_id: i32,
) -> Result<GroupRow, diesel::result::Error> {
    crate::schema::groups::table
        .filter(crate::schema::groups::id.eq(group_id))
        .select(GroupRow::as_select())
        .first(connection)
        .await
}

async fn insert_local_group(
    connection: &mut PostgresConnection,
    name: &str,
    description: &str,
) -> Result<GroupRow, PostgresStorageError> {
    let identity_scope_id = crate::schema::identity_scopes::table
        .filter(crate::schema::identity_scopes::name.eq(LOCAL_IDENTITY_SCOPE))
        .select(crate::schema::identity_scopes::id)
        .first::<i32>(connection)
        .await?;
    diesel::insert_into(crate::schema::groups::table)
        .values((
            crate::schema::groups::identity_scope_id.eq(identity_scope_id),
            crate::schema::groups::groupname.eq(name),
            crate::schema::groups::description.eq(description),
            crate::schema::groups::managed_by.eq(LOCAL_PROVIDER_KIND),
        ))
        .get_result::<GroupRow>(connection)
        .await
        .map_err(PostgresStorageError::from)
}

async fn lock_group(
    connection: &mut PostgresConnection,
    group_id: i32,
) -> Result<GroupRow, PostgresStorageError> {
    let group = crate::schema::groups::table
        .filter(crate::schema::groups::id.eq(group_id))
        .for_update()
        .select(GroupRow::as_select())
        .first::<GroupRow>(connection)
        .await?;
    assert_locked_revision_precondition(
        connection,
        &RevisionOwner::Group.key(group_id),
        group.revision,
    )
    .await?;
    Ok(group)
}

async fn ensure_group_allows_local_write(
    connection: &mut PostgresConnection,
    group_id: i32,
) -> Result<(), PostgresStorageError> {
    let manager = crate::schema::groups::table
        .filter(crate::schema::groups::id.eq(group_id))
        .select(crate::schema::groups::managed_by)
        .first::<String>(connection)
        .await?;
    if manager == LOCAL_PROVIDER_KIND {
        Ok(())
    } else {
        Err(PostgresStorageError::permission_denied(
            "Provider-managed groups are read-only in Hubuum",
        ))
    }
}

async fn ensure_group_has_no_owned_service_accounts(
    connection: &mut PostgresConnection,
    group_id: i32,
) -> Result<(), PostgresStorageError> {
    let mut owned = crate::schema::service_accounts::table
        .inner_join(
            crate::schema::principals::table
                .on(crate::schema::principals::id.eq(crate::schema::service_accounts::id)),
        )
        .filter(crate::schema::service_accounts::owner_group_id.eq(group_id))
        .select((
            crate::schema::service_accounts::id,
            crate::schema::principals::name,
        ))
        .order(crate::schema::service_accounts::id.asc())
        .limit(OWNED_SERVICE_ACCOUNT_PREVIEW_LIMIT + 1)
        .load::<(i32, String)>(connection)
        .await?;
    if owned.is_empty() {
        return Ok(());
    }
    let omitted = owned.len() > OWNED_SERVICE_ACCOUNT_PREVIEW_LIMIT as usize;
    owned.truncate(OWNED_SERVICE_ACCOUNT_PREVIEW_LIMIT as usize);
    let list = owned
        .iter()
        .map(|(id, name)| format!("{name} (id {id})"))
        .collect::<Vec<_>>()
        .join(", ");
    let suffix = if omitted {
        "; additional service accounts omitted"
    } else {
        ""
    };
    Err(PostgresStorageError::conflict(format!(
        "Group owns service accounts; reassign or delete them first: {list}{suffix}"
    )))
}

async fn insert_manual_membership(
    connection: &mut PostgresConnection,
    principal_id: i32,
    group_id: i32,
) -> Result<(PrincipalGroupRow, bool), PostgresStorageError> {
    let effective_created = insert_effective_membership(connection, principal_id, group_id).await?;
    ensure_group_allows_local_write(connection, group_id).await?;
    let local_scope_id = crate::schema::identity_scopes::table
        .filter(crate::schema::identity_scopes::name.eq(LOCAL_IDENTITY_SCOPE))
        .select(crate::schema::identity_scopes::id)
        .first::<i32>(connection)
        .await?;
    diesel::insert_into(crate::schema::group_membership_sources::table)
        .values((
            crate::schema::group_membership_sources::principal_id.eq(principal_id),
            crate::schema::group_membership_sources::group_id.eq(group_id),
            crate::schema::group_membership_sources::source.eq(MANUAL_MEMBERSHIP_SOURCE),
            crate::schema::group_membership_sources::source_scope_id.eq(local_scope_id),
            crate::schema::group_membership_sources::source_key.eq(""),
        ))
        .on_conflict_do_nothing()
        .execute(connection)
        .await?;
    let membership = load_principal_group_row(connection, principal_id, group_id).await?;
    Ok((membership, effective_created))
}

async fn insert_effective_membership(
    connection: &mut PostgresConnection,
    principal_id: i32,
    group_id: i32,
) -> Result<bool, PostgresStorageError> {
    let owner_key = RevisionOwner::membership_key(principal_id, group_id);
    let current = crate::schema::group_memberships::table
        .filter(crate::schema::group_memberships::principal_id.eq(principal_id))
        .filter(crate::schema::group_memberships::group_id.eq(group_id))
        .for_update()
        .select(PrincipalGroupRow::as_select())
        .first::<PrincipalGroupRow>(connection)
        .await
        .optional()?;
    if let Some(membership) = current {
        assert_locked_revision_precondition(connection, &owner_key, membership.revision).await?;
        return Ok(false);
    }
    assert_revision_precondition_allows_missing_target(&owner_key)?;
    let inserted = diesel::insert_into(crate::schema::group_memberships::table)
        .values((
            crate::schema::group_memberships::principal_id.eq(principal_id),
            crate::schema::group_memberships::group_id.eq(group_id),
        ))
        .on_conflict_do_nothing()
        .execute(connection)
        .await?;
    if inserted == 0 {
        load_principal_group_row(connection, principal_id, group_id).await?;
    }
    Ok(inserted > 0)
}

async fn remove_manual_membership_source(
    connection: &mut PostgresConnection,
    principal_id: i32,
    group_id: i32,
) -> Result<Option<PrincipalGroupRow>, PostgresStorageError> {
    ensure_group_allows_local_write(connection, group_id).await?;
    let owner_key = RevisionOwner::membership_key(principal_id, group_id);
    let membership = crate::schema::group_memberships::table
        .filter(crate::schema::group_memberships::principal_id.eq(principal_id))
        .filter(crate::schema::group_memberships::group_id.eq(group_id))
        .for_update()
        .select(PrincipalGroupRow::as_select())
        .first::<PrincipalGroupRow>(connection)
        .await
        .optional()?;
    let Some(membership) = membership else {
        assert_revision_precondition_allows_missing_target(&owner_key)?;
        return Ok(None);
    };
    assert_locked_revision_precondition(connection, &owner_key, membership.revision).await?;
    let local_scope_id = crate::schema::identity_scopes::table
        .filter(crate::schema::identity_scopes::name.eq(LOCAL_IDENTITY_SCOPE))
        .select(crate::schema::identity_scopes::id)
        .first::<i32>(connection)
        .await?;
    diesel::delete(
        crate::schema::group_membership_sources::table
            .filter(crate::schema::group_membership_sources::principal_id.eq(principal_id))
            .filter(crate::schema::group_membership_sources::group_id.eq(group_id))
            .filter(crate::schema::group_membership_sources::source.eq(MANUAL_MEMBERSHIP_SOURCE))
            .filter(crate::schema::group_membership_sources::source_scope_id.eq(local_scope_id))
            .filter(crate::schema::group_membership_sources::source_key.eq("")),
    )
    .execute(connection)
    .await?;
    let remaining = crate::schema::group_membership_sources::table
        .filter(crate::schema::group_membership_sources::principal_id.eq(principal_id))
        .filter(crate::schema::group_membership_sources::group_id.eq(group_id))
        .count()
        .get_result::<i64>(connection)
        .await?;
    if remaining == 0 {
        let deleted = diesel::delete(
            crate::schema::group_memberships::table
                .filter(crate::schema::group_memberships::principal_id.eq(principal_id))
                .filter(crate::schema::group_memberships::group_id.eq(group_id)),
        )
        .execute(connection)
        .await?;
        return Ok((deleted > 0).then_some(membership));
    }
    Ok(None)
}

async fn load_principal_group_row(
    connection: &mut PostgresConnection,
    principal_id: i32,
    group_id: i32,
) -> Result<PrincipalGroupRow, diesel::result::Error> {
    crate::schema::group_memberships::table
        .filter(crate::schema::group_memberships::principal_id.eq(principal_id))
        .filter(crate::schema::group_memberships::group_id.eq(group_id))
        .select(PrincipalGroupRow::as_select())
        .first(connection)
        .await
}

fn apply_member_filters<'query>(
    mut query: GroupMemberQuery<'query>,
    options: &QueryOptions,
) -> Result<GroupMemberQuery<'query>, PostgresStorageError> {
    for parameter in options.filters() {
        match parameter.field {
            FilterField::Id => {
                crate::postgres_integer_filter!(query, parameter, crate::schema::principals::id)
            }
            FilterField::Name | FilterField::Username => {
                crate::postgres_string_filter!(query, parameter, crate::schema::principals::name)
            }
            FilterField::CreatedAt => crate::postgres_datetime_filter!(
                query,
                parameter,
                crate::schema::group_memberships::created_at
            ),
            FilterField::UpdatedAt => crate::postgres_datetime_filter!(
                query,
                parameter,
                crate::schema::group_memberships::updated_at
            ),
            FilterField::Revision => crate::postgres_revision_filter!(
                query,
                parameter,
                crate::schema::group_memberships::revision
            ),
            _ => {
                return Err(PostgresStorageError::invalid_input(format!(
                    "Field '{}' isn't searchable (or does not exist) for principals",
                    parameter.field
                )));
            }
        }
    }
    Ok(query)
}

fn member_cursor_field(field: &FilterField) -> Result<CursorSqlField, PostgresStorageError> {
    Ok(match field {
        FilterField::Id => cursor_field("principals.id", CursorSqlType::Integer),
        FilterField::Name | FilterField::Username => {
            cursor_field("principals.name", CursorSqlType::String)
        }
        FilterField::CreatedAt => {
            cursor_field("group_memberships.created_at", CursorSqlType::DateTime)
        }
        FilterField::UpdatedAt => {
            cursor_field("group_memberships.updated_at", CursorSqlType::DateTime)
        }
        FilterField::Revision => cursor_field("group_memberships.revision", CursorSqlType::BigInt),
        _ => {
            return Err(PostgresStorageError::invalid_input(format!(
                "Field '{field}' is not orderable for principals"
            )));
        }
    })
}

fn group_cursor_field(field: &FilterField) -> Result<CursorSqlField, PostgresStorageError> {
    Ok(match field {
        FilterField::Id => cursor_field("groups.id", CursorSqlType::Integer),
        FilterField::Name | FilterField::Groupname => {
            cursor_field("groups.groupname", CursorSqlType::String)
        }
        FilterField::Description => cursor_field("groups.description", CursorSqlType::String),
        FilterField::CreatedAt => cursor_field("groups.created_at", CursorSqlType::DateTime),
        FilterField::UpdatedAt => cursor_field("groups.updated_at", CursorSqlType::DateTime),
        FilterField::Revision => cursor_field("groups.revision", CursorSqlType::BigInt),
        _ => {
            return Err(PostgresStorageError::invalid_input(format!(
                "Field '{field}' is not orderable for groups"
            )));
        }
    })
}

const fn cursor_field(column: &'static str, sql_type: CursorSqlType) -> CursorSqlField {
    CursorSqlField {
        column,
        sql_type,
        nullable: false,
    }
}

fn group_event(
    group: &GroupRow,
    action: Action,
    context: &EventContext,
    summary: String,
) -> Result<NewEvent, PostgresStorageError> {
    NewEvent::new(EntityType::Group, action, context.actor_kind(), summary)
        .map_err(|error| PostgresStorageError::database(error.to_string()))
        .and_then(|event| {
            Ok(event
                .with_context(context)
                .with_entity_id(hubuum_events_core::EventEntityId::new(group.id)?)
                .with_entity_name(group.groupname.clone()))
        })
}

fn membership_event(
    membership: &PrincipalGroupRow,
    action: Action,
    context: &EventContext,
    summary: String,
) -> Result<NewEvent, PostgresStorageError> {
    NewEvent::new(EntityType::UserGroup, action, context.actor_kind(), summary)
        .map_err(|error| PostgresStorageError::database(error.to_string()))
        .map(|event| {
            event.with_context(context).with_metadata(json!({
                "principal_id": membership.principal_id,
                "group_id": membership.group_id,
            }))
        })
}

fn validate_positive_id(id: i32, field: &str) -> Result<(), PostgresStorageError> {
    if id > 0 {
        Ok(())
    } else {
        Err(PostgresStorageError::invalid_input(format!(
            "{field} must be greater than zero"
        )))
    }
}
