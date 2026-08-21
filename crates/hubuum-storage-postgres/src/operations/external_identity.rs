//! PostgreSQL implementation of external-identity synchronization contracts.

use std::collections::{HashMap, HashSet};

use chrono::NaiveDateTime;
use diesel::dsl::not;
use diesel::prelude::{ExpressionMethods, OptionalExtension, QueryDsl};
use diesel::upsert::excluded;
use diesel::{Insertable, JoinOnDsl, Queryable, QueryableByName, Selectable, SelectableHelper};
use diesel_async::RunQueryDsl;
use hubuum_domain::{EXTERNAL_MEMBERSHIP_SOURCE, LOCAL_PROVIDER_KIND, UserId};
use hubuum_events_core::{Action, EntityType, EventContext, NewEvent};
use hubuum_storage_core::{
    MutationOutcome, StorageExternalPrincipalState, StorageExternalUserSync, StorageSyncedHuman,
};
use serde_json::json;

use crate::operations::event_record::append_event;
use crate::{PostgresConnection, PostgresRuntime, PostgresStorageError};

const HUMAN_PRINCIPAL_KIND: &str = "human";
const DATABASE_UTC_NOW_QUERY: &str = "SELECT clock_timestamp() AT TIME ZONE 'UTC' AS now";

#[derive(Queryable)]
struct ExternalPrincipalRow {
    id: i32,
    kind: String,
    name: String,
    provider_managed: bool,
}

#[derive(Queryable, Selectable)]
#[diesel(table_name = crate::schema::users)]
struct ExternalUserRow {
    id: i32,
    #[diesel(column_name = kind)]
    _kind: String,
    #[diesel(column_name = password)]
    _password: Option<String>,
    proper_name: Option<String>,
    email: Option<String>,
    created_at: NaiveDateTime,
    updated_at: NaiveDateTime,
    anonymized_at: Option<NaiveDateTime>,
}

impl ExternalUserRow {
    fn into_storage(self) -> Result<StorageSyncedHuman, PostgresStorageError> {
        Ok(StorageSyncedHuman::new(
            UserId::new(self.id)?,
            self.proper_name,
            self.email,
            self.created_at,
            self.updated_at,
            self.anonymized_at,
        ))
    }
}

#[derive(Insertable)]
#[diesel(table_name = crate::schema::groups)]
struct ExternalGroupRow {
    identity_scope_id: i32,
    groupname: String,
    description: String,
    managed_by: String,
    external_key: Option<String>,
    last_sync_attempted_at: Option<NaiveDateTime>,
    last_sync_success_at: Option<NaiveDateTime>,
}

#[derive(Insertable)]
#[diesel(table_name = crate::schema::group_memberships)]
struct ExternalMembershipRow {
    principal_id: i32,
    group_id: i32,
}

#[derive(Insertable)]
#[diesel(table_name = crate::schema::group_membership_sources)]
struct ExternalMembershipSourceRow {
    principal_id: i32,
    group_id: i32,
    source: &'static str,
    source_scope_id: i32,
    source_key: String,
}

#[derive(QueryableByName)]
struct DatabaseTimeRow {
    #[diesel(sql_type = diesel::sql_types::Timestamp)]
    now: NaiveDateTime,
}

pub async fn get_external_principal_state(
    runtime: &PostgresRuntime,
    principal_id: i32,
) -> Result<Option<StorageExternalPrincipalState>, PostgresStorageError> {
    validate_positive_id(principal_id, "principal id")?;
    let row = runtime
        .with_connection(async move |connection| {
            crate::schema::users::table
                .inner_join(
                    crate::schema::principals::table
                        .on(crate::schema::users::id.eq(crate::schema::principals::id)),
                )
                .inner_join(
                    crate::schema::identity_scopes::table
                        .on(crate::schema::principals::identity_scope_id
                            .eq(crate::schema::identity_scopes::id)),
                )
                .filter(crate::schema::users::id.eq(principal_id))
                .select((
                    crate::schema::identity_scopes::provider_kind,
                    crate::schema::principals::provider_managed,
                    crate::schema::principals::external_subject,
                    crate::schema::principals::last_sync_attempted_at,
                    crate::schema::principals::last_sync_success_at,
                    crate::schema::identity_scopes::name,
                    crate::schema::principals::name,
                ))
                .first::<(
                    String,
                    bool,
                    Option<String>,
                    Option<NaiveDateTime>,
                    Option<NaiveDateTime>,
                    String,
                    String,
                )>(connection)
                .await
                .optional()
        })
        .await?;

    let Some((
        provider,
        provider_managed,
        external_subject,
        last_sync_attempted_at,
        last_sync_success_at,
        identity_scope,
        username,
    )) = row
    else {
        return Ok(None);
    };
    if provider == LOCAL_PROVIDER_KIND || !provider_managed {
        return Ok(None);
    }
    let external_subject = external_subject.ok_or_else(|| {
        PostgresStorageError::unavailable("External user is missing provider subject")
    })?;
    Ok(Some(StorageExternalPrincipalState::new(
        identity_scope,
        username,
        external_subject,
        last_sync_attempted_at,
        last_sync_success_at,
    )))
}

pub async fn mark_external_sync_attempted(
    runtime: &PostgresRuntime,
    principal_id: i32,
) -> Result<(), PostgresStorageError> {
    validate_positive_id(principal_id, "principal id")?;
    runtime
        .with_connection(async move |connection| {
            diesel::update(
                crate::schema::principals::table
                    .filter(crate::schema::principals::id.eq(principal_id)),
            )
            .set(crate::schema::principals::last_sync_attempted_at.eq(diesel::dsl::now))
            .execute(connection)
            .await?;
            Ok::<_, PostgresStorageError>(())
        })
        .await
}

pub async fn sync_external_user(
    runtime: &PostgresRuntime,
    request: StorageExternalUserSync,
) -> Result<MutationOutcome<StorageSyncedHuman>, PostgresStorageError> {
    let (scope_name, provider_kind, subject, name, proper_name, email, groups) =
        request.into_parts();
    validate_required(&scope_name, "identity scope")?;
    validate_required(&provider_kind, "provider kind")?;
    validate_required(&subject, "external subject")?;
    validate_required(&name, "principal name")?;
    let mut seen_group_keys = HashSet::with_capacity(groups.len());
    for group in &groups {
        validate_required(group.key(), "external group key")?;
        validate_required(group.name(), "external group name")?;
        if !seen_group_keys.insert(group.key().to_string()) {
            return Err(PostgresStorageError::bad_request(format!(
                "duplicate external group key '{}'",
                group.key()
            )));
        }
    }
    let synced_group_count = groups.len();

    runtime
        .with_transaction(async move |connection| {
            let scope = crate::operations::identity_scope::ensure_identity_scope_on_connection(
                connection,
                &scope_name,
                &provider_kind,
            )
            .await?;
            let sync_time = database_now(connection).await?;
            let scope_id = scope.id().id();
            let principal =
                reconcile_principal(connection, scope_id, &subject, &name, sync_time).await?;
            if principal.kind != HUMAN_PRINCIPAL_KIND {
                return Err(PostgresStorageError::conflict(
                    "external identity subject belongs to a non-human principal",
                ));
            }

            let user = reconcile_user(connection, principal.id, proper_name, email).await?;
            diesel::update(
                crate::schema::principals::table
                    .filter(crate::schema::principals::id.eq(principal.id)),
            )
            .set((
                crate::schema::principals::provider_managed.eq(true),
                crate::schema::principals::external_subject.eq(&subject),
                crate::schema::principals::last_sync_attempted_at.eq(sync_time),
                crate::schema::principals::last_sync_success_at.eq(sync_time),
            ))
            .execute(connection)
            .await?;

            let synced_group_ids = reconcile_groups(
                connection,
                scope_id,
                principal.id,
                &provider_kind,
                groups,
                sync_time,
            )
            .await?;
            remove_stale_memberships(connection, scope_id, principal.id, &synced_group_ids).await?;

            let context = EventContext::system();
            let event = NewEvent::new(
                EntityType::ExternalIdentitySync,
                Action::Succeeded,
                context.actor_kind(),
                format!("External identity '{name}' synced in scope '{scope_name}'"),
            )
            .map_err(|error| PostgresStorageError::database(error.to_string()))?
            .with_context(&context)
            .with_entity_id(hubuum_events_core::EventEntityId::new(principal.id)?)
            .with_entity_name(name)
            .with_metadata(json!({
                "principal_id": principal.id,
                "identity_scope": scope_name,
                "provider_kind": provider_kind,
                "external_subject": subject,
                "synced_group_count": synced_group_count,
            }));
            let audit = append_event(connection, &event)
                .await?
                .into_audit_receipt()?;
            Ok::<_, PostgresStorageError>(MutationOutcome::committed(user.into_storage()?, audit))
        })
        .await
}

async fn reconcile_principal(
    connection: &mut PostgresConnection,
    scope_id: i32,
    subject: &str,
    name: &str,
    sync_time: NaiveDateTime,
) -> Result<ExternalPrincipalRow, PostgresStorageError> {
    let selection = (
        crate::schema::principals::id,
        crate::schema::principals::kind,
        crate::schema::principals::name,
        crate::schema::principals::provider_managed,
    );
    let existing = crate::schema::principals::table
        .filter(crate::schema::principals::identity_scope_id.eq(scope_id))
        .filter(crate::schema::principals::external_subject.eq(subject))
        .select(selection)
        .first::<ExternalPrincipalRow>(connection)
        .await
        .optional()?;
    if let Some(existing) = existing {
        if existing.name == name {
            return Ok(existing);
        }
        return diesel::update(
            crate::schema::principals::table.filter(crate::schema::principals::id.eq(existing.id)),
        )
        .set((
            crate::schema::principals::name.eq(name),
            crate::schema::principals::provider_managed.eq(true),
            crate::schema::principals::last_sync_attempted_at.eq(sync_time),
            crate::schema::principals::last_sync_success_at.eq(sync_time),
        ))
        .returning(selection)
        .get_result::<ExternalPrincipalRow>(connection)
        .await
        .map_err(PostgresStorageError::from);
    }

    let inserted = diesel::insert_into(crate::schema::principals::table)
        .values((
            crate::schema::principals::identity_scope_id.eq(scope_id),
            crate::schema::principals::kind.eq(HUMAN_PRINCIPAL_KIND),
            crate::schema::principals::name.eq(name),
            crate::schema::principals::provider_managed.eq(true),
            crate::schema::principals::external_subject.eq(subject),
            crate::schema::principals::last_sync_attempted_at.eq(sync_time),
            crate::schema::principals::last_sync_success_at.eq(sync_time),
        ))
        .on_conflict_do_nothing()
        .returning(selection)
        .get_result::<ExternalPrincipalRow>(connection)
        .await
        .optional()?;
    if let Some(inserted) = inserted {
        return Ok(inserted);
    }

    let principal = crate::schema::principals::table
        .filter(crate::schema::principals::identity_scope_id.eq(scope_id))
        .filter(crate::schema::principals::name.eq(name))
        .select(selection)
        .first::<ExternalPrincipalRow>(connection)
        .await?;
    if principal.provider_managed && principal.kind == HUMAN_PRINCIPAL_KIND {
        Ok(principal)
    } else {
        Err(PostgresStorageError::conflict(
            "identity scope already contains a different principal with this name",
        ))
    }
}

async fn reconcile_user(
    connection: &mut PostgresConnection,
    principal_id: i32,
    proper_name: Option<String>,
    email: Option<String>,
) -> Result<ExternalUserRow, PostgresStorageError> {
    let written = diesel::insert_into(crate::schema::users::table)
        .values((
            crate::schema::users::id.eq(principal_id),
            crate::schema::users::password.eq::<Option<String>>(None),
            crate::schema::users::proper_name.eq(proper_name),
            crate::schema::users::email.eq(email),
        ))
        .on_conflict(crate::schema::users::id)
        .do_update()
        .set((
            crate::schema::users::proper_name.eq(excluded(crate::schema::users::proper_name)),
            crate::schema::users::email.eq(excluded(crate::schema::users::email)),
        ))
        .returning(ExternalUserRow::as_returning())
        .get_result::<ExternalUserRow>(connection)
        .await
        .optional()?;
    match written {
        Some(user) => Ok(user),
        None => crate::schema::users::table
            .find(principal_id)
            .select(ExternalUserRow::as_select())
            .first::<ExternalUserRow>(connection)
            .await
            .map_err(PostgresStorageError::from),
    }
}

async fn reconcile_groups(
    connection: &mut PostgresConnection,
    scope_id: i32,
    principal_id: i32,
    provider_kind: &str,
    groups: Vec<hubuum_storage_core::StorageExternalGroup>,
    sync_time: NaiveDateTime,
) -> Result<Vec<i32>, PostgresStorageError> {
    if groups.is_empty() {
        return Ok(Vec::new());
    }
    let rows = groups
        .into_iter()
        .map(|group| {
            let (key, name, description) = group.into_parts();
            ExternalGroupRow {
                identity_scope_id: scope_id,
                groupname: name,
                description: description.unwrap_or_default(),
                managed_by: provider_kind.to_string(),
                external_key: Some(key),
                last_sync_attempted_at: Some(sync_time),
                last_sync_success_at: Some(sync_time),
            }
        })
        .collect::<Vec<_>>();
    diesel::insert_into(crate::schema::groups::table)
        .values(&rows)
        .on_conflict((
            crate::schema::groups::identity_scope_id,
            crate::schema::groups::external_key,
        ))
        .do_update()
        .set((
            crate::schema::groups::groupname.eq(excluded(crate::schema::groups::groupname)),
            crate::schema::groups::description.eq(excluded(crate::schema::groups::description)),
            crate::schema::groups::managed_by.eq(excluded(crate::schema::groups::managed_by)),
            crate::schema::groups::last_sync_attempted_at
                .eq(excluded(crate::schema::groups::last_sync_attempted_at)),
            crate::schema::groups::last_sync_success_at
                .eq(excluded(crate::schema::groups::last_sync_success_at)),
        ))
        .execute(connection)
        .await?;
    // No-op revision triggers may suppress rows from `RETURNING`; resolve the
    // complete requested set explicitly after the batched upsert.
    let external_keys = rows
        .iter()
        .filter_map(|row| row.external_key.clone())
        .collect::<Vec<_>>();
    let saved = crate::schema::groups::table
        .filter(crate::schema::groups::identity_scope_id.eq(scope_id))
        .filter(crate::schema::groups::external_key.eq_any(&external_keys))
        .select((
            crate::schema::groups::id,
            crate::schema::groups::external_key,
        ))
        .load::<(i32, Option<String>)>(connection)
        .await?;
    let ids_by_key = saved
        .into_iter()
        .map(|(id, key)| {
            key.map(|key| (key, id)).ok_or_else(|| {
                PostgresStorageError::database(
                    "external group upsert returned a row without an external key",
                )
            })
        })
        .collect::<Result<HashMap<_, _>, _>>()?;
    let group_ids = rows
        .iter()
        .map(|row| {
            row.external_key
                .as_ref()
                .and_then(|key| ids_by_key.get(key))
                .copied()
                .ok_or_else(|| {
                    PostgresStorageError::database(
                        "external group upsert did not return every requested group",
                    )
                })
        })
        .collect::<Result<Vec<_>, _>>()?;

    let memberships = group_ids
        .iter()
        .map(|group_id| ExternalMembershipRow {
            principal_id,
            group_id: *group_id,
        })
        .collect::<Vec<_>>();
    diesel::insert_into(crate::schema::group_memberships::table)
        .values(&memberships)
        .on_conflict_do_nothing()
        .execute(connection)
        .await?;
    let sources = rows
        .iter()
        .zip(&group_ids)
        .map(|(group, group_id)| ExternalMembershipSourceRow {
            principal_id,
            group_id: *group_id,
            source: EXTERNAL_MEMBERSHIP_SOURCE,
            source_scope_id: scope_id,
            source_key: group.external_key.clone().unwrap_or_default(),
        })
        .collect::<Vec<_>>();
    diesel::insert_into(crate::schema::group_membership_sources::table)
        .values(&sources)
        .on_conflict_do_nothing()
        .execute(connection)
        .await?;
    Ok(group_ids)
}

async fn remove_stale_memberships(
    connection: &mut PostgresConnection,
    scope_id: i32,
    principal_id: i32,
    synced_group_ids: &[i32],
) -> Result<(), PostgresStorageError> {
    diesel::delete(
        crate::schema::group_membership_sources::table
            .filter(crate::schema::group_membership_sources::principal_id.eq(principal_id))
            .filter(crate::schema::group_membership_sources::source.eq(EXTERNAL_MEMBERSHIP_SOURCE))
            .filter(crate::schema::group_membership_sources::source_scope_id.eq(scope_id))
            .filter(not(
                crate::schema::group_membership_sources::group_id.eq_any(synced_group_ids)
            )),
    )
    .execute(connection)
    .await?;
    let retained_group_ids = crate::schema::group_membership_sources::table
        .filter(crate::schema::group_membership_sources::principal_id.eq(principal_id))
        .select(crate::schema::group_membership_sources::group_id)
        .load::<i32>(connection)
        .await?;
    diesel::delete(
        crate::schema::group_memberships::table
            .filter(crate::schema::group_memberships::principal_id.eq(principal_id))
            .filter(not(
                crate::schema::group_memberships::group_id.eq_any(retained_group_ids)
            )),
    )
    .execute(connection)
    .await?;
    Ok(())
}

async fn database_now(
    connection: &mut PostgresConnection,
) -> Result<NaiveDateTime, PostgresStorageError> {
    diesel::sql_query(DATABASE_UTC_NOW_QUERY)
        .get_result::<DatabaseTimeRow>(connection)
        .await
        .map(|row| row.now)
        .map_err(PostgresStorageError::from)
}

fn validate_required(value: &str, field: &str) -> Result<(), PostgresStorageError> {
    if value.trim().is_empty() {
        Err(PostgresStorageError::bad_request(format!(
            "{field} must not be empty"
        )))
    } else {
        Ok(())
    }
}

fn validate_positive_id(id: i32, field: &str) -> Result<(), PostgresStorageError> {
    if id > 0 {
        Ok(())
    } else {
        Err(PostgresStorageError::bad_request(format!(
            "{field} must be greater than zero"
        )))
    }
}
