use super::{
    ImportReferenceChange, ImportRuntime, assert_import_create_condition, assert_import_revision,
    import_timestamp_pair, imported_timestamps, updated_or_current,
    with_imported_timestamp_override,
};
use crate::operations::auth_sessions;
use crate::operations::group::GroupRow;
use crate::operations::principal::PrincipalRow;
use crate::operations::service_account::ServiceAccountRow;
use crate::operations::user_row::UserRow;
use crate::{PostgresConnection, PostgresRevision, PostgresStorageError};
use argon2::Argon2;
use argon2::password_hash::PasswordHasher;
use argon2::password_hash::phc::PasswordHash;
use chrono::NaiveDateTime;
use diesel::prelude::{ExpressionMethods, OptionalExtension, QueryDsl};
use diesel_async::RunQueryDsl;
use hubuum_domain::PlaintextPassword;
use hubuum_storage_core::{
    StorageImportGroup, StorageImportGroupKey, StorageImportGroupMembership,
    StorageImportIdentityScope, StorageImportIdentityScopeKey, StorageImportMembershipSourceParts,
    StorageImportPrincipal, StorageImportPrincipalKey, StorageImportPrincipalParts,
    StorageImportPrincipalSubtype, StorageImportTimestamps, StorageImportWriteCondition,
};
use std::sync::{Arc, LazyLock};
use tokio::sync::Semaphore;

static PASSWORD_WORK_SEMAPHORE: LazyLock<Arc<Semaphore>> =
    LazyLock::new(|| Arc::new(Semaphore::new(PASSWORD_WORK_MAX_CONCURRENCY)));

const PASSWORD_WORK_MAX_CONCURRENCY: usize = 4;

pub(super) async fn execute_identity_scope(
    connection: &mut PostgresConnection,
    input: StorageImportIdentityScope,
    overwrite: bool,
) -> Result<Option<ImportReferenceChange>, PostgresStorageError> {
    let parts = input.into_parts();
    let reference = parts.reference.clone();
    let existing = crate::schema::identity_scopes::table
        .filter(crate::schema::identity_scopes::name.eq(&parts.name))
        .select((
            crate::schema::identity_scopes::id,
            crate::schema::identity_scopes::created_at,
            crate::schema::identity_scopes::updated_at,
            crate::schema::identity_scopes::revision,
        ))
        .for_update()
        .first::<(i32, NaiveDateTime, NaiveDateTime, PostgresRevision)>(connection)
        .await
        .optional()?;
    let identity_scope_id = match existing {
        Some((_, _, _, revision)) if !overwrite => {
            assert_import_revision(parts.condition, revision)?;
            return Err(PostgresStorageError::conflict(format!(
                "Identity scope '{}' already exists",
                parts.name
            )));
        }
        Some((id, existing_created_at, existing_updated_at, revision)) => {
            assert_import_revision(parts.condition, revision)?;
            let (created_at, updated_at) = parts
                .timestamps
                .map(import_timestamp_pair)
                .unwrap_or((existing_created_at, existing_updated_at));
            with_imported_timestamp_override(connection, async |connection| {
                updated_or_current(
                    diesel::update(
                        crate::schema::identity_scopes::table
                            .filter(crate::schema::identity_scopes::id.eq(id)),
                    )
                    .set((
                        crate::schema::identity_scopes::provider_kind.eq(parts.provider_kind),
                        crate::schema::identity_scopes::created_at.eq(created_at),
                        crate::schema::identity_scopes::updated_at.eq(updated_at),
                    ))
                    .returning(crate::schema::identity_scopes::id)
                    .get_result::<i32>(connection)
                    .await
                    .optional(),
                    async || {
                        crate::schema::identity_scopes::table
                            .filter(crate::schema::identity_scopes::id.eq(id))
                            .select(crate::schema::identity_scopes::id)
                            .first::<i32>(connection)
                            .await
                    },
                )
                .await
                .map_err(PostgresStorageError::from)
            })
            .await?
        }
        None => {
            assert_import_create_condition(parts.condition)?;
            let (created_at, updated_at) = imported_timestamps(parts.timestamps);
            diesel::insert_into(crate::schema::identity_scopes::table)
                .values((
                    crate::schema::identity_scopes::name.eq(parts.name),
                    crate::schema::identity_scopes::provider_kind.eq(parts.provider_kind),
                    crate::schema::identity_scopes::created_at.eq(created_at),
                    crate::schema::identity_scopes::updated_at.eq(updated_at),
                ))
                .returning(crate::schema::identity_scopes::id)
                .get_result::<i32>(connection)
                .await?
        }
    };
    Ok(reference
        .map(|reference| ImportReferenceChange::IdentityScope(reference, identity_scope_id)))
}

pub(super) async fn execute_group(
    connection: &mut PostgresConnection,
    state: &ImportRuntime,
    input: StorageImportGroup,
    overwrite: bool,
) -> Result<Option<ImportReferenceChange>, PostgresStorageError> {
    let parts = input.into_parts();
    let identity_scope_id = resolve_identity_scope(
        connection,
        state,
        parts.identity_scope_ref.as_deref(),
        parts.identity_scope_key.as_ref(),
    )
    .await?;
    let existing = crate::schema::groups::table
        .filter(crate::schema::groups::identity_scope_id.eq(identity_scope_id))
        .filter(crate::schema::groups::groupname.eq(&parts.name))
        .for_update()
        .first::<GroupRow>(connection)
        .await
        .optional()?;
    let group_id = match existing {
        Some(existing) if !overwrite => {
            assert_import_revision(parts.condition, existing.revision)?;
            return Err(PostgresStorageError::conflict(format!(
                "Group '{}' already exists in its identity scope",
                parts.name
            )));
        }
        Some(existing) => {
            assert_import_revision(parts.condition, existing.revision)?;
            let (created_at, updated_at) = parts
                .timestamps
                .map(import_timestamp_pair)
                .unwrap_or((existing.created_at, existing.updated_at));
            with_imported_timestamp_override(connection, async |connection| {
                updated_or_current(
                    diesel::update(
                        crate::schema::groups::table
                            .filter(crate::schema::groups::id.eq(existing.id)),
                    )
                    .set((
                        crate::schema::groups::description.eq(parts.description),
                        crate::schema::groups::managed_by.eq(parts.managed_by),
                        crate::schema::groups::external_key.eq(parts.external_key),
                        crate::schema::groups::last_sync_attempted_at.eq(parts
                            .last_sync_attempted_at
                            .map(|timestamp| timestamp.naive_utc())),
                        crate::schema::groups::last_sync_success_at.eq(parts
                            .last_sync_success_at
                            .map(|timestamp| timestamp.naive_utc())),
                        crate::schema::groups::created_at.eq(created_at),
                        crate::schema::groups::updated_at.eq(updated_at),
                    ))
                    .returning(crate::schema::groups::id)
                    .get_result::<i32>(connection)
                    .await
                    .optional(),
                    async || {
                        crate::schema::groups::table
                            .filter(crate::schema::groups::id.eq(existing.id))
                            .select(crate::schema::groups::id)
                            .first::<i32>(connection)
                            .await
                    },
                )
                .await
                .map_err(PostgresStorageError::from)
            })
            .await?
        }
        None => {
            assert_import_create_condition(parts.condition)?;
            let (created_at, updated_at) = imported_timestamps(parts.timestamps);
            diesel::insert_into(crate::schema::groups::table)
                .values((
                    crate::schema::groups::groupname.eq(parts.name),
                    crate::schema::groups::description.eq(parts.description),
                    crate::schema::groups::identity_scope_id.eq(identity_scope_id),
                    crate::schema::groups::managed_by.eq(parts.managed_by),
                    crate::schema::groups::external_key.eq(parts.external_key),
                    crate::schema::groups::last_sync_attempted_at.eq(parts
                        .last_sync_attempted_at
                        .map(|timestamp| timestamp.naive_utc())),
                    crate::schema::groups::last_sync_success_at.eq(parts
                        .last_sync_success_at
                        .map(|timestamp| timestamp.naive_utc())),
                    crate::schema::groups::created_at.eq(created_at),
                    crate::schema::groups::updated_at.eq(updated_at),
                ))
                .returning(crate::schema::groups::id)
                .get_result::<i32>(connection)
                .await?
        }
    };
    Ok(parts
        .reference
        .map(|reference| ImportReferenceChange::Group(reference, group_id)))
}

pub(super) async fn execute_principal(
    connection: &mut PostgresConnection,
    state: &ImportRuntime,
    input: StorageImportPrincipal,
    overwrite: bool,
) -> Result<Option<ImportReferenceChange>, PostgresStorageError> {
    let parts = input.into_parts();
    let identity_scope_id = resolve_identity_scope(
        connection,
        state,
        parts.identity_scope_ref.as_deref(),
        parts.identity_scope_key.as_ref(),
    )
    .await?;
    let (owner_group_id, created_by) = match &parts.subtype {
        StorageImportPrincipalSubtype::Human { .. } => (None, None),
        StorageImportPrincipalSubtype::ServiceAccount {
            owner_group_ref,
            owner_group_key,
            created_by_ref,
            created_by_key,
            ..
        } => (
            Some(
                resolve_group(
                    connection,
                    state,
                    owner_group_ref.as_deref(),
                    owner_group_key.as_ref(),
                )
                .await?,
            ),
            if created_by_ref.is_some() || created_by_key.is_some() {
                Some(
                    resolve_principal(
                        connection,
                        state,
                        created_by_ref.as_deref(),
                        created_by_key.as_ref(),
                    )
                    .await?,
                )
            } else {
                None
            },
        ),
    };
    let principal_id = upsert_principal(
        connection,
        &parts,
        identity_scope_id,
        owner_group_id,
        created_by,
        overwrite,
    )
    .await?;
    Ok(parts
        .reference
        .map(|reference| ImportReferenceChange::Principal(reference, principal_id)))
}

pub(super) async fn execute_group_membership(
    connection: &mut PostgresConnection,
    state: &ImportRuntime,
    input: StorageImportGroupMembership,
    overwrite: bool,
) -> Result<(), PostgresStorageError> {
    let parts = input.into_parts();
    let principal_id = resolve_principal(
        connection,
        state,
        parts.principal_ref.as_deref(),
        parts.principal_key.as_ref(),
    )
    .await?;
    let group_id = resolve_group(
        connection,
        state,
        parts.group_ref.as_deref(),
        parts.group_key.as_ref(),
    )
    .await?;
    let mut sources = Vec::with_capacity(parts.sources.len());
    for source in parts.sources {
        let source = source.into_parts();
        let scope_id = resolve_identity_scope(
            connection,
            state,
            source.source_scope_ref.as_deref(),
            source.source_scope_key.as_ref(),
        )
        .await?;
        sources.push((source, scope_id));
    }
    upsert_group_membership(
        connection,
        principal_id,
        group_id,
        &sources,
        parts.condition,
        parts.timestamps,
        overwrite,
    )
    .await
}

pub(super) async fn resolve_identity_scope(
    connection: &mut PostgresConnection,
    state: &ImportRuntime,
    reference: Option<&str>,
    key: Option<&StorageImportIdentityScopeKey>,
) -> Result<i32, PostgresStorageError> {
    if let Some(reference) = reference
        && let Some(id) = state.identity_scopes_by_ref.get(reference)
    {
        return Ok(*id);
    }
    let name = key
        .cloned()
        .map(StorageImportIdentityScopeKey::into_parts)
        .map(|parts| parts.name)
        .ok_or_else(|| {
            PostgresStorageError::invalid_input(
                "Identity-scope reference was not resolved and no identity_scope_key was supplied",
            )
        })?;
    crate::schema::identity_scopes::table
        .filter(crate::schema::identity_scopes::name.eq(name))
        .select(crate::schema::identity_scopes::id)
        .first::<i32>(connection)
        .await
        .optional()?
        .ok_or_else(|| PostgresStorageError::not_found("Identity scope was not found"))
}

pub(super) async fn resolve_group(
    connection: &mut PostgresConnection,
    state: &ImportRuntime,
    reference: Option<&str>,
    key: Option<&StorageImportGroupKey>,
) -> Result<i32, PostgresStorageError> {
    if let Some(reference) = reference
        && let Some(id) = state.groups_by_ref.get(reference)
    {
        return Ok(*id);
    }
    let parts = key
        .cloned()
        .map(StorageImportGroupKey::into_parts)
        .ok_or_else(|| {
            PostgresStorageError::invalid_input(
                "Group reference was not resolved and no group_key was supplied",
            )
        })?;
    let scope_name = parts.identity_scope;
    crate::schema::groups::table
        .inner_join(crate::schema::identity_scopes::table)
        .filter(crate::schema::identity_scopes::name.eq(scope_name))
        .filter(crate::schema::groups::groupname.eq(parts.name))
        .select(crate::schema::groups::id)
        .first::<i32>(connection)
        .await
        .optional()?
        .ok_or_else(|| PostgresStorageError::not_found("Group was not found"))
}

pub(super) async fn resolve_principal(
    connection: &mut PostgresConnection,
    state: &ImportRuntime,
    reference: Option<&str>,
    key: Option<&StorageImportPrincipalKey>,
) -> Result<i32, PostgresStorageError> {
    if let Some(reference) = reference
        && let Some(id) = state.principals_by_ref.get(reference)
    {
        return Ok(*id);
    }
    let parts = key
        .cloned()
        .map(StorageImportPrincipalKey::into_parts)
        .ok_or_else(|| {
            PostgresStorageError::invalid_input(
                "Principal reference was not resolved and no principal_key was supplied",
            )
        })?;
    let scope_name = parts.identity_scope;
    crate::schema::principals::table
        .inner_join(crate::schema::identity_scopes::table)
        .filter(crate::schema::identity_scopes::name.eq(scope_name))
        .filter(crate::schema::principals::name.eq(parts.name))
        .select(crate::schema::principals::id)
        .first::<i32>(connection)
        .await
        .optional()?
        .ok_or_else(|| PostgresStorageError::not_found("Principal was not found"))
}

async fn upsert_principal(
    connection: &mut PostgresConnection,
    parts: &StorageImportPrincipalParts,
    identity_scope_id: i32,
    owner_group_id: Option<i32>,
    created_by: Option<i32>,
    overwrite: bool,
) -> Result<i32, PostgresStorageError> {
    validate_principal_credentials(&parts.subtype)?;
    let supplied_password = match &parts.subtype {
        StorageImportPrincipalSubtype::Human {
            password: Some(password),
            password_hash: None,
            ..
        } => Some(
            hash_password_async(
                PlaintextPassword::new(password.clone())
                    .map_err(|error| PostgresStorageError::invalid_input(error.to_string()))?,
            )
            .await?,
        ),
        StorageImportPrincipalSubtype::Human {
            password: None,
            password_hash,
            ..
        } => password_hash.clone(),
        StorageImportPrincipalSubtype::Human {
            password: Some(_),
            password_hash: Some(_),
            ..
        } => unreachable!("credentials were validated above"),
        StorageImportPrincipalSubtype::ServiceAccount { .. } => None,
    };
    let expected_kind = match &parts.subtype {
        StorageImportPrincipalSubtype::Human { .. } => "human",
        StorageImportPrincipalSubtype::ServiceAccount { .. } => "service_account",
    };
    let existing = crate::schema::principals::table
        .filter(crate::schema::principals::identity_scope_id.eq(identity_scope_id))
        .filter(crate::schema::principals::name.eq(&parts.name))
        .for_update()
        .first::<PrincipalRow>(connection)
        .await
        .optional()?;
    if let Some(existing) = &existing {
        assert_import_revision(parts.condition, existing.revision)?;
        if !overwrite {
            return Err(PostgresStorageError::conflict(format!(
                "Principal '{}' already exists in its identity scope",
                parts.name
            )));
        }
        if existing.kind != expected_kind {
            return Err(PostgresStorageError::conflict(format!(
                "Principal '{}' exists with kind '{}' instead of '{expected_kind}'",
                parts.name, existing.kind
            )));
        }
    }
    let principal_id = match existing {
        Some(existing) => {
            let (created_at, updated_at) = parts
                .timestamps
                .map(import_timestamp_pair)
                .unwrap_or((existing.created_at, existing.updated_at));
            with_imported_timestamp_override(connection, async |connection| {
                updated_or_current(
                    diesel::update(
                        crate::schema::principals::table
                            .filter(crate::schema::principals::id.eq(existing.id)),
                    )
                    .set((
                        crate::schema::principals::provider_managed.eq(parts.provider_managed),
                        crate::schema::principals::settings.eq(&parts.settings),
                        crate::schema::principals::external_subject.eq(&parts.external_subject),
                        crate::schema::principals::last_sync_attempted_at.eq(parts
                            .last_sync_attempted_at
                            .map(|timestamp| timestamp.naive_utc())),
                        crate::schema::principals::last_sync_success_at.eq(parts
                            .last_sync_success_at
                            .map(|timestamp| timestamp.naive_utc())),
                        crate::schema::principals::created_at.eq(created_at),
                        crate::schema::principals::updated_at.eq(updated_at),
                    ))
                    .returning(crate::schema::principals::id)
                    .get_result::<i32>(connection)
                    .await
                    .optional(),
                    async || {
                        crate::schema::principals::table
                            .filter(crate::schema::principals::id.eq(existing.id))
                            .select(crate::schema::principals::id)
                            .first::<i32>(connection)
                            .await
                    },
                )
                .await
                .map_err(PostgresStorageError::from)
            })
            .await?
        }
        None => {
            assert_import_create_condition(parts.condition)?;
            let (created_at, updated_at) = imported_timestamps(parts.timestamps);
            diesel::insert_into(crate::schema::principals::table)
                .values((
                    crate::schema::principals::kind.eq(expected_kind),
                    crate::schema::principals::name.eq(&parts.name),
                    crate::schema::principals::identity_scope_id.eq(identity_scope_id),
                    crate::schema::principals::provider_managed.eq(parts.provider_managed),
                    crate::schema::principals::settings.eq(&parts.settings),
                    crate::schema::principals::external_subject.eq(&parts.external_subject),
                    crate::schema::principals::last_sync_attempted_at.eq(parts
                        .last_sync_attempted_at
                        .map(|timestamp| timestamp.naive_utc())),
                    crate::schema::principals::last_sync_success_at.eq(parts
                        .last_sync_success_at
                        .map(|timestamp| timestamp.naive_utc())),
                    crate::schema::principals::created_at.eq(created_at),
                    crate::schema::principals::updated_at.eq(updated_at),
                ))
                .returning(crate::schema::principals::id)
                .get_result::<i32>(connection)
                .await?
        }
    };

    match &parts.subtype {
        StorageImportPrincipalSubtype::Human {
            proper_name,
            email,
            anonymized_at,
            ..
        } => {
            let existing = crate::schema::users::table
                .filter(crate::schema::users::id.eq(principal_id))
                .first::<UserRow>(connection)
                .await
                .optional()?;
            let (created_at, updated_at) = parts
                .timestamps
                .map(import_timestamp_pair)
                .or_else(|| {
                    existing
                        .as_ref()
                        .map(|row| (row.created_at, row.updated_at))
                })
                .unwrap_or_else(|| imported_timestamps(None));
            if let Some(existing) = existing {
                if supplied_password.is_some() || anonymized_at.is_some() {
                    auth_sessions::revoke_principal(connection, principal_id).await?;
                    diesel::update(
                        crate::schema::tokens::table
                            .filter(crate::schema::tokens::principal_id.eq(principal_id))
                            .filter(crate::schema::tokens::revoked_at.is_null()),
                    )
                    .set(crate::schema::tokens::revoked_at.eq(diesel::dsl::now))
                    .execute(connection)
                    .await?;
                }
                with_imported_timestamp_override(connection, async |connection| {
                    diesel::update(
                        crate::schema::users::table
                            .filter(crate::schema::users::id.eq(principal_id)),
                    )
                    .set((
                        crate::schema::users::password.eq(supplied_password.or(existing.password)),
                        crate::schema::users::proper_name.eq(proper_name),
                        crate::schema::users::email.eq(email),
                        crate::schema::users::anonymized_at
                            .eq(anonymized_at.map(|timestamp| timestamp.naive_utc())),
                        crate::schema::users::created_at.eq(created_at),
                        crate::schema::users::updated_at.eq(updated_at),
                    ))
                    .execute(connection)
                    .await?;
                    Ok(())
                })
                .await?;
            } else {
                diesel::insert_into(crate::schema::users::table)
                    .values((
                        crate::schema::users::id.eq(principal_id),
                        crate::schema::users::kind.eq("human"),
                        crate::schema::users::password.eq(supplied_password),
                        crate::schema::users::proper_name.eq(proper_name),
                        crate::schema::users::email.eq(email),
                        crate::schema::users::anonymized_at
                            .eq(anonymized_at.map(|timestamp| timestamp.naive_utc())),
                        crate::schema::users::created_at.eq(created_at),
                        crate::schema::users::updated_at.eq(updated_at),
                    ))
                    .execute(connection)
                    .await?;
            }
        }
        StorageImportPrincipalSubtype::ServiceAccount {
            description,
            disabled_at,
            ..
        } => {
            let owner_group_id = owner_group_id.ok_or_else(|| {
                PostgresStorageError::invalid_input(
                    "Service-account import requires an owner group",
                )
            })?;
            let existing = crate::schema::service_accounts::table
                .filter(crate::schema::service_accounts::id.eq(principal_id))
                .first::<ServiceAccountRow>(connection)
                .await
                .optional()?;
            let (created_at, updated_at) = parts
                .timestamps
                .map(import_timestamp_pair)
                .or_else(|| {
                    existing
                        .as_ref()
                        .map(|row| (row.created_at, row.updated_at))
                })
                .unwrap_or_else(|| imported_timestamps(None));
            if existing.is_some() {
                with_imported_timestamp_override(connection, async |connection| {
                    diesel::update(
                        crate::schema::service_accounts::table
                            .filter(crate::schema::service_accounts::id.eq(principal_id)),
                    )
                    .set((
                        crate::schema::service_accounts::description.eq(description),
                        crate::schema::service_accounts::owner_group_id.eq(owner_group_id),
                        crate::schema::service_accounts::created_by.eq(created_by),
                        crate::schema::service_accounts::disabled_at
                            .eq(disabled_at.map(|timestamp| timestamp.naive_utc())),
                        crate::schema::service_accounts::created_at.eq(created_at),
                        crate::schema::service_accounts::updated_at.eq(updated_at),
                    ))
                    .execute(connection)
                    .await?;
                    Ok(())
                })
                .await?;
            } else {
                diesel::insert_into(crate::schema::service_accounts::table)
                    .values((
                        crate::schema::service_accounts::id.eq(principal_id),
                        crate::schema::service_accounts::kind.eq("service_account"),
                        crate::schema::service_accounts::description.eq(description),
                        crate::schema::service_accounts::owner_group_id.eq(owner_group_id),
                        crate::schema::service_accounts::created_by.eq(created_by),
                        crate::schema::service_accounts::disabled_at
                            .eq(disabled_at.map(|timestamp| timestamp.naive_utc())),
                        crate::schema::service_accounts::created_at.eq(created_at),
                        crate::schema::service_accounts::updated_at.eq(updated_at),
                    ))
                    .execute(connection)
                    .await?;
            }
        }
    }
    Ok(principal_id)
}

async fn upsert_group_membership(
    connection: &mut PostgresConnection,
    principal_id: i32,
    group_id: i32,
    sources: &[(StorageImportMembershipSourceParts, i32)],
    condition: Option<StorageImportWriteCondition>,
    timestamps: Option<StorageImportTimestamps>,
    overwrite: bool,
) -> Result<(), PostgresStorageError> {
    let existing = crate::schema::group_memberships::table
        .filter(crate::schema::group_memberships::principal_id.eq(principal_id))
        .filter(crate::schema::group_memberships::group_id.eq(group_id))
        .select((
            crate::schema::group_memberships::created_at,
            crate::schema::group_memberships::updated_at,
            crate::schema::group_memberships::revision,
        ))
        .for_update()
        .first::<(NaiveDateTime, NaiveDateTime, PostgresRevision)>(connection)
        .await
        .optional()?;
    if let Some((_, _, revision)) = existing {
        assert_import_revision(condition, revision)?;
        if !overwrite {
            return Err(PostgresStorageError::conflict(format!(
                "Principal {principal_id} is already a member of group {group_id}"
            )));
        }
    } else {
        assert_import_create_condition(condition)?;
    }
    with_imported_timestamp_override(connection, async |connection| {
        let membership_timestamps = timestamps
            .map(import_timestamp_pair)
            .or(existing.map(|(created, updated, _)| (created, updated)))
            .unwrap_or_else(|| imported_timestamps(None));
        if existing.is_some() {
            diesel::update(
                crate::schema::group_memberships::table
                    .filter(crate::schema::group_memberships::principal_id.eq(principal_id))
                    .filter(crate::schema::group_memberships::group_id.eq(group_id)),
            )
            .set((
                crate::schema::group_memberships::created_at.eq(membership_timestamps.0),
                crate::schema::group_memberships::updated_at.eq(membership_timestamps.1),
            ))
            .execute(connection)
            .await?;
        } else {
            diesel::insert_into(crate::schema::group_memberships::table)
                .values((
                    crate::schema::group_memberships::principal_id.eq(principal_id),
                    crate::schema::group_memberships::group_id.eq(group_id),
                    crate::schema::group_memberships::created_at.eq(membership_timestamps.0),
                    crate::schema::group_memberships::updated_at.eq(membership_timestamps.1),
                ))
                .execute(connection)
                .await?;
        }
        for (source, source_scope_id) in sources {
            let existing_source = crate::schema::group_membership_sources::table
                .filter(crate::schema::group_membership_sources::principal_id.eq(principal_id))
                .filter(crate::schema::group_membership_sources::group_id.eq(group_id))
                .filter(crate::schema::group_membership_sources::source.eq(&source.source))
                .filter(
                    crate::schema::group_membership_sources::source_scope_id.eq(*source_scope_id),
                )
                .filter(crate::schema::group_membership_sources::source_key.eq(&source.source_key))
                .select((
                    crate::schema::group_membership_sources::created_at,
                    crate::schema::group_membership_sources::updated_at,
                ))
                .first::<(NaiveDateTime, NaiveDateTime)>(connection)
                .await
                .optional()?;
            let source_timestamps = source
                .timestamps
                .map(import_timestamp_pair)
                .or(existing_source)
                .unwrap_or_else(|| imported_timestamps(None));
            if existing_source.is_some() {
                diesel::update(
                    crate::schema::group_membership_sources::table
                        .filter(
                            crate::schema::group_membership_sources::principal_id.eq(principal_id),
                        )
                        .filter(crate::schema::group_membership_sources::group_id.eq(group_id))
                        .filter(crate::schema::group_membership_sources::source.eq(&source.source))
                        .filter(
                            crate::schema::group_membership_sources::source_scope_id
                                .eq(*source_scope_id),
                        )
                        .filter(
                            crate::schema::group_membership_sources::source_key
                                .eq(&source.source_key),
                        ),
                )
                .set((
                    crate::schema::group_membership_sources::created_at.eq(source_timestamps.0),
                    crate::schema::group_membership_sources::updated_at.eq(source_timestamps.1),
                ))
                .execute(connection)
                .await?;
            } else {
                diesel::insert_into(crate::schema::group_membership_sources::table)
                    .values((
                        crate::schema::group_membership_sources::principal_id.eq(principal_id),
                        crate::schema::group_membership_sources::group_id.eq(group_id),
                        crate::schema::group_membership_sources::source.eq(&source.source),
                        crate::schema::group_membership_sources::source_scope_id
                            .eq(*source_scope_id),
                        crate::schema::group_membership_sources::source_key.eq(&source.source_key),
                        crate::schema::group_membership_sources::created_at.eq(source_timestamps.0),
                        crate::schema::group_membership_sources::updated_at.eq(source_timestamps.1),
                    ))
                    .execute(connection)
                    .await?;
            }
        }
        Ok(())
    })
    .await
}

fn validate_principal_credentials(
    subtype: &StorageImportPrincipalSubtype,
) -> Result<(), PostgresStorageError> {
    let StorageImportPrincipalSubtype::Human {
        password,
        password_hash,
        ..
    } = subtype
    else {
        return Ok(());
    };
    if password.is_some() && password_hash.is_some() {
        return Err(PostgresStorageError::invalid_input(
            "A human principal import accepts password or password_hash, not both",
        ));
    }
    if let Some(password) = password {
        PlaintextPassword::new(password.clone())
            .map_err(|error| PostgresStorageError::invalid_input(error.to_string()))?;
    }
    if let Some(hash) = password_hash {
        let parsed = PasswordHash::new(hash).map_err(|_| {
            PostgresStorageError::invalid_input(
                "Imported password_hash must be a valid Argon2 password hash",
            )
        })?;
        if !matches!(
            parsed.algorithm.as_str(),
            "argon2d" | "argon2i" | "argon2id"
        ) {
            return Err(PostgresStorageError::invalid_input(
                "Imported password_hash must use an Argon2 algorithm",
            ));
        }
    }
    Ok(())
}

async fn hash_password_async(password: PlaintextPassword) -> Result<String, PostgresStorageError> {
    let permit = PASSWORD_WORK_SEMAPHORE
        .clone()
        .acquire_owned()
        .await
        .map_err(|error| {
            PostgresStorageError::internal(format!("Password worker is unavailable: {error}"))
        })?;
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        Argon2::default()
            .hash_password(password.as_str().as_bytes())
            .map(|hash| hash.to_string())
            .map_err(|error| PostgresStorageError::internal(error.to_string()))
    })
    .await
    .map_err(|error| PostgresStorageError::internal(format!("Password worker failed: {error}")))?
}
