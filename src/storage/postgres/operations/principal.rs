use hubuum_events_core::EventContext;
use serde_json::{Value, json};

use crate::api::etag::RevisionOwner;
use crate::errors::ApiError;
use crate::events::{Action, EntityType, NewEvent, emit_event};
use crate::models::{
    NewPrincipal, Principal, PrincipalKind, PrincipalSettings, PrincipalSettingsPatch,
    PrincipalSettingsResponse, ResourceRevision, ServiceAccount, ServiceAccountPointResponse, User,
    UserPointResponse, UserResponse, UserWithName,
};
use crate::storage::postgres::prelude::*;
use crate::storage::postgres::{
    PostgresConnection, assert_locked_revision_precondition, with_connection, with_transaction,
};

pub trait InsertPrincipalRecord {
    /// Insert the principal row and return it (principal-first id allocation).
    async fn insert(&self, conn: &mut PostgresConnection) -> Result<Principal, ApiError>;
}

impl InsertPrincipalRecord for NewPrincipal<'_> {
    async fn insert(&self, conn: &mut PostgresConnection) -> Result<Principal, ApiError> {
        use crate::schema::principals;

        diesel::insert_into(principals::table)
            .values((
                principals::identity_scope_id.eq(self.identity_scope_id),
                principals::kind.eq(self.kind),
                principals::name.eq(self.name),
            ))
            .get_result::<Principal>(conn)
            .await
            .map_err(ApiError::from)
    }
}

pub async fn load_principal_by_id(
    pool: &impl crate::storage::StorageContext,
    principal_id_value: i32,
) -> Result<Principal, ApiError> {
    use crate::schema::principals::dsl::{id, principals as principals_table};
    with_connection(pool, async |conn| {
        principals_table
            .filter(id.eq(principal_id_value))
            .first::<Principal>(conn)
            .await
    })
    .await
}

pub(crate) async fn principal_revision_conn(
    conn: &mut PostgresConnection,
    principal_id_value: i32,
) -> Result<crate::models::ResourceRevision, ApiError> {
    use crate::schema::principals::dsl::{id, principals, revision};

    Ok(principals
        .filter(id.eq(principal_id_value))
        .select(revision)
        .first(conn)
        .await?)
}

pub(crate) async fn lock_principal_revision_conn(
    conn: &mut PostgresConnection,
    principal_id_value: i32,
) -> Result<crate::models::ResourceRevision, ApiError> {
    use crate::schema::principals::dsl::{id, principals, revision};

    let owner_revision = principals
        .filter(id.eq(principal_id_value))
        .select(revision)
        .for_update()
        .first(conn)
        .await?;
    crate::storage::postgres::assert_locked_revision_precondition(
        conn,
        &RevisionOwner::Principal.key(principal_id_value),
        owner_revision,
    )
    .await?;
    Ok(owner_revision)
}

pub async fn load_principal_settings(
    pool: &impl crate::storage::StorageContext,
    principal_id_value: i32,
) -> Result<PrincipalSettingsResponse, ApiError> {
    use crate::schema::principals::dsl::{id, principals as principals_table, revision, settings};

    let (value, stored_revision) = with_connection(pool, async |conn| {
        principals_table
            .filter(id.eq(principal_id_value))
            .select((settings, revision))
            .first::<(serde_json::Value, crate::models::ResourceRevision)>(conn)
            .await
    })
    .await?;
    Ok(PrincipalSettingsResponse::new(
        principal_id_value,
        stored_revision,
        stored_principal_settings(principal_id_value, value)?,
    ))
}

#[derive(Debug, Clone, Copy)]
pub enum PrincipalSettingsMutation {
    Replace,
    Patch,
    Reset,
}

pub async fn mutate_principal_settings(
    pool: &impl crate::storage::StorageContext,
    principal_id_value: i32,
    mutation: PrincipalSettingsMutation,
    input: PrincipalSettings,
    event_context: &EventContext,
) -> Result<PrincipalSettingsResponse, ApiError> {
    let mutation = match mutation {
        PrincipalSettingsMutation::Replace => PrincipalSettingsWrite::Replace(input),
        PrincipalSettingsMutation::Patch => {
            PrincipalSettingsWrite::Patch(PrincipalSettingsPatch::MergePatch(input))
        }
        PrincipalSettingsMutation::Reset => PrincipalSettingsWrite::Reset,
    };
    write_principal_settings(pool, principal_id_value, mutation, event_context).await
}

pub(crate) async fn apply_principal_settings_patch(
    pool: &impl crate::storage::StorageContext,
    principal_id_value: i32,
    patch: PrincipalSettingsPatch,
    event_context: &EventContext,
) -> Result<PrincipalSettingsResponse, ApiError> {
    write_principal_settings(
        pool,
        principal_id_value,
        PrincipalSettingsWrite::Patch(patch),
        event_context,
    )
    .await
}

enum PrincipalSettingsWrite {
    Replace(PrincipalSettings),
    Patch(PrincipalSettingsPatch),
    Reset,
}

impl PrincipalSettingsWrite {
    fn apply(self, before: &PrincipalSettings) -> Result<PrincipalSettings, ApiError> {
        match self {
            Self::Replace(settings) => Ok(settings),
            Self::Patch(patch) => patch.apply(before),
            Self::Reset => Ok(PrincipalSettings::default()),
        }
    }
}

async fn write_principal_settings(
    pool: &impl crate::storage::StorageContext,
    principal_id_value: i32,
    mutation: PrincipalSettingsWrite,
    event_context: &EventContext,
) -> Result<PrincipalSettingsResponse, ApiError> {
    use crate::schema::principals;

    with_transaction(
        pool,
        async |conn| -> Result<PrincipalSettingsResponse, ApiError> {
            let (kind, name, stored_before, before_revision) = principals::table
                .filter(principals::id.eq(principal_id_value))
                .select((
                    principals::kind,
                    principals::name,
                    principals::settings,
                    principals::revision,
                ))
                .for_update()
                .first::<(String, String, Value, ResourceRevision)>(conn)
                .await?;
            assert_locked_revision_precondition(
                conn,
                &RevisionOwner::Principal.key(principal_id_value),
                before_revision,
            )
            .await?;
            let before = stored_principal_settings(principal_id_value, stored_before)?;
            let after = mutation.apply(&before)?;

            if before == after {
                return Ok(PrincipalSettingsResponse::new(
                    principal_id_value,
                    before_revision,
                    after,
                ));
            }

            let after_revision =
                diesel::update(principals::table.filter(principals::id.eq(principal_id_value)))
                    .set(principals::settings.eq(after.as_value()))
                    .returning(principals::revision)
                    .get_result::<ResourceRevision>(conn)
                    .await?;

            let entity_type = match PrincipalKind::from_db(&kind)? {
                PrincipalKind::Human => EntityType::User,
                PrincipalKind::ServiceAccount => EntityType::ServiceAccount,
            };
            let event = NewEvent::new(
                entity_type,
                Action::Updated,
                event_context.actor_kind(),
                format!("Principal settings for '{name}' updated"),
            )?
            .with_context(event_context)
            .with_entity_id(principal_id_value)
            .with_entity_name(name)
            .with_before(json!({ "revision": before_revision, "settings": before }))
            .with_after(json!({ "revision": after_revision, "settings": after }));
            emit_event(conn, &event).await?;

            Ok(PrincipalSettingsResponse::new(
                principal_id_value,
                after_revision,
                after,
            ))
        },
    )
    .await
}

fn stored_principal_settings(
    principal_id_value: i32,
    value: Value,
) -> Result<PrincipalSettings, ApiError> {
    PrincipalSettings::new(value).map_err(|_| {
        ApiError::InternalServerError(format!(
            "Principal '{principal_id_value}' has invalid settings in the database"
        ))
    })
}

/// Load the rich, untagged user representation in one database snapshot.
pub(crate) async fn load_user_response(
    pool: &impl crate::storage::StorageContext,
    user_id_value: i32,
) -> Result<UserResponse, ApiError> {
    use crate::schema::{identity_scopes, principals, users};

    let row = with_connection(pool, async |conn| {
        users::table
            .inner_join(principals::table.on(principals::id.eq(users::id)))
            .inner_join(
                identity_scopes::table.on(identity_scopes::id.eq(principals::identity_scope_id)),
            )
            .filter(users::id.eq(user_id_value))
            .select((
                User::as_select(),
                identity_scopes::name,
                identity_scopes::provider_kind,
                principals::name,
                principals::provider_managed,
                principals::last_sync_attempted_at,
                principals::last_sync_success_at,
                principals::revision,
            ))
            .first(conn)
            .await
    })
    .await?;

    Ok(UserResponse::from(UserWithName::from_tuple(row)))
}

/// Load the user point body and its validator revision in one SQL statement.
pub(crate) async fn load_user_point_response(
    pool: &impl crate::storage::StorageContext,
    user_id_value: i32,
) -> Result<UserPointResponse, ApiError> {
    use crate::schema::{principals, users};

    let (user, identity_scope_id, name, provider_managed, revision) =
        with_connection(pool, async |conn| {
            users::table
                .inner_join(principals::table.on(principals::id.eq(users::id)))
                .filter(users::id.eq(user_id_value))
                .select((
                    User::as_select(),
                    principals::identity_scope_id,
                    principals::name,
                    principals::provider_managed,
                    principals::revision,
                ))
                .first(conn)
                .await
        })
        .await?;

    Ok(UserPointResponse::from_parts(
        user,
        identity_scope_id,
        name,
        provider_managed,
        revision,
    ))
}

/// Load the service-account point body and revision in one SQL statement.
pub(crate) async fn load_service_account_point_response(
    pool: &impl crate::storage::StorageContext,
    service_account_id_value: i32,
) -> Result<ServiceAccountPointResponse, ApiError> {
    use crate::schema::{principals, service_accounts};

    let (service_account, identity_scope_id, name, revision) =
        with_connection(pool, async |conn| {
            service_accounts::table
                .inner_join(principals::table.on(principals::id.eq(service_accounts::id)))
                .filter(service_accounts::id.eq(service_account_id_value))
                .select((
                    ServiceAccount::as_select(),
                    principals::identity_scope_id,
                    principals::name,
                    principals::revision,
                ))
                .first(conn)
                .await
        })
        .await?;

    Ok(ServiceAccountPointResponse::from_parts(
        service_account,
        identity_scope_id,
        name,
        revision,
    ))
}
