use hubuum_events_core::EventContext;
use serde_json::json;

use crate::api::etag::RevisionOwner;
use crate::db::prelude::*;
use crate::db::{DbConnection, DbPool, with_connection, with_transaction};
use crate::errors::ApiError;
use crate::events::{Action, EntityType, NewEvent, emit_event};
use crate::models::{
    NewPrincipal, Principal, PrincipalKind, PrincipalSettings, PrincipalSettingsResponse,
    ServiceAccount, ServiceAccountPointResponse, User, UserPointResponse, UserResponse,
    UserWithName,
};

pub trait InsertPrincipalRecord {
    /// Insert the principal row and return it (principal-first id allocation).
    async fn insert(&self, conn: &mut DbConnection) -> Result<Principal, ApiError>;
}

impl InsertPrincipalRecord for NewPrincipal<'_> {
    async fn insert(&self, conn: &mut DbConnection) -> Result<Principal, ApiError> {
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
    pool: &DbPool,
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
    conn: &mut DbConnection,
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
    conn: &mut DbConnection,
    principal_id_value: i32,
) -> Result<crate::models::ResourceRevision, ApiError> {
    use crate::schema::principals::dsl::{id, principals, revision};

    let owner_revision = principals
        .filter(id.eq(principal_id_value))
        .select(revision)
        .for_update()
        .first(conn)
        .await?;
    crate::db::assert_locked_revision_precondition(
        conn,
        &RevisionOwner::Principal.key(principal_id_value),
        owner_revision,
    )
    .await?;
    Ok(owner_revision)
}

pub async fn load_principal_settings(
    pool: &DbPool,
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
    pool: &DbPool,
    principal_id_value: i32,
    mutation: PrincipalSettingsMutation,
    input: PrincipalSettings,
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
                .first::<(
                    String,
                    String,
                    serde_json::Value,
                    crate::models::ResourceRevision,
                )>(conn)
                .await?;
            crate::db::assert_locked_revision_precondition(
                conn,
                &RevisionOwner::Principal.key(principal_id_value),
                before_revision,
            )
            .await?;
            let before = stored_principal_settings(principal_id_value, stored_before)?;
            let after = match mutation {
                PrincipalSettingsMutation::Replace => input,
                PrincipalSettingsMutation::Patch => before.clone().merge_patch(&input),
                PrincipalSettingsMutation::Reset => PrincipalSettings::default(),
            };

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
                    .get_result::<crate::models::ResourceRevision>(conn)
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
    value: serde_json::Value,
) -> Result<PrincipalSettings, ApiError> {
    PrincipalSettings::new(value).map_err(|_| {
        ApiError::InternalServerError(format!(
            "Principal '{principal_id_value}' has invalid settings in the database"
        ))
    })
}

/// Load a principal and, when it is human, its `users` row in one left-joined
/// query. A service account simply has no `users` row, so the user is `None`.
pub async fn load_principal_with_user(
    pool: &DbPool,
    principal_id_value: i32,
) -> Result<(Principal, Option<User>), ApiError> {
    use crate::schema::{principals, users};

    with_connection(pool, async |conn| {
        principals::table
            .left_join(users::table.on(users::id.eq(principals::id)))
            .filter(principals::id.eq(principal_id_value))
            .select((Principal::as_select(), Option::<User>::as_select()))
            .first::<(Principal, Option<User>)>(conn)
            .await
    })
    .await
}

/// Load the rich, untagged user representation in one database snapshot.
pub(crate) async fn load_user_response(
    pool: &DbPool,
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
    pool: &DbPool,
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
    pool: &DbPool,
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
