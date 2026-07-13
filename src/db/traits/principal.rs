use hubuum_events_core::EventContext;
use serde_json::json;

use crate::db::prelude::*;
use crate::db::{DbConnection, DbPool, with_connection, with_transaction};
use crate::errors::ApiError;
use crate::events::{Action, EntityType, NewEvent, emit_event};
use crate::models::{NewPrincipal, Principal, PrincipalKind, PrincipalSettings, User};

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

pub async fn load_principal_settings(
    pool: &DbPool,
    principal_id_value: i32,
) -> Result<PrincipalSettings, ApiError> {
    use crate::schema::principals::dsl::{id, principals as principals_table, settings};

    let value = with_connection(pool, async |conn| {
        principals_table
            .filter(id.eq(principal_id_value))
            .select(settings)
            .first::<serde_json::Value>(conn)
            .await
    })
    .await?;
    stored_principal_settings(principal_id_value, value)
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
) -> Result<PrincipalSettings, ApiError> {
    use crate::schema::principals;

    with_transaction(pool, async |conn| -> Result<PrincipalSettings, ApiError> {
        let (kind, name, stored_before) = principals::table
            .filter(principals::id.eq(principal_id_value))
            .select((principals::kind, principals::name, principals::settings))
            .for_update()
            .first::<(String, String, serde_json::Value)>(conn)
            .await?;
        let before = stored_principal_settings(principal_id_value, stored_before)?;
        let after = match mutation {
            PrincipalSettingsMutation::Replace => input,
            PrincipalSettingsMutation::Patch => before.clone().merge_patch(&input),
            PrincipalSettingsMutation::Reset => PrincipalSettings::default(),
        };

        if before == after {
            return Ok(after);
        }

        diesel::update(principals::table.filter(principals::id.eq(principal_id_value)))
            .set(principals::settings.eq(after.as_value()))
            .execute(conn)
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
        .with_before(json!({ "settings": before }))
        .with_after(json!({ "settings": after }));
        emit_event(conn, &event).await?;

        Ok(after)
    })
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

pub struct PrincipalIdentityMetadata {
    pub identity_scope: String,
    pub provider_kind: String,
    pub name: String,
    pub provider_managed: bool,
    pub last_sync_attempted_at: Option<chrono::NaiveDateTime>,
    pub last_sync_success_at: Option<chrono::NaiveDateTime>,
}

pub async fn principal_identity_scope_and_name(
    pool: &DbPool,
    principal_id_value: i32,
) -> Result<(String, String), ApiError> {
    use crate::schema::{identity_scopes, principals};

    with_connection(pool, async |conn| {
        principals::table
            .inner_join(identity_scopes::table)
            .filter(principals::id.eq(principal_id_value))
            .select((identity_scopes::name, principals::name))
            .first::<(String, String)>(conn)
            .await
    })
    .await
}

pub async fn principal_identity_metadata(
    pool: &DbPool,
    principal_id_value: i32,
) -> Result<PrincipalIdentityMetadata, ApiError> {
    use crate::schema::{identity_scopes, principals};

    let (
        identity_scope,
        provider_kind,
        name,
        provider_managed,
        last_sync_attempted_at,
        last_sync_success_at,
    ) = with_connection(pool, async |conn| {
        principals::table
            .inner_join(identity_scopes::table)
            .filter(principals::id.eq(principal_id_value))
            .select((
                identity_scopes::name,
                identity_scopes::provider_kind,
                principals::name,
                principals::provider_managed,
                principals::last_sync_attempted_at,
                principals::last_sync_success_at,
            ))
            .first::<(
                String,
                String,
                String,
                bool,
                Option<chrono::NaiveDateTime>,
                Option<chrono::NaiveDateTime>,
            )>(conn)
            .await
    })
    .await?;

    Ok(PrincipalIdentityMetadata {
        identity_scope,
        provider_kind,
        name,
        provider_managed,
        last_sync_attempted_at,
        last_sync_success_at,
    })
}
