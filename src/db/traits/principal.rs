use diesel::prelude::*;

use crate::db::{DbPool, with_connection};
use crate::errors::ApiError;
use crate::models::{NewPrincipal, Principal, User};

pub trait InsertPrincipalRecord {
    /// Insert the principal row and return it (principal-first id allocation).
    fn insert(&self, conn: &mut PgConnection) -> Result<Principal, ApiError>;
}

impl InsertPrincipalRecord for NewPrincipal<'_> {
    fn insert(&self, conn: &mut PgConnection) -> Result<Principal, ApiError> {
        use crate::schema::principals;

        diesel::insert_into(principals::table)
            .values((
                principals::identity_scope_id.eq(self.identity_scope_id),
                principals::kind.eq(self.kind),
                principals::name.eq(self.name),
            ))
            .get_result::<Principal>(conn)
            .map_err(ApiError::from)
    }
}

pub async fn load_principal_by_id(
    pool: &DbPool,
    principal_id_value: i32,
) -> Result<Principal, ApiError> {
    use crate::schema::principals::dsl::{id, principals as principals_table};
    with_connection(pool, |conn| {
        principals_table
            .filter(id.eq(principal_id_value))
            .first::<Principal>(conn)
    })
}

/// Load a principal and, when it is human, its `users` row in one left-joined
/// query. A service account simply has no `users` row, so the user is `None`.
pub async fn load_principal_with_user(
    pool: &DbPool,
    principal_id_value: i32,
) -> Result<(Principal, Option<User>), ApiError> {
    use crate::schema::{principals, users};

    with_connection(pool, |conn| {
        principals::table
            .left_join(users::table.on(users::id.eq(principals::id)))
            .filter(principals::id.eq(principal_id_value))
            .select((Principal::as_select(), Option::<User>::as_select()))
            .first::<(Principal, Option<User>)>(conn)
    })
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

    with_connection(pool, |conn| {
        principals::table
            .inner_join(identity_scopes::table)
            .filter(principals::id.eq(principal_id_value))
            .select((identity_scopes::name, principals::name))
            .first::<(String, String)>(conn)
    })
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
    ) = with_connection(pool, |conn| {
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
    })?;

    Ok(PrincipalIdentityMetadata {
        identity_scope,
        provider_kind,
        name,
        provider_managed,
        last_sync_attempted_at,
        last_sync_success_at,
    })
}
