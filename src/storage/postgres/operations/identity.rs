use std::collections::{HashMap, HashSet};

use crate::errors::ApiError;
use crate::models::IdentityScope;
use crate::schema::identity_scopes;
use crate::storage::postgres::prelude::*;
use crate::storage::postgres::{PostgresConnection, with_connection};

#[derive(Debug, Queryable, Selectable)]
#[diesel(table_name = crate::schema::identity_scopes)]
pub(crate) struct IdentityScopeRow {
    pub(crate) id: i32,
    pub(crate) name: String,
    pub(crate) provider_kind: String,
    pub(crate) created_at: chrono::NaiveDateTime,
    pub(crate) updated_at: chrono::NaiveDateTime,
    pub(crate) revision: PostgresRevision,
}

impl From<IdentityScopeRow> for IdentityScope {
    fn from(row: IdentityScopeRow) -> Self {
        Self {
            id: row.id,
            name: row.name,
            provider_kind: row.provider_kind,
            created_at: row.created_at,
            updated_at: row.updated_at,
            revision: row.revision.into_domain(),
        }
    }
}

#[derive(Insertable)]
#[diesel(table_name = crate::schema::identity_scopes)]
struct NewIdentityScopeRow<'a> {
    name: &'a str,
    provider_kind: &'a str,
}

pub(crate) async fn identity_scope_id_by_name_conn(
    conn: &mut PostgresConnection,
    scope_name: &str,
) -> Result<i32, ApiError> {
    use crate::schema::identity_scopes::dsl::{identity_scopes as scopes, name};
    scopes
        .filter(name.eq(scope_name))
        .select(identity_scopes::id)
        .first::<i32>(conn)
        .await
        .map_err(ApiError::from)
}

pub async fn identity_scope_by_name(
    pool: &crate::storage::postgres::PostgresPool,
    scope_name: &str,
) -> Result<IdentityScope, ApiError> {
    use crate::schema::identity_scopes::dsl::{identity_scopes as scopes, name};
    with_connection(pool, async |conn| {
        scopes
            .filter(name.eq(scope_name))
            .first::<IdentityScopeRow>(conn)
            .await
            .map(Into::into)
    })
    .await
}

pub(crate) async fn identity_scope_name_by_id(
    pool: &crate::storage::postgres::PostgresPool,
    scope_id: i32,
) -> Result<String, ApiError> {
    with_connection(pool, async |conn| {
        identity_scopes::table
            .filter(identity_scopes::id.eq(scope_id))
            .select(identity_scopes::name)
            .first::<String>(conn)
            .await
    })
    .await
}

pub async fn identity_scope_names_by_ids(
    pool: &crate::storage::postgres::PostgresPool,
    scope_ids: &[i32],
) -> Result<HashMap<i32, String>, ApiError> {
    if scope_ids.is_empty() {
        return Ok(HashMap::new());
    }

    let unique_ids = scope_ids.iter().copied().collect::<HashSet<_>>();
    let query_ids = unique_ids.iter().copied().collect::<Vec<_>>();
    let rows = with_connection(pool, async |conn| {
        identity_scopes::table
            .filter(identity_scopes::id.eq_any(&query_ids))
            .select((identity_scopes::id, identity_scopes::name))
            .load::<(i32, String)>(conn)
            .await
    })
    .await?;
    if rows.len() != unique_ids.len() {
        return Err(ApiError::InternalServerError(
            "One or more identity scopes could not be resolved".to_string(),
        ));
    }

    Ok(rows.into_iter().collect())
}

pub async fn ensure_identity_scope(
    pool: &crate::storage::postgres::PostgresPool,
    scope_name: &str,
    provider: &str,
) -> Result<IdentityScope, ApiError> {
    use crate::schema::identity_scopes::dsl::{identity_scopes as scopes, name};
    with_connection(pool, async |conn| {
        let written = diesel::insert_into(scopes)
            .values(NewIdentityScopeRow {
                name: scope_name,
                provider_kind: provider,
            })
            .on_conflict(name)
            .do_update()
            .set(identity_scopes::provider_kind.eq(provider))
            .get_result::<IdentityScopeRow>(conn)
            .await
            .optional()?;
        match written {
            Some(scope) => Ok(scope.into()),
            None => scopes
                .filter(name.eq(scope_name))
                .first::<IdentityScopeRow>(conn)
                .await
                .map(Into::into),
        }
    })
    .await
}
