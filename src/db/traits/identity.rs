use diesel::prelude::*;
use std::collections::{HashMap, HashSet};

use crate::db::{DbPool, with_connection};
use crate::errors::ApiError;
use crate::models::{IdentityScope, NewIdentityScope};
use crate::schema::identity_scopes;

pub(crate) fn identity_scope_id_by_name_conn(
    conn: &mut PgConnection,
    scope_name: &str,
) -> Result<i32, ApiError> {
    use crate::schema::identity_scopes::dsl::{identity_scopes as scopes, name};
    scopes
        .filter(name.eq(scope_name))
        .select(identity_scopes::id)
        .first::<i32>(conn)
        .map_err(ApiError::from)
}

pub async fn identity_scope_by_name(
    pool: &DbPool,
    scope_name: &str,
) -> Result<IdentityScope, ApiError> {
    use crate::schema::identity_scopes::dsl::{identity_scopes as scopes, name};
    with_connection(pool, |conn| {
        scopes
            .filter(name.eq(scope_name))
            .first::<IdentityScope>(conn)
    })
}

pub async fn identity_scope_names_by_ids(
    pool: &DbPool,
    scope_ids: &[i32],
) -> Result<HashMap<i32, String>, ApiError> {
    if scope_ids.is_empty() {
        return Ok(HashMap::new());
    }

    let unique_ids = scope_ids.iter().copied().collect::<HashSet<_>>();
    let query_ids = unique_ids.iter().copied().collect::<Vec<_>>();
    let rows = with_connection(pool, |conn| {
        identity_scopes::table
            .filter(identity_scopes::id.eq_any(&query_ids))
            .select((identity_scopes::id, identity_scopes::name))
            .load::<(i32, String)>(conn)
    })?;
    if rows.len() != unique_ids.len() {
        return Err(ApiError::InternalServerError(
            "One or more identity scopes could not be resolved".to_string(),
        ));
    }

    Ok(rows.into_iter().collect())
}

pub async fn ensure_identity_scope(
    pool: &DbPool,
    scope_name: &str,
    provider: &str,
) -> Result<IdentityScope, ApiError> {
    use crate::schema::identity_scopes::dsl::{identity_scopes as scopes, name};
    with_connection(pool, |conn| {
        diesel::insert_into(scopes)
            .values(NewIdentityScope {
                name: scope_name,
                provider_kind: provider,
            })
            .on_conflict(name)
            .do_update()
            .set(identity_scopes::provider_kind.eq(provider))
            .get_result::<IdentityScope>(conn)
    })
}
