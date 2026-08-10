use std::collections::{HashMap, HashSet};

use crate::errors::ApiError;
use crate::models::{IdentityScope, NewIdentityScope};
use crate::schema::identity_scopes;
use crate::storage::postgres::prelude::*;
use crate::storage::postgres::{PostgresConnection, with_connection};

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
    pool: &impl crate::storage::StorageContext,
    scope_name: &str,
) -> Result<IdentityScope, ApiError> {
    use crate::schema::identity_scopes::dsl::{identity_scopes as scopes, name};
    with_connection(pool, async |conn| {
        scopes
            .filter(name.eq(scope_name))
            .first::<IdentityScope>(conn)
            .await
    })
    .await
}

pub(crate) async fn identity_scope_name_by_id(
    pool: &impl crate::storage::StorageContext,
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
    pool: &impl crate::storage::StorageContext,
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
    pool: &impl crate::storage::StorageContext,
    scope_name: &str,
    provider: &str,
) -> Result<IdentityScope, ApiError> {
    use crate::schema::identity_scopes::dsl::{identity_scopes as scopes, name};
    with_connection(pool, async |conn| {
        let written = diesel::insert_into(scopes)
            .values(NewIdentityScope {
                name: scope_name,
                provider_kind: provider,
            })
            .on_conflict(name)
            .do_update()
            .set(identity_scopes::provider_kind.eq(provider))
            .get_result::<IdentityScope>(conn)
            .await
            .optional()?;
        match written {
            Some(scope) => Ok(scope),
            None => {
                scopes
                    .filter(name.eq(scope_name))
                    .first::<IdentityScope>(conn)
                    .await
            }
        }
    })
    .await
}
