use diesel::prelude::*;

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
