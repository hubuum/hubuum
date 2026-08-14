use std::collections::{HashMap, HashSet};

use crate::schema::identity_scopes;
use crate::{PostgresConnection, PostgresRevision, PostgresStorageError};
use diesel::prelude::*;
use diesel_async::RunQueryDsl;
use hubuum_storage_core::StorageIdentityScope;

#[derive(Debug, Queryable, Selectable)]
#[diesel(table_name = crate::schema::identity_scopes)]
struct IdentityScopeRow {
    id: i32,
    name: String,
    provider_kind: String,
    created_at: chrono::NaiveDateTime,
    updated_at: chrono::NaiveDateTime,
    revision: PostgresRevision,
}

impl From<IdentityScopeRow> for StorageIdentityScope {
    fn from(row: IdentityScopeRow) -> Self {
        Self::new(
            row.id,
            row.name,
            row.provider_kind,
            row.created_at,
            row.updated_at,
            row.revision.get(),
        )
    }
}

#[derive(Insertable)]
#[diesel(table_name = crate::schema::identity_scopes)]
struct NewIdentityScopeRow<'a> {
    name: &'a str,
    provider_kind: &'a str,
}

pub async fn identity_scope_id_by_name_on_connection(
    conn: &mut PostgresConnection,
    scope_name: &str,
) -> Result<i32, PostgresStorageError> {
    use crate::schema::identity_scopes::dsl::{identity_scopes as scopes, name};
    scopes
        .filter(name.eq(scope_name))
        .select(identity_scopes::id)
        .first::<i32>(conn)
        .await
        .map_err(PostgresStorageError::from)
}

pub async fn identity_scope_by_name_on_connection(
    connection: &mut PostgresConnection,
    scope_name: &str,
) -> Result<StorageIdentityScope, PostgresStorageError> {
    use crate::schema::identity_scopes::dsl::{identity_scopes as scopes, name};
    scopes
        .filter(name.eq(scope_name))
        .first::<IdentityScopeRow>(connection)
        .await
        .map(Into::into)
        .map_err(PostgresStorageError::from)
}

pub async fn identity_scope_name_by_id_on_connection(
    connection: &mut PostgresConnection,
    scope_id: i32,
) -> Result<String, PostgresStorageError> {
    identity_scopes::table
        .filter(identity_scopes::id.eq(scope_id))
        .select(identity_scopes::name)
        .first::<String>(connection)
        .await
        .map_err(PostgresStorageError::from)
}

pub async fn identity_scope_names_by_ids_on_connection(
    connection: &mut PostgresConnection,
    scope_ids: &[i32],
) -> Result<HashMap<i32, String>, PostgresStorageError> {
    if scope_ids.is_empty() {
        return Ok(HashMap::new());
    }

    let unique_ids = scope_ids.iter().copied().collect::<HashSet<_>>();
    let query_ids = unique_ids.iter().copied().collect::<Vec<_>>();
    let rows = identity_scopes::table
        .filter(identity_scopes::id.eq_any(&query_ids))
        .select((identity_scopes::id, identity_scopes::name))
        .load::<(i32, String)>(connection)
        .await?;
    if rows.len() != unique_ids.len() {
        return Err(PostgresStorageError::database(
            "One or more identity scopes could not be resolved",
        ));
    }

    Ok(rows.into_iter().collect())
}

pub async fn ensure_identity_scope_on_connection(
    connection: &mut PostgresConnection,
    scope_name: &str,
    provider: &str,
) -> Result<StorageIdentityScope, PostgresStorageError> {
    use crate::schema::identity_scopes::dsl::{identity_scopes as scopes, name};
    let written = diesel::insert_into(scopes)
        .values(NewIdentityScopeRow {
            name: scope_name,
            provider_kind: provider,
        })
        .on_conflict(name)
        .do_update()
        .set(identity_scopes::provider_kind.eq(provider))
        .get_result::<IdentityScopeRow>(connection)
        .await
        .optional()?;
    match written {
        Some(scope) => Ok(scope.into()),
        None => scopes
            .filter(name.eq(scope_name))
            .first::<IdentityScopeRow>(connection)
            .await
            .map(Into::into)
            .map_err(PostgresStorageError::from),
    }
}
