use diesel::sql_types::{BigInt, Bool};

use crate::errors::ApiError;
use crate::models::identity::{
    LOCAL_IDENTITY_SCOPE, LOCAL_PROVIDER_KIND, MANUAL_MEMBERSHIP_SOURCE,
};
use crate::models::{NewPrincipal, PrincipalKind};
use crate::storage::StorageDefaultAdminBootstrap;
use crate::storage::postgres::operations::group::GroupRow;
use crate::storage::postgres::operations::identity::identity_scope_id_by_name_conn;
use crate::storage::postgres::operations::principal::InsertPrincipalRecord;
use crate::storage::postgres::operations::user::UserRow;
use crate::storage::postgres::prelude::*;
use crate::storage::postgres::{with_connection, with_transaction};

const DEFAULT_ADMIN_BOOTSTRAP_LOCK_KEY: i64 = 4_801_000_000_100;

#[derive(QueryableByName)]
struct AdvisoryLockRow {
    #[diesel(sql_type = Bool)]
    locked: bool,
}

async fn default_admin_bootstrap_required_conn(
    conn: &mut crate::storage::postgres::PostgresConnection,
) -> Result<bool, ApiError> {
    let user_exists = diesel::select(diesel::dsl::exists(
        crate::schema::users::table.select(crate::schema::users::id),
    ))
    .get_result::<bool>(conn)
    .await?;
    if user_exists {
        return Ok(false);
    }

    let group_exists = diesel::select(diesel::dsl::exists(
        crate::schema::groups::table.select(crate::schema::groups::id),
    ))
    .get_result::<bool>(conn)
    .await?;
    Ok(!group_exists)
}

/// Check whether the database is empty enough to require initial administrator
/// bootstrap.
///
/// This is an optimization only. [`bootstrap_default_admin`] repeats the check
/// while holding the bootstrap advisory lock so concurrent replicas remain
/// correct.
pub async fn default_admin_bootstrap_required(
    pool: &crate::storage::postgres::PostgresPool,
) -> Result<bool, ApiError> {
    with_connection(pool, async |conn| {
        default_admin_bootstrap_required_conn(conn).await
    })
    .await
}

/// Create the initial local administrator atomically when the database is empty.
///
/// The transaction-scoped advisory lock serializes startup across replicas. A
/// process that waits for another replica observes the completed transaction and
/// returns `false` instead of racing the unique group or principal constraints.
pub async fn bootstrap_default_admin(
    pool: &crate::storage::postgres::PostgresPool,
    request: StorageDefaultAdminBootstrap,
) -> Result<bool, ApiError> {
    let admin_groupname = request.admin_group_name().to_string();
    let hashed_password = request.password_hash().to_string();
    with_transaction(pool, async |conn| -> Result<bool, ApiError> {
        let lock = diesel::sql_query("SELECT TRUE AS locked FROM pg_advisory_xact_lock($1)")
            .bind::<BigInt, _>(DEFAULT_ADMIN_BOOTSTRAP_LOCK_KEY)
            .get_result::<AdvisoryLockRow>(conn)
            .await?;
        if !lock.locked {
            return Err(ApiError::InternalServerError(
                "Failed to acquire default administrator bootstrap lock".to_string(),
            ));
        }

        if !default_admin_bootstrap_required_conn(conn).await? {
            return Ok(false);
        }

        let local_scope_id = identity_scope_id_by_name_conn(conn, LOCAL_IDENTITY_SCOPE).await?;
        let group = diesel::insert_into(crate::schema::groups::table)
            .values((
                crate::schema::groups::identity_scope_id.eq(local_scope_id),
                crate::schema::groups::groupname.eq(&admin_groupname),
                crate::schema::groups::description.eq("Default admin group."),
                crate::schema::groups::managed_by.eq(LOCAL_PROVIDER_KIND),
            ))
            .get_result::<GroupRow>(conn)
            .await?;
        let principal = NewPrincipal {
            identity_scope_id: local_scope_id,
            kind: PrincipalKind::Human.as_str(),
            name: "admin",
        }
        .insert(conn)
        .await?;
        let user = diesel::insert_into(crate::schema::users::table)
            .values((
                crate::schema::users::id.eq(principal.id),
                crate::schema::users::password.eq(Some(&hashed_password)),
                crate::schema::users::proper_name.eq(Some("Administrator")),
            ))
            .get_result::<UserRow>(conn)
            .await?;

        diesel::insert_into(crate::schema::group_memberships::table)
            .values((
                crate::schema::group_memberships::principal_id.eq(user.id),
                crate::schema::group_memberships::group_id.eq(group.id),
            ))
            .execute(conn)
            .await?;
        diesel::insert_into(crate::schema::group_membership_sources::table)
            .values((
                crate::schema::group_membership_sources::principal_id.eq(user.id),
                crate::schema::group_membership_sources::group_id.eq(group.id),
                crate::schema::group_membership_sources::source.eq(MANUAL_MEMBERSHIP_SOURCE),
                crate::schema::group_membership_sources::source_scope_id.eq(local_scope_id),
                crate::schema::group_membership_sources::source_key.eq(""),
            ))
            .execute(conn)
            .await?;

        Ok(true)
    })
    .await
}
