use diesel::QueryableByName;
use diesel::prelude::{ExpressionMethods, QueryDsl};
use diesel::sql_types::{BigInt, Bool};
use diesel_async::RunQueryDsl;
use hubuum_domain::{LOCAL_IDENTITY_SCOPE, LOCAL_PROVIDER_KIND, MANUAL_MEMBERSHIP_SOURCE};
use hubuum_storage_core::{AuthenticationPrincipalKind, StorageDefaultAdminBootstrap};

use crate::operations::identity_scope::identity_scope_id_by_name_on_connection;
use crate::{PostgresConnection, PostgresRuntime, PostgresStorageError};

const DEFAULT_ADMIN_BOOTSTRAP_LOCK_KEY: i64 = 4_801_000_000_100;

#[derive(QueryableByName)]
struct AdvisoryLockRow {
    #[diesel(sql_type = Bool)]
    locked: bool,
}

async fn default_admin_bootstrap_required_conn(
    conn: &mut PostgresConnection,
) -> Result<bool, PostgresStorageError> {
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
pub async fn is_default_admin_bootstrap_required(
    runtime: &PostgresRuntime,
) -> Result<bool, PostgresStorageError> {
    runtime
        .with_connection(async |conn| default_admin_bootstrap_required_conn(conn).await)
        .await
}

/// Create the initial local administrator atomically when the database is empty.
///
/// The transaction-scoped advisory lock serializes startup across replicas. A
/// process that waits for another replica observes the completed transaction and
/// returns `false` instead of racing the unique group or principal constraints.
pub async fn bootstrap_default_admin(
    runtime: &PostgresRuntime,
    request: StorageDefaultAdminBootstrap,
) -> Result<bool, PostgresStorageError> {
    let admin_groupname = request.admin_group_name().to_string();
    let hashed_password = request.password_hash().to_string();
    runtime
        .with_transaction(async |conn| -> Result<bool, PostgresStorageError> {
            let lock = diesel::sql_query("SELECT TRUE AS locked FROM pg_advisory_xact_lock($1)")
                .bind::<BigInt, _>(DEFAULT_ADMIN_BOOTSTRAP_LOCK_KEY)
                .get_result::<AdvisoryLockRow>(conn)
                .await?;
            if !lock.locked {
                return Err(PostgresStorageError::database(
                    "Failed to acquire default administrator bootstrap lock",
                ));
            }

            if !default_admin_bootstrap_required_conn(conn).await? {
                return Ok(false);
            }

            let local_scope_id =
                identity_scope_id_by_name_on_connection(conn, LOCAL_IDENTITY_SCOPE).await?;
            let group_id = diesel::insert_into(crate::schema::groups::table)
                .values((
                    crate::schema::groups::identity_scope_id.eq(local_scope_id),
                    crate::schema::groups::groupname.eq(&admin_groupname),
                    crate::schema::groups::description.eq("Default admin group."),
                    crate::schema::groups::managed_by.eq(LOCAL_PROVIDER_KIND),
                ))
                .returning(crate::schema::groups::id)
                .get_result::<i32>(conn)
                .await?;
            let principal_id = diesel::insert_into(crate::schema::principals::table)
                .values((
                    crate::schema::principals::identity_scope_id.eq(local_scope_id),
                    crate::schema::principals::kind.eq(AuthenticationPrincipalKind::Human.as_str()),
                    crate::schema::principals::name.eq("admin"),
                ))
                .returning(crate::schema::principals::id)
                .get_result::<i32>(conn)
                .await?;
            let user_id = diesel::insert_into(crate::schema::users::table)
                .values((
                    crate::schema::users::id.eq(principal_id),
                    crate::schema::users::password.eq(Some(&hashed_password)),
                    crate::schema::users::proper_name.eq(Some("Administrator")),
                ))
                .returning(crate::schema::users::id)
                .get_result::<i32>(conn)
                .await?;

            diesel::insert_into(crate::schema::group_memberships::table)
                .values((
                    crate::schema::group_memberships::principal_id.eq(user_id),
                    crate::schema::group_memberships::group_id.eq(group_id),
                ))
                .execute(conn)
                .await?;
            diesel::insert_into(crate::schema::group_membership_sources::table)
                .values((
                    crate::schema::group_membership_sources::principal_id.eq(user_id),
                    crate::schema::group_membership_sources::group_id.eq(group_id),
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
