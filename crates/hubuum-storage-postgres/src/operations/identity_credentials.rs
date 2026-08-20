use diesel::prelude::{ExpressionMethods, JoinOnDsl, QueryDsl};
use diesel_async::RunQueryDsl;
use hubuum_domain::LOCAL_IDENTITY_SCOPE;
use hubuum_storage_core::StorageLocalPasswordReset;

use crate::{PostgresRuntime, PostgresStorageError};

/// Replace one local human's pre-hashed credential and revoke every active
/// bearer token in the same transaction.
pub async fn reset_local_password(
    runtime: &PostgresRuntime,
    request: StorageLocalPasswordReset,
) -> Result<usize, PostgresStorageError> {
    runtime
        .with_transaction(async move |connection| {
            use crate::schema::{identity_scopes, principals, tokens, users};

            let (principal_id, provider_managed) = users::table
                .inner_join(principals::table.on(users::id.eq(principals::id)))
                .inner_join(
                    identity_scopes::table
                        .on(principals::identity_scope_id.eq(identity_scopes::id)),
                )
                .filter(principals::name.eq(request.principal_name()))
                .filter(identity_scopes::name.eq(LOCAL_IDENTITY_SCOPE))
                .select((users::id, principals::provider_managed))
                .first::<(i32, bool)>(connection)
                .await?;
            if provider_managed {
                return Err(PostgresStorageError::permission_denied(
                    "Provider-managed users are read-only in Hubuum",
                ));
            }

            diesel::update(users::table.filter(users::id.eq(principal_id)))
                .set(users::password.eq(Some(request.password_hash())))
                .execute(connection)
                .await?;
            diesel::update(
                tokens::table
                    .filter(tokens::principal_id.eq(principal_id))
                    .filter(tokens::revoked_at.is_null()),
            )
            .set(tokens::revoked_at.eq(diesel::dsl::now))
            .execute(connection)
            .await
            .map_err(PostgresStorageError::from)
        })
        .await
}
