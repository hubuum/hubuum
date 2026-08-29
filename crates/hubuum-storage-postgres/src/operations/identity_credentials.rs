use diesel::prelude::{ExpressionMethods, JoinOnDsl, QueryDsl};
use diesel_async::RunQueryDsl;
use hubuum_domain::LOCAL_IDENTITY_SCOPE;
use hubuum_storage_core::{StorageLocalPasswordReset, StorageMutationOutcome};

use crate::{PostgresRuntime, PostgresStorageError};

/// Replace one local human's pre-hashed credential and revoke every active
/// bearer token in the same transaction.
pub async fn reset_local_password(
    runtime: &PostgresRuntime,
    request: StorageLocalPasswordReset,
) -> Result<StorageMutationOutcome<usize>, PostgresStorageError> {
    let (principal_name, password_hash, event_context) = request.into_parts();
    runtime
        .with_transaction(async move |connection| {
            use crate::schema::{identity_scopes, principals, users};

            let principal_id = users::table
                .inner_join(principals::table.on(users::id.eq(principals::id)))
                .inner_join(
                    identity_scopes::table
                        .on(principals::identity_scope_id.eq(identity_scopes::id)),
                )
                .filter(principals::name.eq(principal_name))
                .filter(identity_scopes::name.eq(LOCAL_IDENTITY_SCOPE))
                .select(users::id)
                .first::<i32>(connection)
                .await?;
            crate::operations::user::set_user_password_on_connection(
                connection,
                principal_id,
                password_hash,
                &event_context,
                true,
            )
            .await
        })
        .await
}
