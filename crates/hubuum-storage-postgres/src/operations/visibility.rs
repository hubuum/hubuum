//! Shared PostgreSQL authorization pushdown for query operation families.

use diesel::JoinOnDsl;
use diesel::prelude::{ExpressionMethods, QueryDsl};
use diesel_async::RunQueryDsl;
use hubuum_query::{FilterField, QueryOptions};
use hubuum_storage_core::{StorageAuthorizationPermission, StorageVisibility};

use crate::operations::authorization::apply_permission_filter;
use crate::{PostgresConnection, PostgresStorageError};

/// Resolve the descendant collections on which a principal holds every
/// requested permission.
///
/// Resource scoping is deliberately applied by each resource query rather
/// than here. A class- or object-scoped request still needs its parent
/// collection for authorization and projection context.
pub(crate) async fn authorized_collection_ids(
    connection: &mut PostgresConnection,
    visibility: &StorageVisibility,
    permissions: &[StorageAuthorizationPermission],
) -> Result<Vec<i32>, PostgresStorageError> {
    use crate::schema::{
        collection_closure, collections, group_memberships, permissions as grants,
    };

    if visibility.is_admin() {
        return collections::table
            .select(collections::id)
            .load(connection)
            .await
            .map_err(PostgresStorageError::from);
    }
    let group_ids = group_memberships::table
        .filter(group_memberships::principal_id.eq(visibility.principal_id().id()))
        .select(group_memberships::group_id);
    let mut records = grants::table
        .filter(grants::group_id.eq_any(group_ids))
        .into_boxed();
    for permission in permissions.iter().copied() {
        apply_permission_filter!(records, permission, true);
    }
    records
        .inner_join(
            collection_closure::table
                .on(grants::collection_id.eq(collection_closure::ancestor_collection_id)),
        )
        .select(collection_closure::descendant_collection_id)
        .distinct()
        .load(connection)
        .await
        .map_err(PostgresStorageError::from)
}

/// Combine explicit permission filters with the operation's mandatory
/// baseline, rejecting unknown permission names at the adapter boundary.
pub(crate) fn required_permissions(
    options: &QueryOptions,
    baseline: impl IntoIterator<Item = StorageAuthorizationPermission>,
) -> Result<Vec<StorageAuthorizationPermission>, PostgresStorageError> {
    let mut permissions = baseline.into_iter().collect::<Vec<_>>();
    for parameter in options.filters() {
        if parameter.field == FilterField::Permissions {
            permissions.push(
                StorageAuthorizationPermission::from_name(&parameter.value)
                    .map_err(|error| PostgresStorageError::invalid_input(error.to_string()))?,
            );
        }
    }
    permissions.sort_unstable();
    permissions.dedup();
    Ok(permissions)
}
