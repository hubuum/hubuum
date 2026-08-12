use crate::errors::ApiError;
use crate::models::Group;
use crate::models::search::QueryOptions;
use crate::pagination::{SKIPPED_TOTAL_COUNT, count_query_options, prepare_db_pagination};
use crate::storage::{IdentityStorage, StorageContext, StorageGroupListQuery, storage_handle};

use super::identity::identity_group_from_storage;

/// List groups without exposing adapter-specific search helpers to handlers.
///
/// Pagination policy belongs to the application layer. The storage request
/// receives the prepared record query plus an optional exact-count query and
/// returns both results as one operation-shaped page.
pub(crate) async fn list(
    backend: &impl StorageContext,
    options: &QueryOptions,
) -> Result<(Vec<Group>, i64), ApiError> {
    let records = prepare_db_pagination::<Group>(options)?;
    let count = options.include_total.then(|| count_query_options(options));
    let (groups, total_count) = storage_handle(backend)
        .list_groups(StorageGroupListQuery::new(records, count))
        .await?
        .into_parts();
    let groups = groups
        .into_iter()
        .map(identity_group_from_storage)
        .collect::<Result<Vec<_>, _>>()?;

    Ok((groups, total_count.unwrap_or(SKIPPED_TOTAL_COUNT)))
}
