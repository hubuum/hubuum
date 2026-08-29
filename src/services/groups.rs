use crate::errors::ApiError;
use crate::models::Group;
use crate::models::search::QueryOptions;
use crate::pagination::{SKIPPED_TOTAL_COUNT, prepare_db_pagination};
use crate::storage::{GroupStorage, StorageContext, StorageGroupListQuery, storage_handle};

use super::identity::identity_group_from_storage;

/// List groups without exposing adapter-specific search helpers to handlers.
///
/// Pagination policy belongs to the application layer. The storage request
/// receives one prepared page query and derives an optional exact count from
/// the same filters.
pub(crate) async fn list(
    backend: &impl StorageContext,
    options: &QueryOptions,
) -> Result<(Vec<Group>, i64), ApiError> {
    let records = prepare_db_pagination::<Group>(options)?;
    let query = StorageGroupListQuery::new(records);
    let (groups, total_count) = storage_handle(backend)
        .list_groups(query)
        .await?
        .into_parts();
    let groups = groups
        .into_iter()
        .map(identity_group_from_storage)
        .collect::<Result<Vec<_>, _>>()?;

    Ok((groups, total_count.unwrap_or(SKIPPED_TOTAL_COUNT)))
}
