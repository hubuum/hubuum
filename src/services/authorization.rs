//! Application-facing local authorization services.
//!
//! API and domain consumers use model projections while the selected storage
//! adapter owns permission-set snapshots, revisions, and persistence rows.

use crate::errors::ApiError;
use crate::models::{CollectionPermissionSet, ResourceRevision};
use crate::permissions::grant_from_storage;
use crate::storage::{AuthorizationPermissionSetQuery, AuthorizationStorage, StorageHandle};

pub(crate) async fn collection_permission_set(
    storage: &StorageHandle,
    collection_id: i32,
    group_id: Option<i32>,
) -> Result<CollectionPermissionSet, ApiError> {
    let (collection_id, revision, grants) = storage
        .load_local_collection_permission_set(AuthorizationPermissionSetQuery::new(
            collection_id,
            group_id,
        ))
        .await?
        .into_parts();
    Ok(CollectionPermissionSet {
        collection_id,
        revision: ResourceRevision::new(revision)?,
        permissions: grants.into_iter().map(grant_from_storage).collect(),
    })
}
