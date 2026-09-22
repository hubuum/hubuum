//! Application-facing local authorization services.
//!
//! API and domain consumers use model projections while the selected storage
//! adapter owns permission-set snapshots, revisions, and persistence rows.

use crate::errors::ApiError;
use crate::models::CollectionPermissionSet;
use crate::permissions::grant_from_storage;
use hubuum_domain::{CollectionId, GroupId};

use crate::storage::{
    AuthorizationDataStorage, StorageAuthorizationPermissionSetQuery, StorageHandle,
};

pub(crate) async fn collection_permission_set(
    storage: &StorageHandle,
    collection_id: i32,
    group_id: Option<i32>,
) -> Result<CollectionPermissionSet, ApiError> {
    let (collection_id, revision, grants) = storage
        .get_local_collection_permission_set(StorageAuthorizationPermissionSetQuery::new(
            CollectionId::new(collection_id)?,
            group_id.map(GroupId::new).transpose()?,
        ))
        .await?
        .into_parts();
    Ok(CollectionPermissionSet {
        collection_id: collection_id.id(),
        revision,
        permissions: grants.into_iter().map(grant_from_storage).collect(),
    })
}
