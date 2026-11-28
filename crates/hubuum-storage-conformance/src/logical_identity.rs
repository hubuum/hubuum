use hubuum_domain::CollectionId;
use hubuum_events_core::EventContext;
use hubuum_query::{FilterField, QueryOptions, SortParam};
use hubuum_storage_core::{
    CollectionStorage, HistoryStorage, LogicalIdentityStorage, StorageCollectionUpdate,
    StorageHistoryCollectionScope, StorageHistoryListQuery, StorageIdentityHistoryKind,
    StorageIdentityResourceKind, StorageIdentityTarget,
};

use crate::FixtureError;

/// The caller supplies an isolated collection and owns its cleanup. A mutation
/// creates a new version of that incarnation while closing its original history.
pub async fn verify_collection_version_identity(
    backend: &(impl HistoryStorage + LogicalIdentityStorage),
    mutations: &(impl CollectionStorage + ?Sized),
    collection: CollectionId,
) -> Result<(), FixtureError> {
    let target = StorageIdentityTarget::Resource {
        kind: StorageIdentityResourceKind::Collection,
        id: collection.into(),
    };
    let before = backend.resolve_logical_identity(target).await?;
    let before = before
        .subject()
        .and_then(|subject| subject.record_identity())
        .ok_or("Native collection has no resolved identity")?;
    mutations
        .update_collection(
            collection,
            StorageCollectionUpdate::new(
                None,
                Some(format!("identity revision {}", before.version())),
            ),
            &EventContext::system(),
        )
        .await?
        .into_value();
    let after = backend.resolve_logical_identity(target).await?;
    let after = after
        .subject()
        .and_then(|subject| subject.record_identity())
        .ok_or("Updated collection has no resolved identity")?;
    if before.resource() != after.resource() || before.version() == after.version() {
        return Err(
            "Updating a collection must retain its incarnation and change its version".into(),
        );
    }
    let query = StorageHistoryListQuery::new(
        collection.into(),
        QueryOptions::new(
            vec![],
            vec![SortParam::new(FilterField::HistoryId, false)],
            Some(20),
            None,
            false,
        )?,
        StorageHistoryCollectionScope::All,
    );
    let (rows, _) = backend.list_collection_history(query).await?.into_parts();
    for row in rows {
        let (_, metadata) = row.into_parts();
        let retained = backend
            .resolve_logical_identity(StorageIdentityTarget::History {
                kind: StorageIdentityHistoryKind::Collection,
                id: metadata.history_entry_id(),
            })
            .await?;
        if retained
            .subject()
            .and_then(|subject| subject.record_identity())
            == Some(before)
        {
            if metadata.valid_to().is_none() {
                return Err("Original collection version was not closed".into());
            }
            if metadata.logical_identity() != Some(&retained) {
                return Err("History projection lost captured identity evidence".into());
            }
            return Ok(());
        }
    }
    Err("Closing collection history replaced its original version identity".into())
}
