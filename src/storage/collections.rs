use std::sync::Arc;

use async_trait::async_trait;

use crate::events::EventContext;
use crate::models::{Collection, CollectionID, NewCollectionWithAssignee, UpdateCollection};

use super::{ClassRelationStore, ClassStore, ObjectStore, StorageError};

/// Persistence capability for the core collection lifecycle.
///
/// Methods are intentionally aggregate-shaped. Implementations retain control
/// over transactions, hierarchy maintenance, initial permission grants, and
/// atomic event persistence.
#[async_trait]
pub trait CollectionStore: Send + Sync {
    async fn get_collection(&self, id: CollectionID) -> Result<Collection, StorageError>;

    async fn create_collection(
        &self,
        command: NewCollectionWithAssignee,
        context: &EventContext,
    ) -> Result<Collection, StorageError>;

    async fn update_collection(
        &self,
        id: CollectionID,
        changes: UpdateCollection,
        context: &EventContext,
    ) -> Result<Collection, StorageError>;

    async fn delete_collection(
        &self,
        id: CollectionID,
        context: &EventContext,
    ) -> Result<(), StorageError>;

    async fn collection_children(&self, id: CollectionID) -> Result<Vec<Collection>, StorageError>;

    async fn collection_ancestors(&self, id: CollectionID)
    -> Result<Vec<Collection>, StorageError>;

    async fn move_collection(
        &self,
        id: CollectionID,
        new_parent_id: CollectionID,
        context: &EventContext,
    ) -> Result<Collection, StorageError>;
}

/// Umbrella storage boundary. New aggregate capabilities can be added here as
/// vertical slices migrate without exposing a database pool to services.
pub trait Storage: CollectionStore + ClassStore + ObjectStore + ClassRelationStore {}

impl<T> Storage for T where T: CollectionStore + ClassStore + ObjectStore + ClassRelationStore {}

#[derive(Clone)]
pub struct DynStorage {
    inner: Arc<dyn Storage>,
}

impl DynStorage {
    pub fn new(storage: impl Storage + 'static) -> Self {
        Self {
            inner: Arc::new(storage),
        }
    }

    pub(crate) fn inner(&self) -> &dyn Storage {
        self.inner.as_ref()
    }
}
