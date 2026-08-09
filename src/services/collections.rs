use crate::errors::ApiError;
use crate::events::EventContext;
use crate::models::{Collection, CollectionID, NewCollectionWithAssignee, UpdateCollection};
use crate::storage::DynStorage;

/// Application-facing collection use cases.
///
/// Handlers depend on this service rather than choosing a PostgreSQL query or
/// transaction helper directly. Authorization remains at the handler boundary
/// while persistence invariants stay behind the storage capability.
#[derive(Clone)]
pub struct CollectionService {
    storage: DynStorage,
}

impl CollectionService {
    pub(crate) fn new(storage: DynStorage) -> Self {
        Self { storage }
    }

    pub async fn get(&self, id: CollectionID) -> Result<Collection, ApiError> {
        self.storage
            .inner()
            .get_collection(id)
            .await
            .map_err(ApiError::from)
    }

    pub async fn create(
        &self,
        command: NewCollectionWithAssignee,
        context: &EventContext,
    ) -> Result<Collection, ApiError> {
        self.storage
            .inner()
            .create_collection(command, context)
            .await
            .map_err(ApiError::from)
    }

    pub async fn update(
        &self,
        id: CollectionID,
        changes: UpdateCollection,
        context: &EventContext,
    ) -> Result<Collection, ApiError> {
        self.storage
            .inner()
            .update_collection(id, changes, context)
            .await
            .map_err(ApiError::from)
    }

    pub async fn delete(&self, id: CollectionID, context: &EventContext) -> Result<(), ApiError> {
        self.storage
            .inner()
            .delete_collection(id, context)
            .await
            .map_err(ApiError::from)
    }

    pub async fn children(&self, id: CollectionID) -> Result<Vec<Collection>, ApiError> {
        self.storage
            .inner()
            .collection_children(id)
            .await
            .map_err(ApiError::from)
    }

    pub async fn ancestors(&self, id: CollectionID) -> Result<Vec<Collection>, ApiError> {
        self.storage
            .inner()
            .collection_ancestors(id)
            .await
            .map_err(ApiError::from)
    }

    pub async fn move_to(
        &self,
        id: CollectionID,
        new_parent_id: CollectionID,
        context: &EventContext,
    ) -> Result<Collection, ApiError> {
        self.storage
            .inner()
            .move_collection(id, new_parent_id, context)
            .await
            .map_err(ApiError::from)
    }
}

#[cfg(test)]
mod tests {
    use actix_web::web::Data;
    use rstest::rstest;

    use crate::db::DbPool;
    use crate::errors::ApiError;
    use crate::events::{Action, EventContext};
    use crate::models::{
        Collection, CollectionID, Group, GroupID, NewCollectionWithAssignee, NewGroup,
        UpdateCollection,
    };
    use crate::services::Services;
    use crate::storage::{DynStorage, MemoryStorage, PostgresStorage};
    use crate::tests::TestScope;

    use super::CollectionService;

    #[derive(Clone, Copy, Debug)]
    enum ContractBackend {
        Memory,
        Postgres,
    }

    struct ContractHarness {
        service: CollectionService,
        group_id: GroupID,
        prefix: String,
        postgres_cleanup: Option<(Data<DbPool>, Group)>,
    }

    impl ContractHarness {
        async fn new(backend: ContractBackend, label: &str) -> Self {
            match backend {
                ContractBackend::Memory => Self {
                    service: Services::from_storage(DynStorage::new(MemoryStorage::new()))
                        .collections()
                        .clone(),
                    group_id: GroupID::new(1).expect("valid memory group id"),
                    prefix: format!("memory_{label}"),
                    postgres_cleanup: None,
                },
                ContractBackend::Postgres => {
                    let scope = TestScope::new();
                    let owner_group = NewGroup {
                        identity_scope: None,
                        groupname: scope.scoped_name(&format!("{label}_owner")),
                        description: Some("collection storage contract owner".to_string()),
                    }
                    .save_without_events(&scope.pool)
                    .await
                    .expect("contract owner group should save");
                    Self {
                        service: Services::from_storage(DynStorage::new(PostgresStorage::new(
                            scope.pool.get_ref().clone(),
                        )))
                        .collections()
                        .clone(),
                        group_id: GroupID::new(owner_group.id).expect("valid owner group id"),
                        prefix: scope.scoped_name(label),
                        postgres_cleanup: Some((scope.pool, owner_group)),
                    }
                }
            }
        }

        async fn create(&self, label: &str, parent: Option<CollectionID>) -> Collection {
            self.service
                .create(
                    NewCollectionWithAssignee {
                        name: format!("{}_{}", self.prefix, label),
                        description: format!("collection contract {label}"),
                        group_id: self.group_id,
                        parent_collection_id: parent,
                    },
                    &EventContext::system(),
                )
                .await
                .expect("contract collection should be created")
        }

        async fn finish(self) {
            if let Some((pool, owner_group)) = self.postgres_cleanup {
                owner_group
                    .delete_without_events(&pool)
                    .await
                    .expect("contract owner group cleanup");
            }
        }
    }

    fn id(collection: &Collection) -> CollectionID {
        CollectionID::new(collection.id).expect("valid collection id")
    }

    #[rstest]
    #[case::postgres(ContractBackend::Postgres)]
    #[case::memory(ContractBackend::Memory)]
    #[actix_web::test]
    async fn collection_contract_create_is_visible_to_point_reads(
        #[case] backend: ContractBackend,
    ) {
        let harness = ContractHarness::new(backend, "create_read").await;
        let created = harness.create("collection", None).await;

        assert_eq!(
            harness.service.get(id(&created)).await.expect("point read"),
            created
        );

        harness
            .service
            .delete(id(&created), &EventContext::system())
            .await
            .expect("collection cleanup");
        harness.finish().await;
    }

    #[rstest]
    #[case::postgres(ContractBackend::Postgres)]
    #[case::memory(ContractBackend::Memory)]
    #[actix_web::test]
    async fn collection_contract_lists_direct_children(#[case] backend: ContractBackend) {
        let harness = ContractHarness::new(backend, "children").await;
        let parent = harness.create("parent", None).await;
        let child = harness.create("child", Some(id(&parent))).await;

        assert_eq!(
            harness
                .service
                .children(id(&parent))
                .await
                .expect("children"),
            vec![child.clone()]
        );

        harness
            .service
            .delete(id(&child), &EventContext::system())
            .await
            .expect("child cleanup");
        harness
            .service
            .delete(id(&parent), &EventContext::system())
            .await
            .expect("parent cleanup");
        harness.finish().await;
    }

    #[rstest]
    #[case::postgres(ContractBackend::Postgres)]
    #[case::memory(ContractBackend::Memory)]
    #[actix_web::test]
    async fn collection_contract_orders_ancestors_nearest_first(#[case] backend: ContractBackend) {
        let harness = ContractHarness::new(backend, "ancestors").await;
        let parent = harness.create("parent", None).await;
        let child = harness.create("child", Some(id(&parent))).await;

        let ancestors = harness
            .service
            .ancestors(id(&child))
            .await
            .expect("ancestors");
        assert_eq!(ancestors.first().map(|item| item.id), Some(parent.id));
        assert_eq!(ancestors.len(), 2, "child should have its parent and root");

        harness
            .service
            .delete(id(&child), &EventContext::system())
            .await
            .expect("child cleanup");
        harness
            .service
            .delete(id(&parent), &EventContext::system())
            .await
            .expect("parent cleanup");
        harness.finish().await;
    }

    #[rstest]
    #[case::postgres(ContractBackend::Postgres)]
    #[case::memory(ContractBackend::Memory)]
    #[actix_web::test]
    async fn collection_contract_changed_update_advances_revision(
        #[case] backend: ContractBackend,
    ) {
        let harness = ContractHarness::new(backend, "changed_update").await;
        let collection = harness.create("collection", None).await;

        let updated = harness
            .service
            .update(
                id(&collection),
                UpdateCollection {
                    name: None,
                    description: Some("updated contract description".to_string()),
                },
                &EventContext::system(),
            )
            .await
            .expect("collection should update");
        assert_eq!(updated.revision.get(), collection.revision.get() + 1);

        harness
            .service
            .delete(id(&updated), &EventContext::system())
            .await
            .expect("collection cleanup");
        harness.finish().await;
    }

    #[rstest]
    #[case::postgres(ContractBackend::Postgres)]
    #[case::memory(ContractBackend::Memory)]
    #[actix_web::test]
    async fn collection_contract_no_op_update_preserves_revision(#[case] backend: ContractBackend) {
        let harness = ContractHarness::new(backend, "no_op_update").await;
        let collection = harness.create("collection", None).await;

        let unchanged = harness
            .service
            .update(
                id(&collection),
                UpdateCollection {
                    name: Some(collection.name.clone()),
                    description: Some(collection.description.clone()),
                },
                &EventContext::system(),
            )
            .await
            .expect("no-op update should return current state");
        assert_eq!(unchanged, collection);

        harness
            .service
            .delete(id(&collection), &EventContext::system())
            .await
            .expect("collection cleanup");
        harness.finish().await;
    }

    #[rstest]
    #[case::postgres(ContractBackend::Postgres)]
    #[case::memory(ContractBackend::Memory)]
    #[actix_web::test]
    async fn collection_contract_move_reparents_the_subtree(#[case] backend: ContractBackend) {
        let harness = ContractHarness::new(backend, "move").await;
        let old_parent = harness.create("old_parent", None).await;
        let new_parent = harness.create("new_parent", None).await;
        let child = harness.create("child", Some(id(&old_parent))).await;

        let moved = harness
            .service
            .move_to(id(&child), id(&new_parent), &EventContext::system())
            .await
            .expect("child should move");
        assert_eq!(moved.parent_collection_id, Some(new_parent.id));
        assert_eq!(moved.revision.get(), child.revision.get() + 1);

        harness
            .service
            .delete(id(&moved), &EventContext::system())
            .await
            .expect("child cleanup");
        for parent in [old_parent, new_parent] {
            harness
                .service
                .delete(id(&parent), &EventContext::system())
                .await
                .expect("parent cleanup");
        }
        harness.finish().await;
    }

    #[rstest]
    #[case::postgres(ContractBackend::Postgres)]
    #[case::memory(ContractBackend::Memory)]
    #[actix_web::test]
    async fn collection_contract_rejects_deleting_a_parent_with_children(
        #[case] backend: ContractBackend,
    ) {
        let harness = ContractHarness::new(backend, "delete_parent").await;
        let parent = harness.create("parent", None).await;
        let child = harness.create("child", Some(id(&parent))).await;

        assert!(matches!(
            harness
                .service
                .delete(id(&parent), &EventContext::system())
                .await,
            Err(ApiError::Conflict(_))
        ));

        harness
            .service
            .delete(id(&child), &EventContext::system())
            .await
            .expect("child cleanup");
        harness
            .service
            .delete(id(&parent), &EventContext::system())
            .await
            .expect("parent cleanup");
        harness.finish().await;
    }

    #[actix_web::test]
    async fn memory_collection_events_exclude_no_op_updates() {
        let storage = MemoryStorage::new();
        let service = Services::from_storage(DynStorage::new(storage.clone()))
            .collections()
            .clone();
        let context = EventContext::system();
        let collection = service
            .create(
                NewCollectionWithAssignee {
                    name: "memory_event_contract".to_string(),
                    description: "memory event contract".to_string(),
                    group_id: GroupID::new(1).expect("valid memory group id"),
                    parent_collection_id: None,
                },
                &context,
            )
            .await
            .expect("memory collection should be created");
        let updated = service
            .update(
                id(&collection),
                UpdateCollection {
                    name: None,
                    description: Some("changed memory event contract".to_string()),
                },
                &context,
            )
            .await
            .expect("memory collection should update");
        service
            .update(
                id(&updated),
                UpdateCollection {
                    name: Some(updated.name.clone()),
                    description: Some(updated.description.clone()),
                },
                &context,
            )
            .await
            .expect("memory no-op should succeed");
        service
            .delete(id(&updated), &context)
            .await
            .expect("memory collection should delete");

        let events = storage.events().await;
        assert_eq!(
            events.iter().map(|event| event.action).collect::<Vec<_>>(),
            vec![Action::Created, Action::Updated, Action::Deleted]
        );
        assert!(
            events
                .iter()
                .all(|event| event.collection_id == collection.id && event.context == context)
        );
    }
}
