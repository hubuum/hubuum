use std::sync::Arc;

use crate::errors::ApiError;
use crate::events::EventContext;
use crate::models::{
    ClassSelector, HubuumClass, NewHubuumClass, ResolvedClassTarget, UpdateHubuumClass,
};
use crate::services::storage_boundary::{
    class_create_to_storage, class_record_from_storage, class_selector_to_storage,
    class_update_to_storage, resolved_class_from_storage, resolved_class_to_storage,
};
use crate::storage::ClassStorage;

/// Application-facing class resolution and lifecycle use cases.
#[derive(Clone)]
pub struct ClassService {
    storage: Arc<dyn ClassStorage>,
}

impl ClassService {
    pub(crate) fn new(storage: Arc<dyn ClassStorage>) -> Self {
        Self { storage }
    }

    pub async fn resolve(&self, selector: ClassSelector) -> Result<ResolvedClassTarget, ApiError> {
        self.storage
            .resolve_class(class_selector_to_storage(selector))
            .await
            .map_err(ApiError::from)
            .and_then(resolved_class_from_storage)
    }

    pub async fn create(
        &self,
        command: NewHubuumClass,
        context: &EventContext,
    ) -> Result<HubuumClass, ApiError> {
        let command = class_create_to_storage(command)?;
        self.storage
            .create_class(command, context)
            .await
            .map_err(ApiError::from)
            .map(|outcome| outcome.into_value())
            .and_then(class_record_from_storage)
    }

    pub async fn update(
        &self,
        target: &ResolvedClassTarget,
        changes: UpdateHubuumClass,
        context: &EventContext,
    ) -> Result<HubuumClass, ApiError> {
        let target = resolved_class_to_storage(target)?;
        let changes = class_update_to_storage(changes)?;
        self.storage
            .update_class(&target, changes, context)
            .await
            .map_err(ApiError::from)
            .map(|outcome| outcome.into_value())
            .and_then(class_record_from_storage)
    }

    pub async fn delete(
        &self,
        target: &ResolvedClassTarget,
        context: &EventContext,
    ) -> Result<(), ApiError> {
        let target = resolved_class_to_storage(target)?;
        self.storage
            .delete_class(&target, context)
            .await
            .map_err(ApiError::from)
            .map(|outcome| outcome.into_value())
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use serde_json::json;

    use crate::errors::ApiError;
    use crate::events::{Action, EventContext};
    use crate::models::{
        ClassSelector, Collection, CollectionID, GroupID, HubuumClass, HubuumClassID,
        NewCollectionWithAssignee, NewGroup, NewHubuumClass, UpdateHubuumClass,
    };
    use crate::services::{CollectionService, Services};
    use crate::storage::MemoryStorageModel;
    use crate::tests::CollectionFixture;
    use crate::tests::storage_contract::{
        LifecycleContractImplementation as ContractImplementation, pool as storage_contract_pool,
        postgres_permit as storage_contract_postgres_permit, prefix as storage_contract_prefix,
    };
    use crate::traits::CanSave;
    use hubuum_storage_postgres::PostgresStorage;

    use super::ClassService;

    #[derive(Clone, Copy, Debug)]
    enum ClassAddress {
        Id,
        Name,
    }

    struct ContractHarness {
        service: ClassService,
        collections: CollectionService,
        collection_id: i32,
        group_id: GroupID,
        prefix: String,
        postgres_cleanup: Option<CollectionFixture>,
        _postgres_permit: Option<tokio::sync::OwnedSemaphorePermit>,
    }

    impl ContractHarness {
        async fn new(backend: ContractImplementation, label: &str) -> Self {
            match backend {
                ContractImplementation::MemoryModel => {
                    let services = Services::from_resource_storage(MemoryStorageModel::new());
                    Self {
                        service: services.classes().clone(),
                        collections: services.collections().clone(),
                        collection_id: CollectionID::new(1).expect("valid root collection id").id(),
                        group_id: GroupID::new(1).expect("valid memory group id"),
                        prefix: format!("memory_{label}"),
                        postgres_cleanup: None,
                        _postgres_permit: None,
                    }
                }
                ContractImplementation::PostgresAdapter => {
                    let permit = storage_contract_postgres_permit().await;
                    let pool = storage_contract_pool();
                    let prefix = storage_contract_prefix(label);
                    let owner_group = NewGroup {
                        identity_scope: None,
                        groupname: format!("{prefix}_owner"),
                        description: Some("class storage contract owner".to_string()),
                    }
                    .save_without_events(&pool)
                    .await
                    .expect("contract owner group should save");
                    let collection = NewCollectionWithAssignee {
                        name: format!("{prefix}_collection"),
                        description: "class storage contract collection".to_string(),
                        group_id: GroupID::new(owner_group.id).expect("valid owner group id"),
                        parent_collection_id: None,
                    }
                    .save_without_events(&pool)
                    .await
                    .expect("contract collection should save");
                    let fixture = CollectionFixture {
                        pool: pool.clone(),
                        collection,
                        owner_group,
                        prefix: prefix.clone(),
                    };
                    let services = Services::from_resource_storage(PostgresStorage::unobserved(
                        pool.get_ref().clone(),
                    ));
                    Self {
                        service: services.classes().clone(),
                        collections: services.collections().clone(),
                        collection_id: fixture.collection.id,
                        group_id: GroupID::new(fixture.owner_group.id)
                            .expect("valid owner group id"),
                        prefix,
                        postgres_cleanup: Some(fixture),
                        _postgres_permit: Some(permit),
                    }
                }
            }
        }

        async fn create(&self, label: &str) -> HubuumClass {
            self.service
                .create(
                    NewHubuumClass {
                        name: format!("{}_{}", self.prefix, label),
                        collection_id: self.collection_id,
                        json_schema: None,
                        validate_schema: None,
                        description: format!("class contract {label}"),
                    },
                    &EventContext::system(),
                )
                .await
                .expect("contract class should be created")
        }

        async fn delete(&self, class: &HubuumClass) {
            let target = self
                .service
                .resolve(ClassSelector::by_id(
                    HubuumClassID::new(class.id).expect("valid class id"),
                ))
                .await
                .expect("class cleanup target should resolve");
            self.service
                .delete(&target, &EventContext::system())
                .await
                .expect("contract class cleanup");
        }

        async fn create_collection(&self, label: &str) -> Collection {
            self.collections
                .create(
                    NewCollectionWithAssignee {
                        name: format!("{}_{}", self.prefix, label),
                        description: format!("class contract collection {label}"),
                        group_id: self.group_id,
                        parent_collection_id: None,
                    },
                    &EventContext::system(),
                )
                .await
                .expect("contract collection should be created")
        }

        async fn finish(self) {
            if let Some(fixture) = self.postgres_cleanup {
                fixture.cleanup().await.expect("contract fixture cleanup");
            }
        }
    }

    fn selector(address: ClassAddress, class: &HubuumClass) -> ClassSelector {
        match address {
            ClassAddress::Id => {
                ClassSelector::by_id(HubuumClassID::new(class.id).expect("valid class id"))
            }
            ClassAddress::Name => ClassSelector::by_name(class.name.clone()),
        }
    }

    #[rstest]
    #[case::postgres_id(ContractImplementation::PostgresAdapter, ClassAddress::Id)]
    #[case::postgres_name(ContractImplementation::PostgresAdapter, ClassAddress::Name)]
    #[case::memory_id(ContractImplementation::MemoryModel, ClassAddress::Id)]
    #[case::memory_name(ContractImplementation::MemoryModel, ClassAddress::Name)]
    #[actix_web::test]
    async fn class_contract_resolves_explicit_addresses(
        #[case] backend: ContractImplementation,
        #[case] address: ClassAddress,
    ) {
        let harness = ContractHarness::new(backend, "resolve").await;
        let class = harness.create("class").await;

        let resolved = harness
            .service
            .resolve(selector(address, &class))
            .await
            .expect("class should resolve");
        assert_eq!(resolved.class(), &class);

        harness.delete(&class).await;
        harness.finish().await;
    }

    #[rstest]
    #[case::postgres(ContractImplementation::PostgresAdapter)]
    #[case::memory(ContractImplementation::MemoryModel)]
    #[actix_web::test]
    async fn class_contract_changed_update_advances_revision(
        #[case] backend: ContractImplementation,
    ) {
        let harness = ContractHarness::new(backend, "changed_update").await;
        let class = harness.create("class").await;
        let target = harness
            .service
            .resolve(selector(ClassAddress::Id, &class))
            .await
            .expect("class should resolve");

        let updated = harness
            .service
            .update(
                &target,
                UpdateHubuumClass {
                    name: None,
                    collection_id: None,
                    json_schema: None,
                    validate_schema: None,
                    description: Some("updated class contract".to_string()),
                },
                &EventContext::system(),
            )
            .await
            .expect("class should update");
        assert_eq!(updated.revision.get(), class.revision.get() + 1);

        harness.delete(&updated).await;
        harness.finish().await;
    }

    #[rstest]
    #[case::postgres(ContractImplementation::PostgresAdapter)]
    #[case::memory(ContractImplementation::MemoryModel)]
    #[actix_web::test]
    async fn class_contract_no_op_update_preserves_revision(
        #[case] backend: ContractImplementation,
    ) {
        let harness = ContractHarness::new(backend, "no_op_update").await;
        let class = harness.create("class").await;
        let target = harness
            .service
            .resolve(selector(ClassAddress::Id, &class))
            .await
            .expect("class should resolve");

        let unchanged = harness
            .service
            .update(
                &target,
                UpdateHubuumClass {
                    name: Some(class.name.clone()),
                    collection_id: Some(class.collection_id),
                    json_schema: None,
                    validate_schema: Some(class.validate_schema),
                    description: Some(class.description.clone()),
                },
                &EventContext::system(),
            )
            .await
            .expect("no-op update should return current state");
        assert_eq!(unchanged, class);

        harness.delete(&class).await;
        harness.finish().await;
    }

    #[rstest]
    #[case::postgres(ContractImplementation::PostgresAdapter)]
    #[case::memory(ContractImplementation::MemoryModel)]
    #[actix_web::test]
    async fn class_contract_update_moves_the_class_between_collections(
        #[case] backend: ContractImplementation,
    ) {
        let harness = ContractHarness::new(backend, "move_collection").await;
        let target_collection = harness.create_collection("target").await;
        let class = harness.create("class").await;
        let target = harness
            .service
            .resolve(selector(ClassAddress::Id, &class))
            .await
            .expect("class should resolve");

        let updated = harness
            .service
            .update(
                &target,
                UpdateHubuumClass {
                    name: None,
                    collection_id: Some(target_collection.id),
                    json_schema: None,
                    validate_schema: None,
                    description: None,
                },
                &EventContext::system(),
            )
            .await
            .expect("class should move");
        assert_eq!(updated.collection_id, target_collection.id);

        harness.delete(&updated).await;
        harness
            .collections
            .delete(
                CollectionID::new(target_collection.id).expect("valid collection id"),
                &EventContext::system(),
            )
            .await
            .expect("target collection cleanup");
        harness.finish().await;
    }

    #[rstest]
    #[case::postgres(ContractImplementation::PostgresAdapter)]
    #[case::memory(ContractImplementation::MemoryModel)]
    #[actix_web::test]
    async fn class_contract_collection_delete_cascades_the_class(
        #[case] backend: ContractImplementation,
    ) {
        let harness = ContractHarness::new(backend, "collection_cascade").await;
        let collection = harness.create_collection("target").await;
        let class = harness
            .service
            .create(
                NewHubuumClass {
                    name: format!("{}_class", harness.prefix),
                    collection_id: collection.id,
                    json_schema: None,
                    validate_schema: None,
                    description: "collection cascade contract".to_string(),
                },
                &EventContext::system(),
            )
            .await
            .expect("class should be created in target collection");

        harness
            .collections
            .delete(
                CollectionID::new(collection.id).expect("valid collection id"),
                &EventContext::system(),
            )
            .await
            .expect("target collection should delete");
        assert!(matches!(
            harness
                .service
                .resolve(selector(ClassAddress::Id, &class))
                .await,
            Err(ApiError::NotFound(_))
        ));

        harness.finish().await;
    }

    #[rstest]
    #[case::postgres(ContractImplementation::PostgresAdapter)]
    #[case::memory(ContractImplementation::MemoryModel)]
    #[actix_web::test]
    async fn class_contract_rejects_a_stale_name_target(#[case] backend: ContractImplementation) {
        let harness = ContractHarness::new(backend, "stale_name").await;
        let class = harness.create("class").await;
        let name_target = harness
            .service
            .resolve(selector(ClassAddress::Name, &class))
            .await
            .expect("name target should resolve");
        let id_target = harness
            .service
            .resolve(selector(ClassAddress::Id, &class))
            .await
            .expect("id target should resolve");
        let renamed = harness
            .service
            .update(
                &id_target,
                UpdateHubuumClass {
                    name: Some(format!("{}_renamed", harness.prefix)),
                    collection_id: None,
                    json_schema: None,
                    validate_schema: None,
                    description: None,
                },
                &EventContext::system(),
            )
            .await
            .expect("class should rename");

        assert!(matches!(
            harness
                .service
                .update(
                    &name_target,
                    UpdateHubuumClass {
                        name: None,
                        collection_id: None,
                        json_schema: None,
                        validate_schema: None,
                        description: Some("stale write".to_string()),
                    },
                    &EventContext::system(),
                )
                .await,
            Err(ApiError::NotFound(_))
        ));

        harness.delete(&renamed).await;
        harness.finish().await;
    }

    #[rstest]
    #[case::postgres(ContractImplementation::PostgresAdapter)]
    #[case::memory(ContractImplementation::MemoryModel)]
    #[actix_web::test]
    async fn class_contract_rejects_invalid_json_schema(#[case] backend: ContractImplementation) {
        let harness = ContractHarness::new(backend, "invalid_schema").await;

        assert!(matches!(
            harness
                .service
                .create(
                    NewHubuumClass {
                        name: format!("{}_class", harness.prefix),
                        collection_id: harness.collection_id,
                        json_schema: Some(json!({"type": 7})),
                        validate_schema: Some(true),
                        description: "invalid schema contract".to_string(),
                    },
                    &EventContext::system(),
                )
                .await,
            Err(ApiError::BadRequest(_))
        ));

        harness.finish().await;
    }

    #[rstest]
    #[case::postgres(ContractImplementation::PostgresAdapter)]
    #[case::memory(ContractImplementation::MemoryModel)]
    #[actix_web::test]
    async fn class_contract_delete_removes_the_resolved_class(
        #[case] backend: ContractImplementation,
    ) {
        let harness = ContractHarness::new(backend, "delete").await;
        let class = harness.create("class").await;
        let target = harness
            .service
            .resolve(selector(ClassAddress::Id, &class))
            .await
            .expect("class should resolve");

        harness
            .service
            .delete(&target, &EventContext::system())
            .await
            .expect("class should delete");
        assert!(matches!(
            harness
                .service
                .resolve(selector(ClassAddress::Id, &class))
                .await,
            Err(ApiError::NotFound(_))
        ));

        harness.finish().await;
    }

    #[actix_web::test]
    async fn memory_class_events_exclude_no_op_updates() {
        let storage = MemoryStorageModel::new();
        let service = Services::from_resource_storage(storage.clone())
            .classes()
            .clone();
        let context = EventContext::system();
        let class = service
            .create(
                NewHubuumClass {
                    name: "memory_class_event_contract".to_string(),
                    collection_id: 1,
                    json_schema: None,
                    validate_schema: None,
                    description: "memory class event contract".to_string(),
                },
                &context,
            )
            .await
            .expect("memory class should be created");
        let target = service
            .resolve(selector(ClassAddress::Id, &class))
            .await
            .expect("memory class should resolve");
        let updated = service
            .update(
                &target,
                UpdateHubuumClass {
                    name: None,
                    collection_id: None,
                    json_schema: None,
                    validate_schema: None,
                    description: Some("changed memory class event contract".to_string()),
                },
                &context,
            )
            .await
            .expect("memory class should update");
        let updated_target = service
            .resolve(selector(ClassAddress::Id, &updated))
            .await
            .expect("updated memory class should resolve");
        service
            .update(
                &updated_target,
                UpdateHubuumClass {
                    name: Some(updated.name.clone()),
                    collection_id: Some(updated.collection_id),
                    json_schema: None,
                    validate_schema: Some(updated.validate_schema),
                    description: Some(updated.description.clone()),
                },
                &context,
            )
            .await
            .expect("memory no-op should succeed");
        service
            .delete(&updated_target, &context)
            .await
            .expect("memory class should delete");

        let events = storage.class_events().await;
        assert_eq!(
            events.iter().map(|event| event.action).collect::<Vec<_>>(),
            vec![Action::Created, Action::Updated, Action::Deleted]
        );
        assert!(
            events
                .iter()
                .all(|event| event.class_id == class.id && event.context == context)
        );
    }
}
