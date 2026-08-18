use std::sync::Arc;

use crate::errors::ApiError;
use crate::events::EventContext;
use crate::models::{
    HubuumClassRelationID, NewHubuumClassRelation, PreparedClassRelation,
    ResolvedClassRelationTarget,
};
use crate::services::storage_boundary::{
    class_relation_create_to_storage, prepared_class_relation_from_storage,
    prepared_class_relation_to_storage, resolved_class_relation_from_storage,
    resolved_class_relation_to_storage,
};
use crate::storage::ClassRelationStore;

/// Application-facing class-relation lifecycle use cases.
#[derive(Clone)]
pub struct ClassRelationService {
    storage: Arc<dyn ClassRelationStore>,
}

impl ClassRelationService {
    pub(crate) fn new(storage: Arc<dyn ClassRelationStore>) -> Self {
        Self { storage }
    }

    pub async fn prepare_create(
        &self,
        command: NewHubuumClassRelation,
    ) -> Result<PreparedClassRelation, ApiError> {
        self.storage
            .prepare_class_relation(class_relation_create_to_storage(command))
            .await
            .map_err(ApiError::from)
            .and_then(prepared_class_relation_from_storage)
    }

    pub async fn resolve(
        &self,
        id: HubuumClassRelationID,
    ) -> Result<ResolvedClassRelationTarget, ApiError> {
        self.storage
            .resolve_class_relation(id.id())
            .await
            .map_err(ApiError::from)
            .and_then(resolved_class_relation_from_storage)
    }

    pub async fn create(
        &self,
        prepared: &PreparedClassRelation,
        context: &EventContext,
    ) -> Result<ResolvedClassRelationTarget, ApiError> {
        let prepared = prepared_class_relation_to_storage(prepared);
        self.storage
            .create_class_relation(&prepared, context)
            .await
            .map_err(ApiError::from)
            .map(|outcome| outcome.into_value())
            .and_then(resolved_class_relation_from_storage)
    }

    pub async fn delete(
        &self,
        target: &ResolvedClassRelationTarget,
        context: &EventContext,
    ) -> Result<(), ApiError> {
        let target = resolved_class_relation_to_storage(target);
        self.storage
            .delete_class_relation(&target, context)
            .await
            .map_err(ApiError::from)
            .map(|outcome| outcome.into_value())
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use crate::errors::ApiError;
    use crate::events::{Action, EventContext};
    use crate::models::{
        ClassSelector, GroupID, HubuumClass, HubuumClassID, HubuumClassRelationID,
        NewCollectionWithAssignee, NewGroup, NewHubuumClass, NewHubuumClassRelation,
        ObjectRelationLimit, ResourceRevision, UpdateHubuumClass,
    };
    use crate::services::Services;
    use crate::storage::{MemoryStorageModel, PostgresStorage};
    use crate::tests::CollectionFixture;
    use crate::tests::storage_contract::{
        LifecycleContractImplementation as ContractImplementation, pool as storage_contract_pool,
        postgres_permit as storage_contract_postgres_permit, prefix as storage_contract_prefix,
    };
    use crate::traits::CanSave;

    struct ContractHarness {
        services: Services,
        storage: Option<MemoryStorageModel>,
        collection_id: i32,
        prefix: String,
        postgres_cleanup: Option<CollectionFixture>,
        _postgres_permit: Option<tokio::sync::OwnedSemaphorePermit>,
    }

    impl ContractHarness {
        async fn new(backend: ContractImplementation, label: &str) -> Self {
            match backend {
                ContractImplementation::MemoryModel => {
                    let storage = MemoryStorageModel::new();
                    let services = Services::from_resource_storage(storage.clone());
                    Self {
                        services,
                        storage: Some(storage),
                        collection_id: 1,
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
                        description: Some("class relation storage contract owner".to_string()),
                    }
                    .save_without_events(&pool)
                    .await
                    .expect("contract owner group should save");
                    let collection = NewCollectionWithAssignee {
                        name: format!("{prefix}_collection"),
                        description: "class relation storage contract collection".to_string(),
                        group_id: GroupID::new(owner_group.id).expect("valid owner group id"),
                        parent_collection_id: None,
                    }
                    .save_without_events(&pool)
                    .await
                    .expect("contract collection should save");
                    let collection_id = collection.id;
                    let fixture = CollectionFixture {
                        pool: pool.clone(),
                        collection,
                        owner_group,
                        prefix: prefix.clone(),
                    };
                    Self {
                        services: Services::from_resource_storage(PostgresStorage::unobserved(
                            pool.get_ref().clone(),
                        )),
                        storage: None,
                        collection_id,
                        prefix,
                        postgres_cleanup: Some(fixture),
                        _postgres_permit: Some(permit),
                    }
                }
            }
        }

        async fn create_class(&self, label: &str) -> HubuumClass {
            self.services
                .classes()
                .create(
                    NewHubuumClass {
                        name: format!("{}_{}", self.prefix, label),
                        collection_id: self.collection_id,
                        json_schema: None,
                        validate_schema: None,
                        description: format!("class relation contract {label}"),
                    },
                    &EventContext::system(),
                )
                .await
                .expect("contract class should create")
        }

        fn command(
            &self,
            from_class: &HubuumClass,
            to_class: &HubuumClass,
        ) -> NewHubuumClassRelation {
            NewHubuumClassRelation {
                from_hubuum_class_id: from_class.id,
                to_hubuum_class_id: to_class.id,
                forward_template_alias: None,
                reverse_template_alias: None,
                from_max_relations: None,
                to_max_relations: None,
            }
        }

        async fn finish(self) {
            if let Some(fixture) = self.postgres_cleanup {
                fixture.cleanup().await.expect("contract fixture cleanup");
            }
        }
    }

    #[rstest]
    #[case::postgres(ContractImplementation::PostgresAdapter)]
    #[case::memory(ContractImplementation::MemoryModel)]
    #[actix_web::test]
    async fn class_relation_contract_normalizes_directional_settings(
        #[case] backend: ContractImplementation,
    ) {
        let harness = ContractHarness::new(backend, "normalize").await;
        let lower = harness.create_class("lower").await;
        let higher = harness.create_class("higher").await;
        let prepared = harness
            .services
            .class_relations()
            .prepare_create(NewHubuumClassRelation {
                from_hubuum_class_id: higher.id,
                to_hubuum_class_id: lower.id,
                forward_template_alias: Some("Higher Links".to_string()),
                reverse_template_alias: Some("Lower Links".to_string()),
                from_max_relations: Some(ObjectRelationLimit::new(1).expect("valid limit")),
                to_max_relations: Some(ObjectRelationLimit::new(2).expect("valid limit")),
            })
            .await
            .expect("relation should prepare");

        assert_eq!(prepared.from_class().id, lower.id);
        assert_eq!(prepared.to_class().id, higher.id);
        assert_eq!(
            prepared.command().forward_template_alias.as_deref(),
            Some("lower_links")
        );
        assert_eq!(
            prepared.command().reverse_template_alias.as_deref(),
            Some("higher_links")
        );
        assert_eq!(
            prepared.command().from_max_relations,
            Some(ObjectRelationLimit::new(2).expect("valid limit"))
        );
        assert_eq!(
            prepared.command().to_max_relations,
            Some(ObjectRelationLimit::new(1).expect("valid limit"))
        );

        harness.finish().await;
    }

    #[rstest]
    #[case::postgres(ContractImplementation::PostgresAdapter)]
    #[case::memory(ContractImplementation::MemoryModel)]
    #[actix_web::test]
    async fn class_relation_contract_rejects_self_relations(
        #[case] backend: ContractImplementation,
    ) {
        let harness = ContractHarness::new(backend, "self_relation").await;
        let class = harness.create_class("class").await;
        let error = harness
            .services
            .class_relations()
            .prepare_create(harness.command(&class, &class))
            .await
            .expect_err("self relation should fail");
        assert!(matches!(error, ApiError::BadRequest(_)));
        harness.finish().await;
    }

    #[rstest]
    #[case::postgres(ContractImplementation::PostgresAdapter)]
    #[case::memory(ContractImplementation::MemoryModel)]
    #[actix_web::test]
    async fn class_relation_contract_creates_and_resolves_the_endpoint_aggregate(
        #[case] backend: ContractImplementation,
    ) {
        let harness = ContractHarness::new(backend, "create_resolve").await;
        let from_class = harness.create_class("from").await;
        let to_class = harness.create_class("to").await;
        let prepared = harness
            .services
            .class_relations()
            .prepare_create(harness.command(&from_class, &to_class))
            .await
            .expect("relation should prepare");
        let created = harness
            .services
            .class_relations()
            .create(&prepared, &EventContext::system())
            .await
            .expect("relation should create");
        let resolved = harness
            .services
            .class_relations()
            .resolve(
                HubuumClassRelationID::new(created.relation().id).expect("valid class relation id"),
            )
            .await
            .expect("relation should resolve");

        assert_eq!(resolved.relation(), created.relation());
        assert_eq!(resolved.relation().revision, ResourceRevision::INITIAL);
        assert_eq!(resolved.from_class(), &from_class);
        assert_eq!(resolved.to_class(), &to_class);
        harness.finish().await;
    }

    #[rstest]
    #[case::postgres(ContractImplementation::PostgresAdapter)]
    #[case::memory(ContractImplementation::MemoryModel)]
    #[actix_web::test]
    async fn class_relation_contract_audits_compatibility_writes_as_system(
        #[case] backend: ContractImplementation,
    ) {
        let harness = ContractHarness::new(backend, "event_suppressed").await;
        let from_class = harness.create_class("from").await;
        let to_class = harness.create_class("to").await;
        let lifecycle = &harness.services.class_relations().storage;
        let created = lifecycle
            .create_class_relation_from_command(
                crate::services::storage_boundary::class_relation_create_to_storage(
                    harness.command(&from_class, &to_class),
                ),
                &EventContext::system(),
            )
            .await
            .expect("event-suppressed relation should create");
        let relation_id =
            crate::services::storage_boundary::class_relation_from_storage(created.into_value())
                .expect("valid stored class relation")
                .id;
        lifecycle
            .resolve_class_relation(relation_id)
            .await
            .expect("event-suppressed relation should resolve");
        lifecycle
            .delete_class_relation_by_id(relation_id, &EventContext::system())
            .await
            .expect("event-suppressed relation should delete")
            .into_value();

        assert!(lifecycle.resolve_class_relation(relation_id).await.is_err());
        if let Some(storage) = harness.storage.as_ref() {
            assert_eq!(storage.class_relation_events().await.len(), 2);
        }
        harness.finish().await;
    }

    #[rstest]
    #[case::postgres(ContractImplementation::PostgresAdapter)]
    #[case::memory(ContractImplementation::MemoryModel)]
    #[actix_web::test]
    async fn class_relation_contract_rejects_duplicate_endpoint_pairs(
        #[case] backend: ContractImplementation,
    ) {
        let harness = ContractHarness::new(backend, "duplicate").await;
        let from_class = harness.create_class("from").await;
        let to_class = harness.create_class("to").await;
        let prepared = harness
            .services
            .class_relations()
            .prepare_create(harness.command(&from_class, &to_class))
            .await
            .expect("relation should prepare");
        harness
            .services
            .class_relations()
            .create(&prepared, &EventContext::system())
            .await
            .expect("first relation should create");
        let error = harness
            .services
            .class_relations()
            .create(&prepared, &EventContext::system())
            .await
            .expect_err("duplicate relation should fail");
        assert!(matches!(error, ApiError::Conflict(_)));
        harness.finish().await;
    }

    #[rstest]
    #[case::postgres(ContractImplementation::PostgresAdapter)]
    #[case::memory(ContractImplementation::MemoryModel)]
    #[actix_web::test]
    async fn class_relation_contract_rejects_a_stale_prepared_endpoint(
        #[case] backend: ContractImplementation,
    ) {
        let harness = ContractHarness::new(backend, "stale_prepare").await;
        let from_class = harness.create_class("from").await;
        let to_class = harness.create_class("to").await;
        let prepared = harness
            .services
            .class_relations()
            .prepare_create(harness.command(&from_class, &to_class))
            .await
            .expect("relation should prepare");
        let class_target = harness
            .services
            .classes()
            .resolve(ClassSelector::by_id(
                HubuumClassID::new(from_class.id).expect("valid class id"),
            ))
            .await
            .expect("class should resolve");
        harness
            .services
            .classes()
            .update(
                &class_target,
                UpdateHubuumClass {
                    name: None,
                    collection_id: None,
                    json_schema: None,
                    validate_schema: None,
                    description: Some("changed after authorization".to_string()),
                },
                &EventContext::system(),
            )
            .await
            .expect("class should update");

        let error = harness
            .services
            .class_relations()
            .create(&prepared, &EventContext::system())
            .await
            .expect_err("stale prepared endpoint should fail");
        assert!(matches!(error, ApiError::NotFound(_)));
        harness.finish().await;
    }

    #[rstest]
    #[case::postgres(ContractImplementation::PostgresAdapter)]
    #[case::memory(ContractImplementation::MemoryModel)]
    #[actix_web::test]
    async fn class_relation_contract_deletes_the_resolved_relation(
        #[case] backend: ContractImplementation,
    ) {
        let harness = ContractHarness::new(backend, "delete").await;
        let from_class = harness.create_class("from").await;
        let to_class = harness.create_class("to").await;
        let prepared = harness
            .services
            .class_relations()
            .prepare_create(harness.command(&from_class, &to_class))
            .await
            .expect("relation should prepare");
        let created = harness
            .services
            .class_relations()
            .create(&prepared, &EventContext::system())
            .await
            .expect("relation should create");
        let relation_id =
            HubuumClassRelationID::new(created.relation().id).expect("valid relation id");
        harness
            .services
            .class_relations()
            .delete(&created, &EventContext::system())
            .await
            .expect("relation should delete");
        assert!(matches!(
            harness
                .services
                .class_relations()
                .resolve(relation_id)
                .await,
            Err(ApiError::NotFound(_))
        ));
        harness.finish().await;
    }

    #[rstest]
    #[case::postgres(ContractImplementation::PostgresAdapter)]
    #[case::memory(ContractImplementation::MemoryModel)]
    #[actix_web::test]
    async fn class_relation_contract_class_delete_cascades_the_relation(
        #[case] backend: ContractImplementation,
    ) {
        let harness = ContractHarness::new(backend, "class_cascade").await;
        let from_class = harness.create_class("from").await;
        let to_class = harness.create_class("to").await;
        let prepared = harness
            .services
            .class_relations()
            .prepare_create(harness.command(&from_class, &to_class))
            .await
            .expect("relation should prepare");
        let created = harness
            .services
            .class_relations()
            .create(&prepared, &EventContext::system())
            .await
            .expect("relation should create");
        let relation_id =
            HubuumClassRelationID::new(created.relation().id).expect("valid relation id");
        let class_target = harness
            .services
            .classes()
            .resolve(ClassSelector::by_id(
                HubuumClassID::new(from_class.id).expect("valid class id"),
            ))
            .await
            .expect("class should resolve");
        harness
            .services
            .classes()
            .delete(&class_target, &EventContext::system())
            .await
            .expect("class should delete");

        assert!(matches!(
            harness
                .services
                .class_relations()
                .resolve(relation_id)
                .await,
            Err(ApiError::NotFound(_))
        ));
        harness.finish().await;
    }

    #[actix_web::test]
    async fn memory_class_relation_events_cover_explicit_lifecycle_writes() {
        let harness = ContractHarness::new(ContractImplementation::MemoryModel, "events").await;
        let from_class = harness.create_class("from").await;
        let to_class = harness.create_class("to").await;
        let context = EventContext::system();
        let prepared = harness
            .services
            .class_relations()
            .prepare_create(harness.command(&from_class, &to_class))
            .await
            .expect("relation should prepare");
        let created = harness
            .services
            .class_relations()
            .create(&prepared, &context)
            .await
            .expect("relation should create");
        harness
            .services
            .class_relations()
            .delete(&created, &context)
            .await
            .expect("relation should delete");

        let events = harness
            .storage
            .as_ref()
            .expect("memory harness should expose storage")
            .class_relation_events()
            .await;
        assert_eq!(
            events.iter().map(|event| event.action).collect::<Vec<_>>(),
            vec![Action::Created, Action::Deleted]
        );
        assert!(events.iter().all(|event| {
            event.class_relation_id == created.relation().id && event.context == context
        }));
        harness.finish().await;
    }
}
