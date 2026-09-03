use std::sync::Arc;

use crate::errors::ApiError;
use crate::events::EventContext;
use crate::models::{
    ObjectRelationCreateSelector, ObjectRelationSelector, PreparedObjectRelation,
    ResolvedObjectRelationTarget,
};
use crate::services::storage_boundary::{
    object_relation_create_selector_to_storage, object_relation_id_to_storage,
    object_relation_selector_to_storage, prepared_object_relation_from_storage,
    prepared_object_relation_to_storage, resolved_object_relation_from_storage,
    resolved_object_relation_to_storage,
};
use crate::storage::{
    ObjectRelationStorage, StorageError, StorageMutationOutcome, StorageObjectRelation,
    StorageObjectRelationCreate, StorageObjectRelationCreateSelector,
    StorageObjectRelationSelector,
};

/// Compose the canonical prepare/create relation lifecycle for an explicit
/// relation command.
pub(crate) async fn prepare_and_create_object_relation(
    storage: &dyn ObjectRelationStorage,
    command: StorageObjectRelationCreate,
    context: &EventContext,
) -> Result<StorageMutationOutcome<StorageObjectRelation>, StorageError> {
    let prepared = storage
        .prepare_object_relation(StorageObjectRelationCreateSelector::Explicit(command))
        .await?;
    Ok(storage
        .create_object_relation(&prepared, context)
        .await?
        .map(|resolved| resolved.into_parts().0))
}

/// Compose the canonical resolve/delete relation lifecycle for an identifier.
pub(crate) async fn resolve_and_delete_object_relation(
    storage: &dyn ObjectRelationStorage,
    relation_id: i32,
    context: &EventContext,
) -> Result<StorageMutationOutcome<()>, StorageError> {
    let target = storage
        .resolve_object_relation(StorageObjectRelationSelector::Id(
            object_relation_id_to_storage(relation_id),
        ))
        .await?;
    storage.delete_object_relation(&target, context).await
}

/// Application-facing object-relation lifecycle use cases.
#[derive(Clone)]
pub struct ObjectRelationService {
    storage: Arc<dyn ObjectRelationStorage>,
}

impl ObjectRelationService {
    pub(crate) fn new(storage: Arc<dyn ObjectRelationStorage>) -> Self {
        Self { storage }
    }

    pub async fn prepare_create(
        &self,
        selector: ObjectRelationCreateSelector,
    ) -> Result<PreparedObjectRelation, ApiError> {
        self.storage
            .prepare_object_relation(object_relation_create_selector_to_storage(selector))
            .await
            .map_err(ApiError::from)
            .and_then(prepared_object_relation_from_storage)
    }

    pub async fn resolve(
        &self,
        selector: ObjectRelationSelector,
    ) -> Result<ResolvedObjectRelationTarget, ApiError> {
        self.storage
            .resolve_object_relation(object_relation_selector_to_storage(selector))
            .await
            .map_err(ApiError::from)
            .and_then(resolved_object_relation_from_storage)
    }

    pub async fn create(
        &self,
        prepared: &PreparedObjectRelation,
        context: &EventContext,
    ) -> Result<ResolvedObjectRelationTarget, ApiError> {
        let prepared = prepared_object_relation_to_storage(prepared)?;
        self.storage
            .create_object_relation(&prepared, context)
            .await
            .map_err(ApiError::from)
            .map(|outcome| outcome.into_value())
            .and_then(resolved_object_relation_from_storage)
    }

    pub async fn delete(
        &self,
        target: &ResolvedObjectRelationTarget,
        context: &EventContext,
    ) -> Result<(), ApiError> {
        let target = resolved_object_relation_to_storage(target)?;
        self.storage
            .delete_object_relation(&target, context)
            .await
            .map_err(ApiError::from)
            .map(|outcome| outcome.into_value())
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use serde_json::json;

    use super::{prepare_and_create_object_relation, resolve_and_delete_object_relation};

    use crate::errors::ApiError;
    use crate::events::{Action, EventContext};
    use crate::models::{
        ClassSelector, GroupID, HubuumClass, HubuumClassID, HubuumObject, HubuumObjectID,
        HubuumObjectRelationID, NewCollectionWithAssignee, NewGroup, NewHubuumClass,
        NewHubuumClassRelation, NewHubuumObject, NewHubuumObjectRelation,
        ObjectRelationCreateSelector, ObjectRelationEndpoint, ObjectRelationLimit,
        ObjectRelationSelector, ResolvedClassRelationTarget, ResourceRevision,
    };
    use crate::services::Services;
    use crate::storage::MemoryStorageModel;
    use crate::tests::CollectionFixture;
    use crate::tests::storage_contract::{
        LifecycleContractImplementation as ContractImplementation, pool as storage_contract_pool,
        postgres_permit as storage_contract_postgres_permit, prefix as storage_contract_prefix,
    };
    use crate::traits::CanSave;
    use hubuum_storage_postgres::PostgresStorage;

    #[derive(Clone, Copy, Debug)]
    enum RelationAddress {
        Id,
        Between,
    }

    struct RelationFixture {
        from_class: HubuumClass,
        to_class: HubuumClass,
        class_relation: ResolvedClassRelationTarget,
        from_object: HubuumObject,
        to_object: HubuumObject,
    }

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
                        description: Some("object relation storage contract owner".to_string()),
                    }
                    .save_without_events(&pool)
                    .await
                    .expect("contract owner group should save");
                    let collection = NewCollectionWithAssignee {
                        name: format!("{prefix}_collection"),
                        description: "object relation storage contract collection".to_string(),
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
                        description: format!("object relation contract {label}"),
                    },
                    &EventContext::system(),
                )
                .await
                .expect("contract class should create")
        }

        async fn create_object(&self, class: &HubuumClass, label: &str) -> HubuumObject {
            let class_target = self
                .services
                .classes()
                .resolve(ClassSelector::by_id(
                    HubuumClassID::new(class.id).expect("valid class id"),
                ))
                .await
                .expect("class should resolve");
            self.services
                .objects()
                .create(
                    &class_target,
                    NewHubuumObject {
                        name: format!("{}_{}", self.prefix, label),
                        collection_id: class.collection_id,
                        hubuum_class_id: class.id,
                        data: json!({"label": label}),
                        description: format!("object relation contract {label}"),
                    },
                    &EventContext::system(),
                )
                .await
                .expect("contract object should create")
        }

        async fn create_class_relation(
            &self,
            from_class: &HubuumClass,
            to_class: &HubuumClass,
            from_limit: Option<ObjectRelationLimit>,
            to_limit: Option<ObjectRelationLimit>,
        ) -> ResolvedClassRelationTarget {
            let prepared = self
                .services
                .class_relations()
                .prepare_create(NewHubuumClassRelation {
                    from_hubuum_class_id: from_class.id,
                    to_hubuum_class_id: to_class.id,
                    forward_template_alias: None,
                    reverse_template_alias: None,
                    from_max_relations: from_limit,
                    to_max_relations: to_limit,
                })
                .await
                .expect("class relation should prepare");
            self.services
                .class_relations()
                .create(&prepared, &EventContext::system())
                .await
                .expect("class relation should create")
        }

        async fn fixture(&self, label: &str) -> RelationFixture {
            let from_class = self.create_class(&format!("{label}_from_class")).await;
            let to_class = self.create_class(&format!("{label}_to_class")).await;
            let class_relation = self
                .create_class_relation(&from_class, &to_class, None, None)
                .await;
            let from_object = self
                .create_object(&from_class, &format!("{label}_from_object"))
                .await;
            let to_object = self
                .create_object(&to_class, &format!("{label}_to_object"))
                .await;
            RelationFixture {
                from_class,
                to_class,
                class_relation,
                from_object,
                to_object,
            }
        }

        fn explicit_selector(&self, fixture: &RelationFixture) -> ObjectRelationCreateSelector {
            ObjectRelationCreateSelector::explicit(NewHubuumObjectRelation {
                from_hubuum_object_id: fixture.from_object.id,
                to_hubuum_object_id: fixture.to_object.id,
                class_relation_id: fixture.class_relation.relation().id,
            })
        }

        fn between_create_selector(
            &self,
            fixture: &RelationFixture,
        ) -> ObjectRelationCreateSelector {
            ObjectRelationCreateSelector::between(
                ObjectRelationEndpoint::new(
                    HubuumClassID::new(fixture.from_class.id).expect("valid from class id"),
                    HubuumObjectID::new(fixture.from_object.id).expect("valid from object id"),
                ),
                ObjectRelationEndpoint::new(
                    HubuumClassID::new(fixture.to_class.id).expect("valid to class id"),
                    HubuumObjectID::new(fixture.to_object.id).expect("valid to object id"),
                ),
            )
        }

        fn selector(
            &self,
            address: RelationAddress,
            fixture: &RelationFixture,
            relation_id: i32,
        ) -> ObjectRelationSelector {
            match address {
                RelationAddress::Id => ObjectRelationSelector::by_id(
                    HubuumObjectRelationID::new(relation_id).expect("valid object relation id"),
                ),
                RelationAddress::Between => ObjectRelationSelector::between(
                    ObjectRelationEndpoint::new(
                        HubuumClassID::new(fixture.from_class.id).expect("valid from class id"),
                        HubuumObjectID::new(fixture.from_object.id).expect("valid from object id"),
                    ),
                    ObjectRelationEndpoint::new(
                        HubuumClassID::new(fixture.to_class.id).expect("valid to class id"),
                        HubuumObjectID::new(fixture.to_object.id).expect("valid to object id"),
                    ),
                ),
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
    async fn object_relation_contract_prepares_a_normalized_endpoint_aggregate(
        #[case] backend: ContractImplementation,
    ) {
        let harness = ContractHarness::new(backend, "normalize").await;
        let fixture = harness.fixture("normalize").await;
        let prepared = harness
            .services
            .object_relations()
            .prepare_create(ObjectRelationCreateSelector::explicit(
                NewHubuumObjectRelation {
                    from_hubuum_object_id: fixture.to_object.id,
                    to_hubuum_object_id: fixture.from_object.id,
                    class_relation_id: fixture.class_relation.relation().id,
                },
            ))
            .await
            .expect("relation should prepare");

        assert!(prepared.from_object().id < prepared.to_object().id);
        assert_eq!(
            prepared.command().from_hubuum_object_id,
            prepared.from_object().id
        );
        assert_eq!(
            prepared.command().to_hubuum_object_id,
            prepared.to_object().id
        );
        assert_eq!(
            prepared.class_relation().relation(),
            fixture.class_relation.relation()
        );
        harness.finish().await;
    }

    #[rstest]
    #[case::postgres_id(ContractImplementation::PostgresAdapter, RelationAddress::Id)]
    #[case::postgres_between(ContractImplementation::PostgresAdapter, RelationAddress::Between)]
    #[case::memory_id(ContractImplementation::MemoryModel, RelationAddress::Id)]
    #[case::memory_between(ContractImplementation::MemoryModel, RelationAddress::Between)]
    #[actix_web::test]
    async fn object_relation_contract_resolves_explicit_addresses(
        #[case] backend: ContractImplementation,
        #[case] address: RelationAddress,
    ) {
        let harness = ContractHarness::new(backend, "resolve").await;
        let fixture = harness.fixture("resolve").await;
        let prepared = harness
            .services
            .object_relations()
            .prepare_create(harness.explicit_selector(&fixture))
            .await
            .expect("relation should prepare");
        let created = harness
            .services
            .object_relations()
            .create(&prepared, &EventContext::system())
            .await
            .expect("relation should create");
        let resolved = harness
            .services
            .object_relations()
            .resolve(harness.selector(address, &fixture, created.relation().id))
            .await
            .expect("relation should resolve");

        assert_eq!(resolved.relation(), created.relation());
        assert_eq!(resolved.relation().revision, ResourceRevision::INITIAL);
        assert_eq!(resolved.from_object(), prepared.from_object());
        assert_eq!(resolved.to_object(), prepared.to_object());
        harness.finish().await;
    }

    #[rstest]
    #[case::postgres(ContractImplementation::PostgresAdapter)]
    #[case::memory(ContractImplementation::MemoryModel)]
    #[actix_web::test]
    async fn object_relation_contract_audits_composed_lifecycle_writes_as_system(
        #[case] backend: ContractImplementation,
    ) {
        let harness = ContractHarness::new(backend, "event_suppressed").await;
        let fixture = harness.fixture("event_suppressed").await;
        let lifecycle = &harness.services.object_relations().storage;
        let created = prepare_and_create_object_relation(
            lifecycle.as_ref(),
            crate::services::storage_boundary::object_relation_create_to_storage(
                NewHubuumObjectRelation {
                    from_hubuum_object_id: fixture.from_object.id,
                    to_hubuum_object_id: fixture.to_object.id,
                    class_relation_id: fixture.class_relation.relation().id,
                },
            ),
            &EventContext::system(),
        )
        .await
        .expect("event-suppressed relation should create");
        let relation_id =
            crate::services::storage_boundary::object_relation_from_storage(created.into_value())
                .expect("valid stored object relation")
                .id;
        lifecycle
            .resolve_object_relation(crate::storage::StorageObjectRelationSelector::Id(
                crate::services::storage_boundary::object_relation_id_to_storage(relation_id),
            ))
            .await
            .expect("event-suppressed relation should resolve");
        resolve_and_delete_object_relation(
            lifecycle.as_ref(),
            relation_id,
            &EventContext::system(),
        )
        .await
        .expect("event-suppressed relation should delete")
        .into_value();

        assert!(
            lifecycle
                .resolve_object_relation(crate::storage::StorageObjectRelationSelector::Id(
                    crate::services::storage_boundary::object_relation_id_to_storage(relation_id),
                ))
                .await
                .is_err()
        );
        if let Some(storage) = harness.storage.as_ref() {
            assert_eq!(storage.object_relation_events().await.len(), 2);
        }
        harness.finish().await;
    }

    #[rstest]
    #[case::postgres(ContractImplementation::PostgresAdapter)]
    #[case::memory(ContractImplementation::MemoryModel)]
    #[actix_web::test]
    async fn object_relation_contract_between_preparation_validates_path_membership(
        #[case] backend: ContractImplementation,
    ) {
        let harness = ContractHarness::new(backend, "path_membership").await;
        let fixture = harness.fixture("path_membership").await;
        let error = harness
            .services
            .object_relations()
            .prepare_create(ObjectRelationCreateSelector::between(
                ObjectRelationEndpoint::new(
                    HubuumClassID::new(fixture.to_class.id).expect("valid wrong class id"),
                    HubuumObjectID::new(fixture.from_object.id).expect("valid object id"),
                ),
                ObjectRelationEndpoint::new(
                    HubuumClassID::new(fixture.from_class.id).expect("valid wrong class id"),
                    HubuumObjectID::new(fixture.to_object.id).expect("valid object id"),
                ),
            ))
            .await
            .expect_err("path membership mismatch should fail");
        assert!(matches!(error, ApiError::NotFound(_)));
        harness.finish().await;
    }

    #[rstest]
    #[case::postgres(ContractImplementation::PostgresAdapter)]
    #[case::memory(ContractImplementation::MemoryModel)]
    #[actix_web::test]
    async fn object_relation_contract_rejects_self_relations(
        #[case] backend: ContractImplementation,
    ) {
        let harness = ContractHarness::new(backend, "self_relation").await;
        let fixture = harness.fixture("self_relation").await;
        let error = harness
            .services
            .object_relations()
            .prepare_create(ObjectRelationCreateSelector::explicit(
                NewHubuumObjectRelation {
                    from_hubuum_object_id: fixture.from_object.id,
                    to_hubuum_object_id: fixture.from_object.id,
                    class_relation_id: fixture.class_relation.relation().id,
                },
            ))
            .await
            .expect_err("self relation should fail");
        assert!(matches!(error, ApiError::BadRequest(_)));
        harness.finish().await;
    }

    #[rstest]
    #[case::postgres(ContractImplementation::PostgresAdapter)]
    #[case::memory(ContractImplementation::MemoryModel)]
    #[actix_web::test]
    async fn object_relation_contract_rejects_objects_from_the_same_class(
        #[case] backend: ContractImplementation,
    ) {
        let harness = ContractHarness::new(backend, "same_class").await;
        let fixture = harness.fixture("same_class").await;
        let sibling = harness
            .create_object(&fixture.from_class, "same_class_sibling")
            .await;
        let error = harness
            .services
            .object_relations()
            .prepare_create(ObjectRelationCreateSelector::explicit(
                NewHubuumObjectRelation {
                    from_hubuum_object_id: fixture.from_object.id,
                    to_hubuum_object_id: sibling.id,
                    class_relation_id: fixture.class_relation.relation().id,
                },
            ))
            .await
            .expect_err("same-class objects should fail");
        assert!(matches!(error, ApiError::BadRequest(_)));
        harness.finish().await;
    }

    #[rstest]
    #[case::postgres(ContractImplementation::PostgresAdapter)]
    #[case::memory(ContractImplementation::MemoryModel)]
    #[actix_web::test]
    async fn object_relation_contract_rejects_a_mismatched_class_relation(
        #[case] backend: ContractImplementation,
    ) {
        let harness = ContractHarness::new(backend, "class_mismatch").await;
        let fixture = harness.fixture("class_mismatch").await;
        let third_class = harness.create_class("class_mismatch_third").await;
        let mismatched = harness
            .create_class_relation(&fixture.from_class, &third_class, None, None)
            .await;
        let error = harness
            .services
            .object_relations()
            .prepare_create(ObjectRelationCreateSelector::explicit(
                NewHubuumObjectRelation {
                    from_hubuum_object_id: fixture.from_object.id,
                    to_hubuum_object_id: fixture.to_object.id,
                    class_relation_id: mismatched.relation().id,
                },
            ))
            .await
            .expect_err("mismatched class relation should fail");
        assert!(matches!(error, ApiError::BadRequest(_)));
        harness.finish().await;
    }

    #[rstest]
    #[case::postgres(ContractImplementation::PostgresAdapter)]
    #[case::memory(ContractImplementation::MemoryModel)]
    #[actix_web::test]
    async fn object_relation_contract_rejects_reverse_duplicates(
        #[case] backend: ContractImplementation,
    ) {
        let harness = ContractHarness::new(backend, "duplicate").await;
        let fixture = harness.fixture("duplicate").await;
        let prepared = harness
            .services
            .object_relations()
            .prepare_create(harness.explicit_selector(&fixture))
            .await
            .expect("relation should prepare");
        harness
            .services
            .object_relations()
            .create(&prepared, &EventContext::system())
            .await
            .expect("first relation should create");
        let reverse = harness
            .services
            .object_relations()
            .prepare_create(ObjectRelationCreateSelector::explicit(
                NewHubuumObjectRelation {
                    from_hubuum_object_id: fixture.to_object.id,
                    to_hubuum_object_id: fixture.from_object.id,
                    class_relation_id: fixture.class_relation.relation().id,
                },
            ))
            .await
            .expect("reverse relation should prepare");
        let error = harness
            .services
            .object_relations()
            .create(&reverse, &EventContext::system())
            .await
            .expect_err("reverse duplicate should fail");
        assert!(matches!(error, ApiError::Conflict(_)));
        harness.finish().await;
    }

    #[rstest]
    #[case::postgres(ContractImplementation::PostgresAdapter)]
    #[case::memory(ContractImplementation::MemoryModel)]
    #[actix_web::test]
    async fn object_relation_contract_enforces_directional_cardinality(
        #[case] backend: ContractImplementation,
    ) {
        let harness = ContractHarness::new(backend, "cardinality").await;
        let from_class = harness.create_class("cardinality_from_class").await;
        let to_class = harness.create_class("cardinality_to_class").await;
        let class_relation = harness
            .create_class_relation(
                &from_class,
                &to_class,
                Some(ObjectRelationLimit::new(1).expect("valid limit")),
                None,
            )
            .await;
        let from_object = harness.create_object(&from_class, "cardinality_from").await;
        let first_to = harness
            .create_object(&to_class, "cardinality_first_to")
            .await;
        let second_to = harness
            .create_object(&to_class, "cardinality_second_to")
            .await;
        let command = |to_object: &HubuumObject| {
            ObjectRelationCreateSelector::explicit(NewHubuumObjectRelation {
                from_hubuum_object_id: from_object.id,
                to_hubuum_object_id: to_object.id,
                class_relation_id: class_relation.relation().id,
            })
        };
        let first = harness
            .services
            .object_relations()
            .prepare_create(command(&first_to))
            .await
            .expect("first relation should prepare");
        harness
            .services
            .object_relations()
            .create(&first, &EventContext::system())
            .await
            .expect("first relation should create");
        let second = harness
            .services
            .object_relations()
            .prepare_create(command(&second_to))
            .await
            .expect("second relation should prepare");
        let error = harness
            .services
            .object_relations()
            .create(&second, &EventContext::system())
            .await
            .expect_err("cardinality should reject second relation");
        assert!(matches!(error, ApiError::Conflict(_)));
        harness.finish().await;
    }

    #[rstest]
    #[case::postgres(ContractImplementation::PostgresAdapter)]
    #[case::memory(ContractImplementation::MemoryModel)]
    #[actix_web::test]
    async fn object_relation_contract_deletes_the_resolved_relation(
        #[case] backend: ContractImplementation,
    ) {
        let harness = ContractHarness::new(backend, "delete").await;
        let fixture = harness.fixture("delete").await;
        let prepared = harness
            .services
            .object_relations()
            .prepare_create(harness.explicit_selector(&fixture))
            .await
            .expect("relation should prepare");
        let created = harness
            .services
            .object_relations()
            .create(&prepared, &EventContext::system())
            .await
            .expect("relation should create");
        let relation_id =
            HubuumObjectRelationID::new(created.relation().id).expect("valid object relation id");
        harness
            .services
            .object_relations()
            .delete(&created, &EventContext::system())
            .await
            .expect("relation should delete");
        assert!(matches!(
            harness
                .services
                .object_relations()
                .resolve(ObjectRelationSelector::by_id(relation_id))
                .await,
            Err(ApiError::NotFound(_))
        ));
        harness.finish().await;
    }

    #[rstest]
    #[case::postgres(ContractImplementation::PostgresAdapter)]
    #[case::memory(ContractImplementation::MemoryModel)]
    #[actix_web::test]
    async fn object_relation_contract_object_delete_cascades_the_relation(
        #[case] backend: ContractImplementation,
    ) {
        let harness = ContractHarness::new(backend, "object_cascade").await;
        let fixture = harness.fixture("object_cascade").await;
        let prepared = harness
            .services
            .object_relations()
            .prepare_create(harness.explicit_selector(&fixture))
            .await
            .expect("relation should prepare");
        let created = harness
            .services
            .object_relations()
            .create(&prepared, &EventContext::system())
            .await
            .expect("relation should create");
        let relation_id =
            HubuumObjectRelationID::new(created.relation().id).expect("valid object relation id");
        let object_target = harness
            .services
            .objects()
            .resolve(crate::models::ObjectSelector::by_id(
                HubuumClassID::new(fixture.from_class.id).expect("valid class id"),
                HubuumObjectID::new(fixture.from_object.id).expect("valid object id"),
            ))
            .await
            .expect("object should resolve");
        harness
            .services
            .objects()
            .delete(&object_target, &EventContext::system())
            .await
            .expect("object should delete");
        assert!(matches!(
            harness
                .services
                .object_relations()
                .resolve(ObjectRelationSelector::by_id(relation_id))
                .await,
            Err(ApiError::NotFound(_))
        ));
        harness.finish().await;
    }

    #[rstest]
    #[case::postgres(ContractImplementation::PostgresAdapter)]
    #[case::memory(ContractImplementation::MemoryModel)]
    #[actix_web::test]
    async fn object_relation_contract_class_relation_delete_cascades_the_relation(
        #[case] backend: ContractImplementation,
    ) {
        let harness = ContractHarness::new(backend, "class_relation_cascade").await;
        let fixture = harness.fixture("class_relation_cascade").await;
        let prepared = harness
            .services
            .object_relations()
            .prepare_create(harness.explicit_selector(&fixture))
            .await
            .expect("relation should prepare");
        let created = harness
            .services
            .object_relations()
            .create(&prepared, &EventContext::system())
            .await
            .expect("relation should create");
        let relation_id =
            HubuumObjectRelationID::new(created.relation().id).expect("valid object relation id");
        harness
            .services
            .class_relations()
            .delete(&fixture.class_relation, &EventContext::system())
            .await
            .expect("class relation should delete");
        assert!(matches!(
            harness
                .services
                .object_relations()
                .resolve(ObjectRelationSelector::by_id(relation_id))
                .await,
            Err(ApiError::NotFound(_))
        ));
        harness.finish().await;
    }

    #[actix_web::test]
    async fn memory_object_relation_events_cover_explicit_lifecycle_writes() {
        let harness = ContractHarness::new(ContractImplementation::MemoryModel, "events").await;
        let fixture = harness.fixture("events").await;
        let context = EventContext::system();
        let prepared = harness
            .services
            .object_relations()
            .prepare_create(harness.between_create_selector(&fixture))
            .await
            .expect("relation should prepare");
        let created = harness
            .services
            .object_relations()
            .create(&prepared, &context)
            .await
            .expect("relation should create");
        harness
            .services
            .object_relations()
            .delete(&created, &context)
            .await
            .expect("relation should delete");

        let events = harness
            .storage
            .as_ref()
            .expect("memory harness should expose storage")
            .object_relation_events()
            .await;
        assert_eq!(
            events.iter().map(|event| event.action).collect::<Vec<_>>(),
            vec![Action::Created, Action::Deleted]
        );
        assert!(events.iter().all(|event| {
            event.object_relation_id == created.relation().id && event.context == context
        }));
        harness.finish().await;
    }
}
