use crate::errors::ApiError;
use crate::events::EventContext;
use crate::models::{
    HubuumObject, NewHubuumObject, ObjectDataPatchDocument, ObjectSelector, ResolvedClassTarget,
    ResolvedObjectTarget, UpdateHubuumObject,
};
use crate::storage::DynStorage;

/// Application-facing object resolution and lifecycle use cases.
#[derive(Clone)]
pub struct ObjectService {
    storage: DynStorage,
}

impl ObjectService {
    pub(crate) fn new(storage: DynStorage) -> Self {
        Self { storage }
    }

    pub async fn resolve(
        &self,
        selector: ObjectSelector,
    ) -> Result<ResolvedObjectTarget, ApiError> {
        self.storage
            .inner()
            .resolve_object(selector)
            .await
            .map_err(ApiError::from)
    }

    pub async fn create(
        &self,
        class: &ResolvedClassTarget,
        command: NewHubuumObject,
        context: &EventContext,
    ) -> Result<HubuumObject, ApiError> {
        self.storage
            .inner()
            .create_object(class, command, context)
            .await
            .map_err(ApiError::from)
    }

    pub async fn update(
        &self,
        target: &ResolvedObjectTarget,
        changes: UpdateHubuumObject,
        context: &EventContext,
    ) -> Result<HubuumObject, ApiError> {
        self.storage
            .inner()
            .update_object(target, changes, context)
            .await
            .map_err(ApiError::from)
    }

    pub async fn patch_data(
        &self,
        target: &ResolvedObjectTarget,
        patch: ObjectDataPatchDocument,
        context: &EventContext,
    ) -> Result<HubuumObject, ApiError> {
        self.storage
            .inner()
            .patch_object_data(target, patch, context)
            .await
            .map_err(ApiError::from)
    }

    pub async fn delete(
        &self,
        target: &ResolvedObjectTarget,
        context: &EventContext,
    ) -> Result<(), ApiError> {
        self.storage
            .inner()
            .delete_object(target, context)
            .await
            .map_err(ApiError::from)
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use serde_json::json;

    use crate::errors::ApiError;
    use crate::events::{Action, EventContext};
    use crate::models::{
        ClassSelector, CollectionID, GroupID, HubuumClass, HubuumClassID, HubuumObject,
        HubuumObjectID, NewCollectionWithAssignee, NewGroup, NewHubuumClass, NewHubuumObject,
        ObjectDataPatchDocument, ObjectSelector, UpdateHubuumObject,
    };
    use crate::services::{
        ClassService, Services, storage_contract_pool, storage_contract_postgres_permit,
        storage_contract_prefix,
    };
    use crate::storage::{DynStorage, MemoryStorage, PostgresStorage};
    use crate::tests::CollectionFixture;
    use crate::traits::CanSave;

    use super::ObjectService;

    #[derive(Clone, Copy, Debug)]
    enum ContractBackend {
        Memory,
        Postgres,
    }

    #[derive(Clone, Copy, Debug)]
    enum ObjectAddress {
        Id,
        Name,
    }

    struct ContractHarness {
        service: ObjectService,
        classes: ClassService,
        class: HubuumClass,
        prefix: String,
        postgres_cleanup: Option<CollectionFixture>,
        _postgres_permit: Option<tokio::sync::OwnedSemaphorePermit>,
    }

    impl ContractHarness {
        async fn new(backend: ContractBackend, label: &str) -> Self {
            match backend {
                ContractBackend::Memory => {
                    let services = Services::from_storage(DynStorage::new(MemoryStorage::new()));
                    let prefix = format!("memory_{label}");
                    let class = create_class(
                        services.classes(),
                        &prefix,
                        CollectionID::new(1).expect("valid root collection id").id(),
                        None,
                    )
                    .await;
                    Self {
                        service: services.objects().clone(),
                        classes: services.classes().clone(),
                        class,
                        prefix,
                        postgres_cleanup: None,
                        _postgres_permit: None,
                    }
                }
                ContractBackend::Postgres => {
                    let permit = storage_contract_postgres_permit().await;
                    let pool = storage_contract_pool();
                    let prefix = storage_contract_prefix(label);
                    let owner_group = NewGroup {
                        identity_scope: None,
                        groupname: format!("{prefix}_owner"),
                        description: Some("object storage contract owner".to_string()),
                    }
                    .save_without_events(&pool)
                    .await
                    .expect("contract owner group should save");
                    let collection = NewCollectionWithAssignee {
                        name: format!("{prefix}_collection"),
                        description: "object storage contract collection".to_string(),
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
                    let services = Services::from_storage(DynStorage::new(PostgresStorage::new(
                        pool.get_ref().clone(),
                    )));
                    let class =
                        create_class(services.classes(), &prefix, fixture.collection.id, None)
                            .await;
                    Self {
                        service: services.objects().clone(),
                        classes: services.classes().clone(),
                        class,
                        prefix,
                        postgres_cleanup: Some(fixture),
                        _postgres_permit: Some(permit),
                    }
                }
            }
        }

        async fn create(&self, label: &str, data: serde_json::Value) -> HubuumObject {
            let target = self
                .classes
                .resolve(ClassSelector::by_id(
                    HubuumClassID::new(self.class.id).expect("valid class id"),
                ))
                .await
                .expect("class should resolve");
            self.service
                .create(
                    &target,
                    NewHubuumObject {
                        name: format!("{}_{}", self.prefix, label),
                        collection_id: self.class.collection_id,
                        hubuum_class_id: self.class.id,
                        data,
                        description: format!("object contract {label}"),
                    },
                    &EventContext::system(),
                )
                .await
                .expect("contract object should be created")
        }

        async fn finish(self) {
            if let Some(fixture) = self.postgres_cleanup {
                fixture.cleanup().await.expect("contract fixture cleanup");
            }
        }
    }

    async fn create_class(
        service: &ClassService,
        prefix: &str,
        collection_id: i32,
        schema: Option<serde_json::Value>,
    ) -> HubuumClass {
        service
            .create(
                NewHubuumClass {
                    name: format!("{prefix}_class"),
                    collection_id,
                    json_schema: schema.clone(),
                    validate_schema: Some(schema.is_some()),
                    description: "object storage contract class".to_string(),
                },
                &EventContext::system(),
            )
            .await
            .expect("contract class should be created")
    }

    fn selector(
        address: ObjectAddress,
        class: &HubuumClass,
        object: &HubuumObject,
    ) -> ObjectSelector {
        match address {
            ObjectAddress::Id => ObjectSelector::by_id(
                HubuumClassID::new(class.id).expect("valid class id"),
                HubuumObjectID::new(object.id).expect("valid object id"),
            ),
            ObjectAddress::Name => ObjectSelector::by_name(class.name.clone(), object.name.clone()),
        }
    }

    fn patch(value: serde_json::Value) -> ObjectDataPatchDocument {
        serde_json::from_value(value).expect("valid object data patch")
    }

    #[rstest]
    #[case::postgres_id(ContractBackend::Postgres, ObjectAddress::Id)]
    #[case::postgres_name(ContractBackend::Postgres, ObjectAddress::Name)]
    #[case::memory_id(ContractBackend::Memory, ObjectAddress::Id)]
    #[case::memory_name(ContractBackend::Memory, ObjectAddress::Name)]
    #[actix_web::test]
    async fn object_contract_resolves_explicit_addresses(
        #[case] backend: ContractBackend,
        #[case] address: ObjectAddress,
    ) {
        let harness = ContractHarness::new(backend, "resolve").await;
        let object = harness.create("object", json!({"value": 1})).await;

        let resolved = harness
            .service
            .resolve(selector(address, &harness.class, &object))
            .await
            .expect("object should resolve");
        assert_eq!(resolved.class(), &harness.class);
        assert_eq!(resolved.object(), &object);

        harness.finish().await;
    }

    #[rstest]
    #[case::postgres(ContractBackend::Postgres)]
    #[case::memory(ContractBackend::Memory)]
    #[actix_web::test]
    async fn object_contract_changed_update_advances_revision(#[case] backend: ContractBackend) {
        let harness = ContractHarness::new(backend, "changed_update").await;
        let object = harness.create("object", json!({"value": 1})).await;
        let target = harness
            .service
            .resolve(selector(ObjectAddress::Id, &harness.class, &object))
            .await
            .expect("object should resolve");

        let updated = harness
            .service
            .update(
                &target,
                UpdateHubuumObject {
                    name: None,
                    collection_id: None,
                    hubuum_class_id: None,
                    data: None,
                    description: Some("updated object contract".to_string()),
                },
                &EventContext::system(),
            )
            .await
            .expect("object should update");
        assert_eq!(updated.revision.get(), object.revision.get() + 1);

        harness.finish().await;
    }

    #[rstest]
    #[case::postgres(ContractBackend::Postgres)]
    #[case::memory(ContractBackend::Memory)]
    #[actix_web::test]
    async fn object_contract_no_op_update_preserves_revision(#[case] backend: ContractBackend) {
        let harness = ContractHarness::new(backend, "no_op_update").await;
        let object = harness.create("object", json!({"value": 1})).await;
        let target = harness
            .service
            .resolve(selector(ObjectAddress::Id, &harness.class, &object))
            .await
            .expect("object should resolve");

        let unchanged = harness
            .service
            .update(
                &target,
                UpdateHubuumObject {
                    name: Some(object.name.clone()),
                    collection_id: Some(object.collection_id),
                    hubuum_class_id: Some(object.hubuum_class_id),
                    data: Some(object.data.clone()),
                    description: Some(object.description.clone()),
                },
                &EventContext::system(),
            )
            .await
            .expect("no-op update should return current state");
        assert_eq!(unchanged, object);

        harness.finish().await;
    }

    #[rstest]
    #[case::postgres(ContractBackend::Postgres)]
    #[case::memory(ContractBackend::Memory)]
    #[actix_web::test]
    async fn object_contract_patch_updates_data_and_revision(#[case] backend: ContractBackend) {
        let harness = ContractHarness::new(backend, "patch").await;
        let object = harness.create("object", json!({"value": 1})).await;
        let target = harness
            .service
            .resolve(selector(ObjectAddress::Id, &harness.class, &object))
            .await
            .expect("object should resolve");

        let updated = harness
            .service
            .patch_data(
                &target,
                patch(json!([{"op": "replace", "path": "/value", "value": 2}])),
                &EventContext::system(),
            )
            .await
            .expect("object data should patch");
        assert_eq!(updated.data, json!({"value": 2}));
        assert_eq!(updated.revision.get(), object.revision.get() + 1);

        harness.finish().await;
    }

    #[rstest]
    #[case::postgres(ContractBackend::Postgres)]
    #[case::memory(ContractBackend::Memory)]
    #[actix_web::test]
    async fn object_contract_no_op_patch_preserves_revision(#[case] backend: ContractBackend) {
        let harness = ContractHarness::new(backend, "no_op_patch").await;
        let object = harness.create("object", json!({"value": 1})).await;
        let target = harness
            .service
            .resolve(selector(ObjectAddress::Id, &harness.class, &object))
            .await
            .expect("object should resolve");

        let unchanged = harness
            .service
            .patch_data(
                &target,
                patch(json!([{"op": "replace", "path": "/value", "value": 1}])),
                &EventContext::system(),
            )
            .await
            .expect("no-op patch should return current state");
        assert_eq!(unchanged, object);

        harness.finish().await;
    }

    #[rstest]
    #[case::postgres(ContractBackend::Postgres)]
    #[case::memory(ContractBackend::Memory)]
    #[actix_web::test]
    async fn object_contract_rejects_data_outside_the_class_schema(
        #[case] backend: ContractBackend,
    ) {
        let mut harness = ContractHarness::new(backend, "schema").await;
        let schema_class = create_class(
            &harness.classes,
            &format!("{}_schema", harness.prefix),
            harness.class.collection_id,
            Some(json!({
                "type": "object",
                "properties": {"value": {"type": "integer"}},
                "required": ["value"]
            })),
        )
        .await;
        harness.class = schema_class.clone();
        let target = harness
            .classes
            .resolve(ClassSelector::by_id(
                HubuumClassID::new(schema_class.id).expect("valid class id"),
            ))
            .await
            .expect("schema class should resolve");

        assert!(matches!(
            harness
                .service
                .create(
                    &target,
                    NewHubuumObject {
                        name: format!("{}_invalid", harness.prefix),
                        collection_id: schema_class.collection_id,
                        hubuum_class_id: schema_class.id,
                        data: json!({"value": "not an integer"}),
                        description: "invalid schema object".to_string(),
                    },
                    &EventContext::system(),
                )
                .await,
            Err(ApiError::ValidationError(_))
        ));

        harness.finish().await;
    }

    #[rstest]
    #[case::postgres(ContractBackend::Postgres)]
    #[case::memory(ContractBackend::Memory)]
    #[actix_web::test]
    async fn object_contract_rejects_a_stale_name_target(#[case] backend: ContractBackend) {
        let harness = ContractHarness::new(backend, "stale_name").await;
        let object = harness.create("object", json!({"value": 1})).await;
        let name_target = harness
            .service
            .resolve(selector(ObjectAddress::Name, &harness.class, &object))
            .await
            .expect("name target should resolve");
        let id_target = harness
            .service
            .resolve(selector(ObjectAddress::Id, &harness.class, &object))
            .await
            .expect("id target should resolve");
        let renamed = harness
            .service
            .update(
                &id_target,
                UpdateHubuumObject {
                    name: Some(format!("{}_renamed", harness.prefix)),
                    collection_id: None,
                    hubuum_class_id: None,
                    data: None,
                    description: None,
                },
                &EventContext::system(),
            )
            .await
            .expect("object should rename");

        assert!(matches!(
            harness
                .service
                .update(
                    &name_target,
                    UpdateHubuumObject {
                        name: None,
                        collection_id: None,
                        hubuum_class_id: None,
                        data: None,
                        description: Some("stale write".to_string()),
                    },
                    &EventContext::system(),
                )
                .await,
            Err(ApiError::NotFound(_))
        ));
        assert_ne!(renamed.name, object.name);

        harness.finish().await;
    }

    #[rstest]
    #[case::postgres(ContractBackend::Postgres)]
    #[case::memory(ContractBackend::Memory)]
    #[actix_web::test]
    async fn object_contract_delete_removes_the_resolved_object(#[case] backend: ContractBackend) {
        let harness = ContractHarness::new(backend, "delete").await;
        let object = harness.create("object", json!({"value": 1})).await;
        let selector = selector(ObjectAddress::Id, &harness.class, &object);
        let target = harness
            .service
            .resolve(selector.clone())
            .await
            .expect("object should resolve");

        harness
            .service
            .delete(&target, &EventContext::system())
            .await
            .expect("object should delete");
        assert!(matches!(
            harness.service.resolve(selector).await,
            Err(ApiError::NotFound(_))
        ));

        harness.finish().await;
    }

    #[rstest]
    #[case::postgres(ContractBackend::Postgres)]
    #[case::memory(ContractBackend::Memory)]
    #[actix_web::test]
    async fn object_contract_class_delete_cascades_the_object(#[case] backend: ContractBackend) {
        let harness = ContractHarness::new(backend, "class_cascade").await;
        let object = harness.create("object", json!({"value": 1})).await;
        let object_selector = selector(ObjectAddress::Id, &harness.class, &object);
        let class_target = harness
            .classes
            .resolve(ClassSelector::by_id(
                HubuumClassID::new(harness.class.id).expect("valid class id"),
            ))
            .await
            .expect("class should resolve");

        harness
            .classes
            .delete(&class_target, &EventContext::system())
            .await
            .expect("class should delete");
        assert!(matches!(
            harness.service.resolve(object_selector).await,
            Err(ApiError::NotFound(_))
        ));

        harness.finish().await;
    }

    #[actix_web::test]
    async fn memory_object_events_exclude_no_op_mutations() {
        let storage = MemoryStorage::new();
        let services = Services::from_storage(DynStorage::new(storage.clone()));
        let class = create_class(services.classes(), "memory_object_events", 1, None).await;
        let class_target = services
            .classes()
            .resolve(ClassSelector::by_id(
                HubuumClassID::new(class.id).expect("valid class id"),
            ))
            .await
            .expect("class should resolve");
        let context = EventContext::system();
        let object = services
            .objects()
            .create(
                &class_target,
                NewHubuumObject {
                    name: "memory_object_event_contract".to_string(),
                    collection_id: class.collection_id,
                    hubuum_class_id: class.id,
                    data: json!({"value": 1}),
                    description: "memory object event contract".to_string(),
                },
                &context,
            )
            .await
            .expect("memory object should create");
        let target = services
            .objects()
            .resolve(selector(ObjectAddress::Id, &class, &object))
            .await
            .expect("memory object should resolve");
        services
            .objects()
            .update(
                &target,
                UpdateHubuumObject {
                    name: Some(object.name.clone()),
                    collection_id: Some(object.collection_id),
                    hubuum_class_id: Some(object.hubuum_class_id),
                    data: Some(object.data.clone()),
                    description: Some(object.description.clone()),
                },
                &context,
            )
            .await
            .expect("memory no-op update should succeed");
        services
            .objects()
            .patch_data(
                &target,
                patch(json!([{"op": "replace", "path": "/value", "value": 1}])),
                &context,
            )
            .await
            .expect("memory no-op patch should succeed");

        let events = storage.object_events().await;
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].action, Action::Created);
    }
}
