use std::sync::Arc;
use std::sync::atomic::{AtomicI32, Ordering};

use diesel::{ExpressionMethods, QueryDsl};
use diesel_async::RunQueryDsl;

use crate::events::{EntityType, EventContext};
use crate::models::NewGroup;
use crate::services::storage_boundary::collection_id_to_storage;
use crate::storage::{
    ClassRelationStorage, ClassStorage, CollectionStorage, MemoryStorageModel,
    ObjectRelationStorage, ObjectStorage, StorageClassCreate, StorageClassRelationCreate,
    StorageClassSelector, StorageCollectionCreate, StorageError, StorageErrorKind,
    StorageObjectCreate, StorageObjectRelationCreate, StorageObjectRelationCreateSelector,
    StorageObjectSelector, TransactionStorage,
};
use hubuum_storage_postgres::PostgresStorage;
use hubuum_storage_postgres::{PostgresPool, with_connection};

use super::storage_contract::{pool, postgres_permit, prefix};

struct TransactionContractResult {
    committed_ids: TransactionEntityIds,
    rolled_back_ids: Arc<TransactionEntityIds>,
}

#[derive(Default)]
struct TransactionEntityIds {
    collection: AtomicI32,
    class: AtomicI32,
    class_relation: AtomicI32,
    object: AtomicI32,
    object_relation: AtomicI32,
}

impl TransactionEntityIds {
    fn id_for(&self, entity_type: EntityType) -> i32 {
        let id = match entity_type {
            EntityType::Collection => &self.collection,
            EntityType::Class => &self.class,
            EntityType::ClassRelation => &self.class_relation,
            EntityType::Object => &self.object,
            EntityType::ObjectRelation => &self.object_relation,
            _ => panic!("transaction contract does not track {entity_type:?}"),
        };
        AtomicI32::load(id, Ordering::Relaxed)
    }

    fn assert_populated(&self) {
        for entity_type in TRANSACTION_ENTITY_TYPES {
            assert!(
                self.id_for(entity_type) > 0,
                "the transaction should allocate a {entity_type:?} id before rollback"
            );
        }
    }
}

const TRANSACTION_ENTITY_TYPES: [EntityType; 5] = [
    EntityType::Collection,
    EntityType::Class,
    EntityType::ClassRelation,
    EntityType::Object,
    EntityType::ObjectRelation,
];

async fn exercise_resource_transaction<S>(
    storage: &S,
    label: &str,
    owner_group_id: i32,
) -> TransactionContractResult
where
    S: TransactionStorage
        + CollectionStorage
        + ClassStorage
        + ClassRelationStorage
        + ObjectStorage
        + ObjectRelationStorage,
{
    let event_context = EventContext::system();
    let collection_name = format!("{label}_collection");
    let from_object_name = format!("{label}_from_object");
    let transaction_label = label.to_string();
    let transaction_collection_name = collection_name.clone();
    let transaction_from_name = from_object_name.clone();
    let (collection, from_class, to_class, class_relation, from_object, to_object, object_relation) =
        storage
            .with_transaction(event_context.clone(), move |transaction| {
                Box::pin(async move {
                    let collection = transaction
                        .collections()
                        .create(StorageCollectionCreate::new(
                            transaction_collection_name,
                            "transaction contract collection",
                            hubuum_domain::GroupId::new(owner_group_id)
                                .expect("validated group id must be positive"),
                            None,
                        ))
                        .await?
                        .into_value();
                    let from_class_record = transaction
                        .classes()
                        .create(
                            StorageClassCreate::builder(
                                format!("{transaction_label}_from_class"),
                                collection.id(),
                                "transaction contract from class",
                            )
                            .build(),
                        )
                        .await?
                        .into_value();
                    let to_class_record = transaction
                        .classes()
                        .create(
                            StorageClassCreate::builder(
                                format!("{transaction_label}_to_class"),
                                collection.id(),
                                "transaction contract to class",
                            )
                            .build(),
                        )
                        .await?
                        .into_value();
                    let from_class = transaction
                        .classes()
                        .resolve(StorageClassSelector::Id(from_class_record.id()))
                        .await?;
                    let to_class = transaction
                        .classes()
                        .resolve(StorageClassSelector::Id(to_class_record.id()))
                        .await?;
                    let prepared_class_relation = transaction
                        .class_relations()
                        .prepare(
                            StorageClassRelationCreate::builder(
                                from_class.class().id(),
                                to_class.class().id(),
                            )
                            .build(),
                        )
                        .await?;
                    let class_relation = transaction
                        .class_relations()
                        .create(&prepared_class_relation)
                        .await?
                        .into_value();
                    let from_object = transaction
                        .objects()
                        .create(
                            &from_class,
                            StorageObjectCreate::new(
                                transaction_from_name,
                                collection.id(),
                                from_class.class().id(),
                                serde_json::json!({"side": "from"}),
                                "transaction contract from object",
                            ),
                        )
                        .await?
                        .into_value();
                    let to_object = transaction
                        .objects()
                        .create(
                            &to_class,
                            StorageObjectCreate::new(
                                format!("{transaction_label}_to_object"),
                                collection.id(),
                                to_class.class().id(),
                                serde_json::json!({"side": "to"}),
                                "transaction contract to object",
                            ),
                        )
                        .await?
                        .into_value();
                    let prepared = transaction
                        .object_relations()
                        .prepare(StorageObjectRelationCreateSelector::Explicit(
                            StorageObjectRelationCreate::new(
                                from_object.id(),
                                to_object.id(),
                                hubuum_domain::ClassRelationId::from(
                                    class_relation.relation().metadata().id(),
                                ),
                            ),
                        ))
                        .await?;
                    let relation = transaction
                        .object_relations()
                        .create(&prepared)
                        .await?
                        .into_value();
                    Ok((
                        collection,
                        from_class,
                        to_class,
                        class_relation,
                        from_object,
                        to_object,
                        relation,
                    ))
                })
            })
            .await
            .expect("resource transaction should commit");

    assert_eq!(
        storage
            .get_collection(collection.id())
            .await
            .expect("committed collection should be visible")
            .name(),
        collection_name
    );
    assert_eq!(
        storage
            .resolve_object(StorageObjectSelector::Names {
                class_name: from_class.class().name().to_string(),
                object_name: from_object_name,
            })
            .await
            .expect("committed object should be visible")
            .object()
            .id(),
        from_object.id()
    );
    assert_eq!(
        object_relation.relation().from_object_id(),
        std::cmp::min(from_object.id(), to_object.id())
    );
    let committed_ids = TransactionEntityIds {
        collection: AtomicI32::new(collection.id().id()),
        class: AtomicI32::new(from_class.class().id().id()),
        class_relation: AtomicI32::new(class_relation.relation().metadata().id().id()),
        object: AtomicI32::new(from_object.id().id()),
        object_relation: AtomicI32::new(object_relation.relation().metadata().id().id()),
    };
    committed_ids.assert_populated();

    let rolled_back_ids = Arc::new(TransactionEntityIds::default());
    let rollback_ids_from_work = rolled_back_ids.clone();
    let rollback_label = format!("{label}_rollback");
    let rollback = storage
        .with_transaction(event_context.clone(), move |transaction| {
            Box::pin(async move {
                let rollback_collection = transaction
                    .collections()
                    .create(StorageCollectionCreate::new(
                        format!("{rollback_label}_collection"),
                        "rolled-back transaction collection",
                        hubuum_domain::GroupId::new(owner_group_id)
                            .expect("validated group id must be positive"),
                        None,
                    ))
                    .await?
                    .into_value();
                rollback_ids_from_work
                    .collection
                    .store(rollback_collection.id().id(), Ordering::Relaxed);
                let rollback_from_class = transaction
                    .classes()
                    .create(
                        StorageClassCreate::builder(
                            format!("{rollback_label}_from_class"),
                            rollback_collection.id(),
                            "rolled-back from class",
                        )
                        .build(),
                    )
                    .await?
                    .into_value();
                rollback_ids_from_work
                    .class
                    .store(rollback_from_class.id().id(), Ordering::Relaxed);
                let rollback_to_class = transaction
                    .classes()
                    .create(
                        StorageClassCreate::builder(
                            format!("{rollback_label}_to_class"),
                            rollback_collection.id(),
                            "rolled-back to class",
                        )
                        .build(),
                    )
                    .await?
                    .into_value();
                let rollback_from_class = transaction
                    .classes()
                    .resolve(StorageClassSelector::Id(rollback_from_class.id()))
                    .await?;
                let rollback_to_class = transaction
                    .classes()
                    .resolve(StorageClassSelector::Id(rollback_to_class.id()))
                    .await?;
                let prepared_class_relation = transaction
                    .class_relations()
                    .prepare(
                        StorageClassRelationCreate::builder(
                            rollback_from_class.class().id(),
                            rollback_to_class.class().id(),
                        )
                        .build(),
                    )
                    .await?;
                let rollback_class_relation = transaction
                    .class_relations()
                    .create(&prepared_class_relation)
                    .await?
                    .into_value();
                rollback_ids_from_work.class_relation.store(
                    rollback_class_relation.relation().metadata().id().id(),
                    Ordering::Relaxed,
                );
                let object = transaction
                    .objects()
                    .create(
                        &rollback_from_class,
                        StorageObjectCreate::new(
                            format!("{rollback_label}_from_object"),
                            rollback_collection.id(),
                            rollback_from_class.class().id(),
                            serde_json::json!({"rolled_back": true}),
                            "transaction contract rollback object",
                        ),
                    )
                    .await?
                    .into_value();
                rollback_ids_from_work
                    .object
                    .store(object.id().id(), Ordering::Relaxed);
                let to_object = transaction
                    .objects()
                    .create(
                        &rollback_to_class,
                        StorageObjectCreate::new(
                            format!("{rollback_label}_to_object"),
                            rollback_collection.id(),
                            rollback_to_class.class().id(),
                            serde_json::json!({"rolled_back": true}),
                            "transaction contract rollback object",
                        ),
                    )
                    .await?
                    .into_value();
                let prepared_object_relation = transaction
                    .object_relations()
                    .prepare(StorageObjectRelationCreateSelector::Explicit(
                        StorageObjectRelationCreate::new(
                            object.id(),
                            to_object.id(),
                            hubuum_domain::ClassRelationId::from(
                                rollback_class_relation.relation().metadata().id(),
                            ),
                        ),
                    ))
                    .await?;
                let rollback_object_relation = transaction
                    .object_relations()
                    .create(&prepared_object_relation)
                    .await?
                    .into_value();
                rollback_ids_from_work.object_relation.store(
                    rollback_object_relation.relation().metadata().id().id(),
                    Ordering::Relaxed,
                );
                Err::<(), _>(StorageError::internal("transaction contract rollback"))
            })
        })
        .await
        .expect_err("application error should roll back the unit of work");
    assert_eq!(rollback.kind(), StorageErrorKind::Internal);
    rolled_back_ids.assert_populated();
    let missing_collection = match storage
        .get_collection(collection_id_to_storage(
            rolled_back_ids.id_for(EntityType::Collection),
        ))
        .await
    {
        Ok(_) => panic!("rolled-back collection must not be visible"),
        Err(error) => error,
    };
    assert_eq!(missing_collection.kind(), StorageErrorKind::NotFound);

    storage
        .delete_object_relation(&object_relation, &event_context)
        .await
        .expect("object relation cleanup should succeed")
        .into_value();
    for object_id in [from_object.id(), to_object.id()] {
        let object = storage
            .get_object(object_id)
            .await
            .expect("object cleanup target should resolve");
        storage
            .delete_object(&object, &event_context)
            .await
            .expect("object cleanup should succeed")
            .into_value();
    }
    storage
        .delete_class_relation(&class_relation, &event_context)
        .await
        .expect("class relation cleanup should succeed")
        .into_value();
    for class in [from_class, to_class] {
        storage
            .delete_class(&class, &event_context)
            .await
            .expect("class cleanup should succeed")
            .into_value();
    }
    storage
        .delete_collection(collection.id(), &event_context)
        .await
        .expect("collection cleanup should succeed")
        .into_value();

    TransactionContractResult {
        committed_ids,
        rolled_back_ids,
    }
}

#[actix_web::test]
async fn memory_transaction_composes_resources_and_rolls_back_events() {
    let storage = MemoryStorageModel::new();
    let result = exercise_resource_transaction(&storage, "memory_transaction", 1).await;

    let committed_ids = &result.committed_ids;
    assert!(
        storage
            .events()
            .await
            .iter()
            .any(|event| event.collection_id == committed_ids.id_for(EntityType::Collection))
    );
    assert!(
        storage
            .class_events()
            .await
            .iter()
            .any(|event| event.class_id == committed_ids.id_for(EntityType::Class))
    );
    assert!(storage.class_relation_events().await.iter().any(|event| {
        event.class_relation_id == committed_ids.id_for(EntityType::ClassRelation)
    }));
    assert!(
        storage
            .object_events()
            .await
            .iter()
            .any(|event| event.object_id == committed_ids.id_for(EntityType::Object))
    );
    assert!(storage.object_relation_events().await.iter().any(|event| {
        event.object_relation_id == committed_ids.id_for(EntityType::ObjectRelation)
    }));

    let ids = result.rolled_back_ids;
    assert!(
        storage
            .events()
            .await
            .iter()
            .all(|event| event.collection_id != ids.id_for(EntityType::Collection))
    );
    assert!(
        storage
            .class_events()
            .await
            .iter()
            .all(|event| event.class_id != ids.id_for(EntityType::Class))
    );
    assert!(
        storage
            .class_relation_events()
            .await
            .iter()
            .all(|event| event.class_relation_id != ids.id_for(EntityType::ClassRelation))
    );
    assert!(
        storage
            .object_events()
            .await
            .iter()
            .all(|event| event.object_id != ids.id_for(EntityType::Object))
    );
    assert!(
        storage
            .object_relation_events()
            .await
            .iter()
            .all(|event| event.object_relation_id != ids.id_for(EntityType::ObjectRelation))
    );
}

#[actix_web::test]
async fn postgres_transaction_composes_resources_and_rolls_back_events() {
    let _permit = postgres_permit().await;
    let pool = pool();
    let storage = PostgresStorage::unobserved(pool.get_ref().clone());
    let owner_group = NewGroup {
        identity_scope: None,
        groupname: prefix("postgres_transaction_owner"),
        description: Some("storage transaction contract owner".to_string()),
    }
    .save_without_events(&pool)
    .await
    .expect("transaction contract owner group should save");
    let result =
        exercise_resource_transaction(&storage, &prefix("postgres_transaction"), owner_group.id)
            .await;

    for entity_type in TRANSACTION_ENTITY_TYPES {
        assert!(
            postgres_event_count(
                pool.get_ref(),
                entity_type,
                result.committed_ids.id_for(entity_type),
            )
            .await
                > 0,
            "the committed PostgreSQL transaction must retain its {entity_type:?} event"
        );
        assert_eq!(
            postgres_event_count(
                pool.get_ref(),
                entity_type,
                result.rolled_back_ids.id_for(entity_type),
            )
            .await,
            0,
            "the rolled-back PostgreSQL transaction must not retain its {entity_type:?} event"
        );
    }
    owner_group
        .delete_without_events(&pool)
        .await
        .expect("transaction contract owner group should delete");
}

async fn postgres_event_count(pool: &PostgresPool, kind: EntityType, id: i32) -> i64 {
    use crate::schema::events::dsl::{entity_id, entity_type, events};

    with_connection(pool, async move |connection| {
        events
            .filter(entity_type.eq(kind.as_str()))
            .filter(entity_id.eq(id))
            .count()
            .get_result::<i64>(connection)
            .await
    })
    .await
    .expect("transaction contract should count object events")
}
