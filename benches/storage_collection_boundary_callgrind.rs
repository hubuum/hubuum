use std::hint::black_box;

use async_trait::async_trait;
use chrono::{NaiveDate, NaiveDateTime};
use futures::executor::block_on;
use gungraun::{library_benchmark, library_benchmark_group, main};
use hubuum::benchmark_support::observed_collection_service;
use hubuum::events::EventContext;
use hubuum::models::{CollectionID, GroupID, NewCollectionWithAssignee};
use hubuum::services::CollectionService;
use hubuum_domain::{CollectionId, ResourceId, ResourceRevision};
use hubuum_events_core::EventSequence;
use hubuum_storage_core::{
    AuditReceipt, CollectionStorage, MutationOutcome, StorageCollection, StorageCollectionCreate,
    StorageCollectionUpdate, StorageError, StorageRecordMetadata,
};
use uuid::Uuid;

const TARGET_COLLECTION_ID: i32 = 42;
const ANCESTOR_COUNT: usize = 12;

struct FixedCollectionStorage {
    collection: StorageCollection,
    ancestors: Vec<StorageCollection>,
}

impl FixedCollectionStorage {
    fn new() -> Self {
        Self {
            collection: collection(TARGET_COLLECTION_ID, None),
            ancestors: (1..=ANCESTOR_COUNT)
                .map(|id| collection(id as i32, (id > 1).then_some(id as i32 - 1)))
                .collect(),
        }
    }
}

#[async_trait]
impl CollectionStorage for FixedCollectionStorage {
    async fn get_collection(&self, _id: CollectionId) -> Result<StorageCollection, StorageError> {
        Ok(self.collection.clone())
    }

    async fn create_collection(
        &self,
        _command: StorageCollectionCreate,
        _context: &EventContext,
    ) -> Result<MutationOutcome<StorageCollection>, StorageError> {
        Ok(MutationOutcome::committed(
            self.collection.clone(),
            audit_receipt(),
        ))
    }

    async fn update_collection(
        &self,
        _id: CollectionId,
        _changes: StorageCollectionUpdate,
        _context: &EventContext,
    ) -> Result<MutationOutcome<StorageCollection>, StorageError> {
        Ok(MutationOutcome::unchanged(self.collection.clone()))
    }

    async fn delete_collection(
        &self,
        _id: CollectionId,
        _context: &EventContext,
    ) -> Result<MutationOutcome<()>, StorageError> {
        Ok(MutationOutcome::committed((), audit_receipt()))
    }

    async fn list_collection_children(
        &self,
        _id: CollectionId,
    ) -> Result<Vec<StorageCollection>, StorageError> {
        Ok(Vec::new())
    }

    async fn list_collection_ancestors(
        &self,
        _id: CollectionId,
    ) -> Result<Vec<StorageCollection>, StorageError> {
        Ok(self.ancestors.clone())
    }

    async fn move_collection(
        &self,
        _id: CollectionId,
        _new_parent_id: CollectionId,
        _context: &EventContext,
    ) -> Result<MutationOutcome<StorageCollection>, StorageError> {
        Ok(MutationOutcome::committed(
            self.collection.clone(),
            audit_receipt(),
        ))
    }
}

fn timestamp() -> NaiveDateTime {
    NaiveDate::from_ymd_opt(2026, 1, 1)
        .expect("benchmark date should be valid")
        .and_hms_opt(0, 0, 0)
        .expect("benchmark time should be valid")
}

fn collection(id: i32, parent_collection_id: Option<i32>) -> StorageCollection {
    StorageCollection::new(
        StorageRecordMetadata::new(
            ResourceId::new(id).expect("benchmark resource id should be valid"),
            timestamp(),
            timestamp(),
            ResourceRevision::INITIAL,
        ),
        format!("collection-{id}"),
        "deterministic benchmark collection",
        parent_collection_id
            .map(|id| CollectionId::new(id).expect("benchmark collection id should be valid")),
    )
}

fn audit_receipt() -> AuditReceipt {
    AuditReceipt::new(
        EventSequence::new(1).expect("benchmark event sequence should be valid"),
        hubuum_events_core::EventId::from(Uuid::nil()),
        hubuum_events_core::EntityType::Collection,
        hubuum_events_core::Action::Created,
        None,
        Some(ResourceRevision::INITIAL),
    )
}

fn setup() -> CollectionService {
    observed_collection_service(FixedCollectionStorage::new())
}

#[library_benchmark(setup = setup)]
fn bench_collection_service_storage_boundary(service: CollectionService) -> usize {
    block_on(async move {
        let collection = service
            .get(black_box(
                CollectionID::new(TARGET_COLLECTION_ID)
                    .expect("benchmark collection id should be valid"),
            ))
            .await
            .expect("fixed storage get should succeed");
        let ancestors = service
            .ancestors(black_box(
                CollectionID::new(TARGET_COLLECTION_ID)
                    .expect("benchmark collection id should be valid"),
            ))
            .await
            .expect("fixed storage ancestors should succeed");

        black_box(
            collection.name.len() + ancestors.iter().map(|item| item.name.len()).sum::<usize>(),
        )
    })
}

#[library_benchmark(setup = setup)]
fn bench_collection_service_audited_mutation_boundary(service: CollectionService) -> i32 {
    block_on(async move {
        let created = service
            .create(
                black_box(NewCollectionWithAssignee {
                    name: "benchmark-created-collection".to_string(),
                    description: "deterministic benchmark mutation".to_string(),
                    group_id: GroupID::new(7).expect("benchmark group id should be valid"),
                    parent_collection_id: None,
                }),
                black_box(&EventContext::system()),
            )
            .await
            .expect("fixed storage create should succeed");
        black_box(created.id)
    })
}

#[library_benchmark]
fn bench_committed_mutation_outcome() -> i32 {
    black_box(MutationOutcome::committed(
        black_box(TARGET_COLLECTION_ID),
        black_box(audit_receipt()),
    ))
    .into_value()
}

library_benchmark_group!(
    name = benches;
    benchmarks =
        bench_collection_service_storage_boundary,
        bench_collection_service_audited_mutation_boundary,
        bench_committed_mutation_outcome
);
main!(library_benchmark_groups = benches);
