use hubuum_artifacts::{
    ArtifactIdentity, ArtifactLimits, ArtifactStore, Destination, DestinationId,
};
use hubuum_storage_postgres::PostgresStorage;

use super::BackupArtifactProducer;
use crate::storage::StorageBackupBudget;
use crate::tests::{TestContext, app_context};

#[tokio::test]
async fn captured_backup_stages_the_exact_snapshot() {
    let test = TestContext::new().await;
    let context = app_context(&test.pool);
    let budget = StorageBackupBudget::new(64 * 1024 * 1024, 1_000_000).unwrap();
    let producer = BackupArtifactProducer::capture(&context, &test.admin_user, None, false, budget)
        .await
        .unwrap();
    // Test-only materialization is the compatibility oracle, not the producer.
    let expected = budget.serialize(&producer.document).unwrap();
    let store = PostgresStorage::unobserved(test.pool.get_ref().clone()).artifact_store();
    let destination = Destination::new(
        DestinationId::generate(),
        test.scoped_name("backup-producer"),
    )
    .unwrap();
    store.register_destination(&destination).await.unwrap();
    let identity = ArtifactIdentity::new(destination.id());
    let limits = ArtifactLimits::builder()
        .block_bytes(64 * 1024)
        .artifact_bytes(budget.max_bytes() as u64)
        .build()
        .unwrap();
    let (verified, _) = producer
        .stage(&store, identity.clone(), limits)
        .await
        .unwrap();
    let mut actual = Vec::new();
    for ordinal in 0..verified.manifest().block_count() {
        actual.extend(store.block(&identity, ordinal).await.unwrap().unwrap());
    }
    assert_eq!(actual, expected);
    store.abort(&identity).await.unwrap();
    // Chunks are reclaimed; attempt/destination fences deliberately survive
    // until this runner-owned database is discarded.
    while store.cleanup().await.unwrap() != 0 {}
}
