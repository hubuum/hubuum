use chrono::{DateTime, Duration};
use diesel::sql_types::Jsonb;
use diesel_async::SimpleAsyncConnection;
use hubuum_domain::VersionIdentity;
use hubuum_storage_core::artifacts::{ArtifactIdentity, ArtifactReference, DestinationId};
use hubuum_storage_core::{
    DeletionControlStorage, HistoryArchiveDescriptor, StorageErrorKind, StorageRecordIdentity,
};
use rstest::rstest;
use serde_json::json;

use super::*;
use crate::test_support::integration_test_pool;
use crate::{PostgresStorage, with_transaction};

fn initial_catalog(resource: ResourceIdentity) -> HistoryArchiveCatalog {
    let start = DateTime::from_timestamp(1_700_000_000, 123).unwrap();
    let payload_digest = [19_u8; 32];
    // Recreate the descriptor shape retained by the JSONB writer.
    let descriptor: HistoryArchiveDescriptor = serde_json::from_value(json!({
        "identity": StorageRecordIdentity::new(resource, VersionIdentity::generate()),
        "valid_from": start,
        "valid_to": start + Duration::nanoseconds(345),
        "payload_digest": payload_digest,
        "dependencies": [VersionIdentity::generate()],
        "format_version": 1,
        "location": ArtifactReference::from_catalog(ArtifactIdentity::new(DestinationId::generate()), [29; 32]),
    })).unwrap();
    let empty = HistoryArchiveCatalog::empty(resource);
    empty.publish(empty.generation(), vec![descriptor]).unwrap()
}

#[derive(Clone, Copy)]
enum CatalogCase {
    LegacyCatalog,
    UpgradedCatalog,
    LegacyReaderAfterUpdate,
    LegacyWriterAfterUpdate,
    StaleUpdate,
}

#[rstest]
#[case::legacy_json(CatalogCase::LegacyCatalog)]
#[case::packed_roundtrip(CatalogCase::UpgradedCatalog)]
#[case::legacy_reader_upgrade_error(CatalogCase::LegacyReaderAfterUpdate)]
#[case::legacy_writer_preserves_packed(CatalogCase::LegacyWriterAfterUpdate)]
#[case::stale_generation(CatalogCase::StaleUpdate)]
#[tokio::test]
async fn update_from_retained_json_preserves_catalog_contract(#[case] check: CatalogCase) {
    let pool = integration_test_pool(2);
    let backend = PostgresStorage::unobserved(pool.clone());
    let registration = backend.register_runtime().await.unwrap();
    let permit = backend
        .capture_application_execution(Some(registration.owner()))
        .await
        .unwrap();
    let resource = ResourceIdentity::generate();
    let first = initial_catalog(resource);
    let second = first
        .publish(first.generation(), first.versions().to_vec())
        .unwrap();
    let result = with_transaction(&pool, async |conn| {
        let legacy = fenced!(sql_query("SELECT public.hubuum_history_archive_catalog_publish($1,0,$2,$3,$4,$5,$6) AS allowed")
            .bind::<SqlUuid,_>(Uuid::from_bytes(*resource.as_bytes()))
            .bind::<Jsonb,_>(json(&first, HISTORY_ARCHIVE_CATALOG_BYTES)?), &permit)
            .get_result::<Allowed>(conn).await?;
        assert!(legacy.allowed);
        if !matches!(check, CatalogCase::LegacyCatalog) {
            store(conn, &second, first.generation(), &permit).await?;
        }
        match check {
                CatalogCase::LegacyCatalog => assert_eq!(load(conn, resource, false, &permit).await?, first),
                CatalogCase::UpgradedCatalog => assert_eq!(load(conn, resource, false, &permit).await?, second),
                CatalogCase::LegacyReaderAfterUpdate => {
                    conn.batch_execute("SAVEPOINT old_reader").await?;
                    let result = fenced!(sql_query("SELECT public.hubuum_history_archive_catalog_read($1,false,$2,$3,$4,$5) AS raw")
                        .bind::<SqlUuid,_>(Uuid::from_bytes(*resource.as_bytes())), &permit).execute(conn).await;
                    conn.batch_execute("ROLLBACK TO SAVEPOINT old_reader").await?;
                    assert!(result.unwrap_err().to_string().contains("requires an upgraded reader"));
                }
                CatalogCase::LegacyWriterAfterUpdate => {
                    let third = second.publish(second.generation(), second.versions().to_vec())?;
                    conn.batch_execute("SAVEPOINT old_writer").await?;
                    let result = fenced!(sql_query("SELECT public.hubuum_history_archive_catalog_publish($1,$2,$3,$4,$5,$6,$7) AS allowed")
                        .bind::<SqlUuid,_>(Uuid::from_bytes(*resource.as_bytes()))
                        .bind::<BigInt,_>(second.generation().value())
                        .bind::<Jsonb,_>(json(&third,HISTORY_ARCHIVE_CATALOG_BYTES)?), &permit)
                        .get_result::<Allowed>(conn).await;
                    conn.batch_execute("ROLLBACK TO SAVEPOINT old_writer").await?;
                    assert!(result.is_err());
                    assert_eq!(load(conn, resource, false, &permit).await?, second);
                }
                CatalogCase::StaleUpdate => {
                    let stale = store(conn, &second, first.generation(), &permit).await.unwrap_err();
                    assert_eq!(stale.kind(), StorageErrorKind::Conflict);
                }
        }
        Err::<(),_>(PostgresStorageError::invalid_input("rollback catalog fixture"))
    }).await;
    backend.unregister_runtime(registration).await.unwrap();
    assert_eq!(result.unwrap_err().to_string(), "rollback catalog fixture");
}
