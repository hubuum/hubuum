//! Native catalog plus embedded index and manifest bytes for the same retained records.
use chrono::{DateTime, Duration};
use hubuum_artifacts::{ArtifactIdentity, ArtifactStore, Destination, DestinationId};
use hubuum_domain::{IdentityReference, ResourceIdentity, VersionIdentity};
use hubuum_storage_core::{
    HistoryArchiveCatalog, HistoryArchiveDescriptor, HistoryArchiveRow,
    StorageBackupHistorySection, StorageIdentityEvidence, StorageIdentitySubject,
    StorageRecordIdentity,
};
use hubuum_storage_memory::MemoryStorage;
use hubuum_storage_postgres::test_support::measure_archive_catalog_allocation;
use rstest::rstest;
use serde_json::json;

use crate::services::history::segments::{
    HistorySegment, SegmentRecord, SegmentScope, VersionInterval,
};

const VERSIONS_PER_RESOURCE: usize = 100;

#[derive(Default)]
struct RetainedFixture {
    catalogs: Vec<HistoryArchiveCatalog>,
    record_bytes: u64,
    segment_bytes: u64,
    index_bytes: u64,
    manifest_bytes: u64,
}

impl RetainedFixture {
    async fn add_resource(&mut self, number: usize, destination: &Destination) {
        // Release the fixture byte store after each verified publication. Only
        // metadata is needed for the native catalog allocation measurement.
        let store = MemoryStorage::new();
        store.register_destination(destination).await.unwrap();
        let resource = ResourceIdentity::generate();
        let scope = SegmentScope::new(StorageBackupHistorySection::CollectionHistory, resource);
        let start = DateTime::from_timestamp(1_700_000_000, 0).unwrap();
        let mut rows = Vec::with_capacity(VERSIONS_PER_RESOURCE);
        let mut records = Vec::with_capacity(VERSIONS_PER_RESOURCE);
        for revision in 0..VERSIONS_PER_RESOURCE {
            let version = VersionIdentity::generate();
            let identity = StorageRecordIdentity::new(resource, version);
            let evidence = StorageIdentityEvidence::try_new(
                Some(StorageIdentitySubject::Resource {
                    identity: IdentityReference::Known { identity: resource },
                    version,
                }),
                Default::default(),
            )
            .unwrap();
            let from = start + Duration::seconds(revision as i64);
            let to = from + Duration::seconds(1);
            // The segment writer is uncompressed. These are the small/large
            // payload byte counts of the lifecycle overlay, applied to collection history.
            let size = if revision + 1 == VERSIONS_PER_RESOURCE {
                64 * 1024
            } else {
                256
            };
            let payload = json!({
                "id": number + 1,
                "name": format!("catalog-{number}"),
                "description": "x".repeat(size),
                "parent_collection_id": null,
                "revision": revision + 1,
                "created_at": start,
                "updated_at": from,
                "logical_identity": evidence,
            });
            let row = HistoryArchiveRow::new(identity, from, Some(to), payload).unwrap();
            let record = SegmentRecord::new(
                scope.clone(),
                VersionInterval::new(resource, version, from, to).unwrap(),
                row.payload().clone(),
            );
            self.record_bytes += serde_json::to_vec(&record).unwrap().len() as u64 + 4;
            rows.push(row);
            records.push(record);
        }
        let segment = HistorySegment::stage(
            &store,
            ArtifactIdentity::new(destination.id()),
            scope,
            records,
        )
        .await
        .unwrap();
        let reference = store.publish(segment.proof()).await.unwrap();
        self.segment_bytes += segment.proof().manifest().byte_size();
        self.index_bytes += segment.index_bytes() as u64;
        self.manifest_bytes += segment.proof().manifest().encoded_len() as u64;
        let descriptors = rows
            .iter()
            .map(|row| {
                HistoryArchiveDescriptor::new(
                    row,
                    Vec::new(),
                    segment.format_version(),
                    reference.clone(),
                )
                .unwrap()
            })
            .collect();
        let empty = HistoryArchiveCatalog::empty(resource);
        self.catalogs
            .push(empty.publish(empty.generation(), descriptors).unwrap());
    }
}

#[rstest]
#[case::ten_thousand(10_000)]
#[case::hundred_thousand(100_000)]
#[actix_web::test]
#[ignore = "PostgreSQL archive catalog allocation diagnostic"]
async fn postgres_archive_catalog_allocation(#[case] versions: usize) {
    let destination = Destination::new(DestinationId::generate(), "catalog-profile").unwrap();
    let mut retained = RetainedFixture::default();
    for number in 0..versions / VERSIONS_PER_RESOURCE {
        retained.add_resource(number, &destination).await;
    }
    let native = measure_archive_catalog_allocation(&retained.catalogs)
        .await
        .unwrap();
    let metadata = native.total_bytes() + retained.index_bytes + retained.manifest_bytes;
    println!(
        "ARCHIVE_CATALOG_ALLOCATION {}",
        json!({
            "versions": versions,
            "native_catalog": native,
            "native_catalog_bytes": native.total_bytes(),
            "framed_record_bytes": retained.record_bytes,
            "segment_bytes_including_index": retained.segment_bytes,
            "embedded_index_bytes": retained.index_bytes,
            "manifest_bytes": retained.manifest_bytes,
            "metadata_lower_bound_bytes": metadata,
            "p1_allowance_using_framed_records": retained.record_bytes / 20 + 1024 * 1024,
            "p1_allowance_including_index_in_denominator": retained.segment_bytes / 20 + 1024 * 1024,
        })
    );
}
