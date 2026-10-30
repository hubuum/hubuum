//! Offline diagnostic for #514. The in-memory store is a fixture, not a worker
//! RSS measurement or a persistent archive catalog.
use chrono::{DateTime, Utc};
use hubuum::benchmark_support::history_segments::{
    HistorySegment, MAX_RECORDS, SEGMENT_BYTES, SegmentRecord, SegmentScope, VersionInterval,
};
use hubuum_artifacts::{ArtifactIdentity, ArtifactStore, Destination, DestinationId};
use hubuum_budget::ByteLimit;
use hubuum_domain::{ResourceIdentity, VersionIdentity};
use hubuum_storage_core::StorageBackupHistorySection;
use hubuum_storage_memory::MemoryStorage;
use serde_json::json;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let count = std::env::args()
        .nth(1)
        .map(|value| value.parse::<usize>())
        .transpose()?
        .unwrap_or(MAX_RECORDS);
    if !(1..=MAX_RECORDS).contains(&count) {
        return Err(format!("Record count must be between 1 and {MAX_RECORDS}").into());
    }
    let store = MemoryStorage::new();
    let destination = Destination::new(DestinationId::generate(), "history-diagnostic")?;
    store.register_destination(&destination).await?;
    let scope = SegmentScope::new(
        StorageBackupHistorySection::ObjectHistory,
        ResourceIdentity::generate(),
    );
    let sample_resource = ResourceIdentity::generate();
    let sample_version = VersionIdentity::generate();
    let row_scope = scope.clone();
    let from: DateTime<Utc> = DateTime::from_timestamp(1_000, 0).unwrap();
    let to: DateTime<Utc> = DateTime::from_timestamp(2_000, 0).unwrap();
    let records = (0..count).map(move |position| {
        let (resource, version) = if position == count / 2 {
            (sample_resource, sample_version)
        } else {
            (ResourceIdentity::generate(), VersionIdentity::generate())
        };
        SegmentRecord::new(
            row_scope.clone(),
            VersionInterval::new(resource, version, from, to).unwrap(),
            json!({"data": "x".repeat(4096)}),
        )
    });
    let segment = HistorySegment::stage(
        &store,
        ArtifactIdentity::new(destination.id()),
        scope,
        records,
    )
    .await?;
    let mut exact_budget = ByteLimit::new(SEGMENT_BYTES).start();
    let exact = segment
        .exact(&store, sample_resource, sample_version, &mut exact_budget)
        .await?
        .ok_or("Missing exact record")?;
    let mut as_of_budget = ByteLimit::new(SEGMENT_BYTES).start();
    let as_of = segment
        .as_of(&store, sample_resource, from, &mut as_of_budget)
        .await?
        .ok_or("Missing as-of record")?;
    if exact != as_of {
        return Err("Exact/as-of records disagree".into());
    }
    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "diagnostic_only": true,
            "records": segment.record_count(),
            "segment_bytes": segment.proof().manifest().byte_size(),
            "index_bytes": segment.index_bytes(),
            "manifest_bytes": segment.proof().manifest().encoded_len(),
            "result_bytes": serde_json::to_vec(&exact)?.len(),
            "exact_block_bytes": exact_budget.used(),
            "as_of_block_bytes": as_of_budget.used(),
            "unqualified": ["production RSS", "durable catalog/index recovery", "authorization and dependency coverage", "task snapshots and source retirement"]
        }))?
    );
    store.abort(segment.proof().manifest().identity()).await?;
    while store.cleanup().await? != 0 {}
    Ok(())
}
