//! Offline diagnostic for #514. The in-memory store is a fixture, not a worker
//! RSS measurement or a persistent archive catalog.
use chrono::{DateTime, Utc};
use hubuum::benchmark_support::history_segments::{
    HistorySegment, MAX_RECORDS, RetainedHistorySegment, SEGMENT_BYTES, SegmentRecord,
    SegmentScope, VersionInterval,
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
    let reference = store.publish(segment.proof()).await?;
    let segment_bytes = segment.proof().manifest().byte_size();
    let manifest_bytes = segment.proof().manifest().encoded_len();
    drop(segment);
    let mut cold_exact_budget = ByteLimit::new(SEGMENT_BYTES).start();
    let reader = RetainedHistorySegment::open(&store, &reference, &mut cold_exact_budget).await?;
    let cold_open_bytes = cold_exact_budget.used();
    let cold_exact = reader
        .exact(
            &store,
            sample_resource,
            sample_version,
            &mut cold_exact_budget,
        )
        .await?;
    drop(reader);
    let mut cold_as_of_budget = ByteLimit::new(SEGMENT_BYTES).start();
    let reader = RetainedHistorySegment::open(&store, &reference, &mut cold_as_of_budget).await?;
    let cold_as_of = reader
        .as_of(&store, sample_resource, from, &mut cold_as_of_budget)
        .await?;
    if cold_exact.as_ref() != Some(&exact) || cold_as_of.as_ref() != Some(&as_of) {
        return Err("Published and staged history lookups disagree".into());
    }
    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "diagnostic_only": true,
            "records": reader.record_count(),
            "segment_bytes": segment_bytes,
            "index_bytes": reader.index_bytes(),
            "manifest_bytes": manifest_bytes,
            "result_bytes": serde_json::to_vec(&exact)?.len(),
            "exact_block_bytes": exact_budget.used(),
            "as_of_block_bytes": as_of_budget.used(),
            "cold_open_bytes": cold_open_bytes,
            "cold_exact_total_bytes": cold_exact_budget.used(),
            "cold_as_of_total_bytes": cold_as_of_budget.used(),
            "unqualified": ["production RSS", "durable catalog/index recovery", "authorization and dependency coverage", "task snapshots and source retirement"]
        }))?
    );
    Ok(())
}
