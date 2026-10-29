//! Explicit resource probe: run in fresh processes through the integration driver.
use super::{claim_worker_test_task, create_worker_test_task, process_claimed_task_for_test};
use crate::models::{NewHubuumClass, NewHubuumObject};
use crate::storage::{StorageTaskKind, TaskQueueStorage};
use crate::tests::TestContext;
use crate::traits::CanSave;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures_util::StreamExt;
use hubuum_storage_core::artifacts::*;
use hubuum_storage_core::{
    StorageTaskLease, StorageTaskLeaseDuration, StorageTaskOutputLookup, TaskCapacityStorage,
};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

struct SlowDestination {
    store: Arc<dyn TaskArtifactStore>,
    rate: u64,
    started: Instant,
    first_write: AtomicU64,
    sealed: AtomicU64,
    verified: AtomicU64,
    bytes: AtomicU64,
}
impl SlowDestination {
    fn millis(&self) -> u64 {
        self.started.elapsed().as_millis() as u64
    }
    async fn delay(&self, bytes: usize) {
        if self.rate > 0 {
            tokio::time::sleep(Duration::from_secs_f64(bytes as f64 / self.rate as f64)).await;
        }
    }
}
#[async_trait]
impl ArtifactStore for SlowDestination {
    async fn register_destination(&self, value: &Destination) -> ArtifactResult<()> {
        self.store.register_destination(value).await
    }
    async fn stage(&self, id: &ArtifactIdentity, limits: ArtifactLimits) -> ArtifactResult<()> {
        self.store.stage(id, limits).await
    }
    async fn append(
        &self,
        id: &ArtifactIdentity,
        ordinal: u32,
        bytes: &[u8],
    ) -> ArtifactResult<()> {
        let _ = self.first_write.compare_exchange(
            0,
            self.millis().max(1),
            Ordering::SeqCst,
            Ordering::SeqCst,
        );
        self.delay(bytes.len()).await;
        self.store.append(id, ordinal, bytes).await?;
        self.bytes.fetch_add(bytes.len() as u64, Ordering::SeqCst);
        Ok(())
    }
    async fn seal(&self, manifest: &ArtifactManifest) -> ArtifactResult<()> {
        self.store.seal(manifest).await?;
        self.sealed.store(self.millis(), Ordering::SeqCst);
        Ok(())
    }
    async fn manifest(&self, id: &ArtifactIdentity) -> ArtifactResult<Vec<u8>> {
        self.store.manifest(id).await
    }
    async fn published_manifest(&self, value: &ArtifactReference) -> ArtifactResult<Vec<u8>> {
        self.store.published_manifest(value).await
    }
    async fn block(&self, id: &ArtifactIdentity, ordinal: u32) -> ArtifactResult<Option<Vec<u8>>> {
        let bytes = self.store.block(id, ordinal).await?;
        if let Some(bytes) = &bytes {
            self.delay(bytes.len()).await;
        } else {
            self.verified.store(self.millis(), Ordering::SeqCst);
        }
        Ok(bytes)
    }
    async fn publish(&self, value: &VerifiedArtifact) -> ArtifactResult<ArtifactReference> {
        self.store.publish(value).await
    }
    async fn abort(&self, id: &ArtifactIdentity) -> ArtifactResult<()> {
        self.store.abort(id).await
    }
    async fn cleanup(&self) -> ArtifactResult<u32> {
        self.store.cleanup().await
    }
}
#[async_trait]
impl TaskArtifactStore for SlowDestination {
    async fn bind_task(
        &self,
        id: &ArtifactIdentity,
        lease: &StorageTaskLease,
        reservation: ArtifactReservation,
        expires: DateTime<Utc>,
    ) -> ArtifactResult<()> {
        self.store.bind_task(id, lease, reservation, expires).await
    }
}

fn setting(name: &str, fallback: u64) -> u64 {
    std::env::var(name).map_or(fallback, |value| value.parse().unwrap())
}

#[cfg(unix)]
fn native_high_water_rss(who: libc::c_int) -> u64 {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    // getrusage initializes the complete structure on success. Darwin reports
    // bytes; the other supported Unix test platforms report KiB.
    assert_eq!(unsafe { libc::getrusage(who, usage.as_mut_ptr()) }, 0);
    let bytes = unsafe { usage.assume_init() }.ru_maxrss as u64;
    if cfg!(target_os = "macos") {
        bytes
    } else {
        bytes * 1024
    }
}

#[actix_web::test]
#[ignore = "explicit PostgreSQL artifact resource profile; tests/python integration artifact-pipeline"]
async fn full_pipeline_resource_probe() {
    use crate::storage::{StorageHandle, TaskExecutionStorage};
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::ERROR)
        .with_test_writer()
        .try_init();
    let config = crate::config::get_config().unwrap();
    let pool = crate::tests::postgres_test_pool(&config.database_url, config.db_pool_size);
    let context = TestContext::with_pool(pool).await;
    let collection = context.collection_fixture("artifact_resource").await;
    let class = NewHubuumClass {
        name: context.scoped_name("artifact_rows"),
        description: String::new(),
        collection_id: collection.collection_id(),
        json_schema: None,
        validate_schema: Some(false),
    }
    .save_without_events(&context.pool)
    .await
    .unwrap();
    let row_bytes = setting("HUBUUM_PROBE_ROW_BYTES", 64 * 1024) as usize;
    let rows = setting("HUBUUM_PROBE_ROWS", 1024) as usize;
    let mode = std::env::var("HUBUUM_PROBE_MODE").unwrap_or_else(|_| "export".into());
    let slots = setting("HUBUUM_PROBE_SLOTS", 2) as usize;
    let rate = setting("HUBUUM_PROBE_RATE", 1024 * 1024);
    for index in 0..rows {
        NewHubuumObject {
            name: format!("row-{index:06}"),
            hubuum_class_id: class.id,
            collection_id: collection.collection_id(),
            description: String::new(),
            data: json!({"payload":"x".repeat(row_bytes - 1024)}),
        }
        .save_without_events(&context.pool)
        .await
        .unwrap();
    }
    let template: crate::models::NewExportTemplate = serde_json::from_value(json!({
        "collection_id":collection.collection_id(),"name":context.scoped_name("artifact.txt"),"description":"resource probe",
        "content_type":"text/plain","template":"{% for item in items %}{{ item.data.payload }}\n{% endfor %}","kind":"export","scope_kind":"objects_in_class","class_id":class.id
    })).unwrap();
    let template = template.save_without_events(&context.pool).await.unwrap();
    let baseline = StorageHandle::postgres(context.pool.get_ref().clone());
    let stop = Arc::new(AtomicBool::new(false));
    let peak = Arc::new(AtomicU64::new(0));
    let monitor = {
        let stop = stop.clone();
        let peak = peak.clone();
        std::thread::spawn(move || {
            let mut system = sysinfo::System::new();
            let pid = sysinfo::get_current_pid().unwrap();
            while !stop.load(Ordering::SeqCst) {
                system.refresh_processes_specifics(
                    sysinfo::ProcessesToUpdate::All,
                    true,
                    sysinfo::ProcessRefreshKind::nothing()
                        .with_memory()
                        .without_tasks(),
                );
                let rss = system
                    .processes()
                    .values()
                    .filter(|process| process.pid() == pid || process.parent() == Some(pid))
                    .map(|process| process.memory())
                    .sum();
                peak.fetch_max(rss, Ordering::SeqCst);
                std::thread::sleep(Duration::from_millis(20));
            }
        })
    };
    let mut pending = Vec::new();
    let mut task_ids = Vec::new();
    for slot in 0..slots {
        let backup = mode == "backup" && slot == 0;
        let payload = if backup {
            json!({"include_history":false})
        } else {
            json!({"export":{"scope":{"kind":"objects_in_class","class_id":class.id},"limits":{"max_items":rows,"max_output_bytes":crate::config::get_config().unwrap().export_max_output_bytes}},"template_id":if slot % 2 == 1 { Some(template.id) } else { None }})
        };
        let task = create_worker_test_task(
            &context,
            if backup {
                StorageTaskKind::Backup
            } else {
                StorageTaskKind::Export
            },
            payload,
            1,
            &format!("artifact-{slot}"),
        )
        .await;
        task_ids.push(hubuum_domain::TaskId::new(task.id).unwrap());
        let claimed = claim_worker_test_task(&context, task.id).await;
        baseline
            .renew_task_lease(
                claimed.lease().clone(),
                StorageTaskLeaseDuration::from_milliseconds(3_600_000).unwrap(),
            )
            .await
            .unwrap();
        let slow = Arc::new(SlowDestination {
            store: baseline.artifact_store(),
            rate,
            started: Instant::now(),
            first_write: AtomicU64::new(0),
            sealed: AtomicU64::new(0),
            verified: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
        });
        let storage = baseline.clone().with_artifact_test_store(slow.clone());
        pending.push(async move {
            process_claimed_task_for_test(&storage, &claimed).await.unwrap();
            let (finished, _) = storage.get_task_access(claimed.lease().task_id()).await.unwrap().into_parts();
            assert_eq!(finished.status(), hubuum_storage_core::StorageTaskStatus::Succeeded, "{:?}", finished.summary());
            let completed_ms = slow.millis();
            let verified_ms = slow.verified.load(Ordering::SeqCst);
            let reference = if backup {
                let StorageTaskOutputLookup::Available(output) = storage.get_backup_output(finished.id()).await.unwrap() else { panic!("missing backup output") }; output.artifact_reference().unwrap().clone()
            } else {
                let StorageTaskOutputLookup::Available(output) = storage.get_export_output(finished.id()).await.unwrap() else { panic!("missing export output") }; output.artifact_reference().unwrap().clone()
            };
            let manifest = ArtifactManifest::decode(&storage.manifest(reference.identity()).await.unwrap(), reference.identity(), ArtifactLimits::format_ceiling()).unwrap();
            // The production response stream sees a slow consumer without retaining the output.
            let mut download = Box::pin(crate::services::artifacts::download(&storage, reference.clone()));
            let mut digest = Sha256::new(); let mut downloaded = 0;
            while let Some(block) = download.next().await {
                let block = block.unwrap(); downloaded += block.len() as u64; digest.update(&block);
                if rate > 0 { tokio::time::sleep(Duration::from_secs_f64(block.len() as f64 / rate as f64)).await; }
            }
            assert_eq!(downloaded, manifest.byte_size());
            assert_eq!(<[u8;32]>::from(digest.finalize()), *manifest.stream_digest());
            let upload_ms = slow.sealed.load(Ordering::SeqCst) - slow.first_write.load(Ordering::SeqCst);
            let evidence = json!({"slot":slot,"kind":if backup { "backup" } else if slot % 2 == 1 { "template" } else { "json" },"bytes":downloaded,"producer_ms":slow.first_write.load(Ordering::SeqCst),"upload_ms":upload_ms,"verification_ms":verified_ms.saturating_sub(slow.sealed.load(Ordering::SeqCst)),"publication_ms":completed_ms.saturating_sub(verified_ms),"download_ms":slow.millis()-completed_ms,"upload_mib_per_second":downloaded as f64 / (1024.0*1024.0) / (upload_ms as f64 / 1000.0)});
            evidence
        });
    }
    let capacity_stop = stop.clone();
    let capacity_storage = baseline.clone();
    let capacity_monitor = tokio::spawn(async move {
        let mut peak = 0;
        while !capacity_stop.load(Ordering::SeqCst) {
            let snapshot = capacity_storage.task_capacity_snapshot().await.unwrap();
            peak = peak
                .max(snapshot.ordinary().resident_bytes() + snapshot.internal().resident_bytes());
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        peak
    });
    let samples = futures_util::future::join_all(pending).await;
    stop.store(true, Ordering::SeqCst);
    monitor.join().unwrap();
    let peak = peak.load(Ordering::SeqCst);
    let resident = capacity_monitor.await.unwrap();
    #[cfg(unix)]
    let native_peak =
        native_high_water_rss(libc::RUSAGE_SELF) + native_high_water_rss(libc::RUSAGE_CHILDREN);
    #[cfg(not(unix))]
    let native_peak: Option<u64> = None;
    println!(
        "ARTIFACT_PIPELINE_EVIDENCE {}",
        json!({"mode":mode,"rows":rows,"row_bytes":row_bytes,"rate":rate,"slots":slots,"worker_and_children_peak_rss":peak,"native_worker_plus_child_high_water_rss":native_peak,"deployment_peak_reserved_bytes":resident,"streams":samples})
    );
    for id in task_ids {
        hubuum_storage_postgres::test_support::delete_task(context.pool.get_ref(), id)
            .await
            .unwrap();
    }
    while baseline.cleanup().await.unwrap() != 0 {}
    collection.cleanup().await.unwrap();
    assert!(
        resident <= 8 * 1024 * 1024 * 1024,
        "H2: deployment reservations {resident}"
    );
    assert!(
        peak <= 512 * 1024 * 1024,
        "H1: worker plus renderer RSS {peak}"
    );
    #[cfg(unix)]
    assert!(
        native_peak <= 512 * 1024 * 1024,
        "H1: native RSS high-water {native_peak}"
    );
}
