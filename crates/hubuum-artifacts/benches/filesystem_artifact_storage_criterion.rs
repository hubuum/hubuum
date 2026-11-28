//! Local filesystem diagnostics; no server, database, or global CONFIG.
#[cfg(unix)]
mod filesystem {
    use std::hint::black_box;
    use std::os::unix::fs::PermissionsExt;

    use criterion::measurement::WallTime;
    use criterion::{BatchSize, BenchmarkGroup, BenchmarkId, Criterion, Throughput};
    use hubuum_artifacts::{
        ArtifactIdentity, ArtifactLimits, ArtifactReader, ArtifactReference, ArtifactStore,
        ArtifactWriter, Destination, DestinationId, FilesystemArtifactStore,
    };
    use sha2::{Digest, Sha256};
    use tempfile::TempDir;
    use tokio::runtime::{Builder, Runtime};

    async fn fixture() -> (FilesystemArtifactStore, TempDir, ArtifactIdentity) {
        let directory = tempfile::Builder::new()
            .prefix("hubuum-artifact-bench-")
            .permissions(std::fs::Permissions::from_mode(0o700))
            .tempdir()
            .unwrap();
        let store = FilesystemArtifactStore::open(directory.path().canonicalize().unwrap())
            .await
            .unwrap();
        let destination = Destination::new(DestinationId::generate(), "benchmark").unwrap();
        store.register_destination(&destination).await.unwrap();
        let identity = ArtifactIdentity::new(destination.id());
        (store, directory, identity)
    }

    async fn publish(
        store: &FilesystemArtifactStore,
        identity: ArtifactIdentity,
        payload: &[u8],
    ) -> ArtifactReference {
        let limits = ArtifactLimits::builder()
            .artifact_bytes(payload.len() as u64)
            .build()
            .unwrap();
        let mut writer = ArtifactWriter::new(store, identity, limits).await.unwrap();
        writer.write(payload).await.unwrap();
        writer.finish().await.unwrap()
    }

    async fn read(store: &FilesystemArtifactStore, reference: &ArtifactReference) -> usize {
        let mut reader = ArtifactReader::open(store, reference).await.unwrap();
        let mut bytes = 0;
        while let Some(block) = reader.next_block().await.unwrap() {
            bytes += black_box(block).len();
        }
        bytes
    }

    fn payload(size: usize) -> Vec<u8> {
        // Repeatable, nontrivial data, generated outside the measured region.
        let mut bytes = Vec::with_capacity(size);
        for ordinal in 0..size.div_ceil(32) {
            bytes.extend_from_slice(&Sha256::digest((ordinal as u64).to_le_bytes()));
        }
        bytes.truncate(size);
        bytes
    }

    fn publish_case(group: &mut BenchmarkGroup<'_, WallTime>, runtime: &Runtime, payload: &[u8]) {
        group.bench_function(
            BenchmarkId::new("write_verify_publish", payload.len()),
            |b| {
                b.iter_batched(
                    || runtime.block_on(fixture()),
                    |(store, directory, identity)| {
                        black_box(runtime.block_on(publish(&store, identity, payload)));
                        // Criterion drops this output after stopping the timer. Bound
                        // disk use to one temporary artifact, even in long profiles.
                        (store, directory)
                    },
                    BatchSize::PerIteration,
                );
            },
        );
    }

    pub fn benchmarks(criterion: &mut Criterion) {
        let runtime = Builder::new_current_thread().enable_all().build().unwrap();
        let mut group = criterion.benchmark_group("filesystem_artifact_storage");
        for size in [64 * 1024, 1024 * 1024, 8 * 1024 * 1024] {
            let payload = payload(size);
            group.throughput(Throughput::Bytes(size as u64));
            publish_case(&mut group, &runtime, &payload);

            // Setup's independent read-back warms the data cache deliberately.
            let (store, directory, identity) = runtime.block_on(fixture());
            let reference = runtime.block_on(publish(&store, identity, &payload));
            group.bench_function(BenchmarkId::new("verified_read_warm", size), |b| {
                b.iter(|| assert_eq!(black_box(runtime.block_on(read(&store, &reference))), size));
            });
            drop((store, directory));
        }
        group.finish();
    }
}

#[cfg(unix)]
criterion::criterion_group!(benches, filesystem::benchmarks);
#[cfg(unix)]
criterion::criterion_main!(benches);

#[cfg(not(unix))]
fn main() {
    eprintln!("Filesystem artifact benchmarks require a Unix filesystem");
}
