//! Run through `integration s3-artifacts --benchmark`; fixture provisioning and
//! bucket teardown are outside measurement. Retained publications are removed
//! with the disposable fixture, never by weakening the publication contract.
use std::time::Duration;

use criterion::{BenchmarkId, Criterion, Throughput};
use hubuum_artifacts::s3::{S3ArtifactStore, S3Config};
use hubuum_artifacts::{
    ArtifactIdentity, ArtifactLimits, ArtifactReader, ArtifactWriter, Destination, DestinationId,
};

fn benchmarks(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let destination = Destination::new(
        DestinationId::from_bytes([0x53; 16]).unwrap(),
        "s3-benchmark",
    )
    .unwrap();
    let config = S3Config::builder(destination)
        .bucket("hubuum-artifact-bench")
        .region("us-east-1")
        .endpoint(
            std::env::var("HUBUUM_TEST_S3_ENDPOINT")
                .expect("run integration s3-artifacts --benchmark"),
        )
        .private_ca_pem(std::fs::read(std::env::var("HUBUUM_TEST_S3_CA").unwrap()).unwrap())
        .credentials("hubuum-fixture", "hubuum-fixture-secret", None)
        .build()
        .unwrap();
    let store = runtime.block_on(S3ArtifactStore::connect(config)).unwrap();
    let mut group = c.benchmark_group("s3_artifacts");
    for size in [64 * 1024, 1024 * 1024, 8 * 1024 * 1024] {
        let bytes = vec![0x53; size];
        group.throughput(Throughput::Bytes(size as u64));
        let write = || {
            runtime.block_on(async {
                let mut writer = ArtifactWriter::new(
                    &store,
                    ArtifactIdentity::new(store.destination().id()),
                    ArtifactLimits::default(),
                )
                .await
                .unwrap();
                writer.write(&bytes).await.unwrap();
                writer.finish().await.unwrap()
            })
        };
        group.bench_with_input(
            BenchmarkId::new("write_verify_publish", size),
            &bytes,
            |b, _| b.iter(&write),
        );
        let reference = write();
        group.bench_with_input(
            BenchmarkId::new("verified_read", size),
            &reference,
            |b, reference| {
                b.iter(|| {
                    runtime.block_on(async {
                        let mut reader = ArtifactReader::open(&store, reference).await.unwrap();
                        while let Some(block) = reader.next_block().await.unwrap() {
                            std::hint::black_box(block);
                        }
                    })
                })
            },
        );
    }
    group.finish();
}

fn main() {
    if std::env::var_os("HUBUUM_TEST_S3_ENDPOINT").is_none() {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let status = std::process::Command::new("python3")
            .current_dir(root)
            .args([
                "-I",
                "-S",
                "tests/python/run.py",
                "integration",
                "s3-artifacts",
                "--benchmark-executable",
            ])
            .arg(std::env::current_exe().unwrap())
            .arg("--")
            .args(std::env::args_os().skip(1))
            .status()
            .expect("start pinned S3 fixture harness");
        assert!(status.success(), "S3 benchmark fixture failed");
        return;
    }
    let mut criterion = Criterion::default()
        .sample_size(10)
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(3))
        .configure_from_args();
    benchmarks(&mut criterion);
    criterion.final_summary();
}
