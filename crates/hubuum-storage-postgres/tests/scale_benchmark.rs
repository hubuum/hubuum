#![cfg(all(
    feature = "integration-test-support",
    feature = "scale-benchmark-support"
))]

use hubuum_scale_core::ScaleBenchmarkBackend;
use hubuum_storage_postgres::scale_benchmark::PostgresScaleBackend;

#[tokio::test]
async fn benchmark_preparation_reports_postgres_identity() {
    let database_url = std::env::var("HUBUUM_DATABASE_URL")
        .expect("HUBUUM_DATABASE_URL must identify the migrated integration-test database");
    let backend = PostgresScaleBackend::connect(&database_url, 1).expect("benchmark backend");

    let preparation = backend
        .prepare_measurement()
        .await
        .expect("benchmark preparation");

    assert_eq!(preparation.identity.name, "postgres");
    assert!(!preparation.identity.version.trim().is_empty());
    assert!(
        preparation
            .identity
            .settings
            .contains_key("max_connections")
    );
}
