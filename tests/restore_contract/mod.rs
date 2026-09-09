use async_trait::async_trait;
use hubuum::storage::StorageBackendKind;
use hubuum::test_support::RestoreContractFixture;
use hubuum_domain::ResourceId;
use hubuum_storage_conformance::{
    BackupRestoreFixture, FixtureError, verify_backup_restore_contract,
    verify_failed_restore_preserves_snapshot,
};
use hubuum_storage_core::*;
use serde_json::{Value, json};

use super::*;

struct RecoveryFixture(RestoreContractFixture);

#[async_trait]
impl BackupRestoreFixture for RecoveryFixture {
    async fn capture(&self, include_history: bool) -> Result<StorageBackupSnapshot, FixtureError> {
        self.0
            .capture(include_history)
            .await
            .map_err(|error| std::io::Error::other(error.to_string()).into())
    }
    async fn restore(&self, snapshot: StorageBackupSnapshot) -> Result<(), FixtureError> {
        self.0
            .restore(snapshot)
            .await
            .map_err(|error| std::io::Error::other(error.to_string()).into())
    }
    async fn fail_restore(&self, snapshot: StorageBackupSnapshot) -> Result<(), FixtureError> {
        self.0
            .fail_restore(snapshot)
            .await
            .map_err(|error| std::io::Error::other(error.to_string()).into())
    }
    async fn mutate(&self) -> Result<(), FixtureError> {
        self.0
            .mutate()
            .await
            .map_err(|error| std::io::Error::other(error.to_string()).into())
    }
}

#[rstest]
#[case::history_free(false)]
#[case::retained_history(true)]
#[tokio::test]
async fn every_selectable_backend_preserves_recovery_across_backup_generations(
    #[case] include_history: bool,
) {
    let _guard = RESTORE_TEST_LOCK.lock().await;
    for kind in StorageBackendKind::ALL {
        let pool =
            postgres_test_pool_with_timeout(&database_url(), 2, DEFAULT_DB_STATEMENT_TIMEOUT_MS);
        let fixture = RecoveryFixture(RestoreContractFixture::new(kind, pool).await.unwrap());
        let result = verify_backup_restore_contract(&fixture, include_history).await;
        fixture
            .0
            .restore_original()
            .await
            .expect("restore original fixture database");
        result
            .unwrap_or_else(|error| panic!("{} recovery contract failed: {error}", kind.as_str()));
    }
}

#[tokio::test]
async fn every_selectable_backend_rolls_back_failed_restore() {
    let _guard = RESTORE_TEST_LOCK.lock().await;
    for kind in StorageBackendKind::ALL {
        let pool =
            postgres_test_pool_with_timeout(&database_url(), 2, DEFAULT_DB_STATEMENT_TIMEOUT_MS);
        let fixture = RecoveryFixture(RestoreContractFixture::new(kind, pool).await.unwrap());
        let result = verify_failed_restore_preserves_snapshot(&fixture).await;
        fixture
            .0
            .restore_original()
            .await
            .expect("restore original fixture database");
        assert!(
            result.is_ok(),
            "{} restore rollback contract failed: {result:?}",
            kind.as_str()
        );
    }
}

fn export_payload(content: StorageExportTaskArtifactContent) -> StorageTaskCompletionPayload {
    StorageTaskCompletionPayload::Export(
        StorageExportTaskArtifact::builder(
            "application/json",
            content,
            json!({}),
            json!([]),
            Utc::now() + chrono::Duration::hours(1),
        )
        .try_build()
        .unwrap(),
    )
}

fn remote_payload() -> StorageTaskCompletionPayload {
    StorageTaskCompletionPayload::RemoteCall(StorageRemoteCallTaskArtifact::new(
        StorageRemoteCallArtifactTarget::new(
            None,
            StorageRemoteTargetSubjectType::Collection,
            ResourceId::new(1).unwrap(),
            Some(StorageRemoteTargetHttpMethod::Get),
            "https://example.invalid",
        ),
        StorageRemoteCallArtifactResponse::new(
            Some(200),
            Some(json!({"retained": "header"})),
            Some("retained body".to_string()),
        ),
        StorageRemoteCallArtifactOutcome::new(3, true, None),
    ))
}

#[rstest]
#[case::json_null(export_payload(StorageExportTaskArtifactContent::Json(Value::Null)))]
#[case::json_object(export_payload(StorageExportTaskArtifactContent::Json(json!({"kept": true}))))]
#[case::text(export_payload(StorageExportTaskArtifactContent::Text("kept".to_string())))]
#[case::remote(remote_payload())]
#[tokio::test]
async fn memory_preserves_terminal_artifacts_across_recovery(
    #[case] payload: StorageTaskCompletionPayload,
) {
    let pool = postgres_test_pool_with_timeout(&database_url(), 2, DEFAULT_DB_STATEMENT_TIMEOUT_MS);
    let fixture = RecoveryFixture(
        RestoreContractFixture::new(StorageBackendKind::Memory, pool)
            .await
            .unwrap(),
    );
    fixture.0.seed_terminal_artifact(payload).await.unwrap();
    verify_backup_restore_contract(&fixture, true)
        .await
        .unwrap();
}
