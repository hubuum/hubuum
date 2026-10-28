use std::collections::BTreeSet;

use async_trait::async_trait;
use hubuum::storage::StorageBackendKind;
use hubuum::test_support::RestoreContractFixture;
use hubuum_domain::{ResourceId, ResourceIdentity};
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
    async fn restore(
        &self,
        snapshot: StorageBackupSnapshot,
    ) -> Result<StorageRestoreIdentityContext, FixtureError> {
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
        // The shared runner compares every field (including provenance and revisions),
        // then writes again and restores a second generation in both history modes.
        let snapshot = fixture.0.capture(true).await.unwrap();
        let (state, history) = snapshot.into_parts();
        assert!(!state[&StorageBackupStateSection::QueryUsageDeclarations].is_empty());
        assert!(
            history.unwrap()[&StorageBackupHistorySection::ComputedFieldHistory]
                .iter()
                .any(|row| row
                    .get("definitions")
                    .and_then(Value::as_array)
                    .is_some_and(|fields| fields
                        .iter()
                        .any(|field| field["definition"]["key"] == "retired_recovery_field")))
        );
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

#[rstest]
#[case::utc("UTC")]
#[case::positive_offset("Asia/Kolkata")]
#[case::negative_offset("America/New_York")]
#[tokio::test]
async fn memory_schema_provenance_survives_postgres_restore(#[case] timezone: &str) {
    use diesel::sql_types::Text;
    let _guard = RESTORE_TEST_LOCK.lock().await;
    let pool = postgres_test_pool_with_timeout(&database_url(), 1, DEFAULT_DB_STATEMENT_TIMEOUT_MS);
    let source = RestoreContractFixture::new(StorageBackendKind::Memory, pool.clone())
        .await
        .unwrap();
    let target = RestoreContractFixture::new(StorageBackendKind::Postgres, pool.clone())
        .await
        .unwrap();
    let snapshot = source.capture(true).await.unwrap();
    with_connection(&pool, async |connection| {
        diesel::sql_query("SELECT set_config('TimeZone', $1, false)")
            .bind::<Text, _>(timezone)
            .execute(connection)
            .await
    })
    .await
    .unwrap();
    let result = async {
        target.restore(snapshot.clone()).await?;
        target.capture(true).await
    }
    .await;
    target.restore_original().await.unwrap();
    let restored = result.unwrap();
    let (expected_state, expected_history) = snapshot.into_parts();
    let (actual_state, actual_history) = restored.into_parts();
    for section in [
        StorageBackupStateSection::ClassSchemaRevisions,
        StorageBackupStateSection::ClassSchemaState,
        StorageBackupStateSection::ObjectSchemaEvidence,
    ] {
        assert_eq!(
            expected_state[&section], actual_state[&section],
            "{timezone}: {section:?}"
        );
    }
    assert_eq!(
        expected_history.unwrap()[&StorageBackupHistorySection::ClassSchemaHistory],
        actual_history.unwrap()[&StorageBackupHistorySection::ClassSchemaHistory]
    );
}

enum ReusedRecord {
    Class,
    Principal,
    Task,
}

#[rstest]
#[case::class(ReusedRecord::Class)]
#[case::principal(ReusedRecord::Principal)]
#[case::task(ReusedRecord::Task)]
#[tokio::test]
async fn restoring_older_backup_cannot_reuse_discarded_logical_identity(
    #[case] record: ReusedRecord,
) {
    let _guard = RESTORE_TEST_LOCK.lock().await;
    for kind in StorageBackendKind::ALL {
        let pool =
            postgres_test_pool_with_timeout(&database_url(), 2, DEFAULT_DB_STATEMENT_TIMEOUT_MS);
        let fixture = RestoreContractFixture::new(kind, pool).await.unwrap();
        let name = TestScope::new().scoped_name("discarded_identity");
        let source = fixture.capture(true).await.unwrap();
        let result = async {
            if matches!(record, ReusedRecord::Task) {
                // Establish the post-restore queue first: PostgreSQL schedules
                // rebuild work, which also legitimately consumes task integers.
                fixture.restore(source.clone()).await?;
            }
            let create = || async {
                let target = match record {
                    ReusedRecord::Principal => {
                        let user = fixture
                            .storage()
                            .create_user(StorageUserCreate::new(
                                None,
                                name.clone(),
                                "fixture-password-hash",
                                None,
                                None,
                                hubuum::events::EventContext::system(),
                            ))
                            .await?
                            .into_value();
                        StorageIdentityTarget::Principal(user.into_parts().id().into())
                    }
                    ReusedRecord::Class => {
                        let class = fixture.create_class(&name).await?;
                        StorageIdentityTarget::Resource {
                            kind: StorageIdentityResourceKind::Class,
                            id: class.id().into(),
                        }
                    }
                    ReusedRecord::Task => StorageIdentityTarget::Task(
                        fixture.create_task(StorageTaskKind::Export).await?,
                    ),
                };
                let evidence = fixture.storage().resolve_logical_identity(target).await?;
                Ok::<_, hubuum::errors::ApiError>((target, evidence))
            };
            let discarded = create().await?;
            fixture.restore(source).await?;
            let replacement = create().await?;
            Ok::<_, hubuum::errors::ApiError>((discarded, replacement))
        }
        .await;
        fixture
            .restore_original()
            .await
            .expect("restore original fixture database");
        let ((old_integer, old_evidence), (new_integer, new_evidence)) = result.unwrap();
        assert_eq!(
            old_integer,
            new_integer,
            "{} must exercise actual integer reuse",
            kind.as_str()
        );
        match record {
            ReusedRecord::Class => assert_ne!(
                old_evidence
                    .subject()
                    .unwrap()
                    .record_identity()
                    .unwrap()
                    .resource(),
                new_evidence
                    .subject()
                    .unwrap()
                    .record_identity()
                    .unwrap()
                    .resource(),
                "{} reused a discarded resource identity",
                kind.as_str()
            ),
            ReusedRecord::Principal | ReusedRecord::Task => assert_ne!(
                old_evidence.subject(),
                new_evidence.subject(),
                "{} reused a discarded identity",
                kind.as_str()
            ),
        }
    }
}

fn class_incarnations(snapshot: &StorageBackupSnapshot) -> BTreeSet<ResourceIdentity> {
    let (state, _) = snapshot.clone().into_parts();
    state[&StorageBackupStateSection::Classes]
        .iter()
        .map(|row| {
            serde_json::from_value::<StorageIdentityEvidence>(
                row.get("logical_identity").unwrap().clone(),
            )
            .unwrap()
            .subject()
            .unwrap()
            .record_identity()
            .unwrap()
            .resource()
        })
        .collect()
}

#[rstest]
#[case::six(6)]
#[case::seven(7)]
#[case::eight(8)]
#[case::nine(9)]
#[case::ten(10)]
#[tokio::test]
async fn legacy_restore_quarantines_correspondence_and_preserves_upgraded_identities(
    #[case] version: i32,
    #[values(false, true)] include_history: bool,
) {
    let _guard = RESTORE_TEST_LOCK.lock().await;
    for kind in StorageBackendKind::ALL {
        let pool =
            postgres_test_pool_with_timeout(&database_url(), 2, DEFAULT_DB_STATEMENT_TIMEOUT_MS);
        let fixture = RestoreContractFixture::new(kind, pool).await.unwrap();
        let source = fixture.capture(include_history).await.unwrap();
        let native_identities = class_incarnations(&source);
        let (mut state, mut history) = source.into_parts();
        for rows in state.values_mut().chain(
            history
                .iter_mut()
                .flat_map(|sections| sections.values_mut()),
        ) {
            for row in rows {
                let mut fields = row.fields().clone();
                fields.remove("logical_identity");
                // Deliberately retain nested schema evidence: a legacy reader
                // must quarantine that too, rather than trusting injected UUIDs.
                *row = StorageBackupRow::try_from_value(Value::Object(fields)).unwrap();
            }
        }
        let legacy = StorageBackupSnapshot::try_new(state, history).unwrap();
        let result = async {
            fixture.restore_legacy(legacy, version).await?;
            let upgraded = fixture.capture(true).await?;
            let identities = class_incarnations(&upgraded);
            fixture.restore(upgraded).await?;
            let repeated = class_incarnations(&fixture.capture(true).await?);
            Ok::<_, hubuum::errors::ApiError>((identities, repeated))
        }
        .await;
        fixture
            .restore_original()
            .await
            .expect("restore original fixture database");
        let (upgraded, repeated) = result
            .unwrap_or_else(|error| panic!("{} format {version} restore: {error}", kind.as_str()));
        assert!(
            native_identities.is_disjoint(&upgraded),
            "legacy data must not assert correspondence to pre-existing archives"
        );
        assert_eq!(
            upgraded, repeated,
            "format 11 must preserve the quarantined import's assigned identities"
        );
    }
}
