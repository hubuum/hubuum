use chrono::Utc;
use hubuum_domain::{ClassRelationId, CollectionId, PrincipalId};
use hubuum_storage_core::*;
use hubuum_storage_postgres::{PostgresFaultController, PostgresFaultPoint, PostgresPool};
use serde_json::{Value, json};
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::backups::create_backup_document;
use crate::errors::ApiError;
use crate::events::EventContext;
use crate::models::{
    BackupDocument, BackupHistory, BackupManifest, BackupRequest, BackupState,
    CURRENT_BACKUP_VERSION, RESTORE_CONFIRMATION_PHRASE, RestoreConfirmRequest, RestoreInitiator,
    RestoreJobID, RestoreJobStatus, RestoreStageRequest,
};
use crate::restores::{
    RestoreSettings, confirm_restore, execute_confirmed_restore, restore_status, stage_restore,
};
use crate::storage::{StorageBackendKind, StorageHandle};
use crate::tests::TestScope;

pub struct RestoreContractFixture {
    kind: StorageBackendKind,
    storage: StorageHandle,
    collection_id: CollectionId,
    class: StorageResolvedClass,
    scope: TestScope,
    mutation: AtomicUsize,
    original: StorageBackupSnapshot,
}

impl RestoreContractFixture {
    pub async fn new(kind: StorageBackendKind, pool: PostgresPool) -> Result<Self, ApiError> {
        let storage = super::restore_contract_storage(kind, pool);
        let original = storage.capture_backup_snapshot(true).await?;
        let scope = TestScope::new();
        let context = EventContext::system();
        let group = storage
            .create_group(
                StorageGroupCreate::new(
                    None,
                    scope.scoped_name("owner"),
                    Some("recovery contract".to_string()),
                ),
                &context,
            )
            .await
            .unwrap()
            .into_value();
        let collection = storage
            .collection_store()
            .create_collection(
                StorageCollectionCreate::new(
                    scope.scoped_name("collection"),
                    "recovery contract",
                    group.id(),
                    None,
                ),
                &context,
            )
            .await
            .unwrap()
            .into_value();
        let class = storage
            .class_store()
            .create_class(
                StorageClassCreate::builder(
                    scope.scoped_name("class"),
                    collection.id(),
                    "recovery contract",
                )
                .build(),
                &context,
            )
            .await
            .unwrap()
            .into_value();
        let other = storage
            .class_store()
            .create_class(
                StorageClassCreate::builder(
                    scope.scoped_name("other"),
                    collection.id(),
                    "recovery contract",
                )
                .schema_policy(
                    StorageClassSchemaPolicy::try_from_parts(Some(json!(true)), true).unwrap(),
                )
                .build(),
                &context,
            )
            .await
            .unwrap()
            .into_value();
        let resolved = storage
            .class_store()
            .resolve_class(StorageClassSelector::Id(class.id()))
            .await
            .unwrap();
        let resolved_other = storage
            .class_store()
            .resolve_class(StorageClassSelector::Id(other.id()))
            .await
            .unwrap();
        let object = storage
            .object_store()
            .create_object(
                &resolved,
                StorageObjectCreate::new(
                    scope.scoped_name("object"),
                    collection.id(),
                    class.id(),
                    Value::Null,
                    "recovery contract",
                ),
                &context,
            )
            .await
            .unwrap()
            .into_value();
        let other_object = storage
            .object_store()
            .create_object(
                &resolved_other,
                StorageObjectCreate::new(
                    scope.scoped_name("other_object"),
                    collection.id(),
                    other.id(),
                    json!({"restored": true}),
                    "recovery contract",
                ),
                &context,
            )
            .await
            .unwrap()
            .into_value();
        let relation = storage
            .class_relation_store()
            .prepare_class_relation(
                StorageClassRelationCreate::builder(class.id(), other.id()).build(),
            )
            .await
            .unwrap();
        let relation = storage
            .class_relation_store()
            .create_class_relation(&relation, &context)
            .await
            .unwrap()
            .into_value();
        let object_relation = storage
            .object_relation_store()
            .prepare_object_relation(StorageObjectRelationCreateSelector::Explicit(
                StorageObjectRelationCreate::new(
                    object.id(),
                    other_object.id(),
                    ClassRelationId::from(relation.relation().metadata().id()),
                ),
            ))
            .await
            .unwrap();
        let _ = storage
            .object_relation_store()
            .create_object_relation(&object_relation, &context)
            .await
            .unwrap();
        let _ = storage
            .create_export_template(StorageExportTemplateCreate::new(
                collection.id(),
                scope.scoped_name("template"),
                StorageExportTemplateDefinition::new(
                    "recovery contract",
                    "text/plain",
                    "{}",
                    "fragment",
                ),
                context.clone(),
            ))
            .await
            .unwrap();
        let _ = storage
            .create_remote_target(StorageRemoteTargetCreate::new(
                collection.id(),
                scope.scoped_name("remote"),
                StorageRemoteTargetDefinition::new(
                    "recovery contract",
                    StorageRemoteTargetTransport::try_new(
                        StorageRemoteTargetHttpMethod::Get,
                        "https://example.invalid",
                        json!({}),
                        None,
                        json!({}),
                        1000,
                    )
                    .unwrap(),
                    StorageRemoteTargetPolicy::try_new(
                        None,
                        vec![StorageRemoteTargetSubjectType::Collection],
                        true,
                    )
                    .unwrap(),
                ),
                context,
            ))
            .await
            .unwrap();
        Ok(Self {
            kind,
            original,
            storage,
            collection_id: collection.id(),
            class: resolved,
            scope,
            mutation: AtomicUsize::new(0),
        })
    }
}

impl RestoreContractFixture {
    pub async fn restore_original(&self) -> Result<(), ApiError> {
        restore_snapshot(&self.storage, self.original.clone()).await
    }
    pub async fn capture(&self, include_history: bool) -> Result<StorageBackupSnapshot, ApiError> {
        let document =
            create_backup_document(&self.storage, &BackupRequest { include_history }).await?;
        Ok(StorageBackupSnapshot::try_new(
            document.state.sections,
            document.history.map(|history| history.sections),
        )
        .map_err(StorageValidationError::into_request_error)?)
    }

    pub async fn restore(&self, snapshot: StorageBackupSnapshot) -> Result<(), ApiError> {
        restore_snapshot(&self.storage, snapshot).await
    }

    pub async fn seed_terminal_artifact(
        &self,
        payload: StorageTaskCompletionPayload,
    ) -> Result<(), ApiError> {
        let task = self
            .storage
            .create_task(
                StorageTaskCreateRequest::builder(
                    payload.task_kind(),
                    PrincipalId::new(1)?,
                    json!({"input": "retained"}),
                    1,
                )
                .try_build(10)?,
            )
            .await?;
        let claim = self
            .storage
            .claim_next_task(StorageTaskLeaseDuration::from_milliseconds(60_000).unwrap())
            .await?
            .unwrap();
        assert_eq!(claim.task().id(), task.id());
        self.storage
            .complete_task(StorageTaskCompletion::new(
                StorageTaskTerminalUpdate::new(
                    claim.lease().clone(),
                    StorageTaskTerminalStatus::Succeeded,
                    StorageTaskResultCounts::try_new(1, 1, 0).unwrap(),
                ),
                StorageTaskEventInput::new("succeeded", "retained completion"),
                payload,
            ))
            .await?;
        Ok(())
    }

    /// Adapter-native failure seam; shared tests own the rollback expectations.
    pub async fn fail_restore(&self, snapshot: StorageBackupSnapshot) -> Result<(), ApiError> {
        let (id, _) = stage_and_confirm_snapshot(&self.storage, snapshot.clone())
            .await
            .expect("stage and confirm the valid rollback fixture");
        let job_id = id;
        let snapshot = match self.kind {
            StorageBackendKind::Postgres => snapshot,
            StorageBackendKind::Memory => {
                // Fail identity decoding after resources have been prepared in
                // the detached replacement. This never reaches normal staging.
                let (mut state, history) = snapshot.into_parts();
                let groups = state.get_mut(&StorageBackupStateSection::Groups).unwrap();
                let mut fields = groups[0].fields().clone();
                fields.insert("name".to_string(), Value::Null);
                groups[0] = StorageBackupRow::try_from_value(Value::Object(fields)).unwrap();
                StorageBackupSnapshot::try_new(state, history).unwrap()
            }
        };
        let request = StorageRestoreApply::new(
            job_id,
            StorageRestoreDocument::at_restore_boundary(
                StorageRestoreDocumentMetadata::new(CURRENT_BACKUP_VERSION, Utc::now(), "test"),
                snapshot,
                Utc::now(),
            ),
        );
        let result = match self.kind {
            StorageBackendKind::Postgres => {
                PostgresFaultController::failing(PostgresFaultPoint::TransactionBeforeCommit)
                    .run(self.storage.apply_restore(request))
                    .await
            }
            StorageBackendKind::Memory => self.storage.apply_restore(request).await,
        };
        let error = result
            .as_ref()
            .expect_err("injected adapter failure must be reached")
            .to_string();
        let expected = match self.kind {
            StorageBackendKind::Postgres => "transaction_before_commit",
            StorageBackendKind::Memory => "Invalid logical backup field 'name'",
        };
        assert!(
            error.contains(expected),
            "unexpected restore failure: {error}"
        );
        self.storage
            .fail_restore_and_resume(StorageRestoreFailure::new(
                job_id,
                "Injected restore failure",
            ))
            .await
            .expect("resume after the injected apply failure");
        result.map(|_| ()).map_err(ApiError::from)
    }

    pub async fn mutate(&self) -> Result<(), ApiError> {
        let iteration = self.mutation.fetch_add(1, Ordering::SeqCst);
        let context = EventContext::system();
        let _ = self
            .storage
            .collection_store()
            .update_collection(
                self.collection_id,
                StorageCollectionUpdate::new(None, Some(format!("mutation {iteration}"))),
                &context,
            )
            .await?;
        let object = self
            .storage
            .object_store()
            .create_object(
                &self.class,
                StorageObjectCreate::new(
                    self.scope.scoped_name(&format!("deleted_{iteration}")),
                    self.collection_id,
                    self.class.class().id(),
                    json!({"temporary": iteration}),
                    "deleted during recovery drill",
                ),
                &context,
            )
            .await?
            .into_value();
        let target = self
            .storage
            .object_store()
            .resolve_object(StorageObjectSelector::Ids {
                class_id: object.class_id(),
                object_id: object.id(),
            })
            .await?;
        let _ = self
            .storage
            .object_store()
            .delete_object(&target, &context)
            .await?;
        Ok(())
    }
}

async fn restore_snapshot(
    storage: &StorageHandle,
    snapshot: StorageBackupSnapshot,
) -> Result<(), ApiError> {
    let (id, capability) = stage_and_confirm_snapshot(storage, snapshot).await?;
    if !execute_confirmed_restore(storage).await? {
        return Err(ApiError::InternalServerError(
            "restore did not execute".to_string(),
        ));
    }
    if restore_status(storage, id, &capability).await?.status != RestoreJobStatus::Succeeded {
        return Err(ApiError::InternalServerError(
            "restore did not succeed".to_string(),
        ));
    }
    Ok(())
}

async fn stage_and_confirm_snapshot(
    storage: &StorageHandle,
    snapshot: StorageBackupSnapshot,
) -> Result<(RestoreJobID, String), ApiError> {
    let (state, history) = snapshot.into_parts();
    let state = BackupState { sections: state };
    let history = history.map(|sections| BackupHistory { sections });
    let manifest = BackupManifest::from_sections(&state, history.as_ref());
    let document = BackupDocument {
        backup_version: CURRENT_BACKUP_VERSION,
        created_at: Utc::now(),
        source_version: env!("CARGO_PKG_VERSION").to_string(),
        state,
        history,
        manifest,
    };
    let bytes = serde_json::to_vec(&document)?;
    let settings = RestoreSettings::new(60, bytes.len() + 1).map_err(ApiError::BadRequest)?;
    let staged = stage_restore(
        storage,
        &settings,
        RestoreStageRequest::new(
            RestoreInitiator::new(None, "test", "recovery-contract")?,
            bytes,
        )?,
    )
    .await?;
    let capability = staged.restore_capability.unwrap();
    let id = RestoreJobID::new(staged.id)?;
    confirm_restore(
        storage,
        id,
        &RestoreConfirmRequest {
            restore_capability: capability.clone(),
            sha256: staged.sha256,
            confirmation: RESTORE_CONFIRMATION_PHRASE.to_string(),
        },
    )
    .await?;
    Ok((id, capability))
}
