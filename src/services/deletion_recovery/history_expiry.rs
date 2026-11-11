//! Gated expiry task execution. Exact admission is durable before independent
//! protection; the pending batch waits for a future qualified removal phase.
use hubuum_domain::PrincipalId;
use hubuum_storage_core::{
    HistoryExpiryOrigin, HistoryExpiryTaskAuthorization, HistoryExpiryTaskPayload,
    HistoryExpiryTaskRequest, HistoryLifecycleStorage, LogicalIdentityStorage,
    StorageIdentitySubject, StorageIdentityTarget, StorageTaskCompletionPayload,
    StorageTaskWaitRequest, TaskExecutionStorage,
};

use crate::errors::ApiError;
use crate::models::{NewTaskEventRecord, TaskResultCounts, TaskStatus};
use crate::permissions::{AppContext, require_unscoped_runtime_admin};
use crate::services::authentication::ExecutionPrincipal;
use crate::services::tasks::{ClaimedTask, TaskStateChange, complete_task};
use crate::storage::storage_handle;

pub(crate) async fn execute(
    context: &AppContext,
    task: &ClaimedTask,
) -> Result<TaskStatus, ApiError> {
    let storage = storage_handle(context);
    let payload = HistoryExpiryTaskPayload::decode(
        task.request_payload
            .as_ref()
            .ok_or_else(|| ApiError::BadRequest("History expiry task payload is absent".into()))?,
    )
    .map_err(|error| ApiError::BadRequest(error.to_string()))?;
    loop {
        crate::tasks::control::checkpoint()?;
        let approval =
            crate::tasks::history::run(&storage, authorize_selection(context, task, &payload))
                .await;
        let request =
            HistoryExpiryTaskRequest::new(task.lease().clone(), approval.as_ref().ok().cloned());
        let checkpoint = match storage.process_history_expiry_task(request).await {
            Ok(checkpoint) => checkpoint,
            Err(error) => return Err(approval.err().unwrap_or_else(|| error.into())),
        };
        if checkpoint.admitted() {
            let encoded = checkpoint
                .encode()
                .map_err(|error| ApiError::InternalServerError(error.to_string()))?;
            return crate::tasks::history::run(&storage, async {
                // Always join publication/read-back before releasing this
                // finite phase. A failed independent write leaves the exact
                // batch pending; neither failure nor waiting removes history.
                if let Err(error) = super::history_policy::protect_expiry(context, &checkpoint).await {
                    tracing::warn!(message = "History expiry awaits independent recovery protection", task_id = task.id, error = %error);
                }
                storage.wait_task(StorageTaskWaitRequest::new(task.lease().clone(), encoded)).await?;
                Ok(TaskStatus::Waiting)
            }).await;
        }
        if checkpoint.pass_finished() {
            let summary = "History expiry scan pass finished without an admitted batch";
            let finished = crate::tasks::history::run(
                &storage,
                complete_task(
                    context,
                    task,
                    TaskStateChange::new(
                        TaskStatus::Succeeded,
                        TaskResultCounts::from_outcomes(0, 0)?,
                    )
                    .summary(summary),
                    NewTaskEventRecord {
                        event_type: "succeeded".into(),
                        message: summary.into(),
                        data: None,
                    },
                    StorageTaskCompletionPayload::HistoryExpiry,
                ),
            )
            .await?;
            return TaskStatus::from_db(&finished.status);
        }
        tokio::task::yield_now().await;
    }
}

async fn authorize_selection(
    context: &AppContext,
    task: &ClaimedTask,
    payload: &HistoryExpiryTaskPayload,
) -> Result<HistoryExpiryTaskAuthorization, ApiError> {
    if task.submitted_token_scoped {
        return Err(ApiError::Forbidden(
            "History expiry requires unscoped authority".into(),
        ));
    }
    match payload.origin() {
        HistoryExpiryOrigin::Manual {
            principal: original,
        } => {
            let submitter = task.submitted_by.ok_or_else(|| {
                ApiError::Forbidden(
                    "History expiry initiating principal is no longer available".into(),
                )
            })?;
            let principal = ExecutionPrincipal::resolve(context, submitter).await?;
            require_unscoped_runtime_admin(context, &principal, false).await?;
            let evidence = storage_handle(context)
                .resolve_logical_identity(StorageIdentityTarget::Principal(PrincipalId::new(
                    submitter,
                )?))
                .await?;
            if !matches!(evidence.subject(), Some(StorageIdentitySubject::Principal { identity })
                if identity.known_identity() == Some(*original))
            {
                return Err(ApiError::Forbidden(
                    "History expiry initiating principal changed".into(),
                ));
            }
            Ok(HistoryExpiryTaskAuthorization::manual(*original))
        }
        HistoryExpiryOrigin::Scheduled { .. } => Ok(HistoryExpiryTaskAuthorization::scheduled()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use chrono::Utc;
    use hubuum_domain::{PrincipalIdentity, TaskId};
    use hubuum_storage_core::{
        ApplicationFence, HistoryLifecycleScan, HistoryPolicyKey, HistoryPolicyScope,
        StorageIdentityHistoryKind, StorageTask, StorageTaskClaim, StorageTaskClaimToken,
        StorageTaskKind, StorageTaskLease, StorageTaskScopeSnapshot, StorageTaskStatus,
    };
    use rstest::rstest;
    use uuid::Uuid;

    use crate::permissions::LocalPermissionBackend;
    use crate::permissions::test_support::MockTreetopBackend;
    use crate::storage::StorageHandle;

    #[rstest]
    #[case::current_admin("allowed", true)]
    #[case::configured_backend_denial("denied", false)]
    #[case::different_original_principal("identity", false)]
    #[case::scoped_submission("scoped", false)]
    #[case::deleted_submitter("deleted", false)]
    #[actix_web::test]
    async fn manual_selection_requires_current_configured_authority(
        #[case] mode: &str,
        #[case] allowed: bool,
    ) {
        let storage = StorageHandle::memory();
        let context = AppContext::new(
            storage.clone(),
            Arc::new(LocalPermissionBackend::new(storage.clone(), "admin".into())),
        );
        let principal = PrincipalId::new(1).unwrap();
        let evidence = storage
            .resolve_logical_identity(StorageIdentityTarget::Principal(principal))
            .await
            .unwrap();
        let original = match evidence.subject().unwrap() {
            StorageIdentitySubject::Principal { identity } => identity.known_identity().unwrap(),
            _ => unreachable!(),
        };
        let context = if mode == "denied" {
            AppContext::new(storage, Arc::new(MockTreetopBackend::new()))
        } else {
            context
        };
        let payload = HistoryExpiryTaskPayload::new(
            HistoryLifecycleScan::new(
                HistoryPolicyKey::new(
                    StorageIdentityHistoryKind::Object,
                    HistoryPolicyScope::Deployment,
                )
                .unwrap(),
                10,
            )
            .unwrap(),
            HistoryExpiryOrigin::Manual {
                principal: if mode == "identity" {
                    PrincipalIdentity::generate()
                } else {
                    original
                },
            },
            ApplicationFence::new(Uuid::new_v4(), 0).unwrap(),
        );
        let now = Utc::now();
        let id = TaskId::new(1).unwrap();
        let stored = StorageTask::builder(
            id,
            StorageTaskKind::HistoryExpiry,
            StorageTaskStatus::Running,
            now,
            now,
        )
        .submitted_by((mode != "deleted").then_some(principal))
        .scope_snapshot(StorageTaskScopeSnapshot::new(
            None,
            mode == "scoped",
            serde_json::json!([]),
        ))
        .started_at(Some(now))
        .lease_expires_at(Some(now + chrono::Duration::minutes(1)))
        .try_build()
        .unwrap();
        let task = ClaimedTask::from_storage(
            StorageTaskClaim::try_new(
                stored,
                StorageTaskLease::new(id, StorageTaskClaimToken::new("expiry-authorization-test")),
            )
            .unwrap(),
        )
        .unwrap();
        let result = authorize_selection(&context, &task, &payload).await;
        assert_eq!(result.is_ok(), allowed, "{mode}: {result:?}");
    }
}
