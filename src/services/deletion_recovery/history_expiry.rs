//! Gated expiry task execution. Admission and exact removal receipts are
//! independently protected before removal and task completion respectively.
use std::future::Future;
use std::time::Duration;

use hubuum_domain::PrincipalId;
use hubuum_storage_core::{
    ApplicationFence, HistoryExpiryOrigin, HistoryExpiryTaskAdmission,
    HistoryExpiryTaskAuthorization, HistoryExpiryTaskPayload, HistoryExpiryTaskRequest,
    HistoryExpiryTaskSubmission, HistoryLifecycleScan, HistoryLifecycleStorage,
    HistoryPolicyBinding, HistoryPolicyReference, LogicalIdentityStorage, StorageIdentitySubject,
    StorageIdentityTarget, StorageTaskCompletionPayload, StorageTaskWaitRequest,
    TaskExecutionStorage,
};
use hubuum_task_core::TaskExecutionContext;

use crate::errors::ApiError;
use crate::extractors::AdminAccess;
use crate::models::{NewTaskEventRecord, TaskResultCounts, TaskStatus};
use crate::permissions::{AppContext, require_unscoped_runtime_admin};
use crate::services::authentication::ExecutionPrincipal;
use crate::services::tasks::{ClaimedTask, TaskStateChange, complete_task};
use crate::storage::storage_handle;

/// Authorization is evaluated before either creation or coalescing. This path
/// requires original OnlineV1 admission just like the executor.
pub(crate) async fn submit_manual(
    context: &AppContext,
    admin: &AdminAccess,
    scan: HistoryLifecycleScan,
) -> Result<HistoryExpiryTaskAdmission, ApiError> {
    let storage = storage_handle(context);
    let (principal, snapshot) = crate::tasks::history::run(&storage, async {
        let principal = super::history_policy::principal(context, admin).await?;
        Ok((principal, storage.capture_history_lifecycle().await?))
    })
    .await?;
    let request = HistoryExpiryTaskSubmission::manual(
        scan,
        ApplicationFence::new(
            snapshot.fence().incarnation(),
            snapshot.fence().deletion_revision().value() as i64,
        )
        .map_err(|error| ApiError::BadRequest(error.to_string()))?,
        PrincipalId::new(admin.user.id)?,
        principal,
    );
    Ok(storage.submit_history_expiry_task(request).await?)
}

/// Bounded round-robin admission over retained expiry policy roots. A newer
/// prospective override does not erase an older closure's governing rule.
/// The shared adapter path coalesces with manual work and retained cleanup.
pub(crate) async fn submit_periodic(context: &AppContext) -> Result<(), ApiError> {
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    const PER_PASS: usize = 16;
    let storage = storage_handle(context);
    let snapshot = crate::tasks::history::run(&storage, async {
        Ok(storage.capture_history_lifecycle().await?)
    })
    .await?;
    let mut roots = BTreeMap::new();
    for record in snapshot.control().authority().policies().records() {
        if matches!(
            record.proposal().binding(),
            HistoryPolicyBinding::Expire { .. }
        ) {
            roots.insert(record.proposal().key(), record);
        }
    }
    let roots: Vec<_> = roots.into_values().collect();
    if roots.is_empty() {
        return Ok(());
    }
    let start = NEXT.fetch_add(PER_PASS, Ordering::Relaxed);
    let application = ApplicationFence::new(
        snapshot.fence().incarnation(),
        snapshot.fence().deletion_revision().value() as i64,
    )
    .map_err(|error| ApiError::InternalServerError(error.to_string()))?;
    for offset in 0..roots.len().min(PER_PASS) {
        crate::tasks::control::checkpoint()?;
        let root = roots[start.wrapping_add(offset) % roots.len()];
        let scan = HistoryLifecycleScan::new(root.proposal().key(), 1000)
            .map_err(|error| ApiError::InternalServerError(error.to_string()))?;
        let request = HistoryExpiryTaskSubmission::periodic(scan, application, root)
            .map_err(|error| ApiError::InternalServerError(error.to_string()))?;
        if let Err(error) = storage.submit_history_expiry_task(request).await {
            tracing::warn!(message = "Periodic history admission remains pending", key = ?root.proposal().key(), error = %error);
        }
    }
    Ok(())
}

/// Called only by an expiry-capable qualified worker. The retained journal is
/// bounded (256 batches), and each adapter serializes one batch with its owner.
pub(crate) async fn recover_pending(context: &AppContext) -> Result<(), ApiError> {
    let storage = storage_handle(context);
    let snapshot = crate::tasks::history::run(&storage, async {
        Ok(storage.capture_history_lifecycle().await?)
    })
    .await?;
    for intent in snapshot.control().authority().expiry().intents() {
        crate::tasks::control::checkpoint()?;
        if let Err(error) = storage
            .recover_history_expiry_task(intent.manifest().identity())
            .await
        {
            tracing::warn!(message = "History expiry recovery remains pending", batch = %intent.manifest().identity(), error = %error);
        }
    }
    Ok(())
}

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
    let execution = crate::tasks::control::execution_context().ok_or_else(|| {
        ApiError::InternalServerError("History expiry requires admitted task execution".into())
    })?;
    loop {
        crate::tasks::control::checkpoint()?;
        let checkpoint = checkpoint_with_retry(&execution, || async {
            let approval =
                crate::tasks::history::run(&storage, authorize_selection(context, task, &payload))
                    .await;
            let request = HistoryExpiryTaskRequest::new(
                task.lease().clone(),
                approval.as_ref().ok().cloned(),
            );
            storage
                .process_history_expiry_task(request)
                .await
                .map_err(|error| approval.err().unwrap_or_else(|| error.into()))
        })
        .await?;
        if checkpoint.admitted() {
            let encoded = checkpoint
                .encode()
                .map_err(|error| ApiError::InternalServerError(error.to_string()))?;
            let protected = crate::tasks::history::run(
                &storage,
                super::history_policy::protect_expiry(context, &checkpoint),
            )
            .await;
            let result = match protected {
                Ok(protection) => storage
                    .process_history_expiry_task(
                        HistoryExpiryTaskRequest::new(task.lease().clone(), None)
                            .with_protection(protection),
                    )
                    .await
                    .map_err(ApiError::from),
                Err(error) => Err(error),
            };
            match result {
                Ok(_) => {
                    tokio::task::yield_now().await;
                    continue;
                }
                Err(error) => {
                    tracing::warn!(message = "History expiry batch remains pending", task_id = task.id, error = %error)
                }
            }
            return crate::tasks::history::run(&storage, async {
                storage
                    .wait_task(StorageTaskWaitRequest::new(task.lease().clone(), encoded))
                    .await?;
                Ok(TaskStatus::Waiting)
            })
            .await;
        }
        if checkpoint.finished() {
            let summary = if checkpoint.removed_count() == 0 {
                "History expiry scan pass finished without an admitted batch"
            } else {
                "History expiry bounded batch completed with independently protected receipts"
            };
            let finished = crate::tasks::history::run(
                &storage,
                complete_task(
                    context,
                    task,
                    TaskStateChange::new(
                        TaskStatus::Succeeded,
                        TaskResultCounts::from_outcomes(checkpoint.removed_count(), 0)?,
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

/// Contention and publication fencing can invalidate an otherwise valid turn.
/// Retry the entire authorization/checkpoint attempt under the original deadline.
async fn checkpoint_with_retry<T, F, Fut>(
    execution: &TaskExecutionContext,
    mut attempt: F,
) -> Result<T, ApiError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, ApiError>>,
{
    loop {
        execution.check()?;
        match attempt().await {
            Err(error @ (ApiError::Conflict(_) | ApiError::ServiceUnavailable(_))) => {
                tracing::warn!(message = "History expiry checkpoint remains pending", error = %error);
                tokio::time::sleep(execution.remaining().min(Duration::from_millis(25))).await;
            }
            result => return result,
        }
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
        HistoryExpiryOrigin::Scheduled {
            policy: Some(root),
            initiator,
        } => {
            let snapshot = storage_handle(context).capture_history_lifecycle().await?;
            if !snapshot
                .control()
                .authority()
                .policies()
                .records()
                .iter()
                .any(|record| {
                    HistoryPolicyReference::from(record) == *root
                        && Some(record.principal()) == *initiator
                        && matches!(
                            record.proposal().binding(),
                            HistoryPolicyBinding::Expire { .. }
                        )
                })
            {
                return Err(ApiError::Forbidden(
                    "Periodic expiry policy root is unavailable".into(),
                ));
            }
            Ok(HistoryExpiryTaskAuthorization::periodic(root.clone()))
        }
        HistoryExpiryOrigin::Scheduled { policy: None, .. } => {
            Ok(HistoryExpiryTaskAuthorization::scheduled())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::ready;
    use std::sync::Arc;

    use chrono::Utc;
    use hubuum_domain::{PrincipalIdentity, TaskId};
    use hubuum_storage_core::{
        ApplicationFence, HistoryLifecycleScan, HistoryPolicyKey, HistoryPolicyScope,
        StorageIdentityHistoryKind, StorageTask, StorageTaskClaim, StorageTaskClaimToken,
        StorageTaskKind, StorageTaskLease, StorageTaskScopeSnapshot, StorageTaskStatus,
    };
    use hubuum_task_core::TaskStopReason;
    use rstest::rstest;
    use uuid::Uuid;

    #[rstest]
    #[case::busy(false)]
    #[case::unavailable(true)]
    #[tokio::test]
    async fn transient_checkpoint_failure_repeats_the_attempt_until_it_succeeds(
        #[case] unavailable: bool,
    ) {
        let (execution, _) = TaskExecutionContext::new(Duration::from_secs(10)).unwrap();
        let mut attempts = 0;
        let completed = checkpoint_with_retry(&execution, || {
            attempts += 1;
            ready(if attempts < 3 {
                Err(if unavailable {
                    ApiError::ServiceUnavailable("publication changed".into())
                } else {
                    ApiError::Conflict("task row is busy".into())
                })
            } else {
                Ok(attempts)
            })
        })
        .await
        .unwrap();
        assert_eq!(completed, 3);
    }

    #[tokio::test]
    async fn checkpoint_retries_cannot_extend_the_original_execution_deadline() {
        let (execution, _) = TaskExecutionContext::new(Duration::from_millis(50)).unwrap();
        let result = tokio::time::timeout(
            Duration::from_secs(5),
            checkpoint_with_retry::<(), _, _>(&execution, || {
                ready(Err(ApiError::Conflict("task row is busy".into())))
            }),
        )
        .await
        .expect("Retries must stop at their original deadline");
        assert!(matches!(
            result,
            Err(ApiError::TaskStopped(TaskStopReason::DeadlineExceeded))
        ));
    }

    #[tokio::test]
    async fn cancellation_stops_checkpoint_retry_before_another_attempt() {
        let (execution, stop) = TaskExecutionContext::new(Duration::from_secs(10)).unwrap();
        let mut attempts = 0;
        let result = checkpoint_with_retry::<(), _, _>(&execution, || {
            attempts += 1;
            stop.request_stop(TaskStopReason::Cancelled);
            ready(Err(ApiError::Conflict("task row is busy".into())))
        })
        .await;
        assert!(matches!(
            result,
            Err(ApiError::TaskStopped(TaskStopReason::Cancelled))
        ));
        assert_eq!(attempts, 1);
    }

    #[tokio::test]
    async fn checkpoint_authorization_denial_is_not_retried() {
        let (execution, _) = TaskExecutionContext::new(Duration::from_secs(10)).unwrap();
        let mut attempts = 0;
        let result = checkpoint_with_retry::<(), _, _>(&execution, || {
            attempts += 1;
            ready(Err(ApiError::Forbidden("administrator revoked".into())))
        })
        .await;
        assert!(matches!(result, Err(ApiError::Forbidden(_))));
        assert_eq!(attempts, 1);
    }

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
        if allowed {
            assert!(result.is_ok(), "{mode}: {result:?}");
        } else {
            assert!(
                matches!(result, Err(ApiError::Forbidden(_))),
                "{mode}: {result:?}"
            );
        }
    }
}
