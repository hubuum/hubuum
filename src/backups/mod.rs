use crate::models::token_scope::TokenScope;
use crate::services::authentication::ExecutionPrincipal;
use chrono::Utc;

use crate::errors::ApiError;
use crate::models::retention::FutureRetention;
use crate::models::{BackupRequest, NewTaskEventRecord, TaskResultCounts, TaskStatus};
use crate::permissions::{AppContext, PrincipalRef};
use crate::services::tasks::{ClaimedTask, TaskStateChange, complete_task};
use crate::storage::{
    StorageBackupBudget, StorageBackupTaskArtifact, StorageTaskCompletionPayload,
};
use crate::traits::AuthzSubject;

mod producer;
pub use producer::BackupArtifactProducer;

#[derive(Clone, Debug)]
pub struct BackupSettings {
    output_retention: FutureRetention,
    budget: StorageBackupBudget,
}

impl BackupSettings {
    pub fn new(output_retention_hours: i64, budget: StorageBackupBudget) -> Result<Self, String> {
        let output_retention =
            FutureRetention::from_hours(output_retention_hours, "backup output retention")?;

        Ok(Self {
            output_retention,
            budget,
        })
    }

    fn output_expires_at(
        &self,
        now: chrono::NaiveDateTime,
    ) -> Result<chrono::NaiveDateTime, ApiError> {
        self.output_retention
            .expires_at(now)
            .map_err(ApiError::BadRequest)
    }

    pub fn output_retention_hours(&self) -> i64 {
        self.output_retention.hours()
    }

    pub fn budget(&self) -> StorageBackupBudget {
        self.budget
    }
}

pub(crate) async fn execute_backup_task(
    context: &AppContext,
    task: &ClaimedTask,
    user: &ExecutionPrincipal,
    scopes: Option<&TokenScope>,
    settings: &BackupSettings,
) -> Result<TaskStatus, ApiError> {
    let payload = task
        .request_payload
        .clone()
        .ok_or_else(|| ApiError::BadRequest("Backup task payload is missing".to_string()))?;
    let request: BackupRequest = serde_json::from_value(payload)?;
    let availability = settings
        .output_retention
        .output_availability()
        .map_err(ApiError::BadRequest)?;
    let expires_at = settings.output_expires_at(Utc::now().naive_utc())?;
    let output = crate::services::artifacts::TaskOutput::begin(
        context,
        task,
        settings.budget().max_bytes(),
        settings.budget().max_bytes() as u64,
        expires_at.and_utc(),
    )
    .await?;
    let result = async {
        let producer = BackupArtifactProducer::capture(
            context,
            user,
            scopes,
            request.include_history,
            settings.budget(),
        )
        .await?;
        crate::tasks::control::checkpoint()?;
        let total_items = producer.summary().total_items;
        let (proof, _) = producer
            .stage(
                output.store.as_ref(),
                output.identity.clone(),
                output.limits,
            )
            .await
            .map_err(crate::services::artifacts::error)?;
        crate::tasks::control::checkpoint()?;
        authorize_backup_request(context, user, scopes).await?;
        let byte_size = proof.manifest().byte_size();
        let sha256 = proof
            .manifest()
            .stream_digest()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        let total_items = i32::try_from(total_items).unwrap_or(i32::MAX);
        let summary = format!(
            "Full backup completed with {} logical rows ({} bytes)",
            total_items, byte_size
        );
        let finished = complete_task(
            context,
            task,
            TaskStateChange::new(
                TaskStatus::Succeeded,
                TaskResultCounts::from_outcomes(total_items, 0)?,
            )
            .summary(summary.clone())
            .started_at(task.started_at),
            NewTaskEventRecord {
                event_type: TaskStatus::Succeeded.as_str().to_string(),
                message: summary,
                data: Some(serde_json::json!({
                    "sha256": sha256,
                    "byte_size": byte_size,
                    "backup_kind": "full",
                    "include_history": request.include_history,
                })),
            },
            StorageTaskCompletionPayload::Backup(StorageBackupTaskArtifact::verified(
                proof,
                availability,
            )),
        )
        .await?;
        TaskStatus::from_db(&finished.status)
    }
    .await;
    if result.is_err() || matches!(result, Ok(TaskStatus::Cancelled)) {
        let _ = output.store.abort(&output.identity).await;
    }
    result
}

pub(crate) async fn authorize_backup_request(
    context: &AppContext,
    user: &impl AuthzSubject,
    scopes: Option<&TokenScope>,
) -> Result<(), ApiError> {
    let principal = PrincipalRef::load(context, user).await?;
    if scopes.is_some() || !context.permission_backend().is_admin(&principal).await? {
        return Err(ApiError::Forbidden(
            "A full backup requires an unscoped administrator token".to_string(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::{BackupSettings, authorize_backup_request};
    use crate::errors::ApiError;
    use crate::permissions::test_support::MockTreetopBackend;
    use crate::storage::StorageBackupBudget;
    use crate::tests::{TestContext, create_test_group};

    #[tokio::test]
    async fn configured_backend_can_deny_a_sql_administrator_backup() {
        let test = TestContext::new().await;
        let context = crate::tests::app_context_with_permission_backend(
            test.pool.get_ref().clone(),
            Arc::new(MockTreetopBackend::new()),
        );

        let result = authorize_backup_request(&context, &test.admin_user, None).await;

        assert!(matches!(result, Err(ApiError::Forbidden(_))));
    }

    #[tokio::test]
    async fn configured_backend_can_allow_a_non_sql_administrator_backup() {
        let test = TestContext::new().await;
        let policy_group = create_test_group(&test.pool).await;
        policy_group
            .add_member_without_events(&test.pool, &test.normal_user)
            .await
            .unwrap();
        let backend = MockTreetopBackend::new();
        backend.add_admin_rule(policy_group.id);
        let context = crate::tests::app_context_with_permission_backend(
            test.pool.get_ref().clone(),
            Arc::new(backend),
        );

        authorize_backup_request(&context, &test.normal_user, None)
            .await
            .unwrap();
    }

    #[test]
    fn backup_settings_reject_zero_retention() {
        let retention_hours = 0;
        let output_bytes = 1024;
        assert!(
            BackupSettings::new(
                retention_hours,
                StorageBackupBudget::new(output_bytes, 100).unwrap()
            )
            .is_err()
        );
    }

    #[test]
    fn backup_settings_reject_unrepresentable_retention() {
        let error = BackupSettings::new(i64::MAX, StorageBackupBudget::new(1024, 100).unwrap())
            .unwrap_err();

        assert_eq!(
            error,
            "backup output retention is outside the supported duration range"
        );
    }
}
