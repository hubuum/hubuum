use std::collections::BTreeMap;

use chrono::{Duration, Utc};
use sha2::{Digest, Sha256};

use crate::db::DbPool;
use crate::db::traits::backup::snapshot_backup_db;
use crate::db::traits::task::{TaskBackend, TaskStateUpdate};
use crate::errors::ApiError;
use crate::models::{
    BackupDocument, BackupHistory, BackupManifest, BackupRequest, BackupState,
    CURRENT_BACKUP_VERSION, NewBackupTaskOutputRecord, NewTaskEventRecord, Permissions, TaskRecord,
    TaskStatus,
};
use crate::permissions::{AppContext, PrincipalRef};
use crate::traits::AuthzSubject;

#[derive(Clone, Debug)]
pub struct BackupSettings {
    output_retention_hours: i64,
    max_active_tasks_per_user: usize,
    max_output_bytes: usize,
}

impl BackupSettings {
    pub fn new(
        output_retention_hours: i64,
        max_active_tasks_per_user: usize,
        max_output_bytes: usize,
    ) -> Result<Self, String> {
        if output_retention_hours <= 0 {
            return Err("backup output retention must be greater than zero".to_string());
        }
        if max_active_tasks_per_user == 0 {
            return Err("backup active-task limit must be greater than zero".to_string());
        }
        if max_output_bytes == 0 {
            return Err("backup output size limit must be greater than zero".to_string());
        }
        Ok(Self {
            output_retention_hours,
            max_active_tasks_per_user,
            max_output_bytes,
        })
    }

    pub fn output_retention_hours(&self) -> i64 {
        self.output_retention_hours
    }

    pub fn max_active_tasks_per_user(&self) -> usize {
        self.max_active_tasks_per_user
    }

    pub fn max_output_bytes(&self) -> usize {
        self.max_output_bytes
    }
}

fn build_manifest(state: &BackupState, history: Option<&BackupHistory>) -> BackupManifest {
    let mut item_counts = BTreeMap::new();
    for (name, rows) in &state.sections {
        item_counts.insert(name.clone(), rows.len() as i64);
    }
    if let Some(history) = history {
        for (name, rows) in &history.sections {
            item_counts.insert(format!("history.{name}"), rows.len() as i64);
        }
    }
    BackupManifest {
        item_counts,
        exclusions: vec![
            "backup_task_outputs (backup artifacts never recursively contain prior backups)"
                .to_string(),
            "authentication tokens and token scopes (credentials must be reissued after restore)"
                .to_string(),
            "class reachability cache (rebuilt by database triggers during restore)".to_string(),
            "active tasks and non-terminal event deliveries".to_string(),
            "restore control-plane tables and server instance heartbeats".to_string(),
        ],
    }
}

pub async fn create_backup_document(
    pool: &DbPool,
    request: &BackupRequest,
) -> Result<BackupDocument, ApiError> {
    let include_history = request.include_history;
    let (state, history) = snapshot_backup_db(pool, include_history).await?;
    let manifest = build_manifest(&state, history.as_ref());
    Ok(BackupDocument {
        backup_version: CURRENT_BACKUP_VERSION,
        created_at: Utc::now().naive_utc(),
        source_version: env!("CARGO_PKG_VERSION").to_string(),
        state,
        history,
        manifest,
    })
}

pub async fn execute_backup_task(
    context: &AppContext,
    task: &TaskRecord,
    user: &impl AuthzSubject,
    scopes: Option<&[Permissions]>,
    settings: &BackupSettings,
) -> Result<(), ApiError> {
    let payload = task
        .request_payload
        .clone()
        .ok_or_else(|| ApiError::BadRequest("Backup task payload is missing".to_string()))?;
    let request: BackupRequest = serde_json::from_value(payload)?;
    authorize_backup_request(context, user, scopes).await?;
    let document = create_backup_document(context, &request).await?;
    let bytes = serde_json::to_vec(&document)?;
    if bytes.len() > settings.max_output_bytes() {
        return Err(ApiError::PayloadTooLarge(format!(
            "Backup output is {} bytes, exceeding the configured {} byte limit",
            bytes.len(),
            settings.max_output_bytes()
        )));
    }
    let sha256 = Sha256::digest(&bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let byte_size = i64::try_from(bytes.len()).unwrap_or(i64::MAX);
    let expires_at = Utc::now().naive_utc() + Duration::hours(settings.output_retention_hours());
    let total_items = document.manifest.item_counts.values().copied().sum::<i64>();
    let total_items = i32::try_from(total_items).unwrap_or(i32::MAX);
    let summary = format!(
        "Full backup completed with {} logical rows ({} bytes)",
        total_items,
        bytes.len()
    );
    task.finalize_backup_with_output(
        context,
        TaskStateUpdate {
            status: TaskStatus::Succeeded,
            summary: Some(summary.clone()),
            processed_items: total_items,
            success_items: total_items,
            failed_items: 0,
            started_at: task.started_at,
            finished_at: None,
        },
        NewTaskEventRecord {
            task_id: task.id,
            event_type: TaskStatus::Succeeded.as_str().to_string(),
            message: summary,
            data: Some(serde_json::json!({
                "sha256": sha256,
                "byte_size": bytes.len(),
                "backup_kind": "full",
                "include_history": request.include_history,
            })),
        },
        NewBackupTaskOutputRecord {
            task_id: task.id,
            document: bytes,
            byte_size,
            sha256,
            output_expires_at: expires_at,
        },
    )
    .await?;
    Ok(())
}

pub(crate) async fn authorize_backup_request(
    context: &AppContext,
    user: &impl AuthzSubject,
    scopes: Option<&[Permissions]>,
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

    use rstest::rstest;

    use super::{BackupSettings, authorize_backup_request};
    use crate::errors::ApiError;
    use crate::permissions::AppContext;
    use crate::permissions::test_support::MockTreetopBackend;
    use crate::tests::{TestContext, create_test_group};

    #[tokio::test]
    async fn configured_backend_can_deny_a_sql_administrator_backup() {
        let test = TestContext::new().await;
        let context = AppContext::new(
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
        let context = AppContext::new(test.pool.get_ref().clone(), Arc::new(backend));

        authorize_backup_request(&context, &test.normal_user, None)
            .await
            .unwrap();
    }

    #[rstest]
    #[case::retention(0, 1, 1024)]
    #[case::active_tasks(24, 0, 1024)]
    #[case::output_size(24, 1, 0)]
    fn backup_settings_reject_zero_limits(
        #[case] retention_hours: i64,
        #[case] active_tasks: usize,
        #[case] output_bytes: usize,
    ) {
        assert!(BackupSettings::new(retention_hours, active_tasks, output_bytes).is_err());
    }
}
