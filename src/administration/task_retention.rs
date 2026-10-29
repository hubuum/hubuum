use std::time::Duration;

use chrono::{DateTime, Utc};
use clap::{Args, ValueEnum};
use hubuum_domain::TaskIdentity;
use hubuum_storage_core::{TaskHoldId, TaskHoldReason};
use uuid::Uuid;

use crate::errors::ApiError;
use crate::services::deletion_recovery::TaskRetentionAdminAction;

#[derive(Clone, Copy, Debug, ValueEnum)]
enum Command {
    Status,
    Preview,
    Enable,
    SetPolicy,
    Hold,
    ReleaseHold,
    Run,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum HoldReason {
    Legal,
    Incident,
    Operator,
}

/// Administrator-only controls; these are deliberately absent from server settings.
#[derive(Args, Default)]
pub(super) struct TaskRetentionAdminOptions {
    /// Inspect or change protected task retention; run performs one bounded cleanup pass
    #[arg(long, value_enum, requires_all = ["control_recovery_directory", "control_recovery_key"], conflicts_with_all = ["control_checkpoint", "verify_control_recovery", "recover_control", "reapply_control", "delete_object_history_version", "backup", "restore", "restore_executor", "verify_backup", "reconcile_query_usage", "database_ready", "reset_password", "audit_templates", "export_template_health", "token_key_status", "database_role_setup_sql", "database_role_grants_sql", "check_database_privileges"])]
    #[cfg_attr(feature = "embedded-migrations", arg(conflicts_with_all = ["migrate", "migration_mode"]))]
    task_retention: Option<Command>,

    /// Current confirmation printed by preview; acknowledges finite legacy deduplication
    #[arg(
        long,
        requires = "task_retention",
        required_if_eq("task_retention", "enable")
    )]
    task_retention_confirmation: Option<String>,

    /// Fixed deduplication seconds after original terminal completion for new admissions
    #[arg(
        long,
        requires = "task_retention",
        required_if_eq("task_retention", "set-policy")
    )]
    task_retention_deduplication_seconds: Option<u64>,

    /// New tasks' detail lifetime after terminal completion; omission disables detail expiry
    #[arg(long, requires = "task_retention")]
    task_retention_details_seconds: Option<u64>,

    /// Stable hold UUID; reuse it when retrying hold creation or release
    #[arg(long, requires = "task_retention", required_if_eq_any = [("task_retention", "hold"), ("task_retention", "release-hold")])]
    task_hold_id: Option<Uuid>,

    /// Stable task UUID protected by the hold
    #[arg(
        long,
        requires = "task_retention",
        required_if_eq("task_retention", "hold")
    )]
    task_hold_task: Option<TaskIdentity>,

    /// Optional fixed RFC 3339 hold expiration; omission retains the hold until explicit release
    #[arg(long, requires = "task_hold_task")]
    task_hold_expires_at: Option<DateTime<Utc>>,

    /// Why the task must be retained
    #[arg(long, value_enum, requires = "task_hold_task")]
    task_hold_reason: Option<HoldReason>,
}

impl TaskRetentionAdminOptions {
    pub(super) fn selected(&self) -> bool {
        self.task_retention.is_some()
    }

    pub(super) fn action(&self) -> Result<Option<TaskRetentionAdminAction>, ApiError> {
        let Some(command) = self.task_retention else {
            return Ok(None);
        };
        let invalid = |message: &str| ApiError::BadRequest(message.to_owned());
        if !matches!(command, Command::Enable) && self.task_retention_confirmation.is_some() {
            return Err(invalid(
                "--task-retention-confirmation only applies to enable",
            ));
        }
        if !matches!(command, Command::SetPolicy)
            && (self.task_retention_deduplication_seconds.is_some()
                || self.task_retention_details_seconds.is_some())
        {
            return Err(invalid("Task retention durations only apply to set-policy"));
        }
        if !matches!(command, Command::Hold | Command::ReleaseHold) && self.task_hold_id.is_some()
            || !matches!(command, Command::Hold) && self.task_hold_task.is_some()
        {
            return Err(invalid(
                "Task hold arguments only apply to hold or release-hold",
            ));
        }
        Ok(Some(match command {
            Command::Status => TaskRetentionAdminAction::Status,
            Command::Preview => TaskRetentionAdminAction::Preview,
            Command::Enable => TaskRetentionAdminAction::Enable {
                confirmation: self
                    .task_retention_confirmation
                    .clone()
                    .ok_or_else(|| invalid("Activation requires the preview confirmation"))?,
            },
            Command::SetPolicy => TaskRetentionAdminAction::SetPolicy {
                deduplication: Duration::from_secs(
                    self.task_retention_deduplication_seconds
                        .ok_or_else(|| invalid("Policy requires a deduplication window"))?,
                ),
                details: self.task_retention_details_seconds.map(Duration::from_secs),
            },
            Command::Hold => TaskRetentionAdminAction::Hold {
                id: TaskHoldId::new(
                    self.task_hold_id
                        .ok_or_else(|| invalid("Hold requires a stable identity"))?,
                )
                .map_err(|error| invalid(&error.to_string()))?,
                task: self
                    .task_hold_task
                    .ok_or_else(|| invalid("Hold requires a stable task identity"))?,
                expires_at: self.task_hold_expires_at,
                reason: match self.task_hold_reason.unwrap_or(HoldReason::Operator) {
                    HoldReason::Legal => TaskHoldReason::Legal,
                    HoldReason::Incident => TaskHoldReason::Incident,
                    HoldReason::Operator => TaskHoldReason::Operator,
                },
            },
            Command::ReleaseHold => TaskRetentionAdminAction::ReleaseHold {
                id: TaskHoldId::new(
                    self.task_hold_id
                        .ok_or_else(|| invalid("Release requires a stable hold identity"))?,
                )
                .map_err(|error| invalid(&error.to_string()))?,
            },
            Command::Run => TaskRetentionAdminAction::Run,
        }))
    }
}

#[cfg(test)]
mod tests {
    use crate::administration::AdminCli;
    use clap::Parser;
    use rstest::rstest;

    fn args(command: &str) -> Vec<String> {
        [
            "hubuum-admin",
            "--task-retention",
            command,
            "--control-recovery-directory",
            "/independent/control",
            "--control-recovery-key",
            "/independent/control.key",
        ]
        .into_iter()
        .map(str::to_owned)
        .collect()
    }

    #[rstest]
    #[case("enable")]
    #[case("set-policy")]
    #[case("hold")]
    #[case("release-hold")]
    fn task_retention_mutations_require_their_explicit_inputs(#[case] command: &str) {
        assert!(AdminCli::try_parse_from(args(command)).is_err());
    }

    #[rstest]
    #[case("status")]
    #[case("preview")]
    #[case("run")]
    fn task_retention_commands_require_independent_repository_configuration(#[case] command: &str) {
        assert!(AdminCli::try_parse_from(["hubuum-admin", "--task-retention", command]).is_err());
        assert!(
            AdminCli::try_parse_from(args(command))
                .unwrap()
                .task_retention
                .action()
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn retention_policy_cannot_accidentally_ignore_a_detail_duration() {
        let mut arguments = args("status");
        arguments.extend(["--task-retention-details-seconds".into(), "60".into()]);
        assert!(
            AdminCli::try_parse_from(arguments)
                .unwrap()
                .task_retention
                .action()
                .is_err()
        );
    }
}
