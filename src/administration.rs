use crate::config::{ControlRecoveryOptions, SchemaValidationOptions};
use clap::{CommandFactory, FromArgMatches, Parser, ValueEnum};
use hubuum_domain::ClassId;
use hubuum_domain::{JsonSchemaLimits, ResourceIdentity, VersionIdentity};
use hubuum_storage_core::{
    DeletionControlStorage, ExecutionStorage, StorageBackupBudget, StorageRecordIdentity,
    StorageRestoreArtifact, StorageRestoreIdentityContext,
};
use serde::Serialize;
use std::collections::BTreeMap;
use std::ffi::OsString;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use uuid::Uuid;

use crate::config::{
    CommandLineDatabaseUrl, DEFAULT_BACKUP_MAX_CAPTURE_ROWS, DEFAULT_BACKUP_MAX_OUTPUT_BYTES,
    DEFAULT_DB_POOL_ACQUIRE_TIMEOUT_MS, DEFAULT_DB_STATEMENT_TIMEOUT_MS,
    DEFAULT_EXPORT_TEMPLATE_FUEL, DEFAULT_EXPORT_TEMPLATE_RECURSION_LIMIT,
    DEFAULT_RESTORE_MAX_UPLOAD_BYTES, DEFAULT_RESTORE_STAGE_RETENTION_MINUTES,
    DEFAULT_TOKEN_LIFETIME_HOURS, DatabaseRoleMode, SecretSourceOptions, token_hash_key_ring,
};
#[cfg(feature = "embedded-migrations")]
use crate::errors::EXIT_CODE_DATABASE_ERROR;
use crate::errors::{ApiError, EXIT_CODE_CONFIG_ERROR, fatal_error};
use crate::logger;
use crate::models::{
    ExportContentType, RESTORE_CONFIRMATION_PHRASE, RestoreConfirmRequest, RestoreInitiator,
    RestoreJobID, RestoreStageRequest,
};
use crate::restores::{
    BackupVerificationReport, RestoreSettings, confirm_restore,
    execute_confirmed_restore_with_recovery, restore_status, stage_restore,
};
use crate::secrets::{self, DatabaseCredential};
use crate::services::identity as identity_service;
use crate::services::operational_administration as operational_service;
#[cfg(feature = "embedded-migrations")]
use crate::storage::reset_disposable_restore_database;
use crate::storage::{
    OperationalStateStorage, RestoreStorage, StorageBackendKind, StorageDatabasePrivilegeReport,
    StorageDatabaseRole, StorageDatabaseRoleNames, StorageHandle, StorageSettings,
    StorageTokenKeyUsage, StorageTokenObservation, TokenStorage, initialize_storage,
    inspect_storage_database_privileges, storage_database_role_grants_sql,
    storage_database_role_setup_sql,
};
#[cfg(feature = "embedded-migrations")]
use crate::storage::{
    inspect_storage_migration_mode, prepare_disposable_restore_database, run_storage_migrations,
};
use crate::utilities::auth::generate_random_password;
use crate::utilities::exporting::validate_template_sources_with_limits;
use crate::utilities::is_valid_log_level;

const DEFAULT_DATABASE_OWNER_ROLE: &str = "hubuum_owner";
const DEFAULT_DATABASE_MIGRATOR_ROLE: &str = "hubuum_migrator";
const DEFAULT_DATABASE_RUNTIME_ROLE: &str = "hubuum_runtime";

fn parse_query_usage_class(value: &str) -> Result<ClassId, String> {
    let id = value.parse::<i32>().map_err(|error| error.to_string())?;
    ClassId::new(id).map_err(|error| error.to_string())
}

fn parse_deletion_intent(value: &str) -> Result<Uuid, String> {
    let identity = value.parse::<Uuid>().map_err(|error| error.to_string())?;
    if identity.is_nil() {
        return Err("Deletion intent must be a non-nil UUID".into());
    }
    Ok(identity)
}

#[derive(Parser)]
#[command(
    author = "Terje Kvernes <terje@kvernes.no>",
    version = env!("CARGO_PKG_VERSION"),
    about = "Admin CLI for Hubuum",
    long_about = None
)]
struct AdminCli {
    #[command(flatten)]
    control_recovery: crate::config::ControlRecoveryOptions,

    /// Publish independent deletion recovery evidence (does not execute deletion)
    #[arg(
        long,
        value_name = "DIRECTORY",
        requires = "control_checkpoint_key",
        conflicts_with_all = ["verify_control_recovery", "backup", "restore", "restore_executor", "verify_backup", "reconcile_query_usage", "database_ready", "reset_password", "audit_templates", "export_template_health", "token_key_status", "database_role_setup_sql", "database_role_grants_sql", "check_database_privileges"]
    )]
    #[cfg_attr(feature = "embedded-migrations", arg(conflicts_with_all = ["migrate", "migration_mode"]))]
    control_checkpoint: Option<PathBuf>,

    /// Verify that local control covers the latest independently stored checkpoint
    #[arg(long, value_name = "DIRECTORY", requires = "control_checkpoint_key", conflicts_with_all = ["control_checkpoint", "backup", "restore", "restore_executor", "verify_backup", "reconcile_query_usage", "database_ready", "reset_password", "audit_templates", "export_template_health", "token_key_status", "database_role_setup_sql", "database_role_grants_sql", "check_database_privileges"])]
    #[cfg_attr(feature = "embedded-migrations", arg(conflicts_with_all = ["migrate", "migration_mode"]))]
    verify_control_recovery: Option<PathBuf>,

    /// Read bounded deletion/recovery status as JSON without granting cleanup or exposure
    #[arg(long, value_name = "DIRECTORY", requires = "control_checkpoint_key", conflicts_with_all = ["control_checkpoint", "verify_control_recovery", "recover_control", "reapply_control", "delete_object_history_version", "backup", "restore", "restore_executor", "verify_backup", "reconcile_query_usage", "database_ready", "reset_password", "audit_templates", "export_template_health", "token_key_status", "database_role_setup_sql", "database_role_grants_sql", "check_database_privileges"])]
    #[cfg_attr(feature = "embedded-migrations", arg(conflicts_with_all = ["migrate", "migration_mode"]))]
    control_status: Option<PathBuf>,

    /// Recover the latest authenticated control checkpoint; all runtime replicas must be stopped
    #[arg(long, value_name = "DIRECTORY", requires = "control_checkpoint_key", conflicts_with_all = ["control_checkpoint", "verify_control_recovery", "backup", "restore", "restore_executor", "verify_backup", "reconcile_query_usage", "database_ready", "reset_password", "audit_templates", "export_template_health", "token_key_status", "database_role_setup_sql", "database_role_grants_sql", "check_database_privileges"])]
    #[cfg_attr(feature = "embedded-migrations", arg(conflicts_with_all = ["migrate", "migration_mode"]))]
    recover_control: Option<PathBuf>,

    /// Reapply retained deletion decisions and protect completion before reopening access
    #[arg(long, value_name = "DIRECTORY", requires = "control_checkpoint_key", conflicts_with_all = ["control_checkpoint", "verify_control_recovery", "recover_control", "delete_object_history_version", "backup", "restore", "restore_executor", "verify_backup", "reconcile_query_usage", "database_ready", "reset_password", "audit_templates", "export_template_health", "token_key_status", "database_role_setup_sql", "database_role_grants_sql", "check_database_privileges"])]
    #[cfg_attr(feature = "embedded-migrations", arg(conflicts_with_all = ["migrate", "migration_mode"]))]
    reapply_control: Option<PathBuf>,

    /// Remove one closed object-history version while all runtimes are stopped; quarantine remains
    #[arg(long, value_name = "VERSION_UUID", requires_all = ["history_resource", "deletion_intent", "control_repository", "control_checkpoint_key"], conflicts_with_all = ["control_checkpoint", "verify_control_recovery", "recover_control", "backup", "restore", "restore_executor", "verify_backup", "reconcile_query_usage", "database_ready", "reset_password", "audit_templates", "export_template_health", "token_key_status", "database_role_setup_sql", "database_role_grants_sql", "check_database_privileges"])]
    #[cfg_attr(feature = "embedded-migrations", arg(conflicts_with_all = ["migrate", "migration_mode"]))]
    delete_object_history_version: Option<VersionIdentity>,

    /// Stable resource identity that owns the selected object-history version
    #[arg(
        long,
        value_name = "RESOURCE_UUID",
        requires = "delete_object_history_version"
    )]
    history_resource: Option<ResourceIdentity>,

    /// Stable deletion intent identity; reuse the same value on every retry
    #[arg(long, value_name = "UUID", requires = "delete_object_history_version", value_parser = parse_deletion_intent)]
    deletion_intent: Option<Uuid>,

    /// Initialized independent recovery repository for the managed-copy operation
    #[arg(
        long,
        value_name = "DIRECTORY",
        requires = "delete_object_history_version"
    )]
    control_repository: Option<PathBuf>,

    /// Owner-only file containing exactly 32 random signing-key bytes
    #[arg(long, value_name = "PATH", required_if_eq_any = [("initialize_control_checkpoint", "true")])]
    control_checkpoint_key: Option<PathBuf>,

    /// Bootstrap an empty independent repository from empty deletion authority
    #[arg(long, requires = "control_checkpoint")]
    initialize_control_checkpoint: bool,
    #[command(flatten)]
    schema_validation: SchemaValidationOptions,
    #[command(flatten)]
    secrets: SecretSourceOptions,

    /// Run one bounded privileged pass preparing and cleaning up query usage resources
    #[arg(long, default_value_t = false, conflicts_with_all = ["backup", "restore", "restore_executor", "verify_backup", "database_role_setup_sql", "database_role_grants_sql", "check_database_privileges", "reset_password", "audit_templates", "export_template_health", "database_ready", "token_key_status"])]
    reconcile_query_usage: bool,

    /// Restrict new resource planning to this class; cleanup remains global
    #[arg(long, requires = "reconcile_query_usage", value_parser = parse_query_usage_class)]
    query_usage_class: Option<ClassId>,

    /// Write a consistent full-system backup document to this path
    #[arg(long, value_name = "PATH")]
    backup: Option<PathBuf>,

    /// Maximum bytes for backup capture and output
    #[arg(long, env = "HUBUUM_BACKUP_MAX_OUTPUT_BYTES", default_value_t = DEFAULT_BACKUP_MAX_OUTPUT_BYTES)]
    backup_max_output_bytes: usize,

    /// Maximum rows enumerated during backup capture
    #[arg(long, env = "HUBUUM_BACKUP_MAX_CAPTURE_ROWS", default_value_t = DEFAULT_BACKUP_MAX_CAPTURE_ROWS)]
    backup_max_capture_rows: usize,

    /// Omit audit, task, delivery, and temporal history from --backup
    #[arg(long, default_value_t = false, requires = "backup")]
    backup_without_history: bool,

    /// Verify a backup document without connecting to a database
    #[arg(
        long,
        value_name = "PATH",
        conflicts_with_all = ["backup", "restore", "restore_executor"]
    )]
    verify_backup: Option<PathBuf>,

    /// Restore a verified backup into this newly created empty disposable database
    #[arg(long, value_name = "URL", requires = "verify_backup")]
    restore_test_database_url: Option<String>,

    /// Leave the restored disposable database intact for manual inspection
    #[arg(long, default_value_t = false, requires = "restore_test_database_url")]
    keep_restore_test_database: bool,

    /// Maximum backup document size accepted by restore and backup verification
    #[arg(
        long,
        env = "HUBUUM_RESTORE_MAX_UPLOAD_BYTES",
        default_value_t = DEFAULT_RESTORE_MAX_UPLOAD_BYTES
    )]
    restore_max_upload_bytes: usize,

    /// Destructively replace all application data from this backup document
    #[arg(long, value_name = "PATH", conflicts_with = "backup")]
    restore: Option<PathBuf>,

    /// Exact destructive confirmation phrase required with --restore
    #[arg(long, value_name = "PHRASE", requires = "restore")]
    restore_confirmation: Option<String>,

    /// Run the isolated long-lived executor for confirmed web restores
    #[arg(long, default_value_t = false)]
    restore_executor: bool,

    /// Reset the password for the specified username
    #[arg(long)]
    reset_password: Option<String>,

    /// Validate all stored export templates against the Jinja renderer
    #[arg(long, default_value_t = false)]
    audit_templates: bool,

    /// Summarize stored export output health by template name
    #[arg(long, default_value_t = false)]
    export_template_health: bool,

    /// Check that the database accepts connections
    #[arg(long, default_value_t = false)]
    database_ready: bool,

    /// Print non-secret token hash key retirement evidence as JSON
    #[arg(long, default_value_t = false)]
    token_key_status: bool,

    /// Run all pending embedded database migrations
    #[cfg(feature = "embedded-migrations")]
    #[arg(long, default_value_t = false)]
    migrate: bool,

    /// Print rolling or offline for pending migrations without changing the database
    #[cfg(feature = "embedded-migrations")]
    #[arg(long, default_value_t = false, conflicts_with = "migrate")]
    migration_mode: bool,

    /// Compatibility alias for --database-role-mode single during migration
    #[cfg(feature = "embedded-migrations")]
    #[arg(
        long,
        default_value_t = false,
        requires = "migrate",
        conflicts_with_all = [
            "database_owner_role",
            "database_migrator_role",
            "database_runtime_role"
        ]
    )]
    legacy_single_role_migration: bool,

    /// Print idempotent SQL for the configured owner, migrator, and runtime roles
    #[arg(long, default_value_t = false)]
    database_role_setup_sql: bool,

    /// Print grant/ownership SQL for roles pre-created by a managed database provider
    #[arg(
        long,
        default_value_t = false,
        conflicts_with = "database_role_setup_sql"
    )]
    database_role_grants_sql: bool,

    /// Check a logical database role against the generated privilege manifest
    #[arg(long, default_value_t = false)]
    check_database_privileges: bool,

    /// Logical role inspected by --check-database-privileges
    #[arg(
        long,
        value_enum,
        default_value = "runtime",
        requires = "check_database_privileges"
    )]
    role: DatabasePrivilegeRole,

    /// Emit machine-readable JSON for supported report commands
    #[arg(long, default_value_t = false)]
    json: bool,

    /// Storage adapter compiled into this application build.
    #[arg(
        long,
        env = "HUBUUM_STORAGE_BACKEND",
        value_enum,
        default_value = "postgresql"
    )]
    storage_backend: StorageBackendKind,

    /// Database credential topology: one shared login or split owner/migrator/runtime roles
    #[arg(
        long,
        env = "HUBUUM_DATABASE_ROLE_MODE",
        value_enum,
        default_value = "single"
    )]
    database_role_mode: DatabaseRoleMode,

    /// Database URL
    #[arg(long, env = "HUBUUM_DATABASE_URL")]
    database_url: Option<String>,

    /// Privileged migration and restore-executor database URL
    #[arg(long, env = "HUBUUM_MIGRATION_DATABASE_URL")]
    migration_database_url: Option<String>,

    /// Non-login role that owns Hubuum database objects
    #[arg(long, env = "HUBUUM_DATABASE_OWNER_ROLE")]
    database_owner_role: Option<String>,

    /// Login/workload role used only by migrations and destructive restore
    #[arg(long, env = "HUBUUM_DATABASE_MIGRATOR_ROLE")]
    database_migrator_role: Option<String>,

    /// Non-owning login role used by API and worker processes
    #[arg(long, env = "HUBUUM_DATABASE_RUNTIME_ROLE")]
    database_runtime_role: Option<String>,

    /// Pool-global storage query timeout in milliseconds (0 disables it)
    #[arg(
        long,
        env = "HUBUUM_DB_STATEMENT_TIMEOUT_MS",
        default_value_t = DEFAULT_DB_STATEMENT_TIMEOUT_MS
    )]
    db_statement_timeout_ms: u64,

    /// Legacy token lifetime used when classifying rows without explicit expiry
    #[arg(
        long,
        env = "HUBUUM_TOKEN_LIFETIME_HOURS",
        default_value_t = DEFAULT_TOKEN_LIFETIME_HOURS
    )]
    token_lifetime_hours: i64,

    /// MiniJinja recursion limit for export template validation
    #[arg(
        long,
        env = "HUBUUM_EXPORT_TEMPLATE_RECURSION_LIMIT",
        default_value_t = DEFAULT_EXPORT_TEMPLATE_RECURSION_LIMIT
    )]
    export_template_recursion_limit: usize,

    /// MiniJinja fuel budget for export template validation
    #[arg(
        long,
        env = "HUBUUM_EXPORT_TEMPLATE_FUEL",
        default_value_t = DEFAULT_EXPORT_TEMPLATE_FUEL
    )]
    export_template_fuel: u64,

    /// Log level
    /// Possible values: trace, debug, info, warn, error
    #[arg(long, env = "HUBUUM_LOG_LEVEL", default_value = "info")]
    log_level: String,
}

pub(crate) fn admin_command() -> clap::Command {
    AdminCli::command()
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum DatabasePrivilegeRole {
    Owner,
    Migrator,
    Runtime,
}

/// Parse and execute one Hubuum administration command.
///
/// This is the workspace-internal boundary used by `hubuum-admin`; callers
/// should use the CLI rather than depending on server persistence types.
pub async fn run_admin_from_environment() -> Result<(), ApiError> {
    let matches = AdminCli::command().get_matches();
    let admin_cli = AdminCli::from_arg_matches(&matches).unwrap_or_else(|error| error.exit());
    let schema_limits = admin_cli.schema_validation.validate()?;
    let database_url_override = CommandLineDatabaseUrl::from_matches(&matches, "database_url");
    let migration_url_override =
        CommandLineDatabaseUrl::from_matches(&matches, "migration_database_url");
    init_logging(
        &admin_cli.log_level,
        admin_cli.reconcile_query_usage
            || admin_cli.delete_object_history_version.is_some()
            || admin_cli.control_checkpoint.is_some()
            || admin_cli.reapply_control.is_some()
            || admin_cli.recover_control.is_some()
            || admin_cli.control_status.is_some()
            || admin_cli.verify_control_recovery.is_some(),
    );

    if let Some(path) = admin_cli.verify_backup.as_deref() {
        verify_backup_file(BackupVerificationOptions {
            capture_budget: StorageBackupBudget::new(
                admin_cli.backup_max_output_bytes,
                admin_cli.backup_max_capture_rows,
            )
            .map_err(|error| ApiError::BadRequest(error.to_string()))?,
            schema_limits,
            path,
            restore_test_database_url: admin_cli.restore_test_database_url.as_deref(),
            configured_database_url: admin_cli.database_url.as_deref(),
            configured_migration_database_url: admin_cli.migration_database_url.as_deref(),
            secret_source: &admin_cli.secrets,
            storage_backend: admin_cli.storage_backend,
            statement_timeout_ms: admin_cli.db_statement_timeout_ms,
            keep_restore_test_database: admin_cli.keep_restore_test_database,
            max_upload_bytes: admin_cli.restore_max_upload_bytes,
            json: admin_cli.json,
        })
        .await?;
        return Ok(());
    }

    if admin_cli.database_role_setup_sql {
        let database_roles = configured_database_roles(&admin_cli)?;
        print!("{}", storage_database_role_setup_sql(&database_roles)?);
        return Ok(());
    }
    if admin_cli.database_role_grants_sql {
        let database_roles = configured_database_roles(&admin_cli)?;
        println!("BEGIN;");
        print!("{}", storage_database_role_grants_sql(&database_roles)?);
        println!("COMMIT;");
        return Ok(());
    }

    if admin_cli.restore.is_some()
        && admin_cli.restore_confirmation.as_deref() != Some(RESTORE_CONFIRMATION_PHRASE)
    {
        return Err(destructive_confirmation_error());
    }

    #[cfg(feature = "embedded-migrations")]
    let migration_requested = admin_cli.migrate || admin_cli.migration_mode;
    #[cfg(not(feature = "embedded-migrations"))]
    let migration_requested = false;
    #[cfg(feature = "embedded-migrations")]
    let database_role_mode = effective_database_role_mode(&admin_cli)?;
    #[cfg(not(feature = "embedded-migrations"))]
    let database_role_mode = admin_cli.database_role_mode;
    if migration_requested && admin_cli.reconcile_query_usage {
        return Err(ApiError::BadRequest(
            "Query usage reconciliation must run separately from migrations".into(),
        ));
    }
    let privileged_database_operation = migration_requested
        || admin_cli.delete_object_history_version.is_some()
        || admin_cli.control_checkpoint.is_some()
        || admin_cli.reapply_control.is_some()
        || admin_cli.recover_control.is_some()
        || admin_cli.control_status.is_some()
        || admin_cli.verify_control_recovery.is_some()
        || admin_cli.restore.is_some()
        || admin_cli.restore_executor
        || admin_cli.reconcile_query_usage;
    secrets::initialize(&admin_cli.secrets)
        .unwrap_or_else(|error| fatal_error(&error.to_string(), EXIT_CODE_CONFIG_ERROR));
    let database_url = if admin_cli.storage_backend == StorageBackendKind::Postgres {
        let migration_url = if privileged_database_operation {
            secrets::resolve_database_url(
                DatabaseCredential::Migration,
                admin_cli.migration_database_url.as_deref(),
                migration_url_override.as_ref(),
            )
            .await
            .unwrap_or_else(|error| fatal_error(&error.to_string(), EXIT_CODE_CONFIG_ERROR))
        } else {
            None
        };
        if privileged_database_operation
            && (database_role_mode.uses_split_roles() || migration_url.is_some())
        {
            migration_url
        } else {
            secrets::resolve_database_url(
                DatabaseCredential::Runtime,
                admin_cli.database_url.as_deref(),
                database_url_override.as_ref(),
            )
            .await
            .unwrap_or_else(|error| fatal_error(&error.to_string(), EXIT_CODE_CONFIG_ERROR))
        }
    } else {
        None
    };
    let storage_settings = match admin_cli.storage_backend {
        StorageBackendKind::Postgres => {
            let database_url = database_url.unwrap_or_else(|| {
                let variable = if privileged_database_operation
                    && database_role_mode.uses_split_roles()
                {
                    "HUBUUM_MIGRATION_DATABASE_URL"
                } else if privileged_database_operation {
                    "HUBUUM_DATABASE_URL (or the optional HUBUUM_MIGRATION_DATABASE_URL override)"
                } else {
                    "HUBUUM_DATABASE_URL"
                };
                fatal_error(
                    &format!("{variable} must be set in environment mode; in file mode provide {} under the secret root, or pass the corresponding URL argument",
                        if privileged_database_operation && database_role_mode.uses_split_roles() {
                            "database/migration-url"
                        } else {
                            "database/url"
                        }),
                    EXIT_CODE_CONFIG_ERROR,
                )
            });
            StorageSettings::postgres(database_url)
                .max_connections(1)
                .statement_timeout_ms(admin_cli.db_statement_timeout_ms)
                .acquire_timeout_ms(DEFAULT_DB_POOL_ACQUIRE_TIMEOUT_MS)
                .build()?
        }
        StorageBackendKind::Memory => StorageSettings::memory(),
    }.with_schema_limits(schema_limits);

    #[cfg(feature = "embedded-migrations")]
    if admin_cli.migration_mode {
        let database_roles = database_role_mode
            .uses_split_roles()
            .then(|| configured_database_roles(&admin_cli))
            .transpose()?;
        println!(
            "{}",
            inspect_storage_migration_mode(&storage_settings, database_roles.as_ref())?.as_str()
        );
        return Ok(());
    }

    #[cfg(feature = "embedded-migrations")]
    if admin_cli.migrate {
        let database_roles = database_role_mode
            .uses_split_roles()
            .then(|| configured_database_roles(&admin_cli))
            .transpose()?;
        let applied = run_storage_migrations(&storage_settings, database_roles.as_ref())
            .unwrap_or_else(|error| {
                fatal_error(
                    &format!("Failed to run storage migrations: {error}"),
                    EXIT_CODE_DATABASE_ERROR,
                )
            });
        if admin_cli.legacy_single_role_migration {
            eprintln!(
                "WARNING: --legacy-single-role-migration is deprecated; single-role migration is \
                 now the default. Use --database-role-mode single explicitly if desired."
            );
        }
        println!("Applied {applied} storage migration(s).");
        return Ok(());
    }

    if admin_cli.check_database_privileges {
        let database_roles = configured_database_roles(&admin_cli)?;
        let role = match admin_cli.role {
            DatabasePrivilegeRole::Owner => StorageDatabaseRole::Owner,
            DatabasePrivilegeRole::Migrator => StorageDatabaseRole::Migrator,
            DatabasePrivilegeRole::Runtime => StorageDatabaseRole::Runtime,
        };
        let report = inspect_storage_database_privileges(&storage_settings, role, &database_roles)
            .await?
            .ok_or_else(|| {
                ApiError::BadRequest(
                    "Database privilege checks require the PostgreSQL storage backend".to_string(),
                )
            })?;
        print_database_privilege_report(&report, admin_cli.json)?;
        if !report.is_safe() {
            return Err(ApiError::BadRequest(format!(
                "Database role '{}' does not satisfy the Hubuum privilege manifest",
                report.role(),
            )));
        }
        return Ok(());
    }

    let storage = initialize_storage(&storage_settings)?;

    if admin_cli.reconcile_query_usage {
        let roles = database_role_mode
            .uses_split_roles()
            .then(|| configured_database_roles(&admin_cli))
            .transpose()?;
        // Native concurrent DDL cannot run inside the ordinary transaction
        // wrapper. Its process registration excludes destructive cleanup until
        // the complete executor pass and report have settled.
        let recovery = crate::services::deletion_recovery::require_runtime_permit(
            &storage,
            &admin_cli.control_recovery,
        )
        .await?;
        let registration = storage
            .register_runtime_isolated(recovery.isolation_domain())
            .await?;
        let recovery = recovery.into_runtime_lifetime();
        let storage = storage.with_runtime_owner(registration.owner());
        let result = async {
            let permit = storage.admit_application_work().await?;
            let scope = storage
                .current_execution_scope()
                .with_application_access(permit);
            let report = storage
                .run_in_scope(
                    scope,
                    crate::storage::reconcile_query_usage_resources(
                        &storage_settings,
                        roles.as_ref(),
                        admin_cli.query_usage_class,
                    ),
                )
                .await?;
            println!(
                "{}",
                serde_json::to_string_pretty(&report).map_err(
                    |_| ApiError::InternalServerError(
                        "Query usage executor report serialization failed".into()
                    )
                )?
            );
            Ok::<(), ApiError>(())
        }
        .await;
        storage.unregister_runtime(registration).await?;
        recovery.release();
        return result;
    }

    if let Some(version) = admin_cli.delete_object_history_version {
        let report = crate::services::deletion_recovery::cleanup_object_history(
            &storage,
            admin_cli
                .control_repository
                .as_deref()
                .expect("required by clap"),
            admin_cli
                .control_checkpoint_key
                .as_deref()
                .expect("required by clap"),
            admin_cli.deletion_intent.expect("required by clap"),
            StorageRecordIdentity::new(
                admin_cli.history_resource.expect("required by clap"),
                version,
            ),
        )
        .await?;
        println!(
            "{}",
            serde_json::to_string_pretty(&report).map_err(|_| ApiError::InternalServerError(
                "Cannot encode copy cleanup report".into()
            ))?
        );
        return Ok(());
    }

    if let Some(directory) = admin_cli.control_status.as_deref() {
        let key = admin_cli
            .control_checkpoint_key
            .as_deref()
            .expect("Clap requires the status authentication key");
        let report = crate::services::deletion_recovery::status(&storage, directory, key).await?;
        println!(
            "{}",
            serde_json::to_string_pretty(&report).map_err(|_| {
                ApiError::InternalServerError("Cannot encode deletion status".into())
            })?
        );
        return Ok(());
    }

    if admin_cli.control_checkpoint.is_some()
        || admin_cli.reapply_control.is_some()
        || admin_cli.recover_control.is_some()
        || admin_cli.verify_control_recovery.is_some()
    {
        let key = admin_cli.control_checkpoint_key.as_deref().ok_or_else(|| {
            ApiError::BadRequest(
                "--control-checkpoint-key is required for independent recovery evidence".into(),
            )
        })?;
        let report = if let Some(directory) = admin_cli.control_checkpoint.as_deref() {
            crate::services::deletion_recovery::checkpoint(
                &storage,
                directory,
                key,
                admin_cli.initialize_control_checkpoint,
            )
            .await?
        } else if let Some(directory) = admin_cli.reapply_control.as_deref() {
            crate::services::deletion_recovery::reapply(&storage, directory, key).await?
        } else if let Some(directory) = admin_cli.recover_control.as_deref() {
            crate::services::deletion_recovery::recover(&storage, directory, key).await?
        } else {
            crate::services::deletion_recovery::verify(
                &storage,
                admin_cli
                    .verify_control_recovery
                    .as_deref()
                    .expect("selected verification"),
                key,
            )
            .await?
        };
        println!(
            "{}",
            serde_json::to_string_pretty(&report).map_err(|_| ApiError::InternalServerError(
                "Cannot encode control recovery report".into()
            ))?
        );
        return Ok(());
    }

    let control_lifetime = if admin_cli.restore_executor || admin_cli.restore.is_some() {
        Some(
            crate::services::deletion_recovery::require_control_runtime_lifetime(
                &storage,
                &admin_cli.control_recovery,
            )
            .await?,
        )
    } else {
        None
    };

    // Ordinary administrator commands are producers too: hold registration
    // until their output and child work settle, rather than recapturing access
    // between database reads and writing a backup or printing a report.
    let ordinary = admin_cli.backup.is_some()
        || admin_cli.reset_password.is_some()
        || admin_cli.audit_templates
        || admin_cli.export_template_health
        || admin_cli.token_key_status;
    let recovery = if ordinary {
        Some(
            crate::services::deletion_recovery::require_runtime_permit(
                &storage,
                &admin_cli.control_recovery,
            )
            .await?,
        )
    } else {
        None
    };
    let mut registration = if let Some(recovery) = &recovery {
        Some(
            storage
                .register_runtime_isolated(recovery.isolation_domain())
                .await?,
        )
    } else {
        None
    };
    let recovery = recovery.map(|permit| permit.into_runtime_lifetime());
    let storage = match &registration {
        Some(registration) => storage.with_runtime_owner(registration.owner()),
        None => storage,
    };
    let scope = if ordinary {
        let permit = match storage.admit_application_work().await {
            Ok(permit) => permit,
            Err(error) => {
                if let Some(registration) = registration.take() {
                    storage.unregister_runtime(registration).await?;
                }
                if let Some(recovery) = recovery {
                    recovery.release();
                }
                return Err(error.into());
            }
        };
        Some(
            storage
                .current_execution_scope()
                .with_application_access(permit),
        )
    } else {
        None
    };
    let operation = async {
        if admin_cli.restore_executor {
            if !matches!(admin_cli.storage_backend, StorageBackendKind::Postgres) {
                return Err(ApiError::BadRequest(
                    "The restore executor requires the PostgreSQL storage backend".to_string(),
                ));
            }
            run_restore_executor(&storage, &admin_cli.control_recovery).await?;
        } else if let Some(path) = admin_cli.backup {
            backup_database(
                &storage,
                &path,
                !admin_cli.backup_without_history,
                StorageBackupBudget::new(
                    admin_cli.backup_max_output_bytes,
                    admin_cli.backup_max_capture_rows,
                )
                .map_err(|error| ApiError::BadRequest(error.to_string()))?,
            )
            .await?;
        } else if let Some(path) = admin_cli.restore {
            restore_database(
                &storage,
                &path,
                admin_cli.restore_confirmation.as_deref(),
                admin_cli.restore_max_upload_bytes,
                &admin_cli.control_recovery,
            )
            .await?;
        } else if let Some(username) = admin_cli.reset_password {
            reset_password(&storage, &username).await?;
        } else if admin_cli.audit_templates {
            audit_templates(
                &storage,
                admin_cli.export_template_recursion_limit,
                admin_cli.export_template_fuel,
            )
            .await?;
        } else if admin_cli.export_template_health {
            load_export_template_health(&storage).await?;
        } else if admin_cli.database_ready {
            storage_ready(&storage).await?;
        } else if admin_cli.token_key_status {
            print_token_key_status(&storage, admin_cli.token_lifetime_hours).await?;
        } else {
            println!("No command specified. Use --help for usage information.");
        }

        Ok::<(), ApiError>(())
    };
    let result = match scope {
        Some(scope) => storage.run_in_scope(scope, operation).await,
        None => operation.await,
    };
    if let Some(registration) = registration {
        // Any detached template child keeps the registration for offline
        // recovery. A command error alone is not proof that its child stopped.
        if crate::utilities::exporting::template_workers_idle() {
            storage.unregister_runtime(registration).await?;
        } else {
            // The lifetime destructor retains OS exclusion until process exit.
            return result;
        }
    }
    if let Some(recovery) = recovery {
        recovery.release();
    }
    if let Some(lifetime) = control_lifetime {
        lifetime.release();
    }
    result
}

fn resolve_database_roles(
    owner: Option<&str>,
    migrator: Option<&str>,
    runtime: Option<&str>,
) -> Result<StorageDatabaseRoleNames, ApiError> {
    StorageDatabaseRoleNames::new(
        owner.unwrap_or(DEFAULT_DATABASE_OWNER_ROLE),
        migrator.unwrap_or(DEFAULT_DATABASE_MIGRATOR_ROLE),
        runtime.unwrap_or(DEFAULT_DATABASE_RUNTIME_ROLE),
    )
    .map_err(|error| ApiError::BadRequest(error.to_string()))
}

fn configured_database_roles(admin_cli: &AdminCli) -> Result<StorageDatabaseRoleNames, ApiError> {
    resolve_database_roles(
        admin_cli.database_owner_role.as_deref(),
        admin_cli.database_migrator_role.as_deref(),
        admin_cli.database_runtime_role.as_deref(),
    )
}

#[cfg(feature = "embedded-migrations")]
fn effective_database_role_mode(admin_cli: &AdminCli) -> Result<DatabaseRoleMode, ApiError> {
    if admin_cli.legacy_single_role_migration && admin_cli.database_role_mode.uses_split_roles() {
        return Err(ApiError::BadRequest(
            "--legacy-single-role-migration cannot be combined with --database-role-mode split"
                .to_string(),
        ));
    }
    if admin_cli.legacy_single_role_migration {
        Ok(DatabaseRoleMode::Single)
    } else {
        Ok(admin_cli.database_role_mode)
    }
}

fn print_database_privilege_report(
    report: &StorageDatabasePrivilegeReport,
    json: bool,
) -> Result<(), ApiError> {
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(report).map_err(|error| {
                ApiError::InternalServerError(format!(
                    "Failed to serialize database privilege report: {error}"
                ))
            })?
        );
        return Ok(());
    }
    println!(
        "Database role '{}' (connected as '{}'): {}",
        report.role(),
        report.connected_role(),
        if report.is_safe() { "safe" } else { "unsafe" }
    );
    for finding in report.dangerous() {
        println!(
            "DANGEROUS {} {}: {}",
            finding.code(),
            finding.object(),
            finding.detail()
        );
    }
    for finding in report.missing() {
        println!(
            "MISSING {} {}: {}",
            finding.code(),
            finding.object(),
            finding.detail()
        );
    }
    Ok(())
}

#[derive(Serialize)]
struct TokenKeyStatusReport {
    ring_identity: String,
    stable: bool,
    require_stable: bool,
    active_key_id: String,
    previous_key_ids: Vec<String>,
    usage: Vec<TokenKeyStatusRow>,
}

#[derive(Serialize)]
struct TokenKeyStatusRow {
    key_id: String,
    configured_state: &'static str,
    active: i64,
    revoked: i64,
    expired: i64,
    latest_validation: Option<chrono::DateTime<chrono::Utc>>,
    earliest_expiry: Option<chrono::DateTime<chrono::Utc>>,
    latest_expiry: Option<chrono::DateTime<chrono::Utc>>,
}

async fn print_token_key_status(
    storage: &StorageHandle,
    token_lifetime_hours: i64,
) -> Result<(), ApiError> {
    let ring =
        token_hash_key_ring().map_err(|error| ApiError::ValidationError(error.to_string()))?;
    let observed_at = chrono::Utc::now();
    let lifetime = chrono::Duration::try_hours(token_lifetime_hours).ok_or_else(|| {
        ApiError::ValidationError("token lifetime is outside the supported range".to_string())
    })?;
    if lifetime <= chrono::Duration::zero() {
        return Err(ApiError::ValidationError(
            "token lifetime must be positive".to_string(),
        ));
    }
    let observation = StorageTokenObservation::try_new(observed_at, observed_at - lifetime)
        .map_err(|error| ApiError::ValidationError(error.to_string()))?;
    let mut stored = storage
        .token_key_usage(observation)
        .await?
        .into_iter()
        .map(|usage| (usage.key_id().map(ToString::to_string), usage))
        .collect::<BTreeMap<_, _>>();
    let mut usage = Vec::new();
    usage.push(status_row(
        ring.active_key_id().to_string(),
        "active",
        stored.remove(&Some(ring.active_key_id().to_string())),
    ));
    let previous_key_ids = ring
        .previous_key_ids()
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    for key_id in &previous_key_ids {
        usage.push(status_row(
            key_id.clone(),
            "previous",
            stored.remove(&Some(key_id.clone())),
        ));
    }
    if let Some(legacy) = stored.remove(&None) {
        usage.push(status_row(
            "legacy-unidentified".to_string(),
            "legacy",
            Some(legacy),
        ));
    }
    for (key_id, unconfigured) in stored {
        usage.push(status_row(
            key_id.unwrap_or_else(|| "legacy-unidentified".to_string()),
            "unconfigured",
            Some(unconfigured),
        ));
    }
    let report = TokenKeyStatusReport {
        ring_identity: ring.identity().to_string(),
        stable: ring.is_stable(),
        require_stable: ring.requires_stable_key(),
        active_key_id: ring.active_key_id().to_string(),
        previous_key_ids,
        usage,
    };
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}

fn status_row(
    key_id: String,
    configured_state: &'static str,
    usage: Option<StorageTokenKeyUsage>,
) -> TokenKeyStatusRow {
    TokenKeyStatusRow {
        key_id,
        configured_state,
        active: usage.as_ref().map_or(0, StorageTokenKeyUsage::active),
        revoked: usage.as_ref().map_or(0, StorageTokenKeyUsage::revoked),
        expired: usage.as_ref().map_or(0, StorageTokenKeyUsage::expired),
        latest_validation: usage
            .as_ref()
            .and_then(StorageTokenKeyUsage::latest_validation),
        earliest_expiry: usage
            .as_ref()
            .and_then(StorageTokenKeyUsage::earliest_expiry),
        latest_expiry: usage.as_ref().and_then(StorageTokenKeyUsage::latest_expiry),
    }
}

async fn backup_database(
    storage: &StorageHandle,
    path: &Path,
    include_history: bool,
    budget: StorageBackupBudget,
) -> Result<(), ApiError> {
    let document = crate::restores::replay::capture(storage, include_history, budget).await?;
    let byte_size = write_backup_file(path, &document, budget)?;
    println!(
        "Wrote consistent backup to '{}' ({} bytes).",
        path.display(),
        byte_size
    );
    Ok(())
}

fn open_backup_file(path: &Path, max_bytes: usize) -> Result<File, ApiError> {
    let file = File::open(path).map_err(|error| {
        ApiError::BadRequest(format!(
            "Failed to open backup document '{}': {error}",
            path.display()
        ))
    })?;
    let metadata = file.metadata().map_err(|error| {
        ApiError::BadRequest(format!(
            "Failed to inspect backup document '{}': {error}",
            path.display()
        ))
    })?;
    if !metadata.file_type().is_file() {
        return Err(ApiError::BadRequest(format!(
            "Backup document '{}' must be an ordinary file",
            path.display()
        )));
    }
    if metadata.len() > u64::try_from(max_bytes).unwrap_or(u64::MAX) {
        return Err(ApiError::PayloadTooLarge(format!(
            "Backup document is {} bytes, exceeding the configured {} byte limit",
            metadata.len(),
            max_bytes
        )));
    }
    Ok(file)
}

fn admin_storage_settings(
    database_url: impl Into<String>,
    statement_timeout_ms: u64,
) -> Result<StorageSettings, ApiError> {
    StorageSettings::postgres(database_url)
        .max_connections(1)
        .statement_timeout_ms(statement_timeout_ms)
        .acquire_timeout_ms(DEFAULT_DB_POOL_ACQUIRE_TIMEOUT_MS)
        .build()
        .map_err(Into::into)
}

fn reject_configured_database_target(
    target: &StorageSettings,
    configured_url: Option<&str>,
    statement_timeout_ms: u64,
) -> Result<(), ApiError> {
    let Some(configured_url) = configured_url.filter(|value| !value.trim().is_empty()) else {
        return Ok(());
    };
    let configured = admin_storage_settings(configured_url, statement_timeout_ms)?;
    if target.same_database_endpoint(&configured) {
        return Err(ApiError::BadRequest(
            "Refused restore verification because the disposable target matches a configured Hubuum database"
                .to_string(),
        ));
    }
    Ok(())
}

struct BackupVerificationOptions<'a> {
    capture_budget: StorageBackupBudget,
    schema_limits: JsonSchemaLimits,
    path: &'a Path,
    restore_test_database_url: Option<&'a str>,
    configured_database_url: Option<&'a str>,
    configured_migration_database_url: Option<&'a str>,
    secret_source: &'a SecretSourceOptions,
    storage_backend: StorageBackendKind,
    statement_timeout_ms: u64,
    keep_restore_test_database: bool,
    max_upload_bytes: usize,
    json: bool,
}

async fn verify_backup_file(
    options: BackupVerificationOptions<'_>,
) -> Result<BackupVerificationReport, ApiError> {
    let artifact = StorageRestoreArtifact::from_reader(
        open_backup_file(options.path, options.max_upload_bytes)?,
        options.max_upload_bytes,
    )?;
    let report = crate::restores::verify_backup_reader(
        artifact.reader(),
        options.max_upload_bytes,
        options.schema_limits,
    )?;
    let report = if let Some(database_url) = options.restore_test_database_url {
        if !matches!(options.storage_backend, StorageBackendKind::Postgres) {
            return Err(ApiError::BadRequest(
                "Isolated restore verification requires the PostgreSQL storage backend".to_string(),
            ));
        }
        let target = admin_storage_settings(database_url, options.statement_timeout_ms)?
            .with_schema_limits(options.schema_limits);
        reject_configured_database_target(
            &target,
            options.configured_database_url,
            options.statement_timeout_ms,
        )?;
        reject_configured_database_target(
            &target,
            options.configured_migration_database_url,
            options.statement_timeout_ms,
        )?;
        // Resolve mounted credentials too: a file-backed production endpoint must
        // retain the same protection as URLs supplied through the CLI/environment.
        secrets::initialize(options.secret_source)
            .map_err(|error| ApiError::BadRequest(error.to_string()))?;
        for credential in [DatabaseCredential::Runtime, DatabaseCredential::Migration] {
            let configured = secrets::resolve_database_url(credential, None, None)
                .await
                .map_err(|error| ApiError::BadRequest(error.to_string()))?;
            reject_configured_database_target(
                &target,
                configured.as_deref(),
                options.statement_timeout_ms,
            )?;
        }
        verify_backup_restore(
            report,
            artifact,
            target,
            options.keep_restore_test_database,
            options.capture_budget,
        )
        .await?
    } else {
        report
    };
    if options.json {
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else if options.restore_test_database_url.is_some() {
        println!(
            "Backup verification passed, including restore into an isolated disposable database."
        );
        if options.keep_restore_test_database {
            println!("The caller remains responsible for deleting the disposable database.");
        } else {
            println!("The disposable database schema was reset after verification.");
        }
        print_backup_verification_summary(&report);
    } else {
        println!("Backup verification passed (format only; no restore was attempted).");
        print_backup_verification_summary(&report);
    }
    Ok(report)
}

#[cfg(feature = "embedded-migrations")]
async fn verify_backup_restore(
    mut report: BackupVerificationReport,
    artifact: StorageRestoreArtifact,
    target: StorageSettings,
    keep_restore_test_database: bool,
    capture_budget: StorageBackupBudget,
) -> Result<BackupVerificationReport, ApiError> {
    let migrations_applied = prepare_disposable_restore_database(&target)?;
    let verification = async {
        let storage = initialize_storage(&target)?;
        let started_at = std::time::Instant::now();
        // The report, staging and comparison all read the same immutable artifact.
        let (_, context) = restore_database_artifact(
            &storage,
            artifact.clone(),
            &ControlRecoveryOptions::default(),
        )
        .await?;
        let restore_duration = started_at.elapsed();
        let readiness = storage.get_readiness_snapshot().await?;
        if !readiness.storage_is_ready() {
            return Err(ApiError::ServiceUnavailable(
                "Restored disposable database did not pass storage readiness".to_string(),
            ));
        }
        let (source, _, _) = crate::restores::replay::parse(
            artifact.reader(),
            artifact.summary().byte_size() as usize,
            storage.schema_limits(),
        )?;
        drop(artifact);
        let source = source.at_restore_boundary(context)?;
        let restored = crate::restores::replay::capture(&storage, true, capture_budget).await?;
        crate::restores::replay::compare(&source, &restored)?;
        Ok::<_, ApiError>(restore_duration)
    }
    .await;
    let target_cleanup = if keep_restore_test_database {
        "caller_managed"
    } else {
        if let Err(cleanup_error) = reset_disposable_restore_database(&target) {
            if verification.is_err() {
                return Err(ApiError::InternalServerError(
                    "Restore verification failed and the disposable database could not be reset; delete it explicitly"
                        .to_string(),
                ));
            }
            return Err(cleanup_error.into());
        }
        "schema_reset"
    };
    let restore_duration = verification?;
    report.record_isolated_restore(migrations_applied, restore_duration, target_cleanup);
    Ok(report)
}

#[cfg(not(feature = "embedded-migrations"))]
async fn verify_backup_restore(
    _report: BackupVerificationReport,
    _artifact: StorageRestoreArtifact,
    _target: StorageSettings,
    _keep_restore_test_database: bool,
    _capture_budget: StorageBackupBudget,
) -> Result<BackupVerificationReport, ApiError> {
    Err(ApiError::NotImplemented(
        "This hubuum-admin build does not include embedded migrations required for isolated restore verification"
            .to_string(),
    ))
}

fn print_backup_verification_summary(report: &BackupVerificationReport) {
    println!("Backup version: {}", report.backup_version());
    println!("Source version: {}", report.source_version());
    println!("SHA-256: {}", report.sha256());
    println!("Bytes: {}", report.byte_size());
    println!("Logical rows: {}", report.total_items());
    println!(
        "History: {}",
        if report.includes_history() {
            "included"
        } else {
            "excluded"
        }
    );
}

fn write_backup_file(
    path: &Path,
    document: &impl Serialize,
    budget: StorageBackupBudget,
) -> Result<usize, ApiError> {
    let io_error = |error| {
        ApiError::InternalServerError(format!(
            "Failed to write backup to '{}': {error}",
            path.display()
        ))
    };
    let (temporary_path, file) = create_backup_temporary_file(path).map_err(io_error)?;
    let mut writer = BufWriter::new(file);
    let write_result = (|| {
        let bytes = budget.serialize_into(document, &mut writer)?;
        writer.flush().map_err(io_error)?;
        writer.get_ref().sync_all().map_err(io_error)?;
        Ok::<_, ApiError>(bytes)
    })();
    drop(writer);

    if let Err(error) = write_result {
        let _ = std::fs::remove_file(&temporary_path);
        return Err(error);
    }

    if let Err(error) = replace_backup_file(&temporary_path, path) {
        let _ = std::fs::remove_file(&temporary_path);
        return Err(io_error(error));
    }

    write_result
}

fn backup_parent_directory(path: &Path) -> &Path {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
}

fn create_backup_temporary_file(path: &Path) -> std::io::Result<(PathBuf, File)> {
    let parent = backup_parent_directory(path);
    let file_name = path.file_name().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "Backup path must include a file name",
        )
    })?;

    for _ in 0..16 {
        let mut temporary_name = OsString::from(".");
        temporary_name.push(file_name);
        temporary_name.push(format!(".{}.tmp", Uuid::new_v4().simple()));
        let temporary_path = parent.join(temporary_name);
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);

        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;

            options.mode(0o600);
        }

        #[cfg(windows)]
        {
            use std::os::windows::fs::OpenOptionsExt;

            options.share_mode(0);
        }

        #[cfg(not(any(unix, windows)))]
        {
            let _ = (temporary_path, options);
            return Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "Owner-only backup file permissions are not supported on this platform",
            ));
        }

        match options.open(&temporary_path) {
            Ok(file) => {
                if let Err(error) = secure_backup_temporary_file(&temporary_path, &file) {
                    drop(file);
                    let _ = std::fs::remove_file(&temporary_path);
                    return Err(error);
                }
                return Ok((temporary_path, file));
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error),
        }
    }

    Err(std::io::Error::new(
        std::io::ErrorKind::AlreadyExists,
        "Could not create a unique temporary backup file",
    ))
}

#[cfg(unix)]
fn secure_backup_temporary_file(_path: &Path, file: &File) -> std::io::Result<()> {
    use std::os::unix::fs::PermissionsExt;

    file.set_permissions(std::fs::Permissions::from_mode(0o600))
}

#[cfg(windows)]
fn secure_backup_temporary_file(path: &Path, _file: &File) -> std::io::Result<()> {
    restrict_windows_backup_file_to_owner(path)
}

#[cfg(not(any(unix, windows)))]
fn secure_backup_temporary_file(_path: &Path, _file: &File) -> std::io::Result<()> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "Owner-only backup file permissions are not supported on this platform",
    ))
}

#[cfg(unix)]
fn replace_backup_file(temporary_path: &Path, path: &Path) -> std::io::Result<()> {
    std::fs::rename(temporary_path, path)?;
    File::open(backup_parent_directory(path))?.sync_all()
}

#[cfg(windows)]
fn replace_backup_file(temporary_path: &Path, path: &Path) -> std::io::Result<()> {
    use std::iter::once;
    use std::os::windows::ffi::OsStrExt;

    const MOVEFILE_REPLACE_EXISTING: u32 = 0x1;
    const MOVEFILE_WRITE_THROUGH: u32 = 0x8;

    #[link(name = "kernel32")]
    unsafe extern "system" {
        #[link_name = "MoveFileExW"]
        fn move_file_ex_w(
            existing_file_name: *const u16,
            new_file_name: *const u16,
            flags: u32,
        ) -> i32;
    }

    let temporary_path: Vec<u16> = temporary_path
        .as_os_str()
        .encode_wide()
        .chain(once(0))
        .collect();
    let path: Vec<u16> = path.as_os_str().encode_wide().chain(once(0)).collect();
    // SAFETY: Both paths are valid, nul-terminated UTF-16 buffers that remain
    // alive for the duration of the call.
    let result = unsafe {
        move_file_ex_w(
            temporary_path.as_ptr(),
            path.as_ptr(),
            MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
        )
    };
    if result == 0 {
        Err(std::io::Error::last_os_error())
    } else {
        Ok(())
    }
}

#[cfg(not(any(unix, windows)))]
fn replace_backup_file(_temporary_path: &Path, _path: &Path) -> std::io::Result<()> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "Atomic backup file replacement is not supported on this platform",
    ))
}

#[cfg(windows)]
fn restrict_windows_backup_file_to_owner(path: &Path) -> std::io::Result<()> {
    const ACL_SCRIPT: &str = r#"
$ErrorActionPreference = 'Stop'
$path = [Environment]::GetEnvironmentVariable('ADMIN_BACKUP_ACL_PATH', 'Process')
$identity = [System.Security.Principal.WindowsIdentity]::GetCurrent().User
$acl = New-Object System.Security.AccessControl.FileSecurity
$acl.SetOwner($identity)
$acl.SetAccessRuleProtection($true, $false)
$rule = [System.Security.AccessControl.FileSystemAccessRule]::new(
    $identity,
    [System.Security.AccessControl.FileSystemRights]::FullControl,
    [System.Security.AccessControl.AccessControlType]::Allow
)
$acl.AddAccessRule($rule)
Set-Acl -LiteralPath $path -AclObject $acl
"#;

    let output = std::process::Command::new("powershell.exe")
        .args([
            "-NoLogo",
            "-NoProfile",
            "-NonInteractive",
            "-Command",
            ACL_SCRIPT,
        ])
        // A parent PowerShell 7 process can supply incompatible module paths.
        // Let Windows PowerShell construct its own path for the built-in ACL cmdlets.
        .env_remove("PSModulePath")
        .env("ADMIN_BACKUP_ACL_PATH", path)
        .output()?;
    if output.status.success() {
        Ok(())
    } else {
        let detail = String::from_utf8_lossy(&output.stderr);
        Err(std::io::Error::other(format!(
            "Failed to apply an owner-only Windows ACL: {}",
            detail.trim()
        )))
    }
}

async fn restore_database(
    storage: &StorageHandle,
    path: &Path,
    confirmation: Option<&str>,
    max_bytes: usize,
    recovery: &ControlRecoveryOptions,
) -> Result<(), ApiError> {
    if confirmation != Some(RESTORE_CONFIRMATION_PHRASE) {
        return Err(destructive_confirmation_error());
    }
    // Restore also accepts finite streams (FIFOs and shell process substitution).
    // The verifier's regular-file restriction must not apply to this path.
    let file = File::open(path).map_err(|error| {
        ApiError::BadRequest(format!(
            "Failed to read restore document '{}': {error}",
            path.display()
        ))
    })?;
    let artifact = StorageRestoreArtifact::from_reader(file, max_bytes)?;
    let (restored, _) = restore_database_artifact(storage, artifact, recovery).await?;
    println!(
        "Restore {} completed with status '{}'.",
        restored.id,
        restored.status.as_str()
    );
    Ok(())
}

async fn restore_database_artifact(
    storage: &StorageHandle,
    artifact: StorageRestoreArtifact,
    recovery: &ControlRecoveryOptions,
) -> Result<
    (
        crate::models::RestoreStageResponse,
        StorageRestoreIdentityContext,
    ),
    ApiError,
> {
    let settings = RestoreSettings::new(
        DEFAULT_RESTORE_STAGE_RETENTION_MINUTES,
        DEFAULT_RESTORE_MAX_UPLOAD_BYTES.max(artifact.summary().byte_size() as usize),
    )
    .map_err(ApiError::BadRequest)?;
    let initiator = RestoreInitiator::new(None, "system", "hubuum-admin")?;
    let request = RestoreStageRequest::new(initiator, artifact)?;
    let staged = stage_restore(storage, &settings, request).await?;
    let capability = staged.restore_capability.clone().ok_or_else(|| {
        ApiError::InternalServerError("Restore stage did not return a capability".to_string())
    })?;
    let confirmed = confirm_restore(
        storage,
        RestoreJobID::new(staged.id)?,
        &RestoreConfirmRequest {
            restore_capability: capability.clone(),
            sha256: staged.sha256,
            confirmation: RESTORE_CONFIRMATION_PHRASE.to_string(),
        },
    )
    .await?;
    let context = storage
        .get_restore_artifact(RestoreJobID::new(confirmed.id)?)
        .await?
        .identity_context()
        .map_err(|error| ApiError::Conflict(error.to_string()))?;
    if !execute_confirmed_restore_with_recovery(storage, recovery).await? {
        return Err(ApiError::Conflict(format!(
            "Restore {} was confirmed but no longer owns draining maintenance",
            confirmed.id
        )));
    }
    let restored = restore_status(storage, RestoreJobID::new(confirmed.id)?, &capability).await?;
    Ok((restored, context))
}

async fn run_restore_executor(
    storage: &StorageHandle,
    recovery: &ControlRecoveryOptions,
) -> Result<(), ApiError> {
    println!("Restore executor is ready for confirmed web restores.");
    loop {
        match execute_confirmed_restore_with_recovery(storage, recovery).await {
            Ok(true) => tracing::info!(message = "Confirmed restore completed"),
            Ok(false) => {}
            Err(error) => {
                tracing::error!(message = "Restore executor iteration failed", error = %error)
            }
        }
        tokio::select! {
            result = wait_for_restore_executor_shutdown() => {
                result?;
                println!("Restore executor stopped.");
                return Ok(());
            }
            _ = tokio::time::sleep(std::time::Duration::from_secs(1)) => {}
        }
    }
}

#[cfg(unix)]
async fn wait_for_restore_executor_shutdown() -> Result<(), ApiError> {
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .map_err(|error| {
        ApiError::InternalServerError(format!(
            "Failed to install restore executor signal handler: {error}"
        ))
    })?;
    tokio::select! {
        result = tokio::signal::ctrl_c() => result.map_err(|error| {
            ApiError::InternalServerError(format!("Restore executor signal error: {error}"))
        }),
        _ = terminate.recv() => Ok(()),
    }
}

#[cfg(not(unix))]
async fn wait_for_restore_executor_shutdown() -> Result<(), ApiError> {
    tokio::signal::ctrl_c().await.map_err(|error| {
        ApiError::InternalServerError(format!("Restore executor signal error: {error}"))
    })
}

fn destructive_confirmation_error() -> ApiError {
    ApiError::BadRequest(format!(
        "WARNING: restore deletes and replaces all Hubuum application data. \
         Re-run with --restore-confirmation '{RESTORE_CONFIRMATION_PHRASE}'"
    ))
}

async fn storage_ready(storage: &StorageHandle) -> Result<(), ApiError> {
    let readiness = storage.get_readiness_snapshot().await?;
    if !readiness.storage_is_ready() {
        return Err(ApiError::ServiceUnavailable(
            "Storage backend schema is not ready".to_string(),
        ));
    }
    println!("Storage backend is ready and all required migrations are applied.");
    Ok(())
}

async fn reset_password(storage: &StorageHandle, username: &str) -> Result<(), ApiError> {
    let new_password = generate_random_password(32);
    identity_service::reset_local_password(storage, username, new_password.clone()).await?;
    println!("Password for user {username} reset to: {new_password}");
    Ok(())
}

async fn audit_templates(
    storage: &StorageHandle,
    export_template_recursion_limit: usize,
    export_template_fuel: u64,
) -> Result<(), ApiError> {
    let templates = operational_service::load_export_templates_for_audit(storage).await?;
    let mut failures = Vec::new();

    for template in &templates {
        let collection_templates = templates
            .iter()
            .filter(|candidate| {
                candidate.collection_id() == template.collection_id()
                    && candidate.id() != template.id()
            })
            .map(|candidate| {
                (
                    candidate.name().to_string(),
                    candidate.template().to_string(),
                )
            })
            .collect::<Vec<_>>();
        let content_type = ExportContentType::from_mime(template.content_type())?;
        if let Err(error) = validate_template_sources_with_limits(
            template.name(),
            template.template(),
            &collection_templates,
            content_type,
            export_template_recursion_limit,
            export_template_fuel,
        )
        .await
        {
            failures.push((
                template.collection_id(),
                template.name().to_string(),
                error.to_string(),
            ));
        }
    }

    if failures.is_empty() {
        println!("All export templates validated successfully.");
        return Ok(());
    }

    for (collection_id, template_name, error) in &failures {
        println!("collection={collection_id} template={template_name}: {error}");
    }

    Err(ApiError::BadRequest(format!(
        "{} template(s) failed validation",
        failures.len()
    )))
}

async fn load_export_template_health(storage: &StorageHandle) -> Result<(), ApiError> {
    let health = operational_service::load_export_template_health(storage).await?;
    if health.is_empty() {
        println!("No stored export outputs found.");
        return Ok(());
    }

    println!("Export template health:");
    for row in &health {
        let template_name = row.template_name().unwrap_or("<json output>");
        let avg_warnings = row.warning_total() as f64 / row.runs() as f64;
        let avg_total_duration_ms = row.total_duration_ms_total() as f64 / row.runs() as f64;
        println!(
            "template={} runs={} avg_warning_count={:.2} max_warning_count={} avg_total_duration_ms={:.2} max_total_duration_ms={}",
            template_name,
            row.runs(),
            avg_warnings,
            row.warning_max(),
            avg_total_duration_ms,
            row.total_duration_ms_max()
        );
    }

    println!("\nWarning-prone templates:");
    for row in health.iter().filter(|row| row.warning_total() > 0) {
        println!(
            "template={} warning_runs={} max_warning_count={}",
            row.template_name().unwrap_or("<json output>"),
            row.runs(),
            row.warning_max()
        );
    }

    println!("\nSlow templates:");
    for row in health.iter().filter(|row| row.total_duration_ms_max() > 0) {
        println!(
            "template={} avg_total_duration_ms={:.2} max_total_duration_ms={}",
            row.template_name().unwrap_or("<json output>"),
            row.total_duration_ms_total() as f64 / row.runs() as f64,
            row.total_duration_ms_max()
        );
    }

    Ok(())
}

fn init_logging(log_level: &str, report_on_stdout: bool) {
    if !is_valid_log_level(log_level) {
        fatal_error(
            &format!("Invalid log level: {log_level}"),
            EXIT_CODE_CONFIG_ERROR,
        );
    }
    let result = if report_on_stdout {
        logger::init_json_logging_to_stderr(log_level)
    } else {
        logger::init_json_logging(log_level)
    };
    if let Err(err) = result {
        fatal_error(&err, EXIT_CODE_CONFIG_ERROR);
    }
}

#[cfg(test)]
mod tests {
    use crate::models::{BackupDocument, BackupManifest, BackupState, CURRENT_BACKUP_VERSION};
    use crate::storage::StorageBackupBudget;
    use chrono::Utc;
    use clap::Parser;
    #[cfg(feature = "embedded-migrations")]
    use clap::{CommandFactory, FromArgMatches};
    use hubuum_domain::ClassId;
    use rstest::rstest;
    use uuid::Uuid;

    #[cfg(feature = "embedded-migrations")]
    use super::effective_database_role_mode;
    use super::{
        AdminCli, DEFAULT_DATABASE_MIGRATOR_ROLE, DEFAULT_DATABASE_OWNER_ROLE,
        DEFAULT_DATABASE_RUNTIME_ROLE, DatabaseRoleMode, StorageBackendKind,
        StorageDatabaseRoleNames, resolve_database_roles,
    };

    fn history_cleanup_arguments() -> Vec<String> {
        vec![
            "hubuum-admin",
            "--delete-object-history-version",
            "a1db7312-acb1-4a4e-8797-bf5bc55d47ec",
            "--history-resource",
            "f92b92b2-db30-4501-9678-a14b9780a12f",
            "--deletion-intent",
            "bcdf80ed-4ba2-435a-ab11-db89ead89a33",
            "--control-repository",
            "/independent/control",
            "--control-checkpoint-key",
            "/independent-secrets/control.key",
        ]
        .into_iter()
        .map(str::to_owned)
        .collect()
    }

    #[rstest]
    #[case("--history-resource")]
    #[case("--deletion-intent")]
    #[case("--control-repository")]
    #[case("--control-checkpoint-key")]
    fn history_cleanup_requires_exact_scope_and_recovery_configuration(#[case] missing: &str) {
        let mut arguments = history_cleanup_arguments();
        let position = arguments
            .iter()
            .position(|argument| *argument == missing)
            .unwrap();
        arguments.drain(position..=position + 1);
        let error = AdminCli::try_parse_from(arguments)
            .err()
            .expect("required argument");
        assert_eq!(
            error.kind(),
            clap::error::ErrorKind::MissingRequiredArgument
        );
    }

    #[rstest]
    #[case("--deletion-intent", "00000000-0000-0000-0000-000000000000")]
    #[case("--history-resource", "1")]
    #[case(
        "--delete-object-history-version",
        "00000000-0000-0000-0000-000000000000"
    )]
    fn history_cleanup_rejects_invalid_identities_before_database_access(
        #[case] argument: &str,
        #[case] invalid: &str,
    ) {
        let mut arguments = history_cleanup_arguments();
        let position = arguments
            .iter()
            .position(|value| *value == argument)
            .unwrap();
        arguments[position + 1] = invalid.to_owned();
        let error = AdminCli::try_parse_from(arguments)
            .err()
            .expect("invalid identity");
        assert_eq!(error.kind(), clap::error::ErrorKind::ValueValidation);
    }

    #[rstest]
    #[case("--backup", "backup.json")]
    #[case("--recover-control", "/independent/control")]
    #[case("--control-status", "/independent/control")]
    fn history_cleanup_cannot_run_with_another_administrator_operation(
        #[case] argument: &str,
        #[case] value: &str,
    ) {
        let mut arguments = history_cleanup_arguments();
        arguments.extend([argument.to_owned(), value.to_owned()]);
        let error = AdminCli::try_parse_from(arguments)
            .err()
            .expect("conflicting operation");
        assert_eq!(error.kind(), clap::error::ErrorKind::ArgumentConflict);
    }

    #[test]
    fn failed_streaming_backup_preserves_the_destination_and_removes_partial_output() {
        let directory = std::env::temp_dir().join(format!("backup-write-{}", Uuid::new_v4()));
        std::fs::create_dir(&directory).unwrap();
        let path = directory.join("backup.json");
        std::fs::write(&path, b"previous backup").unwrap();
        let document = BackupDocument {
            backup_version: CURRENT_BACKUP_VERSION,
            created_at: Utc::now(),
            source_version: "test".into(),
            state: BackupState::default(),
            history: None,
            manifest: BackupManifest::default(),
        };
        let result =
            super::write_backup_file(&path, &document, StorageBackupBudget::new(16, 1).unwrap());
        let contents = std::fs::read(&path).unwrap();
        let files = std::fs::read_dir(&directory).unwrap().count();
        std::fs::remove_dir_all(&directory).unwrap();
        assert!(matches!(result, Err(super::ApiError::PayloadTooLarge(_))));
        assert_eq!(contents, b"previous backup");
        assert_eq!(files, 1, "failed writes must not retain a partial artifact");
    }

    #[test]
    fn storage_backend_selection_defaults_only_for_an_empty_value() {
        let empty = AdminCli::try_parse_from(["hubuum-admin", "--storage-backend", ""])
            .expect("an empty storage backend should select the default");
        let unsupported =
            match AdminCli::try_parse_from(["hubuum-admin", "--storage-backend", "unsupported"]) {
                Ok(_) => panic!("an unregistered storage backend must be rejected"),
                Err(error) => error,
            };

        assert_eq!(empty.storage_backend, StorageBackendKind::Postgres);
        assert_eq!(unsupported.kind(), clap::error::ErrorKind::InvalidValue);
    }

    #[rstest::rstest]
    #[case::bytes("--backup-max-output-bytes", 1048576)]
    #[case::rows("--backup-max-capture-rows", 42)]
    fn backup_capture_limits_accept_operator_overrides(#[case] flag: &str, #[case] value: usize) {
        let cli = AdminCli::try_parse_from(["hubuum-admin", flag, &value.to_string()]).unwrap();
        let configured = match flag {
            "--backup-max-output-bytes" => cli.backup_max_output_bytes,
            _ => cli.backup_max_capture_rows,
        };
        assert_eq!(configured, value);
    }

    #[test]
    fn backup_verification_limit_accepts_an_explicit_operator_override() {
        let cli =
            AdminCli::try_parse_from(["hubuum-admin", "--restore-max-upload-bytes", "1073741824"])
                .unwrap();

        assert_eq!(cli.restore_max_upload_bytes, 1_073_741_824);
    }

    #[test]
    fn absent_database_role_configuration_uses_documented_defaults() {
        let roles = resolve_database_roles(None, None, None).unwrap();

        assert_eq!(
            roles,
            StorageDatabaseRoleNames::new(
                DEFAULT_DATABASE_OWNER_ROLE,
                DEFAULT_DATABASE_MIGRATOR_ROLE,
                DEFAULT_DATABASE_RUNTIME_ROLE,
            )
            .unwrap()
        );
    }

    #[test]
    fn database_role_configuration_overrides_individual_defaults() {
        let roles = resolve_database_roles(None, None, Some("existing_hubuum_login")).unwrap();

        assert_eq!(roles.runtime(), "existing_hubuum_login");
    }

    #[cfg(feature = "embedded-migrations")]
    fn parse_legacy_migration_cli(arguments: &[&str]) -> Result<AdminCli, clap::Error> {
        // These cases exercise explicit CLI arguments independently of the
        // split-role environment configured by the database test runner.
        let mut command = AdminCli::command();
        for argument in [
            "database_owner_role",
            "database_migrator_role",
            "database_runtime_role",
        ] {
            command = command.mut_arg(argument, |argument| argument.env(None::<&str>));
        }
        let matches = command.try_get_matches_from(arguments)?;
        AdminCli::from_arg_matches(&matches)
    }

    #[cfg(feature = "embedded-migrations")]
    #[test]
    fn legacy_single_role_migration_requires_migrate() {
        let error =
            match parse_legacy_migration_cli(&["hubuum-admin", "--legacy-single-role-migration"]) {
                Ok(_) => panic!("the compatibility bridge must require --migrate"),
                Err(error) => error,
            };

        assert_eq!(
            error.kind(),
            clap::error::ErrorKind::MissingRequiredArgument
        );
    }

    #[cfg(feature = "embedded-migrations")]
    #[test]
    fn legacy_single_role_migration_rejects_split_role_names() {
        let error = match parse_legacy_migration_cli(&[
            "hubuum-admin",
            "--migrate",
            "--legacy-single-role-migration",
            "--database-runtime-role",
            "existing_hubuum_login",
        ]) {
            Ok(_) => panic!("legacy migration must not accept split-role configuration"),
            Err(error) => error,
        };

        assert_eq!(error.kind(), clap::error::ErrorKind::ArgumentConflict);
    }

    #[cfg(feature = "embedded-migrations")]
    #[test]
    fn legacy_single_role_migration_rejects_split_mode() {
        let cli = parse_legacy_migration_cli(&[
            "hubuum-admin",
            "--migrate",
            "--legacy-single-role-migration",
            "--database-role-mode",
            "split",
        ])
        .unwrap();

        let error = effective_database_role_mode(&cli).unwrap_err();

        assert!(error.to_string().contains("cannot be combined"));
    }

    #[test]
    fn database_role_mode_accepts_explicit_single_and_split_values() {
        for (value, expected) in [
            ("single", DatabaseRoleMode::Single),
            ("split", DatabaseRoleMode::Split),
        ] {
            let cli =
                AdminCli::try_parse_from(["hubuum-admin", "--database-role-mode", value]).unwrap();

            assert_eq!(cli.database_role_mode, expected);
        }
    }
    #[rstest::rstest]
    #[case("--backup", "output.json")]
    #[case("--restore", "input.json")]
    #[case("--verify-backup", "input.json")]
    #[test]
    fn query_usage_execution_is_an_independent_admin_action(
        #[case] flag: &str,
        #[case] value: &str,
    ) {
        assert!(
            AdminCli::try_parse_from(["hubuum-admin", "--reconcile-query-usage", flag, value])
                .is_err()
        );
    }

    #[test]
    fn query_usage_execution_accepts_a_validated_class_scope() {
        let cli = AdminCli::try_parse_from([
            "hubuum-admin",
            "--reconcile-query-usage",
            "--query-usage-class",
            "42",
        ])
        .unwrap();
        assert!(cli.reconcile_query_usage);
        assert_eq!(cli.query_usage_class, Some(ClassId::new(42).unwrap()));
        assert!(
            AdminCli::try_parse_from([
                "hubuum-admin",
                "--reconcile-query-usage",
                "--query-usage-class",
                "0"
            ])
            .is_err()
        );
    }
}
