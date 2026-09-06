use clap::{Parser, ValueEnum};
use serde::Serialize;
use std::collections::BTreeMap;
use std::ffi::OsString;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use uuid::Uuid;

use crate::backups::create_backup_document;
use crate::config::{
    DEFAULT_DB_POOL_ACQUIRE_TIMEOUT_MS, DEFAULT_DB_STATEMENT_TIMEOUT_MS,
    DEFAULT_EXPORT_TEMPLATE_FUEL, DEFAULT_EXPORT_TEMPLATE_RECURSION_LIMIT,
    DEFAULT_RESTORE_MAX_UPLOAD_BYTES, DEFAULT_RESTORE_STAGE_RETENTION_MINUTES,
    DEFAULT_TOKEN_LIFETIME_HOURS, DatabaseRoleMode, token_hash_key_ring,
};
#[cfg(feature = "embedded-migrations")]
use crate::errors::EXIT_CODE_DATABASE_ERROR;
use crate::errors::{ApiError, EXIT_CODE_CONFIG_ERROR, fatal_error};
use crate::logger;
#[cfg(feature = "embedded-migrations")]
use crate::models::BackupDocument;
use crate::models::{
    BackupRequest, ExportContentType, RESTORE_CONFIRMATION_PHRASE, RestoreConfirmRequest,
    RestoreInitiator, RestoreJobID, RestoreStageRequest,
};
#[cfg(feature = "embedded-migrations")]
use crate::restores::verify_restored_backup_matches;
use crate::restores::{
    BackupVerificationReport, RestoreSettings, confirm_restore, execute_confirmed_restore,
    restore_status, stage_restore, verify_backup_document,
};
use crate::services::identity as identity_service;
use crate::services::operational_administration as operational_service;
#[cfg(feature = "embedded-migrations")]
use crate::storage::reset_disposable_restore_database;
use crate::storage::{
    OperationalStateStorage, StorageBackendKind, StorageDatabasePrivilegeReport,
    StorageDatabaseRole, StorageDatabaseRoleNames, StorageHandle, StorageSettings,
    StorageTokenKeyUsage, StorageTokenObservation, TokenStorage, initialize_storage,
    inspect_storage_database_privileges, storage_database_role_grants_sql,
    storage_database_role_setup_sql,
};
#[cfg(feature = "embedded-migrations")]
use crate::storage::{prepare_disposable_restore_database, run_storage_migrations};
use crate::utilities::auth::generate_random_password;
use crate::utilities::exporting::validate_template_sources_with_limits;
use crate::utilities::is_valid_log_level;

const DEFAULT_DATABASE_OWNER_ROLE: &str = "hubuum_owner";
const DEFAULT_DATABASE_MIGRATOR_ROLE: &str = "hubuum_migrator";
const DEFAULT_DATABASE_RUNTIME_ROLE: &str = "hubuum_runtime";

#[derive(Parser)]
#[command(
    author = "Terje Kvernes <terje@kvernes.no>",
    version = env!("CARGO_PKG_VERSION"),
    about = "Admin CLI for Hubuum",
    long_about = None
)]
struct AdminCli {
    /// Write a consistent full-system backup document to this path
    #[arg(long, value_name = "PATH")]
    backup: Option<PathBuf>,

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

    /// Maximum backup document size accepted by offline and isolated verification
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
    let admin_cli = AdminCli::parse();
    init_logging(&admin_cli.log_level);

    if let Some(path) = admin_cli.verify_backup.as_deref() {
        verify_backup_file(BackupVerificationOptions {
            path,
            restore_test_database_url: admin_cli.restore_test_database_url.as_deref(),
            configured_database_url: admin_cli.database_url.as_deref(),
            configured_migration_database_url: admin_cli.migration_database_url.as_deref(),
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
    let migration_requested = admin_cli.migrate;
    #[cfg(not(feature = "embedded-migrations"))]
    let migration_requested = false;
    #[cfg(feature = "embedded-migrations")]
    let database_role_mode = effective_database_role_mode(&admin_cli)?;
    #[cfg(not(feature = "embedded-migrations"))]
    let database_role_mode = admin_cli.database_role_mode;
    let privileged_database_operation =
        migration_requested || admin_cli.restore.is_some() || admin_cli.restore_executor;
    let configured_database_url = admin_cli
        .database_url
        .as_deref()
        .filter(|url| !url.trim().is_empty());
    let configured_migration_database_url = admin_cli
        .migration_database_url
        .as_deref()
        .filter(|url| !url.trim().is_empty());
    let database_url = if privileged_database_operation && database_role_mode.uses_split_roles() {
        configured_migration_database_url.map(str::to_owned)
    } else if privileged_database_operation {
        configured_migration_database_url
            .or(configured_database_url)
            .map(str::to_owned)
    } else {
        configured_database_url.map(str::to_owned)
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
                    &format!("{variable} must be set if not provided as an argument"),
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
    };

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

    if admin_cli.restore_executor {
        if !matches!(admin_cli.storage_backend, StorageBackendKind::Postgres) {
            return Err(ApiError::BadRequest(
                "The restore executor requires the PostgreSQL storage backend".to_string(),
            ));
        }
        run_restore_executor(&storage).await?;
    } else if let Some(path) = admin_cli.backup {
        backup_database(&storage, &path, !admin_cli.backup_without_history).await?;
    } else if let Some(path) = admin_cli.restore {
        restore_database(&storage, &path, admin_cli.restore_confirmation.as_deref()).await?;
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

    Ok(())
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
) -> Result<(), ApiError> {
    let document = create_backup_document(storage, &BackupRequest { include_history }).await?;
    let bytes = serde_json::to_vec_pretty(&document)?;
    write_backup_file(path, &bytes).map_err(|error| {
        ApiError::InternalServerError(format!(
            "Failed to write backup to '{}': {error}",
            path.display()
        ))
    })?;
    println!(
        "Wrote consistent backup to '{}' ({} bytes).",
        path.display(),
        bytes.len()
    );
    Ok(())
}

fn read_backup_file(path: &Path, max_bytes: usize) -> Result<Vec<u8>, ApiError> {
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
    let read_limit = u64::try_from(max_bytes)
        .unwrap_or(u64::MAX - 1)
        .saturating_add(1);
    let mut bytes = Vec::with_capacity(
        usize::try_from(metadata.len())
            .unwrap_or(max_bytes)
            .min(max_bytes),
    );
    file.take(read_limit)
        .read_to_end(&mut bytes)
        .map_err(|error| {
            ApiError::BadRequest(format!(
                "Failed to read backup document '{}': {error}",
                path.display()
            ))
        })?;
    if bytes.len() > max_bytes {
        return Err(ApiError::PayloadTooLarge(format!(
            "Backup document exceeds the configured {max_bytes} byte limit"
        )));
    }
    Ok(bytes)
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
    path: &'a Path,
    restore_test_database_url: Option<&'a str>,
    configured_database_url: Option<&'a str>,
    configured_migration_database_url: Option<&'a str>,
    storage_backend: StorageBackendKind,
    statement_timeout_ms: u64,
    keep_restore_test_database: bool,
    max_upload_bytes: usize,
    json: bool,
}

async fn verify_backup_file(
    options: BackupVerificationOptions<'_>,
) -> Result<BackupVerificationReport, ApiError> {
    let bytes = read_backup_file(options.path, options.max_upload_bytes)?;
    let report = verify_backup_document(&bytes, options.max_upload_bytes)?;
    let report = if let Some(database_url) = options.restore_test_database_url {
        if !matches!(options.storage_backend, StorageBackendKind::Postgres) {
            return Err(ApiError::BadRequest(
                "Isolated restore verification requires the PostgreSQL storage backend".to_string(),
            ));
        }
        let target = admin_storage_settings(database_url, options.statement_timeout_ms)?;
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
        verify_backup_restore(report, bytes, target, options.keep_restore_test_database).await?
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
    bytes: Vec<u8>,
    target: StorageSettings,
    keep_restore_test_database: bool,
) -> Result<BackupVerificationReport, ApiError> {
    let migrations_applied = prepare_disposable_restore_database(&target)?;
    let verification = async {
        let storage = initialize_storage(&target)?;
        let source: BackupDocument = serde_json::from_slice(&bytes)?;
        let started_at = std::time::Instant::now();
        restore_database_bytes(&storage, bytes).await?;
        let restore_duration = started_at.elapsed();
        let readiness = storage.get_readiness_snapshot().await?;
        if !readiness.storage_is_ready() {
            return Err(ApiError::ServiceUnavailable(
                "Restored disposable database did not pass storage readiness".to_string(),
            ));
        }
        let restored = create_backup_document(
            &storage,
            &BackupRequest {
                include_history: source.history.is_some(),
            },
        )
        .await?;
        verify_restored_backup_matches(&source, &restored)?;
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
    _bytes: Vec<u8>,
    _target: StorageSettings,
    _keep_restore_test_database: bool,
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

fn write_backup_file(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let (temporary_path, mut file) = create_backup_temporary_file(path)?;
    let write_result = file.write_all(bytes).and_then(|_| file.sync_all());
    drop(file);

    if let Err(error) = write_result {
        let _ = std::fs::remove_file(&temporary_path);
        return Err(error);
    }

    if let Err(error) = replace_backup_file(&temporary_path, path) {
        let _ = std::fs::remove_file(&temporary_path);
        return Err(error);
    }

    Ok(())
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
) -> Result<(), ApiError> {
    if confirmation != Some(RESTORE_CONFIRMATION_PHRASE) {
        return Err(destructive_confirmation_error());
    }
    // Restore also accepts finite streams (FIFOs and shell process substitution).
    // The verifier's regular-file restriction must not apply to this path.
    let bytes = std::fs::read(path).map_err(|error| {
        ApiError::BadRequest(format!(
            "Failed to read restore document '{}': {error}",
            path.display()
        ))
    })?;
    let restored = restore_database_bytes(storage, bytes).await?;
    println!(
        "Restore {} completed with status '{}'.",
        restored.id,
        restored.status.as_str()
    );
    Ok(())
}

async fn restore_database_bytes(
    storage: &StorageHandle,
    bytes: Vec<u8>,
) -> Result<crate::models::RestoreStageResponse, ApiError> {
    let settings = RestoreSettings::new(
        DEFAULT_RESTORE_STAGE_RETENTION_MINUTES,
        DEFAULT_RESTORE_MAX_UPLOAD_BYTES.max(bytes.len()),
    )
    .map_err(ApiError::BadRequest)?;
    let initiator = RestoreInitiator::new(None, "system", "hubuum-admin")?;
    let request = RestoreStageRequest::new(initiator, bytes)?;
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
    if !execute_confirmed_restore(storage).await? {
        return Err(ApiError::Conflict(format!(
            "Restore {} was confirmed but no longer owns draining maintenance",
            confirmed.id
        )));
    }
    let restored = restore_status(storage, RestoreJobID::new(confirmed.id)?, &capability).await?;
    Ok(restored)
}

async fn run_restore_executor(storage: &StorageHandle) -> Result<(), ApiError> {
    println!("Restore executor is ready for confirmed web restores.");
    loop {
        match execute_confirmed_restore(storage).await {
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
        ) {
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

fn init_logging(log_level: &str) {
    if !is_valid_log_level(log_level) {
        fatal_error(
            &format!("Invalid log level: {log_level}"),
            EXIT_CODE_CONFIG_ERROR,
        );
    }
    if let Err(err) = logger::init_json_logging(log_level) {
        fatal_error(&err, EXIT_CODE_CONFIG_ERROR);
    }
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    #[cfg(feature = "embedded-migrations")]
    use super::effective_database_role_mode;
    use super::{
        AdminCli, DEFAULT_DATABASE_MIGRATOR_ROLE, DEFAULT_DATABASE_OWNER_ROLE,
        DEFAULT_DATABASE_RUNTIME_ROLE, DatabaseRoleMode, StorageBackendKind,
        StorageDatabaseRoleNames, resolve_database_roles,
    };

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
    #[test]
    fn legacy_single_role_migration_requires_migrate() {
        let error =
            match AdminCli::try_parse_from(["hubuum-admin", "--legacy-single-role-migration"]) {
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
        let error = match AdminCli::try_parse_from([
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
        let cli = AdminCli::try_parse_from([
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
}
