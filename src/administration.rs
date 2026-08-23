use clap::Parser;
use std::ffi::OsString;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use uuid::Uuid;

use crate::backups::create_backup_document;
use crate::config::{
    DEFAULT_DB_POOL_ACQUIRE_TIMEOUT_MS, DEFAULT_DB_STATEMENT_TIMEOUT_MS,
    DEFAULT_EXPORT_TEMPLATE_FUEL, DEFAULT_EXPORT_TEMPLATE_RECURSION_LIMIT,
    DEFAULT_RESTORE_MAX_UPLOAD_BYTES, DEFAULT_RESTORE_STAGE_RETENTION_MINUTES,
};
#[cfg(feature = "embedded-migrations")]
use crate::errors::EXIT_CODE_DATABASE_ERROR;
use crate::errors::{ApiError, EXIT_CODE_CONFIG_ERROR, fatal_error};
use crate::logger;
use crate::models::{
    BackupRequest, ExportContentType, RESTORE_CONFIRMATION_PHRASE, RestoreConfirmRequest,
    RestoreInitiator, RestoreJobID, RestoreStageRequest,
};
use crate::restores::{RestoreSettings, confirm_restore, stage_restore};
use crate::services::identity as identity_service;
use crate::services::operational_administration as operational_service;
#[cfg(feature = "embedded-migrations")]
use crate::storage::run_storage_migrations;
use crate::storage::{
    OperationalStateStorage, StorageBackendKind, StorageHandle, StorageSettings, initialize_storage,
};
use crate::utilities::auth::generate_random_password;
use crate::utilities::exporting::validate_template_sources_with_limits;
use crate::utilities::is_valid_log_level;

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

    /// Destructively replace all application data from this backup document
    #[arg(long, value_name = "PATH", conflicts_with = "backup")]
    restore: Option<PathBuf>,

    /// Exact destructive confirmation phrase required with --restore
    #[arg(long, value_name = "PHRASE", requires = "restore")]
    restore_confirmation: Option<String>,

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

    /// Run all pending embedded database migrations
    #[cfg(feature = "embedded-migrations")]
    #[arg(long, default_value_t = false)]
    migrate: bool,

    /// Storage adapter compiled into this application build.
    #[arg(
        long,
        env = "HUBUUM_STORAGE_BACKEND",
        value_enum,
        default_value = "postgresql"
    )]
    storage_backend: StorageBackendKind,

    /// Database URL
    #[arg(long, env = "HUBUUM_DATABASE_URL")]
    database_url: Option<String>,

    /// Pool-global storage query timeout in milliseconds (0 disables it)
    #[arg(
        long,
        env = "HUBUUM_DB_STATEMENT_TIMEOUT_MS",
        default_value_t = DEFAULT_DB_STATEMENT_TIMEOUT_MS
    )]
    db_statement_timeout_ms: u64,

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

/// Parse and execute one Hubuum administration command.
///
/// This is the workspace-internal boundary used by `hubuum-admin`; callers
/// should use the CLI rather than depending on server persistence types.
pub async fn run_admin_from_environment() -> Result<(), ApiError> {
    let admin_cli = AdminCli::parse();
    init_logging(&admin_cli.log_level);

    if admin_cli.restore.is_some()
        && admin_cli.restore_confirmation.as_deref() != Some(RESTORE_CONFIRMATION_PHRASE)
    {
        return Err(destructive_confirmation_error());
    }

    let storage_settings = match admin_cli.storage_backend {
        StorageBackendKind::Postgres => {
            let database_url = admin_cli.database_url.unwrap_or_else(|| {
                std::env::var("HUBUUM_DATABASE_URL").unwrap_or_else(|_| {
                    fatal_error(
                        "HUBUUM_DATABASE_URL must be set if not provided as an argument",
                        EXIT_CODE_CONFIG_ERROR,
                    )
                })
            });
            StorageSettings::postgres(database_url)
                .max_connections(1)
                .statement_timeout_ms(admin_cli.db_statement_timeout_ms)
                .acquire_timeout_ms(DEFAULT_DB_POOL_ACQUIRE_TIMEOUT_MS)
                .build()?
        }
    };

    #[cfg(feature = "embedded-migrations")]
    if admin_cli.migrate {
        let applied = run_storage_migrations(&storage_settings).unwrap_or_else(|error| {
            fatal_error(
                &format!("Failed to run storage migrations: {error}"),
                EXIT_CODE_DATABASE_ERROR,
            )
        });
        println!("Applied {applied} storage migration(s).");
        return Ok(());
    }

    let storage = initialize_storage(&storage_settings)?;

    if let Some(path) = admin_cli.backup {
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
    } else {
        println!("No command specified. Use --help for usage information.");
    }

    Ok(())
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
    let bytes = std::fs::read(path).map_err(|error| {
        ApiError::BadRequest(format!(
            "Failed to read restore document '{}': {error}",
            path.display()
        ))
    })?;
    let settings = RestoreSettings::new(
        DEFAULT_RESTORE_STAGE_RETENTION_MINUTES,
        DEFAULT_RESTORE_MAX_UPLOAD_BYTES.max(bytes.len()),
    )
    .map_err(ApiError::BadRequest)?;
    let initiator = RestoreInitiator::new(None, "system", "hubuum-admin")?;
    let request = RestoreStageRequest::new(initiator, bytes)?;
    let staged = stage_restore(storage, &settings, request).await?;
    let capability = staged.restore_capability.ok_or_else(|| {
        ApiError::InternalServerError("Restore stage did not return a capability".to_string())
    })?;
    let restored = confirm_restore(
        storage,
        RestoreJobID::new(staged.id)?,
        &RestoreConfirmRequest {
            restore_capability: capability,
            sha256: staged.sha256,
            confirmation: RESTORE_CONFIRMATION_PHRASE.to_string(),
        },
    )
    .await?;
    println!(
        "Restore {} completed with status '{}'.",
        restored.id,
        restored.status.as_str()
    );
    Ok(())
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

    use super::{AdminCli, StorageBackendKind};

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
}
