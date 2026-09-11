use std::path::PathBuf;
use std::process::{Command, Output};
use std::sync::atomic::{AtomicU64, Ordering};

use chrono::{Duration, Utc};
use diesel::insert_into;
use hubuum::config::DEFAULT_DB_STATEMENT_TIMEOUT_MS;
#[cfg(feature = "embedded-migrations")]
use hubuum::errors::{EXIT_CODE_CONFIG_ERROR, EXIT_CODE_DATABASE_ERROR};
use hubuum::models::NewUser;
use hubuum::models::identity::{LOCAL_IDENTITY_SCOPE, LOCAL_PROVIDER_KIND};
use hubuum::schema::{collections, export_templates, identity_scopes, principals, users};
use hubuum::test_support::postgres_test_pool_with_timeout;
use hubuum::utilities::auth::verify_password;
use hubuum_storage_postgres::diesel_async_prelude::*;
use hubuum_storage_postgres::{PostgresPool, with_connection};

static NEXT_TEST_ID: AtomicU64 = AtomicU64::new(1);

fn admin_binary() -> &'static str {
    env!("CARGO_BIN_EXE_hubuum-admin")
}

fn database_url() -> String {
    std::env::var("HUBUUM_DATABASE_URL")
        .expect("HUBUUM_DATABASE_URL must point to the migrated test database")
}

fn database_pool(database_url: &str) -> PostgresPool {
    postgres_test_pool_with_timeout(database_url, 2, DEFAULT_DB_STATEMENT_TIMEOUT_MS)
}

fn admin_command(database_url: &str) -> Command {
    let mut command = Command::new(admin_binary());
    command.args(["--database-url", database_url]);
    command
}

fn unique_name(prefix: &str) -> String {
    let sequence = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
    format!("{prefix}_{}_{sequence}", std::process::id())
}

fn assert_command_succeeded(output: &Output) {
    assert!(
        output.status.success(),
        "command failed with {:?}\nstdout:\n{}\nstderr:\n{}",
        output.status.code(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
}

struct SecretDirectory(PathBuf);

impl SecretDirectory {
    fn new() -> Self {
        let root = std::env::temp_dir().join(unique_name("admin_secrets"));
        std::fs::create_dir_all(root.join("database")).unwrap();
        Self(root)
    }

    fn write(&self, name: &str, value: &str) {
        std::fs::write(self.0.join("database").join(name), value).unwrap();
    }

    fn command(&self) -> Command {
        let mut command = Command::new(admin_binary());
        command
            .env("HUBUUM_SECRET_SOURCE", "file")
            .env("HUBUUM_SECRET_FILE_ROOT", &self.0)
            .env_remove("HUBUUM_DATABASE_URL")
            .env_remove("HUBUUM_MIGRATION_DATABASE_URL");
        command
    }
}

impl Drop for SecretDirectory {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}

#[rstest::rstest]
#[case::environment(false)]
#[case::command_line(true)]
fn database_readiness_uses_file_credentials(#[case] use_cli: bool) {
    let secrets = SecretDirectory::new();
    secrets.write("url", &database_url());
    let mut command = secrets.command();
    command.env(
        "HUBUUM_DATABASE_URL",
        "unsupported://ignored-environment/db",
    );
    if use_cli {
        command
            .env("HUBUUM_SECRET_SOURCE", "environment")
            .env("HUBUUM_SECRET_FILE_ROOT", "/missing-environment-root")
            .args(["--secret-source", "file", "--secret-file-root"])
            .arg(&secrets.0);
    }
    let output = command.arg("--database-ready").output().unwrap();
    assert_command_succeeded(&output);
}

#[test]
fn explicit_database_url_overrides_file_credentials() {
    let secrets = SecretDirectory::new();
    secrets.write("url", "unsupported://ignored-file/db");
    let output = secrets
        .command()
        .args(["--database-ready", "--database-url", &database_url()])
        .output()
        .unwrap();
    assert_command_succeeded(&output);
}

#[test]
fn explicit_environment_source_overrides_file_environment() {
    let secrets = SecretDirectory::new();
    secrets.write("url", "unsupported://ignored-file/db");
    let output = secrets
        .command()
        .env("HUBUUM_DATABASE_URL", database_url())
        .args(["--database-ready", "--secret-source", "environment"])
        .output()
        .unwrap();
    assert_command_succeeded(&output);
}

#[test]
fn missing_database_file_does_not_use_environment_credentials() {
    let secrets = SecretDirectory::new();
    let output = secrets
        .command()
        .env("HUBUUM_DATABASE_URL", database_url())
        .arg("--database-ready")
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("database/url"));
}

#[rstest::rstest]
#[case::single("single", "url")]
#[case::single_override("single", "migration-url")]
#[case::split("split", "migration-url")]
fn restore_executor_resolves_the_topology_specific_file(
    #[case] mode: &str,
    #[case] filename: &str,
) {
    let secrets = SecretDirectory::new();
    // A deliberately unsupported scheme proves the chosen file reaches storage
    // configuration without ever starting an executor or touching the database.
    secrets.write(filename, "unsupported://file-credential-canary/db");
    let output = secrets
        .command()
        .args(["--restore-executor", "--database-role-mode", mode])
        .output()
        .unwrap();
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("Unsupported database type"));
    assert!(!stderr.contains("file-credential-canary"));
}

#[test]
fn split_restore_executor_requires_a_migration_file() {
    let secrets = SecretDirectory::new();
    secrets.write("url", &database_url());
    let output = secrets
        .command()
        .env("HUBUUM_MIGRATION_DATABASE_URL", database_url())
        .args(["--restore-executor", "--database-role-mode", "split"])
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("database/migration-url"));
}

#[rstest::rstest]
#[case::runtime("url")]
#[case::migration("migration-url")]
fn backup_restore_verification_rejects_file_backed_production_targets(#[case] filename: &str) {
    let secrets = SecretDirectory::new();
    let url = database_url();
    let backup = secrets.0.join("backup.json");
    let output = admin_command(&url)
        .arg("--backup")
        .arg(&backup)
        .output()
        .unwrap();
    assert_command_succeeded(&output);
    secrets.write(filename, &url);
    let output = secrets
        .command()
        .arg("--verify-backup")
        .arg(&backup)
        .args(["--restore-test-database-url", &url])
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr)
            .contains("target matches a configured Hubuum database")
    );
}

#[test]
fn admin_help_exposes_reset_password() {
    let output = Command::new(admin_binary())
        .arg("--help")
        .output()
        .expect("hubuum-admin --help should run");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("--reset-password"));
    assert!(stdout.contains("--backup"));
    assert!(stdout.contains("--verify-backup"));
    assert!(stdout.contains("--restore"));
    assert!(stdout.contains("--restore-executor"));
    #[cfg(feature = "embedded-migrations")]
    assert!(stdout.contains("--legacy-single-role-migration"));
    assert!(stdout.contains("--database-role-setup-sql"));
    assert!(stdout.contains("--database-role-grants-sql"));
    assert!(stdout.contains("--check-database-privileges"));
    assert!(stdout.contains("--database-role-mode"));
}

#[cfg(feature = "embedded-migrations")]
#[rstest::rstest]
#[case::unset(None)]
#[case::empty(Some(""))]
fn single_role_migration_uses_the_database_url_when_the_override_is_absent(
    #[case] migration_database_url: Option<&str>,
) {
    let mut command = Command::new(admin_binary());
    command
        .env_remove("HUBUUM_MIGRATION_DATABASE_URL")
        .env_remove("HUBUUM_DATABASE_ROLE_MODE")
        .args([
            "--migrate",
            "--database-url",
            "postgres://hubuum@127.0.0.1:1/unreachable",
        ]);
    if let Some(database_url) = migration_database_url {
        command.args(["--migration-database-url", database_url]);
    }
    let output = command
        .output()
        .expect("hubuum-admin --migrate should report its connection failure");

    assert_eq!(output.status.code(), Some(EXIT_CODE_DATABASE_ERROR));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("Failed to run storage migrations"));
}

#[cfg(feature = "embedded-migrations")]
#[rstest::rstest]
#[case::unset(None)]
#[case::empty(Some(""))]
fn split_role_migration_requires_the_privileged_database_url(
    #[case] migration_database_url: Option<&str>,
) {
    let mut command = Command::new(admin_binary());
    command
        .env_remove("HUBUUM_DATABASE_URL")
        .env_remove("HUBUUM_MIGRATION_DATABASE_URL")
        .env_remove("HUBUUM_DATABASE_ROLE_MODE")
        .args([
            "--migrate",
            "--database-role-mode",
            "split",
            "--database-url",
            "postgres://hubuum@127.0.0.1:1/unreachable",
        ]);
    if let Some(database_url) = migration_database_url {
        command.args(["--migration-database-url", database_url]);
    }
    let output = command
        .output()
        .expect("hubuum-admin --migrate should validate split-role configuration");

    assert_eq!(output.status.code(), Some(EXIT_CODE_CONFIG_ERROR));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("HUBUUM_MIGRATION_DATABASE_URL must be set"));
}

#[cfg(windows)]
#[test]
fn backup_files_remain_owner_only_with_an_incompatible_powershell_module_path() {
    let directory = SecretDirectory::new();
    let modules = directory.0.join("modules");
    let security_module = modules.join("Microsoft.PowerShell.Security");
    std::fs::create_dir_all(&security_module).unwrap();
    std::fs::write(
        security_module.join("Microsoft.PowerShell.Security.psm1"),
        "function Set-Acl { throw 'Inherited module path must not be used for backup ACLs' }",
    )
    .unwrap();
    let backup = directory.0.join("backup.json");
    let output = admin_command(&database_url())
        .env("PSModulePath", &modules)
        .arg("--backup")
        .arg(&backup)
        .output()
        .unwrap();
    assert_command_succeeded(&output);

    // Read the resulting ACL through .NET, independently of Set-Acl and module lookup.
    let output = Command::new("powershell.exe")
        .args([
            "-NoLogo",
            "-NoProfile",
            "-NonInteractive",
            "-Command",
            r#"
$ErrorActionPreference = 'Stop'
$path = [Environment]::GetEnvironmentVariable('TEST_BACKUP_ACL_PATH', 'Process')
$identity = [System.Security.Principal.WindowsIdentity]::GetCurrent().User
$acl = [System.IO.File]::GetAccessControl($path)
$owner = $acl.GetOwner([System.Security.Principal.SecurityIdentifier])
$rules = $acl.GetAccessRules($true, $true, [System.Security.Principal.SecurityIdentifier])
if ($owner -ne $identity -or -not $acl.AreAccessRulesProtected -or $rules.Count -ne 1) {
    throw 'Backup ACL must be owned by the current user with one protected access rule'
}
$rule = $rules[0]
if ($rule.IdentityReference -ne $identity -or $rule.IsInherited -or
    $rule.AccessControlType -ne [System.Security.AccessControl.AccessControlType]::Allow -or
    $rule.FileSystemRights -ne [System.Security.AccessControl.FileSystemRights]::FullControl) {
    throw 'Backup ACL must grant only the current user full control'
}
"#,
        ])
        .env_remove("PSModulePath")
        .env("TEST_BACKUP_ACL_PATH", &backup)
        .output()
        .unwrap();
    assert_command_succeeded(&output);
}

#[cfg(unix)]
#[test]
fn backup_files_are_owner_only_and_atomically_replaced() {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    let database_url = database_url();
    let path = std::env::temp_dir().join(format!("{}.json", unique_name("hubuum_admin_backup")));

    let output = admin_command(&database_url)
        .args(["--backup", path.to_str().expect("UTF-8 backup path")])
        .output()
        .expect("hubuum-admin --backup should run");
    assert_command_succeeded(&output);
    assert_eq!(
        std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
        0o600
    );
    let original_inode = std::fs::metadata(&path).unwrap().ino();

    let verification = Command::new(admin_binary())
        .env_remove("HUBUUM_DATABASE_URL")
        .env_remove("HUBUUM_MIGRATION_DATABASE_URL")
        .args([
            "--verify-backup",
            path.to_str().expect("UTF-8 backup path"),
            "--json",
        ])
        .output()
        .expect("hubuum-admin --verify-backup should run without a database");
    assert_command_succeeded(&verification);
    let report: serde_json::Value = serde_json::from_slice(&verification.stdout).unwrap();
    assert_eq!(report["result"], "passed");
    assert_eq!(report["mode"], "format_only");
    assert_eq!(report["backup_version"], 6);
    assert!(report["total_items"].as_i64().unwrap() > 0);

    let unsafe_restore_test = admin_command(&database_url)
        .args([
            "--verify-backup",
            path.to_str().expect("UTF-8 backup path"),
            "--restore-test-database-url",
            &database_url,
        ])
        .output()
        .expect("hubuum-admin should reject the configured database as a restore-test target");
    assert!(!unsafe_restore_test.status.success());
    assert!(
        String::from_utf8_lossy(&unsafe_restore_test.stderr)
            .contains("target matches a configured Hubuum database")
    );

    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
    let output = admin_command(&database_url)
        .args(["--backup", path.to_str().expect("UTF-8 backup path")])
        .output()
        .expect("hubuum-admin --backup should overwrite");
    assert_command_succeeded(&output);
    let replaced_metadata = std::fs::metadata(&path).unwrap();
    assert_eq!(replaced_metadata.permissions().mode() & 0o777, 0o600);
    assert_ne!(replaced_metadata.ino(), original_inode);

    std::fs::remove_file(path).unwrap();
}

#[test]
fn backup_verification_rejects_malformed_input_before_database_configuration() {
    let path = std::env::temp_dir().join(format!("{}.json", unique_name("invalid_backup")));
    std::fs::write(
        &path,
        br#"{"backup_version":6,"secret":"verification-canary"}"#,
    )
    .unwrap();

    let output = Command::new(admin_binary())
        .env_remove("HUBUUM_DATABASE_URL")
        .env_remove("HUBUUM_MIGRATION_DATABASE_URL")
        .args(["--verify-backup", path.to_str().unwrap(), "--json"])
        .output()
        .expect("hubuum-admin --verify-backup should reject malformed input");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("not valid backup JSON"));
    assert!(!stderr.contains("verification-canary"));
    assert!(!stderr.contains("must be set"));
    std::fs::remove_file(path).unwrap();
}

#[test]
fn restore_requires_destructive_confirmation_before_database_access() {
    let output = Command::new(admin_binary())
        .args([
            "--restore",
            "backup.json",
            "--database-url",
            "mongodb://localhost/hubuum",
        ])
        .output()
        .expect("hubuum-admin --restore should start");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("restore deletes and replaces all Hubuum application data"));
    assert!(!stderr.contains("Unsupported database type"));
}

#[cfg(unix)]
mod restore_streams {
    use std::fs::{OpenOptions, remove_file};
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;
    use std::path::PathBuf;
    use std::process::{Command, Output, Stdio};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::thread::{self, JoinHandle};
    use std::time::{Duration, Instant};

    use hubuum::models::RESTORE_CONFIRMATION_PHRASE;
    use rstest::rstest;

    use super::{admin_binary, unique_name};

    fn run_command(mut command: Command) -> Output {
        let mut child = command
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("administrator command should start");
        let deadline = Instant::now() + Duration::from_secs(30);
        while child
            .try_wait()
            .expect("command status should be available")
            .is_none()
        {
            if Instant::now() >= deadline {
                let _ = child.kill();
                let output = child
                    .wait_with_output()
                    .expect("timed-out command should exit");
                panic!("stream-input command timed out: {output:?}");
            }
            thread::sleep(Duration::from_millis(10));
        }
        child
            .wait_with_output()
            .expect("command output should be available")
    }

    struct FifoInput {
        path: PathBuf,
        cancelled: Arc<AtomicBool>,
        producer: Option<JoinHandle<()>>,
    }

    impl FifoInput {
        fn new() -> Self {
            let path = std::env::temp_dir().join(unique_name("restore_fifo"));
            assert!(
                Command::new("mkfifo")
                    .args(["-m", "600"])
                    .arg(&path)
                    .status()
                    .expect("mkfifo should run")
                    .success()
            );
            let cancelled = Arc::new(AtomicBool::new(false));
            let producer_path = path.clone();
            let producer_cancelled = cancelled.clone();
            let producer = thread::spawn(move || {
                let deadline = Instant::now() + Duration::from_secs(30);
                // Do not leave a producer blocked in open if CLI validation
                // fails before the consumer opens the FIFO.
                while !producer_cancelled.load(Ordering::Relaxed) && Instant::now() < deadline {
                    match OpenOptions::new()
                        .write(true)
                        .custom_flags(libc::O_NONBLOCK)
                        .open(&producer_path)
                    {
                        Ok(mut file) => {
                            // The verifier may close the pipe without reading.
                            let _ = file.write_all(br#"{"backup_version":6}"#);
                            return;
                        }
                        Err(error) if error.raw_os_error() == Some(libc::ENXIO) => {
                            thread::sleep(Duration::from_millis(10));
                        }
                        Err(error) => panic!("failed to open FIFO producer: {error}"),
                    }
                }
            });
            Self {
                path,
                cancelled,
                producer: Some(producer),
            }
        }
    }

    impl Drop for FifoInput {
        fn drop(&mut self) {
            self.cancelled.store(true, Ordering::Relaxed);
            if let Some(producer) = self.producer.take() {
                let _ = producer.join();
            }
            let _ = remove_file(&self.path);
        }
    }

    #[rstest]
    #[case::restore("--restore", "Restore document is not valid backup JSON")]
    #[case::verify("--verify-backup", "must be an ordinary file")]
    fn fifo_input_preserves_each_commands_read_policy(
        #[case] operation: &str,
        #[case] expected_error: &str,
    ) {
        let input = FifoInput::new();
        let mut command = Command::new(admin_binary());
        command.args(["--storage-backend", "memory"]);
        command.arg(operation).arg(&input.path);
        if operation == "--restore" {
            command.args(["--restore-confirmation", RESTORE_CONFIRMATION_PHRASE]);
        }

        let output = run_command(command);

        assert!(!output.status.success());
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            stderr.contains(expected_error),
            "unexpected error: {stderr}"
        );
    }

    #[test]
    fn restore_reads_process_substitution_before_backup_validation() {
        let mut command = Command::new("bash");
        command.env_remove("BASH_ENV").args([
            "-c",
            r#"exec "$1" --restore <(printf '%s' '{"backup_version":6}') "${@:2}""#,
            "restore-process-substitution-test",
            admin_binary(),
            "--storage-backend",
            "memory",
            "--restore-confirmation",
            RESTORE_CONFIRMATION_PHRASE,
        ]);
        let output = run_command(command);

        assert!(!output.status.success());
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            stderr.contains("Restore document is not valid backup JSON"),
            "unexpected error: {stderr}"
        );
    }
}

#[test]
fn invalid_log_level_is_reported_before_logging_is_initialized() {
    let output = Command::new(admin_binary())
        .args(["--log-level", "not-a-level"])
        .output()
        .expect("hubuum-admin with an invalid log level should run");

    assert_eq!(output.status.code(), Some(2));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("Invalid log level: not-a-level"));
}

#[test]
fn reset_password_does_not_parse_server_config_arguments() {
    let output = Command::new(admin_binary())
        .args([
            "--reset-password",
            "admin",
            "--database-url",
            "mongodb://localhost/hubuum",
        ])
        .output()
        .expect("hubuum-admin --reset-password should start");

    assert!(!output.status.success());

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("Unsupported database type"));
    assert!(!stderr.contains("Invalid application configuration"));
    assert!(!stderr.contains("unexpected argument '--reset-password'"));
    assert!(!stderr.contains("panicked at"));
}

#[tokio::test]
async fn reset_password_replaces_the_stored_credential() {
    let database_url = database_url();
    let pool = database_pool(&database_url);
    hubuum::services::identity::ensure_identity_scope(
        &pool,
        LOCAL_IDENTITY_SCOPE,
        LOCAL_PROVIDER_KIND,
    )
    .await
    .expect("local identity scope");

    let username = unique_name("admin_cli_reset");
    let old_password = unique_name("old_password");
    hubuum::services::identity::create_user(
        &pool,
        NewUser {
            identity_scope: None,
            name: username.clone(),
            password: old_password.clone(),
            proper_name: None,
            email: None,
        },
        &hubuum::events::EventContext::system(),
    )
    .await
    .expect("test user");

    let output = admin_command(&database_url)
        .args(["--reset-password", username.as_str()])
        .output()
        .expect("hubuum-admin --reset-password should run");
    assert_command_succeeded(&output);

    let stdout = String::from_utf8_lossy(&output.stdout);
    let prefix = format!("Password for user {username} reset to: ");
    let new_password = stdout
        .lines()
        .find_map(|line| line.strip_prefix(&prefix))
        .expect("generated password in stdout");
    assert_ne!(new_password, old_password);

    let password_hash = with_connection(&pool, async |conn| {
        users::table
            .inner_join(principals::table.on(users::id.eq(principals::id)))
            .inner_join(
                identity_scopes::table.on(principals::identity_scope_id.eq(identity_scopes::id)),
            )
            .filter(principals::name.eq(&username))
            .filter(identity_scopes::name.eq(LOCAL_IDENTITY_SCOPE))
            .select(users::password)
            .first::<Option<String>>(conn)
            .await
    })
    .await
    .expect("updated user")
    .expect("stored password hash");
    assert!(verify_password(new_password, &password_hash).expect("new password verification"));
    assert!(!verify_password(&old_password, &password_hash).expect("old password verification"));
}

#[tokio::test]
async fn audit_templates_rejects_an_invalid_stored_template() {
    let database_url = database_url();
    let pool = database_pool(&database_url);
    let template_name = unique_name("admin_cli_audit_template");

    let collection_id = with_connection(&pool, async |conn| {
        collections::table
            .filter(collections::parent_collection_id.is_null())
            .select(collections::id)
            .first::<i32>(conn)
            .await
    })
    .await
    .expect("root collection");

    let stored_template_name = template_name.clone();
    with_connection(&pool, async move |conn| {
        insert_into(export_templates::table)
            .values((
                export_templates::collection_id.eq(collection_id),
                export_templates::name.eq(stored_template_name),
                export_templates::description.eq("admin CLI audit fixture"),
                export_templates::content_type.eq("text/plain"),
                export_templates::template.eq("{{"),
                export_templates::kind.eq("fragment"),
            ))
            .execute(conn)
            .await
    })
    .await
    .expect("stored export template");

    let output = admin_command(&database_url)
        .arg("--audit-templates")
        .output()
        .expect("hubuum-admin --audit-templates should run");
    assert!(!output.status.success());

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains(&format!("template={template_name}")));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("template(s) failed validation"));
}

#[tokio::test]
async fn export_template_health_reports_persisted_output_statistics() {
    let database_url = database_url();
    let pool = database_pool(&database_url);
    let template_name = unique_name("admin_cli_health_template");
    let now = Utc::now();
    let task = hubuum::test_support::create_persisted_test_task(
        &pool,
        hubuum_storage_postgres::test_support::TestTaskCreate::internal_completed(
            hubuum_storage_core::StorageTaskKind::Export,
            hubuum_storage_core::StorageTaskStatus::Succeeded,
        )
        .expect("terminal export fixture must be valid")
        .request_payload(None)
        .progress(
            hubuum_storage_core::StorageTaskProgress::try_new(1, 1, 1, 0)
                .expect("non-negative progress should be valid"),
        ),
    )
    .await
    .expect("stored export task");
    hubuum_storage_postgres::test_support::store_export_output(
        &pool,
        hubuum_domain::TaskId::new(task.id).expect("persisted task id must be positive"),
        hubuum_storage_core::StorageExportTaskArtifact::builder(
            "application/json",
            hubuum_storage_core::StorageExportTaskArtifactContent::Json(
                serde_json::json!({ "ok": true }),
            ),
            serde_json::json!({}),
            serde_json::json!(["first", "second"]),
            now + Duration::hours(1),
        )
        .template_name(Some(template_name.clone()))
        .warning_state(2, false)
        .durations(
            hubuum_storage_core::StorageTaskDurations::try_new(125, 20, 30, 75)
                .expect("non-negative durations should be valid"),
        )
        .try_build()
        .expect("export artifact should be valid"),
    )
    .await
    .expect("stored export output");

    let output = admin_command(&database_url)
        .arg("--export-template-health")
        .output()
        .expect("hubuum-admin --export-template-health should run");
    assert_command_succeeded(&output);

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Export template health:"));
    assert!(stdout.contains(&format!(
        "template={template_name} runs=1 avg_warning_count=2.00 max_warning_count=2 avg_total_duration_ms=125.00 max_total_duration_ms=125"
    )));
    assert!(stdout.contains(&format!(
        "template={template_name} warning_runs=1 max_warning_count=2"
    )));
    assert!(stdout.contains(&format!(
        "template={template_name} avg_total_duration_ms=125.00 max_total_duration_ms=125"
    )));
}
