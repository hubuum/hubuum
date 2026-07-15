use std::process::{Command, Output};
use std::sync::atomic::{AtomicU64, Ordering};

use chrono::{Duration, Utc};
use diesel::insert_into;
use hubuum::config::DEFAULT_DB_STATEMENT_TIMEOUT_MS;
use hubuum::db::prelude::*;
use hubuum::db::traits::identity::ensure_identity_scope;
use hubuum::db::{DbPool, init_pool_with_statement_timeout, with_connection, with_transaction};
use hubuum::models::identity::{LOCAL_IDENTITY_SCOPE, LOCAL_PROVIDER_KIND};
use hubuum::models::{
    NewExportTaskOutputRecord, NewTaskRecord, NewUser, TaskKind, TaskStatus, User,
};
use hubuum::schema::{collections, export_task_outputs, export_templates, tasks};
use hubuum::utilities::auth::verify_password;

static NEXT_TEST_ID: AtomicU64 = AtomicU64::new(1);

fn admin_binary() -> &'static str {
    env!("CARGO_BIN_EXE_hubuum-admin")
}

fn database_url() -> String {
    std::env::var("HUBUUM_DATABASE_URL")
        .expect("HUBUUM_DATABASE_URL must point to the migrated test database")
}

fn database_pool(database_url: &str) -> DbPool {
    init_pool_with_statement_timeout(database_url, 2, DEFAULT_DB_STATEMENT_TIMEOUT_MS)
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
    assert!(stdout.contains("--restore"));
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
    ensure_identity_scope(&pool, LOCAL_IDENTITY_SCOPE, LOCAL_PROVIDER_KIND)
        .await
        .expect("local identity scope");

    let username = unique_name("admin_cli_reset");
    let old_password = unique_name("old_password");
    NewUser {
        identity_scope: None,
        name: username.clone(),
        password: old_password.clone(),
        proper_name: None,
        email: None,
    }
    .save_without_events(&pool)
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

    let updated = User::get_by_name(&pool, &username)
        .await
        .expect("updated user");
    let password_hash = updated.password.expect("stored password hash");
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
    let stored_template_name = template_name.clone();

    with_transaction(
        &pool,
        async move |conn| -> Result<(), diesel::result::Error> {
            let now = Utc::now().naive_utc();
            let task_id = insert_into(tasks::table)
                .values(NewTaskRecord {
                    kind: TaskKind::Export.as_str().to_string(),
                    status: TaskStatus::Succeeded.as_str().to_string(),
                    submitted_by: None,
                    idempotency_key: None,
                    request_hash: None,
                    request_payload: None,
                    summary: None,
                    total_items: 1,
                    processed_items: 1,
                    success_items: 1,
                    failed_items: 0,
                    submitted_token_id: None,
                    submitted_token_scoped: false,
                    submitted_token_scopes: serde_json::json!([]),
                    request_redacted_at: None,
                    started_at: Some(now),
                    finished_at: Some(now),
                })
                .returning(tasks::id)
                .get_result::<i32>(conn)
                .await?;

            insert_into(export_task_outputs::table)
                .values(NewExportTaskOutputRecord {
                    task_id,
                    template_name: Some(stored_template_name),
                    content_type: "application/json".to_string(),
                    json_output: Some(serde_json::json!({ "ok": true })),
                    text_output: None,
                    meta_json: serde_json::json!({}),
                    warnings_json: serde_json::json!(["first", "second"]),
                    warning_count: 2,
                    truncated: false,
                    output_expires_at: now + Duration::hours(1),
                    total_duration_ms: 125,
                    query_duration_ms: 20,
                    hydration_duration_ms: 30,
                    render_duration_ms: 75,
                })
                .execute(conn)
                .await?;
            Ok(())
        },
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
