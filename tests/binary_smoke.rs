use std::process::{Command, Output};

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
fn server_binary_exposes_help() {
    let output = Command::new(env!("CARGO_BIN_EXE_hubuum-server"))
        .arg("--help")
        .output()
        .expect("hubuum-server --help should run");

    assert_command_succeeded(&output);
    assert!(String::from_utf8_lossy(&output.stdout).contains("Hubuum server"));
}

#[test]
fn admin_binary_exposes_version() {
    let output = Command::new(env!("CARGO_BIN_EXE_hubuum-admin"))
        .arg("--version")
        .output()
        .expect("hubuum-admin --version should run");

    assert_command_succeeded(&output);
    assert!(String::from_utf8_lossy(&output.stdout).contains(env!("CARGO_PKG_VERSION")));
}

#[test]
fn openapi_binary_emits_a_valid_document() {
    let output = Command::new(env!("CARGO_BIN_EXE_hubuum-openapi"))
        .output()
        .expect("hubuum-openapi should run");

    assert_command_succeeded(&output);
    let document: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("valid OpenAPI JSON");
    assert_eq!(document["openapi"], "3.1.0");
    assert_eq!(document["info"]["title"], "Hubuum REST API");
}

#[rstest::rstest]
#[case(env!("CARGO_BIN_EXE_hubuum-server"))]
#[case(env!("CARGO_BIN_EXE_hubuum-admin"))]
fn binary_help_documents_secret_source_options(#[case] binary: &str) {
    let output = Command::new(binary).arg("--help").output().unwrap();
    assert_command_succeeded(&output);
    let help = String::from_utf8_lossy(&output.stdout);
    assert!(help.contains("--secret-source"));
    assert!(help.contains("--secret-file-root"));
}

#[rstest::rstest]
#[case(env!("CARGO_BIN_EXE_hubuum-server"), None)]
#[case(env!("CARGO_BIN_EXE_hubuum-admin"), Some("--database-ready"))]
fn file_source_requires_a_root_before_connecting(
    #[case] binary: &str,
    #[case] operation: Option<&str>,
) {
    let mut command = Command::new(binary);
    command
        .env_remove("HUBUUM_SECRET_FILE_ROOT")
        .args(["--secret-source", "file"]);
    if let Some(operation) = operation {
        command.arg(operation);
    }
    let output = command.output().unwrap();
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("HUBUUM_SECRET_FILE_ROOT is required")
    );
}

fn isolated_server() -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_hubuum-server"));
    for (name, _) in std::env::vars_os() {
        if name.to_string_lossy().starts_with("HUBUUM_") {
            command.env_remove(name);
        }
    }
    command
}

#[rstest::rstest]
#[case("api", None, None)]
#[case("worker", Some("fixture"), None)]
#[case("all", Some("fixture"), Some("invalid-key-material"))]
fn server_refuses_to_start_without_a_usable_encryption_key(
    #[case] role: &str,
    #[case] alias: Option<&str>,
    #[case] key: Option<&str>,
) {
    let mut command = isolated_server();
    command.args([
        "--runtime-role",
        role,
        "--database-url",
        "postgres://127.0.0.1:1/unused",
    ]);
    if let Some(alias) = alias {
        command.env("HUBUUM_RUNTIME_SETTINGS_ENCRYPTION_KEY_REF", alias);
    }
    if let Some(key) = key {
        command.env("HUBUUM_RUNTIME_SETTINGS_KEY_FIXTURE", key);
    }
    let output = command.output().unwrap();
    assert!(!output.status.success());
    let error = String::from_utf8_lossy(&output.stderr);
    assert!(
        error.contains("HUBUUM_RUNTIME_SETTINGS_ENCRYPTION_KEY_REF"),
        "{error}"
    );
    assert!(!error.contains("invalid-key-material"));
}

#[rstest::rstest]
#[case("api", "environment")]
#[case("worker", "file")]
#[case("all", "environment")]
fn validation_uses_selected_key_source_without_binding(#[case] role: &str, #[case] source: &str) {
    let directory = std::env::temp_dir().join(format!("hubuum-validate-{}", uuid::Uuid::new_v4()));
    let occupied = std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let mut command = isolated_server();
    command.args([
        "--validate",
        "--storage-backend",
        "memory",
        "--runtime-role",
        role,
        "--secret-source",
        source,
        "--runtime-settings-encryption-key-ref",
        "fixture",
        "--port",
        &occupied.local_addr().unwrap().port().to_string(),
    ]);
    if source == "file" {
        std::fs::create_dir_all(directory.join("runtime-settings")).unwrap();
        std::fs::write(directory.join("runtime-settings/fixture"), [0xab; 32]).unwrap();
        command.arg("--secret-file-root").arg(&directory);
    } else {
        command.env(
            "HUBUUM_RUNTIME_SETTINGS_KEY_FIXTURE",
            "base64:AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE=",
        );
    }
    let output = command.output().unwrap();
    if source == "file" {
        std::fs::remove_dir_all(directory).unwrap();
    }
    assert_command_succeeded(&output);
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("validated"));
    assert!(!stdout.contains("AQEBAQE"));
}
