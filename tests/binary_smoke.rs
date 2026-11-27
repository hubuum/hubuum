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

#[rstest::rstest]
#[case::command_line(true)]
#[case::environment(false)]
fn server_rejects_aws_cold_retrieval_at_startup(#[case] command_line: bool) {
    let mut command = Command::new(env!("CARGO_BIN_EXE_hubuum-server"));
    let profile = "aws-glacier-flexible-retrieval";
    if command_line {
        command.args(["--archive-cold-retrieval", profile]);
    } else {
        command.env("HUBUUM_ARCHIVE_COLD_RETRIEVAL", profile);
    }
    let output = command.output().unwrap();
    assert_eq!(output.status.code(), Some(2));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("--archive-cold-retrieval"));
    assert!(stderr.contains("possible values: disabled"));
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
