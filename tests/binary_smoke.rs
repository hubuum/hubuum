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
