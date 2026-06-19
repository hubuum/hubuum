use std::process::Command;

fn admin_binary() -> &'static str {
    env!("CARGO_BIN_EXE_hubuum-admin")
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
