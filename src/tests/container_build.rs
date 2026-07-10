use std::collections::BTreeSet;
use std::fs;
use std::path::PathBuf;

#[test]
fn dockerfile_copies_every_workspace_manifest() {
    let repository = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let manifest = fs::read_to_string(repository.join("Cargo.toml"))
        .expect("repository Cargo.toml should be readable");
    let manifest = toml::from_str::<toml::Value>(&manifest)
        .expect("repository Cargo.toml should be valid TOML");
    let members = manifest
        .get("workspace")
        .and_then(|workspace| workspace.get("members"))
        .and_then(toml::Value::as_array)
        .expect("Cargo.toml should declare workspace.members");

    let expected = members
        .iter()
        .map(|member| {
            let member = member
                .as_str()
                .expect("workspace member paths should be strings");
            assert!(
                !member.contains(['*', '?', '[']),
                "Docker manifest parity requires explicit workspace member paths, got '{member}'"
            );
            format!("{member}/Cargo.toml")
        })
        .collect::<BTreeSet<_>>();

    let dockerfile = fs::read_to_string(repository.join("Dockerfile"))
        .expect("repository Dockerfile should be readable");
    let mut copied = BTreeSet::new();
    for line in dockerfile.lines().map(str::trim) {
        let Some(copy) = line.strip_prefix("COPY ") else {
            continue;
        };
        let fields = copy.split_whitespace().collect::<Vec<_>>();
        let Some(source) = fields.first().copied() else {
            continue;
        };
        if !source.starts_with("crates/") || !source.ends_with("/Cargo.toml") {
            continue;
        }
        assert_eq!(
            fields.len(),
            2,
            "workspace manifest COPY must have one source and one destination: {line}"
        );
        assert_eq!(
            fields[1],
            format!("./{source}"),
            "workspace manifest must be copied to its original relative path"
        );
        copied.insert(source.to_string());
    }

    assert_eq!(
        copied, expected,
        "Dockerfile dependency-cache manifest COPY entries must exactly match Cargo workspace members"
    );
}
