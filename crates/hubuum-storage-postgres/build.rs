use std::{env, fs, path::PathBuf};

fn main() {
    println!("cargo:rerun-if-changed=migrations");
    let mut versions = fs::read_dir("migrations")
        .expect("migration directory must be readable")
        .map(|entry| entry.expect("migration entry must be readable"))
        .filter(|entry| entry.path().is_dir())
        .map(|entry| {
            let name = entry
                .file_name()
                .into_string()
                .expect("migration name must be UTF-8");
            let (version, _) = name
                .split_once('_')
                .expect("migration directory must have a version prefix");
            version.replace('-', "")
        })
        .collect::<Vec<_>>();
    versions.sort();
    let latest = versions.last().expect("at least one migration must exist");
    println!("cargo:rustc-env=PACKAGED_POSTGRES_MIGRATION_VERSION={latest}");
    let output = PathBuf::from(env::var_os("OUT_DIR").expect("Cargo supplies OUT_DIR"));
    fs::write(
        output.join("migration_versions.rs"),
        format!("const KNOWN_DATABASE_MIGRATION_VERSIONS: &[&str] = &{versions:?};\n"),
    )
    .expect("migration version manifest must be writable");
}
