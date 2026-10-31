use std::fs;

fn main() {
    println!("cargo:rerun-if-changed=migrations");

    let latest = fs::read_dir("migrations")
        .expect("migration directory must be readable")
        .map(|entry| entry.expect("migration directory entry must be readable"))
        .filter(|entry| {
            entry
                .file_type()
                .expect("migration entry type must be readable")
                .is_dir()
        })
        .map(|entry| {
            entry
                .file_name()
                .into_string()
                .expect("migration directory name must be UTF-8")
        })
        .max()
        .expect("at least one migration must exist");
    let version = latest
        .split_once('_')
        .map_or(latest.as_str(), |(version, _)| version)
        .replace('-', "");
    println!("cargo:rustc-env=PACKAGED_POSTGRES_MIGRATION_VERSION={version}");
}
