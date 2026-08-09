//! Compile-time-adjacent guards for the application/storage boundary.

#[cfg(test)]
use std::fs;
#[cfg(test)]
use std::path::{Path, PathBuf};

#[cfg(test)]
fn repository_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

#[cfg(test)]
fn rust_files(directory: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    let mut pending = vec![directory.to_path_buf()];

    while let Some(current) = pending.pop() {
        for entry in fs::read_dir(&current)
            .unwrap_or_else(|error| panic!("could not read {}: {error}", current.display()))
        {
            let path = entry.expect("directory entry should be readable").path();
            if path.is_dir() {
                pending.push(path);
            } else if path.extension().is_some_and(|extension| extension == "rs") {
                files.push(path);
            }
        }
    }

    files.sort();
    files
}

#[cfg(test)]
fn is_storage_adapter(root: &Path, path: &Path) -> bool {
    ["postgres", "memory"].into_iter().any(|adapter| {
        path == root.join(format!("src/storage/{adapter}.rs"))
            || path.starts_with(root.join(format!("src/storage/{adapter}")))
    })
}

#[test]
fn app_context_exposes_only_an_opaque_backend_handle() {
    let root = repository_root();
    let context_path = root.join("src/permissions/context.rs");
    let context_source = fs::read_to_string(&context_path)
        .unwrap_or_else(|error| panic!("could not read {}: {error}", context_path.display()));
    let trait_path = root.join("src/traits/context.rs");
    let trait_source = fs::read_to_string(&trait_path)
        .unwrap_or_else(|error| panic!("could not read {}: {error}", trait_path.display()));
    let application_path = root.join("src/application.rs");
    let application_source = fs::read_to_string(&application_path)
        .unwrap_or_else(|error| panic!("could not read {}: {error}", application_path.display()));

    assert!(
        context_source.contains("backend: BackendHandle"),
        "AppContext must own the opaque backend handle"
    );
    for forbidden in [
        "pub db_pool:",
        "pub fn postgres_pool",
        "pub fn clone_postgres_pool",
        "impl Deref for AppContext",
    ] {
        assert!(
            !context_source.contains(forbidden),
            "AppContext exposes forbidden backend detail: {forbidden}"
        );
    }
    let backend_context_body = trait_source
        .split_once("pub trait BackendContext")
        .and_then(|(_, remainder)| remainder.split_once("\n}"))
        .map(|(body, _)| body)
        .expect("BackendContext should have a readable trait body");
    assert!(
        !backend_context_body.contains("fn db_pool"),
        "BackendContext must not expose a database-pool accessor"
    );
    assert!(
        trait_source.contains("pub(crate) fn backend_pool"),
        "backend adapters need a crate-private implementation access point"
    );
    assert!(
        !application_source.contains(".app_data(Data::new(app_pool"),
        "the production HTTP server must not register a raw database pool"
    );
    assert!(
        !application_source.contains("pool: db::DbPool"),
        "operational HTTP services must receive AppContext rather than DbPool"
    );
}

#[test]
fn application_consumers_do_not_import_database_implementation_details() {
    let root = repository_root();
    let mut paths = Vec::new();
    for directory in ["src/api", "src/extractors", "src/middlewares"] {
        paths.extend(rust_files(&root.join(directory)));
    }
    paths.push(root.join("src/observability/metrics/scrape.rs"));

    let mut violations = Vec::new();
    for path in paths {
        let source = fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("could not read {}: {error}", path.display()));
        for forbidden in [
            "crate::db",
            "DbPool",
            "backend_pool",
            "diesel::",
            "diesel_async",
            "postgres_pool",
            "pool: AppContext",
            "pool: &AppContext",
        ] {
            if source.contains(forbidden) {
                violations.push(format!("{} imports or uses {forbidden}", path.display()));
            }
        }
    }

    assert!(
        violations.is_empty(),
        "application consumers crossed the opaque backend boundary:\n{}",
        violations.join("\n")
    );
}

#[test]
fn backend_neutral_layers_do_not_import_database_implementation_details() {
    let root = repository_root();
    let mut violations = Vec::new();

    for directory in ["src/services", "src/storage"] {
        for path in rust_files(&root.join(directory)) {
            if is_storage_adapter(&root, &path) {
                continue;
            }

            let source = fs::read_to_string(&path)
                .unwrap_or_else(|error| panic!("could not read {}: {error}", path.display()));
            for forbidden in [
                "crate::db::traits",
                "crate::db::with_connection",
                "crate::db::with_transaction",
                "diesel::",
                "diesel_async",
                "BackendContext",
                "postgres_pool",
                "backend_pool",
            ] {
                if source.contains(forbidden) {
                    violations.push(format!("{} imports or uses {forbidden}", path.display()));
                }
            }
            if path.starts_with(root.join("src/storage")) && source.contains("ApiError") {
                violations.push(format!(
                    "{} couples backend-neutral storage to ApiError",
                    path.display()
                ));
            }
        }
    }

    assert!(
        violations.is_empty(),
        "backend-neutral service/storage code crossed into the PostgreSQL compatibility layer:\n{}",
        violations.join("\n")
    );
}

#[test]
fn selectable_storage_backends_are_complete_and_test_models_are_not_selectable() {
    let root = repository_root();
    let contract_path = root.join("src/storage/contract.rs");
    let contract_source = fs::read_to_string(&contract_path)
        .unwrap_or_else(|error| panic!("could not read {}: {error}", contract_path.display()));
    let context_path = root.join("src/traits/context.rs");
    let context_source = fs::read_to_string(&context_path)
        .unwrap_or_else(|error| panic!("could not read {}: {error}", context_path.display()));

    let contract_body = contract_source
        .split_once("pub(crate) trait StorageBackend:")
        .and_then(|(_, remainder)| remainder.split_once("\n{"))
        .map(|(body, _)| body)
        .expect("StorageBackend should have a readable aggregate trait declaration");
    for required in [
        "LifecycleStorage",
        "IdentityAndAuthorizationStorage",
        "QueryAndHistoryStorage",
        "WorkflowStorage",
        "OperationalStorage",
        "sealed::CertifiedStorageBackend",
    ] {
        assert!(
            contract_body.contains(required),
            "complete storage contract is missing {required}"
        );
    }
    assert!(
        contract_source.contains("impl sealed::CertifiedStorageBackend for PostgresStorage"),
        "PostgreSQL must be explicitly certified in the central storage contract"
    );
    assert!(
        !contract_source.contains("CertifiedStorageBackend for MemoryStorageModel"),
        "the focused memory contract model must not be selectable as a full backend"
    );
    assert!(
        context_source.contains("assert_complete_storage_backend(&backend)"),
        "application composition must enforce the complete storage contract"
    );
}

#[test]
fn storage_error_translation_has_one_way_dependency_direction() {
    let root = repository_root();
    let errors_path = root.join("src/errors.rs");
    let errors_source = fs::read_to_string(&errors_path)
        .unwrap_or_else(|error| panic!("could not read {}: {error}", errors_path.display()));
    let postgres_error_path = root.join("src/storage/postgres/error.rs");
    let postgres_error_source = fs::read_to_string(&postgres_error_path).unwrap_or_else(|error| {
        panic!("could not read {}: {error}", postgres_error_path.display())
    });

    assert!(
        !errors_source.contains("impl From<ApiError> for StorageError"),
        "application errors must not provide a reverse conversion into storage"
    );
    assert!(
        errors_source.contains("impl From<StorageError> for ApiError"),
        "the application layer must own storage-to-API error translation"
    );
    assert!(
        postgres_error_source.contains("struct PostgresStorageError"),
        "the PostgreSQL adapter must own its backend-specific error"
    );
    assert!(
        postgres_error_source.contains("impl From<PostgresStorageError> for StorageError"),
        "the PostgreSQL adapter error must translate at the storage boundary"
    );
}

#[test]
fn persistence_facades_do_not_reexport_internal_layers_wholesale() {
    let root = repository_root();
    let backend_path = root.join("src/backend.rs");
    let backend_source = fs::read_to_string(&backend_path)
        .unwrap_or_else(|error| panic!("could not read {}: {error}", backend_path.display()));
    let library_path = root.join("src/lib.rs");
    let library_source = fs::read_to_string(&library_path)
        .unwrap_or_else(|error| panic!("could not read {}: {error}", library_path.display()));

    assert!(
        !backend_source.contains("pub(crate) use crate::db::traits as capabilities"),
        "the application capability facade must explicitly whitelist operations"
    );
    assert!(
        !library_source.contains("pub mod storage;"),
        "storage adapters are internal application composition details"
    );
}
