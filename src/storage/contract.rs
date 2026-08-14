use super::PostgresStorage;

pub(crate) use hubuum_storage_core::{StorageBackend, StorageIdentity};

pub(crate) use super::registry::{StorageBackendDescriptor, StorageBackendKind};

impl StorageBackend for PostgresStorage {}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_complete_backend<T: StorageBackend>() {}

    #[test]
    fn postgres_satisfies_the_complete_storage_backend_contract() {
        assert_complete_backend::<PostgresStorage>();
    }
}
