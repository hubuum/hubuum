pub(crate) use hubuum_storage_core::StorageBackend;

pub(crate) use super::registry::StorageBackendDescriptor;

mod private {
    pub trait Sealed {}

    impl Sealed for hubuum_storage_postgres::PostgresStorage {}
}

/// Application-level certification gate for selectable storage adapters.
///
/// Implementing the structural [`StorageBackend`] capability aggregate is not
/// sufficient to enter the runtime registry. An adapter is added here only
/// after the shared behavioral conformance suite passes for it.
pub(crate) trait CertifiedStorageBackend: StorageBackend + private::Sealed {}

impl CertifiedStorageBackend for hubuum_storage_postgres::PostgresStorage {}

#[cfg(test)]
mod tests {
    use super::*;
    use hubuum_storage_postgres::PostgresStorage;

    fn assert_certified_backend<T: CertifiedStorageBackend>() {}

    #[test]
    fn postgres_satisfies_the_complete_storage_backend_contract() {
        assert_certified_backend::<PostgresStorage>();
    }
}
