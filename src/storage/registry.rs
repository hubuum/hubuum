/// Storage adapters compiled into this Hubuum application.
///
/// This registry belongs to application composition, not to the reusable
/// storage contract. A backend crate implements the contract without knowing
/// which adapters a particular Hubuum binary chooses to include.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum StorageBackendKind {
    Postgresql,
}

impl StorageBackendKind {
    /// Every backend kind selectable by this application build.
    #[cfg(test)]
    pub(crate) const ALL: [Self; 1] = [Self::Postgresql];

    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Postgresql => "postgresql",
        }
    }
}

/// Non-secret metadata for the adapter selected by application composition.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct StorageBackendDescriptor {
    kind: StorageBackendKind,
}

impl StorageBackendDescriptor {
    pub(crate) const fn new(kind: StorageBackendKind) -> Self {
        Self { kind }
    }

    pub(crate) const fn kind(self) -> StorageBackendKind {
        self.kind
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn descriptor_reports_the_selected_backend() {
        let descriptor = StorageBackendDescriptor::new(StorageBackendKind::Postgresql);

        assert_eq!(descriptor.kind(), StorageBackendKind::Postgresql);
    }
}
