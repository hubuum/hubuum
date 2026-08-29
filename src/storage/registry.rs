/// Storage adapters compiled into this Hubuum application.
///
/// This registry belongs to application composition, not to the reusable
/// storage contract. A backend crate implements the contract without knowing
/// which adapters a particular Hubuum binary chooses to include.
#[derive(
    clap::ValueEnum,
    serde::Deserialize,
    serde::Serialize,
    Clone,
    Copy,
    Debug,
    Default,
    PartialEq,
    Eq,
)]
pub enum StorageBackendKind {
    #[default]
    #[serde(rename = "postgresql")]
    #[value(name = "postgresql", alias = "")]
    Postgres,
}

impl StorageBackendKind {
    /// Every backend kind selectable by this application build.
    #[cfg(test)]
    pub(crate) const ALL: [Self; 1] = [Self::Postgres];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Postgres => "postgresql",
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
        let descriptor = StorageBackendDescriptor::new(StorageBackendKind::Postgres);

        assert_eq!(descriptor.kind(), StorageBackendKind::Postgres);
    }
}
