use hubuum_domain::{ResourceIdentity, VersionIdentity};
use serde::{Deserialize, Serialize};

/// Stable identity of a stored resource and its represented version.
///
/// Integer IDs and revision counters may repeat after restoring an older
/// application timeline. This pair is preserved by restore; subsequent writes
/// preserve the resource identity and mint a fresh version identity.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct StorageRecordIdentity {
    resource: ResourceIdentity,
    version: VersionIdentity,
}

impl StorageRecordIdentity {
    #[must_use]
    pub const fn new(resource: ResourceIdentity, version: VersionIdentity) -> Self {
        Self { resource, version }
    }

    #[must_use]
    pub const fn resource(self) -> ResourceIdentity {
        self.resource
    }

    #[must_use]
    pub const fn version(self) -> VersionIdentity {
        self.version
    }

    /// A new logical version of the same incarnation, not a replacement resource.
    #[must_use]
    pub fn next_version(self) -> Self {
        Self::new(self.resource, VersionIdentity::generate())
    }
}
