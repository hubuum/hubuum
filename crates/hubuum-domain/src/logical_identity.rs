//! Logical identities are independent of reusable database integers and leases.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use uuid::{Uuid, Variant};

use crate::{LegacyRecordId, ResourceRevision};

/// Invalid external representation of a logical identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IdentityParseError(&'static str);

impl fmt::Display for IdentityParseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.0)
    }
}

impl std::error::Error for IdentityParseError {}

macro_rules! logical_identity {
    ($name:ident, $description:literal) => {
        #[doc = $description]
        #[derive(
            Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize,
        )]
        #[serde(try_from = "String", into = "String")]
        pub struct $name([u8; 16]);

        impl $name {
            /// Mint a new identity; restoring an existing record must preserve its identity instead.
            ///
            /// # Panics
            ///
            /// Panics if the operating system cannot supply random bytes.
            #[must_use]
            pub fn generate() -> Self {
                Self(*Uuid::new_v4().as_bytes())
            }

            /// Validate a database or protocol UUID without exposing a UUID library in the API.
            pub fn from_bytes(bytes: [u8; 16]) -> Result<Self, IdentityParseError> {
                let uuid = Uuid::from_bytes(bytes);
                if uuid.get_variant() != Variant::RFC4122 || uuid.get_version().is_none() {
                    return Err(IdentityParseError(
                        "Logical identity must be a non-nil RFC UUID",
                    ));
                }
                Ok(Self(bytes))
            }

            #[must_use]
            pub const fn as_bytes(&self) -> &[u8; 16] {
                &self.0
            }
        }

        impl FromStr for $name {
            type Err = IdentityParseError;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                let uuid = Uuid::parse_str(value)
                    .map_err(|_| IdentityParseError("Logical identity must be a valid UUID"))?;
                Self::from_bytes(*uuid.as_bytes())
            }
        }

        impl TryFrom<String> for $name {
            type Error = IdentityParseError;

            fn try_from(value: String) -> Result<Self, Self::Error> {
                value.parse()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                Uuid::from_bytes(self.0).fmt(formatter)
            }
        }

        impl From<$name> for String {
            fn from(value: $name) -> Self {
                value.to_string()
            }
        }
    };
}

logical_identity!(
    ResourceIdentity,
    "Immutable identity of one resource incarnation."
);
logical_identity!(
    PrincipalIdentity,
    "Immutable human or service-account identity."
);
logical_identity!(
    TaskIdentity,
    "Task identity retained independently of task details or execution leases."
);
logical_identity!(
    VersionIdentity,
    "Immutable identity of one historical version; closure preserves it."
);
logical_identity!(
    LegacyIdentityCohort,
    "Isolated namespace of one legacy identity upgrade; not an archive lineage proof."
);

/// Retained integer attribution that cannot establish a known logical identity.
///
/// There is deliberately no conversion from this value to a known identity.
/// A current row with the same integer is not evidence of correspondence.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct UnresolvedIdentity {
    cohort: LegacyIdentityCohort,
    legacy_id: LegacyRecordId,
    #[serde(skip_serializing_if = "Option::is_none")]
    revision: Option<ResourceRevision>,
}

impl UnresolvedIdentity {
    #[must_use]
    pub fn new(cohort: LegacyIdentityCohort, legacy_id: impl Into<LegacyRecordId>) -> Self {
        Self {
            cohort,
            legacy_id: legacy_id.into(),
            revision: None,
        }
    }

    #[must_use]
    pub const fn cohort(self) -> LegacyIdentityCohort {
        self.cohort
    }

    #[must_use]
    pub const fn legacy_id(self) -> LegacyRecordId {
        self.legacy_id
    }

    #[must_use]
    pub const fn at_revision(mut self, revision: ResourceRevision) -> Self {
        self.revision = Some(revision);
        self
    }

    #[must_use]
    pub const fn revision(self) -> Option<ResourceRevision> {
        self.revision
    }
}

/// Captured identity evidence. Absence/system attribution is represented by
/// `Option<IdentityReference<T>>`, separately from unresolved legacy attribution.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
pub enum IdentityReference<T> {
    Known { identity: T },
    Unresolved { legacy: UnresolvedIdentity },
}

impl<T: Copy> IdentityReference<T> {
    /// Only captured known evidence can supply a logical identity.
    #[must_use]
    pub const fn known_identity(self) -> Option<T> {
        match self {
            Self::Known { identity } => Some(identity),
            Self::Unresolved { .. } => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ResourceId;
    use rstest::rstest;

    #[rstest]
    #[case::malformed("not-an-identity")]
    #[case::nil("00000000-0000-0000-0000-000000000000")]
    #[case::max("ffffffff-ffff-ffff-ffff-ffffffffffff")]
    #[case::reserved_variant("12345678-1234-4234-1234-123456789abc")]
    #[case::reserved_version("12345678-1234-0234-8234-123456789abc")]
    fn decoding_rejects_invalid_identity(#[case] value: &str) {
        let encoded = serde_json::to_string(value).unwrap();
        assert!(serde_json::from_str::<ResourceIdentity>(&encoded).is_err());
    }

    #[test]
    fn identity_round_trip_preserves_bytes() {
        let original = ResourceIdentity::generate();
        let restored: ResourceIdentity =
            serde_json::from_str(&serde_json::to_string(&original).unwrap()).unwrap();
        assert_eq!(restored.as_bytes(), original.as_bytes());
    }

    #[test]
    fn unresolved_attribution_cannot_supply_a_known_principal() {
        let reference = IdentityReference::<PrincipalIdentity>::Unresolved {
            legacy: UnresolvedIdentity::new(
                LegacyIdentityCohort::generate(),
                ResourceId::new(42).unwrap(),
            ),
        };
        assert_eq!(reference.known_identity(), None);
    }

    #[test]
    fn reference_decode_rejects_mixed_evidence() {
        let encoded = serde_json::json!({
            "state": "known",
            "identity": PrincipalIdentity::generate(),
            "legacy": UnresolvedIdentity::new(
                LegacyIdentityCohort::generate(), ResourceId::new(42).unwrap()
            ),
        });
        assert!(serde_json::from_value::<IdentityReference<PrincipalIdentity>>(encoded).is_err());
    }
}
