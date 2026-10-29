use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::{ArtifactError, ArtifactResult};

macro_rules! identity {
    ($name:ident) => {
        #[derive(
            Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize,
        )]
        #[serde(try_from = "String", into = "String")]
        pub struct $name([u8; 16]);

        impl TryFrom<[u8; 16]> for $name {
            type Error = ArtifactError;
            fn try_from(bytes: [u8; 16]) -> ArtifactResult<Self> {
                Self::from_bytes(bytes)
            }
        }
        impl From<$name> for [u8; 16] {
            fn from(value: $name) -> Self {
                value.0
            }
        }
        impl TryFrom<String> for $name {
            type Error = ArtifactError;
            fn try_from(value: String) -> ArtifactResult<Self> {
                let id = Uuid::parse_str(&value)
                    .map_err(|_| ArtifactError::Invalid("Invalid artifact identity".into()))?;
                Self::from_bytes(*id.as_bytes())
            }
        }
        impl From<$name> for String {
            fn from(value: $name) -> Self {
                Uuid::from_bytes(value.0).to_string()
            }
        }
        impl $name {
            pub fn generate() -> Self {
                Self(*Uuid::new_v4().as_bytes())
            }
            pub fn from_bytes(bytes: [u8; 16]) -> ArtifactResult<Self> {
                if bytes == [0; 16] {
                    return Err(ArtifactError::Invalid(
                        concat!(stringify!($name), " cannot be nil").into(),
                    ));
                }
                Ok(Self(bytes))
            }
            pub fn as_bytes(&self) -> &[u8; 16] {
                &self.0
            }
        }
    };
}

identity!(ArtifactId);
identity!(AttemptId);
identity!(DestinationId);
identity!(ObjectVersion);

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Destination {
    id: DestinationId,
    name: String,
}

impl Destination {
    pub fn new(id: DestinationId, name: impl Into<String>) -> ArtifactResult<Self> {
        let name = name.into();
        if name.is_empty()
            || name.len() > 128
            || !name
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
        {
            return Err(ArtifactError::Invalid(
                "Destination name must contain 1–128 ASCII letters, digits, '.', '_' or '-'".into(),
            ));
        }
        Ok(Self { id, name })
    }
    pub fn id(&self) -> DestinationId {
        self.id
    }
    pub fn name(&self) -> &str {
        &self.name
    }
}

/// Logical artifact, attempt and physical location are intentionally distinct.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactIdentity {
    artifact: ArtifactId,
    attempt: AttemptId,
    destination: DestinationId,
    version: ObjectVersion,
}

impl ArtifactIdentity {
    pub fn new(destination: DestinationId) -> Self {
        Self {
            artifact: ArtifactId::generate(),
            attempt: AttemptId::generate(),
            destination,
            version: ObjectVersion::generate(),
        }
    }
    pub fn from_parts(
        artifact: ArtifactId,
        attempt: AttemptId,
        destination: DestinationId,
        version: ObjectVersion,
    ) -> Self {
        Self {
            artifact,
            attempt,
            destination,
            version,
        }
    }
    pub fn artifact(&self) -> ArtifactId {
        self.artifact
    }
    pub fn attempt(&self) -> AttemptId {
        self.attempt
    }
    pub fn destination(&self) -> DestinationId {
        self.destination
    }
    pub fn version(&self) -> ObjectVersion {
        self.version
    }
}
