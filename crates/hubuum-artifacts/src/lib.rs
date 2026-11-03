//! Transfer orchestration and immutable stores for the neutral artifact contract.
//! The storage SDK owns identities, verification proofs and bounded readers.
//! This internal crate owns the producer bridge, Unix filesystem adapter and
//! optional S3 byte-store adapter.
#[cfg(unix)]
mod filesystem;
mod producer;
#[cfg(feature = "s3")]
pub mod s3;
#[cfg(unix)]
pub use filesystem::{FilesystemArtifactStore, FilesystemError};
pub use hubuum_storage_core::artifacts::*;
pub use producer::stage_producer;
