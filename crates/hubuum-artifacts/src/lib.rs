//! Transfer orchestration and local Unix storage for the neutral artifact contract.
//! The storage SDK owns identities, verification proofs and bounded readers;
//! this internal crate owns the producer bridge and filesystem byte-store adapter.
#[cfg(unix)]
mod filesystem;
mod producer;
#[cfg(unix)]
pub use filesystem::{FilesystemArtifactStore, FilesystemError};
pub use hubuum_storage_core::artifacts::*;
pub use producer::stage_producer;
