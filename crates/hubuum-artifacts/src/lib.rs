//! Application transfer orchestration for the backend-neutral artifact contract.
//! The storage SDK owns identities, verification proofs and bounded readers;
//! this internal crate owns the runtime-dependent producer bridge.
#[cfg(unix)]
mod filesystem;
mod producer;
#[cfg(unix)]
pub use filesystem::{FilesystemArtifactStore, FilesystemError};
pub use hubuum_storage_core::artifacts::*;
pub use producer::stage_producer;
