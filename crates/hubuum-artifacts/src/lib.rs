//! Application transfer orchestration for the backend-neutral artifact contract.
//! The storage SDK owns identities, verification proofs and bounded readers;
//! this internal crate owns the runtime-dependent producer bridge.
mod producer;
#[cfg(feature = "s3")]
pub mod s3;
pub use hubuum_storage_core::artifacts::*;
pub use producer::stage_producer;
