//! Transitional PostgreSQL computed materialization hook.
//!
//! Definition lifecycle and rebuild execution live in
//! `hubuum-storage-postgres`. This module remains only for the PostgreSQL
//! import workflow until that workflow moves into the adapter crate.

mod materialization;

pub(crate) use materialization::materialize_object_in_transaction;
pub use materialization::source_data_sha256;
