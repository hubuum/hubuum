// Trait-level tests instantiate concrete backends; only compile them when
// at least one backend implementation is available.
#[cfg(feature = "permissions-local")]
pub mod backend_trait;
