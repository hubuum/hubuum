// Trait-level tests instantiate concrete backends; only compile them when
// at least one backend implementation is available.
#[cfg(feature = "permissions-local")]
pub mod backend_trait;

#[cfg(feature = "permissions-local")]
pub mod auth_target;

#[cfg(feature = "permissions-local")]
pub mod visibility;

#[cfg(feature = "permissions-local")]
pub mod mock_treetop;
