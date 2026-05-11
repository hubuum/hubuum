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

// Live-Treetop parity tests require both backends: permissions-local for
// the scaffold (comparison baseline) and permissions-treetop for the live
// connection. Only compile when both features are enabled.
#[cfg(all(feature = "permissions-local", feature = "permissions-treetop"))]
pub mod live_treetop_parity;

// Exporter round-trip tests require both backends: permissions-local for
// generating SQL fixtures and permissions-treetop for MockTreetopBackend.
#[cfg(all(feature = "permissions-local", feature = "permissions-treetop"))]
pub mod exporter_round_trip;
