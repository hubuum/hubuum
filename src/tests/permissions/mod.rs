pub mod backend_trait;

pub mod auth_target;

pub mod visibility;

pub mod mock_treetop;

// Live-Treetop parity tests require the optional external backend. The local
// comparison baseline is always available.
#[cfg(feature = "permissions-treetop")]
pub mod live_treetop_parity;

// Exporter round-trip tests exercise the optional Treetop integration.
#[cfg(feature = "permissions-treetop")]
pub mod exporter_round_trip;
