pub mod mock_treetop;
// The re-export is for tests only — `cargo build` doesn't compile any
// consumer of these symbols, so without the cfg gate clippy reports the
// `pub use` as unused.
#[cfg(test)]
pub use mock_treetop::{MockAllowRule, MockTreetopBackend};
