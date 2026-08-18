use crate::{ImportStorage, RestoreStorage};

/// Explicit maintenance mutation surface.
///
/// Imports and restores intentionally preserve or reconstruct durable state
/// without masquerading as ordinary user mutations. They are kept out of the
/// audited lifecycle traits so those traits can require an event context and
/// return [`crate::MutationOutcome`] without an unaudited escape hatch.
pub trait MaintenanceStorage: ImportStorage + RestoreStorage + Send + Sync {}

impl<T> MaintenanceStorage for T where T: ImportStorage + RestoreStorage + Send + Sync {}
