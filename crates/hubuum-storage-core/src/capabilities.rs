//! Semantic discovery surface for the storage contract.
//!
//! Each module keeps capability traits beside their request, result, and
//! protocol types. Adapter authors should use these modules instead of
//! searching the crate-wide compatibility reexports at the crate root.

/// Common errors, pages, records, and mutation outcomes.
pub mod common {
    pub use crate::mutation::*;
    pub use crate::page::*;
    pub use crate::record::*;
    pub use crate::{StorageError, StorageErrorKind};
}

/// Complete-backend composition.
pub mod backend {
    pub use crate::backend::*;
}

/// Atomic resource lifecycle and transaction capabilities.
pub mod resources {
    pub use crate::relation_lifecycle::*;
    pub use crate::resource_lifecycle::*;
    pub use crate::transaction::*;
}

/// Authentication, identity, group, principal, and authorization capabilities.
pub mod identity {
    pub use crate::authorization::*;
    pub use crate::collection_authorization::*;
    pub use crate::identity::*;
    pub use crate::identity_operations::*;
    pub use crate::identity_resources::*;
    pub use crate::identity_tokens::*;
    pub use crate::identity_users::*;
}

/// Backend-neutral read-model capabilities.
pub mod queries {
    pub use crate::catalog::*;
    pub use crate::computed_objects::*;
    pub use crate::history::*;
    pub use crate::inventory::*;
    pub use crate::object_aggregate::*;
    pub use crate::relation_query::*;
    pub use crate::unified_search::*;
}

/// Long-running and application workflow capabilities.
pub mod workflows {
    pub use crate::backup_snapshot::*;
    pub use crate::computed_field_lifecycle::*;
    pub use crate::export_query::*;
    pub use crate::export_template_lifecycle::*;
    pub use crate::import_workflow::*;
    pub use crate::remote_target::*;
    pub use crate::restore::*;
    pub use crate::task_execution::*;
    pub use crate::task_queue::*;
}

/// Audit, fan-out, delivery, administration, and retention capabilities.
pub mod events {
    pub use crate::event_administration::*;
    pub use crate::events::*;
}

/// Execution context, observation, operational state, and process integration.
pub mod operations {
    pub use crate::execution::*;
    pub use crate::metrics::*;
    pub use crate::operational::*;
    pub use crate::telemetry::*;
    pub use crate::worker_notifications::*;
}

/// Backup/import payload representations shared by workflow adapters.
pub mod import_export {
    pub use crate::backup_snapshot::*;
    pub use crate::export_query::*;
    pub use crate::export_template_lifecycle::*;
    pub use crate::import_workflow::*;
}
