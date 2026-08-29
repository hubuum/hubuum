use crate::{
    EventStorage, IdentityStorage, OperationalStorage, QueryStorage, ResourceStorage,
    WorkflowStorage,
};

/// Complete storage contract accepted by an application composition root.
///
/// Capability traits remain independently useful for focused services and
/// tests. A selectable backend implements this aggregate only after it
/// implements every required family. Missing behavior is therefore a compile
/// error instead of a runtime `unsupported` path.
///
/// This trait describes static Rust composition. It is not a dynamic plugin
/// interface and does not define runtime discovery or contract versioning.
pub trait StorageBackend:
    ResourceStorage
    + IdentityStorage
    + QueryStorage
    + WorkflowStorage
    + EventStorage
    + OperationalStorage
{
}
