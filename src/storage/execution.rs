use std::future::Future;

use crate::events::MutationProvenance;

use super::{
    StorageCallSite, StorageContext, StorageExecution, StorageRevisionPrecondition, storage_handle,
};

/// Run application work with bounded diagnostic attribution interpreted by the
/// selected storage backend.
pub async fn with_storage_call_site<C, F>(
    context: &C,
    call_site: StorageCallSite,
    future: F,
) -> F::Output
where
    C: StorageContext + ?Sized,
    F: Future,
{
    storage_handle(context)
        .run_with_call_site(call_site, future)
        .await
}

/// Send-capable diagnostic scope for work spawned across a task or thread
/// boundary.
pub async fn with_storage_call_site_send<C, F>(
    context: &C,
    call_site: StorageCallSite,
    future: F,
) -> F::Output
where
    C: StorageContext + ?Sized,
    F: Future + Send,
    F::Output: Send,
{
    storage_handle(context)
        .run_with_call_site_send(call_site, future)
        .await
}

/// Run application work with durable mutation provenance interpreted by the
/// selected storage backend.
pub async fn with_mutation_provenance<C, F>(
    context: &C,
    provenance: Option<MutationProvenance>,
    future: F,
) -> F::Output
where
    C: StorageContext + ?Sized,
    F: Future,
{
    storage_handle(context)
        .run_with_mutation_provenance(provenance, future)
        .await
}

/// Run a conditional mutation with a backend-neutral revision assertion.
pub async fn with_revision_precondition<C, F>(
    context: &C,
    precondition: Option<StorageRevisionPrecondition>,
    future: F,
) -> F::Output
where
    C: StorageContext + ?Sized,
    F: Future,
{
    storage_handle(context)
        .run_with_revision_precondition(precondition, future)
        .await
}
