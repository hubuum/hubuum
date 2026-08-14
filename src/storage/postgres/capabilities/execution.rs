use super::super::*;

impl StorageIdentity for PostgresStorage {
    fn storage_name(&self) -> &'static str {
        "postgresql"
    }
}

impl ExportQueryStorage for PostgresStorage {
    fn run_export_queries<'a, F, R>(
        &'a self,
        budget: Option<StorageQueryBudget>,
        future: F,
    ) -> Pin<Box<dyn Future<Output = R> + 'a>>
    where
        F: Future<Output = R> + 'a,
        R: 'a,
    {
        Box::pin(runtime::with_export_query_budget_scope(budget, future))
    }
}

impl StorageExecution for PostgresStorage {
    fn run_with_call_site<'a, F, R>(
        &'a self,
        call_site: StorageCallSite,
        future: F,
    ) -> Pin<Box<dyn Future<Output = R> + 'a>>
    where
        F: Future<Output = R> + 'a,
        R: 'a,
    {
        Box::pin(runtime::with_storage_call_site_scope(
            call_site,
            hubuum_storage_postgres::with_storage_call_site(call_site, future),
        ))
    }

    fn run_with_call_site_send<'a, F, R>(
        &'a self,
        call_site: StorageCallSite,
        future: F,
    ) -> Pin<Box<dyn Future<Output = R> + Send + 'a>>
    where
        F: Future<Output = R> + Send + 'a,
        R: Send + 'a,
    {
        Box::pin(runtime::with_storage_call_site_scope(
            call_site,
            hubuum_storage_postgres::with_storage_call_site(call_site, future),
        ))
    }

    fn run_with_mutation_provenance<'a, F, R>(
        &'a self,
        provenance: Option<MutationProvenance>,
        future: F,
    ) -> Pin<Box<dyn Future<Output = R> + 'a>>
    where
        F: Future<Output = R> + 'a,
        R: 'a,
    {
        Box::pin(runtime::with_mutation_provenance_scope(provenance, future))
    }

    fn run_with_revision_precondition<'a, F, R>(
        &'a self,
        precondition: Option<StorageRevisionPrecondition>,
        future: F,
    ) -> Pin<Box<dyn Future<Output = R> + 'a>>
    where
        F: Future<Output = R> + 'a,
        R: 'a,
    {
        Box::pin(runtime::with_revision_precondition_scope(
            precondition,
            future,
        ))
    }
}
