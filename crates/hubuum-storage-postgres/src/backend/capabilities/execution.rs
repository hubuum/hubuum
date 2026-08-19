use super::super::*;

impl StorageBackendIdentity for PostgresStorage {
    fn storage_name(&self) -> &'static str {
        "postgresql"
    }
}

impl StorageExecution for PostgresStorage {
    fn run_in_scope<'a, F, R>(
        &'a self,
        scope: StorageExecutionScope,
        future: F,
    ) -> Pin<Box<dyn Future<Output = R> + 'a>>
    where
        F: Future<Output = R> + 'a,
        R: 'a,
    {
        let call_site = scope.call_site_override();
        let provenance = scope.mutation_provenance_override().cloned();
        let precondition = scope.revision_precondition_override().cloned();
        let query_budget = scope.query_budget_override();
        let mut scoped: Pin<Box<dyn Future<Output = R> + 'a>> = Box::pin(future);
        if let Some(query_budget) = query_budget {
            scoped = Box::pin(crate::with_query_budget(query_budget, scoped));
        }
        if let Some(precondition) = precondition {
            scoped = Box::pin(crate::with_revision_precondition(precondition, scoped));
        }
        if let Some(provenance) = provenance {
            scoped = Box::pin(crate::with_mutation_provenance(provenance, scoped));
        }
        if let Some(call_site) = call_site {
            scoped = Box::pin(crate::with_storage_call_site(call_site, scoped));
        }
        scoped
    }

    fn run_in_scope_send<'a, F, R>(
        &'a self,
        scope: StorageExecutionScope,
        future: F,
    ) -> Pin<Box<dyn Future<Output = R> + Send + 'a>>
    where
        F: Future<Output = R> + Send + 'a,
        R: Send + 'a,
    {
        let call_site = scope.call_site_override();
        let provenance = scope.mutation_provenance_override().cloned();
        let precondition = scope.revision_precondition_override().cloned();
        let query_budget = scope.query_budget_override();
        let mut scoped: Pin<Box<dyn Future<Output = R> + Send + 'a>> = Box::pin(future);
        if let Some(query_budget) = query_budget {
            scoped = Box::pin(crate::with_query_budget(query_budget, scoped));
        }
        if let Some(precondition) = precondition {
            scoped = Box::pin(crate::with_revision_precondition(precondition, scoped));
        }
        if let Some(provenance) = provenance {
            scoped = Box::pin(crate::with_mutation_provenance(provenance, scoped));
        }
        if let Some(call_site) = call_site {
            scoped = Box::pin(crate::with_storage_call_site(call_site, scoped));
        }
        scoped
    }
}
