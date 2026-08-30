use super::*;

impl ExecutionStorage for MemoryStorage {
    fn run_in_scope<'a, F, R>(
        &'a self,
        scope: StorageExecutionScope,
        future: F,
    ) -> Pin<Box<dyn Future<Output = R> + 'a>>
    where
        F: Future<Output = R> + 'a,
        R: 'a,
    {
        let scope = MEMORY_EXECUTION_SCOPE
            .try_with(|current| merge_memory_execution_scope(current, &scope))
            .unwrap_or(scope);
        Box::pin(MEMORY_EXECUTION_SCOPE.scope(scope, future))
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
        let scope = MEMORY_EXECUTION_SCOPE
            .try_with(|current| merge_memory_execution_scope(current, &scope))
            .unwrap_or(scope);
        Box::pin(MEMORY_EXECUTION_SCOPE.scope(scope, future))
    }
}

fn merge_memory_execution_scope(
    parent: &StorageExecutionScope,
    child: &StorageExecutionScope,
) -> StorageExecutionScope {
    let mut merged = StorageExecutionScope::default();
    if let Some(call_site) = child
        .call_site_override()
        .or_else(|| parent.call_site_override())
    {
        merged = merged.with_call_site(call_site);
    }
    if let Some(provenance) = child
        .mutation_provenance_override()
        .or_else(|| parent.mutation_provenance_override())
    {
        merged = merged.with_mutation_provenance(provenance.clone());
    }
    if let Some(precondition) = child
        .revision_precondition_override()
        .or_else(|| parent.revision_precondition_override())
    {
        merged = merged.with_revision_precondition(precondition.clone());
    }
    if let Some(budget) = child
        .query_budget_override()
        .or_else(|| parent.query_budget_override())
    {
        merged = merged.with_query_budget(budget);
    }
    merged
}

pub(crate) fn enforce_memory_revision_precondition(
    target: StorageRevisionTarget,
    current_revision: ResourceRevision,
) -> Result<(), StorageError> {
    let precondition = MEMORY_EXECUTION_SCOPE
        .try_with(|scope| scope.revision_precondition_override().cloned().flatten())
        .ok()
        .flatten();
    let Some(precondition) = precondition.filter(|condition| condition.target() == target) else {
        return Ok(());
    };
    if precondition.revisions().is_empty() || precondition.revisions().contains(&current_revision) {
        return Ok(());
    }
    Err(StorageError::revision_conflict(
        "The resource revision does not match the requested precondition",
        current_revision,
    ))
}

struct MemoryTransaction {
    storage: MemoryStorage,
    event_context: EventContext,
}

impl StorageTransaction for MemoryTransaction {
    fn collections(&self) -> TransactionalCollections<'_> {
        TransactionalCollections::new(&self.storage, &self.event_context)
    }

    fn classes(&self) -> TransactionalClasses<'_> {
        TransactionalClasses::new(&self.storage, &self.event_context)
    }

    fn class_relations(&self) -> TransactionalClassRelations<'_> {
        TransactionalClassRelations::new(&self.storage, &self.event_context)
    }

    fn objects(&self) -> TransactionalObjects<'_> {
        TransactionalObjects::new(&self.storage, &self.event_context)
    }

    fn object_relations(&self) -> TransactionalObjectRelations<'_> {
        TransactionalObjectRelations::new(&self.storage, &self.event_context)
    }
}

#[async_trait]
impl TransactionStorage for MemoryStorage {
    async fn with_transaction<F, R>(
        &self,
        event_context: EventContext,
        operation: F,
    ) -> Result<R, StorageError>
    where
        F: for<'transaction> FnOnce(
                &'transaction dyn StorageTransaction,
            ) -> StorageTransactionFuture<'transaction, R>
            + Send,
        R: Send,
    {
        let mut committed = self.state.write().await;
        let transaction = MemoryTransaction {
            storage: Self {
                state: Arc::new(RwLock::new(committed.clone())),
            },
            event_context,
        };
        let result = operation(&transaction).await;
        if result.is_ok() {
            *committed = transaction.storage.state.read().await.clone();
        }
        result
    }
}

impl StorageBackend for MemoryStorage {}

#[test]
fn an_external_crate_can_implement_the_complete_backend_contract() {
    fn assert_complete<T: StorageBackend + Clone + 'static>() {}
    assert_complete::<MemoryStorage>();
}
