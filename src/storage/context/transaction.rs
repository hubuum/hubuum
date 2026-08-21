use async_trait::async_trait;

use super::*;

#[async_trait]
impl TransactionStorage for StorageHandle {
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
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::Transactions,
            "run",
            async {
                dispatch_backend!(self, |backend| {
                    backend.with_transaction(event_context, operation).await
                })
            },
        )
        .await
    }
}
