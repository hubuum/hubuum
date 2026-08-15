use async_trait::async_trait;

use super::*;

#[async_trait]
impl TransactionalStorage for StorageHandle {
    async fn transaction<F, R>(
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
        observe_storage_call(self.backend_name(), "transactions", "run", async {
            dispatch_backend!(self, |backend| {
                backend.transaction(event_context, operation).await
            })
        })
        .await
    }
}
