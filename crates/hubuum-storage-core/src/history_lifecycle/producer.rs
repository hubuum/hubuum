//! A durable, single-assignment producer slot. Drop is never completion.
use super::HistoryReadContext;
use crate::StorageError;
use std::fmt;
use std::future::Future;
use uuid::Uuid;

#[derive(Clone, Eq, PartialEq)]
pub struct HistoryProducerContext {
    id: Uuid,
    ownership: Uuid,
    parent: HistoryReadContext,
}
impl HistoryProducerContext {
    pub const fn id(&self) -> Uuid {
        self.id
    }
    /// Adapter comparison material; never log or serialize the nonce.
    pub const fn ownership_token(&self) -> Uuid {
        self.ownership
    }
    pub const fn parent(&self) -> &HistoryReadContext {
        &self.parent
    }
}
impl fmt::Debug for HistoryProducerContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HistoryProducerContext")
            .field("id", &self.id)
            .field("publication", &self.parent.id())
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
pub struct HistoryProducerOwner(HistoryProducerContext);
impl HistoryProducerOwner {
    /// Adapters return this only after atomic parent/slot admission. There is
    /// one assignment per parent lifetime, including a retired slot tombstone.
    pub fn generate(parent: HistoryReadContext) -> Self {
        Self(HistoryProducerContext {
            id: Uuid::new_v4(),
            ownership: Uuid::new_v4(),
            parent,
        })
    }
    pub fn context(&self) -> &HistoryProducerContext {
        &self.0
    }
    /// This future proves physical producer/consumer completion, independently
    /// of functional success. Error/drop/timeout leaves the native slot charged.
    pub async fn after_completed<F>(
        self,
        completed: F,
    ) -> Result<HistoryProducerSettlement, StorageError>
    where
        F: Future<Output = Result<(), StorageError>>,
    {
        completed.await?;
        Ok(HistoryProducerSettlement(self.0))
    }
}

#[derive(Debug)]
pub struct HistoryProducerSettlement(HistoryProducerContext);
impl HistoryProducerSettlement {
    pub const fn context(&self) -> &HistoryProducerContext {
        &self.0
    }
}

#[derive(Clone, Debug)]
pub(super) struct ProducerRecord {
    pub(super) context: HistoryProducerContext,
    pub(super) retired: bool,
    pub(super) publication_claimed: bool,
}
