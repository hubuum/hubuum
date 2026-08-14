use std::future::Future;

use tracing::error;

use crate::PostgresStorageError;

/// PostgreSQL transaction boundaries exposed only for rollback verification.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PostgresFailpoint {
    CollectionCreateAfterRecords,
    TaskFinalizeAfterEvent,
}

impl PostgresFailpoint {
    const fn as_str(self) -> &'static str {
        match self {
            Self::CollectionCreateAfterRecords => "collection_create_after_records",
            Self::TaskFinalizeAfterEvent => "task_finalize_after_event",
        }
    }
}

tokio::task_local! {
    static ACTIVE_FAILPOINT: PostgresFailpoint;
}

#[doc(hidden)]
pub fn check_failpoint(point: PostgresFailpoint) -> Result<(), PostgresStorageError> {
    if ACTIVE_FAILPOINT
        .try_with(|active| *active == point)
        .unwrap_or(false)
    {
        error!(
            message = "Injected PostgreSQL adapter failure",
            backend = "postgresql",
            failpoint = point.as_str(),
        );
        return Err(PostgresStorageError::database(format!(
            "injected PostgreSQL failure at {}",
            point.as_str()
        )));
    }
    Ok(())
}

#[doc(hidden)]
pub async fn with_failpoint<T>(point: PostgresFailpoint, future: impl Future<Output = T>) -> T {
    ACTIVE_FAILPOINT.scope(point, future).await
}
