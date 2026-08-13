#[cfg(test)]
use std::future::Future;

#[cfg(test)]
use tracing::error;

use crate::errors::ApiError;

/// Adapter-private failure boundaries used to prove PostgreSQL rollback
/// behavior. These are deliberately not part of the storage contract: each
/// adapter owns its transaction mechanics and its corresponding native tests.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PostgresFailpoint {
    CollectionCreateAfterRecords,
    TaskFinalizeAfterEvent,
}

impl PostgresFailpoint {
    #[cfg(test)]
    const fn as_str(self) -> &'static str {
        match self {
            Self::CollectionCreateAfterRecords => "collection_create_after_records",
            Self::TaskFinalizeAfterEvent => "task_finalize_after_event",
        }
    }
}

#[cfg(test)]
tokio::task_local! {
    static ACTIVE_FAILPOINT: PostgresFailpoint;
}

pub(super) fn check(point: PostgresFailpoint) -> Result<(), ApiError> {
    #[cfg(test)]
    if ACTIVE_FAILPOINT
        .try_with(|active| *active == point)
        .unwrap_or(false)
    {
        error!(
            message = "Injected PostgreSQL adapter failure",
            backend = "postgresql",
            failpoint = point.as_str(),
        );
        return Err(ApiError::DatabaseError(format!(
            "injected PostgreSQL failure at {}",
            point.as_str()
        )));
    }

    #[cfg(not(test))]
    let _ = point;

    Ok(())
}

#[cfg(test)]
pub(crate) async fn with_failpoint<T>(
    point: PostgresFailpoint,
    future: impl Future<Output = T>,
) -> T {
    ACTIVE_FAILPOINT.scope(point, future).await
}
