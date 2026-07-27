use crate::db::prelude::*;
use crate::db::{database_schema_is_ready, with_connection};
use crate::errors::ApiError;
use crate::traits::BackendContext;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadinessSnapshot {
    pub schema_ready: bool,
    pub maintenance_state: String,
}

pub trait ProbeBackend {
    async fn readiness_snapshot(&self) -> Result<ReadinessSnapshot, ApiError>;
}

impl<T> ProbeBackend for T
where
    T: BackendContext + Sync + ?Sized,
{
    async fn readiness_snapshot(&self) -> Result<ReadinessSnapshot, ApiError> {
        with_connection(self.db_pool(), async |conn| {
            use crate::schema::system_maintenance::dsl::{id, state, system_maintenance};

            let schema_ready = database_schema_is_ready(conn).await?;
            let maintenance_state = system_maintenance
                .filter(id.eq(1_i16))
                .select(state)
                .first::<String>(conn)
                .await?;

            Ok::<_, diesel::result::Error>(ReadinessSnapshot {
                schema_ready,
                maintenance_state,
            })
        })
        .await
    }
}
