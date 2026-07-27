use crate::db::prelude::*;
use crate::db::{database_schema_is_ready, with_connection};
use crate::errors::ApiError;
use crate::models::MaintenanceState;
use crate::traits::BackendContext;

pub(crate) struct ReadinessSnapshot {
    schema_ready: bool,
    maintenance_state: MaintenanceState,
}

impl ReadinessSnapshot {
    pub(crate) fn schema_is_ready(&self) -> bool {
        self.schema_ready
    }

    pub(crate) fn maintenance_state(&self) -> MaintenanceState {
        self.maintenance_state
    }
}

pub(crate) trait ProbeBackend {
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

            Ok::<_, ApiError>(ReadinessSnapshot {
                schema_ready,
                maintenance_state: MaintenanceState::from_db(&maintenance_state)?,
            })
        })
        .await
    }
}
