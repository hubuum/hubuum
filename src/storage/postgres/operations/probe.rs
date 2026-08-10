use crate::errors::ApiError;
use crate::models::MaintenanceState;
use crate::storage::ReadinessSnapshot;
use crate::storage::postgres::prelude::*;
use crate::storage::postgres::{PostgresPool, postgres_schema_is_ready, with_connection};

pub(crate) async fn load_readiness_snapshot(
    pool: &PostgresPool,
) -> Result<ReadinessSnapshot, ApiError> {
    with_connection(pool, async |conn| {
        use crate::schema::system_maintenance::dsl::{id, state, system_maintenance};

        let schema_ready = postgres_schema_is_ready(conn).await?;
        let maintenance_state = system_maintenance
            .filter(id.eq(1_i16))
            .select(state)
            .first::<String>(conn)
            .await?;
        let maintenance_state = MaintenanceState::try_from(maintenance_state.as_str())
            .map_err(|error| ApiError::InternalServerError(error.to_string()))?;

        Ok::<_, ApiError>(ReadinessSnapshot::new(schema_ready, maintenance_state))
    })
    .await
}
