use crate::errors::ApiError;
use crate::models::MaintenanceState;
use crate::storage::postgres::prelude::*;
use crate::storage::postgres::{PostgresConnection, PostgresPool, with_connection};

pub(crate) async fn maintenance_state_conn(
    conn: &mut PostgresConnection,
) -> Result<MaintenanceState, ApiError> {
    use crate::schema::system_maintenance::dsl::{id, state, system_maintenance};

    let state_value = system_maintenance
        .filter(id.eq(1_i16))
        .select(state)
        .first::<String>(conn)
        .await?;
    MaintenanceState::try_from(state_value.as_str())
        .map_err(|error| ApiError::InternalServerError(error.to_string()))
}

pub(crate) async fn load_maintenance_state(
    pool: &PostgresPool,
) -> Result<MaintenanceState, ApiError> {
    with_connection(pool, maintenance_state_conn).await
}
