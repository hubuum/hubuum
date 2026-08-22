use diesel::prelude::{ExpressionMethods, QueryDsl};
use diesel_async::RunQueryDsl;
use hubuum_domain::MaintenanceState;

use crate::schema::system_maintenance::dsl::{id, state, system_maintenance};
use crate::{PostgresConnection, PostgresRuntime, PostgresStorageError};

pub async fn maintenance_state_on_connection(
    connection: &mut PostgresConnection,
) -> Result<MaintenanceState, PostgresStorageError> {
    let state_value = system_maintenance
        .filter(id.eq(1_i16))
        .select(state)
        .first::<String>(connection)
        .await?;
    MaintenanceState::try_from(state_value.as_str()).map_err(|error| {
        PostgresStorageError::database(format!("Invalid persisted maintenance state: {error}"))
    })
}

pub async fn load_maintenance_state(
    runtime: &PostgresRuntime,
) -> Result<MaintenanceState, PostgresStorageError> {
    runtime
        .with_connection(maintenance_state_on_connection)
        .await
}
