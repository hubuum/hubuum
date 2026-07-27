use crate::db::prelude::*;
use crate::db::{DbConnection, DbPool, with_connection};
use crate::errors::ApiError;
use crate::models::MaintenanceState;

pub(crate) async fn maintenance_state_conn(
    conn: &mut DbConnection,
) -> Result<MaintenanceState, ApiError> {
    use crate::schema::system_maintenance::dsl::{id, state, system_maintenance};

    let state_value = system_maintenance
        .filter(id.eq(1_i16))
        .select(state)
        .first::<String>(conn)
        .await?;
    MaintenanceState::from_db(&state_value)
}

pub(crate) async fn maintenance_state_db(pool: &DbPool) -> Result<MaintenanceState, ApiError> {
    with_connection(pool, maintenance_state_conn).await
}
