use crate::db::prelude::*;
use crate::db::{DbConnection, DbPool, with_connection};
use crate::errors::ApiError;

pub(crate) async fn maintenance_state_conn(
    conn: &mut DbConnection,
) -> Result<String, diesel::result::Error> {
    use crate::schema::system_maintenance::dsl::{id, state, system_maintenance};

    system_maintenance
        .filter(id.eq(1_i16))
        .select(state)
        .first::<String>(conn)
        .await
}

pub(crate) async fn maintenance_state_db(pool: &DbPool) -> Result<String, ApiError> {
    with_connection(pool, maintenance_state_conn).await
}
