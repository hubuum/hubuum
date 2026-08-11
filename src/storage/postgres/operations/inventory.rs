use diesel::sql_types::{BigInt, Integer};

use crate::errors::ApiError;
use crate::storage::postgres::prelude::*;
use crate::storage::postgres::{PostgresPool, with_connection};
use crate::storage::{StorageInventoryCounts, StorageObjectsByClassCount};

#[derive(QueryableByName)]
struct ObjectsByClassCountRow {
    #[diesel(sql_type = BigInt)]
    total_objects: i64,
    #[diesel(sql_type = BigInt)]
    total_classes: i64,
    #[diesel(sql_type = BigInt)]
    total_collections: i64,
    #[diesel(sql_type = diesel::sql_types::Nullable<Integer>)]
    class_id: Option<i32>,
    #[diesel(sql_type = diesel::sql_types::Nullable<BigInt>)]
    count: Option<i64>,
}

pub(crate) async fn load_inventory_counts(
    pool: &PostgresPool,
) -> Result<StorageInventoryCounts, ApiError> {
    with_connection(pool, async |conn| {
        let rows = diesel::sql_query(
            "WITH object_counts AS (\
                 SELECT hubuum_class_id AS class_id, COUNT(*) AS count \
                 FROM hubuumobject GROUP BY hubuum_class_id\
             ), totals AS (\
                 SELECT \
                     (SELECT COUNT(*) FROM hubuumobject) AS total_objects, \
                     (SELECT COUNT(*) FROM hubuumclass) AS total_classes, \
                     (SELECT COUNT(*) FROM collections) AS total_collections\
             ) \
             SELECT totals.total_objects, totals.total_classes, totals.total_collections, \
                    object_counts.class_id, object_counts.count \
             FROM totals LEFT JOIN object_counts ON TRUE \
             ORDER BY object_counts.class_id",
        )
        .load::<ObjectsByClassCountRow>(conn)
        .await?;
        let first = rows
            .as_slice()
            .first()
            .expect("inventory totals query always returns at least one row");
        let objects_by_class = rows
            .iter()
            .filter_map(|row| {
                row.class_id
                    .zip(row.count)
                    .map(|(class_id, count)| StorageObjectsByClassCount::new(class_id, count))
            })
            .collect();

        Ok::<_, ApiError>(StorageInventoryCounts::new(
            first.total_objects,
            first.total_classes,
            first.total_collections,
            objects_by_class,
        ))
    })
    .await
}
