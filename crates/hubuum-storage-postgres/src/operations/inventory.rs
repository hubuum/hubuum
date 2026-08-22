use diesel::QueryableByName;
use diesel::sql_types::{BigInt, Integer, Nullable};
use diesel_async::RunQueryDsl;
use hubuum_domain::ClassId;
use hubuum_storage_core::{StorageInventoryCounts, StorageObjectsByClassCount};

use crate::{PostgresRuntime, PostgresStorageError};

#[derive(QueryableByName)]
struct ObjectsByClassCountRow {
    #[diesel(sql_type = BigInt)]
    total_objects: i64,
    #[diesel(sql_type = BigInt)]
    total_classes: i64,
    #[diesel(sql_type = BigInt)]
    total_collections: i64,
    #[diesel(sql_type = Nullable<Integer>)]
    class_id: Option<i32>,
    #[diesel(sql_type = Nullable<BigInt>)]
    count: Option<i64>,
}

pub async fn load_inventory_counts(
    runtime: &PostgresRuntime,
) -> Result<StorageInventoryCounts, PostgresStorageError> {
    runtime
        .with_connection(async |connection| {
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
            .load::<ObjectsByClassCountRow>(connection)
            .await?;
            let first = rows
                .as_slice()
                .first()
                .expect("inventory totals query always returns at least one row");
            let objects_by_class = rows
                .iter()
                .filter_map(|row| row.class_id.zip(row.count))
                .map(|(class_id, count)| {
                    Ok(StorageObjectsByClassCount::new(
                        ClassId::new(class_id)?,
                        count,
                    ))
                })
                .collect::<Result<Vec<_>, PostgresStorageError>>()?;

            Ok::<_, PostgresStorageError>(StorageInventoryCounts::new(
                first.total_objects,
                first.total_classes,
                first.total_collections,
                objects_by_class,
            ))
        })
        .await
}
