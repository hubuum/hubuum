use std::collections::HashMap;

use crate::db::prelude::*;
use crate::db::{DbPool, with_connection};
use crate::errors::ApiError;
use crate::models::search::QueryOptions;
use chrono::{DateTime, Utc};

/// Batch-resolve a set of actor ids to principal names (anonymized users keep
/// their tombstoned principal name; ids with no matching principal are absent).
pub async fn resolve_actor_usernames(
    pool: &DbPool,
    mut actor_ids: Vec<i32>,
) -> Result<HashMap<i32, String>, ApiError> {
    use crate::schema::principals::dsl::{id, name, principals};
    actor_ids.sort_unstable();
    actor_ids.dedup();
    if actor_ids.is_empty() {
        return Ok(HashMap::new());
    }
    let rows: Vec<(i32, String)> = with_connection(pool, async |conn| {
        principals
            .filter(id.eq_any(&actor_ids))
            .select((id, name))
            .load(conn)
            .await
    })
    .await?;
    Ok(rows.into_iter().collect())
}

macro_rules! history_db_fns {
    ($paginate_fn:ident, $as_of_fn:ident, $($schema:tt)::+, $ty:ty) => {
        pub async fn $paginate_fn(
            entity_id: i32,
            pool: &$crate::db::DbPool,
            query_options: &$crate::models::search::QueryOptions,
        ) -> Result<(Vec<$ty>, i64), $crate::errors::ApiError> {
            use $crate::db::prelude::*;
            use $($schema)::+::dsl::*;
            let total = $crate::pagination::exact_count_or_skipped(query_options, async || {
                $crate::db::with_connection(pool, async |conn| {
                    $($schema)::+::table
                        .filter(id.eq(entity_id))
                        .count()
                        .get_result::<i64>(conn)
                        .await
                })
                .await
            }).await?;
            let mut query = $($schema)::+::table.into_boxed().filter(id.eq(entity_id));
            $crate::apply_query_options!(query, query_options, $ty);
            let items = $crate::db::with_connection(pool, async |conn| {
                query.load::<$ty>(conn).await
            }).await?;
            Ok((items, total))
        }

        pub async fn $as_of_fn(
            entity_id: i32,
            at: chrono::DateTime<chrono::Utc>,
            pool: &$crate::db::DbPool,
        ) -> Result<Option<$ty>, $crate::errors::ApiError> {
            use $crate::db::prelude::*;
            use $($schema)::+::dsl::*;
            $crate::db::with_connection(pool, async |conn| {
                $($schema)::+::table
                    .into_boxed()
                    .filter(id.eq(entity_id))
                    .filter(valid_from.le(at))
                    .filter(valid_to.is_null().or(valid_to.gt(at)))
                    .order(history_id.desc())
                    .first::<$ty>(conn)
                    .await
                    .optional()
            })
            .await
        }
    };
}

history_db_fns!(
    collection_history_paginated_with_total_count,
    collection_as_of,
    crate::schema::collections_history,
    crate::models::CollectionHistory
);

history_db_fns!(
    class_history_paginated_with_total_count,
    class_as_of,
    crate::schema::hubuumclass_history,
    crate::models::HubuumClassHistory
);

history_db_fns!(
    export_template_history_paginated_with_total_count,
    export_template_as_of,
    crate::schema::export_templates_history,
    crate::models::ExportTemplateHistory
);

history_db_fns!(
    remote_target_history_paginated_with_total_count,
    remote_target_as_of,
    crate::schema::remote_targets_history,
    crate::models::RemoteTargetHistory
);

pub async fn object_history_paginated_with_total_count(
    object_id: i32,
    class_id: i32,
    pool: &DbPool,
    query_options: &QueryOptions,
) -> Result<(Vec<crate::models::HubuumObjectHistory>, i64), ApiError> {
    use crate::schema::hubuumobject_history::dsl as history;

    let total = crate::pagination::exact_count_or_skipped(query_options, async || {
        with_connection(pool, async |conn| {
            history::hubuumobject_history
                .filter(history::id.eq(object_id))
                .filter(history::hubuum_class_id.eq(class_id))
                .count()
                .get_result::<i64>(conn)
                .await
        })
        .await
    })
    .await?;
    let mut query = history::hubuumobject_history
        .into_boxed()
        .filter(history::id.eq(object_id))
        .filter(history::hubuum_class_id.eq(class_id));
    crate::apply_query_options!(query, query_options, crate::models::HubuumObjectHistory);
    let items = with_connection(pool, async |conn| {
        query.load::<crate::models::HubuumObjectHistory>(conn).await
    })
    .await?;
    Ok((items, total))
}

pub async fn object_as_of(
    object_id: i32,
    class_id: i32,
    at: DateTime<Utc>,
    pool: &DbPool,
) -> Result<Option<crate::models::HubuumObjectHistory>, ApiError> {
    use crate::schema::hubuumobject_history::dsl as history;

    with_connection(pool, async |conn| {
        history::hubuumobject_history
            .into_boxed()
            .filter(history::id.eq(object_id))
            .filter(history::hubuum_class_id.eq(class_id))
            .filter(history::valid_from.le(at))
            .filter(history::valid_to.is_null().or(history::valid_to.gt(at)))
            .order(history::history_id.desc())
            .first::<crate::models::HubuumObjectHistory>(conn)
            .await
            .optional()
    })
    .await
}
