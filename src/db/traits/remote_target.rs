use diesel::prelude::*;

use crate::apply_query_options;
use crate::db::{DbPool, with_connection};
use crate::errors::ApiError;
use crate::models::remote_target::{
    NewRemoteCallResult, NewRemoteTargetRow, RemoteCallResult, RemoteTarget, RemoteTargetID,
    RemoteTargetRow, UpdateRemoteTargetRow,
};
use crate::models::search::{FilterField, QueryOptions};
use crate::{date_search, numeric_search, string_search};

pub(crate) trait LoadRemoteTargetRecord {
    async fn load_remote_target_record(&self, pool: &DbPool) -> Result<RemoteTargetRow, ApiError>;
}

impl LoadRemoteTargetRecord for RemoteTargetID {
    async fn load_remote_target_record(&self, pool: &DbPool) -> Result<RemoteTargetRow, ApiError> {
        use crate::schema::remote_targets::dsl::{id, remote_targets};

        with_connection(pool, |conn| {
            remote_targets
                .filter(id.eq(self.id()))
                .first::<RemoteTargetRow>(conn)
        })
    }
}

pub(crate) trait SaveRemoteTargetRecord {
    async fn save_remote_target_record(&self, pool: &DbPool) -> Result<RemoteTargetRow, ApiError>;
}

impl SaveRemoteTargetRecord for NewRemoteTargetRow {
    async fn save_remote_target_record(&self, pool: &DbPool) -> Result<RemoteTargetRow, ApiError> {
        use crate::schema::remote_targets::dsl::remote_targets;

        with_connection(pool, |conn| {
            diesel::insert_into(remote_targets)
                .values(self)
                .get_result::<RemoteTargetRow>(conn)
        })
    }
}

pub(crate) trait UpdateRemoteTargetRecord {
    async fn update_remote_target_record(
        &self,
        pool: &DbPool,
        target_id: i32,
    ) -> Result<RemoteTargetRow, ApiError>;
}

impl UpdateRemoteTargetRecord for UpdateRemoteTargetRow {
    async fn update_remote_target_record(
        &self,
        pool: &DbPool,
        target_id: i32,
    ) -> Result<RemoteTargetRow, ApiError> {
        use crate::schema::remote_targets::dsl::{id, remote_targets};

        with_connection(pool, |conn| {
            crate::db::updated_or_current(
                diesel::update(remote_targets.filter(id.eq(target_id)))
                    .set(self)
                    .get_result::<RemoteTargetRow>(conn)
                    .optional(),
                || remote_targets.filter(id.eq(target_id)).first(conn),
            )
        })
    }
}

pub(crate) trait DeleteRemoteTargetRecord {
    async fn delete_remote_target_record(&self, pool: &DbPool) -> Result<(), ApiError>;
}

impl DeleteRemoteTargetRecord for RemoteTargetID {
    async fn delete_remote_target_record(&self, pool: &DbPool) -> Result<(), ApiError> {
        use crate::schema::remote_targets::dsl::{id, remote_targets};

        with_connection(pool, |conn| {
            diesel::delete(remote_targets.filter(id.eq(self.id()))).execute(conn)
        })?;
        Ok(())
    }
}

pub(crate) async fn list_rows_with_total_count(
    pool: &DbPool,
    allowed_namespace_ids: &[i32],
    query_options: &QueryOptions,
) -> Result<(Vec<RemoteTargetRow>, i64), ApiError> {
    let query = build_list_query(allowed_namespace_ids, query_options)?;
    let total_count = with_connection(pool, |conn| query.count().get_result::<i64>(conn))?;

    let mut query = build_list_query(allowed_namespace_ids, query_options)?;
    apply_query_options!(query, query_options, RemoteTarget);
    let rows = with_connection(pool, |conn| query.load::<RemoteTargetRow>(conn))?;

    Ok((rows, total_count))
}

fn build_list_query<'a>(
    allowed_namespace_ids: &'a [i32],
    query_options: &'a QueryOptions,
) -> Result<crate::schema::remote_targets::BoxedQuery<'a, diesel::pg::Pg>, ApiError> {
    use crate::schema::remote_targets::dsl::{
        class_id, created_at, description, id, method, name, namespace_id, remote_targets,
        updated_at,
    };

    let mut query = remote_targets
        .into_boxed()
        .filter(namespace_id.eq_any(allowed_namespace_ids));

    for param in &query_options.filters {
        let operator = param.operator.clone();
        match param.field {
            FilterField::Id => numeric_search!(query, param, operator, id),
            FilterField::Name => string_search!(query, param, operator, name),
            FilterField::Description => string_search!(query, param, operator, description),
            FilterField::NamespaceId | FilterField::Namespaces => {
                numeric_search!(query, param, operator, namespace_id)
            }
            FilterField::ClassId => numeric_search!(query, param, operator, class_id),
            FilterField::Kind => string_search!(query, param, operator, method),
            FilterField::CreatedAt => date_search!(query, param, operator, created_at),
            FilterField::UpdatedAt => date_search!(query, param, operator, updated_at),
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{}' isn't searchable for remote targets",
                    param.field
                )));
            }
        }
    }

    Ok(query)
}

pub async fn insert_remote_call_result(
    pool: &DbPool,
    entry: NewRemoteCallResult,
) -> Result<RemoteCallResult, ApiError> {
    use crate::schema::remote_call_results::dsl::{remote_call_results, task_id};

    with_connection(pool, |conn| {
        diesel::insert_into(remote_call_results)
            .values(&entry)
            .on_conflict(task_id)
            .do_update()
            .set((
                crate::schema::remote_call_results::target_id.eq(entry.target_id),
                crate::schema::remote_call_results::subject_type.eq(entry.subject_type.clone()),
                crate::schema::remote_call_results::subject_id.eq(entry.subject_id),
                crate::schema::remote_call_results::method.eq(entry.method.clone()),
                crate::schema::remote_call_results::rendered_url.eq(entry.rendered_url.clone()),
                crate::schema::remote_call_results::response_status.eq(entry.response_status),
                crate::schema::remote_call_results::response_headers
                    .eq(entry.response_headers.clone()),
                crate::schema::remote_call_results::response_body_preview
                    .eq(entry.response_body_preview.clone()),
                crate::schema::remote_call_results::duration_ms.eq(entry.duration_ms),
                crate::schema::remote_call_results::success.eq(entry.success),
                crate::schema::remote_call_results::error.eq(entry.error.clone()),
            ))
            .get_result::<RemoteCallResult>(conn)
    })
}

impl RemoteTargetID {
    pub async fn instance(&self, pool: &DbPool) -> Result<RemoteTarget, ApiError> {
        self.load_remote_target_record(pool).await?.try_into()
    }
}

impl RemoteTarget {
    pub async fn list_with_total_count(
        pool: &DbPool,
        allowed_namespace_ids: &[i32],
        query_options: &QueryOptions,
    ) -> Result<(Vec<RemoteTarget>, i64), ApiError> {
        let (rows, total) =
            list_rows_with_total_count(pool, allowed_namespace_ids, query_options).await?;
        let targets = rows
            .into_iter()
            .map(RemoteTarget::try_from)
            .collect::<Result<Vec<_>, _>>()?;
        Ok((targets, total))
    }
}
