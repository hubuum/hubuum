use diesel::prelude::*;

use crate::apply_query_options;
use crate::db::{DbPool, with_connection, with_transaction};
use crate::errors::ApiError;
use crate::events::{Action, EntityType, EventContext, NewEvent, emit_event};
use crate::models::remote_target::{
    NewRemoteCallResult, NewRemoteTargetRow, RemoteCallResult, RemoteTarget, RemoteTargetID,
    RemoteTargetRow, UpdateRemoteTargetRow,
};
use crate::models::search::{FilterField, QueryOptions};
use crate::{date_search, numeric_search, string_search};

fn remote_target_event(
    row: &RemoteTargetRow,
    action: Action,
    context: &EventContext,
    summary: impl Into<String>,
) -> Result<NewEvent, ApiError> {
    Ok(NewEvent::new(
        EntityType::RemoteTarget,
        action,
        context.actor_kind(),
        summary,
    )?
    .with_context(context)
    .with_entity_id(row.id)
    .with_entity_name(row.name.clone())
    .with_namespace_id(row.namespace_id))
}

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
    async fn save_remote_target_record_without_events(
        &self,
        pool: &DbPool,
    ) -> Result<RemoteTargetRow, ApiError>;

    async fn save_remote_target_record(
        &self,
        pool: &DbPool,
        context: Option<&EventContext>,
    ) -> Result<RemoteTargetRow, ApiError> {
        let _ = context;
        self.save_remote_target_record_without_events(pool).await
    }
}

impl SaveRemoteTargetRecord for NewRemoteTargetRow {
    async fn save_remote_target_record_without_events(
        &self,
        pool: &DbPool,
    ) -> Result<RemoteTargetRow, ApiError> {
        use crate::schema::remote_targets::dsl::remote_targets;

        with_connection(pool, |conn| {
            diesel::insert_into(remote_targets)
                .values(self)
                .get_result::<RemoteTargetRow>(conn)
        })
    }

    async fn save_remote_target_record(
        &self,
        pool: &DbPool,
        context: Option<&EventContext>,
    ) -> Result<RemoteTargetRow, ApiError> {
        let Some(context) = context else {
            return self.save_remote_target_record_without_events(pool).await;
        };

        use crate::schema::remote_targets::dsl::remote_targets;

        with_transaction(pool, |conn| -> Result<RemoteTargetRow, ApiError> {
            let row = diesel::insert_into(remote_targets)
                .values(self)
                .get_result::<RemoteTargetRow>(conn)?;
            let event = remote_target_event(
                &row,
                Action::Created,
                context,
                format!("Remote target '{}' created", row.name),
            )?
            .with_after(row.audit_snapshot());
            emit_event(conn, &event)?;
            Ok(row)
        })
    }
}

pub(crate) trait UpdateRemoteTargetRecord {
    async fn update_remote_target_record_without_events(
        &self,
        pool: &DbPool,
        target_id: i32,
    ) -> Result<RemoteTargetRow, ApiError>;

    async fn update_remote_target_record(
        &self,
        pool: &DbPool,
        target_id: i32,
        context: Option<&EventContext>,
    ) -> Result<RemoteTargetRow, ApiError> {
        let _ = context;
        self.update_remote_target_record_without_events(pool, target_id)
            .await
    }
}

impl UpdateRemoteTargetRecord for UpdateRemoteTargetRow {
    async fn update_remote_target_record_without_events(
        &self,
        pool: &DbPool,
        target_id: i32,
    ) -> Result<RemoteTargetRow, ApiError> {
        use crate::schema::remote_targets::dsl::{id, remote_targets};

        with_connection(pool, |conn| {
            let updated = diesel::update(remote_targets.filter(id.eq(target_id)))
                .set(self)
                .get_result::<RemoteTargetRow>(conn)
                .optional()?;
            match updated {
                Some(target) => Ok(target),
                None => remote_targets.filter(id.eq(target_id)).first(conn),
            }
        })
    }

    async fn update_remote_target_record(
        &self,
        pool: &DbPool,
        target_id: i32,
        context: Option<&EventContext>,
    ) -> Result<RemoteTargetRow, ApiError> {
        let Some(context) = context else {
            return self
                .update_remote_target_record_without_events(pool, target_id)
                .await;
        };

        use crate::schema::remote_targets::dsl::{id, remote_targets};

        with_transaction(pool, |conn| -> Result<RemoteTargetRow, ApiError> {
            let before = remote_targets
                .filter(id.eq(target_id))
                .first::<RemoteTargetRow>(conn)?;
            let after = diesel::update(remote_targets.filter(id.eq(target_id)))
                .set(self)
                .get_result::<RemoteTargetRow>(conn)?;
            let event = remote_target_event(
                &after,
                Action::Updated,
                context,
                format!("Remote target '{}' updated", after.name),
            )?
            .with_before(before.audit_snapshot())
            .with_after(after.audit_snapshot());
            emit_event(conn, &event)?;
            Ok(after)
        })
    }
}

pub(crate) trait DeleteRemoteTargetRecord {
    async fn delete_remote_target_record_without_events(
        &self,
        pool: &DbPool,
    ) -> Result<(), ApiError>;

    async fn delete_remote_target_record(
        &self,
        pool: &DbPool,
        context: Option<&EventContext>,
    ) -> Result<(), ApiError> {
        let _ = context;
        self.delete_remote_target_record_without_events(pool).await
    }
}

impl DeleteRemoteTargetRecord for RemoteTargetID {
    async fn delete_remote_target_record_without_events(
        &self,
        pool: &DbPool,
    ) -> Result<(), ApiError> {
        use crate::schema::remote_targets::dsl::{id, remote_targets};

        with_connection(pool, |conn| {
            diesel::delete(remote_targets.filter(id.eq(self.id()))).execute(conn)
        })?;
        Ok(())
    }

    async fn delete_remote_target_record(
        &self,
        pool: &DbPool,
        context: Option<&EventContext>,
    ) -> Result<(), ApiError> {
        let Some(context) = context else {
            return self.delete_remote_target_record_without_events(pool).await;
        };

        use crate::schema::remote_targets::dsl::{id, remote_targets};

        with_transaction(pool, |conn| -> Result<(), ApiError> {
            let before = remote_targets
                .filter(id.eq(self.id()))
                .first::<RemoteTargetRow>(conn)?;
            diesel::delete(remote_targets.filter(id.eq(self.id()))).execute(conn)?;
            let event = remote_target_event(
                &before,
                Action::Deleted,
                context,
                format!("Remote target '{}' deleted", before.name),
            )?
            .with_before(before.audit_snapshot());
            emit_event(conn, &event)?;
            Ok(())
        })
    }
}

pub(crate) async fn emit_remote_target_invoked_event(
    pool: &DbPool,
    target: &RemoteTarget,
    context: &EventContext,
    task_id: i32,
    subject_type: &str,
    subject_id: i32,
) -> Result<(), ApiError> {
    with_connection(pool, |conn| -> Result<(), ApiError> {
        let event = NewEvent::new(
            EntityType::RemoteTarget,
            Action::Invoked,
            context.actor_kind(),
            format!("Remote target '{}' invoked", target.name),
        )?
        .with_context(context)
        .with_entity_id(target.id)
        .with_entity_name(target.name.clone())
        .with_namespace_id(target.namespace_id)
        .with_metadata(serde_json::json!({
            "task_id": task_id,
            "subject_type": subject_type,
            "subject_id": subject_id,
        }));
        emit_event(conn, &event)?;
        Ok(())
    })
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
