use diesel::prelude::*;
use serde_json::json;

use crate::apply_query_options;
use crate::db::{DbPool, with_connection, with_transaction};
use crate::errors::ApiError;
use crate::events::{Action, EntityType, EventContext, NewEvent, emit_event};
use crate::models::event_subscription::{
    EventSink, EventSinkID, EventSinkRow, EventSubscription, EventSubscriptionID,
    EventSubscriptionRow, NewEventSinkRow, NewEventSubscriptionRow, UpdateEventSinkRow,
    UpdateEventSubscriptionRow,
};
use crate::models::search::{FilterField, QueryOptions};

pub(crate) trait LoadEventSinkRecord {
    async fn load_event_sink_record(&self, pool: &DbPool) -> Result<EventSinkRow, ApiError>;
}

impl LoadEventSinkRecord for EventSinkID {
    async fn load_event_sink_record(&self, pool: &DbPool) -> Result<EventSinkRow, ApiError> {
        use crate::schema::event_sinks::dsl::{event_sinks, id};

        with_connection(pool, |conn| {
            event_sinks
                .filter(id.eq(self.id()))
                .first::<EventSinkRow>(conn)
        })
    }
}

pub(crate) trait SaveEventSinkRecord {
    async fn save_event_sink_record(
        &self,
        pool: &DbPool,
        event_context: &EventContext,
    ) -> Result<EventSinkRow, ApiError>;

    #[cfg(test)]
    async fn save_event_sink_record_without_events(
        &self,
        pool: &DbPool,
    ) -> Result<EventSinkRow, ApiError>;
}

impl SaveEventSinkRecord for NewEventSinkRow {
    async fn save_event_sink_record(
        &self,
        pool: &DbPool,
        event_context: &EventContext,
    ) -> Result<EventSinkRow, ApiError> {
        insert_event_sink_record(self, pool, Some(event_context)).await
    }

    #[cfg(test)]
    async fn save_event_sink_record_without_events(
        &self,
        pool: &DbPool,
    ) -> Result<EventSinkRow, ApiError> {
        insert_event_sink_record(self, pool, None).await
    }
}

async fn insert_event_sink_record(
    row: &NewEventSinkRow,
    pool: &DbPool,
    event_context: Option<&EventContext>,
) -> Result<EventSinkRow, ApiError> {
    use crate::schema::event_sinks::dsl::event_sinks;

    with_transaction(pool, |conn| -> Result<EventSinkRow, ApiError> {
        let created = diesel::insert_into(event_sinks)
            .values(row)
            .get_result::<EventSinkRow>(conn)?;
        emit_event_sink_audit(conn, Action::Created, event_context, None, &created)?;
        Ok(created)
    })
}

pub(crate) trait UpdateEventSinkRecord {
    async fn update_event_sink_record(
        &self,
        pool: &DbPool,
        sink_id: i32,
        event_context: &EventContext,
    ) -> Result<EventSinkRow, ApiError>;
}

impl UpdateEventSinkRecord for UpdateEventSinkRow {
    async fn update_event_sink_record(
        &self,
        pool: &DbPool,
        sink_id: i32,
        event_context: &EventContext,
    ) -> Result<EventSinkRow, ApiError> {
        update_event_sink_record_impl(self, pool, sink_id, Some(event_context)).await
    }
}

async fn update_event_sink_record_impl(
    row: &UpdateEventSinkRow,
    pool: &DbPool,
    sink_id: i32,
    event_context: Option<&EventContext>,
) -> Result<EventSinkRow, ApiError> {
    use crate::schema::event_sinks::dsl::{event_sinks, id};

    with_transaction(pool, |conn| -> Result<EventSinkRow, ApiError> {
        let before = event_sinks
            .filter(id.eq(sink_id))
            .first::<EventSinkRow>(conn)?;
        let updated = diesel::update(event_sinks.filter(id.eq(sink_id)))
            .set(row)
            .get_result::<EventSinkRow>(conn)?;
        emit_event_sink_audit(
            conn,
            Action::Updated,
            event_context,
            Some(&before),
            &updated,
        )?;
        Ok(updated)
    })
}

pub(crate) trait DeleteEventSinkRecord {
    async fn delete_event_sink_record(
        &self,
        pool: &DbPool,
        event_context: &EventContext,
    ) -> Result<(), ApiError>;
}

impl DeleteEventSinkRecord for EventSinkID {
    async fn delete_event_sink_record(
        &self,
        pool: &DbPool,
        event_context: &EventContext,
    ) -> Result<(), ApiError> {
        delete_event_sink_record_impl(self, pool, Some(event_context)).await
    }
}

async fn delete_event_sink_record_impl(
    sink_id: &EventSinkID,
    pool: &DbPool,
    event_context: Option<&EventContext>,
) -> Result<(), ApiError> {
    use crate::schema::event_sinks::dsl::{event_sinks, id};

    with_transaction(pool, |conn| -> Result<(), ApiError> {
        let before = event_sinks
            .filter(id.eq(sink_id.id()))
            .first::<EventSinkRow>(conn)?;
        emit_event_sink_audit(conn, Action::Deleted, event_context, Some(&before), &before)?;
        diesel::delete(event_sinks.filter(id.eq(sink_id.id())))
            .execute(conn)
            .map_err(ApiError::from)?;
        Ok(())
    })?;
    Ok(())
}

pub(crate) trait LoadEventSubscriptionRecord {
    async fn load_event_subscription_record(
        &self,
        pool: &DbPool,
    ) -> Result<EventSubscriptionRow, ApiError>;
}

impl LoadEventSubscriptionRecord for EventSubscriptionID {
    async fn load_event_subscription_record(
        &self,
        pool: &DbPool,
    ) -> Result<EventSubscriptionRow, ApiError> {
        use crate::schema::event_subscriptions::dsl::{event_subscriptions, id};

        with_connection(pool, |conn| {
            event_subscriptions
                .filter(id.eq(self.id()))
                .first::<EventSubscriptionRow>(conn)
        })
    }
}

pub(crate) trait SaveEventSubscriptionRecord {
    async fn save_event_subscription_record(
        &self,
        pool: &DbPool,
        event_context: &EventContext,
    ) -> Result<EventSubscriptionRow, ApiError>;

    #[cfg(test)]
    async fn save_event_subscription_record_without_events(
        &self,
        pool: &DbPool,
    ) -> Result<EventSubscriptionRow, ApiError>;
}

impl SaveEventSubscriptionRecord for NewEventSubscriptionRow {
    async fn save_event_subscription_record(
        &self,
        pool: &DbPool,
        event_context: &EventContext,
    ) -> Result<EventSubscriptionRow, ApiError> {
        insert_event_subscription_record(self, pool, Some(event_context)).await
    }

    #[cfg(test)]
    async fn save_event_subscription_record_without_events(
        &self,
        pool: &DbPool,
    ) -> Result<EventSubscriptionRow, ApiError> {
        insert_event_subscription_record(self, pool, None).await
    }
}

async fn insert_event_subscription_record(
    row: &NewEventSubscriptionRow,
    pool: &DbPool,
    event_context: Option<&EventContext>,
) -> Result<EventSubscriptionRow, ApiError> {
    use crate::schema::event_subscriptions::dsl::event_subscriptions;

    with_transaction(pool, |conn| -> Result<EventSubscriptionRow, ApiError> {
        let created = diesel::insert_into(event_subscriptions)
            .values(row)
            .get_result::<EventSubscriptionRow>(conn)?;
        emit_event_subscription_audit(conn, Action::Created, event_context, None, &created)?;
        Ok(created)
    })
}

pub(crate) trait UpdateEventSubscriptionRecord {
    async fn update_event_subscription_record(
        &self,
        pool: &DbPool,
        subscription_id: i32,
        event_context: &EventContext,
    ) -> Result<EventSubscriptionRow, ApiError>;
}

impl UpdateEventSubscriptionRecord for UpdateEventSubscriptionRow {
    async fn update_event_subscription_record(
        &self,
        pool: &DbPool,
        subscription_id: i32,
        event_context: &EventContext,
    ) -> Result<EventSubscriptionRow, ApiError> {
        update_event_subscription_record_impl(self, pool, subscription_id, Some(event_context))
            .await
    }
}

async fn update_event_subscription_record_impl(
    row: &UpdateEventSubscriptionRow,
    pool: &DbPool,
    subscription_id: i32,
    event_context: Option<&EventContext>,
) -> Result<EventSubscriptionRow, ApiError> {
    use crate::schema::event_subscriptions::dsl::{event_subscriptions, id};

    with_transaction(pool, |conn| -> Result<EventSubscriptionRow, ApiError> {
        let before = event_subscriptions
            .filter(id.eq(subscription_id))
            .first::<EventSubscriptionRow>(conn)?;
        let updated = diesel::update(event_subscriptions.filter(id.eq(subscription_id)))
            .set(row)
            .get_result::<EventSubscriptionRow>(conn)?;
        emit_event_subscription_audit(
            conn,
            Action::Updated,
            event_context,
            Some(&before),
            &updated,
        )?;
        Ok(updated)
    })
}

pub(crate) trait DeleteEventSubscriptionRecord {
    async fn delete_event_subscription_record(
        &self,
        pool: &DbPool,
        event_context: &EventContext,
    ) -> Result<(), ApiError>;
}

impl DeleteEventSubscriptionRecord for EventSubscriptionID {
    async fn delete_event_subscription_record(
        &self,
        pool: &DbPool,
        event_context: &EventContext,
    ) -> Result<(), ApiError> {
        delete_event_subscription_record_impl(self, pool, Some(event_context)).await
    }
}

async fn delete_event_subscription_record_impl(
    subscription_id: &EventSubscriptionID,
    pool: &DbPool,
    event_context: Option<&EventContext>,
) -> Result<(), ApiError> {
    use crate::schema::event_subscriptions::dsl::{event_subscriptions, id};

    with_transaction(pool, |conn| -> Result<(), ApiError> {
        let before = event_subscriptions
            .filter(id.eq(subscription_id.id()))
            .first::<EventSubscriptionRow>(conn)?;
        emit_event_subscription_audit(
            conn,
            Action::Deleted,
            event_context,
            Some(&before),
            &before,
        )?;
        diesel::delete(event_subscriptions.filter(id.eq(subscription_id.id())))
            .execute(conn)
            .map_err(ApiError::from)?;
        Ok(())
    })?;
    Ok(())
}

fn emit_event_sink_audit(
    conn: &mut PgConnection,
    action: Action,
    event_context: Option<&EventContext>,
    before: Option<&EventSinkRow>,
    after: &EventSinkRow,
) -> Result<(), ApiError> {
    let Some(event_context) = event_context else {
        return Ok(());
    };
    let event = NewEvent::new(
        EntityType::EventSink,
        action,
        event_context.actor_kind(),
        format!("Event sink '{}' {}", after.name, action.as_str()),
    )?
    .with_context(event_context)
    .with_entity_id(after.id)
    .with_entity_name(&after.name)
    .with_before_opt(before.map(event_sink_snapshot))
    .with_after_opt((action != Action::Deleted).then(|| event_sink_snapshot(after)))
    .with_metadata(json!({
        "sink_id": after.id,
        "kind": after.kind,
        "enabled": after.enabled,
    }));
    emit_event(conn, &event)?;
    Ok(())
}

fn emit_event_subscription_audit(
    conn: &mut PgConnection,
    action: Action,
    event_context: Option<&EventContext>,
    before: Option<&EventSubscriptionRow>,
    after: &EventSubscriptionRow,
) -> Result<(), ApiError> {
    let Some(event_context) = event_context else {
        return Ok(());
    };
    let event = NewEvent::new(
        EntityType::EventSubscription,
        action,
        event_context.actor_kind(),
        format!("Event subscription '{}' {}", after.name, action.as_str()),
    )?
    .with_context(event_context)
    .with_entity_id(after.id)
    .with_entity_name(&after.name)
    .with_collection_id(after.collection_id)
    .with_before_opt(before.map(event_subscription_snapshot))
    .with_after_opt((action != Action::Deleted).then(|| event_subscription_snapshot(after)))
    .with_metadata(json!({
        "subscription_id": after.id,
        "sink_id": after.sink_id,
        "collection_id": after.collection_id,
        "enabled": after.enabled,
    }));
    emit_event(conn, &event)?;
    Ok(())
}

fn event_sink_snapshot(row: &EventSinkRow) -> serde_json::Value {
    let config = crate::models::event_subscription::redact_event_sink_config(&row.config);
    json!({
        "id": row.id,
        "name": row.name,
        "kind": row.kind,
        "config": config,
        "secret_ref": row.secret_ref,
        "enabled": row.enabled,
    })
}

fn event_subscription_snapshot(row: &EventSubscriptionRow) -> serde_json::Value {
    json!({
        "id": row.id,
        "collection_id": row.collection_id,
        "sink_id": row.sink_id,
        "name": row.name,
        "description": row.description,
        "entity_types": row.entity_types,
        "actions": row.actions,
        "filter": row.filter,
        "routing": row.routing,
        "enabled": row.enabled,
    })
}

pub(crate) async fn list_event_sink_rows_with_total_count(
    pool: &DbPool,
    query_options: &QueryOptions,
) -> Result<(Vec<EventSinkRow>, i64), ApiError> {
    let query = build_event_sink_query(query_options)?;
    let total_count = crate::pagination::exact_count_or_skipped(query_options, || {
        with_connection(pool, |conn| query.count().get_result::<i64>(conn))
    })?;
    let mut query = build_event_sink_query(query_options)?;
    apply_query_options!(query, query_options, EventSink);
    let rows = with_connection(pool, |conn| query.load::<EventSinkRow>(conn))?;
    Ok((rows, total_count))
}

fn build_event_sink_query(
    query_options: &QueryOptions,
) -> Result<crate::schema::event_sinks::BoxedQuery<'static, diesel::pg::Pg>, ApiError> {
    use crate::schema::event_sinks::dsl::{created_at, event_sinks, id, kind, name};

    let mut query = event_sinks.into_boxed();
    for param in query_options.filters.clone() {
        let operator = param.operator.clone();
        match param.field {
            FilterField::Id => crate::numeric_search!(query, param, operator, id),
            FilterField::Name => crate::string_search!(query, param, operator, name),
            FilterField::Kind => crate::string_search!(query, param, operator, kind),
            FilterField::CreatedAt => crate::date_search!(query, param, operator, created_at),
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{}' is not searchable for event sinks",
                    param.field
                )));
            }
        }
    }
    Ok(query)
}

pub(crate) async fn list_event_subscription_rows_with_total_count(
    pool: &DbPool,
    collection: i32,
    query_options: &QueryOptions,
) -> Result<(Vec<EventSubscriptionRow>, i64), ApiError> {
    let base = build_event_subscription_query(collection, query_options)?;
    let total_count = crate::pagination::exact_count_or_skipped(query_options, || {
        with_connection(pool, |conn| base.count().get_result::<i64>(conn))
    })?;
    let mut query = build_event_subscription_query(collection, query_options)?;
    apply_query_options!(query, query_options, EventSubscription);
    let rows = with_connection(pool, |conn| query.load::<EventSubscriptionRow>(conn))?;
    Ok((rows, total_count))
}

fn build_event_subscription_query(
    collection: i32,
    query_options: &QueryOptions,
) -> Result<crate::schema::event_subscriptions::BoxedQuery<'static, diesel::pg::Pg>, ApiError> {
    use crate::schema::event_subscriptions::dsl::{
        collection_id, created_at, event_subscriptions, id, name,
    };

    let mut query = event_subscriptions
        .filter(collection_id.eq(collection))
        .into_boxed();
    for param in query_options.filters.clone() {
        let operator = param.operator.clone();
        match param.field {
            FilterField::Id => crate::numeric_search!(query, param, operator, id),
            FilterField::Name => crate::string_search!(query, param, operator, name),
            FilterField::CreatedAt => crate::date_search!(query, param, operator, created_at),
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{}' is not searchable for event subscriptions",
                    param.field
                )));
            }
        }
    }
    Ok(query)
}

impl EventSinkID {
    pub async fn instance(&self, pool: &DbPool) -> Result<EventSink, ApiError> {
        self.load_event_sink_record(pool).await?.try_into()
    }
}

impl EventSubscriptionID {
    pub async fn instance(&self, pool: &DbPool) -> Result<EventSubscription, ApiError> {
        self.load_event_subscription_record(pool).await?.try_into()
    }
}

impl EventSink {
    pub async fn list_with_total_count(
        pool: &DbPool,
        query_options: &QueryOptions,
    ) -> Result<(Vec<EventSink>, i64), ApiError> {
        let (rows, total) = list_event_sink_rows_with_total_count(pool, query_options).await?;
        let sinks = rows
            .into_iter()
            .map(EventSink::try_from)
            .collect::<Result<Vec<_>, _>>()?;
        Ok((sinks, total))
    }
}

impl EventSubscription {
    pub async fn list_with_total_count(
        pool: &DbPool,
        collection_id: i32,
        query_options: &QueryOptions,
    ) -> Result<(Vec<EventSubscription>, i64), ApiError> {
        let (rows, total) =
            list_event_subscription_rows_with_total_count(pool, collection_id, query_options)
                .await?;
        let subscriptions = rows
            .into_iter()
            .map(EventSubscription::try_from)
            .collect::<Result<Vec<_>, _>>()?;
        Ok((subscriptions, total))
    }
}
