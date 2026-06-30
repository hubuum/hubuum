use diesel::prelude::*;

use crate::apply_query_options;
use crate::db::{DbPool, with_connection};
use crate::errors::ApiError;
use crate::models::event_subscription::{
    EventSink, EventSinkID, EventSinkRow, EventSubscription, EventSubscriptionID,
    EventSubscriptionRow, NewEventSinkRow, NewEventSubscriptionRow, UpdateEventSinkRow,
    UpdateEventSubscriptionRow,
};
use crate::models::search::QueryOptions;

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
    async fn save_event_sink_record(&self, pool: &DbPool) -> Result<EventSinkRow, ApiError>;
}

impl SaveEventSinkRecord for NewEventSinkRow {
    async fn save_event_sink_record(&self, pool: &DbPool) -> Result<EventSinkRow, ApiError> {
        use crate::schema::event_sinks::dsl::event_sinks;

        with_connection(pool, |conn| {
            diesel::insert_into(event_sinks)
                .values(self)
                .get_result::<EventSinkRow>(conn)
        })
    }
}

pub(crate) trait UpdateEventSinkRecord {
    async fn update_event_sink_record(
        &self,
        pool: &DbPool,
        sink_id: i32,
    ) -> Result<EventSinkRow, ApiError>;
}

impl UpdateEventSinkRecord for UpdateEventSinkRow {
    async fn update_event_sink_record(
        &self,
        pool: &DbPool,
        sink_id: i32,
    ) -> Result<EventSinkRow, ApiError> {
        use crate::schema::event_sinks::dsl::{event_sinks, id};

        with_connection(pool, |conn| {
            diesel::update(event_sinks.filter(id.eq(sink_id)))
                .set(self)
                .get_result::<EventSinkRow>(conn)
        })
    }
}

pub(crate) trait DeleteEventSinkRecord {
    async fn delete_event_sink_record(&self, pool: &DbPool) -> Result<(), ApiError>;
}

impl DeleteEventSinkRecord for EventSinkID {
    async fn delete_event_sink_record(&self, pool: &DbPool) -> Result<(), ApiError> {
        use crate::schema::event_sinks::dsl::{event_sinks, id};

        with_connection(pool, |conn| {
            diesel::delete(event_sinks.filter(id.eq(self.id()))).execute(conn)
        })?;
        Ok(())
    }
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
    ) -> Result<EventSubscriptionRow, ApiError>;
}

impl SaveEventSubscriptionRecord for NewEventSubscriptionRow {
    async fn save_event_subscription_record(
        &self,
        pool: &DbPool,
    ) -> Result<EventSubscriptionRow, ApiError> {
        use crate::schema::event_subscriptions::dsl::event_subscriptions;

        with_connection(pool, |conn| {
            diesel::insert_into(event_subscriptions)
                .values(self)
                .get_result::<EventSubscriptionRow>(conn)
        })
    }
}

pub(crate) trait UpdateEventSubscriptionRecord {
    async fn update_event_subscription_record(
        &self,
        pool: &DbPool,
        subscription_id: i32,
    ) -> Result<EventSubscriptionRow, ApiError>;
}

impl UpdateEventSubscriptionRecord for UpdateEventSubscriptionRow {
    async fn update_event_subscription_record(
        &self,
        pool: &DbPool,
        subscription_id: i32,
    ) -> Result<EventSubscriptionRow, ApiError> {
        use crate::schema::event_subscriptions::dsl::{event_subscriptions, id};

        with_connection(pool, |conn| {
            diesel::update(event_subscriptions.filter(id.eq(subscription_id)))
                .set(self)
                .get_result::<EventSubscriptionRow>(conn)
        })
    }
}

pub(crate) trait DeleteEventSubscriptionRecord {
    async fn delete_event_subscription_record(&self, pool: &DbPool) -> Result<(), ApiError>;
}

impl DeleteEventSubscriptionRecord for EventSubscriptionID {
    async fn delete_event_subscription_record(&self, pool: &DbPool) -> Result<(), ApiError> {
        use crate::schema::event_subscriptions::dsl::{event_subscriptions, id};

        with_connection(pool, |conn| {
            diesel::delete(event_subscriptions.filter(id.eq(self.id()))).execute(conn)
        })?;
        Ok(())
    }
}

pub(crate) async fn list_event_sink_rows_with_total_count(
    pool: &DbPool,
    query_options: &QueryOptions,
) -> Result<(Vec<EventSinkRow>, i64), ApiError> {
    use crate::schema::event_sinks::dsl::event_sinks;

    let total_count = with_connection(pool, |conn| event_sinks.count().get_result::<i64>(conn))?;
    let mut query = event_sinks.into_boxed();
    apply_query_options!(query, query_options, EventSink);
    let rows = with_connection(pool, |conn| query.load::<EventSinkRow>(conn))?;
    Ok((rows, total_count))
}

pub(crate) async fn list_event_subscription_rows_with_total_count(
    pool: &DbPool,
    namespace: i32,
    query_options: &QueryOptions,
) -> Result<(Vec<EventSubscriptionRow>, i64), ApiError> {
    use crate::schema::event_subscriptions::dsl::{event_subscriptions, namespace_id};

    let base = event_subscriptions.filter(namespace_id.eq(namespace));
    let total_count = with_connection(pool, |conn| base.count().get_result::<i64>(conn))?;
    let mut query = event_subscriptions
        .filter(namespace_id.eq(namespace))
        .into_boxed();
    apply_query_options!(query, query_options, EventSubscription);
    let rows = with_connection(pool, |conn| query.load::<EventSubscriptionRow>(conn))?;
    Ok((rows, total_count))
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
        namespace_id: i32,
        query_options: &QueryOptions,
    ) -> Result<(Vec<EventSubscription>, i64), ApiError> {
        let (rows, total) =
            list_event_subscription_rows_with_total_count(pool, namespace_id, query_options)
                .await?;
        let subscriptions = rows
            .into_iter()
            .map(EventSubscription::try_from)
            .collect::<Result<Vec<_>, _>>()?;
        Ok((subscriptions, total))
    }
}
