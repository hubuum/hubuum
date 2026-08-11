use hubuum_events_core::EventEnvelope;
use hubuum_storage_core::{
    StorageAuditEvent, StorageAuditEventListQuery, StorageEventDelivery,
    StorageEventDeliveryListQuery, StorageEventPage, StorageEventSink, StorageEventSinkCreate,
    StorageEventSinkDelete, StorageEventSinkListQuery, StorageEventSinkUpdate,
    StorageEventSubscription, StorageEventSubscriptionCreate, StorageEventSubscriptionDelete,
    StorageEventSubscriptionListQuery, StorageEventSubscriptionUpdate,
};

use crate::errors::ApiError;
use crate::events::EventResponse;
use crate::models::{
    EventDelivery as EventDeliveryRow, EventDeliveryID, EventSink, EventSinkID, EventSinkRow,
    EventSubscription, EventSubscriptionID, EventSubscriptionRow, NewEventSinkRow,
    NewEventSubscriptionRow, UpdateEventSinkRow, UpdateEventSubscriptionRow,
};
use crate::storage::postgres::PostgresPool;

use super::event_subscription::{
    DeleteEventSinkRecord, DeleteEventSubscriptionRecord, SaveEventSinkRecord,
    SaveEventSubscriptionRecord, UpdateEventSinkRecord, UpdateEventSubscriptionRecord,
};

fn storage_audit_event(event: EventResponse) -> StorageAuditEvent {
    let before_revision = event
        .before_revision
        .map(crate::models::ResourceRevision::get);
    let after_revision = event
        .after_revision
        .map(crate::models::ResourceRevision::get);
    let envelope = EventEnvelope {
        id: event.id,
        event_id: event.event_id,
        occurred_at: event.occurred_at,
        entity_type: event.entity_type,
        entity_id: event.entity_id,
        entity_name: event.entity_name,
        collection_id: event.collection_id,
        action: event.action,
        actor_user_id: event.actor_user_id,
        actor_kind: event.actor_kind,
        provenance: event.provenance,
        request_id: event.request_id,
        correlation_id: event.correlation_id,
        summary: event.summary,
        before: event.before,
        after: event.after,
        metadata: event.metadata,
        schema_version: event.schema_version,
    };
    StorageAuditEvent::new(envelope, before_revision, after_revision)
}

pub(crate) async fn list_audit_events(
    pool: &PostgresPool,
    query: StorageAuditEventListQuery,
) -> Result<StorageEventPage<StorageAuditEvent>, ApiError> {
    let filters = query.filters();
    let filters = super::events::EventListFilters {
        entity_type: filters.entity_type_value(),
        entity_id: filters.entity_id_value(),
        action: filters.action_value(),
        actor_kind: filters.actor_kind_value(),
        actor_user_id: filters.actor_user_id_value(),
        initiator_user_id: filters.initiator_user_id_value(),
        collection_id: filters.collection_id_value(),
        occurred_after: filters.occurred_after_value(),
        occurred_before: filters.occurred_before_value(),
    };
    let include_total = query.options().include_total;
    let (events, total) = super::events::list_events_with_total_count(
        pool,
        query.accessible_collection_ids(),
        query.include_collection_less(),
        &filters,
        query.options(),
    )
    .await?;
    Ok(StorageEventPage::new(
        events.into_iter().map(storage_audit_event).collect(),
        include_total.then_some(total),
    ))
}

fn storage_event_sink(sink: EventSink) -> StorageEventSink {
    StorageEventSink::builder(
        sink.id,
        sink.name,
        sink.kind.as_str().to_string(),
        sink.created_at,
        sink.updated_at,
        sink.revision.get(),
    )
    .configuration(sink.config)
    .secret_ref(sink.secret_ref)
    .enabled(sink.enabled)
    .build()
}

fn storage_event_sink_row(sink: EventSinkRow) -> StorageEventSink {
    StorageEventSink::builder(
        sink.id,
        sink.name,
        sink.kind,
        sink.created_at,
        sink.updated_at,
        sink.revision.get(),
    )
    .configuration(sink.config)
    .secret_ref(sink.secret_ref)
    .enabled(sink.enabled)
    .build()
}

pub(crate) async fn enabled_event_sink_count(pool: &PostgresPool) -> Result<i64, ApiError> {
    super::event_subscription::enabled_event_sink_count(pool).await
}

pub(crate) async fn list_event_sinks(
    pool: &PostgresPool,
    query: StorageEventSinkListQuery,
) -> Result<StorageEventPage<StorageEventSink>, ApiError> {
    let include_total = query.options().include_total;
    let (sinks, total) = EventSink::list_with_total_count(pool, query.options()).await?;
    Ok(StorageEventPage::new(
        sinks.into_iter().map(storage_event_sink).collect(),
        include_total.then_some(total),
    ))
}

pub(crate) async fn load_event_sink(
    pool: &PostgresPool,
    sink_id: i32,
) -> Result<StorageEventSink, ApiError> {
    EventSinkID::new(sink_id)?
        .instance(pool)
        .await
        .map(storage_event_sink)
}

pub(crate) async fn create_event_sink(
    pool: &PostgresPool,
    request: StorageEventSinkCreate,
) -> Result<StorageEventSink, ApiError> {
    let row = NewEventSinkRow {
        name: request.name().to_string(),
        kind: request.kind().to_string(),
        config: request.configuration().clone(),
        secret_ref: request.secret_ref().map(str::to_string),
        enabled: request.enabled(),
    };
    row.save_event_sink_record(pool, request.event_context())
        .await
        .map(storage_event_sink_row)
}

pub(crate) async fn update_event_sink(
    pool: &PostgresPool,
    request: StorageEventSinkUpdate,
) -> Result<StorageEventSink, ApiError> {
    let row = UpdateEventSinkRow {
        name: request.name_value().map(str::to_string),
        kind: request.kind_value().map(str::to_string),
        config: request.configuration_value().cloned(),
        secret_ref: request
            .secret_ref_value()
            .map(|value| value.map(str::to_string)),
        enabled: request.enabled_value(),
    };
    row.update_event_sink_record(pool, request.id(), request.event_context())
        .await
        .map(storage_event_sink_row)
}

pub(crate) async fn delete_event_sink(
    pool: &PostgresPool,
    request: StorageEventSinkDelete,
) -> Result<(), ApiError> {
    EventSinkID::new(request.id())?
        .delete_event_sink_record(pool, request.event_context())
        .await
}

fn storage_event_subscription(subscription: EventSubscription) -> StorageEventSubscription {
    StorageEventSubscription::builder(
        subscription.id,
        subscription.collection_id,
        subscription.sink_id,
        subscription.name,
        subscription.created_at,
        subscription.updated_at,
        subscription.revision.get(),
    )
    .description(subscription.description)
    .entity_types(subscription.entity_types)
    .actions(subscription.actions)
    .filter(subscription.filter)
    .routing(subscription.routing)
    .enabled(subscription.enabled)
    .build()
}

fn storage_event_subscription_row(
    subscription: EventSubscriptionRow,
) -> Result<StorageEventSubscription, ApiError> {
    EventSubscription::try_from(subscription).map(storage_event_subscription)
}

pub(crate) async fn list_event_subscriptions(
    pool: &PostgresPool,
    query: StorageEventSubscriptionListQuery,
) -> Result<StorageEventPage<StorageEventSubscription>, ApiError> {
    let include_total = query.options().include_total;
    let (subscriptions, total) =
        EventSubscription::list_with_total_count(pool, query.collection_id(), query.options())
            .await?;
    Ok(StorageEventPage::new(
        subscriptions
            .into_iter()
            .map(storage_event_subscription)
            .collect(),
        include_total.then_some(total),
    ))
}

async fn load_scoped_event_subscription(
    pool: &PostgresPool,
    collection_id: i32,
    subscription_id: i32,
) -> Result<EventSubscription, ApiError> {
    let subscription = EventSubscriptionID::new(subscription_id)?
        .instance(pool)
        .await?;
    if subscription.collection_id != collection_id {
        return Err(ApiError::NotFound(
            "Event subscription not found in collection".to_string(),
        ));
    }
    Ok(subscription)
}

pub(crate) async fn load_event_subscription(
    pool: &PostgresPool,
    collection_id: i32,
    subscription_id: i32,
) -> Result<StorageEventSubscription, ApiError> {
    load_scoped_event_subscription(pool, collection_id, subscription_id)
        .await
        .map(storage_event_subscription)
}

pub(crate) async fn create_event_subscription(
    pool: &PostgresPool,
    request: StorageEventSubscriptionCreate,
) -> Result<StorageEventSubscription, ApiError> {
    let row = NewEventSubscriptionRow {
        collection_id: request.collection_id(),
        sink_id: request.sink_id(),
        name: request.name().to_string(),
        description: request.description().to_string(),
        entity_types: serde_json::to_value(request.entity_types())?,
        actions: serde_json::to_value(request.actions())?,
        filter: serde_json::to_value(request.filter())?,
        routing: request.routing().clone(),
        enabled: request.enabled(),
    };
    storage_event_subscription_row(
        row.save_event_subscription_record(pool, request.event_context())
            .await?,
    )
}

pub(crate) async fn update_event_subscription(
    pool: &PostgresPool,
    request: StorageEventSubscriptionUpdate,
) -> Result<StorageEventSubscription, ApiError> {
    load_scoped_event_subscription(pool, request.collection_id(), request.id()).await?;
    let row = UpdateEventSubscriptionRow {
        sink_id: request.sink_id_value(),
        name: request.name_value().map(str::to_string),
        description: request.description_value().map(str::to_string),
        entity_types: request
            .entity_types_value()
            .map(serde_json::to_value)
            .transpose()?,
        actions: request
            .actions_value()
            .map(serde_json::to_value)
            .transpose()?,
        filter: request
            .filter_value()
            .map(serde_json::to_value)
            .transpose()?,
        routing: request.routing_value().cloned(),
        enabled: request.enabled_value(),
    };
    storage_event_subscription_row(
        row.update_event_subscription_record(pool, request.id(), request.event_context())
            .await?,
    )
}

pub(crate) async fn delete_event_subscription(
    pool: &PostgresPool,
    request: StorageEventSubscriptionDelete,
) -> Result<(), ApiError> {
    load_scoped_event_subscription(pool, request.collection_id(), request.id()).await?;
    EventSubscriptionID::new(request.id())?
        .delete_event_subscription_record(pool, request.event_context())
        .await
}

fn storage_event_delivery(delivery: EventDeliveryRow) -> StorageEventDelivery {
    StorageEventDelivery::builder(
        delivery.id,
        delivery.event_id,
        delivery.subscription_id,
        delivery.status,
        delivery.next_attempt_at,
        delivery.created_at,
        delivery.updated_at,
    )
    .attempts(delivery.attempts)
    .last_error(delivery.last_error)
    .locked_until(delivery.locked_until)
    .build()
}

pub(crate) async fn list_event_deliveries(
    pool: &PostgresPool,
    query: StorageEventDeliveryListQuery,
) -> Result<StorageEventPage<StorageEventDelivery>, ApiError> {
    let include_total = query.options().include_total;
    let (deliveries, total) = super::event_delivery::list_event_deliveries_with_total_count(
        pool,
        query.subscription_id_value(),
        query.options(),
    )
    .await?;
    Ok(StorageEventPage::new(
        deliveries.into_iter().map(storage_event_delivery).collect(),
        include_total.then_some(total),
    ))
}

pub(crate) async fn load_event_delivery(
    pool: &PostgresPool,
    delivery_id: i64,
) -> Result<StorageEventDelivery, ApiError> {
    super::event_delivery::load_event_delivery(pool, EventDeliveryID::new(delivery_id)?)
        .await
        .map(storage_event_delivery)
}

pub(crate) async fn release_event_delivery_for_retry(
    pool: &PostgresPool,
    delivery_id: i64,
) -> Result<StorageEventDelivery, ApiError> {
    super::event_delivery::release_event_delivery_for_retry(
        pool,
        EventDeliveryID::new(delivery_id)?,
    )
    .await
    .map(storage_event_delivery)
}

pub(crate) async fn mark_event_delivery_dead(
    pool: &PostgresPool,
    delivery_id: i64,
) -> Result<StorageEventDelivery, ApiError> {
    super::event_delivery::mark_event_delivery_dead(pool, EventDeliveryID::new(delivery_id)?)
        .await
        .map(storage_event_delivery)
}
