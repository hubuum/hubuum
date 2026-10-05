use crate::models::SystemEventSubscription;
use crate::models::event_subscription::validate_webhook_templates;
use hubuum_domain::{CollectionId, PrincipalId};
use hubuum_events_core::EventSubscriptionScope;
use hubuum_storage_core::{EventSinkGrantAction, StorageEventSinkGrantChange};
use std::collections::HashMap;
use std::str::FromStr;

use crate::errors::ApiError;
use crate::events::{Action, ActorKind, EntityType, EventContext, EventResponse};
use crate::models::search::QueryOptions;
use crate::models::{
    EventDeliveryResponse, EventSink, EventSinkKind, EventSubscription, NewEventSink,
    NewEventSubscription, UpdateEventSink, UpdateEventSubscription, validate_sink_parts,
    validate_subscription_parts,
};
use crate::pagination::SKIPPED_TOTAL_COUNT;

use crate::storage::{
    AuditEventStorage, EventConfigurationStorage, EventDeliveryAdministrationStorage,
    StorageAuditEvent, StorageAuditEventFilters, StorageAuditEventListQuery, StorageContext,
    StorageEventDelivery, StorageEventDeliveryListQuery, StorageEventSink, StorageEventSinkCreate,
    StorageEventSinkDelete, StorageEventSinkListQuery, StorageEventSinkUpdate,
    StorageEventSubscription, StorageEventSubscriptionCreate, StorageEventSubscriptionDelete,
    StorageEventSubscriptionListQuery, StorageEventSubscriptionUpdate, storage_handle,
};
use crate::utilities::extensions::CustomStringExtensions;

fn storage_entity_types(values: &[String]) -> Result<Vec<EntityType>, ApiError> {
    values
        .iter()
        .map(|value| {
            EntityType::parse(value)
                .map_err(|error| ApiError::BadRequest(format!("bad entity_type: {error}")))
        })
        .collect()
}

fn storage_actions(values: &[String]) -> Result<Vec<Action>, ApiError> {
    values
        .iter()
        .map(|value| {
            Action::parse(value)
                .map_err(|error| ApiError::BadRequest(format!("bad action: {error}")))
        })
        .collect()
}

fn audit_event_from_storage(event: StorageAuditEvent) -> Result<EventResponse, ApiError> {
    let (event, before_revision, after_revision) = event.into_parts();
    Ok(EventResponse {
        id: event.id().get(),
        event_id: event.event_id(),
        occurred_at: event.occurred_at().naive_utc(),
        entity_type: event.entity_type().as_str().to_string(),
        entity_id: event.entity_id().map(|id| id.get()),
        entity_name: event.entity_name().map(ToOwned::to_owned),
        collection_id: event.collection_id().map(|id| id.id()),
        action: event.action().as_str().to_string(),
        actor_user_id: event.actor_user_id().map(|id| id.id()),
        actor_kind: event.actor_kind().as_str().to_string(),
        provenance: event.provenance().clone(),
        request_id: event.request_id(),
        correlation_id: event.correlation_id().map(|id| id.as_str().to_owned()),
        summary: event.summary().to_string(),
        before: event.before().cloned(),
        after: event.after().cloned(),
        metadata: event.metadata().clone(),
        schema_version: event.schema_version(),
        before_revision,
        after_revision,
    })
}

pub(crate) async fn list_audit_events(
    backend: &impl StorageContext,
    accessible_collection_ids: Vec<i32>,
    include_collection_less: bool,
    filters: StorageAuditEventFilters,
    options: QueryOptions,
) -> Result<(Vec<EventResponse>, i64), ApiError> {
    let page = storage_handle(backend)
        .list_audit_events(StorageAuditEventListQuery::new(
            accessible_collection_ids
                .into_iter()
                .map(CollectionId::new)
                .collect::<Result<Vec<_>, _>>()?,
            include_collection_less,
            filters,
            options,
        ))
        .await?;
    let (events, total) = page.into_parts();
    Ok((
        events
            .into_iter()
            .map(audit_event_from_storage)
            .collect::<Result<Vec<_>, _>>()?,
        total.unwrap_or(SKIPPED_TOTAL_COUNT),
    ))
}

pub(crate) fn parse_audit_event_filters(
    passthrough: &mut HashMap<String, Vec<String>>,
    entity_filter: Option<(EntityType, i32)>,
) -> Result<StorageAuditEventFilters, ApiError> {
    let entity_type = parse_optional_catalog_filter(passthrough, "entity_type", EntityType::parse)?;
    let entity_id = parse_optional_i32_filter(passthrough, "entity_id")?;
    let action = parse_optional_catalog_filter(passthrough, "action", Action::parse)?;
    let actor_kind = parse_optional_catalog_filter(passthrough, "actor_kind", ActorKind::parse)?;
    let actor_user_id = parse_optional_i32_filter(passthrough, "actor_user_id")?;
    let initiator_user_id = parse_optional_i32_filter(passthrough, "initiator_user_id")?;
    let collection_id = parse_optional_i32_filter(passthrough, "collection_id")?;
    let occurred_after = parse_optional_date_filter(passthrough, "occurred_after")?;
    let occurred_before = parse_optional_date_filter(passthrough, "occurred_before")?;
    let (entity_type, entity_id) = match entity_filter {
        Some(_) if entity_type.is_some() => {
            return Err(ApiError::BadRequest(
                "entity_type is fixed by this route".to_string(),
            ));
        }
        Some(_) if entity_id.is_some() => {
            return Err(ApiError::BadRequest(
                "entity_id is fixed by this route".to_string(),
            ));
        }
        Some((entity_type, entity_id)) => (Some(entity_type), Some(entity_id)),
        None => (entity_type, entity_id),
    };

    Ok(StorageAuditEventFilters::new()
        .entity_type(entity_type)
        .entity_id(entity_id.map(|id| {
            hubuum_events_core::EventEntityId::new(id)
                .expect("validated audit entity id must be positive")
        }))
        .action(action)
        .actor_kind(actor_kind)
        .actor_user_id(actor_user_id.map(PrincipalId::new).transpose()?)
        .initiator_user_id(initiator_user_id.map(PrincipalId::new).transpose()?)
        .collection_id(collection_id.map(CollectionId::new).transpose()?)
        .occurred_after(occurred_after.map(|timestamp| timestamp.and_utc()))
        .occurred_before(occurred_before.map(|timestamp| timestamp.and_utc())))
}

fn take_single(
    passthrough: &mut HashMap<String, Vec<String>>,
    key: &str,
) -> Result<Option<String>, ApiError> {
    match passthrough.remove(key) {
        Some(values) if values.len() > 1 => Err(ApiError::BadRequest(format!("duplicate {key}"))),
        Some(mut values) => Ok(values.pop()),
        None => Ok(None),
    }
}

fn parse_optional_i32_filter(
    passthrough: &mut HashMap<String, Vec<String>>,
    key: &str,
) -> Result<Option<i32>, ApiError> {
    take_single(passthrough, key)?
        .map(|value| {
            value
                .parse::<i32>()
                .map_err(|error| ApiError::BadRequest(format!("bad {key}: {error}")))
        })
        .transpose()
}

fn parse_optional_date_filter(
    passthrough: &mut HashMap<String, Vec<String>>,
    key: &str,
) -> Result<Option<chrono::NaiveDateTime>, ApiError> {
    take_single(passthrough, key)?
        .map(|value| {
            let mut values = value.as_date()?;
            if values.len() != 1 {
                return Err(ApiError::BadRequest(format!(
                    "{key} must contain one value"
                )));
            }
            Ok(values.remove(0))
        })
        .transpose()
}

fn parse_optional_catalog_filter<T, F>(
    passthrough: &mut HashMap<String, Vec<String>>,
    key: &str,
    parse: F,
) -> Result<Option<T>, ApiError>
where
    F: Fn(&str) -> Result<T, hubuum_events_core::EventCatalogError>,
{
    take_single(passthrough, key)?
        .map(|value| {
            parse(&value).map_err(|error| ApiError::BadRequest(format!("bad {key}: {error}")))
        })
        .transpose()
}

fn event_sink_from_storage(sink: StorageEventSink) -> Result<EventSink, ApiError> {
    let kind = EventSinkKind::from_str(sink.kind()).map_err(|_| {
        ApiError::InternalServerError(
            "Storage backend returned an unsupported event sink kind".to_string(),
        )
    })?;
    Ok(EventSink {
        collection_id: sink.collection_id().map(CollectionId::id),
        delivery_policy: Some(sink.delivery_policy()),
        id: sink.id().id(),
        name: sink.name().to_string(),
        kind,
        config: sink.configuration().clone(),
        secret_ref: sink.secret_ref().map(str::to_string),
        enabled: sink.enabled(),
        created_at: sink.created_at().naive_utc(),
        updated_at: sink.updated_at().naive_utc(),
        revision: sink.revision(),
    })
}

fn normalize_optional_string(value: Option<String>) -> Option<String> {
    value
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

pub(crate) async fn count_enabled_event_sinks(
    backend: &impl StorageContext,
) -> Result<i64, ApiError> {
    Ok(storage_handle(backend).count_enabled_event_sinks().await?)
}

pub(crate) async fn list_event_sinks(
    backend: &impl StorageContext,
    options: QueryOptions,
) -> Result<(Vec<EventSink>, i64), ApiError> {
    let page = storage_handle(backend)
        .list_event_sinks(StorageEventSinkListQuery::new(options))
        .await?;
    let (sinks, total) = page.into_parts();
    Ok((
        sinks
            .into_iter()
            .map(event_sink_from_storage)
            .collect::<Result<Vec<_>, _>>()?,
        total.unwrap_or(SKIPPED_TOTAL_COUNT),
    ))
}

pub(crate) async fn get_event_sink(
    backend: &impl StorageContext,
    sink_id: i32,
) -> Result<EventSink, ApiError> {
    event_sink_from_storage(
        storage_handle(backend)
            .get_event_sink(
                hubuum_domain::EventSinkId::new(sink_id)
                    .expect("validated event sink id must be positive"),
            )
            .await?,
    )
}

pub(crate) async fn create_event_sink(
    backend: &impl StorageContext,
    sink: NewEventSink,
    event_context: EventContext,
) -> Result<EventSink, ApiError> {
    create_event_sink_in_collection(backend, sink, None, event_context).await
}

pub(crate) async fn create_collection_event_sink(
    backend: &impl StorageContext,
    collection_id: CollectionId,
    sink: NewEventSink,
    event_context: EventContext,
) -> Result<EventSink, ApiError> {
    validate_collection_sink(sink.kind, &sink.config, sink.secret_ref.as_deref())?;
    create_event_sink_in_collection(backend, sink, Some(collection_id), event_context).await
}

fn validate_collection_sink(
    kind: EventSinkKind,
    config: &serde_json::Value,
    secret_ref: Option<&str>,
) -> Result<(), ApiError> {
    if kind != EventSinkKind::Webhook
        || secret_ref.is_some()
        || config.get("url_secret_ref").is_some()
    {
        return Err(ApiError::BadRequest("Collection-owned sinks must be webhooks with their own destination_url; server secret aliases and other transports require an administrator".to_string()));
    }
    if config
        .get("destination_url")
        .and_then(serde_json::Value::as_str)
        .is_none()
    {
        return Err(ApiError::BadRequest(
            "Collection webhooks require config.destination_url".to_string(),
        ));
    }
    Ok(())
}

async fn create_event_sink_in_collection(
    backend: &impl StorageContext,
    sink: NewEventSink,
    collection_id: Option<CollectionId>,
    event_context: EventContext,
) -> Result<EventSink, ApiError> {
    validate_sink_parts(sink.kind, &sink.config, sink.secret_ref.as_deref())?;
    validate_webhook_templates(sink.kind, &sink.config).await?;
    let request = StorageEventSinkCreate::builder(sink.name, sink.kind.as_str(), event_context)
        .collection_id(collection_id)
        .delivery_policy(sink.delivery_policy.unwrap_or_default())
        .configuration(sink.config)
        .secret_ref(normalize_optional_string(sink.secret_ref))
        .enabled(sink.enabled)
        .try_build()?;
    event_sink_from_storage(
        storage_handle(backend)
            .create_event_sink(request)
            .await?
            .into_value(),
    )
}

pub(crate) async fn update_event_sink(
    backend: &impl StorageContext,
    sink_id: i32,
    update: UpdateEventSink,
    existing: &EventSink,
    event_context: EventContext,
) -> Result<EventSink, ApiError> {
    let kind = update.kind.unwrap_or(existing.kind);
    let config = update.config.as_ref().unwrap_or(&existing.config);
    let secret_ref = match update.secret_ref.as_ref() {
        Some(value) => value.as_deref(),
        None => existing.secret_ref.as_deref(),
    };
    if existing.collection_id.is_some() {
        validate_collection_sink(kind, config, secret_ref)?;
    }
    validate_sink_parts(kind, config, secret_ref)?;
    validate_webhook_templates(
        update.kind.unwrap_or(existing.kind),
        update.config.as_ref().unwrap_or(&existing.config),
    )
    .await?;
    let request = StorageEventSinkUpdate::builder(
        hubuum_domain::EventSinkId::new(sink_id).expect("validated event sink id must be positive"),
        event_context,
    )
    .name(update.name)
    .kind(update.kind.map(|value| value.as_str().to_string()))
    .configuration(update.config)
    .delivery_policy(update.delivery_policy)
    .secret_ref(update.secret_ref.map(normalize_optional_string))
    .enabled(update.enabled)
    .try_build()?;
    event_sink_from_storage(
        storage_handle(backend)
            .update_event_sink(request)
            .await?
            .into_value(),
    )
}

pub(crate) async fn delete_event_sink(
    backend: &impl StorageContext,
    sink_id: i32,
    event_context: EventContext,
) -> Result<(), ApiError> {
    storage_handle(backend)
        .delete_event_sink(StorageEventSinkDelete::new(
            hubuum_domain::EventSinkId::new(sink_id)
                .expect("validated event sink id must be positive"),
            event_context,
        ))
        .await?
        .into_value();
    Ok(())
}

fn event_subscription_from_storage(
    subscription: StorageEventSubscription,
) -> Result<EventSubscription, ApiError> {
    Ok(EventSubscription {
        id: subscription.id().id(),
        collection_id: subscription
            .scope()
            .collection_id()
            .ok_or_else(|| {
                ApiError::InternalServerError("Expected a collection subscription".to_string())
            })?
            .id(),
        sink_id: subscription.sink_id().id(),
        name: subscription.name().to_string(),
        description: subscription.description().to_string(),
        entity_types: subscription
            .entity_types()
            .iter()
            .map(|value| value.as_str().to_string())
            .collect(),
        actions: subscription
            .actions()
            .iter()
            .map(|value| value.as_str().to_string())
            .collect(),
        filter: subscription.filter().clone(),
        routing: subscription.routing().clone(),
        enabled: subscription.enabled(),
        created_at: subscription.created_at().naive_utc(),
        updated_at: subscription.updated_at().naive_utc(),
        revision: subscription.revision(),
    })
}

pub(crate) async fn list_event_subscriptions(
    backend: &impl StorageContext,
    collection_id: i32,
    options: QueryOptions,
) -> Result<(Vec<EventSubscription>, i64), ApiError> {
    let page = storage_handle(backend)
        .list_event_subscriptions(StorageEventSubscriptionListQuery::new(
            CollectionId::new(collection_id)?,
            options,
        ))
        .await?;
    let (subscriptions, total) = page.into_parts();
    Ok((
        subscriptions
            .into_iter()
            .map(event_subscription_from_storage)
            .collect::<Result<Vec<_>, _>>()?,
        total.unwrap_or(SKIPPED_TOTAL_COUNT),
    ))
}

pub(crate) async fn get_event_subscription(
    backend: &impl StorageContext,
    collection_id: i32,
    subscription_id: i32,
) -> Result<EventSubscription, ApiError> {
    event_subscription_from_storage(
        storage_handle(backend)
            .get_event_subscription(
                CollectionId::new(collection_id)?.into(),
                hubuum_domain::EventSubscriptionId::new(subscription_id)
                    .expect("validated event subscription id must be positive"),
            )
            .await?,
    )
}

pub(crate) async fn create_event_subscription(
    backend: &impl StorageContext,
    collection_id: i32,
    subscription: NewEventSubscription,
    event_context: EventContext,
) -> Result<EventSubscription, ApiError> {
    let authorized = storage_handle(backend)
        .resolve_event_sink_use(CollectionId::new(collection_id)?, subscription.sink_id)
        .await?;
    validate_webhook_routing(authorized.sink(), &subscription.routing)?;
    validate_subscription_parts(
        &subscription.entity_types,
        &subscription.actions,
        &subscription.filter,
        &subscription.routing,
    )?;
    let entity_types = storage_entity_types(&subscription.entity_types)?;
    let actions = storage_actions(&subscription.actions)?;
    let request = StorageEventSubscriptionCreate::builder(
        authorized.collection_id(),
        authorized.sink().id(),
        subscription.name,
        event_context,
    )
    .description(subscription.description)
    .entity_types(entity_types)
    .actions(actions)
    .filter(subscription.filter)
    .routing(subscription.routing)
    .enabled(subscription.enabled)
    .try_build()?;
    event_subscription_from_storage(
        storage_handle(backend)
            .create_event_subscription(request)
            .await?
            .into_value(),
    )
}

pub(crate) async fn update_event_subscription(
    backend: &impl StorageContext,
    collection_id: i32,
    subscription_id: i32,
    update: UpdateEventSubscription,
    existing: &EventSubscription,
    event_context: EventContext,
) -> Result<EventSubscription, ApiError> {
    let entity_types = update
        .entity_types
        .as_deref()
        .unwrap_or(&existing.entity_types);
    let actions = update.actions.as_deref().unwrap_or(&existing.actions);
    let filter = update.filter.as_ref().unwrap_or(&existing.filter);
    let routing = update.routing.as_ref().unwrap_or(&existing.routing);
    validate_subscription_parts(entity_types, actions, filter, routing)?;
    let sink_id = update
        .sink_id
        .unwrap_or(hubuum_domain::EventSinkId::new(existing.sink_id)?);
    let authorized = storage_handle(backend)
        .resolve_event_sink_use(CollectionId::new(collection_id)?, sink_id)
        .await?;
    validate_webhook_routing(authorized.sink(), routing)?;
    let storage_entity_types = update
        .entity_types
        .as_deref()
        .map(storage_entity_types)
        .transpose()?;
    let storage_actions = update.actions.as_deref().map(storage_actions).transpose()?;
    let request = StorageEventSubscriptionUpdate::builder(
        CollectionId::new(collection_id)?,
        hubuum_domain::EventSubscriptionId::new(subscription_id)
            .expect("validated event subscription id must be positive"),
        event_context,
    )
    .sink_id(update.sink_id)
    .name(update.name)
    .description(update.description)
    .entity_types(storage_entity_types)
    .actions(storage_actions)
    .filter(update.filter)
    .routing(update.routing)
    .enabled(update.enabled)
    .try_build()?;
    event_subscription_from_storage(
        storage_handle(backend)
            .update_event_subscription(request)
            .await?
            .into_value(),
    )
}

pub(crate) async fn delete_event_subscription(
    backend: &impl StorageContext,
    collection_id: i32,
    subscription_id: i32,
    event_context: EventContext,
) -> Result<(), ApiError> {
    storage_handle(backend)
        .delete_event_subscription(StorageEventSubscriptionDelete::new(
            CollectionId::new(collection_id)?,
            hubuum_domain::EventSubscriptionId::new(subscription_id)
                .expect("validated event subscription id must be positive"),
            event_context,
        ))
        .await?
        .into_value();
    Ok(())
}

fn system_event_subscription_from_storage(
    subscription: StorageEventSubscription,
) -> Result<SystemEventSubscription, ApiError> {
    Ok(SystemEventSubscription {
        id: subscription.id().id(),
        sink_id: subscription.sink_id().id(),
        name: subscription.name().to_string(),
        description: subscription.description().to_string(),
        entity_types: subscription
            .entity_types()
            .iter()
            .map(|value| value.as_str().to_string())
            .collect(),
        actions: subscription
            .actions()
            .iter()
            .map(|value| value.as_str().to_string())
            .collect(),
        filter: subscription.filter().clone(),
        routing: subscription.routing().clone(),
        enabled: subscription.enabled(),
        created_at: subscription.created_at().naive_utc(),
        updated_at: subscription.updated_at().naive_utc(),
        revision: subscription.revision(),
    })
}

pub(crate) async fn list_system_event_subscriptions(
    backend: &impl StorageContext,
    options: QueryOptions,
) -> Result<(Vec<SystemEventSubscription>, i64), ApiError> {
    let page = storage_handle(backend)
        .list_event_subscriptions(StorageEventSubscriptionListQuery::new(
            EventSubscriptionScope::System,
            options,
        ))
        .await?;
    let (subscriptions, total) = page.into_parts();
    Ok((
        subscriptions
            .into_iter()
            .map(system_event_subscription_from_storage)
            .collect::<Result<Vec<_>, _>>()?,
        total.unwrap_or(SKIPPED_TOTAL_COUNT),
    ))
}

pub(crate) async fn get_system_event_subscription(
    backend: &impl StorageContext,
    subscription_id: i32,
) -> Result<SystemEventSubscription, ApiError> {
    system_event_subscription_from_storage(
        storage_handle(backend)
            .get_event_subscription(
                EventSubscriptionScope::System,
                hubuum_domain::EventSubscriptionId::new(subscription_id)
                    .expect("validated event subscription id must be positive"),
            )
            .await?,
    )
}

pub(crate) async fn create_system_event_subscription(
    backend: &impl StorageContext,
    subscription: NewEventSubscription,
    event_context: EventContext,
) -> Result<SystemEventSubscription, ApiError> {
    let sink = storage_handle(backend)
        .get_event_sink(subscription.sink_id)
        .await?;
    validate_webhook_routing(&sink, &subscription.routing)?;
    validate_subscription_parts(
        &subscription.entity_types,
        &subscription.actions,
        &subscription.filter,
        &subscription.routing,
    )?;
    let entity_types = storage_entity_types(&subscription.entity_types)?;
    let actions = storage_actions(&subscription.actions)?;
    let request = StorageEventSubscriptionCreate::builder(
        EventSubscriptionScope::System,
        subscription.sink_id,
        subscription.name,
        event_context,
    )
    .description(subscription.description)
    .entity_types(entity_types)
    .actions(actions)
    .filter(subscription.filter)
    .routing(subscription.routing)
    .enabled(subscription.enabled)
    .try_build()?;
    system_event_subscription_from_storage(
        storage_handle(backend)
            .create_event_subscription(request)
            .await?
            .into_value(),
    )
}

pub(crate) async fn update_system_event_subscription(
    backend: &impl StorageContext,
    subscription_id: i32,
    update: UpdateEventSubscription,
    existing: &SystemEventSubscription,
    event_context: EventContext,
) -> Result<SystemEventSubscription, ApiError> {
    if let Some(sink_id) = update.sink_id {
        storage_handle(backend).get_event_sink(sink_id).await?;
    }
    let entity_types = update
        .entity_types
        .as_deref()
        .unwrap_or(&existing.entity_types);
    let actions = update.actions.as_deref().unwrap_or(&existing.actions);
    let filter = update.filter.as_ref().unwrap_or(&existing.filter);
    let routing = update.routing.as_ref().unwrap_or(&existing.routing);
    validate_subscription_parts(entity_types, actions, filter, routing)?;
    let sink_id = update
        .sink_id
        .unwrap_or(hubuum_domain::EventSinkId::new(existing.sink_id)?);
    validate_webhook_routing(
        &storage_handle(backend).get_event_sink(sink_id).await?,
        routing,
    )?;
    let storage_entity_types = update
        .entity_types
        .as_deref()
        .map(storage_entity_types)
        .transpose()?;
    let storage_actions = update.actions.as_deref().map(storage_actions).transpose()?;
    let request = StorageEventSubscriptionUpdate::builder(
        EventSubscriptionScope::System,
        hubuum_domain::EventSubscriptionId::new(subscription_id)
            .expect("validated event subscription id must be positive"),
        event_context,
    )
    .sink_id(update.sink_id)
    .name(update.name)
    .description(update.description)
    .entity_types(storage_entity_types)
    .actions(storage_actions)
    .filter(update.filter)
    .routing(update.routing)
    .enabled(update.enabled)
    .try_build()?;
    system_event_subscription_from_storage(
        storage_handle(backend)
            .update_event_subscription(request)
            .await?
            .into_value(),
    )
}

pub(crate) async fn delete_system_event_subscription(
    backend: &impl StorageContext,
    subscription_id: i32,
    event_context: EventContext,
) -> Result<(), ApiError> {
    storage_handle(backend)
        .delete_event_subscription(StorageEventSubscriptionDelete::new(
            EventSubscriptionScope::System,
            hubuum_domain::EventSubscriptionId::new(subscription_id)
                .expect("validated event subscription id must be positive"),
            event_context,
        ))
        .await?
        .into_value();
    Ok(())
}

fn event_delivery_from_storage(delivery: StorageEventDelivery) -> EventDeliveryResponse {
    EventDeliveryResponse {
        id: delivery.id().id(),
        event_id: delivery.event_id().get(),
        subscription_id: delivery.subscription_id().id(),
        status: delivery.status().as_str().to_string(),
        purpose: delivery.purpose(),
        deferred_reason: delivery.deferred_reason().map(str::to_owned),
        attempts: delivery.attempts(),
        next_attempt_at: delivery.next_attempt_at().naive_utc(),
        last_error: delivery.last_error().map(str::to_string),
        locked_until: delivery
            .locked_until()
            .map(|timestamp| timestamp.naive_utc()),
        created_at: delivery.created_at().naive_utc(),
        updated_at: delivery.updated_at().naive_utc(),
    }
}

pub(crate) async fn list_event_deliveries(
    backend: &impl StorageContext,
    options: QueryOptions,
) -> Result<(Vec<EventDeliveryResponse>, i64), ApiError> {
    let page = storage_handle(backend)
        .list_event_deliveries(StorageEventDeliveryListQuery::new(options))
        .await?;
    let (deliveries, total) = page.into_parts();
    Ok((
        deliveries
            .into_iter()
            .map(event_delivery_from_storage)
            .collect(),
        total.unwrap_or(SKIPPED_TOTAL_COUNT),
    ))
}

pub(crate) async fn get_event_delivery(
    backend: &impl StorageContext,
    delivery_id: i64,
) -> Result<EventDeliveryResponse, ApiError> {
    Ok(event_delivery_from_storage(
        storage_handle(backend)
            .get_event_delivery(
                hubuum_domain::EventDeliveryId::new(delivery_id)
                    .expect("validated event delivery id must be positive"),
            )
            .await?,
    ))
}

pub(crate) async fn release_event_delivery_for_retry(
    backend: &impl StorageContext,
    delivery_id: i64,
) -> Result<EventDeliveryResponse, ApiError> {
    Ok(event_delivery_from_storage(
        storage_handle(backend)
            .release_event_delivery_for_retry(
                hubuum_domain::EventDeliveryId::new(delivery_id)
                    .expect("validated event delivery id must be positive"),
            )
            .await?,
    ))
}

pub(crate) async fn mark_event_delivery_dead(
    backend: &impl StorageContext,
    delivery_id: i64,
) -> Result<EventDeliveryResponse, ApiError> {
    Ok(event_delivery_from_storage(
        storage_handle(backend)
            .mark_event_delivery_dead(
                hubuum_domain::EventDeliveryId::new(delivery_id)
                    .expect("validated event delivery id must be positive"),
            )
            .await?,
    ))
}

fn validate_webhook_routing(
    sink: &StorageEventSink,
    routing: &serde_json::Value,
) -> Result<(), ApiError> {
    let result = match sink.kind() {
        "webhook" => hubuum_event_sink_webhook::Configuration::parse(sink.configuration())
            .and_then(|c| c.validate_routing(routing)),
        _ => Ok(()),
    };
    result.map_err(|e| ApiError::BadRequest(e.to_string()))
}

pub(crate) async fn preview_event_notification(
    backend: &impl StorageContext,
    sink_id: hubuum_domain::EventSinkId,
    request: &crate::models::EventNotificationRequest,
) -> Result<crate::models::EventNotificationPreview, ApiError> {
    let input = storage_handle(backend)
        .load_event_notification(hubuum_storage_core::StorageEventNotificationSelection::new(
            sink_id,
            request.subscription_id,
            request.event_id,
        ))
        .await?;
    let kind = EventSinkKind::from_str(input.sink().kind())?;
    if kind != EventSinkKind::Webhook {
        return Err(ApiError::BadRequest(
            "Preview and test are supported for webhook sinks".to_string(),
        ));
    }
    let prepared = crate::events::webhook_sink()
        .prepare(
            input.event(),
            hubuum_event_sinks_common::SinkDelivery::new(
                input.sink().configuration(),
                input.subscription().routing(),
                None,
            )
            .for_test(true),
        )
        .await
        .map_err(|error| ApiError::BadRequest(error.to_string()))?;
    Ok(crate::models::EventNotificationPreview {
        sink_kind: kind,
        payload: prepared.payload().clone(),
    })
}

pub(crate) async fn test_event_notification(
    backend: &impl StorageContext,
    sink_id: hubuum_domain::EventSinkId,
    request: &crate::models::EventNotificationRequest,
    context: EventContext,
) -> Result<EventDeliveryResponse, ApiError> {
    preview_event_notification(backend, sink_id, request).await?;
    let result = storage_handle(backend)
        .enqueue_event_notification_test(
            hubuum_storage_core::StorageEventNotificationSelection::new(
                sink_id,
                request.subscription_id,
                request.event_id,
            ),
            context,
        )
        .await?;
    Ok(event_delivery_from_storage(result.into_value()))
}

pub(crate) async fn list_collection_event_sinks(
    backend: &impl StorageContext,
    collection_id: CollectionId,
    options: QueryOptions,
) -> Result<(Vec<EventSink>, i64), ApiError> {
    let (sinks, total) = storage_handle(backend)
        .list_event_sinks(StorageEventSinkListQuery::new(options).for_collection(collection_id))
        .await?
        .into_parts();
    Ok((
        sinks
            .into_iter()
            .map(event_sink_from_storage)
            .collect::<Result<_, _>>()?,
        total.unwrap_or(SKIPPED_TOTAL_COUNT),
    ))
}

pub(crate) async fn get_collection_owned_sink(
    backend: &impl StorageContext,
    collection_id: CollectionId,
    sink_id: hubuum_domain::EventSinkId,
) -> Result<EventSink, ApiError> {
    let sink = get_event_sink(backend, sink_id.id()).await?;
    if sink.collection_id != Some(collection_id.id()) {
        return Err(ApiError::NotFound(
            "Collection-owned event sink not found".to_string(),
        ));
    }
    Ok(sink)
}

pub(crate) async fn list_event_sink_collections(
    backend: &impl StorageContext,
    sink_id: hubuum_domain::EventSinkId,
) -> Result<Vec<i32>, ApiError> {
    Ok(storage_handle(backend)
        .list_event_sink_collections(sink_id)
        .await?
        .into_iter()
        .map(CollectionId::id)
        .collect())
}
pub(crate) async fn change_event_sink_grant(
    backend: &impl StorageContext,
    sink_id: hubuum_domain::EventSinkId,
    collection_id: CollectionId,
    action: EventSinkGrantAction,
    event_context: EventContext,
) -> Result<(), ApiError> {
    storage_handle(backend)
        .change_event_sink_grant(StorageEventSinkGrantChange::new(
            sink_id,
            collection_id,
            action,
            event_context,
        ))
        .await?
        .into_value();
    Ok(())
}
