use std::fmt;

use chrono::NaiveDateTime;
use diesel::prelude::{ExpressionMethods, QueryDsl};
use diesel::{AsChangeset, Insertable, Queryable, Selectable};
use diesel_async::RunQueryDsl;
use hubuum_domain::{CollectionId, EventSinkId, EventSubscriptionId};
use hubuum_events_core::{Action, EntityType, EventContext, NewEvent, redact_event_sink_config};
use hubuum_query::{FilterField, QueryOptions};
use hubuum_storage_core::{
    AuditReceipt, MutationOutcome, StorageEventSink, StorageEventSinkCreate,
    StorageEventSinkDelete, StorageEventSinkListQuery, StorageEventSinkUpdate,
    StorageEventSubscription, StorageEventSubscriptionCreate, StorageEventSubscriptionDelete,
    StorageEventSubscriptionListQuery, StorageEventSubscriptionUpdate, StoragePage,
};
use serde_json::{Value, json};

use crate::cursor::{CursorSqlField, CursorSqlType};
use crate::revision::RevisionOwner;
use crate::runtime::assert_locked_revision_precondition;
use crate::{PostgresConnection, PostgresRevision, PostgresRuntime, PostgresStorageError};

use super::event_record::append_event;

macro_rules! impl_redacted_sink_debug {
    ($target:ty, $($field:ident),+ $(,)?) => {
        impl fmt::Debug for $target {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                let mut debug = formatter.debug_struct(stringify!($target));
                $(debug.field(stringify!($field), &self.$field);)+
                debug.field("configuration", &"<redacted>").finish()
            }
        }
    };
}

macro_rules! impl_redacted_subscription_debug {
    ($target:ty, $($field:ident),+ $(,)?) => {
        impl fmt::Debug for $target {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                let mut debug = formatter.debug_struct(stringify!($target));
                $(debug.field(stringify!($field), &self.$field);)+
                debug.field("routing", &"<redacted>").finish()
            }
        }
    };
}

#[derive(Clone, Queryable, Selectable)]
#[diesel(table_name = crate::schema::event_sinks)]
struct EventSinkRow {
    id: i32,
    name: String,
    kind: String,
    config: Value,
    secret_ref: Option<String>,
    enabled: bool,
    created_at: NaiveDateTime,
    updated_at: NaiveDateTime,
    revision: PostgresRevision,
}

impl_redacted_sink_debug!(
    EventSinkRow,
    id,
    name,
    kind,
    enabled,
    created_at,
    updated_at,
    revision,
);

impl TryFrom<EventSinkRow> for StorageEventSink {
    type Error = PostgresStorageError;

    fn try_from(row: EventSinkRow) -> Result<Self, Self::Error> {
        Ok(Self::builder(
            EventSinkId::new(row.id)?,
            row.name,
            row.kind,
            row.created_at,
            row.updated_at,
            row.revision.into_domain(),
        )
        .configuration(row.config)
        .secret_ref(row.secret_ref)
        .enabled(row.enabled)
        .build())
    }
}

#[derive(Insertable)]
#[diesel(table_name = crate::schema::event_sinks)]
struct NewEventSinkRow {
    name: String,
    kind: String,
    config: Value,
    secret_ref: Option<String>,
    enabled: bool,
}

impl_redacted_sink_debug!(NewEventSinkRow, name, kind, enabled);

#[derive(AsChangeset)]
#[diesel(table_name = crate::schema::event_sinks)]
struct UpdateEventSinkRow {
    name: Option<String>,
    kind: Option<String>,
    config: Option<Value>,
    secret_ref: Option<Option<String>>,
    enabled: Option<bool>,
}

impl_redacted_sink_debug!(UpdateEventSinkRow, name, kind, enabled);

impl UpdateEventSinkRow {
    fn has_changes(&self, current: &EventSinkRow) -> bool {
        self.name
            .as_ref()
            .is_some_and(|value| value != &current.name)
            || self
                .kind
                .as_ref()
                .is_some_and(|value| value != &current.kind)
            || self
                .config
                .as_ref()
                .is_some_and(|value| value != &current.config)
            || self
                .secret_ref
                .as_ref()
                .is_some_and(|value| value != &current.secret_ref)
            || self.enabled.is_some_and(|value| value != current.enabled)
    }
}

#[derive(Clone, Queryable, Selectable)]
#[diesel(table_name = crate::schema::event_subscriptions)]
struct EventSubscriptionRow {
    id: i32,
    collection_id: i32,
    sink_id: i32,
    name: String,
    description: String,
    entity_types: Value,
    actions: Value,
    filter: Value,
    routing: Value,
    enabled: bool,
    created_at: NaiveDateTime,
    updated_at: NaiveDateTime,
    revision: PostgresRevision,
}

impl_redacted_subscription_debug!(
    EventSubscriptionRow,
    id,
    collection_id,
    sink_id,
    name,
    description,
    entity_types,
    actions,
    filter,
    enabled,
    created_at,
    updated_at,
    revision,
);

impl TryFrom<EventSubscriptionRow> for StorageEventSubscription {
    type Error = PostgresStorageError;

    fn try_from(row: EventSubscriptionRow) -> Result<Self, Self::Error> {
        Ok(Self::builder(
            EventSubscriptionId::new(row.id)?,
            CollectionId::new(row.collection_id)?,
            EventSinkId::new(row.sink_id)?,
            row.name,
            row.created_at,
            row.updated_at,
            row.revision.into_domain(),
        )
        .description(row.description)
        .entity_types(decode_json(
            row.entity_types,
            "event subscription entity types",
        )?)
        .actions(decode_json(row.actions, "event subscription actions")?)
        .filter(decode_json(row.filter, "event subscription filter")?)
        .routing(row.routing)
        .enabled(row.enabled)
        .build())
    }
}

#[derive(Insertable)]
#[diesel(table_name = crate::schema::event_subscriptions)]
struct NewEventSubscriptionRow {
    collection_id: i32,
    sink_id: i32,
    name: String,
    description: String,
    entity_types: Value,
    actions: Value,
    filter: Value,
    routing: Value,
    enabled: bool,
}

impl_redacted_subscription_debug!(
    NewEventSubscriptionRow,
    collection_id,
    sink_id,
    name,
    description,
    entity_types,
    actions,
    filter,
    enabled,
);

#[derive(AsChangeset)]
#[diesel(table_name = crate::schema::event_subscriptions)]
struct UpdateEventSubscriptionRow {
    sink_id: Option<i32>,
    name: Option<String>,
    description: Option<String>,
    entity_types: Option<Value>,
    actions: Option<Value>,
    filter: Option<Value>,
    routing: Option<Value>,
    enabled: Option<bool>,
}

impl_redacted_subscription_debug!(
    UpdateEventSubscriptionRow,
    sink_id,
    name,
    description,
    entity_types,
    actions,
    filter,
    enabled,
);

impl UpdateEventSubscriptionRow {
    fn has_changes(&self, current: &EventSubscriptionRow) -> bool {
        self.sink_id.is_some_and(|value| value != current.sink_id)
            || self
                .name
                .as_ref()
                .is_some_and(|value| value != &current.name)
            || self
                .description
                .as_ref()
                .is_some_and(|value| value != &current.description)
            || self
                .entity_types
                .as_ref()
                .is_some_and(|value| value != &current.entity_types)
            || self
                .actions
                .as_ref()
                .is_some_and(|value| value != &current.actions)
            || self
                .filter
                .as_ref()
                .is_some_and(|value| value != &current.filter)
            || self
                .routing
                .as_ref()
                .is_some_and(|value| value != &current.routing)
            || self.enabled.is_some_and(|value| value != current.enabled)
    }
}

pub async fn count_enabled_event_sinks(
    runtime: &PostgresRuntime,
) -> Result<i64, PostgresStorageError> {
    use crate::schema::event_sinks::dsl::{enabled, event_sinks};
    use diesel::dsl::count_star;

    runtime
        .with_connection(async |connection| {
            event_sinks
                .filter(enabled.eq(true))
                .select(count_star())
                .first::<i64>(connection)
                .await
        })
        .await
}

pub async fn list_event_sinks(
    runtime: &PostgresRuntime,
    query: StorageEventSinkListQuery,
) -> Result<StoragePage<StorageEventSink>, PostgresStorageError> {
    let include_total = query.options().include_total();
    runtime
        .with_read_only_snapshot(async |connection| {
            let total = if include_total {
                Some(
                    build_event_sink_query(query.options())?
                        .count()
                        .get_result::<i64>(connection)
                        .await?,
                )
            } else {
                None
            };
            let mut records = build_event_sink_query(query.options())?;
            let fields = query
                .options()
                .sort()
                .iter()
                .map(|sort| event_sink_cursor_field(&sort.field))
                .collect::<Result<Vec<_>, _>>()?;
            crate::apply_query_options_with_fields!(records, query.options(), fields);
            let rows = records
                .load::<EventSinkRow>(connection)
                .await?
                .into_iter()
                .map(StorageEventSink::try_from)
                .collect::<Result<Vec<_>, _>>()?;
            StoragePage::try_new(rows, total).map_err(PostgresStorageError::from)
        })
        .await
}

pub async fn get_event_sink(
    runtime: &PostgresRuntime,
    sink_id: i32,
) -> Result<StorageEventSink, PostgresStorageError> {
    runtime
        .with_connection(async |connection| load_event_sink_row(connection, sink_id).await)
        .await
        .and_then(StorageEventSink::try_from)
}

pub async fn create_event_sink(
    runtime: &PostgresRuntime,
    request: StorageEventSinkCreate,
) -> Result<MutationOutcome<StorageEventSink>, PostgresStorageError> {
    let row = NewEventSinkRow {
        name: request.name().to_string(),
        kind: request.kind().to_string(),
        config: request.configuration().clone(),
        secret_ref: request.secret_ref().map(str::to_string),
        enabled: request.enabled(),
    };
    runtime
        .with_transaction(
            async |connection| -> Result<MutationOutcome<StorageEventSink>, PostgresStorageError> {
                use crate::schema::event_sinks::dsl::event_sinks;

                let created = diesel::insert_into(event_sinks)
                    .values(row)
                    .get_result::<EventSinkRow>(connection)
                    .await?;
                let audit = append_sink_audit(
                    connection,
                    Action::Created,
                    request.event_context(),
                    None,
                    &created,
                )
                .await?;
                Ok(MutationOutcome::committed(
                    StorageEventSink::try_from(created)?,
                    audit,
                ))
            },
        )
        .await
}

pub async fn update_event_sink(
    runtime: &PostgresRuntime,
    request: StorageEventSinkUpdate,
) -> Result<MutationOutcome<StorageEventSink>, PostgresStorageError> {
    let sink_id = request.id().id();
    let changes = UpdateEventSinkRow {
        name: request.name_value().map(str::to_string),
        kind: request.kind_value().map(str::to_string),
        config: request.configuration_value().cloned(),
        secret_ref: request
            .secret_ref_value()
            .map(|value| value.map(str::to_string)),
        enabled: request.enabled_value(),
    };
    runtime
        .with_transaction(
            async |connection| -> Result<MutationOutcome<StorageEventSink>, PostgresStorageError> {
                use crate::schema::event_sinks::dsl::{event_sinks, id};

                let before = event_sinks
                    .filter(id.eq(sink_id))
                    .for_update()
                    .first::<EventSinkRow>(connection)
                    .await?;
                assert_locked_revision_precondition(
                    connection,
                    &RevisionOwner::EventSink.key(before.id),
                    before.revision,
                )
                .await?;
                if !changes.has_changes(&before) {
                    return Ok(MutationOutcome::unchanged(StorageEventSink::try_from(
                        before,
                    )?));
                }
                let updated = diesel::update(event_sinks.filter(id.eq(sink_id)))
                    .set(changes)
                    .get_result::<EventSinkRow>(connection)
                    .await?;
                let audit = append_sink_audit(
                    connection,
                    Action::Updated,
                    request.event_context(),
                    Some(&before),
                    &updated,
                )
                .await?;
                Ok(MutationOutcome::committed(
                    StorageEventSink::try_from(updated)?,
                    audit,
                ))
            },
        )
        .await
}

pub async fn delete_event_sink(
    runtime: &PostgresRuntime,
    request: StorageEventSinkDelete,
) -> Result<MutationOutcome<()>, PostgresStorageError> {
    let sink_id = request.id().id();
    runtime
        .with_transaction(
            async |connection| -> Result<MutationOutcome<()>, PostgresStorageError> {
                use crate::schema::event_sinks::dsl::{event_sinks, id};

                let before = event_sinks
                    .filter(id.eq(sink_id))
                    .for_update()
                    .first::<EventSinkRow>(connection)
                    .await?;
                assert_locked_revision_precondition(
                    connection,
                    &RevisionOwner::EventSink.key(before.id),
                    before.revision,
                )
                .await?;
                let audit = append_sink_audit(
                    connection,
                    Action::Deleted,
                    request.event_context(),
                    Some(&before),
                    &before,
                )
                .await?;
                diesel::delete(event_sinks.filter(id.eq(sink_id)))
                    .execute(connection)
                    .await?;
                Ok(MutationOutcome::committed((), audit))
            },
        )
        .await
}

pub async fn list_event_subscriptions(
    runtime: &PostgresRuntime,
    query: StorageEventSubscriptionListQuery,
) -> Result<StoragePage<StorageEventSubscription>, PostgresStorageError> {
    let include_total = query.options().include_total();
    runtime
        .with_read_only_snapshot(async |connection| {
            let total = if include_total {
                Some(
                    build_event_subscription_query(query.collection_id().id(), query.options())?
                        .count()
                        .get_result::<i64>(connection)
                        .await?,
                )
            } else {
                None
            };
            let mut records =
                build_event_subscription_query(query.collection_id().id(), query.options())?;
            let fields = query
                .options()
                .sort()
                .iter()
                .map(|sort| event_subscription_cursor_field(&sort.field))
                .collect::<Result<Vec<_>, _>>()?;
            crate::apply_query_options_with_fields!(records, query.options(), fields);
            let rows = records
                .load::<EventSubscriptionRow>(connection)
                .await?
                .into_iter()
                .map(StorageEventSubscription::try_from)
                .collect::<Result<Vec<_>, _>>()?;
            StoragePage::try_new(rows, total).map_err(PostgresStorageError::from)
        })
        .await
}

pub async fn get_event_subscription(
    runtime: &PostgresRuntime,
    collection_id: i32,
    subscription_id: i32,
) -> Result<StorageEventSubscription, PostgresStorageError> {
    runtime
        .with_connection(async |connection| {
            load_scoped_subscription_row(connection, collection_id, subscription_id)
                .await?
                .try_into()
        })
        .await
}

pub async fn create_event_subscription(
    runtime: &PostgresRuntime,
    request: StorageEventSubscriptionCreate,
) -> Result<MutationOutcome<StorageEventSubscription>, PostgresStorageError> {
    let row = NewEventSubscriptionRow {
        collection_id: request.collection_id().id(),
        sink_id: request.sink_id().id(),
        name: request.name().to_string(),
        description: request.description().to_string(),
        entity_types: encode_json(request.entity_types(), "event subscription entity types")?,
        actions: encode_json(request.actions(), "event subscription actions")?,
        filter: encode_json(request.filter(), "event subscription filter")?,
        routing: request.routing().clone(),
        enabled: request.enabled(),
    };
    runtime
        .with_transaction(
            async |connection| -> Result<MutationOutcome<StorageEventSubscription>, PostgresStorageError> {
            use crate::schema::event_subscriptions::dsl::event_subscriptions;

            let created = diesel::insert_into(event_subscriptions)
                .values(row)
                .get_result::<EventSubscriptionRow>(connection)
                .await?;
            let audit = append_subscription_audit(
                connection,
                Action::Created,
                request.event_context(),
                None,
                &created,
            )
            .await?;
            Ok(MutationOutcome::committed(created.try_into()?, audit))
            },
        )
        .await
}

pub async fn update_event_subscription(
    runtime: &PostgresRuntime,
    request: StorageEventSubscriptionUpdate,
) -> Result<MutationOutcome<StorageEventSubscription>, PostgresStorageError> {
    let subscription_id = request.id().id();
    let collection_id_value = request.collection_id().id();
    let changes = UpdateEventSubscriptionRow {
        sink_id: request.sink_id_value().map(hubuum_domain::EventSinkId::id),
        name: request.name_value().map(str::to_string),
        description: request.description_value().map(str::to_string),
        entity_types: request
            .entity_types_value()
            .map(|value| encode_json(value, "event subscription entity types"))
            .transpose()?,
        actions: request
            .actions_value()
            .map(|value| encode_json(value, "event subscription actions"))
            .transpose()?,
        filter: request
            .filter_value()
            .map(|value| encode_json(value, "event subscription filter"))
            .transpose()?,
        routing: request.routing_value().cloned(),
        enabled: request.enabled_value(),
    };
    runtime
        .with_transaction(
            async |connection| -> Result<MutationOutcome<StorageEventSubscription>, PostgresStorageError> {
                use crate::schema::event_subscriptions::dsl::{collection_id, event_subscriptions, id};

            let before = event_subscriptions
                .filter(id.eq(subscription_id))
                .filter(collection_id.eq(collection_id_value))
                .for_update()
                .first::<EventSubscriptionRow>(connection)
                .await?;
            assert_locked_revision_precondition(
                connection,
                &RevisionOwner::EventSubscription.key(before.id),
                before.revision,
            )
            .await?;
            if !changes.has_changes(&before) {
                return Ok(MutationOutcome::unchanged(before.try_into()?));
            }
            let updated = diesel::update(event_subscriptions.filter(id.eq(subscription_id)))
                .set(changes)
                .get_result::<EventSubscriptionRow>(connection)
                .await?;
            let audit = append_subscription_audit(
                connection,
                Action::Updated,
                request.event_context(),
                Some(&before),
                &updated,
            )
            .await?;
                Ok(MutationOutcome::committed(updated.try_into()?, audit))
            },
        )
        .await
}

pub async fn delete_event_subscription(
    runtime: &PostgresRuntime,
    request: StorageEventSubscriptionDelete,
) -> Result<MutationOutcome<()>, PostgresStorageError> {
    let subscription_id = request.id().id();
    let collection_id_value = request.collection_id().id();
    runtime
        .with_transaction(
            async |connection| -> Result<MutationOutcome<()>, PostgresStorageError> {
                use crate::schema::event_subscriptions::dsl::{
                    collection_id, event_subscriptions, id,
                };

                let before = event_subscriptions
                    .filter(id.eq(subscription_id))
                    .filter(collection_id.eq(collection_id_value))
                    .for_update()
                    .first::<EventSubscriptionRow>(connection)
                    .await?;
                assert_locked_revision_precondition(
                    connection,
                    &RevisionOwner::EventSubscription.key(before.id),
                    before.revision,
                )
                .await?;
                let audit = append_subscription_audit(
                    connection,
                    Action::Deleted,
                    request.event_context(),
                    Some(&before),
                    &before,
                )
                .await?;
                diesel::delete(event_subscriptions.filter(id.eq(subscription_id)))
                    .execute(connection)
                    .await?;
                Ok::<_, PostgresStorageError>(MutationOutcome::committed((), audit))
            },
        )
        .await
}

async fn load_event_sink_row(
    connection: &mut PostgresConnection,
    sink_id: i32,
) -> Result<EventSinkRow, PostgresStorageError> {
    use crate::schema::event_sinks::dsl::{event_sinks, id};

    event_sinks
        .filter(id.eq(sink_id))
        .first::<EventSinkRow>(connection)
        .await
        .map_err(PostgresStorageError::from)
}

async fn load_scoped_subscription_row(
    connection: &mut PostgresConnection,
    collection: i32,
    subscription_id: i32,
) -> Result<EventSubscriptionRow, PostgresStorageError> {
    use crate::schema::event_subscriptions::dsl::{collection_id, event_subscriptions, id};

    event_subscriptions
        .filter(id.eq(subscription_id))
        .filter(collection_id.eq(collection))
        .first::<EventSubscriptionRow>(connection)
        .await
        .map_err(PostgresStorageError::from)
}

async fn append_sink_audit(
    connection: &mut PostgresConnection,
    action: Action,
    context: &EventContext,
    before: Option<&EventSinkRow>,
    after: &EventSinkRow,
) -> Result<AuditReceipt, PostgresStorageError> {
    let event = NewEvent::new(
        EntityType::EventSink,
        action,
        context.actor_kind(),
        format!("Event sink '{}' {}", after.name, action.as_str()),
    )
    .map_err(|error| PostgresStorageError::database(error.to_string()))?
    .with_context(context)
    .with_entity_id(hubuum_events_core::EventEntityId::new(after.id)?)
    .with_entity_name(&after.name)
    .with_before_opt(before.map(event_sink_snapshot))
    .with_after_opt((action != Action::Deleted).then(|| event_sink_snapshot(after)))
    .with_metadata(json!({
        "sink_id": after.id,
        "kind": after.kind,
        "enabled": after.enabled,
    }));
    append_event(connection, &event)
        .await?
        .into_audit_receipt()
        .map_err(Into::into)
}

async fn append_subscription_audit(
    connection: &mut PostgresConnection,
    action: Action,
    context: &EventContext,
    before: Option<&EventSubscriptionRow>,
    after: &EventSubscriptionRow,
) -> Result<AuditReceipt, PostgresStorageError> {
    let event = NewEvent::new(
        EntityType::EventSubscription,
        action,
        context.actor_kind(),
        format!("Event subscription '{}' {}", after.name, action.as_str()),
    )
    .map_err(|error| PostgresStorageError::database(error.to_string()))?
    .with_context(context)
    .with_entity_id(hubuum_events_core::EventEntityId::new(after.id)?)
    .with_entity_name(&after.name)
    .with_collection_id(hubuum_domain::CollectionId::new(after.collection_id)?)
    .with_before_opt(before.map(event_subscription_snapshot))
    .with_after_opt((action != Action::Deleted).then(|| event_subscription_snapshot(after)))
    .with_metadata(json!({
        "subscription_id": after.id,
        "sink_id": after.sink_id,
        "collection_id": after.collection_id,
        "enabled": after.enabled,
    }));
    append_event(connection, &event)
        .await?
        .into_audit_receipt()
        .map_err(Into::into)
}

fn event_sink_snapshot(row: &EventSinkRow) -> Value {
    json!({
        "id": row.id,
        "name": row.name,
        "kind": row.kind,
        "config": redact_event_sink_config(&row.config),
        "secret_ref": row.secret_ref,
        "enabled": row.enabled,
        "revision": row.revision,
    })
}

fn event_subscription_snapshot(row: &EventSubscriptionRow) -> Value {
    json!({
        "id": row.id,
        "collection_id": row.collection_id,
        "sink_id": row.sink_id,
        "name": row.name,
        "description": row.description,
        "entity_types": row.entity_types,
        "actions": row.actions,
        "filter": row.filter,
        "routing": redact_event_sink_config(&row.routing),
        "enabled": row.enabled,
        "revision": row.revision,
    })
}

fn build_event_sink_query(
    options: &QueryOptions,
) -> Result<crate::schema::event_sinks::BoxedQuery<'static, diesel::pg::Pg>, PostgresStorageError> {
    use crate::schema::event_sinks::dsl::{created_at, event_sinks, id, kind, name, revision};

    let mut query = event_sinks.into_boxed();
    for parameter in options.filters() {
        match parameter.field {
            FilterField::Id => crate::postgres_integer_filter!(query, parameter, id),
            FilterField::Name => crate::postgres_string_filter!(query, parameter, name),
            FilterField::Kind => crate::postgres_string_filter!(query, parameter, kind),
            FilterField::CreatedAt => {
                crate::postgres_datetime_filter!(query, parameter, created_at)
            }
            FilterField::Revision => {
                crate::postgres_revision_filter!(query, parameter, revision)
            }
            _ => {
                return Err(PostgresStorageError::invalid_input(format!(
                    "Field '{}' is not searchable for event sinks",
                    parameter.field
                )));
            }
        }
    }
    Ok(query)
}

fn build_event_subscription_query(
    collection: i32,
    options: &QueryOptions,
) -> Result<
    crate::schema::event_subscriptions::BoxedQuery<'static, diesel::pg::Pg>,
    PostgresStorageError,
> {
    use crate::schema::event_subscriptions::dsl::{
        collection_id, created_at, event_subscriptions, id, name, revision,
    };

    let mut query = event_subscriptions
        .filter(collection_id.eq(collection))
        .into_boxed();
    for parameter in options.filters() {
        match parameter.field {
            FilterField::Id => crate::postgres_integer_filter!(query, parameter, id),
            FilterField::Name => crate::postgres_string_filter!(query, parameter, name),
            FilterField::CreatedAt => {
                crate::postgres_datetime_filter!(query, parameter, created_at)
            }
            FilterField::Revision => {
                crate::postgres_revision_filter!(query, parameter, revision)
            }
            _ => {
                return Err(PostgresStorageError::invalid_input(format!(
                    "Field '{}' is not searchable for event subscriptions",
                    parameter.field
                )));
            }
        }
    }
    Ok(query)
}

fn event_sink_cursor_field(field: &FilterField) -> Result<CursorSqlField, PostgresStorageError> {
    Ok(match field {
        FilterField::Id => cursor_field("event_sinks.id", CursorSqlType::Integer),
        FilterField::Name => cursor_field("event_sinks.name", CursorSqlType::String),
        FilterField::Kind => cursor_field("event_sinks.kind", CursorSqlType::String),
        FilterField::CreatedAt => cursor_field("event_sinks.created_at", CursorSqlType::DateTime),
        FilterField::Revision => cursor_field("event_sinks.revision", CursorSqlType::BigInt),
        _ => {
            return Err(PostgresStorageError::invalid_input(format!(
                "Field '{field}' is not orderable for event sinks"
            )));
        }
    })
}

fn event_subscription_cursor_field(
    field: &FilterField,
) -> Result<CursorSqlField, PostgresStorageError> {
    Ok(match field {
        FilterField::Id => cursor_field("event_subscriptions.id", CursorSqlType::Integer),
        FilterField::Name => cursor_field("event_subscriptions.name", CursorSqlType::String),
        FilterField::CreatedAt => {
            cursor_field("event_subscriptions.created_at", CursorSqlType::DateTime)
        }
        FilterField::Revision => {
            cursor_field("event_subscriptions.revision", CursorSqlType::BigInt)
        }
        _ => {
            return Err(PostgresStorageError::invalid_input(format!(
                "Field '{field}' is not orderable for event subscriptions"
            )));
        }
    })
}

const fn cursor_field(column: &'static str, sql_type: CursorSqlType) -> CursorSqlField {
    CursorSqlField {
        column,
        sql_type,
        nullable: false,
    }
}

fn encode_json<T>(value: &T, label: &str) -> Result<Value, PostgresStorageError>
where
    T: serde::Serialize + ?Sized,
{
    serde_json::to_value(value).map_err(|error| {
        PostgresStorageError::database(format!("Could not serialize {label}: {error}"))
    })
}

fn decode_json<T>(value: Value, label: &str) -> Result<T, PostgresStorageError>
where
    T: serde::de::DeserializeOwned,
{
    serde_json::from_value(value).map_err(|error| {
        PostgresStorageError::database(format!("Could not deserialize {label}: {error}"))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subscription_audit_snapshot_redacts_routing_credentials() {
        let timestamp = chrono::DateTime::from_timestamp(1_700_000_000, 0)
            .unwrap()
            .naive_utc();
        let row = EventSubscriptionRow {
            id: 1,
            collection_id: 2,
            sink_id: 3,
            name: "webhook".to_string(),
            description: String::new(),
            entity_types: json!(["collection"]),
            actions: json!(["created"]),
            filter: json!({}),
            routing: json!({
                "url": "https://user:audit-password@example.invalid/events",
                "headers": {"X-API-Key": "audit-api-key"}
            }),
            enabled: true,
            created_at: timestamp,
            updated_at: timestamp,
            revision: PostgresRevision::INITIAL,
        };

        let snapshot = event_subscription_snapshot(&row);

        assert_eq!(
            snapshot["routing"]["url"],
            "https://[redacted]@example.invalid/events"
        );
        assert_eq!(snapshot["routing"]["headers"]["X-API-Key"], "[redacted]");
        assert!(!snapshot.to_string().contains("audit-password"));
        assert!(!snapshot.to_string().contains("audit-api-key"));
    }
}
