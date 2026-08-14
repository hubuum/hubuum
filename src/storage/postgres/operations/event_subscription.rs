use std::{fmt, str::FromStr};

use crate::storage::postgres::prelude::*;
use chrono::NaiveDateTime;
use serde_json::json;

use crate::api::etag::RevisionOwner;
use crate::apply_query_options;
use crate::errors::ApiError;
use crate::events::{Action, EntityType, EventContext, NewEvent};
use crate::models::REDACTED_DEBUG_VALUE;
use crate::models::event_subscription::{
    EventSink, EventSinkID, EventSinkKind, EventSubscription, EventSubscriptionID,
};
use crate::models::search::{FilterField, QueryOptions, SortParam};
use crate::pagination::{
    CursorPaginated, CursorSqlField, CursorSqlMapping, CursorSqlType, CursorValue,
};
use crate::storage::postgres::operations::event_record::emit_event;
use crate::storage::postgres::{with_connection, with_transaction};

macro_rules! impl_redacted_event_sink_row_debug {
    ($target:ty, $($field:ident),+ $(,)?) => {
        impl fmt::Debug for $target {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                let mut debug = formatter.debug_struct(stringify!($target));
                $(debug.field(stringify!($field), &self.$field);)+
                debug
                    .field("configuration", &REDACTED_DEBUG_VALUE)
                    .finish()
            }
        }
    };
}

macro_rules! impl_redacted_event_subscription_row_debug {
    ($target:ty, $($field:ident),+ $(,)?) => {
        impl fmt::Debug for $target {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                let mut debug = formatter.debug_struct(stringify!($target));
                $(debug.field(stringify!($field), &self.$field);)+
                debug.field("routing", &REDACTED_DEBUG_VALUE).finish()
            }
        }
    };
}

#[derive(Clone, Queryable, Selectable)]
#[diesel(table_name = crate::schema::event_sinks)]
pub(crate) struct EventSinkRow {
    pub(crate) id: i32,
    pub(crate) name: String,
    pub(crate) kind: String,
    pub(crate) config: serde_json::Value,
    pub(crate) secret_ref: Option<String>,
    pub(crate) enabled: bool,
    pub(crate) created_at: NaiveDateTime,
    pub(crate) updated_at: NaiveDateTime,
    pub(crate) revision: PostgresRevision,
}

impl_redacted_event_sink_row_debug!(
    EventSinkRow,
    id,
    name,
    kind,
    enabled,
    created_at,
    updated_at,
);

#[derive(Clone, Insertable)]
#[diesel(table_name = crate::schema::event_sinks)]
pub(crate) struct NewEventSinkRow {
    pub(crate) name: String,
    pub(crate) kind: String,
    pub(crate) config: serde_json::Value,
    pub(crate) secret_ref: Option<String>,
    pub(crate) enabled: bool,
}

impl_redacted_event_sink_row_debug!(NewEventSinkRow, name, kind, enabled);

#[derive(Clone, AsChangeset)]
#[diesel(table_name = crate::schema::event_sinks)]
pub(crate) struct UpdateEventSinkRow {
    pub(crate) name: Option<String>,
    pub(crate) kind: Option<String>,
    pub(crate) config: Option<serde_json::Value>,
    pub(crate) secret_ref: Option<Option<String>>,
    pub(crate) enabled: Option<bool>,
}

impl_redacted_event_sink_row_debug!(UpdateEventSinkRow, name, kind, enabled);

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
pub(crate) struct EventSubscriptionRow {
    pub(crate) id: i32,
    pub(crate) collection_id: i32,
    pub(crate) sink_id: i32,
    pub(crate) name: String,
    pub(crate) description: String,
    pub(crate) entity_types: serde_json::Value,
    pub(crate) actions: serde_json::Value,
    pub(crate) filter: serde_json::Value,
    pub(crate) routing: serde_json::Value,
    pub(crate) enabled: bool,
    pub(crate) created_at: NaiveDateTime,
    pub(crate) updated_at: NaiveDateTime,
    pub(crate) revision: PostgresRevision,
}

impl_redacted_event_subscription_row_debug!(
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
);

#[derive(Clone, Insertable)]
#[diesel(table_name = crate::schema::event_subscriptions)]
pub(crate) struct NewEventSubscriptionRow {
    pub(crate) collection_id: i32,
    pub(crate) sink_id: i32,
    pub(crate) name: String,
    pub(crate) description: String,
    pub(crate) entity_types: serde_json::Value,
    pub(crate) actions: serde_json::Value,
    pub(crate) filter: serde_json::Value,
    pub(crate) routing: serde_json::Value,
    pub(crate) enabled: bool,
}

impl_redacted_event_subscription_row_debug!(
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

#[derive(Clone, AsChangeset)]
#[diesel(table_name = crate::schema::event_subscriptions)]
pub(crate) struct UpdateEventSubscriptionRow {
    pub(crate) sink_id: Option<i32>,
    pub(crate) name: Option<String>,
    pub(crate) description: Option<String>,
    pub(crate) entity_types: Option<serde_json::Value>,
    pub(crate) actions: Option<serde_json::Value>,
    pub(crate) filter: Option<serde_json::Value>,
    pub(crate) routing: Option<serde_json::Value>,
    pub(crate) enabled: Option<bool>,
}

impl_redacted_event_subscription_row_debug!(
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

impl TryFrom<EventSinkRow> for EventSink {
    type Error = ApiError;

    fn try_from(row: EventSinkRow) -> Result<Self, Self::Error> {
        Ok(Self {
            id: row.id,
            name: row.name,
            kind: EventSinkKind::from_str(&row.kind)?,
            config: row.config,
            secret_ref: row.secret_ref,
            enabled: row.enabled,
            created_at: row.created_at,
            updated_at: row.updated_at,
            revision: row.revision.into_domain(),
        })
    }
}

impl TryFrom<EventSubscriptionRow> for EventSubscription {
    type Error = ApiError;

    fn try_from(row: EventSubscriptionRow) -> Result<Self, Self::Error> {
        Ok(Self {
            id: row.id,
            collection_id: row.collection_id,
            sink_id: row.sink_id,
            name: row.name,
            description: row.description,
            entity_types: serde_json::from_value(row.entity_types)?,
            actions: serde_json::from_value(row.actions)?,
            filter: serde_json::from_value(row.filter)?,
            routing: row.routing,
            enabled: row.enabled,
            created_at: row.created_at,
            updated_at: row.updated_at,
            revision: row.revision.into_domain(),
        })
    }
}

impl CursorPaginated for EventSinkRow {
    fn supports_sort(field: &FilterField) -> bool {
        matches!(
            field,
            FilterField::Id
                | FilterField::Name
                | FilterField::Kind
                | FilterField::CreatedAt
                | FilterField::Revision
        )
    }

    fn cursor_value(&self, field: &FilterField) -> Result<CursorValue, ApiError> {
        match field {
            FilterField::Id => Ok(CursorValue::Integer(i64::from(self.id))),
            FilterField::Name => Ok(CursorValue::String(self.name.clone())),
            FilterField::Kind => Ok(CursorValue::String(self.kind.clone())),
            FilterField::CreatedAt => Ok(CursorValue::DateTime(self.created_at)),
            FilterField::Revision => Ok(CursorValue::Integer(self.revision.get())),
            _ => Err(ApiError::BadRequest(format!(
                "Unsupported sort field '{}' for event sinks",
                field
            ))),
        }
    }

    fn default_sort() -> Vec<SortParam> {
        vec![SortParam {
            field: FilterField::Id,
            descending: false,
        }]
    }

    fn tie_breaker_sort() -> Vec<SortParam> {
        Self::default_sort()
    }
}

impl CursorSqlMapping for EventSinkRow {
    fn sql_field(field: &FilterField) -> Result<CursorSqlField, ApiError> {
        Ok(match field {
            FilterField::Id => CursorSqlField {
                column: "event_sinks.id",
                sql_type: CursorSqlType::Integer,
                nullable: false,
            },
            FilterField::Name => CursorSqlField {
                column: "event_sinks.name",
                sql_type: CursorSqlType::String,
                nullable: false,
            },
            FilterField::Kind => CursorSqlField {
                column: "event_sinks.kind",
                sql_type: CursorSqlType::String,
                nullable: false,
            },
            FilterField::CreatedAt => CursorSqlField {
                column: "event_sinks.created_at",
                sql_type: CursorSqlType::DateTime,
                nullable: false,
            },
            FilterField::Revision => CursorSqlField {
                column: "event_sinks.revision",
                sql_type: CursorSqlType::BigInt,
                nullable: false,
            },
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{}' is not orderable for event sinks",
                    field
                )));
            }
        })
    }
}

impl CursorPaginated for EventSubscriptionRow {
    fn supports_sort(field: &FilterField) -> bool {
        matches!(
            field,
            FilterField::Id | FilterField::Name | FilterField::CreatedAt | FilterField::Revision
        )
    }

    fn cursor_value(&self, field: &FilterField) -> Result<CursorValue, ApiError> {
        match field {
            FilterField::Id => Ok(CursorValue::Integer(i64::from(self.id))),
            FilterField::Name => Ok(CursorValue::String(self.name.clone())),
            FilterField::CreatedAt => Ok(CursorValue::DateTime(self.created_at)),
            FilterField::Revision => Ok(CursorValue::Integer(self.revision.get())),
            _ => Err(ApiError::BadRequest(format!(
                "Unsupported sort field '{}' for event subscriptions",
                field
            ))),
        }
    }

    fn default_sort() -> Vec<SortParam> {
        vec![SortParam {
            field: FilterField::Id,
            descending: false,
        }]
    }

    fn tie_breaker_sort() -> Vec<SortParam> {
        Self::default_sort()
    }
}

impl CursorSqlMapping for EventSubscriptionRow {
    fn sql_field(field: &FilterField) -> Result<CursorSqlField, ApiError> {
        Ok(match field {
            FilterField::Id => CursorSqlField {
                column: "event_subscriptions.id",
                sql_type: CursorSqlType::Integer,
                nullable: false,
            },
            FilterField::Name => CursorSqlField {
                column: "event_subscriptions.name",
                sql_type: CursorSqlType::String,
                nullable: false,
            },
            FilterField::CreatedAt => CursorSqlField {
                column: "event_subscriptions.created_at",
                sql_type: CursorSqlType::DateTime,
                nullable: false,
            },
            FilterField::Revision => CursorSqlField {
                column: "event_subscriptions.revision",
                sql_type: CursorSqlType::BigInt,
                nullable: false,
            },
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{}' is not orderable for event subscriptions",
                    field
                )));
            }
        })
    }
}

pub(crate) trait LoadEventSinkRecord {
    async fn load_event_sink_record(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
    ) -> Result<EventSinkRow, ApiError>;
}

impl LoadEventSinkRecord for EventSinkID {
    async fn load_event_sink_record(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
    ) -> Result<EventSinkRow, ApiError> {
        use crate::schema::event_sinks::dsl::{event_sinks, id};

        with_connection(pool, async |conn| {
            event_sinks
                .filter(id.eq(self.id()))
                .first::<EventSinkRow>(conn)
                .await
        })
        .await
    }
}

pub(crate) trait SaveEventSinkRecord {
    async fn save_event_sink_record(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
        event_context: &EventContext,
    ) -> Result<EventSinkRow, ApiError>;

    #[cfg(any(test, feature = "integration-test-support"))]
    async fn save_event_sink_record_without_events(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
    ) -> Result<EventSinkRow, ApiError>;
}

impl SaveEventSinkRecord for NewEventSinkRow {
    async fn save_event_sink_record(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
        event_context: &EventContext,
    ) -> Result<EventSinkRow, ApiError> {
        insert_event_sink_record(self, pool, Some(event_context)).await
    }

    #[cfg(any(test, feature = "integration-test-support"))]
    async fn save_event_sink_record_without_events(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
    ) -> Result<EventSinkRow, ApiError> {
        insert_event_sink_record(self, pool, None).await
    }
}

async fn insert_event_sink_record(
    row: &NewEventSinkRow,
    pool: &crate::storage::postgres::PostgresPool,
    event_context: Option<&EventContext>,
) -> Result<EventSinkRow, ApiError> {
    use crate::schema::event_sinks::dsl::event_sinks;

    with_transaction(pool, async |conn| -> Result<EventSinkRow, ApiError> {
        let created = diesel::insert_into(event_sinks)
            .values(row)
            .get_result::<EventSinkRow>(conn)
            .await?;
        emit_event_sink_audit(conn, Action::Created, event_context, None, &created).await?;
        Ok(created)
    })
    .await
}

pub(crate) trait UpdateEventSinkRecord {
    async fn update_event_sink_record(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
        sink_id: i32,
        event_context: &EventContext,
    ) -> Result<EventSinkRow, ApiError>;
}

impl UpdateEventSinkRecord for UpdateEventSinkRow {
    async fn update_event_sink_record(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
        sink_id: i32,
        event_context: &EventContext,
    ) -> Result<EventSinkRow, ApiError> {
        update_event_sink_record_impl(self, pool, sink_id, Some(event_context)).await
    }
}

async fn update_event_sink_record_impl(
    row: &UpdateEventSinkRow,
    pool: &crate::storage::postgres::PostgresPool,
    sink_id: i32,
    event_context: Option<&EventContext>,
) -> Result<EventSinkRow, ApiError> {
    use crate::schema::event_sinks::dsl::{event_sinks, id};

    with_transaction(pool, async |conn| -> Result<EventSinkRow, ApiError> {
        let before = event_sinks
            .filter(id.eq(sink_id))
            .for_update()
            .first::<EventSinkRow>(conn)
            .await?;
        crate::storage::postgres::assert_locked_revision_precondition(
            conn,
            &RevisionOwner::EventSink.key(before.id),
            before.revision,
        )
        .await?;
        if !row.has_changes(&before) {
            return Ok(before);
        }
        let updated = diesel::update(event_sinks.filter(id.eq(sink_id)))
            .set(row)
            .get_result::<EventSinkRow>(conn)
            .await?;
        emit_event_sink_audit(
            conn,
            Action::Updated,
            event_context,
            Some(&before),
            &updated,
        )
        .await?;
        Ok(updated)
    })
    .await
}

pub(crate) trait DeleteEventSinkRecord {
    async fn delete_event_sink_record(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
        event_context: &EventContext,
    ) -> Result<(), ApiError>;
}

impl DeleteEventSinkRecord for EventSinkID {
    async fn delete_event_sink_record(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
        event_context: &EventContext,
    ) -> Result<(), ApiError> {
        delete_event_sink_record_impl(self, pool, Some(event_context)).await
    }
}

async fn delete_event_sink_record_impl(
    sink_id: &EventSinkID,
    pool: &crate::storage::postgres::PostgresPool,
    event_context: Option<&EventContext>,
) -> Result<(), ApiError> {
    use crate::schema::event_sinks::dsl::{event_sinks, id};

    with_transaction(pool, async |conn| -> Result<(), ApiError> {
        let before = event_sinks
            .filter(id.eq(sink_id.id()))
            .for_update()
            .first::<EventSinkRow>(conn)
            .await?;
        emit_event_sink_audit(conn, Action::Deleted, event_context, Some(&before), &before).await?;
        diesel::delete(event_sinks.filter(id.eq(sink_id.id())))
            .execute(conn)
            .await
            .map_err(ApiError::from)?;
        Ok(())
    })
    .await?;
    Ok(())
}

pub(crate) trait LoadEventSubscriptionRecord {
    async fn load_event_subscription_record(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
    ) -> Result<EventSubscriptionRow, ApiError>;
}

impl LoadEventSubscriptionRecord for EventSubscriptionID {
    async fn load_event_subscription_record(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
    ) -> Result<EventSubscriptionRow, ApiError> {
        use crate::schema::event_subscriptions::dsl::{event_subscriptions, id};

        with_connection(pool, async |conn| {
            event_subscriptions
                .filter(id.eq(self.id()))
                .first::<EventSubscriptionRow>(conn)
                .await
        })
        .await
    }
}

pub(crate) trait SaveEventSubscriptionRecord {
    async fn save_event_subscription_record(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
        event_context: &EventContext,
    ) -> Result<EventSubscriptionRow, ApiError>;

    #[cfg(any(test, feature = "integration-test-support"))]
    async fn save_event_subscription_record_without_events(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
    ) -> Result<EventSubscriptionRow, ApiError>;
}

impl SaveEventSubscriptionRecord for NewEventSubscriptionRow {
    async fn save_event_subscription_record(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
        event_context: &EventContext,
    ) -> Result<EventSubscriptionRow, ApiError> {
        insert_event_subscription_record(self, pool, Some(event_context)).await
    }

    #[cfg(any(test, feature = "integration-test-support"))]
    async fn save_event_subscription_record_without_events(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
    ) -> Result<EventSubscriptionRow, ApiError> {
        insert_event_subscription_record(self, pool, None).await
    }
}

async fn insert_event_subscription_record(
    row: &NewEventSubscriptionRow,
    pool: &crate::storage::postgres::PostgresPool,
    event_context: Option<&EventContext>,
) -> Result<EventSubscriptionRow, ApiError> {
    use crate::schema::event_subscriptions::dsl::event_subscriptions;

    with_transaction(
        pool,
        async |conn| -> Result<EventSubscriptionRow, ApiError> {
            let created = diesel::insert_into(event_subscriptions)
                .values(row)
                .get_result::<EventSubscriptionRow>(conn)
                .await?;
            emit_event_subscription_audit(conn, Action::Created, event_context, None, &created)
                .await?;
            Ok(created)
        },
    )
    .await
}

pub(crate) trait UpdateEventSubscriptionRecord {
    async fn update_event_subscription_record(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
        subscription_id: i32,
        event_context: &EventContext,
    ) -> Result<EventSubscriptionRow, ApiError>;
}

impl UpdateEventSubscriptionRecord for UpdateEventSubscriptionRow {
    async fn update_event_subscription_record(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
        subscription_id: i32,
        event_context: &EventContext,
    ) -> Result<EventSubscriptionRow, ApiError> {
        update_event_subscription_record_impl(self, pool, subscription_id, Some(event_context))
            .await
    }
}

async fn update_event_subscription_record_impl(
    row: &UpdateEventSubscriptionRow,
    pool: &crate::storage::postgres::PostgresPool,
    subscription_id: i32,
    event_context: Option<&EventContext>,
) -> Result<EventSubscriptionRow, ApiError> {
    use crate::schema::event_subscriptions::dsl::{event_subscriptions, id};

    with_transaction(
        pool,
        async |conn| -> Result<EventSubscriptionRow, ApiError> {
            let before = event_subscriptions
                .filter(id.eq(subscription_id))
                .for_update()
                .first::<EventSubscriptionRow>(conn)
                .await?;
            crate::storage::postgres::assert_locked_revision_precondition(
                conn,
                &RevisionOwner::EventSubscription.key(before.id),
                before.revision,
            )
            .await?;
            if !row.has_changes(&before) {
                return Ok(before);
            }
            let updated = diesel::update(event_subscriptions.filter(id.eq(subscription_id)))
                .set(row)
                .get_result::<EventSubscriptionRow>(conn)
                .await?;
            emit_event_subscription_audit(
                conn,
                Action::Updated,
                event_context,
                Some(&before),
                &updated,
            )
            .await?;
            Ok(updated)
        },
    )
    .await
}

pub(crate) trait DeleteEventSubscriptionRecord {
    async fn delete_event_subscription_record(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
        event_context: &EventContext,
    ) -> Result<(), ApiError>;
}

impl DeleteEventSubscriptionRecord for EventSubscriptionID {
    async fn delete_event_subscription_record(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
        event_context: &EventContext,
    ) -> Result<(), ApiError> {
        delete_event_subscription_record_impl(self, pool, Some(event_context)).await
    }
}

async fn delete_event_subscription_record_impl(
    subscription_id: &EventSubscriptionID,
    pool: &crate::storage::postgres::PostgresPool,
    event_context: Option<&EventContext>,
) -> Result<(), ApiError> {
    use crate::schema::event_subscriptions::dsl::{event_subscriptions, id};

    with_transaction(pool, async |conn| -> Result<(), ApiError> {
        let before = event_subscriptions
            .filter(id.eq(subscription_id.id()))
            .for_update()
            .first::<EventSubscriptionRow>(conn)
            .await?;
        emit_event_subscription_audit(conn, Action::Deleted, event_context, Some(&before), &before)
            .await?;
        diesel::delete(event_subscriptions.filter(id.eq(subscription_id.id())))
            .execute(conn)
            .await
            .map_err(ApiError::from)?;
        Ok(())
    })
    .await?;
    Ok(())
}

async fn emit_event_sink_audit(
    conn: &mut crate::storage::postgres::PostgresConnection,
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
    emit_event(conn, &event).await?;
    Ok(())
}

async fn emit_event_subscription_audit(
    conn: &mut crate::storage::postgres::PostgresConnection,
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
    emit_event(conn, &event).await?;
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
        "revision": row.revision,
    })
}

fn event_subscription_snapshot(row: &EventSubscriptionRow) -> serde_json::Value {
    let routing = crate::models::event_subscription::redact_event_sink_config(&row.routing);
    json!({
        "id": row.id,
        "collection_id": row.collection_id,
        "sink_id": row.sink_id,
        "name": row.name,
        "description": row.description,
        "entity_types": row.entity_types,
        "actions": row.actions,
        "filter": row.filter,
        "routing": routing,
        "enabled": row.enabled,
        "revision": row.revision,
    })
}

pub(crate) async fn list_event_sink_rows_with_total_count(
    pool: &crate::storage::postgres::PostgresPool,
    query_options: &QueryOptions,
) -> Result<(Vec<EventSinkRow>, i64), ApiError> {
    let query = build_event_sink_query(query_options)?;
    let total_count = crate::pagination::exact_count_or_skipped(query_options, async || {
        with_connection(pool, async |conn| {
            query.count().get_result::<i64>(conn).await
        })
        .await
    })
    .await?;
    let mut query = build_event_sink_query(query_options)?;
    apply_query_options!(query, query_options, EventSinkRow);
    let rows = with_connection(pool, async |conn| query.load::<EventSinkRow>(conn).await).await?;
    Ok((rows, total_count))
}

pub async fn enabled_event_sink_count(
    pool: &crate::storage::postgres::PostgresPool,
) -> Result<i64, ApiError> {
    use diesel::dsl::count_star;

    use crate::schema::event_sinks::dsl::{enabled, event_sinks};

    with_connection(pool, async |conn| {
        event_sinks
            .filter(enabled.eq(true))
            .select(count_star())
            .first::<i64>(conn)
            .await
    })
    .await
}

fn build_event_sink_query(
    query_options: &QueryOptions,
) -> Result<crate::schema::event_sinks::BoxedQuery<'static, diesel::pg::Pg>, ApiError> {
    use crate::schema::event_sinks::dsl::{created_at, event_sinks, id, kind, name, revision};

    let mut query = event_sinks.into_boxed();
    for param in query_options.filters.clone() {
        let operator = param.operator.clone();
        match param.field {
            FilterField::Id => crate::numeric_search!(query, param, operator, id),
            FilterField::Name => crate::string_search!(query, param, operator, name),
            FilterField::Kind => crate::string_search!(query, param, operator, kind),
            FilterField::CreatedAt => crate::date_search!(query, param, operator, created_at),
            FilterField::Revision => crate::revision_search!(query, param, operator, revision),
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
    pool: &crate::storage::postgres::PostgresPool,
    collection: i32,
    query_options: &QueryOptions,
) -> Result<(Vec<EventSubscriptionRow>, i64), ApiError> {
    let base = build_event_subscription_query(collection, query_options)?;
    let total_count = crate::pagination::exact_count_or_skipped(query_options, async || {
        with_connection(pool, async |conn| {
            base.count().get_result::<i64>(conn).await
        })
        .await
    })
    .await?;
    let mut query = build_event_subscription_query(collection, query_options)?;
    apply_query_options!(query, query_options, EventSubscriptionRow);
    let rows = with_connection(pool, async |conn| {
        query.load::<EventSubscriptionRow>(conn).await
    })
    .await?;
    Ok((rows, total_count))
}

fn build_event_subscription_query(
    collection: i32,
    query_options: &QueryOptions,
) -> Result<crate::schema::event_subscriptions::BoxedQuery<'static, diesel::pg::Pg>, ApiError> {
    use crate::schema::event_subscriptions::dsl::{
        collection_id, created_at, event_subscriptions, id, name, revision,
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
            FilterField::Revision => crate::revision_search!(query, param, operator, revision),
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

pub(crate) async fn load_event_sink_instance(
    pool: &crate::storage::postgres::PostgresPool,
    sink_id: &EventSinkID,
) -> Result<EventSink, ApiError> {
    sink_id.load_event_sink_record(pool).await?.try_into()
}

pub(crate) async fn load_event_subscription_instance(
    pool: &crate::storage::postgres::PostgresPool,
    subscription_id: &EventSubscriptionID,
) -> Result<EventSubscription, ApiError> {
    subscription_id
        .load_event_subscription_record(pool)
        .await?
        .try_into()
}

impl EventSink {
    pub async fn list_with_total_count(
        pool: &crate::storage::postgres::PostgresPool,
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
        pool: &crate::storage::postgres::PostgresPool,
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

#[cfg(test)]
mod tests {
    use super::*;

    fn timestamp() -> NaiveDateTime {
        chrono::DateTime::from_timestamp(1_700_000_000, 0)
            .unwrap()
            .naive_utc()
    }

    fn assert_omits(debug: &str, secrets: &[&str]) {
        assert!(debug.contains(REDACTED_DEBUG_VALUE));
        for secret in secrets {
            assert!(!debug.contains(secret));
        }
    }

    #[test]
    fn event_sink_row_debug_redacts_configuration() {
        let row = EventSinkRow {
            id: 1,
            name: "webhook".to_string(),
            kind: "webhook".to_string(),
            config: serde_json::json!({
                "headers": {"authorization": "stored-config-secret"}
            }),
            secret_ref: Some("stored-secret-reference".to_string()),
            enabled: true,
            created_at: timestamp(),
            updated_at: timestamp(),
            revision: PostgresRevision::INITIAL,
        };

        assert_omits(
            &format!("{row:?}"),
            &["stored-config-secret", "stored-secret-reference"],
        );
    }

    #[test]
    fn event_subscription_row_debug_redacts_routing() {
        let row = EventSubscriptionRow {
            id: 1,
            collection_id: 2,
            sink_id: 3,
            name: "webhook".to_string(),
            description: String::new(),
            entity_types: serde_json::json!(["collection"]),
            actions: serde_json::json!(["created"]),
            filter: serde_json::json!({}),
            routing: serde_json::json!({
                "url": "https://example.invalid/hook?key=stored-routing-secret"
            }),
            enabled: true,
            created_at: timestamp(),
            updated_at: timestamp(),
            revision: PostgresRevision::INITIAL,
        };

        assert_omits(&format!("{row:?}"), &["stored-routing-secret"]);
    }

    #[test]
    fn event_subscription_audit_snapshot_redacts_routing_credentials() {
        let credential = "audit-password";
        let api_key = "audit-api-key";
        let row = EventSubscriptionRow {
            id: 1,
            collection_id: 2,
            sink_id: 3,
            name: "webhook".to_string(),
            description: String::new(),
            entity_types: serde_json::json!(["collection"]),
            actions: serde_json::json!(["created"]),
            filter: serde_json::json!({}),
            routing: serde_json::json!({
                "url": format!("https://user:{credential}@example.invalid/events"),
                "headers": {"X-API-Key": api_key}
            }),
            enabled: true,
            created_at: timestamp(),
            updated_at: timestamp(),
            revision: PostgresRevision::INITIAL,
        };

        let snapshot = event_subscription_snapshot(&row);

        assert_eq!(
            snapshot["routing"]["url"],
            "https://[redacted]@example.invalid/events"
        );
        assert_eq!(snapshot["routing"]["headers"]["X-API-Key"], "[redacted]");
        assert!(!snapshot.to_string().contains(credential));
        assert!(!snapshot.to_string().contains(api_key));
    }
}
