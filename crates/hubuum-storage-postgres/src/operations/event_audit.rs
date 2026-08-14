use std::collections::HashSet;

use diesel::SelectableHelper;
use diesel::prelude::{BoolExpressionMethods, ExpressionMethods, QueryDsl};
use diesel_async::RunQueryDsl;
use hubuum_events_core::{Action, EntityType};
use hubuum_query::{FilterField, QueryOptions};
use hubuum_storage_core::{
    StorageAuditEvent, StorageAuditEventFilters, StorageAuditEventListQuery, StorageEventPage,
};

use crate::cursor::{CursorSqlField, CursorSqlType};
use crate::{PostgresRuntime, PostgresStorageError};

use super::event_rows::{StoredEventProjection, enrich_stored_events};

/// List visibility-scoped audit events from one consistent PostgreSQL snapshot.
pub async fn list_audit_events(
    runtime: &PostgresRuntime,
    query: StorageAuditEventListQuery,
) -> Result<StorageEventPage<StorageAuditEvent>, PostgresStorageError> {
    let include_total = query.options().include_total;
    runtime
        .with_read_only_snapshot(
            async |connection| -> Result<StorageEventPage<StorageAuditEvent>, PostgresStorageError> {
                let total = if include_total {
                    Some(
                        build_audit_event_query(
                            query.accessible_collection_ids(),
                            query.include_collection_less(),
                            query.filters(),
                            query.options(),
                        )?
                        .count()
                        .get_result::<i64>(connection)
                        .await?,
                    )
                } else {
                    None
                };

                let mut records = build_audit_event_query(
                    query.accessible_collection_ids(),
                    query.include_collection_less(),
                    query.filters(),
                    query.options(),
                )?;
                let fields = query
                    .options()
                    .sort
                    .iter()
                    .map(|sort| audit_event_cursor_field(&sort.field))
                    .collect::<Result<Vec<_>, _>>()?;
                crate::apply_query_options_with_fields!(records, query.options(), fields);
                let mut event_rows = records
                    .select(StoredEventProjection::as_select())
                    .load::<StoredEventProjection>(connection)
                    .await?;
                let principal_names = enrich_stored_events(connection, &mut event_rows).await?;
                let accessible_collection_ids = query
                    .accessible_collection_ids()
                    .iter()
                    .copied()
                    .collect::<HashSet<_>>();
                let rows = event_rows
                    .into_iter()
                    .map(|event| {
                        let directly_visible = event.collection_id.is_some_and(|collection_id| {
                            accessible_collection_ids.contains(&collection_id)
                        }) || (query.include_collection_less() && event.collection_id.is_none());
                        event.into_audit_event(&principal_names, !directly_visible)
                    })
                    .collect();
                Ok(StorageEventPage::new(rows, total))
            },
        )
        .await
}

fn build_audit_event_query(
    accessible_collection_ids: &[i32],
    include_collection_less: bool,
    filters: &StorageAuditEventFilters,
    options: &QueryOptions,
) -> Result<crate::schema::events::BoxedQuery<'static, diesel::pg::Pg>, PostgresStorageError> {
    use crate::schema::event_related_collections::dsl as related;
    use crate::schema::events::dsl::{
        action, actor_kind, actor_user_id, after_revision, before_revision, collection_id,
        entity_id, entity_type, events, id as event_row_id, initiator_user_id, occurred_at,
    };

    let mut query = events.into_boxed();
    if !include_collection_less && accessible_collection_ids.is_empty() {
        return Ok(query.filter(event_row_id.eq(-1_i64)));
    }

    let accessible_collection_ids = accessible_collection_ids.to_vec();
    let related_is_visible = diesel::dsl::exists(
        related::event_related_collections
            .filter(related::event_id.eq(event_row_id))
            .filter(related::collection_id.eq_any(accessible_collection_ids.iter().copied())),
    );
    if include_collection_less {
        if !accessible_collection_ids.is_empty() {
            query = query.filter(
                collection_id
                    .eq_any(accessible_collection_ids.iter().copied())
                    .or(collection_id.is_null())
                    .or(related_is_visible),
            );
        }
    } else {
        query = query.filter(
            collection_id
                .eq_any(accessible_collection_ids.iter().copied())
                .or(related_is_visible),
        );
    }

    if let Some(value) = filters.entity_type_value() {
        query = query.filter(entity_type.eq(value.as_str()));
    }
    if let Some(value) = filters.entity_id_value() {
        query = query.filter(entity_id.eq(Some(value)));
    }
    if let Some(value) = filters.action_value() {
        query = query.filter(action.eq(value.as_str()));
    }
    if let Some(value) = filters.actor_kind_value() {
        query = query.filter(actor_kind.eq(value.as_str()));
    }
    if let Some(value) = filters.actor_user_id_value() {
        query = query.filter(actor_user_id.eq(Some(value)));
    }
    if let Some(value) = filters.initiator_user_id_value() {
        let queued_events = diesel::alias!(crate::schema::events as queued_events);
        let queued_initiator = queued_events
            .field(crate::schema::events::initiator_user_id)
            .eq(Some(value))
            .or(queued_events
                .field(crate::schema::events::actor_user_id)
                .eq(Some(value)));
        let queued_fallback = diesel::dsl::exists(
            queued_events
                .filter(
                    queued_events
                        .field(crate::schema::events::entity_type)
                        .eq(EntityType::Task.as_str()),
                )
                .filter(
                    queued_events
                        .field(crate::schema::events::action)
                        .eq(Action::Queued.as_str()),
                )
                .filter(
                    queued_events
                        .field(crate::schema::events::entity_id)
                        .eq(crate::schema::events::entity_id),
                )
                .filter(queued_initiator),
        );
        query = query.filter(
            initiator_user_id.eq(Some(value)).or(entity_type
                .eq(EntityType::Task.as_str())
                .and(queued_fallback)),
        );
    }
    if let Some(value) = filters.collection_id_value() {
        query = query.filter(collection_id.eq(Some(value)));
    }
    if let Some(value) = filters.occurred_after_value() {
        query = query.filter(occurred_at.ge(value));
    }
    if let Some(value) = filters.occurred_before_value() {
        query = query.filter(occurred_at.le(value));
    }

    for parameter in &options.filters {
        match parameter.field {
            FilterField::BeforeRevision => {
                crate::postgres_revision_filter!(query, parameter, before_revision)
            }
            FilterField::AfterRevision => {
                crate::postgres_revision_filter!(query, parameter, after_revision)
            }
            _ => {
                return Err(PostgresStorageError::bad_request(format!(
                    "Field '{}' is not searchable for events",
                    parameter.field
                )));
            }
        }
    }
    Ok(query)
}

fn audit_event_cursor_field(field: &FilterField) -> Result<CursorSqlField, PostgresStorageError> {
    Ok(match field {
        FilterField::Id => CursorSqlField {
            column: "events.id",
            sql_type: CursorSqlType::BigInt,
            nullable: false,
        },
        FilterField::OccurredAt => CursorSqlField {
            column: "events.occurred_at",
            sql_type: CursorSqlType::DateTime,
            nullable: false,
        },
        _ => {
            return Err(PostgresStorageError::bad_request(format!(
                "Field '{field}' is not orderable for events"
            )));
        }
    })
}
