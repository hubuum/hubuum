use crate::storage::postgres::prelude::*;
use std::collections::{HashMap, HashSet};

use crate::apply_query_options;
use crate::errors::ApiError;
use crate::events::{Action, ActorKind, EntityType, Event, EventResponse, PrincipalNames};
use crate::models::search::QueryOptions;
use crate::storage::postgres::operations::event_record::EventRow;
use crate::storage::postgres::operations::history::resolve_principal_names;
use crate::storage::postgres::with_connection;

#[derive(Debug, Clone, Default)]
pub struct EventListFilters {
    pub entity_type: Option<EntityType>,
    pub entity_id: Option<i32>,
    pub action: Option<Action>,
    pub actor_kind: Option<ActorKind>,
    pub actor_user_id: Option<i32>,
    pub initiator_user_id: Option<i32>,
    pub collection_id: Option<i32>,
    pub occurred_after: Option<chrono::NaiveDateTime>,
    pub occurred_before: Option<chrono::NaiveDateTime>,
}

pub async fn list_events_with_total_count(
    pool: &crate::storage::postgres::PostgresPool,
    accessible_collection_ids: &[i32],
    include_collection_less: bool,
    filters: &EventListFilters,
    query_options: &QueryOptions,
) -> Result<(Vec<EventResponse>, i64), ApiError> {
    crate::logger::log_operation_read(filters.entity_type, filters.action, filters.entity_id);

    let query = build_event_query(
        accessible_collection_ids,
        include_collection_less,
        filters,
        query_options,
    )?;
    let total_count = crate::pagination::exact_count_or_skipped(query_options, async || {
        with_connection(pool, async |conn| {
            query.count().get_result::<i64>(conn).await
        })
        .await
    })
    .await?;

    let mut query = build_event_query(
        accessible_collection_ids,
        include_collection_less,
        filters,
        query_options,
    )?;
    apply_query_options!(query, query_options, EventRow);
    let mut rows = with_connection(pool, async |conn| query.load::<EventRow>(conn).await)
        .await?
        .into_iter()
        .map(Event::from)
        .collect::<Vec<_>>();
    apply_legacy_task_provenance(pool, &mut rows).await?;
    let principal_ids = rows
        .iter()
        .flat_map(|event| [event.actor_user_id, event.initiator_user_id])
        .flatten()
        .collect();
    let principal_names = resolve_principal_names(pool, principal_ids).await?;
    let accessible_collection_ids = accessible_collection_ids
        .iter()
        .copied()
        .collect::<HashSet<_>>();
    let rows = rows
        .into_iter()
        .map(|event| {
            event_response_for_visibility(
                event,
                &accessible_collection_ids,
                include_collection_less,
                &principal_names,
            )
        })
        .collect();

    Ok((rows, total_count))
}

fn build_event_query<'a>(
    accessible_collection_ids: &'a [i32],
    include_collection_less: bool,
    filters: &EventListFilters,
    query_options: &QueryOptions,
) -> Result<crate::schema::events::BoxedQuery<'a, diesel::pg::Pg>, ApiError> {
    use crate::schema::event_related_collections::dsl as related;
    use crate::schema::events::dsl::{
        action, actor_kind, actor_user_id, after_revision, before_revision, collection_id,
        entity_id, entity_type, events, id as event_row_id, initiator_user_id, occurred_at,
    };

    let mut query = events.into_boxed();

    if !include_collection_less && accessible_collection_ids.is_empty() {
        return Ok(query.filter(event_row_id.eq(-1_i64)));
    }

    let related_is_visible = diesel::dsl::exists(
        related::event_related_collections
            .filter(related::event_id.eq(event_row_id))
            .filter(related::collection_id.eq_any(accessible_collection_ids)),
    );

    if include_collection_less {
        if !accessible_collection_ids.is_empty() {
            query = query.filter(
                collection_id
                    .eq_any(accessible_collection_ids)
                    .or(collection_id.is_null())
                    .or(related_is_visible),
            );
        }
    } else {
        query = query.filter(
            collection_id
                .eq_any(accessible_collection_ids)
                .or(related_is_visible),
        );
    }

    if let Some(value) = filters.entity_type {
        query = query.filter(entity_type.eq(value.as_str()));
    }
    if let Some(value) = filters.entity_id {
        query = query.filter(entity_id.eq(Some(value)));
    }
    if let Some(value) = filters.action {
        query = query.filter(action.eq(value.as_str()));
    }
    if let Some(value) = filters.actor_kind {
        query = query.filter(actor_kind.eq(value.as_str()));
    }
    if let Some(value) = filters.actor_user_id {
        query = query.filter(actor_user_id.eq(Some(value)));
    }
    if let Some(value) = filters.initiator_user_id {
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
    if let Some(value) = filters.collection_id {
        query = query.filter(collection_id.eq(Some(value)));
    }
    if let Some(value) = filters.occurred_after {
        query = query.filter(occurred_at.ge(value));
    }
    if let Some(value) = filters.occurred_before {
        query = query.filter(occurred_at.le(value));
    }

    for param in &query_options.filters {
        let operator = param.operator.clone();
        match param.field {
            crate::models::search::FilterField::BeforeRevision => {
                crate::revision_search!(query, param, operator, before_revision)
            }
            crate::models::search::FilterField::AfterRevision => {
                crate::revision_search!(query, param, operator, after_revision)
            }
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{}' is not searchable for events",
                    param.field
                )));
            }
        }
    }

    Ok(query)
}

fn event_response_for_visibility(
    event: Event,
    accessible_collection_ids: &HashSet<i32>,
    include_collection_less: bool,
    principal_names: &PrincipalNames,
) -> EventResponse {
    let is_directly_visible = event
        .collection_id
        .is_some_and(|id| accessible_collection_ids.contains(&id))
        || (include_collection_less && event.collection_id.is_none());
    let response = EventResponse::from_event_with_names(event, principal_names);
    if is_directly_visible {
        response
    } else {
        response.redact_indirect_audit_payloads()
    }
}

async fn apply_legacy_task_provenance(
    pool: &crate::storage::postgres::PostgresPool,
    events_to_enrich: &mut [Event],
) -> Result<(), ApiError> {
    let task_ids = events_to_enrich
        .iter()
        .filter(|event| {
            event.entity_type == EntityType::Task.as_str()
                && (event.initiator_user_id.is_none() || event.task_id.is_none())
        })
        .filter_map(|event| event.entity_id)
        .collect::<Vec<_>>();
    if task_ids.is_empty() {
        return Ok(());
    }

    let queued_initiators = load_queued_task_initiators(pool, &task_ids).await?;

    for event in events_to_enrich {
        if event.entity_type != EntityType::Task.as_str() {
            continue;
        }
        let Some(task_id) = event.entity_id else {
            continue;
        };
        event.task_id.get_or_insert(task_id);
        if event.initiator_user_id.is_none() {
            event.initiator_user_id = queued_initiators.get(&task_id).copied().flatten();
        }
    }
    Ok(())
}

pub(crate) async fn load_queued_task_initiators(
    pool: &crate::storage::postgres::PostgresPool,
    task_ids: &[i32],
) -> Result<HashMap<i32, Option<i32>>, ApiError> {
    use crate::schema::events::dsl as stored;

    if task_ids.is_empty() {
        return Ok(HashMap::new());
    }
    let queued_rows = with_connection(pool, async |conn| {
        stored::events
            .filter(stored::entity_type.eq(EntityType::Task.as_str()))
            .filter(stored::action.eq(Action::Queued.as_str()))
            .filter(stored::entity_id.eq_any(task_ids.iter().copied().map(Some)))
            .order(stored::id.asc())
            .select((
                stored::entity_id,
                stored::initiator_user_id,
                stored::actor_user_id,
            ))
            .load::<(Option<i32>, Option<i32>, Option<i32>)>(conn)
            .await
    })
    .await?;
    Ok(queued_rows
        .into_iter()
        .filter_map(|(task_id, initiator_user_id, actor_user_id)| {
            task_id.map(|task_id| (task_id, initiator_user_id.or(actor_user_id)))
        })
        .collect())
}
