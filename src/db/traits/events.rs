use crate::db::prelude::*;
use diesel::dsl::sql;
use diesel::sql_types::Bool;

use crate::apply_query_options;
use crate::db::{DbPool, with_connection};
use crate::errors::ApiError;
use crate::events::{Action, ActorKind, EntityType, Event, EventResponse};
use crate::models::search::QueryOptions;
use crate::utilities::extensions::CustomStringExtensions;

#[derive(Debug, Clone, Default)]
pub struct EventListFilters {
    pub entity_type: Option<EntityType>,
    pub entity_id: Option<i32>,
    pub action: Option<Action>,
    pub actor_kind: Option<ActorKind>,
    pub actor_user_id: Option<i32>,
    pub collection_id: Option<i32>,
    pub occurred_after: Option<chrono::NaiveDateTime>,
    pub occurred_before: Option<chrono::NaiveDateTime>,
}

pub async fn list_events_with_total_count(
    pool: &DbPool,
    accessible_collection_ids: &[i32],
    include_collection_less: bool,
    filters: &EventListFilters,
    query_options: &QueryOptions,
) -> Result<(Vec<EventResponse>, i64), ApiError> {
    crate::logger::log_operation_read(filters.entity_type, filters.action, filters.entity_id);

    let query = build_event_query(accessible_collection_ids, include_collection_less, filters)?;
    let total_count = crate::pagination::exact_count_or_skipped(query_options, async || {
        with_connection(pool, async |conn| {
            query.count().get_result::<i64>(conn).await
        })
        .await
    })
    .await?;

    let mut query = build_event_query(accessible_collection_ids, include_collection_less, filters)?;
    apply_query_options!(query, query_options, EventResponse);
    let rows = with_connection(pool, async |conn| query.load::<Event>(conn).await).await?;
    let rows = rows
        .into_iter()
        .map(|event| {
            event_response_for_visibility(event, accessible_collection_ids, include_collection_less)
        })
        .collect();

    Ok((rows, total_count))
}

fn build_event_query<'a>(
    accessible_collection_ids: &'a [i32],
    include_collection_less: bool,
    filters: &EventListFilters,
) -> Result<crate::schema::events::BoxedQuery<'a, diesel::pg::Pg>, ApiError> {
    use crate::schema::events::dsl::{
        action, actor_kind, actor_user_id, collection_id, entity_id, entity_type, events,
        occurred_at,
    };

    let mut query = events.into_boxed();

    if !include_collection_less && accessible_collection_ids.is_empty() {
        return Ok(query.filter(sql::<Bool>("FALSE")));
    }

    if include_collection_less {
        if !accessible_collection_ids.is_empty() {
            query = query.filter(
                collection_id
                    .eq_any(accessible_collection_ids)
                    .or(collection_id.is_null())
                    .or(sql::<Bool>(&related_collection_filter_sql(
                        accessible_collection_ids,
                    ))),
            );
        }
    } else {
        query = query.filter(
            collection_id
                .eq_any(accessible_collection_ids)
                .or(sql::<Bool>(&related_collection_filter_sql(
                    accessible_collection_ids,
                ))),
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
    if let Some(value) = filters.collection_id {
        query = query.filter(collection_id.eq(Some(value)));
    }
    if let Some(value) = filters.occurred_after {
        query = query.filter(occurred_at.ge(value));
    }
    if let Some(value) = filters.occurred_before {
        query = query.filter(occurred_at.le(value));
    }

    Ok(query)
}

fn related_collection_filter_sql(accessible_collection_ids: &[i32]) -> String {
    if accessible_collection_ids.is_empty() {
        return "FALSE".to_string();
    }

    accessible_collection_ids
        .iter()
        .map(|id| {
            let numeric_probe = serde_json::json!({ "related_collection_ids": [id] });
            let string_probe = serde_json::json!({ "related_collection_ids": [id.to_string()] });
            format!(
                "events.metadata @> '{}'::jsonb OR events.metadata @> '{}'::jsonb",
                numeric_probe, string_probe
            )
        })
        .collect::<Vec<_>>()
        .join(" OR ")
}

fn event_response_for_visibility(
    event: Event,
    accessible_collection_ids: &[i32],
    include_collection_less: bool,
) -> EventResponse {
    let is_directly_visible = event
        .collection_id
        .is_some_and(|id| accessible_collection_ids.contains(&id))
        || (include_collection_less && event.collection_id.is_none());
    let response = EventResponse::from(event);
    if is_directly_visible {
        response
    } else {
        response.redact_indirect_audit_payloads()
    }
}

pub fn parse_event_filters(
    passthrough: &mut std::collections::HashMap<String, Vec<String>>,
) -> Result<EventListFilters, ApiError> {
    Ok(EventListFilters {
        entity_type: parse_optional_catalog_filter(
            passthrough,
            "entity_type",
            EntityType::from_db,
        )?,
        entity_id: parse_optional_i32_filter(passthrough, "entity_id")?,
        action: parse_optional_catalog_filter(passthrough, "action", Action::from_db)?,
        actor_kind: parse_optional_catalog_filter(passthrough, "actor_kind", ActorKind::from_db)?,
        actor_user_id: parse_optional_i32_filter(passthrough, "actor_user_id")?,
        collection_id: parse_optional_i32_filter(passthrough, "collection_id")?,
        occurred_after: parse_optional_date_filter(passthrough, "occurred_after")?,
        occurred_before: parse_optional_date_filter(passthrough, "occurred_before")?,
    })
}

fn take_single(
    passthrough: &mut std::collections::HashMap<String, Vec<String>>,
    key: &str,
) -> Result<Option<String>, ApiError> {
    match passthrough.remove(key) {
        Some(values) if values.len() > 1 => Err(ApiError::BadRequest(format!("duplicate {key}"))),
        Some(mut values) => Ok(values.pop()),
        None => Ok(None),
    }
}

fn parse_optional_i32_filter(
    passthrough: &mut std::collections::HashMap<String, Vec<String>>,
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
    passthrough: &mut std::collections::HashMap<String, Vec<String>>,
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
    passthrough: &mut std::collections::HashMap<String, Vec<String>>,
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
