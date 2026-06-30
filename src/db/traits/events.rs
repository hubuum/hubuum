use diesel::dsl::sql;
use diesel::prelude::*;
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
    pub namespace_id: Option<i32>,
    pub occurred_after: Option<chrono::NaiveDateTime>,
    pub occurred_before: Option<chrono::NaiveDateTime>,
}

pub async fn list_events_with_total_count(
    pool: &DbPool,
    accessible_namespace_ids: &[i32],
    include_namespace_less: bool,
    filters: &EventListFilters,
    query_options: &QueryOptions,
) -> Result<(Vec<Event>, i64), ApiError> {
    let query = build_event_query(accessible_namespace_ids, include_namespace_less, filters)?;
    let total_count = with_connection(pool, |conn| query.count().get_result::<i64>(conn))?;

    let mut query = build_event_query(accessible_namespace_ids, include_namespace_less, filters)?;
    apply_query_options!(query, query_options, EventResponse);
    let rows = with_connection(pool, |conn| query.load::<Event>(conn))?;

    Ok((rows, total_count))
}

fn build_event_query<'a>(
    accessible_namespace_ids: &'a [i32],
    include_namespace_less: bool,
    filters: &EventListFilters,
) -> Result<crate::schema::events::BoxedQuery<'a, diesel::pg::Pg>, ApiError> {
    use crate::schema::events::dsl::{
        action, actor_kind, actor_user_id, entity_id, entity_type, events, namespace_id,
        occurred_at,
    };

    let mut query = events.into_boxed();

    if !include_namespace_less && accessible_namespace_ids.is_empty() {
        return Ok(query.filter(sql::<Bool>("FALSE")));
    }

    if include_namespace_less {
        if !accessible_namespace_ids.is_empty() {
            query = query.filter(
                namespace_id
                    .eq_any(accessible_namespace_ids)
                    .or(namespace_id.is_null())
                    .or(sql::<Bool>(&related_namespace_filter_sql(
                        accessible_namespace_ids,
                    ))),
            );
        }
    } else {
        query = query.filter(
            namespace_id
                .eq_any(accessible_namespace_ids)
                .or(sql::<Bool>(&related_namespace_filter_sql(
                    accessible_namespace_ids,
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
    if let Some(value) = filters.namespace_id {
        query = query.filter(namespace_id.eq(Some(value)));
    }
    if let Some(value) = filters.occurred_after {
        query = query.filter(occurred_at.ge(value));
    }
    if let Some(value) = filters.occurred_before {
        query = query.filter(occurred_at.le(value));
    }

    Ok(query)
}

fn related_namespace_filter_sql(accessible_namespace_ids: &[i32]) -> String {
    if accessible_namespace_ids.is_empty() {
        return "FALSE".to_string();
    }

    let ids = accessible_namespace_ids
        .iter()
        .map(i32::to_string)
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "EXISTS (
            SELECT 1
            FROM jsonb_array_elements_text(events.metadata->'related_namespace_ids') AS related(namespace_id)
            WHERE related.namespace_id::integer IN ({ids})
        )"
    )
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
        namespace_id: parse_optional_i32_filter(passthrough, "namespace_id")?,
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
