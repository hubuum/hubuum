use std::fmt;

use crate::storage::postgres::prelude::*;
use chrono::{NaiveDateTime, Utc};
use uuid::Uuid;

use crate::errors::ApiError;
#[cfg(feature = "integration-test-support")]
use crate::models::EventDeliveryResponse;
use crate::models::search::{FilterField, Operator, ParsedQueryParamExt, QueryOptions, SortParam};
use crate::models::{EventDeliveryID, EventDeliveryStatus, redacted_debug_option};
use crate::pagination::{
    CursorPaginated, CursorSqlField, CursorSqlMapping, CursorSqlType, CursorValue,
};
use crate::storage::postgres::with_connection;

#[derive(Clone, Queryable, Selectable, PartialEq, Eq)]
#[diesel(table_name = crate::schema::event_deliveries)]
pub(crate) struct EventDeliveryRow {
    pub(crate) id: i64,
    pub(crate) event_id: i64,
    pub(crate) subscription_id: i32,
    pub(crate) status: String,
    pub(crate) attempts: i32,
    pub(crate) next_attempt_at: NaiveDateTime,
    pub(crate) last_error: Option<String>,
    pub(crate) locked_until: Option<NaiveDateTime>,
    pub(crate) claim_token: Option<Uuid>,
    pub(crate) created_at: NaiveDateTime,
    pub(crate) updated_at: NaiveDateTime,
}

impl fmt::Debug for EventDeliveryRow {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EventDeliveryRow")
            .field("id", &self.id)
            .field("event_id", &self.event_id)
            .field("subscription_id", &self.subscription_id)
            .field("status", &self.status)
            .field("attempts", &self.attempts)
            .field("next_attempt_at", &self.next_attempt_at)
            .field("last_error", &redacted_debug_option(&self.last_error))
            .field("locked_until", &self.locked_until)
            .field("claim_token", &redacted_debug_option(&self.claim_token))
            .field("created_at", &self.created_at)
            .field("updated_at", &self.updated_at)
            .finish()
    }
}

impl CursorPaginated for EventDeliveryRow {
    fn supports_sort(field: &FilterField) -> bool {
        matches!(
            field,
            FilterField::Id
                | FilterField::Status
                | FilterField::CreatedAt
                | FilterField::UpdatedAt
                | FilterField::NextAttemptAt
        )
    }

    fn cursor_value(&self, field: &FilterField) -> Result<CursorValue, ApiError> {
        match field {
            FilterField::Id => Ok(CursorValue::Integer(self.id)),
            FilterField::Status => Ok(CursorValue::String(self.status.clone())),
            FilterField::CreatedAt => Ok(CursorValue::DateTime(self.created_at)),
            FilterField::UpdatedAt => Ok(CursorValue::DateTime(self.updated_at)),
            FilterField::NextAttemptAt => Ok(CursorValue::DateTime(self.next_attempt_at)),
            _ => Err(ApiError::BadRequest(format!(
                "Unsupported sort field '{}' for event deliveries",
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

impl CursorSqlMapping for EventDeliveryRow {
    fn sql_field(field: &FilterField) -> Result<CursorSqlField, ApiError> {
        Ok(match field {
            FilterField::Id => CursorSqlField {
                column: "event_deliveries.id",
                sql_type: CursorSqlType::Integer,
                nullable: false,
            },
            FilterField::Status => CursorSqlField {
                column: "event_deliveries.status",
                sql_type: CursorSqlType::String,
                nullable: false,
            },
            FilterField::CreatedAt => CursorSqlField {
                column: "event_deliveries.created_at",
                sql_type: CursorSqlType::DateTime,
                nullable: false,
            },
            FilterField::UpdatedAt => CursorSqlField {
                column: "event_deliveries.updated_at",
                sql_type: CursorSqlType::DateTime,
                nullable: false,
            },
            FilterField::NextAttemptAt => CursorSqlField {
                column: "event_deliveries.next_attempt_at",
                sql_type: CursorSqlType::DateTime,
                nullable: false,
            },
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{}' is not orderable for event deliveries",
                    field
                )));
            }
        })
    }
}

pub(crate) async fn load_event_delivery(
    pool: &crate::storage::postgres::PostgresPool,
    delivery_id: EventDeliveryID,
) -> Result<EventDeliveryRow, ApiError> {
    use crate::schema::event_deliveries::dsl::{event_deliveries, id};

    with_connection(pool, async |conn| {
        event_deliveries
            .filter(id.eq(delivery_id.id()))
            .first::<EventDeliveryRow>(conn)
            .await
    })
    .await
}

#[cfg(feature = "integration-test-support")]
fn event_delivery_response(delivery: EventDeliveryRow) -> EventDeliveryResponse {
    EventDeliveryResponse {
        id: delivery.id,
        event_id: delivery.event_id,
        subscription_id: delivery.subscription_id,
        status: delivery.status,
        attempts: delivery.attempts,
        next_attempt_at: delivery.next_attempt_at,
        last_error: delivery.last_error,
        locked_until: delivery.locked_until,
        created_at: delivery.created_at,
        updated_at: delivery.updated_at,
    }
}

#[cfg(feature = "integration-test-support")]
pub(crate) async fn load_event_delivery_for_event(
    pool: &crate::storage::postgres::PostgresPool,
    event_id_value: i64,
) -> Result<EventDeliveryResponse, ApiError> {
    use crate::schema::event_deliveries::dsl::{event_deliveries, event_id};

    with_connection(pool, async |conn| {
        event_deliveries
            .filter(event_id.eq(event_id_value))
            .first::<EventDeliveryRow>(conn)
            .await
    })
    .await
    .map(event_delivery_response)
}

#[cfg(feature = "integration-test-support")]
pub(crate) async fn set_event_delivery_status_for_test(
    pool: &crate::storage::postgres::PostgresPool,
    delivery_id: i64,
    delivery_status: EventDeliveryStatus,
) -> Result<(), ApiError> {
    use crate::schema::event_deliveries::dsl::{event_deliveries, id, status};

    with_connection(pool, async |conn| {
        diesel::update(event_deliveries.filter(id.eq(delivery_id)))
            .set(status.eq(delivery_status.as_str()))
            .execute(conn)
            .await
    })
    .await?;
    Ok(())
}

#[cfg(feature = "integration-test-support")]
pub(crate) async fn set_event_delivery_claim_token_for_test(
    pool: &crate::storage::postgres::PostgresPool,
    delivery_id: i64,
    delivery_claim_token: Uuid,
) -> Result<(), ApiError> {
    use crate::schema::event_deliveries::dsl::{claim_token, event_deliveries, id};

    with_connection(pool, async |conn| {
        diesel::update(event_deliveries.filter(id.eq(delivery_id)))
            .set(claim_token.eq(Some(delivery_claim_token)))
            .execute(conn)
            .await
    })
    .await?;
    Ok(())
}

pub(crate) async fn list_event_deliveries_with_total_count(
    pool: &crate::storage::postgres::PostgresPool,
    subscription_id_filter: Option<i32>,
    query_options: &QueryOptions,
) -> Result<(Vec<EventDeliveryRow>, i64), ApiError> {
    let query = build_event_delivery_query(subscription_id_filter, query_options)?;
    let total_count = crate::pagination::exact_count_or_skipped(query_options, async || {
        with_connection(pool, async |conn| {
            query.count().get_result::<i64>(conn).await
        })
        .await
    })
    .await?;
    let mut query = build_event_delivery_query(subscription_id_filter, query_options)?;
    crate::apply_query_options!(query, query_options, EventDeliveryRow);
    let deliveries = with_connection(pool, async |conn| {
        query.load::<EventDeliveryRow>(conn).await
    })
    .await?;
    Ok((deliveries, total_count))
}

fn build_event_delivery_query(
    subscription_id_filter: Option<i32>,
    query_options: &QueryOptions,
) -> Result<crate::schema::event_deliveries::BoxedQuery<'static, diesel::pg::Pg>, ApiError> {
    use crate::schema::event_deliveries::dsl::{
        created_at, event_deliveries, id, next_attempt_at, status, subscription_id, updated_at,
    };

    let mut query = event_deliveries.into_boxed();
    if let Some(value) = subscription_id_filter {
        query = query.filter(subscription_id.eq(value));
    }
    for param in query_options.filters.clone() {
        let operator = param.operator.clone();
        match param.field {
            FilterField::Id => {
                let values = param
                    .value_as_integer()?
                    .into_iter()
                    .map(i64::from)
                    .collect::<Vec<_>>();
                let (op, negated) = operator.op_and_neg();
                match (op, negated) {
                    (Operator::Equals, false) | (Operator::In, false) => {
                        query = query.filter(id.eq_any(values))
                    }
                    (Operator::Equals, true) | (Operator::In, true) => {
                        query = query.filter(diesel::dsl::not(id.eq_any(values)))
                    }
                    _ => {
                        return Err(ApiError::OperatorMismatch(format!(
                            "Operator '{operator:?}' not implemented for field '{}' (type: bigint)",
                            param.field
                        )));
                    }
                }
            }
            FilterField::Status => crate::string_search!(query, param, operator, status),
            FilterField::CreatedAt => crate::date_search!(query, param, operator, created_at),
            FilterField::UpdatedAt => crate::date_search!(query, param, operator, updated_at),
            FilterField::NextAttemptAt => {
                crate::date_search!(query, param, operator, next_attempt_at)
            }
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{}' is not searchable for event deliveries",
                    param.field
                )));
            }
        }
    }
    Ok(query)
}

pub(crate) async fn release_event_delivery_for_retry(
    pool: &crate::storage::postgres::PostgresPool,
    delivery_id: EventDeliveryID,
) -> Result<EventDeliveryRow, ApiError> {
    use crate::schema::event_deliveries::dsl::{
        claim_token, event_deliveries, id, last_error, locked_until, next_attempt_at, status,
    };

    with_connection(
        pool,
        async |conn| -> Result<EventDeliveryRow, diesel::result::Error> {
            let delivery = diesel::update(event_deliveries.filter(id.eq(delivery_id.id())).filter(
                status.eq_any([
                    EventDeliveryStatus::Failed.as_str(),
                    EventDeliveryStatus::Dead.as_str(),
                ]),
            ))
            .set((
                status.eq(EventDeliveryStatus::Pending.as_str()),
                next_attempt_at.eq(Utc::now().naive_utc()),
                locked_until.eq::<Option<chrono::NaiveDateTime>>(None),
                claim_token.eq::<Option<Uuid>>(None),
                last_error.eq::<Option<String>>(None),
            ))
            .get_result::<EventDeliveryRow>(conn)
            .await?;
            crate::storage::postgres::notifications::notify_event_delivery(conn).await?;
            Ok(delivery)
        },
    )
    .await
}

pub(crate) async fn mark_event_delivery_dead(
    pool: &crate::storage::postgres::PostgresPool,
    delivery_id: EventDeliveryID,
) -> Result<EventDeliveryRow, ApiError> {
    use crate::schema::event_deliveries::dsl::{
        claim_token, event_deliveries, id, last_error, locked_until, status,
    };

    with_connection(pool, async |conn| {
        diesel::update(
            event_deliveries
                .filter(id.eq(delivery_id.id()))
                .filter(status.ne(EventDeliveryStatus::Succeeded.as_str())),
        )
        .set((
            status.eq(EventDeliveryStatus::Dead.as_str()),
            locked_until.eq::<Option<chrono::NaiveDateTime>>(None),
            claim_token.eq::<Option<Uuid>>(None),
            last_error.eq(Some("marked dead by operator".to_string())),
        ))
        .get_result::<EventDeliveryRow>(conn)
        .await
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_delivery_row_debug_redacts_claim_token_and_error() {
        let timestamp = chrono::DateTime::from_timestamp(1_700_000_000, 0)
            .unwrap()
            .naive_utc();
        let claim_token = Uuid::parse_str("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee").unwrap();
        let delivery = EventDeliveryRow {
            id: 1,
            event_id: 2,
            subscription_id: 3,
            status: EventDeliveryStatus::InFlight.as_str().to_string(),
            attempts: 1,
            next_attempt_at: timestamp,
            last_error: Some("delivery-error-secret".to_string()),
            locked_until: Some(timestamp),
            claim_token: Some(claim_token),
            created_at: timestamp,
            updated_at: timestamp,
        };

        let debug = format!("{delivery:?}");

        assert!(debug.contains(crate::models::REDACTED_DEBUG_VALUE));
        assert!(!debug.contains("delivery-error-secret"));
        assert!(!debug.contains(&claim_token.to_string()));
    }
}
