#[cfg(any(test, feature = "integration-test-support"))]
use std::fmt;

#[cfg(any(test, feature = "integration-test-support"))]
use crate::storage::postgres::prelude::*;
#[cfg(any(test, feature = "integration-test-support"))]
use chrono::NaiveDateTime;
#[cfg(any(test, feature = "integration-test-support"))]
use uuid::Uuid;

#[cfg(feature = "integration-test-support")]
use crate::errors::ApiError;
#[cfg(feature = "integration-test-support")]
use crate::models::EventDeliveryResponse;
#[cfg(any(test, feature = "integration-test-support"))]
use crate::models::EventDeliveryStatus;
#[cfg(any(test, feature = "integration-test-support"))]
use crate::models::redacted_debug_option;
#[cfg(feature = "integration-test-support")]
use crate::storage::postgres::with_connection;

#[cfg(any(test, feature = "integration-test-support"))]
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

#[cfg(any(test, feature = "integration-test-support"))]
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
