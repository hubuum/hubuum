use diesel::{BoolExpressionMethods, ExpressionMethods, OptionalExtension, QueryDsl};
use diesel_async::RunQueryDsl;
use hubuum_domain::{CollectionId, EventSinkId};
use hubuum_events_core::{
    Action, AuditDocument, EntityType, EventEntityId, EventSubscriptionScope, NewEvent,
};
use hubuum_storage_core::{
    EventSinkGrantAction, StorageEventSinkGrantChange, StorageMutationOutcome,
};
use serde_json::json;

use super::event_record::append_event;
use crate::schema::{event_sink_collection_grants as grants, event_sinks};
use crate::{PostgresConnection, PostgresRuntime, PostgresStorageError};

/// Serialize scope changes and delivery admission on the sink row. Call before
/// locking subscriptions or deliveries to preserve the common lock order.
pub(super) async fn authorize_sink_on_connection(
    connection: &mut PostgresConnection,
    scope: EventSubscriptionScope,
    sink_id: EventSinkId,
) -> Result<(), PostgresStorageError> {
    let owner = event_sinks::table
        .find(sink_id.id())
        .select(event_sinks::collection_id)
        .for_no_key_update()
        .first::<Option<i32>>(connection)
        .await?;
    let allowed = match (scope.collection_id(), owner) {
        (None, None) => true,
        (Some(collection), Some(owner)) => collection.id() == owner,
        (Some(collection), None) => grants::table
            .filter(
                grants::sink_id
                    .eq(sink_id.id())
                    .and(grants::collection_id.eq(collection.id())),
            )
            .select(grants::sink_id)
            .first::<i32>(connection)
            .await
            .optional()?
            .is_some(),
        (None, Some(_)) => false,
    };
    if !allowed {
        return Err(PostgresStorageError::permission_denied(
            "Sink is not available to this collection",
        ));
    }
    Ok(())
}

pub async fn list_event_sink_collections(
    runtime: &PostgresRuntime,
    sink_id: EventSinkId,
) -> Result<Vec<CollectionId>, PostgresStorageError> {
    runtime
        .with_connection(async |connection| {
            event_sinks::table
                .find(sink_id.id())
                .select(event_sinks::id)
                .first::<i32>(connection)
                .await?;
            grants::table
                .filter(grants::sink_id.eq(sink_id.id()))
                .select(grants::collection_id)
                .order(grants::collection_id.asc())
                .load::<i32>(connection)
                .await?
                .into_iter()
                .map(|id| CollectionId::new(id).map_err(PostgresStorageError::from))
                .collect()
        })
        .await
}

pub async fn change_event_sink_grant(
    runtime: &PostgresRuntime,
    request: StorageEventSinkGrantChange,
) -> Result<StorageMutationOutcome<()>, PostgresStorageError> {
    runtime.with_transaction(async |connection| {
        let owner = event_sinks::table.find(request.sink_id().id()).select(event_sinks::collection_id)
            .for_no_key_update().first::<Option<i32>>(connection).await?;
        if owner.is_some() {
            return Err(PostgresStorageError::invalid_input("Collection-owned sinks cannot be shared with other collections"));
        }
        let changed = match request.action() {
            EventSinkGrantAction::Grant => diesel::insert_into(grants::table).values((
                grants::sink_id.eq(request.sink_id().id()), grants::collection_id.eq(request.collection_id().id()),
            )).on_conflict_do_nothing().execute(connection).await?,
            EventSinkGrantAction::Revoke => diesel::delete(grants::table.filter(grants::sink_id.eq(request.sink_id().id()))
                .filter(grants::collection_id.eq(request.collection_id().id()))).execute(connection).await?,
        };
        if changed == 0 { return Ok(StorageMutationOutcome::unchanged(())); }
        let document = AuditDocument::try_new("Event sink collection grant changed", None, None, json!({
            "sink_id": request.sink_id().id(), "collection_id": request.collection_id().id(),
            "granted": request.action() == EventSinkGrantAction::Grant,
        }))?;
        let event = NewEvent::from_document(EntityType::EventSink, Action::Updated, request.event_context().actor_kind(), document)
            .map_err(|error| PostgresStorageError::invalid_input(error.to_string()))?
            .with_context(request.event_context()).with_entity_id(EventEntityId::new(request.sink_id().id())?)
            .with_collection_id(request.collection_id());
        let receipt = append_event(connection, &event).await?.into_audit_receipt();
        Ok(StorageMutationOutcome::committed((), receipt))
    }).await
}
