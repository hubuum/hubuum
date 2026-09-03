use std::collections::HashMap;

use chrono::NaiveDateTime;
use diesel::prelude::{ExpressionMethods, QueryDsl};
use diesel::{Queryable, Selectable};
use diesel_async::RunQueryDsl;
use hubuum_domain::{CollectionId, PrincipalId, TaskId};
use hubuum_events_core::{
    Action, ActorKind, EntityType, EventEntityId, EventEnvelope, EventSequence, Provenance,
    ProvenanceActor, ProvenancePrincipal, TraceLink,
};
use hubuum_storage_core::StorageAuditEvent;
use serde_json::Value;
use uuid::Uuid;

use crate::{PostgresConnection, PostgresRevision, PostgresStorageError};

fn invalid_event_envelope(error: impl std::fmt::Debug) -> PostgresStorageError {
    PostgresStorageError::invalid_persisted_value("event envelope", error)
}

/// Adapter-private stored event projection shared by audit and delivery reads.
#[derive(Clone, Queryable, Selectable)]
#[diesel(table_name = crate::schema::events)]
pub(super) struct StoredEventProjection {
    pub(super) id: i64,
    event_id: Uuid,
    pub(super) occurred_at: NaiveDateTime,
    pub(super) entity_type: String,
    pub(super) entity_id: Option<i32>,
    entity_name: Option<String>,
    pub(super) collection_id: Option<i32>,
    action: String,
    pub(super) actor_user_id: Option<i32>,
    actor_kind: String,
    request_id: Option<Uuid>,
    correlation_id: Option<String>,
    summary: String,
    before: Option<Value>,
    after: Option<Value>,
    metadata: Value,
    schema_version: i32,
    pub(super) initiator_user_id: Option<i32>,
    task_id: Option<i32>,
    before_revision: Option<PostgresRevision>,
    after_revision: Option<PostgresRevision>,
    trace_id: Option<String>,
    trace_span_id: Option<String>,
    trace_flags: Option<i16>,
    trace_context_version: Option<i16>,
}

impl StoredEventProjection {
    fn apply_legacy_task_provenance(&mut self, queued_initiators: &HashMap<i32, Option<i32>>) {
        if self.entity_type != EntityType::Task.as_str() {
            return;
        }
        let Some(task_id) = self.entity_id else {
            return;
        };
        self.task_id.get_or_insert(task_id);
        if self.initiator_user_id.is_none() {
            self.initiator_user_id = queued_initiators.get(&task_id).copied().flatten();
        }
    }

    pub(super) fn into_envelope(
        self,
        principal_names: &HashMap<i32, String>,
    ) -> Result<EventEnvelope, PostgresStorageError> {
        let entity_type = EntityType::parse(&self.entity_type).map_err(invalid_event_envelope)?;
        let action = Action::parse(&self.action).map_err(invalid_event_envelope)?;
        let actor_kind = ActorKind::parse(&self.actor_kind).map_err(invalid_event_envelope)?;
        let actor_user_id = self
            .actor_user_id
            .map(PrincipalId::new)
            .transpose()
            .map_err(invalid_event_envelope)?;
        let initiator_user_id = self
            .initiator_user_id
            .map(PrincipalId::new)
            .transpose()
            .map_err(invalid_event_envelope)?;
        let principal = |principal_id: PrincipalId| ProvenancePrincipal {
            principal_id,
            name: principal_names.get(&principal_id.id()).cloned(),
        };
        let provenance = Provenance {
            actor: ProvenanceActor {
                kind: Some(actor_kind.as_str().to_string()),
                principal: actor_user_id.map(principal),
            },
            initiator: initiator_user_id.map(principal),
            task_id: self
                .task_id
                .map(TaskId::new)
                .transpose()
                .map_err(invalid_event_envelope)?,
        };
        let trace_link = trace_link_from_columns(
            self.trace_id,
            self.trace_span_id,
            self.trace_flags,
            self.trace_context_version,
        )?;
        EventEnvelope::builder()
            .id(EventSequence::new(self.id).map_err(invalid_event_envelope)?)
            .event_id(self.event_id)
            .occurred_at(self.occurred_at.and_utc())
            .entity_type(entity_type)
            .entity_id(
                self.entity_id
                    .map(EventEntityId::new)
                    .transpose()
                    .map_err(invalid_event_envelope)?,
            )
            .entity_name(self.entity_name)
            .collection_id(
                self.collection_id
                    .map(CollectionId::new)
                    .transpose()
                    .map_err(invalid_event_envelope)?,
            )
            .action(action)
            .actor_user_id(actor_user_id)
            .actor_kind(actor_kind)
            .provenance(provenance)
            .request_id(self.request_id)
            .correlation_id(self.correlation_id)
            .trace_link(trace_link)
            .summary(self.summary)
            .before(self.before)
            .after(self.after)
            .metadata(self.metadata)
            .schema_version(self.schema_version)
            .try_build()
            .map_err(invalid_event_envelope)
    }

    pub(super) fn into_audit_event(
        self,
        principal_names: &HashMap<i32, String>,
        redact_payloads: bool,
    ) -> Result<StorageAuditEvent, PostgresStorageError> {
        let before_revision = self.before_revision.map(PostgresRevision::into_domain);
        let after_revision = self.after_revision.map(PostgresRevision::into_domain);
        let envelope = self.into_envelope(principal_names)?;
        let envelope = if redact_payloads {
            envelope.without_payloads()
        } else {
            envelope
        };
        Ok(StorageAuditEvent::new(
            envelope,
            before_revision,
            after_revision,
        ))
    }
}

pub(crate) fn trace_link_from_columns(
    trace_id: Option<String>,
    span_id: Option<String>,
    trace_flags: Option<i16>,
    version: Option<i16>,
) -> Result<Option<TraceLink>, PostgresStorageError> {
    match (trace_id, span_id, trace_flags, version) {
        (None, None, None, None) => Ok(None),
        (Some(trace_id), Some(span_id), Some(trace_flags), Some(version)) => {
            let trace_flags = u8::try_from(trace_flags).map_err(invalid_event_envelope)?;
            let version = u8::try_from(version).map_err(invalid_event_envelope)?;
            TraceLink::new(trace_id, span_id, trace_flags, version)
                .map(Some)
                .map_err(invalid_event_envelope)
        }
        _ => Err(PostgresStorageError::database(
            "Persisted trace link must contain either all fields or no fields",
        )),
    }
}

pub(super) async fn enrich_stored_events(
    connection: &mut PostgresConnection,
    events: &mut [StoredEventProjection],
) -> Result<HashMap<i32, String>, PostgresStorageError> {
    let task_ids = events
        .iter()
        .filter(|event| {
            event.entity_type == EntityType::Task.as_str()
                && (event.initiator_user_id.is_none() || event.task_id.is_none())
        })
        .filter_map(|event| event.entity_id)
        .collect::<Vec<_>>();
    let queued_initiators = load_queued_task_initiators(connection, &task_ids).await?;
    for event in events.iter_mut() {
        event.apply_legacy_task_provenance(&queued_initiators);
    }

    let principal_ids = events
        .iter()
        .flat_map(|event| [event.actor_user_id, event.initiator_user_id])
        .flatten()
        .collect::<Vec<_>>();
    load_principal_names(connection, principal_ids).await
}

async fn load_queued_task_initiators(
    connection: &mut PostgresConnection,
    task_ids: &[i32],
) -> Result<HashMap<i32, Option<i32>>, PostgresStorageError> {
    use crate::schema::events::dsl as stored;

    if task_ids.is_empty() {
        return Ok(HashMap::new());
    }
    Ok(stored::events
        .filter(stored::entity_type.eq(EntityType::Task.as_str()))
        .filter(stored::action.eq(Action::Queued.as_str()))
        .filter(stored::entity_id.eq_any(task_ids.iter().copied().map(Some)))
        .order(stored::id.asc())
        .select((
            stored::entity_id,
            stored::initiator_user_id,
            stored::actor_user_id,
        ))
        .load::<(Option<i32>, Option<i32>, Option<i32>)>(connection)
        .await?
        .into_iter()
        .filter_map(|(task_id, initiator_user_id, actor_user_id)| {
            task_id.map(|task_id| (task_id, initiator_user_id.or(actor_user_id)))
        })
        .collect())
}

async fn load_principal_names(
    connection: &mut PostgresConnection,
    mut principal_ids: Vec<i32>,
) -> Result<HashMap<i32, String>, PostgresStorageError> {
    use crate::schema::principals::dsl::{id, name, principals};

    principal_ids.sort_unstable();
    principal_ids.dedup();
    if principal_ids.is_empty() {
        return Ok(HashMap::new());
    }
    Ok(principals
        .filter(id.eq_any(principal_ids))
        .select((id, name))
        .load::<(i32, String)>(connection)
        .await?
        .into_iter()
        .collect())
}

#[cfg(test)]
mod tests {
    use hubuum_storage_core::StorageErrorKind;

    use super::*;

    fn projection() -> StoredEventProjection {
        StoredEventProjection {
            id: 1,
            event_id: Uuid::new_v4(),
            occurred_at: chrono::Utc::now().naive_utc(),
            entity_type: EntityType::Collection.as_str().to_string(),
            entity_id: Some(1),
            entity_name: Some("collection".to_string()),
            collection_id: Some(1),
            action: Action::Created.as_str().to_string(),
            actor_user_id: None,
            actor_kind: ActorKind::System.as_str().to_string(),
            request_id: None,
            correlation_id: None,
            summary: "created collection".to_string(),
            before: None,
            after: Some(serde_json::json!({"name": "collection"})),
            metadata: serde_json::json!({}),
            schema_version: 1,
            initiator_user_id: None,
            task_id: None,
            before_revision: None,
            after_revision: None,
            trace_id: None,
            trace_span_id: None,
            trace_flags: None,
            trace_context_version: None,
        }
    }

    #[test]
    fn corrupt_persisted_event_envelopes_are_backend_failures() {
        let mut projection = projection();
        projection.action = Action::Updated.as_str().to_string();
        projection.entity_type = EntityType::ObjectRelation.as_str().to_string();

        let error = projection
            .into_envelope(&HashMap::new())
            .expect_err("invalid catalog pair must fail decoding");

        assert_eq!(error.kind(), StorageErrorKind::Backend);
    }

    #[test]
    fn persisted_event_trace_link_is_revalidated_and_hidden_from_json() {
        let mut projection = projection();
        projection.trace_id = Some("4bf92f3577b34da6a3ce929d0e0e4736".to_string());
        projection.trace_span_id = Some("00f067aa0ba902b7".to_string());
        projection.trace_flags = Some(1);
        projection.trace_context_version = Some(0);

        let envelope = projection.into_envelope(&HashMap::new()).unwrap();
        assert_eq!(
            envelope.trace_link().map(TraceLink::trace_id),
            Some("4bf92f3577b34da6a3ce929d0e0e4736")
        );
        assert!(
            !serde_json::to_string(&envelope)
                .unwrap()
                .contains("4bf92f3577b34da6a3ce929d0e0e4736")
        );
    }

    #[test]
    fn partial_persisted_event_trace_link_is_backend_corruption() {
        let error = trace_link_from_columns(
            Some("4bf92f3577b34da6a3ce929d0e0e4736".to_string()),
            None,
            None,
            None,
        )
        .unwrap_err();

        assert_eq!(error.kind(), StorageErrorKind::Backend);
    }
}
