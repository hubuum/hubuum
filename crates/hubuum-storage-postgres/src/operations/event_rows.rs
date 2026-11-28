use std::collections::HashMap;

use chrono::NaiveDateTime;
use diesel::prelude::{ExpressionMethods, QueryDsl};
use diesel::{Queryable, Selectable};
use diesel_async::RunQueryDsl;
use hubuum_domain::{CollectionId, PrincipalId, PrincipalIdentity, TaskId};
use hubuum_events_core::{
    Action, ActorKind, CorrelationId, EntityType, EventEntityId, EventEnvelope, EventSequence,
    Provenance, ProvenanceActor, ProvenancePrincipal, TraceLink,
};
use hubuum_storage_core::{StorageAuditEvent, StorageIdentityEvidence};
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
    logical_identity: Option<Value>,
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
    pub(super) fn into_envelope(
        self,
        principal_names: &HashMap<PrincipalIdentity, String>,
    ) -> Result<EventEnvelope, PostgresStorageError> {
        self.into_captured_envelope(principal_names)
            .map(|(envelope, _)| envelope)
    }

    fn into_captured_envelope(
        self,
        principal_names: &HashMap<PrincipalIdentity, String>,
    ) -> Result<(EventEnvelope, StorageIdentityEvidence), PostgresStorageError> {
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
        let evidence = super::history::history_identity(self.logical_identity)?;
        let principal = |principal_id: PrincipalId, path: &str| ProvenancePrincipal {
            principal_id,
            name: evidence
                .principal_reference(path)
                .and_then(|identity| principal_names.get(&identity).cloned()),
        };
        let provenance = Provenance {
            actor: ProvenanceActor {
                kind: Some(actor_kind.as_str().to_string()),
                principal: actor_user_id.map(|id| principal(id, "/actor_principal_id")),
            },
            initiator: initiator_user_id.map(|id| principal(id, "/initiator_principal_id")),
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
            .correlation_id(
                self.correlation_id
                    .map(CorrelationId::new)
                    .transpose()
                    .map_err(invalid_event_envelope)?,
            )
            .trace_link(trace_link)
            .summary(self.summary)
            .before(self.before)
            .after(self.after)
            .metadata(self.metadata)
            .schema_version(self.schema_version)
            .try_build()
            .map_err(invalid_event_envelope)
            .map(|envelope| (envelope, evidence))
    }

    pub(super) fn into_audit_event(
        self,
        principal_names: &HashMap<PrincipalIdentity, String>,
        redact_payloads: bool,
    ) -> Result<StorageAuditEvent, PostgresStorageError> {
        let before_revision = self.before_revision.map(PostgresRevision::into_domain);
        let after_revision = self.after_revision.map(PostgresRevision::into_domain);
        let (envelope, evidence) = self.into_captured_envelope(principal_names)?;
        let envelope = if redact_payloads {
            envelope.without_payloads()
        } else {
            envelope
        };
        Ok(
            StorageAuditEvent::new(envelope, before_revision, after_revision)
                .with_logical_identity(evidence),
        )
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
) -> Result<HashMap<PrincipalIdentity, String>, PostgresStorageError> {
    let mut principal_ids = Vec::new();
    for event in events {
        let evidence = super::history::history_identity(event.logical_identity.clone())?;
        principal_ids.extend(
            ["/actor_principal_id", "/initiator_principal_id"]
                .into_iter()
                .filter_map(|path| evidence.principal_reference(path)),
        );
    }
    principal_ids.sort_unstable();
    principal_ids.dedup();
    if principal_ids.is_empty() {
        return Ok(HashMap::new());
    }
    use crate::schema::principals::dsl::{name as principal_name, principals};
    use diesel::dsl::sql;
    use diesel::sql_types::Text;
    let identity = sql::<Text>("logical_identity #>> '{subject,identity,identity}'");
    principals
        .filter(
            identity
                .clone()
                .eq_any(principal_ids.into_iter().map(|id| id.to_string())),
        )
        .select((identity, principal_name))
        .load::<(String, String)>(connection)
        .await?
        .into_iter()
        .map(|(identity, name)| Ok((identity.parse().map_err(invalid_event_envelope)?, name)))
        .collect()
}

#[cfg(test)]
mod tests {
    use hubuum_storage_core::StorageErrorKind;

    use super::*;

    fn projection() -> StoredEventProjection {
        StoredEventProjection {
            logical_identity: Some(serde_json::json!({"subject": null, "references": {}})),
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
            schema_version: hubuum_events_core::CURRENT_EVENT_SCHEMA_VERSION,
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
