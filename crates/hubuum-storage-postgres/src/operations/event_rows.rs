use std::collections::HashMap;

use chrono::NaiveDateTime;
use diesel::prelude::{ExpressionMethods, QueryDsl};
use diesel::{Queryable, Selectable};
use diesel_async::RunQueryDsl;
use hubuum_events_core::{
    Action, EntityType, EventEnvelope, Provenance, ProvenanceActor, ProvenancePrincipal,
};
use hubuum_storage_core::StorageAuditEvent;
use serde_json::Value;
use uuid::Uuid;

use crate::{PostgresConnection, PostgresRevision, PostgresStorageError};

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

    pub(super) fn into_envelope(self, principal_names: &HashMap<i32, String>) -> EventEnvelope {
        let principal = |principal_id| ProvenancePrincipal {
            principal_id,
            name: principal_names.get(&principal_id).cloned(),
        };
        let provenance = Provenance {
            actor: ProvenanceActor {
                kind: Some(self.actor_kind.clone()),
                principal: self.actor_user_id.map(principal),
            },
            initiator: self.initiator_user_id.map(principal),
            task_id: self.task_id,
        };
        EventEnvelope {
            id: self.id,
            event_id: self.event_id,
            occurred_at: self.occurred_at,
            entity_type: self.entity_type,
            entity_id: self.entity_id,
            entity_name: self.entity_name,
            collection_id: self.collection_id,
            action: self.action,
            actor_user_id: self.actor_user_id,
            actor_kind: self.actor_kind,
            provenance,
            request_id: self.request_id,
            correlation_id: self.correlation_id,
            summary: self.summary,
            before: self.before,
            after: self.after,
            metadata: self.metadata,
            schema_version: self.schema_version,
        }
    }

    pub(super) fn into_audit_event(
        self,
        principal_names: &HashMap<i32, String>,
        redact_payloads: bool,
    ) -> StorageAuditEvent {
        let before_revision = self.before_revision.map(PostgresRevision::get);
        let after_revision = self.after_revision.map(PostgresRevision::get);
        let mut envelope = self.into_envelope(principal_names);
        if redact_payloads {
            envelope.before = None;
            envelope.after = None;
        }
        StorageAuditEvent::new(envelope, before_revision, after_revision)
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
