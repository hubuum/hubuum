use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::models::{EventSink, EventSinkKind, ResourceRevision};

/// Safe destination discovery. Configuration, URLs and secret aliases are not
/// part of the collection discovery contract.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct CollectionEventSink {
    pub id: i32,
    pub name: String,
    pub kind: EventSinkKind,
    pub enabled: bool,
    pub collection_id: Option<i32>,
    pub revision: ResourceRevision,
    pub routing: EventSinkRouting,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum EventSinkRouting {
    Fixed,
    WebhookUrl,
    EmailRecipients,
    ValkeyStream,
    Amqp,
}

impl From<EventSink> for CollectionEventSink {
    fn from(sink: EventSink) -> Self {
        let routing = match sink.kind {
            EventSinkKind::Webhook
                if sink.config.get("destination_url").is_some()
                    || sink.config.get("url_secret_ref").is_some() =>
            {
                EventSinkRouting::Fixed
            }
            EventSinkKind::Webhook => EventSinkRouting::WebhookUrl,
            EventSinkKind::Email => EventSinkRouting::EmailRecipients,
            EventSinkKind::ValkeyStream => EventSinkRouting::ValkeyStream,
            EventSinkKind::Amqp => EventSinkRouting::Amqp,
        };
        Self {
            id: sink.id,
            name: sink.name,
            kind: sink.kind,
            enabled: sink.enabled,
            collection_id: sink.collection_id,
            revision: sink.revision,
            routing,
        }
    }
}
