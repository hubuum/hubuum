//! Explicit provenance for event emission (#72).
//!
//! Request middleware stores [`RequestProvenance`] in Actix extensions. Handlers
//! combine that request-scoped data with the authenticated actor and pass the
//! resulting [`EventContext`] down to mutation code, where `emit_event` can run
//! inside the same database transaction as the domain write.

use actix_web::{HttpMessage, HttpRequest};
use serde::{Deserialize, Serialize};
use std::net::IpAddr;
use uuid::Uuid;

pub use hubuum_events_core::EventContext;

/// Request-scoped provenance extracted by middleware.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequestProvenance {
    request_id: Uuid,
    correlation_id: Option<String>,
    client_ip: Option<IpAddr>,
}

impl RequestProvenance {
    pub fn new(request_id: Uuid, correlation_id: Option<String>) -> Self {
        Self::new_with_client_ip(request_id, correlation_id, None)
    }

    pub fn new_with_client_ip(
        request_id: Uuid,
        correlation_id: Option<String>,
        client_ip: Option<IpAddr>,
    ) -> Self {
        Self {
            request_id,
            correlation_id,
            client_ip,
        }
    }

    pub fn request_id(&self) -> Uuid {
        self.request_id
    }

    pub fn correlation_id(&self) -> Option<&str> {
        self.correlation_id.as_deref()
    }

    pub fn client_ip(&self) -> Option<IpAddr> {
        self.client_ip
    }

    pub fn user_event_context(&self, actor_user_id: i32) -> EventContext {
        EventContext::user(
            actor_user_id,
            Some(self.request_id),
            self.correlation_id.clone(),
        )
    }

    pub fn worker_event_context(&self) -> EventContext {
        EventContext::worker(Some(self.request_id), self.correlation_id.clone())
    }

    /// Read provenance previously inserted by [`crate::middlewares::TracingMiddleware`].
    pub fn from_request(req: &HttpRequest) -> Option<Self> {
        req.extensions().get::<Self>().cloned()
    }
}
