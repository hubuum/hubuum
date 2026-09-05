//! Explicit provenance for event emission (#72).
//!
//! Request middleware stores [`RequestProvenance`] in Actix extensions. Handlers
//! combine that request-scoped data with the authenticated actor and pass the
//! resulting [`EventContext`] down to mutation code, where the active storage
//! adapter can append an event
//! inside the same database transaction as the domain write.

use actix_web::{HttpMessage, HttpRequest};
use hubuum_domain::PrincipalId;
use hubuum_events_core::{CorrelationId, TraceLink};
use serde::{Deserialize, Serialize};
use std::net::IpAddr;
use uuid::Uuid;

pub use hubuum_events_core::EventContext;

/// Request-scoped provenance extracted by middleware.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequestProvenance {
    request_id: Uuid,
    correlation_id: Option<CorrelationId>,
    client_ip: Option<IpAddr>,
    #[serde(skip)]
    trace_link: Option<TraceLink>,
}

impl RequestProvenance {
    pub fn new(request_id: Uuid, correlation_id: Option<CorrelationId>) -> Self {
        Self::new_with_client_ip(request_id, correlation_id, None)
    }

    pub fn new_with_client_ip(
        request_id: Uuid,
        correlation_id: Option<CorrelationId>,
        client_ip: Option<IpAddr>,
    ) -> Self {
        Self {
            request_id,
            correlation_id,
            client_ip,
            trace_link: None,
        }
    }

    pub fn request_id(&self) -> Uuid {
        self.request_id
    }

    pub fn correlation_id(&self) -> Option<&str> {
        self.correlation_id.as_ref().map(CorrelationId::as_str)
    }

    pub fn client_ip(&self) -> Option<IpAddr> {
        self.client_ip
    }

    pub(crate) fn trace_link(&self) -> Option<&TraceLink> {
        self.trace_link.as_ref()
    }

    #[must_use]
    pub fn with_trace_link(mut self, trace_link: Option<TraceLink>) -> Self {
        self.trace_link = trace_link;
        self
    }

    pub fn user_event_context(&self, actor_user_id: i32) -> EventContext {
        EventContext::user(
            PrincipalId::new(actor_user_id).expect("authenticated principal id must be positive"),
            Some(self.request_id),
            self.correlation_id.clone(),
        )
        .with_trace_link(self.trace_link.clone())
    }

    /// Read provenance previously inserted by [`crate::middlewares::TracingMiddleware`].
    pub fn from_request(req: &HttpRequest) -> Option<Self> {
        req.extensions().get::<Self>().cloned()
    }
}
