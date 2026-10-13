use hubuum_domain::{PrincipalId, PrincipalKind};
use serde::{Deserialize, Serialize};

use crate::{AuditDocument, EventEnvelopeError};

/// Immutable event-time identity and authorization snapshot in `metadata.subject`.
/// This is the affected principal, independently of the operation's actor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "EventSubjectWire")]
#[cfg_attr(feature = "schema", derive(utoipa::ToSchema))]
pub struct EventSubject {
    principal_id: PrincipalId,
    name: String,
    kind: PrincipalKind,
    identity_scope: String,
    authentication_provider_kind: String,
    is_admin: bool,
}

#[derive(Deserialize)]
struct EventSubjectWire {
    principal_id: PrincipalId,
    name: String,
    kind: PrincipalKind,
    identity_scope: String,
    authentication_provider_kind: String,
    is_admin: bool,
}

impl TryFrom<EventSubjectWire> for EventSubject {
    type Error = EventEnvelopeError;

    fn try_from(value: EventSubjectWire) -> Result<Self, Self::Error> {
        Self::try_new(
            value.principal_id,
            value.name,
            value.kind,
            value.identity_scope,
            value.authentication_provider_kind,
            value.is_admin,
        )
    }
}

impl EventSubject {
    pub fn try_new(
        principal_id: PrincipalId,
        name: String,
        kind: PrincipalKind,
        identity_scope: String,
        authentication_provider_kind: String,
        is_admin: bool,
    ) -> Result<Self, EventEnvelopeError> {
        for (field, value) in [
            ("name", &name),
            ("identity_scope", &identity_scope),
            (
                "authentication_provider_kind",
                &authentication_provider_kind,
            ),
        ] {
            if value.trim().is_empty() {
                return Err(EventEnvelopeError::new(format!(
                    "event subject {field} must not be empty"
                )));
            }
        }
        Ok(Self {
            principal_id,
            name,
            kind,
            identity_scope,
            authentication_provider_kind,
            is_admin,
        })
    }

    pub fn principal_id(&self) -> PrincipalId {
        self.principal_id
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn kind(&self) -> PrincipalKind {
        self.kind
    }

    pub fn identity_scope(&self) -> &str {
        &self.identity_scope
    }

    pub fn authentication_provider_kind(&self) -> &str {
        &self.authentication_provider_kind
    }

    pub fn is_admin(&self) -> bool {
        self.is_admin
    }

    /// Safe, credential-free document shared by every token-issuance adapter.
    pub fn successful_login_document(&self) -> AuditDocument {
        AuditDocument::builder(format!(
            "{} '{}' (principal {}, scope '{}') successfully logged in via {}",
            if self.is_admin {
                "Administrator"
            } else {
                "Principal"
            },
            self.name,
            self.principal_id,
            self.identity_scope,
            self.authentication_provider_kind,
        ))
        .metadata(serde_json::json!({"subject": self}))
        .try_build()
        .expect("typed login metadata is an object without resource revisions")
    }
}
