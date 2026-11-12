//! Explicit trusted-storage decoding; authentication proofs are never request DTOs.
use super::*;
use serde_json::Value;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Document {
    binding: InteractionBinding,
    mode: InteractionMode,
    created_at: DateTime<Utc>,
    expires_at: DateTime<Utc>,
    interval_seconds: u32,
    next_poll_at: DateTime<Utc>,
    browser: Option<BrowserDocument>,
    attempts: Vec<Uuid>,
    phase: PhaseDocument,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct BrowserDocument {
    cookie: InteractionDigest,
    csrf: InteractionDigest,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
enum PhaseDocument {
    Pending {},
    Authenticating {
        attempt: Uuid,
        deadline: DateTime<Utc>,
    },
    AwaitingApproval {
        subject: String,
    },
    Ready {
        subject: String,
    },
    Denied {},
    Expired {},
    Cancelled {},
    SessionConsumed {
        session_id: Uuid,
    },
    ApprovalConsumed {
        approval_id: i32,
    },
    VerificationConsumed {
        candidate_id: Uuid,
    },
    Failed {},
}

impl InteractionLifecycle {
    /// Encode for trusted persistence. The document contains private correlation
    /// and subject evidence and must not be returned by a transport handler.
    pub fn to_document(&self) -> Value {
        let phase = match &self.phase {
            Phase::Pending => PhaseDocument::Pending {},
            Phase::Authenticating(claim) => PhaseDocument::Authenticating {
                attempt: claim.attempt,
                deadline: claim.deadline,
            },
            Phase::AwaitingApproval(proof) => PhaseDocument::AwaitingApproval {
                subject: proof.subject().to_owned(),
            },
            Phase::Ready(proof) => PhaseDocument::Ready {
                subject: proof.subject().to_owned(),
            },
            Phase::Denied => PhaseDocument::Denied {},
            Phase::Expired => PhaseDocument::Expired {},
            Phase::Cancelled => PhaseDocument::Cancelled {},
            Phase::Failed => PhaseDocument::Failed {},
            Phase::Consumed(InteractionIssuedResult::Session(session_id)) => {
                PhaseDocument::SessionConsumed {
                    session_id: *session_id,
                }
            }
            Phase::Consumed(InteractionIssuedResult::Approval(approval_id)) => {
                PhaseDocument::ApprovalConsumed {
                    approval_id: *approval_id,
                }
            }
            Phase::Consumed(InteractionIssuedResult::ProviderVerification(candidate_id)) => {
                PhaseDocument::VerificationConsumed {
                    candidate_id: *candidate_id,
                }
            }
        };
        serde_json::to_value(Document {
            binding: self.binding.clone(),
            mode: self.mode,
            created_at: self.created_at,
            expires_at: self.expires_at,
            interval_seconds: self.interval_seconds,
            next_poll_at: self.next_poll_at,
            browser: self.browser.as_ref().map(|value| BrowserDocument {
                cookie: value.cookie.clone(),
                csrf: value.csrf.clone(),
            }),
            attempts: self.attempts.clone(),
            phase,
        })
        .expect("interaction persistence contains only JSON-compatible values")
    }

    /// Restore an adapter-owned document, rechecking correlated invariants.
    /// This is not an authentication-proof constructor for untrusted requests.
    pub fn from_document(value: Value) -> Result<Self, InteractionTransitionError> {
        let doc: Document =
            serde_json::from_value(value).map_err(|_| InteractionTransitionError::Invalid)?;
        let mut restored = Self::try_new(doc.binding, doc.mode, doc.created_at, doc.expires_at)?;
        if restored.created_at != doc.created_at
            || restored.expires_at != doc.expires_at
            || doc.interval_seconds < INITIAL_INTERACTION_POLL_SECONDS
            || !doc
                .interval_seconds
                .is_multiple_of(INITIAL_INTERACTION_POLL_SECONDS)
            || doc.next_poll_at < doc.created_at
            || doc.next_poll_at > doc.expires_at
            || doc.attempts.len() > 3
            || doc
                .attempts
                .iter()
                .enumerate()
                .any(|(index, attempt)| attempt.is_nil() || doc.attempts[..index].contains(attempt))
        {
            return Err(InteractionTransitionError::Invalid);
        }
        restored.interval_seconds = doc.interval_seconds;
        restored.next_poll_at = doc.next_poll_at;
        restored.attempts = doc.attempts;
        restored.browser = doc
            .browser
            .map(|value| InteractionBrowserBinding::try_new(value.cookie, value.csrf))
            .transpose()?;
        restored.phase = match doc.phase {
            PhaseDocument::Pending {} => Phase::Pending,
            PhaseDocument::Authenticating { attempt, deadline } => {
                if restored.attempts.last() != Some(&attempt)
                    || deadline <= doc.created_at
                    || deadline > doc.expires_at
                {
                    return Err(InteractionTransitionError::Invalid);
                }
                Phase::Authenticating(InteractionAuthenticationClaim {
                    binding: restored.binding.clone(),
                    attempt,
                    deadline,
                })
            }
            PhaseDocument::AwaitingApproval { subject } => Phase::AwaitingApproval(
                VerifiedInteractionSubject::new(restored.binding.clone(), subject)
                    .map_err(|_| InteractionTransitionError::Invalid)?,
            ),
            PhaseDocument::Ready { subject } => Phase::Ready(
                VerifiedInteractionSubject::new(restored.binding.clone(), subject)
                    .map_err(|_| InteractionTransitionError::Invalid)?,
            ),
            PhaseDocument::Denied {} => Phase::Denied,
            PhaseDocument::Expired {} => Phase::Expired,
            PhaseDocument::Cancelled {} => Phase::Cancelled,
            PhaseDocument::Failed {} => Phase::Failed,
            PhaseDocument::SessionConsumed { session_id } => {
                Phase::Consumed(InteractionIssuedResult::Session(session_id))
            }
            PhaseDocument::ApprovalConsumed { approval_id } => {
                Phase::Consumed(InteractionIssuedResult::Approval(approval_id))
            }
            PhaseDocument::VerificationConsumed { candidate_id } => {
                Phase::Consumed(InteractionIssuedResult::ProviderVerification(candidate_id))
            }
        };
        if restored.mode == InteractionMode::Credentials && restored.browser.is_some()
            || matches!(restored.phase, Phase::AwaitingApproval(_))
                && (restored.mode != InteractionMode::Browser || restored.browser.is_none())
            || matches!(restored.phase, Phase::Authenticating(_))
                && restored.mode == InteractionMode::Browser
                && restored.browser.is_none()
            || matches!(
                restored.phase,
                Phase::Ready(_) | Phase::AwaitingApproval(_) | Phase::Consumed(_)
            ) && restored.attempts.is_empty()
            || !matches!(
                restored.phase,
                Phase::Pending | Phase::Authenticating(_) | Phase::AwaitingApproval(_)
            ) && restored.browser.is_some()
            || restored
                .issued_result()
                .is_some_and(|result| !result.matches(&restored.binding))
        {
            return Err(InteractionTransitionError::Invalid);
        }
        Ok(restored)
    }
}
