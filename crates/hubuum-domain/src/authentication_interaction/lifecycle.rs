//! Pure transitions; adapters must lock and persist mutations atomically with issuance.
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::{
    InteractionBinding, InteractionDigest, InteractionPurpose, VerifiedInteractionSubject,
};

pub const MAX_INTERACTION_LIFETIME_SECONDS: i64 = 600;
pub const DEFAULT_INTERACTION_LIFETIME_SECONDS: i64 = 300;
pub const INITIAL_INTERACTION_POLL_SECONDS: u32 = 5;
pub const INTERACTION_AUTHENTICATION_CLAIM_SECONDS: i64 = 30;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InteractionState {
    Pending,
    Ready,
    Denied,
    Expired,
    Cancelled,
    Consumed,
    Failed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InteractionMode {
    Credentials,
    Browser,
}

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum InteractionTransitionError {
    #[error("invalid interaction lifetime")]
    InvalidLifetime,
    #[error("invalid interaction binding or transition")]
    Invalid,
    #[error("authentication configuration changed")]
    ConfigurationChanged,
    #[error("interaction expired")]
    Expired,
    #[error("interaction was consumed")]
    Consumed,
    #[error("interaction was cancelled")]
    Cancelled,
    #[error("interaction was denied")]
    Denied,
    #[error("interaction failed")]
    Failed,
    #[error("interaction is not ready")]
    Pending,
    #[error("poll interval must increase to {retry_after_seconds} seconds")]
    SlowDown { retry_after_seconds: u32 },
}

/// Browser cookie and form challenge are independently generated capabilities.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InteractionBrowserBinding {
    cookie: InteractionDigest,
    csrf: InteractionDigest,
}
impl InteractionBrowserBinding {
    pub fn try_new(
        cookie: InteractionDigest,
        csrf: InteractionDigest,
    ) -> Result<Self, InteractionTransitionError> {
        if cookie == csrf {
            return Err(InteractionTransitionError::Invalid);
        }
        Ok(Self { cookie, csrf })
    }
    pub const fn cookie(&self) -> &InteractionDigest {
        &self.cookie
    }
    pub const fn csrf(&self) -> &InteractionDigest {
        &self.csrf
    }
}

/// A leased attempt. An ambiguous exchange is never reset to pending after expiry.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InteractionAuthenticationClaim {
    binding: InteractionBinding,
    attempt: Uuid,
    deadline: DateTime<Utc>,
}
impl InteractionAuthenticationClaim {
    pub const fn binding(&self) -> &InteractionBinding {
        &self.binding
    }
    pub const fn attempt(&self) -> Uuid {
        self.attempt
    }
    pub const fn deadline(&self) -> DateTime<Utc> {
        self.deadline
    }
}

/// Only non-secret issuance metadata survives consumption.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InteractionIssuedResult {
    Session(Uuid),
    Approval(i32),
    ProviderVerification(Uuid),
}
impl InteractionIssuedResult {
    fn matches(self, binding: &InteractionBinding) -> bool {
        match (self, binding.authority().purpose()) {
            (Self::Session(id), InteractionPurpose::Login) => !id.is_nil(),
            (Self::Approval(id), InteractionPurpose::CredentialApproval) => id > 0,
            (Self::ProviderVerification(id), InteractionPurpose::ProviderVerification) => {
                binding.authority().candidate_id() == Some(id)
            }
            _ => false,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Phase {
    Pending,
    Authenticating(InteractionAuthenticationClaim),
    AwaitingApproval(VerifiedInteractionSubject),
    Ready(VerifiedInteractionSubject),
    Denied,
    Expired,
    Cancelled,
    Consumed(InteractionIssuedResult),
    Failed,
}

/// Independent of clocks, HTTP and storage. Supply a trusted observation time
/// and persist mutations, including polling backoff, before returning errors.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InteractionLifecycle {
    binding: InteractionBinding,
    mode: InteractionMode,
    created_at: DateTime<Utc>,
    expires_at: DateTime<Utc>,
    interval_seconds: u32,
    next_poll_at: DateTime<Utc>,
    browser: Option<InteractionBrowserBinding>,
    attempts: Vec<Uuid>,
    phase: Phase,
}
impl InteractionLifecycle {
    pub fn try_new(
        binding: InteractionBinding,
        mode: InteractionMode,
        created_at: DateTime<Utc>,
        expires_at: DateTime<Utc>,
    ) -> Result<Self, InteractionTransitionError> {
        // PostgreSQL timestamps have microsecond precision. Canonicalize once
        // so persisted JSON and indexed columns carry identical deadlines.
        let created_at = DateTime::from_timestamp_micros(created_at.timestamp_micros())
            .ok_or(InteractionTransitionError::InvalidLifetime)?;
        let expires_at = DateTime::from_timestamp_micros(expires_at.timestamp_micros())
            .ok_or(InteractionTransitionError::InvalidLifetime)?;
        let lifetime = expires_at.signed_duration_since(created_at);
        if lifetime <= Duration::zero()
            || lifetime > Duration::seconds(MAX_INTERACTION_LIFETIME_SECONDS)
        {
            return Err(InteractionTransitionError::InvalidLifetime);
        }
        Ok(Self {
            binding,
            mode,
            created_at,
            expires_at,
            interval_seconds: INITIAL_INTERACTION_POLL_SECONDS,
            next_poll_at: created_at
                .checked_add_signed(Duration::seconds(INITIAL_INTERACTION_POLL_SECONDS.into()))
                .unwrap_or(expires_at)
                .min(expires_at),
            browser: None,
            attempts: Vec::new(),
            phase: Phase::Pending,
        })
    }
    pub const fn binding(&self) -> &InteractionBinding {
        &self.binding
    }
    pub const fn mode(&self) -> InteractionMode {
        self.mode
    }
    pub const fn created_at(&self) -> DateTime<Utc> {
        self.created_at
    }
    pub const fn expires_at(&self) -> DateTime<Utc> {
        self.expires_at
    }
    pub const fn interval_seconds(&self) -> u32 {
        self.interval_seconds
    }
    pub const fn browser(&self) -> Option<&InteractionBrowserBinding> {
        self.browser.as_ref()
    }

    pub fn state(&self, now: DateTime<Utc>) -> InteractionState {
        match self.phase {
            Phase::Consumed(_) => InteractionState::Consumed,
            Phase::Denied => InteractionState::Denied,
            Phase::Cancelled => InteractionState::Cancelled,
            Phase::Failed => InteractionState::Failed,
            Phase::Expired => InteractionState::Expired,
            _ if now < self.created_at || now >= self.expires_at => InteractionState::Expired,
            Phase::Authenticating(ref claim) if now >= claim.deadline => InteractionState::Failed,
            Phase::Ready(_) => InteractionState::Ready,
            _ => InteractionState::Pending,
        }
    }
    fn active(&mut self, now: DateTime<Utc>) -> Result<(), InteractionTransitionError> {
        let error = match self.state(now) {
            InteractionState::Pending | InteractionState::Ready => return Ok(()),
            InteractionState::Expired => {
                self.phase = Phase::Expired;
                InteractionTransitionError::Expired
            }
            InteractionState::Failed => {
                self.phase = Phase::Failed;
                InteractionTransitionError::Failed
            }
            InteractionState::Consumed => InteractionTransitionError::Consumed,
            InteractionState::Cancelled => InteractionTransitionError::Cancelled,
            InteractionState::Denied => InteractionTransitionError::Denied,
        };
        self.browser = None;
        Err(error)
    }
    /// Early polling changes backoff even though it returns SlowDown.
    pub fn poll(
        &mut self,
        now: DateTime<Utc>,
    ) -> Result<InteractionState, InteractionTransitionError> {
        let state = self.state(now);
        if state == InteractionState::Consumed {
            return Ok(state);
        }
        self.active(now)?;
        if now < self.next_poll_at {
            self.interval_seconds = self.interval_seconds.saturating_add(5);
            self.next_poll_at = now
                .checked_add_signed(Duration::seconds(self.interval_seconds.into()))
                .unwrap_or(self.expires_at)
                .min(self.expires_at);
            return Err(InteractionTransitionError::SlowDown {
                retry_after_seconds: self.interval_seconds,
            });
        }
        self.next_poll_at = now
            .checked_add_signed(Duration::seconds(self.interval_seconds.into()))
            .unwrap_or(self.expires_at)
            .min(self.expires_at);
        Ok(state)
    }
    /// The first browser cannot be replaced by another holder of the user code.
    pub fn bind_browser(
        &mut self,
        browser: InteractionBrowserBinding,
        now: DateTime<Utc>,
    ) -> Result<(), InteractionTransitionError> {
        self.active(now)?;
        if self.mode != InteractionMode::Browser || !matches!(self.phase, Phase::Pending) {
            return Err(InteractionTransitionError::Invalid);
        }
        match &self.browser {
            Some(existing) if existing != &browser => Err(InteractionTransitionError::Invalid),
            Some(_) => Ok(()),
            None => {
                self.browser = Some(browser);
                Ok(())
            }
        }
    }
    pub fn claim_authentication(
        &mut self,
        attempt: Uuid,
        browser: Option<&InteractionBrowserBinding>,
        now: DateTime<Utc>,
    ) -> Result<InteractionAuthenticationClaim, InteractionTransitionError> {
        self.active(now)?;
        if attempt.is_nil()
            || !matches!(self.phase, Phase::Pending)
            || self.attempts.len() >= 3
            || self.attempts.contains(&attempt)
        {
            return Err(InteractionTransitionError::Invalid);
        }
        match self.mode {
            InteractionMode::Credentials if browser.is_some() => {
                return Err(InteractionTransitionError::Invalid);
            }
            InteractionMode::Browser if browser.is_none() || self.browser.as_ref() != browser => {
                return Err(InteractionTransitionError::Invalid);
            }
            _ => {}
        }
        let claim = InteractionAuthenticationClaim {
            binding: self.binding.clone(),
            attempt,
            deadline: now
                .checked_add_signed(Duration::seconds(INTERACTION_AUTHENTICATION_CLAIM_SECONDS))
                .unwrap_or(self.expires_at)
                .min(self.expires_at),
        };
        self.attempts.push(attempt);
        self.phase = Phase::Authenticating(claim.clone());
        Ok(claim)
    }
    fn claimed(
        &mut self,
        claim: &InteractionAuthenticationClaim,
        now: DateTime<Utc>,
    ) -> Result<(), InteractionTransitionError> {
        self.active(now)?;
        if !matches!(&self.phase, Phase::Authenticating(current) if current == claim) {
            return Err(InteractionTransitionError::Invalid);
        }
        Ok(())
    }
    pub fn finish_authentication(
        &mut self,
        claim: &InteractionAuthenticationClaim,
        proof: VerifiedInteractionSubject,
        approval_challenge: Option<InteractionBrowserBinding>,
        now: DateTime<Utc>,
    ) -> Result<(), InteractionTransitionError> {
        self.claimed(claim, now)?;
        proof
            .require_binding(&self.binding)
            .map_err(|_| InteractionTransitionError::Invalid)?;
        self.phase = match self.mode {
            InteractionMode::Credentials => {
                if approval_challenge.is_some() {
                    return Err(InteractionTransitionError::Invalid);
                }
                Phase::Ready(proof)
            }
            InteractionMode::Browser => {
                let next = approval_challenge.ok_or(InteractionTransitionError::Invalid)?;
                let original = self
                    .browser
                    .as_ref()
                    .ok_or(InteractionTransitionError::Invalid)?;
                if next.cookie != original.cookie || next.csrf == original.csrf {
                    return Err(InteractionTransitionError::Invalid);
                }
                self.browser = Some(next);
                Phase::AwaitingApproval(proof)
            }
        };
        Ok(())
    }
    pub fn reject_authentication(
        &mut self,
        claim: &InteractionAuthenticationClaim,
        now: DateTime<Utc>,
    ) -> Result<(), InteractionTransitionError> {
        self.claimed(claim, now)?;
        self.phase = if self.mode == InteractionMode::Credentials && self.attempts.len() < 3 {
            Phase::Pending
        } else {
            Phase::Failed
        };
        if matches!(self.phase, Phase::Failed) {
            self.browser = None;
        }
        Ok(())
    }
    pub fn decide_browser(
        &mut self,
        browser: &InteractionBrowserBinding,
        approve: bool,
        now: DateTime<Utc>,
    ) -> Result<(), InteractionTransitionError> {
        self.active(now)?;
        if self.browser.as_ref() != Some(browser) {
            return Err(InteractionTransitionError::Invalid);
        }
        let Phase::AwaitingApproval(proof) = &self.phase else {
            return Err(InteractionTransitionError::Invalid);
        };
        self.phase = if approve {
            Phase::Ready(proof.clone())
        } else {
            Phase::Denied
        };
        self.browser = None;
        Ok(())
    }
    pub fn ready_subject(
        &mut self,
        now: DateTime<Utc>,
    ) -> Result<&VerifiedInteractionSubject, InteractionTransitionError> {
        self.active(now)?;
        match &self.phase {
            Phase::Ready(proof) => Ok(proof),
            _ => Err(InteractionTransitionError::Pending),
        }
    }
    /// Persist this transition in the same transaction as the supplied result.
    pub fn consume(
        &mut self,
        result: InteractionIssuedResult,
        now: DateTime<Utc>,
    ) -> Result<(), InteractionTransitionError> {
        self.ready_subject(now)?;
        if !result.matches(&self.binding) {
            return Err(InteractionTransitionError::Invalid);
        }
        self.phase = Phase::Consumed(result);
        self.browser = None;
        Ok(())
    }
    pub fn cancel(&mut self, now: DateTime<Utc>) -> Result<(), InteractionTransitionError> {
        if self.state(now) == InteractionState::Cancelled {
            return Ok(());
        }
        self.active(now)?;
        self.phase = Phase::Cancelled;
        self.browser = None;
        Ok(())
    }
    pub const fn issued_result(&self) -> Option<InteractionIssuedResult> {
        match self.phase {
            Phase::Consumed(value) => Some(value),
            _ => None,
        }
    }

    pub fn authenticated_subject(&self) -> Option<&VerifiedInteractionSubject> {
        match &self.phase {
            Phase::Ready(proof) | Phase::AwaitingApproval(proof) => Some(proof),
            _ => None,
        }
    }

    pub fn authentication_claim(&self) -> Option<&InteractionAuthenticationClaim> {
        match &self.phase {
            Phase::Authenticating(claim) => Some(claim),
            _ => None,
        }
    }

    /// Discard reusable authority after a failed configuration or actor fence.
    /// Consumed metadata remains available for orphan-session cleanup.
    pub fn invalidate(&mut self) {
        if !matches!(self.phase, Phase::Consumed(_)) {
            self.phase = Phase::Failed;
            self.browser = None;
        }
    }
}

mod persistence;

#[cfg(test)]
mod tests;
