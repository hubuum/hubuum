//! Administrator policy review/application with independent recovery protection.

use std::collections::BTreeSet;

use hubuum_domain::{PrincipalId, PrincipalIdentity};
use hubuum_storage_core::{
    ApplicationFence, DeletionCheckpointIdentity, HistoryAuthorityRevision,
    HistoryExpiryTaskCheckpoint, HistoryExposureProtection, HistoryHoldTransition,
    HistoryLifecycleControlChange, HistoryLifecycleOperationEvidence, HistoryLifecycleScan,
    HistoryLifecycleSnapshot, HistoryLifecycleStorage, HistoryPolicyApplyError,
    HistoryPolicyApplyRequest, HistoryPolicyChange, HistoryPolicyPreview, HistoryPolicyRecord,
    HistoryPolicyReviewConflict, HistoryPreviewCoverage, HistoryPreviewImpact,
    HistoryRetentionHold, LogicalIdentityStorage, StorageIdentitySubject, StorageIdentityTarget,
    TaskRetentionStorage,
};

use super::repository::{CheckpointRepository, CheckpointSession};
use super::{covers_checkpoint, unavailable};
use crate::config::ControlRecoveryOptions;
use crate::errors::ApiError;
use crate::extractors::AdminAccess;
use crate::models::history_lifecycle::{
    HistoryAuthorityProtection, HistoryLifecycleInspection, HistoryPolicyListRequest,
    HistoryPolicyPage, HistoryRetentionHoldRequest,
};
use crate::permissions::AppContext;
use crate::storage::storage_handle;

pub(super) async fn principal(
    context: &AppContext,
    admin: &AdminAccess,
) -> Result<PrincipalIdentity, ApiError> {
    // Re-evaluate configured authorization at application time; an issued review
    // is neither a durable permission grant nor a bypass of external policy.
    if !context.is_admin(&admin.user).await? {
        return Err(ApiError::Forbidden("Administrator access required".into()));
    }
    let identity = storage_handle(context)
        .resolve_logical_identity(StorageIdentityTarget::Principal(PrincipalId::new(
            admin.user.id,
        )?))
        .await?;
    match identity.subject() {
        Some(StorageIdentitySubject::Principal { identity }) => identity
            .known_identity()
            .ok_or_else(|| unavailable("Policy authority requires a resolved principal identity")),
        _ => Err(unavailable(
            "Policy authority requires a resolved principal identity",
        )),
    }
}

pub(crate) async fn preview(
    context: &AppContext,
    admin: &AdminAccess,
    proposal: HistoryPolicyChange,
) -> Result<HistoryPolicyPreview, ApiError> {
    let principal = principal(context, admin).await?;
    let storage = storage_handle(context);
    let snapshot = storage.capture_history_lifecycle().await?;
    let now = snapshot.observed_at();
    if proposal.expected_policy_revision()
        != snapshot
            .control()
            .authority()
            .policies()
            .current_revision(proposal.key())
    {
        return Err(ApiError::HistoryPolicyReview(
            HistoryPolicyReviewConflict::StalePreview,
        ));
    }
    let proposed = snapshot
        .control()
        .authority()
        .apply(proposal.clone(), principal, now)
        .map_err(|error| ApiError::BadRequest(error.to_string()))?;
    let scan = HistoryLifecycleScan::new(proposal.key(), HistoryPreviewImpact::MAX_SCANNED)
        .map_err(|error| ApiError::BadRequest(error.to_string()))?;
    let scanned = storage.scan_history_lifecycle(scan).await?;
    let mut samples = Vec::new();
    let mut eligible = 0;
    for candidate in scanned.candidates() {
        let decision = proposed
            .inspect(candidate, now, &[])
            .map_err(|error| ApiError::BadRequest(error.to_string()))?;
        eligible += u32::from(decision.eligible());
        if samples.len() < HistoryPreviewImpact::MAX_SAMPLES {
            samples.push(decision);
        }
    }
    let impact = HistoryPreviewImpact::new(
        if scanned.complete() {
            HistoryPreviewCoverage::Complete
        } else {
            HistoryPreviewCoverage::Bounded
        },
        scanned.scanned(),
        eligible,
        samples,
    )
    .map_err(|error| ApiError::BadRequest(error.to_string()))?;
    let preview = HistoryPolicyPreview::issue(principal, proposal, snapshot.fence(), now, impact)
        .map_err(|error| ApiError::BadRequest(error.to_string()))?;
    let change = snapshot
        .control()
        .issue_preview(preview.clone(), snapshot.fence())
        .map_err(|_| ApiError::HistoryPolicyReview(HistoryPolicyReviewConflict::StalePreview))?;
    storage.commit_history_lifecycle(change).await?;
    Ok(preview)
}

fn apply_error(error: HistoryPolicyApplyError) -> ApiError {
    match error {
        HistoryPolicyApplyError::Invalid(error) => ApiError::BadRequest(error.to_string()),
        HistoryPolicyApplyError::Conflict(reason) => ApiError::HistoryPolicyReview(reason),
    }
}
async fn session(options: &ControlRecoveryOptions) -> Result<CheckpointSession, ApiError> {
    let (directory, key) = options.paths()?.ok_or_else(|| {
        unavailable("Policy application requires configured independent control recovery")
    })?;
    let directory = directory.to_owned();
    let key = key.to_owned();
    tokio::task::spawn_blocking(move || {
        CheckpointRepository::open(&directory, &key)?.into_session()
    })
    .await
    .map_err(|_| unavailable("Policy checkpoint worker failed"))?
}

pub(crate) async fn apply(
    context: &AppContext,
    admin: &AdminAccess,
    options: &ControlRecoveryOptions,
    request: HistoryPolicyApplyRequest,
) -> Result<HistoryPolicyRecord, ApiError> {
    let principal = principal(context, admin).await?;
    let session = session(options).await?;
    let storage = storage_handle(context);
    let before = storage.capture_control_checkpoint().await?;
    if !covers_checkpoint(&before, session.checkpoint()) {
        return Err(unavailable(
            "Recover current control authority before applying policy",
        ));
    }
    let snapshot = storage.capture_history_lifecycle().await?;
    let change = snapshot
        .control()
        .apply(request, principal, snapshot.fence(), snapshot.observed_at())
        .map_err(apply_error)?;
    let applied = commit_protected(context, session, change).await?;
    applied
        .control()
        .authority()
        .policies()
        .records()
        .last()
        .cloned()
        .ok_or_else(|| unavailable("Applied policy was not captured"))
}

async fn commit_protected(
    context: &AppContext,
    session: CheckpointSession,
    change: HistoryLifecycleControlChange,
) -> Result<HistoryLifecycleSnapshot, ApiError> {
    let storage = storage_handle(context);
    let applied = storage.commit_history_lifecycle(change).await?;
    protect_snapshot(context, session, applied)
        .await
        .map(|(snapshot, _)| snapshot)
}

async fn protect_snapshot(
    context: &AppContext,
    mut session: CheckpointSession,
    applied: HistoryLifecycleSnapshot,
) -> Result<(HistoryLifecycleSnapshot, CheckpointSession), ApiError> {
    let storage = storage_handle(context);
    let expected_authority = applied.control().authority().clone();
    let capture = storage.capture_control_checkpoint().await?;
    if !capture
        .history_lifecycle()
        .is_some_and(|authority| authority.covers(&expected_authority))
    {
        return Err(unavailable(
            "Committed history authority was not captured for recovery protection",
        ));
    }
    let session = tokio::task::spawn_blocking(move || {
        session.publish_control(&capture)?;
        if !session
            .checkpoint()
            .history_lifecycle()
            .is_some_and(|authority| authority.covers(&expected_authority))
        {
            return Err(unavailable(
                "Independent evidence does not cover committed history authority",
            ));
        }
        Ok(session)
    })
    .await
    .map_err(|_| unavailable("History policy protection worker failed"))??;
    Ok((applied, session))
}

pub(super) async fn protect_expiry(
    context: &AppContext,
    checkpoint: &HistoryExpiryTaskCheckpoint,
) -> Result<HistoryExposureProtection, ApiError> {
    let session = protected_session(context, context.history_retention()).await?;
    let snapshot = storage_handle(context).capture_history_lifecycle().await?;
    checkpoint
        .retained_manifest(&snapshot)
        .map_err(|_| unavailable("History expiry lacks its original retained admission"))?;
    let (snapshot, session) = protect_snapshot(context, session, snapshot).await?;
    let fence = ApplicationFence::new(
        snapshot.fence().incarnation(),
        snapshot.fence().deletion_revision().value() as i64,
    )
    .map_err(|_| unavailable("Invalid expiry application fence"))?;
    let authority = session
        .checkpoint()
        .history_lifecycle()
        .cloned()
        .ok_or_else(|| unavailable("Independent history authority is absent"))?;
    let identity = DeletionCheckpointIdentity::new(session.checkpoint().identity().to_owned())
        .map_err(|_| unavailable("Invalid independently verified checkpoint identity"))?;
    Ok(HistoryExposureProtection::new(authority, fence, identity))
}

pub(crate) async fn list(
    context: &AppContext,
    admin: &AdminAccess,
    query: HistoryPolicyListRequest,
    options: &ControlRecoveryOptions,
) -> Result<HistoryPolicyPage, ApiError> {
    principal(context, admin).await?;
    if query.limit == 0 || query.limit > 100 {
        return Err(ApiError::BadRequest(
            "Policy page limit must be 1..=100".into(),
        ));
    }
    // Hold the independent writer session across a single composite primary
    // capture, so a concurrent protected apply cannot make its checkpoint newer
    // than the authority used for this page and its protection verdict.
    let repository = if options.configured() {
        Some(session(options).await)
    } else {
        None
    };
    let primary = storage_handle(context).capture_control_checkpoint().await?;
    let authority = primary.history_lifecycle().cloned().unwrap_or_default();
    let records = authority
        .policies()
        .records()
        .iter()
        .filter(|record| record.revision() > query.after_revision)
        .take(usize::from(query.limit) + 1)
        .cloned()
        .collect::<Vec<_>>();
    let more = records.len() > usize::from(query.limit);
    let records = records
        .into_iter()
        .take(usize::from(query.limit))
        .collect::<Vec<_>>();
    let next_revision = more.then(|| records.last().expect("positive page limit").revision());
    let (protection, protected_authority_revision) = match &repository {
        None => (HistoryAuthorityProtection::Unconfigured, None),
        Some(Err(_)) => (HistoryAuthorityProtection::Unavailable, None),
        Some(Ok(session)) => {
            let checkpoint = session.checkpoint();
            let protected = checkpoint.history_lifecycle();
            let state = if !primary.deletion().state().covers(checkpoint.state())
                || protected.is_some_and(|required| !authority.covers(required))
            {
                HistoryAuthorityProtection::RecoveryRequired
            } else if protected.is_some_and(|protected| protected.covers(&authority)) {
                HistoryAuthorityProtection::Protected
            } else {
                HistoryAuthorityProtection::Pending
            };
            (state, protected.map(|authority| authority.revision()))
        }
    };
    Ok(HistoryPolicyPage {
        authority_revision: authority.revision(),
        protected_authority_revision,
        protection,
        records,
        next_revision,
    })
}

pub(crate) async fn inspect(
    context: &AppContext,
    admin: &AdminAccess,
    scan: HistoryLifecycleScan,
) -> Result<HistoryLifecycleInspection, ApiError> {
    principal(context, admin).await?;
    let storage = storage_handle(context);
    let snapshot = storage.capture_history_lifecycle().await?;
    let scanned = storage.scan_history_lifecycle(scan).await?;
    let now = snapshot.observed_at();
    let decisions = scanned
        .candidates()
        .iter()
        .map(|candidate| snapshot.control().authority().inspect(candidate, now, &[]))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| ApiError::BadRequest(error.to_string()))?;
    Ok(HistoryLifecycleInspection {
        authority_revision: snapshot.control().authority().revision(),
        inspected_at: snapshot.observed_at(),
        coverage: if scanned.complete() {
            HistoryPreviewCoverage::Complete
        } else {
            HistoryPreviewCoverage::Bounded
        },
        scanned: scanned.scanned().to_string(),
        unresolved_identities: scanned.unresolved_identities().to_string(),
        eligible_observed: decisions
            .iter()
            .filter(|decision| decision.eligible())
            .count()
            .to_string(),
        samples: decisions
            .into_iter()
            .take(HistoryPreviewImpact::MAX_SAMPLES)
            .collect(),
    })
}

async fn protected_session(
    context: &AppContext,
    options: &ControlRecoveryOptions,
) -> Result<CheckpointSession, ApiError> {
    let session = session(options).await?;
    if !covers_checkpoint(
        &storage_handle(context).capture_control_checkpoint().await?,
        session.checkpoint(),
    ) {
        return Err(unavailable(
            "Recover current control authority before changing retention protection",
        ));
    }
    Ok(session)
}

/// Settle one natural hold expiry, preserving its original effective deadline.
/// The independent writer session serializes replicas; the backend CAS still
/// protects against changes made outside this service. No payload is removed.
pub(crate) async fn settle_expired_hold(context: &AppContext) -> Result<bool, ApiError> {
    let options = context.history_retention();
    if !options.configured() {
        return Ok(false);
    }
    let mut session = protected_session(context, options).await?;
    let storage = storage_handle(context);
    let snapshot = storage.capture_history_lifecycle().await?;
    let mut seen = BTreeSet::new();
    let expired = snapshot
        .control()
        .authority()
        .hold_revisions()
        .iter()
        .rev()
        .filter(|entry| seen.insert(entry.hold().identity()))
        .find(|entry| {
            entry.transition() == HistoryHoldTransition::Created
                && entry
                    .hold()
                    .expires_at()
                    .is_some_and(|deadline| deadline <= snapshot.observed_at())
        });
    if let Some(entry) = expired {
        let change = snapshot
            .control()
            .settle_expired_hold(
                entry.hold().identity(),
                snapshot.fence(),
                snapshot.observed_at(),
            )
            .map_err(|error| unavailable(&error.to_string()))?;
        commit_protected(context, session, change).await?;
        return Ok(true);
    }
    // A previous transition may have committed before checkpoint publication
    // failed. Repair protection without emitting another transition or moving
    // an original clock, even when no further hold is due.
    let authority = snapshot.control().authority();
    if authority.revision().value() > 0
        && !session
            .checkpoint()
            .history_lifecycle()
            .is_some_and(|protected| protected.covers(authority))
    {
        let capture = storage.capture_control_checkpoint().await?;
        tokio::task::spawn_blocking(move || session.publish_control(&capture))
            .await
            .map_err(|_| unavailable("History hold protection worker failed"))??;
    }
    Ok(false)
}

pub(crate) async fn create_hold(
    context: &AppContext,
    admin: &AdminAccess,
    options: &ControlRecoveryOptions,
    request: HistoryRetentionHoldRequest,
) -> Result<HistoryRetentionHold, ApiError> {
    let principal = principal(context, admin).await?;
    let key = request.key(context).await?;
    let session = protected_session(context, options).await?;
    let storage = storage_handle(context);
    let snapshot = storage.capture_history_lifecycle().await?;
    let hold = if let Some(original) = snapshot
        .control()
        .authority()
        .hold_revisions()
        .iter()
        .find(|entry| entry.hold().identity() == request.identity)
        .map(|entry| entry.hold())
    {
        if original.key() != key
            || original.expires_at() != request.expires_at
            || original.principal() != principal
        {
            return Err(ApiError::Conflict(
                "Hold identity already has a different immutable creation request".into(),
            ));
        }
        original.clone()
    } else {
        if request.expected_authority_revision != snapshot.control().authority().revision() {
            return Err(ApiError::HistoryPolicyReview(
                HistoryPolicyReviewConflict::StalePreview,
            ));
        }
        HistoryRetentionHold::new(
            request.identity,
            key,
            snapshot.observed_at(),
            request.expires_at,
            principal,
        )
        .map_err(|error| ApiError::BadRequest(error.to_string()))?
    };
    let change = snapshot
        .control()
        .create_hold(hold, snapshot.fence())
        .map_err(|error| ApiError::BadRequest(error.to_string()))?;
    let applied = commit_protected(context, session, change).await?;
    applied
        .control()
        .authority()
        .current_holds()
        .find(|hold| hold.identity() == request.identity)
        .cloned()
        .ok_or_else(|| unavailable("Committed hold was not captured"))
}

pub(crate) async fn release_hold(
    context: &AppContext,
    admin: &AdminAccess,
    options: &ControlRecoveryOptions,
    identity: uuid::Uuid,
    expected: HistoryAuthorityRevision,
) -> Result<HistoryRetentionHold, ApiError> {
    let principal = principal(context, admin).await?;
    let session = protected_session(context, options).await?;
    let snapshot = storage_handle(context).capture_history_lifecycle().await?;
    let hold = snapshot
        .control()
        .authority()
        .current_holds()
        .find(|hold| hold.identity() == identity)
        .ok_or_else(|| ApiError::NotFound("Hold was not found".into()))?;
    if hold.released_at().is_none() && expected != snapshot.control().authority().revision() {
        return Err(ApiError::HistoryPolicyReview(
            HistoryPolicyReviewConflict::StalePreview,
        ));
    }
    let change = snapshot
        .control()
        .release_hold(
            identity,
            principal,
            snapshot.fence(),
            snapshot.observed_at(),
        )
        .map_err(|error| ApiError::BadRequest(error.to_string()))?;
    let applied = commit_protected(context, session, change).await?;
    applied
        .control()
        .authority()
        .current_holds()
        .find(|hold| hold.identity() == identity)
        .cloned()
        .ok_or_else(|| unavailable("Committed hold release was not captured"))
}

#[cfg(all(test, unix))]
mod tests;

pub(crate) async fn operation(
    context: &AppContext,
    admin: &AdminAccess,
    identity: uuid::Uuid,
) -> Result<HistoryLifecycleOperationEvidence, ApiError> {
    principal(context, admin).await?;
    storage_handle(context)
        .capture_history_lifecycle()
        .await?
        .control()
        .operation_evidence(identity)
        .ok_or_else(|| {
            ApiError::NotFound("History lifecycle operation evidence is unavailable".into())
        })
}
