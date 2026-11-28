use hubuum_domain::{PrincipalId, PrincipalIdentity, TaskIdentity};
use hubuum_storage_core::{
    ExpiredTaskSubmission, LogicalIdentityStorage, StorageIdentitySubject, StorageIdentityTarget,
    StorageTaskSubmissionOutcome, TaskActivationOutcome, TaskAdmissionTicket, TaskReceiptBinding,
    TaskSubmissionResolution,
};
use hubuum_task_core::IdempotencyKey;
use serde::Serialize;

use crate::errors::ApiError;
use crate::models::TaskRecord;
use crate::permissions::{
    AppContext, AuthorizationContext, AuthorizationMode, PermissionDecision, PrincipalRef,
    ResourceRef,
};
use crate::storage::storage_handle;
use crate::tasks::kick_task_worker;
use crate::traits::AuthzSubject;

use super::{
    TaskSubmission, TaskSubmissionOperation, authorize_submission_task, submit_task,
    task_from_storage,
};

/// Proof that the current durable owner passed the selected ReadTask policy.
pub(crate) struct AuthorizedExpiredTaskSubmission(ExpiredTaskSubmission);

impl AuthorizedExpiredTaskSubmission {
    pub(crate) fn receipt(&self) -> &ExpiredTaskSubmission {
        &self.0
    }
}

/// Original client request identity, captured before mutable dependencies or
/// one-time approval are resolved. No request body is retained here.
pub(crate) struct TaskSubmissionAttempt {
    binding: Option<TaskReceiptBinding>,
    key: Option<IdempotencyKey>,
}

#[derive(Clone, Copy)]
enum ActivationResponseAccess {
    CreationAuthorized,
    RequiresTaskRead,
}

impl TaskSubmissionAttempt {
    pub(crate) async fn prepare<S: AuthzSubject + ?Sized>(
        context: &AppContext,
        requestor: &S,
        key: Option<IdempotencyKey>,
        operation: TaskSubmissionOperation,
        original: &impl Serialize,
        legacy_digest: Option<&str>,
    ) -> Result<Self, ApiError> {
        let binding = match key.as_ref() {
            Some(key) if context.task_receipts().is_configured() => {
                let identity = principal_identity(context, requestor).await?;
                context.task_receipts().prepare_submission(
                    identity,
                    key,
                    operation.kind(),
                    operation.as_str(),
                    original,
                    legacy_digest,
                )?
            }
            _ => None,
        };
        Ok(Self { binding, key })
    }

    /// A miss is advisory. Storage rechecks receipt generation and policy while
    /// reserving fresh work; a concurrent winner cannot consume capacity twice.
    pub(crate) async fn lookup<S: AuthzSubject + ?Sized>(
        &self,
        context: &AppContext,
        requestor: &S,
    ) -> Result<Option<TaskRecord>, ApiError> {
        let Some(binding) = self.binding.as_ref() else {
            return Ok(None);
        };
        let resolution = context
            .task_receipts()
            .resolve_submission(context, binding.lookup().clone())
            .await?;
        match resolution {
            TaskSubmissionResolution::Fresh | TaskSubmissionResolution::ProtectionDisabled => {
                Ok(None)
            }
            TaskSubmissionResolution::Available(task) => {
                self.authorize_owner(context, requestor).await?;
                authorize_submission_task(context, requestor, *task)
                    .await
                    .map(Some)
            }
            TaskSubmissionResolution::PendingProtection(ticket) => {
                authorize_receipt(
                    context,
                    requestor,
                    ticket.receipt().submission().principal(),
                    ticket.receipt().task(),
                )
                .await?;
                self.activate(
                    context,
                    requestor,
                    ticket,
                    ActivationResponseAccess::RequiresTaskRead,
                )
                .await
                .map(Some)
            }
            TaskSubmissionResolution::DetailsExpired(receipt) => {
                Err(expired_error(context, requestor, receipt).await?)
            }
            TaskSubmissionResolution::LegacyUnverifiable(receipt) => {
                authorize_receipt(context, requestor, receipt.owner(), receipt.task()).await?;
                Err(ApiError::LegacyTaskRequestUnverifiable)
            }
            TaskSubmissionResolution::ExpiryProtectionRequired(_) => Err(protection_pending()),
        }
    }

    pub(crate) async fn submit<S: AuthzSubject + ?Sized>(
        self,
        context: &AppContext,
        requestor: &S,
        submission: TaskSubmission,
    ) -> Result<TaskRecord, ApiError> {
        let submission = submission
            .idempotency_key(self.key.clone())
            .receipt_binding(self.binding.clone());
        match submit_task(context, submission).await? {
            StorageTaskSubmissionOutcome::CreatedReady(task) => {
                kick_task_worker(context.clone());
                task_from_storage(task)
            }
            StorageTaskSubmissionOutcome::PendingProtection(ticket) => {
                self.activate(
                    context,
                    requestor,
                    ticket,
                    ActivationResponseAccess::CreationAuthorized,
                )
                .await
            }
            StorageTaskSubmissionOutcome::DuplicatePendingProtection(ticket) => {
                authorize_receipt(
                    context,
                    requestor,
                    ticket.receipt().submission().principal(),
                    ticket.receipt().task(),
                )
                .await?;
                self.activate(
                    context,
                    requestor,
                    ticket,
                    ActivationResponseAccess::RequiresTaskRead,
                )
                .await
            }
            StorageTaskSubmissionOutcome::DuplicateAvailable(task) => {
                self.authorize_owner(context, requestor).await?;
                authorize_submission_task(context, requestor, task).await
            }
            StorageTaskSubmissionOutcome::DuplicateDetailsExpired(receipt) => {
                Err(expired_error(context, requestor, receipt).await?)
            }
            StorageTaskSubmissionOutcome::LegacyUnverifiable(receipt) => {
                authorize_receipt(context, requestor, receipt.owner(), receipt.task()).await?;
                Err(ApiError::LegacyTaskRequestUnverifiable)
            }
            StorageTaskSubmissionOutcome::ExpiryProtectionRequired(_) => Err(protection_pending()),
        }
    }

    async fn activate<S: AuthzSubject + ?Sized>(
        &self,
        context: &AppContext,
        requestor: &S,
        ticket: TaskAdmissionTicket,
        response_access: ActivationResponseAccess,
    ) -> Result<TaskRecord, ApiError> {
        let outcome = context
            .task_receipts()
            .protect_admission(context, ticket)
            .await?;
        if matches!(&outcome, TaskActivationOutcome::Activated(_)) {
            kick_task_worker(context.clone());
        }
        self.activation_response(context, requestor, outcome, response_access)
            .await
    }

    async fn activation_response<S: AuthzSubject + ?Sized>(
        &self,
        context: &AppContext,
        requestor: &S,
        outcome: TaskActivationOutcome,
        access: ActivationResponseAccess,
    ) -> Result<TaskRecord, ApiError> {
        let task = match outcome {
            TaskActivationOutcome::Activated(task) | TaskActivationOutcome::AlreadyActive(task) => {
                task
            }
            TaskActivationOutcome::DetailsExpired(receipt) => {
                return Err(expired_error(context, requestor, receipt).await?);
            }
        };
        // Receipt authorization permits helping the original admission; it
        // does not grant access to the retained task's full response. A fresh
        // creator keeps its creation permission even if another helper won.
        match access {
            ActivationResponseAccess::CreationAuthorized => task_from_storage(task),
            ActivationResponseAccess::RequiresTaskRead => {
                self.authorize_owner(context, requestor).await?;
                authorize_submission_task(context, requestor, task).await
            }
        }
    }

    async fn authorize_owner<S: AuthzSubject + ?Sized>(
        &self,
        context: &AppContext,
        requestor: &S,
    ) -> Result<(), ApiError> {
        if let Some(binding) = &self.binding {
            require_current_owner(context, requestor, binding.lookup().identity().principal())
                .await?;
        }
        Ok(())
    }
}

fn protection_pending() -> ApiError {
    ApiError::ServiceUnavailable(
        "Task receipt protection must finish before this submission can be resolved".into(),
    )
}

async fn principal_identity<S: AuthzSubject + ?Sized>(
    context: &impl crate::storage::StorageContext,
    requestor: &S,
) -> Result<PrincipalIdentity, ApiError> {
    let evidence = storage_handle(context)
        .resolve_logical_identity(StorageIdentityTarget::Principal(
            requestor.validated_principal_id()?,
        ))
        .await?;
    match evidence.subject() {
        Some(StorageIdentitySubject::Principal { identity }) => {
            identity.known_identity().ok_or_else(|| {
                ApiError::ServiceUnavailable(
                    "Task submission requires resolved principal identity".into(),
                )
            })
        }
        _ => Err(ApiError::ServiceUnavailable(
            "Task submission principal identity is unavailable".into(),
        )),
    }
}

async fn require_current_owner<S: AuthzSubject + ?Sized>(
    context: &impl AuthorizationContext,
    requestor: &S,
    owner: PrincipalIdentity,
) -> Result<PrincipalId, ApiError> {
    if principal_identity(context, requestor).await? != owner {
        return Err(ApiError::NotFound("Task submission not found".into()));
    }
    requestor.validated_principal_id()
}

async fn authorize_receipt<S: AuthzSubject + ?Sized>(
    context: &impl AuthorizationContext,
    requestor: &S,
    owner: PrincipalIdentity,
    task: TaskIdentity,
) -> Result<(), ApiError> {
    let current = require_current_owner(context, requestor, owner).await?;
    if let AuthorizationMode::Delegated(backend) = context.authorization_mode() {
        let principal = PrincipalRef::load(context, requestor).await?;
        let resource = ResourceRef::task_receipt(task, current);
        if backend.authorize_task(&principal, &resource).await? != PermissionDecision::Allow {
            return Err(ApiError::Forbidden("Permission denied".into()));
        }
    }
    Ok(())
}

async fn expired_error<S: AuthzSubject + ?Sized>(
    context: &impl AuthorizationContext,
    requestor: &S,
    receipt: ExpiredTaskSubmission,
) -> Result<ApiError, ApiError> {
    authorize_receipt(context, requestor, receipt.owner(), receipt.task()).await?;
    Ok(ApiError::TaskDetailsExpired(
        crate::models::task::TaskDetailsExpiredResponse::authorized(
            &AuthorizedExpiredTaskSubmission(receipt),
        ),
    ))
}

#[cfg(test)]
mod tests;
