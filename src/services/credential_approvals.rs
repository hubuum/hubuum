//! Fresh authentication authorizes exactly one bound credential operation.
use crate::errors::ApiError;
use crate::events::EventContext;
use crate::extractors::Authenticated;
use crate::models::credential_approval::{
    CredentialApprovalRecord, CredentialApprovalResponse, CredentialApprovalSecret,
    CredentialOperation,
};
use crate::models::{LoginUser, PrincipalID, User, configured_token_lifetime};
use crate::permissions::AppContext;
use crate::services::identity;
use crate::storage::storage_handle;
use chrono::{SubsecRound, Utc};
use hubuum_domain::{IdentityScopeId, TokenId, UserId};
use hubuum_storage_core::{
    IdentityScopeStorage, StorageCredentialApprovalCreate, StorageCredentialApprovalMetadata,
    StorageCredentialClaim, StorageCredentialUse, StorageError, StorageTokenCreate,
    StorageTokenRenew, StorageUserCreate, StorageUserUpdate, TokenStorage, UserStorage,
};

/// An authenticated human with an unscoped credential. The originating token
/// identity is retained all the way to the adapter's transactional recheck.
pub struct CredentialActor {
    user: User,
    token_id: TokenId,
}
impl CredentialActor {
    pub async fn resolve(
        context: &AppContext,
        authenticated: &Authenticated,
    ) -> Result<Self, ApiError> {
        if !authenticated.principal.is_human() || authenticated.scopes().is_some() {
            return Err(ApiError::Forbidden(
                "Credential approval requires an unscoped human credential".into(),
            ));
        }
        let user = identity::get_user(context, authenticated.principal.id().id()).await?;
        Ok(Self {
            user,
            token_id: authenticated.token_meta.id(),
        })
    }
    pub fn id(&self) -> i32 {
        self.user.id
    }
    fn claim(
        &self,
        secret: &CredentialApprovalSecret,
        operation: &CredentialOperation,
    ) -> Result<StorageCredentialClaim, ApiError> {
        let now = Utc::now().naive_utc().trunc_subsecs(6);
        let cutoff = configured_token_lifetime()?.cutoff_from(now)?.and_utc();
        let mut claim = StorageCredentialClaim::new(
            PrincipalID::new(self.user.id)?,
            self.token_id,
            operation.kind(),
            secret.digest(),
            secret.bind(operation)?,
            cutoff,
        )
        .target(operation.target());
        if let CredentialOperation::ConfirmRestore { restore_id, .. } = operation {
            claim = claim.restore_job(*restore_id);
        }
        Ok(claim)
    }
    async fn authorize(
        &self,
        context: &AppContext,
        operation: &CredentialOperation,
    ) -> Result<(), ApiError> {
        let admin = context.is_admin(&self.user).await?;
        match operation {
            CredentialOperation::CreateUser { .. }
            | CredentialOperation::UpdateUser { .. }
            | CredentialOperation::ImportCredentials { .. }
            | CredentialOperation::ConfirmRestore { .. } => {
                if !admin {
                    return Err(ApiError::Forbidden("Administrator access required".into()));
                }
            }
            CredentialOperation::CreateToken { principal_id, .. }
            | CredentialOperation::RenewToken { principal_id, .. } => {
                use crate::traits::PrincipalIdApplicationExt;
                let principal = principal_id.principal(context).await?;
                let allowed = admin
                    || if principal.principal_kind() == crate::models::PrincipalKind::Human {
                        self.user.id == principal_id.id()
                    } else {
                        let account =
                            identity::get_service_account(context, principal_id.id()).await?;
                        identity::is_human_owner_group_member(
                            context,
                            self.user.id,
                            account.owner_group_id,
                        )
                        .await?
                    };
                if !allowed {
                    return Err(ApiError::NotFound("Principal not found".into()));
                }
                if identity::is_service_account_disabled(context, principal_id.id()).await? {
                    return Err(ApiError::Conflict("Service account is disabled".into()));
                }
            }
        }
        Ok(())
    }
    /// Verify the stored provider identity, never a caller-selected account.
    pub async fn password_login(
        &self,
        context: &AppContext,
        password: String,
    ) -> Result<LoginUser, ApiError> {
        let point = identity::get_user_point(context, self.user.id).await?;
        let scope = storage_handle(context)
            .resolve_identity_scope_name(IdentityScopeId::new(point.identity_scope_id)?)
            .await?;
        let login = LoginUser {
            identity_scope: Some(scope),
            name: point.name,
            password,
        };
        login.validate()?;
        Ok(login)
    }
}

/// Cannot be constructed without checking current authority and binding the
/// exact request to an approval. Durable single use remains adapter-owned.
pub struct ApprovedCredentialOperation {
    operation: CredentialOperation,
    claim: StorageCredentialClaim,
}

pub struct FreshCredentialAuthentication {
    actor_id: i32,
    authenticated_at: chrono::DateTime<Utc>,
}
pub async fn verify_password(
    context: &AppContext,
    actor: &CredentialActor,
    login: LoginUser,
) -> Result<FreshCredentialAuthentication, ApiError> {
    let user = crate::auth::login(context, login).await?;
    if user.id != actor.id() {
        return Err(ApiError::Unauthorized("Authentication failed".into()));
    }
    Ok(FreshCredentialAuthentication {
        actor_id: user.id,
        authenticated_at: Utc::now().trunc_subsecs(6),
    })
}

pub async fn issue(
    context: &AppContext,
    actor: &CredentialActor,
    mut operation: CredentialOperation,
    proof: FreshCredentialAuthentication,
    event_context: &EventContext,
) -> Result<CredentialApprovalResponse, ApiError> {
    if proof.actor_id != actor.id() {
        return Err(ApiError::Unauthorized("Authentication failed".into()));
    }
    actor.authorize(context, &operation).await?;
    operation.resolve_defaults()?;
    if let CredentialOperation::CreateToken {
        principal_id,
        token,
    } = &operation
    {
        token.clone().into_create_request(*principal_id)?;
    }
    let secret = CredentialApprovalSecret::generate();
    let claim = actor.claim(&secret, &operation)?;
    let authenticated_at = proof.authenticated_at;
    let metadata = storage_handle(context)
        .create_credential_approval(StorageCredentialApprovalCreate::new(
            claim,
            authenticated_at,
            event_context.clone(),
        ))
        .await?
        .into_value();
    tracing::info!(
        message = "Credential approval issued",
        approval_id = metadata.id(),
        actor_id = actor.id(),
        operation = operation.kind().as_str()
    );
    Ok(CredentialApprovalResponse {
        approval: secret,
        record: metadata.into(),
        token_expires_at: operation.token_expiry(),
    })
}

pub async fn approve(
    context: &AppContext,
    actor: &CredentialActor,
    operation: CredentialOperation,
    secret: CredentialApprovalSecret,
) -> Result<ApprovedCredentialOperation, ApiError> {
    actor.authorize(context, &operation).await?;
    // Defaults were resolved when approving. Omitting expiry now changes the
    // fingerprint and cannot silently extend a previously authorized lifetime.
    let claim = actor.claim(&secret, &operation)?;
    Ok(ApprovedCredentialOperation { operation, claim })
}

pub async fn get(
    context: &AppContext,
    actor: &CredentialActor,
    id: i32,
) -> Result<CredentialApprovalRecord, ApiError> {
    let metadata: StorageCredentialApprovalMetadata =
        storage_handle(context).get_credential_approval(id).await?;
    if metadata.actor_id().id() != actor.id() && !context.is_admin(&actor.user).await? {
        return Err(ApiError::NotFound("Credential approval not found".into()));
    }
    Ok(metadata.into())
}

pub async fn mint_token(
    context: &AppContext,
    approved: ApprovedCredentialOperation,
    event_context: &EventContext,
) -> Result<crate::models::IssuedToken, ApiError> {
    let raw = crate::utilities::auth::generate_token();
    let policy = identity::token_policy(crate::models::token::configured_token_issuance_policy()?);
    let audit = approved.claim.clone();
    let backend = storage_handle(context);
    let result = match approved.operation {
        CredentialOperation::CreateToken {
            principal_id,
            token,
        } => {
            let parts = token.into_create_request(principal_id)?.into_parts();
            let request = StorageTokenCreate::new(
                principal_id,
                raw.storage_digest()?,
                policy,
                event_context.clone(),
            )
            .name(parts.name)
            .description(parts.description)
            .expires_at(parts.expires_at.map(|at| at.and_utc()))
            .scope(parts.scope.as_ref().map(identity::token_scope_to_storage))
            .with_credential_claim(approved.claim);
            backend.create_token(request).await
        }
        CredentialOperation::RenewToken {
            principal_id,
            token_id,
            token,
        } => {
            backend
                .renew_token(
                    StorageTokenRenew::new(
                        token_id,
                        principal_id,
                        raw.storage_digest()?,
                        token.expires_at.map(|at| at.and_utc()),
                        policy,
                        event_context.clone(),
                    )
                    .with_credential_claim(approved.claim),
                )
                .await
        }
        _ => {
            return Err(ApiError::InternalServerError(
                "Invalid approved credential operation".into(),
            ));
        }
    };
    let metadata = observe_consumption(result, &audit)?.into_value();
    let expiry = metadata
        .expires_at()
        .ok_or_else(|| ApiError::InternalServerError("Issued token expiry missing".into()))?;
    Ok(crate::models::IssuedToken::new(raw, expiry.naive_utc()))
}

pub async fn write_user(
    context: &AppContext,
    approved: ApprovedCredentialOperation,
    event_context: &EventContext,
) -> Result<User, ApiError> {
    let audit = approved.claim.clone();
    let backend = storage_handle(context);
    let result = match approved.operation {
        CredentialOperation::CreateUser { user } => {
            let user = user.hash_password().await?;
            backend
                .create_user(
                    StorageUserCreate::new(
                        user.identity_scope,
                        user.name,
                        user.password,
                        user.proper_name,
                        user.email,
                        event_context.clone(),
                    )
                    .with_credential_claim(approved.claim),
                )
                .await
        }
        CredentialOperation::UpdateUser { user_id, user } => {
            let user = user.hash_password().await?;
            backend
                .update_user(
                    StorageUserUpdate::new(
                        UserId::new(user_id.id())?,
                        user.password,
                        user.proper_name,
                        user.email,
                        event_context.clone(),
                    )
                    .with_credential_claim(approved.claim),
                )
                .await
        }
        _ => {
            return Err(ApiError::InternalServerError(
                "Invalid approved user operation".into(),
            ));
        }
    };
    Ok(identity::user_from_storage(
        observe_consumption(result, &audit)?.into_value(),
    ))
}

fn observe_consumption<T>(
    result: Result<T, StorageError>,
    claim: &StorageCredentialClaim,
) -> Result<T, ApiError> {
    match result {
        Ok(value) => {
            tracing::info!(
                message = "Credential approval consumed with committed mutation",
                actor_id = claim.actor_id().id(),
                origin_token_id = claim.token_id().id(),
                operation = claim.operation().as_str()
            );
            Ok(value)
        }
        Err(error) => {
            tracing::warn!(
                message = "Credential approval mutation rejected",
                actor_id = claim.actor_id().id(),
                origin_token_id = claim.token_id().id(),
                operation = claim.operation().as_str(),
                reason = error.kind().as_str()
            );
            Err(error.into())
        }
    }
}

pub(crate) fn approved_import(
    approved: ApprovedCredentialOperation,
    context: &EventContext,
) -> Result<(crate::models::ImportRequest, StorageCredentialUse), ApiError> {
    match approved.operation {
        CredentialOperation::ImportCredentials { import } => Ok((
            *import,
            StorageCredentialUse::new(approved.claim, context.clone()),
        )),
        _ => Err(ApiError::InternalServerError(
            "Invalid approved import operation".into(),
        )),
    }
}

pub(crate) async fn confirm_restore(
    context: &AppContext,
    approved: ApprovedCredentialOperation,
    event_context: &EventContext,
) -> Result<crate::models::RestoreStageResponse, ApiError> {
    match approved.operation {
        CredentialOperation::ConfirmRestore {
            restore_id,
            confirmation,
        } => {
            let actor_id = approved.claim.actor_id().id();
            let result = crate::restores::confirm_restore_with_approval(
                context,
                restore_id,
                &confirmation,
                Some(StorageCredentialUse::new(
                    approved.claim,
                    event_context.clone(),
                )),
            )
            .await;
            match &result {
                Ok(_) => tracing::info!(
                    message = "Credential approval consumed with restore confirmation",
                    actor_id,
                    restore_id = restore_id.id()
                ),
                Err(error) => tracing::warn!(
                    message = "Credential-approved restore confirmation rejected",
                    actor_id,
                    restore_id = restore_id.id(),
                    reason = error.class()
                ),
            }
            result
        }
        _ => Err(ApiError::InternalServerError(
            "Invalid approved restore operation".into(),
        )),
    }
}
