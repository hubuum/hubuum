use crate::{StorageError, StorageMutationOutcome, StoragePage};
use async_trait::async_trait;
use hubuum_domain::ResourceRevision;
use hubuum_domain::{GroupId, IdentityScopeId, PrincipalId, ServiceAccountId, TokenId, UserId};
use hubuum_events_core::EventContext;
use hubuum_query::QueryOptions;

pub use crate::IdentityStorage;
pub use crate::auth_interactions::*;
pub use crate::auth_sessions::*;
pub use crate::authorization::*;
pub use crate::authorization_resources::*;
pub use crate::collection_authorization::*;
pub use crate::credential_activity::*;
pub use crate::credential_approval::*;
pub use crate::identity::*;
pub use crate::identity_operations::*;
pub use crate::identity_resources::*;
pub use crate::identity_tokens::*;
pub use crate::identity_users::*;
pub use crate::upstream_authentication::*;

/// Authentication data every selectable storage backend must provide.
///
/// Implementations own persistence joins and row decoding. Consumers receive
/// only the projections required to build application authentication state.
#[async_trait]
pub trait AuthenticationStorage: Send + Sync {
    async fn create_auth_interaction(
        &self,
        request: crate::StorageInteractionCreate,
    ) -> Result<crate::StorageInteractionView, StorageError>;
    async fn bind_auth_interaction_browser(
        &self,
        code: hubuum_domain::InteractionDigest,
        browser: hubuum_domain::InteractionBrowserBinding,
    ) -> Result<crate::StorageInteractionOutcome, StorageError>;
    async fn advance_auth_interaction(
        &self,
        command: crate::StorageInteractionCommand,
    ) -> Result<crate::StorageInteractionOutcome, StorageError>;
    async fn complete_auth_interaction(
        &self,
        request: crate::StorageInteractionComplete,
    ) -> Result<crate::StorageInteractionCompletion, StorageError>;
    async fn revoke_auth_interaction_session(
        &self,
        access: crate::StorageInteractionAccess,
        context: hubuum_events_core::EventContext,
    ) -> Result<crate::StorageMutationOutcome<Option<crate::StorageUpstreamSession>>, StorageError>;
    async fn purge_auth_interactions(&self, limit: usize) -> Result<usize, StorageError>;

    async fn record_credential_creation(
        &self,
        token: TokenId,
        creation: crate::StorageCredentialCreation,
    ) -> Result<(), StorageError>;
    async fn observe_credential_source(
        &self,
        token: TokenId,
        source: crate::ObservedSource,
        policy: crate::CredentialActivityPolicy,
    ) -> Result<(), StorageError>;
    async fn credential_activity(
        &self,
        query: crate::StorageActivityQuery,
    ) -> Result<crate::StorageCredentialActivity, StorageError>;
    async fn purge_credential_sources(
        &self,
        policy: crate::CredentialActivityPolicy,
        limit: usize,
    ) -> Result<usize, StorageError>;

    async fn create_auth_session(
        &self,
        request: crate::StorageSessionCreate,
    ) -> Result<crate::StorageMutationOutcome<crate::StorageSessionIssued>, StorageError>;
    async fn refresh_principal(
        &self,
        credential: crate::RefreshCredential,
    ) -> Result<PrincipalId, StorageError>;
    async fn rotate_auth_session(
        &self,
        request: crate::StorageSessionRotate,
    ) -> Result<crate::StorageMutationOutcome<crate::StorageSessionRotation>, StorageError>;
    async fn revoke_auth_sessions(
        &self,
        target: crate::SessionRevocation,
        context: hubuum_events_core::EventContext,
    ) -> Result<crate::StorageMutationOutcome<crate::StorageTokenRevocations>, StorageError>;
    async fn purge_auth_sessions(&self, limit: usize) -> Result<usize, StorageError>;

    /// Validate one presented bearer credential and return its minimal
    /// authentication projection.
    ///
    /// Implementations must reject revoked and expired credentials, reject
    /// credentials owned by disabled principals, and may update non-security
    /// usage telemetry without failing an otherwise successful validation.
    async fn authenticate_bearer_token(
        &self,
        attempt: StorageAuthenticationAttempt,
    ) -> Result<StorageAuthenticatedToken, StorageError>;

    async fn get_authentication_identity(
        &self,
        principal_id: PrincipalId,
    ) -> Result<StorageAuthenticationIdentity, StorageError>;

    async fn get_authentication_token_scope(
        &self,
        query: StorageAuthenticationTokenScopeQuery,
    ) -> Result<Option<StorageAuthenticationTokenScope>, StorageError>;
}

/// Complete human-user persistence required of every selectable backend.
#[async_trait]
pub trait UserStorage: Send + Sync {
    async fn get_user(&self, id: UserId) -> Result<StorageUser, StorageError>;

    async fn get_user_by_name(
        &self,
        identity_scope: String,
        name: String,
    ) -> Result<StorageUser, StorageError>;

    async fn get_user_details(&self, id: UserId) -> Result<StorageUserDetails, StorageError>;

    async fn list_users(
        &self,
        query: StorageUserListQuery,
    ) -> Result<StoragePage<StorageUserListItem>, StorageError>;

    async fn create_user(
        &self,
        request: StorageUserCreate,
    ) -> Result<StorageMutationOutcome<StorageUser>, StorageError>;

    async fn update_user(
        &self,
        request: StorageUserUpdate,
    ) -> Result<StorageMutationOutcome<StorageUser>, StorageError>;

    /// Replace a local password and revoke active credentials atomically.
    async fn set_user_password(
        &self,
        request: StorageUserPasswordUpdate,
    ) -> Result<StorageMutationOutcome<usize>, StorageError>;

    async fn delete_user(
        &self,
        request: StorageUserDelete,
    ) -> Result<StorageMutationOutcome<usize>, StorageError>;

    async fn anonymize_user(
        &self,
        request: StorageUserAnonymize,
    ) -> Result<StorageMutationOutcome<()>, StorageError>;
}

/// Complete group lifecycle behavior required from a backend.
#[async_trait]
pub trait GroupStorage: Send + Sync {
    /// List groups with stable filtering, cursor pagination, and an optional
    /// exact total.
    async fn list_groups(
        &self,
        query: StorageGroupListQuery,
    ) -> Result<StoragePage<StorageIdentityGroup>, StorageError>;

    async fn get_group(&self, group_id: GroupId) -> Result<StorageIdentityGroup, StorageError>;

    async fn resolve_group_identity_scope_name(
        &self,
        group_id: GroupId,
    ) -> Result<String, StorageError>;

    async fn create_group(
        &self,
        command: StorageGroupCreate,
        context: &EventContext,
    ) -> Result<crate::StorageMutationOutcome<StorageIdentityGroup>, StorageError>;

    async fn update_group(
        &self,
        group_id: GroupId,
        update: StorageGroupUpdate,
        context: &EventContext,
    ) -> Result<crate::StorageMutationOutcome<StorageIdentityGroup>, StorageError>;

    async fn delete_group(
        &self,
        group_id: GroupId,
        context: &EventContext,
    ) -> Result<crate::StorageMutationOutcome<usize>, StorageError>;
}

/// Principal point and settings behavior required from every backend.
#[async_trait]
pub trait PrincipalStorage: Send + Sync {
    async fn get_principal(
        &self,
        principal_id: PrincipalId,
    ) -> Result<StoragePrincipal, StorageError>;

    async fn get_principal_settings(
        &self,
        principal_id: PrincipalId,
    ) -> Result<StoragePrincipalSettings, StorageError>;

    async fn update_principal_settings(
        &self,
        principal_id: PrincipalId,
        mutation: StoragePrincipalSettingsMutation,
        context: &EventContext,
    ) -> Result<crate::StorageMutationOutcome<StoragePrincipalSettings>, StorageError>;
}

/// Initial local-identity bootstrap and credential-recovery operations.
#[async_trait]
pub trait LocalIdentityCredentialStorage: Send + Sync {
    /// Return whether the backend is empty enough to require its initial local
    /// administrator. This is an optimization; the atomic bootstrap operation
    /// must repeat the check under its backend-native coordination primitive.
    async fn is_default_admin_bootstrap_required(&self) -> Result<bool, StorageError>;

    /// Atomically create the initial local administrator when still required.
    /// Concurrent callers must produce at most one administrator and all later
    /// callers return `false`.
    async fn bootstrap_default_admin(
        &self,
        request: StorageDefaultAdminBootstrap,
    ) -> Result<bool, StorageError>;

    /// Replace one local human credential and atomically revoke every active
    /// bearer token owned by that principal. The credential change, token
    /// revocations, and returned audit receipt share one commit boundary.
    async fn reset_local_password(
        &self,
        request: StorageLocalPasswordReset,
    ) -> Result<StorageMutationOutcome<usize>, StorageError>;
}

/// Identity-provider scope discovery and reconciliation.
#[async_trait]
pub trait IdentityScopeStorage: Send + Sync {
    /// Create the named scope if absent, or reconcile its provider kind when it
    /// already exists, and return the authoritative stored row.
    async fn ensure_identity_scope(
        &self,
        request: StorageIdentityScopeEnsure,
    ) -> Result<StorageIdentityScope, StorageError>;

    /// Resolve one scope ID to its name, returning `NotFound` when it does not
    /// exist.
    async fn resolve_identity_scope_name(
        &self,
        scope_id: IdentityScopeId,
    ) -> Result<String, StorageError>;

    /// Resolve every distinct requested scope ID.
    ///
    /// An empty request returns an empty result. A non-empty request must fail
    /// rather than return a partial mapping when any ID cannot be resolved.
    async fn resolve_identity_scope_names(
        &self,
        scope_ids: Vec<IdentityScopeId>,
    ) -> Result<Vec<(IdentityScopeId, String)>, StorageError>;
}

/// Principal-to-group membership reads and mutations.
#[async_trait]
pub trait GroupMembershipStorage: Send + Sync {
    /// Load one effective principal-to-group membership with its authoritative
    /// revision, returning `NotFound` when no membership source remains.
    async fn get_principal_group(
        &self,
        principal_id: PrincipalId,
        group_id: GroupId,
    ) -> Result<StoragePrincipalGroup, StorageError>;

    /// List every effective group membership for one principal with stable
    /// filtering, cursor pagination, and optional exact total.
    async fn list_principal_groups(
        &self,
        query: StoragePrincipalGroupListQuery,
    ) -> Result<StoragePage<StorageIdentityGroup>, StorageError>;

    /// Return whether the principal is both human and an effective member of
    /// the service account owner group.
    async fn is_human_owner_group_member(
        &self,
        principal_id: PrincipalId,
        owner_group_id: GroupId,
    ) -> Result<bool, StorageError>;

    /// Load every principal in one group for policy evaluation.
    async fn load_group_member_principals(
        &self,
        group_id: GroupId,
    ) -> Result<Vec<StoragePrincipal>, StorageError>;

    /// List group memberships with stable filtering, cursor pagination, and an
    /// optional exact total.
    async fn list_group_members(
        &self,
        group_id: GroupId,
        query_options: QueryOptions,
    ) -> Result<StoragePage<StorageGroupMember>, StorageError>;

    async fn add_group_member(
        &self,
        principal_id: PrincipalId,
        group_id: GroupId,
        context: &EventContext,
    ) -> Result<StorageMutationOutcome<StoragePrincipalGroup>, StorageError>;

    async fn remove_group_member(
        &self,
        principal_id: PrincipalId,
        group_id: GroupId,
        context: &EventContext,
    ) -> Result<StorageMutationOutcome<()>, StorageError>;
}

/// Service-account lifecycle and authorization state.
#[async_trait]
pub trait ServiceAccountStorage: Send + Sync {
    /// Return `true` only for a disabled service-account principal.
    ///
    /// Human principals and IDs without a service-account row return `false`.
    async fn is_service_account_disabled(
        &self,
        principal_id: PrincipalId,
    ) -> Result<bool, StorageError>;

    /// Load the service-account row for one principal ID.
    async fn get_service_account(
        &self,
        service_account_id: ServiceAccountId,
    ) -> Result<StorageServiceAccount, StorageError>;

    /// Load one service account together with the principal-owned name, scope,
    /// and revision needed for a strong point response.
    async fn get_service_account_details(
        &self,
        service_account_id: ServiceAccountId,
    ) -> Result<StorageServiceAccountDetails, StorageError>;

    /// List service accounts manageable by the requestor, applying owner-group
    /// authorization, administrator override, filtering, stable paging, and an
    /// optional exact total inside the backend.
    async fn list_manageable_service_accounts(
        &self,
        query: StorageServiceAccountListQuery,
    ) -> Result<StoragePage<StorageServiceAccountListItem>, StorageError>;

    /// Atomically create one local service account, its principal projection,
    /// and the required lifecycle event.
    async fn create_service_account(
        &self,
        request: StorageServiceAccountCreate,
    ) -> Result<StorageMutationOutcome<StorageServiceAccount>, StorageError>;

    /// Atomically apply a service-account patch and its required lifecycle
    /// event, preserving no-op revision behavior.
    async fn update_service_account(
        &self,
        request: StorageServiceAccountUpdate,
    ) -> Result<StorageMutationOutcome<StorageServiceAccount>, StorageError>;

    /// Atomically disable a service account, revoke its active credentials,
    /// cancel its pending work, and emit the required lifecycle event.
    async fn disable_service_account(
        &self,
        request: StorageServiceAccountMutation,
    ) -> Result<StorageMutationOutcome<StorageServiceAccountDisableOutcome>, StorageError>;

    /// Atomically delete an eligible service account and emit the required
    /// lifecycle event, enforcing backend-owned deletion constraints.
    async fn delete_service_account(
        &self,
        request: StorageServiceAccountMutation,
    ) -> Result<StorageMutationOutcome<()>, StorageError>;
}

/// External-provider identity refresh and reconciliation.
#[async_trait]
pub trait ExternalIdentityStorage: Send + Sync {
    /// Resolve an eligible retained session for this fresh proof without leasing
    /// or exposing its old credential. Replacement commits with the interaction.
    async fn upstream_approval_session(
        &self,
        proof: hubuum_domain::FreshExternalAuthentication,
    ) -> Result<Option<crate::StorageUpstreamApproval>, StorageError>;
    /// Atomically remove the recoverable token and lease its single renewal attempt.
    async fn claim_upstream_credential(
        &self,
        revision: ResourceRevision,
        principal: PrincipalId,
    ) -> Result<crate::StorageUpstreamClaimOutcome, StorageError>;
    /// Reconcile current identity/memberships and replace the rotated encrypted token atomically.
    async fn finish_upstream_renewal(
        &self,
        request: crate::StorageUpstreamRenewal,
    ) -> Result<(), StorageError>;
    async fn fail_upstream_renewal(
        &self,
        claim: crate::StorageUpstreamClaim,
        failure: crate::StorageUpstreamFailure,
    ) -> Result<(), StorageError>;
    /// Capture credentials for best-effort revocation, after the caller authorizes logout.
    /// Local session revocation must destroy the stored copies before outbound calls.
    async fn upstream_logout_credentials(
        &self,
        target: crate::StorageUpstreamRevocation,
    ) -> Result<Vec<crate::StorageUpstreamSession>, StorageError>;
    /// Resolve only the immutable provider scope and subject, never a profile name or email.
    async fn find_external_identity(
        &self,
        identity: hubuum_domain::ExternalIdentity,
    ) -> Result<Option<PrincipalId>, StorageError>;

    /// Return refresh state only for a provider-managed external human.
    ///
    /// Missing, local, and unmanaged principals return `None`; inconsistent
    /// external records fail closed.
    async fn get_external_principal_state(
        &self,
        principal_id: PrincipalId,
    ) -> Result<Option<StorageExternalPrincipalState>, StorageError>;

    /// Record a failed refresh. Rejection atomically invalidates cached authority,
    /// pending login evidence, and active tokens; an outage preserves the last success.
    async fn record_external_sync_failure(
        &self,
        revision: ResourceRevision,
        principal_id: PrincipalId,
        failure: StorageExternalSyncFailure,
    ) -> Result<(), StorageError>;

    /// Atomically reconcile one external human, its profile, effective external
    /// group memberships, synchronization timestamps, and lifecycle events.
    async fn sync_external_user(
        &self,
        revision: ResourceRevision,
        request: StorageExternalUserSync,
    ) -> Result<StorageMutationOutcome<StorageSyncedHuman>, StorageError>;
}

/// Complete bearer-token lifecycle required of every selectable backend.
#[async_trait]
pub trait TokenStorage: Send + Sync {
    /// Persist an operation-bound approval and its audit event atomically.
    async fn create_credential_approval(
        &self,
        request: crate::StorageCredentialApprovalCreate,
    ) -> Result<StorageMutationOutcome<crate::StorageCredentialApprovalMetadata>, StorageError>;
    /// Non-secret retained evidence; callers enforce visibility before returning it.
    async fn get_credential_approval(
        &self,
        id: i32,
    ) -> Result<crate::StorageCredentialApprovalMetadata, StorageError>;

    /// Return hash-free retained token metadata using the requested lifecycle
    /// state, filters, stable cursor page, and optional exact total.
    async fn list_retained_tokens(
        &self,
        query: StorageTokenListQuery,
    ) -> Result<StoragePage<StorageTokenMetadata>, StorageError>;

    /// Aggregate persisted token usage by non-secret hash key identity.
    async fn token_key_usage(
        &self,
        observation: StorageTokenObservation,
    ) -> Result<Vec<StorageTokenKeyUsage>, StorageError>;

    /// Commit token.created and, for a login request, authentication.succeeded
    /// with its supplied immutable subject in the same transaction. Return both
    /// audit receipts; never derive the subject from later identity state.
    async fn create_token(
        &self,
        request: StorageTokenCreate,
    ) -> Result<StorageMutationOutcome<StorageTokenMetadata>, StorageError>;

    async fn renew_token(
        &self,
        request: StorageTokenRenew,
    ) -> Result<StorageMutationOutcome<StorageTokenMetadata>, StorageError>;

    async fn get_token_metadata(
        &self,
        principal_id: PrincipalId,
        token_id: TokenId,
        observation: StorageTokenObservation,
    ) -> Result<StorageTokenMetadata, StorageError>;

    /// Load metadata for token IDs in the same order, including duplicates.
    async fn load_token_metadata_by_ids(
        &self,
        token_ids: Vec<TokenId>,
        observation: StorageTokenObservation,
    ) -> Result<Vec<StorageTokenMetadata>, StorageError>;

    async fn revoke_token(
        &self,
        request: StorageTokenRevoke,
    ) -> Result<StorageMutationOutcome<StorageTokenRevocations>, StorageError>;

    async fn revoke_token_by_hash(
        &self,
        request: StorageTokenHashRevoke,
    ) -> Result<StorageMutationOutcome<Option<crate::StorageUpstreamSession>>, StorageError>;

    async fn revoke_all_principal_tokens(
        &self,
        request: StoragePrincipalTokensRevoke,
    ) -> Result<StorageMutationOutcome<StorageTokenRevocations>, StorageError>;
}
