//! Strict transport requests for scoped history retention.

use chrono::{DateTime, Utc};
use hubuum_domain::{ResourceId, ResourceIdentity};
use hubuum_storage_core::{
    HistoryAuthorityRevision, HistoryExpiryCancellation, HistoryExpiryDecision,
    HistoryExpiryInputBytes, HistoryExpiryIntent, HistoryExpiryManifest, HistoryExpiryOrigin,
    HistoryExpiryReadSpan, HistoryExpiryRemovalReceipt, HistoryHoldRevision,
    HistoryLifecycleOperationEvidence, HistoryLifecycleScan, HistoryPolicyAction,
    HistoryPolicyApplication, HistoryPolicyApplyRequest, HistoryPolicyBinding, HistoryPolicyChange,
    HistoryPolicyKey, HistoryPolicyRecord, HistoryPolicyReference, HistoryPolicyReviewEvidence,
    HistoryPolicyReviewReference, HistoryPolicyRevision, HistoryPolicyScope,
    HistoryPreviewCoverage, HistoryRetentionAnchor, HistoryReviewFence, HistoryScopeAttribution,
    HistorySizeTransition, LogicalIdentityStorage, StorageIdentityHistoryKind,
    StorageIdentityResourceKind, StorageIdentityTarget, StorageRecordIdentity,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use utoipa::ToSchema;
use uuid::Uuid;

use crate::errors::ApiError;
use crate::permissions::AppContext;
use crate::storage::storage_handle;

#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum HistoryPolicyScopeRequest {
    Deployment,
    Collection {
        #[schema(value_type = String, format = "uuid")]
        identity: ResourceIdentity,
        #[serde(
            default,
            deserialize_with = "present_optional",
            skip_serializing_if = "Option::is_none"
        )]
        #[schema(nullable = false)]
        current_id: Option<ResourceId>,
    },
    Class {
        #[schema(value_type = String, format = "uuid")]
        identity: ResourceIdentity,
        #[serde(
            default,
            deserialize_with = "present_optional",
            skip_serializing_if = "Option::is_none"
        )]
        #[schema(nullable = false)]
        current_id: Option<ResourceId>,
    },
}
impl HistoryPolicyScopeRequest {
    async fn resolve(self, context: &AppContext) -> Result<HistoryPolicyScope, ApiError> {
        let (scope, kind, identity, current_id) = match self {
            Self::Deployment => return Ok(HistoryPolicyScope::Deployment),
            Self::Collection {
                identity,
                current_id,
            } => (
                HistoryPolicyScope::Collection { identity },
                StorageIdentityResourceKind::Collection,
                identity,
                current_id,
            ),
            Self::Class {
                identity,
                current_id,
            } => (
                HistoryPolicyScope::Class { identity },
                StorageIdentityResourceKind::Class,
                identity,
                current_id,
            ),
        };
        if let Some(id) = current_id {
            let evidence = storage_handle(context)
                .resolve_logical_identity(StorageIdentityTarget::Resource { kind, id })
                .await?;
            if evidence
                .subject()
                .and_then(|subject| subject.record_identity())
                .map(|target| target.resource())
                != Some(identity)
            {
                return Err(ApiError::BadRequest("Current integer ID and stable policy scope identity refer to different resources".into()));
            }
        }
        Ok(scope)
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct HistoryPolicyChangeRequest {
    action: HistoryPolicyAction,
    family: StorageIdentityHistoryKind,
    scope: HistoryPolicyScopeRequest,
    expected_policy_revision: HistoryPolicyRevision,
    binding: HistoryPolicyBinding,
    application: HistoryPolicyApplication,
}
impl HistoryPolicyChangeRequest {
    pub async fn resolve(self, context: &AppContext) -> Result<HistoryPolicyChange, ApiError> {
        let HistoryPolicyAction::ReplacePolicy = self.action;
        let scope = self.scope.resolve(context).await?;
        let key = HistoryPolicyKey::new(self.family, scope)
            .map_err(|error| ApiError::BadRequest(error.to_string()))?;
        HistoryPolicyChange::new(
            key,
            self.expected_policy_revision,
            self.binding,
            self.application,
        )
        .map_err(|error| ApiError::BadRequest(error.to_string()))
    }
}
#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct HistoryPolicyPreviewRequest {
    pub change: HistoryPolicyChangeRequest,
}
#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct HistoryPolicyApplicationRequest {
    proposal: HistoryPolicyChangeRequest,
    #[serde(
        default,
        deserialize_with = "present_optional",
        skip_serializing_if = "Option::is_none"
    )]
    #[schema(nullable = false)]
    review: Option<HistoryPolicyReviewReference>,
}
impl HistoryPolicyApplicationRequest {
    pub async fn resolve(
        self,
        context: &AppContext,
    ) -> Result<HistoryPolicyApplyRequest, ApiError> {
        Ok(HistoryPolicyApplyRequest::new(
            self.proposal.resolve(context).await?,
            self.review,
        ))
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct HistoryLifecycleInspectionRequest {
    family: StorageIdentityHistoryKind,
    scope: HistoryPolicyScopeRequest,
    limit: u32,
}
impl HistoryLifecycleInspectionRequest {
    pub async fn resolve(self, context: &AppContext) -> Result<HistoryLifecycleScan, ApiError> {
        let key = HistoryPolicyKey::new(self.family, self.scope.resolve(context).await?)
            .map_err(|error| ApiError::BadRequest(error.to_string()))?;
        HistoryLifecycleScan::new(key, self.limit)
            .map_err(|error| ApiError::BadRequest(error.to_string()))
    }
}
#[derive(Clone, Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct HistoryPolicyListRequest {
    #[serde(default = "initial_revision")]
    pub after_revision: HistoryPolicyRevision,
    #[serde(default = "page_limit")]
    pub limit: u16,
}
fn initial_revision() -> HistoryPolicyRevision {
    HistoryPolicyRevision::new(0).expect("zero revision")
}
fn page_limit() -> u16 {
    100
}
#[derive(Serialize, ToSchema)]
pub struct HistoryPolicyPage {
    pub authority_revision: HistoryAuthorityRevision,
    pub protected_authority_revision: Option<HistoryAuthorityRevision>,
    pub protection: HistoryAuthorityProtection,
    pub records: Vec<HistoryPolicyRecord>,
    pub next_revision: Option<HistoryPolicyRevision>,
}
#[derive(Serialize, ToSchema)]
pub struct HistoryLifecycleInspection {
    pub inspected_at: DateTime<Utc>,
    pub authority_revision: HistoryAuthorityRevision,
    pub coverage: HistoryPreviewCoverage,
    pub scanned: String,
    pub unresolved_identities: String,
    pub eligible_observed: String,
    pub samples: Vec<HistoryExpiryDecision>,
}

#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct HistoryRetentionHoldRequest {
    #[schema(value_type = String, format = "uuid")]
    pub identity: Uuid,
    pub family: StorageIdentityHistoryKind,
    pub scope: HistoryPolicyScopeRequest,
    pub expected_authority_revision: HistoryAuthorityRevision,
    #[serde(default, deserialize_with = "optional_utc")]
    pub expires_at: Option<DateTime<Utc>>,
}
impl HistoryRetentionHoldRequest {
    pub async fn key(&self, context: &AppContext) -> Result<HistoryPolicyKey, ApiError> {
        HistoryPolicyKey::new(self.family, self.scope.clone().resolve(context).await?)
            .map_err(|error| ApiError::BadRequest(error.to_string()))
    }
}
fn optional_utc<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<DateTime<Utc>>, D::Error> {
    let value = Option::<String>::deserialize(deserializer)?;
    value
        .map(|value| {
            if !(value.ends_with('Z') || value.ends_with("+00:00")) {
                return Err(serde::de::Error::custom(
                    "Hold expiry must use RFC 3339 UTC (Z or +00:00)",
                ));
            }
            DateTime::parse_from_rfc3339(&value)
                .map(|at| at.with_timezone(&Utc))
                .map_err(serde::de::Error::custom)
        })
        .transpose()
}
#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct HistoryRetentionHoldReleaseRequest {
    pub expected_authority_revision: HistoryAuthorityRevision,
}

#[derive(Clone, Copy, Debug, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum HistoryAuthorityProtection {
    Protected,
    Pending,
    RecoveryRequired,
    Unconfigured,
    Unavailable,
}

pub const HISTORY_LIFECYCLE_MAX_REQUEST_BYTES: usize = 16 * 1024;
fn present_optional<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    T::deserialize(deserializer).map(Some)
}

/// Public projection of administrator evidence. Original authorization facts
/// remain outside this transport shape; protection is independently verified.
#[derive(Debug, Serialize, ToSchema)]
#[serde(tag = "kind", content = "record", rename_all = "snake_case")]
pub enum HistoryLifecycleOperationResponse {
    Policy(HistoryPolicyRecord),
    Hold(HistoryHoldRevision),
    SizePressure(HistorySizeTransition),
    ExpiryIntent(HistoryExpiryOperationRecord),
    Review {
        proposal: HistoryPolicyChange,
        review: HistoryPolicyReviewEvidence,
    },
}

/// Digest of the original complete manifest, not a durability/removal receipt.
#[derive(Debug, Serialize, ToSchema)]
#[serde(transparent)]
#[schema(value_type = String, pattern = "^[0-9a-f]{64}$")]
pub struct HistoryExpiryManifestDigest(String);

#[derive(Debug, Serialize, ToSchema)]
pub struct HistoryExpiryOperationRecord {
    manifest: HistoryExpiryManifestResponse,
    manifest_digest: HistoryExpiryManifestDigest,
    cancellation: Option<HistoryExpiryCancellation>,
    removals: Vec<HistoryExpiryRemovalReceipt>,
}
#[derive(Debug, Serialize, ToSchema)]
struct HistoryExpiryManifestResponse {
    identity: Uuid,
    run: Uuid,
    origin: HistoryExpiryOrigin,
    key: HistoryPolicyKey,
    fence: HistoryReviewFence,
    selected_at: DateTime<Utc>,
    input_bytes: u32,
    members: Vec<HistoryExpiryMemberResponse>,
}
#[derive(Debug, Serialize, ToSchema)]
struct HistoryExpiryMemberResponse {
    family: StorageIdentityHistoryKind,
    target: StorageRecordIdentity,
    anchor: HistoryRetentionAnchor,
    read_span: HistoryExpiryReadSpan,
    attributions: Vec<HistoryScopeAttribution>,
    policies: Vec<HistoryPolicyReference>,
    input_bytes: HistoryExpiryInputBytes,
}

impl TryFrom<HistoryLifecycleOperationEvidence> for HistoryLifecycleOperationResponse {
    type Error = ApiError;
    fn try_from(value: HistoryLifecycleOperationEvidence) -> Result<Self, Self::Error> {
        Ok(match value {
            HistoryLifecycleOperationEvidence::Policy(record) => Self::Policy(record),
            HistoryLifecycleOperationEvidence::Hold(record) => Self::Hold(record),
            HistoryLifecycleOperationEvidence::SizePressure(record) => Self::SizePressure(record),
            HistoryLifecycleOperationEvidence::Review { proposal, review } => {
                Self::Review { proposal, review }
            }
            HistoryLifecycleOperationEvidence::ExpiryIntent(intent) => {
                Self::ExpiryIntent(HistoryExpiryOperationRecord::from_intent(intent)?)
            }
        })
    }
}
impl HistoryExpiryOperationRecord {
    fn from_intent(intent: HistoryExpiryIntent) -> Result<Self, ApiError> {
        let manifest = intent.manifest();
        let encoded = serde_json::to_vec(manifest).map_err(|_| {
            ApiError::InternalServerError("Cannot encode original history expiry evidence".into())
        })?;
        let manifest_digest = HistoryExpiryManifestDigest(
            Sha256::digest(encoded)
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect(),
        );
        Ok(Self {
            manifest: HistoryExpiryManifestResponse::from_manifest(manifest),
            manifest_digest,
            cancellation: intent.cancellation().cloned(),
            removals: intent.removals().to_vec(),
        })
    }
}
impl HistoryExpiryManifestResponse {
    fn from_manifest(manifest: &HistoryExpiryManifest) -> Self {
        Self {
            identity: manifest.identity(),
            run: manifest.run(),
            origin: manifest.origin().clone(),
            key: manifest.key(),
            fence: manifest.fence(),
            selected_at: manifest.selected_at(),
            input_bytes: manifest.input_bytes(),
            members: manifest
                .members()
                .iter()
                .map(|member| HistoryExpiryMemberResponse {
                    family: member.family(),
                    target: member.target(),
                    anchor: member.anchor(),
                    read_span: member.read_span().clone(),
                    attributions: member.attributions().to_vec(),
                    policies: member.policies().to_vec(),
                    input_bytes: member.input_bytes(),
                })
                .collect(),
        }
    }
}

#[cfg(test)]
mod operation_response_tests;
