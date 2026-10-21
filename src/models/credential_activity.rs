use chrono::{DateTime, Utc};
use hubuum_storage_core::{CredentialActivityPolicy, ObservedSource, StorageCredentialActivity};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActivityPageQuery {
    #[serde(default)]
    pub offset: usize,
    #[serde(default = "default_limit")]
    pub limit: usize,
}
fn default_limit() -> usize {
    50
}

#[derive(Debug, Serialize, ToSchema)]
pub struct CredentialSourceResponse {
    pub address: Option<String>,
    pub address_method: String,
    pub first_seen: DateTime<Utc>,
    pub last_seen: DateTime<Utc>,
}
#[derive(Debug, Serialize, ToSchema)]
pub struct CredentialCreationResponse {
    pub issuer_id: Option<i32>,
    pub issuance_method: String,
    pub issued_at: DateTime<Utc>,
    pub address: Option<String>,
    pub address_method: String,
}
#[derive(Debug, Serialize, ToSchema)]
pub struct CredentialActivityResponse {
    /// Present when observations are shared by the rotating session family.
    pub session_id: Option<Uuid>,
    /// Null for historical credentials or unavailable issuance evidence.
    pub creation: Option<CredentialCreationResponse>,
    pub sources: Vec<CredentialSourceResponse>,
    pub total: usize,
    pub offset: usize,
    pub limit: usize,
    pub next_offset: Option<usize>,
    pub collection_enabled: bool,
    pub best_effort: bool,
    pub retention_days: i64,
    pub source_cap: usize,
    /// Non-null means at least one distinct source was omitted after reaching the cap.
    pub overflow_since: Option<DateTime<Utc>>,
}
impl CredentialActivityResponse {
    pub fn new(
        activity: StorageCredentialActivity,
        query: &ActivityPageQuery,
        policy: CredentialActivityPolicy,
    ) -> Self {
        let address = |source: ObservedSource| source.address().map(|ip| ip.to_string());
        Self {
            session_id: activity.session_id().map(|id| id.value()),
            creation: activity.creation().map(|c| CredentialCreationResponse {
                issuer_id: c.issuer().map(|id| id.id()),
                issuance_method: c.method().as_str().into(),
                issued_at: c.issued_at(),
                address: address(c.source()),
                address_method: c.source().method().into(),
            }),
            sources: activity
                .sources()
                .iter()
                .map(|s| CredentialSourceResponse {
                    address: address(s.source()),
                    address_method: s.source().method().into(),
                    first_seen: s.first_seen(),
                    last_seen: s.last_seen(),
                })
                .collect(),
            total: activity.total(),
            offset: query.offset,
            limit: query.limit,
            next_offset: (query.offset + activity.sources().len() < activity.total())
                .then_some(query.offset + activity.sources().len()),
            collection_enabled: policy.enabled(),
            best_effort: true,
            retention_days: policy.retention_days(),
            source_cap: policy.source_cap(),
            overflow_since: activity.overflow_since(),
        }
    }
}
