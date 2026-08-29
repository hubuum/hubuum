use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::models::ResourceRevision;

pub use hubuum_domain::{
    EXTERNAL_MEMBERSHIP_SOURCE, LDAP_PROVIDER_KIND, LOCAL_IDENTITY_SCOPE, LOCAL_PROVIDER_KIND,
    MANUAL_MEMBERSHIP_SOURCE,
};

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, ToSchema)]
pub struct IdentityScope {
    pub id: i32,
    pub name: String,
    pub provider_kind: String,
    pub created_at: chrono::NaiveDateTime,
    pub updated_at: chrono::NaiveDateTime,
    pub revision: ResourceRevision,
}
