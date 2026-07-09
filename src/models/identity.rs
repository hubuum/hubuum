use diesel::prelude::*;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::schema::identity_scopes;

pub const LOCAL_IDENTITY_SCOPE: &str = "local";
pub const LOCAL_PROVIDER_KIND: &str = "local";
pub const LDAP_PROVIDER_KIND: &str = "ldap";
pub const MANUAL_MEMBERSHIP_SOURCE: &str = "manual";
pub const EXTERNAL_MEMBERSHIP_SOURCE: &str = "external";

#[derive(
    Serialize, Deserialize, Queryable, Selectable, Insertable, PartialEq, Debug, Clone, ToSchema,
)]
#[diesel(table_name = identity_scopes)]
pub struct IdentityScope {
    pub id: i32,
    pub name: String,
    pub provider_kind: String,
    pub created_at: chrono::NaiveDateTime,
    pub updated_at: chrono::NaiveDateTime,
}

#[derive(Insertable)]
#[diesel(table_name = identity_scopes)]
pub struct NewIdentityScope<'a> {
    pub name: &'a str,
    pub provider_kind: &'a str,
}
