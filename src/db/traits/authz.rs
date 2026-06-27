//! The principal-centric authorization subject contract.
//!
//! `AuthzSubject` is **identity-only**: it answers "who is this principal and
//! which groups / admin status do they have", and nothing about token scopes.
//! Scopes are threaded *separately* as an `Option<&[Permissions]>` parameter on
//! every authz / search / report entry point:
//!
//! * `None`        — unscoped: full principal authority (internal/background
//!   callers, password-login tokens).
//! * `Some(slice)` — the token's effective scope set. An empty slice denies
//!   everything (a `scoped = true` token with no rows).
//!
//! Request handlers pass the live token scopes from the `Authenticated`
//! extractor; task workers pass the scope snapshot persisted on the task; plain
//! internal callers pass `None`.

use diesel::pg::Pg;
use diesel::prelude::*;
use diesel::sql_types::Integer;

use crate::db::{DbPool, with_connection};
use crate::errors::ApiError;
use crate::models::permissions::Permissions;
use crate::models::{Principal, PrincipalID, ServiceAccount, ServiceAccountID, User, UserID};
use crate::schema::{group_memberships, groups};

/// Cheap, local access to a subject's principal id (no backend round-trip).
///
/// For `User` / `ServiceAccount` / `Principal` the principal id IS `self.id`
/// (class-table inheritance); for the `*ID` newtypes it is the wrapped value.
pub trait PrincipalIdAccessor {
    fn principal_id(&self) -> i32;
}

impl PrincipalIdAccessor for User {
    fn principal_id(&self) -> i32 {
        self.id
    }
}
impl PrincipalIdAccessor for Principal {
    fn principal_id(&self) -> i32 {
        self.id
    }
}
impl PrincipalIdAccessor for ServiceAccount {
    fn principal_id(&self) -> i32 {
        self.id
    }
}
impl PrincipalIdAccessor for UserID {
    fn principal_id(&self) -> i32 {
        self.id()
    }
}
impl PrincipalIdAccessor for PrincipalID {
    fn principal_id(&self) -> i32 {
        self.id()
    }
}
impl PrincipalIdAccessor for ServiceAccountID {
    fn principal_id(&self) -> i32 {
        self.id()
    }
}
impl<T: PrincipalIdAccessor + ?Sized> PrincipalIdAccessor for &T {
    fn principal_id(&self) -> i32 {
        (**self).principal_id()
    }
}

/// Identity-only authorization subject: principal id, group membership, admin
/// status, and kind. Implemented once (blanket) for everything that can name a
/// principal id. Carries NO scope state — see the module docs.
#[allow(async_fn_in_trait)]
pub trait AuthzSubject: PrincipalIdAccessor {
    /// Boxed subquery of the group ids this principal belongs to. This is the
    /// single chokepoint every group-based permission query funnels through.
    fn group_ids_subquery<'a>(&self) -> group_memberships::BoxedQuery<'a, Pg, Integer> {
        use crate::schema::group_memberships::dsl::{group_id, group_memberships, principal_id};
        group_memberships
            .filter(principal_id.eq(self.principal_id()))
            .select(group_id)
            .into_boxed()
    }

    /// The configured admin group name.
    async fn admin_groupname(&self) -> Result<String, ApiError> {
        Ok(crate::config::get_config()?.admin_groupname.clone())
    }

    /// Is this principal a member of the named group?
    async fn is_in_group_by_name(
        &self,
        groupname_queried: &str,
        pool: &DbPool,
    ) -> Result<bool, ApiError> {
        use diesel::dsl::{exists, select};
        let pid = self.principal_id();
        let is_in_group = with_connection(pool, |conn| {
            select(exists(
                group_memberships::table
                    .inner_join(groups::table)
                    .filter(group_memberships::principal_id.eq(pid))
                    .filter(groups::groupname.eq(groupname_queried)),
            ))
            .get_result(conn)
        })?;
        Ok(is_in_group)
    }

    /// Is this principal an admin (member of the configured admin group)?
    ///
    /// Note: this is a pure group-membership fact. It does NOT make a service
    /// account a *human IAM* administrator — that separation is enforced by the
    /// `kind = 'human'` gate on the human/IAM extractors, not here.
    async fn is_admin(&self, pool: &DbPool) -> Result<bool, ApiError> {
        let groupname = self.admin_groupname().await?;
        self.is_in_group_by_name(&groupname, pool).await
    }
}

impl<T: PrincipalIdAccessor + ?Sized> AuthzSubject for T {}

/// Fail-closed token-scope pre-filter.
///
/// Returns `true` iff the token scope set permits *all* of `requested`:
/// * `None`        ⇒ unscoped ⇒ always allowed.
/// * `Some(slice)` ⇒ every requested permission must be present; an empty slice
///   therefore denies everything.
///
/// Callers apply this **before** the admin-bypass so a scoped admin token can
/// never exceed its scopes.
pub fn scope_allows(scopes: Option<&[Permissions]>, requested: &[Permissions]) -> bool {
    match scopes {
        None => true,
        Some(allowed) => requested.iter().all(|p| allowed.contains(p)),
    }
}

/// Load a token's scope set from `token_scopes`, validating each stored string
/// against the `Permissions` enum (fail-closed on an unknown value). Only called
/// when `tokens.scoped` is true; the resulting slice may be empty (deny-all).
pub async fn load_token_scopes(pool: &DbPool, token_id: i32) -> Result<Vec<Permissions>, ApiError> {
    use crate::schema::token_scopes::dsl::{permission, token_id as ts_token_id, token_scopes};

    let raw: Vec<String> = with_connection(pool, |conn| {
        token_scopes
            .filter(ts_token_id.eq(token_id))
            .select(permission)
            .load::<String>(conn)
    })?;

    raw.iter().map(|s| Permissions::from_string(s)).collect()
}
