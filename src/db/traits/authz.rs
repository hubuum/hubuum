//! The principal-centric authorization subject contract.
//!
//! `AuthzSubject` is **identity-only**: it answers "who is this principal and
//! which groups / admin status do they have", and nothing about token scopes.
//! Scopes are threaded *separately* as an `Option<&TokenScope>` parameter on
//! every authz / search / export entry point:
//!
//! * `None`        — unscoped: full principal authority (internal/background
//!   callers, password-login tokens).
//! * `Some(slice)` — the token's effective scope set. An empty slice denies
//!   everything (a `scoped = true` token with no rows).
//!
//! Request handlers pass the live token scopes from the `Authenticated`
//! extractor; task workers pass the scope snapshot persisted on the task; plain
//! internal callers pass `None`.

use crate::db::prelude::*;
use diesel::pg::Pg;
use diesel::sql_types::Integer;

use crate::db::{DbPool, with_connection, with_connection_async};
use crate::errors::ApiError;
use crate::models::identity::LOCAL_IDENTITY_SCOPE;
use crate::models::permissions::Permissions;
use crate::models::{
    CollectionID, HubuumClassID, HubuumObjectID, Principal, PrincipalID, PrincipalToken,
    ServiceAccount, ServiceAccountID, TokenResourceScope, TokenScope, User, UserID,
};
use crate::permissions::ResourceRef;
use crate::schema::{
    group_memberships, groups, identity_scopes, token_class_scopes, token_collection_scopes,
    token_object_scopes, token_scopes,
};

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

    /// The configured admin identity scope name.
    async fn admin_identity_scope(&self) -> Result<String, ApiError> {
        Ok(crate::config::get_config()?
            .admin_identity_scope
            .clone()
            .unwrap_or_else(|| LOCAL_IDENTITY_SCOPE.to_string()))
    }

    /// Is this principal a member of the named group?
    async fn is_in_group_by_name(
        &self,
        groupname_queried: &str,
        pool: &DbPool,
    ) -> Result<bool, ApiError> {
        use diesel::dsl::{exists, select};
        let pid = self.principal_id();
        let scope = self.admin_identity_scope().await?;
        let group_name = groupname_queried.to_string();
        let is_in_group = with_connection_async(pool.clone(), async move |conn| {
            select(exists(
                group_memberships::table
                    .inner_join(groups::table)
                    .inner_join(
                        identity_scopes::table
                            .on(groups::identity_scope_id.eq(identity_scopes::id)),
                    )
                    .filter(group_memberships::principal_id.eq(pid))
                    .filter(groups::groupname.eq(group_name))
                    .filter(identity_scopes::name.eq(scope)),
            ))
            .get_result(conn)
            .await
        })
        .await?;
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
/// * `Some(scope)` ⇒ every requested permission must be present when the
///   permission dimension is enabled; an empty enabled dimension denies all.
///
/// Callers apply this **before** the admin-bypass so a scoped admin token can
/// never exceed its scopes.
pub fn scope_allows(scopes: Option<&TokenScope>, requested: &[Permissions]) -> bool {
    match scopes {
        None => true,
        Some(scope) => scope.allows_permissions(requested),
    }
}

/// Fail-closed resource-identity pre-filter for a token scope.
pub fn scope_allows_resource(scope: Option<&TokenScope>, resource: &ResourceRef) -> bool {
    scope.is_none_or(|scope| scope.allows_resource(resource))
}

/// Require every resource touched by an operation to be inside the token's
/// resource boundary.
pub fn scope_allows_resources(scope: Option<&TokenScope>, resources: &[ResourceRef]) -> bool {
    resources
        .iter()
        .all(|resource| scope_allows_resource(scope, resource))
}

/// Load a token's permission dimension from `token_scopes`, validating each
/// stored string against the `Permissions` enum (fail-closed on an unknown
/// value). A flagged dimension may contain zero rows, which means deny-all.
pub async fn load_token_scopes(pool: &DbPool, token_id: i32) -> Result<Vec<Permissions>, ApiError> {
    use crate::schema::token_scopes::dsl::{permission, token_id as ts_token_id, token_scopes};

    let raw: Vec<String> = with_connection(pool, async |conn| {
        token_scopes
            .filter(ts_token_id.eq(token_id))
            .select(permission)
            .load::<String>(conn)
            .await
    })
    .await?;

    raw.iter().map(|s| Permissions::from_string(s)).collect()
}

struct StoredTokenScopeRows {
    permissions: Vec<String>,
    collection_ids: Vec<i32>,
    class_ids: Vec<i32>,
    object_ids: Vec<i32>,
}

impl StoredTokenScopeRows {
    fn into_scope(self, token: &PrincipalToken) -> Result<TokenScope, ApiError> {
        let Self {
            permissions,
            collection_ids,
            class_ids,
            object_ids,
        } = self;
        let permissions = token
            .permission_scoped
            .then(|| {
                permissions
                    .iter()
                    .map(|permission| Permissions::from_string(permission))
                    .collect::<Result<Vec<_>, _>>()
            })
            .transpose()?;
        let resources = token
            .resource_scoped
            .then(|| {
                collection_ids
                    .into_iter()
                    .map(|id| CollectionID::new(id).map(TokenResourceScope::Collection))
                    .chain(
                        class_ids
                            .into_iter()
                            .map(|id| HubuumClassID::new(id).map(TokenResourceScope::Class)),
                    )
                    .chain(
                        object_ids
                            .into_iter()
                            .map(|id| HubuumObjectID::new(id).map(TokenResourceScope::Object)),
                    )
                    .collect::<Result<Vec<_>, _>>()
            })
            .transpose()?;

        TokenScope::from_stored_parts(permissions, resources)
    }
}

/// Load all narrowing dimensions for an authenticated token. The two scoped
/// flags are the source of truth, so a flagged dimension with no rows becomes a
/// present-but-empty deny-all dimension.
pub async fn load_token_scope(
    pool: &DbPool,
    token: &PrincipalToken,
) -> Result<Option<TokenScope>, ApiError> {
    if !token.is_scoped() {
        return Ok(None);
    }

    let stored_scope = with_connection(pool, async |conn| -> Result<_, ApiError> {
        let permissions = if token.permission_scoped {
            token_scopes::table
                .filter(token_scopes::token_id.eq(token.id))
                .select(token_scopes::permission)
                .load::<String>(conn)
                .await?
        } else {
            Vec::new()
        };
        let collection_ids = if token.resource_scoped {
            token_collection_scopes::table
                .filter(token_collection_scopes::token_id.eq(token.id))
                .select(token_collection_scopes::collection_id)
                .load::<i32>(conn)
                .await?
        } else {
            Vec::new()
        };
        let class_ids = if token.resource_scoped {
            token_class_scopes::table
                .filter(token_class_scopes::token_id.eq(token.id))
                .select(token_class_scopes::class_id)
                .load::<i32>(conn)
                .await?
        } else {
            Vec::new()
        };
        let object_ids = if token.resource_scoped {
            token_object_scopes::table
                .filter(token_object_scopes::token_id.eq(token.id))
                .select(token_object_scopes::object_id)
                .load::<i32>(conn)
                .await?
        } else {
            Vec::new()
        };
        Ok(StoredTokenScopeRows {
            permissions,
            collection_ids,
            class_ids,
            object_ids,
        })
    })
    .await?;

    stored_scope.into_scope(token).map(Some)
}
