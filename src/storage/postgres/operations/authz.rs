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

use crate::storage::postgres::prelude::*;
use diesel::{pg::Pg, sql_types::Integer};

use crate::errors::ApiError;
use crate::models::permissions::Permissions;
use crate::models::{
    CollectionID, HubuumClassID, HubuumObjectID, PrincipalToken, TokenResourceScope, TokenScope,
};
use crate::schema::{
    group_memberships, token_class_scopes, token_collection_scopes, token_object_scopes,
    token_scopes,
};
use crate::storage::postgres::{PostgresConnection, with_connection};
pub use crate::traits::{scope_allows, scope_allows_resource, scope_allows_resources};

/// Identity-only authorization subject: principal id, group membership, admin
/// status, and kind. Implemented once (blanket) for everything that can name a
/// principal id. Carries NO scope state — see the module docs.
#[allow(async_fn_in_trait)]
pub trait AuthzSubject: crate::traits::AuthzSubject {
    /// Boxed subquery of the group ids this principal belongs to. This is the
    /// single chokepoint every group-based permission query funnels through.
    fn group_ids_subquery<'a>(&self) -> group_memberships::BoxedQuery<'a, Pg, Integer> {
        use crate::schema::group_memberships::dsl::{group_id, group_memberships, principal_id};
        group_memberships
            .filter(principal_id.eq(self.principal_id()))
            .select(group_id)
            .into_boxed()
    }
}

impl<T: crate::traits::AuthzSubject + ?Sized> AuthzSubject for T {}

/// Load a token's permission dimension from `token_scopes`, validating each
/// stored string against the `Permissions` enum (fail-closed on an unknown
/// value). A flagged dimension may contain zero rows, which means deny-all.
pub async fn load_token_scopes(
    pool: &impl crate::storage::StorageContext,
    token_id: i32,
) -> Result<Vec<Permissions>, ApiError> {
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

#[derive(Clone, Default)]
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

/// Load one token's complete boundary on an existing connection.
///
/// This is used by transactional credential operations that must lock a token
/// and copy or audit its scope without releasing the transaction in between.
pub(crate) async fn load_token_scope_conn(
    conn: &mut PostgresConnection,
    token: &PrincipalToken,
) -> Result<Option<TokenScope>, ApiError> {
    if !token.is_scoped() {
        return Ok(None);
    }

    let permissions = if token.permission_scoped {
        token_scopes::table
            .filter(token_scopes::token_id.eq(token.id))
            .order_by(token_scopes::permission.asc())
            .select(token_scopes::permission)
            .load::<String>(conn)
            .await?
    } else {
        Vec::new()
    };
    let collection_ids = if token.resource_scoped {
        token_collection_scopes::table
            .filter(token_collection_scopes::token_id.eq(token.id))
            .order_by(token_collection_scopes::collection_id.asc())
            .select(token_collection_scopes::collection_id)
            .load::<i32>(conn)
            .await?
    } else {
        Vec::new()
    };
    let class_ids = if token.resource_scoped {
        token_class_scopes::table
            .filter(token_class_scopes::token_id.eq(token.id))
            .order_by(token_class_scopes::class_id.asc())
            .select(token_class_scopes::class_id)
            .load::<i32>(conn)
            .await?
    } else {
        Vec::new()
    };
    let object_ids = if token.resource_scoped {
        token_object_scopes::table
            .filter(token_object_scopes::token_id.eq(token.id))
            .order_by(token_object_scopes::object_id.asc())
            .select(token_object_scopes::object_id)
            .load::<i32>(conn)
            .await?
    } else {
        Vec::new()
    };

    StoredTokenScopeRows {
        permissions,
        collection_ids,
        class_ids,
        object_ids,
    }
    .into_scope(token)
    .map(Some)
}

/// Load complete token boundaries on an existing connection.
///
/// Callers that hold token row locks use this form so cascading retention
/// deletion cannot remove scope rows between the token and scope reads.
pub(crate) async fn load_token_scopes_for_tokens_conn(
    conn: &mut PostgresConnection,
    tokens: &[PrincipalToken],
) -> Result<Vec<Option<TokenScope>>, ApiError> {
    use std::collections::HashMap;

    let mut permission_token_ids = tokens
        .iter()
        .filter(|token| token.permission_scoped)
        .map(|token| token.id)
        .collect::<Vec<_>>();
    permission_token_ids.sort_unstable();
    permission_token_ids.dedup();

    let mut resource_token_ids = tokens
        .iter()
        .filter(|token| token.resource_scoped)
        .map(|token| token.id)
        .collect::<Vec<_>>();
    resource_token_ids.sort_unstable();
    resource_token_ids.dedup();

    if permission_token_ids.is_empty() && resource_token_ids.is_empty() {
        return Ok(vec![None; tokens.len()]);
    }

    let permissions = if permission_token_ids.is_empty() {
        Vec::new()
    } else {
        token_scopes::table
            .filter(token_scopes::token_id.eq_any(&permission_token_ids))
            .order_by((token_scopes::token_id.asc(), token_scopes::permission.asc()))
            .select((token_scopes::token_id, token_scopes::permission))
            .load::<(i32, String)>(conn)
            .await?
    };
    let collection_ids = if resource_token_ids.is_empty() {
        Vec::new()
    } else {
        token_collection_scopes::table
            .filter(token_collection_scopes::token_id.eq_any(&resource_token_ids))
            .order_by((
                token_collection_scopes::token_id.asc(),
                token_collection_scopes::collection_id.asc(),
            ))
            .select((
                token_collection_scopes::token_id,
                token_collection_scopes::collection_id,
            ))
            .load::<(i32, i32)>(conn)
            .await?
    };
    let class_ids = if resource_token_ids.is_empty() {
        Vec::new()
    } else {
        token_class_scopes::table
            .filter(token_class_scopes::token_id.eq_any(&resource_token_ids))
            .order_by((
                token_class_scopes::token_id.asc(),
                token_class_scopes::class_id.asc(),
            ))
            .select((token_class_scopes::token_id, token_class_scopes::class_id))
            .load::<(i32, i32)>(conn)
            .await?
    };
    let object_ids = if resource_token_ids.is_empty() {
        Vec::new()
    } else {
        token_object_scopes::table
            .filter(token_object_scopes::token_id.eq_any(&resource_token_ids))
            .order_by((
                token_object_scopes::token_id.asc(),
                token_object_scopes::object_id.asc(),
            ))
            .select((
                token_object_scopes::token_id,
                token_object_scopes::object_id,
            ))
            .load::<(i32, i32)>(conn)
            .await?
    };

    let mut rows_by_token = tokens
        .iter()
        .filter(|token| token.is_scoped())
        .map(|token| (token.id, StoredTokenScopeRows::default()))
        .collect::<HashMap<_, _>>();

    for (token_id, permission) in permissions {
        if let Some(rows) = rows_by_token.get_mut(&token_id) {
            rows.permissions.push(permission);
        }
    }
    for (token_id, collection_id) in collection_ids {
        if let Some(rows) = rows_by_token.get_mut(&token_id) {
            rows.collection_ids.push(collection_id);
        }
    }
    for (token_id, class_id) in class_ids {
        if let Some(rows) = rows_by_token.get_mut(&token_id) {
            rows.class_ids.push(class_id);
        }
    }
    for (token_id, object_id) in object_ids {
        if let Some(rows) = rows_by_token.get_mut(&token_id) {
            rows.object_ids.push(object_id);
        }
    }

    tokens
        .iter()
        .map(|token| {
            if !token.is_scoped() {
                return Ok(None);
            }
            rows_by_token
                .get(&token.id)
                .cloned()
                .unwrap_or_default()
                .into_scope(token)
                .map(Some)
        })
        .collect()
}
