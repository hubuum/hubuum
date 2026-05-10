use async_trait::async_trait;
use diesel::prelude::*;
use treetop_client::{
    AuthorizeBriefResponse, AuthorizeRequest, BatchResult, Client, Request as TreetopRequest,
};

use crate::config::AppConfig;
use crate::db::{DbPool, with_connection};
use crate::errors::ApiError;
use crate::models::search::QueryOptions;
use crate::models::{GroupPermission, Namespace, Permission, Permissions, PermissionsList};

use super::backend::PermissionBackend;
use super::types::{PermissionDecision, PermissionRequest, PrincipalRef, ResourceRef};
use super::visibility::paginate_authorized;

pub mod error;
pub mod mapping;

pub use error::treetop_to_api_error;
pub use mapping::{cedar_action, cedar_resource, cedar_user};

/// Production permission backend that delegates to a Treetop policy server.
///
/// - Connect once at startup via `TreetopPermissionBackend::connect`.
/// - `authorize_many` batches all permission checks into a single Treetop request.
/// - `is_admin` dispatches to Treetop with a System resource check.
/// - Reverse queries (`namespaces_user_can`) load candidates from the local DB
///   then filter via Treetop batch authorization.
/// - Mutations (`apply_permissions`, `revoke_permissions`, `revoke_all`) return
///   `ApiError::NotImplemented` — permissions are managed out-of-band.
pub struct TreetopPermissionBackend {
    client: Client,
    pool: DbPool,
}

impl TreetopPermissionBackend {
    /// Connect to a Treetop server and perform a startup health check.
    ///
    /// Returns a fatal `ApiError` if the server is unreachable or unhealthy —
    /// per the spec, we fail-closed-fatal on startup health failures.
    pub async fn connect(url: &str, cfg: &AppConfig, pool: DbPool) -> Result<Self, ApiError> {
        // Wire AppConfig timeouts + the dev-only accept-invalid-certs flag
        // through to the upstream ClientBuilder. CA certificate loading
        // (HUBUUM_TREETOP_CA_CERT) requires constructing a reqwest::Certificate,
        // and reqwest is not a direct dependency of this crate yet. If an
        // operator sets that env var we surface an explicit error rather than
        // silently ignoring it.
        let mut builder = Client::builder(url)
            .connect_timeout(std::time::Duration::from_millis(
                cfg.treetop_connect_timeout_ms,
            ))
            .request_timeout(std::time::Duration::from_millis(
                cfg.treetop_request_timeout_ms,
            ));

        if cfg.treetop_accept_invalid_certs {
            builder = builder.danger_accept_invalid_certs(true);
        }

        if cfg.treetop_ca_cert.is_some() {
            return Err(ApiError::InternalServerError(
                "HUBUUM_TREETOP_CA_CERT is set but CA certificate loading is not yet wired \
                 — the project would need to take a direct dependency on reqwest to construct \
                 the Certificate. Unset the env var or add the wiring."
                    .to_string(),
            ));
        }

        let client = builder.build().map_err(treetop_to_api_error)?;

        // Startup health check — fail-closed-fatal per Q9 of the spec.
        client.health().await.map_err(treetop_to_api_error)?;

        Ok(Self { client, pool })
    }

    /// Build the underlying batch authorize request for a vector of
    /// PermissionRequests. Returns the wire request plus the per-request
    /// span (start_index, count) so the caller can collapse decisions
    /// back into per-request results in input order.
    ///
    /// Each PermissionRequest may contain multiple permissions (conjunctive
    /// AND at our layer). We expand each request into N Cedar requests (one
    /// per permission), then remember the spans so we can AND them back
    /// together after Treetop returns per-Cedar-request decisions.
    fn build_batch(
        principal: &PrincipalRef,
        requests: &[PermissionRequest],
    ) -> (AuthorizeRequest, Vec<(usize, usize)>) {
        let user = cedar_user(principal);
        let mut batch = AuthorizeRequest::new();
        let mut spans = Vec::with_capacity(requests.len());
        let mut idx = 0;
        for req in requests {
            let resource = cedar_resource(&req.resource);
            let count = req.permissions.len();
            spans.push((idx, count));
            for perm in &req.permissions {
                batch = batch.add_request(TreetopRequest::new(
                    user.clone(),
                    cedar_action(*perm),
                    resource.clone(),
                ));
                idx += 1;
            }
        }
        (batch, spans)
    }
}

/// Helper to extract boolean decisions from a Treetop authorize response.
///
/// The upstream `AuthorizeBriefResponse` has `.results()` returning a
/// `Vec<IndexedResult<AuthorizeDecisionBrief>>`. Each result is either
/// `BatchResult::Success { data }` or `BatchResult::Failed { message }`.
/// We extract a boolean per Cedar request: Success + Allow => true,
/// anything else => false.
fn extract_decisions(response: &AuthorizeBriefResponse) -> Vec<bool> {
    response
        .results()
        .iter()
        .map(|indexed_result| match &indexed_result.result {
            BatchResult::Success { data } => {
                matches!(data.decision, treetop_client::DecisionBrief::Allow)
            }
            BatchResult::Failed { .. } => false,
        })
        .collect()
}

#[async_trait]
impl PermissionBackend for TreetopPermissionBackend {
    async fn authorize_many(
        &self,
        principal: &PrincipalRef,
        requests: Vec<PermissionRequest>,
    ) -> Result<Vec<PermissionDecision>, ApiError> {
        if requests.is_empty() {
            return Ok(Vec::new());
        }

        let (batch, spans) = Self::build_batch(principal, &requests);

        let response = self
            .client
            .authorize(&batch)
            .await
            .map_err(treetop_to_api_error)?;

        // Extract per-Cedar-request boolean decisions.
        let cedar_decisions = extract_decisions(&response);

        // Collapse across the spans: each input PermissionRequest is Allow
        // iff ALL its per-permission Cedar decisions are Allow.
        Ok(spans
            .into_iter()
            .map(|(start, count)| {
                let all_allow = (start..start + count).all(|i| cedar_decisions[i]);
                if all_allow {
                    PermissionDecision::Allow
                } else {
                    PermissionDecision::Deny
                }
            })
            .collect())
    }

    async fn is_admin(&self, principal: &PrincipalRef) -> Result<bool, ApiError> {
        // Delegate to authorize against System resource. Use the same
        // "ReadCollection on System" overload that MockTreetopBackend
        // adopted (Task 5.1). Cedar policies decide what's admin.
        let request = PermissionRequest {
            resource: ResourceRef::system(),
            permissions: vec![Permissions::ReadCollection],
        };
        let decision = self.authorize(principal, request).await?;
        Ok(decision == PermissionDecision::Allow)
    }

    async fn namespaces_user_can(
        &self,
        principal: &PrincipalRef,
        permissions: &[Permissions],
    ) -> Result<Vec<Namespace>, ApiError> {
        // Enumerate candidates from the local DB, filter via Treetop.
        // We load all namespaces without any permission filtering, then
        // use paginate_authorized to filter via Treetop batch authorization.
        let all_namespaces = with_connection(&self.pool, |conn| {
            crate::schema::namespaces::table.load::<Namespace>(conn)
        })?;
        let perms = permissions.to_vec();
        let page = paginate_authorized(
            self,
            principal,
            all_namespaces,
            perms,
            0,
            usize::MAX, // no pagination at this entry point — caller handles it
            |ns: &Namespace| ResourceRef::namespace(ns.id),
        )
        .await?;
        Ok(page.rows)
    }

    async fn groups_with_permissions_on(
        &self,
        _namespace_id: i32,
        _permissions_filter: &[Permissions],
        _page: &QueryOptions,
    ) -> Result<(Vec<GroupPermission>, i64), ApiError> {
        // For each (group, permission) pair on the namespace, ask Treetop
        // whether that group has that permission. Synthesize GroupPermission
        // rows with the resulting boolean grid.
        //
        // Implementation note: this is genuinely expensive if there are
        // many groups, but the namespace listing endpoint always paginates,
        // so we only materialize one page at a time.
        //
        // TODO: implement candidate enumeration over all groups + per-permission
        // authorize. The synthetic GroupPermission shape mirrors
        // LocalPermissionBackend's row.
        Err(ApiError::NotImplemented(
            "TreetopPermissionBackend::groups_with_permissions_on is not yet implemented — \
             synthetic permission listing for Treetop mode is low-priority and only affects \
             the namespace permission-listing endpoints."
                .to_string(),
        ))
    }

    async fn group_permission_on(
        &self,
        _namespace_id: i32,
        _group_id: i32,
    ) -> Result<Option<Permission>, ApiError> {
        // Synthesize a Permission row by asking Treetop "does this group
        // have <each Permissions variant> on this namespace?" 24 questions,
        // 24 booleans, one synthetic row.
        //
        // id=0, created_at/updated_at=now (documented in the spec — these
        // fields are synthetic for Treetop-mode responses).
        //
        // TODO: synthesize Permission row from per-variant authorize results.
        Err(ApiError::NotImplemented(
            "TreetopPermissionBackend::group_permission_on is not yet implemented — \
             synthetic permission rows for Treetop mode are low-priority and only affect \
             the single-group permission lookup endpoint."
                .to_string(),
        ))
    }

    async fn apply_permissions(
        &self,
        _namespace_id: i32,
        _group_id: i32,
        _list: PermissionsList<Permissions>,
        _replace_existing: bool,
    ) -> Result<Permission, ApiError> {
        Err(ApiError::NotImplemented(
            "permission mutations are managed out-of-band when using the treetop backend"
                .to_string(),
        ))
    }

    async fn revoke_permissions(
        &self,
        _namespace_id: i32,
        _group_id: i32,
        _list: PermissionsList<Permissions>,
    ) -> Result<Permission, ApiError> {
        Err(ApiError::NotImplemented(
            "permission mutations are managed out-of-band when using the treetop backend"
                .to_string(),
        ))
    }

    async fn revoke_all(&self, _namespace_id: i32, _group_id: i32) -> Result<(), ApiError> {
        Err(ApiError::NotImplemented(
            "permission mutations are managed out-of-band when using the treetop backend"
                .to_string(),
        ))
    }

    fn supports_mutation(&self) -> bool {
        false
    }

    fn supports_sql_visibility_join(&self) -> bool {
        false
    }

    fn kind(&self) -> &'static str {
        "treetop"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_batch_produces_correct_spans() {
        let principal = PrincipalRef::new(42, vec![100, 200]);
        let requests = vec![
            PermissionRequest {
                resource: ResourceRef::namespace(1),
                permissions: vec![Permissions::ReadCollection, Permissions::UpdateCollection],
            },
            PermissionRequest {
                resource: ResourceRef::namespace(2),
                permissions: vec![Permissions::ReadClass],
            },
            PermissionRequest {
                resource: ResourceRef::namespace(3),
                permissions: vec![
                    Permissions::CreateObject,
                    Permissions::ReadObject,
                    Permissions::UpdateObject,
                ],
            },
        ];

        let (batch, spans) = TreetopPermissionBackend::build_batch(&principal, &requests);

        // First request: 2 permissions -> span (0, 2)
        // Second request: 1 permission -> span (2, 1)
        // Third request: 3 permissions -> span (3, 3)
        assert_eq!(spans, vec![(0, 2), (2, 1), (3, 3)]);

        // Total Cedar requests = 2 + 1 + 3 = 6
        assert_eq!(batch.requests.len(), 6);
    }
}
