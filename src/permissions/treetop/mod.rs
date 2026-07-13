use std::time::{Duration, Instant};

use crate::db::prelude::*;
use async_trait::async_trait;
use treetop_client::{
    Action, AuthorizeBriefResponse, AuthorizeRequest, BatchResult, Client, DecisionBrief,
    Request as TreetopRequest,
};

use crate::config::AppConfig;
use crate::db::{DbPool, with_connection};
use crate::errors::ApiError;
use crate::models::search::{FilterField, QueryOptions, QueryParamsExt};
use crate::models::{Collection, Group, GroupPermission, Permission, Permissions, PermissionsList};
use crate::pagination::{known_count_or_skipped, paginate_in_memory};
use crate::schema::collections;

use super::backend::PermissionBackend;
use super::observability::{record_authorize_many, record_is_admin, record_reverse_query};
use super::types::{PermissionDecision, PermissionRequest, PrincipalRef, ResourceRef};

const BACKEND_KIND: &str = "treetop";
const MAX_CEDAR_REQUESTS_PER_BATCH: usize = 512;

pub mod error;
pub mod mapping;

pub use error::treetop_to_api_error;
pub use mapping::{cedar_action, cedar_resource, cedar_user};

/// Production permission backend that delegates to a Treetop policy server.
///
/// - Connect once at startup via `TreetopPermissionBackend::connect`.
/// - `authorize_many` batches all permission checks into a single Treetop request.
/// - `is_admin` dispatches to Treetop with a System resource check.
/// - Reverse queries (`collections_user_can`) load candidates from the local DB
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
            .connect_timeout(Duration::from_millis(cfg.treetop_connect_timeout_ms))
            .request_timeout(Duration::from_millis(cfg.treetop_request_timeout_ms));

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
    #[cfg(test)]
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

    async fn authorize_flat_permission_checks(
        &self,
        checks: &[(PrincipalRef, Permissions, ResourceRef)],
    ) -> Result<Vec<bool>, ApiError> {
        let mut decisions = Vec::with_capacity(checks.len());
        for chunk in checks.chunks(MAX_CEDAR_REQUESTS_PER_BATCH) {
            let batch = AuthorizeRequest::from_requests(chunk.iter().map(
                |(principal, permission, resource)| {
                    TreetopRequest::new(
                        cedar_user(principal),
                        cedar_action(*permission),
                        cedar_resource(resource),
                    )
                },
            ));
            let response = self
                .client
                .authorize(&batch)
                .await
                .map_err(treetop_to_api_error)?;
            decisions.extend(extract_decisions(&response, chunk.len())?);
        }
        Ok(decisions)
    }
}

/// Helper to extract boolean decisions from a Treetop authorize response.
///
/// The upstream `AuthorizeBriefResponse` has `.results()` returning a
/// `Vec<IndexedResult<AuthorizeDecisionBrief>>`. Each result is either
/// `BatchResult::Success { data }` or `BatchResult::Failed { message }`.
/// We extract a boolean per Cedar request: Success + Allow => true,
/// anything else => false.
fn extract_decisions(
    response: &AuthorizeBriefResponse,
    expected_count: usize,
) -> Result<Vec<bool>, ApiError> {
    if response.results().len() != expected_count {
        return Err(ApiError::PermissionBackendUnavailable(format!(
            "Treetop returned {} batch results for {expected_count} requests",
            response.results().len()
        )));
    }

    let mut decisions = vec![None; expected_count];
    for indexed_result in response.results() {
        if indexed_result.index >= expected_count {
            return Err(ApiError::PermissionBackendUnavailable(format!(
                "Treetop returned out-of-range batch index {} for {expected_count} requests",
                indexed_result.index
            )));
        }
        if decisions[indexed_result.index].is_some() {
            return Err(ApiError::PermissionBackendUnavailable(format!(
                "Treetop returned duplicate batch index {}",
                indexed_result.index
            )));
        }
        let decision = match &indexed_result.result {
            BatchResult::Success { data } => {
                matches!(data.decision, DecisionBrief::Allow)
            }
            BatchResult::Failed { message } => {
                return Err(ApiError::PermissionBackendUnavailable(format!(
                    "Treetop failed batch item {}: {message}",
                    indexed_result.index
                )));
            }
        };
        decisions[indexed_result.index] = Some(decision);
    }

    decisions
        .into_iter()
        .enumerate()
        .map(|(index, decision)| {
            decision.ok_or_else(|| {
                ApiError::PermissionBackendUnavailable(format!(
                    "Treetop omitted batch result index {index}"
                ))
            })
        })
        .collect()
}

// Re-export the synthesize helpers from test_support so they're available
// within this module. The actual implementations live in test_support to
// avoid circular dependencies when building without the treetop feature.
use crate::permissions::test_support::mock_treetop::{
    permission_has_any_grant, synthesize_permission, synthesize_permission_for_group,
};

#[async_trait]
impl PermissionBackend for TreetopPermissionBackend {
    async fn authorize_many(
        &self,
        principal: &PrincipalRef,
        requests: Vec<PermissionRequest>,
    ) -> Result<Vec<PermissionDecision>, ApiError> {
        let start = Instant::now();
        let request_count = requests.len();

        if requests.is_empty() {
            record_authorize_many(BACKEND_KIND, 0, 0, 0, 0, start.elapsed());
            return Ok(Vec::new());
        }

        let mut checks = Vec::new();
        let mut spans = Vec::with_capacity(requests.len());
        for request in &requests {
            let start = checks.len();
            checks.extend(
                request
                    .permissions
                    .iter()
                    .map(|permission| (principal.clone(), *permission, request.resource.clone())),
            );
            spans.push((start, request.permissions.len()));
        }
        let cedar_request_count = checks.len();
        let cedar_decisions = self.authorize_flat_permission_checks(&checks).await?;

        // Collapse across the spans: each input PermissionRequest is Allow
        // iff ALL its per-permission Cedar decisions are Allow.
        let decisions: Vec<PermissionDecision> = spans
            .into_iter()
            .map(|(start, count)| {
                let all_allow = (start..start + count).all(|i| cedar_decisions[i]);
                if all_allow {
                    PermissionDecision::Allow
                } else {
                    PermissionDecision::Deny
                }
            })
            .collect();

        let allow_count = decisions
            .iter()
            .filter(|d| **d == PermissionDecision::Allow)
            .count();
        let deny_count = decisions.len() - allow_count;
        record_authorize_many(
            BACKEND_KIND,
            request_count,
            cedar_request_count,
            allow_count,
            deny_count,
            start.elapsed(),
        );

        Ok(decisions)
    }

    async fn is_admin(&self, principal: &PrincipalRef) -> Result<bool, ApiError> {
        // Delegate to authorize against System resource. Use the same
        // "ReadCollection on System" overload that MockTreetopBackend
        // adopted (Task 5.1). Cedar policies decide what's admin.
        let start = Instant::now();
        let request = PermissionRequest {
            resource: ResourceRef::system(),
            permissions: vec![Permissions::ReadCollection],
        };
        let decision = self.authorize(principal, request).await?;
        let allowed = decision == PermissionDecision::Allow;
        record_is_admin(BACKEND_KIND, allowed, start.elapsed());
        Ok(allowed)
    }

    async fn authorize_task(
        &self,
        principal: &PrincipalRef,
        task: &ResourceRef,
    ) -> Result<PermissionDecision, ApiError> {
        let batch = AuthorizeRequest::single(TreetopRequest::new(
            cedar_user(principal),
            Action::new("ReadTask"),
            cedar_resource(task),
        ));
        let response = self
            .client
            .authorize(&batch)
            .await
            .map_err(treetop_to_api_error)?;
        Ok(if extract_decisions(&response, 1)?[0] {
            PermissionDecision::Allow
        } else {
            PermissionDecision::Deny
        })
    }

    async fn authorize_tasks(
        &self,
        principal: &PrincipalRef,
        tasks: &[ResourceRef],
    ) -> Result<Vec<PermissionDecision>, ApiError> {
        let mut decisions = Vec::with_capacity(tasks.len());
        for chunk in tasks.chunks(MAX_CEDAR_REQUESTS_PER_BATCH) {
            let batch = AuthorizeRequest::from_requests(chunk.iter().map(|task| {
                TreetopRequest::new(
                    cedar_user(principal),
                    Action::new("ReadTask"),
                    cedar_resource(task),
                )
            }));
            let response = self
                .client
                .authorize(&batch)
                .await
                .map_err(treetop_to_api_error)?;
            decisions.extend(extract_decisions(&response, chunk.len())?.into_iter().map(
                |allowed| {
                    if allowed {
                        PermissionDecision::Allow
                    } else {
                        PermissionDecision::Deny
                    }
                },
            ));
        }
        Ok(decisions)
    }

    async fn collections_user_can(
        &self,
        principal: &PrincipalRef,
        permissions: &[Permissions],
    ) -> Result<Vec<Collection>, ApiError> {
        // Enumerate candidates from the local DB, filter via Treetop.
        // We load all collections without any permission filtering, then
        // use paginate_authorized to filter via Treetop batch authorization.
        let start = Instant::now();
        let all_collections = with_connection(&self.pool, async |conn| {
            collections::table.load::<Collection>(conn).await
        })
        .await?;
        let candidate_count = all_collections.len();
        let tested_permissions = if permissions.is_empty() {
            Permissions::all()
        } else {
            permissions
        };
        let checks = all_collections
            .iter()
            .flat_map(|collection| {
                tested_permissions.iter().map(move |permission| {
                    (
                        principal.clone(),
                        *permission,
                        ResourceRef::for_permission_on_collection(*permission, collection.id),
                    )
                })
            })
            .collect::<Vec<_>>();
        let decisions = self.authorize_flat_permission_checks(&checks).await?;
        let width = tested_permissions.len();
        let rows = all_collections
            .into_iter()
            .zip(decisions.chunks(width))
            .filter_map(|(collection, decisions)| {
                let allowed = if permissions.is_empty() {
                    decisions.iter().any(|decision| *decision)
                } else {
                    decisions.iter().all(|decision| *decision)
                };
                allowed.then_some(collection)
            })
            .collect::<Vec<_>>();
        record_reverse_query(
            BACKEND_KIND,
            "collections_user_can",
            candidate_count,
            rows.len(),
            start.elapsed(),
        );
        Ok(rows)
    }

    async fn groups_with_permissions_on(
        &self,
        collection_id: i32,
        permissions_filter: &[Permissions],
        page: &QueryOptions,
    ) -> Result<(Vec<GroupPermission>, i64), ApiError> {
        use crate::schema::groups::dsl::{
            created_at, groupname, groups as groups_dsl, id, updated_at,
        };
        use crate::{date_search, numeric_search, string_search};

        let start = Instant::now();

        let mut group_query = groups_dsl.into_boxed();
        for param in &page.filters {
            let operator = param.operator.clone();
            match param.field {
                FilterField::Id => numeric_search!(group_query, param, operator, id),
                FilterField::Name | FilterField::Groupname => {
                    string_search!(group_query, param, operator, groupname)
                }
                FilterField::CreatedAt => {
                    date_search!(group_query, param, operator, created_at)
                }
                FilterField::UpdatedAt => {
                    date_search!(group_query, param, operator, updated_at)
                }
                FilterField::Permissions => {}
                _ => {
                    return Err(ApiError::BadRequest(format!(
                        "Field '{}' isn't searchable (or does not exist) for permissions",
                        param.field
                    )));
                }
            }
        }
        let all_groups: Vec<Group> = with_connection(&self.pool, async |conn| {
            group_query.load::<Group>(conn).await
        })
        .await?;
        let candidate_count = all_groups.len();

        if all_groups.is_empty() {
            return Ok((Vec::new(), known_count_or_skipped(page, 0)));
        }

        // For each group, build every Permission request against this
        // collection. Flatten into one big batch — Treetop returns decisions
        // in input order, so we know which group/permission each maps to.
        let perms = Permissions::all();
        let mut effective_filter = page.filters.permissions()?;
        effective_filter.ensure_contains(permissions_filter);
        let checks = all_groups
            .iter()
            .flat_map(|group| {
                let principal = PrincipalRef::new(0, [group.id]);
                perms.iter().map(move |permission| {
                    (
                        principal.clone(),
                        *permission,
                        ResourceRef::for_permission_on_collection(*permission, collection_id),
                    )
                })
            })
            .collect::<Vec<_>>();
        let decisions = self.authorize_flat_permission_checks(&checks).await?;

        let mut all_results: Vec<GroupPermission> = Vec::new();
        for (group, decisions) in all_groups.iter().zip(decisions.chunks(perms.len())) {
            let row = synthesize_permission_for_group(collection_id, group, decisions);

            // Filter:
            //   - empty filter → include if any permission is Allow
            //   - non-empty   → include only if ALL filter permissions are Allow
            let include = if effective_filter.iter().next().is_none() {
                permission_has_any_grant(&row)
            } else {
                effective_filter.iter().all(|wanted| {
                    let idx = perms
                        .iter()
                        .position(|p| p == wanted)
                        .expect("Permissions::all() must contain every variant");
                    decisions[idx]
                })
            };

            if include {
                all_results.push(GroupPermission {
                    group: group.clone(),
                    permission: row,
                });
            }
        }

        let total_count = known_count_or_skipped(page, all_results.len() as i64);
        let rows = paginate_in_memory(all_results, page)?;

        record_reverse_query(
            BACKEND_KIND,
            "groups_with_permissions_on",
            candidate_count,
            rows.len(),
            start.elapsed(),
        );

        Ok((rows, total_count))
    }

    async fn group_permission_on(
        &self,
        collection_id: i32,
        group_id: i32,
    ) -> Result<Option<Permission>, ApiError> {
        let start = Instant::now();
        let principal = PrincipalRef::new(0, vec![group_id]);
        let checks = Permissions::all()
            .iter()
            .map(|perm| PermissionRequest {
                resource: ResourceRef::for_permission_on_collection(*perm, collection_id),
                permissions: vec![*perm],
            })
            .collect();

        let decisions: Vec<bool> = self
            .authorize_many(&principal, checks)
            .await?
            .into_iter()
            .map(|d| d == PermissionDecision::Allow)
            .collect();

        let row = synthesize_permission(collection_id, group_id, &decisions);
        let result = if permission_has_any_grant(&row) {
            Some(row)
        } else {
            None
        };
        record_reverse_query(
            BACKEND_KIND,
            "group_permission_on",
            1,
            result.as_ref().map(|_| 1).unwrap_or(0),
            start.elapsed(),
        );
        Ok(result)
    }

    async fn apply_permissions(
        &self,
        _collection_id: i32,
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
        _collection_id: i32,
        _group_id: i32,
        _list: PermissionsList<Permissions>,
    ) -> Result<Permission, ApiError> {
        Err(ApiError::NotImplemented(
            "permission mutations are managed out-of-band when using the treetop backend"
                .to_string(),
        ))
    }

    async fn revoke_all(&self, _collection_id: i32, _group_id: i32) -> Result<(), ApiError> {
        Err(ApiError::NotImplemented(
            "permission mutations are managed out-of-band when using the treetop backend"
                .to_string(),
        ))
    }

    fn supports_mutation(&self) -> bool {
        false
    }

    fn supports_sql_visibility_pushdown(&self) -> bool {
        false
    }

    fn uses_sql_permission_store(&self) -> bool {
        false
    }

    fn supports_permission_provenance(&self) -> bool {
        false
    }

    fn kind(&self) -> &'static str {
        "treetop"
    }
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, from_value, json};

    use super::*;

    fn response(results: Value, successful: usize, failed: usize) -> AuthorizeBriefResponse {
        from_value(json!({
            "results": results,
            "version": { "hash": "test", "loaded_at": "2025-01-01T00:00:00Z" },
            "successful": successful,
            "failed": failed
        }))
        .unwrap()
    }

    #[test]
    fn build_batch_produces_correct_spans() {
        let principal = PrincipalRef::new(42, vec![100, 200]);
        let requests = vec![
            PermissionRequest {
                resource: ResourceRef::collection(1),
                permissions: vec![Permissions::ReadCollection, Permissions::UpdateCollection],
            },
            PermissionRequest {
                resource: ResourceRef::collection(2),
                permissions: vec![Permissions::ReadClass],
            },
            PermissionRequest {
                resource: ResourceRef::collection(3),
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

    #[test]
    fn failed_batch_item_is_a_backend_error() {
        let response = response(
            json!([{
                "index": 0,
                "status": "failed",
                "error": "schema mismatch"
            }]),
            0,
            1,
        );

        assert!(matches!(
            extract_decisions(&response, 1),
            Err(ApiError::PermissionBackendUnavailable(_))
        ));
    }

    #[test]
    fn missing_batch_item_is_a_backend_error() {
        let response = response(json!([]), 0, 0);

        assert!(matches!(
            extract_decisions(&response, 1),
            Err(ApiError::PermissionBackendUnavailable(_))
        ));
    }

    #[test]
    fn batch_decisions_are_ordered_by_response_index() {
        let response = response(
            json!([
                {
                    "index": 1,
                    "status": "success",
                    "result": {
                        "decision": "Deny",
                        "version": { "hash": "test", "loaded_at": "2025-01-01T00:00:00Z" },
                        "policy_id": ""
                    }
                },
                {
                    "index": 0,
                    "status": "success",
                    "result": {
                        "decision": "Allow",
                        "version": { "hash": "test", "loaded_at": "2025-01-01T00:00:00Z" },
                        "policy_id": "allow"
                    }
                }
            ]),
            2,
            0,
        );

        assert_eq!(extract_decisions(&response, 2).unwrap(), vec![true, false]);
    }
}
