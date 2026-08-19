use std::path::Path;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use reqwest::Certificate;
use treetop_client::{
    Action, AuthorizeBriefResponse, AuthorizeRequest, BatchResult, Client, DecisionBrief,
    Request as TreetopRequest, ValidationError,
};

use crate::config::AppConfig;
use crate::errors::ApiError;
use crate::models::search::{QueryOptions, QueryParamsExt};
use crate::models::{
    Collection, CollectionID, GroupID, GroupPermission, Permission, Permissions, PermissionsList,
};
use crate::pagination::{known_count_or_skipped, paginate_in_memory};
use crate::storage::{AuthorizationStorage, StorageHandle};
use crate::utilities::bounded_file::{MAX_CERTIFICATE_BUNDLE_BYTES, read_bounded_regular_file};

use super::backend::PermissionBackend;
use super::observability::{record_authorize_many, record_is_admin, record_reverse_query};
use super::storage::{collection_from_storage, group_from_storage};
use super::types::{PermissionDecision, PermissionRequest, PrincipalRef, ResourceRef};

const BACKEND_KIND: &str = "treetop";
const MAX_CEDAR_REQUESTS_PER_BATCH: usize = 512;

pub mod error;
pub mod mapping;

pub use error::{treetop_to_api_error, treetop_validation_to_api_error};
pub use mapping::{cedar_action, cedar_resource, cedar_user};

/// Production permission backend that delegates to a Treetop policy server.
///
/// - Connect once at startup via `TreetopPermissionBackend::connect`.
/// - `authorize_many` sends permission checks in bounded Treetop batches.
/// - `is_admin` dispatches to Treetop with a System resource check.
/// - Reverse queries (`collections_user_can`) load candidates from the local DB
///   then filter via Treetop batch authorization.
/// - Mutations (`apply_permissions`, `revoke_permissions`, `revoke_all`) return
///   `ApiError::NotImplemented` — permissions are managed out-of-band.
pub struct TreetopPermissionBackend {
    client: Client,
    storage: StorageHandle,
}

impl TreetopPermissionBackend {
    /// Connect to a Treetop server and perform a startup health check.
    ///
    /// Returns a fatal `ApiError` if the server is unreachable or unhealthy —
    /// per the spec, we fail-closed-fatal on startup health failures.
    pub(crate) async fn connect(
        url: &str,
        cfg: &AppConfig,
        storage: StorageHandle,
    ) -> Result<Self, ApiError> {
        let mut builder = Client::builder(url)
            .connect_timeout(Duration::from_millis(cfg.treetop_connect_timeout_ms))
            .request_timeout(Duration::from_millis(cfg.treetop_request_timeout_ms));

        if cfg.treetop_accept_invalid_certs {
            builder = builder.danger_accept_invalid_certs(true);
        }

        if let Some(path) = cfg.treetop_ca_cert.as_deref() {
            for certificate in load_treetop_ca_certificates(Path::new(path))? {
                builder = builder.add_root_certificate(certificate);
            }
        }

        let client = builder.build().map_err(treetop_to_api_error)?;

        // Startup health check — fail-closed-fatal per Q9 of the spec.
        client.health().await.map_err(treetop_to_api_error)?;

        Ok(Self { client, storage })
    }

    async fn authorize_cedar_requests(
        &self,
        mut requests: Box<dyn Iterator<Item = Result<TreetopRequest, ValidationError>> + Send + '_>,
        expected_count: usize,
    ) -> Result<Vec<bool>, ApiError> {
        let mut decisions = Vec::with_capacity(expected_count);
        loop {
            let chunk = requests
                .by_ref()
                .take(MAX_CEDAR_REQUESTS_PER_BATCH)
                .collect::<Result<Vec<_>, _>>()
                .map_err(treetop_validation_to_api_error)?;
            if chunk.is_empty() {
                break;
            }
            let chunk_len = chunk.len();
            let batch = AuthorizeRequest::from_requests(chunk);
            let response = self
                .client
                .authorize(&batch)
                .await
                .map_err(treetop_to_api_error)?;
            decisions.extend(extract_decisions(&response, chunk_len)?);
        }
        if decisions.len() != expected_count {
            return Err(ApiError::InternalServerError(format!(
                "constructed {} Treetop requests, expected {expected_count}",
                decisions.len()
            )));
        }
        Ok(decisions)
    }
}

fn load_treetop_ca_certificates(path: &Path) -> Result<Vec<Certificate>, ApiError> {
    let pem = read_bounded_regular_file(
        path,
        "Treetop CA certificate bundle",
        MAX_CERTIFICATE_BUNDLE_BYTES,
    )
    .map_err(|error| ApiError::InternalServerError(error.to_string()))?;
    let certificates = Certificate::from_pem_bundle(&pem).map_err(|error| {
        ApiError::InternalServerError(format!(
            "Failed to parse Treetop CA certificate bundle '{}': {error}",
            path.display()
        ))
    })?;
    if certificates.is_empty() {
        return Err(ApiError::InternalServerError(format!(
            "Treetop CA certificate bundle '{}' contains no certificates",
            path.display()
        )));
    }
    Ok(certificates)
}

fn permission_check_count(requests: &[PermissionRequest]) -> Result<usize, ApiError> {
    requests.iter().try_fold(0_usize, |count, request| {
        count
            .checked_add(request.permissions.len())
            .ok_or_else(|| ApiError::InternalServerError("too many permission checks".into()))
    })
}

fn collapse_permission_decisions(
    requests: &[PermissionRequest],
    cedar_decisions: &[bool],
) -> Result<Vec<PermissionDecision>, ApiError> {
    let expected_count = permission_check_count(requests)?;
    if cedar_decisions.len() != expected_count {
        return Err(ApiError::InternalServerError(format!(
            "received {} Cedar decisions for {expected_count} permission checks",
            cedar_decisions.len()
        )));
    }

    let mut decision_offset = 0;
    Ok(requests
        .iter()
        .map(|request| {
            let end = decision_offset + request.permissions.len();
            let all_allow = cedar_decisions[decision_offset..end]
                .iter()
                .all(|allowed| *allowed);
            decision_offset = end;
            if all_allow {
                PermissionDecision::Allow
            } else {
                PermissionDecision::Deny
            }
        })
        .collect())
}

/// Helper to extract boolean decisions from a Treetop authorize response.
///
/// The upstream `AuthorizeBriefResponse` has `.results()` returning a
/// `Vec<IndexedResult<AuthorizeDecisionBrief>>`. Each result is either
/// `BatchResult::Success { data }` or `BatchResult::Failed { message }`.
/// We extract a boolean per Cedar request. Structural and per-item errors fail
/// the complete authorization closed without returning upstream diagnostics.
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
            BatchResult::Failed { .. } => {
                return Err(ApiError::PermissionBackendUnavailable(format!(
                    "Treetop rejected batch item {}",
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

        let cedar_request_count = permission_check_count(&requests)?;
        let user = cedar_user(principal).map_err(treetop_validation_to_api_error)?;
        let cedar_requests = requests.iter().flat_map(|request| {
            let user = user.clone();
            let resource = cedar_resource(&request.resource);
            request.permissions.iter().map(move |permission| {
                Ok(TreetopRequest::new(
                    user.clone(),
                    cedar_action(*permission)?,
                    resource.clone()?,
                ))
            })
        });
        let cedar_decisions = self
            .authorize_cedar_requests(Box::new(cedar_requests), cedar_request_count)
            .await?;

        // Each input request is allowed iff all of its contiguous Cedar checks allow it.
        let decisions = collapse_permission_decisions(&requests, &cedar_decisions)?;

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
            cedar_user(principal).map_err(treetop_validation_to_api_error)?,
            Action::new("ReadTask").map_err(treetop_validation_to_api_error)?,
            cedar_resource(task).map_err(treetop_validation_to_api_error)?,
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
        let user = cedar_user(principal).map_err(treetop_validation_to_api_error)?;
        let requests = tasks.iter().map(|task| {
            Ok(TreetopRequest::new(
                user.clone(),
                Action::new("ReadTask")?,
                cedar_resource(task)?,
            ))
        });
        Ok(self
            .authorize_cedar_requests(Box::new(requests), tasks.len())
            .await?
            .into_iter()
            .map(|allowed| {
                if allowed {
                    PermissionDecision::Allow
                } else {
                    PermissionDecision::Deny
                }
            })
            .collect())
    }

    async fn collections_user_can(
        &self,
        principal: &PrincipalRef,
        permissions: &[Permissions],
    ) -> Result<Vec<Collection>, ApiError> {
        // Enumerate candidates from storage, then filter via Treetop.
        // We load all collections without any permission filtering, then
        // use paginate_authorized to filter via Treetop batch authorization.
        let start = Instant::now();
        let all_collections = self
            .storage
            .list_authorization_collection_candidates()
            .await?
            .into_iter()
            .map(collection_from_storage)
            .collect::<Result<Vec<_>, _>>()?;
        let candidate_count = all_collections.len();
        let tested_permissions = if permissions.is_empty() {
            Permissions::all()
        } else {
            permissions
        };
        let width = tested_permissions.len();
        let check_count = candidate_count.checked_mul(width).ok_or_else(|| {
            ApiError::InternalServerError("too many collection permission checks".into())
        })?;
        let user = cedar_user(principal).map_err(treetop_validation_to_api_error)?;
        let requests = all_collections.iter().flat_map(|collection| {
            let user = user.clone();
            tested_permissions.iter().map(move |permission| {
                let resource =
                    ResourceRef::for_permission_on_collection(*permission, collection.id);
                Ok(TreetopRequest::new(
                    user.clone(),
                    cedar_action(*permission)?,
                    cedar_resource(&resource)?,
                ))
            })
        });
        let decisions = self
            .authorize_cedar_requests(Box::new(requests), check_count)
            .await?;
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
        collection_id: CollectionID,
        permissions_filter: &[Permissions],
        page: &QueryOptions,
    ) -> Result<(Vec<GroupPermission>, i64), ApiError> {
        let start = Instant::now();
        let collection_id = collection_id.id();
        let all_groups = self
            .storage
            .list_authorization_group_candidates(page.clone())
            .await?
            .into_iter()
            .map(group_from_storage)
            .collect::<Result<Vec<_>, _>>()?;
        let candidate_count = all_groups.len();

        if all_groups.is_empty() {
            return Ok((Vec::new(), known_count_or_skipped(page, 0)));
        }

        // For each group, build every Permission request against this
        // collection. Treetop returns decisions in input order, so we know
        // which group/permission each maps to.
        let perms = Permissions::all();
        let mut effective_filter = page.filters().permissions()?;
        effective_filter.ensure_contains(permissions_filter);
        let check_count = all_groups.len().checked_mul(perms.len()).ok_or_else(|| {
            ApiError::InternalServerError("too many group permission checks".into())
        })?;
        let requests = all_groups.iter().flat_map(|group| {
            let user = cedar_user(&PrincipalRef::new(0, [group.id]));
            perms.iter().map(move |permission| {
                let resource =
                    ResourceRef::for_permission_on_collection(*permission, collection_id);
                Ok(TreetopRequest::new(
                    user.clone()?,
                    cedar_action(*permission)?,
                    cedar_resource(&resource)?,
                ))
            })
        });
        let decisions = self
            .authorize_cedar_requests(Box::new(requests), check_count)
            .await?;

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
        collection_id: CollectionID,
        group_id: GroupID,
    ) -> Result<Option<Permission>, ApiError> {
        let start = Instant::now();
        let collection_id = collection_id.id();
        let group_id = group_id.id();
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
        _collection_id: CollectionID,
        _group_id: GroupID,
        _list: PermissionsList,
        _replace_existing: bool,
    ) -> Result<Permission, ApiError> {
        Err(ApiError::NotImplemented(
            "permission mutations are managed out-of-band when using the treetop backend"
                .to_string(),
        ))
    }

    async fn revoke_permissions(
        &self,
        _collection_id: CollectionID,
        _group_id: GroupID,
        _list: PermissionsList,
    ) -> Result<Permission, ApiError> {
        Err(ApiError::NotImplemented(
            "permission mutations are managed out-of-band when using the treetop backend"
                .to_string(),
        ))
    }

    async fn revoke_all(
        &self,
        _collection_id: CollectionID,
        _group_id: GroupID,
    ) -> Result<(), ApiError> {
        Err(ApiError::NotImplemented(
            "permission mutations are managed out-of-band when using the treetop backend"
                .to_string(),
        ))
    }

    fn supports_mutation(&self) -> bool {
        false
    }

    fn supports_storage_visibility_filtering(&self) -> bool {
        false
    }

    fn uses_local_permission_store(&self) -> bool {
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
    use std::path::PathBuf;

    use serde_json::{Value, from_value, json};
    use uuid::Uuid;

    use super::*;

    const TEST_CA_PEM: &str = concat!(
        "-----BEGIN CERTIFICATE-----\n",
        "MIIBtjCCAVugAwIBAgITBmyf1XSXNmY/Owua2eiedgPySjAKBggqhkjOPQQDAjA5\n",
        "MQswCQYDVQQGEwJVUzEPMA0GA1UEChMGQW1hem9uMRkwFwYDVQQDExBBbWF6b24g\n",
        "Um9vdCBDQSAzMB4XDTE1MDUyNjAwMDAwMFoXDTQwMDUyNjAwMDAwMFowOTELMAkG\n",
        "A1UEBhMCVVMxDzANBgNVBAoTBkFtYXpvbjEZMBcGA1UEAxMQQW1hem9uIFJvb3Qg\n",
        "Q0EgMzBZMBMGByqGSM49AgEGCCqGSM49AwEHA0IABCmXp8ZBf8ANm+gBG1bG8lKl\n",
        "ui2yEujSLtf6ycXYqm0fc4E7O5hrOXwzpcVOho6AF2hiRVd9RFgdszflZwjrZt6j\n",
        "QjBAMA8GA1UdEwEB/wQFMAMBAf8wDgYDVR0PAQH/BAQDAgGGMB0GA1UdDgQWBBSr\n",
        "ttvXBp43rDCGB5Fwx5zEGbF4wDAKBggqhkjOPQQDAgNJADBGAiEA4IWSoxe3jfkr\n",
        "BqWTrBqYaGFy+uGh0PsceGCmQ5nFuMQCIQCcAu/xlJyzlvnrxir4tiz+OpAUFteM\n",
        "YyRIHN8wfdVoOw==\n",
        "-----END CERTIFICATE-----\n",
    );

    struct TestCaBundle {
        path: PathBuf,
    }

    impl TestCaBundle {
        fn new(contents: &[u8]) -> Self {
            let path =
                std::env::temp_dir().join(format!("hubuum-treetop-ca-{}.pem", Uuid::new_v4()));
            std::fs::write(&path, contents).expect("test CA bundle should be written");
            Self { path }
        }

        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TestCaBundle {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.path);
        }
    }

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
    fn configured_ca_bundle_loads_pem_certificates() {
        let contents = format!("{TEST_CA_PEM}{TEST_CA_PEM}");
        let bundle = TestCaBundle::new(contents.as_bytes());

        let certificates =
            load_treetop_ca_certificates(bundle.path()).expect("test CA bundle should load");

        assert_eq!(certificates.len(), 2);
    }

    #[test]
    fn configured_ca_bundle_rejects_files_without_certificates() {
        let bundle = TestCaBundle::new(b"");

        let error = load_treetop_ca_certificates(bundle.path())
            .expect_err("empty test CA bundle should be rejected");

        assert_eq!(
            error,
            ApiError::InternalServerError(format!(
                "Treetop CA certificate bundle '{}' contains no certificates",
                bundle.path().display()
            ))
        );
    }

    #[test]
    fn configured_ca_bundle_rejects_malformed_pem() {
        let bundle = TestCaBundle::new(b"not a certificate");

        let error = load_treetop_ca_certificates(bundle.path())
            .expect_err("malformed test CA bundle should be rejected");

        assert_eq!(
            error,
            ApiError::InternalServerError(format!(
                "Treetop CA certificate bundle '{}' contains no certificates",
                bundle.path().display()
            ))
        );
    }

    #[test]
    fn configured_ca_bundle_rejects_oversized_files() {
        let contents = vec![b'x'; MAX_CERTIFICATE_BUNDLE_BYTES + 1];
        let bundle = TestCaBundle::new(&contents);

        let error = load_treetop_ca_certificates(bundle.path())
            .expect_err("oversized test CA bundle should be rejected");

        assert!(matches!(
            error,
            ApiError::InternalServerError(message)
                if message.contains("exceeds the 4194304-byte limit")
        ));
    }

    #[test]
    fn per_request_decisions_are_collapsed_conjunctively() {
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

        let decisions =
            collapse_permission_decisions(&requests, &[true, true, false, true, false, true])
                .unwrap();

        assert_eq!(
            decisions,
            vec![
                PermissionDecision::Allow,
                PermissionDecision::Deny,
                PermissionDecision::Deny,
            ]
        );
    }

    #[test]
    fn collapsing_an_incomplete_decision_set_fails_closed() {
        let requests = vec![PermissionRequest {
            resource: ResourceRef::collection(1),
            permissions: vec![Permissions::ReadCollection, Permissions::UpdateCollection],
        }];

        assert!(matches!(
            collapse_permission_decisions(&requests, &[true]),
            Err(ApiError::InternalServerError(_))
        ));
    }

    #[test]
    fn failed_batch_item_is_a_backend_error() {
        let canary = "fixture-policy-secret-canary";
        let response = response(
            json!([{
                "index": 0,
                "status": "failed",
                "error": canary
            }]),
            0,
            1,
        );

        let error = extract_decisions(&response, 1).expect_err("batch failure must fail closed");
        assert!(matches!(error, ApiError::PermissionBackendUnavailable(_)));
        assert!(!error.to_string().contains(canary));
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
    fn extra_batch_item_is_a_backend_error() {
        let response = response(
            json!([
                {
                    "index": 0,
                    "status": "success",
                    "result": {
                        "decision": "Allow",
                        "version": { "hash": "test", "loaded_at": "2025-01-01T00:00:00Z" },
                        "policy_id": "allow"
                    }
                },
                {
                    "index": 1,
                    "status": "success",
                    "result": {
                        "decision": "Deny",
                        "version": { "hash": "test", "loaded_at": "2025-01-01T00:00:00Z" },
                        "policy_id": ""
                    }
                }
            ]),
            2,
            0,
        );

        assert!(matches!(
            extract_decisions(&response, 1),
            Err(ApiError::PermissionBackendUnavailable(_))
        ));
    }

    #[test]
    fn duplicate_batch_index_is_a_backend_error() {
        let response = response(
            json!([
                {
                    "index": 0,
                    "status": "success",
                    "result": {
                        "decision": "Allow",
                        "version": { "hash": "test", "loaded_at": "2025-01-01T00:00:00Z" },
                        "policy_id": "allow"
                    }
                },
                {
                    "index": 0,
                    "status": "success",
                    "result": {
                        "decision": "Deny",
                        "version": { "hash": "test", "loaded_at": "2025-01-01T00:00:00Z" },
                        "policy_id": ""
                    }
                }
            ]),
            2,
            0,
        );

        assert!(matches!(
            extract_decisions(&response, 2),
            Err(ApiError::PermissionBackendUnavailable(_))
        ));
    }

    #[test]
    fn out_of_range_batch_index_is_a_backend_error() {
        let response = response(
            json!([{
                "index": 1,
                "status": "success",
                "result": {
                    "decision": "Allow",
                    "version": { "hash": "test", "loaded_at": "2025-01-01T00:00:00Z" },
                    "policy_id": "allow"
                }
            }]),
            1,
            0,
        );

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
