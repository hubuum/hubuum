use std::future::Future;
use std::pin::Pin;
use std::sync::Mutex;

use async_trait::async_trait;

use crate::errors::ApiError;
use crate::models::search::{QueryOptions, QueryParamsExt};
use crate::models::{
    Collection, CollectionID, Group, GroupID, GroupPermission, Permission, Permissions,
    PermissionsList,
};
use crate::pagination::{known_count_or_skipped, paginate_in_memory, prepare_db_pagination};

use super::super::backend::{CompleteCollectionCandidateLimit, PermissionBackend};
use super::super::types::{
    PermissionDecision, PermissionRequest, PrincipalRef, ResourceFields, ResourceKind, ResourceRef,
};

use crate::permissions::synthesis::{
    permission_has_any_grant, synthesize_permission, synthesize_permission_for_group,
};

/// A single Allow rule. The mock evaluates a request as Allow iff there
/// exists a rule whose group_id is in the principal's group set, whose
/// action matches the requested permission, whose kind/id matches (with
/// `id = None` meaning "any id of this kind"), and whose attrs (when
/// specified) match the request resource's attrs.
#[derive(Debug, Clone)]
pub struct MockAllowRule {
    pub group_id: i32,
    pub action: Permissions,
    pub resource_kind: ResourceKind,
    /// When None, matches any id within `resource_kind`.
    pub resource_id: Option<i32>,
    /// Optional attrs filter. Only the fields set here are matched; an
    /// attr that's None on the rule means "don't care".
    pub attrs: ResourceFields,
}

/// Marker for "is admin" decision. The mock matches admin via a rule
/// whose `action == Permissions::ReadCollection` AND `resource_kind ==
/// ResourceKind::System` — chosen because System resources never carry
/// useful permissions in the real schema, so this overload is internal
/// to the mock.
const ADMIN_ACTION_MARKER: Permissions = Permissions::ReadCollection;

type AuthorizationHook = Box<dyn FnOnce() -> Pin<Box<dyn Future<Output = ()> + Send>> + Send>;

struct DeferredAuthorizationHook {
    calls_to_skip: usize,
    hook: AuthorizationHook,
}

struct StoredMockRule {
    rule: MockAllowRule,
    prospective_only: bool,
}

#[derive(Default)]
pub struct MockTreetopBackend {
    rules: Mutex<Vec<StoredMockRule>>,
    task_read_rules: Mutex<Vec<(i32, Option<i32>)>>,
    /// Optional override of the candidate group set used by
    /// groups_with_permissions_on. When None, the method returns
    /// NotImplemented (matching the previous behavior). Set this in
    /// tests that want to exercise the groups-listing path.
    group_candidates: Mutex<Option<Vec<Group>>>,
    authorization_hook: Mutex<Option<DeferredAuthorizationHook>>,
    authorization_batch_sizes: Mutex<Vec<usize>>,
}

impl MockTreetopBackend {
    pub fn new() -> Self {
        Self {
            rules: Mutex::new(Vec::new()),
            task_read_rules: Mutex::new(Vec::new()),
            group_candidates: Mutex::new(None),
            authorization_hook: Mutex::new(None),
            authorization_batch_sizes: Mutex::new(Vec::new()),
        }
    }

    pub fn add_rule(&self, rule: MockAllowRule) {
        self.rules.lock().unwrap().push(StoredMockRule {
            rule,
            prospective_only: false,
        });
    }

    /// Match prospective resources and collection probes, which have no stored
    /// ID. This keeps tests from granting existing objects through a wildcard.
    pub fn add_prospective_rule(&self, rule: MockAllowRule) {
        assert!(
            rule.resource_id.is_none(),
            "prospective rules cannot name stored IDs"
        );
        self.rules.lock().unwrap().push(StoredMockRule {
            rule,
            prospective_only: true,
        });
    }

    /// Add an admin rule — the principal's group_id grants admin status.
    pub fn add_admin_rule(&self, group_id: i32) {
        self.add_rule(MockAllowRule {
            group_id,
            action: ADMIN_ACTION_MARKER,
            resource_kind: ResourceKind::System,
            resource_id: None,
            attrs: ResourceFields::default(),
        });
    }

    pub fn add_task_read_rule(&self, group_id: i32, task_id: Option<i32>) {
        self.task_read_rules
            .lock()
            .unwrap()
            .push((group_id, task_id));
    }

    /// Set the group candidates for groups_with_permissions_on. When set,
    /// the mock will synthesize Permission rows for these groups instead
    /// of returning NotImplemented.
    pub fn set_group_candidates(&self, groups: Vec<Group>) {
        *self.group_candidates.lock().unwrap() = Some(groups);
    }

    pub fn set_authorization_hook<F, Fut>(&self, hook: F)
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.set_authorization_hook_after_calls(0, hook);
    }

    pub fn set_authorization_hook_after_calls<F, Fut>(&self, calls_to_skip: usize, hook: F)
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        *self.authorization_hook.lock().unwrap() = Some(DeferredAuthorizationHook {
            calls_to_skip,
            hook: Box::new(move || Box::pin(hook())),
        });
    }

    pub fn authorization_batch_sizes(&self) -> Vec<usize> {
        self.authorization_batch_sizes.lock().unwrap().clone()
    }

    fn rule_matches(rule: &MockAllowRule, request: &PermissionRequest, perm: Permissions) -> bool {
        if rule.action != perm {
            return false;
        }
        if rule.resource_kind != request.resource.kind() {
            return false;
        }
        if let Some(id) = rule.resource_id
            && Some(id) != request.resource.id()
        {
            return false;
        }
        // attrs match: every Some field on the rule must equal the
        // corresponding field on the request resource. None on rule = wildcard.
        if rule.attrs.collection_id.is_some()
            && rule.attrs.collection_id != request.resource.fields().collection_id
        {
            return false;
        }
        if rule.attrs.from_collection_id.is_some()
            && rule.attrs.from_collection_id != request.resource.fields().from_collection_id
        {
            return false;
        }
        if rule.attrs.to_collection_id.is_some()
            && rule.attrs.to_collection_id != request.resource.fields().to_collection_id
        {
            return false;
        }
        if rule.attrs.class_id.is_some()
            && rule.attrs.class_id != request.resource.fields().class_id
        {
            return false;
        }
        if rule.attrs.from_class_id.is_some()
            && rule.attrs.from_class_id != request.resource.fields().from_class_id
        {
            return false;
        }
        if rule.attrs.to_class_id.is_some()
            && rule.attrs.to_class_id != request.resource.fields().to_class_id
        {
            return false;
        }
        if rule.attrs.from_object_id.is_some()
            && rule.attrs.from_object_id != request.resource.fields().from_object_id
        {
            return false;
        }
        if rule.attrs.to_object_id.is_some()
            && rule.attrs.to_object_id != request.resource.fields().to_object_id
        {
            return false;
        }
        if rule.attrs.class_relation_id.is_some()
            && rule.attrs.class_relation_id != request.resource.fields().class_relation_id
        {
            return false;
        }
        if rule.attrs.submitted_by.is_some()
            && rule.attrs.submitted_by != request.resource.fields().submitted_by
        {
            return false;
        }
        if rule.attrs.name.is_some() && rule.attrs.name != request.resource.fields().name {
            return false;
        }
        true
    }

    fn evaluate(
        &self,
        principal: &PrincipalRef,
        request: &PermissionRequest,
    ) -> PermissionDecision {
        let rules = self.rules.lock().unwrap();
        // Conjunctive: all requested permissions must be satisfied.
        let all_allowed = request.permissions.iter().all(|perm| {
            rules.iter().any(|r| {
                (!r.prospective_only || request.resource.id().is_none())
                    && principal.group_ids.contains(&r.rule.group_id)
                    && Self::rule_matches(&r.rule, request, *perm)
            })
        });
        if all_allowed {
            PermissionDecision::Allow
        } else {
            PermissionDecision::Deny
        }
    }
}

#[async_trait]
impl PermissionBackend for MockTreetopBackend {
    async fn authorize_many(
        &self,
        principal: &PrincipalRef,
        requests: Vec<PermissionRequest>,
    ) -> Result<Vec<PermissionDecision>, ApiError> {
        self.authorization_batch_sizes
            .lock()
            .unwrap()
            .push(requests.len());
        let hook = {
            let mut deferred = self.authorization_hook.lock().unwrap();
            if deferred
                .as_ref()
                .is_some_and(|hook| hook.calls_to_skip == 0)
            {
                deferred.take().map(|hook| hook.hook)
            } else {
                if let Some(hook) = deferred.as_mut() {
                    hook.calls_to_skip -= 1;
                }
                None
            }
        };
        if let Some(hook) = hook {
            hook().await;
        }
        // Order preserved by zipping per request.
        Ok(requests
            .iter()
            .map(|r| self.evaluate(principal, r))
            .collect())
    }

    async fn authorize_task(
        &self,
        principal: &PrincipalRef,
        task: &ResourceRef,
    ) -> Result<PermissionDecision, ApiError> {
        let allowed = self
            .task_read_rules
            .lock()
            .unwrap()
            .iter()
            .any(|(group_id, task_id)| {
                principal.group_ids.contains(group_id)
                    && task_id.is_none_or(|task_id| Some(task_id) == task.id())
            });
        Ok(if allowed {
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
        let rules = self.task_read_rules.lock().unwrap();
        Ok(tasks
            .iter()
            .map(|task| {
                if rules.iter().any(|(group_id, task_id)| {
                    principal.group_ids.contains(group_id)
                        && task_id.is_none_or(|task_id| Some(task_id) == task.id())
                }) {
                    PermissionDecision::Allow
                } else {
                    PermissionDecision::Deny
                }
            })
            .collect())
    }

    async fn collections_user_can(
        &self,
        _principal: &PrincipalRef,
        _permissions: &[Permissions],
        _candidate_limit: CompleteCollectionCandidateLimit,
    ) -> Result<Vec<Collection>, ApiError> {
        Err(ApiError::NotImplemented(
            "MockTreetopBackend does not enumerate collections — exercise via the real Treetop in Phase 5.4".to_string(),
        ))
    }

    async fn groups_with_permissions_on(
        &self,
        collection_id: CollectionID,
        permissions_filter: &[Permissions],
        page: &QueryOptions,
    ) -> Result<(Vec<GroupPermission>, i64), ApiError> {
        let collection_id = collection_id.id();
        let groups_opt = self.group_candidates.lock().unwrap().clone();
        let all_groups = match groups_opt {
            Some(g) => g,
            None => {
                return Err(ApiError::NotImplemented(
                    "MockTreetopBackend does not enumerate groups — call set_group_candidates() in tests".to_string(),
                ))
            }
        };

        if all_groups.is_empty() {
            return Ok((Vec::new(), known_count_or_skipped(page, 0)));
        }

        let perms = Permissions::all();
        let mut effective_filter = page.filters().permissions()?;
        effective_filter.ensure_contains(permissions_filter);
        let mut all_results: Vec<GroupPermission> = Vec::new();

        for group in &all_groups {
            let principal = PrincipalRef::new(0, vec![group.id]);
            let requests: Vec<PermissionRequest> = perms
                .iter()
                .map(|p| PermissionRequest {
                    resource: ResourceRef::for_permission_on_collection(*p, collection_id),
                    permissions: vec![*p],
                })
                .collect();

            let decisions: Vec<bool> = self
                .authorize_many(&principal, requests)
                .await?
                .into_iter()
                .map(|d| d == PermissionDecision::Allow)
                .collect();

            let row = synthesize_permission_for_group(collection_id, group, &decisions);

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
        let prepared_page = prepare_db_pagination::<GroupPermission>(page)?;
        let rows = paginate_in_memory(all_results, &prepared_page)?;

        Ok((rows, total_count))
    }

    async fn group_permission_on(
        &self,
        collection_id: CollectionID,
        group_id: GroupID,
    ) -> Result<Option<Permission>, ApiError> {
        let collection_id = collection_id.id();
        let group_id = group_id.id();
        let principal = PrincipalRef::new(0, vec![group_id]);
        let requests: Vec<PermissionRequest> = Permissions::all()
            .iter()
            .map(|perm| PermissionRequest {
                resource: ResourceRef::for_permission_on_collection(*perm, collection_id),
                permissions: vec![*perm],
            })
            .collect();

        let decisions: Vec<bool> = self
            .authorize_many(&principal, requests)
            .await?
            .into_iter()
            .map(|d| d == PermissionDecision::Allow)
            .collect();

        let row = synthesize_permission(collection_id, group_id, &decisions);
        Ok(if permission_has_any_grant(&row) {
            Some(row)
        } else {
            None
        })
    }

    async fn apply_permissions(
        &self,
        _collection_id: CollectionID,
        _group_id: GroupID,
        _list: PermissionsList,
        _replace_existing: bool,
    ) -> Result<Permission, ApiError> {
        Err(ApiError::NotImplemented(
            "permission mutations are managed out-of-band when using a treetop-style backend"
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
            "permission mutations are managed out-of-band when using a treetop-style backend"
                .to_string(),
        ))
    }

    async fn revoke_all(
        &self,
        _collection_id: CollectionID,
        _group_id: GroupID,
    ) -> Result<(), ApiError> {
        Err(ApiError::NotImplemented(
            "permission mutations are managed out-of-band when using a treetop-style backend"
                .to_string(),
        ))
    }

    async fn is_admin(&self, principal: &PrincipalRef) -> Result<bool, ApiError> {
        // Admin decision is a backend rule lookup, not SQL group membership.
        let rules = self.rules.lock().unwrap();
        let is_admin = rules.iter().any(|r| {
            r.rule.action == ADMIN_ACTION_MARKER
                && r.rule.resource_kind == ResourceKind::System
                && principal.group_ids.contains(&r.rule.group_id)
        });
        Ok(is_admin)
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
        "mock-treetop"
    }
}
