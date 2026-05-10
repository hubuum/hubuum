use std::sync::Mutex;

use async_trait::async_trait;

use crate::errors::ApiError;
use crate::models::search::QueryOptions;
use crate::models::{GroupPermission, Namespace, Permission, Permissions, PermissionsList};

use super::super::backend::PermissionBackend;
use super::super::types::{
    PermissionDecision, PermissionRequest, PrincipalRef, ResourceAttrs, ResourceKind,
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
    pub attrs: ResourceAttrs,
}

/// Marker for "is admin" decision. The mock matches admin via a rule
/// whose `action == Permissions::ReadCollection` AND `resource_kind ==
/// ResourceKind::System` — chosen because System resources never carry
/// useful permissions in the real schema, so this overload is internal
/// to the mock.
const ADMIN_ACTION_MARKER: Permissions = Permissions::ReadCollection;

#[derive(Default)]
pub struct MockTreetopBackend {
    rules: Mutex<Vec<MockAllowRule>>,
}

impl MockTreetopBackend {
    pub fn new() -> Self {
        Self {
            rules: Mutex::new(Vec::new()),
        }
    }

    pub fn add_rule(&self, rule: MockAllowRule) {
        self.rules.lock().unwrap().push(rule);
    }

    /// Add an admin rule — the principal's group_id grants admin status.
    pub fn add_admin_rule(&self, group_id: i32) {
        self.add_rule(MockAllowRule {
            group_id,
            action: ADMIN_ACTION_MARKER,
            resource_kind: ResourceKind::System,
            resource_id: None,
            attrs: ResourceAttrs::default(),
        });
    }

    fn rule_matches(rule: &MockAllowRule, request: &PermissionRequest, perm: Permissions) -> bool {
        if rule.action != perm {
            return false;
        }
        if rule.resource_kind != request.resource.kind {
            return false;
        }
        if let Some(id) = rule.resource_id {
            if id != request.resource.id {
                return false;
            }
        }
        // attrs match: every Some field on the rule must equal the
        // corresponding field on the request resource. None on rule = wildcard.
        if rule.attrs.namespace_id.is_some()
            && rule.attrs.namespace_id != request.resource.attrs.namespace_id
        {
            return false;
        }
        if rule.attrs.from_namespace_id.is_some()
            && rule.attrs.from_namespace_id != request.resource.attrs.from_namespace_id
        {
            return false;
        }
        if rule.attrs.to_namespace_id.is_some()
            && rule.attrs.to_namespace_id != request.resource.attrs.to_namespace_id
        {
            return false;
        }
        if rule.attrs.class_id.is_some() && rule.attrs.class_id != request.resource.attrs.class_id {
            return false;
        }
        if rule.attrs.from_class_id.is_some()
            && rule.attrs.from_class_id != request.resource.attrs.from_class_id
        {
            return false;
        }
        if rule.attrs.to_class_id.is_some()
            && rule.attrs.to_class_id != request.resource.attrs.to_class_id
        {
            return false;
        }
        if rule.attrs.from_object_id.is_some()
            && rule.attrs.from_object_id != request.resource.attrs.from_object_id
        {
            return false;
        }
        if rule.attrs.to_object_id.is_some()
            && rule.attrs.to_object_id != request.resource.attrs.to_object_id
        {
            return false;
        }
        if rule.attrs.class_relation_id.is_some()
            && rule.attrs.class_relation_id != request.resource.attrs.class_relation_id
        {
            return false;
        }
        if rule.attrs.submitted_by.is_some()
            && rule.attrs.submitted_by != request.resource.attrs.submitted_by
        {
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
                principal.group_ids.contains(&r.group_id) && Self::rule_matches(r, request, *perm)
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
        // Order preserved by zipping per request.
        Ok(requests
            .iter()
            .map(|r| self.evaluate(principal, r))
            .collect())
    }

    async fn namespaces_user_can(
        &self,
        _principal: &PrincipalRef,
        _permissions: &[Permissions],
    ) -> Result<Vec<Namespace>, ApiError> {
        Err(ApiError::NotImplemented(
            "MockTreetopBackend does not enumerate namespaces — exercise via the real Treetop in Phase 5.4".to_string(),
        ))
    }

    async fn groups_with_permissions_on(
        &self,
        _namespace_id: i32,
        _permissions_filter: &[Permissions],
        _page: &QueryOptions,
    ) -> Result<(Vec<GroupPermission>, i64), ApiError> {
        Err(ApiError::NotImplemented(
            "MockTreetopBackend does not enumerate groups".to_string(),
        ))
    }

    async fn group_permission_on(
        &self,
        _namespace_id: i32,
        _group_id: i32,
    ) -> Result<Option<Permission>, ApiError> {
        Err(ApiError::NotImplemented(
            "MockTreetopBackend does not synthesize Permission rows".to_string(),
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
            "permission mutations are managed out-of-band when using a treetop-style backend"
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
            "permission mutations are managed out-of-band when using a treetop-style backend"
                .to_string(),
        ))
    }

    async fn revoke_all(&self, _namespace_id: i32, _group_id: i32) -> Result<(), ApiError> {
        Err(ApiError::NotImplemented(
            "permission mutations are managed out-of-band when using a treetop-style backend"
                .to_string(),
        ))
    }

    async fn is_admin(&self, principal: &PrincipalRef) -> Result<bool, ApiError> {
        // Admin decision is a backend rule lookup, not SQL group membership.
        let rules = self.rules.lock().unwrap();
        let is_admin = rules.iter().any(|r| {
            r.action == ADMIN_ACTION_MARKER
                && r.resource_kind == ResourceKind::System
                && principal.group_ids.contains(&r.group_id)
        });
        Ok(is_admin)
    }

    fn supports_mutation(&self) -> bool {
        false
    }
    fn kind(&self) -> &'static str {
        "mock-treetop"
    }
}
