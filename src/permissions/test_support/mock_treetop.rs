use std::sync::Mutex;

use async_trait::async_trait;
use chrono::DateTime;

use crate::errors::ApiError;
use crate::models::search::{QueryOptions, QueryParamsExt};
use crate::models::{Collection, Group, GroupPermission, Permission, Permissions, PermissionsList};
use crate::pagination::{known_count_or_skipped, paginate_in_memory};

use super::super::backend::PermissionBackend;
use super::super::types::{
    PermissionDecision, PermissionRequest, PrincipalRef, ResourceAttrs, ResourceKind, ResourceRef,
};

/// Build a synthetic Permission row from a per-variant decision list.
/// Used by both MockTreetopBackend and TreetopPermissionBackend for
/// synthesizing Permission rows from per-variant authorize results.
pub(crate) fn synthesize_permission(
    collection_id: i32,
    group_id: i32,
    decisions: &[bool],
) -> Permission {
    use Permissions::*;
    let synthetic_timestamp = DateTime::UNIX_EPOCH.naive_utc();

    let perms = Permissions::all();
    debug_assert_eq!(
        perms.len(),
        decisions.len(),
        "synthesize_permission: decisions length must match Permissions::all() length"
    );

    let mut row = Permission {
        // Synthetic rows have no database identity. Reusing the group id gives
        // cursor pagination a stable, unique key across requests.
        id: group_id,
        collection_id,
        group_id,
        has_read_collection: false,
        has_update_collection: false,
        has_delete_collection: false,
        has_delegate_collection: false,
        has_create_class: false,
        has_read_class: false,
        has_update_class: false,
        has_delete_class: false,
        has_create_object: false,
        has_read_object: false,
        has_update_object: false,
        has_delete_object: false,
        has_create_class_relation: false,
        has_read_class_relation: false,
        has_update_class_relation: false,
        has_delete_class_relation: false,
        has_create_object_relation: false,
        has_read_object_relation: false,
        has_update_object_relation: false,
        has_delete_object_relation: false,
        has_read_template: false,
        has_create_template: false,
        has_update_template: false,
        has_delete_template: false,
        has_read_remote_target: false,
        has_create_remote_target: false,
        has_update_remote_target: false,
        has_delete_remote_target: false,
        has_execute_remote_target: false,
        has_read_audit: false,
        has_manage_event_subscription: false,
        created_at: synthetic_timestamp,
        updated_at: synthetic_timestamp,
    };

    for (perm, decision) in perms.iter().zip(decisions) {
        if !decision {
            continue;
        }
        match perm {
            ReadCollection => row.has_read_collection = true,
            UpdateCollection => row.has_update_collection = true,
            DeleteCollection => row.has_delete_collection = true,
            DelegateCollection => row.has_delegate_collection = true,
            CreateClass => row.has_create_class = true,
            ReadClass => row.has_read_class = true,
            UpdateClass => row.has_update_class = true,
            DeleteClass => row.has_delete_class = true,
            CreateObject => row.has_create_object = true,
            ReadObject => row.has_read_object = true,
            UpdateObject => row.has_update_object = true,
            DeleteObject => row.has_delete_object = true,
            CreateClassRelation => row.has_create_class_relation = true,
            ReadClassRelation => row.has_read_class_relation = true,
            UpdateClassRelation => row.has_update_class_relation = true,
            DeleteClassRelation => row.has_delete_class_relation = true,
            CreateObjectRelation => row.has_create_object_relation = true,
            ReadObjectRelation => row.has_read_object_relation = true,
            UpdateObjectRelation => row.has_update_object_relation = true,
            DeleteObjectRelation => row.has_delete_object_relation = true,
            ReadTemplate => row.has_read_template = true,
            CreateTemplate => row.has_create_template = true,
            UpdateTemplate => row.has_update_template = true,
            DeleteTemplate => row.has_delete_template = true,
            ReadRemoteTarget => row.has_read_remote_target = true,
            CreateRemoteTarget => row.has_create_remote_target = true,
            UpdateRemoteTarget => row.has_update_remote_target = true,
            DeleteRemoteTarget => row.has_delete_remote_target = true,
            ExecuteRemoteTarget => row.has_execute_remote_target = true,
            ReadAudit => row.has_read_audit = true,
            ManageEventSubscription => row.has_manage_event_subscription = true,
        }
    }

    row
}

pub(crate) fn synthesize_permission_for_group(
    collection_id: i32,
    group: &Group,
    decisions: &[bool],
) -> Permission {
    let mut permission = synthesize_permission(collection_id, group.id, decisions);
    permission.created_at = group.created_at;
    permission.updated_at = group.updated_at;
    permission
}

/// Whether a synthesized Permission has at least one true field.
pub(crate) fn permission_has_any_grant(p: &Permission) -> bool {
    p.has_read_collection
        || p.has_update_collection
        || p.has_delete_collection
        || p.has_delegate_collection
        || p.has_create_class
        || p.has_read_class
        || p.has_update_class
        || p.has_delete_class
        || p.has_create_object
        || p.has_read_object
        || p.has_update_object
        || p.has_delete_object
        || p.has_create_class_relation
        || p.has_read_class_relation
        || p.has_update_class_relation
        || p.has_delete_class_relation
        || p.has_create_object_relation
        || p.has_read_object_relation
        || p.has_update_object_relation
        || p.has_delete_object_relation
        || p.has_read_template
        || p.has_create_template
        || p.has_update_template
        || p.has_delete_template
        || p.has_read_remote_target
        || p.has_create_remote_target
        || p.has_update_remote_target
        || p.has_delete_remote_target
        || p.has_execute_remote_target
        || p.has_read_audit
        || p.has_manage_event_subscription
}

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
    task_read_rules: Mutex<Vec<(i32, Option<i32>)>>,
    /// Optional override of the candidate group set used by
    /// groups_with_permissions_on. When None, the method returns
    /// NotImplemented (matching the previous behavior). Set this in
    /// tests that want to exercise the groups-listing path.
    group_candidates: Mutex<Option<Vec<Group>>>,
}

impl MockTreetopBackend {
    pub fn new() -> Self {
        Self {
            rules: Mutex::new(Vec::new()),
            task_read_rules: Mutex::new(Vec::new()),
            group_candidates: Mutex::new(None),
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

    fn rule_matches(rule: &MockAllowRule, request: &PermissionRequest, perm: Permissions) -> bool {
        if rule.action != perm {
            return false;
        }
        if rule.resource_kind != request.resource.kind {
            return false;
        }
        if let Some(id) = rule.resource_id
            && id != request.resource.id
        {
            return false;
        }
        // attrs match: every Some field on the rule must equal the
        // corresponding field on the request resource. None on rule = wildcard.
        if rule.attrs.collection_id.is_some()
            && rule.attrs.collection_id != request.resource.attrs.collection_id
        {
            return false;
        }
        if rule.attrs.from_collection_id.is_some()
            && rule.attrs.from_collection_id != request.resource.attrs.from_collection_id
        {
            return false;
        }
        if rule.attrs.to_collection_id.is_some()
            && rule.attrs.to_collection_id != request.resource.attrs.to_collection_id
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
                    && task_id.is_none_or(|task_id| task_id == task.id)
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
                        && task_id.is_none_or(|task_id| task_id == task.id)
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
    ) -> Result<Vec<Collection>, ApiError> {
        Err(ApiError::NotImplemented(
            "MockTreetopBackend does not enumerate collections — exercise via the real Treetop in Phase 5.4".to_string(),
        ))
    }

    async fn groups_with_permissions_on(
        &self,
        collection_id: i32,
        permissions_filter: &[Permissions],
        page: &QueryOptions,
    ) -> Result<(Vec<GroupPermission>, i64), ApiError> {
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
        let mut effective_filter = page.filters.permissions()?;
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
        let rows = paginate_in_memory(all_results, page)?;

        Ok((rows, total_count))
    }

    async fn group_permission_on(
        &self,
        collection_id: i32,
        group_id: i32,
    ) -> Result<Option<Permission>, ApiError> {
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
        _collection_id: i32,
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
        _collection_id: i32,
        _group_id: i32,
        _list: PermissionsList<Permissions>,
    ) -> Result<Permission, ApiError> {
        Err(ApiError::NotImplemented(
            "permission mutations are managed out-of-band when using a treetop-style backend"
                .to_string(),
        ))
    }

    async fn revoke_all(&self, _collection_id: i32, _group_id: i32) -> Result<(), ApiError> {
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
        "mock-treetop"
    }
}
