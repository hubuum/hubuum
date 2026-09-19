use crate::errors::ApiError;
use crate::services::authorization_resources::task_authorization_resources;
use chrono::Utc;
use hubuum_storage_core::StorageAuthorizationResourceKey;
use hubuum_storage_core::{
    TaskDiscoveryPredicate as P, TaskDiscoverySearch, TaskRemoteSideEffectState,
};
use std::collections::{BTreeSet, HashMap, HashSet};

pub(super) const FILTERS: &[&str] = &[
    "class_id",
    "object_id",
    "collection_id",
    "relation_type",
    "relation_id",
    "schema_revision",
    "schema_work_kind",
    "schema_work_status",
    "computation_revision",
    "remote_target_id",
    "remote_side_effect_state",
    "export_scope_kind",
    "export_template_id",
    "export_has_warnings",
    "export_truncated",
    "import_dry_run",
    "import_atomicity",
    "import_collision_policy",
    "import_permission_policy",
    "import_has_failed_items",
    "backup_include_history",
    "output_state",
];

pub(super) fn parse(values: &HashMap<String, String>) -> Result<TaskDiscoverySearch, ApiError> {
    fn value<T: serde::de::DeserializeOwned>(key: &str, raw: &str) -> Result<T, ApiError> {
        serde_json::from_value(serde_json::Value::String(raw.into()))
            .map_err(|_| ApiError::BadRequest(format!("invalid {key} filter")))
    }
    let mut predicates = Vec::new();
    for (key, raw) in values {
        let invalid = || ApiError::BadRequest(format!("invalid {key} filter"));
        macro_rules! id {
            ($ty:ty, $variant:ident) => {
                P::$variant(<$ty>::new(raw.parse().map_err(|_| invalid())?).map_err(|_| invalid())?)
            };
        }
        macro_rules! boolean {
            ($variant:ident) => {
                P::$variant(raw.parse::<bool>().map_err(|_| invalid())?)
            };
        }
        let predicate = match key.as_str() {
            "class_id" => id!(hubuum_domain::ClassId, Class),
            "object_id" => id!(hubuum_domain::ObjectId, Object),
            "collection_id" => id!(hubuum_domain::CollectionId, Collection),
            "remote_target_id" => id!(hubuum_domain::RemoteTargetId, RemoteTarget),
            "export_template_id" => id!(hubuum_domain::ExportTemplateId, ExportTemplate),
            "schema_revision" => {
                P::SchemaRevision(serde_json::from_str(raw).map_err(|_| invalid())?)
            }
            "computation_revision" => P::ComputationRevision(raw.parse().map_err(|_| invalid())?),
            "schema_work_kind" => P::SchemaWorkKind(value(key, raw)?),
            "schema_work_status" => P::SchemaWorkStatus(value(key, raw)?),
            "export_scope_kind" => P::ExportScope(value(key, raw)?),
            "export_has_warnings" => boolean!(ExportHasWarnings),
            "export_truncated" => boolean!(ExportTruncated),
            "import_dry_run" => boolean!(ImportDryRun),
            "import_has_failed_items" => boolean!(ImportHasFailedItems),
            "backup_include_history" => boolean!(BackupIncludeHistory),
            "import_atomicity" => P::ImportAtomicity(value(key, raw)?),
            "import_collision_policy" => P::ImportCollisionPolicy(value(key, raw)?),
            "import_permission_policy" => P::ImportPermissionPolicy(value(key, raw)?),
            "output_state" => P::OutputState(value(key, raw)?),
            "remote_side_effect_state" => P::RemoteSideEffect(match raw.as_str() {
                "not_sent" => TaskRemoteSideEffectState::NotSent,
                "possibly_sent" => TaskRemoteSideEffectState::PossiblySent,
                "legacy_unknown" => TaskRemoteSideEffectState::LegacyUnknown,
                _ => return Err(invalid()),
            }),
            _ => continue,
        };
        predicates.push(predicate);
    }
    match (values.get("relation_type"), values.get("relation_id")) {
        (None, None) => {}
        (Some(kind), Some(id)) => {
            let invalid = || {
                ApiError::BadRequest("relation_type must be class_relation or object_relation and relation_id must be positive".into())
            };
            let id = id.parse().map_err(|_| invalid())?;
            predicates.push(match kind.as_str() {
                "class_relation" => P::ClassRelation(
                    hubuum_domain::ClassRelationId::new(id).map_err(|_| invalid())?,
                ),
                "object_relation" => P::ObjectRelation(
                    hubuum_domain::ObjectRelationId::new(id).map_err(|_| invalid())?,
                ),
                _ => return Err(invalid()),
            });
        }
        _ => {
            return Err(ApiError::BadRequest(
                "relation_type and relation_id are required together".into(),
            ));
        }
    }
    TaskDiscoverySearch::try_new(predicates, Utc::now())
        .map_err(|e| ApiError::BadRequest(e.to_string()))
}

use crate::extractors::Authenticated;
use crate::models::{Permissions, TaskRecord};
use crate::permissions::{AppContext, PermissionDecision, PermissionRequest, PrincipalRef};
use crate::traits::{scope_allows, scope_allows_resource};
use hubuum_storage_core::{TaskExplicitTarget, TaskMetadataDetails};

#[derive(Clone, Copy, PartialEq, Eq)]
enum Reference {
    Target(TaskExplicitTarget),
    Template(hubuum_domain::ExportTemplateId),
    Remote(hubuum_domain::RemoteTargetId),
}
impl Reference {
    fn key(self) -> StorageAuthorizationResourceKey {
        use StorageAuthorizationResourceKey as K;
        match self {
            Self::Target(TaskExplicitTarget::Class { class_id }) => K::Class(class_id),
            Self::Target(TaskExplicitTarget::Object { object_id, .. }) => K::Object(object_id),
            Self::Target(TaskExplicitTarget::Collection { collection_id }) => {
                K::Collection(collection_id)
            }
            Self::Target(TaskExplicitTarget::ClassRelation { relation_id }) => {
                K::ClassRelation(relation_id)
            }
            Self::Target(TaskExplicitTarget::ObjectRelation { relation_id }) => {
                K::ObjectRelation(relation_id)
            }
            Self::Template(id) => K::ExportTemplate(id),
            Self::Remote(id) => K::RemoteTarget(id),
        }
    }
}
fn reference_permission(key: StorageAuthorizationResourceKey) -> Permissions {
    use StorageAuthorizationResourceKey as K;
    match key {
        K::Class(_) => Permissions::ReadClass,
        K::Object(_) => Permissions::ReadObject,
        K::Collection(_) => Permissions::ReadCollection,
        K::ClassRelation(_) => Permissions::ReadClassRelation,
        K::ObjectRelation(_) => Permissions::ReadObjectRelation,
        K::ExportTemplate(_) => Permissions::ReadTemplate,
        K::RemoteTarget(_) => Permissions::ReadRemoteTarget,
    }
}
pub(super) async fn authorize_filters(
    context: &AppContext,
    requestor: &Authenticated,
    search: &hubuum_storage_core::StorageTaskSearch,
) -> Result<(), ApiError> {
    let Some(discovery) = search.discovery() else {
        return Ok(());
    };
    let mut references = Vec::new();
    for predicate in discovery.predicates() {
        let reference = match predicate {
            P::Class(id) => Reference::Target(TaskExplicitTarget::Class { class_id: *id }),
            P::Object(id) => Reference::Target(TaskExplicitTarget::Object {
                object_id: *id,
                class_id: None,
            }),
            P::Collection(id) => {
                Reference::Target(TaskExplicitTarget::Collection { collection_id: *id })
            }
            P::ClassRelation(id) => {
                Reference::Target(TaskExplicitTarget::ClassRelation { relation_id: *id })
            }
            P::ObjectRelation(id) => {
                Reference::Target(TaskExplicitTarget::ObjectRelation { relation_id: *id })
            }
            P::ExportTemplate(id) => Reference::Template(*id),
            P::RemoteTarget(id) => Reference::Remote(*id),
            _ => continue,
        };
        references.push(reference.key());
    }
    let resources = task_authorization_resources(context, references.iter().copied()).await?;
    let mut requests = Vec::with_capacity(references.len());
    for key in references {
        let resource = resources
            .get(&key)
            .cloned()
            .ok_or_else(|| ApiError::NotFound("Task discovery resource was not found".into()))?;
        let permission = reference_permission(key);
        if !scope_allows(requestor.scopes(), &[permission])
            || !scope_allows_resource(requestor.scopes(), &resource)
        {
            return Err(ApiError::Forbidden("Permission denied".into()));
        }
        requests.push(PermissionRequest {
            resource: resource.normalized_for_permission(permission),
            permissions: vec![permission],
        });
    }
    if !requests.is_empty() {
        let expected = requests.len();
        let principal = PrincipalRef::load(context, &requestor.principal).await?;
        let decisions = context
            .permission_backend()
            .authorize_many(&principal, requests)
            .await?;
        if decisions.len() != expected {
            return Err(ApiError::InternalServerError(
                "Permission backend returned an unexpected number of discovery decisions".into(),
            ));
        }
        if decisions
            .iter()
            .any(|decision| *decision != PermissionDecision::Allow)
        {
            return Err(ApiError::Forbidden("Permission denied".into()));
        }
    }
    Ok(())
}

fn references(task: &TaskRecord) -> Vec<Reference> {
    let mut references = Vec::new();
    let Some(metadata) = task.discovery_metadata.as_ref() else {
        return references;
    };
    if let Some(target) = metadata.details().target() {
        references.push(Reference::Target(target));
        if let TaskExplicitTarget::Object {
            class_id: Some(class_id),
            ..
        } = target
        {
            references.push(Reference::Target(TaskExplicitTarget::Class { class_id }));
        }
    }
    match metadata.details() {
        TaskMetadataDetails::Export {
            template_id: Some(id),
            ..
        } => references.push(Reference::Template(*id)),
        TaskMetadataDetails::RemoteCall {
            remote_target_id: Some(id),
            ..
        } => references.push(Reference::Remote(*id)),
        _ => {}
    }
    references
}

/// Resolve and authorize a whole page in batches. Repeated references share a
/// decision; missing resources never disclose associations.
pub(crate) async fn redact(
    context: &AppContext,
    requestor: &Authenticated,
    tasks: &mut [TaskRecord],
) -> Result<(), ApiError> {
    let unique: BTreeSet<_> = tasks
        .iter()
        .flat_map(references)
        .map(Reference::key)
        .collect();
    if unique.is_empty() {
        return Ok(());
    }
    let resources = task_authorization_resources(context, unique.iter().copied()).await?;
    let mut allowed = HashSet::new();
    let mut indexes = Vec::new();
    let mut requests = Vec::new();
    for key in unique {
        let permission = reference_permission(key);
        let Some(resource) = resources.get(&key) else {
            continue;
        };
        if !scope_allows(requestor.scopes(), &[permission])
            || !scope_allows_resource(requestor.scopes(), resource)
        {
            continue;
        }
        indexes.push(key);
        requests.push(PermissionRequest {
            resource: resource.normalized_for_permission(permission),
            permissions: vec![permission],
        });
    }
    if !requests.is_empty() {
        let principal = PrincipalRef::load(context, &requestor.principal).await?;
        let decisions = context
            .permission_backend()
            .authorize_many(&principal, requests)
            .await?;
        if decisions.len() != indexes.len() {
            return Err(ApiError::InternalServerError(
                "Permission backend returned an unexpected number of discovery decisions".into(),
            ));
        }
        for (index, decision) in indexes.into_iter().zip(decisions) {
            if decision == PermissionDecision::Allow {
                allowed.insert(index);
            }
        }
    }
    for task in tasks {
        task.discovery_authorized = references(task)
            .into_iter()
            .all(|reference| allowed.contains(&reference.key()));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::super::parse_task_list_query;
    use rstest::rstest;
    #[rstest]
    #[case("schema_revision=2")]
    #[case("computation_revision=2")]
    #[case("class_id=1&computation_revision=-1")]
    #[case("relation_id=1")]
    #[case("relation_type=class_relation")]
    #[case("relation_type=invalid&relation_id=1")]
    #[case("kind=import&class_id=1")]
    #[case("kind=export,backup&export_truncated=true")]
    #[case("export_truncated=true&import_dry_run=false")]
    #[case("export_has_warnings=0")]
    #[case("export_scope_kind=invalid")]
    #[case("output_state=missing")]
    #[case("schema_work_status=completed")]
    #[case("remote_target_id=-1")]
    #[case("class_id=1&class_id=2")]
    fn rejects_invalid_discovery_filters(#[case] query: &str) {
        assert!(parse_task_list_query(query).is_err(), "{query}");
    }
    #[rstest]
    #[case("class_id=42&kind=schema_validation,reindex&terminal=true")]
    #[case("schema_revision=1&class_id=42&schema_work_kind=impact")]
    #[case("export_has_warnings=false&export_truncated=false")]
    #[case("output_state=unknown")]
    #[case("relation_type=object_relation&relation_id=2")]
    fn accepts_combined_discovery_filters(#[case] query: &str) {
        assert!(parse_task_list_query(query).is_ok(), "{query}");
    }
}
