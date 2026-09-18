use crate::errors::ApiError;
use chrono::Utc;
use hubuum_storage_core::{
    TaskDiscoveryPredicate as P, TaskDiscoverySearch, TaskRemoteSideEffectState,
};
use std::collections::HashMap;

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
use crate::permissions::{AppContext, AuthzTarget, ResourceRef, authorize_resources};
use hubuum_storage_core::{TaskExplicitTarget, TaskMetadataDetails};

#[derive(Clone, Copy, PartialEq, Eq)]
enum Reference {
    Target(TaskExplicitTarget),
    Template(hubuum_domain::ExportTemplateId),
    Remote(hubuum_domain::RemoteTargetId),
}
async fn authorize(
    context: &AppContext,
    requestor: &Authenticated,
    reference: Reference,
) -> Result<(), ApiError> {
    let (resource, permission) = resolve(context, reference).await?;
    authorize_resources(
        context.permission_backend(),
        context,
        &requestor.principal,
        requestor.scopes(),
        vec![permission],
        vec![resource],
    )
    .await
}
async fn resolve(
    context: &AppContext,
    reference: Reference,
) -> Result<(ResourceRef, Permissions), ApiError> {
    Ok(match reference {
        Reference::Target(TaskExplicitTarget::Class { class_id }) => (
            class_id.to_resource_ref(context).await?,
            Permissions::ReadClass,
        ),
        Reference::Target(TaskExplicitTarget::Object { object_id, .. }) => (
            object_id.to_resource_ref(context).await?,
            Permissions::ReadObject,
        ),
        Reference::Target(TaskExplicitTarget::Collection { collection_id }) => (
            collection_id.to_resource_ref(context).await?,
            Permissions::ReadCollection,
        ),
        Reference::Target(TaskExplicitTarget::ClassRelation { relation_id }) => (
            relation_id.to_resource_ref(context).await?,
            Permissions::ReadClassRelation,
        ),
        Reference::Target(TaskExplicitTarget::ObjectRelation { relation_id }) => (
            relation_id.to_resource_ref(context).await?,
            Permissions::ReadObjectRelation,
        ),
        Reference::Template(id) => (
            id.to_resource_ref(context).await?,
            Permissions::ReadTemplate,
        ),
        Reference::Remote(id) => {
            let target =
                crate::services::remote_targets::get_remote_target(context, id.id()).await?;
            (
                ResourceRef::remote_target(target.id, target.collection_id, Some(target.name)),
                Permissions::ReadRemoteTarget,
            )
        }
    })
}

pub(super) async fn authorize_filters(
    context: &AppContext,
    requestor: &Authenticated,
    search: &hubuum_storage_core::StorageTaskSearch,
) -> Result<(), ApiError> {
    let Some(discovery) = search.discovery() else {
        return Ok(());
    };
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
        authorize(context, requestor, reference).await?;
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

/// Resolve class/object facts and authorize a whole page in batches. Repeated
/// references share a decision; missing resources never disclose associations.
pub(crate) async fn redact(
    context: &AppContext,
    requestor: &Authenticated,
    tasks: &mut [TaskRecord],
) -> Result<(), ApiError> {
    use crate::permissions::{PermissionDecision, PermissionRequest, PrincipalRef};
    use crate::services::authorization_resources::{
        schema_compliance_authorization_resources, task_class_authorization_resources,
    };
    use crate::traits::{scope_allows, scope_allows_resource};
    let mut unique = Vec::new();
    for reference in tasks.iter().flat_map(references) {
        if !unique.contains(&reference) {
            unique.push(reference);
        }
    }
    if unique.is_empty() {
        return Ok(());
    }
    let class_ids = unique
        .iter()
        .filter_map(|reference| match reference {
            Reference::Target(TaskExplicitTarget::Class { class_id }) => Some(class_id.id()),
            _ => None,
        })
        .collect::<Vec<_>>();
    let object_ids = unique
        .iter()
        .filter_map(|reference| match reference {
            Reference::Target(TaskExplicitTarget::Object { object_id, .. }) => Some(object_id.id()),
            _ => None,
        })
        .collect::<Vec<_>>();
    let classes = if class_ids.is_empty() {
        HashMap::new()
    } else {
        task_class_authorization_resources(context, requestor.principal.id().id(), &class_ids)
            .await?
    };
    let objects = if object_ids.is_empty() {
        HashMap::new()
    } else {
        schema_compliance_authorization_resources(context, object_ids).await?
    };
    let mut allowed = vec![false; unique.len()];
    let mut indexes = Vec::new();
    let mut requests = Vec::new();
    for (index, reference) in unique.iter().enumerate() {
        let resolved = match reference {
            Reference::Target(TaskExplicitTarget::Class { class_id }) => classes
                .get(&class_id.id())
                .cloned()
                .map(|r| (r, Permissions::ReadClass)),
            Reference::Target(TaskExplicitTarget::Object { object_id, .. }) => objects
                .get(&object_id.id())
                .cloned()
                .map(|r| (r, Permissions::ReadObject)),
            _ => match resolve(context, *reference).await {
                Ok(value) => Some(value),
                Err(ApiError::NotFound(_) | ApiError::Forbidden(_)) => None,
                Err(error) => return Err(error),
            },
        };
        let Some((resource, permission)) = resolved else {
            continue;
        };
        if !scope_allows(requestor.scopes(), &[permission])
            || !scope_allows_resource(requestor.scopes(), &resource)
        {
            continue;
        }
        indexes.push(index);
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
            allowed[index] = decision == PermissionDecision::Allow;
        }
    }
    for task in tasks {
        task.discovery_authorized = references(task).iter().all(|reference| {
            unique
                .iter()
                .position(|v| v == reference)
                .is_some_and(|index| allowed[index])
        });
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
