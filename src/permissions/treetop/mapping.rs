use crate::models::Permissions;
use crate::permissions::types::{PrincipalRef, ResourceKind, ResourceRef};
use treetop_client::{Action, AttrValue, Resource, User, ValidationError};

/// Build a Cedar User principal from our PrincipalRef.
///
/// Maps numeric user_id and group_ids to string IDs per the spec amendment
/// (Phase 5, §4 — group identity is numeric, not by name).
pub fn cedar_user(principal: &PrincipalRef) -> Result<User, ValidationError> {
    let user = User::new(principal.user_id.to_string())?;
    let group_strs: Vec<String> = principal.group_ids.iter().map(|g| g.to_string()).collect();
    let group_refs: Vec<&str> = group_strs.iter().map(String::as_str).collect();
    user.with_group_names(&group_refs)
}

/// Build a Cedar Action from a Permissions enum.
///
/// Per the spec amendment, we use the existing Permissions display names as
/// the canonical v1 action IDs (ReadCollection, CreateClass, ReadObject,
/// DeleteTemplate, etc.). This avoids ambiguity from generic read/update/delete
/// actions across different resource kinds.
pub fn cedar_action(perm: Permissions) -> Result<Action, ValidationError> {
    Action::new(perm.to_string())
}

/// Build a Cedar Resource from our ResourceRef.
///
/// Maps Hubuum resource kinds to Cedar entity types:
/// - System → HubuumSystem (global singleton)
/// - Collection → HubuumCollection (with collection_id attr)
/// - Class → HubuumClass (with collection_id attr)
/// - Object → HubuumObject (with collection_id, class_id attrs)
/// - ClassRelation → HubuumClassRelation (with from/to collection/class attrs)
/// - ObjectRelation → HubuumObjectRelation (with from/to collection/class/object attrs + class_relation_id)
/// - Template → HubuumTemplate (with collection_id attr)
/// - Task → HubuumTask (with submitted_by attr)
///
/// Attributes are added only when present in ResourceFields (no null/zero placeholders).
pub fn cedar_resource(resource: &ResourceRef) -> Result<Resource, ValidationError> {
    let id_str = resource.policy_identity();
    let mut r = match resource.kind() {
        ResourceKind::System => Resource::new("HubuumSystem", "global"),
        ResourceKind::Collection => Resource::new("HubuumCollection", &id_str),
        ResourceKind::Class => Resource::new("HubuumClass", &id_str),
        ResourceKind::Object => Resource::new("HubuumObject", &id_str),
        ResourceKind::ClassRelation => Resource::new("HubuumClassRelation", &id_str),
        ResourceKind::ObjectRelation => Resource::new("HubuumObjectRelation", &id_str),
        ResourceKind::Template => Resource::new("HubuumTemplate", &id_str),
        ResourceKind::Task => Resource::new("HubuumTask", &id_str),
        ResourceKind::RemoteTarget => Resource::new("HubuumRemoteTarget", &id_str),
        ResourceKind::Audit => Resource::new("HubuumAudit", &id_str),
        ResourceKind::EventSubscription => Resource::new("HubuumEventSubscription", &id_str),
    }?;

    let attrs = &resource.fields();
    if let Some(ns) = attrs.collection_id {
        r = r.with_attr("collection_id", AttrValue::Long(ns as i64))?;
    }
    if let Some(class_id) = attrs.class_id {
        r = r.with_attr("class_id", AttrValue::Long(class_id as i64))?;
    }
    if let Some(from_ns) = attrs.from_collection_id {
        r = r.with_attr("from_collection_id", AttrValue::Long(from_ns as i64))?;
    }
    if let Some(to_ns) = attrs.to_collection_id {
        r = r.with_attr("to_collection_id", AttrValue::Long(to_ns as i64))?;
    }
    if let Some(from_class) = attrs.from_class_id {
        r = r.with_attr("from_class_id", AttrValue::Long(from_class as i64))?;
    }
    if let Some(to_class) = attrs.to_class_id {
        r = r.with_attr("to_class_id", AttrValue::Long(to_class as i64))?;
    }
    if let Some(from_obj) = attrs.from_object_id {
        r = r.with_attr("from_object_id", AttrValue::Long(from_obj as i64))?;
    }
    if let Some(to_obj) = attrs.to_object_id {
        r = r.with_attr("to_object_id", AttrValue::Long(to_obj as i64))?;
    }
    if let Some(class_rel) = attrs.class_relation_id {
        r = r.with_attr("class_relation_id", AttrValue::Long(class_rel as i64))?;
    }
    if let Some(submitted_by) = attrs.submitted_by {
        r = r.with_attr("submitted_by", AttrValue::Long(submitted_by as i64))?;
    }
    if let Some(ref name) = attrs.name {
        r = r.with_attr("name", AttrValue::String(name.clone()))?;
    }
    Ok(r)
}

#[cfg(test)]
mod tests {
    use serde_json::to_value;

    use super::*;
    use crate::permissions::{ClassResourceEndpoint, ObjectResourceEndpoint};

    #[test]
    fn collection_resource_carries_collection_id_attr() {
        let r = ResourceRef::collection(7);
        let cedar = cedar_resource(&r).unwrap();
        let json = to_value(&cedar).unwrap();
        assert_eq!(json["kind"], "HubuumCollection");
        assert_eq!(json["id"], "7");
        // The treetop Resource serializes attrs as a map with typed values
        assert!(json["attrs"].is_object());
        assert_eq!(json["attrs"]["collection_id"]["type"], "Long");
        assert_eq!(json["attrs"]["collection_id"]["value"], 7);
    }

    #[test]
    fn system_resource_is_global_singleton() {
        let r = ResourceRef::system();
        let cedar = cedar_resource(&r).unwrap();
        let json = to_value(&cedar).unwrap();
        assert_eq!(json["kind"], "HubuumSystem");
        assert_eq!(json["id"], "global");
        // System has no attrs
        assert!(json.get("attrs").is_none() || json["attrs"].as_object().unwrap().is_empty());
    }

    #[test]
    fn class_relation_resource_carries_from_to_collection_attrs() {
        let r = ResourceRef::class_relation(
            Some(42),
            ClassResourceEndpoint::new(5, 10),
            ClassResourceEndpoint::new(6, 11),
        );
        let cedar = cedar_resource(&r).unwrap();
        let json = to_value(&cedar).unwrap();
        assert_eq!(json["kind"], "HubuumClassRelation");
        assert_eq!(json["attrs"]["from_collection_id"]["value"], 5);
        assert_eq!(json["attrs"]["to_collection_id"]["value"], 6);
        assert_eq!(json["attrs"]["from_class_id"]["value"], 10);
        assert_eq!(json["attrs"]["to_class_id"]["value"], 11);
    }

    #[test]
    fn object_relation_carries_all_context_attrs() {
        let r = ResourceRef::object_relation(
            Some(99),
            ObjectResourceEndpoint::new(1, 10, 100),
            ObjectResourceEndpoint::new(2, 20, 200),
            5,
        );
        let cedar = cedar_resource(&r).unwrap();
        let json = to_value(&cedar).unwrap();
        assert_eq!(json["kind"], "HubuumObjectRelation");
        assert_eq!(json["attrs"]["from_collection_id"]["value"], 1);
        assert_eq!(json["attrs"]["to_collection_id"]["value"], 2);
        assert_eq!(json["attrs"]["from_class_id"]["value"], 10);
        assert_eq!(json["attrs"]["to_class_id"]["value"], 20);
        assert_eq!(json["attrs"]["from_object_id"]["value"], 100);
        assert_eq!(json["attrs"]["to_object_id"]["value"], 200);
        assert_eq!(json["attrs"]["class_relation_id"]["value"], 5);
    }

    #[test]
    fn permissions_action_uses_pascal_case_display_name() {
        let action = cedar_action(Permissions::ReadObject).unwrap();
        let json = to_value(&action).unwrap();
        assert_eq!(json["id"], "ReadObject");
    }

    #[test]
    fn all_permission_variants_map_to_expected_action_names() {
        let fixtures = [
            (Permissions::ReadCollection, "ReadCollection"),
            (Permissions::UpdateCollection, "UpdateCollection"),
            (Permissions::DeleteCollection, "DeleteCollection"),
            (Permissions::DelegateCollection, "DelegateCollection"),
            (Permissions::CreateClass, "CreateClass"),
            (Permissions::ReadClass, "ReadClass"),
            (Permissions::UpdateClass, "UpdateClass"),
            (Permissions::DeleteClass, "DeleteClass"),
            (Permissions::CreateObject, "CreateObject"),
            (Permissions::ReadObject, "ReadObject"),
            (Permissions::UpdateObject, "UpdateObject"),
            (Permissions::DeleteObject, "DeleteObject"),
            (Permissions::CreateClassRelation, "CreateClassRelation"),
            (Permissions::ReadClassRelation, "ReadClassRelation"),
            (Permissions::UpdateClassRelation, "UpdateClassRelation"),
            (Permissions::DeleteClassRelation, "DeleteClassRelation"),
            (Permissions::CreateObjectRelation, "CreateObjectRelation"),
            (Permissions::ReadObjectRelation, "ReadObjectRelation"),
            (Permissions::UpdateObjectRelation, "UpdateObjectRelation"),
            (Permissions::DeleteObjectRelation, "DeleteObjectRelation"),
            (Permissions::ReadTemplate, "ReadTemplate"),
            (Permissions::CreateTemplate, "CreateTemplate"),
            (Permissions::UpdateTemplate, "UpdateTemplate"),
            (Permissions::DeleteTemplate, "DeleteTemplate"),
        ];

        for (perm, expected_name) in fixtures {
            let action = cedar_action(perm).unwrap();
            let json = to_value(&action).unwrap();
            assert_eq!(
                json["id"], expected_name,
                "Permission {perm:?} should map to action {expected_name}"
            );
        }
    }

    #[test]
    fn cedar_user_uses_numeric_user_and_group_ids() {
        let p = PrincipalRef::new(42, vec![100, 200]);
        let user = cedar_user(&p).unwrap();
        let json = to_value(&user).unwrap();
        assert_eq!(json["id"], "42");
        assert_eq!(json["groups"][0]["id"], "100");
        assert_eq!(json["groups"][1]["id"], "200");
    }

    #[test]
    fn cedar_user_with_no_groups() {
        let p = PrincipalRef::new(7, vec![]);
        let user = cedar_user(&p).unwrap();
        let json = to_value(&user).unwrap();
        assert_eq!(json["id"], "7");
        assert!(json["groups"].is_array());
        assert_eq!(json["groups"].as_array().unwrap().len(), 0);
    }

    #[test]
    fn task_resource_carries_submitted_by_attr() {
        let r = ResourceRef::task(123, Some(999));
        let cedar = cedar_resource(&r).unwrap();
        let json = to_value(&cedar).unwrap();
        assert_eq!(json["kind"], "HubuumTask");
        assert_eq!(json["attrs"]["submitted_by"]["value"], 999);
    }

    #[test]
    fn resource_with_name_attr() {
        let r = ResourceRef::class(5, 1, Some("HostClass".to_string()));
        let cedar = cedar_resource(&r).unwrap();
        let json = to_value(&cedar).unwrap();
        assert_eq!(json["attrs"]["name"]["type"], "String");
        assert_eq!(json["attrs"]["name"]["value"], "HostClass");
    }
}
