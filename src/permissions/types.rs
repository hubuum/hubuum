use crate::models::Permissions;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrincipalRef {
    pub user_id: i32,
    pub group_ids: Vec<i32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResourceKind {
    System,
    Namespace,
    Class,
    Object,
    ClassRelation,
    ObjectRelation,
    Template,
    Task,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ResourceAttrs {
    pub namespace_id: Option<i32>,
    pub class_id: Option<i32>,
    pub from_namespace_id: Option<i32>,
    pub to_namespace_id: Option<i32>,
    pub submitted_by: Option<i32>,
    pub name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResourceRef {
    pub kind: ResourceKind,
    pub id: i32,
    pub attrs: ResourceAttrs,
}

impl ResourceRef {
    pub fn namespace(namespace_id: i32) -> Self {
        Self {
            kind: ResourceKind::Namespace,
            id: namespace_id,
            attrs: ResourceAttrs {
                namespace_id: Some(namespace_id),
                ..Default::default()
            },
        }
    }

    pub fn system() -> Self {
        Self {
            kind: ResourceKind::System,
            id: 0,
            attrs: ResourceAttrs::default(),
        }
    }

    pub fn namespace_id(&self) -> Option<i32> {
        self.attrs.namespace_id
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PermissionRequest {
    pub resource: ResourceRef,
    pub permissions: Vec<Permissions>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PermissionDecision {
    Allow,
    Deny,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthorizedRequest {
    pub request: PermissionRequest,
    pub decision: PermissionDecision,
}

/// A target that can be authorized against. Implemented by every model that
/// can be the subject of a permission check (Namespace, HubuumClass,
/// HubuumObject, …).
#[async_trait::async_trait]
pub trait AuthzTarget: Send + Sync {
    async fn to_resource_ref(
        &self,
        pool: &crate::db::DbPool,
    ) -> Result<ResourceRef, crate::errors::ApiError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn namespace_helper_sets_namespace_id_attr() {
        let r = ResourceRef::namespace(42);
        assert_eq!(r.kind, ResourceKind::Namespace);
        assert_eq!(r.id, 42);
        assert_eq!(r.namespace_id(), Some(42));
    }

    #[test]
    fn system_resource_has_no_namespace() {
        let r = ResourceRef::system();
        assert_eq!(r.kind, ResourceKind::System);
        assert_eq!(r.namespace_id(), None);
    }
}
