use async_trait::async_trait;

use crate::errors::ApiError;
use crate::models::Permissions;
use crate::services::storage_boundary::principal_id_to_storage;
use crate::storage::{AuthorizationDataStorage, storage_handle};
use crate::traits::PrincipalIdAccessor;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrincipalRef {
    pub user_id: i32,
    pub group_ids: Vec<i32>,
}

impl PrincipalRef {
    /// Build a principal with a normalized (sorted, deduplicated) group list.
    /// Sorting keeps Treetop request payloads deterministic so equivalent
    /// principals always serialize identically — handy for caching, log
    /// diffing, and snapshot tests.
    pub fn new(user_id: i32, group_ids: impl IntoIterator<Item = i32>) -> Self {
        let mut group_ids: Vec<i32> = group_ids.into_iter().collect();
        group_ids.sort_unstable();
        group_ids.dedup();
        Self { user_id, group_ids }
    }

    pub async fn load<S>(
        pool: &impl crate::storage::StorageContext,
        subject: &S,
    ) -> Result<Self, ApiError>
    where
        S: PrincipalIdAccessor + ?Sized,
    {
        let user_id = subject.principal_id();
        let principal = storage_handle(pool)
            .get_authorization_principal(principal_id_to_storage(user_id))
            .await?;
        Ok(Self::new(
            user_id,
            principal.into_group_ids().into_iter().map(|id| id.id()),
        ))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResourceKind {
    System,
    Collection,
    Class,
    Object,
    ClassRelation,
    ObjectRelation,
    Template,
    Task,
    RemoteTarget,
    Audit,
    EventSubscription,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ResourceFields {
    pub collection_id: Option<i32>,
    pub class_id: Option<i32>,
    pub from_collection_id: Option<i32>,
    pub to_collection_id: Option<i32>,
    pub from_class_id: Option<i32>,
    pub to_class_id: Option<i32>,
    pub from_object_id: Option<i32>,
    pub to_object_id: Option<i32>,
    pub class_relation_id: Option<i32>,
    pub submitted_by: Option<i32>,
    pub name: Option<String>,
}

/// A resolved class endpoint, keeping the class and its collection together.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClassResourceEndpoint {
    collection_id: i32,
    class_id: i32,
}
impl ClassResourceEndpoint {
    pub fn new(collection_id: i32, class_id: i32) -> Self {
        Self {
            collection_id,
            class_id,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectResourceEndpoint {
    class: ClassResourceEndpoint,
    object_id: i32,
}
impl ObjectResourceEndpoint {
    pub fn new(collection_id: i32, class_id: i32, object_id: i32) -> Self {
        Self {
            class: ClassResourceEndpoint::new(collection_id, class_id),
            object_id,
        }
    }
}

/// Identity is explicit: a prospective resource has no database identifier.
#[derive(Debug, Clone, PartialEq, Eq)]
enum ResourceIdentity {
    Existing(i32),
    Prospective,
}

/// Correlated attributes are selected by the resource variant, never a caller's
/// independent kind tag. Optional names and task owners reflect stored data.
#[derive(Debug, Clone, PartialEq, Eq)]
enum ResourceAttrs {
    Collection {
        collection_id: i32,
        name: Option<String>,
    },
    Class {
        collection_id: i32,
        name: Option<String>,
    },
    Object {
        class: ClassResourceEndpoint,
        name: Option<String>,
    },
    ClassRelation {
        from: ClassResourceEndpoint,
        to: ClassResourceEndpoint,
    },
    ObjectRelation {
        from: ObjectResourceEndpoint,
        to: ObjectResourceEndpoint,
        class_relation_id: i32,
    },
    Template {
        collection_id: i32,
        name: Option<String>,
    },
    RemoteTarget {
        collection_id: i32,
        name: Option<String>,
    },
    Task {
        submitted_by: Option<i32>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ResourceState {
    System,
    Entity {
        identity: ResourceIdentity,
        attrs: ResourceAttrs,
    },
    CollectionProbe {
        permission: Permissions,
        collection_id: i32,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResourceRef {
    state: ResourceState,
}

impl ResourceRef {
    fn entity(id: Option<i32>, attrs: ResourceAttrs) -> Self {
        Self {
            state: ResourceState::Entity {
                identity: id.map_or(ResourceIdentity::Prospective, ResourceIdentity::Existing),
                attrs,
            },
        }
    }
    pub fn collection(collection_id: i32) -> Self {
        Self::named_collection(collection_id, None)
    }
    pub fn named_collection(collection_id: i32, name: Option<String>) -> Self {
        Self::entity(
            Some(collection_id),
            ResourceAttrs::Collection {
                collection_id,
                name,
            },
        )
    }
    pub fn class(id: i32, collection_id: i32, name: Option<String>) -> Self {
        Self::entity(
            Some(id),
            ResourceAttrs::Class {
                collection_id,
                name,
            },
        )
    }
    pub fn object(id: i32, class: ClassResourceEndpoint, name: Option<String>) -> Self {
        Self::entity(Some(id), ResourceAttrs::Object { class, name })
    }
    pub fn prospective_object(class: ClassResourceEndpoint) -> Self {
        Self::entity(None, ResourceAttrs::Object { class, name: None })
    }
    pub fn class_relation(
        id: Option<i32>,
        from: ClassResourceEndpoint,
        to: ClassResourceEndpoint,
    ) -> Self {
        Self::entity(id, ResourceAttrs::ClassRelation { from, to })
    }
    pub fn object_relation(
        id: Option<i32>,
        from: ObjectResourceEndpoint,
        to: ObjectResourceEndpoint,
        class_relation_id: i32,
    ) -> Self {
        Self::entity(
            id,
            ResourceAttrs::ObjectRelation {
                from,
                to,
                class_relation_id,
            },
        )
    }
    pub fn template(id: i32, collection_id: i32, name: Option<String>) -> Self {
        Self::entity(
            Some(id),
            ResourceAttrs::Template {
                collection_id,
                name,
            },
        )
    }
    pub fn remote_target(id: i32, collection_id: i32, name: Option<String>) -> Self {
        Self::entity(
            Some(id),
            ResourceAttrs::RemoteTarget {
                collection_id,
                name,
            },
        )
    }
    pub fn task(id: i32, submitted_by: Option<i32>) -> Self {
        Self::entity(Some(id), ResourceAttrs::Task { submitted_by })
    }
    pub fn system() -> Self {
        Self {
            state: ResourceState::System,
        }
    }
    pub fn for_permission_on_collection(permission: Permissions, collection_id: i32) -> Self {
        if permission_kind(permission) == ResourceKind::Collection {
            Self::collection(collection_id)
        } else {
            Self {
                state: ResourceState::CollectionProbe {
                    permission,
                    collection_id,
                },
            }
        }
    }
    pub fn normalized_for_permission(&self, permission: Permissions) -> Self {
        if self.kind() == permission_kind(permission) {
            return self.clone();
        }
        if permission_kind(permission) == ResourceKind::Object
            && let ResourceState::Entity {
                identity: ResourceIdentity::Existing(class_id),
                attrs: ResourceAttrs::Class { collection_id, .. },
            } = &self.state
        {
            return Self::prospective_object(ClassResourceEndpoint::new(*collection_id, *class_id));
        }
        let fields = self.fields();
        match fields
            .collection_id
            .or(fields.from_collection_id)
            .or(fields.to_collection_id)
        {
            Some(collection_id) => Self::for_permission_on_collection(permission, collection_id),
            None => self.clone(),
        }
    }
    pub fn id(&self) -> Option<i32> {
        match self.state {
            ResourceState::Entity {
                identity: ResourceIdentity::Existing(id),
                ..
            } => Some(id),
            _ => None,
        }
    }
    pub fn policy_identity(&self) -> String {
        match self.state {
            ResourceState::System => "global".into(),
            ResourceState::Entity {
                identity: ResourceIdentity::Existing(id),
                ..
            } => id.to_string(),
            ResourceState::Entity {
                identity: ResourceIdentity::Prospective,
                ..
            } => "prospective".into(),
            ResourceState::CollectionProbe { collection_id, .. } => {
                format!("collection-probe:{collection_id}")
            }
        }
    }
    pub fn kind(&self) -> ResourceKind {
        match &self.state {
            ResourceState::System => ResourceKind::System,
            ResourceState::CollectionProbe { permission, .. } => permission_kind(*permission),
            ResourceState::Entity { attrs, .. } => match attrs {
                ResourceAttrs::Collection { .. } => ResourceKind::Collection,
                ResourceAttrs::Class { .. } => ResourceKind::Class,
                ResourceAttrs::Object { .. } => ResourceKind::Object,
                ResourceAttrs::ClassRelation { .. } => ResourceKind::ClassRelation,
                ResourceAttrs::ObjectRelation { .. } => ResourceKind::ObjectRelation,
                ResourceAttrs::Template { .. } => ResourceKind::Template,
                ResourceAttrs::RemoteTarget { .. } => ResourceKind::RemoteTarget,
                ResourceAttrs::Task { .. } => ResourceKind::Task,
            },
        }
    }
    pub fn collection_id(&self) -> Option<i32> {
        self.fields().collection_id
    }
    /// Read-only policy projection. This optional-field record is never accepted
    /// as an authorization resource; it also supports partial mock rule filters.
    pub fn fields(&self) -> ResourceFields {
        let mut fields = ResourceFields::default();
        match &self.state {
            ResourceState::System => {}
            ResourceState::CollectionProbe {
                permission,
                collection_id,
            } => {
                fields.collection_id = Some(*collection_id);
                if matches!(
                    permission_kind(*permission),
                    ResourceKind::ClassRelation | ResourceKind::ObjectRelation
                ) {
                    fields.from_collection_id = Some(*collection_id);
                    fields.to_collection_id = Some(*collection_id);
                }
            }
            ResourceState::Entity { attrs, .. } => match attrs {
                ResourceAttrs::Collection {
                    collection_id,
                    name,
                }
                | ResourceAttrs::Class {
                    collection_id,
                    name,
                }
                | ResourceAttrs::Template {
                    collection_id,
                    name,
                }
                | ResourceAttrs::RemoteTarget {
                    collection_id,
                    name,
                } => {
                    fields.collection_id = Some(*collection_id);
                    fields.name = name.clone();
                }
                ResourceAttrs::Object { class, name } => {
                    fields.collection_id = Some(class.collection_id);
                    fields.class_id = Some(class.class_id);
                    fields.name = name.clone();
                }
                ResourceAttrs::ClassRelation { from, to } => fields.set_class_endpoints(from, to),
                ResourceAttrs::ObjectRelation {
                    from,
                    to,
                    class_relation_id,
                } => {
                    fields.set_class_endpoints(&from.class, &to.class);
                    fields.from_object_id = Some(from.object_id);
                    fields.to_object_id = Some(to.object_id);
                    fields.class_relation_id = Some(*class_relation_id);
                }
                ResourceAttrs::Task { submitted_by } => fields.submitted_by = *submitted_by,
            },
        }
        fields
    }
}

impl ResourceFields {
    fn set_class_endpoints(&mut self, from: &ClassResourceEndpoint, to: &ClassResourceEndpoint) {
        self.collection_id = (from.collection_id == to.collection_id).then_some(from.collection_id);
        self.from_collection_id = Some(from.collection_id);
        self.to_collection_id = Some(to.collection_id);
        self.from_class_id = Some(from.class_id);
        self.to_class_id = Some(to.class_id);
    }
}

fn permission_kind(permission: Permissions) -> ResourceKind {
    match permission {
        Permissions::ReadCollection
        | Permissions::UpdateCollection
        | Permissions::DeleteCollection
        | Permissions::DelegateCollection
        | Permissions::ReadRemoteTarget
        | Permissions::CreateRemoteTarget
        | Permissions::UpdateRemoteTarget
        | Permissions::DeleteRemoteTarget
        | Permissions::ExecuteRemoteTarget
        | Permissions::ReadAudit
        | Permissions::ManageEventSubscription => ResourceKind::Collection,
        Permissions::CreateClass
        | Permissions::ReadClass
        | Permissions::UpdateClass
        | Permissions::DeleteClass => ResourceKind::Class,
        Permissions::CreateObject
        | Permissions::ReadObject
        | Permissions::UpdateObject
        | Permissions::DeleteObject => ResourceKind::Object,
        Permissions::CreateClassRelation
        | Permissions::ReadClassRelation
        | Permissions::UpdateClassRelation
        | Permissions::DeleteClassRelation => ResourceKind::ClassRelation,
        Permissions::CreateObjectRelation
        | Permissions::ReadObjectRelation
        | Permissions::UpdateObjectRelation
        | Permissions::DeleteObjectRelation => ResourceKind::ObjectRelation,
        Permissions::ReadTemplate
        | Permissions::CreateTemplate
        | Permissions::UpdateTemplate
        | Permissions::DeleteTemplate => ResourceKind::Template,
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

/// One request paired with its decision. Returned by
/// `PermissionBackend::authorize_candidates` so call sites that need both
/// the original request and the decision (e.g. list visibility filters,
/// where the request carries the resource being filtered) get them
/// together without re-zipping.
///
/// Note: this carries decisions for *every* request, including denials.
/// Call sites filter on `decision == PermissionDecision::Allow` themselves.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthorizationResult {
    pub request: PermissionRequest,
    pub decision: PermissionDecision,
}

/// A target that can be authorized against. Implemented by every model that
/// can be the subject of a permission check (Collection, HubuumClass,
/// HubuumObject, …).
#[async_trait]
pub trait AuthzTarget: Send + Sync {
    async fn to_resource_ref(
        &self,
        backend: &impl crate::storage::StorageContext,
    ) -> Result<ResourceRef, ApiError>;
}

#[async_trait]
impl<T> AuthzTarget for &T
where
    T: AuthzTarget + ?Sized + Sync,
{
    async fn to_resource_ref(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<ResourceRef, ApiError> {
        (*self).to_resource_ref(pool).await
    }
}

#[cfg(test)]
mod tests {
    use std::iter::empty;

    use super::*;

    #[test]
    fn collection_helper_sets_collection_id_attr() {
        let r = ResourceRef::collection(42);
        assert_eq!(r.kind(), ResourceKind::Collection);
        assert_eq!(r.id(), Some(42));
        assert_eq!(r.collection_id(), Some(42));
    }

    #[test]
    fn system_resource_has_no_collection() {
        let r = ResourceRef::system();
        assert_eq!(r.kind(), ResourceKind::System);
        assert_eq!(r.collection_id(), None);
    }

    #[test]
    fn principal_new_sorts_and_deduplicates_group_ids() {
        let p = PrincipalRef::new(7, vec![3, 1, 3, 2, 1]);
        assert_eq!(p.user_id, 7);
        assert_eq!(p.group_ids, vec![1, 2, 3]);
    }

    #[test]
    fn principal_new_handles_empty_groups() {
        let p = PrincipalRef::new(42, empty());
        assert_eq!(p.group_ids, Vec::<i32>::new());
    }

    #[test]
    fn collection_target_is_normalized_to_schema_compatible_class() {
        let resource =
            ResourceRef::collection(42).normalized_for_permission(Permissions::CreateClass);

        assert_eq!(resource.kind(), ResourceKind::Class);
        assert_eq!(resource.id(), None);
        assert_eq!(resource.fields().collection_id, Some(42));
    }

    #[test]
    fn collection_target_is_normalized_to_schema_compatible_template() {
        let resource =
            ResourceRef::collection(42).normalized_for_permission(Permissions::CreateTemplate);

        assert_eq!(resource.kind(), ResourceKind::Template);
        assert_eq!(resource.fields().collection_id, Some(42));
    }

    #[test]
    fn reverse_relation_check_uses_relation_resource_shape() {
        let resource =
            ResourceRef::for_permission_on_collection(Permissions::ReadObjectRelation, 42);

        assert_eq!(resource.kind(), ResourceKind::ObjectRelation);
        assert_eq!(resource.fields().from_collection_id, Some(42));
        assert_eq!(resource.fields().to_collection_id, Some(42));
        assert_eq!(resource.fields().from_object_id, None);
        assert_eq!(resource.fields().to_object_id, None);
    }
}
