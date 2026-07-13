use async_trait::async_trait;

use crate::db::{DbPool, with_connection};
use crate::errors::ApiError;
use crate::models::Permissions;
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

    pub async fn load<S>(pool: &DbPool, subject: &S) -> Result<Self, ApiError>
    where
        S: PrincipalIdAccessor + ?Sized,
    {
        use crate::db::prelude::*;
        use crate::schema::group_memberships::dsl::{group_id, group_memberships, principal_id};

        let user_id = subject.principal_id();
        let group_ids = with_connection(pool, async |conn| {
            group_memberships
                .filter(principal_id.eq(user_id))
                .select(group_id)
                .load::<i32>(conn)
                .await
        })
        .await?;
        Ok(Self::new(user_id, group_ids))
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
pub struct ResourceAttrs {
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResourceRef {
    pub kind: ResourceKind,
    pub id: i32,
    pub attrs: ResourceAttrs,
}

impl ResourceRef {
    pub fn collection(collection_id: i32) -> Self {
        Self {
            kind: ResourceKind::Collection,
            id: collection_id,
            attrs: ResourceAttrs {
                collection_id: Some(collection_id),
                ..Default::default()
            },
        }
    }

    /// Build the resource shape used to ask whether a permission applies
    /// anywhere within a collection. Child kinds use a prospective entity
    /// with complete collection-scoping attributes; exported Cedar policies
    /// match those attributes rather than the placeholder id.
    pub fn for_permission_on_collection(permission: Permissions, collection_id: i32) -> Self {
        let mut attrs = ResourceAttrs {
            collection_id: Some(collection_id),
            ..Default::default()
        };
        let kind = match permission {
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
            | Permissions::DeleteObject => {
                attrs.class_id = Some(0);
                ResourceKind::Object
            }
            Permissions::CreateClassRelation
            | Permissions::ReadClassRelation
            | Permissions::UpdateClassRelation
            | Permissions::DeleteClassRelation => {
                attrs.from_collection_id = Some(collection_id);
                attrs.to_collection_id = Some(collection_id);
                attrs.from_class_id = Some(0);
                attrs.to_class_id = Some(0);
                ResourceKind::ClassRelation
            }
            Permissions::CreateObjectRelation
            | Permissions::ReadObjectRelation
            | Permissions::UpdateObjectRelation
            | Permissions::DeleteObjectRelation => {
                attrs.from_collection_id = Some(collection_id);
                attrs.to_collection_id = Some(collection_id);
                attrs.from_class_id = Some(0);
                attrs.to_class_id = Some(0);
                attrs.from_object_id = Some(0);
                attrs.to_object_id = Some(0);
                attrs.class_relation_id = Some(0);
                ResourceKind::ObjectRelation
            }
            Permissions::ReadTemplate
            | Permissions::CreateTemplate
            | Permissions::UpdateTemplate
            | Permissions::DeleteTemplate => ResourceKind::Template,
        };
        let id = if kind == ResourceKind::Collection {
            collection_id
        } else {
            0
        };
        Self { kind, id, attrs }
    }

    pub fn normalized_for_permission(&self, permission: Permissions) -> Self {
        let expected_kind = match permission {
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
        };
        if self.kind == expected_kind {
            return self.clone();
        }

        let collection_id = self
            .collection_id()
            .or(self.attrs.from_collection_id)
            .or(self.attrs.to_collection_id)
            .unwrap_or(self.id);
        let mut prospective = Self::for_permission_on_collection(permission, collection_id);
        if expected_kind == ResourceKind::Object && self.kind == ResourceKind::Class {
            prospective.attrs.class_id = Some(self.id);
        }
        prospective
    }

    /// Construct the global System resource. Currently only Treetop's
    /// `is_admin` dispatches against it; the SQL backend reads the admin
    /// group directly. Marked `dead_code`-allow so a build without the
    /// optional Treetop backend doesn't lint the helper away.
    #[allow(dead_code)]
    pub fn system() -> Self {
        Self {
            kind: ResourceKind::System,
            id: 0,
            attrs: ResourceAttrs::default(),
        }
    }

    pub fn collection_id(&self) -> Option<i32> {
        self.attrs.collection_id
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
    async fn to_resource_ref(&self, pool: &DbPool) -> Result<ResourceRef, ApiError>;
}

#[async_trait]
impl<T> AuthzTarget for &T
where
    T: AuthzTarget + ?Sized + Sync,
{
    async fn to_resource_ref(&self, pool: &DbPool) -> Result<ResourceRef, ApiError> {
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
        assert_eq!(r.kind, ResourceKind::Collection);
        assert_eq!(r.id, 42);
        assert_eq!(r.collection_id(), Some(42));
    }

    #[test]
    fn system_resource_has_no_collection() {
        let r = ResourceRef::system();
        assert_eq!(r.kind, ResourceKind::System);
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

        assert_eq!(resource.kind, ResourceKind::Class);
        assert_eq!(resource.id, 0);
        assert_eq!(resource.attrs.collection_id, Some(42));
    }

    #[test]
    fn collection_target_is_normalized_to_schema_compatible_template() {
        let resource =
            ResourceRef::collection(42).normalized_for_permission(Permissions::CreateTemplate);

        assert_eq!(resource.kind, ResourceKind::Template);
        assert_eq!(resource.attrs.collection_id, Some(42));
    }

    #[test]
    fn reverse_relation_check_uses_relation_resource_shape() {
        let resource =
            ResourceRef::for_permission_on_collection(Permissions::ReadObjectRelation, 42);

        assert_eq!(resource.kind, ResourceKind::ObjectRelation);
        assert_eq!(resource.attrs.from_collection_id, Some(42));
        assert_eq!(resource.attrs.to_collection_id, Some(42));
        assert_eq!(resource.attrs.from_object_id, Some(0));
        assert_eq!(resource.attrs.to_object_id, Some(0));
    }
}
