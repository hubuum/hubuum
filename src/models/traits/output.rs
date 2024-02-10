use crate::models::group::Group;
use crate::models::output::{
    GroupClassPermission, GroupNamespacePermission, GroupObjectPermission,
};
use crate::models::permissions::{ClassPermission, NamespacePermission, ObjectPermission};

pub trait FromTuple<T> {
    fn from_tuple(t: (Group, T)) -> Self;
}

impl FromTuple<NamespacePermission> for GroupNamespacePermission {
    fn from_tuple(t: (Group, NamespacePermission)) -> Self {
        GroupNamespacePermission {
            group: t.0,
            namespace_permission: t.1,
        }
    }
}

impl FromTuple<ClassPermission> for GroupClassPermission {
    fn from_tuple(t: (Group, ClassPermission)) -> Self {
        GroupClassPermission {
            group: t.0,
            class_permission: t.1,
        }
    }
}

impl FromTuple<ObjectPermission> for GroupObjectPermission {
    fn from_tuple(t: (Group, ObjectPermission)) -> Self {
        GroupObjectPermission {
            group: t.0,
            object_permission: t.1,
        }
    }
}
