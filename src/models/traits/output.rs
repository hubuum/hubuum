use crate::models::group::Group;
use crate::models::{GroupPermission, Permission, Permissions, PermissionsList};

pub trait FromTuple<T> {
    fn from_tuple(t: (Group, T)) -> Self;
}

impl FromTuple<Permission> for GroupPermission {
    fn from_tuple(t: (Group, Permission)) -> Self {
        GroupPermission {
            group: t.0,
            permission: t.1,
        }
    }
}
