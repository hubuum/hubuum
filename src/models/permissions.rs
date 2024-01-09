use crate::models::group::GroupID;
use crate::models::user::UserID;

use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::hash::Hash;

#[derive(Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum NamespacePermissions {
    Create,
    Read,
    Update,
    Delete,
    Delegate,
}

#[derive(Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DataPermissions {
    Create,
    Read,
    Update,
    Delete,
}

#[derive(Serialize, Deserialize)]
pub enum Assignee {
    Group(GroupID),
    User(UserID),
}

#[derive(Serialize, Deserialize)]
pub struct NamespacePermissionAssignment {
    pub assignee: Assignee,
    pub permissions: HashSet<NamespacePermissions>,
}

impl NamespacePermissions {
    pub fn db_field(&self) -> &'static str {
        match self {
            NamespacePermissions::Create => "has_create",
            NamespacePermissions::Read => "has_read",
            NamespacePermissions::Update => "has_update",
            NamespacePermissions::Delete => "has_delete",
            NamespacePermissions::Delegate => "has_delegate",
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct DataPermission {
    pub assignee: Assignee,
    pub permissions: HashSet<DataPermissions>,
}

impl DataPermissions {
    pub fn db_field(&self) -> &'static str {
        match self {
            DataPermissions::Create => "has_create",
            DataPermissions::Read => "has_read",
            DataPermissions::Update => "has_update",
            DataPermissions::Delete => "has_delete",
        }
    }
}
