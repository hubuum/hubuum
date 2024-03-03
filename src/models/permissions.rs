use diesel::prelude::*;
use std::{fmt, fmt::Display, slice};

use serde::{Deserialize, Serialize};

use crate::{errors::ApiError, schema::permissions};

#[derive(Debug, PartialEq, Eq, Hash, Serialize, Deserialize, Clone, Copy)]
pub enum Permissions {
    ReadCollection,
    UpdateCollection,
    DeleteCollection,
    DelegateCollection,
    CreateClass,
    ReadClass,
    UpdateClass,
    DeleteClass,
    CreateObject,
    ReadObject,
    UpdateObject,
    DeleteObject,
}

impl Permissions {
    pub fn from_string(s: &str) -> Result<Permissions, ApiError> {
        match s {
            "ReadCollection" => Ok(Permissions::ReadCollection),
            "UpdateColletion" => Ok(Permissions::UpdateCollection),
            "DeleteCollection" => Ok(Permissions::DeleteCollection),
            "DelegateCollection" => Ok(Permissions::DelegateCollection),
            "CreateClass" => Ok(Permissions::CreateClass),
            "ReadClass" => Ok(Permissions::ReadClass),
            "UpdateClass" => Ok(Permissions::UpdateClass),
            "DeleteClass" => Ok(Permissions::DeleteClass),
            "CreateObject" => Ok(Permissions::CreateObject),
            "ReadObject" => Ok(Permissions::ReadObject),
            "UpdateObject" => Ok(Permissions::UpdateObject),
            "DeleteObject" => Ok(Permissions::DeleteObject),
            _ => Err(ApiError::BadRequest(format!("Invalid permission: '{}'", s))),
        }
    }
}
impl Display for Permissions {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Permissions::ReadCollection => "ReadCollection",
                Permissions::UpdateCollection => "UpdateCollection",
                Permissions::DeleteCollection => "DeleteCollection",
                Permissions::DelegateCollection => "DelegateCollection",
                Permissions::CreateClass => "CreateClass",
                Permissions::ReadClass => "ReadClass",
                Permissions::UpdateClass => "UpdateClass",
                Permissions::DeleteClass => "DeleteClass",
                Permissions::CreateObject => "CreateObject",
                Permissions::ReadObject => "ReadObject",
                Permissions::UpdateObject => "UpdateObject",
                Permissions::DeleteObject => "DeleteObject",
            }
        )
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PermissionsList<T: Serialize + PartialEq>(Vec<T>);

impl<T: Serialize + PartialEq> PermissionsList<T> {
    // Constructor that accepts a generic IntoIterator so we can create a PermissionsList from any
    // collection of items that can be converted into an iterator.
    pub fn new<I: IntoIterator<Item = T>>(items: I) -> Self {
        PermissionsList(items.into_iter().collect())
    }

    pub fn contains(&self, item: &T) -> bool {
        self.0.contains(item)
    }

    // Method to get an iterator over references to the items in the Vec<T>
    pub fn iter(&self) -> slice::Iter<'_, T> {
        self.0.iter()
    }
}

impl<T: Serialize + PartialEq + Display> Display for PermissionsList<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let formatted = self
            .0
            .iter()
            .map(|item| item.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        write!(f, "{}", formatted)
    }
}

impl<'a, T: Serialize + PartialEq> IntoIterator for &'a PermissionsList<T> {
    type Item = &'a T;
    type IntoIter = slice::Iter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

pub trait PermissionFilter<'a, Q> {
    fn filter(self, query: Q) -> Q;
}

impl<'a> PermissionFilter<'a, permissions::BoxedQuery<'a, diesel::pg::Pg>> for Permissions {
    fn filter(
        self,
        query: permissions::BoxedQuery<diesel::pg::Pg>,
    ) -> permissions::BoxedQuery<diesel::pg::Pg> {
        match self {
            Permissions::ReadCollection => query.filter(permissions::has_read_namespace.eq(true)),
            Permissions::UpdateCollection => {
                query.filter(permissions::has_update_namespace.eq(true))
            }
            Permissions::DeleteCollection => {
                query.filter(permissions::has_delete_namespace.eq(true))
            }
            Permissions::DelegateCollection => {
                query.filter(permissions::has_delegate_namespace.eq(true))
            }
            Permissions::CreateClass => query.filter(permissions::has_create_class.eq(true)),
            Permissions::ReadClass => query.filter(permissions::has_read_class.eq(true)),
            Permissions::UpdateClass => query.filter(permissions::has_update_class.eq(true)),
            Permissions::DeleteClass => query.filter(permissions::has_delete_class.eq(true)),
            Permissions::CreateObject => query.filter(permissions::has_create_object.eq(true)),
            Permissions::ReadObject => query.filter(permissions::has_read_object.eq(true)),
            Permissions::UpdateObject => query.filter(permissions::has_update_object.eq(true)),
            Permissions::DeleteObject => query.filter(permissions::has_delete_object.eq(true)),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Queryable, Clone, Copy)]
#[diesel(table_name = permissions)]
pub struct Permission {
    pub id: i32,
    pub namespace_id: i32,
    pub group_id: i32,
    pub has_read_namespace: bool,
    pub has_update_namespace: bool,
    pub has_delete_namespace: bool,
    pub has_delegate_namespace: bool,
    pub has_create_class: bool,
    pub has_read_class: bool,
    pub has_update_class: bool,
    pub has_delete_class: bool,
    pub has_create_object: bool,
    pub has_read_object: bool,
    pub has_update_object: bool,
    pub has_delete_object: bool,
    pub created_at: chrono::NaiveDateTime,
    pub updated_at: chrono::NaiveDateTime,
}

// Insertable permission models.
#[derive(Debug, Serialize, Deserialize, Insertable)]
#[diesel(table_name = permissions)]
pub struct NewPermission {
    pub namespace_id: i32,
    pub group_id: i32,
    pub has_read_namespace: bool,
    pub has_update_namespace: bool,
    pub has_delete_namespace: bool,
    pub has_delegate_namespace: bool,
    pub has_create_class: bool,
    pub has_read_class: bool,
    pub has_update_class: bool,
    pub has_delete_class: bool,
    pub has_create_object: bool,
    pub has_read_object: bool,
    pub has_update_object: bool,
    pub has_delete_object: bool,
}

#[derive(Debug, Serialize, Deserialize, AsChangeset, Default)]
#[diesel(table_name = permissions)]
pub struct UpdatePermission {
    pub has_read_namespace: Option<bool>,
    pub has_update_namespace: Option<bool>,
    pub has_delete_namespace: Option<bool>,
    pub has_delegate_namespace: Option<bool>,
    pub has_create_class: Option<bool>,
    pub has_read_class: Option<bool>,
    pub has_update_class: Option<bool>,
    pub has_delete_class: Option<bool>,
    pub has_create_object: Option<bool>,
    pub has_read_object: Option<bool>,
    pub has_update_object: Option<bool>,
    pub has_delete_object: Option<bool>,
}
