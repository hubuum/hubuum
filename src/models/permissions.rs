use diesel::prelude::*;

use serde::{Deserialize, Serialize};

use crate::schema::classpermissions;
use crate::schema::namespacepermissions;
use crate::schema::objectpermissions;

#[derive(Debug, PartialEq, Eq, Hash, Serialize, Deserialize, Clone)]
pub enum NamespacePermissions {
    CreateClass,
    CreateObject,
    ReadCollection,
    UpdateCollection,
    DeleteCollection,
    DelegateCollection,
}

#[derive(Debug, PartialEq, Eq, Hash, Serialize, Deserialize, Clone)]
pub enum ClassPermissions {
    CreateObject,
    ReadClass,
    UpdateClass,
    DeleteClass,
}

// We use the object suffix for consistency with other models.
#[allow(clippy::enum_variant_names)]
#[derive(Debug, PartialEq, Eq, Hash, Serialize, Deserialize, Clone)]
pub enum ObjectPermissions {
    ReadObject,
    UpdateObject,
    DeleteObject,
}

pub trait PermissionFilter<'a, Q> {
    fn filter(self, query: Q) -> Q;
}

impl<'a> PermissionFilter<'a, classpermissions::BoxedQuery<'a, diesel::pg::Pg>>
    for ClassPermissions
{
    fn filter(
        self,
        query: classpermissions::BoxedQuery<diesel::pg::Pg>,
    ) -> classpermissions::BoxedQuery<diesel::pg::Pg> {
        match self {
            ClassPermissions::CreateObject => {
                query.filter(classpermissions::has_create_object.eq(true))
            }
            ClassPermissions::ReadClass => query.filter(classpermissions::has_read_class.eq(true)),
            ClassPermissions::UpdateClass => {
                query.filter(classpermissions::has_update_class.eq(true))
            }
            ClassPermissions::DeleteClass => {
                query.filter(classpermissions::has_delete_class.eq(true))
            }
        }
    }
}

impl<'a> PermissionFilter<'a, objectpermissions::BoxedQuery<'a, diesel::pg::Pg>>
    for ObjectPermissions
{
    fn filter(
        self,
        query: objectpermissions::BoxedQuery<diesel::pg::Pg>,
    ) -> objectpermissions::BoxedQuery<diesel::pg::Pg> {
        match self {
            ObjectPermissions::ReadObject => {
                query.filter(objectpermissions::has_read_object.eq(true))
            }
            ObjectPermissions::UpdateObject => {
                query.filter(objectpermissions::has_update_object.eq(true))
            }
            ObjectPermissions::DeleteObject => {
                query.filter(objectpermissions::has_delete_object.eq(true))
            }
        }
    }
}

impl<'a> PermissionFilter<'a, namespacepermissions::BoxedQuery<'a, diesel::pg::Pg>>
    for NamespacePermissions
{
    fn filter(
        self,
        query: namespacepermissions::BoxedQuery<diesel::pg::Pg>,
    ) -> namespacepermissions::BoxedQuery<diesel::pg::Pg> {
        match self {
            NamespacePermissions::CreateClass => {
                query.filter(namespacepermissions::has_create_class.eq(true))
            }
            NamespacePermissions::CreateObject => {
                query.filter(namespacepermissions::has_create_object.eq(true))
            }
            NamespacePermissions::ReadCollection => {
                query.filter(namespacepermissions::has_read_namespace.eq(true))
            }
            NamespacePermissions::UpdateCollection => {
                query.filter(namespacepermissions::has_update_namespace.eq(true))
            }
            NamespacePermissions::DeleteCollection => {
                query.filter(namespacepermissions::has_delete_namespace.eq(true))
            }
            NamespacePermissions::DelegateCollection => {
                query.filter(namespacepermissions::has_delegate_namespace.eq(true))
            }
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Queryable)]
#[diesel(table_name = namespacepermissions)]
pub struct NamespacePermission {
    pub id: i32,
    pub namespace_id: i32,
    pub group_id: i32,
    pub has_create_object: bool,
    pub has_create_class: bool,
    pub has_read_namespace: bool,
    pub has_update_namespace: bool,
    pub has_delete_namespace: bool,
    pub has_delegate_namespace: bool,
}

#[derive(Debug, Serialize, Deserialize, Queryable)]
#[diesel(table_name = classpermissions)]
pub struct ClassPermission {
    pub id: i32,
    pub namespace_id: i32,
    pub group_id: i32,
    pub has_create_object: bool,
    pub has_read_class: bool,
    pub has_update_class: bool,
    pub has_delete_class: bool,
}

#[derive(Debug, Serialize, Deserialize, Queryable)]
#[diesel(table_name = objectpermissions)]
pub struct ObjectPermission {
    pub id: i32,
    pub namespace_id: i32,
    pub group_id: i32,
    pub has_read_object: bool,
    pub has_update_object: bool,
    pub has_delete_object: bool,
}

// Insertable permission models.
#[derive(Debug, Serialize, Deserialize, Insertable)]
#[diesel(table_name = namespacepermissions)]
pub struct NewNamespacePermission {
    pub namespace_id: i32,
    pub group_id: i32,
    pub has_create_object: bool,
    pub has_create_class: bool,
    pub has_read_namespace: bool,
    pub has_update_namespace: bool,
    pub has_delete_namespace: bool,
    pub has_delegate_namespace: bool,
}

#[derive(Debug, Serialize, Deserialize, Insertable)]
#[diesel(table_name = classpermissions)]
pub struct NewClassPermission {
    pub namespace_id: i32,
    pub group_id: i32,
    pub has_create_object: bool,
    pub has_read_class: bool,
    pub has_update_class: bool,
    pub has_delete_class: bool,
}

#[derive(Debug, Serialize, Deserialize, Insertable)]
#[diesel(table_name = objectpermissions)]
pub struct NewObjectPermission {
    pub namespace_id: i32,
    pub group_id: i32,
    pub has_read_object: bool,
    pub has_update_object: bool,
    pub has_delete_object: bool,
}
