use diesel::prelude::*;
use serde::{Deserialize, Serialize};

use crate::schema::namespacepermissions;
use crate::schema::namespaces;
use crate::schema::objectpermissions;

#[derive(Serialize, Deserialize, Queryable, Insertable)]
#[diesel(table_name = namespaces)]
pub struct Namespace {
    pub id: i32,
    pub name: String,
    pub description: String,
}

#[derive(Serialize, Deserialize, Queryable, Insertable)]
#[diesel(table_name = namespacepermissions)]
pub struct NamespacePermission {
    pub id: i32,
    pub namespace_id: i32,
    pub user_id: i32,
    pub group_id: i32,
    pub has_create: bool,
    pub has_read: bool,
    pub has_update: bool,
    pub has_delete: bool,
    pub has_delegate: bool,
}

#[derive(Serialize, Deserialize, Queryable, Insertable)]
#[diesel(table_name = objectpermissions)]
pub struct ObjectPermission {
    pub id: i32,
    pub namespace_id: i32,
    pub user_id: i32,
    pub group_id: i32,
    pub has_create: bool,
    pub has_read: bool,
    pub has_update: bool,
    pub has_delete: bool,
}
