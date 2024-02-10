// These are models that are used to serialize the output of the API
// They are not used to interact with the database

// A typical use is to combine the output of multiple models into a single response

use crate::models::group::Group;
use crate::models::permissions::{ClassPermission, NamespacePermission, ObjectPermission};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct GroupNamespacePermission {
    pub group: Group,
    pub namespace_permission: NamespacePermission,
}

#[derive(Serialize, Deserialize)]
pub struct GroupClassPermission {
    pub group: Group,
    pub class_permission: ClassPermission,
}

#[derive(Serialize, Deserialize)]
pub struct GroupObjectPermission {
    pub group: Group,
    pub object_permission: ObjectPermission,
}
