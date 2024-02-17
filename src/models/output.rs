// These are models that are used to serialize the output of the API
// They are not used to interact with the database

// A typical use is to combine the output of multiple models into a single response

use crate::models::group::Group;
use crate::models::permissions::Permission;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone)]
pub struct GroupPermission {
    pub group: Group,
    pub permission: Permission,
}
