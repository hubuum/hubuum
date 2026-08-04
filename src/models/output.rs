// These are models that are used to serialize the output of the API
// They are not used to interact with the database

// A typical use is to combine the output of multiple models into a single response

use crate::models::{Collection, Group, HubuumClass, Permission, ResourceRevision};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Serialize, Deserialize, Clone, ToSchema)]
pub struct GroupPermission {
    pub group: Group,
    pub permission: Permission,
}

/// The revisioned, SQL-owned authorization state for a collection.
///
/// Individual permission rows are implementation details of this aggregate;
/// callers condition mutations on the revision of the complete set.
#[derive(Serialize, Deserialize, Clone, ToSchema)]
pub struct CollectionPermissionSet {
    pub collection_id: i32,
    pub revision: ResourceRevision,
    pub permissions: Vec<GroupPermission>,
}

#[derive(Serialize, Deserialize, Clone, ToSchema)]
pub struct EffectiveGroupPermission {
    pub target_collection: Collection,
    pub source_collection: Collection,
    pub depth: i32,
    pub inherited: bool,
    pub group: Group,
    pub permission: Permission,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, ToSchema)]
pub struct HubuumClassExpanded {
    pub id: i32,
    pub name: String,
    pub collection: crate::models::collection::Collection,
    pub json_schema: Option<serde_json::Value>,
    pub validate_schema: bool,
    pub description: String,
    pub created_at: chrono::NaiveDateTime,
    pub updated_at: chrono::NaiveDateTime,
    pub revision: ResourceRevision,
}

impl PartialEq<HubuumClass> for HubuumClassExpanded {
    fn eq(&self, other: &HubuumClass) -> bool {
        self.id == other.id
            && self.name == other.name
            && self.collection.id == other.collection_id
            && self.json_schema == other.json_schema
            && self.validate_schema == other.validate_schema
            && self.description == other.description
            && self.created_at == other.created_at
            && self.updated_at == other.updated_at
            && self.revision == other.revision
    }
}

impl PartialEq<HubuumClassExpanded> for HubuumClass {
    fn eq(&self, other: &HubuumClassExpanded) -> bool {
        self.id == other.id
            && self.name == other.name
            && self.collection_id == other.collection.id
            && self.json_schema == other.json_schema
            && self.validate_schema == other.validate_schema
            && self.description == other.description
            && self.created_at == other.created_at
            && self.updated_at == other.updated_at
            && self.revision == other.revision
    }
}
