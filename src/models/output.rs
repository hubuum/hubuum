// These are models that are used to serialize the output of the API
// They are not used to interact with the database

// A typical use is to combine the output of multiple models into a single response

use crate::models::{Group, HubuumClass, Namespace, Permission};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Serialize, Deserialize, Clone, ToSchema)]
pub struct GroupPermission {
    pub group: Group,
    pub permission: Permission,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, ToSchema)]
pub struct HubuumClassExpanded {
    pub id: i32,
    pub name: String,
    pub namespace: crate::models::namespace::Namespace,
    pub json_schema: Option<serde_json::Value>,
    pub validate_schema: bool,
    pub description: String,
    pub created_at: chrono::NaiveDateTime,
    pub updated_at: chrono::NaiveDateTime,
}

impl PartialEq<HubuumClass> for HubuumClassExpanded {
    fn eq(&self, other: &HubuumClass) -> bool {
        self.id == other.id
            && self.name == other.name
            && self.namespace.id == other.namespace_id
            && self.json_schema == other.json_schema
            && self.validate_schema == other.validate_schema
            && self.description == other.description
            && self.created_at == other.created_at
            && self.updated_at == other.updated_at
    }
}

impl PartialEq<HubuumClassExpanded> for HubuumClass {
    fn eq(&self, other: &HubuumClassExpanded) -> bool {
        self.id == other.id
            && self.name == other.name
            && self.namespace_id == other.namespace.id
            && self.json_schema == other.json_schema
            && self.validate_schema == other.validate_schema
            && self.description == other.description
            && self.created_at == other.created_at
            && self.updated_at == other.updated_at
    }
}
