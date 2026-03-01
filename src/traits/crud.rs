use crate::db::DbPool;
use crate::errors::ApiError;

pub trait CanDelete {
    async fn delete(&self, pool: &DbPool) -> Result<(), ApiError>;
}

pub trait CanSave {
    type Output;
    async fn save(&self, pool: &DbPool) -> Result<Self::Output, ApiError>;
}

pub trait CanUpdate {
    type Output;
    async fn update(&self, pool: &DbPool, entry_id: i32) -> Result<Self::Output, ApiError>;
}

#[allow(dead_code)]
pub trait Validate {
    /// Complete validation of the object.
    ///
    /// This returns errors if:
    /// - If the object's class requires validation (validate_schema), and the object's data
    ///   fails validation against the class's JSON schema (json_schema).
    async fn validate(&self, pool: &DbPool) -> Result<(), ApiError>;
}

#[allow(dead_code)]
pub trait ValidateAgainstSchema {
    /// Validate the object's data against the class's JSON schema.
    ///
    /// This does not check if the class requires validation (validate_schema), it
    /// only checks if the data is valid against the schema.
    ///
    /// Returns OK() if any of the following are true:
    /// - The class does not have a schema (json_schema).
    /// - The object data is valid against the schema.
    async fn validate_against_schema(&self, schema: &serde_json::Value) -> Result<(), ApiError>;
}
