use crate::db::DbPool;
use crate::db::traits::relations::{
    DeleteObjectRelationRecord, LoadObjectRelationRecord, SaveObjectRelationRecord,
};

use crate::errors::ApiError;

use crate::models::{
    HubuumObjectRelation, HubuumObjectRelationID, HubuumObjectWithPath, NewHubuumObjectRelation,
    ObjectClosureRow, RelatedObjectClosureRow,
};
use crate::traits::accessors::{IdAccessor, InstanceAdapter};
use crate::traits::crud::{DeleteAdapter, SaveAdapter};

impl IdAccessor for HubuumObjectRelationID {
    fn accessor_id(&self) -> i32 {
        self.0
    }
}

impl InstanceAdapter<HubuumObjectRelation> for HubuumObjectRelationID {
    async fn instance_adapter(&self, pool: &DbPool) -> Result<HubuumObjectRelation, ApiError> {
        self.load_object_relation_record(pool).await
    }
}
impl IdAccessor for HubuumObjectRelation {
    fn accessor_id(&self) -> i32 {
        self.id
    }
}

impl InstanceAdapter<HubuumObjectRelation> for HubuumObjectRelation {
    async fn instance_adapter(&self, _pool: &DbPool) -> Result<HubuumObjectRelation, ApiError> {
        Ok(*self)
    }
}

impl DeleteAdapter for HubuumObjectRelation {
    async fn delete_adapter(&self, pool: &DbPool) -> Result<(), ApiError> {
        self.delete_object_relation_record(pool).await
    }
}

impl DeleteAdapter for HubuumObjectRelationID {
    async fn delete_adapter(&self, pool: &DbPool) -> Result<(), ApiError> {
        self.delete_object_relation_record(pool).await
    }
}

impl SaveAdapter for NewHubuumObjectRelation {
    type Output = HubuumObjectRelation;

    async fn save_adapter(&self, pool: &DbPool) -> Result<HubuumObjectRelation, ApiError> {
        self.save_object_relation_record(pool).await
    }
}

impl ObjectClosureRow {
    pub fn to_descendant_object_with_path(&self) -> HubuumObjectWithPath {
        HubuumObjectWithPath {
            id: self.descendant_object_id,
            name: self.descendant_name.clone(),
            namespace_id: self.descendant_namespace_id,
            hubuum_class_id: self.descendant_class_id,
            data: self.descendant_data.clone(),
            description: self.descendant_description.clone(),
            created_at: self.descendant_created_at,
            updated_at: self.descendant_updated_at,
            path: self.path.clone(),
        }
    }
}

impl RelatedObjectClosureRow {
    pub fn to_descendant_object_with_path(&self) -> HubuumObjectWithPath {
        HubuumObjectWithPath {
            id: self.descendant_object_id,
            name: self.descendant_name.clone(),
            namespace_id: self.descendant_namespace_id,
            hubuum_class_id: self.descendant_class_id,
            data: self.descendant_data.clone(),
            description: self.descendant_description.clone(),
            created_at: self.descendant_created_at,
            updated_at: self.descendant_updated_at,
            path: self.path.clone(),
        }
    }
}

// Trait for converting closure rows to API-ready object payloads.
pub trait ToHubuumObjects {
    fn to_descendant_objects_with_path(self) -> Vec<HubuumObjectWithPath>;
}

impl ToHubuumObjects for Vec<ObjectClosureRow> {
    fn to_descendant_objects_with_path(self) -> Vec<HubuumObjectWithPath> {
        self.into_iter()
            .map(|ocv| ocv.to_descendant_object_with_path())
            .collect()
    }
}

impl ToHubuumObjects for Vec<RelatedObjectClosureRow> {
    fn to_descendant_objects_with_path(self) -> Vec<HubuumObjectWithPath> {
        self.into_iter()
            .map(|ocv| ocv.to_descendant_object_with_path())
            .collect()
    }
}
