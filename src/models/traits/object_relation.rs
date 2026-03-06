use crate::db::DbPool;
use crate::db::traits::relations::{
    DeleteObjectRelationRecord, LoadObjectRelationRecord, SaveObjectRelationRecord,
};

use crate::errors::ApiError;

use crate::models::{
    HubuumObject, HubuumObjectRelation, HubuumObjectRelationID, HubuumObjectWithPath,
    NewHubuumObjectRelation, ObjectClosureRow,
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
    #[allow(dead_code)]
    pub fn to_descendant_object(&self) -> HubuumObject {
        HubuumObject {
            id: self.descendant_object_id,
            name: self.descendant_name.clone(),
            namespace_id: self.descendant_namespace_id,
            hubuum_class_id: self.descendant_class_id,
            data: self.descendant_data.clone(),
            description: self.descendant_description.clone(),
            created_at: self.descendant_created_at,
            updated_at: self.descendant_updated_at,
        }
    }

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

    #[allow(dead_code)]
    pub fn to_ascendant_object(&self) -> HubuumObject {
        HubuumObject {
            id: self.ancestor_object_id,
            name: self.ancestor_name.clone(),
            namespace_id: self.ancestor_namespace_id,
            hubuum_class_id: self.ancestor_class_id,
            data: self.ancestor_data.clone(),
            description: self.ancestor_description.clone(),
            created_at: self.ancestor_created_at,
            updated_at: self.ancestor_updated_at,
        }
    }
}

// Trait for converting iterators of ObjectClosureRow to Vec<HubuumObject>
#[allow(dead_code)]
pub trait ToHubuumObjects {
    fn to_descendant_objects(self) -> Vec<HubuumObject>;
    fn to_descendant_objects_with_path(self) -> Vec<HubuumObjectWithPath>;
    fn to_ascendant_objects(self) -> Vec<HubuumObject>;
}

impl ToHubuumObjects for Vec<ObjectClosureRow> {
    fn to_descendant_objects(self) -> Vec<HubuumObject> {
        self.into_iter()
            .map(|ocv| ocv.to_descendant_object())
            .collect()
    }

    fn to_descendant_objects_with_path(self) -> Vec<HubuumObjectWithPath> {
        self.into_iter()
            .map(|ocv| ocv.to_descendant_object_with_path())
            .collect()
    }

    fn to_ascendant_objects(self) -> Vec<HubuumObject> {
        self.into_iter()
            .map(|ocv| ocv.to_ascendant_object())
            .collect()
    }
}
