use diesel::prelude::*;
use std::{fmt, fmt::Display, slice};

use tracing::{debug, trace};

use serde::{Deserialize, Serialize};

use crate::db::traits::relations::{
    DeleteClassRelationRecord, LoadClassRelationRecord, SaveClassRelationRecord,
};
use crate::db::traits::GetNamespace;
use crate::db::DbPool;
use crate::{errors::ApiError, schema::hubuumclass_relation, schema::hubuumobject_relation};

use crate::models::search::{FilterField, SortParam};
use crate::models::{
    ClassClosureView, HubuumClass, HubuumClassRelation, HubuumClassRelationID,
    HubuumClassRelationTransitive, HubuumClassWithPath, HubuumObject, HubuumObjectRelation,
    HubuumObjectRelationID, Namespace, NewHubuumClassRelation, NewHubuumObjectRelation,
    ObjectClosureView,
};
use crate::traits::{
    ClassAccessors, CursorPaginated, CursorSqlField, CursorSqlMapping, CursorSqlType,
    CursorValue, NamespaceAccessors, ObjectAccessors, SelfAccessors,
};
use crate::traits::accessors::{
    ClassAdapter, IdAccessor, InstanceAdapter, NamespaceAdapter, ObjectAdapter,
};
use crate::traits::crud::{DeleteAdapter, SaveAdapter};

impl IdAccessor for HubuumClassRelationID {
    fn accessor_id(&self) -> i32 {
        self.0
    }
}

impl InstanceAdapter<HubuumClassRelation> for HubuumClassRelationID {
    async fn instance_adapter(&self, pool: &DbPool) -> Result<HubuumClassRelation, ApiError> {
        self.load_class_relation_record(pool).await
    }
}
impl IdAccessor for HubuumClassRelation {
    fn accessor_id(&self) -> i32 {
        self.id
    }
}

impl InstanceAdapter<HubuumClassRelation> for HubuumClassRelation {
    async fn instance_adapter(&self, _pool: &DbPool) -> Result<HubuumClassRelation, ApiError> {
        Ok(*self)
    }
}

impl DeleteAdapter for HubuumClassRelation {
    async fn delete_adapter(&self, pool: &DbPool) -> Result<(), ApiError> {
        self.delete_class_relation_record(pool).await
    }
}

impl SaveAdapter for NewHubuumClassRelation {
    type Output = HubuumClassRelation;

    async fn save_adapter(&self, pool: &DbPool) -> Result<HubuumClassRelation, ApiError> {
        self.save_class_relation_record(pool).await
    }
}

impl DeleteAdapter for HubuumClassRelationID {
    async fn delete_adapter(&self, pool: &DbPool) -> Result<(), ApiError> {
        self.delete_class_relation_record(pool).await
    }
}

impl NamespaceAdapter<(Namespace, Namespace), (i32, i32)> for NewHubuumClassRelation {
    async fn namespace_adapter(&self, pool: &DbPool) -> Result<(Namespace, Namespace), ApiError> {
        use crate::db::traits::GetNamespace;
        self.namespace_from_backend(pool).await
    }

    async fn namespace_id_adapter(&self, pool: &DbPool) -> Result<(i32, i32), ApiError> {
        let (ns1, ns2) = self.namespace(pool).await?;
        Ok((ns1.id, ns2.id))
    }
}

impl NamespaceAdapter<(Namespace, Namespace), (i32, i32)> for NewHubuumObjectRelation {
    async fn namespace_adapter(&self, pool: &DbPool) -> Result<(Namespace, Namespace), ApiError> {
        use crate::db::traits::GetNamespace;
        self.namespace_from_backend(pool).await
    }

    async fn namespace_id_adapter(&self, pool: &DbPool) -> Result<(i32, i32), ApiError> {
        let (ns1, ns2) = self.namespace(pool).await?;
        Ok((ns1.id, ns2.id))
    }
}

impl NamespaceAdapter<(Namespace, Namespace), (i32, i32)> for HubuumObjectRelationID {
    async fn namespace_adapter(&self, pool: &DbPool) -> Result<(Namespace, Namespace), ApiError> {
        self.instance(pool).await?.namespace(pool).await
    }

    async fn namespace_id_adapter(&self, pool: &DbPool) -> Result<(i32, i32), ApiError> {
        self.instance(pool).await?.namespace_id(pool).await
    }
}

impl NamespaceAdapter<(Namespace, Namespace), (i32, i32)> for HubuumObjectRelation {
    async fn namespace_adapter(&self, pool: &DbPool) -> Result<(Namespace, Namespace), ApiError> {
        use crate::db::traits::GetNamespace;
        self.namespace_from_backend(pool).await
    }

    async fn namespace_id_adapter(&self, pool: &DbPool) -> Result<(i32, i32), ApiError> {
        let (ns1, ns2) = self.namespace(pool).await?;
        Ok((ns1.id, ns2.id))
    }
}

impl NamespaceAdapter<(Namespace, Namespace), (i32, i32)> for HubuumClassRelation {
    async fn namespace_adapter(&self, pool: &DbPool) -> Result<(Namespace, Namespace), ApiError> {
        use crate::db::traits::GetNamespace;
        self.namespace_from_backend(pool).await
    }

    async fn namespace_id_adapter(&self, pool: &DbPool) -> Result<(i32, i32), ApiError> {
        let (ns1, ns2) = self.namespace(pool).await?;
        Ok((ns1.id, ns2.id))
    }
}

impl ClassAdapter<(HubuumClass, HubuumClass), (i32, i32)> for HubuumClassRelation {
    async fn class_adapter(&self, pool: &DbPool) -> Result<(HubuumClass, HubuumClass), ApiError> {
        use crate::db::traits::GetClass;
        self.class_from_backend(pool).await
    }

    async fn class_id_adapter(&self, _pool: &DbPool) -> Result<(i32, i32), ApiError> {
        Ok((self.from_hubuum_class_id, self.to_hubuum_class_id))
    }
}

impl NamespaceAdapter<(Namespace, Namespace), (i32, i32)> for HubuumClassRelationID {
    async fn namespace_adapter(&self, pool: &DbPool) -> Result<(Namespace, Namespace), ApiError> {
        self.instance(pool).await?.namespace(pool).await
    }

    async fn namespace_id_adapter(&self, pool: &DbPool) -> Result<(i32, i32), ApiError> {
        self.instance(pool).await?.namespace_id(pool).await
    }
}

impl ClassAdapter<(HubuumClass, HubuumClass), (i32, i32)> for HubuumClassRelationID {
    async fn class_adapter(&self, pool: &DbPool) -> Result<(HubuumClass, HubuumClass), ApiError> {
        use crate::db::traits::GetClass;
        self.class_from_backend(pool).await
    }

    async fn class_id_adapter(&self, pool: &DbPool) -> Result<(i32, i32), ApiError> {
        self.instance(pool).await?.class_id(pool).await
    }
}

impl ClassAdapter<(HubuumClass, HubuumClass), (i32, i32)> for NewHubuumClassRelation {
    async fn class_adapter(&self, pool: &DbPool) -> Result<(HubuumClass, HubuumClass), ApiError> {
        use crate::db::traits::GetClass;
        self.class_from_backend(pool).await
    }

    async fn class_id_adapter(&self, _pool: &DbPool) -> Result<(i32, i32), ApiError> {
        Ok((self.from_hubuum_class_id, self.to_hubuum_class_id))
    }
}

impl ObjectAdapter<(HubuumObject, HubuumObject), (i32, i32)> for NewHubuumObjectRelation {
    async fn object_adapter(&self, pool: &DbPool) -> Result<(HubuumObject, HubuumObject), ApiError> {
        use crate::db::traits::GetObject;
        self.object_from_backend(pool).await
    }

    async fn object_id_adapter(&self, _pool: &DbPool) -> Result<(i32, i32), ApiError> {
        Ok((self.from_hubuum_object_id, self.to_hubuum_object_id))
    }
}

impl ObjectAdapter<(HubuumObject, HubuumObject), (i32, i32)> for HubuumObjectRelationID {
    async fn object_adapter(&self, pool: &DbPool) -> Result<(HubuumObject, HubuumObject), ApiError> {
        use crate::db::traits::GetObject;
        self.object_from_backend(pool).await
    }

    async fn object_id_adapter(&self, pool: &DbPool) -> Result<(i32, i32), ApiError> {
        self.instance(pool).await?.object_id(pool).await
    }
}

impl ObjectAdapter<(HubuumObject, HubuumObject), (i32, i32)> for HubuumObjectRelation {
    async fn object_adapter(&self, pool: &DbPool) -> Result<(HubuumObject, HubuumObject), ApiError> {
        use crate::db::traits::GetObject;
        self.object_from_backend(pool).await
    }

    async fn object_id_adapter(&self, _pool: &DbPool) -> Result<(i32, i32), ApiError> {
        Ok((self.from_hubuum_object_id, self.to_hubuum_object_id))
    }
}

impl ClassClosureView {
    #[allow(dead_code)]
    pub fn to_ascendant_class(&self) -> HubuumClass {
        HubuumClass {
            id: self.ancestor_class_id,
            name: self.ancestor_name.clone(),
            namespace_id: self.ancestor_namespace_id,
            description: self.ancestor_description.clone(),
            json_schema: self.ancestor_json_schema.clone(),
            validate_schema: self.ancestor_validate_schema,
            created_at: self.ancestor_created_at,
            updated_at: self.ancestor_updated_at,
        }
    }

    #[allow(dead_code)]
    pub fn to_descendant_class(&self) -> HubuumClass {
        HubuumClass {
            id: self.descendant_class_id,
            name: self.descendant_name.clone(),
            namespace_id: self.descendant_namespace_id,
            description: self.descendant_description.clone(),
            json_schema: self.descendant_json_schema.clone(),
            validate_schema: self.descendant_validate_schema,
            created_at: self.descendant_created_at,
            updated_at: self.descendant_updated_at,
        }
    }

    #[allow(dead_code)]
    pub fn to_descendant_class_with_path(&self) -> HubuumClassWithPath {
        HubuumClassWithPath {
            id: self.descendant_class_id,
            name: self.descendant_name.clone(),
            namespace_id: self.descendant_namespace_id,
            description: self.descendant_description.clone(),
            json_schema: self.descendant_json_schema.clone(),
            validate_schema: self.descendant_validate_schema,
            created_at: self.descendant_created_at,
            updated_at: self.descendant_updated_at,
            path: self.path.clone(),
        }
    }
}

#[allow(dead_code)]
pub trait ToHubuumClasses {
    fn to_descendant_classes(self) -> Vec<HubuumClass>;
    fn to_descendant_classes_with_path(self) -> Vec<HubuumClassWithPath>;
    fn to_ascendant_classes(self) -> Vec<HubuumClass>;
}

impl ToHubuumClasses for Vec<ClassClosureView> {
    fn to_descendant_classes(self) -> Vec<HubuumClass> {
        self.into_iter()
            .map(|ocv| ocv.to_descendant_class())
            .collect()
    }

    fn to_descendant_classes_with_path(self) -> Vec<HubuumClassWithPath> {
        self.into_iter()
            .map(|ocv| ocv.to_descendant_class_with_path())
            .collect()
    }

    fn to_ascendant_classes(self) -> Vec<HubuumClass> {
        self.into_iter()
            .map(|ocv| ocv.to_ascendant_class())
            .collect()
    }
}

impl CursorPaginated for HubuumClassRelation {
    fn supports_sort(field: &FilterField) -> bool {
        matches!(
            field,
            FilterField::Id
                | FilterField::ClassFrom
                | FilterField::ClassTo
                | FilterField::CreatedAt
                | FilterField::UpdatedAt
        )
    }

    fn cursor_value(&self, field: &FilterField) -> Result<CursorValue, ApiError> {
        Ok(match field {
            FilterField::Id => CursorValue::Integer(self.id as i64),
            FilterField::ClassFrom => CursorValue::Integer(self.from_hubuum_class_id as i64),
            FilterField::ClassTo => CursorValue::Integer(self.to_hubuum_class_id as i64),
            FilterField::CreatedAt => CursorValue::DateTime(self.created_at),
            FilterField::UpdatedAt => CursorValue::DateTime(self.updated_at),
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{}' is not orderable for class relations",
                    field
                )));
            }
        })
    }

    fn default_sort() -> Vec<SortParam> {
        vec![SortParam {
            field: FilterField::Id,
            descending: false,
        }]
    }

    fn tie_breaker_sort() -> Vec<SortParam> {
        Self::default_sort()
    }
}

impl CursorSqlMapping for HubuumClassRelation {
    fn sql_field(field: &FilterField) -> Result<CursorSqlField, ApiError> {
        Ok(match field {
            FilterField::Id => CursorSqlField {
                column: "hubuumclass_relation.id",
                sql_type: CursorSqlType::Integer,
                nullable: false,
            },
            FilterField::ClassFrom => CursorSqlField {
                column: "hubuumclass_relation.from_hubuum_class_id",
                sql_type: CursorSqlType::Integer,
                nullable: false,
            },
            FilterField::ClassTo => CursorSqlField {
                column: "hubuumclass_relation.to_hubuum_class_id",
                sql_type: CursorSqlType::Integer,
                nullable: false,
            },
            FilterField::CreatedAt => CursorSqlField {
                column: "hubuumclass_relation.created_at",
                sql_type: CursorSqlType::DateTime,
                nullable: false,
            },
            FilterField::UpdatedAt => CursorSqlField {
                column: "hubuumclass_relation.updated_at",
                sql_type: CursorSqlType::DateTime,
                nullable: false,
            },
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{}' is not orderable for class relations",
                    field
                )));
            }
        })
    }
}

impl CursorPaginated for HubuumObjectRelation {
    fn supports_sort(field: &FilterField) -> bool {
        matches!(
            field,
            FilterField::Id
                | FilterField::ClassRelation
                | FilterField::ObjectFrom
                | FilterField::ObjectTo
                | FilterField::CreatedAt
                | FilterField::UpdatedAt
        )
    }

    fn cursor_value(&self, field: &FilterField) -> Result<CursorValue, ApiError> {
        Ok(match field {
            FilterField::Id => CursorValue::Integer(self.id as i64),
            FilterField::ClassRelation => CursorValue::Integer(self.class_relation_id as i64),
            FilterField::ObjectFrom => CursorValue::Integer(self.from_hubuum_object_id as i64),
            FilterField::ObjectTo => CursorValue::Integer(self.to_hubuum_object_id as i64),
            FilterField::CreatedAt => CursorValue::DateTime(self.created_at),
            FilterField::UpdatedAt => CursorValue::DateTime(self.updated_at),
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{}' is not orderable for object relations",
                    field
                )));
            }
        })
    }

    fn default_sort() -> Vec<SortParam> {
        vec![SortParam {
            field: FilterField::Id,
            descending: false,
        }]
    }

    fn tie_breaker_sort() -> Vec<SortParam> {
        Self::default_sort()
    }
}

impl CursorSqlMapping for HubuumObjectRelation {
    fn sql_field(field: &FilterField) -> Result<CursorSqlField, ApiError> {
        Ok(match field {
            FilterField::Id => CursorSqlField {
                column: "hubuumobject_relation.id",
                sql_type: CursorSqlType::Integer,
                nullable: false,
            },
            FilterField::ClassRelation => CursorSqlField {
                column: "hubuumobject_relation.class_relation_id",
                sql_type: CursorSqlType::Integer,
                nullable: false,
            },
            FilterField::ObjectFrom => CursorSqlField {
                column: "hubuumobject_relation.from_hubuum_object_id",
                sql_type: CursorSqlType::Integer,
                nullable: false,
            },
            FilterField::ObjectTo => CursorSqlField {
                column: "hubuumobject_relation.to_hubuum_object_id",
                sql_type: CursorSqlType::Integer,
                nullable: false,
            },
            FilterField::CreatedAt => CursorSqlField {
                column: "hubuumobject_relation.created_at",
                sql_type: CursorSqlType::DateTime,
                nullable: false,
            },
            FilterField::UpdatedAt => CursorSqlField {
                column: "hubuumobject_relation.updated_at",
                sql_type: CursorSqlType::DateTime,
                nullable: false,
            },
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{}' is not orderable for object relations",
                    field
                )));
            }
        })
    }
}

impl CursorPaginated for HubuumClassRelationTransitive {
    fn supports_sort(field: &FilterField) -> bool {
        matches!(
            field,
            FilterField::ClassFrom | FilterField::ClassTo | FilterField::Depth | FilterField::Path
        )
    }

    fn cursor_value(&self, field: &FilterField) -> Result<CursorValue, ApiError> {
        Ok(match field {
            FilterField::ClassFrom => CursorValue::Integer(self.ancestor_class_id as i64),
            FilterField::ClassTo => CursorValue::Integer(self.descendant_class_id as i64),
            FilterField::Depth => CursorValue::Integer(self.depth as i64),
            FilterField::Path => {
                CursorValue::IntegerArray(self.path.iter().filter_map(|item| *item).collect())
            }
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{}' is not orderable for transitive class relations",
                    field
                )));
            }
        })
    }

    fn default_sort() -> Vec<SortParam> {
        vec![
            SortParam {
                field: FilterField::Depth,
                descending: false,
            },
            SortParam {
                field: FilterField::Path,
                descending: false,
            },
        ]
    }

    fn tie_breaker_sort() -> Vec<SortParam> {
        vec![
            SortParam {
                field: FilterField::ClassFrom,
                descending: false,
            },
            SortParam {
                field: FilterField::ClassTo,
                descending: false,
            },
            SortParam {
                field: FilterField::Depth,
                descending: false,
            },
            SortParam {
                field: FilterField::Path,
                descending: false,
            },
        ]
    }
}

impl CursorSqlMapping for HubuumClassRelationTransitive {
    fn sql_field(field: &FilterField) -> Result<CursorSqlField, ApiError> {
        Ok(match field {
            FilterField::ClassFrom => CursorSqlField {
                column: "hubuumclass_closure.ancestor_class_id",
                sql_type: CursorSqlType::Integer,
                nullable: false,
            },
            FilterField::ClassTo => CursorSqlField {
                column: "hubuumclass_closure.descendant_class_id",
                sql_type: CursorSqlType::Integer,
                nullable: false,
            },
            FilterField::Depth => CursorSqlField {
                column: "hubuumclass_closure.depth",
                sql_type: CursorSqlType::Integer,
                nullable: false,
            },
            FilterField::Path => CursorSqlField {
                column: "hubuumclass_closure.path",
                sql_type: CursorSqlType::IntegerArray,
                nullable: true,
            },
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{}' is not orderable for transitive class relations",
                    field
                )));
            }
        })
    }
}

impl CursorPaginated for ObjectClosureView {
    fn supports_sort(field: &FilterField) -> bool {
        matches!(
            field,
            FilterField::Id
                | FilterField::Name
                | FilterField::Description
                | FilterField::Namespaces
                | FilterField::NamespaceId
                | FilterField::ClassId
                | FilterField::Classes
                | FilterField::CreatedAt
                | FilterField::UpdatedAt
                | FilterField::ObjectFrom
                | FilterField::ObjectTo
                | FilterField::ClassFrom
                | FilterField::ClassTo
                | FilterField::NamespacesFrom
                | FilterField::NamespacesTo
                | FilterField::NameFrom
                | FilterField::NameTo
                | FilterField::DescriptionFrom
                | FilterField::DescriptionTo
                | FilterField::CreatedAtFrom
                | FilterField::CreatedAtTo
                | FilterField::UpdatedAtFrom
                | FilterField::UpdatedAtTo
                | FilterField::Depth
                | FilterField::Path
        )
    }

    fn cursor_value(&self, field: &FilterField) -> Result<CursorValue, ApiError> {
        Ok(match field {
            FilterField::Id | FilterField::ObjectTo => {
                CursorValue::Integer(self.descendant_object_id as i64)
            }
            FilterField::ObjectFrom => CursorValue::Integer(self.ancestor_object_id as i64),
            FilterField::Name | FilterField::NameTo => {
                CursorValue::String(self.descendant_name.clone())
            }
            FilterField::NameFrom => CursorValue::String(self.ancestor_name.clone()),
            FilterField::Description | FilterField::DescriptionTo => {
                CursorValue::String(self.descendant_description.clone())
            }
            FilterField::DescriptionFrom => CursorValue::String(self.ancestor_description.clone()),
            FilterField::Namespaces | FilterField::NamespaceId | FilterField::NamespacesTo => {
                CursorValue::Integer(self.descendant_namespace_id as i64)
            }
            FilterField::NamespacesFrom => CursorValue::Integer(self.ancestor_namespace_id as i64),
            FilterField::ClassId | FilterField::Classes | FilterField::ClassTo => {
                CursorValue::Integer(self.descendant_class_id as i64)
            }
            FilterField::ClassFrom => CursorValue::Integer(self.ancestor_class_id as i64),
            FilterField::CreatedAt | FilterField::CreatedAtTo => {
                CursorValue::DateTime(self.descendant_created_at)
            }
            FilterField::CreatedAtFrom => CursorValue::DateTime(self.ancestor_created_at),
            FilterField::UpdatedAt | FilterField::UpdatedAtTo => {
                CursorValue::DateTime(self.descendant_updated_at)
            }
            FilterField::UpdatedAtFrom => CursorValue::DateTime(self.ancestor_updated_at),
            FilterField::Depth => CursorValue::Integer(self.depth as i64),
            FilterField::Path => CursorValue::IntegerArray(self.path.clone()),
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{}' is not orderable for related objects",
                    field
                )));
            }
        })
    }

    fn default_sort() -> Vec<SortParam> {
        vec![
            SortParam {
                field: FilterField::Path,
                descending: false,
            },
            SortParam {
                field: FilterField::Id,
                descending: false,
            },
        ]
    }

    fn tie_breaker_sort() -> Vec<SortParam> {
        Self::default_sort()
    }
}

impl CursorSqlMapping for ObjectClosureView {
    fn sql_field(field: &FilterField) -> Result<CursorSqlField, ApiError> {
        Ok(match field {
            FilterField::Id | FilterField::ObjectTo => CursorSqlField {
                column: "object_closure_view.descendant_object_id",
                sql_type: CursorSqlType::Integer,
                nullable: false,
            },
            FilterField::ObjectFrom => CursorSqlField {
                column: "object_closure_view.ancestor_object_id",
                sql_type: CursorSqlType::Integer,
                nullable: false,
            },
            FilterField::Name | FilterField::NameTo => CursorSqlField {
                column: "object_closure_view.descendant_name",
                sql_type: CursorSqlType::String,
                nullable: false,
            },
            FilterField::NameFrom => CursorSqlField {
                column: "object_closure_view.ancestor_name",
                sql_type: CursorSqlType::String,
                nullable: false,
            },
            FilterField::Description | FilterField::DescriptionTo => CursorSqlField {
                column: "object_closure_view.descendant_description",
                sql_type: CursorSqlType::String,
                nullable: false,
            },
            FilterField::DescriptionFrom => CursorSqlField {
                column: "object_closure_view.ancestor_description",
                sql_type: CursorSqlType::String,
                nullable: false,
            },
            FilterField::Namespaces | FilterField::NamespaceId | FilterField::NamespacesTo => {
                CursorSqlField {
                    column: "object_closure_view.descendant_namespace_id",
                    sql_type: CursorSqlType::Integer,
                    nullable: false,
                }
            }
            FilterField::NamespacesFrom => CursorSqlField {
                column: "object_closure_view.ancestor_namespace_id",
                sql_type: CursorSqlType::Integer,
                nullable: false,
            },
            FilterField::ClassId | FilterField::Classes | FilterField::ClassTo => CursorSqlField {
                column: "object_closure_view.descendant_class_id",
                sql_type: CursorSqlType::Integer,
                nullable: false,
            },
            FilterField::ClassFrom => CursorSqlField {
                column: "object_closure_view.ancestor_class_id",
                sql_type: CursorSqlType::Integer,
                nullable: false,
            },
            FilterField::CreatedAt | FilterField::CreatedAtTo => CursorSqlField {
                column: "object_closure_view.descendant_created_at",
                sql_type: CursorSqlType::DateTime,
                nullable: false,
            },
            FilterField::CreatedAtFrom => CursorSqlField {
                column: "object_closure_view.ancestor_created_at",
                sql_type: CursorSqlType::DateTime,
                nullable: false,
            },
            FilterField::UpdatedAt | FilterField::UpdatedAtTo => CursorSqlField {
                column: "object_closure_view.descendant_updated_at",
                sql_type: CursorSqlType::DateTime,
                nullable: false,
            },
            FilterField::UpdatedAtFrom => CursorSqlField {
                column: "object_closure_view.ancestor_updated_at",
                sql_type: CursorSqlType::DateTime,
                nullable: false,
            },
            FilterField::Depth => CursorSqlField {
                column: "object_closure_view.depth",
                sql_type: CursorSqlType::Integer,
                nullable: false,
            },
            FilterField::Path => CursorSqlField {
                column: "object_closure_view.path",
                sql_type: CursorSqlType::IntegerArray,
                nullable: false,
            },
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{}' is not orderable for related objects",
                    field
                )));
            }
        })
    }
}
