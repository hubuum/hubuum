use std::collections::HashMap;
use tracing::warn;

use crate::errors::ApiError;
use crate::models::group::Group;
use crate::models::search::{FilterField, SortParam};
use crate::models::{
    Collection, CollectionID, GroupPermission, HubuumClass, HubuumClassExpanded, Permission,
};
use crate::traits::{
    BackendContext, CursorPaginated, CursorSqlField, CursorSqlMapping, CursorSqlType, CursorValue,
    SelfAccessors,
};

/// Convert a `(Group, T)` tuple into a richer output type.
pub trait FromTuple<T> {
    fn from_tuple(t: (Group, T)) -> Self;
}

/// Expand a value by loading its collection from the backend.
///
/// Use this when the caller has a backend context available and wants a fully expanded output
/// value rather than an ID-only representation.
pub trait ExpandCollection<T> {
    async fn expand_collection<C>(&self, backend: &C) -> Result<T, ApiError>
    where
        C: BackendContext + ?Sized;
}

impl ExpandCollection<HubuumClassExpanded> for HubuumClass {
    async fn expand_collection<C>(&self, backend: &C) -> Result<HubuumClassExpanded, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        let collection = CollectionID::new(self.collection_id)?
            .instance(backend)
            .await?;

        Ok(HubuumClassExpanded {
            id: self.id,
            name: self.name.clone(),
            collection,
            json_schema: self.json_schema.clone(),
            validate_schema: self.validate_schema,
            description: self.description.clone(),
            created_at: self.created_at,
            updated_at: self.updated_at,
        })
    }
}

/// Expand a value by looking up collections in a precomputed map rather than hitting the backend.
pub trait ExpandCollectionFromMap<T> {
    fn expand_collection_from_map(&self, collection_map: &HashMap<i32, Collection>) -> T;
}

impl FromTuple<Permission> for GroupPermission {
    fn from_tuple(t: (Group, Permission)) -> Self {
        GroupPermission {
            group: t.0,
            permission: t.1,
        }
    }
}

impl ExpandCollectionFromMap<Vec<HubuumClassExpanded>> for Vec<HubuumClass> {
    fn expand_collection_from_map(
        &self,
        collection_map: &HashMap<i32, Collection>,
    ) -> Vec<HubuumClassExpanded> {
        self.iter()
            .map(|class| class.expand_collection_from_map(collection_map))
            .collect()
    }
}

impl ExpandCollectionFromMap<HubuumClassExpanded> for HubuumClass {
    fn expand_collection_from_map(
        &self,
        collection_map: &HashMap<i32, Collection>,
    ) -> HubuumClassExpanded {
        let collection = match collection_map.get(&self.collection_id) {
            Some(collection) => collection.clone(),
            None => {
                warn!(
                    message = "Collection mapping failed",
                    id = self.collection_id,
                    class = self.name,
                    class_id = self.id
                );
                Collection {
                    id: self.collection_id,
                    name: "Unknown".to_string(),
                    description: "Unknown".to_string(),
                    created_at: chrono::NaiveDateTime::default(),
                    updated_at: chrono::NaiveDateTime::default(),
                    parent_collection_id: None,
                }
            }
        };

        HubuumClassExpanded {
            id: self.id,
            name: self.name.clone(),
            collection,
            json_schema: self.json_schema.clone(),
            validate_schema: self.validate_schema,
            description: self.description.clone(),
            created_at: self.created_at,
            updated_at: self.updated_at,
        }
    }
}

impl CursorPaginated for HubuumClassExpanded {
    fn supports_sort(field: &FilterField) -> bool {
        matches!(
            field,
            FilterField::Id
                | FilterField::Name
                | FilterField::Description
                | FilterField::Collections
                | FilterField::CollectionId
                | FilterField::CreatedAt
                | FilterField::UpdatedAt
        )
    }

    fn cursor_value(&self, field: &FilterField) -> Result<CursorValue, ApiError> {
        Ok(match field {
            FilterField::Id => CursorValue::Integer(self.id as i64),
            FilterField::Name => CursorValue::String(self.name.clone()),
            FilterField::Description => CursorValue::String(self.description.clone()),
            FilterField::Collections | FilterField::CollectionId => {
                CursorValue::Integer(self.collection.id as i64)
            }
            FilterField::CreatedAt => CursorValue::DateTime(self.created_at),
            FilterField::UpdatedAt => CursorValue::DateTime(self.updated_at),
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{}' is not orderable for classes",
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

impl CursorSqlMapping for HubuumClassExpanded {
    fn sql_field(field: &FilterField) -> Result<CursorSqlField, ApiError> {
        Ok(match field {
            FilterField::Id => CursorSqlField {
                column: "hubuumclass.id",
                sql_type: CursorSqlType::Integer,
                nullable: false,
            },
            FilterField::Name => CursorSqlField {
                column: "hubuumclass.name",
                sql_type: CursorSqlType::String,
                nullable: false,
            },
            FilterField::Description => CursorSqlField {
                column: "hubuumclass.description",
                sql_type: CursorSqlType::String,
                nullable: false,
            },
            FilterField::Collections | FilterField::CollectionId => CursorSqlField {
                column: "hubuumclass.collection_id",
                sql_type: CursorSqlType::Integer,
                nullable: false,
            },
            FilterField::CreatedAt => CursorSqlField {
                column: "hubuumclass.created_at",
                sql_type: CursorSqlType::DateTime,
                nullable: false,
            },
            FilterField::UpdatedAt => CursorSqlField {
                column: "hubuumclass.updated_at",
                sql_type: CursorSqlType::DateTime,
                nullable: false,
            },
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{}' is not orderable for classes",
                    field
                )));
            }
        })
    }
}

impl CursorPaginated for GroupPermission {
    fn supports_sort(field: &FilterField) -> bool {
        matches!(
            field,
            FilterField::Id
                | FilterField::Name
                | FilterField::Groupname
                | FilterField::CreatedAt
                | FilterField::UpdatedAt
        )
    }

    fn cursor_value(&self, field: &FilterField) -> Result<CursorValue, ApiError> {
        Ok(match field {
            FilterField::Id => CursorValue::Integer(self.permission.id as i64),
            FilterField::Name | FilterField::Groupname => {
                CursorValue::String(self.group.groupname.clone())
            }
            FilterField::CreatedAt => CursorValue::DateTime(self.permission.created_at),
            FilterField::UpdatedAt => CursorValue::DateTime(self.permission.updated_at),
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{}' is not orderable for group permissions",
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

impl CursorSqlMapping for GroupPermission {
    fn sql_field(field: &FilterField) -> Result<CursorSqlField, ApiError> {
        Ok(match field {
            FilterField::Id => CursorSqlField {
                column: "permissions.id",
                sql_type: CursorSqlType::Integer,
                nullable: false,
            },
            FilterField::Name | FilterField::Groupname => CursorSqlField {
                column: "groups.groupname",
                sql_type: CursorSqlType::String,
                nullable: false,
            },
            FilterField::CreatedAt => CursorSqlField {
                column: "permissions.created_at",
                sql_type: CursorSqlType::DateTime,
                nullable: false,
            },
            FilterField::UpdatedAt => CursorSqlField {
                column: "permissions.updated_at",
                sql_type: CursorSqlType::DateTime,
                nullable: false,
            },
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{}' is not orderable for group permissions",
                    field
                )));
            }
        })
    }
}
