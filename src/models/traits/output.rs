use std::collections::HashMap;
use tracing::warn;

use crate::db::DbPool;
use crate::errors::ApiError;
use crate::models::group::Group;
use crate::models::search::{FilterField, SortParam};
use crate::models::{
    GroupPermission, HubuumClass, HubuumClassExpanded, Namespace, NamespaceID, Permission,
    Permissions, PermissionsList,
};
use crate::traits::{
    CursorPaginated, CursorSqlField, CursorSqlMapping, CursorSqlType, CursorValue, SelfAccessors,
};

pub trait FromTuple<T> {
    fn from_tuple(t: (Group, T)) -> Self;
}

pub trait ExpandNamespace<T> {
    async fn expand_namespace(&self, pool: &crate::db::DbPool) -> Result<T, ApiError>;
}

impl ExpandNamespace<HubuumClassExpanded> for HubuumClass {
    async fn expand_namespace(&self, pool: &DbPool) -> Result<HubuumClassExpanded, ApiError> {
        let namespace = NamespaceID(self.namespace_id).instance(pool).await?;

        Ok(HubuumClassExpanded {
            id: self.id,
            name: self.name.clone(),
            namespace,
            json_schema: self.json_schema.clone(),
            validate_schema: self.validate_schema,
            description: self.description.clone(),
            created_at: self.created_at,
            updated_at: self.updated_at,
        })
    }
}

pub trait ExpandNamespaceFromMap<T> {
    fn expand_namespace_from_map(&self, namespace_map: &HashMap<i32, Namespace>) -> T;
}

impl FromTuple<Permission> for GroupPermission {
    fn from_tuple(t: (Group, Permission)) -> Self {
        GroupPermission {
            group: t.0,
            permission: t.1,
        }
    }
}

impl ExpandNamespaceFromMap<Vec<HubuumClassExpanded>> for Vec<HubuumClass> {
    fn expand_namespace_from_map(
        &self,
        namespace_map: &HashMap<i32, Namespace>,
    ) -> Vec<HubuumClassExpanded> {
        self.iter()
            .map(|class| class.expand_namespace_from_map(namespace_map))
            .collect()
    }
}

impl ExpandNamespaceFromMap<HubuumClassExpanded> for HubuumClass {
    fn expand_namespace_from_map(
        &self,
        namespace_map: &HashMap<i32, Namespace>,
    ) -> HubuumClassExpanded {
        let namespace = match namespace_map.get(&self.namespace_id) {
            Some(namespace) => namespace.clone(),
            None => {
                warn!(
                    message = "Namespace mapping failed",
                    id = self.namespace_id,
                    class = self.name,
                    class_id = self.id
                );
                Namespace {
                    id: self.namespace_id,
                    name: "Unknown".to_string(),
                    description: "Unknown".to_string(),
                    created_at: chrono::NaiveDateTime::default(),
                    updated_at: chrono::NaiveDateTime::default(),
                }
            }
        };

        HubuumClassExpanded {
            id: self.id,
            name: self.name.clone(),
            namespace,
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
                | FilterField::Namespaces
                | FilterField::NamespaceId
                | FilterField::CreatedAt
                | FilterField::UpdatedAt
        )
    }

    fn cursor_value(&self, field: &FilterField) -> Result<CursorValue, ApiError> {
        Ok(match field {
            FilterField::Id => CursorValue::Integer(self.id as i64),
            FilterField::Name => CursorValue::String(self.name.clone()),
            FilterField::Namespaces | FilterField::NamespaceId => {
                CursorValue::Integer(self.namespace.id as i64)
            }
            FilterField::CreatedAt => CursorValue::DateTime(self.created_at),
            FilterField::UpdatedAt => CursorValue::DateTime(self.updated_at),
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{}' is not orderable for classes",
                    field
                )))
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
            FilterField::Namespaces | FilterField::NamespaceId => CursorSqlField {
                column: "hubuumclass.namespace_id",
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
                )))
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
                )))
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
                )))
            }
        })
    }
}
