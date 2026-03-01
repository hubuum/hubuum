use crate::db::traits::namespace::{
    DeleteNamespaceRecord, SaveNamespaceForGroupRecord, SaveNamespaceWithAssigneeRecord,
    UpdateNamespaceRecord,
};
use crate::db::DbPool;
use crate::errors::ApiError;
use crate::models::group::GroupID;
use crate::models::namespace::{
    Namespace, NamespaceID, NewNamespace, NewNamespaceWithAssignee, UpdateNamespace,
};
use crate::models::permissions::{NewPermission, Permission, Permissions, PermissionsList};
use crate::models::search::{FilterField, SortParam};
use crate::models::traits::GroupAccessors;
use crate::models::user::User;
use crate::traits::{
    CanUpdate, CursorPaginated, CursorSqlField, CursorSqlMapping, CursorSqlType,
    NamespaceAccessors, PermissionController, SelfAccessors,
};
use crate::traits::crud::{DeleteAdapter, SaveAdapter, UpdateAdapter};
use diesel::prelude::*;
use tracing::debug;

impl SaveAdapter for Namespace {
    type Output = Namespace;

    async fn save_adapter(&self, pool: &DbPool) -> Result<Self::Output, ApiError> {
        let updated_namespace = UpdateNamespace {
            name: Some(self.name.clone()),
            description: Some(self.description.clone()),
        };
        updated_namespace.update(pool, self.id).await
    }
}

impl DeleteAdapter for Namespace {
    async fn delete_adapter(&self, pool: &DbPool) -> Result<(), ApiError> {
        self.delete_namespace_record(pool).await
    }
}

impl DeleteAdapter for NamespaceID {
    async fn delete_adapter(&self, pool: &DbPool) -> Result<(), ApiError> {
        self.delete_namespace_record(pool).await
    }
}

impl UpdateAdapter for UpdateNamespace {
    type Output = Namespace;

    async fn update_adapter(&self, pool: &DbPool, nid: i32) -> Result<Self::Output, ApiError> {
        self.update_namespace_record(pool, nid).await
    }
}

impl SaveAdapter for NewNamespaceWithAssignee {
    type Output = Namespace;

    async fn save_adapter(&self, pool: &DbPool) -> Result<Namespace, ApiError> {
        self.save_namespace_with_assignee_record(pool).await
    }
}

impl SelfAccessors<Namespace> for Namespace {
    fn id(&self) -> i32 {
        self.id
    }

    async fn instance(&self, _pool: &DbPool) -> Result<Namespace, ApiError> {
        Ok(self.clone())
    }
}

impl NamespaceAccessors for Namespace {
    async fn namespace(&self, _pool: &DbPool) -> Result<Namespace, ApiError> {
        Ok(self.clone())
    }

    async fn namespace_id(&self, _pool: &DbPool) -> Result<i32, ApiError> {
        Ok(self.id)
    }
}

impl NamespaceAccessors for &Namespace {
    async fn namespace(&self, _pool: &DbPool) -> Result<Namespace, ApiError> {
        Ok((**self).clone())
    }

    async fn namespace_id(&self, _pool: &DbPool) -> Result<i32, ApiError> {
        Ok(self.id)
    }
}

impl SelfAccessors<Namespace> for NamespaceID {
    fn id(&self) -> i32 {
        self.0
    }

    async fn instance(&self, pool: &DbPool) -> Result<Namespace, ApiError> {
        self.namespace(pool).await
    }
}

impl NamespaceAccessors for NamespaceID {
    async fn namespace_id(&self, _pool: &DbPool) -> Result<i32, ApiError> {
        Ok(self.0)
    }

    async fn namespace(&self, pool: &DbPool) -> Result<Namespace, ApiError> {
        use crate::db::traits::GetNamespace;
        self.namespace_from_backend(pool).await
    }
}

impl NamespaceAccessors for &NamespaceID {
    async fn namespace(&self, pool: &DbPool) -> Result<Namespace, ApiError> {
        self.instance(pool).await
    }

    async fn namespace_id(&self, _pool: &DbPool) -> Result<i32, ApiError> {
        Ok(self.0)
    }
}

impl NewNamespace {
    pub async fn save_and_grant_all_to(
        self,
        pool: &DbPool,
        assignee: GroupID,
    ) -> Result<Namespace, ApiError> {
        self.save_namespace_for_group_record(pool, assignee.0).await
    }

    pub async fn update_with_permissions(
        self,
        pool: &DbPool,
        ns_with_assignee: NewNamespaceWithAssignee,
    ) -> Result<Namespace, ApiError> {
        self.save_namespace_for_group_record(pool, ns_with_assignee.group_id)
            .await
    }
}

impl PermissionController for Namespace {}
impl PermissionController for NamespaceID {}

impl CursorPaginated for Namespace {
    fn supports_sort(field: &FilterField) -> bool {
        matches!(
            field,
            FilterField::Id | FilterField::Name | FilterField::CreatedAt | FilterField::UpdatedAt
        )
    }

    fn cursor_value(&self, field: &FilterField) -> Result<crate::traits::CursorValue, ApiError> {
        Ok(match field {
            FilterField::Id => crate::traits::CursorValue::Integer(self.id as i64),
            FilterField::Name => crate::traits::CursorValue::String(self.name.clone()),
            FilterField::CreatedAt => crate::traits::CursorValue::DateTime(self.created_at),
            FilterField::UpdatedAt => crate::traits::CursorValue::DateTime(self.updated_at),
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{}' is not orderable for namespaces",
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

impl CursorSqlMapping for Namespace {
    fn sql_field(field: &FilterField) -> Result<CursorSqlField, ApiError> {
        Ok(match field {
            FilterField::Id => CursorSqlField {
                column: "namespaces.id",
                sql_type: CursorSqlType::Integer,
                nullable: false,
            },
            FilterField::Name => CursorSqlField {
                column: "namespaces.name",
                sql_type: CursorSqlType::String,
                nullable: false,
            },
            FilterField::CreatedAt => CursorSqlField {
                column: "namespaces.created_at",
                sql_type: CursorSqlType::DateTime,
                nullable: false,
            },
            FilterField::UpdatedAt => CursorSqlField {
                column: "namespaces.updated_at",
                sql_type: CursorSqlType::DateTime,
                nullable: false,
            },
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{}' is not orderable for namespaces",
                    field
                )));
            }
        })
    }
}
