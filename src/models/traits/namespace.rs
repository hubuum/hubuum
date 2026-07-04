use crate::db::DbPool;
use crate::db::traits::namespace::{
    DeleteNamespaceRecord, SaveNamespaceForGroupRecord, SaveNamespaceWithAssigneeRecord,
    UpdateNamespaceRecord,
};
use crate::errors::ApiError;
use crate::events::EventContext;
use crate::models::group::GroupID;
use crate::models::namespace::{
    Namespace, NamespaceID, NewNamespace, NewNamespaceWithAssignee, UpdateNamespace,
};
use crate::models::search::{FilterField, SortParam};
use crate::traits::accessors::{IdAccessor, InstanceAdapter, NamespaceAdapter};
use crate::traits::crud::{DeleteAdapter, SaveAdapter, UpdateAdapter};
use crate::traits::{
    BackendContext, CanUpdate, CursorPaginated, CursorSqlField, CursorSqlMapping, CursorSqlType,
    NamespaceAccessors, PermissionController,
};

impl SaveAdapter for Namespace {
    type Output = Namespace;

    async fn save_adapter_without_events(&self, pool: &DbPool) -> Result<Self::Output, ApiError> {
        let updated_namespace = UpdateNamespace {
            name: Some(self.name.clone()),
            description: Some(self.description.clone()),
        };
        updated_namespace.update_without_events(pool, self.id).await
    }

    async fn save_adapter(
        &self,
        pool: &DbPool,
        context: &EventContext,
    ) -> Result<Self::Output, ApiError> {
        let updated_namespace = UpdateNamespace {
            name: Some(self.name.clone()),
            description: Some(self.description.clone()),
        };
        updated_namespace
            .update_namespace_record(pool, self.id, Some(context))
            .await
    }
}

impl DeleteAdapter for Namespace {
    async fn delete_adapter_without_events(&self, pool: &DbPool) -> Result<(), ApiError> {
        self.delete_namespace_record_without_events(pool).await
    }

    async fn delete_adapter(&self, pool: &DbPool, context: &EventContext) -> Result<(), ApiError> {
        self.delete_namespace_record(pool, Some(context)).await
    }
}

impl DeleteAdapter for NamespaceID {
    async fn delete_adapter_without_events(&self, pool: &DbPool) -> Result<(), ApiError> {
        self.delete_namespace_record_without_events(pool).await
    }

    async fn delete_adapter(&self, pool: &DbPool, context: &EventContext) -> Result<(), ApiError> {
        self.delete_namespace_record(pool, Some(context)).await
    }
}

impl UpdateAdapter for UpdateNamespace {
    type Output = Namespace;

    async fn update_adapter_without_events(
        &self,
        pool: &DbPool,
        nid: i32,
    ) -> Result<Self::Output, ApiError> {
        self.update_namespace_record_without_events(pool, nid).await
    }

    async fn update_adapter(
        &self,
        pool: &DbPool,
        nid: i32,
        context: &EventContext,
    ) -> Result<Self::Output, ApiError> {
        self.update_namespace_record(pool, nid, Some(context)).await
    }
}

impl SaveAdapter for NewNamespaceWithAssignee {
    type Output = Namespace;

    async fn save_adapter_without_events(&self, pool: &DbPool) -> Result<Namespace, ApiError> {
        self.save_namespace_with_assignee_record_without_events(pool)
            .await
    }

    async fn save_adapter(
        &self,
        pool: &DbPool,
        context: &EventContext,
    ) -> Result<Namespace, ApiError> {
        self.save_namespace_with_assignee_record(pool, Some(context))
            .await
    }
}

impl IdAccessor for Namespace {
    fn accessor_id(&self) -> i32 {
        self.id
    }
}

impl InstanceAdapter<Namespace> for Namespace {
    async fn instance_adapter(&self, _pool: &DbPool) -> Result<Namespace, ApiError> {
        Ok(self.clone())
    }
}

impl NamespaceAdapter for Namespace {
    async fn namespace_adapter(&self, _pool: &DbPool) -> Result<Namespace, ApiError> {
        Ok(self.clone())
    }

    async fn namespace_id_adapter(&self, _pool: &DbPool) -> Result<NamespaceID, ApiError> {
        NamespaceID::new(self.id)
    }
}

impl IdAccessor for NamespaceID {
    fn accessor_id(&self) -> i32 {
        // Deref to the owned (Copy) value on purpose: with a `&self` receiver, `self.id()`
        // binds to the `SelfAccessors::id` trait method, which calls back into `accessor_id`
        // and recurses. The inherent `id` is only selected on an owned receiver.
        (*self).id()
    }
}

impl InstanceAdapter<Namespace> for NamespaceID {
    async fn instance_adapter(&self, pool: &DbPool) -> Result<Namespace, ApiError> {
        self.namespace(pool).await
    }
}

impl NamespaceAdapter for NamespaceID {
    async fn namespace_id_adapter(&self, _pool: &DbPool) -> Result<NamespaceID, ApiError> {
        Ok(*self)
    }

    async fn namespace_adapter(&self, pool: &DbPool) -> Result<Namespace, ApiError> {
        use crate::db::traits::GetNamespace;
        self.namespace_from_backend(pool).await
    }
}

impl NewNamespace {
    /// Create a namespace and grant the full namespace permission set to the assignee group.
    ///
    /// This is a convenience wrapper around the backend transaction that creates the namespace
    /// record and the corresponding permission record together.
    pub async fn save_and_grant_all_to<C>(
        self,
        backend: &C,
        assignee: GroupID,
    ) -> Result<Namespace, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        self.save_namespace_for_group_record_without_events(backend.db_pool(), assignee.id())
            .await
    }

    /// Persist the namespace and apply permissions using the assignee embedded in the supplied
    /// `NewNamespaceWithAssignee`.
    ///
    /// This delegates into the same backend helper as [`Self::save_and_grant_all_to`], but takes
    /// the assignee from the provided wrapper value.
    pub async fn update_with_permissions<C>(
        self,
        backend: &C,
        ns_with_assignee: NewNamespaceWithAssignee,
    ) -> Result<Namespace, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        self.save_namespace_for_group_record_without_events(
            backend.db_pool(),
            ns_with_assignee.group_id,
        )
        .await
    }
}

impl PermissionController for Namespace {}
impl PermissionController for NamespaceID {}

impl CursorPaginated for Namespace {
    fn supports_sort(field: &FilterField) -> bool {
        matches!(
            field,
            FilterField::Id
                | FilterField::Name
                | FilterField::Description
                | FilterField::CreatedAt
                | FilterField::UpdatedAt
        )
    }

    fn cursor_value(&self, field: &FilterField) -> Result<crate::traits::CursorValue, ApiError> {
        Ok(match field {
            FilterField::Id => crate::traits::CursorValue::Integer(self.id as i64),
            FilterField::Name => crate::traits::CursorValue::String(self.name.clone()),
            FilterField::Description => {
                crate::traits::CursorValue::String(self.description.clone())
            }
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
            FilterField::Description => CursorSqlField {
                column: "namespaces.description",
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
