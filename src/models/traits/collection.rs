use crate::db::DbPool;
use crate::db::traits::collection::{
    DeleteCollectionRecord, SaveCollectionForGroupRecord, SaveCollectionWithAssigneeRecord,
    UpdateCollectionRecord,
};
use crate::errors::ApiError;
use crate::events::EventContext;
use crate::models::collection::{
    Collection, CollectionID, NewCollection, NewCollectionWithAssignee, UpdateCollection,
};
use crate::models::group::GroupID;
use crate::models::search::{FilterField, SortParam};
use crate::traits::accessors::{CollectionAdapter, IdAccessor, InstanceAdapter};
use crate::traits::crud::{DeleteAdapter, SaveAdapter, UpdateAdapter};
use crate::traits::{
    BackendContext, CanUpdate, CollectionAccessors, CursorPaginated, CursorSqlField,
    CursorSqlMapping, CursorSqlType, PermissionController,
};

impl SaveAdapter for Collection {
    type Output = Collection;

    async fn save_adapter_without_events(&self, pool: &DbPool) -> Result<Self::Output, ApiError> {
        let updated_collection = UpdateCollection {
            name: Some(self.name.clone()),
            description: Some(self.description.clone()),
        };
        updated_collection
            .update_without_events(pool, self.id)
            .await
    }

    async fn save_adapter(
        &self,
        pool: &DbPool,
        context: &EventContext,
    ) -> Result<Self::Output, ApiError> {
        let updated_collection = UpdateCollection {
            name: Some(self.name.clone()),
            description: Some(self.description.clone()),
        };
        updated_collection
            .update_collection_record(pool, self.id, Some(context))
            .await
    }
}

impl DeleteAdapter for Collection {
    async fn delete_adapter_without_events(&self, pool: &DbPool) -> Result<(), ApiError> {
        self.delete_collection_record_without_events(pool).await
    }

    async fn delete_adapter(&self, pool: &DbPool, context: &EventContext) -> Result<(), ApiError> {
        self.delete_collection_record(pool, Some(context)).await
    }
}

impl DeleteAdapter for CollectionID {
    async fn delete_adapter_without_events(&self, pool: &DbPool) -> Result<(), ApiError> {
        self.delete_collection_record_without_events(pool).await
    }

    async fn delete_adapter(&self, pool: &DbPool, context: &EventContext) -> Result<(), ApiError> {
        self.delete_collection_record(pool, Some(context)).await
    }
}

impl UpdateAdapter for UpdateCollection {
    type Output = Collection;

    async fn update_adapter_without_events(
        &self,
        pool: &DbPool,
        nid: i32,
    ) -> Result<Self::Output, ApiError> {
        self.update_collection_record_without_events(pool, nid)
            .await
    }

    async fn update_adapter(
        &self,
        pool: &DbPool,
        nid: i32,
        context: &EventContext,
    ) -> Result<Self::Output, ApiError> {
        self.update_collection_record(pool, nid, Some(context))
            .await
    }
}

impl SaveAdapter for NewCollectionWithAssignee {
    type Output = Collection;

    async fn save_adapter_without_events(&self, pool: &DbPool) -> Result<Collection, ApiError> {
        self.save_collection_with_assignee_record_without_events(pool)
            .await
    }

    async fn save_adapter(
        &self,
        pool: &DbPool,
        context: &EventContext,
    ) -> Result<Collection, ApiError> {
        self.save_collection_with_assignee_record(pool, Some(context))
            .await
    }
}

impl IdAccessor for Collection {
    fn accessor_id(&self) -> i32 {
        self.id
    }
}

impl InstanceAdapter<Collection> for Collection {
    async fn instance_adapter(&self, _pool: &DbPool) -> Result<Collection, ApiError> {
        Ok(self.clone())
    }
}

impl CollectionAdapter for Collection {
    async fn collection_adapter(&self, _pool: &DbPool) -> Result<Collection, ApiError> {
        Ok(self.clone())
    }

    async fn collection_id_adapter(&self, _pool: &DbPool) -> Result<CollectionID, ApiError> {
        CollectionID::new(self.id)
    }
}

impl IdAccessor for CollectionID {
    fn accessor_id(&self) -> i32 {
        // Deref to the owned (Copy) value on purpose: with a `&self` receiver, `self.id()`
        // binds to the `SelfAccessors::id` trait method, which calls back into `accessor_id`
        // and recurses. The inherent `id` is only selected on an owned receiver.
        (*self).id()
    }
}

impl InstanceAdapter<Collection> for CollectionID {
    async fn instance_adapter(&self, pool: &DbPool) -> Result<Collection, ApiError> {
        self.collection(pool).await
    }
}

impl CollectionAdapter for CollectionID {
    async fn collection_id_adapter(&self, _pool: &DbPool) -> Result<CollectionID, ApiError> {
        Ok(*self)
    }

    async fn collection_adapter(&self, pool: &DbPool) -> Result<Collection, ApiError> {
        use crate::db::traits::GetCollection;
        self.collection_from_backend(pool).await
    }
}

impl NewCollection {
    /// Create a collection and grant the full collection permission set to the assignee group.
    ///
    /// This is a convenience wrapper around the backend transaction that creates the collection
    /// record and the corresponding permission record together.
    pub async fn save_and_grant_all_to<C>(
        self,
        backend: &C,
        assignee: GroupID,
    ) -> Result<Collection, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        self.save_collection_for_group_record_without_events(backend.db_pool(), assignee.id())
            .await
    }

    /// Persist the collection and apply permissions using the assignee embedded in the supplied
    /// `NewCollectionWithAssignee`.
    ///
    /// This delegates into the same backend helper as [`Self::save_and_grant_all_to`], but takes
    /// the assignee from the provided wrapper value.
    pub async fn update_with_permissions<C>(
        self,
        backend: &C,
        ns_with_assignee: NewCollectionWithAssignee,
    ) -> Result<Collection, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        self.save_collection_for_group_record_without_events(
            backend.db_pool(),
            ns_with_assignee.group_id,
        )
        .await
    }
}

impl PermissionController for Collection {}
impl PermissionController for CollectionID {}

impl CursorPaginated for Collection {
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
                    "Field '{}' is not orderable for collections",
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

impl CursorSqlMapping for Collection {
    fn sql_field(field: &FilterField) -> Result<CursorSqlField, ApiError> {
        Ok(match field {
            FilterField::Id => CursorSqlField {
                column: "collections.id",
                sql_type: CursorSqlType::Integer,
                nullable: false,
            },
            FilterField::Name => CursorSqlField {
                column: "collections.name",
                sql_type: CursorSqlType::String,
                nullable: false,
            },
            FilterField::Description => CursorSqlField {
                column: "collections.description",
                sql_type: CursorSqlType::String,
                nullable: false,
            },
            FilterField::CreatedAt => CursorSqlField {
                column: "collections.created_at",
                sql_type: CursorSqlType::DateTime,
                nullable: false,
            },
            FilterField::UpdatedAt => CursorSqlField {
                column: "collections.updated_at",
                sql_type: CursorSqlType::DateTime,
                nullable: false,
            },
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{}' is not orderable for collections",
                    field
                )));
            }
        })
    }
}
