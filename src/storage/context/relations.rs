use super::*;

#[async_trait]
impl ObjectAggregateStorage for StorageHandle {
    async fn aggregate_objects(
        &self,
        query: ObjectAggregateStorageQuery,
        authorization: ObjectAggregateAuthorization<'_>,
    ) -> Result<StorageObjectAggregatePage, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::ObjectAggregate,
            "aggregate_objects",
            async {
                dispatch_backend!(self, |backend| {
                    backend.aggregate_objects(query, authorization).await
                })
            },
        )
        .await
    }
}

#[async_trait]
impl RelationQueryStorage for StorageHandle {
    async fn list_class_relations(
        &self,
        query: RelationListQuery,
    ) -> Result<StoragePage<StorageClassRelation>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::RelationQuery,
            "list_class_relations",
            async {
                dispatch_backend!(self, |backend| {
                    backend.list_class_relations(query).await
                })
            },
        )
        .await
    }

    async fn list_object_relations(
        &self,
        query: RelationListQuery,
    ) -> Result<StoragePage<StorageObjectRelation>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::RelationQuery,
            "list_object_relations",
            async {
                dispatch_backend!(self, |backend| {
                    backend.list_object_relations(query).await
                })
            },
        )
        .await
    }

    async fn list_class_relations_touching(
        &self,
        query: RelationTouchingQuery,
    ) -> Result<StoragePage<StorageClassRelation>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::RelationQuery,
            "list_class_relations_touching",
            async {
                dispatch_backend!(self, |backend| {
                    backend.list_class_relations_touching(query).await
                })
            },
        )
        .await
    }

    async fn list_object_relations_touching(
        &self,
        query: RelationTouchingQuery,
    ) -> Result<StoragePage<StorageObjectRelation>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::RelationQuery,
            "list_object_relations_touching",
            async {
                dispatch_backend!(self, |backend| {
                    backend.list_object_relations_touching(query).await
                })
            },
        )
        .await
    }

    async fn list_class_relations_touching_ids(
        &self,
        query: RelationIdsQuery,
    ) -> Result<Vec<StorageClassRelation>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::RelationQuery,
            "list_class_relations_touching_ids",
            async {
                dispatch_backend!(self, |backend| {
                    backend.list_class_relations_touching_ids(query).await
                })
            },
        )
        .await
    }

    async fn list_class_relations_between_ids(
        &self,
        query: RelationIdsQuery,
    ) -> Result<Vec<StorageClassRelation>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::RelationQuery,
            "list_class_relations_between_ids",
            async {
                dispatch_backend!(self, |backend| {
                    backend.list_class_relations_between_ids(query).await
                })
            },
        )
        .await
    }

    async fn list_object_relations_between_ids(
        &self,
        query: RelationIdsQuery,
    ) -> Result<Vec<StorageObjectRelation>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::RelationQuery,
            "list_object_relations_between_ids",
            async {
                dispatch_backend!(self, |backend| {
                    backend.list_object_relations_between_ids(query).await
                })
            },
        )
        .await
    }

    async fn list_object_relations_touching_ids(
        &self,
        query: ObjectRelationsTouchingIdsQuery,
    ) -> Result<Vec<StorageObjectRelation>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::RelationQuery,
            "list_object_relations_touching_ids",
            async {
                dispatch_backend!(self, |backend| {
                    backend.list_object_relations_touching_ids(query).await
                })
            },
        )
        .await
    }

    async fn list_related_classes(
        &self,
        query: RelationGraphQuery,
    ) -> Result<StoragePage<StorageClassGraphRow>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::RelationQuery,
            "list_related_classes",
            async { dispatch_backend!(self, |backend| backend.list_related_classes(query).await) },
        )
        .await
    }

    async fn list_related_objects(
        &self,
        query: RelationGraphQuery,
    ) -> Result<StoragePage<StorageObjectGraphRow>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::RelationQuery,
            "list_related_objects",
            async { dispatch_backend!(self, |backend| backend.list_related_objects(query).await) },
        )
        .await
    }

    async fn list_related_objects_for_roots(
        &self,
        query: RelatedObjectsForRootsQuery,
    ) -> Result<Vec<StorageRelatedObjectIncludeRow>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::RelationQuery,
            "list_related_objects_for_roots",
            async {
                dispatch_backend!(self, |backend| {
                    backend.list_related_objects_for_roots(query).await
                })
            },
        )
        .await
    }

    async fn list_bidirectionally_related_objects_for_roots(
        &self,
        query: BidirectionalRelatedObjectsQuery,
    ) -> Result<Vec<StorageRelatedObjectForRootRow>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::RelationQuery,
            "list_bidirectionally_related_objects_for_roots",
            async {
                dispatch_backend!(self, |backend| {
                    backend
                        .list_bidirectionally_related_objects_for_roots(query)
                        .await
                })
            },
        )
        .await
    }
}

#[async_trait]
impl UnifiedSearchStorage for StorageHandle {
    async fn search_collections(
        &self,
        query: UnifiedSearchQuery,
    ) -> Result<Vec<StorageCollection>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::UnifiedSearch,
            "search_collections",
            async {
                dispatch_backend!(self, |backend| { backend.search_collections(query).await })
            },
        )
        .await
    }

    async fn search_classes(
        &self,
        query: UnifiedSearchQuery,
    ) -> Result<Vec<StorageClass>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::UnifiedSearch,
            "search_classes",
            async { dispatch_backend!(self, |backend| { backend.search_classes(query).await }) },
        )
        .await
    }

    async fn search_objects(
        &self,
        query: UnifiedSearchQuery,
    ) -> Result<Vec<StorageObject>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::UnifiedSearch,
            "search_objects",
            async { dispatch_backend!(self, |backend| { backend.search_objects(query).await }) },
        )
        .await
    }
}
