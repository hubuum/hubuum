use super::*;

#[async_trait]
impl ObjectAggregateStorage for StorageHandle {
    async fn aggregate_objects(
        &self,
        query: ObjectAggregateStorageQuery,
        authorizer: Option<&dyn ObjectAggregateAuthorizer>,
    ) -> Result<StorageObjectAggregatePage, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "object_aggregates",
            "aggregate",
            async {
                dispatch_backend!(self, |backend| {
                    backend.aggregate_objects(query, authorizer).await
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
    ) -> Result<RelationPage<StorageClassRelation>, StorageError> {
        observe_storage_call(self.backend_name(), "relations", "list_classes", async {
            dispatch_backend!(self, |backend| {
                backend.list_class_relations(query).await
            })
        })
        .await
    }

    async fn list_object_relations(
        &self,
        query: RelationListQuery,
    ) -> Result<RelationPage<StorageObjectRelation>, StorageError> {
        observe_storage_call(self.backend_name(), "relations", "list_objects", async {
            dispatch_backend!(self, |backend| {
                backend.list_object_relations(query).await
            })
        })
        .await
    }

    async fn list_class_relations_touching(
        &self,
        query: RelationTouchingQuery,
    ) -> Result<RelationPage<StorageClassRelation>, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "relations",
            "classes_touching",
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
    ) -> Result<RelationPage<StorageObjectRelation>, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "relations",
            "objects_touching",
            async {
                dispatch_backend!(self, |backend| {
                    backend.list_object_relations_touching(query).await
                })
            },
        )
        .await
    }

    async fn class_relations_touching_ids(
        &self,
        query: RelationIdsQuery,
    ) -> Result<Vec<StorageClassRelation>, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "relations",
            "classes_touching_ids",
            async {
                dispatch_backend!(self, |backend| {
                    backend.class_relations_touching_ids(query).await
                })
            },
        )
        .await
    }

    async fn class_relations_between_ids(
        &self,
        query: RelationIdsQuery,
    ) -> Result<Vec<StorageClassRelation>, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "relations",
            "classes_between_ids",
            async {
                dispatch_backend!(self, |backend| {
                    backend.class_relations_between_ids(query).await
                })
            },
        )
        .await
    }

    async fn object_relations_between_ids(
        &self,
        query: RelationIdsQuery,
    ) -> Result<Vec<StorageObjectRelation>, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "relations",
            "objects_between_ids",
            async {
                dispatch_backend!(self, |backend| {
                    backend.object_relations_between_ids(query).await
                })
            },
        )
        .await
    }

    async fn object_relations_touching_ids(
        &self,
        query: ObjectRelationsTouchingIdsQuery,
    ) -> Result<Vec<StorageObjectRelation>, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "relations",
            "objects_touching_ids",
            async {
                dispatch_backend!(self, |backend| {
                    backend.object_relations_touching_ids(query).await
                })
            },
        )
        .await
    }

    async fn related_classes(
        &self,
        query: RelationGraphQuery,
    ) -> Result<RelationPage<StorageClassGraphRow>, StorageError> {
        observe_storage_call(self.backend_name(), "relations", "related_classes", async {
            dispatch_backend!(self, |backend| backend.related_classes(query).await)
        })
        .await
    }

    async fn related_objects(
        &self,
        query: RelationGraphQuery,
    ) -> Result<RelationPage<StorageObjectGraphRow>, StorageError> {
        observe_storage_call(self.backend_name(), "relations", "related_objects", async {
            dispatch_backend!(self, |backend| backend.related_objects(query).await)
        })
        .await
    }

    async fn related_objects_for_roots(
        &self,
        query: RelatedObjectsForRootsQuery,
    ) -> Result<Vec<StorageRelatedObjectIncludeRow>, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "relations",
            "related_objects_for_roots",
            async {
                dispatch_backend!(self, |backend| {
                    backend.related_objects_for_roots(query).await
                })
            },
        )
        .await
    }

    async fn bidirectionally_related_objects_for_roots(
        &self,
        query: BidirectionalRelatedObjectsQuery,
    ) -> Result<Vec<StorageRelatedObjectForRootRow>, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "relations",
            "bidirectional_objects_for_roots",
            async {
                dispatch_backend!(self, |backend| {
                    backend
                        .bidirectionally_related_objects_for_roots(query)
                        .await
                })
            },
        )
        .await
    }
}

#[async_trait]
impl UnifiedSearchStorage for StorageHandle {
    async fn search_unified_collections(
        &self,
        query: UnifiedSearchQuery,
    ) -> Result<Vec<UnifiedSearchCollection>, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "unified_search",
            "collections",
            async {
                dispatch_backend!(self, |backend| {
                    backend.search_unified_collections(query).await
                })
            },
        )
        .await
    }

    async fn search_unified_classes(
        &self,
        query: UnifiedSearchQuery,
    ) -> Result<Vec<UnifiedSearchClass>, StorageError> {
        observe_storage_call(self.backend_name(), "unified_search", "classes", async {
            dispatch_backend!(self, |backend| {
                backend.search_unified_classes(query).await
            })
        })
        .await
    }

    async fn search_unified_objects(
        &self,
        query: UnifiedSearchQuery,
    ) -> Result<Vec<UnifiedSearchObject>, StorageError> {
        observe_storage_call(self.backend_name(), "unified_search", "objects", async {
            dispatch_backend!(self, |backend| {
                backend.search_unified_objects(query).await
            })
        })
        .await
    }
}
