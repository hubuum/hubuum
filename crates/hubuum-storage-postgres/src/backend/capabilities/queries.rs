use super::super::*;

#[async_trait]
impl HistoryStorage for PostgresStorage {
    async fn resolve_history_principal_names(
        &self,
        principal_ids: Vec<PrincipalId>,
    ) -> Result<Vec<StorageHistoryPrincipalName>, StorageError> {
        crate::operations::history::resolve_principal_names(self.runtime(), principal_ids)
            .await
            .map_err(StorageError::from)
    }

    async fn list_collection_history(
        &self,
        query: StorageHistoryListQuery,
    ) -> Result<StoragePage<StorageCollectionHistoryRecord>, StorageError> {
        crate::operations::history::list_collection_history(self.runtime(), query)
            .await
            .map_err(StorageError::from)
    }

    async fn get_collection_history_as_of(
        &self,
        query: StorageHistoryAsOfQuery,
    ) -> Result<Option<StorageCollectionHistoryRecord>, StorageError> {
        crate::operations::history::get_collection_history_as_of(self.runtime(), query)
            .await
            .map_err(StorageError::from)
    }

    async fn list_class_history(
        &self,
        query: StorageHistoryListQuery,
    ) -> Result<StoragePage<StorageClassHistoryRecord>, StorageError> {
        crate::operations::history::list_class_history(self.runtime(), query)
            .await
            .map_err(StorageError::from)
    }

    async fn get_class_history_as_of(
        &self,
        query: StorageHistoryAsOfQuery,
    ) -> Result<Option<StorageClassHistoryRecord>, StorageError> {
        crate::operations::history::get_class_history_as_of(self.runtime(), query)
            .await
            .map_err(StorageError::from)
    }

    async fn list_object_history(
        &self,
        query: StorageObjectHistoryListQuery,
    ) -> Result<StoragePage<StorageObjectHistoryRecord>, StorageError> {
        crate::operations::history::list_object_history(self.runtime(), query)
            .await
            .map_err(StorageError::from)
    }

    async fn get_object_history_as_of(
        &self,
        query: StorageObjectHistoryAsOfQuery,
    ) -> Result<Option<StorageObjectHistoryRecord>, StorageError> {
        crate::operations::history::get_object_history_as_of(self.runtime(), query)
            .await
            .map_err(StorageError::from)
    }

    async fn list_export_template_history(
        &self,
        query: StorageHistoryListQuery,
    ) -> Result<StoragePage<StorageExportTemplateHistoryRecord>, StorageError> {
        crate::operations::history::list_export_template_history(self.runtime(), query)
            .await
            .map_err(StorageError::from)
    }

    async fn get_export_template_history_as_of(
        &self,
        query: StorageHistoryAsOfQuery,
    ) -> Result<Option<StorageExportTemplateHistoryRecord>, StorageError> {
        crate::operations::history::get_export_template_history_as_of(self.runtime(), query)
            .await
            .map_err(StorageError::from)
    }

    async fn list_remote_target_history(
        &self,
        query: StorageHistoryListQuery,
    ) -> Result<StoragePage<StorageRemoteTargetHistoryRecord>, StorageError> {
        crate::operations::history::list_remote_target_history(self.runtime(), query)
            .await
            .map_err(StorageError::from)
    }

    async fn get_remote_target_history_as_of(
        &self,
        query: StorageHistoryAsOfQuery,
    ) -> Result<Option<StorageRemoteTargetHistoryRecord>, StorageError> {
        crate::operations::history::get_remote_target_history_as_of(self.runtime(), query)
            .await
            .map_err(StorageError::from)
    }
}

#[async_trait]
impl CatalogStorage for PostgresStorage {
    async fn list_collections(
        &self,
        query: StorageCatalogListQuery,
    ) -> Result<StoragePage<StorageCollection>, StorageError> {
        crate::operations::catalog::list_collections(self.runtime(), query)
            .await
            .map_err(StorageError::from)
    }

    async fn list_classes(
        &self,
        query: StorageCatalogListQuery,
    ) -> Result<StoragePage<StorageClassWithCollection>, StorageError> {
        crate::operations::catalog::list_classes(self.runtime(), query)
            .await
            .map_err(StorageError::from)
    }

    async fn list_objects(
        &self,
        query: StorageCatalogListQuery,
    ) -> Result<StoragePage<StorageObject>, StorageError> {
        crate::operations::catalog::list_objects(self.runtime(), query)
            .await
            .map_err(StorageError::from)
    }
}

#[async_trait]
impl ComputedObjectStorage for PostgresStorage {
    async fn list_computed_objects(
        &self,
        query: StorageComputedObjectListQuery,
    ) -> Result<StorageComputedObjectPage, StorageError> {
        crate::operations::computed_objects::list_computed_objects(self.runtime(), query)
            .await
            .map_err(StorageError::from)
    }

    async fn enrich_objects_with_computed(
        &self,
        query: StorageComputedObjectEnrichmentQuery,
    ) -> Result<Vec<StorageComputedObject>, StorageError> {
        crate::operations::computed_objects::enrich_objects_with_computed(self.runtime(), query)
            .await
            .map_err(StorageError::from)
    }
}

#[async_trait]
impl ObjectAggregateStorage for PostgresStorage {
    async fn aggregate_objects(
        &self,
        query: StorageObjectAggregateQuery,
        authorization: StorageObjectAggregateAuthorization<'_>,
    ) -> Result<StorageObjectAggregatePage, StorageError> {
        crate::operations::object_aggregate::aggregate_objects(self.runtime(), query, authorization)
            .await
            .map_err(StorageError::from)
    }
}

#[async_trait]
impl RelationQueryStorage for PostgresStorage {
    async fn list_class_relations(
        &self,
        query: StorageRelationListQuery,
    ) -> Result<StoragePage<StorageClassRelation>, StorageError> {
        crate::operations::relation_query::list_class_relations(self.runtime(), query)
            .await
            .map_err(StorageError::from)
    }

    async fn list_object_relations(
        &self,
        query: StorageRelationListQuery,
    ) -> Result<StoragePage<StorageObjectRelation>, StorageError> {
        crate::operations::relation_query::list_object_relations(self.runtime(), query)
            .await
            .map_err(StorageError::from)
    }

    async fn list_class_relations_touching(
        &self,
        query: StorageRelationTouchingQuery,
    ) -> Result<StoragePage<StorageClassRelation>, StorageError> {
        crate::operations::relation_query::list_class_relations_touching(self.runtime(), query)
            .await
            .map_err(StorageError::from)
    }

    async fn list_object_relations_touching(
        &self,
        query: StorageRelationTouchingQuery,
    ) -> Result<StoragePage<StorageObjectRelation>, StorageError> {
        crate::operations::relation_query::list_object_relations_touching(self.runtime(), query)
            .await
            .map_err(StorageError::from)
    }

    async fn list_class_relations_touching_ids(
        &self,
        query: StorageRelationIdsQuery,
    ) -> Result<Vec<StorageClassRelation>, StorageError> {
        crate::operations::relation_query::list_class_relations_touching_ids(self.runtime(), query)
            .await
            .map_err(StorageError::from)
    }

    async fn list_class_relations_between_ids(
        &self,
        query: StorageRelationIdsQuery,
    ) -> Result<Vec<StorageClassRelation>, StorageError> {
        crate::operations::relation_query::list_class_relations_between_ids(self.runtime(), query)
            .await
            .map_err(StorageError::from)
    }

    async fn list_object_relations_between_ids(
        &self,
        query: StorageRelationIdsQuery,
    ) -> Result<Vec<StorageObjectRelation>, StorageError> {
        crate::operations::relation_query::list_object_relations_between_ids(self.runtime(), query)
            .await
            .map_err(StorageError::from)
    }

    async fn list_object_relations_touching_ids(
        &self,
        query: StorageObjectRelationsTouchingIdsQuery,
    ) -> Result<Vec<StorageObjectRelation>, StorageError> {
        crate::operations::relation_query::list_object_relations_touching_ids(self.runtime(), query)
            .await
            .map_err(StorageError::from)
    }

    async fn list_related_classes(
        &self,
        query: StorageRelationGraphQuery,
    ) -> Result<StoragePage<StorageClassGraphRow>, StorageError> {
        crate::operations::relation_query::list_related_classes(self.runtime(), query)
            .await
            .map_err(StorageError::from)
    }

    async fn list_related_objects(
        &self,
        query: StorageRelationGraphQuery,
    ) -> Result<StoragePage<StorageObjectGraphRow>, StorageError> {
        crate::operations::relation_query::list_related_objects(self.runtime(), query)
            .await
            .map_err(StorageError::from)
    }

    async fn list_related_objects_for_roots(
        &self,
        query: StorageRelatedObjectsForRootsQuery,
    ) -> Result<Vec<StorageRelatedObjectIncludeRow>, StorageError> {
        crate::operations::relation_query::list_related_objects_for_roots(self.runtime(), query)
            .await
            .map_err(StorageError::from)
    }

    async fn list_bidirectionally_related_objects_for_roots(
        &self,
        query: StorageBidirectionalRelatedObjectsQuery,
    ) -> Result<Vec<StorageRelatedObjectForRootRow>, StorageError> {
        crate::operations::relation_query::list_bidirectionally_related_objects_for_roots(
            self.runtime(),
            query,
        )
        .await
        .map_err(StorageError::from)
    }
}

#[async_trait]
impl UnifiedSearchStorage for PostgresStorage {
    async fn search_collections(
        &self,
        query: StorageUnifiedSearchQuery,
    ) -> Result<StorageCandidatePage<StorageUnifiedSearchCandidate<StorageCollection>>, StorageError>
    {
        crate::operations::unified_search::search_collections(self.runtime(), query)
            .await
            .map_err(StorageError::from)
    }

    async fn search_classes(
        &self,
        query: StorageUnifiedSearchQuery,
    ) -> Result<
        StorageCandidatePage<StorageUnifiedSearchCandidate<StorageClassWithCollection>>,
        StorageError,
    > {
        crate::operations::unified_search::search_classes(self.runtime(), query)
            .await
            .map_err(StorageError::from)
    }

    async fn search_objects(
        &self,
        query: StorageUnifiedSearchQuery,
    ) -> Result<StorageCandidatePage<StorageUnifiedSearchCandidate<StorageObject>>, StorageError>
    {
        crate::operations::unified_search::search_objects(self.runtime(), query)
            .await
            .map_err(StorageError::from)
    }
}

#[async_trait]
impl InventoryStorage for PostgresStorage {
    async fn get_inventory_counts(&self) -> Result<StorageInventoryCounts, StorageError> {
        crate::operations::inventory::load_inventory_counts(self.runtime())
            .await
            .map_err(StorageError::from)
    }
}
