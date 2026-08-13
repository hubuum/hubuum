use super::super::*;

fn history_collection_filter(
    scope: &HistoryCollectionScope,
) -> operations::history::HistoryCollectionFilter<'_> {
    match scope {
        HistoryCollectionScope::All => operations::history::HistoryCollectionFilter::All,
        HistoryCollectionScope::Visible(collection_ids) => {
            operations::history::HistoryCollectionFilter::Visible(collection_ids)
        }
    }
}

#[async_trait]
impl HistoryStorage for PostgresStorage {
    async fn resolve_history_principal_names(
        &self,
        principal_ids: Vec<i32>,
    ) -> Result<Vec<HistoryPrincipalName>, StorageError> {
        operations::history::resolve_principal_name_rows(&self.pool, principal_ids)
            .await
            .map(|rows| {
                rows.into_iter()
                    .map(operations::history::principal_name_to_storage)
                    .collect()
            })
            .map_err(map_postgres_error)
    }

    async fn list_collection_history(
        &self,
        query: HistoryListQuery,
    ) -> Result<HistoryPage<CollectionHistoryRecord>, StorageError> {
        let (entity_id, query_options, scope) = query.into_parts();
        operations::history::collection_history_paginated_with_total_count(
            entity_id,
            &self.pool,
            &query_options,
            history_collection_filter(&scope),
        )
        .await
        .map(|(rows, total)| {
            HistoryPage::new(
                rows.into_iter()
                    .map(operations::history::collection_history_to_storage)
                    .collect(),
                total,
            )
        })
        .map_err(map_postgres_error)
    }

    async fn collection_history_as_of(
        &self,
        query: HistoryAsOfQuery,
    ) -> Result<Option<CollectionHistoryRecord>, StorageError> {
        let (entity_id, at) = query.into_parts();
        operations::history::collection_as_of(entity_id, at, &self.pool)
            .await
            .map(|row| row.map(operations::history::collection_history_to_storage))
            .map_err(map_postgres_error)
    }

    async fn list_class_history(
        &self,
        query: HistoryListQuery,
    ) -> Result<HistoryPage<ClassHistoryRecord>, StorageError> {
        let (entity_id, query_options, scope) = query.into_parts();
        operations::history::class_history_paginated_with_total_count(
            entity_id,
            &self.pool,
            &query_options,
            history_collection_filter(&scope),
        )
        .await
        .map(|(rows, total)| {
            HistoryPage::new(
                rows.into_iter()
                    .map(operations::history::class_history_to_storage)
                    .collect(),
                total,
            )
        })
        .map_err(map_postgres_error)
    }

    async fn class_history_as_of(
        &self,
        query: HistoryAsOfQuery,
    ) -> Result<Option<ClassHistoryRecord>, StorageError> {
        let (entity_id, at) = query.into_parts();
        operations::history::class_as_of(entity_id, at, &self.pool)
            .await
            .map(|row| row.map(operations::history::class_history_to_storage))
            .map_err(map_postgres_error)
    }

    async fn list_object_history(
        &self,
        query: ObjectHistoryListQuery,
    ) -> Result<HistoryPage<ObjectHistoryRecord>, StorageError> {
        let (object_id, class_id, query_options, scope) = query.into_parts();
        operations::history::object_history_paginated_with_total_count(
            object_id,
            class_id,
            &self.pool,
            &query_options,
            history_collection_filter(&scope),
        )
        .await
        .map(|(rows, total)| {
            HistoryPage::new(
                rows.into_iter()
                    .map(operations::history::object_history_to_storage)
                    .collect(),
                total,
            )
        })
        .map_err(map_postgres_error)
    }

    async fn object_history_as_of(
        &self,
        query: ObjectHistoryAsOfQuery,
    ) -> Result<Option<ObjectHistoryRecord>, StorageError> {
        let (object_id, class_id, at) = query.into_parts();
        operations::history::object_as_of(object_id, class_id, at, &self.pool)
            .await
            .map(|row| row.map(operations::history::object_history_to_storage))
            .map_err(map_postgres_error)
    }

    async fn list_export_template_history(
        &self,
        query: HistoryListQuery,
    ) -> Result<HistoryPage<ExportTemplateHistoryRecord>, StorageError> {
        let (entity_id, query_options, scope) = query.into_parts();
        operations::history::export_template_history_paginated_with_total_count(
            entity_id,
            &self.pool,
            &query_options,
            history_collection_filter(&scope),
        )
        .await
        .map(|(rows, total)| {
            HistoryPage::new(
                rows.into_iter()
                    .map(operations::history::export_template_history_to_storage)
                    .collect(),
                total,
            )
        })
        .map_err(map_postgres_error)
    }

    async fn export_template_history_as_of(
        &self,
        query: HistoryAsOfQuery,
    ) -> Result<Option<ExportTemplateHistoryRecord>, StorageError> {
        let (entity_id, at) = query.into_parts();
        operations::history::export_template_as_of(entity_id, at, &self.pool)
            .await
            .map(|row| row.map(operations::history::export_template_history_to_storage))
            .map_err(map_postgres_error)
    }

    async fn list_remote_target_history(
        &self,
        query: HistoryListQuery,
    ) -> Result<HistoryPage<RemoteTargetHistoryRecord>, StorageError> {
        let (entity_id, query_options, scope) = query.into_parts();
        operations::history::remote_target_history_paginated_with_total_count(
            entity_id,
            &self.pool,
            &query_options,
            history_collection_filter(&scope),
        )
        .await
        .map(|(rows, total)| {
            HistoryPage::new(
                rows.into_iter()
                    .map(operations::history::remote_target_history_to_storage)
                    .collect(),
                total,
            )
        })
        .map_err(map_postgres_error)
    }

    async fn remote_target_history_as_of(
        &self,
        query: HistoryAsOfQuery,
    ) -> Result<Option<RemoteTargetHistoryRecord>, StorageError> {
        let (entity_id, at) = query.into_parts();
        operations::history::remote_target_as_of(entity_id, at, &self.pool)
            .await
            .map(|row| row.map(operations::history::remote_target_history_to_storage))
            .map_err(map_postgres_error)
    }
}

#[async_trait]
impl CatalogStorage for PostgresStorage {
    async fn list_collections(
        &self,
        query: CatalogListQuery,
    ) -> Result<CatalogPage<StorageCollection>, StorageError> {
        operations::catalog::list_collections(&self.pool, query)
            .await
            .map_err(map_postgres_error)
    }

    async fn list_classes(
        &self,
        query: CatalogListQuery,
    ) -> Result<CatalogPage<StorageClass>, StorageError> {
        operations::catalog::list_classes(&self.pool, query)
            .await
            .map_err(map_postgres_error)
    }

    async fn list_objects(
        &self,
        query: CatalogListQuery,
    ) -> Result<CatalogPage<StorageObject>, StorageError> {
        operations::catalog::list_objects(&self.pool, query)
            .await
            .map_err(map_postgres_error)
    }
}

#[async_trait]
impl ComputedObjectStorage for PostgresStorage {
    async fn list_computed_objects(
        &self,
        query: ComputedObjectListQuery,
    ) -> Result<ComputedObjectPage, StorageError> {
        operations::computed_objects::list_computed_objects(&self.pool, query)
            .await
            .map_err(map_postgres_error)
    }

    async fn enrich_objects_with_computed(
        &self,
        query: ComputedObjectEnrichmentQuery,
    ) -> Result<Vec<StorageComputedObject>, StorageError> {
        operations::computed_objects::enrich_computed_objects(&self.pool, query)
            .await
            .map_err(map_postgres_error)
    }
}

#[async_trait]
impl ObjectAggregateStorage for PostgresStorage {
    async fn aggregate_objects(
        &self,
        query: ObjectAggregateStorageQuery,
        authorizer: Option<&dyn ObjectAggregateAuthorizer>,
    ) -> Result<StorageObjectAggregatePage, StorageError> {
        operations::user::aggregate_objects(&self.pool, query, authorizer)
            .await
            .map_err(map_postgres_error)
    }
}

#[async_trait]
impl RelationQueryStorage for PostgresStorage {
    async fn list_class_relations(
        &self,
        query: RelationListQuery,
    ) -> Result<RelationPage<StorageClassRelation>, StorageError> {
        operations::relation_query::list_class_relations(&self.pool, query)
            .await
            .map_err(map_postgres_error)
    }

    async fn list_object_relations(
        &self,
        query: RelationListQuery,
    ) -> Result<RelationPage<StorageObjectRelation>, StorageError> {
        operations::relation_query::list_object_relations(&self.pool, query)
            .await
            .map_err(map_postgres_error)
    }

    async fn list_class_relations_touching(
        &self,
        query: RelationTouchingQuery,
    ) -> Result<RelationPage<StorageClassRelation>, StorageError> {
        operations::relation_query::list_class_relations_touching(&self.pool, query)
            .await
            .map_err(map_postgres_error)
    }

    async fn list_object_relations_touching(
        &self,
        query: RelationTouchingQuery,
    ) -> Result<RelationPage<StorageObjectRelation>, StorageError> {
        operations::relation_query::list_object_relations_touching(&self.pool, query)
            .await
            .map_err(map_postgres_error)
    }

    async fn class_relations_touching_ids(
        &self,
        query: RelationIdsQuery,
    ) -> Result<Vec<StorageClassRelation>, StorageError> {
        operations::relation_query::class_relations_touching_ids(&self.pool, query)
            .await
            .map_err(map_postgres_error)
    }

    async fn class_relations_between_ids(
        &self,
        query: RelationIdsQuery,
    ) -> Result<Vec<StorageClassRelation>, StorageError> {
        operations::relation_query::class_relations_between_ids(&self.pool, query)
            .await
            .map_err(map_postgres_error)
    }

    async fn object_relations_between_ids(
        &self,
        query: RelationIdsQuery,
    ) -> Result<Vec<StorageObjectRelation>, StorageError> {
        operations::relation_query::object_relations_between_ids(&self.pool, query)
            .await
            .map_err(map_postgres_error)
    }

    async fn object_relations_touching_ids(
        &self,
        query: ObjectRelationsTouchingIdsQuery,
    ) -> Result<Vec<StorageObjectRelation>, StorageError> {
        operations::relation_query::object_relations_touching_ids(&self.pool, query)
            .await
            .map_err(map_postgres_error)
    }

    async fn related_classes(
        &self,
        query: RelationGraphQuery,
    ) -> Result<RelationPage<StorageClassGraphRow>, StorageError> {
        operations::relation_query::related_classes(&self.pool, query)
            .await
            .map_err(map_postgres_error)
    }

    async fn related_objects(
        &self,
        query: RelationGraphQuery,
    ) -> Result<RelationPage<StorageObjectGraphRow>, StorageError> {
        operations::relation_query::related_objects(&self.pool, query)
            .await
            .map_err(map_postgres_error)
    }

    async fn related_objects_for_roots(
        &self,
        query: RelatedObjectsForRootsQuery,
    ) -> Result<Vec<StorageRelatedObjectIncludeRow>, StorageError> {
        operations::relation_query::related_objects_for_roots(&self.pool, query)
            .await
            .map_err(map_postgres_error)
    }

    async fn bidirectionally_related_objects_for_roots(
        &self,
        query: BidirectionalRelatedObjectsQuery,
    ) -> Result<Vec<StorageRelatedObjectForRootRow>, StorageError> {
        operations::relation_query::bidirectionally_related_objects_for_roots(&self.pool, query)
            .await
            .map_err(map_postgres_error)
    }
}

#[async_trait]
impl UnifiedSearchStorage for PostgresStorage {
    async fn search_unified_collections(
        &self,
        query: UnifiedSearchQuery,
    ) -> Result<Vec<UnifiedSearchCollection>, StorageError> {
        operations::ranked_search::search_collections(&self.pool, query)
            .await
            .map_err(map_postgres_error)
    }

    async fn search_unified_classes(
        &self,
        query: UnifiedSearchQuery,
    ) -> Result<Vec<UnifiedSearchClass>, StorageError> {
        operations::ranked_search::search_classes(&self.pool, query)
            .await
            .map_err(map_postgres_error)
    }

    async fn search_unified_objects(
        &self,
        query: UnifiedSearchQuery,
    ) -> Result<Vec<UnifiedSearchObject>, StorageError> {
        operations::ranked_search::search_objects(&self.pool, query)
            .await
            .map_err(map_postgres_error)
    }
}
