use hubuum_domain::PrincipalId;

use super::*;

#[async_trait]
impl HistoryStorage for StorageHandle {
    async fn resolve_history_principal_names(
        &self,
        principal_ids: Vec<PrincipalId>,
    ) -> Result<Vec<HistoryPrincipalName>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            "history",
            "resolve_principal_names",
            async {
                dispatch_backend!(self, |backend| {
                    backend.resolve_history_principal_names(principal_ids).await
                })
            },
        )
        .await
    }

    async fn list_collection_history(
        &self,
        query: HistoryListQuery,
    ) -> Result<StorageCountedPage<CollectionHistoryRecord>, StorageError> {
        self.observe_storage_call(self.backend_name(), "history", "list_collections", async {
            dispatch_backend!(self, |backend| {
                backend.list_collection_history(query).await
            })
        })
        .await
    }

    async fn get_collection_history_as_of(
        &self,
        query: HistoryAsOfQuery,
    ) -> Result<Option<CollectionHistoryRecord>, StorageError> {
        self.observe_storage_call(self.backend_name(), "history", "collection_as_of", async {
            dispatch_backend!(self, |backend| {
                backend.get_collection_history_as_of(query).await
            })
        })
        .await
    }

    async fn list_class_history(
        &self,
        query: HistoryListQuery,
    ) -> Result<StorageCountedPage<ClassHistoryRecord>, StorageError> {
        self.observe_storage_call(self.backend_name(), "history", "list_classes", async {
            dispatch_backend!(self, |backend| { backend.list_class_history(query).await })
        })
        .await
    }

    async fn get_class_history_as_of(
        &self,
        query: HistoryAsOfQuery,
    ) -> Result<Option<ClassHistoryRecord>, StorageError> {
        self.observe_storage_call(self.backend_name(), "history", "class_as_of", async {
            dispatch_backend!(self, |backend| {
                backend.get_class_history_as_of(query).await
            })
        })
        .await
    }

    async fn list_object_history(
        &self,
        query: ObjectHistoryListQuery,
    ) -> Result<StorageCountedPage<ObjectHistoryRecord>, StorageError> {
        self.observe_storage_call(self.backend_name(), "history", "list_objects", async {
            dispatch_backend!(self, |backend| { backend.list_object_history(query).await })
        })
        .await
    }

    async fn get_object_history_as_of(
        &self,
        query: ObjectHistoryAsOfQuery,
    ) -> Result<Option<ObjectHistoryRecord>, StorageError> {
        self.observe_storage_call(self.backend_name(), "history", "object_as_of", async {
            dispatch_backend!(self, |backend| {
                backend.get_object_history_as_of(query).await
            })
        })
        .await
    }

    async fn list_export_template_history(
        &self,
        query: HistoryListQuery,
    ) -> Result<StorageCountedPage<ExportTemplateHistoryRecord>, StorageError> {
        self.observe_storage_call(self.backend_name(), "history", "list_templates", async {
            dispatch_backend!(self, |backend| {
                backend.list_export_template_history(query).await
            })
        })
        .await
    }

    async fn get_export_template_history_as_of(
        &self,
        query: HistoryAsOfQuery,
    ) -> Result<Option<ExportTemplateHistoryRecord>, StorageError> {
        self.observe_storage_call(self.backend_name(), "history", "template_as_of", async {
            dispatch_backend!(self, |backend| {
                backend.get_export_template_history_as_of(query).await
            })
        })
        .await
    }

    async fn list_remote_target_history(
        &self,
        query: HistoryListQuery,
    ) -> Result<StorageCountedPage<RemoteTargetHistoryRecord>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            "history",
            "list_remote_targets",
            async {
                dispatch_backend!(self, |backend| {
                    backend.list_remote_target_history(query).await
                })
            },
        )
        .await
    }

    async fn get_remote_target_history_as_of(
        &self,
        query: HistoryAsOfQuery,
    ) -> Result<Option<RemoteTargetHistoryRecord>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            "history",
            "remote_target_as_of",
            async {
                dispatch_backend!(self, |backend| {
                    backend.get_remote_target_history_as_of(query).await
                })
            },
        )
        .await
    }
}

#[async_trait]
impl CatalogStorage for StorageHandle {
    async fn list_collections(
        &self,
        query: CatalogListQuery,
    ) -> Result<StoragePage<StorageCollection>, StorageError> {
        self.observe_storage_call(self.backend_name(), "catalog", "collections", async {
            dispatch_backend!(self, |backend| backend.list_collections(query).await)
        })
        .await
    }

    async fn list_classes(
        &self,
        query: CatalogListQuery,
    ) -> Result<StoragePage<StorageClass>, StorageError> {
        self.observe_storage_call(self.backend_name(), "catalog", "classes", async {
            dispatch_backend!(self, |backend| backend.list_classes(query).await)
        })
        .await
    }

    async fn list_objects(
        &self,
        query: CatalogListQuery,
    ) -> Result<StoragePage<StorageObject>, StorageError> {
        self.observe_storage_call(self.backend_name(), "catalog", "objects", async {
            dispatch_backend!(self, |backend| backend.list_objects(query).await)
        })
        .await
    }
}

#[async_trait]
impl ComputedObjectStorage for StorageHandle {
    async fn list_computed_objects(
        &self,
        query: ComputedObjectListQuery,
    ) -> Result<ComputedObjectPage, StorageError> {
        self.observe_storage_call(self.backend_name(), "computed_objects", "list", async {
            dispatch_backend!(self, |backend| {
                backend.list_computed_objects(query).await
            })
        })
        .await
    }

    async fn enrich_objects_with_computed(
        &self,
        query: ComputedObjectEnrichmentQuery,
    ) -> Result<Vec<StorageComputedObject>, StorageError> {
        self.observe_storage_call(self.backend_name(), "computed_objects", "enrich", async {
            dispatch_backend!(self, |backend| {
                backend.enrich_objects_with_computed(query).await
            })
        })
        .await
    }
}
