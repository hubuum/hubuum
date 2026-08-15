use async_trait::async_trait;

use crate::storage::{
    ExportTemplateStorage, StorageError, StorageExportTemplate, StorageExportTemplateCreate,
    StorageExportTemplateDelete, StorageExportTemplateListQuery, StorageExportTemplatePage,
    StorageExportTemplateReplace,
};

use super::PostgresStorage;

#[async_trait]
impl ExportTemplateStorage for PostgresStorage {
    async fn get_export_template(
        &self,
        template_id: i32,
    ) -> Result<StorageExportTemplate, StorageError> {
        hubuum_storage_postgres::operations::export_template::get_export_template(
            self.runtime(),
            template_id,
        )
        .await
        .map_err(StorageError::from)
    }

    async fn list_export_templates(
        &self,
        query: StorageExportTemplateListQuery,
    ) -> Result<StorageExportTemplatePage, StorageError> {
        hubuum_storage_postgres::operations::export_template::list_export_templates(
            self.runtime(),
            query,
        )
        .await
        .map_err(StorageError::from)
    }

    async fn list_export_templates_in_collection(
        &self,
        collection_id: i32,
        exclude_template_id: Option<i32>,
    ) -> Result<Vec<StorageExportTemplate>, StorageError> {
        hubuum_storage_postgres::operations::export_template::list_export_templates_in_collection(
            self.runtime(),
            collection_id,
            exclude_template_id,
        )
        .await
        .map_err(StorageError::from)
    }

    async fn export_template_class_collection_id(
        &self,
        class_id: i32,
    ) -> Result<Option<i32>, StorageError> {
        hubuum_storage_postgres::operations::export_template::export_template_class_collection_id(
            self.runtime(),
            class_id,
        )
        .await
        .map_err(StorageError::from)
    }

    async fn create_export_template(
        &self,
        request: StorageExportTemplateCreate,
    ) -> Result<StorageExportTemplate, StorageError> {
        hubuum_storage_postgres::operations::export_template::create_export_template(
            self.runtime(),
            request,
        )
        .await
        .map_err(StorageError::from)
    }

    async fn replace_export_template(
        &self,
        request: StorageExportTemplateReplace,
    ) -> Result<StorageExportTemplate, StorageError> {
        hubuum_storage_postgres::operations::export_template::replace_export_template(
            self.runtime(),
            request,
        )
        .await
        .map_err(StorageError::from)
    }

    async fn delete_export_template(
        &self,
        request: StorageExportTemplateDelete,
    ) -> Result<(), StorageError> {
        hubuum_storage_postgres::operations::export_template::delete_export_template(
            self.runtime(),
            request,
        )
        .await
        .map_err(StorageError::from)
    }
}
