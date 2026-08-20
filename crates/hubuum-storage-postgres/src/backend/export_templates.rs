use async_trait::async_trait;
use hubuum_domain::{ClassId, CollectionId, ExportTemplateId};

use hubuum_storage_core::{
    ExportTemplateStorage, MutationOutcome, StorageError, StorageExportTemplate,
    StorageExportTemplateCreate, StorageExportTemplateDelete, StorageExportTemplateListQuery,
    StorageExportTemplateReplace, StoragePage,
};

use super::PostgresStorage;

#[async_trait]
impl ExportTemplateStorage for PostgresStorage {
    async fn get_export_template(
        &self,
        template_id: ExportTemplateId,
    ) -> Result<StorageExportTemplate, StorageError> {
        crate::operations::export_template::get_export_template(self.runtime(), template_id.id())
            .await
            .map_err(StorageError::from)
    }

    async fn list_export_templates(
        &self,
        query: StorageExportTemplateListQuery,
    ) -> Result<StoragePage<StorageExportTemplate>, StorageError> {
        crate::operations::export_template::list_export_templates(self.runtime(), query)
            .await
            .map_err(StorageError::from)
    }

    async fn list_export_templates_in_collection(
        &self,
        collection_id: CollectionId,
        exclude_template_id: Option<ExportTemplateId>,
    ) -> Result<Vec<StorageExportTemplate>, StorageError> {
        crate::operations::export_template::list_export_templates_in_collection(
            self.runtime(),
            collection_id.id(),
            exclude_template_id.map(ExportTemplateId::id),
        )
        .await
        .map_err(StorageError::from)
    }

    async fn export_template_class_collection_id(
        &self,
        class_id: ClassId,
    ) -> Result<Option<CollectionId>, StorageError> {
        crate::operations::export_template::export_template_class_collection_id(
            self.runtime(),
            class_id.id(),
        )
        .await?
        .map(CollectionId::new)
        .transpose()
        .map_err(|error| StorageError::internal(error.to_string()))
    }

    async fn create_export_template(
        &self,
        request: StorageExportTemplateCreate,
    ) -> Result<MutationOutcome<StorageExportTemplate>, StorageError> {
        crate::operations::export_template::create_export_template(self.runtime(), request)
            .await
            .map_err(StorageError::from)
    }

    async fn replace_export_template(
        &self,
        request: StorageExportTemplateReplace,
    ) -> Result<MutationOutcome<StorageExportTemplate>, StorageError> {
        crate::operations::export_template::replace_export_template(self.runtime(), request)
            .await
            .map_err(StorageError::from)
    }

    async fn delete_export_template(
        &self,
        request: StorageExportTemplateDelete,
    ) -> Result<MutationOutcome<()>, StorageError> {
        crate::operations::export_template::delete_export_template(self.runtime(), request)
            .await
            .map_err(StorageError::from)
    }
}
