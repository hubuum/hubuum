use async_trait::async_trait;

use crate::models::ExportTemplateID;
use crate::pagination::{SKIPPED_TOTAL_COUNT, known_count_or_skipped};
use crate::storage::{
    ExportTemplateStorage, StorageError, StorageExportTemplate, StorageExportTemplateCreate,
    StorageExportTemplateDefinition, StorageExportTemplateDelete, StorageExportTemplateListQuery,
    StorageExportTemplatePage, StorageExportTemplateReplace, StorageRecordMetadata,
};

use super::PostgresStorage;
use super::error::map_postgres_error;
use super::operations::export_template::{
    DeleteExportTemplateRecord, ExportTemplateRow, LoadExportTemplateRecord, NewExportTemplateRow,
    SaveExportTemplateRecord, UpdateExportTemplateRecord, UpdateExportTemplateRow,
    class_collection_id, list_all_rows_with_total_count, list_rows_with_total_count,
    load_rows_in_collection,
};

fn template_to_storage(row: ExportTemplateRow) -> StorageExportTemplate {
    StorageExportTemplate::new(
        StorageRecordMetadata::new(row.id, row.created_at, row.updated_at, row.revision.get()),
        row.collection_id,
        row.name,
        StorageExportTemplateDefinition::new(
            row.description,
            row.content_type,
            row.template,
            row.kind,
        )
        .with_scope(row.scope_kind, row.class_id)
        .with_default_query(row.default_query)
        .with_include(row.include)
        .with_relation_context(row.relation_context)
        .with_default_missing_data_policy(row.default_missing_data_policy)
        .with_default_limits(row.default_limits),
    )
}

struct ExportTemplateDefinitionRowParts {
    description: String,
    content_type: String,
    template: String,
    kind: String,
    scope_kind: Option<String>,
    class_id: Option<i32>,
    default_query: Option<String>,
    include: Option<serde_json::Value>,
    relation_context: Option<serde_json::Value>,
    default_missing_data_policy: Option<String>,
    default_limits: Option<serde_json::Value>,
}

fn definition_into_row_parts(
    definition: StorageExportTemplateDefinition,
) -> ExportTemplateDefinitionRowParts {
    let (
        description,
        content_type,
        template,
        kind,
        scope_kind,
        class_id,
        default_query,
        include,
        relation_context,
        default_missing_data_policy,
        default_limits,
    ) = definition.into_parts();
    ExportTemplateDefinitionRowParts {
        description,
        content_type,
        template,
        kind,
        scope_kind,
        class_id,
        default_query,
        include,
        relation_context,
        default_missing_data_policy,
        default_limits,
    }
}

fn rows_to_storage(rows: Vec<ExportTemplateRow>) -> Vec<StorageExportTemplate> {
    rows.into_iter().map(template_to_storage).collect()
}

#[async_trait]
impl ExportTemplateStorage for PostgresStorage {
    async fn get_export_template(
        &self,
        template_id: i32,
    ) -> Result<StorageExportTemplate, StorageError> {
        ExportTemplateID::new(template_id)
            .map_err(map_postgres_error)?
            .load_export_template_record(self.pool())
            .await
            .map(template_to_storage)
            .map_err(map_postgres_error)
    }

    async fn list_export_templates(
        &self,
        query: StorageExportTemplateListQuery,
    ) -> Result<StorageExportTemplatePage, StorageError> {
        let (collection_ids, options) = query.into_parts();
        if collection_ids.as_ref().is_some_and(Vec::is_empty) {
            return Ok(StorageExportTemplatePage::new(
                Vec::new(),
                (known_count_or_skipped(&options, 0) != SKIPPED_TOTAL_COUNT).then_some(0),
            ));
        }
        let (rows, total) = match collection_ids {
            Some(collection_ids) => {
                list_rows_with_total_count(self.pool(), &collection_ids, &options).await
            }
            None => list_all_rows_with_total_count(self.pool(), &options).await,
        }
        .map_err(map_postgres_error)?;
        Ok(StorageExportTemplatePage::new(
            rows_to_storage(rows),
            (total != SKIPPED_TOTAL_COUNT).then_some(total),
        ))
    }

    async fn list_export_templates_in_collection(
        &self,
        collection_id: i32,
        exclude_template_id: Option<i32>,
    ) -> Result<Vec<StorageExportTemplate>, StorageError> {
        load_rows_in_collection(self.pool(), collection_id, exclude_template_id)
            .await
            .map(rows_to_storage)
            .map_err(map_postgres_error)
    }

    async fn export_template_class_collection_id(
        &self,
        class_id: i32,
    ) -> Result<Option<i32>, StorageError> {
        class_collection_id(self.pool(), class_id)
            .await
            .map_err(map_postgres_error)
    }

    async fn create_export_template(
        &self,
        request: StorageExportTemplateCreate,
    ) -> Result<StorageExportTemplate, StorageError> {
        let (collection_id, name, definition, event_context) = request.into_parts();
        let definition = definition_into_row_parts(definition);
        NewExportTemplateRow {
            collection_id,
            name,
            description: definition.description,
            content_type: definition.content_type,
            template: definition.template,
            kind: definition.kind,
            scope_kind: definition.scope_kind,
            class_id: definition.class_id,
            default_query: definition.default_query,
            include: definition.include,
            relation_context: definition.relation_context,
            default_missing_data_policy: definition.default_missing_data_policy,
            default_limits: definition.default_limits,
        }
        .save_export_template_record(self.pool(), event_context.as_ref())
        .await
        .map(template_to_storage)
        .map_err(map_postgres_error)
    }

    async fn replace_export_template(
        &self,
        request: StorageExportTemplateReplace,
    ) -> Result<StorageExportTemplate, StorageError> {
        let (template_id, collection_id, name, definition, event_context) = request.into_parts();
        let definition = definition_into_row_parts(definition);
        UpdateExportTemplateRow {
            collection_id: Some(collection_id),
            name: Some(name),
            description: Some(definition.description),
            template: Some(definition.template),
            kind: Some(definition.kind),
            scope_kind: Some(definition.scope_kind),
            class_id: Some(definition.class_id),
            default_query: Some(definition.default_query),
            include: Some(definition.include),
            relation_context: Some(definition.relation_context),
            default_missing_data_policy: Some(definition.default_missing_data_policy),
            default_limits: Some(definition.default_limits),
        }
        .update_export_template_record(self.pool(), template_id, event_context.as_ref())
        .await
        .map(template_to_storage)
        .map_err(map_postgres_error)
    }

    async fn delete_export_template(
        &self,
        request: StorageExportTemplateDelete,
    ) -> Result<(), StorageError> {
        let (template_id, event_context) = request.into_parts();
        ExportTemplateID::new(template_id)
            .map_err(map_postgres_error)?
            .delete_export_template_record(self.pool(), event_context.as_ref())
            .await
            .map_err(map_postgres_error)
    }
}
