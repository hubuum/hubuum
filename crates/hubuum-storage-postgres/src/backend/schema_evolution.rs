use super::*;
use crate::operations::schema_evolution;
use hubuum_domain::{SchemaReference, TaskId};

#[async_trait]
impl SchemaEvolutionStorage for PostgresStorage {
    async fn get_schema_impact_boundary(
        &self,
        target: SchemaReference,
    ) -> Result<StorageSchemaImpactBoundary, StorageError> {
        schema_evolution::get_schema_impact_boundary(self.runtime(), target)
            .await
            .map_err(StorageError::from)
    }
    async fn schema_compliance_counts(&self) -> Result<StorageComplianceCounts, StorageError> {
        schema_evolution::schema_compliance_counts(self.runtime())
            .await
            .map_err(StorageError::from)
    }
    async fn list_schema_revisions(
        &self,
        query: StorageSchemaPage,
    ) -> Result<Vec<StorageSchemaRevision>, StorageError> {
        schema_evolution::list_schema_revisions(self.runtime(), query)
            .await
            .map_err(StorageError::from)
    }
    async fn get_schema_state(
        &self,
        class_id: ClassId,
    ) -> Result<StorageClassSchemaState, StorageError> {
        schema_evolution::get_schema_state(self.runtime(), class_id)
            .await
            .map_err(StorageError::from)
    }
    async fn stage_schema_revision(
        &self,
        request: StorageSchemaStage,
    ) -> Result<StorageMutationOutcome<StorageSchemaRevision>, StorageError> {
        crate::with_mutation_provenance(
            Some(request.context().mutation_provenance().clone()),
            schema_evolution::stage_schema_revision(self.runtime(), request),
        )
        .await
        .map_err(StorageError::from)
    }
    async fn abandon_schema_revision(
        &self,
        target: SchemaReference,
        authorized_collection: CollectionId,
        context: &EventContext,
    ) -> Result<StorageMutationOutcome<StorageSchemaRevision>, StorageError> {
        crate::with_mutation_provenance(
            Some(context.mutation_provenance().clone()),
            schema_evolution::abandon_schema_revision(
                self.runtime(),
                target,
                authorized_collection,
                context,
            ),
        )
        .await
        .map_err(StorageError::from)
    }
    async fn activate_schema_revision(
        &self,
        request: StorageSchemaActivation,
    ) -> Result<StorageMutationOutcome<StorageSchemaActivationResult>, StorageError> {
        crate::with_mutation_provenance(
            Some(request.context().mutation_provenance().clone()),
            schema_evolution::activate_schema_revision(self.runtime(), request),
        )
        .await
        .map_err(StorageError::from)
    }
    async fn request_schema_work(
        &self,
        request: StorageSchemaWorkRequest,
    ) -> Result<StorageMutationOutcome<StorageSchemaWork>, StorageError> {
        schema_evolution::request_schema_work(self.runtime(), request)
            .await
            .map_err(StorageError::from)
    }
    async fn get_schema_work(&self, task_id: TaskId) -> Result<StorageSchemaWork, StorageError> {
        schema_evolution::get_schema_work(self.runtime(), task_id)
            .await
            .map_err(StorageError::from)
    }
    async fn process_schema_work(
        &self,
        lease: StorageTaskLease,
        limits: StorageSchemaBatchLimits,
    ) -> Result<StorageSchemaWork, StorageError> {
        schema_evolution::process_schema_work(self.runtime(), lease, limits)
            .await
            .map_err(StorageError::from)
    }
    async fn cancel_schema_work(
        &self,
        task_id: TaskId,
        authorized_collection: CollectionId,
        context: &EventContext,
    ) -> Result<StorageMutationOutcome<StorageSchemaWork>, StorageError> {
        schema_evolution::cancel_schema_work(
            self.runtime(),
            task_id,
            authorized_collection,
            context,
        )
        .await
        .map_err(StorageError::from)
    }
    async fn list_schema_compliance(
        &self,
        query: StorageSchemaPage,
        status: Option<StorageComplianceStatus>,
    ) -> Result<Vec<StorageObjectCompliance>, StorageError> {
        schema_evolution::list_schema_compliance(self.runtime(), query, status)
            .await
            .map_err(StorageError::from)
    }
}
