use super::*;
use hubuum_domain::SchemaReference;
use hubuum_storage_core::schema_evolution::*;

#[async_trait]
impl SchemaEvolutionStorage for StorageHandle {
    async fn get_schema_impact_boundary(
        &self,
        target: SchemaReference,
    ) -> Result<StorageSchemaImpactBoundary, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::SchemaEvolution,
            "get_schema_impact_boundary",
            async {
                dispatch_backend!(self, |backend| {
                    backend.get_schema_impact_boundary(target).await
                })
            },
        )
        .await
    }
    async fn schema_compliance_counts(&self) -> Result<StorageComplianceCounts, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::SchemaEvolution,
            "schema_compliance_counts",
            async {
                dispatch_backend!(self, |backend| { backend.schema_compliance_counts().await })
            },
        )
        .await
    }

    async fn list_schema_revisions(
        &self,
        query: StorageSchemaPage,
    ) -> Result<Vec<StorageSchemaRevision>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::SchemaEvolution,
            "list_schema_revisions",
            async {
                dispatch_backend!(self, |backend| {
                    backend.list_schema_revisions(query).await
                })
            },
        )
        .await
    }
    async fn get_schema_state(
        &self,
        class_id: ClassId,
    ) -> Result<StorageClassSchemaState, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::SchemaEvolution,
            "get_schema_state",
            async {
                dispatch_backend!(self, |backend| { backend.get_schema_state(class_id).await })
            },
        )
        .await
    }
    async fn stage_schema_revision(
        &self,
        request: StorageSchemaStage,
    ) -> Result<StorageMutationOutcome<StorageSchemaRevision>, StorageError> {
        let policy = "stage";
        let observed = self.observe_storage_call(
            self.backend_name(),
            StorageCapability::SchemaEvolution,
            "stage_schema_revision",
            async {
                dispatch_backend!(self, |backend| {
                    backend.stage_schema_revision(request).await
                })
            },
        );
        let result = observed.await;
        crate::observability::metrics::schema_mutation(policy, &result);
        result
    }
    async fn abandon_schema_revision(
        &self,
        target: SchemaReference,
        authorized_collection: CollectionId,
        context: &EventContext,
    ) -> Result<StorageMutationOutcome<StorageSchemaRevision>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::SchemaEvolution,
            "abandon_schema_revision",
            async {
                dispatch_backend!(self, |backend| {
                    backend
                        .abandon_schema_revision(target, authorized_collection, context)
                        .await
                })
            },
        )
        .await
    }
    async fn activate_schema_revision(
        &self,
        request: StorageSchemaActivation,
    ) -> Result<StorageMutationOutcome<StorageSchemaActivationResult>, StorageError> {
        let policy = match request.policy() {
            StorageSchemaActivationPolicy::RejectIncompatible => "reject_incompatible",
            StorageSchemaActivationPolicy::AllowPending => "allow_pending",
        };
        let observed = self.observe_storage_call(
            self.backend_name(),
            StorageCapability::SchemaEvolution,
            "activate_schema_revision",
            async {
                dispatch_backend!(self, |backend| {
                    backend.activate_schema_revision(request).await
                })
            },
        );
        let result = observed.await;
        crate::observability::metrics::schema_mutation(policy, &result);
        if let Ok(outcome) = &result
            && outcome.is_committed()
            && outcome.value().dependent_rebuild_task_id().is_some()
        {
            crate::observability::metrics::schema_dependency_rebuild();
        }
        result
    }
    async fn request_schema_work(
        &self,
        request: StorageSchemaWorkRequest,
    ) -> Result<StorageMutationOutcome<StorageSchemaWork>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::SchemaEvolution,
            "request_schema_work",
            async {
                dispatch_backend!(self, |backend| {
                    backend.request_schema_work(request).await
                })
            },
        )
        .await
    }
    async fn get_schema_work(&self, task_id: TaskId) -> Result<StorageSchemaWork, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::SchemaEvolution,
            "get_schema_work",
            async { dispatch_backend!(self, |backend| { backend.get_schema_work(task_id).await }) },
        )
        .await
    }
    async fn process_schema_work(
        &self,
        lease: StorageTaskLease,
        limits: StorageSchemaBatchLimits,
    ) -> Result<StorageSchemaWork, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::SchemaEvolution,
            "process_schema_work",
            async {
                dispatch_backend!(self, |backend| {
                    backend.process_schema_work(lease, limits).await
                })
            },
        )
        .await
    }
    async fn cancel_schema_work(
        &self,
        task_id: TaskId,
        authorized_collection: CollectionId,
        context: &EventContext,
    ) -> Result<StorageMutationOutcome<StorageSchemaWork>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::SchemaEvolution,
            "cancel_schema_work",
            async {
                dispatch_backend!(self, |backend| {
                    backend
                        .cancel_schema_work(task_id, authorized_collection, context)
                        .await
                })
            },
        )
        .await
    }
    async fn list_schema_compliance(
        &self,
        query: StorageSchemaPage,
        status: Option<StorageComplianceStatus>,
    ) -> Result<Vec<StorageObjectCompliance>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::SchemaEvolution,
            "list_schema_compliance",
            async {
                dispatch_backend!(self, |backend| {
                    backend.list_schema_compliance(query, status).await
                })
            },
        )
        .await
    }
}
