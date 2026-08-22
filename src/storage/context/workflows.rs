use super::*;

#[async_trait]
impl ExportTemplateStorage for StorageHandle {
    async fn get_export_template(
        &self,
        template_id: ExportTemplateId,
    ) -> Result<StorageExportTemplate, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::ExportTemplate,
            "get_export_template",
            async {
                dispatch_backend!(self, |backend| {
                    backend.get_export_template(template_id).await
                })
            },
        )
        .await
    }

    async fn list_export_templates(
        &self,
        query: StorageExportTemplateListQuery,
    ) -> Result<StoragePage<StorageExportTemplate>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::ExportTemplate,
            "list_export_templates",
            async {
                dispatch_backend!(self, |backend| {
                    backend.list_export_templates(query).await
                })
            },
        )
        .await
    }

    async fn list_export_templates_in_collection(
        &self,
        collection_id: CollectionId,
        exclude_template_id: Option<ExportTemplateId>,
    ) -> Result<Vec<StorageExportTemplate>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::ExportTemplate,
            "list_export_templates_in_collection",
            async {
                dispatch_backend!(self, |backend| {
                    backend
                        .list_export_templates_in_collection(collection_id, exclude_template_id)
                        .await
                })
            },
        )
        .await
    }

    async fn get_export_template_class_collection_id(
        &self,
        class_id: ClassId,
    ) -> Result<Option<CollectionId>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::ExportTemplate,
            "get_export_template_class_collection_id",
            async {
                dispatch_backend!(self, |backend| {
                    backend
                        .get_export_template_class_collection_id(class_id)
                        .await
                })
            },
        )
        .await
    }

    async fn create_export_template(
        &self,
        request: StorageExportTemplateCreate,
    ) -> Result<MutationOutcome<StorageExportTemplate>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::ExportTemplate,
            "create_export_template",
            async {
                dispatch_backend!(self, |backend| {
                    backend.create_export_template(request).await
                })
            },
        )
        .await
    }

    async fn replace_export_template(
        &self,
        request: StorageExportTemplateReplace,
    ) -> Result<MutationOutcome<StorageExportTemplate>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::ExportTemplate,
            "replace_export_template",
            async {
                dispatch_backend!(self, |backend| {
                    backend.replace_export_template(request).await
                })
            },
        )
        .await
    }

    async fn delete_export_template(
        &self,
        request: StorageExportTemplateDelete,
    ) -> Result<MutationOutcome<()>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::ExportTemplate,
            "delete_export_template",
            async {
                dispatch_backend!(self, |backend| {
                    backend.delete_export_template(request).await
                })
            },
        )
        .await
    }
}

#[async_trait]
impl RemoteTargetStorage for StorageHandle {
    async fn get_remote_target(
        &self,
        target_id: RemoteTargetId,
    ) -> Result<StorageRemoteTarget, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::RemoteTarget,
            "get_remote_target",
            async {
                dispatch_backend!(self, |backend| {
                    backend.get_remote_target(target_id).await
                })
            },
        )
        .await
    }

    async fn list_remote_targets(
        &self,
        query: StorageRemoteTargetListQuery,
    ) -> Result<StoragePage<StorageRemoteTarget>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::RemoteTarget,
            "list_remote_targets",
            async {
                dispatch_backend!(self, |backend| { backend.list_remote_targets(query).await })
            },
        )
        .await
    }

    async fn create_remote_target(
        &self,
        request: StorageRemoteTargetCreate,
    ) -> Result<MutationOutcome<StorageRemoteTarget>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::RemoteTarget,
            "create_remote_target",
            async {
                dispatch_backend!(self, |backend| {
                    backend.create_remote_target(request).await
                })
            },
        )
        .await
    }

    async fn update_remote_target(
        &self,
        request: StorageRemoteTargetUpdate,
    ) -> Result<MutationOutcome<StorageRemoteTarget>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::RemoteTarget,
            "update_remote_target",
            async {
                dispatch_backend!(self, |backend| {
                    backend.update_remote_target(request).await
                })
            },
        )
        .await
    }

    async fn delete_remote_target(
        &self,
        request: StorageRemoteTargetDelete,
    ) -> Result<MutationOutcome<()>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::RemoteTarget,
            "delete_remote_target",
            async {
                dispatch_backend!(self, |backend| {
                    backend.delete_remote_target(request).await
                })
            },
        )
        .await
    }

    async fn record_remote_target_invocation(
        &self,
        request: StorageRemoteTargetInvocation,
    ) -> Result<MutationOutcome<()>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::RemoteTarget,
            "record_remote_target_invocation",
            async {
                dispatch_backend!(self, |backend| {
                    backend.record_remote_target_invocation(request).await
                })
            },
        )
        .await
    }
}

#[async_trait]
impl ImportStorage for StorageHandle {
    async fn get_import_root_collection(&self) -> Result<StorageCollection, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::Import,
            "get_import_root_collection",
            async {
                dispatch_backend!(self, |backend| {
                    backend.get_import_root_collection().await
                })
            },
        )
        .await
    }

    async fn get_import_collection_by_id(
        &self,
        collection_id: CollectionId,
    ) -> Result<Option<StorageCollection>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::Import,
            "get_import_collection_by_id",
            async {
                dispatch_backend!(self, |backend| {
                    backend.get_import_collection_by_id(collection_id).await
                })
            },
        )
        .await
    }

    async fn get_import_collection_by_key(
        &self,
        key: &StorageImportCollectionKey,
    ) -> Result<Option<StorageCollection>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::Import,
            "get_import_collection_by_key",
            async {
                dispatch_backend!(self, |backend| {
                    backend.get_import_collection_by_key(key).await
                })
            },
        )
        .await
    }

    async fn list_import_collections_by_name(
        &self,
        name: &str,
    ) -> Result<Vec<StorageCollection>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::Import,
            "list_import_collections_by_name",
            async {
                dispatch_backend!(self, |backend| {
                    backend.list_import_collections_by_name(name).await
                })
            },
        )
        .await
    }

    async fn get_import_collection_child_by_name(
        &self,
        parent_collection_id: CollectionId,
        name: &str,
    ) -> Result<Option<StorageCollection>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::Import,
            "get_import_collection_child_by_name",
            async {
                dispatch_backend!(self, |backend| {
                    backend
                        .get_import_collection_child_by_name(parent_collection_id, name)
                        .await
                })
            },
        )
        .await
    }

    async fn get_import_class_by_name(
        &self,
        collection_id: CollectionId,
        name: &str,
    ) -> Result<Option<StorageClassRecord>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::Import,
            "get_import_class_by_name",
            async {
                dispatch_backend!(self, |backend| {
                    backend.get_import_class_by_name(collection_id, name).await
                })
            },
        )
        .await
    }

    async fn list_import_classes_by_names(
        &self,
        collection_id: CollectionId,
        names: &[String],
    ) -> Result<Vec<StorageClassRecord>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::Import,
            "list_import_classes_by_names",
            async {
                dispatch_backend!(self, |backend| {
                    backend
                        .list_import_classes_by_names(collection_id, names)
                        .await
                })
            },
        )
        .await
    }

    async fn get_import_object_by_name(
        &self,
        class_id: ClassId,
        name: &str,
    ) -> Result<Option<StorageObject>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::Import,
            "get_import_object_by_name",
            async {
                dispatch_backend!(self, |backend| {
                    backend.get_import_object_by_name(class_id, name).await
                })
            },
        )
        .await
    }

    async fn list_import_objects_by_names(
        &self,
        class_id: ClassId,
        names: &[String],
    ) -> Result<Vec<StorageObject>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::Import,
            "list_import_objects_by_names",
            async {
                dispatch_backend!(self, |backend| {
                    backend.list_import_objects_by_names(class_id, names).await
                })
            },
        )
        .await
    }

    async fn has_import_class_relation(
        &self,
        left_class_id: ClassId,
        right_class_id: ClassId,
    ) -> Result<bool, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::Import,
            "has_import_class_relation",
            async {
                dispatch_backend!(self, |backend| {
                    backend
                        .has_import_class_relation(left_class_id, right_class_id)
                        .await
                })
            },
        )
        .await
    }

    async fn has_import_object_relation(
        &self,
        left_object_id: ObjectId,
        right_object_id: ObjectId,
    ) -> Result<bool, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::Import,
            "has_import_object_relation",
            async {
                dispatch_backend!(self, |backend| {
                    backend
                        .has_import_object_relation(left_object_id, right_object_id)
                        .await
                })
            },
        )
        .await
    }

    async fn has_import_group(
        &self,
        identity_scope: &str,
        group_name: &str,
    ) -> Result<bool, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::Import,
            "has_import_group",
            async {
                dispatch_backend!(self, |backend| {
                    backend.has_import_group(identity_scope, group_name).await
                })
            },
        )
        .await
    }

    async fn preflight_import(
        &self,
        plan: StorageImportPlan,
        mode: StorageImportMode,
    ) -> Result<StorageImportPreflight, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::Import,
            "preflight_import",
            async {
                dispatch_backend!(self, |backend| {
                    backend.preflight_import(plan, mode).await
                })
            },
        )
        .await
    }

    async fn apply_import_strict(&self, plan: StorageImportPlan) -> Result<(), StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::Import,
            "apply_import_strict",
            async {
                dispatch_backend!(self, |backend| { backend.apply_import_strict(plan).await })
            },
        )
        .await
    }

    async fn apply_import_best_effort(
        &self,
        plan: StorageImportPlan,
        mode: StorageImportMode,
    ) -> Result<StorageImportApply, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::Import,
            "apply_import_best_effort",
            async {
                dispatch_backend!(self, |backend| {
                    backend.apply_import_best_effort(plan, mode).await
                })
            },
        )
        .await
    }

    async fn record_import_results(
        &self,
        results: Vec<StorageImportResult>,
    ) -> Result<(), StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::Import,
            "record_import_results",
            async {
                dispatch_backend!(self, |backend| {
                    backend.record_import_results(results).await
                })
            },
        )
        .await
    }
}

#[async_trait]
impl RestoreStorage for StorageHandle {
    async fn stage_restore(
        &self,
        request: StorageRestoreStageCreate,
    ) -> Result<StorageRestoreJob, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::Restore,
            "stage_restore",
            async { dispatch_backend!(self, |backend| backend.stage_restore(request).await) },
        )
        .await
    }

    async fn get_restore_job(
        &self,
        job_id: RestoreJobId,
    ) -> Result<StorageRestoreJob, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::Restore,
            "get_restore_job",
            async { dispatch_backend!(self, |backend| backend.get_restore_job(job_id).await) },
        )
        .await
    }

    async fn get_restore_status(
        &self,
        job_id: RestoreJobId,
    ) -> Result<StorageRestoreStatus, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::Restore,
            "get_restore_status",
            async {
                dispatch_backend!(self, |backend| { backend.get_restore_status(job_id).await })
            },
        )
        .await
    }

    async fn expire_restore_stage(&self, job_id: RestoreJobId) -> Result<bool, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::Restore,
            "expire_restore_stage",
            async {
                dispatch_backend!(self, |backend| {
                    backend.expire_restore_stage(job_id).await
                })
            },
        )
        .await
    }

    async fn start_restore_draining(
        &self,
        job_id: RestoreJobId,
    ) -> Result<DateTime<Utc>, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::Restore,
            "start_restore_draining",
            async {
                dispatch_backend!(self, |backend| {
                    backend.start_restore_draining(job_id).await
                })
            },
        )
        .await
    }

    async fn apply_restore(
        &self,
        request: StorageRestoreApply,
    ) -> Result<StorageRestoreCompletion, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::Restore,
            "apply_restore",
            async { dispatch_backend!(self, |backend| backend.apply_restore(request).await) },
        )
        .await
    }

    async fn fail_restore_and_resume(
        &self,
        request: StorageRestoreFailure,
    ) -> Result<(), StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::Restore,
            "fail_restore_and_resume",
            async {
                dispatch_backend!(self, |backend| {
                    backend.fail_restore_and_resume(request).await
                })
            },
        )
        .await
    }

    async fn get_restore_coordinator_snapshot(
        &self,
    ) -> Result<StorageRestoreCoordinatorSnapshot, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::Restore,
            "get_restore_coordinator_snapshot",
            async {
                dispatch_backend!(self, |backend| {
                    backend.get_restore_coordinator_snapshot().await
                })
            },
        )
        .await
    }

    async fn resume_maintenance_without_restore(&self) -> Result<(), StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::Restore,
            "resume_maintenance_without_restore",
            async {
                dispatch_backend!(self, |backend| {
                    backend.resume_maintenance_without_restore().await
                })
            },
        )
        .await
    }

    async fn resume_terminal_restore(&self, job_id: RestoreJobId) -> Result<(), StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::Restore,
            "resume_terminal_restore",
            async {
                dispatch_backend!(self, |backend| {
                    backend.resume_terminal_restore(job_id).await
                })
            },
        )
        .await
    }

    async fn tick_restore_coordinator(
        &self,
        instance_id: Uuid,
        local_work_is_idle: &(dyn Fn() -> bool + Send + Sync),
        expire_validated_jobs: bool,
    ) -> Result<StorageRestoreCoordinatorSnapshot, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::Restore,
            "tick_restore_coordinator",
            async {
                dispatch_backend!(self, |backend| {
                    backend
                        .tick_restore_coordinator(
                            instance_id,
                            local_work_is_idle,
                            expire_validated_jobs,
                        )
                        .await
                })
            },
        )
        .await
    }

    async fn get_restore_drain_state(
        &self,
        heartbeat_cutoff: DateTime<Utc>,
    ) -> Result<StorageRestoreDrainState, StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::Restore,
            "get_restore_drain_state",
            async {
                dispatch_backend!(self, |backend| {
                    backend.get_restore_drain_state(heartbeat_cutoff).await
                })
            },
        )
        .await
    }

    async fn remove_restore_instance(&self, instance_id: Uuid) -> Result<(), StorageError> {
        self.observe_storage_call(
            self.backend_name(),
            StorageCapability::Restore,
            "remove_restore_instance",
            async {
                dispatch_backend!(self, |backend| {
                    backend.remove_restore_instance(instance_id).await
                })
            },
        )
        .await
    }
}
