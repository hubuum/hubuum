use super::*;

#[async_trait]
impl ExportTemplateStorage for StorageHandle {
    async fn get_export_template(
        &self,
        template_id: i32,
    ) -> Result<StorageExportTemplate, StorageError> {
        observe_storage_call(self.backend_name(), "export_templates", "get", async {
            dispatch_backend!(self, |backend| {
                backend.get_export_template(template_id).await
            })
        })
        .await
    }

    async fn list_export_templates(
        &self,
        query: StorageExportTemplateListQuery,
    ) -> Result<StorageExportTemplatePage, StorageError> {
        observe_storage_call(self.backend_name(), "export_templates", "list", async {
            dispatch_backend!(self, |backend| {
                backend.list_export_templates(query).await
            })
        })
        .await
    }

    async fn list_export_templates_in_collection(
        &self,
        collection_id: i32,
        exclude_template_id: Option<i32>,
    ) -> Result<Vec<StorageExportTemplate>, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "export_templates",
            "list_in_collection",
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

    async fn export_template_class_collection_id(
        &self,
        class_id: i32,
    ) -> Result<Option<i32>, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "export_templates",
            "class_collection",
            async {
                dispatch_backend!(self, |backend| {
                    backend.export_template_class_collection_id(class_id).await
                })
            },
        )
        .await
    }

    async fn create_export_template(
        &self,
        request: StorageExportTemplateCreate,
    ) -> Result<StorageExportTemplate, StorageError> {
        observe_storage_call(self.backend_name(), "export_templates", "create", async {
            dispatch_backend!(self, |backend| {
                backend.create_export_template(request).await
            })
        })
        .await
    }

    async fn replace_export_template(
        &self,
        request: StorageExportTemplateReplace,
    ) -> Result<StorageExportTemplate, StorageError> {
        observe_storage_call(self.backend_name(), "export_templates", "replace", async {
            dispatch_backend!(self, |backend| {
                backend.replace_export_template(request).await
            })
        })
        .await
    }

    async fn delete_export_template(
        &self,
        request: StorageExportTemplateDelete,
    ) -> Result<(), StorageError> {
        observe_storage_call(self.backend_name(), "export_templates", "delete", async {
            dispatch_backend!(self, |backend| {
                backend.delete_export_template(request).await
            })
        })
        .await
    }
}

#[async_trait]
impl RemoteTargetStorage for StorageHandle {
    async fn get_remote_target(&self, target_id: i32) -> Result<StorageRemoteTarget, StorageError> {
        observe_storage_call(self.backend_name(), "remote_targets", "get", async {
            dispatch_backend!(self, |backend| {
                backend.get_remote_target(target_id).await
            })
        })
        .await
    }

    async fn list_remote_targets(
        &self,
        query: StorageRemoteTargetListQuery,
    ) -> Result<StorageRemoteTargetPage, StorageError> {
        observe_storage_call(self.backend_name(), "remote_targets", "list", async {
            dispatch_backend!(self, |backend| { backend.list_remote_targets(query).await })
        })
        .await
    }

    async fn create_remote_target(
        &self,
        request: StorageRemoteTargetCreate,
    ) -> Result<StorageRemoteTarget, StorageError> {
        observe_storage_call(self.backend_name(), "remote_targets", "create", async {
            dispatch_backend!(self, |backend| {
                backend.create_remote_target(request).await
            })
        })
        .await
    }

    async fn update_remote_target(
        &self,
        request: StorageRemoteTargetUpdate,
    ) -> Result<StorageRemoteTarget, StorageError> {
        observe_storage_call(self.backend_name(), "remote_targets", "update", async {
            dispatch_backend!(self, |backend| {
                backend.update_remote_target(request).await
            })
        })
        .await
    }

    async fn delete_remote_target(
        &self,
        request: StorageRemoteTargetDelete,
    ) -> Result<(), StorageError> {
        observe_storage_call(self.backend_name(), "remote_targets", "delete", async {
            dispatch_backend!(self, |backend| {
                backend.delete_remote_target(request).await
            })
        })
        .await
    }

    async fn record_remote_target_invocation(
        &self,
        request: StorageRemoteTargetInvocation,
    ) -> Result<(), StorageError> {
        observe_storage_call(
            self.backend_name(),
            "remote_targets",
            "record_invocation",
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
    async fn import_root_collection(&self) -> Result<StorageCollection, StorageError> {
        observe_storage_call(self.backend_name(), "imports", "root_collection", async {
            dispatch_backend!(self, |backend| { backend.import_root_collection().await })
        })
        .await
    }

    async fn import_collection_by_id(
        &self,
        collection_id: i32,
    ) -> Result<Option<StorageCollection>, StorageError> {
        observe_storage_call(self.backend_name(), "imports", "collection_by_id", async {
            dispatch_backend!(self, |backend| {
                backend.import_collection_by_id(collection_id).await
            })
        })
        .await
    }

    async fn import_collection_by_key(
        &self,
        key: &StorageImportCollectionKey,
    ) -> Result<Option<StorageCollection>, StorageError> {
        observe_storage_call(self.backend_name(), "imports", "collection_by_key", async {
            dispatch_backend!(self, |backend| {
                backend.import_collection_by_key(key).await
            })
        })
        .await
    }

    async fn import_collections_by_name(
        &self,
        name: &str,
    ) -> Result<Vec<StorageCollection>, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "imports",
            "collections_by_name",
            async {
                dispatch_backend!(self, |backend| {
                    backend.import_collections_by_name(name).await
                })
            },
        )
        .await
    }

    async fn import_collection_child_by_name(
        &self,
        parent_collection_id: i32,
        name: &str,
    ) -> Result<Option<StorageCollection>, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "imports",
            "collection_child_by_name",
            async {
                dispatch_backend!(self, |backend| {
                    backend
                        .import_collection_child_by_name(parent_collection_id, name)
                        .await
                })
            },
        )
        .await
    }

    async fn import_class_by_name(
        &self,
        collection_id: i32,
        name: &str,
    ) -> Result<Option<StorageClassRecord>, StorageError> {
        observe_storage_call(self.backend_name(), "imports", "class_by_name", async {
            dispatch_backend!(self, |backend| {
                backend.import_class_by_name(collection_id, name).await
            })
        })
        .await
    }

    async fn import_classes_by_names(
        &self,
        collection_id: i32,
        names: &[String],
    ) -> Result<Vec<StorageClassRecord>, StorageError> {
        observe_storage_call(self.backend_name(), "imports", "classes_by_names", async {
            dispatch_backend!(self, |backend| {
                backend.import_classes_by_names(collection_id, names).await
            })
        })
        .await
    }

    async fn import_object_by_name(
        &self,
        class_id: i32,
        name: &str,
    ) -> Result<Option<StorageObject>, StorageError> {
        observe_storage_call(self.backend_name(), "imports", "object_by_name", async {
            dispatch_backend!(self, |backend| {
                backend.import_object_by_name(class_id, name).await
            })
        })
        .await
    }

    async fn import_objects_by_names(
        &self,
        class_id: i32,
        names: &[String],
    ) -> Result<Vec<StorageObject>, StorageError> {
        observe_storage_call(self.backend_name(), "imports", "objects_by_names", async {
            dispatch_backend!(self, |backend| {
                backend.import_objects_by_names(class_id, names).await
            })
        })
        .await
    }

    async fn import_class_relation_exists(
        &self,
        left_class_id: i32,
        right_class_id: i32,
    ) -> Result<bool, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "imports",
            "class_relation_exists",
            async {
                dispatch_backend!(self, |backend| {
                    backend
                        .import_class_relation_exists(left_class_id, right_class_id)
                        .await
                })
            },
        )
        .await
    }

    async fn import_object_relation_exists(
        &self,
        left_object_id: i32,
        right_object_id: i32,
    ) -> Result<bool, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "imports",
            "object_relation_exists",
            async {
                dispatch_backend!(self, |backend| {
                    backend
                        .import_object_relation_exists(left_object_id, right_object_id)
                        .await
                })
            },
        )
        .await
    }

    async fn import_group_exists(
        &self,
        identity_scope: &str,
        group_name: &str,
    ) -> Result<bool, StorageError> {
        observe_storage_call(self.backend_name(), "imports", "group_exists", async {
            dispatch_backend!(self, |backend| {
                backend
                    .import_group_exists(identity_scope, group_name)
                    .await
            })
        })
        .await
    }

    async fn preflight_import(
        &self,
        plan: StorageImportPlan,
        mode: StorageImportMode,
    ) -> Result<StorageImportPreflight, StorageError> {
        observe_storage_call(self.backend_name(), "imports", "preflight", async {
            dispatch_backend!(self, |backend| {
                backend.preflight_import(plan, mode).await
            })
        })
        .await
    }

    async fn apply_import_strict(&self, plan: StorageImportPlan) -> Result<(), StorageError> {
        observe_storage_call(self.backend_name(), "imports", "apply_strict", async {
            dispatch_backend!(self, |backend| { backend.apply_import_strict(plan).await })
        })
        .await
    }

    async fn apply_import_best_effort(
        &self,
        plan: StorageImportPlan,
        mode: StorageImportMode,
    ) -> Result<StorageImportApply, StorageError> {
        observe_storage_call(self.backend_name(), "imports", "apply_best_effort", async {
            dispatch_backend!(self, |backend| {
                backend.apply_import_best_effort(plan, mode).await
            })
        })
        .await
    }

    async fn record_import_results(
        &self,
        results: Vec<StorageImportResult>,
    ) -> Result<(), StorageError> {
        observe_storage_call(self.backend_name(), "imports", "record_results", async {
            dispatch_backend!(self, |backend| {
                backend.record_import_results(results).await
            })
        })
        .await
    }
}

#[async_trait]
impl RestoreStorage for StorageHandle {
    async fn stage_restore(
        &self,
        request: StorageRestoreStageCreate,
    ) -> Result<StorageRestoreJob, StorageError> {
        observe_storage_call(self.backend_name(), "restores", "stage", async {
            dispatch_backend!(self, |backend| backend.stage_restore(request).await)
        })
        .await
    }

    async fn get_restore_job(&self, job_id: i64) -> Result<StorageRestoreJob, StorageError> {
        observe_storage_call(self.backend_name(), "restores", "get_job", async {
            dispatch_backend!(self, |backend| backend.get_restore_job(job_id).await)
        })
        .await
    }

    async fn get_restore_status(&self, job_id: i64) -> Result<StorageRestoreStatus, StorageError> {
        observe_storage_call(self.backend_name(), "restores", "get_status", async {
            dispatch_backend!(self, |backend| { backend.get_restore_status(job_id).await })
        })
        .await
    }

    async fn expire_restore_stage(&self, job_id: i64) -> Result<bool, StorageError> {
        observe_storage_call(self.backend_name(), "restores", "expire", async {
            dispatch_backend!(self, |backend| {
                backend.expire_restore_stage(job_id).await
            })
        })
        .await
    }

    async fn start_restore_draining(&self, job_id: i64) -> Result<NaiveDateTime, StorageError> {
        observe_storage_call(self.backend_name(), "restores", "start_draining", async {
            dispatch_backend!(self, |backend| {
                backend.start_restore_draining(job_id).await
            })
        })
        .await
    }

    async fn apply_restore(
        &self,
        request: StorageRestoreApply,
    ) -> Result<StorageRestoreCompletion, StorageError> {
        observe_storage_call(self.backend_name(), "restores", "apply", async {
            dispatch_backend!(self, |backend| backend.apply_restore(request).await)
        })
        .await
    }

    async fn fail_restore_and_resume(
        &self,
        request: StorageRestoreFailure,
    ) -> Result<(), StorageError> {
        observe_storage_call(self.backend_name(), "restores", "fail_and_resume", async {
            dispatch_backend!(self, |backend| {
                backend.fail_restore_and_resume(request).await
            })
        })
        .await
    }

    async fn restore_coordinator_snapshot(
        &self,
    ) -> Result<StorageRestoreCoordinatorSnapshot, StorageError> {
        observe_storage_call(
            self.backend_name(),
            "restores",
            "coordinator_snapshot",
            async {
                dispatch_backend!(self, |backend| {
                    backend.restore_coordinator_snapshot().await
                })
            },
        )
        .await
    }

    async fn resume_maintenance_without_restore(&self) -> Result<(), StorageError> {
        observe_storage_call(
            self.backend_name(),
            "restores",
            "resume_without_job",
            async {
                dispatch_backend!(self, |backend| {
                    backend.resume_maintenance_without_restore().await
                })
            },
        )
        .await
    }

    async fn resume_terminal_restore(&self, job_id: i64) -> Result<(), StorageError> {
        observe_storage_call(self.backend_name(), "restores", "resume_terminal", async {
            dispatch_backend!(self, |backend| {
                backend.resume_terminal_restore(job_id).await
            })
        })
        .await
    }

    async fn tick_restore_coordinator(
        &self,
        instance_id: Uuid,
        local_work_is_idle: &(dyn Fn() -> bool + Send + Sync),
        expire_validated_jobs: bool,
    ) -> Result<StorageRestoreCoordinatorSnapshot, StorageError> {
        observe_storage_call(self.backend_name(), "restores", "tick", async {
            dispatch_backend!(self, |backend| {
                backend
                    .tick_restore_coordinator(
                        instance_id,
                        local_work_is_idle,
                        expire_validated_jobs,
                    )
                    .await
            })
        })
        .await
    }

    async fn restore_drain_state(
        &self,
        heartbeat_cutoff: NaiveDateTime,
    ) -> Result<StorageRestoreDrainState, StorageError> {
        observe_storage_call(self.backend_name(), "restores", "drain_state", async {
            dispatch_backend!(self, |backend| {
                backend.restore_drain_state(heartbeat_cutoff).await
            })
        })
        .await
    }

    async fn remove_restore_instance(&self, instance_id: Uuid) -> Result<(), StorageError> {
        observe_storage_call(self.backend_name(), "restores", "remove_instance", async {
            dispatch_backend!(self, |backend| {
                backend.remove_restore_instance(instance_id).await
            })
        })
        .await
    }
}
