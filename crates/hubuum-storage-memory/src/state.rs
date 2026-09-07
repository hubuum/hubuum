use super::*;

impl MemoryState {
    pub(super) fn new() -> Self {
        let now = Utc::now();
        let metadata = StorageRecordMetadata::try_new(
            ResourceId::new(ROOT_COLLECTION_ID).expect("root resource id is valid"),
            now,
            now,
            ResourceRevision::INITIAL,
        )
        .expect("root collection metadata is valid");
        let root = StorageCollection::try_new(metadata, "root", "Root collection", None)
            .expect("root collection is valid");
        let local_scope_id = IdentityScopeId::new(1).expect("local identity scope id is valid");
        let local_scope = StorageIdentityScope::try_new(
            local_scope_id,
            LOCAL_IDENTITY_SCOPE,
            LOCAL_PROVIDER_KIND,
            now,
            now,
            ResourceRevision::INITIAL,
        )
        .expect("local identity scope is valid");
        let admin_principal_id = PrincipalId::new(1).expect("admin principal id is valid");
        let admin_metadata = StorageRecordMetadata::try_new(
            ResourceId::new(admin_principal_id.id()).expect("admin resource id is valid"),
            now,
            now,
            ResourceRevision::INITIAL,
        )
        .expect("admin principal metadata is valid");
        let admin_principal = StoragePrincipal::builder(
            admin_metadata,
            PrincipalKind::Human,
            "admin",
            local_scope_id,
        )
        .try_build()
        .expect("admin principal is valid");
        let admin_user = StorageUser::try_new(
            UserId::new(admin_principal_id.id()).expect("admin user id is valid"),
            Some("memory-adapter-placeholder-password-hash".to_string()),
            Some("Administrator".to_string()),
            None,
            now,
            now,
            None,
        )
        .expect("admin user is valid");
        let admin_group_id = GroupId::new(1).expect("admin group id is valid");
        let admin_group_metadata = StorageRecordMetadata::try_new(
            ResourceId::new(admin_group_id.id()).expect("admin group resource id is valid"),
            now,
            now,
            ResourceRevision::INITIAL,
        )
        .expect("admin group metadata is valid");
        let admin_group = StorageIdentityGroup::builder(
            admin_group_metadata,
            "admin",
            "Administrators",
            local_scope_id,
            LOCAL_PROVIDER_KIND,
        )
        .try_build()
        .expect("admin group is valid");
        let admin_membership = StoragePrincipalGroup::try_new(
            admin_principal_id,
            admin_group_id,
            now,
            now,
            ResourceRevision::INITIAL,
        )
        .expect("admin group membership is valid");
        Self {
            next_collection_id: ROOT_COLLECTION_ID + 1,
            next_class_id: 1,
            next_object_id: 1,
            next_class_relation_id: 1,
            next_object_relation_id: 1,
            next_event_sequence: 1,
            next_identity_scope_id: 2,
            next_principal_id: 2,
            next_group_id: 2,
            next_token_id: 1,
            next_task_id: 1,
            next_task_event_sequence: 1,
            next_import_result_id: 1,
            import_execution_receipts: BTreeSet::new(),
            next_computed_field_id: 1,
            next_export_template_id: 1,
            next_remote_target_id: 1,
            next_authorization_grant_id: 1,
            next_event_sink_id: 1,
            next_event_subscription_id: 1,
            next_event_delivery_id: 1,
            next_history_id: 1,
            next_restore_job_id: 1,
            fanout_event_cursor: 0,
            collections: BTreeMap::from([(ROOT_COLLECTION_ID, root)]),
            classes: BTreeMap::new(),
            objects: BTreeMap::new(),
            class_relations: BTreeMap::new(),
            object_relations: BTreeMap::new(),
            identity_scopes: BTreeMap::from([(local_scope_id.id(), local_scope)]),
            principals: BTreeMap::from([(admin_principal_id.id(), admin_principal)]),
            users: BTreeMap::from([(
                admin_principal_id.id(),
                MemoryUserRecord {
                    user: admin_user,
                    identity_scope_id: local_scope_id,
                    name: "admin".to_string(),
                    provider_managed: false,
                    external_subject: None,
                    last_sync_attempted_at: None,
                    last_sync_success_at: None,
                },
            )]),
            groups: BTreeMap::from([(admin_group_id.id(), admin_group)]),
            memberships: BTreeMap::from([(
                (admin_principal_id.id(), admin_group_id.id()),
                admin_membership,
            )]),
            external_memberships: BTreeSet::new(),
            tokens: BTreeMap::new(),
            service_accounts: BTreeMap::new(),
            tasks: BTreeMap::new(),
            task_events: BTreeMap::new(),
            import_task_results: BTreeMap::new(),
            export_outputs: BTreeMap::new(),
            backup_outputs: BTreeMap::new(),
            export_templates: BTreeMap::new(),
            remote_targets: BTreeMap::new(),
            computed_fields: BTreeMap::new(),
            computation_states: BTreeMap::new(),
            computed_rebuild_tasks: BTreeMap::new(),
            authorization_grants: BTreeMap::new(),
            event_sinks: BTreeMap::new(),
            event_subscriptions: BTreeMap::new(),
            event_deliveries: BTreeMap::new(),
            event_delivery_claims: BTreeMap::new(),
            event_retention_batches: BTreeMap::new(),
            history: Vec::new(),
            restore_jobs: BTreeMap::new(),
            maintenance_state: MaintenanceState::Normal,
            maintenance_restore_job_id: None,
            maintenance_generation: 0,
            restore_instances: BTreeMap::new(),
            events: Vec::new(),
        }
    }
}
