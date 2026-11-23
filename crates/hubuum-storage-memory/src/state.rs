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
        let mut logical_identities = logical_identity::MemoryLogicalIdentities::default();
        logical_identities.capture(&local_scope);
        logical_identities.capture(&admin_principal);
        logical_identities.capture(&admin_group);
        logical_identities.capture(&root);
        let root_identity = logical_identities.history(
            &MemoryHistoryValue::Collection(root.clone()),
            StorageHistoryOperation::Create,
            None,
        );
        let deletion_control = DeletionControlState::empty(Uuid::new_v4(), Uuid::new_v4())
            .expect("generated control identities");
        let mut state = Self {
            task_control: TaskControlState::initial(
                deletion_control.lineage(),
                deletion_control.epoch(),
                TaskRetentionPolicy::disabled_default(),
            )
            .expect("generated task control identities"),
            task_reapplication: None,
            restore_repairs_pending: false,
            task_purge_attempts: BTreeMap::new(),
            next_task_purge_attempt: 1,
            task_capacity: Default::default(),
            api_requests: Default::default(),
            artifacts: Default::default(),
            history_archive: Default::default(),
            application_control: ApplicationControlState::initial(&deletion_control),
            pending_restore: false,
            deletion_control,
            runtime_registrations: BTreeMap::new(),
            logical_identities,
            schema_repair_reports: Default::default(),
            generation: Arc::new(()),
            schema_history: Vec::new(),
            schema_revisions: BTreeMap::new(),
            schema_active: BTreeMap::new(),
            schema_epochs: BTreeMap::new(),
            schema_evidence: BTreeMap::new(),
            schema_work: BTreeMap::new(),
            schema_findings: BTreeMap::new(),
            collection_ids: IdSequence::with_existing(ROOT_COLLECTION_ID),
            class_ids: IdSequence::default(),
            object_ids: IdSequence::default(),
            class_relation_ids: IdSequence::default(),
            object_relation_ids: IdSequence::default(),
            event_sequences: IdSequence::default(),
            identity_scope_ids: IdSequence::with_existing(1),
            principal_ids: IdSequence::with_existing(1),
            group_ids: IdSequence::with_existing(1),
            token_ids: IdSequence::default(),
            task_ids: IdSequence::default(),
            import_execution_receipts: BTreeSet::new(),
            computed_field_ids: IdSequence::default(),
            export_template_ids: IdSequence::default(),
            remote_target_ids: IdSequence::default(),
            authorization_grant_ids: IdSequence::default(),
            event_sink_ids: IdSequence::default(),
            event_subscription_ids: IdSequence::default(),
            delivery_ids: IdSequence::default(),
            history_ids: IdSequence::with_existing(1),
            restore_job_ids: IdSequence::default(),
            fanout_event_cursor: 0,
            collections: BTreeMap::from([(ROOT_COLLECTION_ID, root.clone())]),
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
            membership_sources: Vec::new(),
            authorization_revisions: BTreeMap::new(),
            tokens: BTreeMap::new(),
            sessions: BTreeMap::new(),
            credential_activity: BTreeMap::new(),
            credential_approvals: Default::default(),
            service_accounts: BTreeMap::new(),
            tasks: BTreeMap::new(),
            task_events: BTreeMap::new(),
            import_task_results: Default::default(),
            schema_profile_outputs: BTreeMap::new(),
            export_outputs: BTreeMap::new(),
            backup_outputs: BTreeMap::new(),
            export_templates: BTreeMap::new(),
            remote_targets: BTreeMap::new(),
            computed_fields: BTreeMap::new(),
            computed_history: Vec::new(),
            query_usage: BTreeMap::new(),
            runtime_settings: super::runtime_settings::MemoryRuntimeSettings::new(),
            query_usage_ids: IdSequence::default(),
            computation_states: BTreeMap::new(),
            computed_rebuild_tasks: BTreeMap::new(),
            authorization_grants: BTreeMap::new(),
            event_sinks: BTreeMap::new(),
            event_sink_grants: BTreeSet::new(),
            event_subscriptions: BTreeMap::new(),
            event_deliveries: Default::default(),
            event_sink_schedule: BTreeMap::new(),
            event_retention_batches: BTreeMap::new(),
            history: vec![MemoryHistoryEntry {
                logical_identity: root_identity,
                id: HistoryRecordId::new(1).expect("initial history id"),
                value: MemoryHistoryValue::Collection(root),
                operation: StorageHistoryOperation::Create,
                valid_from: now,
                valid_to: None,
                actor_id: None,
                actor_kind: Some("system".to_string()),
                initiator_principal_id: None,
                task_id: None,
            }],
            relation_history: BTreeMap::new(),
            remote_call_results: Vec::new(),
            export_output_ids: BTreeMap::new(),
            restore_jobs: BTreeMap::new(),
            restore_receipts: BTreeMap::new(),
            event_dispatched_at: BTreeMap::new(),
            restore_coordinator: Default::default(),
            events: Vec::new(),
        };
        let key = (admin_principal_id.id(), admin_group_id.id());
        let membership = state.memberships[&key].clone();
        state.store_membership(key, membership);
        state
    }
}
