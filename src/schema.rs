// @generated automatically by Diesel CLI.

diesel::table! {
    backup_task_outputs (id) {
        id -> Int4,
        task_id -> Int4,
        document -> Bytea,
        byte_size -> Int8,
        #[max_length = 64]
        sha256 -> Varchar,
        output_expires_at -> Timestamp,
        created_at -> Timestamp,
    }
}

diesel::table! {
    collection_closure (ancestor_collection_id, descendant_collection_id) {
        ancestor_collection_id -> Int4,
        descendant_collection_id -> Int4,
        depth -> Int4,
    }
}

diesel::table! {
    collections (id) {
        id -> Int4,
        name -> Varchar,
        description -> Varchar,
        created_at -> Timestamp,
        updated_at -> Timestamp,
        parent_collection_id -> Nullable<Int4>,
    }
}

diesel::table! {
    collections_history (history_id) {
        id -> Int4,
        name -> Varchar,
        description -> Varchar,
        created_at -> Timestamp,
        updated_at -> Timestamp,
        parent_collection_id -> Nullable<Int4>,
        op -> Varchar,
        valid_from -> Timestamptz,
        valid_to -> Nullable<Timestamptz>,
        actor_id -> Nullable<Int4>,
        history_id -> Int8,
    }
}

diesel::table! {
    event_deliveries (id) {
        id -> Int8,
        event_id -> Int8,
        subscription_id -> Int4,
        status -> Varchar,
        attempts -> Int4,
        next_attempt_at -> Timestamp,
        last_error -> Nullable<Text>,
        locked_until -> Nullable<Timestamp>,
        claim_token -> Nullable<Uuid>,
        created_at -> Timestamp,
        updated_at -> Timestamp,
    }
}

diesel::table! {
    event_related_collections (event_id, collection_id) {
        event_id -> Int8,
        collection_id -> Int4,
    }
}

diesel::table! {
    event_sinks (id) {
        id -> Int4,
        name -> Varchar,
        kind -> Varchar,
        config -> Jsonb,
        secret_ref -> Nullable<Varchar>,
        enabled -> Bool,
        created_at -> Timestamp,
        updated_at -> Timestamp,
    }
}

diesel::table! {
    event_subscriptions (id) {
        id -> Int4,
        collection_id -> Int4,
        sink_id -> Int4,
        name -> Varchar,
        description -> Varchar,
        entity_types -> Jsonb,
        actions -> Jsonb,
        filter -> Jsonb,
        routing -> Jsonb,
        enabled -> Bool,
        created_at -> Timestamp,
        updated_at -> Timestamp,
    }
}

diesel::table! {
    events (id) {
        id -> Int8,
        event_id -> Uuid,
        occurred_at -> Timestamp,
        entity_type -> Text,
        entity_id -> Nullable<Int4>,
        entity_name -> Nullable<Text>,
        collection_id -> Nullable<Int4>,
        action -> Text,
        actor_user_id -> Nullable<Int4>,
        actor_kind -> Text,
        request_id -> Nullable<Uuid>,
        correlation_id -> Nullable<Text>,
        summary -> Text,
        before -> Nullable<Jsonb>,
        after -> Nullable<Jsonb>,
        metadata -> Jsonb,
        schema_version -> Int4,
        dispatched_at -> Nullable<Timestamp>,
        fanout_locked_until -> Nullable<Timestamp>,
        fanout_claim_token -> Nullable<Uuid>,
    }
}

diesel::table! {
    export_task_outputs (id) {
        id -> Int4,
        task_id -> Int4,
        template_name -> Nullable<Varchar>,
        content_type -> Varchar,
        json_output -> Nullable<Jsonb>,
        text_output -> Nullable<Text>,
        meta_json -> Jsonb,
        warnings_json -> Jsonb,
        warning_count -> Int4,
        truncated -> Bool,
        output_expires_at -> Timestamp,
        total_duration_ms -> Int4,
        query_duration_ms -> Int4,
        hydration_duration_ms -> Int4,
        render_duration_ms -> Int4,
        created_at -> Timestamp,
    }
}

diesel::table! {
    export_templates (id) {
        id -> Int4,
        collection_id -> Int4,
        name -> Varchar,
        description -> Varchar,
        content_type -> Varchar,
        template -> Text,
        kind -> Varchar,
        scope_kind -> Nullable<Varchar>,
        class_id -> Nullable<Int4>,
        default_query -> Nullable<Text>,
        include -> Nullable<Jsonb>,
        relation_context -> Nullable<Jsonb>,
        default_missing_data_policy -> Nullable<Varchar>,
        default_limits -> Nullable<Jsonb>,
        created_at -> Timestamp,
        updated_at -> Timestamp,
    }
}

diesel::table! {
    export_templates_history (history_id) {
        id -> Int4,
        collection_id -> Int4,
        name -> Varchar,
        description -> Varchar,
        content_type -> Varchar,
        template -> Text,
        kind -> Varchar,
        scope_kind -> Nullable<Varchar>,
        class_id -> Nullable<Int4>,
        default_query -> Nullable<Text>,
        include -> Nullable<Jsonb>,
        relation_context -> Nullable<Jsonb>,
        default_missing_data_policy -> Nullable<Varchar>,
        default_limits -> Nullable<Jsonb>,
        created_at -> Timestamp,
        updated_at -> Timestamp,
        op -> Varchar,
        valid_from -> Timestamptz,
        valid_to -> Nullable<Timestamptz>,
        actor_id -> Nullable<Int4>,
        history_id -> Int8,
    }
}

diesel::table! {
    group_membership_sources (principal_id, group_id, source, source_scope_id, source_key) {
        principal_id -> Int4,
        group_id -> Int4,
        source -> Varchar,
        source_scope_id -> Int4,
        source_key -> Varchar,
        created_at -> Timestamp,
        updated_at -> Timestamp,
    }
}

diesel::table! {
    group_memberships (principal_id, group_id) {
        principal_id -> Int4,
        group_id -> Int4,
        created_at -> Timestamp,
        updated_at -> Timestamp,
    }
}

diesel::table! {
    groups (id) {
        id -> Int4,
        groupname -> Varchar,
        description -> Varchar,
        created_at -> Timestamp,
        updated_at -> Timestamp,
        identity_scope_id -> Int4,
        managed_by -> Varchar,
        external_key -> Nullable<Varchar>,
        last_sync_attempted_at -> Nullable<Timestamp>,
        last_sync_success_at -> Nullable<Timestamp>,
    }
}

diesel::table! {
    hubuumclass (id) {
        id -> Int4,
        name -> Varchar,
        collection_id -> Int4,
        json_schema -> Nullable<Jsonb>,
        validate_schema -> Bool,
        description -> Varchar,
        created_at -> Timestamp,
        updated_at -> Timestamp,
    }
}

diesel::table! {
    hubuumclass_history (history_id) {
        id -> Int4,
        name -> Varchar,
        collection_id -> Int4,
        json_schema -> Nullable<Jsonb>,
        validate_schema -> Bool,
        description -> Varchar,
        created_at -> Timestamp,
        updated_at -> Timestamp,
        op -> Varchar,
        valid_from -> Timestamptz,
        valid_to -> Nullable<Timestamptz>,
        actor_id -> Nullable<Int4>,
        history_id -> Int8,
    }
}

diesel::table! {
    hubuumclass_reachability (id) {
        id -> Int8,
        ancestor_class_id -> Int4,
        descendant_class_id -> Int4,
        depth -> Int4,
        path -> Array<Nullable<Int4>>,
    }
}

diesel::table! {
    hubuumclass_relation (id) {
        id -> Int4,
        from_hubuum_class_id -> Int4,
        to_hubuum_class_id -> Int4,
        forward_template_alias -> Nullable<Varchar>,
        reverse_template_alias -> Nullable<Varchar>,
        created_at -> Timestamp,
        updated_at -> Timestamp,
    }
}

diesel::table! {
    hubuumclass_relation_history (history_id) {
        id -> Int4,
        from_hubuum_class_id -> Int4,
        to_hubuum_class_id -> Int4,
        forward_template_alias -> Nullable<Varchar>,
        reverse_template_alias -> Nullable<Varchar>,
        created_at -> Timestamp,
        updated_at -> Timestamp,
        op -> Varchar,
        valid_from -> Timestamptz,
        valid_to -> Nullable<Timestamptz>,
        actor_id -> Nullable<Int4>,
        history_id -> Int8,
    }
}

diesel::table! {
    hubuumobject (id) {
        id -> Int4,
        name -> Varchar,
        collection_id -> Int4,
        hubuum_class_id -> Int4,
        data -> Jsonb,
        description -> Varchar,
        created_at -> Timestamp,
        updated_at -> Timestamp,
    }
}

diesel::table! {
    hubuumobject_history (history_id) {
        id -> Int4,
        name -> Varchar,
        collection_id -> Int4,
        hubuum_class_id -> Int4,
        data -> Jsonb,
        description -> Varchar,
        created_at -> Timestamp,
        updated_at -> Timestamp,
        op -> Varchar,
        valid_from -> Timestamptz,
        valid_to -> Nullable<Timestamptz>,
        actor_id -> Nullable<Int4>,
        history_id -> Int8,
    }
}

diesel::table! {
    hubuumobject_relation (id) {
        id -> Int4,
        from_hubuum_object_id -> Int4,
        to_hubuum_object_id -> Int4,
        class_relation_id -> Int4,
        created_at -> Timestamp,
        updated_at -> Timestamp,
    }
}

diesel::table! {
    hubuumobject_relation_history (history_id) {
        id -> Int4,
        from_hubuum_object_id -> Int4,
        to_hubuum_object_id -> Int4,
        class_relation_id -> Int4,
        created_at -> Timestamp,
        updated_at -> Timestamp,
        op -> Varchar,
        valid_from -> Timestamptz,
        valid_to -> Nullable<Timestamptz>,
        actor_id -> Nullable<Int4>,
        history_id -> Int8,
    }
}

diesel::table! {
    identity_scopes (id) {
        id -> Int4,
        name -> Varchar,
        provider_kind -> Varchar,
        created_at -> Timestamp,
        updated_at -> Timestamp,
    }
}

diesel::table! {
    import_task_results (id) {
        id -> Int4,
        task_id -> Int4,
        item_ref -> Nullable<Varchar>,
        entity_kind -> Varchar,
        action -> Varchar,
        identifier -> Nullable<Text>,
        outcome -> Varchar,
        error -> Nullable<Text>,
        details -> Nullable<Jsonb>,
        created_at -> Timestamp,
    }
}

diesel::table! {
    permissions (id) {
        id -> Int4,
        collection_id -> Int4,
        group_id -> Int4,
        has_read_collection -> Bool,
        has_update_collection -> Bool,
        has_delete_collection -> Bool,
        has_delegate_collection -> Bool,
        has_create_class -> Bool,
        has_read_class -> Bool,
        has_update_class -> Bool,
        has_delete_class -> Bool,
        has_create_object -> Bool,
        has_read_object -> Bool,
        has_update_object -> Bool,
        has_delete_object -> Bool,
        has_create_class_relation -> Bool,
        has_read_class_relation -> Bool,
        has_update_class_relation -> Bool,
        has_delete_class_relation -> Bool,
        has_create_object_relation -> Bool,
        has_read_object_relation -> Bool,
        has_update_object_relation -> Bool,
        has_delete_object_relation -> Bool,
        has_read_template -> Bool,
        has_create_template -> Bool,
        has_update_template -> Bool,
        has_delete_template -> Bool,
        has_read_remote_target -> Bool,
        has_create_remote_target -> Bool,
        has_update_remote_target -> Bool,
        has_delete_remote_target -> Bool,
        has_execute_remote_target -> Bool,
        created_at -> Timestamp,
        updated_at -> Timestamp,
        has_read_audit -> Bool,
        has_manage_event_subscription -> Bool,
    }
}

diesel::table! {
    principals (id) {
        id -> Int4,
        kind -> Varchar,
        name -> Varchar,
        created_at -> Timestamp,
        updated_at -> Timestamp,
        identity_scope_id -> Int4,
        provider_managed -> Bool,
        settings -> Jsonb,
        external_subject -> Nullable<Varchar>,
        last_sync_attempted_at -> Nullable<Timestamp>,
        last_sync_success_at -> Nullable<Timestamp>,
    }
}

diesel::table! {
    remote_call_results (id) {
        id -> Int4,
        task_id -> Int4,
        target_id -> Nullable<Int4>,
        subject_type -> Varchar,
        subject_id -> Int4,
        method -> Varchar,
        rendered_url -> Text,
        response_status -> Nullable<Int4>,
        response_headers -> Nullable<Jsonb>,
        response_body_preview -> Nullable<Text>,
        duration_ms -> Int4,
        success -> Bool,
        error -> Nullable<Text>,
        created_at -> Timestamp,
    }
}

diesel::table! {
    remote_targets (id) {
        id -> Int4,
        collection_id -> Int4,
        class_id -> Nullable<Int4>,
        name -> Varchar,
        description -> Varchar,
        method -> Varchar,
        url_template -> Text,
        headers_template -> Jsonb,
        body_template -> Nullable<Text>,
        auth_config -> Jsonb,
        allowed_subject_types -> Jsonb,
        timeout_ms -> Int4,
        enabled -> Bool,
        created_at -> Timestamp,
        updated_at -> Timestamp,
    }
}

diesel::table! {
    remote_targets_history (history_id) {
        id -> Int4,
        collection_id -> Int4,
        class_id -> Nullable<Int4>,
        name -> Varchar,
        description -> Varchar,
        method -> Varchar,
        url_template -> Text,
        headers_template -> Jsonb,
        body_template -> Nullable<Text>,
        auth_config -> Jsonb,
        allowed_subject_types -> Jsonb,
        timeout_ms -> Int4,
        enabled -> Bool,
        created_at -> Timestamp,
        updated_at -> Timestamp,
        op -> Varchar,
        valid_from -> Timestamptz,
        valid_to -> Nullable<Timestamptz>,
        actor_id -> Nullable<Int4>,
        history_id -> Int8,
    }
}

diesel::table! {
    restore_jobs (id) {
        id -> Int8,
        status -> Varchar,
        requested_by -> Nullable<Int4>,
        requested_by_identity_scope -> Varchar,
        requested_by_name -> Varchar,
        document -> Bytea,
        byte_size -> Int8,
        #[max_length = 64]
        sha256 -> Varchar,
        #[max_length = 64]
        capability_hash -> Varchar,
        validation_summary -> Jsonb,
        error -> Nullable<Text>,
        expires_at -> Timestamp,
        confirmed_at -> Nullable<Timestamp>,
        finished_at -> Nullable<Timestamp>,
        created_at -> Timestamp,
        updated_at -> Timestamp,
    }
}

diesel::table! {
    server_instances (instance_id) {
        instance_id -> Uuid,
        maintenance_generation -> Int8,
        drained -> Bool,
        last_heartbeat_at -> Timestamp,
        started_at -> Timestamp,
    }
}

diesel::table! {
    service_accounts (id) {
        id -> Int4,
        kind -> Varchar,
        description -> Varchar,
        owner_group_id -> Int4,
        created_by -> Nullable<Int4>,
        disabled_at -> Nullable<Timestamp>,
        created_at -> Timestamp,
        updated_at -> Timestamp,
    }
}

diesel::table! {
    system_maintenance (id) {
        id -> Int2,
        generation -> Int8,
        state -> Varchar,
        restore_job_id -> Nullable<Int8>,
        entered_at -> Nullable<Timestamp>,
        updated_at -> Timestamp,
    }
}

diesel::table! {
    tasks (id) {
        id -> Int4,
        kind -> Varchar,
        status -> Varchar,
        submitted_by -> Nullable<Int4>,
        idempotency_key -> Nullable<Varchar>,
        request_hash -> Nullable<Varchar>,
        request_payload -> Nullable<Jsonb>,
        summary -> Nullable<Text>,
        total_items -> Int4,
        processed_items -> Int4,
        success_items -> Int4,
        failed_items -> Int4,
        submitted_token_id -> Nullable<Int4>,
        submitted_token_scoped -> Bool,
        submitted_token_scopes -> Jsonb,
        request_redacted_at -> Nullable<Timestamp>,
        started_at -> Nullable<Timestamp>,
        finished_at -> Nullable<Timestamp>,
        deleted_at -> Nullable<Timestamp>,
        deleted_by -> Nullable<Int4>,
        created_at -> Timestamp,
        updated_at -> Timestamp,
        lease_token -> Nullable<Uuid>,
        lease_expires_at -> Nullable<Timestamp>,
        attempt_count -> Int4,
    }
}

diesel::table! {
    token_scopes (token_id, permission) {
        token_id -> Int4,
        permission -> Varchar,
    }
}

diesel::table! {
    tokens (id) {
        id -> Int4,
        token -> Varchar,
        principal_id -> Int4,
        name -> Nullable<Varchar>,
        description -> Nullable<Varchar>,
        issued -> Timestamp,
        expires_at -> Nullable<Timestamp>,
        last_used_at -> Nullable<Timestamp>,
        revoked_at -> Nullable<Timestamp>,
        scoped -> Bool,
    }
}

diesel::table! {
    users (id) {
        id -> Int4,
        kind -> Varchar,
        password -> Nullable<Varchar>,
        proper_name -> Nullable<Varchar>,
        email -> Nullable<Varchar>,
        created_at -> Timestamp,
        updated_at -> Timestamp,
        anonymized_at -> Nullable<Timestamp>,
    }
}

diesel::joinable!(backup_task_outputs -> tasks (task_id));
diesel::joinable!(event_deliveries -> event_subscriptions (subscription_id));
diesel::joinable!(event_deliveries -> events (event_id));
diesel::joinable!(event_related_collections -> events (event_id));
diesel::joinable!(event_subscriptions -> collections (collection_id));
diesel::joinable!(event_subscriptions -> event_sinks (sink_id));
diesel::joinable!(export_task_outputs -> tasks (task_id));
diesel::joinable!(export_templates -> collections (collection_id));
diesel::joinable!(export_templates -> hubuumclass (class_id));
diesel::joinable!(group_membership_sources -> groups (group_id));
diesel::joinable!(group_membership_sources -> identity_scopes (source_scope_id));
diesel::joinable!(group_membership_sources -> principals (principal_id));
diesel::joinable!(group_memberships -> groups (group_id));
diesel::joinable!(group_memberships -> principals (principal_id));
diesel::joinable!(groups -> identity_scopes (identity_scope_id));
diesel::joinable!(hubuumclass -> collections (collection_id));
diesel::joinable!(hubuumobject -> collections (collection_id));
diesel::joinable!(hubuumobject -> hubuumclass (hubuum_class_id));
diesel::joinable!(hubuumobject_relation -> hubuumclass_relation (class_relation_id));
diesel::joinable!(import_task_results -> tasks (task_id));
diesel::joinable!(permissions -> collections (collection_id));
diesel::joinable!(permissions -> groups (group_id));
diesel::joinable!(principals -> identity_scopes (identity_scope_id));
diesel::joinable!(remote_call_results -> remote_targets (target_id));
diesel::joinable!(remote_call_results -> tasks (task_id));
diesel::joinable!(remote_targets -> collections (collection_id));
diesel::joinable!(remote_targets -> hubuumclass (class_id));
diesel::joinable!(service_accounts -> groups (owner_group_id));
diesel::joinable!(system_maintenance -> restore_jobs (restore_job_id));
diesel::joinable!(tasks -> tokens (submitted_token_id));
diesel::joinable!(token_scopes -> tokens (token_id));
diesel::joinable!(tokens -> principals (principal_id));

diesel::allow_tables_to_appear_in_same_query!(
    backup_task_outputs,
    collection_closure,
    collections,
    collections_history,
    event_deliveries,
    event_related_collections,
    event_sinks,
    event_subscriptions,
    events,
    export_task_outputs,
    export_templates,
    export_templates_history,
    group_membership_sources,
    group_memberships,
    groups,
    hubuumclass,
    hubuumclass_history,
    hubuumclass_reachability,
    hubuumclass_relation,
    hubuumclass_relation_history,
    hubuumobject,
    hubuumobject_history,
    hubuumobject_relation,
    hubuumobject_relation_history,
    identity_scopes,
    import_task_results,
    permissions,
    principals,
    remote_call_results,
    remote_targets,
    remote_targets_history,
    restore_jobs,
    server_instances,
    service_accounts,
    system_maintenance,
    tasks,
    token_scopes,
    tokens,
    users,
);
