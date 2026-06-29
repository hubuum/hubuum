// @generated automatically by Diesel CLI.

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
        namespace_id -> Int4,
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
        namespace_id -> Nullable<Int4>,
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
    }
}

diesel::table! {
    hubuumclass (id) {
        id -> Int4,
        name -> Varchar,
        namespace_id -> Int4,
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
        namespace_id -> Int4,
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
        namespace_id -> Int4,
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
        namespace_id -> Int4,
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
    namespaces (id) {
        id -> Int4,
        name -> Varchar,
        description -> Varchar,
        created_at -> Timestamp,
        updated_at -> Timestamp,
    }
}

diesel::table! {
    namespaces_history (history_id) {
        id -> Int4,
        name -> Varchar,
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
    permissions (id) {
        id -> Int4,
        namespace_id -> Int4,
        group_id -> Int4,
        has_read_namespace -> Bool,
        has_update_namespace -> Bool,
        has_delete_namespace -> Bool,
        has_delegate_namespace -> Bool,
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
        namespace_id -> Int4,
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
        namespace_id -> Int4,
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
    report_task_outputs (id) {
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
    report_templates (id) {
        id -> Int4,
        namespace_id -> Int4,
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
    report_templates_history (history_id) {
        id -> Int4,
        namespace_id -> Int4,
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
        password -> Varchar,
        proper_name -> Nullable<Varchar>,
        email -> Nullable<Varchar>,
        created_at -> Timestamp,
        updated_at -> Timestamp,
    }
}

diesel::joinable!(event_deliveries -> event_subscriptions (subscription_id));
diesel::joinable!(event_deliveries -> events (event_id));
diesel::joinable!(event_subscriptions -> event_sinks (sink_id));
diesel::joinable!(event_subscriptions -> namespaces (namespace_id));
diesel::joinable!(group_memberships -> groups (group_id));
diesel::joinable!(group_memberships -> principals (principal_id));
diesel::joinable!(hubuumclass -> namespaces (namespace_id));
diesel::joinable!(hubuumobject -> hubuumclass (hubuum_class_id));
diesel::joinable!(hubuumobject -> namespaces (namespace_id));
diesel::joinable!(hubuumobject_relation -> hubuumclass_relation (class_relation_id));
diesel::joinable!(import_task_results -> tasks (task_id));
diesel::joinable!(permissions -> groups (group_id));
diesel::joinable!(permissions -> namespaces (namespace_id));
diesel::joinable!(remote_call_results -> remote_targets (target_id));
diesel::joinable!(remote_call_results -> tasks (task_id));
diesel::joinable!(remote_targets -> hubuumclass (class_id));
diesel::joinable!(remote_targets -> namespaces (namespace_id));
diesel::joinable!(report_task_outputs -> tasks (task_id));
diesel::joinable!(report_templates -> hubuumclass (class_id));
diesel::joinable!(report_templates -> namespaces (namespace_id));
diesel::joinable!(service_accounts -> groups (owner_group_id));
diesel::joinable!(tasks -> tokens (submitted_token_id));
diesel::joinable!(token_scopes -> tokens (token_id));
diesel::joinable!(tokens -> principals (principal_id));

diesel::allow_tables_to_appear_in_same_query!(
    event_deliveries,
    event_sinks,
    event_subscriptions,
    events,
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
    import_task_results,
    namespaces,
    namespaces_history,
    permissions,
    principals,
    remote_call_results,
    remote_targets,
    remote_targets_history,
    report_task_outputs,
    report_templates,
    report_templates_history,
    service_accounts,
    tasks,
    token_scopes,
    tokens,
    users,
);
