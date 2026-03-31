// @generated automatically by Diesel CLI.

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
        created_at -> Timestamp,
        updated_at -> Timestamp,
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
        created_at -> Timestamp,
        updated_at -> Timestamp,
    }
}

diesel::table! {
    task_events (id) {
        id -> Int4,
        task_id -> Int4,
        event_type -> Varchar,
        message -> Text,
        data -> Nullable<Jsonb>,
        created_at -> Timestamp,
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
    tokens (token) {
        token -> Varchar,
        user_id -> Int4,
        issued -> Timestamp,
    }
}

diesel::table! {
    user_groups (user_id, group_id) {
        user_id -> Int4,
        group_id -> Int4,
        created_at -> Timestamp,
        updated_at -> Timestamp,
    }
}

diesel::table! {
    users (id) {
        id -> Int4,
        username -> Varchar,
        password -> Varchar,
        email -> Nullable<Varchar>,
        created_at -> Timestamp,
        updated_at -> Timestamp,
    }
}

diesel::joinable!(hubuumclass -> namespaces (namespace_id));
diesel::joinable!(hubuumobject -> hubuumclass (hubuum_class_id));
diesel::joinable!(hubuumobject -> namespaces (namespace_id));
diesel::joinable!(hubuumobject_relation -> hubuumclass_relation (class_relation_id));
diesel::joinable!(import_task_results -> tasks (task_id));
diesel::joinable!(permissions -> groups (group_id));
diesel::joinable!(permissions -> namespaces (namespace_id));
diesel::joinable!(report_task_outputs -> tasks (task_id));
diesel::joinable!(report_templates -> namespaces (namespace_id));
diesel::joinable!(task_events -> tasks (task_id));
diesel::joinable!(tokens -> users (user_id));
diesel::joinable!(user_groups -> groups (group_id));
diesel::joinable!(user_groups -> users (user_id));

diesel::allow_tables_to_appear_in_same_query!(
    groups,
    hubuumclass,
    hubuumclass_reachability,
    hubuumclass_relation,
    hubuumobject,
    hubuumobject_relation,
    import_task_results,
    namespaces,
    permissions,
    report_task_outputs,
    report_templates,
    task_events,
    tasks,
    tokens,
    user_groups,
    users,
);
