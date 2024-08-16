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
        json_schema -> Jsonb,
        validate_schema -> Bool,
        description -> Varchar,
        created_at -> Timestamp,
        updated_at -> Timestamp,
    }
}

diesel::table! {
    hubuumclass_closure (ancestor_class_id, descendant_class_id, path) {
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
    hubuumobject_closure (ancestor_object_id, descendant_object_id, path) {
        ancestor_object_id -> Int4,
        descendant_object_id -> Int4,
        depth -> Int4,
        path -> Array<Nullable<Int4>>,
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

diesel::table! {
    class_closure_view (ancestor_class_id, descendant_class_id, path) {
        ancestor_class_id -> Int4,
        descendant_class_id -> Int4,
        depth -> Int4,
        path -> Array<Int4>,
        ancestor_name -> Text,
        descendant_name -> Text,
        ancestor_namespace_id -> Int4,
        descendant_namespace_id -> Int4,
        ancestor_json_schema -> Jsonb,
        descendant_json_schema -> Jsonb,
        ancestor_validate_schema -> Bool,
        descendant_validate_schema -> Bool,
        ancestor_description -> Text,
        descendant_description -> Text,
        ancestor_created_at -> Timestamp,
        descendant_created_at -> Timestamp,
        ancestor_updated_at -> Timestamp,
        descendant_updated_at -> Timestamp,
    }
}

diesel::table! {
    object_closure_view (ancestor_object_id, descendant_object_id, path) {
        ancestor_object_id -> Int4,
        descendant_object_id -> Int4,
        depth -> Int4,
        path -> Array<Int4>,
        ancestor_name -> Text,
        descendant_name -> Text,
        ancestor_namespace_id -> Int4,
        descendant_namespace_id -> Int4,
        ancestor_class_id -> Int4,
        descendant_class_id -> Int4,
        ancestor_description -> Text,
        descendant_description -> Text,
        ancestor_data -> Jsonb,
        descendant_data -> Jsonb,
        ancestor_created_at -> Timestamp,
        descendant_created_at -> Timestamp,
        ancestor_updated_at -> Timestamp,
        descendant_updated_at -> Timestamp,
    }
}

diesel::joinable!(hubuumclass -> namespaces (namespace_id));
diesel::joinable!(hubuumobject -> hubuumclass (hubuum_class_id));
diesel::joinable!(hubuumobject -> namespaces (namespace_id));
diesel::joinable!(hubuumobject_relation -> hubuumclass_relation (class_relation_id));
diesel::joinable!(permissions -> groups (group_id));
diesel::joinable!(permissions -> namespaces (namespace_id));
diesel::joinable!(tokens -> users (user_id));
diesel::joinable!(user_groups -> groups (group_id));
diesel::joinable!(user_groups -> users (user_id));

diesel::allow_tables_to_appear_in_same_query!(
    groups,
    hubuumclass,
    hubuumclass_closure,
    hubuumclass_relation,
    hubuumobject,
    hubuumobject_closure,
    hubuumobject_relation,
    namespaces,
    permissions,
    tokens,
    user_groups,
    users,
    class_closure_view,
    object_closure_view,
);
