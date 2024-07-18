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
    hubuumobject_relation (id) {
        id -> Int4,
        class_relation -> Int4,
        from_hubuum_object_id -> Int4,
        to_hubuum_object_id -> Int4,
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

diesel::joinable!(hubuumclass -> namespaces (namespace_id));
diesel::joinable!(hubuumobject -> hubuumclass (hubuum_class_id));
diesel::joinable!(hubuumobject -> namespaces (namespace_id));
diesel::joinable!(hubuumobject_relation -> hubuumclass_relation (class_relation));
diesel::joinable!(permissions -> groups (group_id));
diesel::joinable!(permissions -> namespaces (namespace_id));
diesel::joinable!(tokens -> users (user_id));
diesel::joinable!(user_groups -> groups (group_id));
diesel::joinable!(user_groups -> users (user_id));

diesel::allow_tables_to_appear_in_same_query!(
    groups,
    hubuumclass,
    hubuumclass_relation,
    hubuumobject,
    hubuumobject_relation,
    namespaces,
    permissions,
    tokens,
    user_groups,
    users,
);
