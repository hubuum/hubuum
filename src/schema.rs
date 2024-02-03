// @generated automatically by Diesel CLI.

diesel::table! {
    classpermissions (id) {
        id -> Int4,
        namespace_id -> Int4,
        group_id -> Int4,
        has_create_object -> Bool,
        has_read_class -> Bool,
        has_update_class -> Bool,
        has_delete_class -> Bool,
    }
}

diesel::table! {
    groups (id) {
        id -> Int4,
        groupname -> Varchar,
        description -> Varchar,
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
    }
}

diesel::table! {
    namespacepermissions (id) {
        id -> Int4,
        namespace_id -> Int4,
        group_id -> Int4,
        has_create_object -> Bool,
        has_create_class -> Bool,
        has_read_namespace -> Bool,
        has_update_namespace -> Bool,
        has_delete_namespace -> Bool,
        has_delegate_namespace -> Bool,
    }
}

diesel::table! {
    namespaces (id) {
        id -> Int4,
        name -> Varchar,
        description -> Varchar,
    }
}

diesel::table! {
    objectpermissions (id) {
        id -> Int4,
        namespace_id -> Int4,
        group_id -> Int4,
        has_read_object -> Bool,
        has_update_object -> Bool,
        has_delete_object -> Bool,
    }
}

diesel::table! {
    tokens (token, user_id) {
        token -> Varchar,
        user_id -> Int4,
        issued -> Timestamp,
    }
}

diesel::table! {
    user_groups (user_id, group_id) {
        user_id -> Int4,
        group_id -> Int4,
    }
}

diesel::table! {
    users (id) {
        id -> Int4,
        username -> Varchar,
        password -> Varchar,
        email -> Nullable<Varchar>,
    }
}

diesel::joinable!(classpermissions -> groups (group_id));
diesel::joinable!(classpermissions -> namespaces (namespace_id));
diesel::joinable!(hubuumclass -> namespaces (namespace_id));
diesel::joinable!(hubuumobject -> hubuumclass (hubuum_class_id));
diesel::joinable!(hubuumobject -> namespaces (namespace_id));
diesel::joinable!(namespacepermissions -> groups (group_id));
diesel::joinable!(namespacepermissions -> namespaces (namespace_id));
diesel::joinable!(objectpermissions -> groups (group_id));
diesel::joinable!(objectpermissions -> namespaces (namespace_id));
diesel::joinable!(tokens -> users (user_id));
diesel::joinable!(user_groups -> groups (group_id));
diesel::joinable!(user_groups -> users (user_id));

diesel::allow_tables_to_appear_in_same_query!(
    classpermissions,
    groups,
    hubuumclass,
    hubuumobject,
    namespacepermissions,
    namespaces,
    objectpermissions,
    tokens,
    user_groups,
    users,
);
