// @generated automatically by Diesel CLI.

diesel::table! {
    group_datapermissions (id) {
        id -> Int4,
        namespace_id -> Int4,
        group_id -> Int4,
        has_create -> Bool,
        has_read -> Bool,
        has_update -> Bool,
        has_delete -> Bool,
    }
}

diesel::table! {
    group_namespacepermissions (id) {
        id -> Int4,
        namespace_id -> Int4,
        group_id -> Int4,
        has_create -> Bool,
        has_read -> Bool,
        has_update -> Bool,
        has_delete -> Bool,
        has_delegate -> Bool,
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
    namespaces (id) {
        id -> Int4,
        name -> Varchar,
        description -> Varchar,
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
    user_datapermissions (id) {
        id -> Int4,
        namespace_id -> Int4,
        user_id -> Int4,
        has_create -> Bool,
        has_read -> Bool,
        has_update -> Bool,
        has_delete -> Bool,
    }
}

diesel::table! {
    user_groups (user_id, group_id) {
        user_id -> Int4,
        group_id -> Int4,
    }
}

diesel::table! {
    user_namespacepermissions (id) {
        id -> Int4,
        namespace_id -> Int4,
        user_id -> Int4,
        has_create -> Bool,
        has_read -> Bool,
        has_update -> Bool,
        has_delete -> Bool,
        has_delegate -> Bool,
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

diesel::joinable!(group_datapermissions -> groups (group_id));
diesel::joinable!(group_datapermissions -> namespaces (namespace_id));
diesel::joinable!(group_namespacepermissions -> groups (group_id));
diesel::joinable!(group_namespacepermissions -> namespaces (namespace_id));
diesel::joinable!(hubuumclass -> namespaces (namespace_id));
diesel::joinable!(hubuumobject -> hubuumclass (hubuum_class_id));
diesel::joinable!(hubuumobject -> namespaces (namespace_id));
diesel::joinable!(tokens -> users (user_id));
diesel::joinable!(user_datapermissions -> namespaces (namespace_id));
diesel::joinable!(user_datapermissions -> users (user_id));
diesel::joinable!(user_groups -> groups (group_id));
diesel::joinable!(user_groups -> users (user_id));
diesel::joinable!(user_namespacepermissions -> namespaces (namespace_id));
diesel::joinable!(user_namespacepermissions -> users (user_id));

diesel::allow_tables_to_appear_in_same_query!(
    group_datapermissions,
    group_namespacepermissions,
    groups,
    hubuumclass,
    hubuumobject,
    namespaces,
    tokens,
    user_datapermissions,
    user_groups,
    user_namespacepermissions,
    users,
);
