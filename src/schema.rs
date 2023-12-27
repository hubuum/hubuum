// @generated automatically by Diesel CLI.

diesel::table! {
    groups (id) {
        id -> Int4,
        groupname -> Varchar,
        description -> Varchar,
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

diesel::joinable!(user_groups -> groups (group_id));
diesel::joinable!(user_groups -> users (user_id));

diesel::allow_tables_to_appear_in_same_query!(groups, user_groups, users,);
