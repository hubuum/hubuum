//! Application-facing persistence capabilities.
//!
//! Consumers depend on operation-shaped contracts from this module and pass a
//! [`crate::storage::StorageContext`]. PostgreSQL adapters remain behind the
//! boundary in `crate::storage::postgres`.

/// Operation-shaped persistence capabilities still used directly by the
/// application layer.
///
/// Keep this list explicit: adding a database module wholesale would make the
/// boundary cosmetic and allow consumers to select arbitrary SQL adapters.
pub(crate) use crate::storage::postgres::operations::{Status, UserPermissions};

pub(crate) mod active_tokens {
    pub(crate) use crate::storage::postgres::operations::active_tokens::retained_token_metadata_by_principal_id_paginated_with_total_count;
}

pub(crate) mod authz {
    pub(crate) use crate::storage::postgres::operations::authz::{load_token_scope, scope_allows};
}

pub(crate) mod collection {
    pub(crate) use crate::storage::postgres::operations::collection::collection_permission_set_from_backend;
}

pub(crate) mod computed_field {
    pub(crate) use crate::storage::postgres::operations::computed_field::{
        ComputedQuerySnapshot, class_computation_state_for, create_personal_definition,
        create_shared_definition, delete_personal_definition, delete_shared_definition,
        enrich_objects_with_computed, enrich_objects_with_computed_query_snapshot,
        get_computed_definition, list_personal_definitions_page, list_shared_definitions,
        preview_computed_definition, request_class_rebuild, resolve_computed_query_fields,
        update_personal_definition, update_shared_definition,
    };
}

pub(crate) mod event_delivery {
    pub(crate) use crate::storage::postgres::operations::event_delivery::{
        list_event_deliveries_with_total_count, load_event_delivery, mark_event_delivery_dead,
        release_event_delivery_for_retry,
    };
}

pub(crate) mod event_observability {
    pub(crate) use crate::storage::postgres::operations::event_observability::load_event_delivery_health;
}

pub(crate) mod event_subscription {
    pub(crate) use crate::storage::postgres::operations::event_subscription::{
        DeleteEventSinkRecord, DeleteEventSubscriptionRecord, SaveEventSinkRecord,
        SaveEventSubscriptionRecord, UpdateEventSinkRecord, UpdateEventSubscriptionRecord,
        enabled_event_sink_count,
    };
}

pub(crate) mod events {
    pub(crate) use crate::storage::postgres::operations::events::{
        list_events_with_total_count, parse_event_filters,
    };
}

pub(crate) mod group {
    pub(crate) use crate::storage::postgres::operations::group::principal_group_by_ids;
}

pub(crate) mod history {
    pub(crate) use crate::storage::postgres::operations::history::{
        HistoryCollectionFilter, class_as_of, class_history_paginated_with_total_count,
        collection_as_of, collection_history_paginated_with_total_count, export_template_as_of,
        export_template_history_paginated_with_total_count, object_as_of,
        object_history_paginated_with_total_count, remote_target_as_of,
        remote_target_history_paginated_with_total_count, resolve_principal_names,
    };
}

pub(crate) mod meta {
    pub(crate) use crate::storage::postgres::operations::meta::{
        load_database_pool_state, load_database_state, load_task_queue_state,
    };
}

pub(crate) mod principal {
    pub(crate) use crate::storage::postgres::operations::principal::load_principal_with_user;
}

pub(crate) mod probe {
    pub(crate) use crate::storage::postgres::operations::probe::ProbeBackend;
}

pub(crate) mod relations {
    pub(crate) use crate::storage::postgres::operations::relations::{
        class_relation_authorization_resources, object_relation_authorization_resources,
    };
}

pub(crate) mod remote_target {
    pub(crate) use crate::storage::postgres::operations::remote_target::{
        DeleteRemoteTargetRecord, SaveRemoteTargetRecord, UpdateRemoteTargetRecord,
        emit_remote_target_invoked_event,
    };
}

pub(crate) mod service_account {
    pub(crate) use crate::storage::postgres::operations::service_account::{
        DisableServiceAccount, SaveServiceAccount, count_manageable_service_accounts,
        is_human_owner_group_member, load_service_account_by_id, principal_is_disabled,
        search_manageable_service_accounts,
    };
}

pub(crate) mod task {
    pub(crate) use crate::storage::postgres::operations::task::{
        TaskBackend, TaskCreateRequest, TaskScopeSnapshot, list_backup_task_output_summaries,
        list_export_task_output_summaries, list_tasks_with_total_count, task_event_responses,
    };
}

pub(crate) mod user {
    use crate::errors::ApiError;
    use crate::models::search::QueryOptions;
    use crate::models::{
        HubuumClass, HubuumClassExpanded, HubuumClassRelation, HubuumObject, HubuumObjectRelation,
        TokenScope,
    };
    use crate::storage::StorageContext;
    use crate::traits::SelfAccessors;

    use super::computed_field::ComputedQuerySnapshot;

    /// The search operations required by application consumers.
    ///
    /// The underlying PostgreSQL trait also contains connection-pool
    /// helpers for adapter composition. Keeping those methods out of this
    /// facade prevents method resolution from becoming a pool escape hatch.
    pub(crate) trait UserSearchBackend {
        async fn search_collections_from_backend_with_admin_status<C>(
            &self,
            context: &C,
            query_options: QueryOptions,
            is_admin: bool,
            scopes: Option<&TokenScope>,
        ) -> Result<Vec<crate::models::Collection>, ApiError>
        where
            C: StorageContext;

        async fn search_classes_from_backend_with_admin_status<C>(
            &self,
            context: &C,
            query_options: QueryOptions,
            is_admin: bool,
            scopes: Option<&TokenScope>,
        ) -> Result<Vec<HubuumClassExpanded>, ApiError>
        where
            C: StorageContext;

        async fn search_objects_from_backend<C>(
            &self,
            context: &C,
            query_options: QueryOptions,
            scopes: Option<&TokenScope>,
        ) -> Result<Vec<HubuumObject>, ApiError>
        where
            C: StorageContext;

        async fn search_objects_from_backend_with_admin_status<C>(
            &self,
            context: &C,
            query_options: QueryOptions,
            is_admin: bool,
            scopes: Option<&TokenScope>,
        ) -> Result<Vec<HubuumObject>, ApiError>
        where
            C: StorageContext;

        async fn search_objects_with_computed_query_from_backend<C>(
            &self,
            context: &C,
            query_options: QueryOptions,
            scopes: Option<&TokenScope>,
            snapshot: &ComputedQuerySnapshot,
        ) -> Result<Vec<HubuumObject>, ApiError>
        where
            C: StorageContext;

        async fn count_objects_with_computed_query_from_backend<C>(
            &self,
            context: &C,
            query_options: QueryOptions,
            scopes: Option<&TokenScope>,
            snapshot: &ComputedQuerySnapshot,
        ) -> Result<i64, ApiError>
        where
            C: StorageContext;

        async fn search_class_relations_from_backend_with_admin_status<C>(
            &self,
            context: &C,
            query_options: QueryOptions,
            is_admin: bool,
            scopes: Option<&TokenScope>,
        ) -> Result<Vec<HubuumClassRelation>, ApiError>
        where
            C: StorageContext;

        async fn class_relations_touching_page_from_backend_with_admin_status<C, K>(
            &self,
            context: &C,
            class: K,
            query_options: QueryOptions,
            is_admin: bool,
            scopes: Option<&TokenScope>,
        ) -> Result<(Vec<HubuumClassRelation>, i64), ApiError>
        where
            C: StorageContext,
            K: SelfAccessors<HubuumClass>;

        async fn search_object_relations_from_backend_with_admin_status<C>(
            &self,
            context: &C,
            query_options: QueryOptions,
            is_admin: bool,
            scopes: Option<&TokenScope>,
        ) -> Result<Vec<HubuumObjectRelation>, ApiError>
        where
            C: StorageContext;

        async fn object_relations_touching_page_from_backend_with_admin_status<C, O>(
            &self,
            context: &C,
            object: O,
            query_options: QueryOptions,
            is_admin: bool,
            scopes: Option<&TokenScope>,
        ) -> Result<(Vec<HubuumObjectRelation>, i64), ApiError>
        where
            C: StorageContext,
            O: SelfAccessors<HubuumObject>;
    }

    impl<T> UserSearchBackend for T
    where
        T: crate::storage::postgres::operations::user::UserSearchBackend + Sync + ?Sized,
    {
        async fn search_collections_from_backend_with_admin_status<C>(
            &self,
            context: &C,
            query_options: QueryOptions,
            is_admin: bool,
            scopes: Option<&TokenScope>,
        ) -> Result<Vec<crate::models::Collection>, ApiError>
        where
            C: StorageContext,
        {
            crate::storage::postgres::operations::user::UserSearchBackend::search_collections_from_backend_with_admin_status(
                    self, context, query_options, is_admin, scopes,
                )
                .await
        }

        async fn search_classes_from_backend_with_admin_status<C>(
            &self,
            context: &C,
            query_options: QueryOptions,
            is_admin: bool,
            scopes: Option<&TokenScope>,
        ) -> Result<Vec<HubuumClassExpanded>, ApiError>
        where
            C: StorageContext,
        {
            crate::storage::postgres::operations::user::UserSearchBackend::search_classes_from_backend_with_admin_status(
                    self, context, query_options, is_admin, scopes,
                )
                .await
        }

        async fn search_objects_from_backend<C>(
            &self,
            context: &C,
            query_options: QueryOptions,
            scopes: Option<&TokenScope>,
        ) -> Result<Vec<HubuumObject>, ApiError>
        where
            C: StorageContext,
        {
            crate::storage::postgres::operations::user::UserSearchBackend::search_objects_from_backend(
                    self,
                    context,
                    query_options,
                    scopes,
                )
                .await
        }

        async fn search_objects_from_backend_with_admin_status<C>(
            &self,
            context: &C,
            query_options: QueryOptions,
            is_admin: bool,
            scopes: Option<&TokenScope>,
        ) -> Result<Vec<HubuumObject>, ApiError>
        where
            C: StorageContext,
        {
            crate::storage::postgres::operations::user::UserSearchBackend::search_objects_from_backend_with_admin_status(
                    self, context, query_options, is_admin, scopes,
                )
                .await
        }

        async fn search_objects_with_computed_query_from_backend<C>(
            &self,
            context: &C,
            query_options: QueryOptions,
            scopes: Option<&TokenScope>,
            snapshot: &ComputedQuerySnapshot,
        ) -> Result<Vec<HubuumObject>, ApiError>
        where
            C: StorageContext,
        {
            crate::storage::postgres::operations::user::UserSearchBackend::search_objects_with_computed_query_from_backend(
                    self, context, query_options, scopes, snapshot,
                )
                .await
        }

        async fn count_objects_with_computed_query_from_backend<C>(
            &self,
            context: &C,
            query_options: QueryOptions,
            scopes: Option<&TokenScope>,
            snapshot: &ComputedQuerySnapshot,
        ) -> Result<i64, ApiError>
        where
            C: StorageContext,
        {
            crate::storage::postgres::operations::user::UserSearchBackend::count_objects_with_computed_query_from_backend(
                    self, context, query_options, scopes, snapshot,
                )
                .await
        }

        async fn search_class_relations_from_backend_with_admin_status<C>(
            &self,
            context: &C,
            query_options: QueryOptions,
            is_admin: bool,
            scopes: Option<&TokenScope>,
        ) -> Result<Vec<HubuumClassRelation>, ApiError>
        where
            C: StorageContext,
        {
            crate::storage::postgres::operations::user::UserSearchBackend::search_class_relations_from_backend_with_admin_status(
                    self, context, query_options, is_admin, scopes,
                )
                .await
        }

        async fn class_relations_touching_page_from_backend_with_admin_status<C, K>(
            &self,
            context: &C,
            class: K,
            query_options: QueryOptions,
            is_admin: bool,
            scopes: Option<&TokenScope>,
        ) -> Result<(Vec<HubuumClassRelation>, i64), ApiError>
        where
            C: StorageContext,
            K: SelfAccessors<HubuumClass>,
        {
            crate::storage::postgres::operations::user::UserSearchBackend::class_relations_touching_page_from_backend_with_admin_status(
                    self, context, class, query_options, is_admin, scopes,
                )
                .await
        }

        async fn search_object_relations_from_backend_with_admin_status<C>(
            &self,
            context: &C,
            query_options: QueryOptions,
            is_admin: bool,
            scopes: Option<&TokenScope>,
        ) -> Result<Vec<HubuumObjectRelation>, ApiError>
        where
            C: StorageContext,
        {
            crate::storage::postgres::operations::user::UserSearchBackend::search_object_relations_from_backend_with_admin_status(
                    self, context, query_options, is_admin, scopes,
                )
                .await
        }

        async fn object_relations_touching_page_from_backend_with_admin_status<C, O>(
            &self,
            context: &C,
            object: O,
            query_options: QueryOptions,
            is_admin: bool,
            scopes: Option<&TokenScope>,
        ) -> Result<(Vec<HubuumObjectRelation>, i64), ApiError>
        where
            C: StorageContext,
            O: SelfAccessors<HubuumObject>,
        {
            crate::storage::postgres::operations::user::UserSearchBackend::object_relations_touching_page_from_backend_with_admin_status(
                    self, context, object, query_options, is_admin, scopes,
                )
                .await
        }
    }

    pub(crate) mod search {
        pub(crate) use crate::storage::postgres::operations::user::search::{
            ExternalRelatedFilterAuthorization, count_computed_objects_with_authorized_ids,
            externally_authorized_related_object_ids, search_computed_objects_with_authorized_ids,
        };
    }
}
pub(crate) use crate::storage::postgres::{
    StorageCallSite, with_mutation_provenance_scope, with_revision_precondition_scope,
    with_storage_call_site,
};
