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
    pub use crate::traits::{scope_allows, scope_allows_resource, scope_allows_resources};
}

pub(crate) mod collection {
    pub(crate) use crate::storage::postgres::operations::collection::collection_permission_set_from_backend;
}

pub(crate) mod event_delivery {
    #[cfg(test)]
    pub(crate) use crate::storage::postgres::operations::event_delivery::{
        ClaimedEventDelivery, claimed_event_delivery_work_item,
    };
    pub(crate) use crate::storage::postgres::operations::event_delivery::{
        list_event_deliveries_with_total_count, load_event_delivery, mark_event_delivery_dead,
        release_event_delivery_for_retry,
    };
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

pub(crate) mod external_identity {
    pub(crate) use crate::storage::postgres::operations::external_identity::{
        ExternalPrincipalState, external_principal_state, mark_external_sync_attempted,
        sync_external_user,
    };
}

pub(crate) mod group {
    pub(crate) use crate::storage::postgres::operations::group::principal_group_by_ids;
}

pub(crate) mod identity {
    pub(crate) use crate::storage::postgres::operations::identity::{
        ensure_identity_scope, identity_scope_name_by_id,
    };
}

pub(crate) mod meta {
    pub(crate) use crate::storage::postgres::operations::meta::{
        load_database_state, load_task_queue_state,
    };
}

pub(crate) mod permissions {
    pub(crate) use crate::storage::postgres::operations::permissions::PermissionControllerBackend;
}

pub(crate) mod relations {
    pub(crate) use crate::storage::postgres::operations::relations::{
        class_relation_authorization_resources, object_authorization_resources,
        object_relation_authorization_resources,
    };
}

pub(crate) mod service_account {
    pub(crate) use crate::storage::postgres::operations::service_account::{
        DisableServiceAccount, SaveServiceAccount, count_manageable_service_accounts,
        is_human_owner_group_member, load_service_account_by_id, principal_is_disabled,
        search_manageable_service_accounts,
    };
}

pub(crate) use crate::storage::postgres::{
    StorageCallSite, with_mutation_provenance_scope, with_revision_precondition_scope,
    with_statement_timeout_scope, with_storage_call_site,
};
