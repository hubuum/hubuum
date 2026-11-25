use crate::api::handlers::{meta, probes};
use crate::api::v1::handlers::auth;
use crate::api::v1::handlers::collection_event_sinks;
use crate::api::v1::handlers::history::HistoryResponse;
use crate::api::v1::handlers::{
    backups, classes, client_config, collections, computed_fields, credential_approvals,
    event_deliveries, event_sinks, event_subscriptions, events, export_templates, exports, groups,
    imports, me, principals, query_usage, relations, remote_targets, restores, runtime_config,
    schema_evolution, search, service_accounts, system_event_subscriptions, tasks, users,
};
use crate::config::running::{
    AuthenticationConfig, BackupConfig, ClientAllowlistStatus, ClientConfig,
    ClientPaginationConfig, DatabaseConfig, EventConfig, ExportConfig,
    LoginRateLimitConfig as RunningLoginRateLimitConfig, NetworkConfig, PaginationConfig,
    RemoteCallConfig, RestoreConfig, RunningConfig, SecretStatus, ServerConfig, TaskConfig,
    TlsConfig,
};
use crate::events::EventResponse;
use crate::models::auth_session::{
    RefreshToken, SessionLoginRequest, SessionRefreshRequest, SessionResponse,
};
use crate::models::task::TaskDiscoveryTarget;
use crate::models::task::{
    RebuildTaskDetails, RemoteCallTaskDetails, RetainedBackupDetails, RetainedExportDetails,
    RetainedImportDetails, SchemaProfileTaskDetails, SchemaTaskDetails, TaskOutputDiscoveryState,
};
use crate::models::{
    BackupDocument, BackupHistory, BackupManifest, BackupRequest, BackupState, BackupTaskDetails,
    ClassComputationState, ClassKey, Collection, CollectionHistory, CollectionKey,
    CollectionPermissionSet, ComputedFieldDefinition, ComputedFieldDefinitionPatch,
    ComputedFieldDefinitionRequest, ComputedFieldDeleteResponse, ComputedFieldErrorResponse,
    ComputedFieldListResponse, ComputedFieldMutationResponse, ComputedFieldPreviewRequest,
    ComputedFieldPreviewResponse, ComputedObjectScopesResponse, ComputedResultType,
    ComputedScopeResponse, CurrentTokenMetadata, EventDeliveryHealthResponse,
    EventDeliveryQueueHealth, EventDeliveryResponse, EventDeliveryStatus,
    EventDeliveryStatusCounts, EventDeliveryUpdateResponse, EventFanoutHealth, EventSink,
    EventSinkDeliveryHealth, EventSinkKey, EventSinkKind, EventSubscription,
    EventSubscriptionDeliveryHealth, EventWorkerHealth, EventWorkerWakeupStats, ExportContentType,
    ExportJsonResponse, ExportLimits, ExportMeta, ExportMissingDataPolicy, ExportRequest,
    ExportScope, ExportScopeKind, ExportTaskDetails, ExportTemplate, ExportTemplateHistory,
    ExportTemplateID, ExportTemplateKind, ExportTemplateRunRequest, ExportWarning, Group, GroupKey,
    GroupPermission, GroupPointResponse, GroupResponse, HistoryDirection, HubuumClass,
    HubuumClassExpanded, HubuumClassHistory, HubuumClassRelation, HubuumClassWithPath,
    HubuumObject, HubuumObjectComputedResponse, HubuumObjectHistory, HubuumObjectReadResponse,
    HubuumObjectRelation, HubuumObjectWithPath, IdentityScopeKey, ImportAtomicity,
    ImportClassInput, ImportClassRelationInput, ImportCollectionInput,
    ImportCollectionPermissionInput, ImportCollisionPolicy, ImportComputedFieldInput,
    ImportComputedFieldVisibility, ImportEventSinkInput, ImportEventSubscriptionInput,
    ImportExportTemplateInput, ImportGraph, ImportGroupInput, ImportGroupMembershipInput,
    ImportIdentityScopeInput, ImportMembershipSourceInput, ImportMode, ImportObjectInput,
    ImportObjectRelationInput, ImportPermissionPolicy, ImportPrincipalInput,
    ImportPrincipalSubtype, ImportRemoteTargetInput, ImportRequest, ImportTaskDetails,
    ImportTaskResultResponse, ImportWriteCondition, LoginUser, MembershipPrincipalResponse,
    NewCollectionWithAssignee, NewEventSink, NewEventSubscription, NewExportTemplate, NewGroup,
    NewHubuumClass, NewHubuumClassRelation, NewHubuumClassRelationFromClass, NewHubuumObject,
    NewHubuumObjectRelation, NewHubuumObjectRequest, NewRemoteTarget, NewServiceAccount, NewUser,
    ObjectAggregateDimensionValue, ObjectAggregateMeasureOperation, ObjectAggregateMeasureState,
    ObjectAggregateMeasureValue, ObjectAggregateRow, ObjectAggregateValueState,
    ObjectDataPatchDocument, ObjectKey, ObjectsByClass, Permission, Permissions,
    PersonalComputedFieldDefinitionRequest, PrincipalKey, PrincipalMemberResponse,
    PrincipalSettings, PrincipalSettingsPatchDocument, PrincipalSettingsResponse,
    PrincipalTokenMetadata, PrincipalTokenPointResponse, RelatedClassGraph, RelatedObjectGraph,
    RemoteAuthConfig, RemoteCallResult, RemoteHttpMethod, RemoteInvocationBodyOverride,
    RemoteInvocationParameters, RemoteInvocationSubject, RemoteTarget, RemoteTargetHistory,
    RemoteTargetID, RemoteTargetInvokeRequest, RemoteTargetSubjectType, ResourceRevision,
    RestoreConfirmRequest, RestoreJobStatus, RestoreStageResponse, RestoreTimestamps,
    RestoreValidationSummary, ServiceAccountPointResponse, ServiceAccountResponse,
    SharedComputedScopeResponse, StructuredSearchDoneEvent, StructuredSearchErrorEvent,
    StructuredSearchStartedEvent, TaskDetails, TaskEventResponse, TaskKind, TaskLinks,
    TaskProgress, TaskResponse, TaskStatus, TokenListState, TokenResourceScope, TokenScopeDetails,
    UnifiedSearchBatchResponse, UnifiedSearchDoneEvent, UnifiedSearchErrorEvent, UnifiedSearchKind,
    UnifiedSearchResponse, UnifiedSearchStartedEvent, UpdateClassRelationLabels, UpdateCollection,
    UpdateEventSink, UpdateEventSubscription, UpdateExportTemplate, UpdateGroup, UpdateHubuumClass,
    UpdateHubuumObject, UpdateHubuumObjectRequest, UpdateRemoteTarget, UpdateServiceAccount,
    UpdateUser, UserPointResponse, UserResponse,
};
use crate::models::{CollectionEventSink, EventSinkRouting};
use crate::pagination::{
    NEXT_CURSOR_HEADER, PAGE_LIMIT_HEADER, TOTAL_COUNT_HEADER, page_limits_or_defaults,
};
use actix_web::{HttpResponse, Responder};
use hubuum_events_core::{
    EventSubject, EventSubscriptionFilter, Provenance, ProvenanceActor, ProvenancePrincipal,
};
use serde::Serialize;
use utoipa::openapi::OpenApi as OpenApiDoc;
use utoipa::openapi::header::Header;
use utoipa::openapi::path::{Operation, Parameter, ParameterBuilder, ParameterIn, PathItem};
use utoipa::openapi::response::Response;
use utoipa::openapi::schema::{ObjectBuilder, Schema};
use utoipa::openapi::security::{ApiKey, ApiKeyValue, Http, HttpAuthScheme, SecurityScheme};
use utoipa::openapi::{Object, RefOr, Required, Type};
use utoipa::{Modify, OpenApi, ToSchema};

#[derive(OpenApi)]
#[openapi(
    info(
        title = "Hubuum REST API",
        version = env!("CARGO_PKG_VERSION"),
        description = "OpenAPI documentation for the Hubuum REST service."
    ),
    servers((url = "/", description = "Current deployment base URL")),
    paths(
        probes::healthz,
        probes::readyz,
        client_config::get_client_config,
        runtime_config::get_running_config,
        runtime_config::get_runtime_settings,
        runtime_config::get_runtime_settings_schema,
        runtime_config::get_runtime_settings_group,
        runtime_config::patch_runtime_settings,
        runtime_config::preflight_authentication_settings,
        runtime_config::set_authorization_settings,
        runtime_config::set_query_observation_settings,
        runtime_config::set_pagination_settings,
        runtime_config::release_authentication_settings,
        runtime_config::release_authorization_settings,
        runtime_config::release_query_observation_settings,
        runtime_config::release_pagination_settings,
        runtime_config::release_runtime_settings_group,
        runtime_config::get_runtime_settings_history,
        meta::get_db_state,
        meta::get_object_and_class_count,
        meta::get_task_queue_state,
        meta::get_login_rate_limit_state,
        meta::release_login_rate_limit_entry,
        meta::clear_login_rate_limit,
        auth::get_auth_providers,
        auth::login,
        auth::refresh,
        auth::logout,
        auth::logout_all,
        auth::logout_token,
        auth::logout_other,
        auth::validate_token,
        users::get_users,
        users::create_user,
        users::get_user,
        users::update_user,
        users::delete_user,
        users::anonymize_user,
        groups::get_groups,
        groups::create_group,
        groups::get_group,
        groups::update_group,
        groups::delete_group,
        groups::get_group_members,
        groups::get_group_member,
        groups::add_group_member,
        groups::delete_group_member,
        service_accounts::create_service_account,
        service_accounts::list_service_accounts,
        service_accounts::get_service_account,
        service_accounts::update_service_account,
        service_accounts::disable_service_account,
        service_accounts::delete_service_account,
        credential_approvals::create_approval,
        credential_approvals::get_approval,
        principals::create_token,
        principals::token_activity,
        principals::list_tokens,
        principals::get_token,
        principals::renew_token,
        principals::revoke_token,
        principals::list_principal_groups,
        principals::list_principal_permissions,
        principals::get_principal_settings,
        principals::put_principal_settings,
        principals::patch_principal_settings,
        principals::delete_principal_settings,
        me::get_me,
        me::list_my_tokens,
        me::list_my_groups,
        me::list_my_permissions,
        me::get_my_settings,
        me::put_my_settings,
        me::patch_my_settings,
        me::delete_my_settings,
        computed_fields::get_personal_computed_fields,
        computed_fields::get_personal_computed_field,
        computed_fields::create_personal_computed_field,
        computed_fields::patch_personal_computed_field,
        computed_fields::delete_personal_computed_field,
        computed_fields::preview_personal_computed_field,
        collections::get_collections,
        collections::create_collection,
        collections::get_collection,
        collections::get_collection_history,
        collections::get_collection_as_of,
        collections::update_collection,
        collections::delete_collection,
        collections::get_collection_children,
        collections::get_collection_ancestors,
        collections::move_collection_parent,
        collections::get_collection_permissions,
        collections::get_collection_group_permissions,
        collections::get_collection_effective_group_permissions,
        collections::grant_collection_group_permissions,
        collections::replace_collection_group_permissions,
        collections::revoke_collection_group_permissions,
        collections::get_collection_group_permission,
        collections::grant_collection_group_permission,
        collections::revoke_collection_group_permission,
        collections::get_collection_principal_permissions,
        collections::get_collection_effective_principal_permissions,
        collections::get_collection_groups_with_permission,
        relations::get_class_relations,
        relations::get_class_relation,
        relations::create_class_relation,
        relations::delete_class_relation,
        relations::update_class_relation_labels,
        relations::get_object_relations,
        relations::get_object_relation,
        relations::create_object_relation,
        relations::delete_object_relation,
        search::get_search,
        search::post_search,
        search::hydrate_search,
        search::post_stream_search,
        search::stream_search,
        exports::run_export,
        exports::get_export,
        exports::get_export_output,
        backups::create_backup,
        backups::get_backup,
        backups::get_backup_output,
        tasks::get_tasks,
        tasks::get_task,
        tasks::get_task_events,
        tasks::cancel_task,
        events::get_events,
        events::get_collection_events,
        events::get_class_events,
        events::get_object_events,
        events::get_user_events,
        events::get_group_events,
        events::get_export_template_events,
        events::get_remote_target_events,
        collection_event_sinks::list,
        collection_event_sinks::get_owned,
        collection_event_sinks::create,
        collection_event_sinks::update,
        collection_event_sinks::remove,
        event_sinks::get_sink_collections,
        event_sinks::grant_sink_collection,
        event_sinks::revoke_sink_collection,
        event_sinks::preview_event_sink,
        event_sinks::test_event_sink,
        event_sinks::create_event_sink,
        event_sinks::get_event_sinks,
        event_sinks::get_event_sink,
        event_sinks::patch_event_sink,
        event_sinks::delete_event_sink,
        event_deliveries::get_event_deliveries,
        event_deliveries::get_event_delivery_health,
        event_deliveries::get_event_delivery,
        event_deliveries::retry_event_delivery,
        event_deliveries::dead_letter_event_delivery,
        system_event_subscriptions::create_system_event_subscription,
        system_event_subscriptions::get_system_event_subscriptions,
        system_event_subscriptions::get_system_event_subscription,
        system_event_subscriptions::patch_system_event_subscription,
        system_event_subscriptions::delete_system_event_subscription,
        event_subscriptions::create_event_subscription,
        event_subscriptions::get_event_subscriptions,
        event_subscriptions::get_event_subscription,
        event_subscriptions::patch_event_subscription,
        event_subscriptions::delete_event_subscription,
        imports::create_import,
        imports::get_import,
        imports::get_import_results,
        restores::create_restore_stage,
        restores::confirm_restore_stage,
        restores::get_restore_status,
        export_templates::get_templates,
        export_templates::create_template,
        export_templates::get_template,
        export_templates::get_template_history,
        export_templates::get_template_as_of,
        export_templates::run_template_export,
        export_templates::patch_template,
        export_templates::delete_template,
        remote_targets::get_remote_targets,
        remote_targets::create_remote_target,
        remote_targets::get_remote_target,
        remote_targets::get_remote_target_history,
        remote_targets::get_remote_target_as_of,
        remote_targets::patch_remote_target,
        remote_targets::delete_remote_target,
        remote_targets::invoke_remote_target,
        query_usage::analyze_query_usage,
        query_usage::list_query_usage,
        query_usage::create_query_usage,
        query_usage::replace_query_usage,
        query_usage::delete_query_usage,
        schema_evolution::list_object_compliance,
        schema_evolution::list_schema_revisions,
        schema_evolution::get_schema_state,
        schema_evolution::stage_schema_revision,
        schema_evolution::get_schema_revision,
        schema_evolution::abandon_schema_revision,
        schema_evolution::activate_schema_revision,
        schema_evolution::analyze_schema_impact,
        schema_evolution::revalidate_schema,
        schema_evolution::get_schema_work,
        schema_evolution::generate_schema_repair_report,
        schema_evolution::get_schema_repair_report,
        schema_evolution::cancel_schema_work,
        classes::get_classes,
        classes::create_class,
        classes::get_class,
        classes::get_class_by_name,
        classes::field_discovery::get_class_fields,
        classes::field_discovery::get_class_fields_by_name,
        crate::api::v1::handlers::schema_profiling::create_schema_profile,
        crate::api::v1::handlers::schema_profiling::get_schema_profile,
        classes::get_class_history,
        classes::get_class_as_of,
        classes::get_class_history_timeline,
        classes::get_class_history_entry,
        classes::update_class,
        classes::update_class_by_name,
        classes::delete_class,
        classes::delete_class_by_name,
        classes::get_class_permissions,
        classes::get_class_permissions_by_name,
        computed_fields::get_shared_computed_fields,
        computed_fields::get_shared_computed_field,
        computed_fields::create_shared_computed_field,
        computed_fields::patch_shared_computed_field,
        computed_fields::delete_shared_computed_field,
        computed_fields::preview_shared_computed_field,
        computed_fields::rebuild_shared_computed_fields,
        classes::get_related_classes,
        classes::get_related_classes_by_name,
        classes::create_class_relation,
        classes::get_class_relation,
        classes::delete_class_relation,
        classes::get_related_class_relations,
        classes::get_related_class_relations_by_name,
        classes::get_related_class_graph,
        classes::get_related_class_graph_by_name,
        classes::object_aggregates::get_object_aggregates,
        classes::object_aggregates::get_object_aggregates_by_name,
        classes::get_objects_in_class,
        classes::get_objects_in_class_by_name,
        classes::create_object_in_class,
        classes::create_object_in_class_by_name,
        classes::get_object_in_class,
        classes::get_object_in_class_by_name,
        classes::get_object_history,
        classes::get_class_objects_as_of,
        classes::get_class_relation_history,
        classes::get_class_relations_as_of,
        classes::get_object_relation_history,
        classes::get_object_relations_as_of,
        classes::get_object_history_by_name,
        classes::get_object_as_of,
        classes::get_object_history_timeline,
        classes::get_object_history_entry,
        classes::get_object_computed_as_of,
        classes::patch_object_in_class,
        classes::patch_object_in_class_by_name,
        classes::patch_object_data_in_class,
        classes::patch_object_data_by_name_in_class,
        classes::delete_object_in_class,
        classes::delete_object_in_class_by_name,
        classes::get_related_objects,
        classes::get_related_objects_by_name,
        classes::get_related_object_relations,
        classes::get_related_object_relations_by_name,
        classes::get_related_object_graph,
        classes::get_related_object_graph_by_name,
        classes::get_object_relation_from_class_and_objects,
        classes::delete_object_relation,
        classes::create_object_relation
    ),
    components(
        schemas(
            ApiErrorResponse,
            MessageResponse,
            probes::ProbeResponse,
            LoginResponse,
            crate::models::credential_activity::CredentialActivityResponse,
            crate::models::credential_activity::CredentialSourceResponse,
            crate::models::credential_activity::CredentialCreationResponse,
            SessionLoginRequest, SessionRefreshRequest, SessionResponse, RefreshToken,
            CountsResponse,
            meta::DbStateResponse,
            meta::TaskQueueStateResponse,
            meta::LoginRateLimitConfigResponse,
            meta::LoginRateLimitEntryResponse,
            meta::LoginRateLimitStateResponse,
            meta::ReleaseRateLimitResponse,
            meta::ClearRateLimitResponse,
            RunningConfig,
            ClientConfig,
            ClientPaginationConfig,
            SecretStatus,
            ServerConfig,
            TlsConfig,
            DatabaseConfig,
            TaskConfig,
            EventConfig,
            ExportConfig,
            BackupConfig,
            RestoreConfig,
            RemoteCallConfig,
            AuthenticationConfig,
            RunningLoginRateLimitConfig,
            PaginationConfig,
            NetworkConfig,
            ClientAllowlistStatus,
            ObjectsByClass,
            UserResponse,
            UserPointResponse,
            NewUser,
            UpdateUser,
            LoginUser,
            auth::AuthProvidersResponse,
            auth::LogoutTokenRequest,
            PrincipalTokenMetadata,
            PrincipalTokenPointResponse,
            PrincipalMemberResponse,
            MembershipPrincipalResponse,
            PrincipalSettings,
            PrincipalSettingsPatchDocument,
            PrincipalSettingsResponse,
            ResourceRevision,
            HistoryDirection,
            NewServiceAccount,
            ServiceAccountResponse,
            ServiceAccountPointResponse,
            UpdateServiceAccount,
            principals::NewTokenRequest,
            principals::RenewTokenRequest,
            principals::GroupGrant,
            principals::PrincipalCollectionPermissions,
            TokenListState,
            CurrentTokenMetadata,
            me::MeResponse,
            Group,
            GroupPointResponse,
            GroupResponse,
            NewGroup,
            UpdateGroup,
            Collection,
            NewCollectionWithAssignee,
            UpdateCollection,
            Permissions,
            TokenResourceScope,
            TokenScopeDetails,
            Permission,
            GroupPermission,
            CollectionPermissionSet,
            HubuumClass,
            HubuumClassHistory,
            HistoryResponse<HubuumClassHistory>,
            HistoryResponse<HubuumObjectHistory>,
            HistoryResponse<CollectionHistory>,
            HistoryResponse<ExportTemplateHistory>,
            HistoryResponse<RemoteTargetHistory>,
            HubuumClassExpanded,
            HubuumClassWithPath,
            NewHubuumClass,
            UpdateHubuumClass,
            HubuumClassRelation,
            NewHubuumClassRelation,
            NewHubuumClassRelationFromClass,
            UpdateClassRelationLabels,
            HubuumObject,
            HubuumObjectHistory,
            NewHubuumObject,
            UpdateHubuumObject,
            NewHubuumObjectRequest,
            UpdateHubuumObjectRequest,
            ObjectDataPatchDocument,
            ComputedResultType,
            ComputedFieldDefinition,
            ComputedFieldDefinitionRequest,
            PersonalComputedFieldDefinitionRequest,
            ComputedFieldDefinitionPatch,
            ClassComputationState,
            ComputedFieldListResponse,
            ComputedFieldMutationResponse,
            ComputedFieldDeleteResponse,
            ComputedFieldPreviewRequest,
            ComputedFieldPreviewResponse,
            ComputedFieldErrorResponse,
            ComputedScopeResponse,
            SharedComputedScopeResponse,
            ComputedObjectScopesResponse,
            HubuumObjectComputedResponse,
            HubuumObjectReadResponse,
            ObjectAggregateValueState,
            ObjectAggregateDimensionValue,
            ObjectAggregateMeasureOperation,
            ObjectAggregateMeasureState,
            ObjectAggregateMeasureValue,
            ObjectAggregateRow,
            HubuumObjectWithPath,
            HubuumObjectRelation,
            RelatedClassGraph,
            RelatedObjectGraph,
            NewHubuumObjectRelation,
            TaskKind,
            TaskStatus,
            TaskProgress,
            TaskLinks,
            TaskDiscoveryTarget,
            SchemaProfileTaskDetails,
            SchemaTaskDetails,
            RebuildTaskDetails,
            RemoteCallTaskDetails,
            RetainedImportDetails,
            RetainedExportDetails,
            RetainedBackupDetails,
            TaskOutputDiscoveryState,
            ImportTaskDetails,
            ExportTaskDetails,
            BackupTaskDetails,
            TaskDetails,
            TaskResponse,
            TaskEventResponse,
            EventResponse,
            Provenance,
            ProvenanceActor,
            ProvenancePrincipal,
            EventDeliveryStatus,
            EventDeliveryResponse,
            EventDeliveryUpdateResponse,
            EventDeliveryStatusCounts,
            EventWorkerWakeupStats,
            EventWorkerHealth,
            EventFanoutHealth,
            EventDeliveryQueueHealth,
            EventSinkDeliveryHealth,
            EventSubscriptionDeliveryHealth,
            EventDeliveryHealthResponse,
            CollectionEventSink,
            EventSinkRouting,
            EventSinkKind,
            EventSink,
            NewEventSink,
            UpdateEventSink,
            EventSubscription,
            EventSubscriptionFilter,
            EventSubject,
            NewEventSubscription,
            UpdateEventSubscription,
            ImportTaskResultResponse,
            ImportAtomicity,
            ImportCollisionPolicy,
            ImportWriteCondition,
            ImportPermissionPolicy,
            ImportMode,
            IdentityScopeKey,
            PrincipalKey,
            EventSinkKey,
            RestoreTimestamps,
            CollectionKey,
            CollectionHistory,
            GroupKey,
            ClassKey,
            ObjectKey,
            ImportCollectionInput,
            ImportClassInput,
            ImportObjectInput,
            ImportComputedFieldVisibility,
            ImportComputedFieldInput,
            ImportClassRelationInput,
            ImportObjectRelationInput,
            ImportCollectionPermissionInput,
            ImportIdentityScopeInput,
            ImportGroupInput,
            ImportPrincipalSubtype,
            ImportPrincipalInput,
            ImportMembershipSourceInput,
            ImportGroupMembershipInput,
            ImportExportTemplateInput,
            ImportRemoteTargetInput,
            ImportEventSinkInput,
            ImportEventSubscriptionInput,
            ImportGraph,
            ImportRequest,
            BackupRequest,
            BackupManifest,
            BackupState,
            BackupHistory,
            BackupDocument,
            RestoreJobStatus,
            RestoreValidationSummary,
            RestoreStageResponse,
            RestoreConfirmRequest,
            ExportScopeKind,
            ExportScope,
            ExportContentType,
            ExportMissingDataPolicy,
            ExportLimits,
            ExportRequest,
            ExportWarning,
            ExportMeta,
            ExportJsonResponse,
            UnifiedSearchKind,
            UnifiedSearchResponse,
            UnifiedSearchBatchResponse,
            UnifiedSearchStartedEvent,
            UnifiedSearchDoneEvent,
            UnifiedSearchErrorEvent,
            StructuredSearchStartedEvent,
            StructuredSearchDoneEvent,
            StructuredSearchErrorEvent,
            ExportTemplateID,
            ExportTemplateKind,
            ExportTemplate,
            ExportTemplateHistory,
            ExportTemplateRunRequest,
            NewExportTemplate,
            UpdateExportTemplate,
            RemoteTargetID,
            RemoteHttpMethod,
            RemoteAuthConfig,
            RemoteTarget,
            RemoteTargetHistory,
            NewRemoteTarget,
            UpdateRemoteTarget,
            RemoteTargetSubjectType,
            RemoteInvocationSubject,
            RemoteInvocationParameters,
            RemoteInvocationBodyOverride,
            RemoteTargetInvokeRequest,
            RemoteCallResult
        )
    ),
    modifiers(&SecurityAddon, &OperationDefaults),
    tags(
        (name = "config", description = "Public client configuration and capability discovery"),
        (name = "admin", description = "Administrative process inspection endpoints"),
        (name = "meta", description = "Meta and database state endpoints"),
        (name = "auth", description = "Authentication and token lifecycle"),
        (name = "users", description = "User management endpoints"),
        (name = "service-accounts", description = "Service-account (non-human principal) management endpoints"),
        (name = "principals", description = "Principal-scoped token, group, and permission endpoints (human users and service accounts)"),
        (name = "groups", description = "Group management endpoints"),
        (name = "collections", description = "Collection and permission endpoints"),
        (name = "relations", description = "Class and object relation endpoints"),
        (name = "search", description = "Unified search endpoints"),
        (name = "classes", description = "Class and object-in-class endpoints"),
        (name = "tasks", description = "Generic long-running task endpoints"),
        (name = "imports", description = "Import submission and import-specific result endpoints"),
        (name = "exports", description = "Server-side export execution endpoints"),
        (name = "backups", description = "Administrator-only full-system logical backup creation and retrieval endpoints"),
        (name = "restores", description = "Administrator-only staged, destructive full-system restore endpoints"),
        (name = "export-templates", description = "Stored export template management endpoints"),
        (name = "remote-targets", description = "Collection-scoped remote target management and invocation endpoints")
    )
)]
pub struct ApiDoc;

pub async fn openapi_json() -> impl Responder {
    HttpResponse::Ok().json(ApiDoc::openapi())
}

#[derive(Serialize, ToSchema)]
#[schema(example = api_error_response_example)]
pub struct ApiErrorResponse {
    /// Machine-readable reason for errors with a client recovery flow.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    pub error: String,
    pub message: String,
}

#[derive(Serialize, ToSchema)]
#[schema(example = message_response_example)]
pub struct MessageResponse {
    message: String,
}

impl MessageResponse {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

#[derive(Serialize, ToSchema)]
#[schema(example = login_response_example)]
pub struct LoginResponse {
    /// Raw bearer token, shown only in this issuance response.
    token: String,
    /// Authoritative expiry persisted for the newly issued token.
    expires_at: chrono::NaiveDateTime,
}

impl LoginResponse {
    pub fn new(token: impl Into<String>, expires_at: chrono::NaiveDateTime) -> Self {
        Self {
            token: token.into(),
            expires_at,
        }
    }

    pub fn from_issued(token: &crate::models::IssuedToken) -> Self {
        Self::new(token.get_token(), token.expires_at())
    }
}

#[derive(Serialize, ToSchema)]
#[schema(example = counts_response_example)]
pub struct CountsResponse {
    pub total_objects: i64,
    pub total_classes: i64,
    pub total_collections: i64,
    pub objects_per_class: Vec<ObjectsByClass>,
}

fn api_error_response_example() -> ApiErrorResponse {
    ApiErrorResponse {
        reason: None,
        error: "Unauthorized".to_string(),
        message: "Authentication failure".to_string(),
    }
}

fn message_response_example() -> MessageResponse {
    MessageResponse::new("Token is valid.")
}

fn login_response_example() -> LoginResponse {
    LoginResponse::new(
        "eyJhbGciOi...example-token",
        chrono::NaiveDate::from_ymd_opt(2026, 7, 26)
            .and_then(|date| date.and_hms_opt(12, 0, 0))
            .expect("example token expiry must be valid"),
    )
}

fn counts_response_example() -> CountsResponse {
    CountsResponse {
        total_objects: 42,
        total_classes: 7,
        total_collections: 3,
        objects_per_class: vec![ObjectsByClass {
            hubuum_class_id: 1,
            count: 6,
        }],
    }
}

struct SecurityAddon;

impl Modify for SecurityAddon {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        let components = openapi.components.get_or_insert_with(Default::default);
        components.add_security_scheme(
            "bearer_auth",
            SecurityScheme::Http(Http::new(HttpAuthScheme::Bearer)),
        );
        components.add_security_scheme(
            "restore_capability",
            SecurityScheme::ApiKey(ApiKey::Header(ApiKeyValue::with_description(
                "X-Hubuum-Restore-Capability",
                "One-time restore capability returned when a backup document is staged",
            ))),
        );
    }
}

struct OperationDefaults;

impl Modify for OperationDefaults {
    fn modify(&self, openapi: &mut OpenApiDoc) {
        for (path, path_item) in &mut openapi.paths.paths {
            for_each_operation_mut(path_item, |method, operation| {
                let operation_id = build_operation_id(method, path);
                operation.operation_id = Some(operation_id.clone());

                if operation
                    .summary
                    .as_ref()
                    .map(|s| s.trim().is_empty())
                    .unwrap_or(true)
                {
                    operation.summary = Some(title_case(&split_identifier_words(&operation_id)));
                }

                if operation
                    .description
                    .as_ref()
                    .map(|s| s.trim().is_empty())
                    .unwrap_or(true)
                {
                    operation.description = Some(format!(
                        "Auto-generated documentation for {} {}.",
                        method.to_uppercase(),
                        path
                    ));
                }

                if path.ends_with("/history/timeline") && method.eq_ignore_ascii_case("get") {
                    add_history_navigation_docs(operation);
                } else if is_cursor_paginated_get(path, method)
                    || operation_has_cursor_pagination(operation)
                {
                    add_cursor_pagination_docs(operation);
                }

                if is_unified_search_get(path, method) {
                    add_unified_search_pagination_docs(operation);
                }

                if is_conditional_operation(path, method) {
                    add_conditional_request_docs(operation);
                }

                if is_etagged_operation(path, method) {
                    add_etag_response_docs(operation);
                }
            });
        }
    }
}

// Keep these operation sets disjoint so every endpoint has one authoritative
// documentation classification. Conditional endpoints whose composite
// response is not revision-owned deliberately live in the untagged set.
const CONDITIONAL_ETAGGED_OPERATIONS: &[(&str, &str)] = &[
    ("patch", "/api/v1/iam/users/{user_id}"),
    ("delete", "/api/v1/iam/users/{user_id}"),
    ("post", "/api/v1/iam/users/{user_id}/anonymize"),
    ("patch", "/api/v1/iam/groups/{group_id}"),
    ("delete", "/api/v1/iam/groups/{group_id}"),
    (
        "post",
        "/api/v1/iam/groups/{group_id}/members/{principal_id}",
    ),
    (
        "delete",
        "/api/v1/iam/groups/{group_id}/members/{principal_id}",
    ),
    ("patch", "/api/v1/iam/service-accounts/{service_account_id}"),
    (
        "post",
        "/api/v1/iam/service-accounts/{service_account_id}/disable",
    ),
    (
        "delete",
        "/api/v1/iam/service-accounts/{service_account_id}",
    ),
    (
        "post",
        "/api/v1/iam/principals/{principal_id}/tokens/{token_id}/revoke",
    ),
    ("put", "/api/v1/iam/principals/{principal_id}/settings"),
    ("patch", "/api/v1/iam/principals/{principal_id}/settings"),
    ("delete", "/api/v1/iam/principals/{principal_id}/settings"),
    ("put", "/api/v1/iam/me/settings"),
    ("patch", "/api/v1/iam/me/settings"),
    ("delete", "/api/v1/iam/me/settings"),
    ("patch", "/api/v1/collections/{collection_id}"),
    ("delete", "/api/v1/collections/{collection_id}"),
    ("put", "/api/v1/collections/{collection_id}/parent"),
    (
        "post",
        "/api/v1/collections/{collection_id}/permissions/group/{group_id}",
    ),
    (
        "put",
        "/api/v1/collections/{collection_id}/permissions/group/{group_id}",
    ),
    (
        "delete",
        "/api/v1/collections/{collection_id}/permissions/group/{group_id}",
    ),
    (
        "post",
        "/api/v1/collections/{collection_id}/permissions/group/{group_id}/{permission}",
    ),
    (
        "delete",
        "/api/v1/collections/{collection_id}/permissions/group/{group_id}/{permission}",
    ),
    ("patch", "/api/v1/event-sinks/{sink_id}"),
    (
        "patch",
        "/api/v1/collections/{collection_id}/event-sinks/{sink_id}",
    ),
    (
        "delete",
        "/api/v1/collections/{collection_id}/event-sinks/{sink_id}",
    ),
    ("delete", "/api/v1/event-sinks/{sink_id}"),
    (
        "patch",
        "/api/v1/system-event-subscriptions/{subscription_id}",
    ),
    (
        "delete",
        "/api/v1/system-event-subscriptions/{subscription_id}",
    ),
    (
        "patch",
        "/api/v1/collections/{collection_id}/event-subscriptions/{subscription_id}",
    ),
    (
        "delete",
        "/api/v1/collections/{collection_id}/event-subscriptions/{subscription_id}",
    ),
    ("patch", "/api/v1/export-templates/{template_id}"),
    ("delete", "/api/v1/export-templates/{template_id}"),
    ("patch", "/api/v1/remote-targets/{target_id}"),
    ("delete", "/api/v1/remote-targets/{target_id}"),
    ("patch", "/api/v1/relations/classes/{relation_id}"),
    ("delete", "/api/v1/relations/classes/{relation_id}"),
    ("delete", "/api/v1/relations/objects/{relation_id}"),
    ("delete", "/api/v1/classes/{class_id}"),
    ("delete", "/api/v1/classes/by-name/{class_name}"),
    ("patch", "/api/v1/classes/{class_id}/{object_id}"),
    (
        "patch",
        "/api/v1/classes/by-name/{class_name}/objects/by-name/{object_name}",
    ),
    ("patch", "/api/v1/classes/{class_id}/{object_id}/data"),
    (
        "patch",
        "/api/v1/classes/by-name/{class_name}/objects/by-name/{object_name}/data",
    ),
    ("delete", "/api/v1/classes/{class_id}/{object_id}"),
    (
        "delete",
        "/api/v1/classes/by-name/{class_name}/objects/by-name/{object_name}",
    ),
    (
        "delete",
        "/api/v1/classes/{class_id}/relations/{relation_id}",
    ),
    (
        "delete",
        "/api/v1/classes/{class_id}/{from_object_id}/relations/{to_class_id}/{to_object_id}",
    ),
    ("patch", "/api/v1/iam/me/computed-fields/{field_id}"),
    ("delete", "/api/v1/iam/me/computed-fields/{field_id}"),
];

const CONDITIONAL_UNTAGGED_OPERATIONS: &[(&str, &str)] = &[
    ("patch", "/api/v1/classes/{class_id}"),
    ("patch", "/api/v1/classes/by-name/{class_name}"),
    (
        "patch",
        "/api/v1/classes/{class_id}/computed-fields/{field_id}",
    ),
    (
        "delete",
        "/api/v1/classes/{class_id}/computed-fields/{field_id}",
    ),
];

const ETAGGED_OPERATIONS: &[(&str, &str)] = &[
    ("post", "/api/v1/iam/users"),
    ("get", "/api/v1/iam/users/{user_id}"),
    ("post", "/api/v1/iam/groups"),
    ("get", "/api/v1/iam/groups/{group_id}"),
    (
        "get",
        "/api/v1/iam/groups/{group_id}/members/{principal_id}",
    ),
    ("post", "/api/v1/iam/service-accounts"),
    ("get", "/api/v1/iam/service-accounts/{service_account_id}"),
    (
        "get",
        "/api/v1/iam/principals/{principal_id}/tokens/{token_id}",
    ),
    ("get", "/api/v1/iam/principals/{principal_id}/settings"),
    ("get", "/api/v1/iam/me/settings"),
    ("post", "/api/v1/collections"),
    ("get", "/api/v1/collections/{collection_id}"),
    ("get", "/api/v1/collections/{collection_id}/permissions"),
    (
        "get",
        "/api/v1/collections/{collection_id}/permissions/group/{group_id}",
    ),
    ("post", "/api/v1/event-sinks"),
    ("post", "/api/v1/collections/{collection_id}/event-sinks"),
    (
        "get",
        "/api/v1/collections/{collection_id}/event-sinks/{sink_id}",
    ),
    ("get", "/api/v1/event-sinks/{sink_id}"),
    ("post", "/api/v1/system-event-subscriptions"),
    (
        "get",
        "/api/v1/system-event-subscriptions/{subscription_id}",
    ),
    (
        "post",
        "/api/v1/collections/{collection_id}/event-subscriptions",
    ),
    (
        "get",
        "/api/v1/collections/{collection_id}/event-subscriptions/{subscription_id}",
    ),
    ("post", "/api/v1/export-templates"),
    ("get", "/api/v1/export-templates/{template_id}"),
    ("post", "/api/v1/remote-targets"),
    ("get", "/api/v1/remote-targets/{target_id}"),
    ("get", "/api/v1/classes/{class_id}"),
    ("get", "/api/v1/classes/by-name/{class_name}"),
    ("post", "/api/v1/classes/{class_id}/"),
    ("post", "/api/v1/classes/by-name/{class_name}/objects"),
    ("get", "/api/v1/classes/{class_id}/{object_id}"),
    (
        "get",
        "/api/v1/classes/by-name/{class_name}/objects/by-name/{object_name}",
    ),
    ("post", "/api/v1/classes/{class_id}/relations"),
    ("get", "/api/v1/classes/{class_id}/relations/{relation_id}"),
    (
        "post",
        "/api/v1/classes/{class_id}/{from_object_id}/relations/{to_class_id}/{to_object_id}",
    ),
    (
        "get",
        "/api/v1/classes/{class_id}/{from_object_id}/relations/{to_class_id}/{to_object_id}",
    ),
    ("post", "/api/v1/relations/classes"),
    ("get", "/api/v1/relations/classes/{relation_id}"),
    ("post", "/api/v1/relations/objects"),
    ("get", "/api/v1/relations/objects/{relation_id}"),
    (
        "get",
        "/api/v1/classes/{class_id}/computed-fields/{field_id}",
    ),
    ("get", "/api/v1/iam/me/computed-fields/{field_id}"),
    ("post", "/api/v1/iam/me/computed-fields"),
];

fn operation_matches(operations: &[(&str, &str)], path: &str, method: &str) -> bool {
    operations.iter().any(|(candidate_method, candidate_path)| {
        method.eq_ignore_ascii_case(candidate_method) && path == *candidate_path
    })
}

fn is_conditional_operation(path: &str, method: &str) -> bool {
    operation_matches(CONDITIONAL_ETAGGED_OPERATIONS, path, method)
        || operation_matches(CONDITIONAL_UNTAGGED_OPERATIONS, path, method)
}

fn is_etagged_operation(path: &str, method: &str) -> bool {
    operation_matches(ETAGGED_OPERATIONS, path, method)
        || operation_matches(CONDITIONAL_ETAGGED_OPERATIONS, path, method)
}

fn add_conditional_request_docs(operation: &mut Operation) {
    let parameters = operation.parameters.get_or_insert_with(Vec::new);
    if !parameters.iter().any(|parameter| {
        matches!(parameter, RefOr::T(parameter)
            if parameter.name.eq_ignore_ascii_case("If-Match")
                && matches!(parameter.parameter_in, ParameterIn::Header))
    }) {
        parameters.push(
            ParameterBuilder::new()
                .name("If-Match")
                .parameter_in(ParameterIn::Header)
                .required(Required::False)
                .description(Some(
                    "Strong ETag from the latest tagged representation. The header is optional for compatibility; when supplied, the mutation is applied only if the resource still matches.",
                ))
                .schema(Some(Object::with_type(Type::String)))
                .build()
                .into(),
        );
    }

    operation
        .responses
        .responses
        .entry("412".to_string())
        .or_insert_with(|| {
            RefOr::T(Response::new(
                "Precondition failed because If-Match does not match the current resource revision.",
            ))
        });
}

fn add_etag_response_docs(operation: &mut Operation) {
    for (status, response) in &mut operation.responses.responses {
        if !status.starts_with('2') {
            continue;
        }
        let RefOr::T(response) = response else {
            continue;
        };
        response
            .headers
            .entry("ETag".to_string())
            .or_insert_with(|| {
                let mut header = Header::default();
                header.description = Some(
                    "Strong validator for this revision-owned representation. It may be omitted when an expanded or backend-specific representation is returned, or when a deleted aggregate no longer exists."
                        .to_string(),
                );
                header.into()
            });
    }
}

const CURSOR_PAGINATED_GET_PATHS: &[&str] = &[
    "/api/v1/iam/users",
    "/api/v1/iam/users/{user_id}/events",
    "/api/v1/iam/service-accounts",
    "/api/v1/iam/me/tokens",
    "/api/v1/iam/me/groups",
    "/api/v1/iam/me/computed-fields",
    "/api/v1/iam/principals/{principal_id}/tokens",
    "/api/v1/iam/principals/{principal_id}/groups",
    "/api/v1/iam/groups",
    "/api/v1/iam/groups/{group_id}/members",
    "/api/v1/iam/groups/{group_id}/events",
    "/api/v1/collections",
    "/api/v1/collections/{collection_id}/event-subscriptions",
    "/api/v1/collections/{collection_id}/events",
    "/api/v1/collections/{collection_id}/history",
    "/api/v1/collections/{collection_id}/permissions",
    "/api/v1/collections/{collection_id}/permissions/principal/{principal_id}",
    "/api/v1/collections/{collection_id}/has_permissions/{permission}",
    "/api/v1/event-deliveries",
    "/api/v1/event-sinks",
    "/api/v1/imports/{task_id}/results",
    "/api/v1/tasks",
    "/api/v1/tasks/{task_id}/events",
    "/api/v1/export-templates",
    "/api/v1/export-templates/{template_id}/events",
    "/api/v1/export-templates/{template_id}/history",
    "/api/v1/remote-targets",
    "/api/v1/remote-targets/{target_id}/events",
    "/api/v1/remote-targets/{remote_target_id}/history",
    "/api/v1/events",
    "/api/v1/relations/classes",
    "/api/v1/relations/objects",
    "/api/v1/classes",
    "/api/v1/classes/{class_id}/events",
    "/api/v1/classes/{class_id}/history",
    "/api/v1/classes/{class_id}/permissions",
    "/api/v1/classes/{class_id}/related/classes",
    "/api/v1/classes/{class_id}/related/relations",
    "/api/v1/classes/{class_id}/",
    "/api/v1/classes/{class_id}/{object_id}/events",
    "/api/v1/classes/{class_id}/{object_id}/history",
    "/api/v1/classes/{class_id}/objects/history",
    "/api/v1/classes/{class_id}/objects/history/as-of",
    "/api/v1/classes/{class_id}/objects/{object_id}/related/objects",
    "/api/v1/classes/{class_id}/objects/{object_id}/related/relations",
    "/api/v1/classes/by-name/{class_name}/permissions",
    "/api/v1/classes/by-name/{class_name}/related/classes",
    "/api/v1/classes/by-name/{class_name}/related/relations",
    "/api/v1/classes/by-name/{class_name}/objects",
    "/api/v1/classes/by-name/{class_name}/objects/by-name/{object_name}/related/objects",
    "/api/v1/classes/by-name/{class_name}/objects/by-name/{object_name}/related/relations",
];

fn is_cursor_paginated_get(path: &str, method: &str) -> bool {
    method.eq_ignore_ascii_case("get") && CURSOR_PAGINATED_GET_PATHS.contains(&path)
}

fn is_unified_search_get(path: &str, method: &str) -> bool {
    method.eq_ignore_ascii_case("get") && matches!(path, "/api/v1/search" | "/api/v1/search/stream")
}

fn operation_has_cursor_pagination(operation: &Operation) -> bool {
    let Some(parameters) = operation.parameters.as_ref() else {
        return false;
    };
    let has_query_parameter = |name: &str| {
        parameters.iter().any(|parameter| {
            matches!(parameter, RefOr::T(parameter)
                if parameter.name == name && matches!(parameter.parameter_in, ParameterIn::Query))
        })
    };

    has_query_parameter("limit") && has_query_parameter("cursor")
}

fn add_cursor_pagination_docs(operation: &mut Operation) {
    let limits = page_limits_or_defaults();
    let default_page_limit = limits.default_limit();
    let max_page_limit = limits.maximum_limit();
    let parameters = operation.parameters.get_or_insert_with(Vec::new);
    upsert_page_limit_parameter(
        parameters,
        "limit",
        &format!(
            "Maximum number of items to return. Defaults to {default_page_limit}. The server clamps values above {max_page_limit}."
        ),
        default_page_limit,
        max_page_limit,
    );
    ensure_query_parameter(
        parameters,
        "sort",
        "Comma-separated sort fields. Cursor pagination uses the requested sort order and appends a stable tie-breaker automatically.",
        Type::String,
    );
    ensure_query_parameter(
        parameters,
        "cursor",
        "Opaque cursor returned in the X-Next-Cursor response header from a previous page. Supply it unchanged to fetch the next page.",
        Type::String,
    );
    ensure_query_parameter(
        parameters,
        "include_total",
        "Whether to execute an exact count query and return X-Total-Count. Defaults to true; set false on latency-sensitive requests that do not need the count.",
        Type::Boolean,
    );

    if let Some(description) = operation.description.as_mut() {
        let pagination_text = format!(
            " Supports cursor pagination through the `limit`, `sort`, and `cursor` query parameters. The exact total hit count is returned in the `{TOTAL_COUNT_HEADER}` response header unless `include_total=false`, and the next page cursor is returned in the `{NEXT_CURSOR_HEADER}` response header."
        );
        if !description.contains(NEXT_CURSOR_HEADER) {
            description.push_str(&pagination_text);
        }
    }

    if let Some(response) = operation.responses.responses.get_mut("200") {
        add_total_count_header(response);
        add_next_cursor_header(response);
        add_page_limit_header(response);
    }
}

fn add_history_navigation_docs(operation: &mut Operation) {
    let limits = page_limits_or_defaults();
    upsert_page_limit_parameter(
        operation.parameters.get_or_insert_with(Vec::new),
        "limit",
        "Maximum navigation entries; values above the configured maximum are clamped.",
        limits.default_limit(),
        limits.maximum_limit(),
    );
    if let Some(description) = operation.description.as_mut() {
        description.push_str(" Entries are ordered by (valid_from, history_id), descending for older and ascending for newer. Reuse X-Next-Cursor with the same selection and direction. include_total defaults to false. Use direction=newer&limit=1 or direction=older&limit=1 without a selection to discover the first or last authorized available entry; these bounds do not prove uninterrupted retention.");
    }
    if let Some(response) = operation.responses.responses.get_mut("200") {
        add_total_count_header(response);
        add_next_cursor_header(response);
        add_page_limit_header(response);
    }
}

fn add_unified_search_pagination_docs(operation: &mut Operation) {
    let limits = page_limits_or_defaults();
    let default_page_limit = limits.default_limit();
    let max_page_limit = limits.maximum_limit();
    upsert_page_limit_parameter(
        operation.parameters.get_or_insert_with(Vec::new),
        "limit_per_kind",
        &format!(
            "Maximum results per kind. Defaults to {default_page_limit}. The server clamps values above {max_page_limit}."
        ),
        default_page_limit,
        max_page_limit,
    );

    if let Some(response) = operation.responses.responses.get_mut("200") {
        add_page_limit_header(response);
    }
}

fn upsert_page_limit_parameter(
    parameters: &mut Vec<RefOr<Parameter>>,
    name: &str,
    description: &str,
    default_page_limit: usize,
    max_page_limit: usize,
) {
    let schema = ObjectBuilder::new()
        .schema_type(Type::Integer)
        .minimum(Some(1usize))
        .maximum(Some(max_page_limit))
        .default(Some(serde_json::json!(default_page_limit)))
        .build();

    if let Some(RefOr::T(parameter)) = parameters.iter_mut().find(|parameter| {
        matches!(parameter, RefOr::T(parameter)
                if parameter.name == name && matches!(parameter.parameter_in, ParameterIn::Query))
    }) {
        parameter.description = Some(description.to_string());
        parameter.required = Required::False;
        parameter.schema = Some(RefOr::T(Schema::Object(schema)));
        return;
    }

    parameters.push(
        ParameterBuilder::new()
            .name(name)
            .parameter_in(ParameterIn::Query)
            .required(Required::False)
            .description(Some(description))
            .schema(Some(schema))
            .build()
            .into(),
    );
}

fn ensure_query_parameter(
    parameters: &mut Vec<RefOr<Parameter>>,
    name: &str,
    description: &str,
    schema_type: Type,
) {
    if parameters.iter().any(|parameter| {
        matches!(parameter, RefOr::T(parameter)
                if parameter.name == name && matches!(parameter.parameter_in, ParameterIn::Query))
    }) {
        return;
    }

    parameters.push(
        ParameterBuilder::new()
            .name(name)
            .parameter_in(ParameterIn::Query)
            .required(Required::False)
            .description(Some(description))
            .schema(Some(Object::with_type(schema_type)))
            .build()
            .into(),
    );
}

fn add_next_cursor_header(response: &mut RefOr<utoipa::openapi::response::Response>) {
    let RefOr::T(response) = response else {
        return;
    };

    response
        .headers
        .entry(NEXT_CURSOR_HEADER.to_string())
        .or_insert_with(|| {
            let mut header = Header::default();
            header.description = Some(
                "Opaque cursor for the next page. This header is omitted when there are no more results."
                    .to_string(),
            );
            header.into()
        });

    if !response.description.contains(NEXT_CURSOR_HEADER) {
        response.description.push_str(&format!(
            " The response body contains the current page items as a JSON array. Use the `{TOTAL_COUNT_HEADER}` header for the exact number of matching results. Use the `{NEXT_CURSOR_HEADER}` header, when present, to request the next page."
        ));
    }
}

fn add_total_count_header(response: &mut RefOr<utoipa::openapi::response::Response>) {
    let RefOr::T(response) = response else {
        return;
    };

    response
        .headers
        .entry(TOTAL_COUNT_HEADER.to_string())
        .or_insert_with(|| {
            let mut header = Header::default();
            header.description = Some(
            "Exact total number of results matching the current filter set, independent of cursor pagination. Omitted when include_total=false."
                    .to_string(),
            );
            header.into()
        });
}

fn add_page_limit_header(response: &mut RefOr<utoipa::openapi::response::Response>) {
    let RefOr::T(response) = response else {
        return;
    };

    response
        .headers
        .entry(PAGE_LIMIT_HEADER.to_string())
        .or_insert_with(|| {
            let mut header = Header::default();
            header.description = Some(
                "Effective page size used by the server after applying the configured default and maximum."
                    .to_string(),
            );
            header.into()
        });
}

fn for_each_operation_mut(
    path_item: &mut PathItem,
    mut callback: impl FnMut(&str, &mut Operation),
) {
    if let Some(operation) = path_item.get.as_mut() {
        callback("get", operation);
    }
    if let Some(operation) = path_item.post.as_mut() {
        callback("post", operation);
    }
    if let Some(operation) = path_item.put.as_mut() {
        callback("put", operation);
    }
    if let Some(operation) = path_item.patch.as_mut() {
        callback("patch", operation);
    }
    if let Some(operation) = path_item.delete.as_mut() {
        callback("delete", operation);
    }
    if let Some(operation) = path_item.options.as_mut() {
        callback("options", operation);
    }
    if let Some(operation) = path_item.head.as_mut() {
        callback("head", operation);
    }
    if let Some(operation) = path_item.trace.as_mut() {
        callback("trace", operation);
    }
}

fn build_operation_id(method: &str, path: &str) -> String {
    let mut parts = vec![method.to_ascii_lowercase()];

    for segment in path.trim_matches('/').split('/') {
        if segment.is_empty() {
            continue;
        }

        if segment.starts_with('{') && segment.ends_with('}') {
            parts.push("by".to_string());
            parts.push(segment[1..segment.len() - 1].to_string());
        } else {
            parts.push(segment.to_string());
        }
    }

    if path.ends_with('/') && path.len() > 1 {
        parts.push("trailing".to_string());
    }

    let mut operation_id = String::new();
    for (index, part) in parts.iter().enumerate() {
        let mut normalized = split_identifier_words(part);
        if index == 0
            && let Some(first) = normalized.first_mut()
        {
            *first = first.to_ascii_lowercase();
        }
        operation_id.push_str(&camel_case(&normalized, index == 0));
    }

    operation_id
}

fn split_identifier_words(input: &str) -> Vec<String> {
    let mut words = Vec::new();
    let mut current = String::new();

    for ch in input.chars() {
        if !ch.is_alphanumeric() {
            if !current.is_empty() {
                words.push(current.clone());
                current.clear();
            }
            continue;
        }

        if ch.is_uppercase() && !current.is_empty() {
            words.push(current.clone());
            current.clear();
        }

        current.push(ch.to_ascii_lowercase());
    }

    if !current.is_empty() {
        words.push(current);
    }

    words
}

fn camel_case(words: &[String], keep_first_lower: bool) -> String {
    let mut out = String::new();
    for (idx, word) in words.iter().enumerate() {
        if keep_first_lower && idx == 0 {
            out.push_str(word);
            continue;
        }
        out.push_str(&capitalize(word));
    }
    out
}

fn title_case(words: &[String]) -> String {
    words
        .iter()
        .map(|w| capitalize(w))
        .collect::<Vec<_>>()
        .join(" ")
}

fn capitalize(input: &str) -> String {
    let mut chars = input.chars();
    match chars.next() {
        Some(first) => format!("{}{}", first.to_ascii_uppercase(), chars.as_str()),
        None => String::new(),
    }
}

#[cfg(test)]
mod tests;
