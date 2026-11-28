use super::*;
use crate::models::user::{
    MAX_LOGIN_IDENTITY_SCOPE_CHARACTERS, MAX_LOGIN_NAME_CHARACTERS, MAX_LOGIN_PASSWORD_CHARACTERS,
};
use actix_web::{
    App,
    http::{Method, StatusCode},
};
use rstest::rstest;
use serde_json::Value;
use std::collections::{BTreeSet, HashSet};

fn openapi_json() -> Value {
    serde_json::to_value(ApiDoc::openapi()).expect("OpenAPI should serialize to JSON")
}

#[test]
fn openapi_local_references_resolve() {
    let document = openapi_json();
    let mut pending = vec![&document];
    while let Some(value) = pending.pop() {
        match value {
            Value::Object(fields) => {
                if let Some(reference) = fields.get("$ref").and_then(Value::as_str)
                    && let Some(pointer) = reference.strip_prefix('#')
                {
                    assert!(
                        document.pointer(pointer).is_some(),
                        "Unresolved OpenAPI reference: {reference}"
                    );
                }
                pending.extend(fields.values());
            }
            Value::Array(items) => pending.extend(items),
            _ => {}
        }
    }
}

#[test]
fn openapi_excludes_persisted_token_storage_schema() {
    let json = openapi_json();
    let schemas = json
        .pointer("/components/schemas")
        .and_then(Value::as_object)
        .expect("OpenAPI component schemas must be an object");

    assert!(!schemas.contains_key("PrincipalToken"));
    assert!(schemas.contains_key("PrincipalTokenMetadata"));
}

#[test]
fn revision_operations_have_one_documentation_classification() {
    let operations = CONDITIONAL_ETAGGED_OPERATIONS
        .iter()
        .chain(CONDITIONAL_UNTAGGED_OPERATIONS)
        .chain(ETAGGED_OPERATIONS)
        .copied()
        .collect::<Vec<_>>();
    let unique = operations.iter().copied().collect::<HashSet<_>>();

    assert_eq!(operations.len(), unique.len());
}

#[test]
fn conditional_operations_document_if_match_and_precondition_failure() {
    let json = openapi_json();

    for (method, path) in CONDITIONAL_ETAGGED_OPERATIONS
        .iter()
        .chain(CONDITIONAL_UNTAGGED_OPERATIONS)
    {
        let operation = &json["paths"][path][method];
        assert!(operation.is_object(), "missing {method} {path}");
        assert!(
            operation["parameters"]
                .as_array()
                .is_some_and(|parameters| parameters.iter().any(|parameter| {
                    parameter["name"] == "If-Match" && parameter["in"] == "header"
                })),
            "{method} {path} must document If-Match"
        );
        assert!(
            operation["responses"].get("412").is_some(),
            "{method} {path} must document 412"
        );
    }
}

#[test]
fn tagged_operations_document_etag_response_headers() {
    let json = openapi_json();

    for (method, path) in ETAGGED_OPERATIONS
        .iter()
        .chain(CONDITIONAL_ETAGGED_OPERATIONS)
    {
        let operation = &json["paths"][path][method];
        assert!(operation.is_object(), "missing {method} {path}");
        assert!(
            operation["responses"]
                .as_object()
                .is_some_and(|responses| responses.iter().any(|(status, response)| {
                    status.starts_with('2') && response["headers"].get("ETag").is_some()
                })),
            "{method} {path} must document ETag"
        );
    }
}

fn path_with_sample_params(path: &str) -> String {
    let mut out = String::with_capacity(path.len());
    let mut in_param = false;

    for ch in path.chars() {
        if ch == '{' {
            in_param = true;
            out.push('1');
            continue;
        }

        if ch == '}' {
            in_param = false;
            continue;
        }

        if !in_param {
            out.push(ch);
        }
    }

    out
}

#[test]
fn openapi_paths_match_mounted_routes() {
    let json = openapi_json();
    let actual_paths = json
        .get("paths")
        .and_then(Value::as_object)
        .expect("OpenAPI paths must be an object")
        .keys()
        .map(|p| p.to_string())
        .collect::<BTreeSet<_>>();

    let expected_paths = [
        "/healthz",
        "/readyz",
        "/api/v1/auth/login",
        "/api/v1/auth/refresh",
        "/api/v1/auth/providers",
        "/api/v1/auth/logout",
        "/api/v1/auth/logout_all",
        "/api/v1/auth/logout/token",
        "/api/v1/auth/logout/uid/{user_id}",
        "/api/v1/auth/validate",
        "/api/v0/meta/db",
        "/api/v0/meta/counts",
        "/api/v0/meta/tasks",
        "/api/v0/meta/login-rate-limit",
        "/api/v0/meta/login-rate-limit/{id}",
        "/api/v1/config",
        "/api/v1/admin/config",
        "/api/v1/admin/task-capacity",
        "/api/v1/admin/history-lifecycle/policies",
        "/api/v1/admin/history-lifecycle/policy-previews",
        "/api/v1/admin/history-lifecycle/inspect",
        "/api/v1/admin/history-lifecycle/accounting",
        "/api/v1/admin/history-lifecycle/runs",
        "/api/v1/admin/history-lifecycle/holds",
        "/api/v1/admin/history-lifecycle/holds/{hold_id}/release",
        "/api/v1/admin/history-lifecycle/operations/{operation_id}",
        "/api/v1/admin/api-request-capacity",
        "/api/v1/admin/runtime-settings",
        "/api/v1/admin/runtime-settings/schema",
        "/api/v1/admin/runtime-settings/{group}",
        "/api/v1/admin/runtime-settings/authentication",
        "/api/v1/admin/runtime-settings/authentication/preflight",
        "/api/v1/admin/runtime-settings/authorization",
        "/api/v1/admin/runtime-settings/pagination",
        "/api/v1/admin/runtime-settings/query_observations",
        "/api/v1/admin/runtime-settings/history/{group}",
        "/api/v1/iam/credential-approvals",
        "/api/v1/iam/credential-approvals/{approval_id}",
        "/api/v1/iam/users",
        "/api/v1/iam/users/{user_id}",
        "/api/v1/iam/users/{user_id}/events",
        "/api/v1/iam/users/{user_id}/anonymize",
        "/api/v1/iam/groups",
        "/api/v1/iam/groups/{group_id}",
        "/api/v1/iam/groups/{group_id}/events",
        "/api/v1/iam/groups/{group_id}/members",
        "/api/v1/iam/groups/{group_id}/members/{principal_id}",
        "/api/v1/iam/service-accounts",
        "/api/v1/iam/service-accounts/{service_account_id}",
        "/api/v1/iam/service-accounts/{service_account_id}/disable",
        "/api/v1/iam/me",
        "/api/v1/iam/me/computed-fields",
        "/api/v1/iam/me/computed-fields/preview",
        "/api/v1/iam/me/computed-fields/{field_id}",
        "/api/v1/iam/me/tokens",
        "/api/v1/iam/me/groups",
        "/api/v1/iam/me/permissions",
        "/api/v1/iam/me/settings",
        "/api/v1/iam/principals/{principal_id}/tokens",
        "/api/v1/iam/principals/{principal_id}/tokens/{token_id}",
        "/api/v1/iam/principals/{principal_id}/tokens/{token_id}/activity",
        "/api/v1/iam/principals/{principal_id}/tokens/{token_id}/renew",
        "/api/v1/iam/principals/{principal_id}/tokens/{token_id}/revoke",
        "/api/v1/iam/principals/{principal_id}/groups",
        "/api/v1/iam/principals/{principal_id}/permissions",
        "/api/v1/iam/principals/{principal_id}/settings",
        "/api/v1/collections",
        "/api/v1/collections/{collection_id}",
        "/api/v1/collections/{collection_id}/events",
        "/api/v1/collections/{collection_id}/history",
        "/api/v1/collections/{collection_id}/history/as-of",
        "/api/v1/collections/{collection_id}/children",
        "/api/v1/collections/{collection_id}/ancestors",
        "/api/v1/collections/{collection_id}/parent",
        "/api/v1/collections/{collection_id}/permissions",
        "/api/v1/collections/{collection_id}/permissions/group/{group_id}",
        "/api/v1/collections/{collection_id}/permissions/group/{group_id}/{permission}",
        "/api/v1/collections/{collection_id}/permissions/effective/group/{group_id}",
        "/api/v1/collections/{collection_id}/permissions/principal/{principal_id}",
        "/api/v1/collections/{collection_id}/permissions/effective/principal/{principal_id}",
        "/api/v1/collections/{collection_id}/has_permissions/{permission}",
        "/api/v1/imports",
        "/api/v1/imports/{task_id}",
        "/api/v1/imports/{task_id}/results",
        "/api/v1/exports",
        "/api/v1/exports/{task_id}",
        "/api/v1/exports/{task_id}/output",
        "/api/v1/backups",
        "/api/v1/backups/{task_id}",
        "/api/v1/backups/{task_id}/output",
        "/api/v1/restores",
        "/api/v1/restores/{restore_id}/confirm",
        "/api/v1/restores/{restore_id}/status",
        "/api/v1/search",
        "/api/v1/search/hydrate",
        "/api/v1/search/stream",
        "/api/v1/tasks",
        "/api/v1/tasks/{task_id}",
        "/api/v1/tasks/{task_id}/events",
        "/api/v1/tasks/{task_id}/cancel",
        "/api/v1/events",
        "/api/v1/event-deliveries",
        "/api/v1/event-deliveries/health",
        "/api/v1/event-deliveries/{delivery_id}",
        "/api/v1/event-deliveries/{delivery_id}/retry",
        "/api/v1/event-deliveries/{delivery_id}/dead",
        "/api/v1/event-sinks",
        "/api/v1/event-sinks/{sink_id}",
        "/api/v1/event-sinks/{sink_id}/collections",
        "/api/v1/event-sinks/{sink_id}/collections/{collection_id}",
        "/api/v1/event-sinks/{sink_id}/preview",
        "/api/v1/event-sinks/{sink_id}/test",
        "/api/v1/system-event-subscriptions",
        "/api/v1/system-event-subscriptions/{subscription_id}",
        "/api/v1/collections/{collection_id}/event-subscriptions",
        "/api/v1/collections/{collection_id}/event-sinks",
        "/api/v1/collections/{collection_id}/event-sinks/{sink_id}",
        "/api/v1/collections/{collection_id}/event-subscriptions/{subscription_id}",
        "/api/v1/export-templates",
        "/api/v1/export-templates/{template_id}",
        "/api/v1/export-templates/{template_id}/events",
        "/api/v1/export-templates/{template_id}/history",
        "/api/v1/export-templates/{template_id}/history/as-of",
        "/api/v1/export-templates/{template_id}/exports",
        "/api/v1/remote-targets",
        "/api/v1/remote-targets/{remote_target_id}/history",
        "/api/v1/remote-targets/{remote_target_id}/history/as-of",
        "/api/v1/remote-targets/{target_id}",
        "/api/v1/remote-targets/{target_id}/events",
        "/api/v1/remote-targets/{target_id}/invoke",
        "/api/v1/relations/classes",
        "/api/v1/relations/classes/{relation_id}",
        "/api/v1/relations/objects",
        "/api/v1/relations/objects/{relation_id}",
        "/api/v1/classes",
        "/api/v1/classes/{class_id}",
        "/api/v1/classes/{class_id}/fields",
        "/api/v1/classes/{class_id}/query-usage",
        "/api/v1/classes/{class_id}/query-usage/{declaration_id}",
        "/api/v1/classes/{class_id}/query-usage/analysis",
        "/api/v1/classes/by-name/{class_name}",
        "/api/v1/classes/by-name/{class_name}/fields",
        "/api/v1/classes/by-name/{class_name}/permissions",
        "/api/v1/classes/by-name/{class_name}/related/classes",
        "/api/v1/classes/by-name/{class_name}/related/relations",
        "/api/v1/classes/by-name/{class_name}/related/graph",
        "/api/v1/classes/by-name/{class_name}/object-aggregates",
        "/api/v1/classes/by-name/{class_name}/objects",
        "/api/v1/classes/by-name/{class_name}/objects/by-name/{object_name}",
        "/api/v1/classes/by-name/{class_name}/objects/by-name/{object_name}/data",
        "/api/v1/classes/by-name/{class_name}/objects/by-name/{object_name}/related/objects",
        "/api/v1/classes/by-name/{class_name}/objects/by-name/{object_name}/related/relations",
        "/api/v1/classes/by-name/{class_name}/objects/by-name/{object_name}/related/graph",
        "/api/v1/classes/{class_id}/schema",
        "/api/v1/classes/{class_id}/schema/objects",
        "/api/v1/classes/{class_id}/schema/revisions",
        "/api/v1/classes/{class_id}/schema/profiles",
        "/api/v1/classes/{class_id}/schema/profiles/{task_id}",
        "/api/v1/classes/{class_id}/schema/revisions/{revision}",
        "/api/v1/classes/{class_id}/schema/revisions/{revision}/activate",
        "/api/v1/classes/{class_id}/schema/revisions/{revision}/impact",
        "/api/v1/classes/{class_id}/schema/revisions/{revision}/revalidate",
        "/api/v1/classes/{class_id}/schema/tasks/{task_id}",
        "/api/v1/classes/{class_id}/schema/tasks/{task_id}/report",
        "/api/v1/classes/{class_id}/computed-fields",
        "/api/v1/classes/{class_id}/computed-fields/preview",
        "/api/v1/classes/{class_id}/computed-fields/rebuild",
        "/api/v1/classes/{class_id}/computed-fields/{field_id}",
        "/api/v1/classes/{class_id}/events",
        "/api/v1/classes/{class_id}/history",
        "/api/v1/classes/{class_id}/history/as-of",
        "/api/v1/classes/{class_id}/history/timeline",
        "/api/v1/classes/{class_id}/history/entries/{history_id}",
        "/api/v1/classes/{class_id}/permissions",
        "/api/v1/classes/{class_id}/object-aggregates",
        "/api/v1/classes/{class_id}/related/classes",
        "/api/v1/classes/{class_id}/related/relations",
        "/api/v1/classes/{class_id}/related/graph",
        "/api/v1/classes/{class_id}/relations",
        "/api/v1/classes/{class_id}/relations/history",
        "/api/v1/classes/{class_id}/relations/history/as-of",
        "/api/v1/classes/{class_id}/relations/{relation_id}",
        "/api/v1/classes/{class_id}/",
        "/api/v1/classes/{class_id}/{object_id}",
        "/api/v1/classes/{class_id}/{object_id}/data",
        "/api/v1/classes/{class_id}/{object_id}/events",
        "/api/v1/classes/{class_id}/{object_id}/history",
        "/api/v1/classes/{class_id}/{object_id}/history/as-of",
        "/api/v1/classes/{class_id}/{object_id}/history/computed/as-of",
        "/api/v1/classes/{class_id}/{object_id}/relations/history",
        "/api/v1/classes/{class_id}/{object_id}/relations/history/as-of",
        "/api/v1/classes/{class_id}/objects/history",
        "/api/v1/classes/{class_id}/objects/history/as-of",
        "/api/v1/classes/{class_id}/{object_id}/history/timeline",
        "/api/v1/classes/{class_id}/{object_id}/history/entries/{history_id}",
        "/api/v1/classes/{class_id}/objects/{object_id}/related/objects",
        "/api/v1/classes/{class_id}/objects/{object_id}/related/relations",
        "/api/v1/classes/{class_id}/objects/{object_id}/related/graph",
        "/api/v1/classes/{class_id}/{from_object_id}/relations/{to_class_id}/{to_object_id}",
    ]
    .into_iter()
    .map(String::from)
    .collect::<BTreeSet<_>>();

    assert_eq!(actual_paths, expected_paths);
}

#[test]
fn restore_upload_schema_references_backup_document() {
    let json = openapi_json();

    assert_eq!(
        json.pointer(
            "/paths/~1api~1v1~1restores/post/requestBody/content/application~1json/schema/$ref"
        )
        .and_then(Value::as_str),
        Some("#/components/schemas/BackupDocument")
    );
}

#[test]
fn restore_job_paths_document_positive_ids() {
    let json = openapi_json();

    for (path, method) in [
        ("~1api~1v1~1restores~1{restore_id}~1confirm", "post"),
        ("~1api~1v1~1restores~1{restore_id}~1status", "get"),
    ] {
        assert_eq!(
            json.pointer(&format!("/paths/{path}/{method}/parameters"))
                .and_then(Value::as_array)
                .and_then(|parameters| parameters
                    .iter()
                    .find(|parameter| parameter["name"] == "restore_id"))
                .and_then(|parameter| parameter.pointer("/schema/minimum")),
            Some(&Value::from(1)),
            "restore job path should document its positive-ID invariant"
        );
    }
}

#[test]
fn login_schema_documents_credential_length_limits() {
    let json = openapi_json();

    for (field, maximum) in [
        ("identity_scope", MAX_LOGIN_IDENTITY_SCOPE_CHARACTERS),
        ("name", MAX_LOGIN_NAME_CHARACTERS),
        ("password", MAX_LOGIN_PASSWORD_CHARACTERS),
    ] {
        assert_eq!(
            json.pointer(&format!(
                "/components/schemas/LoginUser/properties/{field}/maxLength"
            )),
            Some(&Value::from(maximum)),
            "LoginUser.{field} should document its enforced length limit"
        );
    }
}

#[test]
fn export_scope_ids_document_positive_minimum() {
    let json = openapi_json();

    for field in ["class_id", "object_id"] {
        assert_eq!(
            json.pointer(&format!(
                "/components/schemas/ExportScope/properties/{field}/minimum"
            )),
            Some(&Value::from(1)),
            "ExportScope.{field} should document its positive-ID invariant"
        );
    }
}

#[test]
fn creation_assignee_group_ids_document_positive_minimum() {
    let json = openapi_json();

    for pointer in [
        "/components/schemas/NewCollectionWithAssignee/properties/group_id/minimum",
        "/components/schemas/NewServiceAccount/properties/owner_group_id/minimum",
    ] {
        assert_eq!(
            json.pointer(pointer),
            Some(&Value::from(1)),
            "creation assignee group id should document its positive-ID invariant"
        );
    }
}

#[test]
fn object_relation_limits_document_positive_nullable_fields() {
    let json = openapi_json();
    assert_eq!(
        json.pointer("/components/schemas/ObjectRelationLimit/minimum"),
        Some(&Value::from(1)),
        "ObjectRelationLimit should document its positive-value invariant"
    );

    for schema_name in [
        "HubuumClassRelation",
        "ImportClassRelationInput",
        "NewHubuumClassRelation",
        "NewHubuumClassRelationFromClass",
    ] {
        for field in ["from_max_relations", "to_max_relations"] {
            let variants = json
                .pointer(&format!(
                    "/components/schemas/{schema_name}/properties/{field}/oneOf"
                ))
                .and_then(Value::as_array)
                .unwrap_or_else(|| {
                    panic!("{schema_name}.{field} should be a nullable limit schema")
                });
            assert!(
                variants
                    .iter()
                    .any(|variant| { variant.get("type").and_then(Value::as_str) == Some("null") }),
                "{schema_name}.{field} should allow null for unlimited relations"
            );
            assert!(
                variants.iter().any(|variant| {
                    variant.get("$ref").and_then(Value::as_str)
                        == Some("#/components/schemas/ObjectRelationLimit")
                }),
                "{schema_name}.{field} should reference ObjectRelationLimit"
            );
        }
    }
}

#[test]
fn object_data_json_patch_media_type_limits_and_statuses_are_documented() {
    let json = openapi_json();
    let operation_pointers = [
        "/paths/~1api~1v1~1classes~1{class_id}~1{object_id}~1data/patch",
        "/paths/~1api~1v1~1classes~1by-name~1{class_name}~1objects~1by-name~1{object_name}~1data/patch",
    ];

    for operation_pointer in operation_pointers {
        let operation = json
            .pointer(operation_pointer)
            .expect("object-data JSON Patch operation");
        assert_eq!(
            operation
                .pointer("/requestBody/content/application~1json-patch+json/schema/$ref")
                .and_then(Value::as_str),
            Some("#/components/schemas/ObjectDataPatchDocument")
        );
        for status in [
            "200", "400", "401", "403", "404", "406", "409", "413", "415", "500",
        ] {
            assert!(
                operation["responses"].get(status).is_some(),
                "missing documented JSON Patch response {status}"
            );
        }
    }
    let id_operation = json
        .pointer(operation_pointers[0])
        .expect("ID-addressed JSON Patch operation");
    assert!(
        id_operation["description"]
            .as_str()
            .is_some_and(|description| description.contains("relative to the root of `data`"))
    );
    let name_operation = json
        .pointer(operation_pointers[1])
        .expect("name-addressed JSON Patch operation");
    assert!(
        name_operation["description"]
            .as_str()
            .is_some_and(|description| description.contains("concurrent rename"))
    );

    let schema = &json["components"]["schemas"]["ObjectDataPatchDocument"];
    assert_eq!(schema["type"], "array");
    assert_eq!(schema["maxItems"], 1_000);
    assert!(schema["items"].is_object());
}

#[test]
fn principal_settings_patch_media_types_limits_and_statuses_are_documented() {
    let json = openapi_json();
    let operation_pointers = [
        "/paths/~1api~1v1~1iam~1me~1settings/patch",
        "/paths/~1api~1v1~1iam~1principals~1{principal_id}~1settings/patch",
    ];

    for operation_pointer in operation_pointers {
        let operation = json
            .pointer(operation_pointer)
            .expect("principal-settings patch operation");
        assert_eq!(
            operation
                .pointer("/requestBody/content/application~1json/schema/$ref")
                .and_then(Value::as_str),
            Some("#/components/schemas/PrincipalSettings")
        );
        assert_eq!(
            operation
                .pointer("/requestBody/content/application~1merge-patch+json/schema/$ref",)
                .and_then(Value::as_str),
            Some("#/components/schemas/PrincipalSettings")
        );
        assert_eq!(
            operation
                .pointer("/requestBody/content/application~1json-patch+json/schema/$ref")
                .and_then(Value::as_str),
            Some("#/components/schemas/PrincipalSettingsPatchDocument")
        );
        for status in ["200", "400", "401", "409", "412", "413", "415", "500"] {
            assert!(
                operation["responses"].get(status).is_some(),
                "missing documented principal-settings patch response {status}"
            );
        }
    }
    let principal_operation = json
        .pointer(operation_pointers[1])
        .expect("principal-addressed settings patch operation");
    assert!(principal_operation["responses"].get("404").is_some());

    let schema = &json["components"]["schemas"]["PrincipalSettingsPatchDocument"];
    assert_eq!(schema["type"], "array");
    assert_eq!(schema["maxItems"], 1_000);
    assert!(schema["items"].is_object());
    assert!(
        schema["description"]
            .as_str()
            .is_some_and(|description| description.contains("final root must remain an object"))
    );
}

#[test]
fn structured_search_schema_documents_protocol_targets_and_bounds() {
    let json = openapi_json();
    let operation = json
        .pointer("/paths/~1api~1v1~1search/post")
        .expect("structured search operation");
    assert_eq!(
        operation
            .pointer("/requestBody/content/application~1json/schema/$ref")
            .and_then(Value::as_str),
        Some("#/components/schemas/StructuredSearchRequest")
    );
    for status in ["200", "400", "401", "403", "404", "413", "415"] {
        assert!(
            operation["responses"].get(status).is_some(),
            "missing documented structured-search response {status}"
        );
    }

    let request = &json["components"]["schemas"]["StructuredSearchRequest"];
    assert_eq!(request["properties"]["version"]["minimum"], 1);
    assert_eq!(request["properties"]["version"]["maximum"], 1);
    assert_eq!(request["properties"]["limit"]["minimum"], 1);
    assert_eq!(request["properties"]["sort"]["maxItems"], 8);
    assert!(request["example"].is_object());

    let targets =
        serde_json::to_string(&json["components"]["schemas"]["StructuredSearchTarget"]).unwrap();
    for target in [
        "collection",
        "class",
        "object",
        "audit_event",
        "user",
        "group",
        "service_account",
    ] {
        assert!(targets.contains(&format!("\"{target}\"")));
    }

    let related = &json["components"]["schemas"]["StructuredRelatedPredicate"];
    assert_eq!(related["properties"]["depth"]["minimum"], 1);
    assert_eq!(related["properties"]["depth"]["maximum"], 10);
    assert_eq!(related["properties"]["filters"]["maxItems"], 16);

    let expressions = json["components"]["schemas"]["StructuredSearchExpression"]["oneOf"]
        .as_array()
        .expect("structured expression variants");
    assert_eq!(expressions.len(), 5);
    assert!(
        expressions
            .iter()
            .all(|expression| expression["maxProperties"] == 2),
        "expression schemas must reject unknown properties"
    );

    let stream = json
        .pointer("/paths/~1api~1v1~1search~1stream/post")
        .expect("structured search stream operation");
    assert_eq!(
        stream
            .pointer("/requestBody/content/application~1json/schema/$ref")
            .and_then(Value::as_str),
        Some("#/components/schemas/StructuredSearchRequest")
    );
    for status in ["200", "400", "401", "413", "415"] {
        assert!(
            stream["responses"].get(status).is_some(),
            "missing documented structured-search stream response {status}"
        );
    }
    assert!(
        stream
            .pointer("/responses/200/content/text~1event-stream")
            .is_some()
    );
    for schema in [
        "StructuredSearchStartedEvent",
        "StructuredSearchDoneEvent",
        "StructuredSearchErrorEvent",
    ] {
        assert!(
            json["components"]["schemas"].get(schema).is_some(),
            "missing structured-search stream schema {schema}"
        );
    }
    assert_eq!(
        json["components"]["schemas"]["StructuredSearchDoneEvent"]["properties"]["page_limit"]["type"],
        "integer"
    );
}

#[test]
fn openapi_contains_expected_operations_and_security_scheme() {
    let json = openapi_json();

    assert!(json.pointer("/paths/~1api~1v1~1iam~1users/get").is_some());
    assert!(json.pointer("/paths/~1api~1v1~1iam~1users/post").is_some());
    assert!(
        json.pointer("/paths/~1api~1v1~1auth~1login/post/responses/429")
            .is_some()
    );
    assert!(json.pointer("/paths/~1api~1v1~1exports/post").is_some());
    assert!(
        json.pointer("/paths/~1api~1v1~1exports~1{task_id}/get")
            .is_some()
    );
    assert!(
        json.pointer("/paths/~1api~1v1~1exports~1{task_id}~1output/get")
            .is_some()
    );
    assert!(
        json.pointer("/paths/~1api~1v1~1export-templates/get")
            .is_some()
    );
    assert!(
        json.pointer("/paths/~1api~1v1~1export-templates~1{template_id}~1exports/post")
            .is_some()
    );
    assert!(
        json.pointer("/paths/~1api~1v1~1relations~1objects/post")
            .is_some()
    );
    assert!(
        json.pointer("/components/securitySchemes/bearer_auth/type")
            .and_then(Value::as_str)
            == Some("http")
    );
    assert!(
        json.pointer("/components/securitySchemes/bearer_auth/scheme")
            .and_then(Value::as_str)
            == Some("bearer")
    );
    assert!(
        json.pointer("/components/securitySchemes/restore_capability/type")
            .and_then(Value::as_str)
            == Some("apiKey")
    );
    assert!(
        json.pointer("/components/securitySchemes/restore_capability/name")
            .and_then(Value::as_str)
            == Some("X-Hubuum-Restore-Capability")
    );
}

#[test]
fn openapi_documents_cursor_pagination_for_list_endpoints() {
    let json = openapi_json();
    let limits = page_limits_or_defaults();
    let default_page_limit = limits.default_limit();
    let max_page_limit = limits.maximum_limit();

    for path in CURSOR_PAGINATED_GET_PATHS {
        let pointer_path = path.replace('~', "~0").replace('/', "~1");
        let operation = json
            .pointer(&format!("/paths/{pointer_path}/get"))
            .unwrap_or_else(|| panic!("paginated GET {path} must be present"));
        let parameters = operation["parameters"]
            .as_array()
            .unwrap_or_else(|| panic!("paginated GET {path} must document query parameters"));
        let limit = parameters
            .iter()
            .find(|parameter| parameter["name"] == "limit")
            .unwrap_or_else(|| panic!("paginated GET {path} must document limit"));

        assert_eq!(
            limit["schema"]["minimum"], 1,
            "incorrect minimum for {path}"
        );
        assert_eq!(
            limit["schema"]["default"], default_page_limit,
            "incorrect default for {path}"
        );
        assert_eq!(
            limit["schema"]["maximum"], max_page_limit,
            "incorrect maximum for {path}"
        );

        for header in [NEXT_CURSOR_HEADER, TOTAL_COUNT_HEADER, PAGE_LIMIT_HEADER] {
            assert!(
                operation["responses"]["200"]["headers"][header]["description"]
                    .as_str()
                    .is_some(),
                "paginated GET {path} must document {header}"
            );
        }
    }

    let parameters = json
        .pointer("/paths/~1api~1v1~1classes/get/parameters")
        .and_then(Value::as_array)
        .expect("classes list parameters must be present");

    let parameter_names = parameters
        .iter()
        .filter_map(|parameter| parameter.get("name").and_then(Value::as_str))
        .collect::<HashSet<_>>();

    assert!(parameter_names.contains("limit"));
    assert!(parameter_names.contains("sort"));
    assert!(parameter_names.contains("cursor"));

    let limit = parameters
        .iter()
        .find(|parameter| parameter["name"] == "limit")
        .expect("limit parameter must be present");
    assert_eq!(limit["schema"]["minimum"], 1);
    assert_eq!(limit["schema"]["default"], default_page_limit);
    assert_eq!(limit["schema"]["maximum"], max_page_limit);

    let unified_search_limit = json
        .pointer("/paths/~1api~1v1~1search/get/parameters")
        .and_then(Value::as_array)
        .and_then(|parameters| {
            parameters
                .iter()
                .find(|parameter| parameter["name"] == "limit_per_kind")
        })
        .expect("unified search limit_per_kind parameter must be present");
    assert_eq!(unified_search_limit["schema"]["minimum"], 1);
    assert_eq!(
        unified_search_limit["schema"]["default"],
        default_page_limit
    );
    assert_eq!(unified_search_limit["schema"]["maximum"], max_page_limit);

    let header_description = json
        .pointer("/paths/~1api~1v1~1classes/get/responses/200/headers/X-Next-Cursor/description")
        .and_then(Value::as_str);
    let total_count_description = json
        .pointer("/paths/~1api~1v1~1classes/get/responses/200/headers/X-Total-Count/description")
        .and_then(Value::as_str);
    let page_limit_description = json
        .pointer("/paths/~1api~1v1~1classes/get/responses/200/headers/X-Page-Limit/description")
        .and_then(Value::as_str);

    assert!(
        header_description.is_some(),
        "X-Next-Cursor header must be documented for paginated list responses"
    );
    assert!(
        total_count_description.is_some(),
        "X-Total-Count header must be documented for paginated list responses"
    );
    assert!(
        page_limit_description.is_some(),
        "X-Page-Limit header must be documented for paginated list responses"
    );

    let unified_page_limit_description = json
        .pointer("/paths/~1api~1v1~1search/get/responses/200/headers/X-Page-Limit/description")
        .and_then(Value::as_str);
    assert!(
        unified_page_limit_description.is_some(),
        "X-Page-Limit header must be documented for unified search"
    );

    let event_delivery_page_limit_description = json
        .pointer(
            "/paths/~1api~1v1~1event-deliveries/get/responses/200/headers/X-Page-Limit/description",
        )
        .and_then(Value::as_str);
    assert!(
        event_delivery_page_limit_description.is_some(),
        "X-Page-Limit header must be documented for annotated cursor pagination"
    );
}

#[rstest]
#[case("/api/v1/classes/{class_id}/")]
#[case("/api/v1/classes/by-name/{class_name}/objects")]
fn related_filter_depth_documents_valid_range(#[case] path: &str) {
    let json = openapi_json();
    let pointer_path = path.replace('~', "~0").replace('/', "~1");
    let parameters = json
        .pointer(&format!("/paths/{pointer_path}/get/parameters"))
        .and_then(Value::as_array)
        .unwrap_or_else(|| panic!("object list GET {path} must document query parameters"));
    let depth = parameters
        .iter()
        .find(|parameter| parameter["name"] == "related.<alias>.depth__lte")
        .unwrap_or_else(|| panic!("object list GET {path} must document related depth"));

    assert_eq!(depth["schema"]["minimum"], 1);
    assert_eq!(
        depth["schema"]["maximum"],
        crate::models::search::MAX_RELATED_FILTER_DEPTH
    );
}

#[test]
fn openapi_operations_have_metadata_unique_operation_ids_and_security() {
    let json = openapi_json();
    let paths = json
        .get("paths")
        .and_then(Value::as_object)
        .expect("OpenAPI paths must be an object");

    let operation_keys = [
        "get", "post", "put", "patch", "delete", "options", "head", "trace",
    ];
    let mut operation_ids = HashSet::new();

    for (path, path_item) in paths {
        let path_item = path_item.as_object().expect("Path item must be an object");

        for method in operation_keys {
            let Some(operation) = path_item.get(method) else {
                continue;
            };
            let operation = operation.as_object().expect("Operation must be an object");

            let operation_id = operation
                .get("operationId")
                .and_then(Value::as_str)
                .expect("operationId must be present");
            assert!(
                !operation_id.trim().is_empty(),
                "operationId is empty for {method} {path}"
            );
            assert!(
                operation_ids.insert(operation_id.to_string()),
                "Duplicate operationId found: {operation_id}"
            );

            let summary = operation
                .get("summary")
                .and_then(Value::as_str)
                .expect("summary must be present");
            assert!(
                !summary.trim().is_empty(),
                "summary is empty for {method} {path}"
            );

            let description = operation
                .get("description")
                .and_then(Value::as_str)
                .expect("description must be present");
            assert!(
                !description.trim().is_empty(),
                "description is empty for {method} {path}"
            );

            let is_public_probe =
                matches!(path.as_str(), "/healthz" | "/readyz") && method == "get";
            let is_login = matches!(path.as_str(), "/api/v1/auth/login" | "/api/v1/auth/refresh")
                && method == "post";
            let is_provider_discovery = path == "/api/v1/auth/providers" && method == "get";
            let is_client_config = path == "/api/v1/config" && method == "get";
            let is_restore_status =
                path == "/api/v1/restores/{restore_id}/status" && method == "get";
            if is_restore_status {
                let security = operation
                    .get("security")
                    .and_then(Value::as_array)
                    .expect("security must be present for restore status endpoint");
                let has_capability = security.iter().any(|entry| {
                    entry
                        .as_object()
                        .map(|obj| obj.contains_key("restore_capability"))
                        .unwrap_or(false)
                });
                assert!(
                    has_capability,
                    "missing restore_capability security for {method} {path}"
                );
            } else if !is_login && !is_provider_discovery && !is_client_config && !is_public_probe {
                let security = operation
                    .get("security")
                    .and_then(Value::as_array)
                    .expect("security must be present for authenticated endpoint");

                let has_bearer = security.iter().any(|entry| {
                    entry
                        .as_object()
                        .map(|obj| obj.contains_key("bearer_auth"))
                        .unwrap_or(false)
                });
                assert!(
                    has_bearer,
                    "missing bearer_auth security for {method} {path}"
                );
            }
        }
    }
}

#[actix_web::test]
async fn openapi_operations_resolve_to_mounted_routes() {
    let json = openapi_json();
    let paths = json
        .get("paths")
        .and_then(Value::as_object)
        .expect("OpenAPI paths must be an object");
    let operation_keys = [
        "get", "post", "put", "patch", "delete", "options", "head", "trace",
    ];

    let app = actix_web::test::init_service(App::new().configure(crate::api::config)).await;

    for (path, path_item) in paths {
        let path_item = path_item.as_object().expect("Path item must be an object");
        let route_uri = path_with_sample_params(path);

        for method in operation_keys {
            if path_item.get(method).is_none() {
                continue;
            }

            let http_method = match method {
                "get" => Method::GET,
                "post" => Method::POST,
                "put" => Method::PUT,
                "patch" => Method::PATCH,
                "delete" => Method::DELETE,
                "options" => Method::OPTIONS,
                "head" => Method::HEAD,
                "trace" => Method::TRACE,
                _ => unreachable!("operation key list only contains known HTTP methods"),
            };
            let req = actix_web::test::TestRequest::default()
                .method(http_method)
                .uri(&route_uri)
                .to_request();
            let res = actix_web::test::call_service(&app, req).await;

            assert_ne!(
                res.status(),
                StatusCode::NOT_FOUND,
                "Documented OpenAPI operation is not mounted: {method} {path} (sample: {route_uri})"
            );
        }
    }
}

#[rstest]
#[case("/api/v1/classes/{class_id}/object-aggregates", "get", "413")]
#[case("/api/v1/classes/by-name/{class_name}/object-aggregates", "get", "413")]
#[case("/api/v1/imports", "post", "429")]
#[case("/api/v1/exports", "post", "429")]
fn admission_preserves_existing_error_response_schema(
    #[case] path: &str,
    #[case] method: &str,
    #[case] status: &str,
) {
    let json = openapi_json();
    let response = &json["paths"][path][method]["responses"][status];
    assert_eq!(
        response["content"]["application/json"]["schema"]["$ref"],
        "#/components/schemas/ApiErrorResponse"
    );
    let envelope = &json["components"]["schemas"]["ApiErrorResponse"];
    assert_eq!(envelope["type"], "object");
    assert_eq!(
        envelope["required"],
        serde_json::json!(["error", "message"])
    );
    assert!(envelope["properties"]["retryable"].is_object());
    assert!(
        response["description"]
            .as_str()
            .unwrap()
            .contains("no-store")
    );
}

#[test]
fn admission_adds_bounded_schema_to_new_responses() {
    let json = openapi_json();
    let schema = &json["paths"]["/api/v1/imports"]["post"]["responses"]["422"]["content"]["application/json"]
        ["schema"];
    assert_eq!(
        schema["anyOf"][1]["$ref"],
        "#/components/schemas/ApiRequestAdmissionErrorResponse"
    );
    let admission = &json["components"]["schemas"]["ApiRequestAdmissionErrorResponse"];
    assert_eq!(
        admission["properties"]["reason"]["$ref"],
        "#/components/schemas/ApiRequestRejection"
    );
    assert!(
        admission["required"]
            .as_array()
            .unwrap()
            .contains(&serde_json::json!("retryable"))
    );
}
