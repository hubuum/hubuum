//! Backend-independent authorization conformance cases.
//!
//! The local PostgreSQL and live Treetop suites build the same semantic corpus
//! with backend-specific fixture identifiers. Intentional differences are
//! explicit data in the corpus so a backend cannot silently omit a case.

use crate::db::traits::authz::{scope_allows, scope_allows_resource};
use crate::models::{CollectionID, HubuumClassID, Permissions, TokenResourceScope, TokenScope};
use crate::permissions::{
    PermissionBackend, PermissionDecision, PermissionRequest, PrincipalRef, ResourceAttrs,
    ResourceKind, ResourceRef,
};

const TREETOP_FIXTURE_SCHEMA: &str = include_str!("../../../docs/treetop/schema.json");

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConformanceBackend {
    Local,
    Treetop,
}

#[derive(Clone, Debug)]
pub struct ConformanceFixture {
    pub normal: PrincipalRef,
    pub administrator: PrincipalRef,
    pub unprivileged: PrincipalRef,
    pub granted_collection_id: i32,
    pub denied_collection_id: i32,
    pub class_id: i32,
    pub object_id: i32,
    pub task_id: i32,
}

#[derive(Clone, Copy, Debug)]
enum ExpectedDecision {
    Shared(PermissionDecision),
    IntentionalDifference {
        local: PermissionDecision,
        treetop: PermissionDecision,
        reason: &'static str,
    },
}

impl ExpectedDecision {
    fn for_backend(self, backend: ConformanceBackend) -> PermissionDecision {
        match (self, backend) {
            (Self::Shared(decision), _) => decision,
            (Self::IntentionalDifference { local, .. }, ConformanceBackend::Local) => local,
            (Self::IntentionalDifference { treetop, .. }, ConformanceBackend::Treetop) => treetop,
        }
    }

    fn reason(self) -> Option<&'static str> {
        match self {
            Self::Shared(_) => None,
            Self::IntentionalDifference { reason, .. } => Some(reason),
        }
    }
}

struct ConformanceCase {
    name: &'static str,
    principal: PrincipalRef,
    request: PermissionRequest,
    scope: Option<TokenScope>,
    expected: ExpectedDecision,
}

fn resource(kind: ResourceKind, id: i32, collection_id: i32, class_id: Option<i32>) -> ResourceRef {
    ResourceRef {
        kind,
        id,
        attrs: ResourceAttrs {
            collection_id: Some(collection_id),
            class_id,
            ..Default::default()
        },
    }
}

fn relation_resource(fixture: &ConformanceFixture, kind: ResourceKind) -> ResourceRef {
    let mut attrs = ResourceAttrs {
        from_collection_id: Some(fixture.granted_collection_id),
        to_collection_id: Some(fixture.denied_collection_id),
        from_class_id: Some(fixture.class_id),
        to_class_id: Some(fixture.class_id + 1),
        ..Default::default()
    };
    if kind == ResourceKind::ObjectRelation {
        attrs.from_object_id = Some(fixture.object_id);
        attrs.to_object_id = Some(fixture.object_id + 1);
        attrs.class_relation_id = Some(fixture.class_id + 100);
    }
    ResourceRef {
        kind,
        id: fixture.object_id + 100,
        attrs,
    }
}

fn request(
    resource: ResourceRef,
    permissions: impl IntoIterator<Item = Permissions>,
) -> PermissionRequest {
    PermissionRequest {
        resource,
        permissions: permissions.into_iter().collect(),
    }
}

fn permission_scope(permissions: Vec<Permissions>) -> TokenScope {
    TokenScope::from_stored_parts(Some(permissions), None)
        .expect("the conformance permission scope must be valid")
}

fn collection_scope(collection_id: i32) -> TokenScope {
    TokenScope::from_stored_parts(
        None,
        Some(vec![TokenResourceScope::Collection(
            CollectionID::new(collection_id).expect("the collection fixture id must be valid"),
        )]),
    )
    .expect("the conformance collection scope must be valid")
}

fn conformance_cases(fixture: &ConformanceFixture) -> Vec<ConformanceCase> {
    let granted_collection = ResourceRef::collection(fixture.granted_collection_id);
    let granted_class = resource(
        ResourceKind::Class,
        fixture.class_id,
        fixture.granted_collection_id,
        None,
    );
    let granted_object = resource(
        ResourceKind::Object,
        fixture.object_id,
        fixture.granted_collection_id,
        Some(fixture.class_id),
    );
    let granted_template = resource(
        ResourceKind::Template,
        fixture.object_id + 10,
        fixture.granted_collection_id,
        None,
    );
    let relation_difference = ExpectedDecision::IntentionalDifference {
        local: PermissionDecision::Deny,
        treetop: PermissionDecision::Allow,
        reason: "local authorization requires grants on both relation endpoints; the exported Treetop policy intentionally uses either endpoint",
    };

    vec![
        ConformanceCase {
            name: "ordinary group collection grant",
            principal: fixture.normal.clone(),
            request: request(granted_collection.clone(), [Permissions::ReadCollection]),
            scope: None,
            expected: ExpectedDecision::Shared(PermissionDecision::Allow),
        },
        ConformanceCase {
            name: "conjunctive permission grant",
            principal: fixture.normal.clone(),
            request: request(
                granted_collection.clone(),
                [Permissions::ReadCollection, Permissions::UpdateCollection],
            ),
            scope: None,
            expected: ExpectedDecision::Shared(PermissionDecision::Allow),
        },
        ConformanceCase {
            name: "ungranted collection is denied",
            principal: fixture.normal.clone(),
            request: request(
                ResourceRef::collection(fixture.denied_collection_id),
                [Permissions::ReadCollection],
            ),
            scope: None,
            expected: ExpectedDecision::Shared(PermissionDecision::Deny),
        },
        ConformanceCase {
            name: "administrator override",
            principal: fixture.administrator.clone(),
            request: request(
                ResourceRef::collection(fixture.denied_collection_id),
                [Permissions::DeleteCollection],
            ),
            scope: None,
            expected: ExpectedDecision::Shared(PermissionDecision::Allow),
        },
        ConformanceCase {
            name: "unknown principal fails closed",
            principal: fixture.unprivileged.clone(),
            request: request(granted_collection.clone(), [Permissions::ReadCollection]),
            scope: None,
            expected: ExpectedDecision::Shared(PermissionDecision::Deny),
        },
        ConformanceCase {
            name: "class list and search visibility",
            principal: fixture.normal.clone(),
            request: request(granted_class.clone(), [Permissions::ReadClass]),
            scope: None,
            expected: ExpectedDecision::Shared(PermissionDecision::Allow),
        },
        ConformanceCase {
            name: "object list and search visibility",
            principal: fixture.normal.clone(),
            request: request(granted_object.clone(), [Permissions::ReadObject]),
            scope: None,
            expected: ExpectedDecision::Shared(PermissionDecision::Allow),
        },
        ConformanceCase {
            name: "import object creation recheck",
            principal: fixture.normal.clone(),
            request: request(granted_object.clone(), [Permissions::CreateObject]),
            scope: None,
            expected: ExpectedDecision::Shared(PermissionDecision::Allow),
        },
        ConformanceCase {
            name: "export template visibility",
            principal: fixture.normal.clone(),
            request: request(granted_template, [Permissions::ReadTemplate]),
            scope: None,
            expected: ExpectedDecision::Shared(PermissionDecision::Allow),
        },
        ConformanceCase {
            name: "remote call execution recheck",
            principal: fixture.normal.clone(),
            request: request(
                granted_collection.clone(),
                [Permissions::ExecuteRemoteTarget],
            ),
            scope: None,
            expected: ExpectedDecision::Shared(PermissionDecision::Allow),
        },
        ConformanceCase {
            name: "audit and temporal history visibility",
            principal: fixture.normal.clone(),
            request: request(granted_collection.clone(), [Permissions::ReadAudit]),
            scope: None,
            expected: ExpectedDecision::Shared(PermissionDecision::Allow),
        },
        ConformanceCase {
            name: "event subscription management",
            principal: fixture.normal.clone(),
            request: request(
                granted_collection.clone(),
                [Permissions::ManageEventSubscription],
            ),
            scope: None,
            expected: ExpectedDecision::Shared(PermissionDecision::Allow),
        },
        ConformanceCase {
            name: "class relation spanning two collections",
            principal: fixture.normal.clone(),
            request: request(
                relation_resource(fixture, ResourceKind::ClassRelation),
                [Permissions::ReadClassRelation],
            ),
            scope: None,
            expected: relation_difference,
        },
        ConformanceCase {
            name: "object relation spanning two collections",
            principal: fixture.normal.clone(),
            request: request(
                relation_resource(fixture, ResourceKind::ObjectRelation),
                [Permissions::ReadObjectRelation],
            ),
            scope: None,
            expected: relation_difference,
        },
        ConformanceCase {
            name: "permission-scoped token allows admitted permission",
            principal: fixture.normal.clone(),
            request: request(granted_collection.clone(), [Permissions::ReadCollection]),
            scope: Some(permission_scope(vec![Permissions::ReadCollection])),
            expected: ExpectedDecision::Shared(PermissionDecision::Allow),
        },
        ConformanceCase {
            name: "permission-scoped token denies omitted permission",
            principal: fixture.normal.clone(),
            request: request(granted_collection.clone(), [Permissions::UpdateCollection]),
            scope: Some(permission_scope(vec![Permissions::ReadCollection])),
            expected: ExpectedDecision::Shared(PermissionDecision::Deny),
        },
        ConformanceCase {
            name: "resource-scoped token allows admitted collection",
            principal: fixture.normal.clone(),
            request: request(granted_object.clone(), [Permissions::ReadObject]),
            scope: Some(collection_scope(fixture.granted_collection_id)),
            expected: ExpectedDecision::Shared(PermissionDecision::Allow),
        },
        ConformanceCase {
            name: "resource-scoped token denies omitted collection",
            principal: fixture.normal.clone(),
            request: request(granted_object, [Permissions::ReadObject]),
            scope: Some(collection_scope(fixture.denied_collection_id)),
            expected: ExpectedDecision::Shared(PermissionDecision::Deny),
        },
        ConformanceCase {
            name: "scoped administrator cannot exceed token permission boundary",
            principal: fixture.administrator.clone(),
            request: request(granted_collection, [Permissions::DeleteCollection]),
            scope: Some(permission_scope(vec![Permissions::ReadCollection])),
            expected: ExpectedDecision::Shared(PermissionDecision::Deny),
        },
    ]
}

async fn authorize_with_scope(
    backend: &dyn PermissionBackend,
    case: &ConformanceCase,
) -> Result<PermissionDecision, crate::errors::ApiError> {
    if !scope_allows(case.scope.as_ref(), &case.request.permissions)
        || !scope_allows_resource(case.scope.as_ref(), &case.request.resource)
    {
        return Ok(PermissionDecision::Deny);
    }
    backend
        .authorize(&case.principal, case.request.clone())
        .await
}

pub async fn assert_backend_conformance(
    backend: &dyn PermissionBackend,
    backend_kind: ConformanceBackend,
    fixture: &ConformanceFixture,
) {
    for case in conformance_cases(fixture) {
        let actual = authorize_with_scope(backend, &case)
            .await
            .unwrap_or_else(|error| panic!("conformance case '{}' failed: {error}", case.name));
        let expected = case.expected.for_backend(backend_kind);
        assert_eq!(
            actual,
            expected,
            "conformance case '{}' disagreed for {backend_kind:?}; intentional difference: {}",
            case.name,
            case.expected.reason().unwrap_or("none")
        );
    }

    let empty = backend
        .authorize_many(&fixture.normal, Vec::new())
        .await
        .expect("an empty candidate set must be accepted");
    assert!(empty.is_empty(), "an empty candidate set must stay empty");

    let repeated = vec![
        request(
            ResourceRef::collection(fixture.granted_collection_id),
            [Permissions::ReadCollection],
        ),
        request(
            ResourceRef::collection(fixture.denied_collection_id),
            [Permissions::ReadCollection],
        ),
        request(
            ResourceRef::collection(fixture.granted_collection_id),
            [Permissions::ReadCollection],
        ),
    ];
    let results = backend
        .authorize_candidates(&fixture.normal, repeated.clone())
        .await
        .expect("repeated candidate authorization must succeed");
    assert_eq!(results.len(), repeated.len());
    assert_eq!(
        results
            .iter()
            .map(|result| result.decision)
            .collect::<Vec<_>>(),
        [
            PermissionDecision::Allow,
            PermissionDecision::Deny,
            PermissionDecision::Allow,
        ],
        "repeated candidates must preserve source order and duplicate decisions"
    );

    let owned_task = ResourceRef {
        kind: ResourceKind::Task,
        id: fixture.task_id,
        attrs: ResourceAttrs {
            submitted_by: Some(fixture.normal.user_id),
            ..Default::default()
        },
    };
    assert_eq!(
        backend
            .authorize_task(&fixture.normal, &owned_task)
            .await
            .expect("task-owner authorization must succeed"),
        PermissionDecision::Allow,
        "task execution and result reads must preserve owner authorization"
    );
    assert_eq!(
        backend
            .authorize_task(&fixture.unprivileged, &owned_task)
            .await
            .expect("non-owner task authorization must return a decision"),
        PermissionDecision::Deny,
        "a non-owner without policy authority must not read a task"
    );

    let class_scope = TokenScope::from_stored_parts(
        None,
        Some(vec![TokenResourceScope::Class(
            HubuumClassID::new(fixture.class_id).expect("the class fixture id must be valid"),
        )]),
    )
    .expect("the class scope fixture must be valid");
    let object = resource(
        ResourceKind::Object,
        fixture.object_id,
        fixture.granted_collection_id,
        Some(fixture.class_id),
    );
    assert!(
        scope_allows_resource(Some(&class_scope), &object),
        "class token scopes must include objects in that class before backend dispatch"
    );
}

#[test]
fn treetop_fixture_schema_covers_every_runtime_permission() {
    use std::collections::BTreeSet;

    let schema: serde_json::Value =
        serde_json::from_str(TREETOP_FIXTURE_SCHEMA).expect("fixture schema must be valid JSON");
    let actions = schema[""]["actions"]
        .as_object()
        .expect("fixture schema must declare actions");
    let actual = actions.keys().map(String::as_str).collect::<BTreeSet<_>>();
    let mut expected = Permissions::all()
        .iter()
        .map(|permission| permission.to_string())
        .collect::<BTreeSet<_>>();
    expected.insert("ReadTask".to_string());
    let expected = expected.iter().map(String::as_str).collect::<BTreeSet<_>>();

    assert_eq!(
        actual, expected,
        "fixture actions drifted from Hubuum's model"
    );
}
