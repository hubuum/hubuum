// Hermetic authorization-conformance policies.
// Keep identifiers synchronized with test-fixture.md, conformance.rs, and
// live_treetop_parity.rs.

// Global administrator override.
permit (principal in Group::"9100", action, resource);

// Collection-scoped API, audit, event, and remote-target behavior.
permit (
    principal in Group::"9101",
    action in [
        Action::"ReadCollection",
        Action::"UpdateCollection",
        Action::"DeleteCollection",
        Action::"DelegateCollection",
        Action::"ReadRemoteTarget",
        Action::"CreateRemoteTarget",
        Action::"UpdateRemoteTarget",
        Action::"DeleteRemoteTarget",
        Action::"ExecuteRemoteTarget",
        Action::"ReadAudit",
        Action::"ManageEventSubscription"
    ],
    resource
) when {
    resource is HubuumCollection && resource == HubuumCollection::"9201"
};

// Class lifecycle and list/search visibility.
permit (
    principal in Group::"9101",
    action in [
        Action::"CreateClass",
        Action::"ReadClass",
        Action::"UpdateClass",
        Action::"DeleteClass"
    ],
    resource
) when {
    resource is HubuumClass && resource.collection_id == 9201
};

// Object lifecycle, imports, and list/search visibility.
permit (
    principal in Group::"9101",
    action in [
        Action::"CreateObject",
        Action::"ReadObject",
        Action::"UpdateObject",
        Action::"DeleteObject"
    ],
    resource
) when {
    resource is HubuumObject && resource.collection_id == 9201
};

// Export-template lifecycle and export reads.
permit (
    principal in Group::"9101",
    action in [
        Action::"ReadTemplate",
        Action::"CreateTemplate",
        Action::"UpdateTemplate",
        Action::"DeleteTemplate"
    ],
    resource
) when {
    resource is HubuumTemplate && resource.collection_id == 9201
};

// This intentionally matches the exported Treetop policy contract: either
// relation endpoint is sufficient. The local backend deliberately requires
// grants on both endpoints, and the shared corpus records that difference.
permit (
    principal in Group::"9101",
    action in [
        Action::"CreateClassRelation",
        Action::"ReadClassRelation",
        Action::"UpdateClassRelation",
        Action::"DeleteClassRelation"
    ],
    resource
) when {
    resource is HubuumClassRelation &&
    (resource.from_collection_id == 9201 || resource.to_collection_id == 9201)
};

permit (
    principal in Group::"9101",
    action in [
        Action::"CreateObjectRelation",
        Action::"ReadObjectRelation",
        Action::"UpdateObjectRelation",
        Action::"DeleteObjectRelation"
    ],
    resource
) when {
    resource is HubuumObjectRelation &&
    (resource.from_collection_id == 9201 || resource.to_collection_id == 9201)
};

// Local task authorization is owner-or-admin. The fixture expresses the same
// owner rule explicitly so queued-worker rechecks can share the corpus.
permit (
    principal == User::"9001",
    action == Action::"ReadTask",
    resource
) when {
    resource is HubuumTask &&
    resource has submitted_by &&
    resource.submitted_by == 9001
};
