# Pluggable Permission Backends

**Status:** Design
**Date:** 2026-05-01
**Author:** Terje Kvernes (with Claude and Codex review)

## Goal

Make Hubuum's permission system pluggable through a Rust trait boundary so the
current Postgres-backed implementation and an external Treetop policy server
can be selected at runtime.

In local mode, Hubuum keeps today's SQL permission behavior. In Treetop mode,
Treetop is authoritative for authorization decisions, including:

- normal resource permissions,
- admin-only access,
- task/import visibility,
- report/search/list visibility filtering.

Both backends ship in the default binary. The selected backend is controlled by
runtime configuration.

## Non-goals

- Decision caching.
- Hubuum-managed runtime sync of SQL permissions into Treetop.
- Runtime upload of policies or schema to Treetop.
- Removing the SQL `permissions` table from the schema.
- Replacing Hubuum's own user/group identity tables.

## Background

The current permission system is built around namespace-scoped SQL grants:

- `PermissionController` in `src/traits/permissions.rs` exposes
  `user_can`, `grant`, `revoke`, `apply_permissions`, and `revoke_all`.
- `PermissionControllerBackend` in `src/db/traits/permissions.rs` contains
  the Diesel/Postgres implementation for point checks and mutations.
- Reverse permission queries live in
  `src/db/traits/namespace/permissions.rs`.
- Search/list visibility is embedded in SQL query builders under
  `src/db/traits/user/search.rs`,
  `src/db/traits/user/unified_search.rs`, and
  `src/db/traits/user/membership.rs`.
- Many handlers call the `can!` macro, which currently collapses every checked
  object to a `NamespaceID` before checking permissions.

That last point is the key implementation constraint: a Treetop backend that
wants typed resources cannot be built by only swapping the existing SQL query
function. The authorization path must preserve the actual target being checked:
namespace, class, object, relation, template, task, or system/admin.

`treetop-client` is the Rust async client for `treetop-rest`. Its authorization
surface is based on `Client::authorize` and `Client::is_allowed`, with request
types:

- `User` / `Group`,
- `Action`,
- `Resource { kind, id, attrs }`,
- batched `AuthorizeRequest`.

The client does not expose a parent field on `Resource`. Parent or containment
context must therefore be represented as resource attributes unless the
Treetop server separately supports parent/entity hierarchy outside this wire
shape.

## Decisions

| # | Decision |
|---|----------|
| 1 | Both backends compile by default. `HUBUUM_PERMISSION_BACKEND=local|treetop` selects the active backend at runtime. |
| 2 | Treetop mode is a full authorization provider, not only a point-check provider. |
| 3 | Treetop permission mutations are not supported through Hubuum. Grant/revoke endpoints return `501 Not Implemented`. |
| 4 | Hubuum remains the identity source. Treetop principals use stable numeric user/group IDs, not mutable names. |
| 5 | Treetop resources use typed `kind` plus stable numeric IDs and attributes for parent/context data. |
| 6 | Treetop list/search visibility is implemented by enumerating DB candidates and batch-authorizing them. Local SQL permission rows are not used as Treetop candidates. |
| 7 | Treetop failures fail closed. Startup health failures are fatal; mid-request outages return `503`. |
| 8 | A one-shot SQL-to-Cedar exporter helps operators bootstrap policies, but Hubuum does not push policies at runtime. |

## Architecture

### Permission Backend Trait

Add a new `src/permissions/` module with a backend trait that covers both point
checks and batch filtering:

```rust
pub trait PermissionBackend: Send + Sync {
    async fn authorize(
        &self,
        principal: PrincipalRef,
        request: PermissionRequest,
    ) -> Result<PermissionDecision, ApiError>;

    async fn authorize_many(
        &self,
        principal: PrincipalRef,
        requests: Vec<PermissionRequest>,
    ) -> Result<Vec<PermissionDecision>, ApiError>;

    async fn filter_authorized(
        &self,
        principal: PrincipalRef,
        requests: Vec<PermissionRequest>,
    ) -> Result<Vec<AuthorizedRequest>, ApiError>;

    async fn apply_permissions(
        &self,
        namespace_id: i32,
        group_id: i32,
        list: PermissionsList<Permissions>,
        replace_existing: bool,
    ) -> Result<Permission, ApiError>;

    async fn revoke_permissions(
        &self,
        namespace_id: i32,
        group_id: i32,
        list: PermissionsList<Permissions>,
    ) -> Result<Permission, ApiError>;

    async fn revoke_all(&self, namespace_id: i32, group_id: i32) -> Result<(), ApiError>;

    fn supports_mutation(&self) -> bool;
}
```

Use `async_trait` or boxed futures, whichever fits the surrounding code best.
The implementation must be object-safe because production state stores
`Arc<dyn PermissionBackend>`.

The shared request types must preserve typed target identity:

```rust
pub struct PrincipalRef {
    pub user_id: i32,
    pub group_ids: Vec<i32>,
}

pub struct PermissionRequest {
    pub resource: ResourceRef,
    pub permissions: Vec<Permissions>,
}

pub struct ResourceRef {
    pub kind: ResourceKind,
    pub id: i32,
    pub namespace_id: Option<i32>,
    pub attrs: ResourceAttrs,
}

pub enum ResourceKind {
    System,
    Namespace,
    Class,
    Object,
    ClassRelation,
    ObjectRelation,
    Template,
    Task,
}
```

`ResourceAttrs` should carry only data needed for policy conditions and trace
debugging. For v1 this includes:

- `namespace_id`,
- `class_id`,
- relation endpoint IDs,
- `submitted_by` for tasks/imports,
- resource name where cheaply available.

### Application Context

Introduce:

```rust
pub struct AppContext {
    pub db_pool: DbPool,
    pub permissions: Arc<dyn PermissionBackend>,
}
```

`BackendContext` should expose both the database pool and permission backend
for permission-aware code:

```rust
pub trait BackendContext {
    fn db_pool(&self) -> &DbPool;
    fn permission_backend(&self) -> &dyn PermissionBackend;
}
```

Do not keep a `DbPool` implementation with a `permission_backend()` panic stub.
That creates runtime failures in tests and helper paths. Instead:

- permission-aware code receives `AppContext`,
- pure DB code continues to receive `DbPool`,
- test helpers construct `AppContext` with `LocalPermissionBackend` by default.

Update Actix app setup to register `web::Data<AppContext>`. Extractors and
handlers that only need authentication may still read the DB pool from the
context.

### Refactor Authorization Call Sites

Refactor these surfaces to use the active backend:

- `UserPermissions::can`,
- `PermissionController::user_can_all`,
- the `can!` macro,
- `GroupMemberships::is_admin`,
- `AdminAccess`,
- `AdminOrSelfAccess`,
- task/import ownership and admin visibility checks,
- import planning permission checks,
- report template checks and report scope execution.

The `can!` macro must stop converting all targets to `NamespaceID`. It should
either:

- accept values implementing an `AuthzTarget` trait, or
- be replaced by explicit helper functions such as
  `authorize_namespace`, `authorize_class`, `authorize_object`, etc.

The implementation must preserve the real resource for Treetop while still
allowing the local backend to resolve the namespace-scoped SQL grant.

### Local Permission Backend

`LocalPermissionBackend` preserves current behavior:

- SQL permission checks still resolve to namespace grants.
- The SQL `permissions` table remains authoritative in local mode.
- Mutations use the current grant/revoke/update/delete behavior.
- Reverse permission endpoints keep their existing SQL semantics.

Local mode should be the default and the existing test suite should continue
to pass without requiring Treetop.

### Treetop Permission Backend

`TreetopPermissionBackend` owns:

```rust
pub struct TreetopPermissionBackend {
    client: treetop_client::Client,
    pool: DbPool,
}
```

It uses the DB for candidate enumeration and user/group lookup, but not for
authorization decisions.

Principal mapping:

- `User::new(user_id.to_string())`
- groups are `Group::new(group_id.to_string())`
- user/group names may be included in context or logs, but are not canonical
  policy identity.

Action mapping:

- Use the existing `Permissions` display names as the canonical v1 action IDs:
  `ReadCollection`, `CreateClass`, `ReadObject`, `DeleteTemplate`, etc.
- This avoids ambiguity from generic `read/update/delete` actions across
  different resource kinds.
- A later policy/schema version may introduce normalized Cedar actions, but the
  exporter and backend must then be versioned together.

Resource mapping:

| Hubuum target | Treetop resource |
|---|---|
| System/admin | `Resource::new("HubuumSystem", "global")` |
| Namespace | `Resource::new("HubuumNamespace", namespace_id)` |
| Class | `Resource::new("HubuumClass", class_id).with_attr("namespace_id", Long(...))` |
| Object | `Resource::new("HubuumObject", object_id).with_attr("namespace_id", Long(...)).with_attr("class_id", Long(...))` |
| Class relation | `Resource::new("HubuumClassRelation", id).with_attr("namespace_id", Long(...))` |
| Object relation | `Resource::new("HubuumObjectRelation", id).with_attr("namespace_id", Long(...))` |
| Template | `Resource::new("HubuumTemplate", template_id).with_attr("namespace_id", Long(...))` |
| Task/import | `Resource::new("HubuumTask", task_id).with_attr("submitted_by", Long(...))` |

Treetop mutations:

- `apply_permissions`, `revoke_permissions`, and `revoke_all` return
  `ApiError::NotImplemented`.
- Namespace creation in Treetop mode must not silently create local grants.
  Either skip the automatic SQL grant or use a separate create path without an
  assignee grant.
- Import `namespace_permissions` items must be rejected or recorded as
  unsupported in Treetop mode because task execution currently calls
  `apply_permissions_db` directly.

Correlation IDs:

- Extract `x-correlation-id` from the incoming request and carry it in request
  extensions or a lightweight request context.
- Treetop calls should use `client.with_correlation_id(correlation_id)`.

### List, Search, Report, and Reverse Queries

In local mode, current SQL permission-aware queries remain.

In Treetop mode, do not use the local `permissions` table to decide candidate
visibility. Candidate sets come from the domain tables:

- namespaces from `namespaces`,
- classes from `hubuumclass`,
- objects from `hubuumobject`,
- relations from relation tables,
- templates from `report_templates`,
- groups from `groups`,
- tasks/imports from task tables.

Filtering algorithm for cursor-paginated endpoints:

1. Build the DB candidate query from non-permission filters.
2. Apply deterministic sort and cursor constraints.
3. Fetch candidates in chunks.
4. Convert candidates to `PermissionRequest`s.
5. Batch-authorize with Treetop.
6. Keep authorized rows until `limit + 1` is reached or candidates are
   exhausted.
7. Compute exact total count by authorizing all matching candidates in chunks.

This is slower than SQL permission joins but preserves current API semantics:

- exact `X-Total-Count`,
- cursor pagination,
- consistent filter behavior,
- no leakage from stale local permission rows.

Reverse permission endpoints in Treetop mode:

- Group listing endpoints enumerate candidate groups from `groups`, then
  authorize each group/action/resource combination.
- `group_permission_on` can synthesize a `Permission` response from 24 Treetop
  decisions, but `id`, `created_at`, and `updated_at` are synthetic. Document
  that these fields have no persistence meaning in Treetop mode.
- Mutation endpoints return `501`.

## Configuration

Add:

- `HUBUUM_PERMISSION_BACKEND=local|treetop`, default `local`.
- `HUBUUM_TREETOP_URL`, required when backend is `treetop`.
- `HUBUUM_TREETOP_CONNECT_TIMEOUT_MS`, default `5000`.
- `HUBUUM_TREETOP_REQUEST_TIMEOUT_MS`, default `30000`.
- `HUBUUM_TREETOP_POOL_MAX_IDLE_PER_HOST`, optional.
- `HUBUUM_TREETOP_CA_CERT`, optional.
- `HUBUUM_TREETOP_ACCEPT_INVALID_CERTS=false`.

Do not require a Treetop upload token unless Hubuum gains an explicit policy
upload command. Runtime authorization does not need `UploadToken`.

Startup behavior:

- Build one shared Treetop client.
- Call `health()` before serving traffic.
- Prefer also checking `status()` so schema/context support problems surface
  during startup.
- Failure exits through `fatal_error()` with a dedicated permission-backend
  exit code.

## SQL to Cedar Export

Add an admin command:

```text
hubuum-admin export-permissions --as-cedar
```

The exporter reads the SQL `permissions` table and emits a bootstrap Cedar
policy file. It must preserve current SQL semantics exactly.

Rules:

- Use numeric `Group::<id>` identities, not group names.
- Include group names and namespace names only in comments.
- Emit resource-type-specific permits.
- Do not convert collection-level `ReadCollection` into a generic `read` grant
  that applies to child resource kinds.
- For namespace-scoped class/object/template/relation permissions, scope the
  permit by the resource kind plus `resource.namespace_id == <namespace_id>`.

Example shape:

```cedar
// permission row id=42, group_id=2 (mathematics-admins), namespace_id=7 (mathematics)
permit(
    principal in Group::"2",
    action == Action::"ReadClass",
    resource
)
when {
    resource has namespace_id &&
    resource.namespace_id == 7
};
```

The exported file is operator-managed. Hubuum does not upload it to Treetop at
runtime.

Ship docs and examples under `docs/treetop/`:

- a bootstrap schema/policy example matching this mapping,
- notes explaining backend selection,
- notes explaining `501` mutation behavior in Treetop mode,
- a short migration workflow from SQL grants to Treetop policies.

## Errors and Observability

Add `ApiError` variants:

- `NotImplemented(String)` -> HTTP `501`.
- `PermissionBackendUnavailable(String)` -> HTTP `503`.

Convert `treetop_client::TreetopError` explicitly:

- transport errors -> `PermissionBackendUnavailable`,
- 5xx API errors -> `PermissionBackendUnavailable`,
- malformed responses -> `PermissionBackendUnavailable`,
- request/schema/policy validation errors -> `InternalServerError` unless a
  clearer client error is known.

Observability:

- Add a startup log with selected backend.
- Wrap backend calls in tracing spans with backend, user ID, resource kind,
  resource ID, namespace ID, permissions, decision, and duration.
- Include correlation ID on Treetop calls when present.

## Tests

Local mode:

- Existing tests pass unchanged with `HUBUUM_PERMISSION_BACKEND=local`.

Mock Treetop tests:

- point allow/deny,
- multi-permission AND semantics,
- admin allow/deny,
- task/import visibility,
- list filtering,
- exact counts,
- cursor page filling across denied candidates,
- 501 mutation behavior,
- 503 fail-closed behavior,
- mapping for every `Permissions` variant and `ResourceKind`.

Exporter tests:

- each SQL permission column maps to the correct Treetop action/resource-kind
  rule,
- collection permissions do not overgrant child resources,
- generated policies use numeric group IDs,
- names appear only in comments.

Integration strategy:

- Do not assume the whole current integration suite can run twice against
  static Cedar fixtures. Existing tests create random fixture IDs and grants.
- Either generate and upload policies per test, or maintain a smaller
  live-Treetop parity suite with deterministic fixture state.
- CI should run the local suite by default and the Treetop suite when a
  Treetop test service is available.

## Risks

- Treetop list/search filtering is more expensive than SQL permission joins.
  The v1 design prioritizes correctness and exact API semantics over speed.
- Synthetic permission rows in Treetop mode may surprise API consumers. This is
  documented and limited to read-only compatibility endpoints.
- Treetop policies can express more than SQL grants. The exporter preserves SQL
  semantics, but hand-written policies may intentionally diverge.
- Resource mapping is a contract. If the Treetop schema changes, Hubuum's
  mapping and exporter must be versioned together.

## Out of Scope

- Decision caching.
- Runtime policy upload/sync from Hubuum to Treetop.
- Removing SQL permission schema.
- Replacing Hubuum user/group identity.
