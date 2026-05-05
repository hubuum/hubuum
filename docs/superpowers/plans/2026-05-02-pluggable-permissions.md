# Pluggable Permission Backends Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make Hubuum's permission system pluggable through a Rust trait
boundary so today's Postgres-backed implementation and an external Treetop
(Cedar) policy server can be selected at runtime with full feature parity.

**Architecture:** A new `PermissionBackend` trait sits behind everything.
`LocalPermissionBackend` wraps the existing SQL behavior; `TreetopPermissionBackend`
delegates to the treetop-client. Both compile by default; runtime selection via
`HUBUUM_PERMISSION_BACKEND={local|treetop}`. Authorization call sites stop
collapsing targets to `NamespaceID` and instead pass typed `ResourceRef`
values so the Treetop backend can construct typed Cedar resources. Reverse
queries (`namespaces_user_can`, `groups_with_permissions_on`) on the Treetop
backend enumerate candidates from the local DB and filter via Treetop's batch
`authorize`. Mutations on the Treetop backend return `501 Not Implemented` —
permissions are managed out-of-band.

**Tech Stack:** Rust 2024 edition, actix-web 4, Diesel 2 + Postgres,
treetop-client (git pin until published), async-trait, rstest.

**Reference spec:** `docs/superpowers/specs/2026-05-01-pluggable-permissions-design.md`.

---

## Plan Amendments (2026-05-05)

After Phase 1 review, the trait/type surface tightened. **Use the amended
shapes wherever earlier versions of the plan show different signatures.**
The trait section in §1 below is the authoritative copy.

- **`PermissionBackend::authorize_many` is the only required decision
  method.** `authorize` and `authorize_candidates` have default impls that
  wrap `authorize_many`. Backends that can batch transport-side (Treetop)
  override `authorize_many`; backends that can't (Local) get less
  boilerplate.
- **`authorize_candidates` is *not* a filter** — it returns
  `Vec<AuthorizationResult>` covering both `Allow` and `Deny`. Callers
  filter on `result.decision == PermissionDecision::Allow` themselves. The
  paired return shape lets call sites correlate decisions back to their
  original requests without re-zipping. (Renamed from `filter_authorized`
  / `AuthorizedRequest` after Phase 1 — those names misleadingly implied
  pre-filtering.)
- **`namespaces_user_can` takes `&[Permissions]`** (a slice, conjunctive
  AND), not a single `Permissions`. Empty slice means "any row qualifies."
- **`PrincipalRef::new(user_id, group_ids)`** is the canonical constructor
  — it sorts and deduplicates the group list so Treetop request payloads
  are deterministic. Prefer this over the struct-literal form at call sites.
- **`ResourceAttrs` carries the relation context fields**: `from_class_id`,
  `to_class_id`, `from_object_id`, `to_object_id`, `class_relation_id`
  (in addition to the originally listed fields). Relation `AuthzTarget`
  impls in Task 3.1 should populate these so policy authors can scope by
  endpoint as well as by namespace.
- **The `BackendContext for DbPool` panic shim** added in Task 1.3 is
  short-lived. Task 3.8 removes it. If you find it at the start of a Phase
  3 task, your first instinct should be: "is this call site ready to
  migrate to `AppContext`?" — not "extend the shim."

---

## Scope Note

This plan is one cohesive subsystem (permissions) with several integration
points. It is large but not multiple independent subsystems, so it stays as
one plan. Phases are ordered so that after each phase the build is green and
existing behavior is preserved.

## File Structure

**New files:**

- `src/permissions/mod.rs` — module entry, `build_permission_backend`, `PermissionBackendKind`
- `src/permissions/backend.rs` — `PermissionBackend` trait
- `src/permissions/types.rs` — `PrincipalRef`, `PermissionRequest`, `PermissionDecision`, `AuthorizationResult`, `ResourceRef`, `ResourceKind`, `ResourceAttrs`, `AuthzTarget`
- `src/permissions/local/mod.rs` — `LocalPermissionBackend` struct and impl
- `src/permissions/local/queries.rs` — local SQL helpers extracted from existing `db/traits/permissions.rs` and `db/traits/namespace/permissions.rs`
- `src/permissions/treetop/mod.rs` — `TreetopPermissionBackend`
- `src/permissions/treetop/mapping.rs` — `Permissions` ↔ `(treetop::Action, treetop::Resource)` table
- `src/permissions/treetop/error.rs` — `treetop_client::TreetopError` → `ApiError` conversion
- `src/permissions/test_support/mod.rs` — `MockTreetopBackend` (in-memory, deterministic)
- `src/permissions/context.rs` — `AppContext` struct
- `src/utilities/correlation.rs` — extract `x-correlation-id` from request
- `src/bin/admin/mod.rs` (split from `src/bin/admin.rs`) — admin CLI subcommands
- `src/bin/admin/export.rs` — `export-permissions --as-cedar`
- `src/tests/permissions/mod.rs` — wires permission test modules
- `src/tests/permissions/backend_trait.rs` — trait-level parity (Local vs Mock Treetop)
- `src/tests/permissions/translator.rs` — exporter round-trip
- `src/tests/permissions/parity_fixture.rs` — alice/bob/chris scenario
- `docs/treetop/README.md` — backend-selection overview
- `docs/treetop/schema.cedarschema` — entity schema
- `docs/treetop/bootstrap.cedar` — empty policy + admin notes

**Modified files:**

- `Cargo.toml` — features and deps
- `src/lib.rs` — register `permissions` module
- `src/main.rs` — wire `AppContext`
- `src/config.rs` — `HUBUUM_PERMISSION_BACKEND`, Treetop knobs
- `src/errors.rs` — `NotImplemented`, `PermissionBackendUnavailable`, exit code
- `src/traits/context.rs` — extend `BackendContext`
- `src/traits/permissions.rs` — `PermissionController` defaults delegate to backend
- `src/db/traits/user/permissions.rs` — `UserPermissions::can` delegates to backend
- `src/db/traits/permissions.rs` — port body of `PermissionControllerBackend` into `LocalPermissionBackend`; trait removed once call sites move
- `src/db/traits/namespace/permissions.rs` — reverse-query free functions move into `LocalPermissionBackend`
- `src/extractors/mod.rs` — read `AppContext` for admin checks
- `src/macros.rs` — `can!` and `check_permissions!` stop collapsing to `NamespaceID`
- `src/api/v1/handlers/{namespaces,classes,relations,templates,reports,tasks,imports,users,groups,search}.rs` — pass typed `AuthzTarget`
- `src/tasks/{planning,execution}.rs` — task/import permission checks
- `src/db/traits/task_import.rs` — visibility filter goes through backend
- `src/db/traits/user/{search,unified_search,membership}.rs` — visibility filter goes through backend
- `src/api/openapi.rs` — register new error codes / responses where needed
- `docs/permissions.md` — backend selection section
- `run_tests.sh` — backend axis (Phase 8)

---

## Phase 0 — Errors and dependencies

### Task 0.1: Add new ApiError variants and exit code

**Files:**
- Modify: `src/errors.rs`
- Test: `src/errors.rs` (existing `mod tests`)

- [ ] **Step 1: Write the failing test**

Add to `src/errors.rs` `mod tests`:

```rust
    #[test]
    fn not_implemented_maps_to_501() {
        let err = ApiError::NotImplemented("permission mutations are managed out-of-band".to_string());
        assert_eq!(err.status_code(), StatusCode::NOT_IMPLEMENTED);
    }

    #[test]
    fn permission_backend_unavailable_maps_to_503() {
        let err = ApiError::PermissionBackendUnavailable("treetop unreachable".to_string());
        assert_eq!(err.status_code(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[test]
    fn permission_backend_error_exit_code_distinct() {
        assert_ne!(EXIT_CODE_PERMISSION_BACKEND_ERROR, EXIT_CODE_GENERIC_ERROR);
        assert_ne!(EXIT_CODE_PERMISSION_BACKEND_ERROR, EXIT_CODE_DATABASE_ERROR);
        assert_ne!(EXIT_CODE_PERMISSION_BACKEND_ERROR, EXIT_CODE_INIT_ERROR);
        assert_ne!(EXIT_CODE_PERMISSION_BACKEND_ERROR, EXIT_CODE_TLS_ERROR);
        assert_ne!(EXIT_CODE_PERMISSION_BACKEND_ERROR, EXIT_CODE_CONFIG_ERROR);
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --lib errors::tests::not_implemented_maps_to_501 errors::tests::permission_backend_unavailable_maps_to_503 errors::tests::permission_backend_error_exit_code_distinct`
Expected: compile errors — `NotImplemented`, `PermissionBackendUnavailable`, and `EXIT_CODE_PERMISSION_BACKEND_ERROR` do not exist.

- [ ] **Step 3: Add the constant and variants**

In `src/errors.rs`, add the constant near the existing `EXIT_CODE_*` block:

```rust
pub const EXIT_CODE_PERMISSION_BACKEND_ERROR: i32 = 6; // Permission backend unreachable / failed startup health check
```

Add to the `ApiError` enum:

```rust
    NotImplemented(String),
    PermissionBackendUnavailable(String),
```

Extend `Display` for the two new variants:

```rust
            ApiError::NotImplemented(message) => write!(f, "{message}"),
            ApiError::PermissionBackendUnavailable(message) => write!(f, "{message}"),
```

Extend `error_response`:

```rust
            ApiError::NotImplemented(message) => HttpResponse::NotImplemented()
                .json(json!({ "error": "Not Implemented", "message": message })),
            ApiError::PermissionBackendUnavailable(message) => {
                HttpResponse::ServiceUnavailable()
                    .insert_header(("Retry-After", "5"))
                    .json(json!({ "error": "Service Unavailable", "message": message }))
            }
```

Extend `status_code`:

```rust
            ApiError::NotImplemented(_) => StatusCode::NOT_IMPLEMENTED,
            ApiError::PermissionBackendUnavailable(_) => StatusCode::SERVICE_UNAVAILABLE,
```

Extend `exit_code`:

```rust
            ApiError::PermissionBackendUnavailable(_) => EXIT_CODE_PERMISSION_BACKEND_ERROR,
```

- [ ] **Step 4: Run tests**

Run: `cargo test --lib errors::tests`
Expected: all green.

- [ ] **Step 5: Commit**

```bash
git add src/errors.rs
git commit -m "feat(errors): add NotImplemented and PermissionBackendUnavailable variants"
```

---

### Task 0.2: Add async-trait dependency and feature scaffolding

**Files:**
- Modify: `Cargo.toml`

- [ ] **Step 1: Edit `Cargo.toml`**

Replace the `[features]` block:

```toml
[features]
default = ["swagger-ui", "permissions-local", "permissions-treetop"]
swagger-ui = ["dep:utoipa-swagger-ui"]
tls-rustls = ["dep:rustls", "actix-web/rustls-0_23"]
tls-openssl = ["dep:openssl", "actix-web/openssl"]
permissions-local = []
permissions-treetop = ["dep:treetop-client"]
```

Add to `[dependencies]`:

```toml
async-trait = "0.1"
# treetop-client is at 0.0.1 and not yet on crates.io. Pin to a known good
# revision; bump explicitly when the upstream API stabilizes.
treetop-client = { git = "https://github.com/terjekv/treetop-client", optional = true }
```

- [ ] **Step 2: Verify both feature combinations build**

Run:

```bash
cargo check --no-default-features --features permissions-local,swagger-ui
cargo check --no-default-features --features permissions-local,permissions-treetop,swagger-ui
cargo check
```

Expected: all three succeed.

- [ ] **Step 3: Commit**

```bash
git add Cargo.toml Cargo.lock
git commit -m "build: add async-trait + permissions-{local,treetop} feature flags"
```

---

## Phase 1 — Trait surface and types

### Task 1.1: Define types module

**Files:**
- Create: `src/permissions/mod.rs`
- Create: `src/permissions/types.rs`
- Modify: `src/lib.rs`
- Test: `src/permissions/types.rs` (inline `mod tests`)

- [ ] **Step 1: Register the module in `src/lib.rs`**

Add the line in alphabetical order with the other `pub mod` lines (or `mod` if private). Inspect existing visibility in `src/lib.rs` and match the dominant style. Add:

```rust
pub mod permissions;
```

- [ ] **Step 2: Create `src/permissions/mod.rs` skeleton**

```rust
pub mod backend;
pub mod context;
pub mod types;

#[cfg(feature = "permissions-local")]
pub mod local;

#[cfg(feature = "permissions-treetop")]
pub mod treetop;

#[cfg(test)]
pub mod test_support;

pub use backend::PermissionBackend;
pub use context::AppContext;
pub use types::{
    AuthorizationResult, AuthzTarget, PermissionDecision, PermissionRequest, PrincipalRef,
    ResourceAttrs, ResourceKind, ResourceRef,
};
```

- [ ] **Step 3: Write the failing test for types**

Create `src/permissions/types.rs`:

```rust
use crate::models::Permissions;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrincipalRef {
    pub user_id: i32,
    pub group_ids: Vec<i32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
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

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ResourceAttrs {
    pub namespace_id: Option<i32>,
    pub class_id: Option<i32>,
    pub from_namespace_id: Option<i32>,
    pub to_namespace_id: Option<i32>,
    pub submitted_by: Option<i32>,
    pub name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResourceRef {
    pub kind: ResourceKind,
    pub id: i32,
    pub attrs: ResourceAttrs,
}

impl ResourceRef {
    pub fn namespace(namespace_id: i32) -> Self {
        Self {
            kind: ResourceKind::Namespace,
            id: namespace_id,
            attrs: ResourceAttrs {
                namespace_id: Some(namespace_id),
                ..Default::default()
            },
        }
    }

    pub fn system() -> Self {
        Self {
            kind: ResourceKind::System,
            id: 0,
            attrs: ResourceAttrs::default(),
        }
    }

    pub fn namespace_id(&self) -> Option<i32> {
        self.attrs.namespace_id
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PermissionRequest {
    pub resource: ResourceRef,
    pub permissions: Vec<Permissions>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PermissionDecision {
    Allow,
    Deny,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthorizationResult {
    pub request: PermissionRequest,
    pub decision: PermissionDecision,
}

/// A target that can be authorized against. Implemented by every model that
/// can be the subject of a permission check (Namespace, HubuumClass,
/// HubuumObject, …).
#[async_trait::async_trait]
pub trait AuthzTarget: Send + Sync {
    async fn to_resource_ref(&self, pool: &crate::db::DbPool) -> Result<ResourceRef, crate::errors::ApiError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn namespace_helper_sets_namespace_id_attr() {
        let r = ResourceRef::namespace(42);
        assert_eq!(r.kind, ResourceKind::Namespace);
        assert_eq!(r.id, 42);
        assert_eq!(r.namespace_id(), Some(42));
    }

    #[test]
    fn system_resource_has_no_namespace() {
        let r = ResourceRef::system();
        assert_eq!(r.kind, ResourceKind::System);
        assert_eq!(r.namespace_id(), None);
    }
}
```

- [ ] **Step 4: Run the tests**

Run: `cargo test --lib permissions::types::tests`
Expected: PASS for both helpers.

- [ ] **Step 5: Commit**

```bash
git add src/lib.rs src/permissions/mod.rs src/permissions/types.rs
git commit -m "feat(permissions): introduce PermissionBackend types and AuthzTarget trait"
```

---

### Task 1.2: Define the `PermissionBackend` trait

**Files:**
- Create: `src/permissions/backend.rs`

- [ ] **Step 1: Write the trait**

```rust
use async_trait::async_trait;

use crate::errors::ApiError;
use crate::models::{
    GroupPermission, Namespace, Permission, Permissions, PermissionsList, QueryOptions,
};

use super::types::{
    AuthorizationResult, PermissionDecision, PermissionRequest, PrincipalRef,
};

#[async_trait]
pub trait PermissionBackend: Send + Sync {
    /// Batch point check: does the principal satisfy each request?
    /// Order of the returned vector matches the order of `requests`.
    ///
    /// This is the only required decision method. The single-request and
    /// filter helpers below default to wrapping it; backends that can
    /// batch transport-side (e.g. Treetop's `AuthorizeRequest`) only need
    /// to override `authorize_many`.
    async fn authorize_many(
        &self,
        principal: &PrincipalRef,
        requests: Vec<PermissionRequest>,
    ) -> Result<Vec<PermissionDecision>, ApiError>;

    /// Single point check. Default: dispatches to `authorize_many` with a
    /// one-element vector. Backends rarely override.
    async fn authorize(
        &self,
        principal: &PrincipalRef,
        request: PermissionRequest,
    ) -> Result<PermissionDecision, ApiError> {
        let mut decisions = self.authorize_many(principal, vec![request]).await?;
        decisions.pop().ok_or_else(|| {
            ApiError::InternalServerError(
                "permission backend returned no decisions for a single request".to_string(),
            )
        })
    }

    /// Filter a candidate set: tags each request with its decision, in
    /// input order. Default: pairs `authorize_many`'s result with the
    /// inputs.
    async fn authorize_candidates(
        &self,
        principal: &PrincipalRef,
        requests: Vec<PermissionRequest>,
    ) -> Result<Vec<AuthorizationResult>, ApiError> {
        let decisions = self.authorize_many(principal, requests.clone()).await?;
        Ok(requests
            .into_iter()
            .zip(decisions)
            .map(|(request, decision)| AuthorizationResult { request, decision })
            .collect())
    }

    /// All namespaces on which the principal has every requested permission.
    /// Used by listing endpoints that want to scope their candidate query
    /// (e.g. `GET /templates`).
    ///
    /// Empty `permissions` means "any permission grants visibility" — the
    /// namespace appears if the principal has any row on it. Callers
    /// usually pass one or more concrete permissions.
    async fn namespaces_user_can(
        &self,
        principal: &PrincipalRef,
        permissions: &[Permissions],
    ) -> Result<Vec<Namespace>, ApiError>;

    /// (group, permission) pairs visible on a namespace, paginated.
    /// Returns `(rows, total_count)` so handlers can populate `X-Total-Count`.
    async fn groups_with_permissions_on(
        &self,
        namespace_id: i32,
        permissions_filter: &[Permissions],
        page: &QueryOptions,
    ) -> Result<(Vec<GroupPermission>, i64), ApiError>;

    /// Single group's permissions on a namespace, or `None` if no row.
    /// In Treetop mode `id` / `created_at` / `updated_at` are synthetic.
    async fn group_permission_on(
        &self,
        namespace_id: i32,
        group_id: i32,
    ) -> Result<Option<Permission>, ApiError>;

    /// Apply (grant or replace) a set of permissions to a group on a namespace.
    /// Treetop returns `ApiError::NotImplemented`.
    async fn apply_permissions(
        &self,
        namespace_id: i32,
        group_id: i32,
        list: PermissionsList<Permissions>,
        replace_existing: bool,
    ) -> Result<Permission, ApiError>;

    /// Revoke specific permissions from a group on a namespace.
    /// Treetop returns `ApiError::NotImplemented`.
    async fn revoke_permissions(
        &self,
        namespace_id: i32,
        group_id: i32,
        list: PermissionsList<Permissions>,
    ) -> Result<Permission, ApiError>;

    /// Revoke all permissions of a group on a namespace.
    /// Treetop returns `ApiError::NotImplemented`.
    async fn revoke_all(&self, namespace_id: i32, group_id: i32) -> Result<(), ApiError>;

    /// Whether mutations are supported. Handlers can early-reject before
    /// calling the mutation methods if they want a cleaner error path.
    fn supports_mutation(&self) -> bool;

    /// Backend kind identifier, used in tracing spans and the startup log.
    fn kind(&self) -> &'static str;
}
```

- [ ] **Step 2: Verify build**

Run: `cargo check --lib`
Expected: succeeds. (Trait has no impls yet but compiles.)

- [ ] **Step 3: Commit**

```bash
git add src/permissions/backend.rs
git commit -m "feat(permissions): define PermissionBackend trait"
```

---

### Task 1.3: Define `AppContext` and extend `BackendContext`

**Files:**
- Create: `src/permissions/context.rs`
- Modify: `src/traits/context.rs`

- [ ] **Step 1: Define `AppContext`**

`src/permissions/context.rs`:

```rust
use std::sync::Arc;

use crate::db::DbPool;
use crate::traits::BackendContext;

use super::backend::PermissionBackend;

#[derive(Clone)]
pub struct AppContext {
    pub db_pool: DbPool,
    pub permissions: Arc<dyn PermissionBackend>,
}

impl AppContext {
    pub fn new(db_pool: DbPool, permissions: Arc<dyn PermissionBackend>) -> Self {
        Self { db_pool, permissions }
    }
}

impl BackendContext for AppContext {
    fn db_pool(&self) -> &DbPool {
        &self.db_pool
    }

    fn permission_backend(&self) -> &dyn PermissionBackend {
        self.permissions.as_ref()
    }
}

impl<T> BackendContext for actix_web::web::Data<T>
where
    T: BackendContext + ?Sized + 'static,
{
    fn db_pool(&self) -> &DbPool {
        self.as_ref().db_pool()
    }

    fn permission_backend(&self) -> &dyn PermissionBackend {
        self.as_ref().permission_backend()
    }
}
```

- [ ] **Step 2: Extend `BackendContext`**

Edit `src/traits/context.rs`. Replace the existing trait definition with:

```rust
use crate::db::DbPool;
use crate::permissions::backend::PermissionBackend;

/// Provides access to shared application services.
///
/// `db_pool()` is always available. `permission_backend()` is only available
/// from `AppContext` and similar full-context wrappers — pure DB-only code
/// paths must take a `&DbPool` directly rather than going through this trait.
pub trait BackendContext {
    fn db_pool(&self) -> &DbPool;

    /// The active permission backend. Production code receives this through
    /// `AppContext`; tests construct an `AppContext` with the local backend
    /// (or a mock) by default.
    fn permission_backend(&self) -> &dyn PermissionBackend;
}

impl<T> BackendContext for &T
where
    T: BackendContext + ?Sized,
{
    fn db_pool(&self) -> &DbPool {
        (*self).db_pool()
    }

    fn permission_backend(&self) -> &dyn PermissionBackend {
        (*self).permission_backend()
    }
}
```

Remove the existing `impl BackendContext for DbPool` block — the trait now
requires `permission_backend()` and a bare `DbPool` cannot supply one. Code
paths that previously took `&dyn BackendContext` but only needed the pool
must change to take `&DbPool` directly. (The next phase migrates those
sites.)

- [ ] **Step 3: Verify build (will likely fail elsewhere)**

Run: `cargo check --lib 2>&1 | head -80`
Expected: errors at every site that relied on `impl BackendContext for DbPool`.
Capture the list of failing files — they are the call sites Phase 2 will
migrate. Do NOT migrate them in this task.

- [ ] **Step 4: Temporarily restore the impl behind a doc-deprecated guard**

To keep the build green during the multi-phase refactor, add this **temporary**
shim back to `src/traits/context.rs` near the bottom:

```rust
/// TEMPORARY: kept so the multi-phase refactor in
/// `docs/superpowers/plans/2026-05-02-pluggable-permissions.md` can land
/// incrementally. Will be removed in Phase 2 once every call site receives
/// either an `AppContext` or a bare `&DbPool`. Calling `permission_backend()`
/// here panics; that surfaces any code path that needs migration.
impl BackendContext for crate::db::DbPool {
    fn db_pool(&self) -> &crate::db::DbPool {
        self
    }

    fn permission_backend(&self) -> &dyn PermissionBackend {
        panic!(
            "DbPool used as BackendContext for a permission-aware operation; \
             switch the caller to AppContext (see plans/2026-05-02-pluggable-permissions.md)"
        )
    }
}
```

This panic shim is intentional and tracked in Phase 2.

- [ ] **Step 5: Verify build**

Run: `cargo build --lib`
Expected: succeeds.

- [ ] **Step 6: Commit**

```bash
git add src/traits/context.rs src/permissions/context.rs src/permissions/mod.rs
git commit -m "feat(permissions): introduce AppContext; extend BackendContext with permission_backend()"
```

---

## Phase 2 — Local backend implementation

### Task 2.1: Move helper queries into `permissions::local::queries`

**Files:**
- Create: `src/permissions/local/mod.rs` (skeleton)
- Create: `src/permissions/local/queries.rs`
- Read: `src/db/traits/permissions.rs`, `src/db/traits/namespace/permissions.rs`

This task lifts the existing logic into the new module without changing
behavior; the `db/traits/...` files temporarily re-export from the new
location.

- [ ] **Step 1: Create the queries module**

`src/permissions/local/queries.rs` — copy these functions verbatim from
their current locations and make them `pub(super)`:

From `src/db/traits/permissions.rs`:
- the body of `user_can_all_from_backend` (rename to `user_can_all_query`)
- the body of `apply_permissions_from_backend` (rename to `apply_permissions_query`)
- the body of `revoke_permissions_from_backend` (rename to `revoke_permissions_query`)
- the body of `revoke_all_from_backend` (rename to `revoke_all_query`)

Each becomes a free `pub(super) async fn` taking `pool: &DbPool` plus the
data inputs (no `self`, no `Serialize + NamespaceAccessors` bound). The
bodies do not change.

From `src/db/traits/namespace/permissions.rs`:
- `user_on_from_backend` → `user_on_query`
- `user_on_paginated_with_total_count_from_backend` → `user_on_paginated_query`
- `user_can_on_any_from_backend` → `user_can_on_any_query`
- `group_can_on_from_backend` → `group_can_on_query`
- `groups_can_on_from_backend` → `groups_can_on_query`
- `groups_can_on_paginated_with_total_count_from_backend` → `groups_can_on_paginated_query`
- `groups_on_from_backend` → `groups_on_query`
- `groups_on_paginated_from_backend` → `groups_on_paginated_query`
- `groups_on_paginated_with_total_count_from_backend` → `groups_on_paginated_with_total_count_query`
- `count_groups_on_paginated_from_backend` → `count_groups_on_paginated_query`
- `group_on_from_backend` → `group_on_query`
- the `permission_filter_sql` helper (private)

- [ ] **Step 2: Re-export from old locations to keep call sites compiling**

In `src/db/traits/permissions.rs`, delete the trait body and replace with
shim re-exports:

```rust
pub use crate::permissions::local::queries::{
    apply_permissions_query as apply_permissions_from_backend,
    revoke_all_query as revoke_all_from_backend,
    revoke_permissions_query as revoke_permissions_from_backend,
    user_can_all_query as user_can_all_from_backend,
};

// PermissionControllerBackend trait is removed; consumers now go through
// `PermissionBackend` (Phase 3).
```

Adjust trait usages in `src/traits/permissions.rs` to call the free
functions directly until Phase 3 rewrites that file. Do the same shim for
`src/db/traits/namespace/permissions.rs`.

- [ ] **Step 3: Verify build**

Run: `cargo build --lib`
Expected: succeeds.

- [ ] **Step 4: Run existing permission tests**

Run: `cargo test --lib permissions`
Expected: any pre-existing tests still pass.

- [ ] **Step 5: Commit**

```bash
git add src/permissions/local/ src/db/traits/permissions.rs src/db/traits/namespace/permissions.rs src/permissions/mod.rs
git commit -m "refactor(permissions): move SQL query helpers into permissions::local::queries"
```

---

### Task 2.2: Implement `LocalPermissionBackend`

**Files:**
- Modify: `src/permissions/local/mod.rs`
- Test: `src/tests/permissions/backend_trait.rs` (created later in Task 2.3)

- [ ] **Step 1: Write the implementation**

`src/permissions/local/mod.rs`:

```rust
pub mod queries;

use async_trait::async_trait;
use chrono::Utc;

use crate::db::DbPool;
use crate::errors::ApiError;
use crate::models::{
    GroupPermission, Namespace, Permission, Permissions, PermissionsList, QueryOptions,
};

use super::backend::PermissionBackend;
use super::types::{
    AuthorizationResult, PermissionDecision, PermissionRequest, PrincipalRef, ResourceKind,
};

pub struct LocalPermissionBackend {
    pool: DbPool,
}

impl LocalPermissionBackend {
    pub fn new(pool: DbPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl PermissionBackend for LocalPermissionBackend {
    async fn authorize_many(
        &self,
        principal: &PrincipalRef,
        requests: Vec<PermissionRequest>,
    ) -> Result<Vec<PermissionDecision>, ApiError> {
        // SQL backend has no transport-side batch; just loop.
        let mut decisions = Vec::with_capacity(requests.len());
        for request in requests {
            let decision = match request.resource.namespace_id() {
                None => {
                    // System-scoped checks: admin short-circuit lives one
                    // level up in PermissionController; here we deny.
                    PermissionDecision::Deny
                }
                Some(namespace_id) => {
                    let allowed = queries::user_can_all_query(
                        &self.pool,
                        principal.user_id,
                        namespace_id,
                        request.permissions,
                    )
                    .await?;
                    if allowed { PermissionDecision::Allow } else { PermissionDecision::Deny }
                }
            };
            decisions.push(decision);
        }
        Ok(decisions)
    }

    // `authorize` and `authorize_candidates` use the trait defaults.

    async fn namespaces_user_can(
        &self,
        principal: &PrincipalRef,
        permissions: &[Permissions],
    ) -> Result<Vec<Namespace>, ApiError> {
        queries::user_can_on_any_query(&self.pool, principal.user_id, permissions).await
    }

    async fn groups_with_permissions_on(
        &self,
        namespace_id: i32,
        permissions_filter: &[Permissions],
        page: &QueryOptions,
    ) -> Result<(Vec<GroupPermission>, i64), ApiError> {
        queries::groups_on_paginated_with_total_count_query(
            &self.pool,
            namespace_id,
            permissions_filter.to_vec(),
            page,
        )
        .await
    }

    async fn group_permission_on(
        &self,
        namespace_id: i32,
        group_id: i32,
    ) -> Result<Option<Permission>, ApiError> {
        match queries::group_on_query(&self.pool, namespace_id, group_id).await {
            Ok(p) => Ok(Some(p)),
            Err(ApiError::NotFound(_)) => Ok(None),
            Err(e) => Err(e),
        }
    }

    async fn apply_permissions(
        &self,
        namespace_id: i32,
        group_id: i32,
        list: PermissionsList<Permissions>,
        replace_existing: bool,
    ) -> Result<Permission, ApiError> {
        queries::apply_permissions_query(
            &self.pool,
            namespace_id,
            group_id,
            list,
            replace_existing,
        )
        .await
    }

    async fn revoke_permissions(
        &self,
        namespace_id: i32,
        group_id: i32,
        list: PermissionsList<Permissions>,
    ) -> Result<Permission, ApiError> {
        queries::revoke_permissions_query(&self.pool, namespace_id, group_id, list).await
    }

    async fn revoke_all(&self, namespace_id: i32, group_id: i32) -> Result<(), ApiError> {
        queries::revoke_all_query(&self.pool, namespace_id, group_id).await
    }

    fn supports_mutation(&self) -> bool {
        true
    }

    fn kind(&self) -> &'static str {
        "local"
    }
}
```

The free `queries::user_can_all_query` signature change (now taking `user_id`
+ `namespace_id` + `permissions` directly instead of `pool` + user object +
permissions) means Task 2.1 must accept that signature. Update Task 2.1's
`user_can_all_query` accordingly: it inlines the `is_admin` check by
delegating to the existing `user.is_admin(pool)` call site — but since that
needs a `User` accessor, instead extract a small helper:

```rust
pub(super) async fn user_can_all_query(
    pool: &DbPool,
    user_id: i32,
    namespace_id: i32,
    permissions_requested: Vec<Permissions>,
) -> Result<bool, ApiError> {
    use crate::models::UserID;
    use crate::traits::{GroupAccessors, GroupMemberships};

    let user = UserID(user_id);
    if user.is_admin(pool).await? {
        return Ok(true);
    }

    let lookup_table = crate::schema::permissions::dsl::permissions;
    let group_id_field = crate::schema::permissions::dsl::group_id;
    let namespace_id_field = crate::schema::permissions::dsl::namespace_id;
    let group_id_subquery = user.group_ids_subquery_from_backend();

    let mut base_query = lookup_table
        .into_boxed()
        .filter(namespace_id_field.eq(namespace_id))
        .filter(group_id_field.eq_any(group_id_subquery));

    for permission in permissions_requested {
        base_query = permission.create_boxed_filter(base_query, true);
    }

    let result: Option<Permission> = crate::db::with_connection(pool, |conn| {
        base_query.first::<Permission>(conn).optional()
    })?;

    Ok(result.is_some())
}

pub(super) async fn user_can_on_any_query(
    pool: &DbPool,
    user_id: i32,
    permissions_requested: &[Permissions],
) -> Result<Vec<Namespace>, ApiError> {
    // Lifted from user_can_on_any_from_backend in
    // src/db/traits/namespace/permissions.rs but extended to take a slice
    // of permissions (matches PermissionBackend::namespaces_user_can).
    // Empty `permissions_requested` means "any row qualifies" — return
    // namespaces where the user has any permission row.
    use crate::models::UserID;
    use crate::traits::{GroupAccessors, GroupMemberships};

    let user = UserID(user_id);
    if user.is_admin(pool).await? {
        return crate::db::with_connection(pool, |conn| {
            crate::schema::namespaces::table.load::<Namespace>(conn)
        });
    }

    let group_id_subquery = user.group_ids_subquery_from_backend();
    let mut filtered = crate::schema::permissions::dsl::permissions
        .into_boxed()
        .filter(crate::schema::permissions::dsl::group_id.eq_any(group_id_subquery));

    for permission in permissions_requested {
        filtered = permission.create_boxed_filter(filtered, true);
    }

    let accessible_namespace_ids: Vec<i32> = crate::db::with_connection(pool, |conn| {
        filtered
            .select(crate::schema::permissions::dsl::namespace_id)
            .load::<i32>(conn)
    })?;

    if accessible_namespace_ids.is_empty() {
        return Ok(vec![]);
    }

    crate::db::with_connection(pool, |conn| {
        crate::schema::namespaces::table
            .filter(crate::schema::namespaces::id.eq_any(accessible_namespace_ids))
            .load::<Namespace>(conn)
    })
}
```

Update Task 2.1's notes / commit if you need to come back here.

- [ ] **Step 2: Verify build**

Run: `cargo build --lib --features permissions-local`
Expected: succeeds.

- [ ] **Step 3: Commit**

```bash
git add src/permissions/local/
git commit -m "feat(permissions): implement LocalPermissionBackend"
```

---

### Task 2.3: Trait-level smoke test against the test database

**Files:**
- Create: `src/tests/permissions/mod.rs`
- Create: `src/tests/permissions/backend_trait.rs`
- Modify: `src/tests/mod.rs` (add `pub mod permissions;`)

- [ ] **Step 1: Wire the test module**

In `src/tests/mod.rs`, add (in alphabetical order):

```rust
pub mod permissions;
```

Create `src/tests/permissions/mod.rs`:

```rust
pub mod backend_trait;
```

- [ ] **Step 2: Write the failing test**

`src/tests/permissions/backend_trait.rs`:

```rust
use std::sync::Arc;

use crate::permissions::{
    AuthzTarget, PermissionBackend, PermissionDecision, PermissionRequest, PrincipalRef,
    ResourceRef,
};
use crate::permissions::local::LocalPermissionBackend;
use crate::models::{Permissions, PermissionsList};
use crate::tests::common::*; // existing test helpers: setup_pool, create_user, ...

#[actix_web::test]
async fn local_backend_grants_then_authorizes_namespace_read() {
    let pool = setup_pool().await;
    let backend: Arc<dyn PermissionBackend> = Arc::new(LocalPermissionBackend::new(pool.clone()));
    let user = create_user(&pool, "perm_test_user").await;
    let group = create_group(&pool, "perm_test_group").await;
    add_user_to_group(&pool, &user, &group).await;
    let namespace = create_namespace(&pool, "perm_test_ns").await;

    let principal = PrincipalRef {
        user_id: user.id,
        group_ids: vec![group.id],
    };

    // Before grant: deny.
    let req = PermissionRequest {
        resource: ResourceRef::namespace(namespace.id),
        permissions: vec![Permissions::ReadCollection],
    };
    let d = backend.authorize(&principal, req.clone()).await.unwrap();
    assert_eq!(d, PermissionDecision::Deny);

    // Grant ReadCollection.
    backend
        .apply_permissions(
            namespace.id,
            group.id,
            PermissionsList::new(vec![Permissions::ReadCollection]),
            false,
        )
        .await
        .unwrap();

    // After grant: allow.
    let d = backend.authorize(&principal, req).await.unwrap();
    assert_eq!(d, PermissionDecision::Allow);
}
```

If `setup_pool`, `create_user`, `create_group`, `add_user_to_group`, or
`create_namespace` do not exist as named: open `src/tests/mod.rs` and the
tests under `src/tests/api/v1/` to find the equivalent helpers and use the
established names. Do not invent helpers.

- [ ] **Step 3: Run the test against the test database**

Set up Postgres per `docs/development.md` if it isn't already. Run:

```bash
cargo test --lib --features permissions-local tests::permissions::backend_trait::local_backend_grants_then_authorizes_namespace_read -- --nocapture
```

Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add src/tests/permissions/ src/tests/mod.rs
git commit -m "test(permissions): smoke test LocalPermissionBackend grant + authorize round-trip"
```

---

### Task 2.4: Add `PermissionBackendKind` and `build_permission_backend`

**Files:**
- Modify: `src/permissions/mod.rs`
- Modify: `src/config.rs`

- [ ] **Step 1: Add config knob**

In `src/config.rs`, add the enum near `TlsBackend`:

```rust
#[derive(ValueEnum, Debug, Deserialize, Serialize, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum PermissionBackendKind {
    #[default]
    Local,
    Treetop,
}

impl PermissionBackendKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Local => "local",
            Self::Treetop => "treetop",
        }
    }
}
```

Add fields to `AppConfig`:

```rust
    /// Active permission backend
    #[clap(
        long,
        env = "HUBUUM_PERMISSION_BACKEND",
        value_enum,
        ignore_case = true,
        default_value = "local"
    )]
    pub permission_backend: PermissionBackendKind,

    /// Treetop server URL (required when HUBUUM_PERMISSION_BACKEND=treetop)
    #[clap(long, env = "HUBUUM_TREETOP_URL", default_value = None)]
    pub treetop_url: Option<String>,

    /// Treetop client connect timeout in milliseconds
    #[clap(long, env = "HUBUUM_TREETOP_CONNECT_TIMEOUT_MS", default_value_t = 5000)]
    pub treetop_connect_timeout_ms: u64,

    /// Treetop client request timeout in milliseconds
    #[clap(long, env = "HUBUUM_TREETOP_REQUEST_TIMEOUT_MS", default_value_t = 30000)]
    pub treetop_request_timeout_ms: u64,

    /// Optional path to a CA certificate to trust for the Treetop client
    #[clap(long, env = "HUBUUM_TREETOP_CA_CERT", default_value = None)]
    pub treetop_ca_cert: Option<String>,

    /// Accept invalid Treetop server certificates (DEVELOPMENT ONLY)
    #[clap(long, env = "HUBUUM_TREETOP_ACCEPT_INVALID_CERTS", default_value_t = false)]
    pub treetop_accept_invalid_certs: bool,
```

Mirror these in the `#[cfg(test)] fn get_config_from_env()` helper (parse
env vars; default to `Local` and `None`). Add to `AppConfig::validate()`:

```rust
        if self.permission_backend == PermissionBackendKind::Treetop && self.treetop_url.is_none() {
            return Err(ApiError::BadRequest(
                "treetop_url is required when permission_backend=treetop".to_string(),
            ));
        }
```

- [ ] **Step 2: Implement `build_permission_backend`**

Append to `src/permissions/mod.rs`:

```rust
use std::sync::Arc;

use crate::config::{AppConfig, PermissionBackendKind};
use crate::db::DbPool;
use crate::errors::ApiError;

pub async fn build_permission_backend(
    cfg: &AppConfig,
    pool: DbPool,
) -> Result<Arc<dyn PermissionBackend>, ApiError> {
    match cfg.permission_backend {
        #[cfg(feature = "permissions-local")]
        PermissionBackendKind::Local => Ok(Arc::new(local::LocalPermissionBackend::new(pool))),

        #[cfg(not(feature = "permissions-local"))]
        PermissionBackendKind::Local => Err(ApiError::BadRequest(
            "binary built without `permissions-local` feature".to_string(),
        )),

        #[cfg(feature = "permissions-treetop")]
        PermissionBackendKind::Treetop => {
            let url = cfg.treetop_url.as_deref().ok_or_else(|| {
                ApiError::BadRequest("HUBUUM_TREETOP_URL is required".to_string())
            })?;
            let backend = treetop::TreetopPermissionBackend::connect(url, cfg, pool).await?;
            Ok(Arc::new(backend))
        }

        #[cfg(not(feature = "permissions-treetop"))]
        PermissionBackendKind::Treetop => Err(ApiError::BadRequest(
            "binary built without `permissions-treetop` feature".to_string(),
        )),
    }
}
```

(`treetop::TreetopPermissionBackend::connect` is added in Phase 5; the
feature gate keeps this compiling until then.)

- [ ] **Step 3: Verify build with the local-only feature combo**

Run: `cargo check --no-default-features --features permissions-local,swagger-ui`
Expected: succeeds.

- [ ] **Step 4: Commit**

```bash
git add src/config.rs src/permissions/mod.rs
git commit -m "feat(config): add HUBUUM_PERMISSION_BACKEND and Treetop knobs"
```

---

### Task 2.5: Wire `AppContext` into actix startup

**Files:**
- Modify: `src/main.rs`

- [ ] **Step 1: Build context at startup**

In `src/main.rs`, after `let pool = init_pool(...)` and before `ensure_task_worker_running`:

```rust
    let permission_backend = match crate::permissions::build_permission_backend(&app_config, pool.clone()).await {
        Ok(b) => b,
        Err(e) => fatal_error(
            &format!("Failed to initialize permission backend: {e}"),
            errors::EXIT_CODE_PERMISSION_BACKEND_ERROR,
        ),
    };
    info!(
        message = "permission backend initialized",
        backend = permission_backend.kind(),
    );

    let app_ctx = crate::permissions::AppContext::new(pool.clone(), permission_backend.clone());
```

(`app_config` already exists above.)

In the `HttpServer::new` closure replace `.app_data(Data::new(pool.clone()))`
**addition** (do not remove the pool yet — both will coexist during the
multi-phase refactor) with:

```rust
            .app_data(Data::new(pool.clone()))
            .app_data(Data::new(app_ctx.clone()))
```

- [ ] **Step 2: Verify the binary still builds**

Run: `cargo build --bin hubuum-server`
Expected: succeeds.

- [ ] **Step 3: Smoke test**

Run the existing `run_tests.sh` or a single API test (whichever is the
project default):

```bash
HUBUUM_PERMISSION_BACKEND=local cargo test --lib tests::api::v1::namespaces -- --test-threads=1
```

Expected: existing tests still pass; the new `app_ctx` is registered but
not yet read by handlers.

- [ ] **Step 4: Commit**

```bash
git add src/main.rs
git commit -m "feat(server): construct AppContext at startup and register it on actix"
```

---

## Phase 3 — Refactor authorization call sites to use the backend

### Task 3.1: Implement `AuthzTarget` for the existing model types

**Files:**
- Modify: `src/permissions/types.rs`
- Modify: `src/models/namespace.rs`, `src/models/class.rs`, `src/models/object.rs`, `src/models/relation.rs`, `src/models/report_template.rs`, `src/models/task.rs`

- [ ] **Step 1: Implement `AuthzTarget` for `Namespace` / `NamespaceID`**

In `src/models/namespace.rs`, after the existing `impl NamespaceAccessors`:

```rust
#[async_trait::async_trait]
impl crate::permissions::AuthzTarget for Namespace {
    async fn to_resource_ref(
        &self,
        _pool: &crate::db::DbPool,
    ) -> Result<crate::permissions::ResourceRef, crate::errors::ApiError> {
        Ok(crate::permissions::ResourceRef::namespace(self.id))
    }
}

#[async_trait::async_trait]
impl crate::permissions::AuthzTarget for crate::models::NamespaceID {
    async fn to_resource_ref(
        &self,
        _pool: &crate::db::DbPool,
    ) -> Result<crate::permissions::ResourceRef, crate::errors::ApiError> {
        Ok(crate::permissions::ResourceRef::namespace(self.0))
    }
}
```

- [ ] **Step 2: Implement for `HubuumClass` / `HubuumClassID`**

In `src/models/class.rs`:

```rust
#[async_trait::async_trait]
impl crate::permissions::AuthzTarget for HubuumClass {
    async fn to_resource_ref(
        &self,
        _pool: &crate::db::DbPool,
    ) -> Result<crate::permissions::ResourceRef, crate::errors::ApiError> {
        use crate::permissions::{ResourceAttrs, ResourceKind, ResourceRef};
        Ok(ResourceRef {
            kind: ResourceKind::Class,
            id: self.id,
            attrs: ResourceAttrs {
                namespace_id: Some(self.namespace_id),
                name: Some(self.name.clone()),
                ..Default::default()
            },
        })
    }
}

#[async_trait::async_trait]
impl crate::permissions::AuthzTarget for HubuumClassID {
    async fn to_resource_ref(
        &self,
        pool: &crate::db::DbPool,
    ) -> Result<crate::permissions::ResourceRef, crate::errors::ApiError> {
        let class = self.class(pool).await?;
        class.to_resource_ref(pool).await
    }
}
```

- [ ] **Step 3: Repeat the same pattern for**

- `HubuumObject` / `HubuumObjectID` in `src/models/object.rs`
  (`ResourceKind::Object`, attrs include `namespace_id` and `class_id`)
- `HubuumClassRelation` / `HubuumClassRelationID` in `src/models/relation.rs`
  (`ResourceKind::ClassRelation`; attrs include `namespace_id`,
  `from_namespace_id`, `to_namespace_id`)
- `HubuumObjectRelation` / `HubuumObjectRelationID` (`ResourceKind::ObjectRelation`)
- `HubuumReportTemplate` / `HubuumReportTemplateID` in
  `src/models/report_template.rs` (`ResourceKind::Template`)
- `HubuumTask` / `HubuumTaskID` in `src/models/task.rs`
  (`ResourceKind::Task`; attrs include `submitted_by`)

For each `*ID` type, follow the convention from `class.rs`: load the full
record, delegate to its `to_resource_ref`. This mirrors the existing
`NamespaceAccessors::namespace_id` resolution pattern.

- [ ] **Step 4: Verify build**

Run: `cargo build --lib`
Expected: succeeds.

- [ ] **Step 5: Commit**

```bash
git add src/models/
git commit -m "feat(permissions): implement AuthzTarget for all model types"
```

---

### Task 3.2: Refactor `PermissionController::user_can_all` to delegate to backend

**Files:**
- Modify: `src/traits/permissions.rs`

- [ ] **Step 1: Replace the trait body**

`src/traits/permissions.rs` becomes a thin wrapper that delegates to
`backend.permission_backend()`:

```rust
use serde::Serialize;

use crate::errors::ApiError;
use crate::models::{Permission, Permissions, PermissionsList, User};
use crate::permissions::{
    AuthzTarget, PermissionBackend, PermissionDecision, PermissionRequest, PrincipalRef,
};
use crate::traits::{BackendContext, NamespaceAccessors, SelfAccessors};
use crate::db::traits::user::GroupMemberships;
use crate::models::traits::GroupAccessors;

#[allow(dead_code)]
pub trait PermissionController: Serialize + AuthzTarget + NamespaceAccessors {
    async fn user_can<C, U>(
        &self,
        backend: &C,
        user: U,
        permission: Permissions,
    ) -> Result<bool, ApiError>
    where
        C: BackendContext + ?Sized,
        U: SelfAccessors<User> + GroupAccessors + GroupMemberships,
    {
        self.user_can_all(backend, user, vec![permission]).await
    }

    async fn user_can_all<C, U>(
        &self,
        backend: &C,
        user: U,
        permissions: Vec<Permissions>,
    ) -> Result<bool, ApiError>
    where
        C: BackendContext + ?Sized,
        U: SelfAccessors<User> + GroupAccessors + GroupMemberships,
    {
        // Admin short-circuit lives here, above the backend.
        if user.is_admin(backend.db_pool()).await? {
            return Ok(true);
        }

        let resource = self.to_resource_ref(backend.db_pool()).await?;
        let principal = PrincipalRef {
            user_id: user.id(),
            group_ids: user.group_ids(backend.db_pool()).await?,
        };
        let request = PermissionRequest { resource, permissions };
        let decision = backend.permission_backend().authorize(&principal, request).await?;
        Ok(decision == PermissionDecision::Allow)
    }

    async fn grant<C>(
        &self,
        backend: &C,
        group_id: i32,
        permission_list: PermissionsList<Permissions>,
    ) -> Result<Permission, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        let nid = self.namespace_id(backend.db_pool()).await?;
        backend
            .permission_backend()
            .apply_permissions(nid, group_id, permission_list, false)
            .await
    }

    async fn apply_permissions<C>(
        &self,
        backend: &C,
        group_id: i32,
        permission_list: PermissionsList<Permissions>,
        replace_existing: bool,
    ) -> Result<Permission, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        let nid = self.namespace_id(backend.db_pool()).await?;
        backend
            .permission_backend()
            .apply_permissions(nid, group_id, permission_list, replace_existing)
            .await
    }

    async fn revoke<C>(
        &self,
        backend: &C,
        group_id: i32,
        permission_list: PermissionsList<Permissions>,
    ) -> Result<Permission, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        let nid = self.namespace_id(backend.db_pool()).await?;
        backend
            .permission_backend()
            .revoke_permissions(nid, group_id, permission_list)
            .await
    }

    async fn grant_one<C>(
        &self,
        backend: &C,
        group_id: i32,
        permission: Permissions,
    ) -> Result<Permission, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        self.grant(backend, group_id, PermissionsList::new(vec![permission])).await
    }

    async fn revoke_one<C>(
        &self,
        backend: &C,
        group_id: i32,
        permission: Permissions,
    ) -> Result<Permission, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        self.revoke(backend, group_id, PermissionsList::new(vec![permission])).await
    }

    async fn set_permissions<C>(
        &self,
        backend: &C,
        group_id: i32,
        permission_list: PermissionsList<Permissions>,
    ) -> Result<Permission, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        self.apply_permissions(backend, group_id, permission_list, true).await
    }

    async fn revoke_all<C>(
        &self,
        backend: &C,
        group_id: i32,
    ) -> Result<(), ApiError>
    where
        C: BackendContext + ?Sized,
    {
        let nid = self.namespace_id(backend.db_pool()).await?;
        backend.permission_backend().revoke_all(nid, group_id).await
    }
}
```

The blanket `impl PermissionController for T` block stays where it is, but
its bound becomes `where T: Serialize + AuthzTarget + NamespaceAccessors`.

- [ ] **Step 2: Add `group_ids` accessor to `GroupAccessors`** (if not present)

Inspect `src/models/traits/user.rs`. If `GroupAccessors` already has a
non-subquery `group_ids(&self, pool: &DbPool) -> Result<Vec<i32>, ApiError>`,
use it. If only the subquery form exists, add the eager version:

```rust
async fn group_ids(&self, pool: &DbPool) -> Result<Vec<i32>, ApiError> {
    use crate::schema::user_groups::dsl::{group_id, user_groups, user_id as ug_user_id};
    crate::db::with_connection(pool, |conn| {
        user_groups
            .filter(ug_user_id.eq(self.id()))
            .select(group_id)
            .load::<i32>(conn)
    })
    .map_err(Into::into)
}
```

- [ ] **Step 3: Verify build**

Run: `cargo build --lib`
Expected: succeeds. (Some test or handler errors may surface — those are
addressed in subsequent tasks.)

- [ ] **Step 4: Commit**

```bash
git add src/traits/permissions.rs src/models/traits/user.rs
git commit -m "refactor(permissions): PermissionController defaults delegate to PermissionBackend"
```

---

### Task 3.3: Refactor `UserPermissions::can` to delegate to backend

**Files:**
- Modify: `src/db/traits/user/permissions.rs`

- [ ] **Step 1: Rewrite the trait method**

```rust
pub trait UserPermissions: SelfAccessors<User> + GroupAccessors + GroupMemberships {
    async fn can<C, P, N, I>(
        &self,
        ctx: &C,
        permissions: P,
        targets: I,
    ) -> Result<(), ApiError>
    where
        C: BackendContext + ?Sized,
        P: IntoIterator<Item = Permissions>,
        I: IntoIterator<Item = N>,
        N: AuthzTarget,
    {
        if self.is_admin(ctx.db_pool()).await? {
            return Ok(());
        }

        let permissions_vec: Vec<Permissions> = permissions.into_iter().collect();
        let principal = PrincipalRef {
            user_id: self.id(),
            group_ids: self.group_ids(ctx.db_pool()).await?,
        };

        let mut requests = Vec::new();
        for target in targets {
            let resource = target.to_resource_ref(ctx.db_pool()).await?;
            requests.push(PermissionRequest {
                resource,
                permissions: permissions_vec.clone(),
            });
        }

        let decisions = ctx
            .permission_backend()
            .authorize_many(&principal, requests)
            .await?;

        if decisions.iter().all(|d| *d == PermissionDecision::Allow) {
            Ok(())
        } else {
            Err(ApiError::Forbidden(
                "User does not have the required permissions".to_string(),
            ))
        }
    }
}
```

Add the imports it needs:

```rust
use crate::permissions::{AuthzTarget, PermissionDecision, PermissionRequest, PrincipalRef};
use crate::traits::BackendContext;
```

The signature now takes a `&C: BackendContext` instead of `&DbPool`. The
parameter name changes from `pool` to `ctx`.

- [ ] **Step 2: Verify build (will break callers)**

Run: `cargo build --lib 2>&1 | head -40`
Expected: errors at every `user.can(&pool, ...)` site — they need to pass
`AppContext` instead of `&DbPool`. These are migrated in Task 3.5.

- [ ] **Step 3: Commit**

```bash
git add src/db/traits/user/permissions.rs
git commit -m "refactor(permissions): UserPermissions::can delegates to PermissionBackend"
```

---

### Task 3.4: Refactor `can!` and `check_permissions!` macros

**Files:**
- Modify: `src/macros.rs`

- [ ] **Step 1: Rewrite `can!`**

Replace the `macro_rules! can` definition:

```rust
#[macro_export]
/// Check that a user has every listed permission on every listed target.
///
/// Targets must implement [`AuthzTarget`]. The macro preserves the typed
/// resource through to the permission backend (Treetop sees `HubuumClass`,
/// `HubuumObject`, etc. as distinct resource types).
macro_rules! can {
    ($ctx:expr, $user:expr, [$($perm:expr),+], $($target:expr),+) => {{
        use $crate::permissions::AuthzTarget as _;
        $user
            .can(
                $ctx,
                vec![$($perm),+],
                vec![$(&$target as &dyn $crate::permissions::AuthzTarget),+],
            )
            .await?
    }};
}
```

- [ ] **Step 2: Rewrite `check_permissions!`**

```rust
#[macro_export]
macro_rules! check_permissions {
    ($target:expr, $ctx:expr, $user:expr, $($permissions:expr),+ $(,)?) => {{
        use $crate::errors::ApiError;
        use $crate::permissions::AuthzTarget as _;
        use $crate::traits::PermissionController;
        use tracing::warn;

        let permissions_vec = vec![$($permissions),+];

        if !$target
            .user_can_all($ctx, $user.clone(), permissions_vec.clone())
            .await?
        {
            let user_id = $user.id();
            let resource = $target.to_resource_ref($ctx.db_pool()).await?;
            warn!(
                message = "Permission denied",
                user_id = user_id,
                resource_kind = ?resource.kind,
                resource_id = resource.id,
                permissions = ?permissions_vec,
            );
            return Err(ApiError::Forbidden(format!(
                "User {} does not have permissions {:?} on {:?}::{}",
                user_id, permissions_vec, resource.kind, resource.id
            )));
        }
    }};
}
```

- [ ] **Step 3: Verify build (will break callers)**

Run: `cargo build --lib 2>&1 | grep "^error" | head -30`
Expected: errors at every `can!(&pool, …)` and `check_permissions!(…, &pool, …)`
caller. These are addressed file-by-file in the next task.

- [ ] **Step 4: Commit**

```bash
git add src/macros.rs
git commit -m "refactor(permissions): can!/check_permissions! macros preserve typed AuthzTarget"
```

---

### Task 3.5: Migrate handler call sites

**Files (one task per file to keep diffs reviewable):**
- `src/api/v1/handlers/namespaces.rs`
- `src/api/v1/handlers/classes.rs`
- `src/api/v1/handlers/relations.rs`
- `src/api/v1/handlers/templates.rs`
- `src/api/v1/handlers/reports.rs`
- `src/api/v1/handlers/tasks.rs`
- `src/api/v1/handlers/imports.rs`
- `src/api/v1/handlers/users.rs`
- `src/api/v1/handlers/groups.rs`
- `src/api/v1/handlers/search.rs`
- `src/api/handlers/auth.rs`
- `src/api/handlers/meta.rs`

For each file:

- [ ] **Step 1: Change the handler signature**

Replace `pool: web::Data<DbPool>` with `ctx: web::Data<AppContext>`. Add
`use crate::permissions::AppContext;` if needed.

- [ ] **Step 2: Pass `&ctx` (or `ctx.as_ref()`) into permission calls**

Replace every `&pool` argument inside `can!(...)`, `check_permissions!(...)`,
`user_can_all(&pool, ...)`, `grant(&pool, ...)`, `revoke(&pool, ...)`,
`revoke_all(&pool, ...)`, etc. with `&ctx` (or `ctx.as_ref()` if a `&dyn`
coercion is needed).

- [ ] **Step 3: For DB-only operations, use `&ctx.db_pool` (or `ctx.db_pool()`)**

Operations that previously took `&pool` and only need DB access (e.g.
`User::get_by_username(&pool, ...)`) keep their pool usage; replace
`&pool` with `&ctx.db_pool`.

- [ ] **Step 4: Verify the file compiles**

Run: `cargo check --lib 2>&1 | grep -A2 "handlers/<file>" | head -20`
Expected: clean.

- [ ] **Step 5: Commit per file**

```bash
git add src/api/v1/handlers/<file>.rs
git commit -m "refactor(handlers/<file>): take AppContext for permission-aware operations"
```

(One commit per file makes review easier and lets the Phase regress quickly
if a single file breaks behavior.)

After all 12 files are migrated, run the full test suite once:

Run: `cargo test --lib`
Expected: green.

---

### Task 3.6: Refactor `AdminAccess` and `AdminOrSelfAccess` extractors

**Files:**
- Modify: `src/extractors/mod.rs`

- [ ] **Step 1: Read `AppContext` instead of `DbPool`**

Replace `req.app_data::<Data<DbPool>>()` with `req.app_data::<Data<AppContext>>()`
in both `AdminAccess::from_request` and `AdminOrSelfAccess::from_request`.
Use `ctx.db_pool()` for the `extract_user_from_token` and `is_admin` calls
(the admin short-circuit still lives at this level — it uses the DB
directly, not the permission backend).

```rust
let ctx = match req.app_data::<Data<AppContext>>() {
    Some(c) => c.clone(),
    None => {
        return future::ready(Err(ApiError::InternalServerError(
            "AppContext not found".to_string(),
        )))
        .boxed_local();
    }
};

let token_result = extract_token(req);
async move {
    let token = token_result?;
    let user = extract_user_from_token(ctx.db_pool(), &token).await?;
    if user.is_admin(ctx.db_pool()).await? {
        Ok(AdminAccess { token, user })
    } else {
        Err(ApiError::Forbidden("Permission denied".to_string()))
    }
}
.boxed_local()
```

Apply the same pattern to `UserAccess` and `AdminOrSelfAccess`.

- [ ] **Step 2: Verify build**

Run: `cargo build --lib`
Expected: succeeds.

- [ ] **Step 3: Run a smoke API test**

Run: `cargo test --lib tests::api::v1::namespaces -- --test-threads=1`
Expected: green.

- [ ] **Step 4: Commit**

```bash
git add src/extractors/mod.rs
git commit -m "refactor(extractors): read AppContext for token/admin extraction"
```

---

### Task 3.7: Refactor task/import permission paths

**Files:**
- Modify: `src/tasks/planning.rs`, `src/tasks/execution.rs`
- Modify: `src/db/traits/task_import.rs`

- [ ] **Step 1: Pass `AppContext` (or backend) into task planning**

Tasks currently grab a `DbPool` from the worker context. The worker should
also hold an `Arc<dyn PermissionBackend>` (constructed once at startup,
shared with the actix server). Update the worker bootstrap (find it in
`src/tasks/mod.rs` or wherever `ensure_task_worker_running` is defined)
to take and store the backend.

Then in `planning.rs` and `execution.rs`:

- Replace `apply_permissions_db` direct calls with
  `backend.apply_permissions(...)`.
- For Treetop-mode rejection of `namespace_permissions` task items:
  before calling `apply_permissions`, check `backend.supports_mutation()`;
  if false, fail the task item with a clear error message
  (`ApiError::NotImplemented("permission imports not supported in treetop mode")`).

- [ ] **Step 2: Pass the backend through `task_import` visibility queries**

In `src/db/traits/task_import.rs`, the visibility filter currently does a
SQL join against `permissions`. In Treetop mode, that table is irrelevant.
Refactor as follows:

```rust
async fn user_visible_tasks<C: BackendContext + ?Sized>(
    ctx: &C,
    principal: &PrincipalRef,
    page: &QueryOptions,
) -> Result<(Vec<HubuumTask>, i64), ApiError> {
    // 1. Load candidate tasks with non-permission filters applied.
    let candidates = load_candidate_tasks(ctx.db_pool(), page).await?;

    // 2. Build PermissionRequests for each candidate.
    let requests: Vec<PermissionRequest> = candidates
        .iter()
        .map(|t| PermissionRequest {
            resource: ResourceRef {
                kind: ResourceKind::Task,
                id: t.id,
                attrs: ResourceAttrs {
                    submitted_by: Some(t.submitted_by),
                    namespace_id: t.namespace_id,
                    ..Default::default()
                },
            },
            permissions: vec![Permissions::ReadCollection],
        })
        .collect();

    // 3. Filter via backend.
    let authorized = ctx
        .permission_backend()
        .authorize_candidates(principal, requests)
        .await?;

    let visible: Vec<HubuumTask> = candidates
        .into_iter()
        .zip(authorized)
        .filter_map(|(t, a)| if a.decision == PermissionDecision::Allow { Some(t) } else { None })
        .collect();

    let total = visible.len() as i64;
    Ok((visible, total))
}
```

(The `Permissions::ReadCollection` check on a Task resource is a placeholder
mapping; you may want to introduce a dedicated `Permissions::ReadTask`
variant — out of scope for this plan, document the decision in the
function's doc-comment.)

- [ ] **Step 3: Run the task tests**

Run: `cargo test --lib tasks -- --test-threads=1`
Expected: green.

- [ ] **Step 4: Commit**

```bash
git add src/tasks/ src/db/traits/task_import.rs
git commit -m "refactor(tasks): route task/import permission and visibility through PermissionBackend"
```

---

### Task 3.8: Remove the temporary `BackendContext for DbPool` shim

**Files:**
- Modify: `src/traits/context.rs`

- [ ] **Step 1: Delete the temporary impl**

Remove the `impl BackendContext for crate::db::DbPool { ... }` block added
in Task 1.3 Step 4.

- [ ] **Step 2: Verify build**

Run: `cargo build --lib`
Expected: succeeds. If anything still relies on `DbPool` as a permission
context, fix that call site to take `&AppContext` (or `&dyn BackendContext`
plus an `AppContext` argument from its caller).

- [ ] **Step 3: Run the full test suite**

Run: `cargo test --lib -- --test-threads=1`
Expected: green.

- [ ] **Step 4: Commit**

```bash
git add src/traits/context.rs
git commit -m "refactor(context): drop DbPool BackendContext shim; AppContext is required"
```

---

## Phase 4 — List/search/report visibility through the backend

### Task 4.1: Move list-permission helpers behind `authorize_candidates`

**Files:**
- Modify: `src/api/v1/handlers/templates.rs`

The `GET /templates` endpoint currently calls
`crate::models::namespace::user_can_on_any(&pool, user, Permissions::ReadTemplate)`.
That function still works in local mode (it queries the SQL `permissions`
table) but does not exist for Treetop. Route the lookup through the backend:

- [ ] **Step 1: Replace the call**

```rust
let principal = PrincipalRef::new(
    requestor.user.id,
    requestor.user.group_ids(ctx.db_pool()).await?,
);
let visible_namespaces = ctx
    .permission_backend()
    .namespaces_user_can(&principal, &[Permissions::ReadTemplate])
    .await?;
```

- [ ] **Step 2: Run the templates tests**

Run: `cargo test --lib tests::api::v1::templates -- --test-threads=1`
Expected: green.

- [ ] **Step 3: Commit**

```bash
git add src/api/v1/handlers/templates.rs
git commit -m "refactor(templates): use PermissionBackend::namespaces_user_can"
```

---

### Task 4.2: Refactor relation visibility (`src/db/traits/relations.rs:467`)

**Files:**
- Modify: `src/db/traits/relations.rs`

- [ ] **Step 1: Replace the direct `user_can_on_any` call**

The function at `src/db/traits/relations.rs:467` currently does:

```rust
let namespaces = user_can_on_any(pool, self, Permissions::ReadObject).await?;
```

Refactor the surrounding function to take `&dyn BackendContext` and use:

```rust
let principal = PrincipalRef::new(self.id(), self.group_ids(ctx.db_pool()).await?);
let namespaces = ctx
    .permission_backend()
    .namespaces_user_can(&principal, &[Permissions::ReadObject])
    .await?;
```

Update the function's callers (the `relations` handler files) to pass
`&ctx` accordingly.

- [ ] **Step 2: Verify**

Run: `cargo test --lib tests::api::v1::relations -- --test-threads=1`
Expected: green.

- [ ] **Step 3: Commit**

```bash
git add src/db/traits/relations.rs src/api/v1/handlers/relations.rs
git commit -m "refactor(relations): route ReadObject visibility through PermissionBackend"
```

---

### Task 4.3: Refactor `groups_on_paginated_with_total_count` callers

**Files:**
- Modify: `src/api/v1/handlers/classes.rs` (lines ~322, ~340, ~353)
- Modify: `src/api/v1/handlers/namespaces.rs` (line ~261)

- [ ] **Step 1: Replace direct calls with backend method**

```rust
let (rows, total) = ctx
    .permission_backend()
    .groups_with_permissions_on(namespace.id, &permissions_filter, &page)
    .await?;
```

`permissions_filter` is the `Vec<Permissions>` already built from query
params (see existing code).

- [ ] **Step 2: Verify**

Run: `cargo test --lib tests::api::v1::classes tests::api::v1::namespaces -- --test-threads=1`
Expected: green.

- [ ] **Step 3: Commit**

```bash
git add src/api/v1/handlers/classes.rs src/api/v1/handlers/namespaces.rs
git commit -m "refactor(handlers): use PermissionBackend::groups_with_permissions_on"
```

---

### Task 4.4: Refactor unified search and search visibility

**Files:**
- Modify: `src/db/traits/user/search.rs`
- Modify: `src/db/traits/user/unified_search.rs`
- Modify: `src/db/traits/user/membership.rs`
- Modify: `src/api/v1/handlers/search.rs`

The current search visibility is a SQL join between the result table and
`permissions`. In Treetop mode that join cannot run. Refactor to the
candidate-then-filter pattern:

- [ ] **Step 1: Add backend-aware visibility helper**

Create a helper in the relevant file:

```rust
async fn filter_visible<C: BackendContext + ?Sized, T: AuthzTargetable>(
    ctx: &C,
    principal: &PrincipalRef,
    candidates: Vec<T>,
    permission: Permissions,
) -> Result<Vec<T>, ApiError> {
    let mut requests = Vec::with_capacity(candidates.len());
    for c in &candidates {
        requests.push(PermissionRequest {
            resource: c.to_resource_ref(ctx.db_pool()).await?,
            permissions: vec![permission],
        });
    }
    let authorized = ctx
        .permission_backend()
        .authorize_candidates(principal, requests)
        .await?;
    Ok(candidates
        .into_iter()
        .zip(authorized)
        .filter_map(|(c, a)| if a.decision == PermissionDecision::Allow { Some(c) } else { None })
        .collect())
}
```

`AuthzTargetable` is just an alias for `AuthzTarget` — use the trait
directly if it's object-safe with the bound.

- [ ] **Step 2: Replace the SQL permission join**

Find the existing pattern (look for `inner_join(permissions...)` calls in
`src/db/traits/user/search.rs` and `unified_search.rs`). For each:

- Build the candidate query without the permission join.
- Apply non-permission filters.
- Materialize a chunk of candidates.
- Call `filter_visible(...)` to drop unauthorized rows.
- Continue paging until `limit + 1` rows are retained or candidates are
  exhausted.

This is mechanical but careful — the chunk size should be configurable
(default to e.g. 100). Document the algorithm at the top of the file.

- [ ] **Step 3: Verify exact total counts still match**

For accurate `X-Total-Count`, the implementation must enumerate **all**
candidates (not just the page), authorize each, and count allowed rows.
Add a `count_visible` companion that does this without materializing the
full row payload. (For local mode this can short-circuit by keeping the
SQL join — see Step 4.)

- [ ] **Step 4: Local-mode optimization**

`LocalPermissionBackend` can keep the SQL join for the common cases via
specialized methods. Add to `PermissionBackend`:

```rust
/// Optional fast-path: returns true if the backend can answer
/// `count_visible` without materializing candidates. Local can; Treetop
/// cannot.
fn supports_sql_visibility_join(&self) -> bool { false }
```

`LocalPermissionBackend` returns `true`; `TreetopPermissionBackend` returns
the default `false`. Search code branches:

- if `supports_sql_visibility_join()` → keep the SQL join path (existing
  code, lifted into a `LocalPermissionBackend` method),
- else → use the candidate-then-filter algorithm.

- [ ] **Step 5: Run search tests**

Run: `cargo test --lib tests::api::v1::search -- --test-threads=1`
Expected: green.

- [ ] **Step 6: Commit**

```bash
git add src/db/traits/user/ src/api/v1/handlers/search.rs src/permissions/backend.rs
git commit -m "refactor(search): route visibility filtering through PermissionBackend"
```

---

## Phase 5 — Treetop backend

### Task 5.1: Add the in-memory `MockTreetopBackend` test support

**Files:**
- Create: `src/permissions/test_support/mod.rs`
- Create: `src/permissions/test_support/mock_treetop.rs`

The mock evaluates Cedar-shaped policies against requests in memory. It is
NOT a real Cedar engine — it implements just enough to test mapping and
trait wiring. For real Cedar evaluation, the live-Treetop suite (Phase 7)
takes over.

- [ ] **Step 1: Define a tiny policy model**

`src/permissions/test_support/mock_treetop.rs`:

```rust
use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use crate::errors::ApiError;
use crate::models::{
    GroupPermission, Namespace, Permission, Permissions, PermissionsList, QueryOptions,
};
use crate::permissions::backend::PermissionBackend;
use crate::permissions::types::{
    AuthorizationResult, PermissionDecision, PermissionRequest, PrincipalRef, ResourceKind,
};

#[derive(Clone)]
pub struct MockPolicy {
    pub group_ids: Vec<i32>,
    pub action: Permissions,
    pub resource_kind: ResourceKind,
    pub resource_id: Option<i32>,         // None = any
    pub namespace_id: Option<i32>,        // attribute filter
}

pub struct MockTreetopBackend {
    policies: Mutex<Vec<MockPolicy>>,
}

impl MockTreetopBackend {
    pub fn new() -> Self {
        Self { policies: Mutex::new(Vec::new()) }
    }

    pub fn add_policy(&self, p: MockPolicy) {
        self.policies.lock().unwrap().push(p);
    }

    fn eval(&self, principal: &PrincipalRef, req: &PermissionRequest) -> PermissionDecision {
        let policies = self.policies.lock().unwrap();
        let all_permitted = req.permissions.iter().all(|requested_perm| {
            policies.iter().any(|p| {
                p.group_ids.iter().any(|g| principal.group_ids.contains(g))
                    && p.action == *requested_perm
                    && p.resource_kind == req.resource.kind
                    && p.resource_id.map_or(true, |rid| rid == req.resource.id)
                    && p.namespace_id.map_or(true, |nid| {
                        req.resource.namespace_id() == Some(nid)
                    })
            })
        });
        if all_permitted { PermissionDecision::Allow } else { PermissionDecision::Deny }
    }
}

#[async_trait]
impl PermissionBackend for MockTreetopBackend {
    async fn authorize(
        &self,
        principal: &PrincipalRef,
        request: PermissionRequest,
    ) -> Result<PermissionDecision, ApiError> {
        Ok(self.eval(principal, &request))
    }

    async fn authorize_many(
        &self,
        principal: &PrincipalRef,
        requests: Vec<PermissionRequest>,
    ) -> Result<Vec<PermissionDecision>, ApiError> {
        Ok(requests.into_iter().map(|r| self.eval(principal, &r)).collect())
    }

    async fn authorize_candidates(
        &self,
        principal: &PrincipalRef,
        requests: Vec<PermissionRequest>,
    ) -> Result<Vec<AuthorizationResult>, ApiError> {
        let decisions = self.authorize_many(principal, requests.clone()).await?;
        Ok(requests
            .into_iter()
            .zip(decisions)
            .map(|(request, decision)| AuthorizationResult { request, decision })
            .collect())
    }

    async fn namespaces_user_can(
        &self,
        _principal: &PrincipalRef,
        _permissions: &[Permissions],
    ) -> Result<Vec<Namespace>, ApiError> {
        Err(ApiError::NotImplemented(
            "MockTreetopBackend does not enumerate namespaces; build via a real DB".to_string(),
        ))
    }

    async fn groups_with_permissions_on(
        &self,
        _namespace_id: i32,
        _permissions_filter: &[Permissions],
        _page: &QueryOptions,
    ) -> Result<(Vec<GroupPermission>, i64), ApiError> {
        Err(ApiError::NotImplemented(
            "MockTreetopBackend does not enumerate groups".to_string(),
        ))
    }

    async fn group_permission_on(
        &self,
        _namespace_id: i32,
        _group_id: i32,
    ) -> Result<Option<Permission>, ApiError> {
        Err(ApiError::NotImplemented("not supported by mock".to_string()))
    }

    async fn apply_permissions(
        &self,
        _namespace_id: i32,
        _group_id: i32,
        _list: PermissionsList<Permissions>,
        _replace_existing: bool,
    ) -> Result<Permission, ApiError> {
        Err(ApiError::NotImplemented(
            "permission mutations are managed out-of-band".to_string(),
        ))
    }

    async fn revoke_permissions(
        &self,
        _namespace_id: i32,
        _group_id: i32,
        _list: PermissionsList<Permissions>,
    ) -> Result<Permission, ApiError> {
        Err(ApiError::NotImplemented(
            "permission mutations are managed out-of-band".to_string(),
        ))
    }

    async fn revoke_all(&self, _namespace_id: i32, _group_id: i32) -> Result<(), ApiError> {
        Err(ApiError::NotImplemented(
            "permission mutations are managed out-of-band".to_string(),
        ))
    }

    fn supports_mutation(&self) -> bool {
        false
    }

    fn kind(&self) -> &'static str {
        "mock-treetop"
    }
}
```

`src/permissions/test_support/mod.rs`:

```rust
pub mod mock_treetop;
pub use mock_treetop::{MockPolicy, MockTreetopBackend};
```

- [ ] **Step 2: Verify build**

Run: `cargo build --lib --tests`
Expected: succeeds.

- [ ] **Step 3: Commit**

```bash
git add src/permissions/test_support/
git commit -m "test(permissions): add MockTreetopBackend for mapping + wiring tests"
```

---

### Task 5.2: Implement `Permissions` ↔ Cedar mapping

**Files:**
- Create: `src/permissions/treetop/mapping.rs`
- Create: `src/permissions/treetop/mod.rs` (skeleton)

- [ ] **Step 1: Skeleton for the treetop module**

`src/permissions/treetop/mod.rs`:

```rust
pub mod error;
pub mod mapping;

use std::time::Duration;

use async_trait::async_trait;
use chrono::Utc;
use treetop_client::{Action, Client, Request, Resource, User};

use crate::config::AppConfig;
use crate::db::DbPool;
use crate::errors::ApiError;
use crate::models::{
    GroupPermission, Namespace, Permission, Permissions, PermissionsList, QueryOptions,
};

use super::backend::PermissionBackend;
use super::types::{
    AuthorizationResult, PermissionDecision, PermissionRequest, PrincipalRef, ResourceKind,
};

pub struct TreetopPermissionBackend {
    client: Client,
    pool: DbPool,
}

impl TreetopPermissionBackend {
    pub async fn connect(
        url: &str,
        cfg: &AppConfig,
        pool: DbPool,
    ) -> Result<Self, ApiError> {
        let mut builder = Client::builder(url);
        builder = builder
            .connect_timeout(Duration::from_millis(cfg.treetop_connect_timeout_ms))
            .request_timeout(Duration::from_millis(cfg.treetop_request_timeout_ms));
        if let Some(ca) = cfg.treetop_ca_cert.as_deref() {
            builder = builder.with_ca_cert_path(ca);
        }
        if cfg.treetop_accept_invalid_certs {
            builder = builder.danger_accept_invalid_certs(true);
        }
        let client = builder
            .build()
            .map_err(|e| ApiError::PermissionBackendUnavailable(format!("treetop client build: {e}")))?;
        client
            .health()
            .await
            .map_err(|e| ApiError::PermissionBackendUnavailable(format!("treetop health: {e}")))?;
        Ok(Self { client, pool })
    }
}

// PermissionBackend impl is added in subsequent tasks.
```

(Adapt method names — `with_ca_cert_path`, `danger_accept_invalid_certs`,
`connect_timeout`, `request_timeout` — to whatever the live `treetop-client`
exposes. If the API differs, follow the upstream README and adjust here.
**Do not invent methods.**)

- [ ] **Step 2: Implement the mapping table**

`src/permissions/treetop/mapping.rs`:

```rust
use treetop_client::{Action, AttrValue, Resource};

use crate::models::Permissions;

use crate::permissions::types::{ResourceKind, ResourceRef};

const TYPE_SYSTEM: &str = "HubuumSystem";
const TYPE_NAMESPACE: &str = "HubuumNamespace";
const TYPE_CLASS: &str = "HubuumClass";
const TYPE_OBJECT: &str = "HubuumObject";
const TYPE_CLASS_RELATION: &str = "HubuumClassRelation";
const TYPE_OBJECT_RELATION: &str = "HubuumObjectRelation";
const TYPE_TEMPLATE: &str = "HubuumTemplate";
const TYPE_TASK: &str = "HubuumTask";

pub fn permission_to_action(perm: Permissions) -> Action {
    Action::new(perm.to_string()) // Permissions::Display already produces stable PascalCase IDs
}

pub fn resource_to_treetop(r: &ResourceRef) -> Resource {
    let (type_name, id_str) = match r.kind {
        ResourceKind::System => (TYPE_SYSTEM, "global".to_string()),
        ResourceKind::Namespace => (TYPE_NAMESPACE, r.id.to_string()),
        ResourceKind::Class => (TYPE_CLASS, r.id.to_string()),
        ResourceKind::Object => (TYPE_OBJECT, r.id.to_string()),
        ResourceKind::ClassRelation => (TYPE_CLASS_RELATION, r.id.to_string()),
        ResourceKind::ObjectRelation => (TYPE_OBJECT_RELATION, r.id.to_string()),
        ResourceKind::Template => (TYPE_TEMPLATE, r.id.to_string()),
        ResourceKind::Task => (TYPE_TASK, r.id.to_string()),
    };
    let mut resource = Resource::new(type_name, id_str);
    if let Some(nid) = r.attrs.namespace_id {
        resource = resource.with_attr("namespace_id", AttrValue::Long(nid as i64));
    }
    if let Some(cid) = r.attrs.class_id {
        resource = resource.with_attr("class_id", AttrValue::Long(cid as i64));
    }
    if let Some(from) = r.attrs.from_namespace_id {
        resource = resource.with_attr("from_namespace_id", AttrValue::Long(from as i64));
    }
    if let Some(to) = r.attrs.to_namespace_id {
        resource = resource.with_attr("to_namespace_id", AttrValue::Long(to as i64));
    }
    if let Some(sb) = r.attrs.submitted_by {
        resource = resource.with_attr("submitted_by", AttrValue::Long(sb as i64));
    }
    if let Some(ref name) = r.attrs.name {
        resource = resource.with_attr("name", AttrValue::String(name.clone()));
    }
    resource
}
```

- [ ] **Step 3: Unit-test the mapping**

Append to `src/permissions/treetop/mapping.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::permissions::types::ResourceAttrs;

    #[test]
    fn read_object_action_uses_pascal_case_name() {
        let a = permission_to_action(Permissions::ReadObject);
        assert_eq!(a.name(), "ReadObject");
    }

    #[test]
    fn class_resource_carries_namespace_attr() {
        let r = ResourceRef {
            kind: ResourceKind::Class,
            id: 45,
            attrs: ResourceAttrs { namespace_id: Some(7), ..Default::default() },
        };
        let res = resource_to_treetop(&r);
        assert_eq!(res.kind(), "HubuumClass");
        assert_eq!(res.id(), "45");
        // The exact attribute introspection API depends on treetop_client;
        // adapt the assertion to use the available accessor.
        assert!(res.has_attr("namespace_id"));
    }
}
```

(`Action::name()`, `Resource::kind()`, `Resource::id()`, `Resource::has_attr(...)`
are placeholders — match the actual public API of `treetop-client`. If those
accessors don't exist, switch the assertions to the closest available form,
e.g. constructing the `Resource` and checking it round-trips through
`serde_json` to the expected JSON shape.)

- [ ] **Step 4: Verify build**

Run: `cargo build --lib --features permissions-treetop`
Expected: succeeds (assuming the treetop-client API matches the placeholder
methods; iterate as needed).

- [ ] **Step 5: Commit**

```bash
git add src/permissions/treetop/
git commit -m "feat(permissions/treetop): add mapping table for Permissions and ResourceRef"
```

---

### Task 5.3: Implement `PermissionBackend` for `TreetopPermissionBackend`

**Files:**
- Modify: `src/permissions/treetop/mod.rs`
- Create: `src/permissions/treetop/error.rs`

- [ ] **Step 1: Implement the error converter**

`src/permissions/treetop/error.rs`:

```rust
use crate::errors::ApiError;

/// Convert a treetop-client error into an ApiError. Transport / 5xx /
/// malformed → PermissionBackendUnavailable; validation → InternalServerError
/// unless a clearer mapping exists.
pub fn to_api_error(err: treetop_client::TreetopError) -> ApiError {
    use treetop_client::TreetopError::*;
    match err {
        Transport(e) => ApiError::PermissionBackendUnavailable(format!("transport: {e}")),
        ServerError(status, body) if status >= 500 => {
            ApiError::PermissionBackendUnavailable(format!("treetop {status}: {body}"))
        }
        ServerError(status, body) => {
            ApiError::InternalServerError(format!("treetop {status}: {body}"))
        }
        Decode(e) => ApiError::PermissionBackendUnavailable(format!("decode: {e}")),
        Validation(e) => ApiError::InternalServerError(format!("validation: {e}")),
        // Add other variants as the upstream type evolves.
        other => ApiError::InternalServerError(format!("treetop: {other}")),
    }
}
```

(Match this against the actual `treetop_client::TreetopError` variants.
If the variant names differ, update accordingly.)

- [ ] **Step 2: Implement the trait**

In `src/permissions/treetop/mod.rs`, append:

```rust
use treetop_client::{AuthorizeRequest, Group};
use crate::permissions::local::queries; // reuse SQL helpers for inventory queries

#[async_trait]
impl PermissionBackend for TreetopPermissionBackend {
    async fn authorize(
        &self,
        principal: &PrincipalRef,
        request: PermissionRequest,
    ) -> Result<PermissionDecision, ApiError> {
        let user = self.user_for(principal);
        let mut batch = AuthorizeRequest::new();
        for perm in &request.permissions {
            batch = batch.add_request(treetop_client::Request::new(
                user.clone(),
                mapping::permission_to_action(*perm),
                mapping::resource_to_treetop(&request.resource),
            ));
        }
        let resp = self.client.authorize(&batch).await.map_err(error::to_api_error)?;
        let all_allowed = resp.decisions().iter().all(|d| d.is_allow());
        Ok(if all_allowed { PermissionDecision::Allow } else { PermissionDecision::Deny })
    }

    async fn authorize_many(
        &self,
        principal: &PrincipalRef,
        requests: Vec<PermissionRequest>,
    ) -> Result<Vec<PermissionDecision>, ApiError> {
        let user = self.user_for(principal);
        let mut batch = AuthorizeRequest::new();
        let mut spans = Vec::new(); // (start_index, count) per request
        let mut idx = 0;
        for req in &requests {
            spans.push((idx, req.permissions.len()));
            for perm in &req.permissions {
                batch = batch.add_request(treetop_client::Request::new(
                    user.clone(),
                    mapping::permission_to_action(*perm),
                    mapping::resource_to_treetop(&req.resource),
                ));
                idx += 1;
            }
        }
        let resp = self.client.authorize(&batch).await.map_err(error::to_api_error)?;
        let decisions = resp.decisions();
        Ok(spans
            .into_iter()
            .map(|(start, count)| {
                let allowed = (start..start + count).all(|i| decisions[i].is_allow());
                if allowed { PermissionDecision::Allow } else { PermissionDecision::Deny }
            })
            .collect())
    }

    async fn authorize_candidates(
        &self,
        principal: &PrincipalRef,
        requests: Vec<PermissionRequest>,
    ) -> Result<Vec<AuthorizationResult>, ApiError> {
        let decisions = self.authorize_many(principal, requests.clone()).await?;
        Ok(requests
            .into_iter()
            .zip(decisions)
            .map(|(request, decision)| AuthorizationResult { request, decision })
            .collect())
    }

    async fn namespaces_user_can(
        &self,
        principal: &PrincipalRef,
        permissions: &[Permissions],
    ) -> Result<Vec<Namespace>, ApiError> {
        // Enumerate from local DB, filter via Treetop.
        let all_namespaces = queries::all_namespaces(&self.pool).await?;
        let requests: Vec<PermissionRequest> = all_namespaces
            .iter()
            .map(|ns| PermissionRequest {
                resource: crate::permissions::types::ResourceRef::namespace(ns.id),
                permissions: permissions.to_vec(),
            })
            .collect();
        let authorized = self.authorize_candidates(principal, requests).await?;
        Ok(all_namespaces
            .into_iter()
            .zip(authorized)
            .filter_map(|(ns, a)| if a.decision == PermissionDecision::Allow { Some(ns) } else { None })
            .collect())
    }

    async fn groups_with_permissions_on(
        &self,
        namespace_id: i32,
        permissions_filter: &[Permissions],
        page: &QueryOptions,
    ) -> Result<(Vec<GroupPermission>, i64), ApiError> {
        // Enumerate groups × permissions, batch-authorize, project rows.
        let all_groups = queries::all_groups(&self.pool).await?;
        let all_perms: Vec<Permissions> = if permissions_filter.is_empty() {
            // 24 boolean slots — enumerate all variants
            crate::models::Permissions::all_variants()
        } else {
            permissions_filter.to_vec()
        };

        let mut rows: Vec<GroupPermission> = Vec::new();
        for group in &all_groups {
            let principal = PrincipalRef { user_id: 0, group_ids: vec![group.id] };
            let requests: Vec<PermissionRequest> = all_perms
                .iter()
                .map(|p| PermissionRequest {
                    resource: crate::permissions::types::ResourceRef::namespace(namespace_id),
                    permissions: vec![*p],
                })
                .collect();
            let decisions = self.authorize_many(&principal, requests).await?;
            let permission = synthesize_permission_row(namespace_id, group.id, &all_perms, &decisions);
            if permission_has_any_true(&permission) {
                rows.push(GroupPermission { group: group.clone(), permission });
            }
        }

        // Apply page/sort here. Mirror the LocalPermissionBackend pagination.
        let total = rows.len() as i64;
        apply_in_memory_pagination(&mut rows, page);
        Ok((rows, total))
    }

    async fn group_permission_on(
        &self,
        namespace_id: i32,
        group_id: i32,
    ) -> Result<Option<Permission>, ApiError> {
        let principal = PrincipalRef { user_id: 0, group_ids: vec![group_id] };
        let all_perms = crate::models::Permissions::all_variants();
        let requests: Vec<PermissionRequest> = all_perms
            .iter()
            .map(|p| PermissionRequest {
                resource: crate::permissions::types::ResourceRef::namespace(namespace_id),
                permissions: vec![*p],
            })
            .collect();
        let decisions = self.authorize_many(&principal, requests).await?;
        let permission = synthesize_permission_row(namespace_id, group_id, &all_perms, &decisions);
        if permission_has_any_true(&permission) {
            Ok(Some(permission))
        } else {
            Ok(None)
        }
    }

    async fn apply_permissions(
        &self,
        _namespace_id: i32,
        _group_id: i32,
        _list: PermissionsList<Permissions>,
        _replace_existing: bool,
    ) -> Result<Permission, ApiError> {
        Err(ApiError::NotImplemented(
            "permission mutations are managed out-of-band when using the treetop backend".to_string(),
        ))
    }

    async fn revoke_permissions(
        &self,
        _namespace_id: i32,
        _group_id: i32,
        _list: PermissionsList<Permissions>,
    ) -> Result<Permission, ApiError> {
        Err(ApiError::NotImplemented(
            "permission mutations are managed out-of-band when using the treetop backend".to_string(),
        ))
    }

    async fn revoke_all(&self, _namespace_id: i32, _group_id: i32) -> Result<(), ApiError> {
        Err(ApiError::NotImplemented(
            "permission mutations are managed out-of-band when using the treetop backend".to_string(),
        ))
    }

    fn supports_mutation(&self) -> bool {
        false
    }

    fn kind(&self) -> &'static str {
        "treetop"
    }
}

impl TreetopPermissionBackend {
    fn user_for(&self, p: &PrincipalRef) -> User {
        let mut u = User::new(p.user_id.to_string());
        for gid in &p.group_ids {
            u = u.with_group(Group::new(gid.to_string()));
        }
        u
    }
}

fn synthesize_permission_row(
    namespace_id: i32,
    group_id: i32,
    perms: &[Permissions],
    decisions: &[PermissionDecision],
) -> Permission {
    let now = Utc::now().naive_utc();
    let mut p = Permission {
        id: 0,
        namespace_id,
        group_id,
        has_read_namespace: false,
        has_update_namespace: false,
        has_delete_namespace: false,
        has_delegate_namespace: false,
        has_create_class: false,
        has_read_class: false,
        has_update_class: false,
        has_delete_class: false,
        has_create_object: false,
        has_read_object: false,
        has_update_object: false,
        has_delete_object: false,
        has_create_class_relation: false,
        has_read_class_relation: false,
        has_update_class_relation: false,
        has_delete_class_relation: false,
        has_create_object_relation: false,
        has_read_object_relation: false,
        has_update_object_relation: false,
        has_delete_object_relation: false,
        has_read_template: false,
        has_create_template: false,
        has_update_template: false,
        has_delete_template: false,
        created_at: now,
        updated_at: now,
    };
    for (perm, decision) in perms.iter().zip(decisions) {
        if *decision == PermissionDecision::Allow {
            set_permission_field(&mut p, *perm, true);
        }
    }
    p
}

fn set_permission_field(p: &mut Permission, perm: Permissions, value: bool) {
    use Permissions::*;
    match perm {
        ReadCollection => p.has_read_namespace = value,
        UpdateCollection => p.has_update_namespace = value,
        DeleteCollection => p.has_delete_namespace = value,
        DelegateCollection => p.has_delegate_namespace = value,
        CreateClass => p.has_create_class = value,
        ReadClass => p.has_read_class = value,
        UpdateClass => p.has_update_class = value,
        DeleteClass => p.has_delete_class = value,
        CreateObject => p.has_create_object = value,
        ReadObject => p.has_read_object = value,
        UpdateObject => p.has_update_object = value,
        DeleteObject => p.has_delete_object = value,
        CreateClassRelation => p.has_create_class_relation = value,
        ReadClassRelation => p.has_read_class_relation = value,
        UpdateClassRelation => p.has_update_class_relation = value,
        DeleteClassRelation => p.has_delete_class_relation = value,
        CreateObjectRelation => p.has_create_object_relation = value,
        ReadObjectRelation => p.has_read_object_relation = value,
        UpdateObjectRelation => p.has_update_object_relation = value,
        DeleteObjectRelation => p.has_delete_object_relation = value,
        ReadTemplate => p.has_read_template = value,
        CreateTemplate => p.has_create_template = value,
        UpdateTemplate => p.has_update_template = value,
        DeleteTemplate => p.has_delete_template = value,
    }
}

fn permission_has_any_true(p: &Permission) -> bool {
    p.has_read_namespace || p.has_update_namespace || p.has_delete_namespace ||
    p.has_delegate_namespace || p.has_create_class || p.has_read_class ||
    p.has_update_class || p.has_delete_class || p.has_create_object ||
    p.has_read_object || p.has_update_object || p.has_delete_object ||
    p.has_create_class_relation || p.has_read_class_relation ||
    p.has_update_class_relation || p.has_delete_class_relation ||
    p.has_create_object_relation || p.has_read_object_relation ||
    p.has_update_object_relation || p.has_delete_object_relation ||
    p.has_read_template || p.has_create_template ||
    p.has_update_template || p.has_delete_template
}

fn apply_in_memory_pagination(_rows: &mut Vec<GroupPermission>, _page: &QueryOptions) {
    // TODO: implement once the QueryOptions surface is reviewed; for now,
    // return all rows. The Treetop reverse-query path is acknowledged in the
    // spec as slower, and full pagination semantics will be added in a
    // follow-up commit before Phase 5 is considered complete.
}
```

Add the supporting helpers to `LocalPermissionBackend::queries`:

```rust
pub(crate) async fn all_namespaces(pool: &DbPool) -> Result<Vec<Namespace>, ApiError> {
    crate::db::with_connection(pool, |conn| {
        crate::schema::namespaces::table.load::<Namespace>(conn)
    })
    .map_err(Into::into)
}

pub(crate) async fn all_groups(pool: &DbPool) -> Result<Vec<Group>, ApiError> {
    crate::db::with_connection(pool, |conn| {
        crate::schema::groups::table.load::<Group>(conn)
    })
    .map_err(Into::into)
}
```

Add the all-variants accessor on `Permissions`:

```rust
impl Permissions {
    pub fn all_variants() -> Vec<Self> {
        vec![
            Permissions::ReadCollection, Permissions::UpdateCollection,
            Permissions::DeleteCollection, Permissions::DelegateCollection,
            Permissions::CreateClass, Permissions::ReadClass,
            Permissions::UpdateClass, Permissions::DeleteClass,
            Permissions::CreateObject, Permissions::ReadObject,
            Permissions::UpdateObject, Permissions::DeleteObject,
            Permissions::CreateClassRelation, Permissions::ReadClassRelation,
            Permissions::UpdateClassRelation, Permissions::DeleteClassRelation,
            Permissions::CreateObjectRelation, Permissions::ReadObjectRelation,
            Permissions::UpdateObjectRelation, Permissions::DeleteObjectRelation,
            Permissions::ReadTemplate, Permissions::CreateTemplate,
            Permissions::UpdateTemplate, Permissions::DeleteTemplate,
        ]
    }
}
```

The `apply_in_memory_pagination` placeholder above is the one allowed
exception to the no-placeholder rule, with an explicit follow-up commit
required before Phase 5 closes. Convert the placeholder to a real
implementation that mirrors `groups_on_paginated_with_total_count_query`'s
sort and limit/offset behavior over `Vec<GroupPermission>` before merging.

- [ ] **Step 3: Verify build**

Run: `cargo build --lib --features permissions-treetop`
Expected: succeeds. (If treetop-client API method names differ, fix here.)

- [ ] **Step 4: Commit**

```bash
git add src/permissions/treetop/ src/permissions/local/queries.rs src/models/permissions.rs
git commit -m "feat(permissions/treetop): implement PermissionBackend trait via treetop-client"
```

---

### Task 5.4: Trait-level test against the mock backend

**Files:**
- Modify: `src/tests/permissions/backend_trait.rs`

- [ ] **Step 1: Add the mock test**

```rust
#[actix_web::test]
async fn mock_treetop_authorizes_per_policy() {
    use crate::permissions::test_support::{MockPolicy, MockTreetopBackend};
    use crate::permissions::types::ResourceKind;

    let backend = MockTreetopBackend::new();
    backend.add_policy(MockPolicy {
        group_ids: vec![100],
        action: Permissions::ReadCollection,
        resource_kind: ResourceKind::Namespace,
        resource_id: None,
        namespace_id: Some(7),
    });

    let principal = PrincipalRef { user_id: 1, group_ids: vec![100] };
    let req = PermissionRequest {
        resource: ResourceRef::namespace(7),
        permissions: vec![Permissions::ReadCollection],
    };

    assert_eq!(backend.authorize(&principal, req.clone()).await.unwrap(), PermissionDecision::Allow);

    // Different namespace → deny.
    let req8 = PermissionRequest {
        resource: ResourceRef::namespace(8),
        permissions: vec![Permissions::ReadCollection],
    };
    assert_eq!(backend.authorize(&principal, req8).await.unwrap(), PermissionDecision::Deny);

    // Two-permission AND requires both — only one granted.
    let req_two = PermissionRequest {
        resource: ResourceRef::namespace(7),
        permissions: vec![Permissions::ReadCollection, Permissions::UpdateCollection],
    };
    assert_eq!(backend.authorize(&principal, req_two).await.unwrap(), PermissionDecision::Deny);
}
```

- [ ] **Step 2: Run**

Run: `cargo test --lib tests::permissions::backend_trait::mock_treetop_authorizes_per_policy`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add src/tests/permissions/backend_trait.rs
git commit -m "test(permissions): mock-treetop AND semantics and resource scoping"
```

---

## Phase 6 — SQL → Cedar exporter

### Task 6.1: Add `export-permissions` subcommand to `hubuum-admin`

**Files:**
- Convert `src/bin/admin.rs` into a small dispatcher
- Create: `src/bin/admin/export.rs`

- [ ] **Step 1: Move existing logic into the dispatcher**

Restructure `src/bin/admin.rs` to use clap subcommands:

```rust
use clap::{Parser, Subcommand};

#[derive(Parser)]
struct AdminCli {
    #[command(subcommand)]
    command: AdminCommand,

    #[arg(long, env = "HUBUUM_DATABASE_URL")]
    database_url: Option<String>,

    #[arg(long, env = "HUBUUM_LOG_LEVEL", default_value = "info")]
    log_level: String,
}

#[derive(Subcommand)]
enum AdminCommand {
    /// Reset the password for the specified username
    ResetPassword { username: String },

    /// Export current SQL permissions as a Cedar policy bundle
    ExportPermissions {
        /// Output format
        #[arg(long, value_enum, default_value_t = ExportFormat::Cedar)]
        as_: ExportFormat,
    },
}

#[derive(clap::ValueEnum, Clone, Copy)]
enum ExportFormat { Cedar }
```

In `main`, dispatch:

```rust
match cli.command {
    AdminCommand::ResetPassword { username } => reset_password(pool, &username).await?,
    AdminCommand::ExportPermissions { as_: ExportFormat::Cedar } => export::export_cedar(pool).await?,
}
```

- [ ] **Step 2: Implement the exporter**

`src/bin/admin/export.rs`:

```rust
use diesel::prelude::*;

use hubuum::db::{DbPool, with_connection};
use hubuum::errors::ApiError;
use hubuum::models::{Group, Namespace, Permission};
use hubuum::schema::{groups, namespaces, permissions as perms_schema};

pub async fn export_cedar(pool: DbPool) -> Result<(), ApiError> {
    let rows: Vec<(Permission, Group, Namespace)> = with_connection(&pool, |conn| {
        perms_schema::table
            .inner_join(groups::table.on(perms_schema::group_id.eq(groups::id)))
            .inner_join(namespaces::table.on(perms_schema::namespace_id.eq(namespaces::id)))
            .load::<(Permission, Group, Namespace)>(conn)
    })?;

    println!("// Generated by `hubuum-admin export-permissions --as cedar`");
    println!("// One permit clause per non-empty (group, namespace, resource_type) bucket.");
    println!();

    for (perm, group, namespace) in &rows {
        emit_namespace_actions(perm, group, namespace);
        emit_class_actions(perm, group, namespace);
        emit_object_actions(perm, group, namespace);
        emit_class_relation_actions(perm, group, namespace);
        emit_object_relation_actions(perm, group, namespace);
        emit_template_actions(perm, group, namespace);
    }

    Ok(())
}

fn emit_namespace_actions(p: &Permission, g: &Group, n: &Namespace) {
    let mut actions = Vec::new();
    if p.has_read_namespace { actions.push("ReadCollection"); }
    if p.has_update_namespace { actions.push("UpdateCollection"); }
    if p.has_delete_namespace { actions.push("DeleteCollection"); }
    if p.has_delegate_namespace { actions.push("DelegateCollection"); }
    if actions.is_empty() { return; }
    println!(
        "// row id={} group={} ({}) namespace={} ({})",
        p.id, g.id, g.groupname, n.id, n.name
    );
    println!("permit(");
    println!("    principal in Group::\"{}\",", g.id);
    println!("    action in [{}],", actions.iter().map(|a| format!("Action::\"{a}\"")).collect::<Vec<_>>().join(", "));
    println!("    resource");
    println!(") when {{");
    println!("    resource is HubuumNamespace && resource == HubuumNamespace::\"{}\"", n.id);
    println!("}};");
    println!();
}

fn emit_class_actions(p: &Permission, g: &Group, n: &Namespace) {
    let mut actions = Vec::new();
    if p.has_read_class { actions.push("ReadClass"); }
    if p.has_update_class { actions.push("UpdateClass"); }
    if p.has_delete_class { actions.push("DeleteClass"); }
    if p.has_create_class { actions.push("CreateClass"); }
    if actions.is_empty() { return; }
    println!("// row id={} group={} namespace={}", p.id, g.id, n.id);
    println!("permit(");
    println!("    principal in Group::\"{}\",", g.id);
    println!("    action in [{}],",
        actions.iter().map(|a| format!("Action::\"{a}\"")).collect::<Vec<_>>().join(", "));
    println!("    resource");
    println!(") when {{");
    println!("    resource is HubuumClass && resource.namespace_id == {}", n.id);
    println!("}};");
    println!();
}

fn emit_object_actions(p: &Permission, g: &Group, n: &Namespace) {
    let mut actions = Vec::new();
    if p.has_read_object { actions.push("ReadObject"); }
    if p.has_update_object { actions.push("UpdateObject"); }
    if p.has_delete_object { actions.push("DeleteObject"); }
    if p.has_create_object { actions.push("CreateObject"); }
    if actions.is_empty() { return; }
    println!("// row id={} group={} namespace={}", p.id, g.id, n.id);
    println!("permit(");
    println!("    principal in Group::\"{}\",", g.id);
    println!("    action in [{}],",
        actions.iter().map(|a| format!("Action::\"{a}\"")).collect::<Vec<_>>().join(", "));
    println!("    resource");
    println!(") when {{");
    println!("    resource is HubuumObject && resource.namespace_id == {}", n.id);
    println!("}};");
    println!();
}

fn emit_class_relation_actions(p: &Permission, g: &Group, n: &Namespace) {
    let mut actions = Vec::new();
    if p.has_read_class_relation { actions.push("ReadClassRelation"); }
    if p.has_update_class_relation { actions.push("UpdateClassRelation"); }
    if p.has_delete_class_relation { actions.push("DeleteClassRelation"); }
    if p.has_create_class_relation { actions.push("CreateClassRelation"); }
    if actions.is_empty() { return; }
    println!("// row id={} group={} namespace={}", p.id, g.id, n.id);
    println!("permit(");
    println!("    principal in Group::\"{}\",", g.id);
    println!("    action in [{}],",
        actions.iter().map(|a| format!("Action::\"{a}\"")).collect::<Vec<_>>().join(", "));
    println!("    resource");
    println!(") when {{");
    println!("    resource is HubuumClassRelation && resource.namespace_id == {}", n.id);
    println!("}};");
    println!();
}

fn emit_object_relation_actions(p: &Permission, g: &Group, n: &Namespace) {
    let mut actions = Vec::new();
    if p.has_read_object_relation { actions.push("ReadObjectRelation"); }
    if p.has_update_object_relation { actions.push("UpdateObjectRelation"); }
    if p.has_delete_object_relation { actions.push("DeleteObjectRelation"); }
    if p.has_create_object_relation { actions.push("CreateObjectRelation"); }
    if actions.is_empty() { return; }
    println!("// row id={} group={} namespace={}", p.id, g.id, n.id);
    println!("permit(");
    println!("    principal in Group::\"{}\",", g.id);
    println!("    action in [{}],",
        actions.iter().map(|a| format!("Action::\"{a}\"")).collect::<Vec<_>>().join(", "));
    println!("    resource");
    println!(") when {{");
    println!("    resource is HubuumObjectRelation && resource.namespace_id == {}", n.id);
    println!("}};");
    println!();
}

fn emit_template_actions(p: &Permission, g: &Group, n: &Namespace) {
    let mut actions = Vec::new();
    if p.has_read_template { actions.push("ReadTemplate"); }
    if p.has_create_template { actions.push("CreateTemplate"); }
    if p.has_update_template { actions.push("UpdateTemplate"); }
    if p.has_delete_template { actions.push("DeleteTemplate"); }
    if actions.is_empty() { return; }
    println!("// row id={} group={} namespace={}", p.id, g.id, n.id);
    println!("permit(");
    println!("    principal in Group::\"{}\",", g.id);
    println!("    action in [{}],",
        actions.iter().map(|a| format!("Action::\"{a}\"")).collect::<Vec<_>>().join(", "));
    println!("    resource");
    println!(") when {{");
    println!("    resource is HubuumTemplate && resource.namespace_id == {}", n.id);
    println!("}};");
    println!();
}

// Note on Create* permissions: in the runtime backend mapping (Phase 5),
// CreateClass / CreateObject / CreateTemplate / CreateClassRelation /
// CreateObjectRelation are issued as authorization requests against the
// CHILD resource type (e.g. resource = HubuumClass when checking CreateClass).
// That keeps the export and the runtime mapping aligned: a `permit ...
// resource is HubuumClass` policy authorizes both ReadClass on an existing
// class AND CreateClass when the action implies creating one. If you change
// the runtime mapping, change the exporter at the same time.
```

Implement the remaining `emit_*_actions` functions following the same
pattern.

- [ ] **Step 3: Smoke test**

```bash
HUBUUM_DATABASE_URL=postgres://localhost/hubuum_test cargo run --bin hubuum-admin -- export-permissions --as cedar | head -40
```

Expected: a Cedar policy file is emitted on stdout.

- [ ] **Step 4: Commit**

```bash
git add src/bin/admin.rs src/bin/admin/export.rs
git commit -m "feat(admin): add export-permissions --as cedar subcommand"
```

---

### Task 6.2: Round-trip test for the exporter

**Files:**
- Create: `src/tests/permissions/translator.rs`
- Modify: `src/tests/permissions/mod.rs`

- [ ] **Step 1: Wire the test module**

Add `pub mod translator;` to `src/tests/permissions/mod.rs`.

- [ ] **Step 2: Write the failing test**

```rust
use crate::tests::common::*;

#[actix_web::test]
async fn exported_policies_grant_same_decisions_as_sql() {
    let pool = setup_pool().await;

    // Seed: alice in mathematics-administrators with read+update on namespace 7.
    let alice = create_user(&pool, "alice").await;
    let admins = create_group(&pool, "mathematics-administrators").await;
    add_user_to_group(&pool, &alice, &admins).await;
    let math_ns = create_namespace(&pool, "mathematics").await;
    grant_permissions(&pool, math_ns.id, admins.id, &[
        Permissions::ReadCollection,
        Permissions::UpdateCollection,
    ]).await;

    // Run the exporter and parse the output into MockPolicy entries.
    let cedar_text = run_export_cedar(pool.clone()).await;
    let mock = MockTreetopBackend::new();
    parse_cedar_into_mock(&cedar_text, &mock);

    // Same principal, same request — both backends agree.
    let principal = PrincipalRef { user_id: alice.id, group_ids: vec![admins.id] };
    let req = PermissionRequest {
        resource: ResourceRef::namespace(math_ns.id),
        permissions: vec![Permissions::ReadCollection],
    };

    let local = LocalPermissionBackend::new(pool.clone());
    assert_eq!(local.authorize(&principal, req.clone()).await.unwrap(), PermissionDecision::Allow);
    assert_eq!(mock.authorize(&principal, req).await.unwrap(), PermissionDecision::Allow);

    // Negative case: not granted.
    let req_delete = PermissionRequest {
        resource: ResourceRef::namespace(math_ns.id),
        permissions: vec![Permissions::DeleteCollection],
    };
    assert_eq!(local.authorize(&principal, req_delete.clone()).await.unwrap(), PermissionDecision::Deny);
    assert_eq!(mock.authorize(&principal, req_delete).await.unwrap(), PermissionDecision::Deny);
}
```

The helpers `run_export_cedar` and `parse_cedar_into_mock` must be added to
`src/tests/common.rs` (or wherever shared test helpers live):

- `run_export_cedar` invokes the same exporter logic as the binary, returning
  the printed output as a `String`. Refactor `export_cedar` in
  `src/bin/admin/export.rs` to write to a `Write` rather than stdout, then
  expose a small library helper `pub fn export_cedar_to(pool: &DbPool, w: &mut impl Write)`
  in `src/permissions/local/mod.rs` (or a sibling `export.rs`). The CLI just
  wraps this with `&mut std::io::stdout()`.
- `parse_cedar_into_mock` is a minimal parser sufficient for this round-trip:
  it scans for `permit(... principal in Group::"N", action in [Action::"X", ...], resource )
  when { resource is TYPE && resource.namespace_id == M };` blocks. **Do not
  implement a full Cedar parser** — just enough for the round-trip.

- [ ] **Step 3: Run**

Run: `cargo test --lib tests::permissions::translator -- --test-threads=1`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add src/tests/permissions/translator.rs src/tests/common.rs src/permissions/local/ src/bin/admin/export.rs
git commit -m "test(permissions): round-trip exported Cedar policies through MockTreetopBackend"
```

---

## Phase 7 — Documentation and live-Treetop parity suite

### Task 7.1: Write `docs/treetop/` materials

**Files:**
- Create: `docs/treetop/README.md`
- Create: `docs/treetop/schema.cedarschema`
- Create: `docs/treetop/bootstrap.cedar`

- [ ] **Step 1: Write the schema**

`docs/treetop/schema.cedarschema`:

```
entity User;
entity Group;
entity HubuumSystem;
entity HubuumNamespace;
entity HubuumClass {
    namespace_id: Long
};
entity HubuumObject {
    namespace_id: Long,
    class_id: Long
};
entity HubuumClassRelation {
    namespace_id: Long,
    from_namespace_id: Long,
    to_namespace_id: Long
};
entity HubuumObjectRelation {
    namespace_id: Long,
    from_namespace_id: Long,
    to_namespace_id: Long
};
entity HubuumTemplate { namespace_id: Long };
entity HubuumTask { submitted_by: Long };

action ReadCollection appliesTo {
    principal: User,
    resource: HubuumNamespace
};
action UpdateCollection appliesTo {
    principal: User,
    resource: HubuumNamespace
};
// ... continue for every Permissions variant; each maps to one action
// applied to the resource type(s) named in
// docs/superpowers/specs/2026-05-01-pluggable-permissions-design.md §3.
```

- [ ] **Step 2: Write the bootstrap policy**

`docs/treetop/bootstrap.cedar`:

```cedar
// Empty policy set. Hubuum's admin group is short-circuited inside the
// server (see admin_groupname config) and does not need a policy here.
//
// Operators should append generated policies (from
// `hubuum-admin export-permissions --as cedar`) or hand-author additional
// permits before serving production traffic.
```

- [ ] **Step 3: Write `docs/treetop/README.md`**

Cover: what Treetop mode is, when to use it, the bootstrap workflow
(upload schema → upload bootstrap policies → generate from SQL → upload),
the runtime config knobs, what `501` means, the synthetic-permission-row
caveat for `group_permission_on`.

- [ ] **Step 4: Update `docs/permissions.md`**

Add a top-level section "Backend selection" linking to `docs/treetop/`.

- [ ] **Step 5: Commit**

```bash
git add docs/treetop/ docs/permissions.md
git commit -m "docs(permissions): add Treetop bootstrap, schema, and backend-selection guide"
```

---

### Task 7.2: Live-Treetop parity test suite

**Files:**
- Create: `src/tests/permissions/parity_fixture.rs`
- Modify: `src/tests/permissions/mod.rs`
- Modify: `run_tests.sh`

- [ ] **Step 1: Wire the test module**

Add `pub mod parity_fixture;` to `src/tests/permissions/mod.rs`.

- [ ] **Step 2: Write the parity test (gated by env var)**

```rust
//! Live-Treetop parity tests.
//!
//! Skipped unless `HUBUUM_TREETOP_TEST_URL` is set. CI sets this only for
//! the dedicated "permissions-treetop" job; local developers do not need
//! a Treetop instance to run the rest of the suite.

use std::sync::Arc;

use crate::permissions::treetop::TreetopPermissionBackend;
use crate::tests::common::*;

fn treetop_url() -> Option<String> {
    std::env::var("HUBUUM_TREETOP_TEST_URL").ok()
}

#[actix_web::test]
async fn live_treetop_alice_reads_mathematics_namespace() {
    let Some(url) = treetop_url() else {
        eprintln!("skipping: HUBUUM_TREETOP_TEST_URL not set");
        return;
    };

    let pool = setup_pool().await;
    let cfg = test_config_with_treetop(&url);

    // Build the alice/bob/chris fixture from docs/permissions.md.
    let (alice, bob, chris, math_ns, _phys_ns) = seed_permissions_fixture(&pool).await;

    // Generate Cedar policies and upload them to the Treetop test server.
    let cedar = run_export_cedar(pool.clone()).await;
    upload_policies_to_treetop(&url, &cedar).await.unwrap();

    let backend = TreetopPermissionBackend::connect(&url, &cfg, pool.clone()).await.unwrap();

    let principal = PrincipalRef {
        user_id: alice.id,
        group_ids: vec![/* central-security */ /* fill from fixture */],
    };
    let req = PermissionRequest {
        resource: ResourceRef::namespace(math_ns.id),
        permissions: vec![Permissions::ReadCollection],
    };
    assert_eq!(backend.authorize(&principal, req).await.unwrap(), PermissionDecision::Allow);
}

// Add ~20 more cases covering all three principals × namespaces × actions
// per docs/permissions.md.
```

`seed_permissions_fixture`, `upload_policies_to_treetop`, and
`test_config_with_treetop` belong in `src/tests/common.rs`.

- [ ] **Step 3: Add a CI hook**

In `run_tests.sh`, add:

```bash
if [ -n "$HUBUUM_TREETOP_TEST_URL" ]; then
    cargo test --lib --features permissions-treetop tests::permissions::parity_fixture -- --test-threads=1
fi
```

- [ ] **Step 4: Commit**

```bash
git add src/tests/permissions/parity_fixture.rs src/tests/permissions/mod.rs src/tests/common.rs run_tests.sh
git commit -m "test(permissions): live-Treetop parity suite gated by HUBUUM_TREETOP_TEST_URL"
```

---

## Phase 8 — Observability and final polish

### Task 8.1: Add tracing spans at the trait boundary

**Files:**
- Modify: `src/permissions/local/mod.rs`
- Modify: `src/permissions/treetop/mod.rs`

- [ ] **Step 1: Wrap each `authorize` call in a span**

In each backend's `authorize` method:

```rust
#[tracing::instrument(
    skip(self, principal, request),
    fields(
        backend = self.kind(),
        user_id = principal.user_id,
        resource_kind = ?request.resource.kind,
        resource_id = request.resource.id,
        namespace_id = ?request.resource.namespace_id(),
        permissions = ?request.permissions,
        decision = tracing::field::Empty,
    ),
)]
async fn authorize(...) -> Result<PermissionDecision, ApiError> {
    let span = tracing::Span::current();
    let result = /* existing body */;
    if let Ok(decision) = &result {
        span.record("decision", &tracing::field::debug(decision));
    }
    result
}
```

- [ ] **Step 2: Add Treetop correlation ID**

In `TreetopPermissionBackend::authorize`, before the `client.authorize`
call, attach the current request's correlation ID if available. If
`treetop-client` exposes `Client::with_correlation_id` returning a new
client, use it; otherwise check whether `AuthorizeRequest` has a
correlation field. **Do not invent API.** If neither exists, log the
correlation ID as an extra span attribute and add a `TODO` comment with the
upstream issue link.

- [ ] **Step 3: Verify**

Run: `RUST_LOG=hubuum::permissions=debug cargo test --lib tests::permissions::backend_trait -- --test-threads=1 --nocapture`
Expected: spans appear in test output with the documented fields.

- [ ] **Step 4: Commit**

```bash
git add src/permissions/local/mod.rs src/permissions/treetop/mod.rs
git commit -m "feat(observability): instrument PermissionBackend calls with backend, decision, duration"
```

---

### Task 8.2: Update OpenAPI for new error codes

**Files:**
- Modify: `src/api/openapi.rs`

- [ ] **Step 1: Add `501` and `503` responses**

For each grant/revoke endpoint listed in `src/api/openapi.rs`, add the
extra response codes:

```rust
(status = 501, description = "Permission mutations not supported by the active backend"),
(status = 503, description = "Permission backend unavailable", headers(("Retry-After", description = "Seconds until retry"))),
```

- [ ] **Step 2: Regenerate `docs/openapi.json`**

Run: `cargo run --bin hubuum-openapi > docs/openapi.json`

- [ ] **Step 3: Verify CI's openapi-drift check**

Run the project's documented openapi check (likely `./scripts/check-openapi-drift.sh`
or similar — read `docs/development.md` for the exact command).

- [ ] **Step 4: Commit**

```bash
git add src/api/openapi.rs docs/openapi.json
git commit -m "docs(openapi): document 501 and 503 responses for permission endpoints"
```

---

### Task 8.3: Final integration smoke test

**Files:**
- Modify: `run_tests.sh`

- [ ] **Step 1: Default suite**

Confirm `run_tests.sh` (without `HUBUUM_TREETOP_TEST_URL`) runs the full
existing suite plus the new `permissions::*` tests under the `local`
backend.

- [ ] **Step 2: Add a brief README note**

Update `docs/development.md` with a paragraph explaining how to run the
Treetop parity suite locally:

```
## Running the Treetop parity suite locally

1. Start a Treetop server (see docs/treetop/README.md).
2. Upload docs/treetop/schema.cedarschema and docs/treetop/bootstrap.cedar.
3. Set HUBUUM_TREETOP_TEST_URL to its base URL.
4. Run: ./run_tests.sh
```

- [ ] **Step 3: Run the full suite locally**

Run: `./run_tests.sh`
Expected: green (without Treetop), green (with Treetop, if you set the env
var and the server is up).

- [ ] **Step 4: Commit**

```bash
git add docs/development.md run_tests.sh
git commit -m "docs(dev): document running the live-Treetop parity suite"
```

---

## Self-Review Notes

**Spec coverage check:**

| Spec section | Plan task(s) |
|---|---|
| §1 PermissionBackend trait | Tasks 1.1, 1.2 |
| §1 ResourceRef typed targets | Tasks 1.1, 3.1 |
| §1 admin short-circuit lives above backend | Task 3.2 (in `PermissionController::user_can_all`) |
| §2 LocalPermissionBackend | Tasks 2.1, 2.2 |
| §2 TreetopPermissionBackend | Tasks 5.2, 5.3 |
| §2 build_permission_backend | Task 2.4 |
| §2 AppContext + BackendContext refactor | Tasks 1.3, 3.8 |
| §2 cargo features | Task 0.2 |
| §3 Cedar action/resource shape | Task 5.2 + docs in Task 7.1 |
| §3 SQL→Cedar exporter | Task 6.1 |
| §3 docs/treetop bootstrap | Task 7.1 |
| §4 tests (mock + live-Treetop) | Tasks 5.4, 7.2 |
| §4 errors (501, 503, exit code) | Task 0.1 |
| §4 observability (spans, startup log) | Tasks 2.5, 8.1 |
| §4 OpenAPI 501/503 | Task 8.2 |
| can! macro stops collapsing to NamespaceID | Task 3.4 |
| AdminAccess / AdminOrSelfAccess refactor | Task 3.6 |
| Task / import permission paths | Task 3.7 |
| List/search/report visibility refactor | Tasks 4.1–4.4 |

**Open follow-ups (acknowledged in plan body):**

- `apply_in_memory_pagination` in Task 5.3 is intentionally placeholder-
  level pending QueryOptions review; must be replaced before Phase 5
  closes.
- Several treetop-client API method names (`with_ca_cert_path`,
  `connect_timeout`, `Resource::has_attr`, etc.) are best-guess based on
  the upstream README. The implementer must check the actual API and
  adjust — the structural plan stays the same.
- `Permissions::ReadTask` is *not* introduced; Task 3.7 reuses
  `Permissions::ReadCollection` against task resources and documents the
  decision. A dedicated task action is a follow-up.
