# Rust API Boundary

Status: accepted on 2026-08-08.

## Decision

The root `hubuum` package is an internal application crate. It is not a
supported third-party server embedding library, even though Cargo requires its
sibling binaries, integration tests, and benchmarks to access `pub` items
through the library target.

Hubuum's supported programmatic application contract is the versioned HTTP API
and its committed OpenAPI document. Rust API consumers should use
`hubuum-client-rust`, which wraps that HTTP contract. The Python client follows
the same boundary.

The storage adapter SDK is classified `experimental-public` and has a
documented crates.io publication graph. Its exact compatibility rules are in
the [Storage Adapter SDK Compatibility policy](storage_adapter_sdk.md). All
other workspace packages remain unpublished. Public Rust visibility in those
internal packages remains an implementation detail until a separate promotion
review.

## Current consumers

The root library has four in-repository consumer groups:

- `hubuum-server`, through the narrow `run_runtime_from_environment` composition
  entrypoint;
- `hubuum-admin`, through the narrow `run_admin_from_environment` command
  entrypoint;
- `hubuum-openapi`, through `generate_openapi_json`; and
- integration tests and benchmarks, which require Rust's external-crate
  visibility because Cargo builds those targets separately.

No workspace crate depends on the root package. The workspace-boundary test
forbids dependencies on `hubuum`, Actix, and Diesel so application-neutral
crates cannot grow a dependency on the server implementation.

An organization code search performed for this decision found no Hubuum-owned
repository consuming the server library. `hubuum-client-rust` and the Python
client consume HTTP/OpenAPI instead. A future external consumer does not become
supported merely because it uses a Git dependency.

## Runtime composition

The library owns process composition while `hubuum-server` remains a thin
binary entrypoint. One application bootstrap initializes shared configuration,
logging, metrics, PostgreSQL, authorization, restore coordination, and
application context. `RuntimeRole` then selects the runtime components:

```text
common application bootstrap
             |
             +-- api: HTTP API and API middleware
             +-- worker: task, event, retention, and worker metrics services
             `-- all: API and worker components under one supervisor
```

The public operational role names remain `api`, `worker`, and `all`. "Task" is
not an accurate worker-role name because that process also runs event fan-out,
event delivery, event retention, token retention, restore coordination, and
worker metrics. Runtime roles remain a deployment-time choice in one binary and
one container image; they are not Cargo feature combinations or separate
binaries.

The composition entrypoints are workspace-internal. They may change with the
server implementation and must use application-owned orchestration rather than
becoming a general embedding facade.

## Package classifications

Every workspace package declares `package.metadata.hubuum.rust-api` in its
manifest.

| Package | Classification |
| --- | --- |
| `hubuum` | Internal application |
| `hubuum-auth-core` | Workspace-internal |
| `hubuum-auth-ldap` | Workspace-internal |
| `hubuum-computed-fields` | Experimental public |
| `hubuum-domain` | Experimental public |
| `hubuum-event-sink-amqp` | Workspace-internal |
| `hubuum-event-sink-email` | Workspace-internal |
| `hubuum-event-sink-valkey` | Workspace-internal |
| `hubuum-event-sink-webhook` | Workspace-internal |
| `hubuum-event-sinks-common` | Workspace-internal |
| `hubuum-events-core` | Experimental public |
| `hubuum-outbound-http` | Workspace-internal |
| `hubuum-query` | Experimental public |
| `hubuum-scale-benchmark` | Workspace-internal |
| `hubuum-scale-core` | Workspace-internal |
| `hubuum-secrets` | Workspace-internal |
| `hubuum-storage-core` | Experimental public |
| `hubuum-storage-conformance` | Experimental public |
| `hubuum-storage-postgres` | Workspace-internal |
| `hubuum-task-core` | Experimental public |
| `hubuum-templates` | Workspace-internal |

The machine values are:

- `internal-application`;
- `workspace-internal`;
- `experimental-public`; and
- `stable-public`.

`experimental-public` is a supported pre-1.0 source API with the versioning and
migration rules in its package policy. It does not mean “unversioned” or
“best-effort.” `stable-public` additionally promises ordinary post-1.0 SemVer.

`scripts/check-rust-api-policy.py` rejects an unclassified package. Internal
classifications require Cargo publishing to be disabled; public classifications
require `publish = true` or a registry allowlist containing `crates-io`, plus a
package-specific policy document that is also packaged as the crate readme.
Public crates cannot depend on internal workspace packages. Packages in a named
release train must share one version and use exact requirements for every
in-train path dependency. The checker uses Cargo's resolved workspace
membership, including automatically admitted in-tree path dependencies. CI and
tagged release validation run the policy and its regression fixtures. The CI
change classifier also discovers declared policy document paths so their
deletion or movement cannot bypass validation as a documentation-only change.

## Internal package rules

Internal application and workspace-internal packages have no third-party SemVer
promise. Maintainers may change their public Rust items without describing the
change as a user-facing Rust API break.

This does not weaken application contracts. A change still requires the
appropriate review when it affects:

- HTTP or OpenAPI;
- configuration or CLI behavior;
- database migrations;
- backup, import, export, event, or metrics formats;
- published binaries and containers; or
- another Hubuum-owned repository pinned to an internal Git revision.

Internal `pub` items should still be small and safe. In particular, do not
broaden access to credential digests, raw secrets, claim capabilities, mutable
global configuration, persistence rows, Diesel schema/query internals, or
transport caches merely to make a test convenient. Prefer crate-local tests,
focused test-support APIs, and application-owned command or service entrypoints.

Public traits in internal crates are not third-party extension points. Backend,
permission, provider, and storage traits become extension contracts only after
an explicit promotion decision.

The root crate's `services` and `storage` modules are therefore internal
application boundaries even though Rust visibility is required by binaries and
benchmarks. See [Application and Storage Boundary](storage_boundary.md) for the
complete backend contract, exact service ports, and adapter rules.

## Promotion policy

Promoting a crate to `experimental-public` or `stable-public` requires a
separate reviewed change. Its policy document must define:

- purpose, intended callers, and supported entrypoints;
- error taxonomy and panic policy;
- asynchronous runtime, thread-safety, and cancellation behavior;
- supported feature combinations and MSRV;
- serialization and wire-compatibility guarantees where applicable;
- security, secret-redaction, and credential-handling guarantees;
- supported third-party trait implementations;
- deprecation and removal timelines; and
- release ownership and downstream compatibility fixtures.

The manifest must allow crates.io publishing and contain registry-ready
metadata and versioned dependencies. Release CI then automatically selects the
package and runs:

- rustdoc with warnings denied and all declared features enabled;
- `cargo package --locked`, including Cargo's clean packaged-source build; and
- pinned `cargo-semver-checks` against the latest crates.io release.

For a crate's initial public release, CI records that no crates.io baseline
exists and skips only the semantic comparison. Documentation and packaging
checks remain mandatory. Once a crates.io release exists, semantic comparison
is mandatory; registry lookup failures other than a definitive missing-package
response fail the job.

An intentional supported-API break requires the correct version change, a
changelog entry labeled as a supported Rust crate break, and migration guidance.
Narrow baseline-specific exceptions are preferred over disabling compatibility
validation.

The storage SDK promotion applies this process to a closed, exact-version
release train. Its package graph, feature matrix, MSRV, enum policy,
deprecation timeline, aggregate evolution rules, release order, and adapter
upgrade steps are defined in
[Storage Adapter SDK Compatibility](storage_adapter_sdk.md).

## Changelog terminology

Changelog entries must distinguish:

- breaking HTTP/OpenAPI changes;
- breaking changes in a supported Rust crate;
- internal or workspace Rust refactors;
- operational or configuration contract changes; and
- database or persisted-format migration requirements.

Do not call an internal trait, model, worker, or persistence signature change a
"breaking Rust API" change solely because Rust visibility is `pub`.

## Local verification

Run the classification policy and its fixtures with:

```bash
python3 scripts/check-rust-api-policy.py
python3 scripts/test-rust-api-policy.py
python3 scripts/test-crates-io-baseline.py
```

The deterministic JSON inventory is available for review with:

```bash
python3 scripts/check-rust-api-policy.py --json
```

The [generated project inventory](generated/project_inventory.md) lists the current
SDK packages, versions, minimum Rust versions, and task kinds. CI regenerates it
in check mode.
