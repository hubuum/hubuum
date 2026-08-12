# Storage Testing and Backend Certification

Hubuum's PostgreSQL path has strong practical integration coverage from the
adapter through the HTTP API. Boundary enforcement is also strong. The main
qualification is portability evidence: PostgreSQL is the only complete
production backend, so a second independent adapter has not yet demonstrated
that every contract is equally implementable outside PostgreSQL.

This document separates what each test layer proves from what it does not.

## Confidence Summary

| Area | Confidence | Reason |
| --- | --- | --- |
| PostgreSQL behavior on a real database | Strong | The full suite creates and migrates an isolated PostgreSQL database and exercises queries, transactions, triggers, workers, services, and APIs. |
| Lifecycle semantics | Strong | Focused contracts run against PostgreSQL and a deterministic memory model, with extensive API coverage above them. |
| Boundary direction and type isolation | Strong | Compile-time aggregate bounds plus architecture and workspace source guards reject known PostgreSQL, Diesel, pool, and `ApiError` leaks. |
| Mandatory family availability | Strong | `StorageBackend` requires every trait, certification is sealed, dispatch is exhaustive, and the registry covers every selectable kind. |
| Every method's observable semantics | Good, not mechanical | Shared tests exercise all families and most operations, but method-level behavioral coverage is curated rather than generated from trait definitions. |
| Application and HTTP behavior | Strong | Large identity, core-data, job, and platform integration suites exercise real storage through handlers and services. |
| Concurrency and failure recovery | Good | Targeted lease, notification, transaction, restore, retention, and atomicity tests exist; systematic fault injection and schedule exploration do not. |
| Cross-backend portability | Moderate | Traits and DTOs are neutral and a lifecycle model provides independent evidence, but only PostgreSQL implements the complete contract. |
| Quantified source coverage | Unknown | The project does not currently publish line or branch coverage for the storage adapter. Test counts alone cannot reveal unvisited branches. |

## The Test Layers

```text
contract DTO unit tests
          |
          v
architecture and workspace guards
          |
          v
shared lifecycle + selectable-backend contracts
          |
          v
PostgreSQL native integration and query budgets
          |
          v
application services and authorization policy
          |
          v
HTTP / CLI / worker / destructive workflow suites
          |
          v
feature, platform, container, and benchmark CI
```

Each layer catches a different failure. Passing an HTTP test does not prove
that another backend can implement the trait, and passing a contract test does
not prove PostgreSQL transaction isolation.

## Contract and DTO Tests

`hubuum-storage-core` unit tests validate constructors, builders, invariants,
redaction, cursor values, error taxonomy, and other backend-neutral behavior.
They are deterministic and require no database.

These tests prove that boundary values enforce their local invariants. They do
not prove that an adapter applies the request correctly.

## Architecture Tests

`src/tests/application_boundary.rs` and `src/tests/workspace_boundaries.rs`
guard the dependency direction. Among other checks, they verify that:

- `AppContext` contains an opaque handle rather than a pool;
- production contexts cannot reconstruct PostgreSQL directly;
- application consumers do not import Diesel, PostgreSQL modules, schema
  modules, connections, or transactions;
- neutral crates do not depend on Actix, Diesel, global application
  configuration, or `ApiError`;
- the aggregate contains every required trait;
- only explicitly certified backends are selectable;
- the memory lifecycle model is not selectable; and
- common observation labels cover the known dispatch surface.

These are valuable compile-time-adjacent regression guards. Some inspect
source text, so they protect known boundaries rather than providing a formal
module-system proof against every possible indirect dependency.

## Shared Lifecycle Contracts

Collection, class, object, class-relation, and object-relation service behavior
runs against both:

- the real PostgreSQL adapter; and
- `MemoryStorageModel`, a deterministic focused model.

This is the strongest independent evidence that lifecycle services rely on
behavior rather than PostgreSQL mechanics. The memory model deliberately stops
there; it does not stand in for a complete alternative backend.

## Selectable-Backend Compatibility

`src/tests/storage_contract.rs` owns the current compatibility registry.
`available_backends()` iterates `StorageBackendKind::ALL`, constructs each
backend through `StorageHandle`, and verifies its descriptor before running the
shared tests.

The suite covers these family-level behaviors:

| Family | Shared compatibility behavior |
| --- | --- |
| `domain_lifecycle` | Lifecycle service contracts plus record compatibility operations |
| `catalog_queries` | Collection, class, and object listing with real matching rows |
| `computed_object_queries` | Computed filtering and enrichment |
| `computed_field_lifecycle` | Shared and personal definitions, class state, scheduling, and claimed rebuild execution |
| `object_aggregates` | Storage-pushdown and delegated authorization modes |
| `relation_queries` | Lists, endpoint sets, graph traversal, exclusions, and multi-root expansion |
| `identity_and_authorization_data` | Authentication projections, scopes, users, service accounts, tokens, groups, memberships, local decisions, grants, and policy snapshots |
| `temporal_history` | All supported entity pages, point-in-time reads, visibility, and provenance names |
| `inventory_queries` | Consistent totals and per-class counts |
| `unified_search` | Collection, class, and object search over real rows |
| `remote_targets` | Point/list reads, audited lifecycle, and invocation provenance |
| `task_queue` | Submission, access, pages, events, results, and retained outputs |
| `task_execution` | Claim, lease, recovery, state changes, completion, failure, and output purge |
| `backup_snapshots` | State-only and history-aware logical snapshot sections |
| `restores` | Staging, projections, expiry, coordination, cleanup, and recovery transitions |
| `imports` | Planning lookups, preflight, strict and best-effort application, and durable results |
| `export_queries` | Mandatory logical budget scope |
| `export_template_lifecycle` | Point/scoped reads, bindings, and audited lifecycle |
| `event_administration` | Audit visibility, sink/subscription lifecycle, fan-out delivery creation, retry, and dead-letter actions |
| `operations` | Metrics, readiness and diagnostics, event health/fan-out/retention, token retention, and compile-time worker notification composition |

The aggregate trait guarantees that every method exists. The compatibility
suite guarantees representative semantics by family and directly invokes most
methods. It does not currently derive a method inventory from the traits, and
some native worker operations—especially delivery claims and notification
listeners—receive their deepest coverage in PostgreSQL-specific tests rather
than a reusable backend test.

The suite is not yet a `hubuum-storage-contract-tests` workspace crate. That is
an extraction target after remaining root-owned domain fixtures are removed.

## PostgreSQL-Specific Coverage

PostgreSQL tests retain responsibility for mechanics that a logical contract
cannot reproduce.

### Native operations

Tests alongside `src/storage/postgres` and
`src/storage/postgres/operations` cover native behavior such as:

- pool and TLS settings plus safe endpoint diagnostics;
- transaction and connection context reset;
- error classification;
- SQL filtering and cursor mapping;
- audit triggers, provenance, and revision serialization;
- computed materialization and rebuild transactions;
- task lease claims, renewal, recovery, and terminal artifacts;
- event fan-out, delivery claims, retention, and notifications;
- token retention coordination;
- restore transactions and recovery; and
- notification visibility only after commit.

Many PostgreSQL operation modules are primarily exercised by integration tests
rather than inline unit tests. This is appropriate for SQL behavior, but it
means file-local test counts are not a useful coverage measure.

### Query budgets and plans

`src/tests/storage_performance.rs` instruments Diesel and checks representative
query counts, pool checkouts, fixed query shapes, no-op write avoidance, and
selected query plans. It protects point reads, lifecycle writes, relations,
history, permission depth, paging, and ancestor traversal from accidental N+1
or round-trip growth.

The PostgreSQL benchmark workflow compares representative storage operations
against the pull request base. Query budgets catch structural regressions;
benchmarks catch latency changes that preserve query counts.

### Migrations and destructive restore

`run_tests.sh` creates a fresh isolated database, applies all migrations, and
runs the suite against that schema. `tests/restore_roundtrip.rs` separately
exercises destructive state replacement and restart recovery.

Migration compatibility with adjacent supported releases is covered by the
release and container workflows, not by the shared backend contract.

## Application and Authorization Coverage

Application services are exercised in three ways:

1. Focused service contracts validate lifecycle behavior independent of HTTP.
2. Permission tests cover local and external policy backends, resource
   construction, visibility, and policy export.
3. HTTP integration suites execute handlers, permission checks, services,
   storage DTO conversion, PostgreSQL operations, and response projection as
   one path.

The integration suites are grouped by surface:

- `tests/api_core_data_suite` covers collections, classes, objects, relations,
  querying, search, computed fields, patches, and aggregates.
- `tests/api_identity_suite` covers authentication, groups, users, service
  accounts, and principal settings.
- `tests/api_jobs_suite` covers tasks, imports, exports, backups, restores,
  export templates, and remote targets.
- `tests/api_platform_suite` covers events, deliveries, subscriptions, probes,
  metrics, runtime configuration, and request diagnostics.
- CLI, binary-smoke, and restore-round-trip targets cover process entry points.

These suites provide strong evidence for the complete PostgreSQL application
path. Because they currently compose PostgreSQL, they do not by themselves
prove that handlers are backend neutral; architecture guards and the shared
registry provide that complementary evidence.

## CI Certification

The pull-request workflows add coverage that a local default build does not:

- default, no-default, all-feature, and rustls-without-OpenSSL combinations;
- Linux x86_64, Linux aarch64, macOS, and Windows;
- release builds with all production features;
- OpenAPI, Rust API, dependency, license, and static checks;
- production-container construction and live single-host rollout tests; and
- runtime, self-contained, and PostgreSQL storage benchmarks.

The repository's required local command remains:

```bash
source .env && ./run_tests.sh
```

Also run the formatting, clippy, Markdown, OpenAPI, Docker, and classification
checks required by `AGENTS.md` for the paths changed.

## Known Gaps

The current suite is solid, but these limitations should remain visible:

1. **Only one complete adapter exists.** Neutral APIs have been designed and
   enforced, but a second production implementation is the best portability
   test.
2. **Method coverage is curated.** Compilation catches a missing method and the
   shared suite covers every family, but there is no generated assertion that
   every trait method has a corresponding semantic scenario.
3. **No published line or branch coverage exists.** A coverage report would
   help identify cold error branches, even if it should not become a simplistic
   merge gate.
4. **Concurrency testing is targeted rather than exhaustive.** The suite tests
   important races, but it does not systematically explore task schedules,
   connection loss, process death, or database failover at every transition.
5. **The compatibility harness still uses root fixtures.** Until extracted, an
   external adapter cannot consume it as a normal workspace dependency.

## Highest-Value Improvements

The next improvements should be:

1. Maintain an explicit trait-method-to-scenario inventory and fail an
   architecture test when a required method lacks a compatibility or documented
   native-only scenario.
2. Extract `hubuum-storage-contract-tests` once the remaining root-domain DTOs
   and fixture interface are neutral.
3. Add deterministic fault-injection around task leases, event delivery,
   restore coordination, and connection loss.
4. Produce periodic line and branch coverage reports for diagnosis, focusing
   review on error and rollback paths rather than a repository-wide percentage.
5. Run the unchanged service and HTTP suites through a second complete adapter
   as part of its certification.

## Interpreting a Green Suite

A fully green suite means the current PostgreSQL-backed application satisfies a
large set of boundary, semantic, persistence, API, operational, packaging, and
performance expectations. It does not mean every possible failure schedule has
been explored or that cross-backend portability has been proven in production.

That distinction is why new backends need both the shared contract and their
own native tests, and why maintainers must keep architecture guards alongside
end-to-end coverage.
