# Storage Testing and Backend Compatibility

Hubuum's PostgreSQL path has strong practical integration coverage from the
adapter through the HTTP API. Boundary enforcement is also strong. The main
qualification is portability evidence: PostgreSQL is the only complete
production backend, so a second independent adapter has not yet demonstrated
that every contract is equally implementable outside PostgreSQL.

This document separates what each test layer proves from what it does not.
The obligations being tested are defined by the normative
[storage contract](contract.md).

## Confidence Summary

| Area | Confidence | Reason |
| --- | --- | --- |
| PostgreSQL behavior on a real database | Strong | The full suite creates and migrates an isolated PostgreSQL database and exercises queries, transactions, triggers, workers, services, and APIs. |
| Resource lifecycle semantics | Strong | Focused family contracts run against PostgreSQL and a deterministic memory model, including whole-graph transaction commit and state-plus-event rollback. |
| Boundary direction and type isolation | Strong | Compile-time aggregate bounds plus architecture and workspace source guards reject known PostgreSQL, Diesel, pool, and `ApiError` leaks. |
| Mandatory family availability | Strong | `StorageBackend` requires every trait, opt-in is explicit, dispatch is exhaustive, and a sealed certification gate covers every selectable kind. |
| Audit/event contract | Strong | The reusable conformance harness verifies a durable receipt, no-op behavior, rollback, outbox-to-sink delivery, and logical/backend/failure telemetry for every registered backend. |
| Every method's observable semantics | Strong inventory, curated scenarios | A machine-checked inventory must exactly match every complete-backend trait method and selected input-enum variant, and each entry names shared or native evidence. The scenarios still require human review for semantic depth. |
| Application and HTTP behavior | Strong | Every registered backend runs a service point read, readiness, and representative authenticated point/list HTTP requests. Larger integration suites exercise the remaining real application path. |
| Concurrency and failure recovery | Good | Targeted lease, notification, transaction, restore, retention, and atomicity tests exist. Task-local PostgreSQL failpoints prove rollback at collection-create and task-finalization seams without changing the public contract. |
| Cross-backend portability | Moderate | Traits and DTOs are neutral and a focused resource model provides independent evidence, but only PostgreSQL implements the complete contract. |
| Quantified source coverage | Unknown | The project does not currently publish line or branch coverage for the storage adapter. Test counts alone cannot reveal unvisited branches. |

## The Test Layers

```text
contract DTO unit tests
          |
          v
architecture and workspace guards
          |
          v
shared resource-family + selectable-backend contracts
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

`crates/hubuum-storage-core/tests/external_adapter_api.rs` is compiled as an
external crate. It verifies that an out-of-tree adapter can construct every
transaction-scoped resource port and use representative typed principal and
query APIs without reaching into private fields.

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
- only adapters that explicitly implement the complete aggregate are selectable;
- the focused memory resource model is not selectable; and
- every logical contract method crosses exactly one common observer with a
  unique, bounded label pair; and
- every selectable backend implements the mandatory transaction capability.

These are valuable compile-time-adjacent regression guards. Some inspect
source text, so they protect known boundaries rather than providing a formal
module-system proof against every possible indirect dependency.

The same architecture suite reads
`docs/storage_boundary/semantic-coverage.toml`. The inventory must match the
aggregate trait set, each trait's methods, and each tracked boundary enum's
variants exactly. Every entry must point to a test function that exists. A new
method or variant therefore fails locally until its intended semantic evidence
is recorded. The observation guard derives its expected method count from this
same inventory, including resource-family methods observed by
`ObservedStorage`, rather than maintaining a second hand-written operation
list.

## Shared Resource-Family Contracts

Collection, class, object, class-relation, and object-relation service behavior
runs against both:

- the real PostgreSQL adapter; and
- `MemoryStorageModel`, a deterministic focused model.

This is the strongest independent evidence that resource services rely on
behavior rather than PostgreSQL mechanics. The memory model deliberately stops
there; it does not stand in for a complete alternative backend.

`src/tests/storage_transactions.rs` applies one generic transaction scenario
to both implementations. The scenario commits a complete resource graph, then
repeats every lifecycle family and deliberately returns an application error.
It verifies that committed state has family-specific audit evidence, then that
the failed callback's state and all five family-specific audit events roll back.

The memory model implements a serializable copy-on-write unit of work for this
purpose. That proves the application-facing semantics are independently
implementable; it does not claim to model PostgreSQL isolation or failure
timing.

## Selectable-Backend Compatibility

`src/tests/storage_contract.rs` owns the current compatibility registry.
`available_backends()` iterates `StorageBackendKind::ALL`, constructs each
backend through `StorageHandle`, and verifies its descriptor before running the
shared tests.

The application fixture registry also provisions an administrator through each
backend. Every registered backend is then exercised through `Services`, the
readiness handler, and authenticated collection point and list routes. The
Actix application receives only `AppContext`; adapter-native clients remain
inside the exhaustive fixture-construction match.

The suite covers these family-level behaviors:

| Family | Shared compatibility behavior |
| --- | --- |
| `domain_lifecycle` | Lifecycle service contracts, record compatibility operations, and cross-family transaction commit/rollback |
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

The aggregate trait guarantees that every method exists. The semantic coverage
inventory guarantees that the method and tracked input-variant lists cannot
drift unnoticed. The compatibility suite supplies representative semantics by
family and directly invokes most methods. Some native worker operations—most
notably delivery claims and notification listeners—still receive their deepest
coverage in PostgreSQL-specific tests, and the inventory records that evidence
as native rather than pretending it is portable.

`hubuum-storage-conformance` owns the reusable, workspace-internal,
backend-independent five-part audit verifier. Each adapter supplies an isolated fixture through
`BackendAuditFixture`; the root registry invokes it for every
`StorageBackendKind::ALL` entry. A selectable backend must demonstrate:

1. a committed mutation receipt that matches its durable event row;
2. a genuine no-op with neither receipt nor appended event;
3. rollback of both state and event at an injected failure;
4. durable fan-out followed by delivery to a recording sink; and
5. logical, native-backend, and failure telemetry observations.

This is behavioral certification, not a type-system proof of implementation
semantics. The application seals its `CertifiedStorageBackend` registry so an
adapter cannot become selectable merely by satisfying Rust method signatures.

## PostgreSQL-Specific Coverage

PostgreSQL tests retain responsibility for mechanics that a logical contract
cannot reproduce.

### Native operations

Tests alongside `crates/hubuum-storage-postgres`, plus the test-only legacy harness under `src/storage/postgres/operations`, cover native behavior such as:

- pool and TLS settings plus safe endpoint diagnostics;
- transaction and connection context reset;
- native commit and rollback beneath the backend-neutral unit of work;
- error classification;
- SQL filtering and cursor mapping;
- audit triggers, provenance, and revision serialization;
- computed materialization and rebuild transactions;
- task lease claims, renewal, recovery, and terminal artifacts;
- event fan-out, delivery claims, retention, and notifications;
- token retention coordination;
- restore transactions and recovery; and
- notification visibility only after commit.

The shared transaction test proves state-plus-event rollback through the
public contract. Adapter-private, task-local failpoints additionally interrupt a compound collection create
after its rows are written and task finalization after its terminal audit event
is written. Both tests assert that the surrounding PostgreSQL transaction
rolls back all intermediate state. Failpoint selection does not cross
`StorageBackend`, and task-local scope prevents parallel tests from injecting
failures into one another.

Many PostgreSQL operation modules are primarily exercised by integration tests
rather than inline unit tests. This is appropriate for SQL behavior, but it
means file-local test counts are not a useful coverage measure.

### Query budgets and plans

`src/tests/storage_performance.rs` instruments Diesel and checks representative query counts, pool checkouts, fixed query shapes, no-op write avoidance, and selected query plans. It protects point reads, lifecycle writes, relations, history, permission depth, paging, ancestor traversal, computed reads, and object aggregation from accidental N+1 or round-trip growth.

The PostgreSQL benchmark workflow compares representative storage operations
against the pull request base. Query budgets catch structural regressions;
benchmarks catch latency changes that preserve query counts.

`storage_collection_boundary_callgrind` keeps the existing read-only service
boundary case and separately measures an audited mutation crossing the common
observer plus construction/consumption of a committed receipt outcome. This
separates application-boundary overhead from database round trips and event
insertion costs.

### Migrations and destructive restore

`run_tests.sh` creates a fresh isolated database, applies all migrations, and
runs the suite against that schema. `tests/restore_roundtrip.rs` separately
exercises destructive state replacement and restart recovery.

The migration source lives with its owner at
`crates/hubuum-storage-postgres/migrations`. Diesel supports this layout both
at build time through `embed_migrations!("migrations")`, resolved relative to
the adapter crate, and at test/deployment time through an explicit
`--migration-dir`. Moving adapter migrations therefore does not expose a path
through `hubuum-storage-core` or make schema ownership part of the neutral API.

Migration compatibility with the adjacent supported release is tested directly in CI. The workflow starts the previous release, creates representative resources and workflow configuration, migrates the database with the candidate, verifies the candidate application, and then restarts the previous application against the migrated schema. A separate static policy rejects migration shapes known to violate adjacent-release compatibility.

That test certifies one supported `N-1` transition. It does not promise arbitrary rollback across multiple releases, and it does not make destructive down migrations safe.

## Application and Authorization Coverage

Application services are exercised in three ways:

1. Focused service contracts validate lifecycle behavior independent of HTTP.
2. Permission tests cover local and external policy backends, resource
   construction, visibility, and policy export.
3. HTTP integration suites execute handlers, permission checks, services,
   storage DTO conversion, PostgreSQL operations, and response projection as
   one path.

In addition, the selectable-backend registry runs a compact service and HTTP
smoke contract for every `StorageBackendKind`. That check protects composition
neutrality when a new adapter is registered; the larger suites protect depth
for the current PostgreSQL application.

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

## CI Verification

The pull-request workflows add coverage that a local default build does not:

- default, no-default, all-feature, and rustls-without-OpenSSL combinations;
- Linux x86_64, Linux aarch64, macOS, and Windows;
- release builds with all production features;
- OpenAPI, Rust API, dependency, license, and static checks;
- production-container construction and live single-host rollout tests; and
- shared Criterion and Gungraun benchmarks plus runtime behavior validation.

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
2. **Evidence is scenario-level, not path coverage.** The inventory proves
   that maintainers classified every method and tracked variant and named an
   existing scenario. It cannot prove that every branch of a method is visited
   or that a broad family scenario asserts every subtle invariant.
3. **No published line or branch coverage exists.** A coverage report would
   help identify cold error branches, even if it should not become a simplistic
   merge gate.
4. **Fault and concurrency testing is targeted rather than exhaustive.** The
   suite tests important races and two deterministic rollback seams, but it
   does not systematically explore task schedules, connection loss, process
   death, or database failover at every transition.
5. **Only the five-part verifier is extracted.** The broader compatibility
   harness still uses root application, administrator, HTTP, and delivery
   fixtures, so an external adapter cannot consume that full suite as a
   standalone crate.

## Highest-Value Improvements

The next improvements should be:

1. Move additional backend-neutral family scenarios into
   `hubuum-storage-conformance` once their root application DTOs and fixture
   interfaces are neutral.
2. Extend deterministic fault injection to event delivery, restore
   coordination, lease loss, and connection failure.
3. Produce periodic line and branch coverage reports for diagnosis, focusing
   review on error and rollback paths rather than a repository-wide percentage.
4. Run the unchanged service, transaction, and HTTP suites through a second complete adapter
   as part of its acceptance testing.

## Interpreting a Green Suite

A fully green suite means the current PostgreSQL-backed application satisfies a
large set of boundary, semantic, persistence, API, operational, packaging, and
performance expectations. It does not mean every possible failure schedule has
been explored or that cross-backend portability has been proven in production.

That distinction is why new backends need both the shared contract and their
own native tests, and why maintainers must keep architecture guards alongside
end-to-end coverage.
