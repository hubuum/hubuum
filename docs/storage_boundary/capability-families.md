# Storage Capability Families

This document maps the complete storage contract. It is the quickest way to
answer three questions:

1. Which trait owns an operation?
2. Which other capabilities supply its inputs or consume its results?
3. Which semantics must every selectable backend preserve?

For normative guarantees, see the [storage contract](contract.md). For
implementation steps, see the [backend author guide](backend-author-guide.md).
For source locations, see the [maintainer guide](maintainer-guide.md).

## How Families and Traits Relate

A **trait** is the Rust interface that makes an operation available. A
**capability family** is a documentation grouping for related traits and
semantics. `hubuum_storage_core::capabilities` exposes broader discovery
modules for resources, identity, queries, workflows, events, and operations;
the more detailed families below are semantic subgroups, not a one-to-one
module map. Neither form represents separately versioned or negotiable runtime
features.

Persistence ports use the suffix `Storage`: for example,
`CollectionStorage`, `CatalogStorage`, `EventRetentionStorage`, and
`ExecutionStorage`. Collaborators that are not backend capabilities use role
names such as `EventArchiveSink`, `StorageObserver`, `StorageTransaction`, and
`ObjectAggregateAuthorizer`. Backend names come from the application registry
rather than a second adapter identity trait.

`StorageBackend` aggregates every required trait. An adapter implements that
aggregate explicitly when it is ready to be selectable. Rust checks all
supertrait requirements at compile time, and the composition registry controls
which complete implementations administrators can select.

```text
StorageBackend
|
|-- foundation
|   |-- domain lifecycle
|   |-- audited transaction composition
|   `-- identity and authorization data
|
|-- read models
|   |-- catalog queries
|   |-- computed object queries
|   |-- computed-field lifecycle
|   |-- object aggregates
|   |-- relation queries
|   |-- temporal history
|   |-- inventory queries
|   `-- unified search
|
|-- workflows
|   |-- remote targets
|   |-- task queue
|   |-- task execution
|   |-- backup snapshots
|   |-- restores
|   |-- imports
|   |-- export queries
|   `-- export-template lifecycle
|
`-- event and operational control
    |-- event administration
    `-- operations
```

The tree groups responsibilities; it is not a runtime call graph. The important
runtime relationships are shown next.

## Runtime Relationships

```text
identity + authorization
          |
          v
visibility for catalog, computed reads, aggregates,
relations, history, unified search, exports, and events

lifecycle and workflow mutations
          |
          v
atomic audit events -> fan-out -> delivery -> retention

safe lifecycle primitives -> TransactionStorage -> one atomic unit of work

task queue -> task execution
     |            |
     `------------+--> imports, exports, backups, remote calls,
                        restores, and computed-field rebuilds

ExecutionStorage wraps calls from requests and workers
          |
          v
call-site attribution, mutation provenance, revision preconditions
```

These relationships do not permit one capability to reach through another and
recover its backend. The application composes use cases; the backend implements
the atomic operations each use case requires.

## Foundation Families

### `domain_lifecycle`

Required traits:

- `TransactionStorage`;
- `CollectionStorage`, `ClassStorage`, and `ObjectStorage`; and
- `ClassRelationStorage` and `ObjectRelationStorage`.

This family owns collection, class, object, and relation resolution and
mutation. Implementations own locking, hierarchy maintenance, JSON validation
coordination, relation cardinality, cascades, initial grants, revisions, and
atomic lifecycle events.

`TransactionStorage` composes safe lifecycle primitives without exposing a
native transaction. Its `StorageTransaction` accessors return crate-owned
operation types for collections, classes, class relations, objects, and object
relations. Transaction-scoped mutations inherit one required `EventContext`,
so state and audit events commit or roll back together.

The resource traits also include validation and bulk lookup. Every ordinary
mutation requires audit context and returns an explicit mutation outcome.
Imports and restores implement their explicit `ImportStorage` and
`RestoreStorage` capabilities; fixture compatibility helpers use system attribution. See
[transactions and side effects](transactions-and-events.md).

These are operation-shaped capabilities, not table repositories. A backend
decides how each operation is implemented and never exposes rows, connections,
or a query builder.

### `identity_and_authorization_data`

Required traits:

- `AuthenticationStorage`;
- `BootstrapStorage`, `IdentityScopeStorage`, `IdentityMembershipStorage`,
  `ServiceAccountStorage`, `ExternalIdentityStorage`, `UserStorage`, and
  `TokenStorage`;
- `AuthorizationStorage` and `CollectionAuthorizationStorage`; and
- `GroupStorage` and `PrincipalStorage`.

This family owns authentication projections, identity scopes, humans, service
accounts, tokens, groups, memberships, local grants, authorization facts, and
the candidate or snapshot data required by external policy engines.

The application still owns token-policy interpretation, administrator policy,
external policy evaluation, public authorization resources, and conversion to
`ApiError`. Storage owns consistent facts and atomic local-grant mutations.

Authorization results feed almost every permission-scoped read family. Those
read contracts accept backend-neutral visibility descriptors or already
authorized identifiers; they do not import a concrete permission backend.

## Read-Model Families

### `catalog_queries`

Required trait: `CatalogStorage`.

Owns permission-aware, filterable, cursor-paged collection, class, and object
lists with optional exact totals. The application owns public cursor encoding
and external-policy post-authorization.

### `computed_object_queries`

Required trait: `ComputedObjectStorage`.

Owns computed filtering, sorting, exact counts, cursor snapshots, and computed
value enrichment. It consumes definitions and materialized values managed by
the computed-field lifecycle family.

### `computed_field_lifecycle`

Required trait: `ComputedFieldLifecycleStorage`.

Owns shared and personal definition lifecycle, per-class computation state,
rebuild scheduling, and rebuild execution under a task lease. Definition
mutations and audit events are atomic. A stale worker must not commit a rebuild
after losing its claim.

### `object_aggregates`

Required trait: `ObjectAggregateStorage`.

Owns permission-scoped grouping, computed values, numeric measures, exact group
counts, and stable cursors. With an external policy engine, the backend retains
bounded candidate paging and accumulation while an application-owned
`ObjectAggregateAuthorizer` returns decisions over neutral candidates.

### `relation_queries`

Required trait: `RelationQueryStorage`.

Owns relation lists and counts, endpoint-set queries, bounded frontier reads,
related-class and related-object graphs, and multi-root expansion for exports.
It must preserve documented direction, path, exclusion, limit, and
alternative-path semantics.

### `temporal_history`

Required trait: `HistoryStorage`.

Owns revision-filtered pages and point-in-time reads for collections, classes,
objects, export templates, and remote targets. Visibility applies before
filtering, counting, and paging. Provenance-name resolution is batched.

### `inventory_queries`

Required trait: `InventoryStorage`.

Owns one consistent administrative snapshot of collection, class, object, and
per-class object counts. Callers must not assemble the snapshot from separately
timed reads.

### `unified_search`

Required trait: `UnifiedSearchStorage`.

Owns ranked collection, class, and object search with stable per-kind cursors
and token visibility pushdown. The three projections form one capability.

## Workflow Families

### `remote_targets`

Required trait: `RemoteTargetStorage`.

Owns point and list reads, atomic audited create/update/delete operations, and
invocation provenance. Transport configuration crosses the boundary only in
redacted-debug DTOs. The application owns template and policy validation plus
the actual outbound call.

### `task_queue`

Required trait: `TaskQueueStorage`.

Owns idempotent submission under active-task limits, access facts, task pages,
events, import results, and retained export and backup outputs. It is the
application-facing history of work, not the worker lease state machine.

### `task_execution`

Required trait: `TaskExecutionStorage`.

Owns claims, lease renewal and recovery, claim-checked events and state
changes, atomic completion artifacts, failure accounting, and output
retention. Claims are opaque tokens that callers can only return to the
backend.

### `backup_snapshots`

Required trait: `BackupSnapshotStorage`.

Owns a consistent projection of live state into the canonical backup sections,
with optional history. The application owns document metadata, serialization,
hashing, and retained task artifacts.

### `restores`

Required trait: `RestoreStorage`.

Owns durable artifact staging, compare-and-set lifecycle transitions, global
drain coordination, rollback-safe state replacement, provenance, cleanup, and
recovery. The application validates and decodes the backup document; the
backend owns destructive transactional application.

### `imports`

Required trait: `ImportStorage`.

Owns planning lookups, rollback-only preflight, strict atomic application,
best-effort per-item application, reference resolution, and durable result
recording. The application supplies an exhaustive typed plan and owns public
validation, authorization, and collision policy.

### `export_queries`

Required trait: `ExecutionStorage` with a `StorageExecutionScope` query-budget
override.

Owns a backend-enforced, optional non-zero query budget around each export read
stage. The application specifies a logical budget, not a database timeout
primitive. PostgreSQL currently implements it with transaction-local
`statement_timeout` that cannot leak through pool reuse.

### `export_template_lifecycle`

Required trait: `ExportTemplateStorage`.

Owns point and scoped list reads, collection-source discovery, class binding
lookup, and atomic audited lifecycle writes. The application owns template
syntax, query, source-composition, permission, and API PATCH validation.

## Event and Operational Families

### `event_administration`

Required traits:

- `AuditEventStorage`;
- `EventSubscriptionStorage`; and
- `EventDeliveryAdministrationStorage`.

This family owns visibility-scoped audit reads, sink and subscription
lifecycle, and claim-free delivery inspection, retry, and dead-letter actions.
Sink and subscription mutations include their lifecycle events atomically.

### `operations`

Required traits:

- `MetricsStorage` and `OperationalStateStorage`;
- `EventDeliveryStorage`, `EventFanoutStorage`, `EventHealthStorage`, and
  `EventRetentionStorage`;
- `TokenRetentionStorage` and `WorkerNotificationStorage`; and
- `ExecutionStorage`.

This family owns probes and administrative snapshots, logical metrics inputs,
retention, worker claims and acknowledgements, native wake-up integration, and
the execution context applied across requests and workers.

Common logical observation is reported through application-owned
`StorageObserver`. An adapter may also define native telemetry for pool,
transaction, and query mechanics. Production composition supplies both; a
no-op implementation is an explicit test, benchmark, or one-shot tool opt-out.

Event workers form a pipeline:

```text
audited mutation
      |
      v
EventFanoutStorage -> durable deliveries -> EventDeliveryStorage
                                              |
                                              v
                                      external transport

execute_event_retention_batch -> EventRetentionStorage claim/complete + EventArchiveSink
WorkerNotificationStorage wakes workers without exposing native listeners
```

`ExecutionStorage` is cross-cutting. One composable `StorageExecutionScope`
carries bounded call-site attribution, mutation provenance, revision
preconditions, and optional query budgets. `run_in_scope` supports task-local
work; `run_in_scope_send` is the explicit `Send` form. An absent override
inherits its surrounding value, while a present `None` deliberately clears
one. The adapter translates the scope into its native mechanism; callers never
select task locals, session variables, or transaction settings.

Retention uses durable claim/archive/complete coordination. The core
`execute_event_retention_batch` helper owns that ordering so adapters cannot
override it. Archive calls run outside the database transaction and are
idempotent by batch ID. Failed archives preserve the exact claim and source
events for retry; completion deletes exactly that claim and is itself
idempotent.

## Changing a Family

Traits and crate versions are the compatibility mechanism for statically linked
adapters. Do not add a parallel contract version, runtime capability
negotiation, or a dynamic plugin ABI. A backend may live in another repository,
but the application selects and links it at compile time.

When a family changes, update all of the following together:

1. The owning trait and `StorageBackend` when the aggregate changes.
2. Common dispatch and observation in `StorageHandle`.
3. Every selectable adapter.
4. The shared compatibility behavior.
5. Backend-native tests where consistency or failure mechanics change.
6. `semantic-coverage.toml`, including exact methods, tracked variants, and
   shared or native scenario evidence.
7. This document and any sanitized administrator settings affected by the
   change.
