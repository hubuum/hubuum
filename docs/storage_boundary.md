# Service and Storage Boundary

Hubuum is incrementally separating application use cases from PostgreSQL and
Diesel details. PostgreSQL remains the production semantic reference; the goal
is a compile-time dependency boundary and faster contract tests, not generic
multi-database support.

The first migrated capabilities cover the core collection, class, object, and
class-relation lifecycles. Collections include:

- point reads;
- create with an initial assignee grant and lifecycle event;
- update, including no-op behavior;
- delete constraints;
- direct children and ordered ancestors; and
- hierarchy moves.

Classes include:

- explicit point resolution by ID or name;
- create with schema validation and a lifecycle event;
- update, including no-op behavior and collection moves;
- selector-aware mutations that reject stale name targets; and
- delete with a lifecycle event.

Objects include:

- explicit point resolution by class/object ID or class/object name;
- create within a resolved class, including JSON Schema validation;
- update and atomic JSON Patch, including revision-preserving no-ops;
- selector-aware mutations that reject stale class or object names; and
- delete with a lifecycle event.

Class relations include:

- normalization and endpoint preparation before authorization;
- point resolution with both endpoint classes;
- create and delete with atomic lifecycle events;
- stale endpoint rejection between authorization and a transactional write;
- directional alias and cardinality-limit preservation; and
- class and collection delete cascades.

## Dependency direction

```text
collection/class/object/class-relation HTTP handlers
                         |
                         v
 CollectionService / ClassService / ObjectService
               / ClassRelationService
                         |
                         v
 CollectionStore / ClassStore / ObjectStore
               / ClassRelationStore
                    /       \
                   v         v
             PostgreSQL    memory
              adapter      adapter
                   |
                   v
          Diesel transactions and queries
```

`AppContext` constructs `Services` with `PostgresStorage` in production. Core
collection, class, object, and class-relation point/lifecycle handlers call
their services; they do not choose a Diesel query or transaction helper for
migrated operations. Permission checks remain at the handler boundary.

Class-relation preparation and resolution return aggregates containing both
endpoint classes. Handlers build permission resources from those aggregates,
so authorization does not perform hidden PostgreSQL lookups. Transactional
writes lock and recheck the same endpoint snapshots before inserting or
deleting a relation.

## Responsibility split

`src/services/*` owns application-facing use-case entrypoints. Services accept
domain request types and return application errors after translating the
backend-neutral storage error taxonomy.

`src/storage/*` owns capability traits and adapters. Capability methods are
aggregate-shaped rather than table-shaped so implementations retain control of
transactions, batching, hierarchy maintenance, initial permission grants, and
atomic lifecycle events.

`src/db/traits/*` continues to own Diesel expressions, PostgreSQL SQL, locks,
and transaction implementation. `PostgresStorage` delegates to these existing
operations without changing their query shape.

`MemoryStorage` is compiled for tests and implements the logical collection,
class, object, and class-relation contracts. It models hierarchy, selector
resolution, endpoint preparation, schema validation, bounded JSON Patch
behavior, revisions, no-op updates, delete constraints, cascades, and lifecycle
event occurrence. It does not claim PostgreSQL locking, trigger,
computed-field materialization, permission-row, or temporal-history
equivalence.

## Contract and performance gates

The shared contract suite runs each migrated collection, class, object, and
class-relation behavior against both PostgreSQL and memory. Each test focuses
on one behavior so backend differences cannot be hidden inside a large
scenario.

The PostgreSQL query-capture tests exercise both services and retain exact
point-read and mutation budgets. The opt-in PostgreSQL Criterion benchmark also
times the collection service path. Trait dispatch or service composition must
therefore not introduce extra pool checkouts, SQL statements, or
application-side pagination.

## Current migration boundary

This is an incremental migration. Collection, class, object, and class-relation
list/search, computed-field enrichment, permission management, object-relation
lifecycles, graph traversal, history, and unrelated aggregates still use
`BackendContext` and the existing model/database traits. Existing direct
persistence APIs also remain for fixtures, imports, restore paths, and
unmigrated callers.

When expanding the boundary:

1. Add a use-case-shaped storage capability rather than a generic repository.
2. Preserve set-based SQL, batching, transactions, and event atomicity in the
   PostgreSQL adapter.
3. Add focused shared logical contract tests.
4. Keep PostgreSQL-only query-budget, locking, rollback, trigger, and
   concurrency tests.
5. Route the selected high-level caller through a service before removing its
   direct `DbPool` dependency.
6. Remove old model/database entrypoints only after every production,
   administrative, worker, import, restore, and test caller has migrated.

Search and cursor pagination remain PostgreSQL-oriented until a concrete
backend-neutral query contract can preserve filtering, stable ordering, count
behavior, authorization, and query budgets without leaking SQL types.
