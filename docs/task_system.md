# Task System Internals

This document describes how the task system is implemented in Hubuum today.

It is intentionally implementation-focused. For public API behavior, see:

- [docs/task_api.md](task_api.md)
- [docs/import_api.md](import_api.md)

## Purpose

The task system provides a generic framework for long-running server-side work.

Current task kind:

- `import`

Reserved task kinds already modeled in the schema:

- `report`
- `export`
- `reindex`

The goal is to keep task lifecycle, queueing, polling, and audit history generic, while letting each task kind supply its own execution logic and its own typed result storage.

## Core model

The implementation has:

- `tasks`
- `task_events`
- `import_task_results`

The first two are generic framework tables.

The last one is a typed per-task-kind result table for imports.

Relevant code:

- [src/models/task.rs](../src/models/task.rs)
- [src/db/traits/task.rs](../src/db/traits/task.rs)
- [migrations/2026-03-07-000001_tasks_and_imports/up.sql](../migrations/2026-03-07-000001_tasks_and_imports/up.sql)

### `tasks`

`tasks` is the canonical queue and lifecycle table.

Each row stores:

- task identity: `id`, `kind`
- lifecycle: `status`, `created_at`, `started_at`, `finished_at`, `updated_at`
- ownership: `submitted_by`
- submission metadata: `idempotency_key`, `request_hash`
- request storage: `request_payload`, `request_redacted_at`
- progress counters: `total_items`, `processed_items`, `success_items`, `failed_items`
- terminal summary: `summary`

### `task_events`

`task_events` is append-only lifecycle/progress history.

Typical events:

- `queued`
- `validating`
- `running`
- `succeeded`
- `failed`
- `partially_succeeded`

The task row is the current state. `task_events` is the history of how the task got there.

### `import_task_results`

`import_task_results` is an import-specific typed result table.

Each row records:

- task id
- item ref
- entity kind
- action
- identifier
- outcome
- error
- details

This table is intentionally separate from `tasks`, because result shape is task-kind-specific.

That is the current architectural rule:

- `tasks` and `task_events` are the generic task framework
- result persistence is typed per task kind

So a future async report implementation would be expected to introduce its own typed result/output table rather than reuse `import_task_results`.

## Status model

Generic statuses are defined in [src/models/task.rs](../src/models/task.rs):

- `queued`
- `validating`
- `running`
- `succeeded`
- `failed`
- `partially_succeeded`
- `cancelled`

Terminal statuses:

- `succeeded`
- `failed`
- `partially_succeeded`
- `cancelled`

For imports:

- `queued` means the task row exists and is waiting to be claimed
- `validating` means a worker has claimed it and is planning/validating the import
- `running` means planning is complete and execution is underway, or a dry-run is materializing results
- terminal states are set after results are written and summary counters are finalized

## How tasks enter the system

Imports are created through:

- `POST /api/v1/imports`

Relevant code:

- [src/api/v1/handlers/imports.rs](../src/api/v1/handlers/imports.rs)

Submission flow:

1. The handler serializes the request payload.
2. It computes a SHA-256 request hash.
3. It reads `Idempotency-Key` if present.
4. It either reuses an existing task for that submitter/idempotency key or inserts a new one.
5. It returns `202 Accepted` with `Location: /api/v1/tasks/{id}`.
6. It kicks the worker so the queue starts draining immediately.

Task creation itself is generic and implemented in:

- [create_generic_task](../src/db/traits/task.rs)

When a task is created:

- the `tasks` row is inserted with `status = queued`
- the full `request_payload` is stored
- counters are initialized to zero
- a `queued` event is appended

## Worker model

The worker implementation lives in:

- [src/tasks/worker.rs](../src/tasks/worker.rs)
- [src/tasks/planning.rs](../src/tasks/planning.rs)
- [src/tasks/execution.rs](../src/tasks/execution.rs)
- [src/tasks/resolution.rs](../src/tasks/resolution.rs)

There are two entry points:

- `ensure_task_worker_running`
- `kick_task_worker`

### Startup workers

`ensure_task_worker_running` is called during server startup from:

- [src/main.rs](../src/main.rs)

It starts a fixed number of background worker loops once per process.

### Kick-on-submit

`kick_task_worker` is called from the import submission path so a newly queued import does not wait for the next idle poll cycle.

### Worker count and polling

Worker behavior is configurable via:

- `HUBUUM_TASK_WORKERS`
- `HUBUUM_TASK_POLL_INTERVAL_MS`

Configuration lives in:

- [src/config.rs](../src/config.rs)

Defaults:

- `HUBUUM_ACTIX_WORKERS`: detected CPU count
- `HUBUUM_TASK_WORKERS`: about half the detected CPU count, minimum `1`
- `HUBUUM_TASK_POLL_INTERVAL_MS`: `200`

The HTTP worker count and background task worker count are intentionally separate.

## Queue claiming

Task claiming is DB-backed and implemented in:

- [claim_next_queued_task](../src/db/traits/task.rs)

Claiming uses:

- `FOR UPDATE`
- `SKIP LOCKED`
- ordering by oldest `created_at`

That gives these properties:

- only one worker can claim a queued row
- multiple workers in one process are safe
- multiple app instances are also safe
- workers skip rows currently locked by another claimant instead of blocking

Claiming immediately transitions the task to:

- `status = validating`
- `started_at = now()`

After claim, the worker appends a `validating` event.

## Dispatch

After a task is claimed, `process_one_task` dispatches by `task.kind`.

Current dispatch:

- `import` -> import executor
- anything else -> unimplemented error

This logic is in:

- [process_one_task](../src/tasks/worker.rs)

The task framework is generic even though only imports execute today.

## Import execution pipeline

Import execution happens in four major phases:

1. load payload
2. plan and validate
3. execute
4. finalize and redact

### 1. Load payload

The worker deserializes `tasks.request_payload` into `ImportRequest`.

If the payload is missing or invalid, the task is marked failed.

### 2. Plan and validate

Planning is implemented in:

- [plan_import](../src/tasks/planning.rs)

Planning walks the import graph in dependency order:

1. namespaces
2. classes
3. objects
4. class relations
5. object relations
6. namespace permissions

Planning resolves:

- refs inside the import document
- natural-key selectors for existing records
- permissions for each intended operation
- collisions with existing data
- schema validation for objects when class schema validation is enabled

Planning output is not just “pass/fail”.

It produces:

- `planned_items`: executable work items
- `failures`: per-item planning failures
- `aborted`: whether policy required early stop

This is important for `best_effort` mode. Best-effort imports may now continue with planned work even if some items failed during planning, as long as policy did not require an early abort.

### 3. Execute

Execution mode depends on `mode.atomicity`.

#### Strict mode

Implemented in:

- [execute_import_strict](../src/tasks/execution.rs)

Behavior:

- all domain mutations run in one SQL transaction
- if one executed item fails, everything rolls back
- if execution succeeds, all planned items are recorded as succeeded

Important boundary:

- the import domain mutations are one transaction
- task bookkeeping is not part of that same transaction

So strict mode means “domain writes are all-or-nothing”, not “all task metadata and domain state live in one giant transaction”.

#### Best-effort mode

Implemented in:

- [execute_import_best_effort](../src/tasks/execution.rs)

Behavior:

- each executable item runs in its own transaction
- successful items remain committed
- failed items are recorded individually
- planning-time failures are also recorded individually
- continuation depends on permission/collision policy

This is what allows partial success.

### 4. Finalize

After execution:

- import per-item results are inserted into the import-specific typed result table `import_task_results`
- summary counters are written to `tasks`
- terminal event is appended
- the original request payload is redacted

Redaction is implemented in:

- [finalize_task_terminal_state](../src/db/traits/task.rs)

Redaction means:

- `request_payload = NULL`
- `request_redacted_at = now()`

The system keeps summary metadata and typed per-task-kind results, but not the original request body after completion.

## Transaction boundaries

This is the most important correctness detail.

### What is transactional in strict mode

The following are inside one transaction:

- namespace/class/object/relation/permission mutations for planned import items

### What is not in that same transaction

The following happen outside that domain mutation transaction:

- claiming the task
- setting `validating`
- appending lifecycle events
- planning and validation reads
- inserting import-specific typed result rows
- updating task counters and terminal summary
- payload redaction

That separation is intentional:

- task state must survive worker crashes
- clients need to observe progress independently of domain-transaction scope
- task audit/history should not disappear because domain execution rolled back

## Collision and permission policy behavior

Planning and execution behavior is shaped by:

- `mode.atomicity`
- `mode.collision_policy`
- `mode.permission_policy`

Current behavior:

- `strict` always aborts on the first planning failure
- `best_effort + permission_policy=continue` records permission failures and continues with remaining plannable work
- `best_effort + permission_policy=abort` stops once a permission failure requires abort
- `collision_policy=abort` records or aborts on collisions depending on atomicity
- `collision_policy=overwrite` turns matching namespace/class/object collisions into update operations

For relations:

- overwrite-like behavior is effectively modeled as `noop` when a matching relation already exists

## Authorization

Task visibility:

- owner can view
- admin can view any task

Generic endpoints:

- `GET /api/v1/tasks/{id}`
- `GET /api/v1/tasks/{id}/events`

Import-specific endpoints:

- `GET /api/v1/imports/{id}`
- `GET /api/v1/imports/{id}/results`

Import execution itself runs under the submitting user’s effective permissions, not as a privileged system bypass.

That means planning checks and execution permission checks are based on the task submitter.

## Queue introspection

Admin queue state is exposed through:

- `GET /api/v0/meta/tasks`

Relevant code:

- [src/api/handlers/meta.rs](../src/api/handlers/meta.rs)

This endpoint reports:

- configured worker counts
- poll interval
- task counts by status
- task counts by kind
- total task events
- total import result rows
- oldest queued task timestamp
- oldest active task timestamp

This is intended as an operational view of queue depth and worker activity.

## Failure handling

If worker dispatch or execution returns an unexpected `ApiError`:

- task status is set to `failed`
- a failure summary is stored
- a `failed` event is appended
- payload is redacted

This ensures the queue does not leave orphaned active tasks on normal error paths.

## Scaling model

Today’s design scales in two dimensions:

### Vertical within one process

Increase:

- `HUBUUM_TASK_WORKERS`

### Horizontal across processes

Run more app instances pointing at the same database.

This works because claiming is DB-coordinated with `SKIP LOCKED`.

The design does not require an external queue for correctness.

## Limitations and current assumptions

- there is no cancellation flow implemented yet
- there is no separate worker binary yet; workers run inside the web process
- progress counters are currently updated at finalize time, not streamed continuously per item
- only imports have a concrete executor and a typed result table today
- task retention is summary/results after payload redaction, not full payload retention

## Tests

The task system now has coverage in three areas:

### Queue mechanics

- concurrent-safe claim behavior for queued rows

### Execution semantics

- strict execution rolls back on runtime failure
- best-effort execution preserves successful items

### API behavior

- task creation, events, results, and redaction
- idempotency reuse
- collision policy behavior
- permission policy behavior
- non-owner access rejection for task/import reads
- admin queue meta endpoint

See:

- [src/db/traits/task.rs](../src/db/traits/task.rs)
- [src/tasks/tests.rs](../src/tasks/tests.rs)
- [src/tests/api/v1/imports.rs](../src/tests/api/v1/imports.rs)
- [src/tests/api/meta.rs](../src/tests/api/meta.rs)

## Recommended mental model

The simplest accurate mental model is:

- `tasks` is the queue and current status table
- `task_events` is the audit/history table
- result tables are typed per task kind
- task workers claim from Postgres directly
- each task kind supplies its own planner and executor
- imports are currently the only real task kind
- strict imports make domain mutations atomically
- best-effort imports trade atomicity for progress
