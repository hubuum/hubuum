# Task API

The task API is the generic interface for long-running operations.

Imports, exports, backups, remote target invocations, and computed-field
rebuilds use the generic task lifecycle. Reindex tasks have server-owned
payloads and are created only through computed-field definition changes or the
class rebuild endpoint.

The current architecture is:

- generic task framework: task submission state, lifecycle, polling, and event history
- typed per-task-kind result tables: import results and export outputs live behind task-kind-specific endpoints rather than in a fully generic result table

Endpoints:

- `GET /api/v1/tasks`
- `GET /api/v1/tasks/{task_id}`
- `GET /api/v1/tasks/{task_id}/events`
- `POST /api/v1/tasks/{task_id}/cancel`

Authentication:

- Bearer token required

Access rules:

- the submitting user can view the task
- admins can view any task

## Task kinds

Current task kinds:

- `import`
- `export`
- `backup`
- `reindex`
- `remote_call`
- `schema_validation`

Public task-producing endpoints today:

- `POST /api/v1/imports`
- `POST /api/v1/exports`
- `POST /api/v1/backups`
- `POST /api/v1/remote-targets/{target_id}/invoke`
- `POST /api/v1/classes/{class_id}/computed-fields/rebuild`

## Task statuses

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

## Cancel a task

```http
POST /api/v1/tasks/12/cancel
Authorization: Bearer <token>
Content-Type: application/json

{"reason":"Submitted with the wrong collection","expected_status":"queued"}
```

Both fields are optional; send `{}` for an unconditional cancellation request.
A reason must be a nonempty single line of at most 512 UTF-8 bytes. Unknown
request fields are rejected. `expected_status` returns `409 Conflict` if a
previously uncancelled, nonterminal task has moved to another status.

A queued task becomes terminal atomically and returns `200 OK`. An active task
retains its status and lease while its durable cancellation request returns
`202 Accepted`. Poll the task until cleanup is acknowledged with a terminal
status. Repeating a request returns the existing state without replacing the
first actor/reason or emitting another event. A task that already finished
returns `200` with its original result, even if `expected_status` differs.

Local authorization allows the submitting principal, an unscoped administrator,
or an unscoped human member of the submitting service account's owner group.
The owner-group rule also permits withdrawing a disabled service account's
work. Authentication still rejects disabled callers. A scoped token may cancel
only tasks submitted with that exact token; it cannot exercise administrator
or service-account management authority. Internal reindex tasks, including
manually requested rebuilds, and schema validation tasks require an unscoped
administrator. Schema cancellation additionally checks `UpdateClass`.

With Treetop, cancellation requires the independent `CancelTask` action; a
`ReadTask` grant does not authorize cancellation. The local scoped-token and
internal-task restrictions still apply. Deploy the updated Cedar schema and
appropriate `CancelTask` policies before enabling the endpoint.

Task responses include `cancel_requested_at`, `cancel_requested_by`,
`cancel_reason`, `execution_deadline_at`, `terminal_reason`, and
`unattempted_items`. Request metadata is distinct from terminal acknowledgement.
`terminal_reason` is `cancel_requested` or `deadline_exceeded` when the task
terminates as `cancelled`. Deleting an actor clears the live actor reference;
audit provenance remains. Reason text is returned to authorized readers but
is excluded from metric labels and lifecycle log messages.

| Kind | Cancellation behavior |
| --- | --- |
| Strict import | Uncommitted domain writes and receipts roll back together. If the domain transaction committed first, its factual completed result is retained. |
| Best-effort import | Committed items remain. Remaining work stops. The summary and `unattempted_items` report the exact remainder. Results include an aggregate `unattempted` row with `details.count`; this row does not increase processed or failed counts. |
| Export | Query, hydration and rendering stop at checkpoints. A cancelled task publishes no partial output. |
| Backup | Capture stops at checkpoints and discards incomplete output. Cancellation is separate from backup verification failure. |
| Reindex | Committed object batches remain, class materialization stays incomplete, and a later class rebuild can restore freshness. |
| Remote call | Before dispatch, no request is sent. After dispatch, external effects may have occurred; cancellation cannot undo them and never automatically retries. |
| Schema validation | Completed batches remain. Further commits are fenced, and cancelled work is never requeued by lease recovery. The existing schema DELETE endpoint retains its atomic batch-fencing behavior. |

For remote calls, `remote_side_effect_state` is `not_sent`, `possibly_sent`, or
`legacy_unknown` for older executions without dispatch evidence. The durable
result retains the task ID, target, method, rendered URL, and any known response
for reconciliation. A known response does not imply that cancellation undid its
effects. Review the remote system before manually submitting replacement work.

Deadlines use the same cleanup protocol and the stable `cancelled` status.
The server pins a per-kind maximum duration at the first claim; queue wait is
excluded. Changing configuration, renewing a lease, or recovering a schema
checkpoint never extends that deadline. See [execution limits](task_system.md#execution-limits).

Request cancellation before confirming a destructive restore. Once maintenance
draining begins, the cancellation endpoint follows the normal API gate and
returns `503`. Workers continue observing already persisted cancellation requests
and deadlines while draining; the restore coordinator waits for their cleanup.
Restore confirmation itself is outside the generic task cancellation protocol.

## Get task

`GET /api/v1/tasks/{task_id}`

Example:

```json
{
  "id": 12,
  "kind": "import",
  "status": "running",
  "submitted_by": 7,
  "created_at": "2026-03-07T10:15:22",
  "started_at": "2026-03-07T10:15:22",
  "finished_at": null,
  "progress": {
    "total_items": 4,
    "processed_items": 2,
    "success_items": 2,
    "failed_items": 0
  },
  "summary": null,
  "request_redacted_at": null,
  "links": {
    "task": "/api/v1/tasks/12",
    "events": "/api/v1/tasks/12/events",
    "import": "/api/v1/imports/12",
    "import_results": "/api/v1/imports/12/results"
  },
  "details": {
    "import": {
      "results_url": "/api/v1/imports/12/results"
    }
  }
}
```

## Response fields

### Top-level state

- `id`
  - task identifier
- `kind`
  - generic task type
- `status`
  - current lifecycle state
- `submitted_by`
  - user ID of the creator

### Timing

- `created_at`
- `started_at`
- `finished_at`
- `request_redacted_at`

### Progress

`progress` is generic and item-count oriented:

```json
{
  "total_items": 10,
  "processed_items": 7,
  "success_items": 6,
  "failed_items": 1
}
```

### Links

`links` always contains generic task URLs and may contain task-kind-specific URLs.

Example for an import:

```json
{
  "task": "/api/v1/tasks/12",
  "events": "/api/v1/tasks/12/events",
  "import": "/api/v1/imports/12",
  "import_results": "/api/v1/imports/12/results"
}
```

For a non-import task kind, the import-specific links may be `null`.

Example for an export:

```json
{
  "task": "/api/v1/tasks/22",
  "events": "/api/v1/tasks/22/events",
  "export": "/api/v1/exports/22",
  "export_output": "/api/v1/exports/22/output"
}
```

## List tasks

`GET /api/v1/tasks`

This returns a paginated list of tasks visible to the caller.

Visibility rules:

- admins see all tasks
- non-admin users automatically see only their own tasks; no `submitted_by` parameter is needed or effective

Example response:

```json
[
  {
    "id": 13,
    "kind": "import",
    "status": "queued",
    "submitted_by": 7,
    "created_at": "2026-03-07T10:20:00",
    "started_at": null,
    "finished_at": null,
    "progress": {
      "total_items": 3,
      "processed_items": 0,
      "success_items": 0,
      "failed_items": 0
    },
    "summary": null,
    "request_redacted_at": null,
    "links": {
      "task": "/api/v1/tasks/13",
      "events": "/api/v1/tasks/13/events",
      "import": "/api/v1/imports/13",
      "import_results": "/api/v1/imports/13/results"
    },
    "details": {
      "import": {
        "results_url": "/api/v1/imports/13/results"
      }
    }
  }
]
```

Pagination:

- supports cursor-based pagination using `limit`, `sort`, and `cursor`
- when following `X-Next-Cursor`, keep the same `sort` and filters
- response may include `X-Next-Cursor` when more results are available

Sorting:

- supported sort fields: `id`, `kind`, `status`, `submitted_by`, `created_at`, `started_at`, `finished_at`
- multiple sort fields are supported with comma-separated order, for example `sort=kind.asc,id.desc`

Filters:

- `kind` (optional): `import`, `export`, `backup`, `reindex`, `remote_call`
- `status` (optional): `queued`, `validating`, `running`, `succeeded`, `failed`, `partially_succeeded`, `cancelled`
- `submitted_by` (optional): admin-only filter by user ID; non-admin callers are always restricted to their own tasks regardless of this parameter

Example:

```text
GET /api/v1/tasks?kind=import&status=running&submitted_by=7&sort=id.desc&limit=25
```

### Details

`details` is reserved for typed task-kind-specific data.

Current example:

```json
{
  "import": {
    "results_url": "/api/v1/imports/12/results"
  }
}
```

Export example:

```json
{
  "export": {
    "output_url": "/api/v1/exports/22/output",
    "output_available": true,
    "output_expires_at": "2026-04-06T10:15:23",
    "template_name": "export.host_room_people",
    "output_content_type": "text/plain",
    "warning_count": 1,
    "truncated": false,
    "total_duration_ms": 148,
    "query_duration_ms": 42,
    "hydration_duration_ms": 31,
    "render_duration_ms": 68
  }
}
```

The export phase timings are returned while the stored export output remains available; after output expiry these fields are `null`. `total_duration_ms` covers query execution, relation-aware hydration, and rendering plus the intervening task bookkeeping. It does not include initial template preparation or final output persistence.

## Task events

`GET /api/v1/tasks/{task_id}/events`

- supports cursor pagination via `limit`, `sort`, and `cursor`

This returns append-only lifecycle and progress history for the task.

Example:

```json
[
  {
    "id": 201,
    "task_id": 12,
    "event_type": "queued",
    "message": "Task queued",
    "data": null,
    "created_at": "2026-03-07T10:15:22",
    "provenance": {
      "actor": {
        "kind": "user",
        "principal": {
          "principal_id": 7,
          "name": "admin"
        }
      },
      "initiator": {
        "principal_id": 7,
        "name": "admin"
      },
      "task_id": 12
    }
  },
  {
    "id": 202,
    "task_id": 12,
    "event_type": "validating",
    "message": "Task claimed for validation",
    "data": null,
    "created_at": "2026-03-07T10:15:22",
    "provenance": {
      "actor": {
        "kind": "worker",
        "principal": null
      },
      "initiator": {
        "principal_id": 7,
        "name": "admin"
      },
      "task_id": 12
    }
  },
  {
    "id": 203,
    "task_id": 12,
    "event_type": "running",
    "message": "Import execution started",
    "data": null,
    "created_at": "2026-03-07T10:15:22"
  },
  {
    "id": 204,
    "task_id": 12,
    "event_type": "succeeded",
    "message": "Import finished with 4 succeeded and 0 failed items",
    "data": {
      "processed_items": 4,
      "success_items": 4,
      "failed_items": 0
    },
    "created_at": "2026-03-07T10:15:23"
  }
]
```

The actor identifies who or what performed that lifecycle transition. The
initiator identifies the principal that submitted the root task and stays the
same for worker, recovery, cleanup, and terminal events. Queue events record
the submitter as both actor and initiator. Names are resolved in one batch for
the response page; deleted principals keep their durable ID and may have a
`null` name. Legacy task events without stored initiator fields use their
queued event as a bounded fallback.

## Polling pattern

Typical client flow:

1. Create a task indirectly through a task-producing endpoint such as `POST /api/v1/imports` or `POST /api/v1/exports`
2. Read the `Location` header or the returned `links.task`
3. Poll `GET /api/v1/tasks/{task_id}` until the status is terminal
4. Optionally fetch `GET /api/v1/tasks/{task_id}/events`
5. If the kind exposes a domain endpoint, follow the typed links

This is where typed per-task-kind result storage shows up in the API. The generic task endpoints tell you what the task is doing; task-kind-specific endpoints expose the typed output model for that kind.

Example:

```text
POST /api/v1/imports
-> 202 Accepted
-> Location: /api/v1/tasks/12

GET /api/v1/tasks/12
-> status: queued

GET /api/v1/tasks/12
-> status: running

GET /api/v1/tasks/12
-> status: succeeded
```

## Task shapes by kind

### Import task

- `kind` is `import`
- `details.import.results_url` is present
- `links.import` and `links.import_results` are present
- import item outcomes come from the import-specific result model, not a generic shared result table

### Export task

- `kind` is `export`
- `details.export.output_url` is present
- `links.export` and `links.export_output` are present
- the stored output lives behind `GET /api/v1/exports/{task_id}/output`

## Errors

Common responses:

- `401 Unauthorized`
  - missing or invalid bearer token
- `403 Forbidden`
  - task belongs to another user and the caller is not admin
- `404 Not Found`
  - task ID does not exist
