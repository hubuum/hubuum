# Task API

The task API is the generic interface for long-running operations.

Imports and reports are public task-producing APIs today, and the task model is designed so later
exports or reindex jobs can use the same status and event model too.

The current architecture is:

- generic task framework: task submission state, lifecycle, polling, and event history
- typed per-task-kind result tables: import results and report outputs live behind task-kind-specific endpoints rather than in a fully generic result table

Endpoints:

- `GET /api/v1/tasks`
- `GET /api/v1/tasks/{task_id}`
- `GET /api/v1/tasks/{task_id}/events`

Authentication:

- Bearer token required

Access rules:

- the submitting user can view the task
- admins can view any task

## Task kinds

Current and reserved task kinds:

- `import`
- `report`
- `export`
- `reindex`

Public task-producing endpoints today:

- `POST /api/v1/imports`
- `POST /api/v1/reports`

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

Example for a report:

```json
{
  "task": "/api/v1/tasks/22",
  "events": "/api/v1/tasks/22/events",
  "report": "/api/v1/reports/22",
  "report_output": "/api/v1/reports/22/output"
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

- `kind` (optional): `import`, `report`, `export`, `reindex`
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

Report example:

```json
{
  "report": {
    "output_url": "/api/v1/reports/22/output",
    "output_available": true,
    "output_expires_at": "2026-04-06T10:15:23",
    "template_name": "report.host_room_people",
    "output_content_type": "text/plain",
    "warning_count": 1,
    "truncated": false
  }
}
```

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
    "created_at": "2026-03-07T10:15:22"
  },
  {
    "id": 202,
    "task_id": 12,
    "event_type": "validating",
    "message": "Task claimed for validation",
    "data": null,
    "created_at": "2026-03-07T10:15:22"
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

## Polling pattern

Typical client flow:

1. Create a task indirectly through a task-producing endpoint such as `POST /api/v1/imports` or `POST /api/v1/reports`
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

### Report task

- `kind` is `report`
- `details.report.output_url` is present
- `links.report` and `links.report_output` are present
- the stored output lives behind `GET /api/v1/reports/{task_id}/output`

## Errors

Common responses:

- `401 Unauthorized`
  - missing or invalid bearer token
- `403 Forbidden`
  - task belongs to another user and the caller is not admin
- `404 Not Found`
  - task ID does not exist
