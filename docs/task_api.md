# Task API

The task API is the generic interface for long-running operations.

Imports are the first task kind implemented today, but the API is designed so later long-running reports, exports, or reindex jobs can use the same status and event model.

The current architecture is:

- generic task framework: task submission state, lifecycle, polling, and event history
- typed per-task-kind result tables: import results live behind import-specific endpoints rather than in a fully generic result table

Endpoints:

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

Only `import` has a public submission endpoint today.

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

## Task events

`GET /api/v1/tasks/{task_id}/events`

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

1. Create a task indirectly through a task-producing endpoint such as `POST /api/v1/imports`
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

### Future report task

A future async report task is expected to reuse:

- the same task state endpoint
- the same event history endpoint
- the same status vocabulary

But it would be expected to expose its own typed output surface, just as imports expose `/api/v1/imports/{task_id}/results`.

Only the task-producing endpoint and any report-specific result links would differ.

## Errors

Common responses:

- `401 Unauthorized`
  - missing or invalid bearer token
- `403 Forbidden`
  - task belongs to another user and the caller is not admin
- `404 Not Found`
  - task ID does not exist
