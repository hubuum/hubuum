# Import API

The import API accepts a graph-shaped request, stores it in the generic task framework, and executes it asynchronously.

Endpoints:

- `POST /api/v1/imports`
- `GET /api/v1/imports/{task_id}`
- `GET /api/v1/imports/{task_id}/results`

Related generic task endpoints:

- `GET /api/v1/tasks`
- `GET /api/v1/tasks/{task_id}`
- `GET /api/v1/tasks/{task_id}/events`

`GET /api/v1/tasks` can be used to list import and other task records visible to the caller. Non-admin users only see their own tasks; admins can list all tasks and filter by `kind`, `status`, and `submitted_by`.

Import results are intentionally not exposed through a generic shared task-result endpoint. They live behind the import-specific typed result endpoint:

- `GET /api/v1/imports/{task_id}/results`

Authentication:

- Bearer token required

## Request model

Imports use client-local refs for items created in the same request and natural-key selectors for existing records.

Do not send database IDs such as `namespace_id`, `hubuum_class_id`, or `group_id` to wire the graph together.

Top-level fields:

| Field | Type | Required | Notes |
| --- | --- | --- | --- |
| `version` | integer | yes | Current version is `1`. |
| `dry_run` | boolean | no | Defaults to `false`. |
| `mode` | object | no | Defaults to `{"atomicity":"strict","collision_policy":"abort","permission_policy":"abort"}`. |
| `graph` | object | yes | Holds the import sections described below. Omitted sections default to empty arrays. |

Example:

```json
{
  "version": 1,
  "dry_run": false,
  "mode": {
    "atomicity": "strict",
    "collision_policy": "overwrite",
    "permission_policy": "abort"
  },
  "graph": {
    "namespaces": [
      {
        "ref": "ns:infra",
        "name": "infra",
        "description": "Infrastructure inventory"
      }
    ],
    "classes": [
      {
        "ref": "class:server",
        "name": "server",
        "description": "Server objects",
        "validate_schema": false,
        "namespace_ref": "ns:infra"
      }
    ],
    "objects": [
      {
        "ref": "object:web-01",
        "name": "web-01",
        "description": "Frontend web node",
        "data": {
          "hostname": "web-01",
          "role": "frontend"
        },
        "class_ref": "class:server"
      }
    ],
    "namespace_permissions": [
      {
        "ref": "acl:ops-read",
        "namespace_ref": "ns:infra",
        "group_key": {
          "groupname": "ops"
        },
        "permissions": [
          "ReadCollection",
          "ReadClass",
          "ReadObject"
        ],
        "replace_existing": false
      }
    ]
  }
}
```

## Linking rules

### Refs

Use `ref` when one imported item should be referenced by another imported item.

Examples:

- class uses `namespace_ref`
- object uses `class_ref`
- permission assignment uses `namespace_ref`

Each individual selector pair is exclusive:

- `namespace_ref` or `namespace_key`
- `class_ref` or `class_key`
- `from_class_ref` or `from_class_key`
- `to_class_ref` or `to_class_key`
- `from_object_ref` or `from_object_key`
- `to_object_ref` or `to_object_key`

Send exactly one selector for each target you need to resolve.

### Natural-key selectors

Use `*_key` when the target already exists.

Examples:

```json
{
  "class_key": {
    "name": "server",
    "namespace_key": {
      "name": "infra"
    }
  }
}
```

```json
{
  "group_key": {
    "groupname": "ops"
  }
}
```

Selector shapes:

- `NamespaceKey`

```json
{
  "name": "infra"
}
```

- `ClassKey`

```json
{
  "name": "server",
  "namespace_key": {
    "name": "infra"
  }
}
```

- `ObjectKey`

```json
{
  "name": "web-01",
  "class_key": {
    "name": "server",
    "namespace_key": {
      "name": "infra"
    }
  }
}
```

## Supported graph sections

- `namespaces`
- `classes`
- `objects`
- `class_relations`
- `object_relations`
- `namespace_permissions`

Section shapes:

### `namespaces`

| Field | Type | Required | Notes |
| --- | --- | --- | --- |
| `ref` | string | no | Client-local reference for later items in the same request. |
| `name` | string | yes | Natural key for namespace lookup. |
| `description` | string | yes | Namespace description. |

### `classes`

| Field | Type | Required | Notes |
| --- | --- | --- | --- |
| `ref` | string | no | Client-local reference for later items in the same request. |
| `name` | string | yes | Class name within the selected namespace. |
| `description` | string | yes | Class description. |
| `json_schema` | JSON value | no | Optional JSON Schema for object validation. |
| `validate_schema` | boolean | no | Defaults to `false` when creating a new class. When overwriting an existing class, omitting it preserves the current value. |
| `namespace_ref` | string | conditional | Use when the namespace is created in the same request. |
| `namespace_key` | object | conditional | Use when the namespace already exists. |

Exactly one of `namespace_ref` or `namespace_key` must be set.

### `objects`

| Field | Type | Required | Notes |
| --- | --- | --- | --- |
| `ref` | string | no | Client-local reference for later items in the same request. |
| `name` | string | yes | Object name within the selected class. |
| `description` | string | yes | Object description. |
| `data` | JSON value | yes | Object payload. Validated against the class schema when enabled. |
| `class_ref` | string | conditional | Use when the class is created in the same request. |
| `class_key` | object | conditional | Use when the class already exists. |

Exactly one of `class_ref` or `class_key` must be set.

### `class_relations`

| Field | Type | Required | Notes |
| --- | --- | --- | --- |
| `ref` | string | no | Client-local reference. |
| `from_class_ref` | string | conditional | Use when the source class is created in the same request. |
| `from_class_key` | object | conditional | Use when the source class already exists. |
| `to_class_ref` | string | conditional | Use when the target class is created in the same request. |
| `to_class_key` | object | conditional | Use when the target class already exists. |

Exactly one of `from_class_ref` or `from_class_key` must be set, and exactly one of `to_class_ref` or `to_class_key` must be set.

Example:

```json
{
  "class_relations": [
    {
      "ref": "rel:server-runs-on-rack",
      "from_class_ref": "class:server",
      "to_class_key": {
        "name": "rack",
        "namespace_key": {
          "name": "infra"
        }
      }
    }
  ]
}
```

### `object_relations`

| Field | Type | Required | Notes |
| --- | --- | --- | --- |
| `ref` | string | no | Client-local reference. |
| `from_object_ref` | string | conditional | Use when the source object is created in the same request. |
| `from_object_key` | object | conditional | Use when the source object already exists. |
| `to_object_ref` | string | conditional | Use when the target object is created in the same request. |
| `to_object_key` | object | conditional | Use when the target object already exists. |

Exactly one of `from_object_ref` or `from_object_key` must be set, and exactly one of `to_object_ref` or `to_object_key` must be set.

Example:

```json
{
  "object_relations": [
    {
      "ref": "rel:web-01-rack-a3",
      "from_object_ref": "object:web-01",
      "to_object_key": {
        "name": "rack-a3",
        "class_key": {
          "name": "rack",
          "namespace_key": {
            "name": "infra"
          }
        }
      }
    }
  ]
}
```

### `namespace_permissions`

| Field | Type | Required | Notes |
| --- | --- | --- | --- |
| `ref` | string | no | Client-local reference. |
| `namespace_ref` | string | conditional | Use when the namespace is created in the same request. |
| `namespace_key` | object | conditional | Use when the namespace already exists. |
| `group_key` | object | yes | Existing group selector. |
| `permissions` | array of strings | yes | Permission names listed below. |
| `replace_existing` | boolean | no | Defaults to `false`. `false` adds the requested permissions to any existing grant. `true` replaces the existing grant for that namespace/group pair. |

Exactly one of `namespace_ref` or `namespace_key` must be set.

Allowed permission values:

- `ReadCollection`
- `UpdateCollection`
- `DeleteCollection`
- `DelegateCollection`
- `CreateClass`
- `ReadClass`
- `UpdateClass`
- `DeleteClass`
- `CreateObject`
- `ReadObject`
- `UpdateObject`
- `DeleteObject`
- `CreateClassRelation`
- `ReadClassRelation`
- `UpdateClassRelation`
- `DeleteClassRelation`
- `CreateObjectRelation`
- `ReadObjectRelation`
- `UpdateObjectRelation`
- `DeleteObjectRelation`
- `ReadTemplate`
- `CreateTemplate`
- `UpdateTemplate`
- `DeleteTemplate`

## Execution options

### `dry_run`

- default: `false`
- `true`
  - validates and plans the import
  - creates task state and result rows
  - does not mutate domain data
- `false`
  - executes the import

### `mode.atomicity`

- default: `strict`
- `strict`
  - all imported mutations succeed or the import fails
- `best_effort`
  - successful items are committed and failed items are reported individually

### `mode.collision_policy`

- default: `abort`
- `abort`
  - existing matching records fail the import or the individual item
- `overwrite`
  - matching records are updated instead of rejected
  - for classes, omitted optional schema settings keep their existing values
  - for objects, the supplied `name`, `description`, and `data` replace the existing values

### `mode.permission_policy`

- default: `abort`
- `abort`
  - permission failures stop strict imports and stop best-effort imports once encountered
- `continue`
  - best-effort imports record permission failures and continue

## Submit example

```http
POST /api/v1/imports HTTP/1.1
Authorization: Bearer <token>
Content-Type: application/json
Idempotency-Key: inventory-import-2026-03-07
```

Response:

```json
{
  "id": 12,
  "kind": "import",
  "status": "queued",
  "submitted_by": 7,
  "created_at": "2026-03-07T10:15:22",
  "started_at": null,
  "finished_at": null,
  "progress": {
    "total_items": 4,
    "processed_items": 0,
    "success_items": 0,
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

Response headers:

- `Location: /api/v1/tasks/12`

## Polling flow

1. `POST /api/v1/imports`
2. Poll `GET /api/v1/tasks/{task_id}` until `status` is terminal
3. Read `GET /api/v1/tasks/{task_id}/events` for lifecycle history
4. Read `GET /api/v1/imports/{task_id}/results` for per-item outcomes

That separation reflects the implementation model:

- generic task framework for lifecycle and polling
- typed per-task-kind result tables for detailed outputs

## Import projection

`GET /api/v1/imports/{task_id}` returns the same task-shaped payload for import tasks.

Example:

```json
{
  "id": 12,
  "kind": "import",
  "status": "succeeded",
  "submitted_by": 7,
  "created_at": "2026-03-07T10:15:22",
  "started_at": "2026-03-07T10:15:22",
  "finished_at": "2026-03-07T10:15:23",
  "progress": {
    "total_items": 4,
    "processed_items": 4,
    "success_items": 4,
    "failed_items": 0
  },
  "summary": "Import finished with 4 succeeded and 0 failed items",
  "request_redacted_at": "2026-03-07T10:15:23",
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

## Per-item results

`GET /api/v1/imports/{task_id}/results`

Example:

```json
[
  {
    "id": 101,
    "task_id": 12,
    "item_ref": "ns:infra",
    "entity_kind": "namespace",
    "action": "create",
    "identifier": "infra",
    "outcome": "succeeded",
    "error": null,
    "details": null,
    "created_at": "2026-03-07T10:15:23"
  },
  {
    "id": 102,
    "task_id": 12,
    "item_ref": "object:web-01",
    "entity_kind": "object",
    "action": "create",
    "identifier": "server::web-01",
    "outcome": "succeeded",
    "error": null,
    "details": null,
    "created_at": "2026-03-07T10:15:23"
  }
]
```

Failed item example:

```json
{
  "id": 109,
  "task_id": 14,
  "item_ref": "acl:ops-admin",
  "entity_kind": "namespace_permission",
  "action": "grant",
  "identifier": "infra::ops",
  "outcome": "failed",
  "error": "User does not have permissions [DelegateCollection] on namespace 3",
  "details": null,
  "created_at": "2026-03-07T10:21:05"
}
```

## Idempotency

If `Idempotency-Key` is provided, repeated submissions by the same user with the same key return the same task instead of creating a duplicate queued import.

## Payload retention

The submitted request payload is stored while the task is active so the worker can recover after restart.

After the import reaches a terminal state, the stored payload is redacted:

- `request_payload` is cleared internally
- `request_redacted_at` is set on the task
- summary state, events, and result rows remain available
