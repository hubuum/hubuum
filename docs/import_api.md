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

- An unscoped bearer token for a runtime administrator is required.
- Runtime administrators may be humans or service accounts in the configured
  admin group. Service accounts do not gain access to human/IAM administration.
- The worker rechecks admin authority before execution. Use a dedicated backup
  or restore service account for automation.

## Request model

Imports use client-local refs for items created in the same request and natural-key selectors for existing records.

Do not send database IDs such as `collection_id`, `hubuum_class_id`, or `group_id` to wire the graph together.

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
    "collections": [
      {
        "ref": "collection:infra",
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
        "collection_ref": "collection:infra"
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
    "collection_permissions": [
      {
        "ref": "acl:ops-read",
        "collection_ref": "collection:infra",
        "group_key": {
          "identity_scope": "local",
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

- class uses `collection_ref`
- object uses `class_ref`
- permission assignment uses `collection_ref`

Each individual selector pair is exclusive:

- `collection_ref` or `collection_key`
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
    "collection_key": {
      "name": "infra"
    }
  }
}
```

```json
{
  "group_key": {
    "identity_scope": "local",
    "groupname": "ops"
  }
}
```

`GroupKey.identity_scope` defaults to `local` when omitted. Set it explicitly
when targeting a provider-managed group, especially when multiple scopes contain
the same group name.

Selector shapes:

- `CollectionKey`

```json
{
  "name": "infra",
  "path": ["infra"]
}
```

`CollectionKey.path` is optional when the collection name is globally unique.
When different branches contain the same collection name, use `path` to make the
selector unambiguous. Paths are absolute from the system root collection and do
not include the literal `root` segment. For example, the collection
`root / company / it / assets` is selected with:

```json
{
  "name": "assets",
  "path": ["company", "it", "assets"]
}
```

The last `path` segment must match `name`. The root collection itself can be
selected with:

```json
{
  "name": "root",
  "path": []
}
```

- `ClassKey`

```json
{
  "name": "server",
  "collection_key": {
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
    "collection_key": {
      "name": "infra"
    }
  }
}
```

## Supported graph sections

Collection graph imports support:

- `collections`
- `classes`
- `objects`
- `class_relations`
- `object_relations`
- `collection_permissions`

An unscoped administrator token can additionally import identity and integration
state:

- `identity_scopes`
- `groups`
- `principals`
- `group_memberships`
- `export_templates`
- `remote_targets`
- `event_sinks`
- `event_subscriptions`

Class-scoped export templates and remote targets must select a class from their
own target collection. Imported export templates receive the same composed
load-and-render validation as templates created through the template API.
Includes, imports, and inheritance can resolve both existing templates and
templates in the same import, but only within the selected collection.

Section shapes:

### `collections`

| Field | Type | Required | Notes |
| --- | --- | --- | --- |
| `ref` | string | no | Client-local reference for later items in the same request. |
| `name` | string | yes | Collection name within the selected parent. |
| `description` | string | yes | Collection description. |
| `parent_collection_ref` | string | no | Parent collection created earlier in the same request. |
| `parent_collection_key` | object | no | Existing parent collection selector. |

If both parent selectors are omitted, the collection is created or matched under
`root`. At most one parent selector may be set. Existing collection lookup uses
`(parent_collection_id, name)`, so two imports may create the same collection
name under different parents in one request.

### `classes`

| Field | Type | Required | Notes |
| --- | --- | --- | --- |
| `ref` | string | no | Client-local reference for later items in the same request. |
| `name` | string | yes | Class name within the selected collection. |
| `description` | string | yes | Class description. |
| `json_schema` | JSON value | no | Optional JSON Schema for object validation. |
| `validate_schema` | boolean | no | Defaults to `false` when creating a new class. When overwriting an existing class, omitting it preserves the current value. |
| `collection_ref` | string | conditional | Use when the collection is created in the same request. |
| `collection_key` | object | conditional | Use when the collection already exists. |

Exactly one of `collection_ref` or `collection_key` must be set.

`json_schema`, when supplied, must itself be a valid JSON Schema. References
in classes with `validate_schema: true` must be local fragments such as
`#/$defs/address`; external HTTP, file, dynamic, and recursive references cannot
be evaluated. This keeps object validation deterministic and prevents schema
evaluation from accessing external resources.

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
        "collection_key": {
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
          "collection_key": {
            "name": "infra"
          }
        }
      }
    }
  ]
}
```

### `collection_permissions`

| Field | Type | Required | Notes |
| --- | --- | --- | --- |
| `ref` | string | no | Client-local reference. |
| `collection_ref` | string | conditional | Use when the collection is created in the same request. |
| `collection_key` | object | conditional | Use when the collection already exists. |
| `group_key` | object | yes | Existing group selector by identity scope and group name. |
| `permissions` | array of strings | yes | Permission names listed below. |
| `replace_existing` | boolean | no | Defaults to `false`. `false` adds the requested permissions to any existing grant. `true` replaces the existing grant for that collection/group pair. |

Exactly one of `collection_ref` or `collection_key` must be set.

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

### Administrator-only section schemas

The extended identity and integration sections are available only to an
unscoped administrator. Their complete request schemas, including enum values
and nested configuration objects, are published in OpenAPI under the component
names below.

| Section | OpenAPI item schema | Linking and validation notes |
| --- | --- | --- |
| `identity_scopes` | `ImportIdentityScopeInput` | `ref` can be used by later identity records; `name` and `provider_kind` are required. |
| `groups` | `ImportGroupInput` | Select the identity scope with exactly one of `identity_scope_ref` or `identity_scope_key`. |
| `principals` | `ImportPrincipalInput` | Select an identity scope and use the flattened `kind` discriminator. Human records accept either `password` or an Argon2 `password_hash`, never both. Service accounts require an owner group selector. |
| `group_memberships` | `ImportGroupMembershipInput` | Select exactly one principal and one group. Each optional source selects its own identity scope. |
| `export_templates` | `ImportExportTemplateInput` | Select one collection and, for class-scoped exports, one class in that collection. Template composition is validated against existing and same-import templates in the collection. |
| `remote_targets` | `ImportRemoteTargetInput` | Select one collection and any required class in that collection. URL, header, body, authentication, subject, and timeout validation matches the normal API. |
| `event_sinks` | `ImportEventSinkInput` | Sink kind, configuration, secret reference, and feature availability are validated before persistence. |
| `event_subscriptions` | `ImportEventSubscriptionInput` | Select one collection and one sink. Entity types, actions, filters, and routing use the normal subscription validators. |

Selector pairs follow the same exactly-one rule as the core graph. For example,
send `principal_ref` or `principal_key`, `group_ref` or `group_key`, and
`sink_ref` or `sink_key`, but never both members of a pair.

Extended records can carry a `timestamps` object with `created_at` and
`updated_at`. It is intended for trusted migration data. On overwrite, supplied
timestamps replace the stored values; when timestamps are omitted, existing
values are preserved. `updated_at` cannot be earlier than `created_at`.
Membership sources can carry their own timestamps.

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
  - successful items are committed and failed items are exported individually

### `mode.collision_policy`

- default: `abort`
- `abort`
  - existing matching records fail the import or the individual item
- `overwrite`
  - matching records are updated instead of rejected
  - for classes, omitted optional schema settings keep their existing values
  - for objects, the supplied `name`, `description`, and `data` replace the existing values
  - matching group memberships and membership sources preserve existing
    timestamps when omitted and apply supplied restore timestamps when present

### `mode.permission_policy`

This field remains accepted for payload compatibility. API imports require
runtime-admin authority and do not perform per-item authorization, so `abort`
and `continue` currently have no effect on API-submitted tasks.

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
    "item_ref": "collection:infra",
    "entity_kind": "collection",
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
  "entity_kind": "collection_permission",
  "action": "grant",
  "identifier": "infra::ops",
  "outcome": "failed",
  "error": "User does not have permissions [DelegateCollection] on collection 3",
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
