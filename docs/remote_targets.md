# Remote Target API

Remote targets let namespace administrators define outbound HTTP actions and let authorized users
invoke one action for one Hubuum object.

The first version is manual invocation only:

- targets are namespace-scoped
- invocations are object-scoped
- execution is queued through the generic task system as `remote_call`
- outbound methods are `get`, `post`, `patch`, and `delete`
- rendered outbound URLs must use `https://`
- auth secrets are referenced by name and resolved from server environment variables at execution time

Endpoints:

- `GET /api/v1/remote-targets`
- `POST /api/v1/remote-targets`
- `GET /api/v1/remote-targets/{target_id}`
- `PATCH /api/v1/remote-targets/{target_id}`
- `DELETE /api/v1/remote-targets/{target_id}`
- `POST /api/v1/classes/{class_id}/objects/{object_id}/remote-targets/{target_id}/invoke`

Related task endpoints:

- `GET /api/v1/tasks/{task_id}`
- `GET /api/v1/tasks/{task_id}/events`

Authentication:

- Bearer token required

## Permissions

Remote target permissions are namespace permissions:

| Permission | Description |
| --- | --- |
| `ReadRemoteTarget` | List or read target definitions in the namespace. |
| `CreateRemoteTarget` | Create targets in the namespace. Also required when moving a target into a namespace. |
| `UpdateRemoteTarget` | Update targets in the namespace. Required on the source namespace when moving a target. |
| `DeleteRemoteTarget` | Delete targets in the namespace. |
| `ExecuteRemoteTarget` | Invoke targets in the namespace. |

Invoking a target also requires `ReadObject` on the object's namespace. The worker re-checks
`ReadObject` and `ExecuteRemoteTarget` for the submitting user before making the outbound call.

## Target model

Example:

```json
{
  "namespace_id": 12,
  "name": "create-ticket",
  "description": "Create a ticket for the object",
  "method": "post",
  "url_template": "https://service.example.com/tickets?asset={{ object.id }}",
  "headers_template": {
    "Content-Type": "application/json",
    "X-Hubuum-Object": "{{ object.name }}"
  },
  "body_template": "{\"summary\":\"{{ object.name }}\",\"data\":{{ object.data | tojson }}}",
  "auth_config": {
    "type": "bearer_secret",
    "secret": "servicenow_token"
  },
  "timeout_ms": 5000,
  "enabled": true
}
```

Fields:

| Field | Type | Required | Notes |
| --- | --- | --- | --- |
| `namespace_id` | integer | yes | Namespace that owns the target and controls permissions. |
| `name` | string | yes | Unique with `namespace_id`. |
| `description` | string | yes | Human-readable description. |
| `method` | string | yes | One of `get`, `post`, `patch`, `delete`. |
| `url_template` | string | yes | MiniJinja template. Rendered URL must be HTTPS. |
| `headers_template` | object | no | JSON object whose values are string templates. Defaults to `{}`. |
| `body_template` | string or null | no | Optional template rendered to the outbound request body. |
| `auth_config` | object | no | Defaults to `{ "type": "none" }`. |
| `timeout_ms` | integer | no | Per-call timeout. Capped by server configuration. Must be greater than zero. |
| `enabled` | boolean | no | Disabled targets cannot be invoked. Defaults to `true`. |

For `get` and `delete`, Hubuum omits the outbound request body unless `body_template` is set.

## Auth configuration

Secrets are never stored in target rows. Targets store a secret reference name, and the worker
resolves the value from an environment variable:

```text
HUBUUM_REMOTE_SECRET_<UPPERCASE_SECRET_NAME>
```

For example, this target auth config:

```json
{
  "type": "bearer_secret",
  "secret": "servicenow_token"
}
```

resolves:

```text
HUBUUM_REMOTE_SECRET_SERVICENOW_TOKEN
```

Supported auth configs:

```json
{ "type": "none" }
```

```json
{
  "type": "bearer_secret",
  "secret": "servicenow_token"
}
```

```json
{
  "type": "basic_secret",
  "username": "api-user",
  "secret": "servicenow_password"
}
```

```json
{
  "type": "api_key_secret",
  "header": "X-API-Key",
  "secret": "inventory_api_key"
}
```

Secret reference names may contain only letters, numbers, and underscores.

## Template context

`url_template`, `headers_template` values, and `body_template` render with the same context:

- `object.id`
- `object.name`
- `object.description`
- `object.namespace_id`
- `object.hubuum_class_id`
- `object.data`
- `class`
- `namespace`
- `parameters`
- `body_override`

Example URL:

```text
https://hooks.example.com/assets/{{ object.id }}?hostname={{ object.data.hostname }}
```

Example body:

```json
"{\"object_id\":{{ object.id }},\"override\":{{ body_override | tojson }}}"
```

The template string must render to the exact outbound body. If the remote endpoint expects JSON,
set `Content-Type` yourself in `headers_template` and render valid JSON from `body_template`.

## Create target

`POST /api/v1/remote-targets`

Returns `201 Created` with the persisted target and `Location: /api/v1/remote-targets/{id}`.

Requires `CreateRemoteTarget` on `namespace_id`.

## List targets

`GET /api/v1/remote-targets`

Returns targets in namespaces where the caller has `ReadRemoteTarget`.

Supported sorting follows the generic cursor pagination model:

- `id`
- `name`
- `description`
- `namespace_id`
- `created_at`
- `updated_at`

Supported filters:

- `id`
- `name`
- `description`
- `namespace_id` or `namespaces`
- `kind` for the HTTP method value
- `created_at`
- `updated_at`

## Update target

`PATCH /api/v1/remote-targets/{target_id}`

Requires `UpdateRemoteTarget` on the current namespace. If `namespace_id` changes, the caller also
needs `CreateRemoteTarget` on the target namespace.

`body_template` is nullable. Omitting it leaves it unchanged; sending `null` clears it.

## Delete target

`DELETE /api/v1/remote-targets/{target_id}`

Requires `DeleteRemoteTarget` on the target namespace.

## Invoke target

`POST /api/v1/classes/{class_id}/objects/{object_id}/remote-targets/{target_id}/invoke`

Request body:

```json
{
  "parameters": {
    "priority": "high"
  },
  "body_override": {
    "comment": "requested by operator"
  }
}
```

Both fields are optional and default to `{}`.

The path class must match the object's class, and the target must belong to the object's namespace.
Disabled targets return `400 Bad Request`.

On success, Hubuum creates a queued task and returns `202 Accepted`:

```json
{
  "id": 42,
  "kind": "remote_call",
  "status": "queued",
  "submitted_by": 7,
  "progress": {
    "total_items": 1,
    "processed_items": 0,
    "success_items": 0,
    "failed_items": 0
  },
  "summary": null,
  "links": {
    "task": "/api/v1/tasks/42",
    "events": "/api/v1/tasks/42/events",
    "import": null,
    "import_results": null,
    "report": null,
    "report_output": null
  },
  "details": null
}
```

The response includes:

```text
Location: /api/v1/tasks/42
```

`Idempotency-Key` is supported. Reusing the same key with the same rendered task submission returns
the same task. Reusing it with a different submission returns `409 Conflict`.

## Execution and result handling

The worker:

1. loads the target, class, object, and namespace
2. re-checks permissions for the submitting user
3. renders URL, headers, and body
4. resolves auth secrets from environment variables
5. performs the outbound HTTP request
6. stores a sanitized result row
7. finalizes the task

Task status rules:

- HTTP `2xx` marks the task `succeeded`
- non-`2xx` marks the task `failed`
- timeout, connection failure, rendering failure, missing secret, disabled target, or permission failure marks the task `failed`

Stored result rows include method, rendered URL, response status, response headers, a capped response
body preview, duration, success flag, and sanitized error text. Secret values are not persisted.

There is no public remote-call result endpoint in this first version. Use the generic task and task
event endpoints to poll status and inspect task-level events.
