# Remote Target API

Remote targets let collection administrators define outbound HTTP actions and let authorized users
invoke one action for one Hubuum subject.

The first version is manual invocation only:

- targets are collection-scoped
- invocations can target one collection, class, object, class relation, or object relation
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
- `POST /api/v1/remote-targets/{target_id}/invoke`

Related task endpoints:

- `GET /api/v1/tasks/{task_id}`
- `GET /api/v1/tasks/{task_id}/events`

Authentication:

- Bearer token required

## Permissions

Remote target permissions are collection permissions:

| Permission | Description |
| --- | --- |
| `ReadRemoteTarget` | List or read target definitions in the collection. |
| `CreateRemoteTarget` | Create targets in the collection. Also required when moving a target into a collection. |
| `UpdateRemoteTarget` | Update targets in the collection. Required on the source collection when moving a target. |
| `DeleteRemoteTarget` | Delete targets in the collection. |
| `ExecuteRemoteTarget` | Invoke targets in the collection. |

Invoking a target also requires read permission for the selected subject. The worker re-checks
subject read permission and `ExecuteRemoteTarget` for the submitting user before making the
outbound call.

| Subject type | Read permission |
| --- | --- |
| `collection` | `ReadCollection` on the collection. |
| `class` | `ReadClass` on the class collection. |
| `object` | `ReadObject` on the object collection. |
| `class_relation` | `ReadClassRelation` on both endpoint collections. |
| `object_relation` | `ReadObjectRelation` on both endpoint collections. |

`ExecuteRemoteTarget` is checked on the target collection. `ReadRemoteTarget` is not required to
invoke a target by ID. For relation subjects, the target collection must be one of the relation
endpoint collections.

## Target model

Example:

```json
{
  "collection_id": 12,
  "class_id": 34,
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
  "allowed_subject_types": ["object"],
  "timeout_ms": 5000,
  "enabled": true
}
```

Fields:

| Field | Type | Required | Notes |
| --- | --- | --- | --- |
| `collection_id` | integer | yes | Collection that owns the target and controls permissions. |
| `class_id` | integer or null | required for object targets | Class scope for targets whose `allowed_subject_types` includes `object`. Must belong to `collection_id`. Must be null or omitted for non-object targets. |
| `name` | string | yes | Unique with `collection_id`. |
| `description` | string | yes | Human-readable description. |
| `method` | string | yes | One of `get`, `post`, `patch`, `delete`. |
| `url_template` | string | yes | MiniJinja template. Rendered URL must be HTTPS. |
| `headers_template` | object | no | JSON object whose values are string export_templates. Defaults to `{}`. Transport-controlled fields are rejected. |
| `body_template` | string or null | no | Optional template rendered to the outbound request body. |
| `auth_config` | object | no | Defaults to `{ "type": "none" }`. |
| `allowed_subject_types` | array | yes | Non-empty list of allowed subject types. Values are `collection`, `class`, `object`, `class_relation`, and `object_relation`. |
| `timeout_ms` | integer | no | Per-call timeout. Capped by server configuration. Must be greater than zero. |
| `enabled` | boolean | no | Disabled targets cannot be invoked. Defaults to `true`. |

For `get` and `delete`, Hubuum omits the outbound request body unless `body_template` is set.

## Auth configuration

Secrets are never stored in target rows. Targets store a secret reference name, and the worker
resolves the value from an environment variable:

```text
HUBUUM_REMOTE_SECRET_<UPPERCASE_SECRET_NAME>
```

The reference name is configuration metadata, not a secret value. Users with `ReadRemoteTarget` can
see which reference name a target uses, but only the worker reads the corresponding environment
variable value.

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

`url_template`, `headers_template` values, and `body_template` render with the same context.
Every invocation includes:

- `subject_type`
- `subject`
- `parameters`
- `body_override`

Subject-specific keys:

| Subject type | Additional context |
| --- | --- |
| `collection` | `collection` |
| `class` | `class`, `collection` |
| `object` | `object`, `class`, `collection` |
| `class_relation` | `class_relation`, `from_class`, `to_class`, `collections` |
| `object_relation` | `object_relation`, `from_object`, `to_object`, `class_relation`, `from_class`, `to_class`, `collections` |

For object subjects, common fields include:

- `object.id`
- `object.name`
- `object.description`
- `object.collection_id`
- `object.hubuum_class_id`
- `object.data`
- `class`
- `collection`

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

Header templates and API-key authentication may not set routing, framing,
connection-specific, or proxy-authentication fields controlled by Hubuum's HTTP
client. This includes `Host`, `Content-Length`, `Transfer-Encoding`,
`Connection`, `Keep-Alive`, `Proxy-Connection`, `Proxy-Authorization`, `TE`,
`Trailer`, `Upgrade`, and `HTTP2-Settings`.

## Create target

`POST /api/v1/remote-targets`

Returns `201 Created` with the persisted target and `Location: /api/v1/remote-targets/{id}`.

Requires `CreateRemoteTarget` on `collection_id`.

## List targets

`GET /api/v1/remote-targets`

Returns targets in collections where the caller has `ReadRemoteTarget`.

Supported sorting follows the generic cursor pagination model:

- `id`
- `name`
- `description`
- `collection_id`
- `created_at`
- `updated_at`

Supported filters:

- `id`
- `name`
- `description`
- `collection_id` or `collections`
- `kind` for the HTTP method value
- `created_at`
- `updated_at`

## Update target

`PATCH /api/v1/remote-targets/{target_id}`

Requires `UpdateRemoteTarget` on the current collection. If `collection_id` changes, the caller also
needs `CreateRemoteTarget` on the target collection.

`body_template` is nullable. Omitting it leaves it unchanged; sending `null` clears it.

## Delete target

`DELETE /api/v1/remote-targets/{target_id}`

Requires `DeleteRemoteTarget` on the target collection.

## Invoke target

`POST /api/v1/remote-targets/{target_id}/invoke`

Request body:

```json
{
  "subject": {
    "type": "object",
    "class_id": 34,
    "object_id": 56
  },
  "parameters": {
    "priority": "high"
  },
  "body_override": {
    "comment": "requested by operator"
  }
}
```

`subject` is required. `parameters` and `body_override` are optional JSON objects and default to
`{}`.

Supported subject shapes:

```json
{ "type": "collection", "collection_id": 12 }
```

```json
{ "type": "class", "class_id": 34 }
```

```json
{ "type": "object", "class_id": 34, "object_id": 56 }
```

```json
{ "type": "class_relation", "relation_id": 78 }
```

```json
{ "type": "object_relation", "relation_id": 90 }
```

For object subjects, `class_id` must match the object's class and the remote target's persisted
`class_id`. The subject type must be listed in the target's `allowed_subject_types`, and the target
collection must be one of the subject collections. Disabled targets return `400 Bad Request`.

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
    "export": null,
    "export_output": null
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

1. loads the target and resolves the requested subject
2. re-checks subject read permission and target execution permission for the submitting user
3. renders URL, headers, and body
4. validates the rendered URL and screens the destination address (see Outbound safety)
5. resolves auth secrets from environment variables
6. performs the outbound HTTP request
7. stores a sanitized result row
8. finalizes the task

Task status rules:

- HTTP `2xx` marks the task `succeeded`
- non-`2xx` marks the task `failed`
- timeout, connection failure, rendering failure, missing secret, disabled target, blocked destination, or permission failure marks the task `failed`

Stored result rows include subject type, subject ID, method, rendered URL, response status, response
headers, a capped response body preview, duration, success flag, and sanitized error text. Secret
values are not persisted, and sensitive response headers (`Set-Cookie`, `Authorization`,
`WWW-Authenticate`, and similar) are redacted before storage.

## Outbound safety

Outbound calls are constrained to mitigate server-side request forgery (SSRF):

- the rendered URL must parse, use `https`, and carry no embedded credentials
- redirects are never followed; a `3xx` response is treated as the final response
- the destination host is resolved and every resolved address is screened. Calls to private,
  loopback, link-local, unique-local, carrier-grade NAT, or cloud-metadata addresses are refused
  unless `HUBUUM_REMOTE_CALL_ALLOW_PRIVATE_TARGETS` is enabled for the deployment
- the screened address is pinned for the connection, so a host cannot rebind to a private address
  between the screening and the request
- URL, header, and body export templates render with the shared MiniJinja fuel and recursion limits used by
  export templates
- each submitting user can have only a bounded number of queued, validating, or running remote-call
  tasks at once
- the response body is read only up to `HUBUUM_REMOTE_CALL_MAX_RESPONSE_BYTES`; anything beyond that
  is discarded

Relevant configuration:

| Setting | Default | Description |
| --- | --- | --- |
| `HUBUUM_REMOTE_CALL_TIMEOUT_MS` | `10000` | Upper bound on a target's per-call `timeout_ms`. |
| `HUBUUM_REMOTE_CALL_MAX_RESPONSE_BYTES` | `262144` | Maximum response body bytes read and stored as a preview. |
| `HUBUUM_REMOTE_CALL_ALLOW_PRIVATE_TARGETS` | `false` | Allow targets that resolve to private/internal addresses. |
| `HUBUUM_REMOTE_CALL_MAX_ACTIVE_TASKS_PER_USER` | `100` | Maximum queued, validating, or running remote-call tasks per submitting user. |
| `HUBUUM_EXPORT_TEMPLATE_RECURSION_LIMIT` | `64` | Shared MiniJinja recursion limit for export and remote target export_templates. |
| `HUBUUM_EXPORT_TEMPLATE_FUEL` | `50000` | Shared MiniJinja execution fuel budget for export and remote target export_templates. |

There is no public remote-call result endpoint in this first version. Use the generic task and task
event endpoints to poll status and inspect task-level events.
