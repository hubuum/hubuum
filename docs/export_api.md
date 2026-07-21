# Export API

The export API executes a runtime-administrator Hubuum query server-side through
the generic task system.

Endpoints:

- `POST /api/v1/exports`
- `POST /api/v1/export-templates/{template_id}/exports`
- `GET /api/v1/exports/{task_id}`
- `GET /api/v1/exports/{task_id}/output`

Authentication:

- An unscoped bearer token for a runtime administrator is required.
- Runtime administrators may be humans or service accounts in the configured
  admin group. Service accounts do not gain access to human/IAM administration.
- The worker rechecks admin authority before execution. Dedicated backup
  service accounts are recommended for automated exports.

## Submission model

```json
{
  "scope": {
    "kind": "objects_in_class",
    "class_id": 42
  },
  "query": "name__contains=server&sort=name",
  "include": {
    "related_objects": {
      "room": {
        "class_id": 91,
        "class_relation_id": 77,
        "direction": "outgoing",
        "sort": "name",
        "max_depth": 1,
        "limit": 1
      }
    }
  },
  "missing_data_policy": "strict",
  "limits": {
    "max_items": 100,
    "max_output_bytes": 262144
  }
}
```

`POST /api/v1/exports` is asynchronous, returns JSON output, and mirrors `POST /api/v1/imports`:

- it returns `202 Accepted`
- the response body is a generic `TaskResponse`
- the response includes `Location: /api/v1/tasks/{id}`
- `Idempotency-Key` is supported with the same reuse/conflict semantics and
  1-to-255-byte limit as imports
- if the submitting user already has too many active export tasks, it returns `429 Too Many Requests`

Use:

- `GET /api/v1/exports/{task_id}` to fetch the export-task projection
- `GET /api/v1/exports/{task_id}/output` to fetch the stored export output

Stored output is refetchable until cleanup. The output endpoint does not rerun the export.

Typical client flow:

```text
POST /api/v1/exports
-> 202 Accepted
-> Location: /api/v1/tasks/12

GET /api/v1/tasks/12
-> poll until status is succeeded, failed, partially_succeeded, or cancelled

GET /api/v1/exports/12/output
-> fetch the stored JSON, text, HTML, or CSV output
```

`GET /api/v1/exports/{task_id}` includes export-specific details when available:

- `details.export.output_url`
- `details.export.output_available`
- `details.export.output_expires_at`
- `details.export.template_name`
- `details.export.output_content_type`
- `details.export.warning_count`
- `details.export.truncated`

### Supported scopes

- `collections`
- `classes`
- `objects_in_class`
  - requires `class_id`
- `class_relations`
- `object_relations`
- `related_objects`
  - requires both `class_id` and `object_id`

### Query semantics

`query` uses the same query-string syntax as the list endpoints, but inside the JSON body as a string.

Examples:

- `name__contains=server&sort=name`
- `from_classes=12&sort=created_at.desc`
- `depth__lte=2&to_classes=91`

Exports do not support cursor pagination. If `cursor` is present in `query`, the request fails with `400 Bad Request`.

If the rendered response exceeds `limits.max_output_bytes`, the export task fails with `413 Payload Too Large`. The request-level value must be greater than zero and cannot exceed the server's `HUBUUM_EXPORT_MAX_OUTPUT_BYTES` setting. The server does not stream partial JSON, HTML, CSV, or text bodies.

### Relation export examples

Export class relations from one class:

```json
{
  "scope": {
    "kind": "class_relations"
  },
  "query": "from_classes=42&sort=created_at.desc",
  "limits": {
    "max_items": 50
  }
}
```

Export object relations for relations pointing at one object:

```json
{
  "scope": {
    "kind": "object_relations"
  },
  "query": "to_objects=101&sort=created_at.desc"
}
```

Export objects related to a root object:

```json
{
  "scope": {
    "kind": "related_objects",
    "class_id": 42,
    "object_id": 101
  },
  "query": "depth__lte=2&to_classes=91&sort=path"
}
```

`related_objects` first verifies that `object_id` belongs to `class_id`, then returns matching related objects. JSON output items include the related object fields plus the relation `path`. For templated output, the export is rooted at the source object: `items` is `[source]`, and relation-aware traversal is exposed through `source.related`, `source.reachable`, and `source.paths`. The `depth` field is available for filtering and sorting through `query`, but is not included in the rendered item payload.

### Including related objects

`objects_in_class` exports can include related objects for every returned object. This is intended for exports such as "host is in room" where the base export lists hosts and the template needs a small bounded set of related room objects.

```json
{
  "scope": {
    "kind": "objects_in_class",
    "class_id": 42
  },
  "query": "name__equals=nommo",
  "include": {
    "related_objects": {
      "room": {
        "class_id": 91,
        "class_relation_id": 77,
        "direction": "outgoing",
        "sort": "name",
        "max_depth": 1,
        "limit": 1
      }
    }
  },
}
```

Each key under `include.related_objects` is an alias. The alias must match `[A-Za-z_][A-Za-z0-9_]*`, and a request can include at most 8 aliases. Aliases are exposed as arrays at `item.related.<alias>` in MiniJinja export templates and as `related.<alias>` in JSON export items. The top-level `related` export item field is reserved for export includes.

```text
{% for item in items %}{{ item.name }} is in {{ item.related.room[0].name }}
{% endfor %}
```

`class_id` is required and selects the related object class to include. `class_relation_id` is optional and restricts traversal to a specific class relation. `direction` is optional and can be `any` (default), `outgoing`, or `incoming`. `sort` is optional and can be `path` (default), `name`, or `created_at`; it decides which related objects are kept first when `limit` is smaller than the number of matches.

`max_depth` defaults to `1` and must be between `1` and `10`. `limit` defaults to `1` and must be between `1` and `50`; it is applied per root object and per alias. Missing related objects render as an empty array, so `item.related.room` is always present in export templates when the alias was requested.

## Template execution

Text, HTML, and CSV exports are executed from executable export templates:

```json
{
  "query": "name__contains=server&sort=name",
  "missing_data_policy": "strict",
  "limits": {
    "max_items": 100,
    "max_output_bytes": 262144
  }
}
```

For `related_objects` export_templates, pass the runtime root object:

```json
{
  "object_id": 101,
  "query": "depth__lte=2&to_classes=91&sort=path"
}
```

The template stores the scope, class, include settings, relation context, content type, and default
query/limits/policy. Runtime `query` replaces the template's default query when supplied.

Executable export templates support every export scope kind:

- `objects_in_class` and `related_objects` are bound to a single class and require the template's `class_id`.
- `collections`, `classes`, `class_relations`, and `object_relations` are class-agnostic and must not set `class_id`.

`object_id` is only accepted at run time for `related_objects` export_templates; supplying it for any other scope
is rejected with `400 Bad Request`.

## Output selection

The server determines the output format at submission time based on the endpoint:

1. `POST /api/v1/exports` returns `application/json`
2. `POST /api/v1/export-templates/{template_id}/exports` returns the template's stored `content_type`

Supported output types:

- `application/json`
- `text/plain`
- `text/html`
- `text/csv`

## JSON output

`GET /api/v1/exports/{task_id}/output` returns JSON output as a stable envelope:

```json
{
  "items": [
    {
      "id": 1,
      "name": "srv-01"
    }
  ],
  "meta": {
    "count": 1,
    "truncated": false,
    "scope": {
      "kind": "objects_in_class",
      "class_id": 42,
      "object_id": null
    },
    "content_type": "application/json"
  },
  "warnings": []
}
```

## Template output

`text/plain`, `text/html`, and `text/csv` outputs require running an executable stored template with
`POST /api/v1/export-templates/{template_id}/exports`.

For concrete template examples and example context data, see [export_template_guide.md](export_template_guide.md).

Templates use Jinja syntax, including loops, conditionals, expressions, macros, and same-collection `include`/`extends`/`import`.

The template context contains:

- `items`
- `meta`
- `warnings`
- `request`
- `source`

For templated object exports with relation hydration enabled, objects expose:

- the normal object fields
- `related`
- `reachable`
- `paths`
- `path_objects`

`related` groups adjacent objects by relation alias. Alias precedence is:

1. explicit `forward_template_alias` / `reverse_template_alias` on the class relation
2. otherwise, the inferred class alias such as `rooms`, `persons`, `policies`, or `classes`

`reachable` groups direct and transitive reachable objects by class alias within the configured
depth. Reachable results are deduplicated by object id. Reachable aliases only appear when there is
at least one visible match for that class alias.
`paths` is the path-preserving companion to `reachable`: it groups direct and transitive reachable
objects by class alias but keeps one entry per visible route instead of deduplicating by target
object id.

Activation rules:

- `related_objects`
  - relation hydration is enabled automatically for templated output
  - `items` becomes `[source]`
  - `source` is the hydrated root object
- `objects_in_class`
  - relation hydration is disabled by default
  - add `"relation_context": { "depth": 1 | 2 }` to enable `related.*` and `reachable.*`
- all other scopes keep plain `items` and do not expose relation-aware traversal

Missing-field behavior:

- `strict`
  - missing lookups fail the export task
- `null`
  - missing lookups render as `null`
- `omit`
  - missing lookups render as an empty string
- rendered missing lookups in `null` and `omit` modes add template warnings that identify the
  stored template where the missing value rendered

Example relation-aware export templates for a Host -> Room -> Person layout:

```text
{% for host in items %}
Host: {{ host.name }}
{% for room in host.related.rooms %}
Room: {{ room.name }}
People:
{% for person in room.related.persons %}- {{ person.name }}
{% endfor %}{% endfor %}{% endfor %}
```

If you want to flatten the transitive people lookup and skip the intermediate room loop, use
`reachable`:

```text
{% for host in items %}
Host: {{ host.name }}
People:
{% if host.reachable.persons is defined %}
{% for person in host.reachable.persons %}- {{ person.name }}
{% endfor %}
{% else %}- none
{% endif %}{% endfor %}
```

If you want to preserve multiple Host -> Room -> Person routes, use `paths`:

```text
{% for host in items %}
Host: {{ host.name }}
People by path:
{% for person in host.paths.persons %}- {{ person.name }} via {{ person.path_objects[1].name }}
{% endfor %}{% endfor %}
```

```html
<ul>{% for host in items %}<li><strong>{{ host.name }}</strong><ul>{% for room in host.related.rooms %}<li>{{ room.name }}<ul>{% for person in room.related.persons %}<li>{{ person.name }}</li>{% endfor %}</ul></li>{% endfor %}</ul></li>{% endfor %}</ul>
```

```csv
host,room,person
{% for host in items %}{% for room in host.related.rooms %}{% for person in room.related.persons %}{{ host.name }},{{ room.name }},{{ person.name }}
{% endfor %}{% endfor %}{% endfor %}
```

For `related_objects`, `items` is `[source]`, so the same export_templates work when the export is rooted
at a single host object. For `objects_in_class`, add `"relation_context": { "depth": 2 }` to
enable `related.*` and `reachable.*`.

### Missing data policy

- `strict`
  - fail the request if a template lookup is missing
- `null`
  - render `null`
- `omit`
  - render an empty string

## Render guards and cleanup

- hydrated relation export templates are limited to `depth <= 2`
- the renderer enforces a recursion limit and a MiniJinja fuel budget
- relation hydration enforces a maximum hydrated object count
- `HUBUUM_EXPORT_STAGE_TIMEOUT_MS` is a **post-completion rejection budget**, not
  an in-flight interrupt: an export is rejected only *after* a stage (query,
  hydration, render) finishes if that stage exceeded the budget. It bounds how
  long a stage is *accepted* to have taken, not how long it is *allowed to run*.
- to actually cancel slow in-flight queries server-side, set
  `HUBUUM_DB_STATEMENT_TIMEOUT_MS` (0 = disabled). This is a **pool-global**
  Postgres `statement_timeout`: it applies to every database query the service
  makes, not only export stages, so choose a value that accommodates legitimate
  long-running operations (e.g. large imports).
- to cancel slow in-flight queries **only while executing exports**, set
  `HUBUUM_EXPORT_DB_STATEMENT_TIMEOUT_MS` (0 = disabled). This is a
  **export-scoped** Postgres `statement_timeout`, applied as a transaction-local
  `SET LOCAL` on export queries (scope query, includes, relation hydration), so
  it bounds export queries aggressively without capping imports, admin
  operations, or other DB work sharing the pool. When set it should typically be
  `<= HUBUUM_EXPORT_STAGE_TIMEOUT_MS`.
- successful stored outputs get an `output_expires_at` timestamp at completion time
- background task workers clean up expired stored outputs and append a `cleanup` task event

Relevant env vars are documented centrally in [Quick Start](quick_start.md):

- `HUBUUM_EXPORT_OUTPUT_RETENTION_HOURS`
- `HUBUUM_EXPORT_OUTPUT_CLEANUP_INTERVAL_SECONDS`
- `HUBUUM_EXPORT_MAX_ACTIVE_TASKS_PER_USER`
- `HUBUUM_EXPORT_TEMPLATE_RECURSION_LIMIT`
- `HUBUUM_EXPORT_TEMPLATE_FUEL`
- `HUBUUM_EXPORT_TEMPLATE_MAX_OBJECTS`
- `HUBUUM_EXPORT_MAX_OUTPUT_BYTES`
- `HUBUUM_EXPORT_STAGE_TIMEOUT_MS`
- `HUBUUM_DB_STATEMENT_TIMEOUT_MS`
- `HUBUUM_EXPORT_DB_STATEMENT_TIMEOUT_MS`

## Cost controls

- `limits.max_items` caps rows returned from the scoped query
- `limits.max_output_bytes` caps the rendered response size up to the server maximum
- `HUBUUM_EXPORT_MAX_ACTIVE_TASKS_PER_USER` caps active queued/validating/running export tasks per submitting user
- if the result set is truncated, `meta.truncated` is set to `true`

## Response headers

These are returned by `GET /api/v1/exports/{task_id}/output`:

- `X-Hubuum-Export-Warnings`
  - number of warnings emitted during rendering
- `X-Hubuum-Export-Truncated`
  - `true` when the result set was truncated to the configured item limit
