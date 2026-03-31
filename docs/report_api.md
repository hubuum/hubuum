# Report API

The report API executes an authorized Hubuum query server-side through the generic task system.

Endpoints:

- `POST /api/v1/reports`
- `GET /api/v1/reports/{task_id}`
- `GET /api/v1/reports/{task_id}/output`

Authentication:

- Bearer token required

## Submission model

```json
{
  "scope": {
    "kind": "objects_in_class",
    "class_id": 42
  },
  "query": "name__contains=server&sort=name",
  "output": {
    "template_id": 12
  },
  "missing_data_policy": "strict",
  "limits": {
    "max_items": 100,
    "max_output_bytes": 262144
  }
}
```

`POST /api/v1/reports` is asynchronous and mirrors `POST /api/v1/imports`:

- it returns `202 Accepted`
- the response body is a generic `TaskResponse`
- the response includes `Location: /api/v1/tasks/{id}`
- `Idempotency-Key` is supported with the same reuse/conflict semantics as imports

Use:

- `GET /api/v1/reports/{task_id}` to fetch the report-task projection
- `GET /api/v1/reports/{task_id}/output` to fetch the stored report output

Stored output is refetchable until cleanup. The output endpoint does not rerun the report.

`GET /api/v1/reports/{task_id}` includes report-specific details when available:

- `details.report.output_url`
- `details.report.output_available`
- `details.report.output_expires_at`
- `details.report.template_name`
- `details.report.output_content_type`
- `details.report.warning_count`
- `details.report.truncated`

### Supported scopes

- `namespaces`
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
- `class_from=12&sort=created_at.desc`
- `depth__lte=2&class_to=91`

Reports do not support cursor pagination. If `cursor` is present in `query`, the request fails with `400 Bad Request`.

If the rendered response exceeds `limits.max_output_bytes`, the report task fails with `413 Payload Too Large`. The server does not stream partial JSON, HTML, CSV, or text bodies.

## Output selection

The server determines the output format at submission time based on:

1. If `output.template_id` is provided, the stored template's `content_type` is used
2. Otherwise, it defaults to `application/json`

Supported output types:

- `application/json`
- `text/plain`
- `text/html`
- `text/csv`

## JSON output

`GET /api/v1/reports/{task_id}/output` returns JSON output as a stable envelope:

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

`text/plain`, `text/html`, and `text/csv` outputs require referencing a stored template via `output.template_id`.

For concrete template examples and example context data, see [template_guide.md](template_guide.md).

Templates use Jinja syntax, including loops, conditionals, expressions, macros, and same-namespace `include`/`extends`/`import`.

The template context contains:

- `items`
- `meta`
- `warnings`
- `request`
- `source`

For templated object reports with relation hydration enabled, objects expose:

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
  - missing lookups fail the report task
- `null`
  - missing lookups render as `null`
- `omit`
  - missing lookups render as an empty string
- rendered missing lookups in `null` and `omit` modes add template warnings that identify the
  stored template where the missing value rendered

Example relation-aware templates for a Host -> Room -> Person layout:

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

For `related_objects`, `items` is `[source]`, so the same templates work when the report is rooted
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

- hydrated relation templates are limited to `depth <= 2`
- the renderer enforces a recursion limit and a MiniJinja fuel budget
- relation hydration enforces a maximum hydrated object count
- successful stored outputs get an `output_expires_at` timestamp at completion time
- background task workers clean up expired stored outputs and append a `cleanup` task event

Relevant env vars are documented centrally in [Quick Start](quick_start.md):

- `HUBUUM_REPORT_OUTPUT_RETENTION_HOURS`
- `HUBUUM_REPORT_OUTPUT_CLEANUP_INTERVAL_SECONDS`
- `HUBUUM_REPORT_TEMPLATE_RECURSION_LIMIT`
- `HUBUUM_REPORT_TEMPLATE_FUEL`
- `HUBUUM_REPORT_TEMPLATE_MAX_OBJECTS`
- `HUBUUM_REPORT_STAGE_TIMEOUT_MS`

## Cost controls

- `limits.max_items` caps rows returned from the scoped query
- `limits.max_output_bytes` caps the rendered response size
- if the result set is truncated, `meta.truncated` is set to `true`

## Response headers

These are returned by `GET /api/v1/reports/{task_id}/output`:

- `X-Hubuum-Report-Warnings`
  - number of warnings emitted during rendering
- `X-Hubuum-Report-Truncated`
  - `true` when the result set was truncated to the configured item limit
