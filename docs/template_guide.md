# Template Guide

Stored report templates format stored report output from the async report API.

Use templates when you want `text/plain`, `text/html`, or `text/csv` output from a stored definition in `POST /api/v1/templates`.
Submit the report with `POST /api/v1/reports`, then fetch the rendered result from
`GET /api/v1/reports/{task_id}/output`.

See also:

- [report_api.md](report_api.md) for report execution semantics
- [permissions.md](permissions.md) for template permissions

## What a template receives

Templates render against a context object with these top-level keys:

- `items`
- `meta`
- `warnings`
- `request`
- `source`
  - present for templated `related_objects` reports and points at the hydrated root object

Rather than start with the full context object, it is usually easier to think in terms of classes and objects.

## Example classes and objects

Assume you have a class called `server` with objects like these:

```json
[
  {
    "id": 101,
    "name": "srv-app-01",
    "description": "Application server",
    "namespace_id": 7,
    "hubuum_class_id": 42,
    "data": {
      "owner": "alice",
      "hostname": "srv-app-01.example.org",
      "environment": "prod",
      "tags": ["prod", "app"]
    }
  },
  {
    "id": 102,
    "name": "srv-db-01",
    "description": "Database server",
    "namespace_id": 7,
    "hubuum_class_id": 42,
    "data": {
      "owner": "bob",
      "hostname": "srv-db-01.example.org",
      "environment": "prod",
      "tags": ["prod", "db"]
    }
  }
]
```

If you run a report over that class:

```json
{
  "scope": {
    "kind": "objects_in_class",
    "class_id": 42
  },
  "query": "name__contains=srv-&sort=name",
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

then `items` contains those objects, so templates can reference fields like:

- `{{ item.name }}`
- `{{ item.description }}`
- `{{ item.data.owner }}`
- `{{ item.data.hostname }}`
- `{{ item.data.environment }}`

The other top-level values are still available when you need them.

Example:

```json
{
  "meta": {
    "count": 2,
    "truncated": false,
    "scope": {
      "kind": "objects_in_class",
      "class_id": 42,
      "object_id": null
    },
    "content_type": "text/plain"
  },
  "warnings": [],
  "request": {
    "scope": {
      "kind": "objects_in_class",
      "class_id": 42,
      "object_id": null
    },
    "query": "name__contains=srv-&sort=name"
  }
}
```

## Template syntax

Stored templates now use Jinja syntax.

Common features:

- `{{ meta.count }}` interpolates a value
- `{% for item in items %}...{% endfor %}` iterates arrays
- `{% if ... %}...{% endif %}` handles conditionals
- `{% include "name" %}`, `{% import "name" as macros %}`, and `{% extends "name" %}` resolve templates by name within the same namespace
- normal Jinja expressions, filters, `set`, and macros are supported

Examples:

```text
{{ meta.count }}
{{ request.scope.kind }}
{% for item in items %}{{ item.name }}
{% endfor %}
{% for item in items %}{% for tag in item.data.tags %}- {{ tag }}
{% endfor %}{% endfor %}
```

## MiniJinja operators and expressions

Templates use the standard MiniJinja expression language unless otherwise noted below.

Common operators:

- comparisons: `==`, `!=`, `<`, `<=`, `>`, `>=`
- boolean logic: `and`, `or`, `not`
- membership: `in`
- arithmetic: `+`, `-`, `*`, `/`, `//`, `%`
- string concatenation: `~`
- indexing and attribute lookup:
  - `item.name`
  - `item.data.hostname`
  - `items[0]`
  - `item.related["rooms"]`

Common control flow:

- `{% if condition %}...{% elif other %}...{% else %}...{% endif %}`
- `{% for item in items %}...{% endfor %}`
- `{% set value = ... %}`

Common MiniJinja features available here:

- expressions and filters such as `|length`, `|sort`, `|default(...)`
- tests such as `is defined`, `is none`, `is string`, `is sequence`
- macros, `include`, `import`, and `extends`
- curated Hubuum helpers:
  - `|csv_cell`
  - `|tojson`
  - `coalesce(...)`
  - `|default_if_empty(...)`
  - `|format_datetime(...)`
  - `|join_nonempty(...)`

Examples:

```text
{% if item.data.owner is defined and item.data.owner %}
Owner: {{ item.data.owner }}
{% endif %}

{% if "prod" in item.data.tags %}
{{ item.name }} is production
{% endif %}

{{ item.name ~ "@" ~ item.data.hostname }}

{% if host.related.rooms|length > 0 %}
First room: {{ host.related.rooms[0].name }}
{% endif %}

{{ "alice,bob"|csv_cell }}
{{ {"host": item.name, "owner": item.data.owner}|tojson }}
{{ coalesce(item.data.primary_contact, item.data.owner, "unknown") }}
{{ item.updated_at|format_datetime("date") }}
```

What we do not add on top of MiniJinja:

- no custom database lookup functions from templates
- no filesystem template loading
- no cross-namespace template loading
- no relation helper functions such as `related_to(...)`

## Stored template composition

Stored templates can compose other stored templates in the same namespace by name.

Recommended naming for reusable stored templates:

- `layout.<name>`
- `macros.<name>`
- `partial.<name>`
- `report.<name>`

Example layout template:

```html
{# name: layout.html #}
<!doctype html>
<html>
<body>
{% block body %}{% endblock %}
</body>
</html>
```

Example macro template:

```text
{# name: macros.txt #}
{% macro owner(item) %}{{ item.data.owner|default("unknown") }}{% endmacro %}
```

Example child template:

```html
{% extends "layout.html" %}
{% import "macros.txt" as macros %}
{% block body %}
<ul>
{% for item in items %}<li>{{ item.name }} - {{ macros.owner(item) }}</li>{% endfor %}
</ul>
{% endblock %}
```

Composition rules:

- template names are resolved only inside the selected template's namespace
- cross-namespace loading is rejected
- filesystem loading is not available
- names containing `/` or `::` are rejected by the loader

## Plain text example

Template:

```text
Report scope: {{ meta.scope.kind }}
Rows: {{ meta.count }}

{% for item in items %}- {{ item.name }} owned by {{ item.data.owner }}
{% endfor %}
```

Rendered output:

```text
Report scope: objects_in_class
Rows: 2

- srv-app-01 owned by alice
- srv-db-01 owned by bob
```

## HTML example

Template:

```html
<h1>Server report</h1>
<ul>{% for item in items %}<li><strong>{{ item.name }}</strong> - {{ item.data.hostname }}</li>{% endfor %}</ul>
```

Rendered output:

```html
<h1>Server report</h1>
<ul><li><strong>srv-app-01</strong> - srv-app-01.example.org</li><li><strong>srv-db-01</strong> - srv-db-01.example.org</li></ul>
```

Interpolated values are HTML-escaped automatically for `text/html` output.

## CSV example

Template:

```csv
name,owner,hostname
{% for item in items %}{{ item.name }},{{ item.data.owner }},{{ item.data.hostname }}
{% endfor %}
```

Rendered output:

```csv
name,owner,hostname
srv-app-01,alice,srv-app-01.example.org
srv-db-01,bob,srv-db-01.example.org
```

## Nested array example

Given item data like:

```json
{
  "name": "srv-app-01",
  "data": {
    "tags": ["prod", "app"]
  }
}
```

Template:

```text
{% for item in items %}{{ item.name }}
{% for tag in item.data.tags %}  - {{ tag }}
{% endfor %}{% endfor %}
```

Rendered output:

```text
srv-app-01
  - prod
  - app
srv-db-01
  - prod
  - db
```

## Missing data policy

Missing values are controlled by `missing_data_policy` on the report request.

Example template:

```text
{% for item in items %}{{ item.name }} owner={{ item.data.primary_contact }}
{% endfor %}
```

If `primary_contact` does not exist:

- `strict`: the report task fails
- `null`: renders `null`
- `omit`: renders an empty string
- `null` and `omit`: rendered missing lookups add a template warning that names the stored template where the missing lookup rendered

Rendered output with `null`:

```text
srv-app-01 owner=null
srv-db-01 owner=null
```

Rendered output with `omit`:

```text
srv-app-01 owner=
srv-db-01 owner=
```

## Relation-aware object templates

Templated object reports can expose hydrated relation-aware objects.

Hydrated objects keep the normal object fields:

- `id`
- `name`
- `description`
- `namespace_id`
- `hubuum_class_id`
- `data`
- `created_at`
- `updated_at`
- `path`
- `path_objects`

They also add:

- `related`
- `reachable`
- `paths`

`related` is a map of adjacent objects grouped by relation alias. Aliases come from:

1. `forward_template_alias` or `reverse_template_alias` on the class relation when set
2. otherwise, the adjacent class name normalized to lower snake case and pluralized predictably

Inferred aliases are normalized like this:

- `Room` -> `rooms`
- `Person` -> `persons`
- `Policy` -> `policies`
- `Class` -> `classes`
- `Access Policy` -> `access_policies`
- `Person async` -> `person_asyncs`

Relation traversal is bidirectional in templates, so `room.related.hosts` works even if the stored
relation was originally created from `Host` to `Room`.

`reachable` is the flattened companion to `related`:

- `related.*`
  - direct neighbors only
  - preserves the hop-by-hop graph shape
- `reachable.*`
  - direct plus transitive neighbors within the remaining depth budget
  - grouped by the reachable object's class alias
  - deduplicated by object id
  - uses the shortest visible path when the same object is reachable in multiple ways
- `paths.*`
  - direct plus transitive neighbors within the remaining depth budget
  - grouped by the reachable object's class alias
  - preserves multiple visible routes to the same target object
  - each entry exposes both `path` and `path_objects`

Examples:

- `host.related.rooms`
  - the rooms directly adjacent to that host
- `host.reachable.rooms`
  - also the rooms directly adjacent to that host
- `host.reachable.persons`
  - people reachable through one or more intermediate objects, such as `Host -> Room -> Person`

When the same reachable object can be found through multiple visible paths, it appears once in the
reachable alias bucket.

### `related_objects`

Templated `related_objects` reports expose:

- `items`
  - always `[source]`
- `source`
  - the hydrated root object

The default relation depth is `2`. You can override it with:

```json
"relation_context": {
  "depth": 1
}
```

### `objects_in_class`

Templated `objects_in_class` reports expose hydrated roots in `items` only when relation hydration
is enabled explicitly:

```json
"relation_context": {
  "depth": 2
}
```

Without `relation_context`, `items` stays a plain list of objects without `related.*` or
`reachable.*`.

### Host -> Room -> Person example

Assume:

- a `Host` object is related to a `Room`
- that `Room` is related to one or more `Person` objects

For a templated `related_objects` report rooted at a host:

```text
{% for host in items %}
Host: {{ host.name }}
{% for room in host.related.rooms %}
Room: {{ room.name }}
People:
{% for person in room.related.persons %}- {{ person.name }}
{% endfor %}{% endfor %}{% endfor %}
```

If you want the host report to flatten reachable people without manually stepping through rooms,
use `reachable`:

```text
{% for host in items %}
Host: {{ host.name }}
People:
{% if host.reachable.persons is defined %}
{% for person in host.reachable.persons %}- {{ person.name }}
{% endfor %}
{% else %}
- none
{% endif %}
{% endfor %}
```

If `alice` is reachable through both `room-101` and `room-102`, `host.reachable.persons` still
contains `alice` only once.

If you want to keep both branches, use `paths`:

```text
{% for host in items %}
Host: {{ host.name }}
People by path:
{% for person in host.paths.persons %}- {{ person.name }} via {{ person.path_objects[1].name }}
{% endfor %}{% endfor %}
```

In that case the same `alice` object will appear once per visible route, so a host connected to
`alice` through `room-101` and `room-102` yields two `paths.persons` entries.

Rendered output:

```text
Host: host-01
People:
- alice
- bob
```

Use `related` when the report should preserve the path shape and show the intermediate room. Use
`reachable` when the report should flatten the result to "all people this host can reach within the
configured depth".

`reachable.*` aliases only appear when there is at least one visible reachable object for that
class alias, so optional lookups should still be guarded in `strict` mode.

### Using `source`

For templated `related_objects` reports, `source` is the same hydrated root object that also
appears as `items[0]`.

Example:

```text
Host: {{ source.name }}
People:
{% for person in source.reachable.persons %}- {{ person.name }}
{% endfor %}
```

Using `items` keeps templates reusable between `related_objects` and `objects_in_class`. Using
`source` is convenient when a report is always rooted at one object.

Rendered output:

```text
Host: host-01
Room: room-101
People:
- alice
- bob
```

For a class-wide host report, use `objects_in_class` plus `relation_context.depth`:

```text
{% for host in items %}
{% for room in host.related.rooms %}
{% for person in room.related.persons %}
{{ host.name }},{{ room.name }},{{ person.name }}
{% endfor %}{% endfor %}{% endfor %}
```

The flatter class-wide version can use `reachable` instead:

```text
{% for host in items %}
{% for person in host.reachable.persons %}
{{ host.name }},{{ person.name }}
{% endfor %}{% endfor %}
```

If you want a more defensive version that handles missing or empty relations cleanly:

```text
{% for host in items %}
Host: {{ host.name }}
{% if host.related.rooms is defined and host.related.rooms %}
{% for room in host.related.rooms %}
Room: {{ room.name }}
{% if room.related.persons is defined and room.related.persons %}
People:
{% for person in room.related.persons %}- {{ person.name }}
{% endfor %}
{% else %}
People:
- none
{% endif %}
{% endfor %}
{% else %}
Room: none
{% endif %}
{% endfor %}
```

The same pattern also works for HTML and CSV:

```html
<ul>{% for host in items %}<li><strong>{{ host.name }}</strong><ul>{% for room in host.related.rooms %}<li>{{ room.name }}<ul>{% for person in room.related.persons %}<li>{{ person.name }}</li>{% endfor %}</ul></li>{% endfor %}</ul></li>{% endfor %}</ul>
```

```csv
host,room,person
{% for host in items %}{% for room in host.related.rooms %}{% for person in room.related.persons %}{{ host.name }},{{ room.name }},{{ person.name }}
{% endfor %}{% endfor %}{% endfor %}
```

## Limits and constraints

- Stored templates support only `text/plain`, `text/html`, and `text/csv`
- `application/json` does not use stored templates
- Template loading for `include`/`import`/`extends` is limited to the same namespace
- Hydrated relation templates are limited to relation depth `<= 2`
- Rendered output still respects `limits.max_output_bytes`

## CSV note

`text/csv` templates are rendered as plain text.

Hubuum now provides `|csv_cell` for individual CSV cells. Use it instead of hand-written quoting
for fields that may contain commas, quotes, or newlines.

Example:

```csv
name,owner
{% for item in items %}{{ item.name|csv_cell }},{{ item.data.owner|default("")|csv_cell }}
{% endfor %}
```

## Missing fields and warnings

Missing values are controlled by `missing_data_policy`:

- `strict`
  - missing lookups fail the report task
- `null`
  - missing lookups render as literal `null`
- `omit`
  - missing lookups render as an empty string

Current behavior:

- rendered missing template lookups in `null` and `omit` modes add a warning per stored template involved in rendering
- the `warnings` top-level context is still available for report warnings such as truncation
- if you want to avoid failures in `strict` mode, guard optional lookups explicitly with checks such as:
  - `{% if item.data.owner is defined %}{{ item.data.owner }}{% endif %}`
  - `{% if host.related.rooms is defined and host.related.rooms %}...{% endif %}`
  - `{% if host.reachable.persons is defined %}...{% endif %}`

## Typical workflow

1. Create a stored template with `POST /api/v1/templates`
2. Reference that template with `output.template_id` in `POST /api/v1/reports`
3. Read the returned `TaskResponse`, wait for completion, then fetch the rendered result from `GET /api/v1/reports/{task_id}/output`

Example report request using a stored template:

```json
{
  "scope": {
    "kind": "objects_in_class",
    "class_id": 42
  },
  "query": "name__contains=srv-&sort=name",
  "output": {
    "template_id": 12
  },
  "missing_data_policy": "omit",
  "relation_context": {
    "depth": 2
  }
}
```
