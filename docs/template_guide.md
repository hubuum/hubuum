# Template Guide

Stored report templates format the output from `POST /api/v1/reports`.

Use templates when you want `text/plain`, `text/html`, or `text/csv` output from a stored definition in `POST /api/v1/templates`.

See also:

- [report_api.md](report_api.md) for report execution semantics
- [permissions.md](permissions.md) for template permissions

## What a template receives

Templates render against a context object with these top-level keys:

- `items`
- `meta`
- `warnings`
- `request`

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

- `{{this.name}}`
- `{{this.description}}`
- `{{this.data.owner}}`
- `{{this.data.hostname}}`
- `{{this.data.environment}}`

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

The stored template language is intentionally small:

- `{{path.to.value}}` interpolates a value
- `{{this.name}}` reads from the current item inside a loop
- `{{this.data.tags[0]}}` or `{{this.data.tags.0}}` reads an array element by index
- `{{this.data["field.with.dots"]}}` reads object keys that cannot be written as dot segments
- `{{#each items}}...{{/each}}` iterates arrays
- nested `each` blocks are supported

Path resolution rules:

- `this` starts from the current loop item
- `root` starts from the full template context
- bare paths try `this` first, then `root`
- bracket keys can use single or double quotes, for example `{{this.data['service-owner']}}`

Examples:

```text
{{meta.count}}
{{request.scope.kind}}
{{#each items}}{{this.data.tags[0]}}
{{/each}}
{{#each items}}{{this.data["owner.name"]}}
{{/each}}
{{#each items}}{{this.name}}
{{/each}}
{{#each items}}{{#each this.data.tags}}- {{this}}
{{/each}}{{/each}}
```

## Plain text example

Template:

```text
Report scope: {{meta.scope.kind}}
Rows: {{meta.count}}

{{#each items}}- {{this.name}} owned by {{this.data.owner}}
{{/each}}
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
<ul>{{#each items}}<li><strong>{{this.name}}</strong> - {{this.data.hostname}}</li>{{/each}}</ul>
```

Rendered output:

```html
<h1>Server report</h1>
<ul><li><strong>srv-app-01</strong> - srv-app-01.example.org</li><li><strong>srv-db-01</strong> - srv-db-01.example.org</li></ul>
```

In the current implementation, interpolated values are HTML-escaped automatically for `text/html` output.

## CSV example

Template:

```csv
name,owner,hostname
{{#each items}}{{this.name}},{{this.data.owner}},{{this.data.hostname}}
{{/each}}
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
{{#each items}}{{this.name}}
{{#each this.data.tags}}  - {{this}}
{{/each}}{{/each}}
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

You can also read a single array element directly:

```text
{{#each items}}{{this.name}} primary_tag={{this.data.tags[0]}}
{{/each}}
```

For data keys that contain dots, spaces, or punctuation, use bracket notation:

```text
{{#each items}}{{this.data["owner.name"]}} {{this.data['service tier']}}
{{/each}}
```

## Relation report example

Relation scopes use the same template context. For `related_objects`, each item is a related object with its normal object fields plus a `path` array describing the relation traversal.

Example report request:

```json
{
  "scope": {
    "kind": "related_objects",
    "class_id": 42,
    "object_id": 101
  },
  "query": "depth__lte=2&to_classes=91&sort=path",
  "output": {
    "template_id": 12
  },
  "missing_data_policy": "strict"
}
```

Template:

```text
Related objects for {{request.scope.object_id}}
{{#each items}}- {{this.name}} path={{this.path}} host={{this.data.hostname}}
{{/each}}
```

For direct relation reports, `class_relations` items contain fields such as `from_hubuum_class_id` and `to_hubuum_class_id`, while `object_relations` items contain fields such as `from_hubuum_object_id`, `to_hubuum_object_id`, and `class_relation_id`.

## Included related objects

`objects_in_class` reports can add bounded related-object arrays to each item with `include.related_objects`. Use this when the report is centered on one class but the template needs nearby objects, such as a host's room.

Example report request:

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
        "max_depth": 1,
        "limit": 1
      }
    }
  },
  "output": {
    "template_id": 12
  },
  "missing_data_policy": "strict"
}
```

Template:

```text
{{#each items}}{{this.name}} is in {{this.related.room[0].name}}
{{/each}}
```

The alias (`room` above) becomes `this.related.room`. Included values are arrays even when `limit` is `1`, and each related object includes its normal object fields plus `path`.

## Missing data policy

Missing values are controlled by `missing_data_policy` on the report request.

Example template:

```text
{{#each items}}{{this.name}} owner={{this.data.primary_contact}}
{{/each}}
```

If `primary_contact` does not exist:

- `strict`: the request fails with `400 Bad Request`
- `null`: renders `null` and adds a warning
- `omit`: renders an empty string and adds a warning

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

Warnings are returned in the JSON envelope when the output is JSON, and are counted in the `X-Hubuum-Report-Warnings` response header for rendered text output.

## Limits and constraints

- Stored templates support only `text/plain`, `text/html`, and `text/csv`
- `application/json` does not use stored templates
- Templates do not support conditionals, helpers, or arbitrary expressions
- If a `{{#each ...}}` target is missing or not an array, behavior follows `missing_data_policy`
- Rendered output still respects `limits.max_output_bytes`

## Typical workflow

1. Create a stored template with `POST /api/v1/templates`
2. Reference that template with `output.template_id` in `POST /api/v1/reports`
3. Set `Accept` to match the template content type, or omit it and let the stored template decide

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
  "missing_data_policy": "omit"
}
```
