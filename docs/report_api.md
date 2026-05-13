# Report API

The report API executes an authorized Hubuum query server-side and returns either a JSON envelope or rendered text.

Endpoint:

- `POST /api/v1/reports`

Authentication:

- Bearer token required

## Request model

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
  "include": {
    "related_objects": {
      "room": {
        "class_id": 91,
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
- `from_classes=12&sort=created_at.desc`
- `depth__lte=2&to_classes=91`

Reports do not support cursor pagination. If `cursor` is present in `query`, the request fails with `400 Bad Request`.

If the rendered response exceeds `limits.max_output_bytes`, the request fails with `413 Payload Too Large`. The server does not stream partial JSON, HTML, CSV, or text bodies.

### Relation report examples

Report class relations from one class:

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

Report object relations for relations pointing at one object:

```json
{
  "scope": {
    "kind": "object_relations"
  },
  "query": "to_objects=101&sort=created_at.desc"
}
```

Report objects related to a root object:

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

`related_objects` first verifies that `object_id` belongs to `class_id`, then returns matching related objects. The returned items include the related object fields plus the relation `path`, so templates can render both the object data and how it was reached. The `depth` field is available for filtering and sorting through `query`, but is not included in the rendered item payload.

### Including related objects

`objects_in_class` reports can include related objects for every returned object. This is intended for reports such as "host is in room" where the base report lists hosts and the template needs a small bounded set of related room objects.

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
  }
}
```

Each key under `include.related_objects` is an alias. The alias must match `[A-Za-z_][A-Za-z0-9_]*` and is exposed as an array at `this.related.<alias>`.

```text
{{#each items}}{{this.name}} is in {{this.related.room[0].name}}
{{/each}}
```

`class_id` is required and selects the related object class to include. `max_depth` defaults to `1` and must be between `1` and `10`. `limit` defaults to `1` and must be between `1` and `50`; it is applied per root object and per alias. Missing related objects render as an empty array, so `this.related.room` is always present when the alias was requested.

## Output selection

The server determines the output format based on:

1. If `output.template_id` is provided, the stored template's `content_type` is used
2. Otherwise, the `Accept` header is consulted
3. If no `Accept` header is present, defaults to `application/json`

Supported output types:

- `application/json`
- `text/plain`
- `text/html`
- `text/csv`

If the `Accept` header does not match one of the supported formats, the server returns `406 Not Acceptable`.

## JSON output

JSON output returns a stable envelope:

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

Templates use a minimal template language:

- `{{path.to.value}}` to interpolate a value
- `{{this.name}}` inside loops
- `{{this.data.tags[0]}}` or `{{this.data.tags.0}}` to read array elements
- `{{this.data["field.with.dots"]}}` to read data keys that are not valid dot segments
- `{{#each items}}...{{/each}}` to iterate arrays
- nested loops are supported

The template context contains:

- `items`
- `meta`
- `warnings`
- `request`

Examples:

```text
{{#each items}}{{this.name}}
{{/each}}
```

```html
<ul>{{#each items}}<li>{{this.name}}</li>{{/each}}</ul>
```

```csv
name,owner
{{#each items}}{{this.name}},{{this.data.owner}}
{{/each}}
```

### Missing data policy

- `strict`
  - fail the request if a template lookup is missing
- `null`
  - render `null` and record a warning
- `omit`
  - render an empty string and record a warning

## Cost controls

- `limits.max_items` caps rows returned from the scoped query
- `limits.max_output_bytes` caps the rendered response size
- if the result set is truncated, `meta.truncated` is set to `true`

## Response headers

- `X-Hubuum-Report-Warnings`
  - number of warnings emitted during rendering
- `X-Hubuum-Report-Truncated`
  - `true` when the result set was truncated to the configured item limit
