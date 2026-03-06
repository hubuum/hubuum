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
- `class_from=12&sort=created_at.desc`
- `depth__lte=2&class_to=91`

Reports do not support cursor pagination. If `cursor` is present in `query`, the request fails with `400 Bad Request`.

If the rendered response exceeds `limits.max_output_bytes`, the request fails with `413 Payload Too Large`. The server does not stream partial JSON, HTML, CSV, or text bodies.

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

Templates use a minimal template language:

- `{{path.to.value}}` to interpolate a value
- `{{this.name}}` inside loops
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
