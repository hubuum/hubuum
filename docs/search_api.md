# Unified Search

Hubuum exposes a grouped unified search API for discovery-oriented clients:

- `GET /api/v1/search`
- `GET /api/v1/search/stream`

Common query parameters:

- `q`: required plain-text query
- `kinds`: optional comma-separated subset of `namespace,class,object`
- `limit_per_kind`: optional per-kind page size
- `cursor_namespaces`, `cursor_classes`, `cursor_objects`: opaque per-kind cursors
- `search_class_schema=true|false`: opt in to class schema text matching
- `search_object_data=true|false`: opt in to object JSON string-value matching

The JSON endpoint returns grouped results and grouped next cursors:

```json
{
  "query": "server",
  "results": {
    "namespaces": [],
    "classes": [],
    "objects": []
  },
  "next": {
    "namespaces": null,
    "classes": null,
    "objects": null
  }
}
```

The stream endpoint returns server-sent events:

- `started`
- one `batch` per completed kind
- `done`
- `error` if the search fails partway through
