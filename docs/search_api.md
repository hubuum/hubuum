# Search API

Hubuum keeps discovery and structured querying at the central search
resource:

- `GET /api/v1/search` performs grouped plain-text discovery.
- `GET /api/v1/search/stream` performs the same discovery over SSE.
- `POST /api/v1/search` evaluates a versioned structured resource-search DSL.

All three operations are read-only.

## Plain-text discovery

Hubuum exposes a grouped unified search API for discovery-oriented clients:

- `GET /api/v1/search`
- `GET /api/v1/search/stream`

Common query parameters:

- `q`: required plain-text query
- `kinds`: optional comma-separated subset of `collection,class,object`
- `limit_per_kind`: optional per-kind page size
- `cursor_collections`, `cursor_classes`, `cursor_objects`: opaque per-kind cursors
- `search_class_schema=true|false`: opt in to class schema text matching
- `search_object_data=true|false`: opt in to object JSON string-value matching

The JSON endpoint returns grouped results and grouped next cursors:

```json
{
  "query": "server",
  "results": {
    "collections": [],
    "classes": [],
    "objects": []
  },
  "next": {
    "collections": null,
    "classes": null,
    "objects": null
  }
}
```

The stream endpoint returns server-sent events:

- `started`
- one `batch` per completed kind, in completion order
- `done`
- `error` if the search fails partway through

The server sends `started` before beginning search work and flushes each batch
as soon as that kind completes. A client disconnect drops the outstanding
batch futures; database statements remain bounded by the configured database
timeout. A terminal `error` event replaces `done` when any batch fails.

## Structured resource search

Use `POST /api/v1/search` when a query needs typed predicates or boolean
expressions that would be ambiguous in a query string. Version 1 searches one
resource kind per request: `collection`, `class`, `object`, `audit_event`, or
`user`, `group`, or `service_account`. The request must use
`Content-Type: application/json` and is limited to 64 KiB.

This is the complete request envelope:

```json
{
  "version": 1,
  "target": {
    "kind": "collection"
  },
  "filter": null,
  "sort": [],
  "limit": 100,
  "cursor": null,
  "include_total": false
}
```

Only `version` and `target` are required. Unknown properties are rejected at
every request level. `filter` and `cursor` may be omitted or `null`; `sort`
defaults to the target's stable default order; `limit` defaults to the server's
configured page limit; and `include_total` defaults to `false`.

### Targets

| Target | JSON | Meaning |
| --- | --- | --- |
| Collection | `{"kind":"collection"}` | Visible collections |
| Class | `{"kind":"class"}` | Visible classes, including their collection projection |
| Object | `{"kind":"object"}` | Visible objects from every class |
| Object in class | `{"kind":"object","class":{"id":42}}` | Visible objects in one exact class |
| Object in named class | `{"kind":"object","class":{"name":"Server"}}` | Visible objects in one exact, uniquely resolved class name |
| Audit event | `{"kind":"audit_event"}` | Visible audit events, with normal indirect-event redaction |
| User | `{"kind":"user"}` | Users; requires a human administrator and an unscoped token |
| Group | `{"kind":"group"}` | Groups; requires a human principal and an unscoped token |
| Service account | `{"kind":"service_account"}` | Service accounts the human caller may manage |

An object class selector is optional and accepts exactly one of `id` or `name`.
The selector is not a fuzzy predicate: the server resolves it before executing
the query. A selected class must itself be visible to the caller.

### Expressions

`filter` is a recursive expression with one of five shapes:

```json
{"op":"and","args":[EXPRESSION, EXPRESSION]}
```

```json
{"op":"or","args":[EXPRESSION, EXPRESSION]}
```

```json
{"op":"not","arg":EXPRESSION}
```

```json
{
  "op": "field",
  "predicate": {
    "field": "name",
    "operator": "icontains",
    "value": "production"
  }
}
```

```json
{
  "op": "related",
  "predicate": {
    "class": {"name": "Room"},
    "filters": [
      {
        "field": "json_data",
        "path": "status",
        "operator": "equals",
        "value": "retired"
      }
    ],
    "depth": 2
  }
}
```

`and` and `or` require at least two child expressions. `not` negates one
complete child expression. `field` applies a predicate to the selected target.
`related` is valid only for object searches.

Expressions use two-valued set semantics: a nullable column or missing JSON
path does not match a positive field predicate, and structural `not` returns
the exact complement within the caller's authorized target universe.

Field names are target-specific. Supplying a valid field from the wrong target,
such as `email` in a collection query, returns `400` before database execution.

| Target | Filter fields | Sort fields | Default sort |
| --- | --- | --- | --- |
| Collection | `id`, `name`, `description`, `created_at`, `updated_at`, `revision` | Same as filters | `id.asc` |
| Class | `id`, `name`, `description`, `collection_id`, `created_at`, `updated_at`, `revision`, `validate_schema`, `json_schema` | All except `validate_schema` and `json_schema` | `id.asc` |
| Object | `id`, `name`, `description`, `collection_id`, `created_at`, `updated_at`, `revision`, `json_data` | All except `json_data` | `id.asc` |
| Audit event | `id`, `occurred_at`, `entity_type`, `entity_id`, `entity_name`, `collection_id`, `action`, `actor_kind`, `actor_user_id`, `initiator_user_id`, `summary`, `metadata` | `id`, `occurred_at` | `occurred_at.desc`, `id.desc` |
| User | `id`, `name`, `identity_scope`, `proper_name`, `email`, `created_at`, `updated_at`, `revision` | Same as filters | `id.asc` |
| Group | `id`, `name`, `description`, `identity_scope`, `managed_by`, `external_key`, `last_sync_attempted_at`, `last_sync_success_at`, `created_at`, `updated_at`, `revision` | `id`, `name`, `description`, `created_at`, `updated_at`, `revision` | `id.asc` |
| Service account | `id`, `name`, `description`, `identity_scope`, `owner_group_id`, `created_by`, `disabled_at`, `created_at`, `updated_at`, `revision` | `id`, `name`, `identity_scope`, `created_at`, `updated_at`, `revision` | `id.asc` |

Related-object filters accept the object fields listed above. Their `filters`
array is an implicit `and`. JSON fields (`json_data`, `json_schema`, and
`metadata`) require a non-empty dotted `path`, such as
`hardware.cpu.count`. Non-JSON fields reject `path`.

### Operators and values

The `operator` property is an enum, not query-string syntax. Version 1 defines:

- Text: `equals`, `iequals`, `contains`, `icontains`, `startswith`,
  `istartswith`, `endswith`, `iendswith`, `like`, `regex`, `in`, `is_null`
- Integer, revision, and timestamp: `equals`, `in`, `gt`, `gte`, `lt`, `lte`,
  `between`, `is_null`
- Boolean: `equals`, `is_null`
- JSON path: the text and ordered comparison operators plus
  `within_network`, `contains_network`, `contains_ip`, `overlaps_network`,
  `inet_equals`, `in`, `all`, `array_length`, `has_key`, and `is_null`

`value` is a JSON string, number, boolean, or an array of those scalar types.
Arrays contain from 1 through 50 values and encode multi-value operators such
as `in` and the two bounds for `between`; string members may not contain commas.
Nested arrays, object-valued entries, and JSON `null` values are rejected.
`is_null` is the only operator that takes no `value`; wrap it in a structural
`not` to express `IS NOT NULL`.

JSON values are interpreted using the existing Hubuum JSON query semantics.
Typed numbers, booleans, RFC 3339 timestamps, IP addresses, and networks use
safe PostgreSQL conversion helpers: a stored value that cannot be converted
does not match rather than aborting the query. `has_key` tests a key below the
selected object path, `all` requires every supplied array value, and
`array_length` compares the selected array's length.

### Object relations

A `related` node means that at least one independently matching target object
is reachable over a permission-visible, bidirectional path. `depth` defaults to
1 and may be from 1 through 10. Wrapping the complete node in `not` means that
no visible matching target exists; it does not mean that an individual target
field differs.

A hidden target or hidden relation never makes a positive existential true. A
visible alternate path remains usable when another possible path is hidden.
The target class must be visible with `ReadClass`; graph traversal requires
`ReadObject` and `ReadObjectRelation` along the path.

### Examples

Find production servers that are not connected to a retired room:

```json
{
  "version": 1,
  "target": {
    "kind": "object",
    "class": {
      "name": "Server"
    }
  },
  "filter": {
    "op": "and",
    "args": [
      {
        "op": "field",
        "predicate": {
          "field": "description",
          "operator": "icontains",
          "value": "production"
        }
      },
      {
        "op": "not",
        "arg": {
          "op": "related",
          "predicate": {
            "class": {
              "name": "Room"
            },
            "filters": [
              {
                "field": "json_data",
                "path": "status",
                "operator": "equals",
                "value": "retired"
              }
            ],
            "depth": 2
          }
        }
      }
    ]
  },
  "sort": [
    {
      "field": "name",
      "direction": "asc"
    }
  ],
  "limit": 100,
  "include_total": false
}
```

Find classes whose JSON schema declares `rack` as a property:

```json
{
  "version": 1,
  "target": {"kind": "class"},
  "filter": {
    "op": "field",
    "predicate": {
      "field": "json_schema",
      "path": "properties",
      "operator": "has_key",
      "value": "rack"
    }
  },
  "sort": [{"field": "name", "direction": "asc"}]
}
```

Find audit events whose summary mentions a failed import:

```json
{
  "version": 1,
  "target": {"kind": "audit_event"},
  "filter": {
    "op": "and",
    "args": [
      {
        "op": "field",
        "predicate": {
          "field": "summary",
          "operator": "icontains",
          "value": "failed"
        }
      },
      {
        "op": "field",
        "predicate": {
          "field": "entity_type",
          "operator": "equals",
          "value": "task"
        }
      }
    ]
  },
  "sort": [{"field": "occurred_at", "direction": "desc"}],
  "limit": 25
}
```

Find users in one identity scope (administrator-only):

```json
{
  "version": 1,
  "target": {"kind": "user"},
  "filter": {
    "op": "field",
    "predicate": {
      "field": "identity_scope",
      "operator": "equals",
      "value": "local"
    }
  },
  "sort": [{"field": "name", "direction": "asc"}],
  "include_total": true
}
```

Find directory-managed groups whose name starts with `platform-`:

```json
{
  "version": 1,
  "target": {"kind": "group"},
  "filter": {
    "op": "and",
    "args": [
      {
        "op": "field",
        "predicate": {
          "field": "managed_by",
          "operator": "equals",
          "value": "ldap"
        }
      },
      {
        "op": "field",
        "predicate": {
          "field": "name",
          "operator": "startswith",
          "value": "platform-"
        }
      }
    ]
  }
}
```

Find enabled service accounts owned by a group the caller can manage:

```json
{
  "version": 1,
  "target": {"kind": "service_account"},
  "filter": {
    "op": "and",
    "args": [
      {
        "op": "field",
        "predicate": {
          "field": "owner_group_id",
          "operator": "equals",
          "value": 42
        }
      },
      {
        "op": "field",
        "predicate": {
          "field": "disabled_at",
          "operator": "is_null"
        }
      }
    ]
  }
}
```

### Response and pagination

Every item is tagged with its resource kind. The `resource` member contains the
same public representation returned by that resource's list endpoint:

```json
{
  "version": 1,
  "kind": "collection",
  "results": [
    {
      "kind": "collection",
      "resource": {
        "id": 7,
        "name": "Infrastructure",
        "description": "Production inventory",
        "parent_collection_id": null,
        "created_at": "2026-08-10T10:00:00",
        "updated_at": "2026-08-10T10:00:00",
        "revision": 1
      }
    }
  ],
  "next": "opaque-cursor",
  "total": 12
}
```

`next` is also returned in `X-Next-Cursor`. When `include_total=true`, `total`
and `X-Total-Count` contain the exact count; otherwise the server skips the SQL
count query, returns `total` as `null`, and omits `X-Total-Count`. Results are stable-sorted with an
automatic ID tie-breaker. A cursor is bound to the canonical request, resolved
object class when present, principal, token, and token revision. Reusing it
after changing the expression, sort, limit, target, or authorization token
returns `400`.

### Authorization

Search never expands what the caller can see:

| Target | Authorization rule |
| --- | --- |
| Collection | `ReadCollection`, including token resource scope |
| Class | `ReadClass` and its collection visibility, including token resource scope |
| Object | `ReadObject` and collection visibility; an explicit target class also requires `ReadClass` |
| Audit event | `ReadAudit`; indirect events retain the same payload redaction as `GET /api/v1/events` |
| User | Human administrator with an unscoped token; service accounts, scoped tokens, and non-admin humans receive `403` |
| Group | Human principal with an unscoped token, matching `GET /api/v1/iam/groups` |
| Service account | Human principal with an unscoped token; administrators see all accounts and other callers see accounts owned by their groups, matching `GET /api/v1/iam/service-accounts` |

Collection, class, and object targets apply the same rules with SQL-backed and
external-policy authorization. Under an external policy backend, candidate
rows are authorized before cursor paging and counting so hidden rows cannot
distort pages or totals. IAM targets deliberately reuse their existing list
endpoint gates and row visibility.

Audit `before` and `after` snapshots are intentionally not searchable. Events
that are visible only through a related collection have those snapshots
redacted, so accepting them as predicates would let result presence or totals
reveal hidden snapshot contents. `metadata` remains searchable because it is
part of every visible audit response.

### Bounds and query planning

Version 1 applies these request limits:

- 64 expression nodes at a maximum nesting depth of 8
- 32 field predicates, including related target filters
- 4 related predicates for object targets, each with at most 16 target filters
- related depth from 1 through 10
- 8 unique sort fields and at most 50 values in one predicate array

Direct boolean predicates compile into typed, bound PostgreSQL expressions for
every target. Object-related leaves use a recursive CTE whose depth is a bind
value, so the SQL text does not grow with traversal depth. The existing relation
indexes cover both traversal directions: the composite
`(from_hubuum_object_id, to_hubuum_object_id)` index covers forward expansion,
and the `to_hubuum_object_id` index covers reverse expansion. No new index is
required by this API.

External-policy collection, class, and object searches examine at most 10,000
candidates before authorization and pagination. External-policy object
relations retain their separate 1,000 target, 10,000 object, and 20,000
relation limits. Requests that exceed a work limit fail with `400` and ask the
caller to narrow the query. PostgreSQL work is bounded by
`HUBUUM_DB_STATEMENT_TIMEOUT_MS`; pool acquisition remains bounded by
`HUBUUM_DB_POOL_ACQUIRE_TIMEOUT_MS`.

### Errors and compatibility

The endpoint uses the normal `ApiError` response shape. Common statuses are:

- `400`: invalid version, target/field/operator combination, expression shape,
  sort, cursor, JSON path, value, or work bound
- `401`: missing or invalid authentication
- `403`: authorization failure, including use of any IAM target by a scoped
  token or service account, or use of the user target by a non-admin human
- `404`: an explicit object class selector did not resolve
- `413`: request body exceeds 64 KiB
- `415`: content type is not `application/json`

Clients must send `version: 1`. New optional fields and new enum members may be
added compatibly within the API version; a future incompatible grammar will use
a new DSL version. Unknown request properties remain errors so misspellings do
not silently broaden a query.

Version 1 returns a completed, globally sorted page. Progressive row streaming
is not part of the POST contract: reachability and stable ordering must finish
before the first row is final, and holding a database connection under client
backpressure would reduce pool capacity. A future `POST /api/v1/search/stream`
can expose progress or completed-page events without claiming that database
rows are progressive.
