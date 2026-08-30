# Search API

Hubuum keeps discovery and structured querying at the central search
resource:

- `GET /api/v1/search` performs grouped plain-text discovery.
- `GET /api/v1/search/stream` performs the same discovery over SSE.
- `POST /api/v1/search` evaluates a versioned structured resource-search DSL.
- `POST /api/v1/search/stream` streams that DSL over SSE.

All four operations are read-only.

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

Use `POST /api/v1/search` or `POST /api/v1/search/stream` when a query needs
typed predicates or boolean expressions that would be ambiguous in a query
string. Both operations accept the same request and execute the same search.
Version 1 searches one resource kind per request: `collection`, `class`,
`object`, `audit_event`, `user`, `group`, or `service_account`. The request must
use `Content-Type: application/json` and is limited to 64 KiB.

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
returns `400`. The server reserves enough of the 64 KiB request limit for the
rest of the compact request envelope before emitting `next`; if a sortable
string is too large to produce a reusable cursor, the search returns `400`
with guidance to use smaller sort values.

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

#### Measured depth-10 baseline

The self-contained `storage_postgres_criterion` benchmark exercises the
production recursive query at the maximum related depth. Its fixture contains
128 independent chains of 11 objects: 1,408 objects and 1,280 relations in
total. The selective case matches one target by exact name; the non-selective
case matches all 128 targets by a case-insensitive substring. Both request 250
rows with `include_total=false`. Fixture creation, migration, `ANALYZE`, and
warmup are outside the timed region.

The reference run on 2026-08-30 used PostgreSQL 18.4 in the pinned benchmark
container, Rust 1.98.0, and an Intel Xeon Silver 4216 host. Criterion collected
100 samples after its normal three-second warmup:

| Target predicate | Matched roots | Median | Median 95% interval |
| --- | ---: | ---: | ---: |
| Exact target name | 1 | 15.636 ms | 15.631–15.645 ms |
| Case-insensitive substring | 128 | 24.039 ms | 24.033–24.044 ms |

These host-specific numbers are a reference baseline, not a latency SLO. Pull
requests compare the same benchmark between base and head. The deterministic
storage test separately pins both cases to one pool checkout, four domain
queries, three transaction-control statements, one recursive result query,
and no count query. This measured workload does not justify another graph
index: the recursive CTE can traverse the existing
`idx_hubuumobject_relation_on_from_to` and
`idx_hubuumobject_relation_on_to` indexes in their respective directions.

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
a new DSL version. Unknown properties on the request, target, expression,
predicate, related predicate, and sort objects remain errors so misspellings do
not silently broaden a query.

### Structured streaming

`POST /api/v1/search/stream` returns `text/event-stream` and accepts the exact
request envelope documented above. It emits these events in order:

- one `started` event before search execution begins
- zero or more `result` events, in the requested stable sort order
- one terminal `done` event after successful execution
- one terminal `error` event instead of results or `done` when execution fails

The payload shapes are:

```text
event: started
data: {"version":1,"kind":"object"}

event: result
data: {"kind":"object","resource":{...}}

event: done
data: {"version":1,"kind":"object","next":"...","total":42,"page_limit":100}
```

An `error` payload contains `version`, `kind`, and the public `message`. Request
envelope failures that can be detected before streaming, such as malformed
JSON, an unsupported DSL version, an oversized body, or an incorrect content
type, use the normal HTTP `ApiError` response. Resolution, authorization, cursor,
and database failures discovered after the SSE response starts use the terminal
`error` event because the HTTP status and headers have already been sent.

Structured results are deliberately page-progressive, not database-row
progressive. Permission-aware reachability, exact totals, and stable ordering
must finish before the first result is final. The completed page releases its
database connection before result events are paced by the client, preventing a
slow or disconnected consumer from occupying the connection pool. A disconnect
before completion drops the pending search future; database statements remain
bounded by the configured statement timeout.

The initial streaming decision uses these measured or enforced bounds:

| Property | Page-progressive behavior |
| --- | --- |
| Time to `started` | Emitted before polling search execution; zero search queries must complete first |
| Time to first `result` | At least the complete authorized page-query latency; 15.636–24.039 ms in the depth-10 reference cases above |
| Connection occupancy under a slow client | Zero after the page future resolves; result pacing owns the completed page, not a database connection |
| Buffered result memory | Linear in the effective page limit; at most 250 rows with the default maximum-page configuration |
| Disconnect before completion | Drops the pending execution future; PostgreSQL work remains bounded by the statement timeout |
| Disconnect during result delivery | Drops only the completed in-memory page and stream state |

Database-row streaming would improve first-result latency only by retaining a
pool connection across client backpressure and by weakening the point at which
authorization and global ordering are final. The measured depth-10 page latency
does not justify that tradeoff for version 1. Revisit this decision if
base/head benchmarks or production pool telemetry show that page completion,
memory, or cancellation behavior is the limiting resource.
