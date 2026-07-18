# Computed object fields

Hubuum can derive response-only values from the raw JSON in one object's
`data` document. Computed fields are deterministic and cannot read relations,
other objects, database state, time, configuration, or external services. A
computed field cannot reference another computed field.

Definitions are class-bound and use one of two visibility scopes:

- Shared definitions belong to a class. They are evaluated for every object in
  that class and are returned to principals that can read those objects.
- Personal definitions belong to one human user and one class. They are
  evaluated only for their owner. Service accounts cannot create or receive
  personal fields.

Shared and personal keys occupy separate maps, so a personal key never
overrides a shared key. Computed values never become part of canonical object
`data`, object history, or object update events.

## Operations and result types

Definitions use semantics version `1` and a closed operation catalog.

| Operation | Behavior | Allowed result type |
| --- | --- | --- |
| `first_non_null` | First resolved value that is neither missing nor JSON `null` | Any declared type matching the result |
| `sum` | Sum of resolved numeric operands | `number` or `integer` |
| `average` | Arithmetic mean of resolved numeric operands | `number` or `integer` |
| `min` | Smallest resolved numeric operand | `number` or `integer` |
| `max` | Largest resolved numeric operand | `number` or `integer` |
| `all_present` | Whether every path resolves to a non-null value | `boolean` |
| `any_present` | Whether at least one path resolves to a non-null value | `boolean` |
| `count_present` | Number of paths that resolve to non-null values | `integer` |
| `all_present_and_equal` | Whether at least two paths are all present and strictly equal | `boolean` |

Numeric aggregates ignore missing and null operands. An aggregate with no
numeric operands returns `null`, including `sum`. A present non-numeric operand
produces a field-level `non_numeric_operand` error.

Equality is recursive and type-aware. Numbers compare by numeric value, object
key order is ignored, and array order is significant. Strings and booleans
compare exactly. No coercion, trimming, case folding, or Unicode normalization
is performed.

## Paths, keys, and fixed limits

Paths use RFC 6901 JSON Pointer syntax relative to the root of object `data`.
The empty pointer selects the document root. `~0` represents `~`, `~1`
represents `/`, and array indexes cannot contain a leading zero. Missing and
null values are distinct from present falsy values such as `false`, `0`, `""`,
`[]`, and `{}`.

The release-owned limits are fixed so the same definition has the same
semantics on every replica:

| Limit | Value |
| --- | --- |
| Shared definitions per class | 32 |
| Personal definitions per user and class | 16 |
| Paths per definition | 16 |
| Pointer length and tokens | 512 bytes and 32 tokens |
| Field key | `[a-z][a-z0-9_]{0,63}` |
| Label and description | 128 bytes and 2,048 bytes |
| Input per evaluation | 1 MiB |
| Traversed nodes per field | 10,000 |
| Work units per shared or personal scope | 50,000 |
| Output per field and scope | 64 KiB and 256 KiB |

Numbers use up to 34 significant decimal digits, exponents from `-308` through
`308`, and round-half-to-even arithmetic. Values outside that range produce a
field-level `numeric_out_of_range` error. There is no float or string coercion.

Definition validation rejects unknown operations, unknown operation members,
duplicate pointers, invalid arity, incompatible result types, and unsupported
semantics versions. Runtime data errors set that field's value to `null` and
add an entry under `errors`; they do not reject the raw object write or the
entire response page.

Stable runtime error codes are:

- `input_too_large`
- `non_numeric_operand`
- `non_integer_result`
- `result_type_mismatch`
- `numeric_out_of_range`
- `evaluation_limit_exceeded`
- `result_too_large`

## Managing shared definitions

Shared management is class-bound:

```text
GET    /api/v1/classes/{class_id}/computed-fields
POST   /api/v1/classes/{class_id}/computed-fields
PATCH  /api/v1/classes/{class_id}/computed-fields/{field_id}
DELETE /api/v1/classes/{class_id}/computed-fields/{field_id}?expected_revision={revision}
POST   /api/v1/classes/{class_id}/computed-fields/preview
POST   /api/v1/classes/{class_id}/computed-fields/rebuild
```

Reading definitions requires `ReadClass`. Creating, changing, deleting,
previewing, or rebuilding shared definitions requires `UpdateCollection` for the
class's current collection. Clients do not choose the permission used by the
server.

PATCH bodies and DELETE queries carry `expected_revision`. A stale revision
returns `409 Conflict`. A value-affecting change atomically updates the
definition, increments the class evaluation revision, marks it rebuilding, and
queues a bounded internal reindex task. Label-only and description-only changes
do not invalidate materialized values.

The rebuild endpoint reuses an active task for the current evaluation revision.
Use the returned `active_task_id` with the task API to inspect progress. One
rebuild transaction processes
`HUBUUM_COMPUTED_REINDEX_BATCH_SIZE` objects, which defaults to `100` and must
be between `1` and `1000`.

## Managing personal definitions

Human users manage their definitions through the self API:

```text
GET    /api/v1/iam/me/computed-fields?class_id={class_id}
POST   /api/v1/iam/me/computed-fields
PATCH  /api/v1/iam/me/computed-fields/{field_id}
DELETE /api/v1/iam/me/computed-fields/{field_id}?expected_revision={revision}
POST   /api/v1/iam/me/computed-fields/preview
```

Creating, changing, and previewing a personal definition requires `ReadClass`.
An owner who later loses class access can still list and delete the inaccessible
definition. Personal definitions are evaluated on read and are never fanned out
during object writes. User anonymization explicitly removes them.

Preview accepts a typed definition and exactly one of an accessible `object_id`
or explicit sample `data`. Personal preview also requires `class_id`. Preview
does not persist a definition or computed result.

## Reading computed values

Object reads opt in with `include=computed`:

```text
GET /api/v1/classes/{class_id}/{object_id}?include=computed
GET /api/v1/classes/{class_id}/?include=computed
```

The response keeps derived values outside raw `data`:

```json
{
  "id": 123,
  "data": {
    "inventory": {
      "hostname": "server-01"
    }
  },
  "computed": {
    "shared": {
      "revision": 7,
      "materialization_stale": false,
      "values": {
        "display_name": "server-01"
      },
      "errors": {}
    },
    "personal": {
      "values": {},
      "errors": {}
    }
  }
}
```

Enriched responses use `Cache-Control: private, no-store` because personal
identity and definition state can affect them. Service-account responses omit
the `personal` member. Requests without `include=computed` retain the original
object response shape and cache behavior.

The `computed` member is response-only. Supplying it in object create or update
JSON is a `400 Bad Request`, including when its value is `null`.

The class object-list endpoint supports ordering by enabled computed fields:

```text
GET /api/v1/classes/{class_id}/?sort=computed.shared.display_name
GET /api/v1/classes/{class_id}/?sort=computed.personal.my_priority.desc
```

`shared` and `personal` match the response namespaces. `public` and `private`
are accepted as aliases. Computed sorts support the normal ascending,
descending, multi-field, deterministic tie-breaker, and cursor-pagination
behavior. Null or failed results sort first in ascending order and last in
descending order. Object and array results use PostgreSQL JSONB ordering.

A personal sort is available only to the owning human user and still requires
class access. Service accounts cannot sort on personal definitions. The
`include=computed` parameter controls the response shape, not sort
availability; sorting without it still returns the raw object shape. Responses
whose order depends on computed state use `Cache-Control: private, no-store`.

With the default SQL authorization backend, sorting happens in PostgreSQL
before pagination. Current shared materialization is used directly, with live
evaluation only for missing or stale cache rows. Personal sorting evaluates
the owner's entire enabled scope from raw object data without write-time user
fan-out, so scope work and output limits remain identical to response
enrichment. Live evaluation cost grows with the candidate count, object JSON
size, and enabled-scope complexity; materialized shared fields are preferable
for high-volume sorting. Computed-sort query counts are independent of page
size, and requests without computed sorting retain the existing query path.
Computed filtering and declarative indexing are not yet supported. Unsupported
query fields fail validation; Hubuum never filters or sorts computed data after
database pagination on the SQL path.

## Materialization freshness

Shared values are written in the same database transaction as each canonical
object insert or update. A class advisory-lock protocol makes an object write
observe either the complete old definition revision or the complete new one.
A backfill locks and re-reads every object before writing its cache row, so it
cannot restore older source data over a concurrent object update.

A stored shared materialization is stale when any of these conditions holds:

- it is missing, including for an object that predates the definition;
- its class evaluation revision differs from the current revision;
- its recorded class differs from the object's current class;
- its SHA-256 digest differs from the canonical, recursively key-sorted object
  `data` digest.

This can happen during a definition rebuild, after an interrupted or failed
rebuild, while restoring a backup, or after data was changed through an older
writer that did not maintain the cache.

Stale storage never makes an enriched response stale. Hubuum evaluates the
current definitions against the returned raw data immediately, sets
`materialization_stale` to `true`, and attempts a guarded read repair. The next
read normally uses the repaired row. Repair failure is recorded in metrics but
does not replace the correct live value with stale data. A manual rebuild is
available for failed or deliberately refreshed classes.

## Backup, events, and metrics

Backup version 3 includes computed-field definitions as authoritative state.
Class rebuild state and object materializations are excluded as rebuildable
caches. Restore validates all definitions, increments each affected class
revision, and queues rebuild tasks.

Shared definition create, update, and delete operations emit first-class
computed-definition events with class and collection context. Rebuilds do not
emit object update events. Personal definitions and values are excluded from
the shared event stream and metric labels.

The runtime exports these low-cardinality metric families:

- `hubuum_computed_field_evaluations_total`
- `hubuum_computed_field_errors_total`
- `hubuum_computed_field_live_fallbacks_total`
- `hubuum_computed_field_read_repairs_total`
- `hubuum_computed_field_rebuild_batches_total`
- `hubuum_computed_field_rebuild_completions_total`
- `hubuum_computed_field_rebuild_duration_seconds`
