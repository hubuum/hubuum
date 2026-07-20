# Querying against the Hubuum API

Hubuum list endpoints share a common query interface for filtering, sorting, and cursor pagination. These query options are applied in the database, not by loading a full result set into memory first.

The response body for list endpoints remains a plain JSON array. Pagination metadata is returned in response headers.

For endpoint-specific field support, see [query_support_matrix.md](query_support_matrix.md).

## Query syntax

Query parameters are passed as standard query string parameters:

- `field=value` means `field__equals=value`
- `field__operator=value` applies an explicit operator
- filters are combined with `AND`
- repeated `sort` fields are expressed as a comma-separated list

Example:

```text
/api/v1/iam/users?name__contains=alice&email__endswith=@example.org&sort=name.asc
```

## Supported operators

### String fields

- `equals`
- `iequals`
- `contains`
- `icontains`
- `startswith`
- `istartswith`
- `endswith`
- `iendswith`
- `like`
- `regex`
- `in`
- `is_null`

### Numeric and date fields

- `equals`
- `gt`
- `gte`
- `lt`
- `lte`
- `between`
- `in`
- `is_null`

### Array fields

- `equals`
- `contains`
- `is_null`

### Boolean fields

- `equals`
- `is_null`

### IP/network JSON fields

- `within_network`
- `contains_network`
- `contains_ip`
- `overlaps_network`
- `inet_equals`

## Negation

You can negate an operator by prefixing it with `not_`.

Examples:

- `name__not_equals=alice`
- `name__not_icontains=test`
- `created_at__not_between=2026-01-01T00:00:00Z,2026-02-01T00:00:00Z`

## Sorting

Use `sort` to request ordering. `order_by` is accepted as an alias.

Supported forms:

- `sort=id`
- `sort=id.asc`
- `sort=id.desc`
- `sort=-id`
- `sort=collection_id.asc,name.desc`

Notes:

- Sort support is endpoint-specific.
- Cursor pagination requires a stable sort, so Hubuum appends a deterministic tie-breaker automatically.
- If you omit `sort`, each endpoint uses its own default stable sort.
- Some relation endpoints support sorting on contextual fields like `from_*`, `to_*`, `depth`, and `path`.
- Class object lists can sort enabled computed fields with
  `computed.shared.<key>` or the owning user's `computed.personal.<key>`.
  `computed.public.<key>` and `computed.private.<key>` are aliases. See
  [Computed fields](computed_fields.md#reading-computed-values) for visibility,
  null, and pagination semantics. Requests that use computed sorting support at
  most two explicit sort fields.

## Computed filtering

Class object lists can filter enabled computed fields with the same scope and
alias names used for computed sorting:

```text
GET /api/v1/classes/12/?computed.shared.display_name__icontains=edge
GET /api/v1/classes/12/?computed.personal.my_priority__between=10,20
```

Computed filtering is intentionally endpoint-specific. Other list endpoints
reject computed filter parameters instead of silently ignoring them. The
definition's declared result type determines which operators and values are
valid. At most two computed filter parameters may appear in one request.
String, numeric, boolean, object, and array definitions are supported;
see [Computed fields](computed_fields.md#reading-computed-values) for the full
operator table, visibility rules, null behavior, and JSON value syntax.
Computed keys may contain `__`; only a recognized operator at the final
`__<operator>` suffix is parsed as filter syntax.

## Cursor pagination

List endpoints use cursor pagination.

Parameters:

- `limit`: maximum number of items to return
- `sort`: page order
- `cursor`: opaque token returned by a previous response
- `include_total`: whether to run the exact count query and return `X-Total-Count`; defaults to `true`

Limits:

- default page size: `100`
- maximum page size: `250`
- maximum encoded cursor size: `64 KiB`
- maximum JSON cursor nesting depth: `64`
- a positive `limit` above the configured maximum is clamped to the maximum
- `limit=0` remains a `400 Bad Request`

Behavior:

- the current page is returned as a JSON array
- by default, paginated responses include `X-Total-Count` with the exact number of matching results
- set `include_total=false` to skip that count query on latency-sensitive requests; `X-Total-Count` is then omitted
- if another page exists, the response includes `X-Next-Cursor`
- `X-Page-Limit` reports the effective page size after applying the default and maximum
- send that cursor back unchanged to fetch the next page
- if `X-Next-Cursor` is absent, there is no next page
- total pages can be derived client-side as `ceil(X-Total-Count / X-Page-Limit)`
- encoded cursors are limited to 64 KiB; a page whose sort values would exceed
  that limit returns `400 Bad Request`, so clients must select fewer sort fields
  or smaller sortable values
- malformed cursors, including JSON sort values PostgreSQL cannot represent or
  values deeper than 64 nested array or object levels, return `400 Bad Request`

Clients should read the effective default and maximum limits from the public
client configuration endpoint rather than assuming the built-in values shown
above:

```text
GET /api/v1/config
```

The served `/api-doc/openapi.json` also applies the effective values to the
`default` and `maximum` schema constraints for `limit` and unified search's
`limit_per_kind`. The committed `docs/openapi.json` is a build-time snapshot and
normally reflects the built-in defaults.

Example:

```text
GET /api/v1/classes?collections=12&limit=2&sort=id.asc
```

Example response header:

```text
X-Total-Count: 6
X-Next-Cursor: eyJzb3J0cyI6W3siZmllbGQiOiJpZCIsImRlc2NlbmRpbmciOmZhbHNlfV0sInZhbHVlcyI6W3sidHlwZSI6ImludGVnZXIiLCJ2YWx1ZSI6Mn1dfQ
```

Next page:

```text
GET /api/v1/classes?collections=12&limit=2&sort=id.asc&cursor=eyJzb3J0cyI6W3siZmllbGQiOiJpZCIsImRlc2NlbmRpbmciOmZhbHNlfV0sInZhbHVlcyI6W3sidHlwZSI6ImludGVnZXIiLCJ2YWx1ZSI6Mn1dfQ
```

## JSON filtering

JSON filters are only available on endpoints that expose JSON-backed fields such as `json_schema`, `json_data`, `from_json_data`, or `to_json_data`.

### Class `json_schema` example

If a class schema contains a numeric property definition such as:

```json
{
  "properties": {
    "latitude": {
      "type": "number",
      "minimum": -90,
      "maximum": 90
    }
  }
}
```

you can filter classes whose schema defines a latitude minimum below zero:

```text
/api/v1/classes?json_schema__lt=properties,latitude,minimum=0
```

### Object `json_data` example

If objects store payloads such as:

```json
{
  "hostname": "srv-01",
  "status": "active",
  "ip": "10.0.0.10"
}
```

you can filter objects in a class by JSON field value:

```text
/api/v1/classes/12/?json_data__equals=status=active
```

You can also use string-oriented operators for textual JSON values:

```text
/api/v1/classes/12/?json_data__contains=hostname=srv
```

Nested JSON paths use comma-separated keys:

```text
/api/v1/classes/12/?json_data__equals=network,address=10.0.0.10
```

JSON-backed numeric, boolean, and date/datetime values build on the same filter interface used elsewhere:

- numeric/date operators: `equals`, `gt`, `gte`, `lt`, `lte`, `between`
- boolean operators: `equals`

Examples:

```text
/api/v1/classes/12/?json_data__equals=metrics,cpu_count=8
/api/v1/classes/12/?json_data__gte=metrics,cpu_count=4
/api/v1/classes/12/?json_data__equals=flags,enabled=true
/api/v1/classes/12/?json_data__gt=maintenance,window_start=2026-03-01
/api/v1/classes/12/?json_data__between=maintenance,window_start=2026-03-01,2026-03-31
```

Date-oriented JSON filters accept the same date formats as other date filters:

- RFC3339 timestamps such as `2026-03-01T12:30:00Z`
- calendar dates such as `2026-03-01`

`between` uses the same comma-separated `min,max` format as the rest of the query interface.

JSON-backed IP address and CIDR values also support network-aware operators:

- `within_network`
  Matches when the stored IP/network is inside the filter network, including equality.
  Example: stored `10.0.0.10`, filter `10.0.0.0/24` -> match.
  Example: stored `10.0.0.0/25`, filter `10.0.0.0/24` -> match.
- `contains_network`
  Matches when the stored network fully contains the filter IP/network, including equality.
  Example: stored `10.0.0.0/24`, filter `10.0.0.0/25` -> match.
  Example: stored `10.0.0.0/24`, filter `10.0.1.0/24` -> no match.
- `contains_ip`
  Matches when the stored network strictly contains the filter host IP.
  Example: stored `10.0.0.0/24`, filter `10.0.0.10` -> match.
  Example: stored `10.0.0.10`, filter `10.0.0.10` -> no match, because a host does not strictly contain itself.
- `overlaps_network`
  Matches when the stored IP/network overlaps the filter network at all.
  Example: stored `10.0.0.0/24`, filter `10.0.0.64/26` -> match.
  Example: stored `10.0.1.0/24`, filter `10.0.0.0/24` -> no match.
- `inet_equals`
  Matches normalized network equality using PostgreSQL `inet` semantics rather than raw string equality.
  Example: stored `10.0.0.10`, filter `10.0.0.10/32` -> match.
  Example: stored `10.0.0.0/24`, filter `10.0.0.0/25` -> no match.

Examples:

```text
/api/v1/classes/12/?json_data__within_network=network,address=10.0.0.0/24
/api/v1/classes/12/?json_data__contains_network=network,address=10.0.0.0/25
/api/v1/classes/12/?json_data__contains_ip=network,address=10.0.0.10
/api/v1/classes/12/?json_data__overlaps_network=network,address=10.0.0.64/26
/api/v1/classes/12/?json_data__inet_equals=network,address=10.0.0.10
```

### JSON array and structure operators

- `in`
- `all`
- `array_length`
- `has_key`
- `is_null`

JSON fields support operators for arrays, key existence, and null checking.
`in` is aliased as `any`; both names parse to the same operator.

- `in` (alias: `any`): Matches when the stored JSON scalar value is one of the given values, or when a stored JSON array contains any of the given values. Values are comma-separated.
  Example: `json_data__in=status=active,standby` matches if `status` is `"active"` or `"standby"`.
  Example: `json_data__in=tags=web,api` matches if `tags` is `["web", "frontend"]` because `"web"` is present.
- `all`: Matches when the stored JSON array contains all of the given values. Values are comma-separated.
  Example: `json_data__all=tags=web,api` matches only if `tags` contains both `"web"` and `"api"`.
- `array_length`: Matches when the stored JSON array has exactly the given number of elements.
  Example: `json_data__array_length=tags=3` matches if `tags` has exactly 3 elements.
- `has_key`: Matches when the stored JSON object contains the given key, regardless of the key's value (including JSON `null`).
  Example: `json_data__has_key=config=hostname` matches if `config` is an object with a `hostname` key.
- `is_null`: Matches when the stored JSON path is null or does not exist. Unlike other operators, `is_null` does not use a `key=value` format; the entire right-hand side is the JSON path.
  Example: `json_data__is_null=optional_field` matches if `optional_field` is missing or JSON `null`.

Examples:

```text
/api/v1/classes/12/?json_data__in=status=active,standby,maintenance
/api/v1/classes/12/?json_data__all=tags=web,api
/api/v1/classes/12/?json_data__array_length=tags=2
/api/v1/classes/12/?json_data__has_key=config=hostname
/api/v1/classes/12/?json_data__is_null=decommissioned_at
/api/v1/classes/12/?json_data__not_is_null=hostname
```

If the JSON path does not exist, or the stored value cannot be interpreted as the requested JSON type, the filter does not match, but it does not fail the request.

## Aggregated object queries

Object aggregation is a separate read-only collection resource, so the normal
object-list response remains unchanged:

```text
GET /api/v1/classes/{class_id}/object-aggregates
GET /api/v1/classes/by-name/{class_name}/object-aggregates
```

The explicit `by-name` alias applies the same query behavior while treating
numeric-looking class names as names.

Supply `group_by` once for each ordered dimension. Between one and three
dimensions are required. Supported dimensions are:

- `name`
- `description`
- `collection_id`
- `created_at`
- `updated_at`
- `json_data.<path>`, using the existing comma-separated nested path grammar
- `computed.shared.<key>`
- `computed.personal.<key>`

The endpoint accepts the normal non-computed object filters, including scalar,
`json_data`, and `permissions` filters. Computed fields may be grouping
dimensions but are not accepted as source filters. For example, this request
first applies a JSON object filter and then
groups the remaining readable objects by country and shared lifecycle:

```text
GET /api/v1/classes/12/object-aggregates?json_data__equals=status=active&group_by=json_data.location,country&group_by=computed.shared.lifecycle&sort=object_count.desc&limit=50
```

Grouping by `created_at` or `updated_at` uses the exact timestamp. The endpoint
does not accept date bucketing, arbitrary expressions, or object-list sort
fields. Aggregate ordering is selected with one of:

- `sort=dimensions.asc`, the default;
- `sort=dimensions.desc`;
- `sort=object_count.asc`;
- `sort=object_count.desc`.

Count ordering always appends the complete dimension tuple in ascending order
as a deterministic tie-breaker. Cursor tokens are bound to the selected
dimensions and sort; changing either while following a cursor returns
`400 Bad Request`. The cursor limit is calculated for each request from a
common 8 KiB HTTP line budget after reserving its route, non-cursor query
parameters, request-line and response-header framing, separators, and line
terminators. If a JSON or computed value at a page boundary would make the
response header or replay request line exceed that limit, the request returns
`413 Payload Too Large`;
shorten the filters, narrow the grouping dimensions, or choose a page limit
that does not end on that value.

When computed aggregation or an external permission backend requires source object
snapshots, Hubuum streams them into byte-bounded batches. An individual snapshot
larger than 8 MiB returns `413 Payload Too Large`. External authorization does
not retain a database connection during its calls, and its compacted
intermediate aggregate rows are also limited to 8 MiB. Narrow the source
filters or grouping dimensions when either bound is exceeded.

Each response row is self-describing:

```json
[
  {
    "dimensions": [
      {
        "field": "json_data.location,country",
        "state": "value",
        "value": "NO"
      },
      {
        "field": "computed.shared.lifecycle",
        "state": "value",
        "value": "production"
      }
    ],
    "object_count": 37
  }
]
```

Dimension states have explicit meanings:

- `value`: the dimension has a value; `value` preserves its JSON type;
- `null`: the JSON path or computed field produced JSON `null`;
- `missing`: a `json_data` path does not exist;
- `unavailable`: a computed field could not produce a value.

JSON objects and arrays retain their structure and group by PostgreSQL JSONB
equality. The `value` member is omitted for `null`, `missing`, and
`unavailable` states.

Authorization and all supported supplied object filters are applied before
aggregation.
This rule also applies when the permission backend cannot push visibility into
SQL: candidate object snapshots are authorized in bounded batches, and only
the immutable authorized snapshots are grouped. Hidden objects therefore
cannot affect bucket counts or aggregate cardinality, and rows are not reloaded
after authorization.

Computed aggregation snapshots the selected current definitions after the first
object is visible, then evaluates those definitions from the authorized object
snapshots. Only the requested shared keys and the requesting owner's requested
personal keys are loaded. If no object is visible, the endpoint returns an
empty page without resolving the selector, so inaccessible definition metadata
is not disclosed. Once an object is visible, unknown, disabled, inaccessible,
and wrong-class selectors return `400 Bad Request`.

Personal dimensions require a human owner with `ReadClass` access and can only
use that owner's enabled definitions. Grouping does not reload source objects
or perform computed read repair. Service accounts cannot group by personal
fields. Responses depending on computed state include
`Cache-Control: private, no-store`.

Pagination headers describe aggregate rows, not source objects:

- `X-Total-Count` is the total number of aggregate rows and is omitted when
  `include_total=false`;
- `X-Next-Cursor` is present when another aggregate page exists;
- `X-Page-Limit` is the effective aggregate page size.

## Contextual endpoints

Some list endpoints derive part of the query from the path.

Examples:

- `/api/v1/classes/{class_id}/related/classes` always constrains the result to classes connected to the class in the path
- `/api/v1/classes/{class_id}/related/relations` always constrains the result to direct relations touching the class in the path
- `/api/v1/classes/{class_id}/` always constrains the result to objects in that class
- `/api/v1/classes/{class_id}/object-aggregates` always constrains source objects to that class and returns aggregate rows
- `/api/v1/classes/{class_id}/objects/{object_id}/related/objects` always constrains the result to objects connected to the object in the path
- `/api/v1/classes/{class_id}/objects/{object_id}/related/relations` always constrains the result to direct relations touching the object in the path

Some contextual endpoints also accept endpoint-specific query options in addition to the shared filter grammar:

- `/api/v1/classes/{class_id}/objects/{object_id}/related/objects` supports `ignore_classes` with a comma-separated class ID list
- `/api/v1/classes/{class_id}/objects/{object_id}/related/objects` supports `ignore_self_class=true|false` and defaults it to `true`

Permission checks are also applied before returning results, so the effective result set is always the intersection of:

- the path context
- the authenticated caller's permissions
- the query filters you supplied

## Endpoint coverage

The shared query interface is currently used by:

- user lists, user tokens, and user groups
- group lists and group members
- collection lists and collection permission listings
- class lists, class permissions, connected-class listings, direct class-relation listings, objects in class, and aggregated objects in class
- global class relation and object relation lists
- connected-object listings
- direct related-relation listings

For the exact filter and sort fields per endpoint, see [query_support_matrix.md](query_support_matrix.md).
