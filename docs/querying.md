# Querying against the Hubuum API

Hubuum list endpoints share a common query interface for filtering, sorting, and
cursor pagination. The selected storage backend executes these operations.
PostgreSQL applies them in the database, preserving native indexes and query
planning.

The response body for list endpoints remains a plain JSON array. Pagination metadata is returned in response headers.

For endpoint-specific field support, see [query_support_matrix.md](query_support_matrix.md).
The mutation-side contract is documented in
[Resource revisions and conditional mutations](resource_revisions.md).

## Try it with Atlas

Load the [Atlas dataset](getting-started/example-dataset.md) to use the same
objects as the other guides. This query returns web-01 followed by web-02:

```http
GET /api/v1/classes/by-name/Server/objects?name__startswith=web-&sort=name
Authorization: Bearer <token>
```

Use `limit=1` to page through them and pass the response's `X-Next-Cursor` as
`cursor` on the next request while preserving the filter and sort. The corpus
checks verify the filtered result and pagination against the server.

## Query syntax

Query parameters are passed as standard query string parameters:

- `field=value` means `field__equals=value`
- `field__operator=value` applies an explicit operator
- filters are combined with `AND`
- repeated `sort` fields are expressed as a comma-separated list
- one common resource query may contain at most 128 parameters and 64 filters

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

### Regular expressions

`regex` and `not_regex` use the selected backend's native dialect. PostgreSQL
uses [advanced regular expressions (ARE)](https://www.postgresql.org/docs/current/functions-matching.html#FUNCTIONS-POSIX-REGEXP);
memory uses the Rust [`regex` crate](https://docs.rs/regex/latest/regex/#syntax).
This is the supported contract, including for ordinary string fields, JSON
text, computed strings, related predicates and structured-search predicates
where those fields support regex. There is no portable mode or escape
translation. Existing PostgreSQL patterns remain valid.

A regex searches anywhere in the value unless anchored. Matching is case
sensitive by default; use the dialect's inline flags, such as `(?i)`, to change
it. `not_regex` negates a successful non-null match test. Missing JSON paths,
JSON nulls and unavailable computed values do not match either operator.
JSON values use the existing [text conversion](#filter-types-and-stored-value-conversion).

| Pattern and input | PostgreSQL | Memory |
| --- | --- | --- |
| `out` against `router` | Matches | Matches |
| `(?i)^ROUTER$` against `router` | Matches | Matches |
| `\yrouter\y` against `router` | Matches | Invalid pattern |
| `router.$` against `router` followed by a newline | Matches | No match |
| `(?s)router.$` against `router` followed by a newline | Matches | Matches |
| `^router$` against `router` followed by a newline | No match | No match |
| `^(router)\1$` against `routerrouter` | Matches | Invalid pattern |
| `router(?=$)` against `router` | Matches | Invalid pattern |
| `^\p{Greek}+$` against `αβ` | Invalid pattern | Matches |

Inline flag names are dialect-specific. PostgreSQL character classes and case
behavior depend on the expression's collation and database locale; POSIX regex
does not support nondeterministic collations. Memory uses Unicode character
classes and simple Unicode case folding, independent of database locale.
Neither backend normalizes Unicode. Before moving saved queries between
backends, test both pattern acceptance and results, especially word boundaries,
newlines, non-ASCII classes and case-insensitive matches. Translating a few
escapes does not establish equivalence.

Detected invalid patterns return `400 Bad Request` (`InvalidInput` in storage).
Error text is not a cross-backend contract. Memory compiles patterns before
scanning rows. PostgreSQL evaluates patterns in native queries, so an empty or
short-circuited query need not detect an invalid pattern; a successful empty
result is not a pattern-validation API.

Regex work is subject to existing request and query budgets. PostgreSQL uses
`HUBUUM_DB_STATEMENT_TIMEOUT_MS` (30,000 ms by default; zero disables the
application-configured timeout); regex has no separate execution deadline. Memory compiles
each filter once per storage operation using the regex library's default size
and nesting limits. Compilation-limit failures are input errors. Matching has
the library's `O(m × n)` bound for compiled pattern size `m` and input length
`n`, not a wall-clock deadline. A page limit alone does not bound the rows
examined, and neither backend promises a regex speedup or index use.

### Numeric and date fields

- `equals`
- `gt`
- `gte`
- `lt`
- `lte`
- `between`
- `in`
- `is_null`

Integer lists accept comma-separated values and inclusive ranges, including
negative ranges such as `-6--2`. A single filter may expand to at most 1,024
unique integers; larger ranges are rejected before they are materialized. Some
operators and endpoints enforce smaller limits. Prefer `between` for one large
continuous interval.

Revisioned resource and temporal-history lists expose the positive `BIGINT`
field `revision`. It supports exact, `in`, comparison, and `between` filters,
plus ascending or descending sorting:

```text
revision=17
revision__in=15,16,17
revision__gte=10&revision__lt=20
revision__between=10,20
sort=-revision
```

Revision parsing is independent of 32-bit resource IDs. Zero, negative,
overflowing, malformed, and oversized list values return `400 Bad Request`.
Audit events additionally support `before_revision` and `after_revision`
filters, but do not support a global revision sort because event revisions
belong to different resources.

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
- A query may request at most eight distinct sort fields. Repeating the same
  field, including with a different direction, is rejected.
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

Class object lists and class object-aggregate queries can filter enabled
computed fields with the same scope and alias names used for computed sorting:

```text
GET /api/v1/classes/12/?computed.shared.display_name__icontains=edge
GET /api/v1/classes/12/?computed.personal.my_priority__between=10,20
GET /api/v1/classes/12/object-aggregates?computed.shared.lifecycle__equals=active&group_by=description
```

Computed filtering is intentionally endpoint-specific. Other endpoints reject
computed filter parameters instead of silently ignoring them. The
definition's declared result type determines which operators and values are
valid. At most two computed filter parameters may appear in one request.
String, numeric, boolean, object, and array definitions are supported;
see [Computed fields](computed_fields.md#reading-computed-values) for the full
operator table, visibility rules, null behavior, and JSON value syntax.
Computed keys may contain `__`; only a recognized operator at the final
`__<operator>` suffix is parsed as filter syntax.

## Filtering class objects by related targets

The two class object-list routes support bounded, bidirectional relationship
filters:

```text
GET /api/v1/classes/{class_id}/
GET /api/v1/classes/by-name/{class_name}/objects
```

Every related target is a named group. The group alias correlates its class,
object, and depth parameters:

```text
related.<alias>.class.name=Room
related.<alias>.object.name__iequals=foo
related.<alias>.depth__lte=5
```

Use exactly one unnegated equality class selector in each group:
`class.name=<name>` or `class.id=<id>`. The alias must start with an ASCII
letter or `_`, may then contain ASCII letters, digits, and `_`, and may be at
most 64 characters. A request may contain at most four groups.

Object predicates in one group must match the same related target object. The
supported target fields are:

- `id`
- `name`
- `description`
- `collection_id`
- `created_at`
- `updated_at`
- `revision`
- `json_data`

Each field accepts its normal type-appropriate operators. `json_data` uses the
same `path=value` grammar and JSON operators documented below. Related fields
cannot be used for sorting.

Groups are independent existential predicates and are combined with `AND`.
For example, this returns hosts related to a room named `foo`, a person named
`bar`, and a person named `zoot`:

```text
GET /api/v1/classes/by-name/Host/objects?related.room.class.name=Room&related.room.object.name=foo&related.bar.class.name=Person&related.bar.object.name=bar&related.zoot.class.name=Person&related.zoot.object.name=zoot
```

The two person aliases may resolve to distinct targets. The same target may
also satisfy more than one compatible group. Ordinary filters and computed
filters apply to the returned host objects and are combined with the related
groups using `AND`.

`depth__lte` is optional and is the only supported depth operator. It defaults
to `1`, has a hard maximum of `10`, and is evaluated independently for each
group. Paths may traverse objects of any class in either relation direction.
The target itself is not considered related to itself.

Negated target-field operators retain existential semantics. For example,
`related.person.object.name__not_equals=bar` means that a visible related
person exists whose name is not `bar`; it does not mean that no related person
is named `bar`. Boolean combinations such as `OR`, group-level `NOT`, and
distinct-target requirements are reserved for a structured search API.

Authorization applies to the whole path. The target class and target object,
every traversed object, and every traversed object relation must be visible to
the caller. A hidden path is ignored, while a fully visible alternative path
may still satisfy the group. A valid query whose target does not exist or is
not visible returns an empty result instead of disclosing the target.

Related filtering adds a bounded graph walk to both the page query and, unless
`include_total=false`, the exact count query. Use the smallest useful depth and
set `include_total=false` on latency-sensitive requests that do not need
`X-Total-Count`.

External policy authorization traverses the graph level by level so hidden
paths are rejected before they can expand the next frontier. Each group has a
safety budget of 1,000 candidate targets, 10,000 examined objects, and 20,000
examined relations. A query that exceeds a budget returns `400 Bad Request`;
narrow the target predicates or reduce the requested depth.

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

Nested JSON paths use comma-separated keys. Every path segment must be
non-empty and contain only ASCII letters, digits, `_`, or `$`:

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

### Filter types and stored-value conversion

Query operands and stored JSON values have different rules. Ordinary resource
fields have fixed types. For JSON filters, the operator and query operand select
the comparison type; a class schema does not override that inference.
PostgreSQL's native conversions and type limits are authoritative for backend
support. Memory implements the common forms below within its own direct object
filters; other adapters need not pay for this compatibility work.

| Comparison | Query operand | Stored JSON value |
| --- | --- | --- |
| Numeric | JSON `equals` and range comparisons currently require signed 32-bit integers; `between` requires two bounds. Decimal or exponent query operands are not supported. | JSON numbers and numeric strings can contain decimals or exponents. Conversion preserves decimal precision rather than converting to floating point. |
| Boolean | JSON `equals` infers a boolean from `true` or `false`, ignoring case. | Booleans and PostgreSQL-style strings such as `true`, `false`, `yes`, `no`, `on`, `off`, `1`, `0`, or unambiguous word prefixes are accepted, ignoring case and surrounding whitespace. JSON numbers are accepted when their JSONB text is exactly `0` or `1`; `1e0` qualifies, but `1.0` does not. |
| Timestamp | RFC3339 with an explicit offset, or `YYYY-MM-DD` interpreted as midnight UTC. `between` accepts two values. | Common forms include calendar dates, RFC3339, and `YYYY-MM-DD HH:MM:SS[.fraction]` with an optional numeric offset. Explicit offsets compare in UTC. |
| Text | Text operators force text comparison. JSON `equals` tries timestamp, boolean, and numeric inference before text. JSON `in`/`all` compare comma-separated text values without scalar inference. | Strings are unquoted. Numbers, booleans, arrays, and objects use their JSONB-style text projection. This is a textual comparison, not structural JSON equality or containment. |
| IP/network | Use the explicit network operators with IPv4, IPv6, or CIDR operands. | Valid address/network strings are compared as networks, not as raw text. |

Missing paths, JSON null, and failed numeric/boolean/timestamp/IP conversions do
not satisfy scalar comparisons, including negated comparisons. Use `is_null` or
`not_is_null` to test missing or null values. A malformed query operand is a
request error; an unconvertible stored value simply does not match. These
predicates apply before totals and pagination. The string `"null"` is text,
not JSON null.

Numeric strings use PostgreSQL's unconstrained `numeric` range: up to 131,072
digits before the decimal point and 16,383 after it. Out-of-range strings do
not match, even under negation. The memory adapter checks these bounds before
formatting and mirrors `-Infinity < finite values < Infinity < NaN`, including
case-insensitive `inf`/`infinity` and `NaN` strings. These are stored strings,
not valid JSON numeric literals, and they do not extend the integer query
operand vocabulary. See PostgreSQL's [numeric types](https://www.postgresql.org/docs/18/datatype-numeric.html).

For text predicates, memory follows PostgreSQL's JSONB output: a space follows
each comma and colon, object keys are ordered by UTF-8 byte length and then
bytes, and numbers expand exponent notation while preserving fractional trailing
zeros. For example, the stored subtree `[1,2]` is searched as `[1, 2]`, and
`1.230e-5` as `0.00001230`. Strings inside containers remain JSON-escaped.
This projection changes neither the stored document nor the API response.
Prefer filtering a leaf path when whole-subtree formatting is irrelevant.
See PostgreSQL's [JSON input and output](https://www.postgresql.org/docs/18/datatype-json.html).

For timestamps, memory accepts numeric offsets such as `+01`, `+0130`, and
`+01:30` on stored SQL-style timestamps, including surrounding whitespace.
One stored string must represent one timestamp; a comma-separated list is not
a stored timestamp. Keep query operands in RFC3339 or calendar-date form.

### Deliberate backend differences

The common forms above do not require adapters to emulate every native feature.
PostgreSQL retains its native execution and input support. In particular:

- **Timestamp extensions and range:** PostgreSQL also accepts named time zones,
  relative/special dates, and other date spellings; memory does not promise these
  forms. Such strings may fail conversion in memory. PostgreSQL's timestamp range
  and microsecond precision differ from Chrono's range and nanosecond precision
  in memory. Use years 0001–9999 and at most six fractional digits for portability.
- **Timezone-less stored timestamps:** memory assumes UTC. PostgreSQL interprets
  them using its session `TimeZone` before converting to UTC. Use explicit offsets,
  or configure PostgreSQL sessions for UTC, to avoid different results.
- **Text matching:** regex dialects, Unicode case folding, and locale-dependent
  matching remain backend-specific. The [string operator notes](#string-fields)
  describe the regex differences. No shared normalization changes native collation.
- **Additional native inputs:** PostgreSQL may accept numeric or IP spellings
  outside the common forms; acceptance by its parser does not establish memory
  support. Use ordinary decimal/exponent strings and complete IP addresses.
- **JSON paths named `null`:** the current PostgreSQL compiler interprets a
  case-insensitive `null` path segment as a SQL array null; memory treats it as
  an object key. Avoid those segments in portable filters.

PostgreSQL's [date/time input documentation](https://www.postgresql.org/docs/18/datatype-datetime.html)
describes its additional timestamp forms. Backend differences affect filtering
only where those forms are used; they are not reasons to move PostgreSQL queries
into application-side evaluation.

### IP address and network filtering

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

Supply `group_by` once for each ordered dimension. Up to three dimensions are
supported:

- `name`
- `description`
- `collection_id`
- `created_at`
- `updated_at`
- `json_data.<path>`, using the existing comma-separated nested path grammar
- `computed.shared.<key>`
- `computed.personal.<key>`

Supply `aggregate` once for each numeric measure, using
`operation:field`. Up to four measures are evaluated in request order. The
operations are `sum`, `average`, `min`, and `max`; fields can be
`json_data.<path>`, `computed.shared.<key>`, or
`computed.personal.<key>`. Computed measures require a definition whose result
type is `number` or `integer`.

At least one `group_by` or `aggregate` parameter is required. Omitting
`group_by` produces one global aggregate row when at least one readable,
filtered source object exists. For example:

```text
GET /api/v1/classes/12/object-aggregates?aggregate=sum:json_data.cost&aggregate=average:computed.shared.health_score
```

Each measure includes `value_count`, the number of finite supported numeric
values that contributed, and `skipped_count`, the remaining objects in the
group. Missing paths, JSON nulls, non-numeric JSON values, unavailable computed
results, and numbers outside the supported computed-decimal range are skipped.
A measure with no contributing values has `state=empty` and omits `value`;
otherwise it has `state=value`. This makes partial data visible instead of
silently presenting a number as if it covered the whole group.

The endpoint accepts the normal object filters, including scalar, `json_data`,
`permissions`, and enabled shared or owned personal computed filters. Computed
source filters use the same typed operators, `public`/`private` aliases, and
two-filter limit as class object lists. For example, this request first applies
JSON and computed filters, groups the remaining readable objects by country
and shared lifecycle, and calculates a cost total per group:

```text
GET /api/v1/classes/12/object-aggregates?json_data__equals=status=active&computed.shared.environment__equals=production&group_by=json_data.location,country&group_by=computed.shared.lifecycle&aggregate=sum:json_data.cost&sort=object_count.desc&limit=50
```

Grouping by `created_at` or `updated_at` uses the exact timestamp. The endpoint
does not accept date bucketing, arbitrary expressions, or object-list sort
fields. Aggregate ordering is selected with one of:

- `sort=dimensions.asc`, the default;
- `sort=dimensions.desc`;
- `sort=object_count.asc`;
- `sort=object_count.desc`.

Count ordering always appends the complete dimension tuple in ascending order
as a deterministic tie-breaker. Measures do not change row ordering. Cursor
tokens are bound to the selected dimensions, measures, and sort; changing any
of them while following a cursor returns
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
filters, grouping dimensions, or measures when either bound is exceeded.

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
    "measures": [
      {
        "field": "json_data.cost",
        "operation": "sum",
        "state": "value",
        "value_count": 35,
        "skipped_count": 2,
        "value": 917.5
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

Computed aggregation snapshots current definitions and applies computed source
filters, dimensions, and measures to source-object snapshots. A filter,
dimension, and measure in the same request share that definition snapshot.
Only the definitions needed by dimensions and measures and the enabled scope
needed by computed-filter evaluation are loaded. With a non-pushdown permission
backend, ordinary-filter candidates are authorized first; if none is visible,
the endpoint returns an empty page without resolving computed fields.
Otherwise, unknown, disabled, inaccessible, wrong-class, and non-numeric
measure selectors return `400 Bad Request`. SQL-pushdown computed filters are
resolved before their database query, matching class object-list behavior even
when the filtered result is empty.

Personal filters, dimensions, and measures require a human owner with
`ReadClass` access and can only use that owner's enabled definitions.
Aggregation does not reload source objects or perform computed read repair.
Service accounts cannot filter, group by, or measure personal fields. Responses
depending on computed state include `Cache-Control: private, no-store`.

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
