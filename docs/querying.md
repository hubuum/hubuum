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
/api/v1/iam/users?username__contains=alice&email__endswith=@example.org&sort=username.asc
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

### Numeric and date fields

- `equals`
- `gt`
- `gte`
- `lt`
- `lte`
- `between`

### Array fields

- `equals`
- `contains`

### Boolean fields

- `equals`

### IP/network JSON fields

- `is_in_network`
- `contains_network`
- `contains_ip`
- `overlaps_network`
- `ip_equals`

## Negation

You can negate an operator by prefixing it with `not_`.

Examples:

- `username__not_equals=alice`
- `name__not_icontains=test`
- `created_at__not_between=2026-01-01T00:00:00Z,2026-02-01T00:00:00Z`

## Sorting

Use `sort` to request ordering. `order_by` is accepted as an alias.

Supported forms:

- `sort=id`
- `sort=id.asc`
- `sort=id.desc`
- `sort=-id`
- `sort=namespace_id.asc,name.desc`

Notes:

- Sort support is endpoint-specific.
- Cursor pagination requires a stable sort, so Hubuum appends a deterministic tie-breaker automatically.
- If you omit `sort`, each endpoint uses its own default stable sort.
- Some relation endpoints support sorting on contextual fields like `from_*`, `to_*`, `depth`, and `path`.

## Cursor pagination

List endpoints use cursor pagination.

Parameters:

- `limit`: maximum number of items to return
- `sort`: page order
- `cursor`: opaque token returned by a previous response

Limits:

- default page size: `100`
- maximum page size: `250`

Behavior:

- the current page is returned as a JSON array
- every paginated response includes `X-Total-Count` with the exact number of matching results
- if another page exists, the response includes `X-Next-Cursor`
- send that cursor back unchanged to fetch the next page
- if `X-Next-Cursor` is absent, there is no next page
- total pages can be derived client-side as `ceil(X-Total-Count / limit)`

Example:

```text
GET /api/v1/classes?namespaces=12&limit=2&sort=id.asc
```

Example response header:

```text
X-Total-Count: 6
X-Next-Cursor: eyJzb3J0cyI6W3siZmllbGQiOiJpZCIsImRlc2NlbmRpbmciOmZhbHNlfV0sInZhbHVlcyI6W3sidHlwZSI6ImludGVnZXIiLCJ2YWx1ZSI6Mn1dfQ
```

Next page:

```text
GET /api/v1/classes?namespaces=12&limit=2&sort=id.asc&cursor=eyJzb3J0cyI6W3siZmllbGQiOiJpZCIsImRlc2NlbmRpbmciOmZhbHNlfV0sInZhbHVlcyI6W3sidHlwZSI6ImludGVnZXIiLCJ2YWx1ZSI6Mn1dfQ
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

JSON-backed IP address and CIDR values also support network-aware operators:

- `is_in_network`
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
- `ip_equals`
  Matches normalized network equality using PostgreSQL `inet` semantics rather than raw string equality.
  Example: stored `10.0.0.10`, filter `10.0.0.10/32` -> match.
  Example: stored `10.0.0.0/24`, filter `10.0.0.0/25` -> no match.

Examples:

```text
/api/v1/classes/12/?json_data__is_in_network=network,address=10.0.0.0/24
/api/v1/classes/12/?json_data__contains_network=network,address=10.0.0.0/25
/api/v1/classes/12/?json_data__contains_ip=network,address=10.0.0.10
/api/v1/classes/12/?json_data__overlaps_network=network,address=10.0.0.64/26
/api/v1/classes/12/?json_data__ip_equals=network,address=10.0.0.10
```

If the JSON path does not exist, or the stored value is not a valid IP/CIDR for one of the network-aware operators, the filter does not match, but it does not fail the request.

## Contextual endpoints

Some list endpoints derive part of the query from the path.

Examples:

- `/api/v1/classes/{class_id}/related/classes` always constrains the result to classes connected to the class in the path
- `/api/v1/classes/{class_id}/related/relations` always constrains the result to direct relations touching the class in the path
- `/api/v1/classes/{class_id}/` always constrains the result to objects in that class
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
- namespace lists and namespace permission listings
- class lists, class permissions, connected-class listings, direct class-relation listings, and objects in class
- global class relation and object relation lists
- connected-object listings
- direct related-relation listings

For the exact filter and sort fields per endpoint, see [query_support_matrix.md](query_support_matrix.md).
