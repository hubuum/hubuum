# Querying against the Hubuum API

The Hubuum API provides a powerful querying system that allows you to filter and sort data in a variety of ways. This document will provide an overview of the querying system and how to use it.

## Querying Basics

Query parameters are passed to the API as query string parameters. Each part (split on `&`) is on the format `key__operator=value`. The `key` is the field you want to filter on, the `operator` is the operation you want to perform, and the `value` is the value you want to filter on. Supported operators are:

For string fields:

- `equals`: The field is equal to the value.
- `iequals`: The field is equal to the value, case-insensitive.
- `contains`: The field contains the value.
- `icontains`: The field contains the value, case-insensitive.
- `startswith`: The field starts with the value.
- `istartswith`: The field starts with the value, case-insensitive.
- `endswith`: The field ends with the value.
- `iendswith`: The field ends with the value, case-insensitive.
- `like`: The field is like the value.
- `regex`: The field matches the regex pattern in the value.

For numeric and date fields:

- `gt`: The field is greater than the value.
- `gte`: The field is greater than or equal to the value.
- `lt`: The field is less than the value.
- `lte`: The field is less than or equal to the value.
- `between`: The field is between the two values.

For array fields:

- `contains`: The array contains the value, eg `array_field__contains=1`.
- `equals`: The array is equal to the value, expressed as a comma-separated list, eg `array_field=1,2,3` (identical to `array_field__equals=1,2,3`).

For boolean fields:

- `equals`: The field is equal to the value, eg `boolean_field__equals=true`.

## Negation

You can negate a filter by prefixing the operator with `not_`. For example, to find all employees that are not named "John", you can use the filter `username__not_equals=John`.

## Combining filters

You can combine filters by separating them with `&`. For example, to find all employees named "John" that are in the "Engineering" department, you can use the filter `username__equals=John&department__equals=Engineering`. All filters are combined with an AND operation.

## Examples

Employees with the the exact username "John": `api/v1/iam/users/?username__equals=John`.
Employees with the username "John" or "john": `api/v1/iam/users/?username__iequals=john`.
Employees with the username containing "John": `api/v1/iam/users/?username__contains=John`.
Employees not named "John" or "john": `api/v1/iam/users/?username__not_icontains=John`.
Employees with the username starting with "John" and ending with "Smith": `api/v1/iam/users/?username__startswith=John&username__endswith=Smith`.

## JSON filtering

These filters can also be applied to nested JSON fields. If you have a JSON schema that looks like this:

```json
{
    "$id": "https://example.com/geographical-location.schema.json",
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "title": "Geographical Location",
    "description": "A geographical location",
    "required": [ "latitude", "longitude" ],
    "type": "object",
    "properties": {
        "latitude": {
            "type": "number",
            "minimum": -90,
            "maximum": 90
        },
        "longitude": {
            "type": "number",
            "minimum": -180,
            "maximum": 180
        }
    },
    "required": [ "latitude", "longitude" ]
}
```

You can find all entries use this schema and that are south of the equator (ie, whos latitude is negative) by searching for
`json_schema__lt=properties,latitude,minimum=0`. If the path does not exist, the filter will NOT match but it will not fail.

## Sorting

You can sort the results of a query by adding a `sort` query parameter. The value of the `sort` parameter is a comma-separated list of fields to sort by. Each field can be followed by `.asc` or `.desc` to specify the sort direction. You may currently only sort by top-level fields.

The fields you may sort on depends on the resource being queried. Currently supported resources and fields are:

- Namespaces (`/api/v1/namespaces/`): `id`, `name`, `created_at`, `updated_at`
- Classes (`/api/v1/classes/`): `id`, `name`, `namespaces`, `created_at`, `updated_at`
- Objects (`/api/v1/classes/{class_id}/`): `id`, `name`, `namespaces`, `classid`, `created_at`, `updated_at` (the `classid` sort option exists for possible future endpoints)

### Examples

- Sorting by name in ascending order: `?sort=name.asc`
- Sorting by name in descending order: `?sort=name.desc`
- Sorting by created_at in ascending order: `?sort=created_at.asc`
- Sorting an object search by namespaces descending, then class_id ascending, then object_id in descending order: `/api/v1/classes/4/?sort=namespaces.desc,class_id.asc,object_id.desc`

### Notes

- The parameter `order_by` is an alias for `sort` and can be used interchangeably.
- The sort order is adhered to, so if you specify multiple fields to sort by, the results will be sorted by the first field, then the second field, and so on.

## Limit

You can limit the number of results returned by a query by adding a `limit` query parameter. The value of the `limit` parameter is the maximum number of results to return. For example, to return only 10 results, you can add the following query: `?limit=10`.
