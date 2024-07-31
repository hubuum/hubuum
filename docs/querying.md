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

Boolean fields can be filtered using the `equals` operator.

## Negation

You can negate a filter by prefixing the operator with `not_`. For example, to find all employees that are not named "John", you can use the filter `username__not_equals=John`.

## Examples

Employees with the the exact username "John": `api/v1/iam/users/?username__equals=John`.
Employees with the username "John" or "john": `api/v1/iam/users/?username__iequals=john`.
Employees with the username containing "John": `api/v1/iam/users/?username__contains=John`.
Employees not named "John" or "john": `api/v1/iam/users/?username__not_icontains=John`.

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
