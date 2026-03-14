# Query Support Matrix

This matrix documents the shared DB-backed query interface used by Hubuum list endpoints.

Notes:

- `limit`, `sort`, and `cursor` apply to every endpoint listed here.
- response bodies remain JSON arrays; the next page cursor is returned in `X-Next-Cursor`
- contextual endpoints may inject exact filters from path parameters
- some endpoints also apply permission scoping before query filters are evaluated

## IAM

| Endpoints | Filter fields | Sort fields | Default sort | Notes |
|-----------|---------------|-------------|--------------|-------|
| `/api/v1/iam/users` | `id`, `name`, `username`, `email`, `created_at`, `updated_at` | `id`, `name`, `username`, `email`, `created_at`, `updated_at` | `id.asc` | `name` and `username` sort/filter the same underlying field |
| `/api/v1/iam/groups` | `id`, `name`, `groupname`, `description`, `created_at`, `updated_at` | `id`, `name`, `groupname`, `description`, `created_at`, `updated_at` | `id.asc` | `name` and `groupname` sort/filter the same underlying field |
| `/api/v1/iam/users/{user_id}/groups` | `id`, `name`, `groupname`, `description`, `created_at`, `updated_at` | `id`, `name`, `groupname`, `description`, `created_at`, `updated_at` | `id.asc` | path constrains the result to one user's memberships |
| `/api/v1/iam/groups/{group_id}/members` | `id`, `name`, `username`, `email`, `created_at`, `updated_at` | `id`, `name`, `username`, `email`, `created_at`, `updated_at` | `id.asc` | path constrains the result to one group's members |
| `/api/v1/iam/users/{user_id}/tokens` | `issued_at`, `name` | `issued_at`, `name` | `issued_at.desc`, `name.asc` | `name` refers to the token string |

## Namespaces and permissions

| Endpoints | Filter fields | Sort fields | Default sort | Notes |
|-----------|---------------|-------------|--------------|-------|
| `/api/v1/namespaces` | `id`, `name`, `description`, `created_at`, `updated_at`, `permissions` | `id`, `name`, `created_at`, `updated_at` | `id.asc` | `permissions` narrows the namespaces to those where the caller has the named permission |
| `/api/v1/namespaces/{namespace_id}/permissions` | `id`, `name`, `groupname`, `created_at`, `updated_at`, `permissions` | `id`, `name`, `groupname`, `created_at`, `updated_at` | `id.asc` | returns `GroupPermission` rows |
| `/api/v1/namespaces/{namespace_id}/permissions/user/{user_id}` | `id`, `name`, `groupname`, `created_at`, `updated_at`, `permissions` | `id`, `name`, `groupname`, `created_at`, `updated_at` | `id.asc` | constrained to one namespace and one user's memberships |
| `/api/v1/namespaces/{namespace_id}/has_permissions/{permission}` | `id`, `name`, `groupname`, `description`, `created_at`, `updated_at` | `id`, `name`, `groupname`, `description`, `created_at`, `updated_at` | `id.asc` | path permission already narrows the result set |

## Classes and objects

| Endpoints | Filter fields | Sort fields | Default sort | Notes |
|-----------|---------------|-------------|--------------|-------|
| `/api/v1/classes` | `id`, `namespaces`, `name`, `description`, `validate_schema`, `json_schema`, `created_at`, `updated_at`, `permissions` | `id`, `name`, `namespaces`, `namespace_id`, `created_at`, `updated_at` | `id.asc` | `json_schema` is only filterable, not sortable |
| `/api/v1/classes/{class_id}/` | `id`, `name`, `description`, `namespaces`, `namespace_id`, `classes`, `class_id`, `json_data`, `created_at`, `updated_at`, `permissions` | `id`, `name`, `namespaces`, `namespace_id`, `classes`, `class_id`, `created_at`, `updated_at` | `id.asc` | path constrains the result to a single class |
| `/api/v1/classes/{class_id}/permissions` | `id`, `name`, `groupname`, `created_at`, `updated_at`, `permissions` | `id`, `name`, `groupname`, `created_at`, `updated_at` | `id.asc` | namespace permission rows for the class's namespace |

## Relations

| Endpoints | Filter fields | Sort fields | Default sort | Notes |
|-----------|---------------|-------------|--------------|-------|
| `/api/v1/relations/classes` | `id`, `from_classes`, `to_classes`, `from_class_name`, `to_class_name`, `created_at`, `updated_at`, `permissions` | `id`, `from_classes`, `to_classes`, `created_at`, `updated_at` | `id.asc` | `from_class_name` and `to_class_name` are filter-only helpers |
| `/api/v1/classes/{class_id}/relations` | `id`, `from_classes`, `to_classes`, `created_at`, `updated_at` | `id`, `from_classes`, `to_classes`, `created_at`, `updated_at` | `id.asc` | path injects an exact `from_classes={class_id}` filter |
| `/api/v1/classes/{class_id}/relations/transitive/` | `from_classes`, `to_classes`, `depth`, `path` | `from_classes`, `to_classes`, `depth`, `path` | `depth.asc`, `path.asc` | path constrains the transitive graph to the class in the URL |
| `/api/v1/classes/{class_id}/relations/transitive/class/{class_id_to}` | `from_classes`, `to_classes`, `depth`, `path` | `from_classes`, `to_classes`, `depth`, `path` | `depth.asc`, `path.asc` | path constrains both endpoints of the transitive lookup |
| `/api/v1/relations/objects` | `id`, `class_relation`, `from_objects`, `to_objects`, `created_at`, `updated_at`, `permissions` | `id`, `class_relation`, `from_objects`, `to_objects`, `created_at`, `updated_at` | `id.asc` | permission filters narrow the namespaces used to scope object relations |

## Related resources

| Endpoints | Filter fields | Sort fields | Default sort | Notes |
|-----------|---------------|-------------|--------------|-------|
| `/api/v1/classes/{class_id}/objects/{object_id}/related/objects` | `id`, `name`, `description`, `namespace_id`, `namespaces`, `class_id`, `classes`, `created_at`, `updated_at`, `from_objects`, `to_objects`, `from_classes`, `to_classes`, `from_namespaces`, `to_namespaces`, `from_name`, `to_name`, `from_description`, `to_description`, `from_created_at`, `to_created_at`, `from_updated_at`, `to_updated_at`, `from_json_data`, `to_json_data`, `depth`, `path` | `id`, `name`, `description`, `namespace_id`, `namespaces`, `class_id`, `classes`, `created_at`, `updated_at`, `from_objects`, `to_objects`, `from_classes`, `to_classes`, `from_namespaces`, `to_namespaces`, `from_name`, `to_name`, `from_description`, `to_description`, `from_created_at`, `to_created_at`, `from_updated_at`, `to_updated_at`, `depth`, `path` | `path.asc`, `id.asc` | returns connected objects with a `path`; sorting and cursor pagination are done against closure-table/object-join columns in SQL; JSON fields are filter-only |
| `/api/v1/classes/{class_id}/objects/{object_id}/related/relations` | `id`, `class_relation`, `from_objects`, `to_objects`, `created_at`, `updated_at`, `permissions` | `id`, `class_relation`, `from_objects`, `to_objects`, `created_at`, `updated_at` | `id.asc` | path constrains the result to direct relations touching the object in the URL |

`/api/v1/classes/{class_id}/objects/{object_id}/related/graph` is not a paginated list endpoint. It accepts connected-object filters such as `depth` to define the included neighborhood and returns a graph object containing `objects` and `relations`.

## Query aliases

Common aliases accepted by the parser:

- `order_by` is an alias for `sort`
- `name` and `username` both target the user name field on user endpoints
- `name` and `groupname` both target the group name field on group endpoints
- `namespaces` and `namespace_id` can both be used for namespace-oriented object and class ordering
- `classes` and `class_id` can both be used for class-oriented object ordering

## Source of truth

This file is the human-oriented summary. For exact route definitions and generated parameter docs, see:

- [querying.md](querying.md)
- [relationship_endpoints.md](relationship_endpoints.md)
- [report_api.md](report_api.md)
- [openapi.json](openapi.json)
