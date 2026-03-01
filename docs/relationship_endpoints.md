# Relationship endpoints

This document summarizes the current class-relation, object-relation, and related-object endpoints.

For filtering, sorting, and cursor pagination support, see:

- [querying.md](querying.md)
- [query_support_matrix.md](query_support_matrix.md)

## Context-free relation endpoints

### Class relations

| Operation | Method | Path | Description |
|-----------|--------|------|-------------|
| List | `GET` | `/api/v1/relations/classes` | List class relations visible to the caller |
| Get | `GET` | `/api/v1/relations/classes/{relation_id}` | Fetch one class relation |
| Create | `POST` | `/api/v1/relations/classes` | Create a class relation |
| Delete | `DELETE` | `/api/v1/relations/classes/{relation_id}` | Delete a class relation |

### Object relations

| Operation | Method | Path | Description |
|-----------|--------|------|-------------|
| List | `GET` | `/api/v1/relations/objects` | List object relations visible to the caller |
| Get | `GET` | `/api/v1/relations/objects/{relation_id}` | Fetch one object relation |
| Create | `POST` | `/api/v1/relations/objects` | Create an object relation |
| Delete | `DELETE` | `/api/v1/relations/objects/{relation_id}` | Delete an object relation |

## Contextual class endpoints

These endpoints are scoped by the class in the path.

| Operation | Method | Path | Description |
|-----------|--------|------|-------------|
| List direct relations | `GET` | `/api/v1/classes/{class_id}/relations` | List direct outgoing relations from the class |
| Create relation | `POST` | `/api/v1/classes/{class_id}/relations` | Create an outgoing relation from the class |
| Delete relation | `DELETE` | `/api/v1/classes/{class_id}/relations/{relation_id}` | Delete a relation from the class context |
| List transitive relations | `GET` | `/api/v1/classes/{class_id}/relations/transitive/` | List transitive class relations involving the class |
| List transitive relations to class | `GET` | `/api/v1/classes/{class_id}/relations/transitive/class/{class_id_to}` | List transitive relations between two classes |

## Contextual object relation endpoints

These endpoints are scoped by the source class and source object in the path.

| Operation | Method | Path | Description |
|-----------|--------|------|-------------|
| List related objects | `GET` | `/api/v1/classes/{class_id}/{from_object_id}/relations` | List objects related to the source object |
| Get relation | `GET` | `/api/v1/classes/{class_id}/{from_object_id}/relations/{to_class_id}/{to_object_id}` | Fetch the relation between the source and target objects |
| Create relation | `POST` | `/api/v1/classes/{class_id}/{from_object_id}/relations/{to_class_id}/{to_object_id}` | Create a relation between the source and target objects |
| Delete relation | `DELETE` | `/api/v1/classes/{class_id}/{from_object_id}/relations/{to_class_id}/{to_object_id}` | Delete the relation between the source and target objects |

## Query behavior

All list endpoints above use the shared query interface:

- filters are parsed from query parameters
- sorting is done in SQL
- cursor pagination is done in SQL
- the current page is returned as a JSON array
- the next page cursor, when present, is returned in `X-Next-Cursor`

## Field support

The relation endpoints do not all support the same fields:

- global relation endpoints support relation-centric fields such as `id`, `from_*`, `to_*`, `class_relation`, `created_at`, and `updated_at`
- transitive endpoints support graph-centric fields such as `depth` and `path`
- related-object listings support both descendant object aliases like `id`, `name`, `class_id`, `namespace_id` and explicit closure-view fields like `from_name`, `to_name`, `from_json_data`, `to_json_data`, `depth`, and `path`

Use [query_support_matrix.md](query_support_matrix.md) for the endpoint-by-endpoint field list.
