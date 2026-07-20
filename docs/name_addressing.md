# Name Addressing

Hubuum exposes explicit `by-name` aliases for automation that knows current
class and object names but has not resolved their numeric IDs. Numeric ID paths
remain canonical. The aliases avoid an extra lookup without making an
unlabelled path segment ambiguous.

## Addressing Rules

Class names are globally unique. Object names are unique within a class, so an
object name route includes both natural-key components:

```http
GET /api/v1/classes/by-name/{class_name}/objects/by-name/{object_name}
Authorization: Bearer <token>
```

Percent-encode each name independently when it contains reserved path
characters. Every explicit `by-name` segment selects name lookup
unconditionally. A name such as `123` is never parsed or retried as ID `123`.
Missing names return `404 Not Found`.

## Supported Current-State Routes

The aliases cover current class and object resources and read-only views rooted
at them:

| Methods | Name-addressed path |
| ------- | ------------------- |
| `GET`, `PATCH`, `DELETE` | `/api/v1/classes/by-name/{class_name}` |
| `GET` | `/api/v1/classes/by-name/{class_name}/permissions` |
| `GET` | `/api/v1/classes/by-name/{class_name}/related/classes` |
| `GET` | `/api/v1/classes/by-name/{class_name}/related/relations` |
| `GET` | `/api/v1/classes/by-name/{class_name}/related/graph` |
| `GET`, `POST` | `/api/v1/classes/by-name/{class_name}/objects` |
| `GET`, `PATCH`, `DELETE` | `/api/v1/classes/by-name/{class_name}/objects/by-name/{object_name}` |
| `PATCH` | `/api/v1/classes/by-name/{class_name}/objects/by-name/{object_name}/data` |
| `GET` | `/api/v1/classes/by-name/{class_name}/objects/by-name/{object_name}/related/objects` |
| `GET` | `/api/v1/classes/by-name/{class_name}/objects/by-name/{object_name}/related/relations` |
| `GET` | `/api/v1/classes/by-name/{class_name}/objects/by-name/{object_name}/related/graph` |

Query parameters, permissions, response bodies, pagination headers, cache
headers, and status codes match the corresponding ID-addressed endpoint.

## Creating An Object Without IDs

Both class-scoped object creation routes infer `hubuum_class_id` and
`collection_id` from the path. A name-addressed request therefore needs no
prior class lookup:

```http
POST /api/v1/classes/by-name/server/objects
Authorization: Bearer <token>
Content-Type: application/json

{
  "name": "web-01",
  "description": "Web server",
  "data": {
    "environment": "production"
  }
}
```

For compatibility, clients may still include `hubuum_class_id` or
`collection_id`. When supplied, each value must match the path class.

## Rename Safety

Name-addressed writes first resolve the current natural key for authorization.
The write transaction then locks the resolved row and requires both its ID and
the original name to still match. Object writes also require the original class
ID, class name, and object name. A class or object renamed between resolution
and lock acquisition causes `404 Not Found`; the operation does not follow the
ID under its new name.

Object creation performs the same class-name check in the transaction that
inserts the object. JSON Patch applies the natural-key check in the transaction
that reads and patches the latest object data.

## Deliberate ID-Only Routes

History and as-of routes remain ID-addressed because a mutable current name
cannot unambiguously identify deleted or historical state. Audit event routes,
computed-field configuration, and relation creation or deletion also remain
ID-addressed. The latter workflows identify subordinate or multiple resources
and need their own complete selector and transactional-lock contracts rather
than an implicit name-to-ID preflight.
