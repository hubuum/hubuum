# Object Data JSON Patch

Hubuum supports targeted, atomic updates to an object's raw JSON `data`
document:

```http
PATCH /api/v1/classes/{class_id}/{object_id}/data
Authorization: Bearer <token>
Content-Type: application/json-patch+json
```

Automation that already knows the class-scoped object name can avoid resolving
its numeric ID first:

```http
PATCH /api/v1/classes/by-name/{class_name}/objects/by-name/{object_name}/data
Authorization: Bearer <token>
Content-Type: application/json-patch+json
```

Class names are globally unique, and object names are unique within a class.
Percent-encode either name path segment when it contains reserved characters.
The explicit `by-name` segments select name lookup unconditionally, so a name
such as `123` is never interpreted as an ID. The server binds the authorization
result to the same object ID, class ID, class name, and object name when
acquiring the row lock; a concurrent rename returns `404 Not Found` instead of
redirecting the patch to a different object.

The request body is an [RFC 6902 JSON Patch](https://www.rfc-editor.org/rfc/rfc6902.html)
operation array. Hubuum supports `add`, `remove`, `replace`, `move`, `copy`, and
`test`.

## Paths And Values

Every `path` and `from` is an
[RFC 6901 JSON Pointer](https://www.rfc-editor.org/rfc/rfc6901.html) relative to
the root of `hubuumobject.data`. Do not prefix pointers with `/data`.

For example, this request adds a `facts` member to the data document:

```json
[
  {
    "op": "add",
    "path": "/facts",
    "value": {
      "source": "inventory",
      "hostname": "srv-01"
    }
  }
]
```

RFC 6902 `add` replaces the complete value when the target object member
already exists. It does not recursively merge object values. An empty path
targets the complete data document:

```json
[
  {
    "op": "replace",
    "path": "",
    "value": {
      "replacement": true
    }
  }
]
```

Normal JSON Patch object and array rules apply:

- A missing intermediate parent makes an operation fail.
- `/-` appends to an existing array.
- Array indices use the RFC 6901 canonical decimal form and must be in range.
- `~1` represents `/` and `~0` represents `~` inside a pointer token.
- `move` and `copy` interpret `from` relative to the same raw data root.

The existing `PATCH /api/v1/classes/{class_id}/{object_id}` endpoint is
unchanged. Supplying `data` there still replaces the entire data value.

## Atomicity And Concurrency

Hubuum locks and reads the current object row before evaluating any operation.
The complete request runs in one database transaction:

```text
lock current object row
apply every operation to the locked data value
validate the final value against the class schema
update raw object data and timestamp
materialize shared computed fields
write temporal history and the object update event
commit
```

If an operation, schema check, database write, computed-field evaluation, or
event write fails, the transaction persists none of the patch or its side
effects. Concurrent patches therefore apply in row-lock serialization order.
Patches to unrelated members compose instead of overwriting data derived from
a stale read.

Use `test` for compare-and-set behavior. It is evaluated against the same
row-locked current value as subsequent operations:

```json
[
  {
    "op": "test",
    "path": "/version",
    "value": 4
  },
  {
    "op": "replace",
    "path": "/version",
    "value": 5
  }
]
```

A patch that produces no data change returns the current object with `200 OK`.
It does not advance `updated_at`, add a history version, or emit an object
update event.

## Authorization And Limits

Both endpoints require the same `UpdateObject` permission as the existing
object PATCH endpoint. The object must belong to the class named in the URL.

Requests use Hubuum's normal 2 MiB JSON request limit. A patch may contain at
most 1,000 operations, and every `path` or `from` pointer may contain at most
128 segments. The resulting raw object data is limited to 2 MiB and 64 nested
containers. Hubuum validates the result after each operation and caps cumulative
application work at 32 MiB, so repeated `copy` operations cannot amplify a
small request into unbounded server work. Results containing a null character
in a string or object key, or a number outside PostgreSQL's JSONB numeric range,
are rejected before persistence. Any validation or limit failure rolls back the
complete patch.

## Status Codes

| Status | Behavior |
| ------ | -------- |
| `200 OK` | The patch succeeded; the response is the updated or unchanged object. |
| `400 Bad Request` | The JSON is malformed, the patch structure or bounds are invalid, or the result contains a value PostgreSQL JSONB cannot represent. |
| `401 Unauthorized` | Authentication is missing or invalid. |
| `403 Forbidden` | The principal lacks `UpdateObject` permission. |
| `404 Not Found` | The class/object pair does not exist. A class mismatch is also reported as not found. |
| `406 Not Acceptable` | The final patched data fails the class JSON Schema. |
| `409 Conflict` | An operation cannot be applied, including a failed `test`; no operation is persisted. |
| `413 Payload Too Large` | The request, resulting object data, nesting depth, or cumulative application work exceeds its limit. |
| `415 Unsupported Media Type` | `Content-Type` is not `application/json-patch+json`. |
| `500 Internal Server Error` | Persistence, computed materialization, or event emission failed; the transaction is rolled back. |
