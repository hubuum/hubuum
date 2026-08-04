# Resource revisions and conditional mutations

Every authoritative mutable entity exposes a positive numeric `revision`.
Revisions are assigned by PostgreSQL and advance exactly once for an effective
domain change. Semantic no-ops preserve both `revision` and `updated_at` and do
not create audit or temporal-history rows.

The revision covers one entity only. An expanded class, for example, contains
the class revision and a separate nested collection revision. Settings and SQL
permission state use aggregate wrappers:

```json
{
  "revision": 4,
  "settings": {
    "theme": "dark"
  }
}
```

```json
{
  "collection_id": 12,
  "revision": 9,
  "permissions": []
}
```

Membership list items represent the membership itself and carry its revision.
Their optional nested `principal` has the independent principal revision.

## HTTP validators

Canonical point reads and mutation responses return a strong opaque `ETag`.
Clients must send that value unchanged in `If-Match` for conditional updates or
deletes. Numeric JSON revisions are useful for queries, events, imports, and
exports, but clients must not manufacture HTTP validators from them.

```http
GET /api/v1/collections/12
Authorization: Bearer <token>
```

```http
PATCH /api/v1/collections/12
Authorization: Bearer <token>
If-Match: "<opaque-etag>"
Content-Type: application/json

{
  "description": "Updated description"
}
```

`If-Match: *` and lists of up to eight compatible strong validators are
supported. Weak, malformed, oversized, mixed wildcard/list, and cross-resource
validators return `400 Bad Request`. A stale validator returns `412
Precondition Failed` with `reason: "stale_resource"` and refetch guidance.
Authorization is evaluated before the revision comparison, and the comparison
runs while the authoritative row is locked.

Missing `If-Match` remains an unconditional write in this release. Callers
should nevertheless adopt validators now; requiring them with `428
Precondition Required` is a future compatibility step.

Expanded or effective representations are untagged when one entity revision
cannot cover the complete response. Class point routes therefore return the
entity-only shape by default and make `include=collection` explicitly expanded
and untagged; class creation and mutation responses remain expanded and are
also untagged. Raw object points are tagged, while `include=computed` is
untagged.

Operational fields excluded from revision advancement are likewise excluded
from tagged point representations. Group points omit directory synchronization
timestamps, and token points omit `last_used_at`. Group and token list responses
retain those operational fields but do not emit per-item ETags.
