# Make your first API requests

You need a running server and a human account. Ask an administrator to load the
[Atlas example dataset](example-dataset.md) and grant your group read access.
Membership in its atlas-readers group gives access to the complete example. These HTTP examples show request
and response shapes; substitute your URL and values in your HTTP client.

## Sign in

Authentication uses the existing `/api/v0/auth` routes. Resource operations use
`/api/v1`. Log in with the account's `name`, not a `username` field:

```http
POST /api/v0/auth/login
Content-Type: application/json

{
  "name": "alice",
  "password": "<your-password>"
}
```

A successful response includes `token` and `expires_at`. Keep the token private
and send it as `Authorization: Bearer <token>` on subsequent requests. For an
external identity provider, supply the appropriate `identity_scope`; see
[external authentication](../external_auth.md).

## Find your collection

```http
GET /api/v1/collections
Authorization: Bearer <token>
```

Find `atlas-demo` and its `atlas-demo-operations` child. The service catalogue
and context live in the parent; server and location records live in the child.
Permissions are evaluated on the relevant collection.

## Inspect a class

A class defines a type of resource and its optional schema. Read the Server
class to see the schema used for inventory records:

```http
GET /api/v1/classes/by-name/Server
Authorization: Bearer <token>
```

It requires a string hostname and a production or staging environment. Compare
it with Context, which has no schema:

```http
GET /api/v1/classes/by-name/Context
Authorization: Bearer <token>
```

## Read an object

An object is one instance of its class. Read web-01 in the Server class:

```http
GET /api/v1/classes/by-name/Server/objects/by-name/web-01
Authorization: Bearer <token>
```

Its response includes IDs, timestamps, a revision, and this `data`:

<!-- atlas-data: object:web-01 -->
```json
{
  "hostname": "web-01.example.invalid",
  "environment": "production",
  "cpu_cores": 8,
  "memory_gib": 32,
  "costs": {
    "compute": 45,
    "storage": 5
  },
  "source": "inventory.example.invalid"
}
```

The hostname and capacity are example reference data from an upstream inventory.
The source field is ordinary JSON metadata; your integration controls how it
is collected and refreshed.

## Follow the relationships

Read Atlas in the Service class, then inspect its connections:

```http
GET /api/v1/classes/by-name/Service/objects/by-name/Atlas
Authorization: Bearer <token>

GET /api/v1/classes/by-name/Service/objects/by-name/Atlas/related/relations
Authorization: Bearer <token>
```

Atlas connects directly to web-01, web-02, Research notes, and Migration checklist.
Those object relations use the Service–Server and Service–Context class
relations. The [dataset guide](example-dataset.md) explains the complete model.

## Make a change

Reading does not require write access. To try creating an additional object,
ask for the atlas-operators role and follow the [name-addressed creation
example](../name_addressing.md#creating-an-object-without-ids). It adds web-03
to Server; the ten-object corpus remains the documented starting point.

Next, try [filtering and pagination](../querying.md), explore
[class schemas](../schema_evolution.md), or use a
[client library](../integrations/clients.md) against the same dataset.

## Understand common responses

| Response | What to check |
| --- | --- |
| `400` | Request fields, query syntax, and schema diagnostics in the error body. |
| `401` | The bearer token is present, valid, and unexpired. |
| `403` | Group permissions and token scope; credential mutations also require [fresh approval](../credential_approvals.md). |
| `404` | Resource identity and visibility; use explicit `by-name` routes for names. |
| `429` | Login rate limits; follow your administrator's retry guidance. |

For all request schemas and response codes, use the [API reference](../integrations/api.md).
