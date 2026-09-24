# Make your first API requests

You need a running server and a human account. An administrator must grant your
group access to the collection you will use. These HTTP examples show request
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

Choose a collection you are allowed to use and note its `id`. An administrator
can create one using the [collection guide](../collection_hierarchy.md). The
following example uses illustrative collection ID `42`.

## Create a class

You need `CreateClass` on the collection. Choose a unique class name:

```http
POST /api/v1/classes
Authorization: Bearer <token>
Content-Type: application/json

{
  "name": "example-server",
  "description": "Servers in the example inventory",
  "collection_id": 42,
  "validate_schema": false
}
```

This evaluation class starts without enforced schema validation. For managed
data, use [class schemas and compliance](../schema_evolution.md).

## Create and read an object

You need `CreateObject` on the class's collection. The name-addressed creation
route infers the class and collection:

```http
POST /api/v1/classes/by-name/example-server/objects
Authorization: Bearer <token>
Content-Type: application/json

{
  "name": "web-01",
  "description": "Example web server",
  "data": {
    "hostname": "web-01.example.com",
    "environment": "evaluation"
  }
}
```

Read it with `ReadObject` access:

```http
GET /api/v1/classes/by-name/example-server/objects/by-name/web-01
Authorization: Bearer <token>
```

You have now created a class and an object with your own JSON data. Next, try
[filtering and pagination](../querying.md), add
[relationships](../relationship_endpoints.md), or use a
[client library](../integrations/clients.md).

## Understand common responses

| Response | What to check |
| --- | --- |
| `400` | Request fields, query syntax, and schema diagnostics in the error body. |
| `401` | The bearer token is present, valid, and unexpired. |
| `403` | Group permissions and token scope; credential mutations also require [fresh approval](../credential_approvals.md). |
| `404` | Resource identity and visibility; use explicit `by-name` routes for names. |
| `429` | Login rate limits; follow your administrator's retry guidance. |

For all request schemas and response codes, use the [API reference](../integrations/api.md).
