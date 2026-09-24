# HTTP API reference

The committed [OpenAPI document](../openapi.json) describes the server API at
the same revision as this site. Download it for inspection or client generation.
CI checks that it matches the specification generated from the Rust code.

For an installed server, use **that instance's specification**:

| Path on your server | Purpose |
| --- | --- |
| `/api-doc/openapi.json` | Machine-readable specification and server `info.version` |
| `/swagger-ui/` | Interactive reference, when built with the `swagger-ui` feature |
| `/api/v1/config` | Public effective limits and authentication settings |
| `/healthz` and `/readyz` | Liveness and readiness probes |

Swagger UI is enabled by default but can be omitted from production builds.
This documentation website is static; it does not connect to your deployment
or collect API credentials.

## Authentication and versioned routes

Human login and provider discovery remain under `/api/v0/auth`. Resource and
administration routes are under `/api/v1`. Send bearer tokens to the server in
the `Authorization` header. See [first requests](../getting-started/first-requests.md)
and the [authentication model](../auth_model.md).

Some operations require more than an ordinary bearer token. Credential changes
require a [single-use password approval](../credential_approvals.md); confirmed
restore status uses its own capability. Follow each operation's contract.

## Shared API behavior

- [Query syntax, sorting, and cursor pagination](../querying.md).
- [Endpoint filter support](../query_support_matrix.md).
- [Explicit name addressing](../name_addressing.md).
- [ETags, revisions, and conditional mutations](../resource_revisions.md).
- [Task status, cancellation, and outputs](../task_api.md).
- [Runtime work and response limits](../runtime_hardening.md).

OpenAPI's version follows the server package version. Read
[releases and compatibility](../releases.md) when generating a client for a
released server instead of `main`.
