# Build an integration

Use a [typed client](clients.md) when one fits your application, or call the
[HTTP API](api.md) directly. The same server-side permissions and limits apply
to every interface.

## Recommended reading order

1. [Core concepts](../concepts.md) and [first API requests](../getting-started/first-requests.md).
2. [Authentication, service accounts, and scoped tokens](../auth_model.md).
3. [Queries and cursor pagination](../querying.md), including the
   [endpoint support matrix](../query_support_matrix.md).
4. [Revisions and conditional mutations](../resource_revisions.md) for safe updates.
5. [Tasks](../task_api.md) for asynchronous operations and retained outputs.

## Choose a workflow

| Integration goal | Server contract |
| --- | --- |
| Synchronize an inventory graph | [Import API](../import_api.md), including idempotency and per-item results |
| Extract data or render a report | [Export API](../export_api.md) and [template guide](../export_template_guide.md) |
| Discover and traverse related resources | [Search](../search_api.md) and [relationships](../relationship_endpoints.md) |
| React to changes | [Events, sinks, and subscriptions](../events.md) |
| Invoke an external system for an object | [Remote targets](../remote_targets.md) |
| Update selected JSON fields | [Atomic JSON Patch](../object_data_json_patch.md) |
| Manage credentials or restore data | [Credential approvals](../credential_approvals.md) and [restore workflow](../backup-restore.md) |

Treat a submitted task as pending work until its terminal status and results
confirm success. Honor output retention, request limits, and pagination rather
than assuming all results arrive in one response.

For a new release, review the [compatibility records](clients.md#choose-compatible-versions)
and the server's [integration coverage](../integration-coverage.md).
