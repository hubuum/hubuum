# Core concepts

Hubuum provides a common model and access layer for inventory data. It can hold
resources from several authoritative systems without requiring all of them to
use the same native data model. Your integration decides how to collect and
refresh that data; Hubuum stores, validates, relates, and exposes it.

## From collections to objects

| Concept | What it represents | Example |
| --- | --- | --- |
| Collection | An organizational and permission boundary in a hierarchy | A platform team's inventory |
| Class | A type of resource, with an optional JSON Schema | Server, application, or location |
| Object | One instance of a class, with JSON data | `web-01` and its hostname, owner, and environment |
| Relation | A link between classes or their objects | An application runs on a server |

Each class and each object belongs to one collection. Do not assume that access
to a class also grants access to its objects: permissions are evaluated against
the relevant collection. Start with a small set of classes and evolve schemas
as the data model becomes clearer.

Collections form a tree under the system `root`. Group grants apply to a
collection and its descendants. Inheritance is additive; a child does not deny
a parent's grant. See [collection hierarchy](collection_hierarchy.md) and
[permissions](permissions.md) before designing tenant or team boundaries.

## Identity and access

A **principal** is a human user or a service account. Principals gain permissions
through group membership. A bearer token identifies a principal and can narrow
its authority through a scope; a token cannot grant authority the principal
does not have.

Use human identities for interactive work and service accounts for automation.
Credential changes require [fresh password approval](credential_approvals.md)
from an authorized human. [Authentication and authorization](auth_model.md)
describes the complete model.

## Work that continues in the background

Imports, exports, backups, remote calls, reindexing, and schema validation use
durable tasks. Submitting a task is not confirmation that the work has finished.
Inspect its status and per-item result before treating it as successful. Outputs
may have retention deadlines. See the [task API](task_api.md).

Schema revisions describe the validation policy for a class. Object compliance
records how data conforms to that policy. Computed fields derive values from
object data; they do not replace the raw data. See
[schema evolution](schema_evolution.md) and [computed fields](computed_fields.md).

## What to explore next

- [Run a server](getting-started/first-server.md) for an evaluation.
- [Choose an interface](integrations/clients.md) for your users or applications.
- [Read the storage architecture](storage_boundary.md) to understand implementation boundaries.
