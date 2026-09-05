# Runtime and credential boundaries

Remote credentials are administrator-owned bindings. A target's final rendered
URL must match a configured HTTPS origin, including its effective port, and the
target must belong to an allowed collection. Redirects remain disabled. Matching
an alias alone no longer authorizes use of its secret.

Set `HUBUUM_REMOTE_CREDENTIAL_POLICIES` to a JSON object such as this one:

<!-- doc-example: hardening/remote-credential-policy -->
```json
{
  "inventory_api": {
    "collection_ids": [42],
    "origins": ["https://inventory.example"]
  }
}
```

Bindings grant access to the whole origin. Use separate origins and credentials
where applications require different trust boundaries. URL templates may choose
paths and queries within that origin. Origins cannot contain paths, userinfo,
queries or fragments. Missing bindings deny credential use.

Webhook sinks with a secret reference or static headers require both
`allowed_collection_ids` and `allowed_origins` in their administrator-managed
configuration. Subscription editors cannot expand that authority. For example:

<!-- doc-example: hardening/webhook-credential-policy -->
```json
{
  "allowed_collection_ids": [42],
  "allowed_origins": ["https://events.example"],
  "headers": {"X-Integration": "hubuum"}
}
```

These examples are exercised by the credential-policy tests with the same
constructors used in production.

## Template execution

Install `hubuum-template-worker` beside the application binaries. Production
containers and release archives include it; local builds use the workspace's
default members. Tests and standalone template benchmarks must build it first:

```sh
cargo build --locked -p hubuum-templates --bin hubuum-template-worker
```

Every render and syntax/composition validation runs in a fresh process with a
128 MiB budget for live Rust heap allocations. The parent admits at most four
workers at a time and applies a five-second deadline including admission and
transport. Worker allocation failure, timeout, or abnormal termination becomes
a template error; it does not abort the server. The allocator budget does not
claim to measure total resident memory: stack, executable pages, and allocator
bookkeeping remain additional process overhead.

The protocol accepts at most 16 MiB of serialized input. Ordinary template
outputs default to 1 MiB; exports keep their configured output limit up to a
hard 32 MiB ceiling. Remote URLs and individual headers have 8 KiB limits,
with at most 128 headers and 64 KiB of rendered header content in aggregate.
Email subjects have a 4 KiB limit. Fuel and recursion limits still apply. Macros, captures,
includes and normal expressions remain supported. Compilation is isolated too.
The former application-wide compiled-template cache is removed, so workloads
with many tiny renders pay process startup and compilation costs.

## Traversal and export work

Traversal requests carry the server depth and work budgets into storage.
`HUBUUM_MAX_TRANSITIVE_DEPTH` applies even when callers omit a depth predicate;
client depth cannot expand the server allowance. Recursive traversal rejects
more than 50,000 explored path rows before final ordering and deduplication.
This is a work bound, not only an output row limit. Dense graphs can therefore
require a narrower depth even when the requested output page is small.

External-policy exports scan candidates in bounded batches of 128, with a
10,000-candidate ceiling. A sparse policy may require a narrower query. The
ceiling prevents an almost-denied export from scanning an entire collection
while accumulating its requested visible page.

See [the reproducible workload measurements](performance/runtime_hardening.md)
for authenticated requests, graph density, external-policy exports, and
concurrent template/schema validation, including their measurement limits.

## Authorization and import upgrades

Upload the updated `docs/treetop/schema.json` before deploying this version.
Prospective collection probes omit unknown endpoint IDs and use distinct
`collection-probe:<collection_id>` identities; prospective creation uses
`prospective`. Policies accessing optional class/object IDs must guard them with
Cedar `has` checks. Stored resources continue to use their database identities.

Apply the new database migrations before starting workers. Import mutations and
item receipts commit in the same transaction. Each commit validates the claim
and lease, including a deferred database check. Recovery reconciles completed
imports from durable results; it does not automatically replay uncertain work.
Planning and dry-run result batches also require the current claim and pass the
same deferred commit fence.
Backups preserve terminal import results and omit execution indexes and claim
tokens. Restoring history never recreates worker authority. Receipt columns,
the concurrent unique index, and constraint validation deploy in separate
migration phases. If a concurrent index build is interrupted, remove its invalid
index before retrying the migration; startup requires the completed migration.

The storage SDK advances as a coordinated 0.2 release. Adapter implementations
must accept traversal budgets, implement claimed import execution, and preserve
subscription collection IDs. See the [generated inventory](generated/project_inventory.md)
for package versions, task kinds, and executables.
