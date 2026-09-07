# Runtime boundaries

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
for authenticated requests, graph density, and external-policy exports,
including their measurement limits.

## Authorization and import upgrades

Upload the updated `docs/treetop/schema.json` before deploying this version.
Prospective collection probes omit unknown endpoint IDs and use distinct
`collection-probe:<collection_id>` identities; prospective creation uses
`prospective`. Policies accessing optional class/object IDs must guard them with
Cedar `has` checks. Stored resources continue to use their database identities.

Apply the single runtime-hardening migration before starting workers. Import mutations and
item receipts commit in the same transaction. Each commit validates the claim
and lease, including a deferred database check. Recovery reconciles completed
imports from durable results; it does not automatically replay uncertain work.
Planning and dry-run result batches also require the current claim and pass the
same deferred commit fence.
Backups preserve terminal import results and omit execution indexes and claim
tokens. Restoring history never recreates worker authority. Graph functions,
receipt columns, the unique index, and constraint validation deploy atomically
in one migration. It allows five seconds to acquire a lock and sixty seconds
per statement. The index build scans existing receipt history while the table
is locked; run the migration during a quiet period. A timeout rolls back the
whole migration, so retrying never requires cleaning up a partially built index.
Startup requires the completed migration.

The migration compatibility checker normally requires concurrent indexes. An
explicit `hubuum-compat: bounded-transactional-index` marker on an index statement
permits a reviewed transactional build only when positive `SET LOCAL` lock and
statement timeouts precede it. Nontransactional migrations cannot use this
exception.

The storage SDK advances as a coordinated 0.2 release. Adapter implementations
must accept traversal budgets and implement claimed import execution. See the
[generated inventory](generated/project_inventory.md)
for package versions, task kinds, and executables.
