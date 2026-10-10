# Schema evolution implementation

For the user workflow, see [class schema evolution](../schema_evolution.md).

## Storage architecture and dependencies

`SchemaEvolutionStorage` is required by `WorkflowStorage`. Domain identities and
compiled proof live in `hubuum-domain`; private-fielded requests, checkpoints,
and evidence live in `hubuum-storage-core`. Application services use the opaque
observed storage handle. PostgreSQL SQL, locking, triggers, and adapter errors
remain in `hubuum-storage-postgres`; memory implements the complete contract.

Activation invalidates shared computed-field evaluation generations and queues
existing fenced rebuild work. `SchemaReference` is the reusable dependency
identity for future indexes and effective/inherited schemas. This change does
not add declarative indexes, inheritance, or automatic JSON transformations.

The coordinated SDK crates move from 0.2 to 0.3 because the mandatory
capability, task/event vocabularies, import DTO, and backup sections change.
Adapters must implement every required schema method, including the separate
`get_schema_work_report` projection with its mandatory `StorageSchemaReportBudget`,
handle `schema_validation`,
map the new event entities and logical sections, and preserve the schema
transaction and lease semantics. See the storage boundary inventories.

## Verification and performance

Shared backend tests cover policies, atomic evidence and imports, redaction,
authorization, concurrent activation, cancellation, recovery, and batch bounds.
Native PostgreSQL tests retain direct constraint violations, an object write
between inspection and commit, and a 128-object workload with 256 KiB JSON rows.
That workload asserts constant activation query count, two checkouts per batch,
and the aggregate byte bound. Ordinary backup/restore conformance exercises the
new sections with both adapters.

Impact regressions scan 8,192 mismatches across one or 256 reasons while bounding
serialized checkpoint size. PostgreSQL probes measure query counts and rendered
SQL/bind bytes across 4 and 32 default batches, rejecting accumulated finding
reads or growing checkpoint writes. Native fault tests verify atomic rollback
and consistent reports while another batch commits.

Run `cargo bench --bench schema_validation_criterion` for deterministic compiled
validation and budget-rejection throughput without database or global config.
Separate groups cover accepted 16/256/1024 KiB payloads and rejected 2/3/4 MiB
payloads, each with 128 integer samples. Fixtures check their expected outcome
before timing. Batches contain at most 64 documents and 8 MiB of serialized JSON;
reported throughput counts documents inspected. Admission also charges actual
JSON escaping with conservative punctuation estimates and schema complexity, so a serialized object below the
worker's byte limit can still exceed the validation budget.
The native storage regression measures the database behavior; benchmark numbers
are hardware-dependent and do not establish a worst-case lock or CPU deadline.
