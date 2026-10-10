# Class query usage declarations

A declaration records an expected workload for one JSON path in one class. It
is separate from the class JSON schema. Acceptance confirms that intent was
recorded; it does not promise an index, resource allocation, or a speedup.

Declarations never change filtering, authorization, ordering, pagination, or
validation. Queries remain usable when a declaration is absent, ignored,
incompatible with the schema, or awaiting adapter work.

## Management

The versioned endpoints are:

| Method | Path | Permission |
| --- | --- | --- |
| GET | `/api/v1/classes/{class_id}/query-usage` | `ReadClass` |
| POST | `/api/v1/classes/{class_id}/query-usage` | `UpdateClass` |
| PUT | `/api/v1/classes/{class_id}/query-usage/{declaration_id}` | `UpdateClass` |
| DELETE | `/api/v1/classes/{class_id}/query-usage/{declaration_id}?expected_revision=1` | `UpdateClass` |

For example, POST this body to record text equality intent:

```json
{
  "pattern": {
    "path": "hardware,serial_number",
    "value_type": "string",
    "operations": ["equals"]
  }
}
```

The initial vocabulary supports `equals` for string and boolean values, and
`equals`, `gt`, `gte`, `lt`, `lte`, and `between` for numeric values. These refer
to existing filter semantics; a declaration cannot override the query parser's
inference from the query value. Numeric query operands retain the query API's
existing integer operand restrictions.

Paths use the existing comma-separated vocabulary, with at most 512 bytes and
32 segments. A class can have at most 32 declarations, with one declaration per
path and value type. Duplicate operations are normalized. PUT replaces the
whole pattern and requires `expected_revision` in the JSON body. DELETE requires
the revision in its query string. Stale revisions and duplicate declarations
return conflicts. Replacing a declaration with identical intent leaves its
revision and audit history unchanged.

Mutations record `query_usage_declaration` audit events. Responses preserve
creation and modification attribution. Class or collection deletion withdraws
associated declarations. Storage rechecks the authorized collection under the
same lock that protects the mutation, including concurrent class moves.

## Schema assessment

Responses assess intent against the current class schema. Straightforward
property and array-item types can be compatible or incompatible; unresolved
references, conditional schemas, missing type information, and alternatives
remain unknown. Assessment is recomputed on reads after schema changes.
Incompatible declarations remain recorded and impose no additional object
constraint. Compatibility does not establish that a native optimization is
available or useful.

## Storage and recovery

Both selectable adapters implement the complete required `QueryUsageStorage`
contract. Memory storage records intent in process memory. PostgreSQL stores it
transactionally with the corresponding audit event. Native actions and
analysis are separate optional adapter capabilities; declaration management
does not depend on either being available.

Full backup version 9 includes logical declarations and their provenance.
Versions 6 through 8 restore with an empty declaration set. Physical optimization
resources are not portable backup content. The restore target must support
version 9 before it can restore any new backup, including one with no declarations.

## Observations and analysis

`POST /api/v1/classes/{class_id}/query-usage/analysis` accepts
`{"proposed": []}` or up to 32 hypothetical patterns. It requires an unscoped
administrator token and `ReadClass`. Reviews can include shared native resource
facts, so ordinary class management permission is insufficient. The operation
never adopts suggestions, changes declarations, or creates native resources.

Memory reports `unavailable`. PostgreSQL reports `insufficient_evidence` when
usable workload evidence is missing or bounded catalog inspection is incomplete.
A `complete` review can contain zero suggestions. Neither missing observations
nor zero native scans establishes that a resource is unused.

Collection is disabled by default. Enable it with
`HUBUUM_QUERY_OBSERVATIONS_ENABLED=true`. Effective settings appear in the
administrator configuration endpoint. CLI flags use the corresponding lowercase
hyphenated names; TOML keys use lowercase underscores under
`[query_observations]`.

| Environment suffix after `HUBUUM_QUERY_OBSERVATIONS_` | Default | Accepted range |
| --- | --- | --- |
| `SAMPLE_EVERY` | 16 | 1–1,000,000 logical requests |
| `MAX_PATTERNS` | 2048 | 1–16,384 per process |
| `MAX_PATTERNS_PER_CLASS` | 64 | 1–128, no greater than the process limit |
| `RETENTION_SECONDS` | 86400 | 1–604,800 |
| `MAX_PREDICATES_PER_QUERY` | 16 | 1–32 |

Collection retains only class attribution, validated paths, scalar types,
operations, successful sampled request counts, timestamps, and whole-query
elapsed time. Query values, object contents, and credentials are not retained.
Class IDs and paths are never metric labels. Count-plus-page requests count once.
Only direct filters with an unambiguous class are observed; unsupported,
negated, related, and computed predicates are omitted. Operands exceeding 4096
bytes are skipped. Failed requests add no observation.

Entries expire from their first observation; continued use does not extend their
retention window. Expiry is applied on subsequent collection or review. Capacity
drops are reported. Observations are process-local and reset on restart; a load
balanced deployment's review covers only the responding process. Whole-query
timing includes other predicates and authorization and cannot establish the cost
or benefit of an individual predicate.

The initial PostgreSQL provider suggests new plain string equality declarations
after at least five successful sampled observations, unless that pattern is
already declared or covered by a matching usable native text expression index.
It inspects at most 128 indexes, applies a two-second transaction statement
timeout, and withholds suggestions if catalog coverage is incomplete. Other
scalar declarations remain valid but are explicitly unsupported by this initial
provider. Paths containing a case-insensitive `null` segment are excluded because
the existing PostgreSQL filter compiler treats that unquoted array element as SQL
NULL. Resource sizes and cumulative native scans are facts at assessment time;
future benefit and maintenance cost remain unknown.

Suggestions carry the same pattern accepted by ordinary declaration creation.
Review and adopt them through the normal permission-checked, audited CRUD
endpoints, which revalidate current state. No automatic adoption is performed.

## PostgreSQL native preparation

The optional PostgreSQL executor initially prepares plain string equality using
a shared hash index on the exact JSON text expression used by the query compiler.
It does not rewrite queries or add object constraints. Numeric, boolean, range,
and unsupported path declarations remain recorded without preparation. Hash keys
have bounded storage, so indexing cannot make valid long text values fail the
btree index-entry size limit.

Run a bounded pass outside the server and normal task worker:

```sh
hubuum-admin --reconcile-query-usage --database-role-mode split
```

Supply the migration credential through the existing secret source and configure
the owner role as for migrations. The runtime account receives read-only access
to native ownership metadata and cannot allocate or delete physical resources.
Schedule the command periodically to prepare new intent and retry cleanup. An
optional `--query-usage-class 42` restricts new planning; cleanup remains global.
Memory storage explicitly reports this executor as unsupported.

Each pass plans at most 64 declarations and maintains at most eight owned
resources, each with at most 128 owners. A durable scan cursor advances global
planning so already covered declarations cannot starve later requests. Creation is limited to a source object table of at most 512 MiB. Native
statements have a two-second timeout and a 100 ms lock timeout. Index creation
uses transactional DDL and can briefly block object writes within that budget;
larger or busy deployments can supply independently managed indexes instead.
These limits may defer preparation indefinitely without rejecting declarations
or changing query results. Run outside peak traffic and inspect the JSON report.
No performance benefit is promised.

Declarations on the same path share one owned resource across classes. Updating
a pattern or deleting its declaration, class, or collection withdraws ownership.
The last withdrawal leaves durable cleanup state until a later pass finishes.
An independently managed matching index satisfies intent without being claimed
or dropped. Cleanup verifies the stored OID, generated name, definition, and
random ownership marker before dropping an index. Renamed, replaced, or otherwise
unverifiable resources retain a diagnostic for operator review.

The executor defers while restore maintenance is active.

Planning commits pending intent before execution. Every attempt rechecks live
owners under locks; DDL and the physical ownership update commit together. A
failed transaction leaves no partially committed index. Restarting the executor
retries persisted work, and withdrawn queued intent cannot recreate a resource.
The report exposes pending, ready, and cleanup states and generic diagnostics.
These are PostgreSQL operational states, not portable declaration states.

Administrative analysis adds verified ownership, sharing counts, removal
eligibility, and adapter progress. `can_prepare` describes adapter support; it
does not establish that the separate executor is running or that its budgets
permit an allocation. Native counters still cannot prove that a particular
class uses a shared index.

Portable backups contain declarations only. Restore clears native ownership and
leaves local resources available for reconciliation or cleanup against restored
intent. Downgrading the native-resource migration requires withdrawing
associated declarations and finishing cleanup first, so ownership evidence is
never silently discarded.

## Operator walkthrough

Use an existing class and ordinary object filters throughout this workflow. The
examples use class `42`, path `hardware,serial_number`, an unscoped administrator
`HUBUUM_TOKEN`, and a `HUBUUM_URL` without a trailing slash.

1. **Observe.** Enable `HUBUUM_QUERY_OBSERVATIONS_ENABLED=true` on the API process
   and restart it. In a disposable rehearsal, also set
   `HUBUUM_QUERY_OBSERVATIONS_SAMPLE_EVERY=1` and issue at least five successful
   requests like this. In production, keep the normal sampling rate and let real
   traffic supply the evidence. Send the analysis request to the same process;
   observations are local to each replica.

   ```sh
   curl --fail-with-body --get "$HUBUUM_URL/api/v1/classes/42/" \
     --header "Authorization: Bearer $HUBUUM_TOKEN" \
     --data-urlencode 'json_data=hardware,serial_number=SN-example'
   ```

2. **Analyze.** Review the observations, limitations, and proposed patterns:

   ```sh
   curl --fail-with-body "$HUBUUM_URL/api/v1/classes/42/query-usage/analysis" \
     --header "Authorization: Bearer $HUBUUM_TOKEN" \
     --header 'Content-Type: application/json' --data '{"proposed":[]}'
   ```

   A suggestion carries a `proposed` pattern. Missing evidence returns
   `insufficient_evidence`; existing coverage or an existing declaration can
   legitimately leave `suggestions` empty. Review does not adopt anything.

3. **Adopt.** If the suggested workload matches your intent, copy its `proposed`
   pattern into the ordinary creation request's `pattern` field:

   ```sh
   curl --fail-with-body "$HUBUUM_URL/api/v1/classes/42/query-usage" \
     --header "Authorization: Bearer $HUBUUM_TOKEN" \
     --header 'Content-Type: application/json' \
     --data '{"pattern":{"path":"hardware,serial_number","value_type":"string","operations":["equals"]}}'
   ```

   Keep the returned declaration `id` and `revision`. The example is appropriate
   only when no declaration already exists for this path and type.

4. **Reconcile.** With the separate migration credential and owner-role settings
   configured, run a privileged pass outside peak traffic:

   ```sh
   hubuum-admin --reconcile-query-usage --database-role-mode split \
     --query-usage-class 42
   ```

   Inspect `deferred` and each resource's `state` and `last_error` in the JSON
   report. Schedule later passes for pending work and cleanup. The class option
   restricts new planning; cleanup still covers all managed resources.

5. **Inspect.** Repeat the analysis request. Check `assessments[].resources` for
   native coverage and ownership, and `assessments[].adapter_progress` for pending,
   ready, or cleanup state. Repeat representative queries and compare query plans
   and timings using PostgreSQL tooling. PostgreSQL's planner decides whether to
   use the index; a ready resource does not promise a speedup. Native scan counters
   are cumulative and shared across classes, not evidence of one class's benefit.

   This preparation supports plain JSON string equality. It does not directly
   accelerate grouping, sorting, substring search, numeric ranges, or computed
   predicates. An eligible equality filter before aggregation may benefit if the
   planner uses the index.

6. **Withdraw.** Read the current declaration revision before deleting it. For
   example, if the declaration is still `id=17`, `revision=1`:

   ```sh
   curl --fail-with-body --request DELETE \
     "$HUBUUM_URL/api/v1/classes/42/query-usage/17?expected_revision=1" \
     --header "Authorization: Bearer $HUBUUM_TOKEN"
   hubuum-admin --reconcile-query-usage --database-role-mode split
   ```

   A stale revision returns a conflict. After successful withdrawal, inspect the
   executor report: a shared resource remains until its last owner withdraws;
   independently managed indexes remain under operator control. Ordinary filters
   continue to work throughout preparation and cleanup.
