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
