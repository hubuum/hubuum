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
