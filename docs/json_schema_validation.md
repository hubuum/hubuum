# JSON Schema validation limits

Validated classes use conservative admission budgets before schema compilation
and before each instance evaluation, including cache hits. Validation stays
inside the existing storage transaction, with its class locks and revision
checks. A validated schema's private budget travels with its compiled validator.

A small schema can describe exponentially repeated work through local references
and combinators. Document-size limits alone cannot prevent that expansion.
Hubuum charges every reference occurrence for the complete referenced subtree
and rejects cycles. Inspection itself stops at the expansion limit and bounds
reference depth on every occurrence. Failed `anyOf`, `oneOf`, and `contains`
checks receive extra weight because the engine can revisit them for errors.

## Limits and migration

| Resource | Limit |
| --- | --- |
| Schema document | 4,096 JSON nodes; 65,536 estimated encoded bytes |
| Instance document | 16,384 JSON nodes; 2,097,152 estimated encoded bytes by default |
| JSON nesting and expanded schema nesting | 64 levels |
| Expanded schema cost | 16,384 work units |
| Schema cost times instance size | 268,435,456 work units by default |
| `uniqueItems` | Instance-work estimate also multiplies by instance node count |
| Numeric representation | 128 characters; exponent between -308 and 308 |
| Compiled pattern and DFA cache | 65,536 bytes each |
| Regex backtracking | 10,000 attempts |

Byte estimates count actual UTF-8 and JSON escaping, with a conservative punctuation allowance. Ordinary strings are no longer charged six bytes for every byte.
Work units are admission estimates, not CPU instruction counts or a wall-clock
service-level guarantee. Pattern limits are configured through the validator's
[pattern options](https://docs.rs/jsonschema/0.49.9/jsonschema/struct.PatternOptions.html).

The server and `hubuum-admin` accept these deployment settings. Values outside
these ranges fail startup; none of the budgets can be disabled.

| Environment variable | CLI option | Default | Allowed range |
| --- | --- | --- | --- |
| `HUBUUM_SCHEMA_MAX_BYTES` | `--schema-max-bytes` | 65,536 | 1,024–1,048,576 |
| `HUBUUM_SCHEMA_MAX_EXPANDED_WORK` | `--schema-max-expanded-work` | 16,384 | 1–65,536 |
| `HUBUUM_SCHEMA_MAX_INSTANCE_BYTES` | `--schema-max-instance-bytes` | 2,097,152 | 1,024–16,777,216 |
| `HUBUUM_SCHEMA_MAX_INSTANCE_WORK` | `--schema-max-instance-work` | 268,435,456 | 1–1,073,741,824 |

For example, to allow up to 4 MiB estimated object documents and twice the default
validation work during restore or background validation:

```bash
HUBUUM_SCHEMA_MAX_INSTANCE_BYTES=4194304
HUBUUM_SCHEMA_MAX_INSTANCE_WORK=536870912
```

Schema admission is separate from transport limits. Ordinary JSON request bodies
retain their existing 2 MiB ceiling, including import requests and the request
envelope. Restore uploads retain their separately configured upload ceiling.

Configure every API process, worker, and administrative restore executor with the
same values and restart them together. The repository Compose deployment forwards
these settings from `.env`; single-host installations write the defaults and
preserve operator changes during `--refresh-config`. The administrator configuration
response includes the effective `schema_validation` budgets. Settings are fixed
for a storage handle's lifetime and are not stored in class revisions or backups.
The SDK uses `JsonSchemaLimits::builder()` and explicit `with_schema_limits` or
`try_new_with_limits` constructors; config-free entry points use the defaults.

Raising a budget admits more expensive work; it does not change the fixed node,
nesting, reference, numeric, or regex guards. Increasing only the byte allowance
may still leave a complex object over its work allowance. The original benchmark's
16 KiB, 256 KiB, and 1 MiB payload strings, each with 128 integers, fit the defaults.

Lowering limits can reject existing schemas or object writes. Check the new
settings against a backup before deployment. Restore validation uses the target
deployment's budgets, including history and saved validation evidence; a backup
that exceeds them is rejected before live state is replaced. After changing object
budgets, request revalidation for enforced classes to refresh stored compliance
counts; changing process configuration does not itself rewrite existing evidence.

This is a **breaking validation change**. Before upgrading, inspect validated
class schemas and replace these unsupported constructs:

- Recursive, dynamic, recursive-draft, or anchored references. Use acyclic local
  JSON Pointer references such as `#/$defs/address`; `~0` and `~1` pointer escapes
  remain supported. Percent-encoded fragments and references to the root are
  rejected.
- Nested `$id` or legacy `id` resource declarations. Keep a single root resource.
- `unevaluatedProperties` and `unevaluatedItems`, whose annotation-dependent
  reevaluation is outside the supported cost model. Use explicit `properties`,
  `additionalProperties`, and `items` constraints where they express the intended
  validation policy; review the semantics when combinators are involved.

Simplify repeated references and combinators, reduce pattern complexity, and
split oversized schemas or data. Reduce excessive numeric precision or exponent.
Errors identify the relevant limit and suggest how to reduce the input.

Schemas already in storage are checked again when compiled for a write; a cache
hit still checks the instance budget. Update unsupported validated class schemas
before resuming object writes. No database migration or revision reset is required. Schema-only metadata
validation retains its existing ability to describe external references when
instance validation is disabled, but applies
schema document limits before meta-schema validation. Enabling instance validation
requires the complete compilation budget and local-reference policy to pass.

## Reproducible resource evidence

Run the standard-library-only probe from the repository root:

```bash
python3 scripts/check-json-schema-budget.py
python3 scripts/check-json-schema-budget.py --release
```

The script builds the `hubuum-domain` test executable, then runs only the
adversarial probe in a separate process. The probe has a six-second wall limit;
Linux hosts additionally enforce five CPU seconds and 512 MiB address space.
Compilation happens before those execution limits. `run_tests.sh` includes the
debug probe, and benchmark CI retains a separate optimized release probe with
the template/schema resource evidence. The concurrency benchmark preserves its
full template workload and validates schema data in batches of four items to
respect admission limits; its timings include all batches and their preparation.

Measurements below are from 12 September 2026 on macOS ARM64, using the locked
`jsonschema` 0.49.9 dependency, the debug profile, and the repository's production
release profile (`opt-level = "z"`, fat LTO, one codegen unit). Each adversarial
measurement includes meta-schema validation, rejected compilation, and rejected
instance validation; the first case also includes cold meta-schema initialization.
The portable six-second wall limit passed in both profiles. Linux CI additionally
runs the CPU and address-space caps described above. These are domain-library
measurements, not server capacity or throughput estimates.

| Expansion budget | Repeated `allOf` depth | Schema bytes | Debug rejection | Release rejection |
| --- | --- | --- | --- | --- |
| 16,384 | 15 | 987 | 17.778 ms | 3.744 ms |
| 16,384 | 20 | 1307 | 7.639 ms | 1.707 ms |
| 16,384 | 25 | 1627 | 7.707 ms | 1.705 ms |
| 65,536 | 15 | 987 | 42.878 ms | 9.476 ms |
| 65,536 | 20 | 1307 | 42.666 ms | 9.543 ms |
| 65,536 | 25 | 1627 | 42.534 ms | 9.547 ms |

All eighteen adversarial reference probes (`allOf`, `anyOf`, and `oneOf` at depths
15, 20, and 25 under both budget configurations) were rejected within the wall cap.
An accepted depth-three `allOf` schema compiled in 0.512 ms debug and
0.119 ms release; 200 alternating valid/invalid instance checks took
31.027 ms debug and 6.255 ms release.
