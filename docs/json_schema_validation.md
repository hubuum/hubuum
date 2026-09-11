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
| Instance document | 16,384 JSON nodes; 1,048,576 estimated encoded bytes |
| JSON nesting and expanded schema nesting | 64 levels |
| Expanded schema cost | 16,384 work units |
| Schema cost times instance size | 16,777,216 work units |
| `uniqueItems` | Instance-work estimate also multiplies by instance node count |
| Numeric representation | 128 characters; exponent between -308 and 308 |
| Compiled pattern and DFA cache | 65,536 bytes each |
| Regex backtracking | 10,000 attempts |

Byte estimates include conservative allowances for JSON escaping and punctuation.
Work units are admission estimates, not CPU instruction counts or a wall-clock
service-level guarantee. Pattern limits are configured through the validator's
[pattern options](https://docs.rs/jsonschema/0.49.9/jsonschema/struct.PatternOptions.html).

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

Measurements below are from 11 September 2026 on Linux x86_64, using the locked
`jsonschema` 0.49.9 dependency, the debug profile, and the repository's production
release profile (`opt-level = "z"`, fat LTO, one codegen unit). Each adversarial measurement includes meta-schema validation, rejected
compilation, and rejected instance validation; the first case also includes
cold meta-schema initialization. These are domain-library test measurements,
not production server capacity or throughput estimates.

| Repeated `allOf` reference depth | Schema bytes | Debug rejection | Release rejection |
| --- | --- | --- | --- |
| 15 | 987 | 23.850 ms | 4.164 ms |
| 20 | 1307 | 6.509 ms | 1.306 ms |
| 25 | 1627 | 6.683 ms | 1.318 ms |

All nine reference probes (`allOf`, `anyOf`, and `oneOf` at depths 15, 20, and 25)
were rejected within the process caps. An accepted depth-three `allOf` schema
compiled in 0.833 ms debug and 0.133 ms release; 200 alternating
valid/invalid instance checks took 56.310 ms debug and 9.957 ms release.
The original issue's pre-fix debug timings remain evidence of amplification;
they must not be interpreted as release-build capacity.
