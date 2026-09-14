# Schema diagnostics

`hubuum-schema-diagnostics` converts errors from Rust's `jsonschema` validator
into structured repair diagnostics. It has no Hubuum domain, database, HTTP,
task, configuration, or runtime dependency.

```rust
use hubuum_schema_diagnostics::SchemaDiagnosticInspection;
use serde_json::json;

let schema = json!({"properties": {"interfaces": {
    "items": {"properties": {"address": {"type": "string"}}}
}}});
let instance = json!({"interfaces": [{"address": 123}]});
let validator = jsonschema::validator_for(&schema)?;
let inspection = SchemaDiagnosticInspection::from_errors(
    &schema,
    &instance,
    validator.iter_errors(&instance),
);
if let SchemaDiagnosticInspection::Invalid(diagnostics) = inspection {
    println!("{}", serde_json::to_string_pretty(&diagnostics)?);
}
# Ok::<(), Box<dyn std::error::Error>>(())
```

The caller owns schema compilation, reference policy, and validation budgets.
Pass errors produced from the same schema and instance. The diagnostic layer
does not rerun validation. Its public `jsonschema` argument types are an
intentional integration surface; persisted diagnostics use crate-owned types.

Diagnostics retain JSON Pointers, keyword and schema locations, bounded expected
constraints, explanations, and actual types/sizes. Actual scalar values are
always redacted. Instance-owned dynamic property names are omitted unless the
name is declared in the schema's `properties`. Pointers use zero-based indexes
and JSON Pointer escaping. Each omission is explicit. Schema documents themselves
must be safe to disclose to the diagnostic reader; schema constraints are not
treated as secrets.

Collection retains at most 32 issues, including explanatory alternative-branch
issues. One lookahead detects additional failures and sets `truncated`; the
remaining number is deliberately unknown. Paths are limited to 512 bytes and
expected constraints to 1,024 bytes. Validator execution failures produce an
uninspectable result. These are collection limits, not a validator execution
deadline; admission limits remain the caller's responsibility.

The crate is a candidate for standalone publication. No publication is part of
the repair-report feature. Until its release policy is separated, its version
and packaging follow the repository's storage SDK train because `hubuum-domain`
depends on it. See the [API policy](../../docs/rust_api/hubuum-schema-diagnostics.md).
