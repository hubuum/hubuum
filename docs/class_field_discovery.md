# Class field discovery

`GET /api/v1/classes/by-name/{class_name}/fields` summarizes fields in an
authorized sample in one request. URL-encode the exact class name; numeric-looking
names are treated as names. This replaces client-side discovery's class lookup,
object list, shared-definition list, and personal-definition list. SDK and CLI
adoption are separate changes.

```http
GET /api/v1/classes/by-name/Hosts/fields?limit=100&depth=6&containers=false
Authorization: Bearer <token>
```

The default sample limit is 100, with a maximum of 250. `depth` defaults to 6 and
accepts 0–32. `containers=true` includes object and array paths as well as leaves.
Unknown or repeated options and invalid values return 400.

The response has `sampled: true`, `sampled_objects`, `effective_sample_limit`,
`max_depth`, `include_containers`, `truncation`, and `fields`. It never requests an
exact whole-class count. Objects are selected by ascending ID, so this is a
deterministic convenience sample, not a random or statistically representative
sample. Concurrent writes can change which objects and definitions are visible
between reads.

Each field contains:

- `field`: the CLI display label, such as `data.interfaces[*].name`, `S:hostname`,
  or `P:note`.
- `source`: `data`, `shared_computed`, or `personal_computed`.
- `path`: structured `property` and `array_item` segments for raw data. These
  distinguish a literal `a.b` property from nested `a.b`, and a literal `[*]`
  property from array traversal. Computed selectors have an empty path.
- `observation_count`: value occurrences, counting repeated array items separately.
- `object_count`: sampled objects with at least one occurrence at this path.
- `types`: occurrence counts keyed by `null`, `bool`, `number`, `string`, `array`,
  or `object`. Null is an observed value; an absent property is not.
- `example`: the first observed value's display preview, or null when unobserved.
  `example_truncated` reports shortening to 72 Unicode characters.
- `error_count`: computed occurrences accompanied by an evaluation error. The
  corresponding null result still contributes a null observation.

Enabled shared and caller-owned personal selectors appear even when no sampled
object supplies a value. Personal discovery loads the complete bounded definition
set for that owner and class, independent of the configured default page size.
Disabled definitions and other owners' definitions are excluded. Service accounts
receive no personal selectors. Fields sort by source, display label, then structured
path; distinct paths with ambiguous display labels remain separate entries.

## Authorization and bounds

The caller needs `ReadClass` for the class. Sampling requires `ReadCollection` and
`ReadObject`, intersected with token permission and resource scopes. External
policy decisions are applied before observations, examples, or sampled-object
counts are produced. The response uses `Cache-Control: private, no-store`.

Discovery bounds the sample to 250 objects, external-policy candidates to 1,024,
path depth to 32, traversal to 100,000 visited nodes across the sample, and total
field cardinality to 1,024. Property names along one path are limited to 512 UTF-8
bytes, previews to 72 Unicode characters, and serialized inventory output to
1 MiB. Container preview serialization stops at its preview budget rather than
building a full serialized subtree. Existing computed-evaluation limits still
apply. Object loading uses the existing bounded catalog page contract.

Enabled computed selectors are retained before data fields are trimmed to the
response-size budget.

`truncation` identifies the limits encountered: `sample_limit`, `candidate_limit`,
`depth`, `work_limit`, `field_limit`, `path_limit`, or `response_limit`. A candidate
limit indicates that the external-policy scan stopped without claiming the class
was exhausted; it does not reveal the number of unauthorized objects. Counts may
be partial when traversal limits are reached. Empty arrays and objects contribute
container observations only when requested, and never invent leaf values.

## CLI compatibility and future schema suggestions

Traversal, depth, array occurrence counts, type labels, computed selectors,
first-value previews, and source ordering follow the CLI implementation at
`46da8b31e2a507f70e8cb63b690eb79b295ee191`. Structured paths, distinct-object counts,
type frequencies, computed-error counts, and explicit truncation provide additional
evidence without changing those display conventions.

[Schema profiling](schema_profiling.md) consumes the raw authorized sample through the
sampling service, preserving property versus array-item paths and null versus
missing values. Display previews cannot establish numeric precision, complete
value sets, required properties, or schema compatibility. A truncated sample is
not a verified full corpus. The field-discovery endpoint makes no schema inference or compatibility guarantee.

## Measured request and decoding costs

Measured on 2026-10-25 with server revision
`58b50b2d01f45d91579724afae7ffbbb039906e3`, an unoptimized debug build running
two Actix workers on a shared Linux host. PostgreSQL used
`postgres:18.6-alpine3.24@sha256:77f585114c32fbca283dc835b0596f4e52b51b4c6662d7810b2f4084f60a1873`.
Each class contained 20 objects with nested hostname, array, null, and padding
fields, eight enabled shared hostname selectors, fresh materializations, and no
personal definitions. Padding was either 1,024 or 262,144 bytes per object.
Both workflows sampled the same 20 objects; each case ran sequentially for 100
iterations after warmup, over loopback HTTP without compression.

| Padding/object | Workflow | Requests | Response bytes | Wall p50/p95 (ms) | Server CPU/iteration (ms) |
| --- | --- | --- | --- | --- | --- |
| 1 KiB | Existing workflow | 4 | 37,722 | 56.37 / 62.67 | 47.6 |
| 1 KiB | Field inventory | 1 | 2,697 | 28.87 / 35.27 | 26.1 |
| 256 KiB | Existing workflow | 4 | 5,260,181 | 1,479.91 / 4,039.93 | 1,563.5 |
| 256 KiB | Field inventory | 1 | 2,697 | 1,096.79 / 1,383.64 | 1,133.8 |

Wall time includes HTTP and reference-client decoding. Server CPU is the server
process's user plus system CPU, sampled through `/proc` with 10 ms ticks and
averaged over the run; it excludes PostgreSQL and the client. The host was shared
with builds and tests, so these are comparative fixture observations rather than
production latency targets. The new endpoint still loads and computes the sample;
the response-size reduction does not eliminate that server work. These debug
numbers must not be compared with the release enrichment benchmark's timings.

Run the committed read-only driver against a prepared fixture:

```sh
python3 -I -S tests/python/run.py integration class-field-discovery \
  --url http://127.0.0.1:39495 --token-file /path/to/token \
  --class-name field-discovery-262144 --limit 20 --iterations 100 \
  --server-pid SERVER_PID --revision SERVER_COMMIT --output measurements.json
```

Optional `--cli /path/to/hubuum-cli --cli-revision CLI_COMMIT` captures actual
CLI diagnostics separately. CLI revision
`46da8b31e2a507f70e8cb63b690eb79b295ee191` confirmed four client API calls and
four HTTP attempts for each fixture. Its single debug runs took 608 ms and
2,273 ms respectively; these include startup/connection effects and are not
deserialization measurements. Field summarization took 0.76 ms and 0.74 ms.

To isolate Rust decoding, the captured response bodies were decoded 1,000 times
using that CLI's SDK 0.19.0 types (`ClassGet`, `ComputedFieldListResponse`,
`Vec<ComputedFieldDefinition>`, and `Vec<ComputedObject>`) and its exact decoder
sequence: `serde_json::Deserializer::from_str`,
`serde_path_to_error::deserialize`, then `Deserializer::end`. File reads, HTTP,
and destruction of the decoded result were outside each timer. The harness was
compiled with `rustc -O` and linked against the pinned CLI's debug dependencies.
The prospective inventory comparison decoded into `serde_json::Value`, since
the SDK does not yet expose the new typed response.

| Padding/object | Existing typed decode p50/p95 (ms) | Inventory untyped decode p50/p95 (ms) |
| --- | --- | --- |
| 1 KiB | 1.170 / 1.427 | 0.076 / 0.118 |
| 256 KiB | 54.139 / 81.989 | 0.076 / 0.079 |

This isolates the current CLI decoding cost, but does not claim a measured future
typed SDK or CLI implementation. The reference driver's Python decoding numbers
are reported separately and are not substituted for Rust measurements.
