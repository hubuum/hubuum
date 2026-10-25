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

Future schema profiling should consume the raw authorized sample through the
sampling service, preserving property versus array-item paths and null versus
missing values. Display previews cannot establish numeric precision, complete
value sets, required properties, or schema compatibility. A truncated sample is
not a verified full corpus. This endpoint adds no profiler dependency and makes no
schema inference or compatibility guarantee.
