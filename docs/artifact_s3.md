# S3 artifact byte store

Issue [#509](https://github.com/hubuum/hubuum/issues/509) adds the optional `s3`
feature of the internal `hubuum-artifacts` crate. It implements the existing
`ArtifactStore` protocol with independent bounded SHA-256 read-back verification.
It does not implement `TaskArtifactStore` or select production task storage.
Exports, backups, protected retention, authorization and atomic task completion
continue to use PostgreSQL. Production routing needs a separate primary-store
fencing and recovery integration. Cold retrieval, archive queries, migration and
retention P6 are outside this capability.

## Configuration and privacy

Create `S3Config::builder(destination)` with a stable, validated `Destination`,
bucket, region and credentials. An optional endpoint must be an HTTPS origin
without credentials, path, query or fragment. Private CA PEM certificates augment
the native trust roots; hostname verification remains enabled. Session tokens
are supported. Credentials are not serialized or printed by configuration Debug;
adapter errors never include SDK request/response Debug or provider messages.
Resolve production secrets through the existing secret provider before building
configuration. There are no new production environment settings or API selectors.

The SDK connector uses the patched `h2` 0.4 line. The dependency guard requires
the [RUSTSEC-2026-0258 fix](https://rustsec.org/advisories/RUSTSEC-2026-0258)
(`>=0.4.16`); vulnerable 0.3 and earlier 0.4 versions remain forbidden.
Actix's inbound HTTP/2 feature stays disabled in every TLS configuration.

`S3ArtifactStore::connect` requires an existing version-enabled private bucket.
It checks versioning, ACL and public-policy status; a missing policy is allowed
with a private ACL. Unknown/denied policy inspection fails closed. Bucket policy,
network restrictions and credential privileges must remain private after startup.
The adapter never makes buckets public or returns presigned download URLs.
The pinned MinIO release emits an uppercase XML policy-status boolean that the
SDK rejects. For an unparseable successful response or an unsupported/missing
policy-status endpoint, the adapter separately requires a confirmed
`NoSuchBucketPolicy` response. It refuses configured policies on that fallback
path, including policies an operator believes are private.

Each configured instance retains its destination UUID, name, bucket, region and
endpoint. Credential/CA rotation preserves identity; replacing a physical bucket
or endpoint requires a new destination UUID. The durable binding rejects changes
within a bucket. Keep all referenced destination configurations available, and
route retained references by their destination UUID, never the current default.
Removing/recreating a bucket or rolling back S3 control state is unsupported.
The standalone adapter is not protected deletion authority or a restore boundary.

Required permissions cover bucket versioning/ACL/policy inspection, current control
object GET and conditional PUT, version-specific data GET/HEAD/DELETE, multipart
create/upload/complete/abort, and object/version/upload listing. It does not request
governance-retention bypass, alter locks, create buckets or modify policies.
An object lock or access denial leaves cleanup pending. Configure credentials and
bucket lifecycle rules so permanent control/ownership records cannot be removed
or superseded by another application. Do not expire managed data independently.

## Transfer and retained format

The namespace is `hubuum-artifacts-v1/<destination UUID>/`. This is an unpublished
internal format, not a supported SDK/storage interchange format. Preserve the
complete namespace and all its control versions while it is in use.

| Key | Contents and lifetime |
| --- | --- |
| `destination` | Immutable JSON binding of name, bucket, region and endpoint; credentials excluded. |
| `attempts/<attempt UUID>` | Permanent canonical identity ownership record. |
| `versions/<object-version UUID>` | Permanent canonical identity ownership record. |
| `states/<artifact UUID>` | Current JSON control record, changed only with conditional ETag PUT. |
| `intents/<artifact>/<attempt>/<object-version>/<random UUID>` | Permanent upload intent: empty before initiation, then JSON containing the bounded provider upload IDs. |
| `data/<artifact>/<attempt>/<object-version>/<random UUID>` | One immutable protocol block at an exact S3 version. |

`hubuum-artifacts-v1/names/<name>` permanently binds a name to its destination
within the bucket. Interrupted ownership registration is retryable for the same
identity; cleanup never releases identity claims. Another artifact cannot reuse
an attempt or object-version UUID after cancellation, cleanup or process restart.

Control JSON contains the identity, four limits (block bytes, total bytes, block
count, manifest bytes), original Unix expiry, phase, ordered block locators/lengths/
SHA-256 hashes, and optional canonical manifest bytes. Provider version strings
must be non-null and at most 1,024 bytes. Control reads/writes are bounded to
8 MiB. Operational writes initially support at most 1,024 blocks, 1 MiB per block,
1 GiB total and a 256 KiB manifest. Lower per-attempt limits remain supported.
This narrower envelope does not change the shared format ceiling.

Every block uses one multipart upload with one final part, so the S3 non-final
5 MiB minimum does not apply. Only one bounded block is transferred at a time;
the adapter does not assemble a complete artifact in memory. The extra request
cost of small multipart objects is deliberate and measured by the benchmark.
SDK retries are limited to two attempts, with 5-second connect, 15-second attempt
and 30-second operation deadlines. Stream-body reads have a separate 30-second
deadline and enforce the declared/read byte ceiling.

Successful upload records an exact provider version in a conditional state update.
An identical append retry compares length and application SHA-256; different bytes,
out-of-order writes and writes after sealing fail. Losing/ambiguous uploads remain
discoverable by their attempt prefix. Sealing freezes block count/length and the
expected producer manifest; it does not create a verification proof.
Each upload's durable intent identifies its exact data key even if initiation's
response is lost. The pinned MinIO implementation supports multipart listing for
an exact object key, rather than general prefix discovery; recovery uses these
intents instead of assuming prefix discovery is complete.
Returned upload IDs (at most 4 KiB) are persisted before sending parts. Cleanup
also saves discovered IDs before aborting, retaining at most 16 IDs per intent
in a record bounded to 1 MiB. Unsupported excess identities fail with an explicit
limit outcome; they cannot silently turn into acknowledged cleanup. Cleanup
retries those IDs even when a provider omits them from its multipart listing,
and verifies `ListParts` is empty or the upload no longer exists after abort.
An initial `ListParts` distinguishes a live upload from an already-completed ID;
the fixture's successful idempotent abort response alone cannot count reclaimed
work. IDs remain durable through failed post-abort verification.
Remaining parts or an ambiguous verification failure keep the cursor pending.

The shared `verify`/`verify_and_publish` protocol independently reads every exact
version, checks manifest identity and ordered bounded blocks, full-stream digest
and exact EOF. S3 ETags and provider full/composite checksums are transport evidence,
never application SHA-256 proof. Publication uses an ETag condition to fence abort
and concurrent state changes. Readers require durable publication and the separately
retained catalog digest, and use the shared verified-block reader.

## Recovery and cleanup

Staging lasts one hour from creation, independent of retries. Participating hosts
need synchronized clocks. A fresh client can reopen durable sealed attempts and
verify without a worker-local spool. This does not permit replaying arbitrary
producers after task lease loss. Cancellation durably changes state to `Deleting`;
an unknown attempt returns `NotFound` because no fence exists. Published independent
artifacts cannot be aborted through the current shared contract.

`cleanup` visits one control record per call, using a process-local scan cursor.
An expired non-published attempt is first conditionally fenced. Staging/sealed
attempts are left alone; published/deleting state has immutable membership, making
orphan reconciliation safe. One permanent upload intent is visited per call,
with an exact-key multipart listing and a conditional durable intent cursor.
An incomplete listing retains the cursor position; empty intent scans wrap it.
This finds partial uploads after provider restart without local state.
Each call removes at most 16 data versions/delete
markers or multipart uploads. Listing pages contain at most 16 entries; retained
block membership is bounded to 1,024. Call maintenance periodically even after a
zero result: it may have visited retained work or reached a cursor wrap.

Deletion always supplies an exact provider version and then HEAD-checks that
version for absence. A current delete marker cannot hide a retained payload.
Locks, holds, outages and ambiguous failures leave the durable obligation intact.
Deleting tombstones remain eligible forever, allowing later maintenance to find
uploads that complete after an interrupted request or cleanup pass. A single
empty listing is not a global erasure receipt. Control/ownership metadata and its
historical S3 versions remain retained; this capability does not claim permanent
erasure or bounded total metadata growth.

## Verification and benchmarks

Run the explicit fixture harness with Python 3.11+ and only its standard library:

```bash
python3 -I -S tests/python/run.py integration s3-artifacts
cargo clippy -p hubuum-artifacts --features s3 --all-targets -- -D warnings
```

The harness downloads upstream MinIO `RELEASE.2025-09-07T16-13-09Z` from
[its GitHub release](https://github.com/minio/minio/releases/tag/RELEASE.2025-09-07T16-13-09Z)
and verifies the platform-specific SHA-256 pinned from the release API's asset
`digest` before making it executable. The exact URL/digest is printed each run.
The digest table is in `tests/python/integration/s3_artifacts.py`; it supports
Linux x86_64/ARM64 and macOS x86_64/ARM64. Removed official container images are not
required. TLS uses a disposable private CA and a separately signed server leaf,
with verified `localhost`/loopback SANs. Data, executable, certificates and logs
are temporary; the server and test processes are reaped on success or failure.
Fixture credentials are fixed disposable values restricted to loopback.

Rust owns protocol assertions, corruption, concurrency, ownership, cancellation,
version-aware deletion, legal-hold behavior, real process restart and outage
recovery. Python owns provisioning, digest verification and process cleanup.
The production integration CI job explicitly runs the S3 feature's Clippy and
live contracts. This evidence qualifies the named MinIO fixture; AWS S3 and other
compatible providers require their own qualification. Unsupported: cold retrieval,
public buckets, unversioned/suspended buckets, HTTP endpoints, presigned downloads,
range reads, automatic credential refresh and cross-bucket provider migration.

The Criterion entrypoint measures 64 KiB, 1 MiB and 8 MiB complete bounded
write/verify/publish flows and warm verified reads:

```bash
cargo bench -p hubuum-artifacts --features s3 --bench s3_artifact_storage_criterion -- --noplot
```

It provisions the same pinned fixture automatically when no fixture endpoint is
present. Bucket creation, TLS setup, process startup and final teardown are outside
the measurement; attempt registration, S3 calls and independent verification are
inside write measurements. Published copies accumulate only in the disposable
fixture and disappear on teardown. Use the existing `storage` benchmark tag for
target selection. Short profiles diagnose this adapter; they do not qualify
producer RSS, multi-replica task throughput, constrained P profiles or arbitrary
provider/network performance. Record source SHA, host and fixture identity with
results, and keep full correctness/container validation enabled.

No migration, supported SDK/API break or production upgrade action is introduced.
Changelog review: this is an internal optional byte-store capability, so no
internal-only changelog entry is added. Task-routing upgrade notes belong to the
later production integration.
