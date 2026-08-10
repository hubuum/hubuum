# Hermetic Treetop conformance fixture

The Treetop authorization contract is exercised by
`scripts/run-treetop-conformance.sh`. The runner starts a real Treetop service,
loads this directory's schema and policies over a loopback fixture server, adds
a private-CA TLS proxy, and executes every ignored test in
`src/tests/permissions/live_treetop_parity.rs`.

The service is immutable and upload-free. Its image digest and corresponding
source revision are recorded in `.github/treetop-conformance.env`. The runner
rejects tag-only images, non-full revisions, and any change to the expected test
count. There is no successful path that skips the suite because an environment
variable is absent.

## Fixed identifiers

The dedicated test database and Cedar fixture use these IDs:

- `User::"9001"`: ordinary user and task owner
- `User::"9002"`: administrator
- `User::"9003"`: user without a grant
- `Group::"9100"`: administrator group
- `Group::"9101"`: ordinary group
- `HubuumCollection::"9201"`: granted collection
- `HubuumCollection::"9202"`: denied collection
- `HubuumClass::"9301"`: class in the granted collection
- `HubuumObject::"9401"`: object in the granted class
- `HubuumTask::"9501"`: task submitted by user 9001

The test resources are synthetic Cedar entities except for collections used by
the reverse-query test. Those collections are inserted into the disposable CI
PostgreSQL database when missing.

## Fixture files

- [`schema.json`](schema.json) is the Cedar JSON schema consumed by Treetop.
  A Rust test requires its action set to equal every Hubuum permission plus the
  internal `ReadTask` action.
- [`test-fixture.cedar`](test-fixture.cedar) grants the ordinary group every
  permission on collection 9201 and denies the same operations on 9202 by
  omission. It also defines the administrator and task-owner rules.
- [`schema.cedarschema`](schema.cedarschema) is the human-readable Cedar schema
  used in deployment documentation. Treetop's schema URL consumes JSON, so the
  conformance runner uses `schema.json`.

The runner uses strict schema validation and waits until the status endpoint
reports both a loaded policy and a schema-backed request context. Its fixture
server withholds the policy until Treetop has fetched the schema, avoiding a
startup race in strict mode. Uploads remain disabled, so no upload token is
generated or stored.

## Shared semantic corpus

`src/tests/permissions/conformance.rs` is run once with the local PostgreSQL
backend and once with Treetop. It covers:

- ordinary group grants, denials, administrator override, and an unprivileged
  principal;
- conjunctive permission checks and permission- and resource-scoped tokens;
- collection, class, object, template, class-relation, object-relation, and
  task resources;
- list and search visibility, import creation checks, export template reads,
  remote execution, audit/history visibility, event subscription management,
  and task-owner checks;
- empty candidate sets, duplicate candidates, and stable response ordering.

One intentional backend difference is encoded as data in the corpus. The local
backend requires relation permission on both endpoint collections. Exported
Treetop policies permit a relation when either endpoint is granted. Both class
and object relations are tested across collections 9201 and 9202 so this
difference cannot be mistaken for an omitted test.

## Live-service and failure coverage

The real-service suite additionally verifies:

- 600 Cedar decisions crossing the 512-decision wire boundary;
- 700 visibility candidates with exact authorized totals, stable ordering,
  duplicate handling, and an eleven-row retained page;
- reverse collection queries and synthetic group-permission rows;
- trusted private-CA TLS, untrusted TLS, and missing CA material;
- connection refusal, request timeout, restart recovery, and termination of an
  in-flight request;
- fail-closed handling of malformed, missing, extra, duplicate, out-of-range,
  and failed batch results through the protocol and extraction tests;
- diagnostic scanning with a secret canary, plus error sanitization that omits
  upstream bodies, credential-bearing URLs, and failed-item messages.

The destructive service tests only accept a container name beginning with
`hubuum-treetop-conformance-`. The runner creates that exact container and
removes it on exit.

## Running locally

Prepare the normal test database and install Docker, `curl`, `jq`, `openssl`,
Python 3, and `socat`. Then run:

```bash
source .env
./scripts/run-treetop-conformance.sh
```

The default diagnostic directory is `target/treetop-conformance`. Set
`HUBUUM_TREETOP_REPORT_DIR` to retain it elsewhere. The report includes fixture
metadata, the loaded Treetop status, the exact test list, test output, container
inspection, and service logs. CI uploads the same report for 30 days even when
the job fails.

Do not invoke the ignored Rust tests directly. Their required fixture inputs
are intentionally fatal when the runner has not supplied them.
