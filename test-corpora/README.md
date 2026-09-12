# Comprehensive functional test corpus

[Download comprehensive.json](comprehensive.json) from the **same Git branch,
commit, or release tag as your server**. For an older server, select that release
tag in GitHub before downloading. A branch moves; use its commit ID when sharing
a reproducible test case. Compatibility is checked against the containing
revision, so the backup's original producing version may be older than the
server when the format and scenarios remain compatible.

This is a normal full-system Hubuum backup with history. Consumers restore the
file into their own disposable deployment. No repository checkout or dataset
generation is required. The file replaces all application data through the
existing restore workflow.

## Contents

The corpus contains exactly **3,000 live objects in twelve classes**. Names and
payloads are synthetic; hostnames and account addresses use `example.invalid`.

| Policy | Classes and object counts | Behavior |
| --- | --- | --- |
| No schema | `untyped-empty` (0), `untyped-notes` (100), `untyped-locations` (300), `untyped-assets` (600) | Unrestricted objects, arrays, empty objects and JSON null |
| Advisory schema | `advisory-servers` (100), `advisory-switches` (200), `advisory-services` (300), `advisory-applications` (400) | Odd-numbered objects conform; even-numbered `-nonconforming` objects deliberately violate the schema |
| Enforced schema | `enforced-servers` (100), `enforced-switches` (200), `enforced-services` (300), `enforced-applications` (400) | All live objects conform; invalid writes must be rejected |

Schemas exercise required properties, optional and nullable owners, nested site
information, string arrays, enum values, integer types, numeric bounds and the
structured fields below.
The empty class and larger classes exercise empty pages and pagination across
the default and maximum ordinary page sizes.

Collections form this hierarchy beneath Hubuum's built-in root:

```text
corpus-inventory
├── corpus-operations
│   └── corpus-branch
└── corpus-restricted
```

`*-services` and `untyped-locations` belong to `corpus-branch`.
`*-applications` and `untyped-assets` belong to `corpus-restricted`.
All remaining classes belong to `corpus-operations`.

There are three class relations and 300 object relations: enforced servers link
to switches, locations and restricted applications. Server/application links
provide examples where one endpoint is hidden from a limited user.

## Object data and computed fields

Structured objects contain these deterministic, varied inputs. Schema-free
classes retain a mix of rich documents, empty objects, arrays and JSON null.

| Data | Examples and uses |
| --- | --- |
| `hostname`, `status`, `site`, `tags`, `owner` | Synthetic names, lifecycle states, Unicode cities, rack numbers, tags and optional/nullable ownership |
| `resources` | CPU cores, memory GiB and disk-size arrays for capacity calculations |
| `costs` | Compute, storage and optional network amounts; fractional values, zero, null and missing operands for aggregates |
| `telemetry` | CPU percentage samples and temperatures, including negative numbers, empty arrays and all-null samples |
| `contacts` | Team, primary and on-call contacts with missing, null and empty-string cases for fallbacks and presence checks |
| `configuration.desired`, `configuration.observed` | Matching and differing nested configurations; booleans and ordered feature arrays for equality checks |
| `interfaces` | Named interfaces with documentation-range IP addresses, nullable addresses, link speeds and up/down flags |
| `labels` | Keys containing `/` and `~` for escaped JSON Pointer examples |
| `checks` | Present `0`, `false`, `""`, `[]` and `{}` values to distinguish falsy values from absence |

`untyped-notes`, `advisory-servers` and `enforced-servers` each have these fifteen
shared definitions, covering all nine operations and all six result types.
The other classes have room for testers to create their own definitions.
Computations read raw data within one object; they cannot follow relations or
reference another computed field.

| Shared key | Operation | Input paths |
| --- | --- | --- |
| `hostname_copy` | `first_non_null` | `/hostname` |
| `owner_display` | `first_non_null` | `/owner`, `/contacts/primary`, `/contacts/team` |
| `monthly_cost` | `sum` | `/costs/compute`, `/costs/storage`, `/costs/network` |
| `cpu_average` | `average` | `/telemetry/cpu_pct/0`, `/telemetry/cpu_pct/1`, `/telemetry/cpu_pct/2` |
| `temperature_min` | `min` | `/telemetry/temperature_c/0`, `/telemetry/temperature_c/1`, `/telemetry/temperature_c/2` |
| `temperature_max` | `max` | `/telemetry/temperature_c/0`, `/telemetry/temperature_c/1`, `/telemetry/temperature_c/2` |
| `contacts_complete` | `all_present` | `/contacts/primary`, `/contacts/on_call` |
| `has_contact` | `any_present` | `/contacts/primary`, `/contacts/on_call` |
| `contact_count` | `count_present` | `/contacts/primary`, `/contacts/on_call` |
| `configuration_matches` | `all_present_and_equal` | `/configuration/desired`, `/configuration/observed` |
| `falsy_present` | `all_present` | `/checks/zero`, `/checks/disabled`, `/checks/text`, `/checks/items`, `/checks/details` |
| `cost_center` | `first_non_null` | `/labels/cost~1center` |
| `rack_slot` | `first_non_null` | `/labels/rack~0slot` |
| `primary_interface` | `first_non_null` | `/interfaces/0` (object result) |
| `interfaces` | `first_non_null` | `/interfaces` (array result) |

The `corpus-reader` account also owns a personal `my_site` definition on
`enforced-servers`, returning `/site` as an object. Other accounts do not receive
that personal value. Backups preserve both shared and personal definitions and
personal ownership. Shared cached values are excluded and rebuilt after restore;
personal values are evaluated when their owner reads an object.

For concrete checks with `include=computed`:

- `enforced-servers-0001` has `monthly_cost: 27.75`, `cpu_average: null`,
  `temperature_min: -4`, `temperature_max: 4` and `owner_display: "Operations team"`.
- `enforced-servers-0002` has `monthly_cost: 35.5`, `cpu_average: 5`,
  `contact_count: 1` and `configuration_matches: true`.
- `enforced-servers-0003` has `owner_display: ""` and `contacts_complete: true`:
  an empty string is present. `falsy_present` is also true on rich objects.
- `enforced-servers-0006` has an empty CPU sample array and `cpu_average: null`.
- `advisory-servers-0002-nonconforming` has a numeric hostname and
  `costs.network: "unmetered"`. Its `hostname_copy` and `monthly_cost` values are
  null, with `result_type_mismatch` and `non_numeric_operand` errors respectively.
  Other fields still compute normally.
- `untyped-notes-0001`, `0002` and `0003` contain `{}`, an array and null.
  Aggregates return null, presence checks return false and the contact count is
  zero, without computed errors. `untyped-notes-0004` has a rich document.

The [recipe](recipe.json) records the definitions and expected example results.
For filtering and sorting, try this class-list query with the enforced server
class ID from the manifest:

```text
GET /api/v1/classes/{class_id}/?include=computed&computed.shared.monthly_cost__gte=40&sort=computed.shared.monthly_cost
```

See [Computed object fields](../docs/computed_fields.md) for definition APIs,
evaluation semantics and query operators.

## Restore and sign in

Download `comprehensive.json` and `comprehensive.manifest.json`. The manifest
records SHA-256, exact byte size, section counts and scenario identifiers.
Use the matching release's `hubuum-admin` binary, including the one in its
container. Mount the backup read-only when using a container.

```bash
hubuum-admin --verify-backup comprehensive.json --json
hubuum-admin --restore comprehensive.json \
  --restore-confirmation "REPLACE ALL HUBUUM DATA"
hubuum-admin --reset-password corpus-admin
hubuum-admin --reset-password corpus-editor
hubuum-admin --reset-password corpus-reader
hubuum-admin --reset-password corpus-outsider
```

These commands use the deployment's configured database credentials. Split-role
deployments must provide the migration credential for restore. See
[Backup and restore](../docs/backup-restore.md) for container configuration,
staged web restore and the separately supervised web restore executor.

Backups exclude passwords and authentication tokens. Each password reset prints
a new password; sign in normally to obtain a fresh token. The standard `admin`
account also remains available for administrative password reset.

| Account | Expected access |
| --- | --- |
| `corpus-admin` | Unscoped administrator; all 3,000 objects and schema administration |
| `corpus-editor` | Read inventory and audit history, and create/update/delete objects in operations and its branch; 1,600 live objects visible |
| `corpus-reader` | Read inventory and audit history in operations and its branch; 1,600 live objects visible; writes forbidden |
| `corpus-outsider` | No grants on the corpus collections |

The editor and reader inherit branch access from operations. Neither can read
the 1,400 objects in the restricted collection or relations with a hidden endpoint.

## Named scenarios

- Browse `untyped-empty`; it has no objects. Page through `untyped-assets` as
  administrator and through `enforced-services` as reader.
- Compare `advisory-servers-0001` with
  `advisory-servers-0002-nonconforming`. The latter has numeric `hostname`, an
  unknown `status`, string `site.rack` and a non-numeric network cost, all accepted
  because validation is off.
- Try the same malformed data in an enforced class. The request must fail and
  preserve the existing object and its resource revision.
- Try enabling validation on `untyped-empty` without supplying a schema. That
  request must fail without changing the class.
- Inspect the history of `enforced-servers-0001`: it has multiple versions and
  ends in status `active`. `retired-server-deleted` survives only in history;
  its ID is recorded as `deleted_object` in the manifest.
- Inspect schema revisions for `enforced-servers`: a retired original, an active
  revision adding a capacity upper bound, and a staged revision requiring owner.
- Request `enforced-servers-0001` with `include=computed`. Its shared
  `hostname_copy` value should equal `data.hostname` after rebuilding completes.
- Try updating that server as reader and editor. Reader writes fail; editor
  writes succeed. Compare their visibility with the administrator's, including
  the manifest's `visible_relation` and `restricted_relation` anchors.

Run the application workers, either through the default `all` runtime role or a
separate worker. Restore rebuilds computed fields and queues schema revalidation;
poll until computed materialization is current and enforced objects are valid.
History dates are authentic generation dates and do not move forward on restore.
Keep `HUBUUM_EVENT_RETENTION_PURGE_ENABLED=false` (the default) to retain the
historical audit examples. Scenarios do not depend on expiring task outputs or
enabled external integrations.

## Maintaining the corpus

Maintainer tooling requires Docker and Python 3.11 or newer using only the Python
standard library. Build the production image from the revision being tested:

```bash
cargo test --bin hubuum-server dockerfile_copies_every_workspace_manifest --locked
docker build \
  --build-arg 'CARGO_BUILD_FLAGS=-F tls-rustls -F tls-openssl --locked --release' \
  --tag hubuum-server:verify .
python3 scripts/test-corpus.py check
python3 scripts/test-corpus.py verify
```

`check` verifies metadata, payloads, schemas, definitions, ownership and anchors
without Docker. `verify`
creates isolated disposable PostgreSQL and application containers, performs
offline and real restore verification, checks the documented application
scenarios, including computed values, errors, filtering, sorting and personal
visibility, then creates and restores another backup. It cleans up its own
containers, database volumes and network on success or failure. It accepts an
application image, never an existing database URL.

Refresh the committed files after changing the recipe or after an incompatible
backup, restore, schema or scenario change:

```bash
python3 scripts/test-corpus.py generate
```

Generation creates the data through supported APIs, writes a production backup,
and completes the same restore rehearsal before replacing the two committed
files. Review and commit the backup and manifest together. Stable names and
generation order make examples predictable; timestamps, event UUIDs and worker
history can change across generations. Independent generations are checked for
the same behavior rather than identical bytes.

To test regeneration without changing the committed files:

```bash
python3 scripts/test-corpus.py generate --directory target/generated-test-corpus
python3 scripts/test-corpus-tooling.py
```

The uncompressed file must remain below 25 MiB. CI restores the committed file
before independently testing regeneration. Required container checks cover PRs,
main and release tags; corpus and relevant server inputs select those checks.
Historical tags keep their original corpus; do not refresh old release files.
