# Explore the Atlas example dataset

Atlas is the shared example inventory for Hubuum's documentation, demonstrations,
and client testing. Load it once, then follow the same classes and objects through
the API, CLI, web frontend, and client libraries.

It is deliberately small: **four classes, ten objects, four class relations,
and ten object relations**, in two collections. All data is fictional;
hostnames use `example.invalid`. No external systems are contacted.

## Classes define the model; objects hold the data

A **class** defines a kind of resource and its schema policy. An **object** is
one instance of that class, with its own JSON data.

| Class | Schema policy | Objects | Authority in this example |
| --- | --- | --- | --- |
| `Service` | Enforced JSON Schema | `Atlas`, `Beacon` | Maintained in Hubuum |
| `Server` | Enforced JSON Schema | `web-01`, `web-02`, `worker-01` | Reference copies from a server inventory |
| `Location` | Schema-free | `Oslo`, `Bergen` | Reference copies from a facilities directory |
| `Context` | Schema-free | `Research notes`, `Migration checklist`, `Capacity observation` | Notes and checklist maintained in Hubuum; observation from telemetry |

Authority and structure are independent. Hubuum can hold authoritative or
reference data with either schema policy—even within the same class.
The example's `data.source` field records that choice as ordinary application
data. It does not enable synchronization, establish trust, or prevent writes.
Your integrations and permission design determine how records are maintained.

`Service` requires an owner, a tier from 1 to 3, and a production or staging
environment. Here is the complete data for its `Atlas` object:

<!-- atlas-data: object:Atlas -->
```json
{
  "owner": "Platform",
  "tier": 1,
  "environment": "production",
  "source": "hubuum"
}
```

`Context` has no JSON Schema. Its `Research notes` object contains:

<!-- atlas-data: object:Research notes -->
```json
{
  "purpose": "Explore new hosting",
  "questions": [
    "Capacity?",
    "Location?"
  ],
  "candidate": true,
  "source": "hubuum"
}
```

Other objects in that class have different shapes: a checklist of steps and a
timestamped capacity observation. See [schema evolution](../schema_evolution.md)
when you are ready to introduce or change a schema.

## Relations connect the instances

Class relations connect `Service–Server`, `Server–Location`, `Service–Context`,
and `Server–Context`. Object relations instantiate those connections:

- Atlas runs on web-01 and web-02; Beacon runs on worker-01.
- web-01 and worker-01 are located in Oslo; web-02 is located in Bergen.
- Atlas links to Research notes and Migration checklist.
- Beacon also links to Migration checklist.
- web-01 links to Capacity observation.

The Server–Location class relation limits each server to one location, while
a location can relate to many servers. Relation aliases such as `servers` and
`location` give templates useful names. The phrases “runs on” and “located in”
describe the example's meaning; they are not additional relation-type fields.
See [relations](../relationship_endpoints.md).

## Download a matching edition

- [Atlas import](../assets/atlas/atlas.import.json): add the inventory through the API.
- [Atlas backup](../assets/atlas/atlas.backup.json): reset a disposable installation.
- [Manifest](../assets/atlas/atlas.manifest.json): checksums, sizes, counts, and producing server version.

These files travel with this documentation edition. Select the edition matching
your server before downloading; development documentation is under `main`.
For reproducible tests, pin a released edition or an exact Git commit rather
than a moving branch. Older documentation editions published before Atlas was
introduced do not contain this dataset.

## Load through the API

Use an evaluation installation and an unscoped administrator token. This import
creates collections and groups, which require administrator access. It contains
no passwords, users, tokens, or group memberships.

Save the import as `atlas.import.json`. With `HUBUUM_URL` set to your API origin
and `HUBUUM_TOKEN` to your administrator token:

```sh
curl --fail-with-body --silent --show-error \
  -H "Authorization: Bearer $HUBUUM_TOKEN" \
  -H "Content-Type: application/json" \
  --data-binary @atlas.import.json \
  "$HUBUUM_URL/api/v1/imports"
```

The response contains a task `id`. Poll it until `status` is `succeeded`, then
inspect the item results. A `202` response alone does not mean the data is loaded.

```http
GET /api/v1/imports/{task_id}
Authorization: Bearer <token>

GET /api/v1/imports/{task_id}/results
Authorization: Bearer <token>
```

The import uses strict atomicity and aborts on name collisions. If a class such
as `Server` already exists, or Atlas is already loaded, the entire import fails
without overwriting inventory. Resolve the conflict in your evaluation setup
or start a fresh instance. Classes have globally unique names; placing them in
another collection does not avoid a class-name collision.

For a preview, set the downloaded document's `dry_run` to `true`, submit it,
and inspect its results. Set it back to `false` to load. A dry run reserves
nothing. See the [import contract](../import_api.md).

## Explore the loaded inventory

Read a class, then one of its objects:

```http
GET /api/v1/classes/by-name/Service
Authorization: Bearer <token>

GET /api/v1/classes/by-name/Service/objects/by-name/Atlas
Authorization: Bearer <token>
```

Find the two web servers in name order:

```http
GET /api/v1/classes/by-name/Server/objects?name__startswith=web-&sort=name
Authorization: Bearer <token>
```

The result contains `web-01` followed by `web-02`. Walk Atlas's direct
connections, or request its neighborhood graph:

```http
GET /api/v1/classes/by-name/Service/objects/by-name/Atlas/related/relations
Authorization: Bearer <token>

GET /api/v1/classes/by-name/Service/objects/by-name/Atlas/related/graph
Authorization: Bearer <token>
```

Atlas has four direct object relations. The neighborhood can include objects
reached through other objects, such as locations. See [querying](../querying.md)
and [name addressing](../name_addressing.md).

The Server class also has a shared `monthly_cost` computed field:

```http
GET /api/v1/classes/by-name/Server/objects/by-name/web-01?include=computed
Authorization: Bearer <token>
```

After the background computation completes,
`computed.shared.values.monthly_cost` is `50`: compute cost `45` plus storage
cost `5`, in fictional cost units. Raw `data` is unchanged. See
[computed fields](../computed_fields.md).

## Explore collection permissions

The collection hierarchy is:

```text
root
└── atlas-demo
    └── atlas-demo-operations
```

`Service` and `Context` live in `atlas-demo`; `Server` and `Location` live
in its operations child. Each object's collection initially matches its class.

| Group | Grant | What a member can do with an unscoped token |
| --- | --- | --- |
| `atlas-readers` | Read collections, classes, objects, and relations on `atlas-demo` | Read all ten objects through inherited access; no writes |
| `atlas-operators` | The same read permissions plus object create, update, and delete on `atlas-demo-operations` | Read and maintain the five servers/locations; cannot read the service catalogue or its context |

The groups start with **no members**. Assign your own evaluation users or service
accounts using the [identity and permission guides](../permissions.md). Test
with a non-admin principal: administrators bypass these collection restrictions.
Membership in both groups combines their grants. Token scopes can narrow them.
Relations with an unreadable endpoint remain hidden.

## Restore the starting point

**A full restore replaces all application data. Use a disposable demo
installation.** The import above is the merge path; this backup is the reset path.

Configure `hubuum-admin` for that disposable database, then:

```sh
hubuum-admin --verify-backup atlas.backup.json --json
hubuum-admin --restore atlas.backup.json \
  --restore-confirmation "REPLACE ALL HUBUUM DATA"
hubuum-admin --reset-password admin
```

The generated backup contains only the standard administrator and the two
empty example groups. Passwords and tokens are excluded. Use the newly printed
password to sign in and obtain a fresh token after restore.

Use the matching server's administrative binary. Follow [backup and restore](../backup-restore.md)
for container mounts, split-role credentials, staged web restore, and the restore
executor. This corpus is a standard Hubuum backup; it needs no special restore API.

## Reuse it for demonstrations and tests

Use the same downloads for the frontend, CLI, and client libraries. Resolve IDs
from class/object names; IDs and revisions can differ after an import. Preserve
the names and relationships when writing examples so readers can continue
across interfaces.

The repository's corpus checks exercise dry runs, successful imports, repeated
import rejection, pagination, filters, computed values, schema rejection,
schema-free writes, permissions, and backup/restore round trips against a real
server in an isolated deployment. Marked JSON examples above are checked against
the import file to catch documentation drift.

For larger pagination, advisory schemas, temporal history, and broader edge
cases, use the separate [3,000-object functional corpus](https://github.com/hubuum/hubuum/blob/main/test-corpora/README.md).
See [documentation maintenance](../contributing/documentation.md#shared-example-dataset)
for the canonical files and verification commands.
