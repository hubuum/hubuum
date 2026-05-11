# Treetop Permission Backend

This guide covers the Treetop permission backend for Hubuum: what it is, when to use it, and how to set it up.

## What is Treetop mode?

Treetop mode delegates all permission authorization decisions to an external Cedar policy server. Instead of querying the local SQL `permissions` table, Hubuum translates each authorization check into a Cedar authorization request and sends it to your Treetop instance. Treetop evaluates the request against its loaded Cedar policies and returns an Allow or Deny decision.

## When to use Treetop

Consider the Treetop backend if you:

- Want centralized policy authoring across multiple services. If your organization runs several applications that all need to share a common authorization model, Treetop lets you write those policies once and share them.
- Need richer policy expressions than the SQL boolean grid allows. Cedar supports attribute-based access control (ABAC), conditions, and complex predicates that can't be represented in the simple SQL permission table.
- Already have a Cedar deployment and want to integrate Hubuum into it.

If none of the above apply, the default Local backend is simpler and requires no external services.

## What stays the same / what changes

When you switch to Treetop mode:

**Unchanged:**
- Identity: users and groups are still managed in the Hubuum database.
- Data model: namespaces, classes, objects, templates, and relations work exactly as before.
- REST surface: all endpoints accept the same requests and return the same response shapes.

**Changed:**
- Permission DECISIONS: every authorization check is delegated to Treetop instead of the local SQL `permissions` table.
- Permission MUTATIONS: the grant/revoke endpoints (`POST /api/v1/namespaces/{id}/permissions`, etc.) return `501 Not Implemented`. Permissions are managed out-of-band via Treetop's policy upload API.
- Admin determination: admin status is determined by a Cedar policy on `HubuumSystem` instead of checking the local `admin_groupname` config. See "Admin override" below.

## Configuration

Set the following environment variables to enable Treetop mode:

- `HUBUUM_PERMISSION_BACKEND=treetop` — required; selects the Treetop backend.
- `HUBUUM_TREETOP_URL` — required; the base URL of your Treetop server (e.g., `https://treetop.example.com`).
- `HUBUUM_TREETOP_CONNECT_TIMEOUT_MS` — optional; connection timeout in milliseconds (default: 5000).
- `HUBUUM_TREETOP_REQUEST_TIMEOUT_MS` — optional; request timeout in milliseconds (default: 10000).
- `HUBUUM_TREETOP_ACCEPT_INVALID_CERTS` — optional; set to `true` to accept invalid TLS certificates (development only; DO NOT use in production).
- `HUBUUM_TREETOP_CA_CERT` — reserved but not yet wired. If you need custom CA certificate loading, see the comment in `src/permissions/treetop/mod.rs::connect`. Setting this variable currently returns a fatal error on startup.

## Bootstrap workflow

Follow these steps to switch an existing Hubuum deployment to Treetop mode (or to set up a new deployment with Treetop from the start):

1. **Stand up a Treetop server.** See the Treetop project documentation for installation and deployment instructions.

2. **Upload the Cedar schema.** Upload `docs/treetop/schema.cedarschema` to your Treetop instance. This tells Treetop which entity types and actions Hubuum will send. The schema must be loaded before Hubuum can authorize requests.

3. **Edit and upload the bootstrap policy.** Open `docs/treetop/bootstrap.cedar` and replace `REPLACE_ME` with your admin group's database id. To find the id, run:

   ```bash
   psql -d hubuum -c "SELECT id FROM groups WHERE groupname = 'admin'"
   ```

   Then upload `bootstrap.cedar` to your Treetop instance. This policy grants the admin group full access to the system; without it, every request returns 403.

4. **(Optional) Export and upload your existing permissions.** If you're migrating from Local mode and want to preserve your existing permission grants, run:

   ```bash
   hubuum-admin export-permissions --as cedar > policies.cedar
   ```

   This generates a Cedar policy bundle that mirrors the current SQL `permissions` table. Upload `policies.cedar` to Treetop alongside `bootstrap.cedar`. (If you're setting up a fresh deployment, skip this step — you'll write your policies from scratch.)

5. **Configure Hubuum and restart.** Set `HUBUUM_PERMISSION_BACKEND=treetop` and `HUBUUM_TREETOP_URL=https://your-treetop-instance` in your environment, then restart Hubuum. Hubuum's startup health check runs `client.health()` against the Treetop server. If the health check fails, Hubuum exits with status code 6 (`EXIT_CODE_PERMISSION_BACKEND_ERROR`).

6. **Verify the integration.** Run the parity test suite (see "Verifying the integration" below) to confirm Treetop is wired correctly.

## What `501` errors mean

When Treetop mode is enabled, all permission mutation endpoints return `501 Not Implemented`:

- `POST /api/v1/namespaces/{id}/permissions` (grant permissions to a group)
- `DELETE /api/v1/namespaces/{id}/permissions/group/{group_id}` (revoke all permissions from a group)
- `PUT /api/v1/namespaces/{id}/permissions/group/{group_id}` (replace permissions for a group)

The `501` response body includes the message:

```json
{
  "error": "permission mutations are managed out-of-band when using the treetop backend"
}
```

To grant or revoke permissions in Treetop mode, edit your Cedar policies and re-upload them via Treetop's policy upload API. Hubuum does not push policies at runtime; it only queries Treetop for authorization decisions.

## Synthetic Permission rows

The `GET /api/v1/namespaces/{id}/permissions/group/{group_id}` endpoint returns a `Permission` object in both Local and Treetop modes. In Treetop mode, the returned object is *synthetic*: it's constructed on-the-fly from Treetop's authorization responses rather than read from the SQL `permissions` table.

The synthetic `Permission` has:

- `id=0` (there is no SQL row to assign a real id)
- `created_at` and `updated_at` set to the current request time (Treetop has no notion of when a permission was "created")
- The boolean permission flags (`has_read_namespace`, `has_create_class`, etc.) reflect what Treetop allowed at query time

Consumers should not rely on `id`, `created_at`, or `updated_at` for synthetic permissions — they are placeholders for API compatibility only.

## Relation policies: OR vs AND semantics

The `hubuum-admin export-permissions --as cedar` command emits relation permits using OR-on-endpoints: a user with permission on EITHER the `from_namespace_id` OR the `to_namespace_id` can act on the relation. This differs from the Local backend, which uses AND-on-both-endpoints (permission required on both).

The divergence is intentional: the OR semantics are simpler to express in Cedar and match the common case where a user managing either endpoint should be able to create/read/update/delete the relation. If you need strict AND semantics, hand-edit the exported policies (or write `forbid` clauses) to enforce the constraint.

Example of the OR predicate emitted by the exporter (from `src/permissions/export.rs`):

```cedar
permit(
    principal in Group::"123",
    action in [Action::"CreateClassRelation", Action::"ReadClassRelation"],
    resource
) when {
    resource is HubuumClassRelation &&
    (resource.from_namespace_id == 5 || resource.to_namespace_id == 5)
};
```

To enforce AND semantics, replace the `||` with `&&`, or add a `forbid` clause that denies the relation unless both conditions hold.

## Verifying the integration

Hubuum includes a live parity test suite that runs against an actual Treetop server. The tests verify that the Treetop integration behaves identically to the Local backend for a known set of entities and policies.

To run the parity tests:

```bash
HUBUUM_TREETOP_TEST_URL=https://your-treetop-instance \
HUBUUM_DATABASE_URL=postgres://user:pass@localhost/hubuum \
  cargo test tests::permissions::live_treetop_parity
```

Without `HUBUUM_TREETOP_TEST_URL`, the tests skip cleanly (they do not fail).

The parity tests expect your Treetop server to have specific test entities and policies loaded. See [`test-fixture.md`](test-fixture.md) for the required setup (test user/group IDs, namespace ID, and the exact Cedar policies the tests assert against).

## Admin override

In Treetop mode, admin status is determined by a Cedar policy instead of the `HUBUUM_ADMIN_GROUPNAME` environment variable. Hubuum's `AdminAccess` extractor calls `backend.is_admin(principal)`, which dispatches a Cedar authorization request:

```
principal: User::"<user_id>" (with groups Group::"<group_id>", ...)
action: ReadCollection
resource: HubuumSystem::"global"
```

If Treetop returns Allow, the user is an admin. If Deny, the user is not. The bootstrap policy grants this permission to a single group (the one whose id you substitute for `REPLACE_ME`). You can add more admin groups by adding more `permit` clauses targeting `HubuumSystem`.

## Further reading

- `schema.cedarschema` — the Cedar entity schema Hubuum's exporter targets.
- `bootstrap.cedar` — minimal policy file to upload before serving traffic.
- `test-fixture.md` — the test entities and policies the parity suite expects.
- `../permissions.md` — the on-the-wire permission model that both Local and Treetop backends conform to.
- `src/permissions/treetop/mapping.rs` — the runtime entity mappings (source of truth for the schema).
- `src/permissions/export.rs` — the Cedar policy exporter.
