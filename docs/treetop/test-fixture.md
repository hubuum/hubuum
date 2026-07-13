# Treetop parity test fixture

The tests in `src/tests/permissions/live_treetop_parity.rs` run against
a live Treetop server when `HUBUUM_TREETOP_TEST_URL` is set. They expect
the server to have the entity schema in `schema.cedarschema` plus the
following test policies loaded.

## Fixed identifiers

The tests use these numeric IDs (chosen high enough to avoid colliding
with normal Hubuum data):

- `User::"9001"` — the test user (normal-tier)
- `User::"9002"` — a second test user (normal-tier, separate from 9001)
- `Group::"9100"` — the admin group
- `Group::"9101"` — a normal group
- `HubuumCollection::"9201"` — the test collection

## Required policies

```cedar
// Admin override: anyone in Group::"9100" gets everything.
permit(principal in Group::"9100", action, resource);

// Normal group reads the test collection.
permit(
    principal in Group::"9101",
    action == Action::"ReadCollection",
    resource
) when {
    resource is HubuumCollection && resource == HubuumCollection::"9201"
};
```

## What the tests assert

|Test|Expects|
|---|---|
|`live_health_check_succeeds`|server reachable, health endpoint returns OK|
|`live_authorize_many_preserves_request_order`|granted=Allow, missing-resource=Deny, missing-action=Deny in input order|
|`live_is_admin_distinguishes_admin_from_normal`|Group 9100 -> admin, Group 9101 -> not|
|`live_collections_user_can_reflects_external_policy`|collection 9201 visible for Group 9101 with ReadCollection|
|`live_group_permission_on_returns_grant_grid_for_known_group`|synthesized Permission row has `has_read_collection=true` for Group 9101 on collection 9201|

## Running locally

```bash
HUBUUM_DATABASE_URL=postgres://... \
HUBUUM_TREETOP_TEST_URL=https://your-treetop-instance \
  cargo test tests::permissions::live_treetop_parity
```

Without `HUBUUM_TREETOP_TEST_URL`, every test exits with a "skipping"
message and counts as passed.
