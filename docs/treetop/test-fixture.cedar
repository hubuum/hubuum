// Live parity-test policies for Hubuum's Treetop adapter.
// Keep the identifiers in sync with test-fixture.md and
// src/tests/permissions/live_treetop_parity.rs.

permit(principal in Group::"9100", action, resource);

permit(
    principal in Group::"9101",
    action == Action::"ReadCollection",
    resource
) when {
    resource is HubuumCollection && resource == HubuumCollection::"9201"
};
