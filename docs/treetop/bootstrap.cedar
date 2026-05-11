// Bootstrap policies for Treetop-backed Hubuum.
//
// Upload these BEFORE pointing Hubuum at your Treetop instance. Without
// the admin override the only paths that work are public ones (token
// validation, etc.); every namespace/class/object/template request
// returns 403.
//
// The admin override is a single permit on the System resource. The
// AdminAccess extractor in Hubuum asks Treetop "is this principal
// allowed to read the System resource?" — admins say yes, everyone
// else says no. The "read" check is a cheap, intentional overload of
// ReadCollection on HubuumSystem; see src/permissions/treetop/mod.rs.
//
// Once you have an admin group provisioned, replace REPLACE_ME with
// that group's database id (a positive integer), upload this file,
// and Hubuum can serve admin requests. Per-namespace permissions are
// generated separately via:
//
//     hubuum-admin export-permissions --as cedar > policies.cedar
//
// then uploaded to Treetop alongside this bootstrap.

permit(
    principal in Group::"REPLACE_ME",
    action,
    resource is HubuumSystem
);
