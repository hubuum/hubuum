// Bootstrap policies for Treetop-backed Hubuum.
//
// Upload these BEFORE pointing Hubuum at your Treetop instance. Without
// the admin override the only paths that work are public ones (token
// validation, etc.); every collection/class/object/template request
// returns 403.
//
// The admin override grants every schema-compatible action/resource pair.
// This includes the System check used by AdminAccess and ordinary API
// resources. Restricting this permit to HubuumSystem would identify the
// principal as an administrator but still deny their normal API requests.
//
// Once you have an admin group provisioned, replace REPLACE_ME with
// that group's database id (a positive integer), upload this file,
// and Hubuum can serve admin requests. Per-collection permissions are
// generated separately via:
//
//     hubuum-admin export-permissions --as cedar > policies.cedar
//
// then uploaded to Treetop alongside this bootstrap.

permit(
    principal in Group::"REPLACE_ME",
    action,
    resource
);
