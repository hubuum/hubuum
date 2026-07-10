# External Authentication

Hubuum supports local users and provider-backed identity scopes. An identity scope
is a principal namespace backed by one provider configuration. The same provider
driver can be used more than once, so multiple LDAP directories or LDAP search
policies can coexist.

## Configuration

Set `HUBUUM_AUTH_CONFIG_PATH` to a TOML file readable by the server.
For single-host container deployments, pass the host file to
`install-single-host.sh --auth-config`; the installer mounts it read-only and
sets the container path automatically. See
[Single-Host Container Deployment](deployment.md#external-authentication-configuration).

```toml
[[ldap]]
scope = "example-directory"
url = "ldaps://ldap.example.org"
bind_dn = "cn=readonly,dc=example,dc=org"
bind_password = "readonly-password"
connect_timeout_seconds = 5
operation_timeout_seconds = 10
user_base_dn = "ou=people,dc=example,dc=org"
user_filter = "(uid={username})"
user_scope = "subtree"
username_attribute = "uid"
subject_attribute = "entryUUID"
display_name_attribute = "cn"
email_attribute = "mail"
group_attributes = ["memberOf"]
group_filters = ["^cn=hubuum-", "^cn=admin,"]
refresh_ttl_seconds = 300
max_stale_seconds = 3600

[[ldap.group_rules]]
pattern = "^cn=([^,]+),ou=groups,dc=example,dc=org$"
name = "$1"
key = "$0"
description = "Directory group $1"
```

LDAP transport is always encrypted. Use `ldaps://` for implicit TLS. An
`ldap://` URL is upgraded with StartTLS before any bind or search, and the
connection fails if the server cannot establish verified TLS. Plaintext LDAP
binds are not supported. Configure `bind_dn` and `bind_password` together, or
omit both for an anonymous service search; an incomplete pair is rejected at
startup.

All group extraction is configuration-driven. Use `group_attributes` and
`group_rules` to map provider attributes to Hubuum groups; do not hard-code
directory-specific group formats in code. The optional `group_filters` list
filters the raw values read from `group_attributes` before mapping. When the
list is non-empty, a value must match at least one regular expression to be
included. Omitting the list or setting it to an empty list preserves all values
that match a group rule.

### Multiple LDAP scopes

Each `[[ldap]]` entry creates an independent provider instance keyed by its
`scope`. Entries may use different servers, or reuse one LDAP server with
different search bases, login attributes, and group semantics. A nested
`[[ldap.group_rules]]` belongs to the immediately preceding `[[ldap]]` entry.

```toml
[[ldap]]
scope = "employees"
url = "ldaps://directory.example.org"
user_base_dn = "ou=employees,dc=example,dc=org"
user_filter = "(uid={username})"
group_attributes = ["memberOf"]

[[ldap.group_rules]]
pattern = "^cn=([^,]+),ou=staff-groups,dc=example,dc=org$"
name = "$1"
key = "$0"

[[ldap]]
scope = "partners"
url = "ldaps://directory.example.org"
user_base_dn = "ou=partners,dc=example,dc=org"
user_filter = "(mail={username})"
username_attribute = "mail"
group_attributes = ["businessCategory"]

[[ldap.group_rules]]
pattern = "^partner:(.+)$"
name = "partner-$1"
key = "$0"
```

Users select the matching scope at login. Materialized users and groups remain
namespaced by that scope, so `employees/admin` and `partners/admin` are distinct
identities even when their display names match.

For local testing, a Docker LDAP fixture such as
`rroemhild/docker-test-openldap` can be used with an `example.org`-style config.
Keep repository examples generic and do not commit organization-specific LDAP
data.

## Login

Local users can omit `identity_scope` or use `local`.

```json
{
  "identity_scope": "example-directory",
  "name": "alice",
  "password": "password"
}
```

The response is the normal bearer token response. Token validation may refresh
provider-managed group membership when the cached sync is older than the scope
TTL. If refresh fails, cached membership remains usable only inside the
configured max-stale window. Further requests back off for the scope refresh TTL
before retrying the provider, so one outage does not serialize every request on
repeated LDAP timeouts. Once the cache exceeds `max_stale_seconds`, requests fail
with `503 Service Unavailable` during that backoff instead of using stale
membership.

## Users And Groups

Provider-managed users and groups are materialized locally so permissions can be
assigned to stable Hubuum IDs.

- Principal names are unique within an identity scope, not globally.
- External groups are real Hubuum groups with local IDs.
- Local and external groups with the same display name are different groups when
  their identity scopes differ.
- Provider-managed users and groups are read-only through Hubuum APIs; edit them
  in the source system.
- Manual group memberships are preserved. Provider-sourced memberships are
  reconciled on sync.

The admin bypass group is selected by `HUBUUM_ADMIN_GROUPNAME` and
`HUBUUM_ADMIN_IDENTITY_SCOPE`. By default, only `local/admin` is the admin group.
