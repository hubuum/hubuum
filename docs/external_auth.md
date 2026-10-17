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
The configuration source must be a regular UTF-8 file no larger than 1 MiB.
Parse errors identify the line and column but deliberately omit the source line
because legacy files may contain credentials such as `bind_password`.

```toml
[[ldap]]
scope = "example-directory"
url = "ldaps://ldap.example.org"
bind_dn = "cn=readonly,dc=example,dc=org"
bind_password_ref = "readonly_password"
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
group_filters = ["^hubuum-", "^admin$"]
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
binds are not supported. Configure `bind_dn` with `bind_password_ref`, then
provide the alias through the selected [secret source](secret_sources.md). The
environment source maps this example to
`HUBUUM_LDAP_SECRET_READONLY_PASSWORD`. Inline `bind_password` remains a
compatibility option, but it cannot be combined with `bind_password_ref`.
Omit the bind DN and both password fields for an anonymous service search;
incomplete or ambiguous credentials are rejected at startup.

All group extraction is configuration-driven. Use `group_attributes` and
`group_rules` to map provider attributes to Hubuum groups; do not hard-code
directory-specific group formats in code. The optional `group_filters` list
filters the group names produced by `group_rules`. When the list is non-empty,
a mapped name must match at least one regular expression to be included.
Omitting the list or setting it to an empty list preserves all groups produced
by a group rule.

### Identity subjects and account eligibility

`subject_attribute` selects the identity key within each scope. Use an immutable
identifier such as `entryUUID` when the provider exposes one. Providers that only
expose usernames can use `subject_attribute = "uid"` (or their username attribute).
Repeated logins with the same configured subject continue to use the same local
account. With username subjects, the directory must prevent username reuse: Hubuum
cannot distinguish a recycled username from the original identity.

When a different subject presents an existing name, synchronization fails without
changing that account, its memberships, or its permissions. A stable subject can
still rename its own principal to an unused name. Changing `subject_attribute`
on an established scope is an identity migration: provision a new scope and
explicitly review permission assignments instead of relying on name matching.

Account eligibility belongs in `user_filter`, which is applied both at login and
at token-time refresh. Include your directory's enabled-account condition; for
example, if your directory uses `employeeType=active` as its eligibility policy:

```toml
user_filter = "(&(uid={username})(employeeType=active))"
```

Use your provider's actual status attributes and matching rules, and ensure the
service bind can search them. Hubuum does not infer account status from arbitrary
returned attributes. A successful service-account search is not a user password
bind, so a directory rejecting disabled users' password binds alone does not
invalidate existing Hubuum tokens. Test that disabling a user removes it from
this configured search. Eligibility changes are detected at the next refresh;
choose `refresh_ttl_seconds` to match your revocation requirements.

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
Login names must satisfy the [username requirements](auth_model.md#username-requirements),
including a nonblank principal name and the 255-character login request limit.

```json
{
  "identity_scope": "example-directory",
  "name": "alice",
  "password": "password"
}
```

The response is the normal bearer token response. Token validation may refresh
provider-managed group membership when the cached sync is older than the scope
TTL. Refresh locates the current directory entry with `user_filter` and the
materialized principal name, then verifies that the entry's
`subject_attribute` still matches the stable subject stored at login. The
subject attribute therefore does not need to support an efficient
directory-wide search. Only temporary provider unavailability permits cached
membership inside the configured max-stale window. A missing/filtered-out account,
a changed subject, an invalid provider response, or a configuration error fails
closed: cached authority is invalidated and existing tokens are revoked. A later
outage cannot restore rejected authority; a new successful login is required.
Further requests back off for the scope refresh TTL before retrying the provider,
so one outage does not serialize every request on repeated LDAP timeouts. Once
the cache exceeds
`max_stale_seconds`, requests fail with `503 Service Unavailable` during that
backoff instead of using stale membership. Both durations must be positive and
representable, and `max_stale_seconds` must be greater than or equal to
`refresh_ttl_seconds`; invalid policies are rejected during startup. A sync
timestamp ahead of the server clock is never treated as fresh or as an active
retry backoff.

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
