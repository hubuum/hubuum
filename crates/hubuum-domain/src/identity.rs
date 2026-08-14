/// Canonical identity scope for locally managed principals.
pub const LOCAL_IDENTITY_SCOPE: &str = "local";

/// Provider marker for locally managed identity records.
pub const LOCAL_PROVIDER_KIND: &str = "local";

/// Provider marker for LDAP-managed identity records.
pub const LDAP_PROVIDER_KIND: &str = "ldap";

/// Membership-source marker for application-managed group assignments.
pub const MANUAL_MEMBERSHIP_SOURCE: &str = "manual";

/// Membership-source marker for directory-managed group assignments.
pub const EXTERNAL_MEMBERSHIP_SOURCE: &str = "external";
