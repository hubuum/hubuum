# Authentication & Authorization Model

This page describes *who* can authenticate to Hubuum, *how* identity is structured,
and *how* a request is authorized. It also covers **service accounts** (non-human
API principals) and **token scopes**, which narrow what an automated credential may
do.

For the collection/group permission model itself (what `ReadCollection`,
`CreateClass`, … mean), see [permissions.md](permissions.md). This page is about
identity, tokens, and the authority gates that sit in front of those permissions.
For provider setup, see [external_auth.md](external_auth.md).

---

## Identity: principals

Every authenticated identity in Hubuum is a **principal**. There are two kinds:

| Kind | Table | Can password-login? | Can hold tokens? | Typical use |
| --- | --- | --- | --- | --- |
| `human` | `users` | Yes | Yes | People |
| `service_account` | `service_accounts` | **No** | Yes | Automation / integrations |

Principals use **class-table inheritance**: a single `principals` row owns the
identity, and a `users` or `service_accounts` row shares the *same id*
(`users.id` / `service_accounts.id` are `INT PRIMARY KEY REFERENCES principals(id)`).
A principal id *is* the user/service-account id.

```text
                 ┌──────────────────────────┐
                 │        principals         │
                 │ id (PK), kind, scope, name│   name is unique inside
                 │ provider metadata         │   one identity scope
                 │ UNIQUE(scope, name)       │
                 └────────────┬─────────────┘
            kind='human'      │      kind='service_account'
        ┌────────────────────┐│┌──────────────────────────────┐
        │       users        │││      service_accounts        │
        │ id (PK,FK), password│││ id (PK,FK), owner_group_id,  │
        │ email, …           │││ description, disabled_at, …  │
        └────────────────────┘ └──────────────────────────────┘
```

Key consequences:

- **The login/display name lives on `principals.name`** — there is no separate
  `users.username`. Names are unique per identity scope, so `local/alice` and
  `example-directory/alice` are distinct principals.
- **Provider ownership lives on the principal.** `identity_scope_id`,
  `provider_managed`, `external_subject`, and sync timestamps describe which
  provider owns the identity. The `users` and `service_accounts` tables remain
  kind-specific profile tables.
- **Provider-managed data is source-authoritative.** Hubuum materializes
  provider users, groups, and memberships so local permission assignments have
  stable ids, but provider-managed users and groups are read-only through local
  mutation APIs.
- **Subtypes are mutually exclusive by construction.** The composite
  `(id, kind)` foreign key makes it impossible for one id to be both a user and a
  service account, or to disagree with `principals.kind`. No triggers or caller
  discipline are needed.
- **Deleting a principal cascades.** Deleting a user/service account deletes the
  `principals` row, which cascades to the subtype row, its `group_memberships`, and
  its `tokens`. Subtype rows are never deleted alone (that would orphan the
  principal).

Identity scopes are provider-backed principal namespaces. `local` is the built-in
scope. LDAP and future external providers can add more scopes; token scopes are a
separate concept and only narrow what a token may do.

Service accounts are currently local principals with Hubuum-issued token
credentials. Long term, service account credentials should be treated as a
credential provider plugged into the same principal model, not as a separate
identity system.

---

## Authorization is group-based

Hubuum authorization is entirely **group → collection** (see
[permissions.md](permissions.md)). Principals do not hold permissions directly;
they hold **group memberships**, and groups hold permissions on collections.
Those grants apply to the collection itself and to descendant collections.

- Membership lives in `group_memberships(principal_id, group_id)` — it is
  principal-centric, so **both users and service accounts can be members of a
  group** and gain that group's permissions.
- The effective permission check funnels through a single group-id subquery over
  `group_memberships`, so the same authorization path serves humans and service
  accounts identically.
- A principal is an **admin** iff it is a member of the configured admin group
  selected by `HUBUUM_ADMIN_GROUPNAME` and `HUBUUM_ADMIN_IDENTITY_SCOPE`.
  `local/admin` and an external-scope `admin` group are different groups. Admin
  membership grants collection authority — but, by itself, it does **not** make a
  service account a human IAM administrator (see
  [Privilege separation](#privilege-separation)).

A freshly created service account has its `owner_group_id` set for *management*
purposes only; that does **not** grant it any runtime collection permission. A
service account gets permissions only via an explicit `group_memberships` insert
(i.e. by being added to a group).

---

## Tokens

Authenticated requests carry a bearer token:

```http
Authorization: Bearer <token>
```

Tokens belong to a principal and have a full lifecycle. The stored value is an HMAC
hash — the raw token is shown exactly once, at creation.

| Field | Meaning |
| --- | --- |
| `name`, `description` | Optional, human-facing labels |
| `issued` | Creation time |
| `expires_at` | Optional per-token expiry; **overrides** the global window |
| `last_used_at` | Advanced on every successful validation |
| `revoked_at` | Soft-revoke marker (the row is retained) |
| `scoped` | Whether the token is scope-limited (see [Scopes](#scopes)) |

### Validation

Validation is a single atomic statement. A token is accepted **only if** it is
simultaneously:

1. present and not revoked (`revoked_at IS NULL`),
2. unexpired — `expires_at > now()`, or, when `expires_at IS NULL`, issued within
   the global window `HUBUUM_TOKEN_LIFETIME_HOURS` (default 24h), and
3. **not** owned by a disabled service account.

On success, `last_used_at` is advanced. Any failure yields `401 Unauthorized` with a
generic message (no distinction between unknown / revoked / expired / disabled).

### Revocation

Revocation is a **soft delete**: `revoked_at` is set and the row is retained for
audit/history. Revoked tokens never validate again.

---

## Scopes

A token may be narrowed independently by permission and by resource identity.
Scopes are a least-privilege mechanism for automation: they can only *narrow*
authority, never widen it.

> **Effective authority = principal/group grants ∩ permission scope ∩ resource scope**

The `scopes` request field is the optional permission dimension. The
`resource_scopes` field is the optional resource dimension. Omitting one dimension
leaves that dimension unrestricted; omitting both creates an unscoped token.

Scope semantics are **fail-closed**, with the persisted dimension flags as the
source of truth rather than child-row presence:

| Token state | Meaning |
| --- | --- |
| Both dimensions omitted | **Unscoped** — full principal authority |
| Permission dimension present | Grants are intersected with its permission rows |
| Resource dimension present | Grants are intersected with its resource rows |
| A flagged dimension has **zero** rows | That dimension denies everything |
| Request body `scopes: []` or `resource_scopes: []` | **Rejected with `400`** — an empty list is a client bug, not "grant nothing" |

Enforcement details:

- The scope check is a Rust pre-filter applied **before** the permission query, and
  **before any admin bypass**. A scoped token held by an admin cannot exceed its
  scopes — the admin "all access" fast paths apply only when the token is unscoped.
- Scopes apply to *every* authority-bearing path, not just `can!` checks:
  search/list/export visibility is intersected with scopes too. An admin's scoped
  token, for example, lists only `scope ∩ grant` collections.
- Scopes may name permissions or resources the principal does not currently have
  access to. Those entries are inert: a scope is never a grant. A point request to
  such a resource returns `403 Forbidden`, while list and search endpoints omit it
  and compute pagination totals after both grant and scope filtering.

### Resource scopes

Each resource entry uses a tagged object:

```json
{
  "resource_scopes": [
    { "kind": "collection", "id": 17 },
    { "kind": "class", "id": 42 },
    { "kind": "object", "id": 99 }
  ]
}
```

IDs must be positive and must name resources that exist when the token is minted.
Duplicate entries are rejected with `400`.

| Entry | Included authority |
| --- | --- |
| `collection` | That collection, its classes, and its objects |
| `class` | That class and its objects, but not its parent collection or sibling classes |
| `object` | That object only |

Entries are additive inside the resource boundary. A relation is visible or
actionable only when **both endpoints** are inside that boundary. Collection-level
features such as templates, remote targets, audit events, and event subscriptions
require an explicit collection scope; a class or object entry does not expose its
parent collection. Runtime-wide system operations remain unavailable to any
resource-scoped token. Task visibility remains principal-owned rather than part of
the collection/class/object hierarchy.

### Valid scope strings

Scope strings are the permission names from the permission model
([permissions.md](permissions.md)):

```text
ReadCollection UpdateCollection DeleteCollection DelegateCollection
CreateClass ReadClass UpdateClass DeleteClass
CreateObject ReadObject UpdateObject DeleteObject
CreateClassRelation ReadClassRelation UpdateClassRelation DeleteClassRelation
CreateObjectRelation ReadObjectRelation UpdateObjectRelation DeleteObjectRelation
CreateTemplate ReadTemplate UpdateTemplate DeleteTemplate
CreateRemoteTarget ReadRemoteTarget UpdateRemoteTarget DeleteRemoteTarget
ExecuteRemoteTarget
```

Unknown strings are rejected (fail-closed) wherever scopes are parsed.

### Scopes and async tasks

Asynchronous work must not later run with more authority than the request that
enqueued it. Remote-call tasks record a **scope snapshot** at enqueue time: the
submitting token id, its effective scoped flag, and both optional scope dimensions.
The worker reconstructs that snapshot fail-closed and enforces it during execution.
Legacy permission-only snapshots remain readable.

Import and export tasks accept only unscoped runtime administrators, so they store
an unscoped marker instead of resource scopes. The worker rechecks the principal's
current runtime-admin authority before execution. Revoking that authority or
disabling the service account while the task is queued makes the task fail closed.

---

## Service accounts

A service account is a non-human principal for automation. It cannot password-login;
it acts exclusively through tokens.

### Lifecycle

| Action | Endpoint | Notes |
| --- | --- | --- |
| Create | `POST /api/v1/iam/service-accounts` | Body: `name`, `owner_group_id`, optional `description` |
| List | `GET /api/v1/iam/service-accounts` | Admin sees all; others see SAs of groups they belong to |
| Get / Update | `GET` / `PATCH /api/v1/iam/service-accounts/{id}` | |
| Disable | `POST /api/v1/iam/service-accounts/{id}/disable` | Soft-revokes all its tokens and cancels its pending tasks |
| Delete | `DELETE /api/v1/iam/service-accounts/{id}` | Cascades via the principal row |

`owner_group_id` uses `ON DELETE RESTRICT`: a group that owns service accounts
cannot be deleted until those SAs are reassigned or deleted. Group deletion returns
`409 Conflict` listing the owned SAs rather than failing with an opaque FK error.

A **disabled** service account: its existing tokens stop validating immediately,
and it cannot mint new tokens (the mint endpoint returns `409 Conflict`). Its
**queued** (not-yet-claimed) tasks are cancelled. A task already claimed by a
worker is caught by the worker's pre-dispatch disabled-SA gate and does not
execute; a task already mid-execution runs to completion (Hubuum does not
interrupt in-flight work, and never mislabels running work as `cancelled`).

### Who may create a service account

- An **admin** may create an SA for any group.
- A **non-admin human** may create an SA only for a group they already belong to
  (you cannot mint an SA owned by a group you are not in).
- Service accounts cannot create service accounts.

### Who may manage a service account

Management means get/update/disable/delete and managing the SA's tokens. The rule
is **admin OR a *human* member of the SA's `owner_group_id`**.

> **A service account never manages itself.** Even if a service account is added to
> its own owner group, it cannot manage itself or mint/revoke its own tokens — only
> *human* owner-group members (and admins) can. This avoids a token bootstrapping
> more tokens.

---

## Tokens and groups, by principal

Token and group-membership management is principal-shaped, so one route family
serves both kinds:

| Endpoint | Purpose | Authorization |
| --- | --- | --- |
| `POST /api/v1/iam/principals/{principal_id}/tokens` | Mint a token (returns raw value once) | human: self or admin; SA: admin or human owner-group member |
| `GET /api/v1/iam/principals/{principal_id}/tokens` | List token metadata (never the hash) | same as above |
| `POST /api/v1/iam/principals/{principal_id}/tokens/{token_id}/revoke` | Soft-revoke a token | same as above |
| `GET /api/v1/iam/principals/{principal_id}/groups` | List the principal's groups | same as above |
| `GET /api/v1/iam/principals/{principal_id}/permissions` | Direct permission rows across **all** collections, grouped by granting group | same as above |
| `GET /api/v1/collections/{collection_id}/permissions/principal/{principal_id}` | Direct permission rows on a single collection for the principal's groups | collection read authority |
| `GET /api/v1/collections/{collection_id}/permissions/effective/principal/{principal_id}` | Direct and inherited permission rows on a single collection for the principal's groups | collection read authority |
| `POST` / `DELETE /api/v1/iam/groups/{group_id}/members/{principal_id}` | Add/remove a member (human or SA) | **admin only** |

Mint accepts `name`, `description`, `expires_at`, permission `scopes`, and
`resource_scopes`. `GET /api/v1/iam/me` returns both dimensions for the current
token; token-list metadata continues to expose the combined `scoped` boolean.

Two safety properties worth calling out:

- **Revoke is scoped by *both* path ids.** A revoke updates
  `WHERE id = {token_id} AND principal_id = {path id}`, so a manager of principal A
  cannot revoke principal B's token by guessing its id (mismatch → `404`).
- **Group-membership mutation is admin-only.** Being a human owner-group member lets
  you manage an SA and its credentials, but it does **not** let you grant that SA
  runtime collection access by adding it to arbitrary groups.

---

## Principal settings

Every principal has a local settings document for preferences. It is not included
in existing user, service-account, principal, or `/iam/me` responses, and it is
not searchable.

| Endpoint | Purpose | Authorization |
| --- | --- | --- |
| `GET /api/v1/iam/me/settings` | Read the current principal settings | Any valid token for the current principal |
| `PUT /api/v1/iam/me/settings` | Replace the current principal settings | Any valid token for the current principal |
| `PATCH /api/v1/iam/me/settings` | Merge a patch into the current principal settings | Any valid token for the current principal |
| `DELETE /api/v1/iam/me/settings` | Reset the current principal settings to `{}` | Any valid token for the current principal |
| `/api/v1/iam/principals/{principal_id}/settings` | The same operations for a named principal | Self, or an unscoped human admin; denied cross-principal requests return `404` |

The settings document root must be a JSON object. Nested values may be any JSON
type. `PUT` replaces the entire document, so it can retain a nested `null` value.

`PATCH` applies object-only JSON Merge Patch semantics:

- Object values merge recursively.
- A `null` patch value removes that key; it does not store a JSON `null`.
- Arrays, strings, numbers, and booleans replace the existing value.
- Patching an object into a missing key or a non-object value starts with `{}`.

For example:

```json
// Current settings
{
  "theme": "light",
  "layout": { "density": "normal", "sidebar": true },
  "tags": ["a"]
}

// PATCH body
{
  "theme": "dark",
  "layout": { "sidebar": null, "columns": 2 },
  "tags": ["b"]
}

// Result
{
  "theme": "dark",
  "layout": { "density": "normal", "columns": 2 },
  "tags": ["b"]
}
```

Settings mutations are serialized with a principal row lock, so concurrent patches
preserve unrelated changes. Each mutation that changes the settings records a
complete before and after snapshot in an `updated` audit event for the target user
or service account. Unchanged mutations are no-ops and do not emit an event.

---

## Request authority: extractors and gates

Each handler declares the authority it requires. There are two families.

**Scope-aware (the only family that accepts scoped tokens and service accounts):**

- `Authenticated` — resolves the principal and, if the token is scoped, its scope
  set. Every downstream authority decision threads the scopes into the
  fail-closed pre-filter. Resource endpoints and task submission use this
  extractor. Imports and exports add an unscoped runtime-admin gate after
  authentication; the gate accepts service accounts but rejects scoped tokens.
  Remote-target invocation retains ordinary scope-aware authorization.

**Human/IAM (privilege-separated):**

- `UserAccess`, `AdminAccess`, `AdminOrSelfAccess`, `ManagementAccess` — used for
  human-only and credential/IAM operations (user CRUD, service-account CRUD,
  principal token management, admin/all-token logout, group-member mutation).

The human/IAM extractors apply two gates, in order, **before** any admin/self check:

1. **Kind gate** — the principal must be `kind = 'human'`. A service account is
   rejected with `403 Forbidden` (cleanly, never a `500`), even if it is in the
   admin group and presents an unscoped token.
2. **Scope gate** — the token must be unscoped. A scoped token is rejected with
   `403 Forbidden`.

So a scoped automation token can be used only on `Authenticated` endpoints (where
the scope pre-filter applies), and only humans with unscoped tokens can reach
IAM/credential-management surfaces.

### Login, validate, logout

- `POST /api/v0/auth/login` — humans only, **by `name`**. Service accounts have no
  password and receive a generic `401`. Failed attempts are rate-limited by
  `name` + client IP (see [login_rate_limiting.md](login_rate_limiting.md)).
- `GET /api/v0/auth/validate` and current-token logout use `Authenticated`, so a
  valid scoped service-account token validates as valid.
- All-token logout / revoke-all are unscoped human/IAM management operations.

---

## Privilege separation

The model guarantees, structurally, that **a service account is never a human IAM
administrator**, regardless of its group membership or token:

- An SA may be granted runtime collection authority by being placed in a group (even
  the admin group) — that is a deliberate, grantable capability.
- But the `kind = 'human'` gate on the IAM/management extractors means an SA can
  never create/modify users, manage service accounts, manage credentials, or mutate
  group membership — it is denied with `403` before any admin check runs.
- Scoped tokens are likewise confined to scope-aware resource endpoints and can
  never reach IAM/management surfaces.

This separation is enforced by construction (extractor gates + database
constraints), not by convention.

---

## See also

- [permissions.md](permissions.md) — the group/collection permission model and the
  meaning of each permission.
- [login_rate_limiting.md](login_rate_limiting.md) — login throttling.
- [task_system.md](task_system.md) / [task_api.md](task_api.md) — the async task
  framework that carries the scope snapshot.
