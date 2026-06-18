# Login Rate Limiting

Hubuum throttles failed login attempts to resist online password guessing. This page
describes the model, how the real client IP is resolved behind proxies, the configuration
knobs, and the admin endpoints for inspecting and releasing throttled scopes.

For the configuration table summary, see [quick_start.md](quick_start.md). This page is the
in-depth reference.

## Threat model

The limiter defends against online credential-guessing on the `POST /api/v0/auth/login`
endpoint. Two attacks motivated the current design (see advisory GHSA-63j4-jh8h-chch):

1. *Password spraying.* One password tried across many usernames from a single host. A
   limiter keyed only on `(username, IP)` leaves every bucket at one attempt, so nothing is
   throttled.
2. *Spoofed forwarding headers.* When the server trusts `X-Forwarded-For`, taking the
   leftmost (attacker-supplied) entry lets an attacker mint a fresh bucket on every request
   by rotating the header, defeating single-account brute force.

## Layered scopes

Every failed login is recorded against up to three scopes, each with its own threshold.
A login is throttled if *any* applicable scope is currently locked out.

| Scope | Key | Default threshold | Catches |
| ----- | --- | ----------------- | ------- |
| User + IP | `(username, client IP)` | `5` | Single-account brute force from one host |
| IP | `client IP` | `20` | Spraying many usernames from one host |
| Subnet | `client IP / prefix` | `100` | Distributed spraying from one network |

The per-IP and per-subnet scopes apply only when a client IP is known and their thresholds
are non-zero. Setting `HUBUUM_LOGIN_RATE_LIMIT_MAX_ATTEMPTS_PER_IP=0` or
`HUBUUM_LOGIN_RATE_LIMIT_MAX_ATTEMPTS_PER_SUBNET=0` disables that scope.

Subnets are formed by masking the client IP to a configurable prefix length (default `/24`
for IPv4, `/64` for IPv6).

A successful login clears only the `(username, IP)` scope. The per-IP and per-subnet
budgets are deliberately left intact, so one user's success cannot reset the spray or
distributed counters for the whole host or network.

## Sliding window and exponential backoff

Each scope keeps a sliding window of recent failure timestamps
(`HUBUUM_LOGIN_RATE_LIMIT_WINDOW_SECONDS`, default `300`). When the number of failures in
the window reaches the scope threshold, the scope is *locked out* and its window is reset.

Lockouts use exponential backoff. The first lockout lasts
`HUBUUM_LOGIN_RATE_LIMIT_BACKOFF_BASE_SECONDS` (default `300`); each subsequent lockout of
the same scope doubles, capped at `HUBUUM_LOGIN_RATE_LIMIT_BACKOFF_MAX_SECONDS`
(default `86400`, i.e. 24 hours):

```text
lockout = min(base * 2^(level - 1), max)
level 1 -> 300s    level 2 -> 600s    level 3 -> 1200s    ...   capped at max
```

Backoff reflects *recent, sustained* abuse. An attacker who resumes immediately after a
lockout expires keeps escalating. If a scope instead stays quiet for a full window after a
lockout expires, the escalation resets to the base level (a genuine cool-off). This avoids
penalizing a legitimate user who triggers one lockout and returns much later.

## Resolving the real client IP

Accurate throttling and the client allowlist both depend on identifying the true client IP,
which is non-trivial behind reverse proxies. `X-Forwarded-For` is attacker-controllable, so
Hubuum never trusts it blindly.

The client IP is resolved from the *right* of the hop chain
`[X-Forwarded-For..., peer]`, where `peer` is the address of the TCP connection Hubuum
actually accepted (the closest, least forgeable hop) and the forwarded entries grow more
attacker-controllable toward the left.

Trust is configured with:

- `HUBUUM_TRUST_IP_HEADERS` (default `false`) — master switch. When `false`, the peer
  address is always used and forwarded headers are ignored.
- `HUBUUM_TRUSTED_PROXIES` (default empty) — comma-separated proxy IPs/CIDRs. Hops in this
  set are skipped from the peer inward, and the first untrusted hop is taken as the client.
  This is the preferred mechanism.
- `HUBUUM_TRUSTED_PROXY_HOPS` (default `0`) — used only when `HUBUUM_TRUSTED_PROXIES` is
  empty. Skips this many hops from the right of the chain.

If `HUBUUM_TRUST_IP_HEADERS` is `true` but neither a trusted-proxy allowlist nor a hop count
is configured, forwarded headers cannot be trusted safely and the peer address is used
instead (a warning is logged once per process).

### Worked examples

Assume one reverse proxy at `203.0.113.1` in front of Hubuum, with
`HUBUUM_TRUST_IP_HEADERS=true` and `HUBUUM_TRUSTED_PROXIES=203.0.113.1/32`.

A genuine client at `198.51.100.9`:

```text
peer = 203.0.113.1
X-Forwarded-For: 198.51.100.9
chain (trustworthy -> claimed): [203.0.113.1, 198.51.100.9]
203.0.113.1 is trusted -> skip; 198.51.100.9 is untrusted -> client = 198.51.100.9
```

The same client trying to spoof an address by sending its own `X-Forwarded-For`:

```text
peer = 203.0.113.1
X-Forwarded-For: 9.9.9.9, 198.51.100.9   (the proxy appends the real client)
chain: [203.0.113.1, 198.51.100.9, 9.9.9.9]
skip trusted 203.0.113.1 -> first untrusted is 198.51.100.9 -> spoofed 9.9.9.9 is ignored
```

An attacker connecting directly (not through the trusted proxy) cannot forge a client IP:
the peer is the attacker's own untrusted address, so it becomes the client regardless of any
`X-Forwarded-For` they send.

## Configuration reference

| Variable | Default | Description |
| -------- | ------- | ----------- |
| `HUBUUM_LOGIN_RATE_LIMIT_ENABLED` | `true` | Master switch for login throttling |
| `HUBUUM_LOGIN_RATE_LIMIT_MAX_ATTEMPTS` | `5` | Max failed attempts per `(username, IP)` per window |
| `HUBUUM_LOGIN_RATE_LIMIT_MAX_ATTEMPTS_PER_IP` | `20` | Max failed attempts per client IP per window (`0` disables) |
| `HUBUUM_LOGIN_RATE_LIMIT_MAX_ATTEMPTS_PER_SUBNET` | `100` | Max failed attempts per client subnet per window (`0` disables) |
| `HUBUUM_LOGIN_RATE_LIMIT_WINDOW_SECONDS` | `300` | Sliding window length in seconds |
| `HUBUUM_LOGIN_RATE_LIMIT_BACKOFF_BASE_SECONDS` | `300` | First lockout duration; doubles on repeated lockouts |
| `HUBUUM_LOGIN_RATE_LIMIT_BACKOFF_MAX_SECONDS` | `86400` | Maximum lockout duration |
| `HUBUUM_LOGIN_RATE_LIMIT_SUBNET_PREFIX_V4` | `24` | IPv4 prefix length for subnet aggregation |
| `HUBUUM_LOGIN_RATE_LIMIT_SUBNET_PREFIX_V6` | `64` | IPv6 prefix length for subnet aggregation |
| `HUBUUM_TRUST_IP_HEADERS` | `false` | Master switch for trusting `X-Forwarded-For` |
| `HUBUUM_TRUSTED_PROXIES` | *(empty)* | Trusted reverse-proxy IPs/CIDRs |
| `HUBUUM_TRUSTED_PROXY_HOPS` | `0` | Trusted hop count (used when no allowlist is set) |

## Admin endpoints

These live under the admin-only `/meta` path and require an admin bearer token. They let an
operator inspect current throttling and release scopes (for example, to unblock a locked-out
user after verifying the cause).

### Inspect state

```text
GET /api/v0/meta/login-rate-limit?include=locked
```

Query parameters (all optional, and combinable):

- `include` — `locked` (default, only currently-locked scopes) or `all` (every tracked
  scope).
- `scope` — restrict to one scope kind: `user_ip`, `ip`, or `subnet`.
- `q` — case-insensitive substring match on the scope identifier (a username, IP, or
  subnet). For example, `q=alice` or `q=198.51.100`.

`tracked_entries` and `locked_entries` are totals across all scopes; `returned_entries` is
the number of scopes left after the filters are applied. The response contains the effective
configuration, those counts, and the matching scopes:

```jsonc
{
  "config": {
    "enabled": true, "max_attempts": 5, "max_attempts_per_ip": 20,
    "max_attempts_per_subnet": 100, "window_seconds": 300,
    "backoff_base_seconds": 300, "backoff_max_seconds": 86400,
    "subnet_prefix_v4": 24, "subnet_prefix_v6": 64
  },
  "tracked_entries": 42,
  "locked_entries": 3,
  "returned_entries": 1,
  "entries": [
    {
      "id": "dTphbGljZXwxLjIuMy40",
      "scope": "user_ip",
      "identifier": "alice|1.2.3.4",
      "attempts": 5,
      "locked": true,
      "locked_for_seconds": 280,
      "lockout_level": 1
    }
  ]
}
```

The `id` is an opaque, URL-safe handle for the scope; use it to release a single entry.

### Release one scope

```text
DELETE /api/v0/meta/login-rate-limit/{id}
```

Releases the scope with the given `id`, returning `{ "released": true }`. Returns `404` if
the entry no longer exists (for example, it already expired).

### Release all scopes

```text
DELETE /api/v0/meta/login-rate-limit
```

Clears all tracked scopes, returning `{ "cleared": <count> }`.

## Operational notes and limitations

Throttling state is held in memory, in the server process. This has two consequences:

- *Per instance.* State is shared across worker threads within one process but not across
  multiple horizontally-scaled instances. Each instance throttles independently.
- *Not persistent.* State is lost on restart.

For single-instance deployments this is sufficient. A shared, persistent backend (for
example Valkey/Redis) is tracked as a future option in
[issue #53](https://github.com/hubuum/hubuum/issues/53).

To recover from accidental lockouts you can release scopes via the admin endpoints above,
relax the relevant thresholds, or (as a blunt instrument) set
`HUBUUM_LOGIN_RATE_LIMIT_ENABLED=false` and restart.
