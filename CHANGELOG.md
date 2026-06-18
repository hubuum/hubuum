# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Security

- Fix login rate-limiter bypass (GHSA-63j4-jh8h-chch). Login throttling is now layered
  across per-`(username, IP)`, per-IP, and per-subnet scopes with exponential backoff, so
  password spraying across many usernames and distributed attempts from one network are
  throttled. The client IP is now resolved from the right of the
  `[X-Forwarded-For..., peer]` hop chain using a configurable trusted-proxy allowlist
  (`HUBUUM_TRUSTED_PROXIES`) or hop count (`HUBUUM_TRUSTED_PROXY_HOPS`), so spoofed
  `X-Forwarded-For` values can no longer mint fresh rate-limit buckets or bypass the
  client allowlist.

### Added

- New login rate-limit configuration: `HUBUUM_LOGIN_RATE_LIMIT_ENABLED`,
  `HUBUUM_LOGIN_RATE_LIMIT_MAX_ATTEMPTS_PER_IP`,
  `HUBUUM_LOGIN_RATE_LIMIT_MAX_ATTEMPTS_PER_SUBNET`,
  `HUBUUM_LOGIN_RATE_LIMIT_BACKOFF_BASE_SECONDS`,
  `HUBUUM_LOGIN_RATE_LIMIT_BACKOFF_MAX_SECONDS`,
  `HUBUUM_LOGIN_RATE_LIMIT_SUBNET_PREFIX_V4`, `HUBUUM_LOGIN_RATE_LIMIT_SUBNET_PREFIX_V6`,
  and proxy-trust configuration `HUBUUM_TRUSTED_PROXIES` / `HUBUUM_TRUSTED_PROXY_HOPS`.

## [0.0.1] - 2026-03-12

### Added

- Initial release of Hubuum.
