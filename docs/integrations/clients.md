# Clients, CLI, and frontend

All interfaces can use the [Atlas example dataset](../getting-started/example-dataset.md).
Load it into one evaluation server, then use the same Service/Atlas,
Server/web-01, Location/Oslo, and Context/Research notes records across clients.
Resolve numeric IDs from names; import does not promise fixed IDs.

These are the current companion projects. Their documentation remains with
their source so installation details and examples can track each release.

## Web frontend

Use the [frontend](https://hubuum.github.io/hubuum-frontend/) for browser
workflows. Its server-side backend-for-frontend holds Hubuum bearer tokens;
the browser uses a session cookie. Multi-replica installations share session
state through Valkey.

The [single-host installer](../deployment.md) can deploy the frontend and server
together. Follow its proxy routing instructions: the frontend owns
`/_hubuum-bff/...`, while `/api/v0/...` and `/api/v1/...` belong to the Hubuum
server. For frontend-only deployment and settings, use the
[frontend repository](https://github.com/hubuum/hubuum-frontend).

## Command-line interface

The [Hubuum CLI](https://github.com/hubuum/hubuum-cli#readme) provides one-shot
commands, an interactive REPL, and script execution. Start with its
[usage guide](https://hubuum.github.io/hubuum-cli/) and
[release downloads](https://github.com/hubuum/hubuum-cli/releases).

The client executable `hubuum-cli` is different from `hubuum-admin`.
`hubuum-cli` connects through the HTTP API. `hubuum-admin` ships with the server
and handles local administration such as database migrations, password resets,
and the restore executor.

## Rust client

[`hubuum_client`](https://crates.io/crates/hubuum_client) provides async and
blocking clients, typed resource IDs, query builders, and task helpers.

- [Versioned setup, examples, and guides](https://hubuum.github.io/hubuum-client-rust/).
- [Generated Rust API reference](https://docs.rs/hubuum_client).

The HTTP client is distinct from the server's
[storage adapter SDK](../storage_adapter_sdk.md). Application integrations
normally use the client, not the internal root `hubuum` crate.

## Python client

[`hubuum-client-python`](https://github.com/hubuum/hubuum-client-python) provides
typed synchronous and asynchronous clients. The distribution is named
`hubuum-client` and its import package is `hubuum_client`.

- [Versioned installation, guides, and API reference](https://hubuum.github.io/hubuum-client-python/).
- [Client configuration](https://hubuum.github.io/hubuum-client-python/v0.0.11/client/).
- [Queries](https://hubuum.github.io/hubuum-client-python/v0.0.11/querying/).
- [API guide](https://hubuum.github.io/hubuum-client-python/v0.0.11/api/).

## Choose compatible versions

Do not infer compatibility from matching version numbers or from a `latest`
tag. Each companion project declares its server targets independently:

| Project | Compatibility and release evidence |
| --- | --- |
| Rust client | [Compatibility matrix](https://hubuum.github.io/hubuum-client-rust/v0.14.1/compatibility/) |
| Python client | [Compatibility matrix](https://hubuum.github.io/hubuum-client-python/v0.0.11/compatibility/) |
| CLI | [Compatibility matrix](https://hubuum.github.io/hubuum-cli/v0.0.14/compatibility/) |
| Frontend | [Compatibility matrix](https://hubuum.github.io/hubuum-frontend/v0.0.19/compatibility/) |

These records target server v0.0.18. Select the matching edition for an older
installation. Verify the workflows you rely on, especially credential
approvals, imports, schema changes, and backup formats. The server's
[integration coverage](../integration-coverage.md) explains which contracts
currently have real-system evidence.
