# The Hubuum ecosystem

<!-- markdownlint-disable-next-line MD033 -->
<span id="one-entry-point-clear-ownership"></span>

The server owns the data model, permissions, and HTTP contracts. Companion
projects provide interfaces for people and applications. Choose the interface
that fits your workflow; all of them connect to a Hubuum server.

| Component | Use it for | Documentation home |
| --- | --- | --- |
| [Hubuum server](https://github.com/hubuum/hubuum) | Data storage, API, authorization, background work, and administration | This site |
| [Web frontend](https://github.com/hubuum/hubuum-frontend) | Interactive inventory and administration in a browser | [Frontend guides](https://hubuum.github.io/hubuum-frontend/) |
| [CLI](https://github.com/hubuum/hubuum-cli) | Interactive terminal work, scripts, and shell automation | [CLI guide](https://hubuum.github.io/hubuum-cli/) |
| [Rust client](https://github.com/hubuum/hubuum-client-rust) | Typed asynchronous and blocking Rust applications | [Rust client guides](https://hubuum.github.io/hubuum-client-rust/) |
| [Python client](https://github.com/hubuum/hubuum-client-python) | Typed synchronous and asynchronous Python applications | [Python client guides](https://hubuum.github.io/hubuum-client-python/) |

The archived [`hubuum-python`](https://github.com/hubuum/hubuum-python) repository
is an earlier implementation. The current Python client is
**`hubuum-client-python`**.

## Choose compatible interfaces

Start with [clients and interfaces](integrations/clients.md) for installation,
API references, and compatibility records. Projects release independently;
select the guide matching your installed version.

For contributors, the [documentation workflow](contributing/documentation.md#connecting-companion-projects)
explains content ownership and shared publishing.
