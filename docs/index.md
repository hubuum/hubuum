---
description: Find your starting point for modeling assets, running Hubuum, and building integrations.
---

# Hubuum documentation

Hubuum is an open-source asset management service. Define the kinds of resources
your organization needs, connect them with relationships, and query them through
a shared API with group-based access control.

<!-- markdownlint-disable-next-line MD033 -->
<div class="grid cards" markdown>

- **Explore Hubuum**

    Understand collections, classes, objects, and how Hubuum fits alongside
    existing sources of inventory data.

    [Learn the concepts](concepts.md) · [Explore the ecosystem](ecosystem.md)

- **Start using it**

    Run a server, make your first requests, and choose a web, command-line,
    or programming interface.

    [Get started](getting-started/index.md) · [User guide](guide/index.md)

- **Run it reliably**

    Install and upgrade, configure identity and permissions, monitor workers,
    and prepare for recovery.

    [Administration](administration/index.md) · [Troubleshooting](administration/troubleshooting.md)

- **Build with Hubuum**

    Connect an application using the HTTP API or typed clients. Find the
    contracts for queries, tasks, imports, and exports.

    [API & integrations](integrations/index.md) · [Contributing](contributing/index.md)

</div>

## Find a specific answer

| I need to… | Start here |
| --- | --- |
| Deploy the server and web interface | [Single-host deployment](deployment.md) |
| Configure the server | [Configuration reference](quick_start.md) |
| Decide who can access data | [Permissions](permissions.md) |
| Filter, sort, or page through objects | [Querying](querying.md) |
| Move data between systems | [Imports](import_api.md) and [exports](export_api.md) |
| Monitor a deployment | [Metrics](metrics.md), [logs](logging.md), and [traces](tracing.md) |
| Recover a deployment | [Backup and restore](backup-restore.md) |
| Use the CLI, Python, Rust, or frontend | [Clients and interfaces](integrations/clients.md) |

## Documentation and release status

The site opens the latest published release. Use the version menu to choose an
older release or explicitly select **main (development)** for unreleased changes.
Hubuum is under active development before `1.0`; check
[releases and compatibility](releases.md) for your installed version.
Production deployments use PostgreSQL. The memory backend is experimental and
non-durable.

The documentation is maintained in the
[Hubuum repository](https://github.com/hubuum/hubuum). Use the edit link on a
page to propose a correction, or [report an issue](https://github.com/hubuum/hubuum/issues).
