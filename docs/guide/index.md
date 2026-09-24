# Working with Hubuum

Use this guide to model inventory and carry out everyday data workflows.
The examples describe the server's behavior across the web interface, CLI, and
API. Interface-specific controls are documented with the
[clients and frontend](../integrations/clients.md).

## Build a useful model

1. Learn the [core concepts](../concepts.md).
2. Plan [collections](../collection_hierarchy.md) around ownership and access.
3. Grant groups the required [permissions](../permissions.md).
4. Define classes and [evolve their validation schemas](../schema_evolution.md).
5. Link objects with [relationships](../relationship_endpoints.md) and derive
   useful values with [computed fields](../computed_fields.md).

## Find and update information

- [Querying](../querying.md): filters, ordering, JSON fields, and cursor pagination.
- [Search](../search_api.md): discover resources or construct structured searches.
- [Name addressing](../name_addressing.md): use known class and object names.
- [JSON Patch](../object_data_json_patch.md): change part of an object's raw data atomically.
- [Resource revisions](../resource_revisions.md): prevent lost updates.
- [Temporal history](../temporal_history.md): inspect past state and provenance.

## Automate recurring work

Use [imports](../import_api.md) for graph data and [exports](../export_api.md)
for extraction and reports. [Export templates](../export_template_guide.md)
produce text, HTML, or CSV from the export context.

Track [background tasks](../task_api.md) through completion, including individual
results and output expiry. [Remote targets](../remote_targets.md) run configured
outbound actions; [events](../events.md) support audit and subscribed delivery.
An import/export workflow is distinct from a full-system
[backup and restore](../backup-restore.md).
