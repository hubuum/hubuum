# Permission model

The [Atlas example](getting-started/example-dataset.md#explore-collection-permissions)
provides two empty groups: atlas-readers inherits read access to the whole
inventory; atlas-operators can maintain objects only in the operations child
collection. Assign evaluation principals to the groups to exercise these rules.

Hubuum divides user-created structures into classes and their objects. Objects are instances of classes. Each class and object belongs to one collection,
but a class and its objects need not share the same collection. A collection may
contain multiple classes and objects.

Permissions within Hubuum are based on the following principles:

- Permissions are granted to groups (only). If one wishes to grant permissions to a specific user only, create a group with a single member.
- Permissions are granted on collections. Permissions are never granted to individual classes or objects.
- Collection permissions are inherited by child collections. Class and object membership is still concrete: each class or object belongs to exactly one collection.
- Permissions are not inherited from classes to objects. If a user has read access to a class, they do not automatically have read access to the objects of that class.

Group membership is principal-centric: both human users and service accounts are
**principals** and gain a group's permissions by being members of it. For the
identity model, tokens, and how token **scopes** narrow these permissions for
automated callers, see [auth_model.md](auth_model.md).

For hierarchy endpoints and examples, see
[collection_hierarchy.md](collection_hierarchy.md).

## Collection hierarchy and inheritance

Collections form a tree rooted at the system `root` collection. Every collection
except `root` has exactly one parent collection, and new collections default to
`root` when `parent_collection_id` is omitted.

A permission row granted to a group on a collection applies to that collection
and all descendant collections. Inheritance is additive only:

- There are no deny rules and no child override rules.
- All permission types inherit, including `DelegateCollection`, `DeleteCollection`,
  `ReadAudit`, and remote-target permissions.
- Token scopes can narrow by permission type and by collection, class, or object
  identity. A scoped token cannot gain access outside the principal's group
  grants.
- Combined permission checks are not unioned across rows. If an operation needs
  `ReadCollection` and `UpdateCollection` on a target collection, one permission
  row on the target or one ancestor must contain both flags.

Permission-management endpoints remain direct-row operations. Granting,
replacing, revoking, and listing stored rows under
`/api/v1/collections/{collection_id}/permissions` affects only the named
collection. Use the effective endpoints when debugging inherited access:

| Endpoint | Meaning |
| --- | --- |
| `GET /api/v1/collections/{collection_id}/permissions/effective/group/{group_id}` | Shows the direct and inherited permission rows that apply to one group. |
| `GET /api/v1/collections/{collection_id}/permissions/effective/principal/{principal_id}` | Shows the direct and inherited permission rows that apply through a principal's group memberships. |
| `GET /api/v1/collections/{collection_id}/has_permissions/{permission}` | Lists groups with that direct or inherited permission on the collection. |

Collections can be inspected and moved with these hierarchy endpoints:

| Endpoint | Meaning |
| --- | --- |
| `GET /api/v1/collections/{collection_id}/children` | Lists direct child collections. |
| `GET /api/v1/collections/{collection_id}/ancestors` | Lists ancestors, nearest parent first. |
| `PUT /api/v1/collections/{collection_id}/parent` | Moves a collection by accepting `{"parent_collection_id": <id>}`. |

Moving a collection is separate from updating its name or description. The
caller needs effective `UpdateCollection` on the collection being moved and
effective `DelegateCollection` on both the old parent and the new parent. The
`admin` group bypass still applies. The root collection cannot be moved or
deleted, and a collection cannot be moved under itself or one of its
descendants. Collections with child collections cannot be deleted.

Collection names are unique among siblings. The same name may appear in
different branches of the tree.

## Permission types

Permissions are granted on collections and control operations on the collection
and its resources. The following tables group them by resource.

### Permissions for collections

The following permissions are available for collections:

| Permission | Description |
| --- | --- |
| `ReadCollection` | Allows reading data about the collection, ie its members or the permissions associated with it. |
| `UpdateCollection` | Allows updating the collection (changing its name). |
| `DeleteCollection` | Allows deleting the collection. |
| `DelegateCollection` | Allows delegating permissions for the collection. |
| `CreateClass` | Allows creating classes within the collection. |
| `CreateObject` | Allows creating objects within the collection. |
| `CreateClassRelation` | Allows creating relationships of classes within the collection. |

Granting a group access to a parent collection grants the same permissions to
all descendant collections. Grant rows can still be added directly on a child
collection when a narrower or additional permission set is needed for that
subtree.

### Permissions for classes

The following permissions are available for classes:

| Permission | Description |
| --- | --- |
| `ReadClass` | Allows reading the class. |
| `UpdateClass` | Allows updating the class (ie, change its name, its definition, validation requirements, etc). |
| `DeleteClass` | Allows deleting the class. Note that deleting a class deletes all objects belonging to that class. |
| `CreateObject` | Allows creating new objects of the class. |

### Permissions for objects

The following permissions are available for objects:

| Permission | Description |
| --- | --- |
| `ReadObject` | Allows reading the object. |
| `UpdateObject` | Allows updating the object. |
| `DeleteObject` | Allows deleting the object. |

### Permissions for class relationships

The following permissions are available for relationships between classes:

| Permission | Description |
| --- | --- |
| `ReadClassRelation` | Allows reading the relationship. |
| `UpdateClassRelation` | Allows updating the relationship. |
| `DeleteClassRelation` | Allows deleting the relationship. |
| `CreateObjectRelation` | Allows creating relationships between objects adhering of the class relationship. |

### Permissions for export templates

Export templates are used to format export output and are scoped to collections. The following permissions control access to export templates:

| Permission | Description |
| --- | --- |
| `ReadTemplate` | Allows reading export template definitions. |
| `CreateTemplate` | Allows creating new export templates within the collection. Also required when moving a template to a different collection (as the target collection permission). |
| `UpdateTemplate` | Allows modifying existing export templates (name, description, template content, collection). Required when moving a template to a different collection (as the source collection permission). |
| `DeleteTemplate` | Allows deleting export_templates from the collection. |

**Important notes about template permissions:**

- Template management is collection-scoped, meaning CRUD operations require the appropriate permission on the template's collection.
- Running a stored-template export requires `ReadTemplate` on the template's
  collection plus the read permissions required by the exported resources.
  Direct exports require the corresponding resource read permissions.
- Moving a template between collections requires both `update_template` on the source collection and `create_template` on the target collection.
- Templates with the same name cannot exist within the same collection (enforced by a unique constraint).
- Valid template content types are: `text/plain`, `text/html`, and `text/csv`. The `application/json` content type is reserved for the default JSON export output and cannot be used for stored export templates.

### Permissions for remote targets

Remote targets define outbound subject actions and are scoped to collections. The following permissions
control target management and invocation:

| Permission | Description |
| --- | --- |
| `ReadRemoteTarget` | Allows listing and reading remote target definitions in the collection. |
| `CreateRemoteTarget` | Allows creating remote targets in the collection. Also required when moving a target into a collection. |
| `UpdateRemoteTarget` | Allows modifying existing targets in the collection. Required on the source collection when moving a target. |
| `DeleteRemoteTarget` | Allows deleting targets from the collection. |
| `ExecuteRemoteTarget` | Allows invoking targets in the collection. |

Invoking a remote target also requires read permission for the selected subject. Collection subjects
require `ReadCollection`; class subjects require `ReadClass`; object subjects require `ReadObject`;
class relation subjects require `ReadClassRelation` on both endpoint collections; object relation
subjects require `ReadObjectRelation` on both endpoint collections. The worker re-checks both subject
read permission and `ExecuteRemoteTarget` for the submitting user before executing the outbound HTTP
call. `ReadRemoteTarget` is not required to invoke a target by ID.

<!-- Previous walkthrough bookmarks now point to the current group-based example. -->
<!-- markdownlint-disable MD033 -->
<span id="example"></span>
<span id="part-1-a-relatively-simple-example"></span>
<span id="part-2-a-second-department"></span>
<span id="part-3-a-bit-of-offloading"></span>
<span id="examples-in-api-form"></span>
<span id="part-1-a-relatively-simple-example_1"></span>
<!-- markdownlint-enable MD033 -->

## Example: grant a group access to Atlas

Load the [Atlas inventory](getting-started/example-dataset.md) and use an
unscoped administrator token for these setup requests. Atlas already provides
`atlas-readers` and `atlas-operators`; this example adds a separate group for
people who may update existing operations objects but may not create or delete
objects. Grant access through a group even when only one person needs it.

### 1. Create the group

```http
POST /api/v1/iam/groups
Authorization: Bearer <admin-token>
Content-Type: application/json

{
  "groupname": "atlas-maintainers",
  "description": "Update existing Atlas operations objects"
}
```

Save the returned group `id` as `<group_id>`. Find the operations collection
with `GET /api/v1/collections?name=atlas-demo-operations` and use its returned
`id` as `<collection_id>`. IDs depend on the installation; do not assume fixed
values.

### 2. Grant collection permissions

```http
POST /api/v1/collections/<collection_id>/permissions/group/<group_id>
Authorization: Bearer <admin-token>
Content-Type: application/json

[
  "ReadCollection",
  "ReadClass",
  "ReadObject",
  "UpdateObject"
]
```

The body is an array of permission names. `POST` adds permissions; `PUT` replaces
the group's direct permission row. These grants also apply to descendants of
the selected collection. They do not grant access to the parent catalogue.

### 3. Add a principal and verify access

An administrator can add an existing human or service account using
`POST /api/v1/iam/groups/<group_id>/members/<principal_id>`. Resolve the principal
ID from the identity API; user IDs and principal IDs are different identities.
See [group membership](auth_model.md#tokens-and-groups-by-principal).

Sign in as that non-admin principal with an unscoped token. With only this group
membership, the principal can read and update the Server and Location objects
in `atlas-demo-operations`, but cannot create or delete them or read objects in
the parent collection. Other group memberships add permissions, while token
scopes may narrow them.

Inspect the effective grants when the result differs from your expectation:

```http
GET /api/v1/collections/<collection_id>/permissions/effective/principal/<principal_id>
Authorization: Bearer <admin-token>
```

Remove the principal from the example group with `DELETE` on the membership
route, then delete the group with `DELETE /api/v1/iam/groups/<group_id>` when
finished. This leaves the original Atlas groups and inventory intact.

## A word about inheritance and admin privileges

In the examples above, the central security group can either be granted access
directly on each department collection, or be granted access on a common parent
collection so those permissions inherit to the department collections. There is
still no implicit access granted to magic groups except for the `admin` group,
which is a special case. The `admin` group has full access to everything and is
intended for Hubuum system administrators only.

### Event integrations

`ManageEventSubscription` permits collection subscription and destination discovery
and deletion. Creating or updating collection subscriptions and owned webhooks also
requires `ReadAudit`. Shared sinks require an explicit administrator grant to the
collection; those grants do not inherit. See [event integrations](events.md#sinks-and-subscriptions).
