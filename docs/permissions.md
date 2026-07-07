# Permission model

Hubuum divides user-created structures into classes and their objects. Objects are instances of classes, and both are contained within a single collection. A collection may contain multiple classes and objects.

Permissions within Hubuum are based on the following principles:

- Permissions are granted to groups (only). If one wishes to grant permissions to a specific user only, create a group with a single member.
- Permissions are granted on collections. Permissions are never granted to individual classes or objects.
- Collection permissions are inherited by child collections. Class and object membership is still concrete: each class or object belongs to exactly one collection.
- Permissions are not inherited from classes to objects. If a user has read access to a class, they do not automatically have read access to the objects of that class.

Group membership is principal-centric: both human users and service accounts are
**principals** and gain a group's permissions by being members of it. For the
identity model, tokens, and how token **scopes** narrow these permissions for
automated callers, see [auth_model.md](auth_model.md).

For examples, hierarchy endpoints, and developer implementation notes, see
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
- Token scopes still only narrow by permission type. A scoped token cannot gain
  collection access outside the principal's group grants.
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

There are three types of permissions for each collection:

1. Permissions for the collections themselves
2. Permissions for classes
3. Permissions for objects

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
| `ReadTemplate` | Allows reading export templates and using them in export generation. Required to view template definitions or to reference a template when running an export. |
| `CreateTemplate` | Allows creating new export templates within the collection. Also required when moving a template to a different collection (as the target collection permission). |
| `UpdateTemplate` | Allows modifying existing export templates (name, description, template content, collection). Required when moving a template to a different collection (as the source collection permission). |
| `DeleteTemplate` | Allows deleting export_templates from the collection. |

**Important notes about template permissions:**

- Templates are collection-scoped, meaning all template operations require the appropriate permission on the template's collection.
- Using a template in an export requires `read_template` permission on the collection containing the template.
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

## Example

### Part 1: A (relatively) simple example

Assume we have a university campus with a number of departments and a sentral security group. We have the following people:

- `alice` is a member of the central security group called `central-security`
- `bob` is a systems administrator at the Department of Mathematics, and a member of the local admin group called `mathematics-administrators`
- `chris` is a front line support technician at the Department of Mathematics, and a member of the local support group called `mathematics-support`

At the Department of Mathematics, the local administrators manage a number of computers. Local administrators have the permissions to manage the computers, while the front line support only has read-only access. The central security group needs to have full permissions to everything at the university as a whole. We can solve this as follows:

- We create a collection `shared` to hold all the shared resources at the university.
- We create a class `computer` to hold all the computers at the university and add it to the `shared` collection.
- We create a collection `mathematics` to hold all the computer objects belonging to the Department of Mathematics.
- We grant `central-security` the following permissions on the `shared` collection itself:
  - `create`
  - `read`
  - `update`
  - `delete`
  - `delegate`
- We grant `central-security` the following permissions on the `mathematics` collection itself:
  - `create`
  - `read`
  - `update`
  - `delete`
  - `delegate`
- We grant `mathematics-administrators` the following permissions on the `mathematics` collection itself:
  - `create`
  - `read`
  - `update`
  - `delete`
  - `delegate`
- We grant `mathematics-administrators` the following class permissions on the `shared` collection to allow them to create new objects of the `computer` class:
  - `read`
  - `create`
- We grant `mathematics-administrators` the following object permissions on the `mathematics` collection to allow them to administer the computers in the collection:
  - `create`
  - `read`
  - `update`
  - `delete`
- We grant `mathematics-support` the following object permissions on the `mathematics` collection:
  - `read`

When `bob` creates `eniac2`, a new computer object, he must assign it a collection. A permission check is performed for `bob` on the class `computers`, and we find that `bob` has `create` permissions on the class through the `mathematics-administrators` group. When `bob` assigns the object to the collection `mathematics`, an object-based permission check is performed for `bob` on the collection `mathematics`, and we find that `bob` has `create` permissions on the collection through the `mathematics-administrators` group. Thus, the object is created and `eniac2` becomes an object member of the collection `mathematics`. This also allows `chris` to read the object, but not to update or delete it.

### Part 2: A second department

Now that we everything from part 1 up and running, we enroll the Department of Physics in Hubuum. The Department of Physics has a local admin group called `physics-administrators` and a local support group called `physics-support`. They have their own computers, and they need to be able to administer them. We can solve this as follows:

- We create a collection `physics` to hold all the computer objects belonging to the Department of Physics.
- We grant `central-security` the following permissions on the `physics` collection itself:
  - `create`
  - `read`
  - `update`
  - `delete`
  - `delegate`
- We grant `physics-administrators` the following permissions on the `physics` collection itself:
  - `create`
  - `read`
  - `update`
  - `delete`
  - `delegate`
- We grant `physics-administrators` the following class permissions on the `shared` collection to allow them to create new objects of the `computer` class:
  - `read`
  - `create`
- We grant `physics-administrators` the following object permissions on the `physics` collection to allow them to administer the computers in the collection:
  - `create`
  - `read`
  - `update`
  - `delete`
- We grant `physics-support` the following object permissions on the `physics` collection:
  - `read`

This mirrors the setup for the Department of Mathematics, but with different groups and collections. Note that the `physics-administrators` group does not have any permissions on the `mathematics` collection, and vice versa. This means that `bob` cannot administer the computers belonging to the Department of Physics, but `alice` (through the `central-security` group) has full access to everything.

### Part 3: A bit of offloading

Now that we have two departments up and running, `bob` is asked to help out tidying out old computers at the Department of Physics. He's not to create new ones, but maybe update some information on some and delete others. The simple solution to this is to (temporarily) add `bob` to the `physics-administrators` group, but if one wants to be more fine-grained, we can grant `bob` (the user, not as a group) the following object permissions on the `physics` collection:

- `read`
- `update`
- `delete`

`bob` can now perform the required tasks, but he cannot create new objects in the `physics` collection. He can create computers (he has `create` permissions on the `shared` collection that holds the `computer` class), but due to the lack of object create permissions on the `physics` collection he cannot assign these computers to this colection.

## Examples, in API form

### Part 1 : A (relatively) simple example

- Create a collection `shared` to hold all the shared resources at the university.

Endpoint: `POST /api/v1/collections`

```json
{
  "name": "shared",
  "description": "Shared resources at the university"
}
```

This will return a link to the new collection, ie `/api/v1/collections/1` as wll as the new collection. We extract the ID, ie `1`, and use it in the following examples.

- A `computer` class that we add to the `shared` collection. We do not add a JSON schema, and we do not add any validation requirements.

Endpoint: `POST /api/v1/classes`

```json
{
  "name": "computer",
  "description": "A computer",
  "collection": 1,  
  "json_schema": null,
  "validation_requirements": false,
}
```

- We create a collection `mathematics` to hold all objects (and classes) belonging to the Department of Mathematics. In this case, we're only adding computers. Assume we get the ID `2` for the new collection.

Endpoint: `POST /api/v1/collections`

```json
{
  "name": "mathematics",
  "description": "Equipment at the Department of Mathematics"
}
```

- We grant the group `central-security` (assume the group has ID 1) all permissions on the `shared` and `mathematics` collections.

Endpoint: `POST /api/v1/permissions/collections/1/groups/1`

```json
{
  "has_create": true,
  "has_read": true,
  "has_update": true,
  "has_delete": true,
  "has_delegate": true
}
```

Endpoint: `POST /api/v1/permissions/collections/2/groups/1`

```json
{
  "has_create": true,
  "has_read": true,
  "has_update": true,
  "has_delete": true,
  "has_delegate": true
}
```

- We grant the group `mathematics-administrators` (assume it has ID 2) all permissions on the `mathematics` collection:

Endpoint: `POST /api/v1/permissions/collections/2/groups/2`

```json
{
  "has_create": true,
  "has_read": true,
  "has_update": true,
  "has_delete": true,
  "has_delegate": true
}
```

- We grant `mathematics-administrators` the following class permissions on the `shared` collection to allow them to create new objects of the `computer` class:

Endpoint: `POST /api/v1/permissions/classes/1/groups/2`

```json
{
  "has_create": true,
  "has_read": true
}
````

- We grant `mathematics-administrators` the following object permissions on the `mathematics` collection to allow them to administer the computers in the collection:

Endpoint: `POST /api/v1/permissions/objects/2/groups/2`

```json
{
  "has_create": true,
  "has_read": true,
  "has_update": true,
  "has_delete": true
}
```

- We grant `mathematics-support` (assume group ID 3) the following object permissions on the `mathematics` collection:

Endpoint: `POST /api/v1/permissions/objects/2/groups/3`

```json
{
  "has_read": true
}
```

## A word about inheritance and admin privileges

In the examples above, the central security group can either be granted access
directly on each department collection, or be granted access on a common parent
collection so those permissions inherit to the department collections. There is
still no implicit access granted to magic groups except for the `admin` group,
which is a special case. The `admin` group has full access to everything and is
intended for Hubuum system administrators only.
