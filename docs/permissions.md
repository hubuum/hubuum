# Permission model

Hubuum divides user-created structures into classes and their objects. Objects are instances of classes, and both are contained within a single collection. A collection may contain multiple classes and objects.

Permissions within Hubuum are based on the following principles:

- Permissions are granted to groups (only). If one wishes to grant permissions to a specific user only, create a group with a single member.
- Permissions are granted on collections. Permissions are never granted to individual classes or objects.
- Permissions are not inherited from any structure to any other. If a user (through a group membership) has read access to a class, they do not automatically have read access to the objects of that class.

## Permission types

There are three types of permissions for each collection:

1. Permissions for the collections themselves
2. Permissions for classes
3. Permissions for objects

### Permissions for collections

The following permissions are available for collections:

| Permission | Description |
| ---------- | ----------- |
| `read_collection`     | Allows reading data about the collection, ie its members or the permissions associated with it. |
| `update_collection`   | Allows updating the collection (changing its name). |
| `delete_collection`   | Allows deleting the collection. |
| `create_collection`   | Allows creating collections within the collection. |
| `create_class`        | Allows creating classes within the collection. |
| `create_object`       | Allows creating objects within the collection. |
| `create_relationship` | Allows creating relationships of classes within the collection. |

The permission to grant groups (or users) access to the collection itself is done by the parent collection. Every collection has a parent collection and the root collection is created when the Hubuum instance is created.

### Permissions for classes

The following permissions are available for classes:

| Permission | Description |
| ---------- | ----------- |
| `read_class`     | Allows reading the class. |
| `update_class`   | Allows updating the class (ie, change its name, its definition, validation requirements, etc). |
| `delete_class`   | Allows deleting the class. Note that deleting a class deletes all objects belonging to that class. |
| `create_object`  | Allows creating new objects of the class. |

### Permissions for objects

The following permissions are available for objects:

| Permission | Description |
| ---------- | ----------- |
| `read_object`     | Allows reading the object. |
| `update_object`   | Allows updating the object. |
| `delete_object`   | Allows deleting the object. |

### Permissions for class relationships

The following permissions are available for relationships between classes:

| Permission | Description |
| ---------- | ----------- |
| `read_class_relationship`     | Allows reading the relationship. |
| `update_class_relationship`   | Allows updating the relationship. |
| `delete_class_relationship`   | Allows deleting the relationship. |
| `create_object_relationship`   | Allows creating relationships between objects adhering of the class relationship. |

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

In the examples above we have to explicitly grant the central security group access to a new collection. This is by design. There is no inheritance of permissions from one collection to another and no implicit access granted to magic groups -- except for the `admin` group, which is a special case. The `admin` group has full access to everything, and is intended for use by the Hubuum system administrators only.
