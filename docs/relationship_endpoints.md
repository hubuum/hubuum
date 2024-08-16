# Operations

## Contextual operations

### Class relations

| Operation | Method | Path | Description |
|-----------|--------|------|-------------|
| Get       | GET    | /classes/{from_class_id}/relations/{to_class_id} | Get a specific class relation |
| Create    | POST   | /classes/{class_id}/relations/{to_class_id} | Create a relation between two classes |
| Delete    | DELETE | /classes/{from_class_id}/relations/{to_class_id} | Delete a relation between two classes |
| List      | GET    | /classes/{class_id}/relations/ | List all relations of a class |
| List      | GET    | /classes/{class_id}/related_classes/ | List all classes a class is related to |

### Object relations

Note that if the objects are not of the class ID preceeding the object ID, the operation will return the status code 400.

| Operation | Method | Path | Description |
|-----------|--------|------|-------------|
| Get       | GET    | /classes/{class_id}/{object_id}/relations/{to_class_id}/{to_object_id} | Get a specific relation between two objects |
| Create    | POST   | /classes/{class_id}/{object_id}/relations/{to_class_id}/{to_object_id} | Create a relation between two objects |
| Delete    | DELETE | /classes/{class_id}/{object_id}/relations/{to_class}/{to_object_id} | Delete a relation between two objects |
| List      | GET    | /classes/{class_id}/{object_id}/relations/ | List all relations of an object |
| List      | GET    | /classes/{class_id}/{object_id}/related_objects/ | List all objects an object is related to |

#### Filter support for list operations

- `to_objects` - INT - Destination objects IDs
- `to_classes` - INT - Destination classes IDs
- `to_name` - STRING - Destination object names
- `to_description` - STRING - Destination object descriptions
- `to_json_data` - JSON - Destination object JSON data
- `to_namespaces` - STRING - Destination object namespaces
- `to_created_at` - DATETIME - Destination object creation date
- `to_updated_at` - DATETIME - Destination object update date
- `depth` - INT - Depth of the relation
- `path` - ARRAY - Path of the relation

See [querying.md](querying.md) for more information on filtering and the available operators for each field type.

## Context free operations

### Class relations

| Operation | Method | Path | Description |
|-----------|--------|------|-------------|
| Create    | POST   | /class_relations/ | Create a relation between two classes |
| Delete    | DELETE | /class_relations/{relation_id} | Delete a relation between two classes |
| List      | GET    | /class_relations/ | List all class relations. Should support filtering. |
| Get       | GET    | /class_relations/{relation_id} | Get a specific class relation |

### Object relations

| Operation | Method | Path | Description |
|-----------|--------|------|-------------|
| Create    | POST   | /object_relations/ | Create a relation between two objects. Needs four parameters (`from_class`, `from_object`, `to_class`, `to_object`) |
| Delete    | DELETE | /object_relations/{relation_id} | Delete a relation between two objects |
| List      | GET    | /object_relations/ | List all object relations. Should support filtering. |
| Get       | GET    | /object_relations/{relation_id} | Get a specific object relation |
