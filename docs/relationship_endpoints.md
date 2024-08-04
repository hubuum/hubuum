# Operations

## Contextual operations

### Class relations

| Operation | Method | Path | Description |
|-----------|--------|------|-------------|
| Create    | POST   | /classes/{class_id}/relations/{to_class_id} | Create a relation between two classes |
| Delete    | DELETE | /classes/{from_class_id}/relations/{to_class_id} | Delete a relation between two classes |
| List      | GET    | /classes/{class_id}/relations/ | List all class relations of a class. Should support filtering. |
| Get       | GET    | /classes/{from_class_id}/relations/{to_class_id} | Get a specific class relation |

### Object relations

// We must validate that there is a direct relation between class_id and the class of to_object_id, and use that relation
// as the class_relation_id for the object relation.

Note that the class_id defines the class of the first object. The class of the second object in inferred from the object.

| Operation | Method | Path | Description |
|-----------|--------|------|-------------|
| Create    | POST   | /classes/{class_id}/{object_id}/relations/{to_class_id}/{to_object_id} | Create a relation between two objects |
| Delete    | DELETE | /classes/{class_id}/{object_id}/relations/{to_class}/{to_object_id} | Delete a relation between two objects |
| List      | GET    | /classes/{class_id}/{object_id}/relations/ | List all objects an object is related to. Should support filtering on class and object IDs. |

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
