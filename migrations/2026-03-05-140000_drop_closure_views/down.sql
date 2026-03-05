CREATE VIEW class_closure_view AS
SELECT
    cc.ancestor_class_id,
    cc.descendant_class_id,
    cc.depth,
    cc.path,
    ac.name AS ancestor_name,
    dc.name AS descendant_name,
    ac.namespace_id AS ancestor_namespace_id,
    dc.namespace_id AS descendant_namespace_id,
    ac.json_schema AS ancestor_json_schema,
    dc.json_schema AS descendant_json_schema,
    ac.validate_schema AS ancestor_validate_schema,
    dc.validate_schema AS descendant_validate_schema,
    ac.description AS ancestor_description,
    dc.description AS descendant_description,
    ac.created_at AS ancestor_created_at,
    dc.created_at AS descendant_created_at,
    ac.updated_at AS ancestor_updated_at,
    dc.updated_at AS descendant_updated_at
FROM hubuumclass_closure cc
JOIN hubuumclass ac ON cc.ancestor_class_id = ac.id
JOIN hubuumclass dc ON cc.descendant_class_id = dc.id;

CREATE VIEW object_closure_view AS
SELECT
    oc.ancestor_object_id,
    oc.descendant_object_id,
    oc.depth,
    oc.path,
    aob.name AS ancestor_name,
    dob.name AS descendant_name,
    aob.namespace_id AS ancestor_namespace_id,
    dob.namespace_id AS descendant_namespace_id,
    aob.hubuum_class_id AS ancestor_class_id,
    dob.hubuum_class_id AS descendant_class_id,
    aob.description AS ancestor_description,
    dob.description AS descendant_description,
    aob.data AS ancestor_data,
    dob.data AS descendant_data,
    aob.created_at AS ancestor_created_at,
    dob.created_at AS descendant_created_at,
    aob.updated_at AS ancestor_updated_at,
    dob.updated_at AS descendant_updated_at
FROM hubuumobject_closure oc
JOIN hubuumobject aob ON oc.ancestor_object_id = aob.id
JOIN hubuumobject dob ON oc.descendant_object_id = dob.id;
