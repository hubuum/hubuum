-- Drop views
DROP VIEW IF EXISTS object_closure_view;
DROP VIEW IF EXISTS class_closure_view;

-- Drop triggers
DROP TRIGGER IF EXISTS cleanup_object_relations ON hubuumclass_relation;
DROP TRIGGER IF EXISTS check_object_relation ON hubuumobject_relation;
DROP TRIGGER IF EXISTS maintain_object_closure ON hubuumobject_relation;
DROP TRIGGER IF EXISTS maintain_class_closure ON hubuumclass_relation;
DROP TRIGGER IF EXISTS update_hubuumobject_relation_updated_at ON hubuumobject_relation;
DROP TRIGGER IF EXISTS update_hubuumclass_relation_updated_at ON hubuumclass_relation;
DROP TRIGGER IF EXISTS update_hubuumobject_updated_at ON hubuumobject;
DROP TRIGGER IF EXISTS update_hubuumclass_updated_at ON hubuumclass;
DROP TRIGGER IF EXISTS update_permissions_updated_at ON permissions;
DROP TRIGGER IF EXISTS update_namespaces_updated_at ON namespaces;
DROP TRIGGER IF EXISTS update_user_groups_updated_at ON user_groups;
DROP TRIGGER IF EXISTS update_groups_updated_at ON groups;
DROP TRIGGER IF EXISTS update_users_updated_at ON users;
DROP TRIGGER IF EXISTS before_insert_or_update_object_relation ON hubuumobject_relation;
DROP TRIGGER IF EXISTS before_insert_or_update_class_relation ON hubuumclass_relation;

-- Drop functions
DROP FUNCTION IF EXISTS get_transitively_linked_objects(INT, INT, INT[]);
DROP FUNCTION IF EXISTS cleanup_invalid_object_relations();
DROP FUNCTION IF EXISTS get_affected_objects(INT, INT);
DROP FUNCTION IF EXISTS validate_object_relation();
DROP FUNCTION IF EXISTS are_classes_related(INT, INT);
DROP FUNCTION IF EXISTS update_object_closure();
DROP FUNCTION IF EXISTS update_class_closure();
DROP FUNCTION IF EXISTS enforce_object_relation_order();
DROP FUNCTION IF EXISTS enforce_class_relation_order();
DROP FUNCTION IF EXISTS update_modified_column();

-- Drop tables
DROP TABLE IF EXISTS hubuumobject_closure CASCADE;
DROP TABLE IF EXISTS hubuumclass_closure CASCADE;
DROP TABLE IF EXISTS hubuumobject_relation CASCADE;
DROP TABLE IF EXISTS hubuumclass_relation CASCADE;
DROP TABLE IF EXISTS hubuumobject CASCADE;
DROP TABLE IF EXISTS hubuumclass CASCADE;
DROP TABLE IF EXISTS permissions CASCADE;
DROP TABLE IF EXISTS namespaces CASCADE;
DROP TABLE IF EXISTS tokens CASCADE;
DROP TABLE IF EXISTS user_groups CASCADE;
DROP TABLE IF EXISTS groups CASCADE;
DROP TABLE IF EXISTS users CASCADE;

-- Drop indexes (not necessary if we're dropping the tables, but included for completeness)
DROP INDEX IF EXISTS idx_hubuumobject_closure_path;
DROP INDEX IF EXISTS idx_hubuumobject_closure_ancestor_descendant;
DROP INDEX IF EXISTS idx_hubuumobject_closure_descendant;
DROP INDEX IF EXISTS idx_hubuumobject_closure_ancestor;
DROP INDEX IF EXISTS idx_hubuumclass_closure_path;
DROP INDEX IF EXISTS idx_hubuumclass_closure_ancestor_descendant;
DROP INDEX IF EXISTS idx_hubuumclass_closure_descendant;
DROP INDEX IF EXISTS idx_hubuumclass_closure_ancestor;
DROP INDEX IF EXISTS idx_hubuumobject_relation_class_relation_id;
DROP INDEX IF EXISTS idx_hubuumobject_relation_on_from_to;
DROP INDEX IF EXISTS idx_hubuumclass_relation_on_from_to;
DROP INDEX IF EXISTS idx_permissions_group_id;
DROP INDEX IF EXISTS idx_permissions_namespace_id;
DROP INDEX IF EXISTS idx_hubuumobject_hubuum_class_id;
DROP INDEX IF EXISTS idx_hubuumobject_namespace_id;
DROP INDEX IF EXISTS idx_hubuumclass_namespace_id;
DROP INDEX IF EXISTS idx_tokens_user_id;
DROP INDEX IF EXISTS idx_namespaces_name;
DROP INDEX IF EXISTS idx_user_groups_group_id;
DROP INDEX IF EXISTS idx_user_groups_user_id;
DROP INDEX IF EXISTS idx_groups_groupname;
DROP INDEX IF EXISTS idx_users_username;