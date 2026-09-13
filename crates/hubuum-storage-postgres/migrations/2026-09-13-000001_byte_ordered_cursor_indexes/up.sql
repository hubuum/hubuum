-- The cursor comparator uses byte ordering, independently of the database locale.
-- Preserve name-and-id ordering in both directions, including scoped name lookups.
-- Deploy during a quiet period: these builds take write locks. A timeout rolls
-- back the whole migration, so it can be retried without leaving partial indexes.
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

CREATE INDEX collections_name_c_id_idx ON public.collections (name COLLATE "C", id); -- hubuum-compat: bounded-transactional-index
CREATE INDEX collections_name_c_desc_id_idx ON public.collections (name COLLATE "C" DESC, id); -- hubuum-compat: bounded-transactional-index

CREATE INDEX collections_parent_name_c_id_idx ON public.collections (parent_collection_id, name COLLATE "C", id); -- hubuum-compat: bounded-transactional-index
CREATE INDEX collections_parent_name_c_desc_id_idx ON public.collections (parent_collection_id, name COLLATE "C" DESC, id); -- hubuum-compat: bounded-transactional-index

CREATE INDEX hubuumclass_name_c_id_idx ON public.hubuumclass (name COLLATE "C", id); -- hubuum-compat: bounded-transactional-index
CREATE INDEX hubuumclass_name_c_desc_id_idx ON public.hubuumclass (name COLLATE "C" DESC, id); -- hubuum-compat: bounded-transactional-index

CREATE INDEX hubuumclass_collection_name_c_id_idx ON public.hubuumclass (collection_id, name COLLATE "C", id); -- hubuum-compat: bounded-transactional-index
CREATE INDEX hubuumclass_collection_name_c_desc_id_idx ON public.hubuumclass (collection_id, name COLLATE "C" DESC, id); -- hubuum-compat: bounded-transactional-index

CREATE INDEX hubuumobject_name_c_id_idx ON public.hubuumobject (name COLLATE "C", id); -- hubuum-compat: bounded-transactional-index
CREATE INDEX hubuumobject_name_c_desc_id_idx ON public.hubuumobject (name COLLATE "C" DESC, id); -- hubuum-compat: bounded-transactional-index

CREATE INDEX hubuumobject_class_name_c_id_idx ON public.hubuumobject (hubuum_class_id, name COLLATE "C", id); -- hubuum-compat: bounded-transactional-index
CREATE INDEX hubuumobject_class_name_c_desc_id_idx ON public.hubuumobject (hubuum_class_id, name COLLATE "C" DESC, id); -- hubuum-compat: bounded-transactional-index

CREATE INDEX hubuumobject_collection_name_c_id_idx ON public.hubuumobject (collection_id, name COLLATE "C", id); -- hubuum-compat: bounded-transactional-index
CREATE INDEX hubuumobject_collection_name_c_desc_id_idx ON public.hubuumobject (collection_id, name COLLATE "C" DESC, id); -- hubuum-compat: bounded-transactional-index

CREATE INDEX groups_name_c_id_idx ON public.groups (groupname COLLATE "C", id); -- hubuum-compat: bounded-transactional-index
CREATE INDEX groups_name_c_desc_id_idx ON public.groups (groupname COLLATE "C" DESC, id); -- hubuum-compat: bounded-transactional-index

CREATE INDEX groups_scope_name_c_id_idx ON public.groups (identity_scope_id, groupname COLLATE "C", id); -- hubuum-compat: bounded-transactional-index
CREATE INDEX groups_scope_name_c_desc_id_idx ON public.groups (identity_scope_id, groupname COLLATE "C" DESC, id); -- hubuum-compat: bounded-transactional-index

CREATE INDEX principals_name_c_id_idx ON public.principals (name COLLATE "C", id); -- hubuum-compat: bounded-transactional-index
CREATE INDEX principals_name_c_desc_id_idx ON public.principals (name COLLATE "C" DESC, id); -- hubuum-compat: bounded-transactional-index

CREATE INDEX principals_scope_name_c_id_idx ON public.principals (identity_scope_id, name COLLATE "C", id); -- hubuum-compat: bounded-transactional-index
CREATE INDEX principals_scope_name_c_desc_id_idx ON public.principals (identity_scope_id, name COLLATE "C" DESC, id); -- hubuum-compat: bounded-transactional-index

CREATE INDEX identity_scopes_name_c_id_idx ON public.identity_scopes (name COLLATE "C", id); -- hubuum-compat: bounded-transactional-index
CREATE INDEX identity_scopes_name_c_desc_id_idx ON public.identity_scopes (name COLLATE "C" DESC, id); -- hubuum-compat: bounded-transactional-index

CREATE INDEX export_templates_collection_name_c_id_idx ON public.export_templates (collection_id, name COLLATE "C", id); -- hubuum-compat: bounded-transactional-index
CREATE INDEX export_templates_collection_name_c_desc_id_idx ON public.export_templates (collection_id, name COLLATE "C" DESC, id); -- hubuum-compat: bounded-transactional-index

CREATE INDEX remote_targets_collection_name_c_id_idx ON public.remote_targets (collection_id, name COLLATE "C", id); -- hubuum-compat: bounded-transactional-index
CREATE INDEX remote_targets_collection_name_c_desc_id_idx ON public.remote_targets (collection_id, name COLLATE "C" DESC, id); -- hubuum-compat: bounded-transactional-index

CREATE INDEX event_sinks_name_c_id_idx ON public.event_sinks (name COLLATE "C", id); -- hubuum-compat: bounded-transactional-index
CREATE INDEX event_sinks_name_c_desc_id_idx ON public.event_sinks (name COLLATE "C" DESC, id); -- hubuum-compat: bounded-transactional-index

CREATE INDEX event_subscriptions_collection_name_c_id_idx ON public.event_subscriptions (collection_id, name COLLATE "C", id); -- hubuum-compat: bounded-transactional-index
CREATE INDEX event_subscriptions_collection_name_c_desc_id_idx ON public.event_subscriptions (collection_id, name COLLATE "C" DESC, id); -- hubuum-compat: bounded-transactional-index

CREATE INDEX computed_fields_shared_name_c_id_idx ON public.computed_field_definitions (class_id, key COLLATE "C", id) WHERE visibility = 'shared'; -- hubuum-compat: bounded-transactional-index
CREATE INDEX computed_fields_shared_name_c_desc_id_idx ON public.computed_field_definitions (class_id, key COLLATE "C" DESC, id) WHERE visibility = 'shared'; -- hubuum-compat: bounded-transactional-index

CREATE INDEX computed_fields_personal_name_c_id_idx ON public.computed_field_definitions (owner_user_id, class_id, key COLLATE "C", id) WHERE visibility = 'personal'; -- hubuum-compat: bounded-transactional-index
CREATE INDEX computed_fields_personal_name_c_desc_id_idx ON public.computed_field_definitions (owner_user_id, class_id, key COLLATE "C" DESC, id) WHERE visibility = 'personal'; -- hubuum-compat: bounded-transactional-index
