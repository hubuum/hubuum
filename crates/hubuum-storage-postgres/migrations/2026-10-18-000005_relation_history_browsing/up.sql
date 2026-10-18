-- Bound transactional index builds during deployment.
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

CREATE INDEX hubuumclass_relation_history_from_page_idx
ON hubuumclass_relation_history (from_hubuum_class_id, history_id)
INCLUDE (valid_from, valid_to, op); -- hubuum-compat: bounded-transactional-index

CREATE INDEX hubuumclass_relation_history_to_page_idx
ON hubuumclass_relation_history (to_hubuum_class_id, history_id)
INCLUDE (valid_from, valid_to, op); -- hubuum-compat: bounded-transactional-index

CREATE INDEX hubuumclass_relation_history_validity_idx
ON hubuumclass_relation_history USING gist (tstzrange(valid_from, valid_to, '[)'))
WHERE op <> 'D'; -- hubuum-compat: bounded-transactional-index

-- Endpoint lookup cannot constrain class membership before reading its history.
CREATE INDEX hubuumobject_history_endpoint_time_idx
ON hubuumobject_history (id, valid_from, history_id); -- hubuum-compat: bounded-transactional-index

CREATE INDEX hubuumobject_relation_history_from_page_idx
ON hubuumobject_relation_history (from_hubuum_object_id, history_id)
INCLUDE (valid_from, valid_to, op); -- hubuum-compat: bounded-transactional-index

CREATE INDEX hubuumobject_relation_history_to_page_idx
ON hubuumobject_relation_history (to_hubuum_object_id, history_id)
INCLUDE (valid_from, valid_to, op); -- hubuum-compat: bounded-transactional-index

CREATE INDEX hubuumobject_relation_history_validity_idx
ON hubuumobject_relation_history USING gist (tstzrange(valid_from, valid_to, '[)'))
WHERE op <> 'D'; -- hubuum-compat: bounded-transactional-index
